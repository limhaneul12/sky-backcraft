//! Durable scheduler recovery transitions and bounded backfill ownership.

use super::{required_schedule, timestamp_from_ms};
use crate::contracts::{
    AttemptId, AttemptState, CollectionScheduleRecord, CollectionScheduleStatus, DatasetId,
    FailureRecord, JobId, JobOutput, JobPayload, JobSubmission, LabError, ScheduleFailureClass,
    ScheduleId, ScheduleRecoveryState, UtcRange, UtcTimestamp,
};
use crate::scheduling::{classify_schedule_failure, recovery_chunk_request, recovery_gap};
use crate::storage::{Store, enum_text, json_error, sql_error, timestamp_ms};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

const PROBE_LEASE_SECONDS: u32 = 60;

#[derive(Debug)]
pub(super) struct StoredRecovery {
    pub failure_class: Option<ScheduleFailureClass>,
    pub state: ScheduleRecoveryState,
    pub attempt_count: u32,
    pub last_probe_at: Option<UtcTimestamp>,
    pub last_recovery_at: Option<UtcTimestamp>,
    pub pending_gap: Option<UtcRange>,
    pub backfill_job_id: Option<JobId>,
    pub backfill_attempt_id: Option<AttemptId>,
    pub next_recovery_at: Option<UtcTimestamp>,
    pub next_chunk_start: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryChunkAdmission {
    target: UtcTimestamp,
    full_gap: UtcRange,
    next_start: UtcTimestamp,
    chunk_index: u32,
    generation: u32,
}

impl Store {
    /// Return bounded recovery work that is due. An expired `PROBING` lease is
    /// included so a process restart cannot strand recovery or spawn a second
    /// untracked operation.
    ///
    /// # Errors
    /// Rejects invalid scan bounds or corrupt/missing persisted recovery rows.
    pub fn schedule_recovery_candidates(
        &self,
        now: UtcTimestamp,
        limit: u32,
    ) -> Result<Vec<CollectionScheduleRecord>, LabError> {
        super::validate_active_scan(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT r.schedule_id FROM schedule_recoveries r \
                 JOIN collection_schedules s ON s.id=r.schedule_id \
                 WHERE s.status='active' AND r.failure_class='recoverable' \
                 AND r.state IN ('recovery_wait','probing') \
                 AND r.next_recovery_at_ms<=?1 \
                 ORDER BY r.next_recovery_at_ms,r.schedule_id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let ids = statement
            .query_map(params![timestamp_ms(now), i64::from(limit)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        ids.into_iter()
            .map(|id| required_schedule(&self.connection, &ScheduleId::new(id)?))
            .collect()
    }

    /// Recovery backfills and verification are reconciled by the producer
    /// owner; they never create a second worker.
    ///
    /// # Errors
    /// Rejects invalid scan bounds or malformed persisted schedule identities.
    pub fn schedule_recovery_reconciliation_candidates(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduleId>, LabError> {
        super::validate_active_scan(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT r.schedule_id FROM schedule_recoveries r \
                 JOIN collection_schedules s ON s.id=r.schedule_id \
                 WHERE r.state IN ('backfilling','verifying_freshness') \
                 ORDER BY r.updated_at_ms,r.schedule_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        statement
            .query_map([limit], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .map(|row| ScheduleId::new(row.map_err(sql_error)?))
            .collect()
    }

    /// Claim one due probe with a durable lease before network I/O begins.
    ///
    /// # Errors
    /// Returns conflict when the candidate is stale, or a storage/contract error.
    pub fn claim_schedule_recovery_probe(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let next = add_seconds(now, PROBE_LEASE_SECONDS)?;
        let changed = self
            .connection
            .execute(
                "UPDATE schedule_recoveries SET state='probing',attempt_count=attempt_count+1,\
                 last_probe_at_ms=?1,next_recovery_at_ms=?2,updated_at_ms=?1 \
                 WHERE schedule_id=?3 AND failure_class='recoverable' \
                 AND state IN ('recovery_wait','probing') \
                 AND next_recovery_at_ms<=?1 \
                 AND EXISTS(SELECT 1 FROM collection_schedules s \
                            WHERE s.id=schedule_id AND s.status='active')",
                params![timestamp_ms(now), timestamp_ms(next), schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "schedule recovery probe is no longer due".into(),
            ));
        }
        required_schedule(&self.connection, schedule_id)
    }

    /// Retry the same failed recovery chunk after its durable wait. The
    /// content-stable job is repinned to a new attempt; no new chunk/job is
    /// created.
    ///
    /// # Errors
    /// Rejects stale waits, corrupt pins, queue capacity, or storage failure.
    pub fn retry_schedule_recovery_backfill(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        let recovery = load_recovery(&transaction, schedule_id)?;
        if schedule.status != CollectionScheduleStatus::Active
            || recovery.state != ScheduleRecoveryState::RecoveryWait
            || recovery.next_recovery_at.is_none_or(|at| at > now)
        {
            return Err(LabError::Conflict(
                "schedule recovery backfill retry is not due".into(),
            ));
        }
        let job_id = recovery.backfill_job_id.clone().ok_or_else(|| {
            LabError::DataCorrupt("recovery backfill wait has no pinned job".into())
        })?;
        continue_backfill_tx(&transaction, &schedule, &recovery, &job_id, now)?;
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Persist a recoverable probe failure and bounded exponential backoff.
    ///
    /// # Errors
    /// Returns invalid recovery state, timestamp overflow, or storage failure.
    pub fn defer_schedule_recovery(
        &mut self,
        schedule_id: &ScheduleId,
        failure: &FailureRecord,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        if classify_schedule_failure(failure) != ScheduleFailureClass::Recoverable {
            return self.require_schedule_operator(schedule_id, failure, now);
        }
        let attempt: u32 = self
            .connection
            .query_row(
                "SELECT attempt_count FROM schedule_recoveries WHERE schedule_id=?1",
                [schedule_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let next = add_seconds(now, recovery_backoff_seconds(attempt))?;
        self.connection
            .execute(
                "UPDATE schedule_recoveries SET failure_class='recoverable',\
                 state='recovery_wait',next_recovery_at_ms=?1,updated_at_ms=?2 \
                 WHERE schedule_id=?3 AND state IN ('probing','recovery_wait')",
                params![timestamp_ms(next), timestamp_ms(now), schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        self.connection
            .execute(
                "UPDATE collection_schedules SET failure_json=?1,\
                 next_action_at_ms=?2 WHERE id=?3",
                params![
                    serde_json::to_string(failure).map_err(json_error)?,
                    timestamp_ms(next),
                    schedule_id.as_str()
                ],
            )
            .map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Freeze the common all-market target and atomically pin the first
    /// bounded ordinary collection job. Repeating after a lost response reads
    /// the already-durable state instead of creating a duplicate.
    ///
    /// # Errors
    /// Rejects inactive/stale recovery, invalid gap geometry, capacity, or storage failure.
    pub fn begin_schedule_recovery_backfill(
        &mut self,
        schedule_id: &ScheduleId,
        target: UtcTimestamp,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        if schedule.recovery_state == ScheduleRecoveryState::Backfilling
            || schedule.recovery_state == ScheduleRecoveryState::VerifyingFreshness
        {
            return Ok(schedule);
        }
        if schedule.status != CollectionScheduleStatus::Active {
            return Err(LabError::Conflict(
                "paused schedule cannot admit recovery backfill".into(),
            ));
        }
        if schedule.recovery_state != ScheduleRecoveryState::Probing {
            return Err(LabError::Conflict(
                "schedule recovery is not in probing state".into(),
            ));
        }
        let gap = recovery_gap(&schedule, target)?;
        match gap {
            None => {
                let verify = verification_range(&schedule, target)?;
                transaction
                    .execute(
                        "UPDATE schedule_recoveries SET state='verifying_freshness',\
                         gap_start_ms=?1,gap_end_ms=?2,next_chunk_start_ms=?2,\
                         next_recovery_at_ms=NULL,updated_at_ms=?3 WHERE schedule_id=?4",
                        params![
                            timestamp_ms(verify.start()),
                            timestamp_ms(verify.end()),
                            timestamp_ms(now),
                            schedule_id.as_str()
                        ],
                    )
                    .map_err(sql_error)?;
            }
            Some(range) => {
                admit_chunk_tx(
                    &transaction,
                    &schedule,
                    RecoveryChunkAdmission {
                        target,
                        full_gap: range,
                        next_start: range.start(),
                        chunk_index: 0,
                        generation: 0,
                    },
                    now,
                )?;
            }
        }
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Reconcile the pinned recovery job and advance one bounded chunk at a
    /// time. Verification reads committed candle rows across every owned
    /// dataset before returning to `ACTIVE`.
    ///
    /// # Errors
    /// Returns corrupt pins/results, invalid transition geometry, or storage failure.
    pub fn reconcile_schedule_recovery(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        match schedule.recovery_state {
            ScheduleRecoveryState::Backfilling => {
                reconcile_backfill_tx(&transaction, &schedule, now)?;
            }
            ScheduleRecoveryState::VerifyingFreshness => {
                verify_freshness_tx(&transaction, &schedule, now)?;
            }
            ScheduleRecoveryState::Active
            | ScheduleRecoveryState::Degraded
            | ScheduleRecoveryState::RecoveryWait
            | ScheduleRecoveryState::Probing
            | ScheduleRecoveryState::OperatorRequired => return Ok(schedule),
        }
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    fn require_schedule_operator(
        &mut self,
        schedule_id: &ScheduleId,
        failure: &FailureRecord,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        record_failure_tx(&transaction, schedule_id, failure, now)?;
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }
}

pub(super) fn load_recovery(
    connection: &Connection,
    schedule_id: &ScheduleId,
) -> Result<StoredRecovery, LabError> {
    let row = connection
        .query_row(
            "SELECT failure_class,state,attempt_count,last_probe_at_ms,last_recovery_at_ms,\
             gap_start_ms,gap_end_ms,next_chunk_start_ms,backfill_job_id,\
             backfill_attempt_id,next_recovery_at_ms \
             FROM schedule_recoveries WHERE schedule_id=?1",
            [schedule_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("schedule recovery row is missing".into()))?;
    let (
        class,
        state,
        attempts,
        last_probe,
        last_recovery,
        gap_start,
        gap_end,
        next_chunk,
        job,
        attempt,
        next,
    ) = row;
    let pending_gap = match (gap_start, gap_end) {
        (Some(start), Some(end)) => Some(UtcRange::new(
            timestamp_from_ms(start)?,
            timestamp_from_ms(end)?,
        )?),
        (None, None) => None,
        _ => {
            return Err(LabError::DataCorrupt(
                "partial schedule recovery gap".into(),
            ));
        }
    };
    Ok(StoredRecovery {
        failure_class: class.map(|value| parse_failure_class(&value)).transpose()?,
        state: parse_recovery_state(&state)?,
        attempt_count: attempts,
        last_probe_at: last_probe.map(timestamp_from_ms).transpose()?,
        last_recovery_at: last_recovery.map(timestamp_from_ms).transpose()?,
        pending_gap,
        backfill_job_id: job.map(JobId::new).transpose()?,
        backfill_attempt_id: attempt.map(AttemptId::new).transpose()?,
        next_recovery_at: next.map(timestamp_from_ms).transpose()?,
        next_chunk_start: next_chunk.map(timestamp_from_ms).transpose()?,
    })
}

pub(super) fn record_degraded_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    retry_at: UtcTimestamp,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class='recoverable',state='degraded',\
             next_recovery_at_ms=?1,updated_at_ms=?2 WHERE schedule_id=?3",
            params![
                timestamp_ms(retry_at),
                timestamp_ms(now),
                schedule_id.as_str()
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

pub(super) fn record_failure_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    failure: &FailureRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let class = classify_schedule_failure(failure);
    let failure_json = serde_json::to_string(failure).map_err(json_error)?;
    match class {
        ScheduleFailureClass::Recoverable => {
            let attempt: u32 = transaction
                .query_row(
                    "SELECT attempt_count FROM schedule_recoveries WHERE schedule_id=?1",
                    [schedule_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let next = add_seconds(now, recovery_backoff_seconds(attempt))?;
            transaction
                .execute(
                    "UPDATE collection_schedules SET status=CASE WHEN status='paused' \
                     THEN 'paused' ELSE 'active' END,failure_json=?1,\
                     next_action_at_ms=?2 WHERE id=?3",
                    params![failure_json, timestamp_ms(next), schedule_id.as_str()],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE schedule_recoveries SET failure_class='recoverable',\
                     state='recovery_wait',next_recovery_at_ms=?1,\
                     backfill_job_id=NULL,backfill_attempt_id=NULL,updated_at_ms=?2 \
                     WHERE schedule_id=?3",
                    params![timestamp_ms(next), timestamp_ms(now), schedule_id.as_str()],
                )
                .map_err(sql_error)?;
        }
        ScheduleFailureClass::OperatorRequired => {
            transaction
                .execute(
                    "UPDATE collection_schedules SET status=CASE WHEN status='paused' \
                     THEN 'paused' ELSE 'blocked' END,failure_json=?1,\
                     next_action_at_ms=?2 WHERE id=?3",
                    params![failure_json, timestamp_ms(now), schedule_id.as_str()],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE schedule_recoveries SET failure_class='operator_required',\
                     state='operator_required',next_recovery_at_ms=NULL,\
                     backfill_job_id=NULL,backfill_attempt_id=NULL,updated_at_ms=?1 \
                     WHERE schedule_id=?2",
                    params![timestamp_ms(now), schedule_id.as_str()],
                )
                .map_err(sql_error)?;
        }
    }
    Ok(())
}

pub(super) fn reset_active_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class=NULL,state='active',attempt_count=0,\
             gap_start_ms=NULL,gap_end_ms=NULL,next_chunk_start_ms=NULL,\
             backfill_job_id=NULL,backfill_attempt_id=NULL,next_recovery_at_ms=NULL,\
             updated_at_ms=?1 WHERE schedule_id=?2",
            params![timestamp_ms(now), schedule_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

pub(super) fn pause_recovery_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    now: UtcTimestamp,
) -> Result<Vec<JobId>, LabError> {
    let recovery = load_recovery(transaction, schedule_id)?;
    let Some(job_id) = recovery.backfill_job_id else {
        return Ok(Vec::new());
    };
    let Some(attempt_id) = recovery.backfill_attempt_id else {
        return Err(LabError::DataCorrupt(
            "recovery backfill job has no attempt".into(),
        ));
    };
    let state = super::load_attempt_state(transaction, &attempt_id)?;
    super::super::job_store::request_cancel_tx(transaction, &job_id, now)?;
    let mut running = Vec::new();
    match state {
        AttemptState::Queued { .. } => {
            transaction
                .execute(
                    "UPDATE schedule_recovery_chunks SET status='cancelled' \
                     WHERE job_id=?1 AND attempt_id=?2",
                    params![job_id.as_str(), attempt_id.as_str()],
                )
                .map_err(sql_error)?;
            pause_wait_tx(transaction, schedule_id, now)?;
        }
        AttemptState::Running { .. } => running.push(job_id),
        _ => pause_wait_tx(transaction, schedule_id, now)?,
    }
    Ok(running)
}

pub(super) fn resume_recovery_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    let recovery = load_recovery(transaction, schedule_id)?;
    match recovery.state {
        ScheduleRecoveryState::Active => Ok(false),
        ScheduleRecoveryState::OperatorRequired => {
            reset_active_tx(transaction, schedule_id, now)?;
            Ok(false)
        }
        ScheduleRecoveryState::Backfilling => {
            let Some(job_id) = recovery.backfill_job_id.clone() else {
                move_to_wait_tx(transaction, schedule_id, now)?;
                return Ok(true);
            };
            let schedule = required_schedule(transaction, schedule_id)?;
            continue_backfill_tx(transaction, &schedule, &recovery, &job_id, now)?;
            Ok(true)
        }
        ScheduleRecoveryState::RecoveryWait => {
            if let Some(job_id) = recovery.backfill_job_id.clone() {
                let schedule = required_schedule(transaction, schedule_id)?;
                continue_backfill_tx(transaction, &schedule, &recovery, &job_id, now)?;
            } else {
                move_to_wait_tx(transaction, schedule_id, now)?;
            }
            Ok(true)
        }
        ScheduleRecoveryState::Degraded
        | ScheduleRecoveryState::Probing
        | ScheduleRecoveryState::VerifyingFreshness => {
            move_to_wait_tx(transaction, schedule_id, now)?;
            Ok(true)
        }
    }
}

fn admit_chunk_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    chunk: RecoveryChunkAdmission,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let request = recovery_chunk_request(
        schedule,
        chunk.target,
        chunk.next_start,
        chunk.chunk_index,
        chunk.generation,
    )?;
    request.validate(now)?;
    let submission = JobSubmission {
        request_id: request.request_id.clone(),
        payload: JobPayload::Collect {
            request: request.clone(),
        },
    };
    let pin = super::super::job_store::insert_job_tx(transaction, &submission, now)?;
    transaction
        .execute(
            "INSERT INTO schedule_recovery_chunks(\
             schedule_id,target_ms,chunk_index,generation,range_start_ms,range_end_ms,request_id,status,\
             job_id,attempt_id,dataset_id,failure_json,created_at_ms\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,'queued',?8,?9,NULL,NULL,?10)",
            params![
                schedule.id.as_str(),
                timestamp_ms(chunk.target),
                chunk.chunk_index,
                chunk.generation,
                timestamp_ms(request.range.start()),
                timestamp_ms(request.range.end()),
                request.request_id.as_str(),
                pin.job_id.as_str(),
                pin.attempt_id.as_str(),
                timestamp_ms(now),
            ],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class='recoverable',state='backfilling',\
             gap_start_ms=?1,gap_end_ms=?2,next_chunk_start_ms=?3,backfill_job_id=?4,\
             backfill_attempt_id=?5,backfill_generation=?6,next_recovery_at_ms=NULL,\
             updated_at_ms=?7 WHERE schedule_id=?8",
            params![
                timestamp_ms(chunk.full_gap.start()),
                timestamp_ms(chunk.full_gap.end()),
                timestamp_ms(request.range.end()),
                pin.job_id.as_str(),
                pin.attempt_id.as_str(),
                chunk.generation,
                timestamp_ms(now),
                schedule.id.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn reconcile_backfill_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let recovery = load_recovery(transaction, &schedule.id)?;
    let job_id = recovery
        .backfill_job_id
        .clone()
        .ok_or_else(|| LabError::DataCorrupt("backfill state has no job".into()))?;
    let attempt_id = recovery
        .backfill_attempt_id
        .clone()
        .ok_or_else(|| LabError::DataCorrupt("backfill state has no attempt".into()))?;
    let state = super::load_attempt_state(transaction, &attempt_id)?;
    match state {
        AttemptState::Queued { .. } => update_chunk_status(transaction, &job_id, "queued"),
        AttemptState::Running { .. } => update_chunk_status(transaction, &job_id, "running"),
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            let JobOutput::Dataset { dataset_id } = output else {
                return Err(LabError::DataCorrupt(
                    "schedule recovery completed with non-dataset output".into(),
                ));
            };
            reconcile_completed_backfill_tx(
                transaction,
                schedule,
                &recovery,
                &job_id,
                &attempt_id,
                &dataset_id,
                now,
            )
        }
        AttemptState::Failed { error, .. } | AttemptState::Blocked { reason: error, .. } => {
            transaction
                .execute(
                    "UPDATE schedule_recovery_chunks SET status='failed',failure_json=?1 \
                     WHERE job_id=?2 AND attempt_id=?3",
                    params![
                        serde_json::to_string(&error).map_err(json_error)?,
                        job_id.as_str(),
                        attempt_id.as_str()
                    ],
                )
                .map_err(sql_error)?;
            if classify_schedule_failure(&error) == ScheduleFailureClass::Recoverable {
                backfill_retry_wait_tx(transaction, &schedule.id, &error, now)
            } else {
                record_failure_tx(transaction, &schedule.id, &error, now)
            }
        }
        AttemptState::Cancelled { reason, .. } | AttemptState::Interrupted { reason, .. } => {
            let failure = FailureRecord {
                code: "TEMPORARILY_BLOCKED".into(),
                message: reason,
            };
            transaction
                .execute(
                    "UPDATE schedule_recovery_chunks SET status='cancelled',failure_json=?1 \
                     WHERE job_id=?2 AND attempt_id=?3",
                    params![
                        serde_json::to_string(&failure).map_err(json_error)?,
                        job_id.as_str(),
                        attempt_id.as_str()
                    ],
                )
                .map_err(sql_error)?;
            if schedule.status == CollectionScheduleStatus::Paused {
                return pause_wait_tx(transaction, &schedule.id, now);
            }
            continue_backfill_tx(transaction, schedule, &recovery, &job_id, now)?;
            Ok(())
        }
    }
}

fn reconcile_completed_backfill_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    recovery: &StoredRecovery,
    job_id: &JobId,
    attempt_id: &AttemptId,
    dataset_id: &DatasetId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recovery_chunks SET status='completed',dataset_id=?1,\
             failure_json=NULL WHERE job_id=?2 AND attempt_id=?3",
            params![dataset_id.as_str(), job_id.as_str(), attempt_id.as_str()],
        )
        .map_err(sql_error)?;
    let gap = recovery
        .pending_gap
        .ok_or_else(|| LabError::DataCorrupt("backfill state has no gap".into()))?;
    let next_start = recovery
        .next_chunk_start
        .ok_or_else(|| LabError::DataCorrupt("backfill state has no cursor".into()))?;
    if next_start < gap.end() {
        let next_index: u32 = transaction
            .query_row(
                "SELECT COALESCE(MAX(chunk_index),-1)+1 FROM schedule_recovery_chunks \
                 WHERE schedule_id=?1 AND target_ms=?2",
                params![schedule.id.as_str(), timestamp_ms(gap.end())],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        admit_chunk_tx(
            transaction,
            schedule,
            RecoveryChunkAdmission {
                target: gap.end(),
                full_gap: gap,
                next_start,
                chunk_index: next_index,
                generation: 0,
            },
            now,
        )?;
    } else {
        transaction
            .execute(
                "UPDATE schedule_recoveries SET state='verifying_freshness',\
                 backfill_job_id=NULL,backfill_attempt_id=NULL,updated_at_ms=?1 \
                 WHERE schedule_id=?2",
                params![timestamp_ms(now), schedule.id.as_str()],
            )
            .map_err(sql_error)?;
    }
    Ok(())
}

fn verify_freshness_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let recovery = load_recovery(transaction, &schedule.id)?;
    let gap = match recovery.pending_gap {
        Some(gap) => gap,
        None => recovery_gap(
            schedule,
            crate::scheduling::latest_completed_boundary(now, schedule.request.interval)?,
        )?
        .ok_or_else(|| LabError::DataCorrupt("recovery verification has no range".into()))?,
    };
    let expected = u64::try_from(gap.bars(schedule.request.interval)?)
        .map_err(|_| LabError::ResourceLimit("recovery verification rows overflow".into()))?;
    let interval = enum_text(&schedule.request.interval)?;
    for market in &schedule.request.markets {
        let (latest, rows): (Option<i64>, i64) = transaction
            .query_row(
                "WITH owned_datasets(dataset_id) AS (\
                   SELECT last_success_dataset_id FROM collection_schedules \
                   WHERE id=?1 AND last_success_dataset_id IS NOT NULL \
                   UNION SELECT dataset_id FROM schedule_fires \
                   WHERE schedule_id=?1 AND status='completed' AND dataset_id IS NOT NULL \
                   UNION SELECT dataset_id FROM schedule_recovery_chunks \
                   WHERE schedule_id=?1 AND status='completed' AND dataset_id IS NOT NULL\
                 ) SELECT MAX(o.close_time_ms),COUNT(DISTINCT o.open_time_ms) \
                 FROM owned_datasets d JOIN dataset_members m ON m.dataset_id=d.dataset_id \
                 JOIN candle_observations o ON o.id=m.observation_id \
                 WHERE o.market=?2 AND o.interval=?3 AND o.completed=1 \
                 AND o.open_time_ms>=?4 AND o.close_time_ms<=?5",
                params![
                    schedule.id.as_str(),
                    market.code(),
                    &interval,
                    timestamp_ms(gap.start()),
                    timestamp_ms(gap.end()),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sql_error)?;
        let present = u64::try_from(rows)
            .map_err(|_| LabError::DataCorrupt("negative recovery row count".into()))?;
        if latest != Some(timestamp_ms(gap.end())) || present != expected {
            let failure = FailureRecord {
                code: "CONTRACT_VIOLATION".into(),
                message: format!(
                    "recovery verification failed for {}: expected_end={} latest={:?} gap_count={}",
                    market.code(),
                    gap.end(),
                    latest.map(timestamp_from_ms).transpose()?,
                    expected.saturating_sub(present)
                ),
            };
            return record_failure_tx(transaction, &schedule.id, &failure, now);
        }
    }
    let latest_dataset: Option<String> = transaction
        .query_row(
            "SELECT dataset_id FROM schedule_recovery_chunks \
             WHERE schedule_id=?1 AND status='completed' AND dataset_id IS NOT NULL \
             ORDER BY target_ms DESC,chunk_index DESC LIMIT 1",
            [schedule.id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let dataset = latest_dataset
        .map(DatasetId::new)
        .transpose()?
        .or_else(|| schedule.last_success_dataset_id.clone())
        .ok_or_else(|| LabError::DataCorrupt("verified recovery has no dataset".into()))?;
    let next = crate::scheduling::next_action_after_cadence(now, schedule.request.cadence_seconds)?;
    transaction
        .execute(
            "UPDATE collection_schedules SET status='active',last_success_dataset_id=?1,\
             last_success_boundary_ms=?2,next_action_at_ms=?3,failure_json=NULL WHERE id=?4",
            params![
                dataset.as_str(),
                timestamp_ms(gap.end()),
                timestamp_ms(next),
                schedule.id.as_str()
            ],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class=NULL,state='active',attempt_count=0,\
             last_recovery_at_ms=?1,gap_start_ms=NULL,gap_end_ms=NULL,next_chunk_start_ms=NULL,\
             backfill_job_id=NULL,backfill_attempt_id=NULL,next_recovery_at_ms=NULL,\
             updated_at_ms=?1 WHERE schedule_id=?2",
            params![timestamp_ms(now), schedule.id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn move_to_wait_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class='recoverable',state='recovery_wait',\
             backfill_job_id=NULL,backfill_attempt_id=NULL,next_recovery_at_ms=?1,\
             updated_at_ms=?1 WHERE schedule_id=?2",
            params![timestamp_ms(now), schedule_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn pause_wait_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class='recoverable',state='recovery_wait',\
             next_recovery_at_ms=NULL,updated_at_ms=?1 WHERE schedule_id=?2",
            params![timestamp_ms(now), schedule_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn backfill_retry_wait_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    failure: &FailureRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let attempt: u32 = transaction
        .query_row(
            "SELECT attempt_count FROM schedule_recoveries WHERE schedule_id=?1",
            [schedule_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let next = add_seconds(now, recovery_backoff_seconds(attempt))?;
    transaction
        .execute(
            "UPDATE collection_schedules SET failure_json=?1,next_action_at_ms=?2 \
             WHERE id=?3",
            params![
                serde_json::to_string(failure).map_err(json_error)?,
                timestamp_ms(next),
                schedule_id.as_str()
            ],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "UPDATE schedule_recoveries SET failure_class='recoverable',state='recovery_wait',\
             next_recovery_at_ms=?1,updated_at_ms=?2 WHERE schedule_id=?3",
            params![timestamp_ms(next), timestamp_ms(now), schedule_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn repin_backfill_tx(
    transaction: &Transaction<'_>,
    schedule_id: &ScheduleId,
    job_id: &JobId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let attempt_id = super::super::job_store::retry_job_tx(transaction, job_id, now)?;
    transaction
        .execute(
            "UPDATE schedule_recovery_chunks SET status='queued',attempt_id=?1,\
             failure_json=NULL WHERE job_id=?2",
            params![attempt_id.as_str(), job_id.as_str()],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "UPDATE schedule_recoveries SET state='backfilling',attempt_count=attempt_count+1,\
             backfill_attempt_id=?1,next_recovery_at_ms=NULL,updated_at_ms=?2 \
             WHERE schedule_id=?3",
            params![attempt_id.as_str(), timestamp_ms(now), schedule_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn continue_backfill_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    recovery: &StoredRecovery,
    job_id: &JobId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let attempt_number: u32 = transaction
        .query_row(
            "SELECT current_attempt_number FROM jobs WHERE id=?1",
            [job_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if attempt_number >= super::super::job_store::MAX_JOB_ATTEMPTS {
        rotate_backfill_job_tx(transaction, schedule, recovery, job_id, now)
    } else {
        repin_backfill_tx(transaction, &schedule.id, job_id, now)
    }
}

fn rotate_backfill_job_tx(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    recovery: &StoredRecovery,
    exhausted_job_id: &JobId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let (target_ms, chunk_index, generation, range_start_ms): (i64, u32, u32, i64) = transaction
        .query_row(
            "SELECT target_ms,chunk_index,generation,range_start_ms \
                 FROM schedule_recovery_chunks WHERE job_id=?1",
            [exhausted_job_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(sql_error)?;
    let full_gap = recovery
        .pending_gap
        .ok_or_else(|| LabError::DataCorrupt("rotating backfill has no frozen gap".into()))?;
    let next_generation = generation
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("recovery generation overflow".into()))?;
    admit_chunk_tx(
        transaction,
        schedule,
        RecoveryChunkAdmission {
            target: timestamp_from_ms(target_ms)?,
            full_gap,
            next_start: timestamp_from_ms(range_start_ms)?,
            chunk_index,
            generation: next_generation,
        },
        now,
    )?;
    transaction
        .execute(
            "UPDATE schedule_recoveries SET attempt_count=attempt_count+1 \
             WHERE schedule_id=?1",
            [schedule.id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn update_chunk_status(
    transaction: &Transaction<'_>,
    job_id: &JobId,
    status: &str,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_recovery_chunks SET status=?1 WHERE job_id=?2",
            params![status, job_id.as_str()],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn add_seconds(now: UtcTimestamp, seconds: u32) -> Result<UtcTimestamp, LabError> {
    now.0
        .checked_add_signed(chrono::Duration::seconds(i64::from(seconds)))
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("schedule recovery timestamp overflow".into()))
}

fn verification_range(
    schedule: &CollectionScheduleRecord,
    target: UtcTimestamp,
) -> Result<UtcRange, LabError> {
    let duration = schedule
        .request
        .interval
        .duration()
        .checked_mul(i32::try_from(schedule.request.lookback_bars).map_err(|_| {
            LabError::ResourceLimit("schedule verification lookback exceeds i32".into())
        })?)
        .ok_or_else(|| LabError::ResourceLimit("schedule verification range overflow".into()))?;
    let start = target
        .0
        .checked_sub_signed(duration)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("schedule verification start overflow".into()))?;
    UtcRange::new(start, target)
}

/// Bounded exponential recovery backoff: 60s doubling capped at one hour.
#[must_use]
pub(super) fn recovery_backoff_seconds(attempt_count: u32) -> u32 {
    let steps = attempt_count.saturating_sub(1).min(6);
    60_u32
        .checked_mul(1_u32 << steps)
        .unwrap_or(3_600)
        .min(3_600)
}

fn parse_failure_class(value: &str) -> Result<ScheduleFailureClass, LabError> {
    match value {
        "recoverable" => Ok(ScheduleFailureClass::Recoverable),
        "operator_required" => Ok(ScheduleFailureClass::OperatorRequired),
        other => Err(LabError::DataCorrupt(format!(
            "unknown schedule failure class {other}"
        ))),
    }
}

fn parse_recovery_state(value: &str) -> Result<ScheduleRecoveryState, LabError> {
    match value {
        "active" => Ok(ScheduleRecoveryState::Active),
        "degraded" => Ok(ScheduleRecoveryState::Degraded),
        "recovery_wait" => Ok(ScheduleRecoveryState::RecoveryWait),
        "probing" => Ok(ScheduleRecoveryState::Probing),
        "backfilling" => Ok(ScheduleRecoveryState::Backfilling),
        "verifying_freshness" => Ok(ScheduleRecoveryState::VerifyingFreshness),
        "operator_required" => Ok(ScheduleRecoveryState::OperatorRequired),
        other => Err(LabError::DataCorrupt(format!(
            "unknown schedule recovery state {other}"
        ))),
    }
}
