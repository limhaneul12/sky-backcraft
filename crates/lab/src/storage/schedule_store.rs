//! SQLite persistence and reconciliation for periodic collection schedules.

use super::{Store, enum_text, json_error, sql_error, timestamp_from_ms, timestamp_ms, u64_to_i64};
use crate::contracts::{
    AttemptId, AttemptState, CollectRequest, CollectionFreshness, CollectionSchedulePage,
    CollectionScheduleRecord, CollectionScheduleStatus, ContentHash, DatasetId, FailureRecord,
    FreshnessMissingReason, JobId, JobOutput, JobPayload, JobSubmission, LabError,
    MAX_ACTIVE_SCHEDULES, MAX_RESEARCH_PAGE, MarketFreshness, RequestId, ScheduleFire,
    ScheduleFireStatus, ScheduleId, UtcTimestamp,
};
use crate::scheduling::{
    ScheduleFailureAction, classify_failure_record, fire_request_id, latest_completed_boundary,
    next_action_after_cadence,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

impl Store {
    /// Persist a validated schedule with request-id idempotency.
    ///
    /// # Errors
    /// Rejects conflicting request reuse, active-schedule capacity, invalid input, or SQLite failure.
    pub fn create_collection_schedule(
        &mut self,
        schedule: &CollectionScheduleRecord,
    ) -> Result<CollectionScheduleRecord, LabError> {
        schedule.request.validate()?;
        if schedule.id != ScheduleId::from_seed(schedule.request.request_id.as_str())
            || schedule.status != CollectionScheduleStatus::Active
            || schedule.created_at != schedule.next_action_at
            || schedule.last_success_dataset_id.is_some()
            || schedule.last_success_boundary.is_some()
            || schedule.last_success_coverage.is_some()
            || schedule.failure.is_some()
            || schedule.in_flight.is_some()
        {
            return Err(LabError::InvalidConfig(
                "new collection schedule must be an unstarted active record".into(),
            ));
        }
        if schedule.input_digest != ContentHash::of_value(&schedule.request)? {
            return Err(LabError::InputHashMismatch(
                "collection schedule request digest mismatch".into(),
            ));
        }
        if let Some(existing_id) = self
            .connection
            .query_row(
                "SELECT id FROM collection_schedules WHERE request_id=?1",
                [schedule.request.request_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
        {
            let existing = load_schedule(&self.connection, &ScheduleId::new(existing_id)?)?
                .ok_or_else(|| {
                    LabError::DataCorrupt(
                        "collection schedule disappeared during idempotency readback".into(),
                    )
                })?;
            return if existing.input_digest == schedule.input_digest {
                Ok(existing)
            } else {
                Err(LabError::Conflict(
                    "request id already names a different collection schedule".into(),
                ))
            };
        }
        enforce_active_capacity(&self.connection)?;
        let request_json = serde_json::to_string(&schedule.request).map_err(json_error)?;
        self.connection
            .execute(
                "INSERT INTO collection_schedules(\
                 id,request_id,input_digest,status,created_at_ms,next_action_at_ms,request_json,\
                 last_success_dataset_id,last_success_boundary_ms,failure_json\
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,NULL,NULL,NULL)",
                params![
                    schedule.id.as_str(),
                    schedule.request.request_id.as_str(),
                    schedule.input_digest.as_str(),
                    enum_text(&CollectionScheduleStatus::Active)?,
                    timestamp_ms(schedule.created_at),
                    timestamp_ms(schedule.next_action_at),
                    request_json,
                ],
            )
            .map_err(sql_error)?;
        load_schedule(&self.connection, &schedule.id)?
            .ok_or_else(|| LabError::Internal("created collection schedule disappeared".into()))
    }

    /// Load one schedule and its current fire, if any.
    ///
    /// # Errors
    /// Returns invalid stored contracts or SQLite failure.
    pub fn get_collection_schedule(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<Option<CollectionScheduleRecord>, LabError> {
        load_schedule(&self.connection, schedule_id)
    }

    /// Page historical schedules in stable creation order.
    ///
    /// # Errors
    /// Rejects page bounds and returns invalid stored contracts or SQLite failure.
    pub fn list_collection_schedules(
        &self,
        offset: u64,
        limit: u32,
    ) -> Result<CollectionSchedulePage, LabError> {
        validate_page(limit)?;
        let total: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM collection_schedules", [], |row| {
                row.get(0)
            })
            .map_err(sql_error)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id FROM collection_schedules \
                 ORDER BY created_at_ms,id LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![limit, u64_to_i64(offset)?], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let ids = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
        drop(statement);
        let records = ids
            .into_iter()
            .map(|id| {
                load_schedule(&self.connection, &ScheduleId::new(id)?)?.ok_or_else(|| {
                    LabError::DataCorrupt("listed collection schedule disappeared".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let total_count = u64::try_from(total)
            .map_err(|_| LabError::DataCorrupt("negative collection schedule count".into()))?;
        let consumed =
            offset
                .checked_add(u64::try_from(records.len()).map_err(|_| {
                    LabError::ResourceLimit("schedule page length exceeds u64".into())
                })?)
                .ok_or_else(|| LabError::ResourceLimit("schedule page offset overflow".into()))?;
        Ok(CollectionSchedulePage {
            records,
            total_count,
            next_offset: (consumed < total_count).then_some(consumed),
        })
    }

    /// Return a bounded deterministic set whose pinned attempts may have changed.
    ///
    /// # Errors
    /// Rejects scan bounds and returns invalid identities or SQLite failure.
    pub fn collection_schedule_reconciliation_candidates(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduleId>, LabError> {
        validate_active_scan(limit)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT f.schedule_id FROM schedule_fires f \
                 WHERE f.status IN ('queued','running') \
                 ORDER BY f.created_at_ms,f.schedule_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        statement
            .query_map([limit], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .map(|row| ScheduleId::new(row.map_err(sql_error)?))
            .collect()
    }

    /// Return active schedules ready either for a new fire or a persisted retry.
    ///
    /// # Errors
    /// Rejects scan bounds and returns invalid identities or SQLite failure.
    pub fn due_collection_schedules(
        &self,
        now: UtcTimestamp,
        limit: u32,
    ) -> Result<Vec<CollectionScheduleRecord>, LabError> {
        validate_active_scan(limit)?;
        let active = enum_text(&CollectionScheduleStatus::Active)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT s.id FROM collection_schedules s \
                 WHERE s.status=?1 AND (\
                   EXISTS (SELECT 1 FROM schedule_fires f WHERE f.schedule_id=s.id \
                           AND f.status='retry_wait' AND f.retry_at_ms<=?2) OR \
                   (s.next_action_at_ms<=?2 AND NOT EXISTS (\
                       SELECT 1 FROM schedule_fires f WHERE f.schedule_id=s.id \
                       AND f.status IN ('planned','queued','running','retry_wait')))\
                 ) ORDER BY s.next_action_at_ms,s.id LIMIT ?3",
            )
            .map_err(sql_error)?;
        let ids = statement
            .query_map(params![active, timestamp_ms(now), limit], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?
            .map(|row| ScheduleId::new(row.map_err(sql_error)?))
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        ids.into_iter()
            .map(|id| required_schedule(&self.connection, &id))
            .collect()
    }

    /// Atomically insert an ordinary collection job and pin it to one schedule fire.
    /// Commit must precede the runtime wake performed by the caller.
    ///
    /// # Errors
    /// Rejects stale/duplicate boundaries, queue capacity, mismatched payloads, or SQLite failure.
    pub fn admit_collection_schedule_fire(
        &mut self,
        schedule_id: &ScheduleId,
        boundary: UtcTimestamp,
        request: CollectRequest,
        now: UtcTimestamp,
    ) -> Result<ScheduleFire, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = load_schedule(&transaction, schedule_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown collection schedule".into()))?;
        ensure_admission_matches(&schedule, boundary, &request)?;
        if let Some(existing) = load_fire(&transaction, schedule_id, boundary)? {
            return if existing.request_id == request.request_id {
                Ok(existing)
            } else {
                Err(LabError::Conflict(
                    "schedule boundary already has a different fire".into(),
                ))
            };
        }
        if schedule.in_flight.is_some() {
            return Err(LabError::Conflict(
                "collection schedule already has an in-flight fire".into(),
            ));
        }
        let submission = JobSubmission {
            request_id: request.request_id.clone(),
            payload: JobPayload::Collect { request },
        };
        let pin = super::job_store::insert_job_tx(&transaction, &submission, now)?;
        transaction
            .execute(
                "INSERT INTO schedule_fires(\
                 id,schedule_id,boundary_ms,request_id,status,job_id,attempt_id,dataset_id,\
                 created_at_ms,retry_count,retry_at_ms,failure_json\
                 ) VALUES (?1,?2,?3,?4,'queued',?5,?6,NULL,?7,0,NULL,NULL)",
                params![
                    submission.request_id.as_str(),
                    schedule_id.as_str(),
                    timestamp_ms(boundary),
                    submission.request_id.as_str(),
                    pin.job_id.as_str(),
                    pin.attempt_id.as_str(),
                    timestamp_ms(now),
                ],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        load_fire(&self.connection, schedule_id, boundary)?.ok_or_else(|| {
            LabError::Internal("admitted collection schedule fire disappeared".into())
        })
    }

    /// Persist a cadence deferral after observing that no newer boundary exists.
    /// Queue-full paths must not call this method.
    ///
    /// # Errors
    /// Rejects inactive schedules or SQLite failure.
    pub fn defer_collection_schedule(
        &mut self,
        schedule_id: &ScheduleId,
        next_action_at: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let changed = self
            .connection
            .execute(
                "UPDATE collection_schedules SET next_action_at_ms=?1 \
                 WHERE id=?2 AND status='active' AND NOT EXISTS (\
                   SELECT 1 FROM schedule_fires WHERE schedule_id=?2 \
                   AND status IN ('planned','queued','running','retry_wait'))",
                params![timestamp_ms(next_action_at), schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "collection schedule cannot be deferred in its current state".into(),
            ));
        }
        required_schedule(&self.connection, schedule_id)
    }

    /// Reconcile one pinned attempt into its schedule/fire projection.
    ///
    /// # Errors
    /// Returns missing/corrupt pins, invalid transitions, timestamp overflow, or SQLite failure.
    pub fn reconcile_collection_schedule(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        let Some(fire) = schedule.in_flight.clone() else {
            return Ok(schedule);
        };
        if fire.status == ScheduleFireStatus::RetryWait {
            return Ok(schedule);
        }
        let attempt_id = fire.attempt_id.as_ref().ok_or_else(|| {
            LabError::DataCorrupt("active schedule fire has no pinned attempt".into())
        })?;
        let state = load_attempt_state(&transaction, attempt_id)?;
        reconcile_state(&transaction, &schedule, &fire, state, now)?;
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Queue a due retry and atomically repin the owner fire to the new attempt.
    ///
    /// # Errors
    /// Rejects early/stale retries, queue capacity, attempt limits, or SQLite failure.
    pub fn retry_collection_schedule_fire(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<ScheduleFire, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let fire = load_current_fire(&transaction, schedule_id)?.ok_or_else(|| {
            LabError::Conflict("collection schedule has no retryable fire".into())
        })?;
        if fire.status != ScheduleFireStatus::RetryWait
            || fire.retry_at.is_none_or(|retry_at| retry_at > now)
        {
            return Err(LabError::Conflict(
                "collection schedule retry is not due".into(),
            ));
        }
        let job_id = fire
            .job_id
            .as_ref()
            .ok_or_else(|| LabError::DataCorrupt("retryable schedule fire has no job".into()))?;
        let attempt_id = super::job_store::retry_job_tx(&transaction, job_id, now)?;
        let changed = transaction
            .execute(
                "UPDATE schedule_fires SET status='queued',attempt_id=?1,retry_at_ms=NULL \
                 WHERE id=?2 AND status='retry_wait' AND attempt_id=?3",
                params![
                    attempt_id.as_str(),
                    fire.request_id.as_str(),
                    fire.attempt_id.as_ref().map(AttemptId::as_str),
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "collection schedule retry pin changed".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        load_fire(&self.connection, schedule_id, fire.boundary)?.ok_or_else(|| {
            LabError::Internal("retried collection schedule fire disappeared".into())
        })
    }

    /// Durably pause admission and cancel the pinned ordinary attempt in one transaction.
    /// Running work is also returned so the runtime can signal its in-memory token.
    ///
    /// # Errors
    /// Returns unknown schedule, invalid stored state, or SQLite failure.
    pub fn pause_collection_schedule(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<(CollectionScheduleRecord, Vec<JobId>), LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        required_schedule(&transaction, schedule_id)?;
        transaction
            .execute(
                "UPDATE collection_schedules SET status='paused' WHERE id=?1",
                [schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        let mut running_jobs = Vec::new();
        if let Some(fire) = load_current_fire(&transaction, schedule_id)? {
            match fire.status {
                ScheduleFireStatus::Queued | ScheduleFireStatus::Running => {
                    let job_id = fire.job_id.ok_or_else(|| {
                        LabError::DataCorrupt("active schedule fire has no job".into())
                    })?;
                    let attempt_id = fire.attempt_id.as_ref().ok_or_else(|| {
                        LabError::DataCorrupt("active schedule fire has no attempt".into())
                    })?;
                    let state = load_attempt_state(&transaction, attempt_id)?;
                    super::job_store::request_cancel_tx(&transaction, &job_id, now)?;
                    match state {
                        AttemptState::Queued { .. } => {
                            transaction
                                .execute(
                                    "UPDATE schedule_fires SET status='cancelled',retry_at_ms=NULL \
                                     WHERE id=?1",
                                    [fire.request_id.as_str()],
                                )
                                .map_err(sql_error)?;
                        }
                        AttemptState::Running { .. } => running_jobs.push(job_id),
                        _ => {}
                    }
                }
                ScheduleFireStatus::RetryWait | ScheduleFireStatus::Planned => {
                    transaction
                        .execute(
                            "UPDATE schedule_fires SET status='cancelled',retry_at_ms=NULL \
                             WHERE id=?1",
                            [fire.request_id.as_str()],
                        )
                        .map_err(sql_error)?;
                }
                ScheduleFireStatus::Completed
                | ScheduleFireStatus::Blocked
                | ScheduleFireStatus::Cancelled => {}
            }
        }
        transaction.commit().map_err(sql_error)?;
        Ok((
            required_schedule(&self.connection, schedule_id)?,
            running_jobs,
        ))
    }

    /// Resume admission. A terminal incomplete fire is atomically retried and repinned.
    ///
    /// # Errors
    /// Rejects a still-running paused fire, active capacity, retry limits, queue capacity, or SQLite failure.
    pub fn resume_collection_schedule(
        &mut self,
        schedule_id: &ScheduleId,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        if schedule.status == CollectionScheduleStatus::Active {
            return Ok(schedule);
        }
        enforce_active_capacity(&transaction)?;
        let latest = load_latest_fire(&transaction, schedule_id)?;
        if let Some(fire) = latest {
            match fire.status {
                ScheduleFireStatus::Queued
                | ScheduleFireStatus::Running
                | ScheduleFireStatus::Planned
                | ScheduleFireStatus::RetryWait => {
                    return Err(LabError::Conflict(
                        "paused collection fire must finish cancellation before resume".into(),
                    ));
                }
                ScheduleFireStatus::Blocked | ScheduleFireStatus::Cancelled => {
                    let job_id = fire.job_id.as_ref().ok_or_else(|| {
                        LabError::DataCorrupt("terminal schedule fire has no job".into())
                    })?;
                    let attempt_id = super::job_store::retry_job_tx(&transaction, job_id, now)?;
                    transaction
                        .execute(
                            "UPDATE schedule_fires SET status='queued',attempt_id=?1,\
                             retry_count=0,retry_at_ms=NULL,failure_json=NULL WHERE id=?2",
                            params![attempt_id.as_str(), fire.request_id.as_str()],
                        )
                        .map_err(sql_error)?;
                }
                ScheduleFireStatus::Completed => {}
            }
        }
        let changed = transaction
            .execute(
                "UPDATE collection_schedules SET status='active',next_action_at_ms=?1,\
                 failure_json=NULL WHERE id=?2 AND status IN ('paused','blocked')",
                params![timestamp_ms(now), schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "collection schedule resume state changed".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Permanently block an active owner after a non-deferrable producer failure.
    /// Queue-capacity rejection is a deferral and must not call this method.
    ///
    /// # Errors
    /// Rejects unknown or paused schedules and returns serialization or SQLite failure.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the database command owns the durable failure transferred across its queue"
    )]
    pub fn block_collection_schedule(
        &mut self,
        schedule_id: &ScheduleId,
        failure: FailureRecord,
        now: UtcTimestamp,
    ) -> Result<CollectionScheduleRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let schedule = required_schedule(&transaction, schedule_id)?;
        match schedule.status {
            CollectionScheduleStatus::Paused => {
                return Err(LabError::Conflict(
                    "paused collection schedule cannot be producer-blocked".into(),
                ));
            }
            CollectionScheduleStatus::Blocked => return Ok(schedule),
            CollectionScheduleStatus::Active => {}
        }
        let failure_json = serde_json::to_string(&failure).map_err(json_error)?;
        let changed = transaction
            .execute(
                "UPDATE collection_schedules SET status='blocked',next_action_at_ms=?1,\
                 failure_json=?2 WHERE id=?3 AND status='active'",
                params![timestamp_ms(now), failure_json, schedule_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "collection schedule changed before producer block".into(),
            ));
        }
        transaction
            .execute(
                "UPDATE schedule_fires SET status='blocked',retry_at_ms=NULL,failure_json=?1 \
                 WHERE schedule_id=?2 AND status IN ('planned','retry_wait')",
                params![
                    serde_json::to_string(&failure).map_err(json_error)?,
                    schedule_id.as_str()
                ],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        required_schedule(&self.connection, schedule_id)
    }

    /// Project freshness from the last successful dataset without inventing observations.
    ///
    /// # Errors
    /// Returns unknown schedule, invalid stored data, arithmetic overflow, or SQLite failure.
    pub fn collection_freshness(
        &self,
        schedule_id: &ScheduleId,
        observed_at: UtcTimestamp,
    ) -> Result<CollectionFreshness, LabError> {
        let schedule = required_schedule(&self.connection, schedule_id)?;
        let expected_end = latest_completed_boundary(observed_at, schedule.request.interval)?;
        let start = expected_end
            .0
            .checked_sub_signed(
                schedule
                    .request
                    .interval
                    .duration()
                    .checked_mul(i32::try_from(schedule.request.lookback_bars).map_err(|_| {
                        LabError::ResourceLimit("schedule lookback exceeds i32".into())
                    })?)
                    .ok_or_else(|| {
                        LabError::ResourceLimit("schedule freshness range overflow".into())
                    })?,
            )
            .ok_or_else(|| {
                LabError::InvalidConfig("schedule freshness timestamp overflow".into())
            })?;
        let mut markets = Vec::with_capacity(schedule.request.markets.len());
        for market in &schedule.request.markets {
            markets.push(project_market_freshness(
                &self.connection,
                schedule.last_success_dataset_id.as_ref(),
                market,
                schedule.request.interval,
                UtcTimestamp(start),
                expected_end,
                schedule.request.lookback_bars,
            )?);
        }
        Ok(CollectionFreshness {
            schedule_id: schedule.id,
            observed_at,
            interval: schedule.request.interval,
            markets,
        })
    }
}

fn ensure_admission_matches(
    schedule: &CollectionScheduleRecord,
    boundary: UtcTimestamp,
    request: &CollectRequest,
) -> Result<(), LabError> {
    if schedule.status != CollectionScheduleStatus::Active {
        return Err(LabError::Conflict(
            "only an active collection schedule can admit work".into(),
        ));
    }
    if schedule
        .last_success_boundary
        .is_some_and(|last| boundary <= last)
    {
        return Err(LabError::Conflict(
            "collection schedule boundary is already complete".into(),
        ));
    }
    let expected_id = fire_request_id(&schedule.id, boundary);
    let expected_start =
        boundary
            .0
            .checked_sub_signed(
                schedule
                    .request
                    .interval
                    .duration()
                    .checked_mul(i32::try_from(schedule.request.lookback_bars).map_err(|_| {
                        LabError::ResourceLimit("schedule lookback exceeds i32".into())
                    })?)
                    .ok_or_else(|| LabError::ResourceLimit("schedule range overflow".into()))?,
            )
            .ok_or_else(|| LabError::InvalidConfig("schedule range timestamp overflow".into()))?;
    let matches = request.request_id == expected_id
        && request.markets == schedule.request.markets
        && request.data_resolution == schedule.request.interval
        && request.range.start() == UtcTimestamp(expected_start)
        && request.range.end() == boundary
        && request.warmup_bars == 0
        && request.completed_only;
    if !matches {
        return Err(LabError::Conflict(
            "collection fire payload differs from frozen schedule".into(),
        ));
    }
    Ok(())
}

fn reconcile_state(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    fire: &ScheduleFire,
    state: AttemptState,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    match state {
        AttemptState::Queued { .. } => update_fire_status(transaction, fire, "queued"),
        AttemptState::Running { .. } => update_fire_status(transaction, fire, "running"),
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            let JobOutput::Dataset { dataset_id } = output else {
                return Err(LabError::DataCorrupt(
                    "collection schedule job completed with non-dataset output".into(),
                ));
            };
            complete_fire(transaction, schedule, fire, &dataset_id, now)
        }
        AttemptState::Failed { error, .. } => {
            reconcile_failed_attempt(transaction, schedule, fire, &error, now)
        }
        AttemptState::Blocked { reason, output, .. } => {
            let dataset_id = match output {
                Some(JobOutput::Dataset { dataset_id }) => Some(dataset_id),
                Some(_) => {
                    return Err(LabError::DataCorrupt(
                        "collection schedule job blocked with non-dataset output".into(),
                    ));
                }
                None => None,
            };
            block_fire(transaction, schedule, fire, &reason, dataset_id.as_ref())
        }
        AttemptState::Cancelled { reason, .. } => reconcile_lifecycle_end(
            transaction,
            schedule,
            fire,
            &FailureRecord {
                code: "CANCELLED".into(),
                message: reason,
            },
            now,
        ),
        AttemptState::Interrupted { reason, .. } => reconcile_lifecycle_end(
            transaction,
            schedule,
            fire,
            &FailureRecord {
                code: "INTERRUPTED".into(),
                message: reason,
            },
            now,
        ),
    }
}

fn reconcile_failed_attempt(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    fire: &ScheduleFire,
    error: &FailureRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    match classify_failure_record(error, fire.retry_count, &schedule.request.retry, now)? {
        ScheduleFailureAction::RetryAt {
            retry_count,
            retry_at,
        } => {
            let failure_json = serde_json::to_string(error).map_err(json_error)?;
            transaction
                .execute(
                    "UPDATE schedule_fires SET status='retry_wait',retry_count=?1,\
                     retry_at_ms=?2,failure_json=?3 WHERE id=?4 AND attempt_id=?5",
                    params![
                        retry_count,
                        timestamp_ms(retry_at),
                        failure_json,
                        fire.request_id.as_str(),
                        fire.attempt_id.as_ref().map(AttemptId::as_str),
                    ],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE collection_schedules SET next_action_at_ms=?1,\
                     failure_json=?2 WHERE id=?3",
                    params![
                        timestamp_ms(retry_at),
                        serde_json::to_string(error).map_err(json_error)?,
                        schedule.id.as_str(),
                    ],
                )
                .map_err(sql_error)?;
            Ok(())
        }
        ScheduleFailureAction::Block => block_fire(transaction, schedule, fire, error, None),
    }
}

fn reconcile_lifecycle_end(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    fire: &ScheduleFire,
    failure: &FailureRecord,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let (status, retry_at) = if schedule.status == CollectionScheduleStatus::Active {
        ("retry_wait", Some(timestamp_ms(now)))
    } else {
        ("cancelled", None)
    };
    transaction
        .execute(
            "UPDATE schedule_fires SET status=?1,retry_at_ms=?2,failure_json=?3 \
             WHERE id=?4 AND attempt_id=?5",
            params![
                status,
                retry_at,
                serde_json::to_string(failure).map_err(json_error)?,
                fire.request_id.as_str(),
                fire.attempt_id.as_ref().map(AttemptId::as_str),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn complete_fire(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    fire: &ScheduleFire,
    dataset_id: &DatasetId,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let changed = transaction
        .execute(
            "UPDATE schedule_fires SET status='completed',dataset_id=?1,retry_at_ms=NULL,\
             failure_json=NULL WHERE id=?2 AND attempt_id=?3",
            params![
                dataset_id.as_str(),
                fire.request_id.as_str(),
                fire.attempt_id.as_ref().map(AttemptId::as_str),
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::Conflict(
            "collection schedule completion pin changed".into(),
        ));
    }
    let next = next_action_after_cadence(now, schedule.request.cadence_seconds)?;
    transaction
        .execute(
            "UPDATE collection_schedules SET last_success_dataset_id=?1,\
             last_success_boundary_ms=?2,next_action_at_ms=?3,failure_json=NULL WHERE id=?4",
            params![
                dataset_id.as_str(),
                timestamp_ms(fire.boundary),
                timestamp_ms(next),
                schedule.id.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn block_fire(
    transaction: &Transaction<'_>,
    schedule: &CollectionScheduleRecord,
    fire: &ScheduleFire,
    failure: &FailureRecord,
    dataset_id: Option<&DatasetId>,
) -> Result<(), LabError> {
    let failure_json = serde_json::to_string(failure).map_err(json_error)?;
    transaction
        .execute(
            "UPDATE schedule_fires SET status='blocked',dataset_id=?1,retry_at_ms=NULL,\
             failure_json=?2 WHERE id=?3 AND attempt_id=?4",
            params![
                dataset_id.map(DatasetId::as_str),
                failure_json,
                fire.request_id.as_str(),
                fire.attempt_id.as_ref().map(AttemptId::as_str),
            ],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "UPDATE collection_schedules SET status='blocked',failure_json=?1 \
             WHERE id=?2",
            params![
                serde_json::to_string(failure).map_err(json_error)?,
                schedule.id.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn update_fire_status(
    transaction: &Transaction<'_>,
    fire: &ScheduleFire,
    status: &str,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE schedule_fires SET status=?1 WHERE id=?2 AND attempt_id=?3",
            params![
                status,
                fire.request_id.as_str(),
                fire.attempt_id.as_ref().map(AttemptId::as_str),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn load_attempt_state(
    connection: &Connection,
    attempt_id: &AttemptId,
) -> Result<AttemptState, LabError> {
    let json = connection
        .query_row(
            "SELECT state_json FROM job_attempts WHERE id=?1",
            [attempt_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("schedule pinned attempt is missing".into()))?;
    serde_json::from_str(&json).map_err(json_error)
}

fn required_schedule(
    connection: &Connection,
    schedule_id: &ScheduleId,
) -> Result<CollectionScheduleRecord, LabError> {
    load_schedule(connection, schedule_id)?
        .ok_or_else(|| LabError::InvalidConfig("unknown collection schedule".into()))
}

fn load_schedule(
    connection: &Connection,
    schedule_id: &ScheduleId,
) -> Result<Option<CollectionScheduleRecord>, LabError> {
    let row = connection
        .query_row(
            "SELECT request_json,input_digest,status,created_at_ms,next_action_at_ms,\
             last_success_dataset_id,last_success_boundary_ms,failure_json \
             FROM collection_schedules WHERE id=?1",
            [schedule_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((request, digest, status, created, next, dataset, boundary, failure)) = row else {
        return Ok(None);
    };
    let mut record = CollectionScheduleRecord {
        id: schedule_id.clone(),
        request: serde_json::from_str(&request).map_err(json_error)?,
        input_digest: ContentHash::try_from(digest)?,
        status: parse_schedule_status(&status)?,
        created_at: timestamp_from_ms(created)?,
        next_action_at: timestamp_from_ms(next)?,
        last_success_dataset_id: dataset.map(DatasetId::new).transpose()?,
        last_success_boundary: boundary.map(timestamp_from_ms).transpose()?,
        last_success_coverage: None,
        failure: failure
            .map(|json| serde_json::from_str(&json).map_err(json_error))
            .transpose()?,
        in_flight: load_current_fire(connection, schedule_id)?,
    };
    record.last_success_coverage = record.derive_last_success_coverage()?;
    Ok(Some(record))
}

fn load_current_fire(
    connection: &Connection,
    schedule_id: &ScheduleId,
) -> Result<Option<ScheduleFire>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT boundary_ms FROM schedule_fires WHERE schedule_id=?1 \
             AND status IN ('planned','queued','running','retry_wait') \
             ORDER BY created_at_ms DESC,id DESC LIMIT 1",
        )
        .map_err(sql_error)?;
    let boundary = statement
        .query_row([schedule_id.as_str()], |row| row.get::<_, i64>(0))
        .optional()
        .map_err(sql_error)?;
    boundary
        .map(|value| load_fire(connection, schedule_id, timestamp_from_ms(value)?))
        .transpose()
        .map(Option::flatten)
}

fn load_latest_fire(
    connection: &Connection,
    schedule_id: &ScheduleId,
) -> Result<Option<ScheduleFire>, LabError> {
    let boundary = connection
        .query_row(
            "SELECT boundary_ms FROM schedule_fires WHERE schedule_id=?1 \
             ORDER BY created_at_ms DESC,id DESC LIMIT 1",
            [schedule_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(sql_error)?;
    boundary
        .map(|value| load_fire(connection, schedule_id, timestamp_from_ms(value)?))
        .transpose()
        .map(Option::flatten)
}

fn load_fire(
    connection: &Connection,
    schedule_id: &ScheduleId,
    boundary: UtcTimestamp,
) -> Result<Option<ScheduleFire>, LabError> {
    let row = connection
        .query_row(
            "SELECT request_id,status,job_id,attempt_id,dataset_id,created_at_ms,\
             retry_count,retry_at_ms,failure_json FROM schedule_fires \
             WHERE schedule_id=?1 AND boundary_ms=?2",
            params![schedule_id.as_str(), timestamp_ms(boundary)],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((request, status, job, attempt, dataset, created, retry_count, retry_at, failure)) =
        row
    else {
        return Ok(None);
    };
    Ok(Some(ScheduleFire {
        schedule_id: schedule_id.clone(),
        boundary,
        request_id: RequestId::new(request)?,
        status: parse_fire_status(&status)?,
        job_id: job.map(JobId::new).transpose()?,
        attempt_id: attempt.map(AttemptId::new).transpose()?,
        dataset_id: dataset.map(DatasetId::new).transpose()?,
        created_at: timestamp_from_ms(created)?,
        retry_count,
        retry_at: retry_at.map(timestamp_from_ms).transpose()?,
        failure: failure
            .map(|json| serde_json::from_str(&json).map_err(json_error))
            .transpose()?,
    }))
}

fn project_market_freshness(
    connection: &Connection,
    dataset_id: Option<&DatasetId>,
    market: &crate::contracts::MarketId,
    interval: crate::contracts::CandleInterval,
    start: UtcTimestamp,
    expected_end: UtcTimestamp,
    expected_bars: u32,
) -> Result<MarketFreshness, LabError> {
    let Some(dataset_id) = dataset_id else {
        return Ok(MarketFreshness {
            market: market.clone(),
            expected_end,
            latest_completed_end: None,
            age_seconds: None,
            gap_count: u64::from(expected_bars),
            missing: Some(FreshnessMissingReason::NeverCollected),
        });
    };
    let interval_text = enum_text(&interval)?;
    let latest: Option<i64> = connection
        .query_row(
            "SELECT MAX(o.close_time_ms) \
             FROM dataset_members m JOIN candle_observations o ON o.id=m.observation_id \
             WHERE m.dataset_id=?1 AND o.market=?2 AND o.interval=?3 AND o.completed=1 \
             AND o.close_time_ms<=?4",
            params![
                dataset_id.as_str(),
                market.code(),
                &interval_text,
                timestamp_ms(expected_end),
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(DISTINCT o.open_time_ms) \
             FROM dataset_members m JOIN candle_observations o ON o.id=m.observation_id \
             WHERE m.dataset_id=?1 AND o.market=?2 AND o.interval=?3 AND o.completed=1 \
             AND o.open_time_ms>=?4 AND o.close_time_ms<=?5",
            params![
                dataset_id.as_str(),
                market.code(),
                &interval_text,
                timestamp_ms(start),
                timestamp_ms(expected_end),
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let latest_completed_end = latest.map(timestamp_from_ms).transpose()?;
    let present = u64::try_from(rows)
        .map_err(|_| LabError::DataCorrupt("negative schedule freshness row count".into()))?;
    let gap_count = u64::from(expected_bars).saturating_sub(present);
    let age_seconds = latest_completed_end
        .map(|latest| {
            u64::try_from((expected_end.0 - latest.0).num_seconds()).map_err(|_| {
                LabError::DataCorrupt("schedule dataset extends past expected boundary".into())
            })
        })
        .transpose()?;
    let missing = match latest_completed_end {
        None => Some(FreshnessMissingReason::NeverCollected),
        Some(latest) if latest < expected_end => {
            Some(FreshnessMissingReason::ExpectedBoundaryMissing)
        }
        Some(_) => None,
    };
    Ok(MarketFreshness {
        market: market.clone(),
        expected_end,
        latest_completed_end,
        age_seconds,
        gap_count,
        missing,
    })
}

fn enforce_active_capacity(connection: &Connection) -> Result<(), LabError> {
    let active: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM collection_schedules WHERE status='active'",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if active >= i64::try_from(MAX_ACTIVE_SCHEDULES).unwrap_or(i64::MAX) {
        return Err(LabError::CapacityExceeded(format!(
            "active collection schedule limit is {MAX_ACTIVE_SCHEDULES}"
        )));
    }
    Ok(())
}

fn validate_page(limit: u32) -> Result<(), LabError> {
    if !(1..=MAX_RESEARCH_PAGE).contains(&limit) {
        return Err(LabError::InvalidConfig(format!(
            "schedule page limit must be in 1..={MAX_RESEARCH_PAGE}"
        )));
    }
    Ok(())
}

fn validate_active_scan(limit: u32) -> Result<(), LabError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |value| value > MAX_ACTIVE_SCHEDULES) {
        return Err(LabError::InvalidConfig(format!(
            "active schedule scan limit must be in 1..={MAX_ACTIVE_SCHEDULES}"
        )));
    }
    Ok(())
}

fn parse_schedule_status(value: &str) -> Result<CollectionScheduleStatus, LabError> {
    match value {
        "active" => Ok(CollectionScheduleStatus::Active),
        "paused" => Ok(CollectionScheduleStatus::Paused),
        "blocked" => Ok(CollectionScheduleStatus::Blocked),
        other => Err(LabError::DataCorrupt(format!(
            "unknown collection schedule status {other}"
        ))),
    }
}

fn parse_fire_status(value: &str) -> Result<ScheduleFireStatus, LabError> {
    match value {
        "planned" => Ok(ScheduleFireStatus::Planned),
        "queued" => Ok(ScheduleFireStatus::Queued),
        "running" => Ok(ScheduleFireStatus::Running),
        "retry_wait" => Ok(ScheduleFireStatus::RetryWait),
        "completed" => Ok(ScheduleFireStatus::Completed),
        "blocked" => Ok(ScheduleFireStatus::Blocked),
        "cancelled" => Ok(ScheduleFireStatus::Cancelled),
        other => Err(LabError::DataCorrupt(format!(
            "unknown collection schedule fire status {other}"
        ))),
    }
}

#[cfg(test)]
#[path = "schedule_store/tests.rs"]
mod tests;
