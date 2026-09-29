use super::run_store::{insert_artifact_row, insert_validation_row};
use super::{Store, enum_text, json_error, sql_error, timestamp_ms, u64_to_i64};
use crate::contracts::{
    ArtifactRef, AttemptId, AttemptState, ContentHash, DatasetSnapshot, JobAttempt, JobId,
    JobOutput, JobProgress, JobRecord, JobStatus, JobSubmission, LabError, ProgressCountUnit,
    RequestId, RunId, UtcTimestamp, ValidationReport,
};
use rusqlite::{OptionalExtension, Transaction, params};

const MAX_QUEUED_JOBS: i64 = 8;
const MAX_JOB_ATTEMPTS: u32 = 32;
const INITIAL_STAGE: &str = "queued";

#[derive(Debug, Clone)]
pub enum AttemptPublication {
    None,
    Run {
        run_id: RunId,
        semantic_digest: ContentHash,
        expected_models: u64,
        validation: ValidationReport,
    },
    Dataset(Box<DatasetSnapshot>),
    Artifacts(Vec<ArtifactRef>),
    Validation(ValidationReport),
}

impl Store {
    /// Submit one durable job, preserving request-id idempotency.
    ///
    /// # Errors
    /// Rejects conflicting request reuse, a full durable queue, or persistence failure.
    pub fn submit_job(
        &mut self,
        submission: &JobSubmission,
        now: UtcTimestamp,
    ) -> Result<JobRecord, LabError> {
        let input_digest = ContentHash::of_value(&submission.payload)?;
        if let Some(job_id) = self
            .connection
            .query_row(
                "SELECT id FROM jobs WHERE request_id=?1",
                [submission.request_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
        {
            let existing = self.get_job(&JobId::new(job_id)?)?.ok_or_else(|| {
                LabError::DataCorrupt("job identity disappeared during idempotency check".into())
            })?;
            return if existing.normalized_input_digest == input_digest {
                Ok(existing)
            } else {
                Err(LabError::Conflict(
                    "request id already names a different job payload".into(),
                ))
            };
        }
        enforce_queue_capacity(&self.connection)?;
        let job_id = JobId::from_seed(submission.request_id.as_str());
        let attempt_id = attempt_id(&job_id, 1);
        let state = AttemptState::Queued { queued_at: now };
        let payload_json = serde_json::to_string(&submission.payload).map_err(json_error)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO jobs(id,request_id,normalized_input_digest,payload_kind,payload_json,current_attempt_number,current_status,created_at_ms) VALUES (?1,?2,?3,?4,?5,1,?6,?7)",
            params![job_id.as_str(), submission.request_id.as_str(), input_digest.as_str(), payload_kind(&submission.payload), payload_json, enum_text(&JobStatus::Queued)?, timestamp_ms(now)],
        ).map_err(sql_error)?;
        insert_attempt(&transaction, &attempt_id, &job_id, 1, &input_digest, &state)?;
        transaction.commit().map_err(sql_error)?;
        self.get_job(&job_id)?
            .ok_or_else(|| LabError::Internal("submitted job disappeared".into()))
    }

    /// Load one job and all immutable attempts in attempt-number order.
    ///
    /// # Errors
    /// Returns an error for corrupt stored state or SQLite failure.
    pub fn get_job(&self, id: &JobId) -> Result<Option<JobRecord>, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT request_id,normalized_input_digest,payload_json FROM jobs WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let Some((request_id, digest, payload)) = row else {
            return Ok(None);
        };
        let payload: crate::contracts::JobPayload =
            serde_json::from_str(&payload).map_err(json_error)?;
        let mut statement = self.connection.prepare(
            "SELECT id,attempt_number,input_digest,state_json,progress_stage,committed_records,last_committed_event_seq FROM job_attempts WHERE job_id=?1 ORDER BY attempt_number",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(sql_error)?;
        let mut attempts = Vec::new();
        for row in rows {
            let (attempt, number, input, state_json, progress_stage, records, event_seq) =
                row.map_err(sql_error)?;
            let state: AttemptState = serde_json::from_str(&state_json).map_err(json_error)?;
            attempts.push(JobAttempt {
                id: AttemptId::new(attempt)?,
                job_id: id.clone(),
                number,
                input_digest: ContentHash::try_from(input)?,
                progress: project_progress(
                    &self.connection,
                    &state,
                    &payload,
                    &progress_stage,
                    records,
                    event_seq,
                )?,
                state,
            });
        }
        Ok(Some(JobRecord {
            id: id.clone(),
            request_id: RequestId::new(request_id)?,
            normalized_input_digest: ContentHash::try_from(digest)?,
            payload,
            attempts,
        }))
    }

    /// Transactionally claim the oldest queued attempt.
    ///
    /// # Errors
    /// Returns an error for corrupt state or SQLite failure.
    pub fn claim_next(&mut self, now: UtcTimestamp) -> Result<Option<JobAttempt>, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let running: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM job_attempts WHERE status=?1",
                [enum_text(&JobStatus::Running)?],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if running > 1 {
            return Err(LabError::DataCorrupt(
                "durable queue contains multiple RUNNING attempts".into(),
            ));
        }
        if running == 1 {
            return Ok(None);
        }
        let candidate = transaction.query_row(
            "SELECT id,job_id FROM job_attempts WHERE status=?1 ORDER BY queued_at_ms,job_id,attempt_number LIMIT 1",
            [enum_text(&JobStatus::Queued)?],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        ).optional().map_err(sql_error)?;
        let Some((attempt, job)) = candidate else {
            return Ok(None);
        };
        let state = AttemptState::Running {
            started_at: now,
            cancel_requested_at: None,
        };
        let changed = transaction.execute(
            "UPDATE job_attempts SET status=?1,state_json=?2,started_at_ms=?3 WHERE id=?4 AND status=?5",
            params![enum_text(&state.status())?, serde_json::to_string(&state).map_err(json_error)?, timestamp_ms(now), attempt, enum_text(&JobStatus::Queued)?],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "queued attempt changed before claim".into(),
            ));
        }
        transaction
            .execute(
                "UPDATE jobs SET current_status=?1 WHERE id=?2",
                params![enum_text(&JobStatus::Running)?, job],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        self.load_attempt(&AttemptId::new(attempt)?).map(Some)
    }

    /// Cancel a queued attempt immediately or persist a running cancellation request.
    ///
    /// # Errors
    /// Returns missing/conflict/corrupt state or SQLite failure.
    pub fn request_cancel(
        &mut self,
        job_id: &JobId,
        now: UtcTimestamp,
    ) -> Result<JobRecord, LabError> {
        let current = self.current_attempt(job_id)?;
        let current_status = current.state.status();
        let next = match current.state {
            AttemptState::Queued { .. } => AttemptState::Cancelled {
                ended_at: now,
                reason: "cancel requested before execution".into(),
            },
            AttemptState::Running { started_at, .. } => AttemptState::Running {
                started_at,
                cancel_requested_at: Some(now),
            },
            _ => {
                return self
                    .get_job(job_id)?
                    .ok_or_else(|| LabError::InvalidConfig("unknown job".into()));
            }
        };
        self.replace_attempt_state(&current.id, current_status, &next)?;
        self.get_job(job_id)?
            .ok_or_else(|| LabError::Internal("cancelled job disappeared".into()))
    }

    /// Queue a fresh attempt for a terminal job while preserving all prior attempts.
    ///
    /// # Errors
    /// Rejects nonterminal jobs, full queue, identity overflow, or SQLite failure.
    pub fn retry_job(&mut self, job_id: &JobId, now: UtcTimestamp) -> Result<JobRecord, LabError> {
        let current = self.current_attempt(job_id)?;
        if !current.state.is_terminal() {
            return Err(LabError::Conflict(
                "only a terminal job can be retried".into(),
            ));
        }
        if current.number >= MAX_JOB_ATTEMPTS {
            return Err(LabError::ResourceLimit(format!(
                "job attempt history limit is {MAX_JOB_ATTEMPTS}"
            )));
        }
        enforce_queue_capacity(&self.connection)?;
        let next_number = current
            .number
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("job attempt number overflow".into()))?;
        let next_id = attempt_id(job_id, next_number);
        let state = AttemptState::Queued { queued_at: now };
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_attempt(
            &transaction,
            &next_id,
            job_id,
            next_number,
            &current.input_digest,
            &state,
        )?;
        let changed = transaction.execute(
            "UPDATE jobs SET current_attempt_number=?1,current_status=?2 WHERE id=?3 AND current_attempt_number=?4",
            params![next_number, enum_text(&JobStatus::Queued)?, job_id.as_str(), current.number],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict("job retry CAS failed".into()));
        }
        transaction.commit().map_err(sql_error)?;
        self.get_job(job_id)?
            .ok_or_else(|| LabError::Internal("retried job disappeared".into()))
    }

    /// Mark all process-abandoned running attempts interrupted.
    ///
    /// # Errors
    /// Returns an error for corrupt state or SQLite failure.
    pub fn recover_interrupted(&mut self, now: UtcTimestamp) -> Result<u64, LabError> {
        let running = enum_text(&JobStatus::Running)?;
        let mut statement = self
            .connection
            .prepare("SELECT a.id,a.state_json,j.payload_json,a.committed_records,a.last_committed_event_seq FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.status=?1 ORDER BY a.job_id,a.attempt_number")
            .map_err(sql_error)?;
        let ids = statement
            .query_map([&running], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for (id, state_json, payload_json, stored_records, stored_event_seq) in &ids {
            let running_state: AttemptState =
                serde_json::from_str(state_json).map_err(json_error)?;
            let state = if matches!(
                running_state,
                AttemptState::Running {
                    cancel_requested_at: Some(_),
                    ..
                }
            ) {
                AttemptState::Cancelled {
                    ended_at: now,
                    reason: "storage owner ended after cancellation was requested".into(),
                }
            } else {
                AttemptState::Interrupted {
                    ended_at: now,
                    reason: "storage owner recovered an abandoned running attempt".into(),
                }
            };
            update_state(&transaction, id, &running, &state)?;
            let payload = serde_json::from_str(payload_json).map_err(json_error)?;
            persist_terminal_progress(
                &transaction,
                &AttemptId::new(id.clone())?,
                &state,
                &payload,
                *stored_records,
                *stored_event_seq,
            )?;
            transaction.execute(
                "UPDATE jobs SET current_status=?1 WHERE id=(SELECT job_id FROM job_attempts WHERE id=?2) AND current_attempt_number=(SELECT attempt_number FROM job_attempts WHERE id=?2)",
                params![enum_text(&state.status())?, id],
            ).map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE runs SET state=?1 WHERE attempt_id=?2 AND state=?3",
                    params![
                        enum_text(&state.status())?,
                        id,
                        enum_text(&JobStatus::Running)?
                    ],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        u64::try_from(ids.len())
            .map_err(|_| LabError::ResourceLimit("interrupted count overflow".into()))
    }

    /// CAS one running attempt to a terminal state.
    ///
    /// # Errors
    /// Rejects nonterminal targets, illegal transitions, stale attempts, or SQLite failure.
    pub fn finish_attempt(
        &mut self,
        attempt_id: &AttemptId,
        terminal: AttemptState,
    ) -> Result<JobAttempt, LabError> {
        if !terminal.is_terminal() {
            return Err(LabError::InvalidConfig(
                "finish_attempt requires terminal state".into(),
            ));
        }
        let current = self.load_attempt(attempt_id)?;
        if matches!(
            current.state,
            AttemptState::Running {
                cancel_requested_at: Some(_),
                ..
            }
        ) && !matches!(terminal, AttemptState::Cancelled { .. })
        {
            return Err(LabError::Cancelled(
                "cancel-requested attempt cannot publish a non-cancelled terminal state".into(),
            ));
        }
        self.replace_attempt_state(attempt_id, JobStatus::Running, &terminal)?;
        drop(terminal);
        self.load_attempt(attempt_id)
    }

    /// Atomically publish prepared success output and terminalize its uncancelled attempt.
    ///
    /// Expensive run reconstruction and artifact hashing happen before the short SQL
    /// transaction. The transaction repeats the RUNNING/no-cancel CAS before publishing.
    ///
    /// # Errors
    /// Rejects mismatched output, failed validation/hash checks, cancellation races,
    /// stale attempts, or SQLite failure. No partial publication is committed on error.
    #[expect(
        clippy::too_many_lines,
        reason = "single short publication transaction keeps CAS and effects visibly atomic"
    )]
    pub fn publish_attempt(
        &mut self,
        attempt_id: &AttemptId,
        terminal: AttemptState,
        publication: AttemptPublication,
    ) -> Result<(), LabError> {
        if !terminal.is_terminal() {
            return Err(LabError::InvalidConfig(
                "publish_attempt requires terminal state".into(),
            ));
        }
        let prepared_dataset = match &publication {
            AttemptPublication::Dataset(snapshot) => {
                Some(self.prepare_dataset_publication(snapshot)?)
            }
            _ => None,
        };
        let prepared_run = match &publication {
            AttemptPublication::Run {
                run_id,
                semantic_digest,
                expected_models,
                validation,
            } => {
                let prepared =
                    self.prepare_run_publication(run_id, semantic_digest, *expected_models)?;
                if serde_json::to_vec(validation).map_err(json_error)?
                    != serde_json::to_vec(&prepared.validation).map_err(json_error)?
                    || !terminal_matches_run(&terminal, run_id, &prepared)
                {
                    return Err(LabError::Conflict(
                        "terminal run output/validation differs from prepared publication".into(),
                    ));
                }
                Some(prepared)
            }
            AttemptPublication::Dataset(snapshot) => {
                if !terminal_matches_dataset(&terminal, &snapshot.manifest.id) {
                    return Err(LabError::Conflict(
                        "terminal dataset output differs from prepared publication".into(),
                    ));
                }
                None
            }
            AttemptPublication::Artifacts(artifacts) => {
                self.validate_artifact_publication(artifacts)?;
                if !terminal_matches_artifacts(&terminal, artifacts) {
                    return Err(LabError::Conflict(
                        "terminal artifact output differs from prepared publication".into(),
                    ));
                }
                None
            }
            AttemptPublication::Validation(report) => {
                if !terminal_matches_validation(&terminal, report)? {
                    return Err(LabError::Conflict(
                        "terminal validation output differs from prepared publication".into(),
                    ));
                }
                None
            }
            AttemptPublication::None => None,
        };

        let transaction = self.connection.transaction().map_err(sql_error)?;
        let (state_json, job_id, payload_json, stored_records, stored_event_seq): (
            String,
            String,
            String,
            i64,
            i64,
        ) = transaction
            .query_row(
                "SELECT a.state_json,a.job_id,j.payload_json,a.committed_records,a.last_committed_event_seq FROM job_attempts a JOIN jobs j ON j.id=a.job_id AND j.current_attempt_number=a.attempt_number WHERE a.id=?1",
                [attempt_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::Conflict("attempt is not the current job attempt".into()))?;
        let current: AttemptState = serde_json::from_str(&state_json).map_err(json_error)?;
        let payload = serde_json::from_str(&payload_json).map_err(json_error)?;
        match current {
            AttemptState::Running {
                cancel_requested_at: None,
                ..
            } => {}
            AttemptState::Running {
                cancel_requested_at: Some(_),
                ..
            } if matches!(&terminal, AttemptState::Cancelled { .. }) => {}
            AttemptState::Running {
                cancel_requested_at: Some(_),
                ..
            } => {
                return Err(LabError::Cancelled(
                    "cancel-requested attempt cannot publish success".into(),
                ));
            }
            _ => {
                return Err(LabError::Conflict(
                    "attempt left RUNNING before publication".into(),
                ));
            }
        }
        match (&publication, prepared_run) {
            (
                AttemptPublication::Run {
                    run_id,
                    semantic_digest,
                    validation,
                    ..
                },
                Some(prepared),
            ) => {
                let changed = transaction.execute(
                    "UPDATE runs SET state=?1,semantic_digest=?2 WHERE id=?3 AND attempt_id=?4 AND state=?5",
                    params![enum_text(&prepared.final_state)?, semantic_digest.as_str(), run_id.as_str(), attempt_id.as_str(), enum_text(&JobStatus::Running)?],
                ).map_err(sql_error)?;
                if changed != 1 {
                    return Err(LabError::Conflict("run publication CAS failed".into()));
                }
                insert_validation_row(&transaction, validation)?;
            }
            (AttemptPublication::Artifacts(artifacts), None) => {
                for artifact in artifacts {
                    insert_artifact_row(&transaction, artifact)?;
                }
            }
            (AttemptPublication::Dataset(snapshot), None) => {
                let prepared = prepared_dataset.as_ref().ok_or_else(|| {
                    LabError::Internal("dataset publication preparation disappeared".into())
                })?;
                super::insert_dataset_rows(&transaction, snapshot, prepared, None)?;
            }
            (AttemptPublication::Validation(report), None) => {
                insert_validation_row(&transaction, report)?;
            }
            (AttemptPublication::None, None) => {}
            _ => {
                return Err(LabError::Internal(
                    "publication preparation mismatch".into(),
                ));
            }
        }
        update_state(
            &transaction,
            attempt_id.as_str(),
            &enum_text(&JobStatus::Running)?,
            &terminal,
        )?;
        insert_attempt_output(&transaction, attempt_id, &terminal)?;
        persist_terminal_progress(
            &transaction,
            attempt_id,
            &terminal,
            &payload,
            stored_records,
            stored_event_seq,
        )?;
        if matches!(
            terminal.status(),
            JobStatus::Failed | JobStatus::Cancelled | JobStatus::Interrupted
        ) {
            transaction
                .execute(
                    "UPDATE runs SET state=?1 WHERE attempt_id=?2 AND state=?3",
                    params![
                        enum_text(&terminal.status())?,
                        attempt_id.as_str(),
                        enum_text(&JobStatus::Running)?
                    ],
                )
                .map_err(sql_error)?;
        }
        let changed = transaction.execute(
            "UPDATE jobs SET current_status=?1 WHERE id=?2 AND current_attempt_number=(SELECT attempt_number FROM job_attempts WHERE id=?3)",
            params![enum_text(&terminal.status())?, job_id, attempt_id.as_str()],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict("job publication CAS failed".into()));
        }
        transaction.commit().map_err(sql_error)?;
        drop(terminal);
        drop(publication);
        Ok(())
    }

    /// Monotonically update durable progress for a running attempt.
    ///
    /// # Errors
    /// Rejects invalid/regressing progress, nonrunning attempts, or SQLite failure.
    pub fn update_progress(
        &mut self,
        attempt_id: &AttemptId,
        progress: &JobProgress,
    ) -> Result<JobAttempt, LabError> {
        if progress.stage.is_empty() || progress.stage.len() > 128 {
            return Err(LabError::InvalidConfig(
                "job progress stage must be 1..=128 bytes".into(),
            ));
        }
        if progress.committed_records.is_some() != progress.count_unit.is_some() {
            return Err(LabError::InvalidConfig(
                "progress count and count_unit must be present together".into(),
            ));
        }
        let payload_kind: String = self.connection.query_row(
            "SELECT j.payload_kind FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=?1",
            [attempt_id.as_str()],
            |row| row.get(0),
        ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("unknown job attempt".into()))?;
        let expected_unit = count_unit_for_kind(&payload_kind)?;
        if progress
            .count_unit
            .is_some_and(|unit| unit != expected_unit)
            || (progress.last_committed_event_seq.is_some() && payload_kind != "BACKTEST")
        {
            return Err(LabError::InvalidConfig(
                "progress counters do not match job kind".into(),
            ));
        }
        let records = progress.committed_records.map(u64_to_i64).transpose()?;
        let event_seq = progress
            .last_committed_event_seq
            .map(u64_to_i64)
            .transpose()?;
        let changed = self.connection.execute(
            "UPDATE job_attempts SET progress_stage=?1,committed_records=COALESCE(?2,committed_records),last_committed_event_seq=COALESCE(?3,last_committed_event_seq) WHERE id=?4 AND status=?5 AND (?2 IS NULL OR committed_records<=?2) AND (?3 IS NULL OR last_committed_event_seq<=?3)",
            params![progress.stage, records, event_seq, attempt_id.as_str(), enum_text(&JobStatus::Running)?],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "progress update is stale, regressing, or nonrunning".into(),
            ));
        }
        self.load_attempt(attempt_id)
    }

    fn current_attempt(&self, job_id: &JobId) -> Result<JobAttempt, LabError> {
        let id = self.connection.query_row(
            "SELECT a.id FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_number=j.current_attempt_number WHERE j.id=?1",
            [job_id.as_str()],
            |row| row.get::<_, String>(0),
        ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("unknown job".into()))?;
        self.load_attempt(&AttemptId::new(id)?)
    }

    pub(super) fn load_attempt(&self, id: &AttemptId) -> Result<JobAttempt, LabError> {
        let row = self.connection.query_row(
            "SELECT a.job_id,a.attempt_number,a.input_digest,a.state_json,a.progress_stage,a.committed_records,a.last_committed_event_seq,j.payload_json FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=?1",
            [id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, u32>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?,row.get::<_, String>(4)?,row.get::<_, i64>(5)?,row.get::<_, i64>(6)?,row.get::<_, String>(7)?)),
        ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("unknown job attempt".into()))?;
        let state: AttemptState = serde_json::from_str(&row.3).map_err(json_error)?;
        let payload = serde_json::from_str(&row.7).map_err(json_error)?;
        Ok(JobAttempt {
            id: id.clone(),
            job_id: JobId::new(row.0)?,
            number: row.1,
            input_digest: ContentHash::try_from(row.2)?,
            progress: project_progress(&self.connection, &state, &payload, &row.4, row.5, row.6)?,
            state,
        })
    }

    fn replace_attempt_state(
        &mut self,
        id: &AttemptId,
        expected: JobStatus,
        next: &AttemptState,
    ) -> Result<(), LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let (job_id, payload_json, stored_records, stored_event_seq): (String, String, i64, i64) = transaction
            .query_row(
                "SELECT a.job_id,j.payload_json,a.committed_records,a.last_committed_event_seq FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=?1",
                [id.as_str()],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown job attempt".into()))?;
        update_state(&transaction, id.as_str(), &enum_text(&expected)?, next)?;
        insert_attempt_output(&transaction, id, next)?;
        let payload = serde_json::from_str(&payload_json).map_err(json_error)?;
        persist_terminal_progress(
            &transaction,
            id,
            next,
            &payload,
            stored_records,
            stored_event_seq,
        )?;
        if matches!(
            next.status(),
            JobStatus::Failed | JobStatus::Cancelled | JobStatus::Interrupted
        ) {
            transaction
                .execute(
                    "UPDATE runs SET state=?1 WHERE attempt_id=?2 AND state=?3",
                    params![
                        enum_text(&next.status())?,
                        id.as_str(),
                        enum_text(&JobStatus::Running)?
                    ],
                )
                .map_err(sql_error)?;
        }
        transaction.execute(
            "UPDATE jobs SET current_status=?1 WHERE id=?2 AND current_attempt_number=(SELECT attempt_number FROM job_attempts WHERE id=?3)",
            params![enum_text(&next.status())?, job_id, id.as_str()],
        ).map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }
}

fn insert_attempt(
    transaction: &Transaction<'_>,
    id: &AttemptId,
    job_id: &JobId,
    number: u32,
    input_digest: &ContentHash,
    state: &AttemptState,
) -> Result<(), LabError> {
    let queued_at = match state {
        AttemptState::Queued { queued_at } => *queued_at,
        _ => return Err(LabError::Internal("new attempt must start queued".into())),
    };
    transaction.execute(
        "INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms) VALUES (?1,?2,?3,?4,?5,?6,?7,0,0,?8)",
        params![id.as_str(), job_id.as_str(), number, input_digest.as_str(), enum_text(&state.status())?, serde_json::to_string(state).map_err(json_error)?, INITIAL_STAGE, timestamp_ms(queued_at)],
    ).map_err(sql_error)?;
    Ok(())
}

fn update_state(
    transaction: &Transaction<'_>,
    id: &str,
    expected_status: &str,
    next: &AttemptState,
) -> Result<(), LabError> {
    let (started, ended, cancel) = state_times(next);
    let changed = transaction.execute(
        "UPDATE job_attempts SET status=?1,state_json=?2,started_at_ms=COALESCE(started_at_ms,?3),ended_at_ms=?4,cancel_requested_at_ms=?5 WHERE id=?6 AND status=?7",
        params![enum_text(&next.status())?, serde_json::to_string(next).map_err(json_error)?, started.map(timestamp_ms), ended.map(timestamp_ms), cancel.map(timestamp_ms), id, expected_status],
    ).map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::Conflict(
            "job attempt state transition CAS failed".into(),
        ));
    }
    Ok(())
}

fn state_times(
    state: &AttemptState,
) -> (
    Option<UtcTimestamp>,
    Option<UtcTimestamp>,
    Option<UtcTimestamp>,
) {
    match state {
        AttemptState::Queued { .. } => (None, None, None),
        AttemptState::Running {
            started_at,
            cancel_requested_at,
        } => (Some(*started_at), None, *cancel_requested_at),
        AttemptState::Completed { ended_at, .. }
        | AttemptState::Partial { ended_at, .. }
        | AttemptState::Failed { ended_at, .. }
        | AttemptState::Cancelled { ended_at, .. }
        | AttemptState::Interrupted { ended_at, .. }
        | AttemptState::Blocked { ended_at, .. } => (None, Some(*ended_at), None),
    }
}

fn insert_attempt_output(
    transaction: &Transaction<'_>,
    attempt_id: &AttemptId,
    state: &AttemptState,
) -> Result<(), LabError> {
    let output = match state {
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            Some(output)
        }
        AttemptState::Blocked { output, .. } => output.as_ref(),
        _ => None,
    };
    match output {
        Some(JobOutput::Dataset { dataset_id }) => {
            transaction
                .execute(
                    "INSERT INTO job_attempt_dataset_outputs(attempt_id,dataset_id) VALUES (?1,?2)",
                    params![attempt_id.as_str(), dataset_id.as_str()],
                )
                .map_err(sql_error)?;
        }
        Some(JobOutput::Run {
            run_id,
            completed_models,
            blocked_models,
        }) => {
            transaction.execute(
                "INSERT INTO job_attempt_run_outputs(attempt_id,run_id,completed_models,blocked_models) VALUES (?1,?2,?3,?4)",
                params![attempt_id.as_str(), run_id.as_str(), u64_to_i64(*completed_models)?, u64_to_i64(*blocked_models)?],
            ).map_err(sql_error)?;
        }
        Some(JobOutput::Artifacts { artifact_ids }) => {
            for (position, artifact) in artifact_ids.iter().enumerate() {
                transaction.execute(
                    "INSERT INTO job_attempt_artifact_outputs(attempt_id,artifact_id,position) VALUES (?1,?2,?3)",
                    params![attempt_id.as_str(), artifact.as_str(), i64::try_from(position).map_err(|_| LabError::ResourceLimit("artifact output position overflow".into()))?],
                ).map_err(sql_error)?;
            }
        }
        Some(JobOutput::Validation { .. }) | None => {}
    }
    Ok(())
}

fn terminal_output(state: &AttemptState) -> Option<&JobOutput> {
    match state {
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            Some(output)
        }
        AttemptState::Blocked { output, .. } => output.as_ref(),
        _ => None,
    }
}

fn terminal_matches_run(
    terminal: &AttemptState,
    run_id: &RunId,
    prepared: &super::run_store::PreparedRunPublication,
) -> bool {
    if terminal.status() != prepared.final_state {
        return false;
    }
    matches!(
        terminal_output(terminal),
        Some(JobOutput::Run {
            run_id: output_run,
            completed_models,
            blocked_models,
        }) if output_run == run_id
            && *completed_models == prepared.completed_models
            && *blocked_models == prepared.blocked_models
    )
}

fn terminal_matches_dataset(
    terminal: &AttemptState,
    dataset_id: &crate::contracts::DatasetId,
) -> bool {
    matches!(
        terminal_output(terminal),
        Some(JobOutput::Dataset { dataset_id: output }) if output == dataset_id
    )
}

fn terminal_matches_artifacts(terminal: &AttemptState, artifacts: &[ArtifactRef]) -> bool {
    let expected = artifacts
        .iter()
        .map(|artifact| &artifact.id)
        .collect::<Vec<_>>();
    matches!(
        terminal_output(terminal),
        Some(JobOutput::Artifacts { artifact_ids })
            if artifact_ids.iter().collect::<Vec<_>>() == expected
    )
}

fn terminal_matches_validation(
    terminal: &AttemptState,
    report: &ValidationReport,
) -> Result<bool, LabError> {
    match terminal_output(terminal) {
        Some(JobOutput::Validation { report: output }) => Ok(serde_json::to_vec(output)
            .map_err(json_error)?
            == serde_json::to_vec(report).map_err(json_error)?),
        _ => Ok(false),
    }
}

fn enforce_queue_capacity(connection: &rusqlite::Connection) -> Result<(), LabError> {
    let queued: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM job_attempts WHERE status=?1",
            [enum_text(&JobStatus::Queued)?],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if queued >= MAX_QUEUED_JOBS {
        return Err(LabError::CapacityExceeded(format!(
            "durable job queue limit is {MAX_QUEUED_JOBS}"
        )));
    }
    Ok(())
}

fn attempt_id(job_id: &JobId, number: u32) -> AttemptId {
    AttemptId::from_seed(&format!("{}:{number}", job_id.as_str()))
}

fn payload_kind(payload: &crate::contracts::JobPayload) -> &'static str {
    match payload {
        crate::contracts::JobPayload::Collect { .. } => "COLLECT",
        crate::contracts::JobPayload::Backtest { .. } => "BACKTEST",
        crate::contracts::JobPayload::Export { .. } => "EXPORT",
        crate::contracts::JobPayload::Verify { .. } => "VERIFY",
    }
}

fn nonnegative_u64(value: i64, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt(format!("negative stored {label}")))
}

fn project_progress(
    connection: &rusqlite::Connection,
    state: &AttemptState,
    payload: &crate::contracts::JobPayload,
    stored_stage: &str,
    stored_records: i64,
    stored_event_seq: i64,
) -> Result<JobProgress, LabError> {
    let stored_records = nonnegative_u64(stored_records, "committed_records")?;
    let stored_event_seq = nonnegative_u64(stored_event_seq, "last_committed_event_seq")?;
    let unit = count_unit_for_kind(payload_kind(payload))?;
    let prior_count = (stored_records > 0).then_some(stored_records);
    let prior_seq = (matches!(payload, crate::contracts::JobPayload::Backtest { .. })
        && stored_event_seq > 0)
        .then_some(stored_event_seq);
    match state {
        AttemptState::Queued { .. } => Ok(JobProgress {
            stage: "queued".into(),
            committed_records: None,
            count_unit: None,
            last_committed_event_seq: None,
        }),
        AttemptState::Running { .. } => Ok(JobProgress {
            stage: stored_stage.into(),
            committed_records: prior_count,
            count_unit: prior_count.map(|_| unit),
            last_committed_event_seq: prior_seq,
        }),
        AttemptState::Completed { output, .. }
        | AttemptState::Partial { output, .. }
        | AttemptState::Blocked {
            output: Some(output),
            ..
        } => terminal_output_progress(connection, state.status(), output),
        AttemptState::Blocked { output: None, .. } => Ok(JobProgress {
            stage: "blocked".into(),
            committed_records: prior_count,
            count_unit: prior_count.map(|_| unit),
            last_committed_event_seq: prior_seq,
        }),
        AttemptState::Failed { .. } => Ok(JobProgress {
            stage: "failed".into(),
            committed_records: prior_count,
            count_unit: prior_count.map(|_| unit),
            last_committed_event_seq: prior_seq,
        }),
        AttemptState::Cancelled { .. } => Ok(JobProgress {
            stage: "cancelled".into(),
            committed_records: prior_count,
            count_unit: prior_count.map(|_| unit),
            last_committed_event_seq: prior_seq,
        }),
        AttemptState::Interrupted { .. } => Ok(JobProgress {
            stage: "interrupted".into(),
            committed_records: prior_count,
            count_unit: prior_count.map(|_| unit),
            last_committed_event_seq: prior_seq,
        }),
    }
}

fn persist_terminal_progress(
    transaction: &Transaction<'_>,
    attempt_id: &AttemptId,
    state: &AttemptState,
    payload: &crate::contracts::JobPayload,
    stored_records: i64,
    stored_event_seq: i64,
) -> Result<(), LabError> {
    let progress = project_progress(
        transaction,
        state,
        payload,
        "terminal",
        stored_records,
        stored_event_seq,
    )?;
    transaction.execute(
        "UPDATE job_attempts SET progress_stage=?1,committed_records=?2,last_committed_event_seq=?3 WHERE id=?4",
        params![progress.stage, u64_to_i64(progress.committed_records.unwrap_or(0))?, u64_to_i64(progress.last_committed_event_seq.unwrap_or(0))?, attempt_id.as_str()],
    ).map_err(sql_error)?;
    Ok(())
}

fn terminal_output_progress(
    connection: &rusqlite::Connection,
    status: JobStatus,
    output: &JobOutput,
) -> Result<JobProgress, LabError> {
    match output {
        JobOutput::Dataset { dataset_id } => {
            let rows: i64 = connection
                .query_row(
                    "SELECT row_count FROM datasets WHERE id=?1",
                    [dataset_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let stage = match status {
                JobStatus::Partial => "dataset_partial",
                JobStatus::Blocked => "dataset_blocked",
                _ => "dataset_published",
            };
            Ok(JobProgress {
                stage: stage.into(),
                committed_records: Some(nonnegative_u64(rows, "dataset row count")?),
                count_unit: Some(ProgressCountUnit::DatasetRows),
                last_committed_event_seq: None,
            })
        }
        JobOutput::Run { run_id, .. } => {
            // Event sequences are contiguous from 1, so the committed count is
            // the last sequence; this stays correct after ledger compaction
            // removes run_events rows.
            let (facts, last): (i64, i64) = connection.query_row(
                "SELECT last_committed_event_seq,last_committed_event_seq FROM runs WHERE id=?1",
                [run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).map_err(sql_error)?;
            let stage = match status {
                JobStatus::Completed => "run_completed",
                JobStatus::Partial => "run_partial",
                JobStatus::Blocked => "run_blocked",
                _ => "run_terminal",
            };
            Ok(JobProgress {
                stage: stage.into(),
                committed_records: Some(nonnegative_u64(facts, "run fact count")?),
                count_unit: Some(ProgressCountUnit::LedgerFacts),
                last_committed_event_seq: Some(nonnegative_u64(last, "run last event sequence")?),
            })
        }
        JobOutput::Artifacts { artifact_ids } => {
            Ok(JobProgress {
                stage: match status {
                    JobStatus::Partial => "artifacts_partial",
                    JobStatus::Blocked => "artifacts_blocked",
                    _ => "artifacts_published",
                }
                .into(),
                committed_records: Some(u64::try_from(artifact_ids.len()).map_err(|_| {
                    LabError::ResourceLimit("artifact progress count overflow".into())
                })?),
                count_unit: Some(ProgressCountUnit::Artifacts),
                last_committed_event_seq: None,
            })
        }
        JobOutput::Validation { report } => Ok(JobProgress {
            stage: match status {
                JobStatus::Partial => "validation_partial",
                JobStatus::Blocked => "validation_blocked",
                _ => "validation_completed",
            }
            .into(),
            committed_records: Some(report.checked_models),
            count_unit: Some(ProgressCountUnit::CheckedModels),
            last_committed_event_seq: None,
        }),
    }
}

fn count_unit_for_kind(kind: &str) -> Result<ProgressCountUnit, LabError> {
    match kind {
        "COLLECT" => Ok(ProgressCountUnit::DatasetRows),
        "BACKTEST" => Ok(ProgressCountUnit::LedgerFacts),
        "EXPORT" => Ok(ProgressCountUnit::Artifacts),
        "VERIFY" => Ok(ProgressCountUnit::CheckedModels),
        _ => Err(LabError::DataCorrupt(format!("unknown job kind {kind}"))),
    }
}
