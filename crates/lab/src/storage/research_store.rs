//! Durable research-suite state and atomic ordinary-job ownership links.

use super::job_store::{insert_job_tx, request_cancel_tx, retry_job_tx};
use super::{Store, enum_text, json_error, sql_error, timestamp_from_ms, timestamp_ms};
use crate::contracts::{
    AttemptId, AttemptState, ContentHash, FailureRecord, FrozenResearchSuite, JobId, JobOutput,
    JobPayload, JobRecord, JobSubmission, LabError, MAX_ACTIVE_SUITES, MAX_RESEARCH_PAGE,
    MAX_SUITE_RUNS, PlanId, ResearchPage, RunId, RunModelComparison, SuiteCase, SuiteCaseId,
    SuiteCaseStatus, SuiteComparisonRow, SuiteId, SuitePhase, SuiteRecord, SuiteStatus,
    SuiteSummary, UtcRange, UtcTimestamp, WalkForwardFold,
};
use crate::research::SelectionOutcome;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

impl Store {
    /// Persist one immutable suite and all stable planned case identities.
    ///
    /// # Errors
    /// Rejects conflicting request reuse, corrupt geometry, active-suite overflow or SQLite failure.
    pub fn create_research_suite(
        &mut self,
        frozen: &FrozenResearchSuite,
        cases: &[SuiteCase],
        now: UtcTimestamp,
    ) -> Result<SuiteRecord, LabError> {
        let expected_frozen =
            crate::research::freeze(frozen.request.clone(), frozen.policy_revisions.clone())?;
        if serde_json::to_value(&expected_frozen).map_err(json_error)?
            != serde_json::to_value(frozen).map_err(json_error)?
        {
            return Err(LabError::InputHashMismatch(
                "frozen suite body or identity is not canonical".into(),
            ));
        }
        let expected = crate::research::initial_cases(frozen)?;
        if serde_json::to_value(&expected).map_err(json_error)?
            != serde_json::to_value(cases).map_err(json_error)?
        {
            return Err(LabError::InputHashMismatch(
                "suite cases differ from frozen geometry".into(),
            ));
        }
        if let Some((id, digest)) = self
            .connection
            .query_row(
                "SELECT id,input_digest FROM research_suites WHERE request_id=?1",
                [frozen.request.request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
        {
            if id != frozen.id.as_str() || digest != frozen.input_digest.as_str() {
                return Err(LabError::Conflict(
                    "suite request id already names different frozen input".into(),
                ));
            }
            return self
                .get_research_suite(&frozen.id)?
                .ok_or_else(|| LabError::DataCorrupt("idempotent suite disappeared".into()));
        }
        let active: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM research_suites WHERE status IN ('running','paused')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if active >= i64::try_from(MAX_ACTIVE_SUITES).unwrap_or(i64::MAX) {
            return Err(LabError::CapacityExceeded(format!(
                "active research suite limit is {MAX_ACTIVE_SUITES}"
            )));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO research_suites(id,request_id,input_digest,status,created_at_ms,next_action_at_ms,frozen_json) VALUES (?1,?2,?3,?4,?5,?5,?6)",
            params![frozen.id.as_str(), frozen.request.request_id.as_str(), frozen.input_digest.as_str(), enum_text(&SuiteStatus::Running)?, timestamp_ms(now), serde_json::to_string(frozen).map_err(json_error)?],
        ).map_err(sql_error)?;
        for fold in &frozen.folds {
            transaction.execute(
                "INSERT INTO research_suite_folds(suite_id,fold_index,fold_json) VALUES (?1,?2,?3)",
                params![frozen.id.as_str(), fold.index, serde_json::to_string(fold).map_err(json_error)?],
            ).map_err(sql_error)?;
        }
        for case in cases {
            insert_case(&transaction, case)?;
        }
        transaction.commit().map_err(sql_error)?;
        self.get_research_suite(&frozen.id)?
            .ok_or_else(|| LabError::Internal("created suite disappeared".into()))
    }

    /// Load a suite with durable folds and cases.
    ///
    /// # Errors
    /// Returns an error for corrupt persisted state or SQLite failure.
    pub fn get_research_suite(&self, id: &SuiteId) -> Result<Option<SuiteRecord>, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT status,created_at_ms,next_action_at_ms,failure_json,frozen_json FROM research_suites WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let Some((status, created_at, next_action_at, failure_json, frozen_json)) = row else {
            return Ok(None);
        };
        let frozen: FrozenResearchSuite = serde_json::from_str(&frozen_json).map_err(json_error)?;
        if frozen.id != *id {
            return Err(LabError::DataCorrupt(
                "stored suite identity differs from frozen body".into(),
            ));
        }
        Ok(Some(SuiteRecord {
            frozen,
            status: enum_from_text(&status)?,
            created_at: timestamp_from_ms(created_at)?,
            next_action_at: timestamp_from_ms(next_action_at)?,
            failure: failure_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?,
            folds: load_folds(&self.connection, id)?,
            cases: load_cases(
                &self.connection,
                id,
                0,
                u64::try_from(MAX_SUITE_RUNS)
                    .map_err(|_| LabError::ResourceLimit("suite run limit overflow".into()))?,
            )?,
        }))
    }

    /// List stable suite summaries in creation order.
    ///
    /// # Errors
    /// Rejects invalid paging or corrupt persisted state.
    pub fn list_research_suites(
        &self,
        offset: u64,
        limit: u32,
    ) -> Result<ResearchPage<SuiteSummary>, LabError> {
        validate_page(limit)?;
        let total_count = count_rows(&self.connection, "research_suites")?;
        let mut statement = self
            .connection
            .prepare("SELECT id FROM research_suites ORDER BY created_at_ms,id LIMIT ?1 OFFSET ?2")
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![i64::from(limit), page_i64(offset)?], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let mut records = Vec::new();
        for row in rows {
            let id = SuiteId::new(row.map_err(sql_error)?)?;
            let record = self
                .get_research_suite(&id)?
                .ok_or_else(|| LabError::DataCorrupt("listed suite disappeared".into()))?;
            records.push(crate::research::summarize(&record));
        }
        Ok(page(records, offset, total_count))
    }

    /// List stable suite cases.
    ///
    /// # Errors
    /// Rejects invalid paging or corrupt persisted state.
    pub fn list_suite_cases(
        &self,
        id: &SuiteId,
        offset: u64,
        limit: u32,
    ) -> Result<ResearchPage<SuiteCase>, LabError> {
        validate_page(limit)?;
        require_suite(&self.connection, id)?;
        let total_count = count_for_suite(&self.connection, "research_suite_cases", id)?;
        Ok(page(
            load_cases(&self.connection, id, offset, u64::from(limit))?,
            offset,
            total_count,
        ))
    }

    /// List compact comparisons for ordinary runs owned by this suite.
    ///
    /// # Errors
    /// Rejects invalid paging or corrupt comparison JSON.
    pub fn list_suite_comparisons(
        &self,
        id: &SuiteId,
        offset: u64,
        limit: u32,
    ) -> Result<ResearchPage<SuiteComparisonRow>, LabError> {
        validate_page(limit)?;
        let frozen = self
            .get_research_suite(id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?
            .frozen;
        let total: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM run_model_comparisons c JOIN research_suite_cases s ON s.run_id=c.run_id WHERE s.suite_id=?1",
            [id.as_str()],
            |row| row.get(0),
        ).map_err(sql_error)?;
        let total_count = nonnegative_u64(total, "suite comparison count")?;
        let mut statement = self.connection.prepare(
            "SELECT s.id,s.fold_index,s.phase,s.scenario_index,s.range_start_ms,s.range_end_ms,c.comparison_json FROM run_model_comparisons c JOIN research_suite_cases s ON s.run_id=c.run_id WHERE s.suite_id=?1 ORDER BY s.case_index,c.model_id LIMIT ?2 OFFSET ?3",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![id.as_str(), i64::from(limit), page_i64(offset)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<u32>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u32>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut records = Vec::new();
        for row in rows {
            let (case_id, fold_index, phase, scenario_index, start, end, comparison) =
                row.map_err(sql_error)?;
            let costs = frozen
                .scenarios
                .get(usize::try_from(scenario_index).map_err(|_| {
                    LabError::DataCorrupt("stored scenario index is not representable".into())
                })?)
                .ok_or_else(|| {
                    LabError::DataCorrupt("stored scenario index exceeds frozen suite".into())
                })?
                .clone();
            records.push(SuiteComparisonRow {
                case_id: SuiteCaseId::new(case_id)?,
                fold_index,
                phase: enum_from_text(&phase)?,
                scenario_index,
                range: UtcRange::new(timestamp_from_ms(start)?, timestamp_from_ms(end)?)?,
                costs,
                comparison: serde_json::from_str(&comparison).map_err(json_error)?,
            });
        }
        Ok(page(records, offset, total_count))
    }

    /// Return bounded running suites ready for one coordinator decision.
    ///
    /// # Errors
    /// Rejects an invalid limit or corrupt identities.
    pub fn due_suite_ids(&self, now: UtcTimestamp, limit: u32) -> Result<Vec<SuiteId>, LabError> {
        if limit == 0 || usize::try_from(limit).unwrap_or(usize::MAX) > MAX_ACTIVE_SUITES {
            return Err(LabError::ResourceLimit(format!(
                "due suite limit must be 1..={MAX_ACTIVE_SUITES}"
            )));
        }
        let mut statement = self.connection.prepare(
            "SELECT id FROM research_suites WHERE status='running' AND next_action_at_ms<=?1 ORDER BY next_action_at_ms,id LIMIT ?2",
        ).map_err(sql_error)?;
        statement
            .query_map(params![timestamp_ms(now), i64::from(limit)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?
            .map(|row| SuiteId::new(row.map_err(sql_error)?))
            .collect()
    }

    /// Read the next admissible planned case without mutating state.
    ///
    /// # Errors
    /// Returns an error for missing/corrupt suite state.
    pub fn next_suite_case(&self, id: &SuiteId) -> Result<Option<SuiteCase>, LabError> {
        let record = self
            .get_research_suite(id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?;
        if record.status != SuiteStatus::Running {
            return Ok(None);
        }
        Ok(record.cases.into_iter().find(|case| {
            matches!(
                case.status,
                SuiteCaseStatus::Planned | SuiteCaseStatus::RetryPending
            ) && (case.phase != SuitePhase::Evaluation
                || record.folds.iter().any(|fold| {
                    Some(fold.index) == case.fold_index
                        && fold.winner.is_some()
                        && fold.selection_digest.is_some()
                }))
        }))
    }

    /// Atomically attach a prepared plan and admit its ordinary child job.
    ///
    /// Queue pressure rolls back both the job and case attachment, so a later
    /// coordinator tick can retry the same stable preparation without repair.
    ///
    /// # Errors
    /// Rejects a stale/nonplanned case, mismatched plan input, full queue or SQLite failure.
    pub fn admit_prepared_suite_case(
        &mut self,
        case_id: &SuiteCaseId,
        plan_id: &PlanId,
        causal_input_digest: &ContentHash,
        submission: &JobSubmission,
        now: UtcTimestamp,
    ) -> Result<JobRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let mut case = load_case_tx(&transaction, case_id)?;
        require_running_suite(&transaction, &case.suite_id)?;
        match (&case.plan_id, &case.causal_input_digest) {
            (None, None) if case.status == SuiteCaseStatus::Planned => {
                case.plan_id = Some(plan_id.clone());
                case.causal_input_digest = Some(causal_input_digest.clone());
            }
            (Some(stored_plan), Some(stored_causal))
                if stored_plan == plan_id && stored_causal == causal_input_digest => {}
            _ => {
                return Err(LabError::Conflict(
                    "suite case preparation is stale or conflicting".into(),
                ));
            }
        }
        validate_case_submission(&transaction, &case, submission)?;
        let pin = insert_job_tx(&transaction, submission, now)?;
        if let (Some(job), Some(attempt)) = (&case.job_id, &case.attempt_id) {
            if job != &pin.job_id || attempt != &pin.attempt_id {
                return Err(LabError::Conflict(
                    "suite case is pinned to another job attempt".into(),
                ));
            }
        } else {
            let changed = transaction.execute(
                "UPDATE research_suite_cases SET status='queued',plan_id=?1,causal_input_digest=?2,job_id=?3,attempt_id=?4 WHERE id=?5 AND status='planned' AND plan_id IS NULL AND causal_input_digest IS NULL AND job_id IS NULL AND attempt_id IS NULL",
                params![plan_id.as_str(), causal_input_digest.as_str(), pin.job_id.as_str(), pin.attempt_id.as_str(), case_id.as_str()],
            ).map_err(sql_error)?;
            if changed != 1 {
                return Err(LabError::Conflict(
                    "prepared suite case admission CAS failed".into(),
                ));
            }
        }
        transaction.commit().map_err(sql_error)?;
        self.get_job(&pin.job_id)?
            .ok_or_else(|| LabError::Internal("admitted suite job disappeared".into()))
    }

    /// Atomically retry an incomplete terminal child and repin its new attempt.
    ///
    /// # Errors
    /// Rejects completed/nonterminal/stale cases, queue pressure or attempt overflow.
    pub fn retry_suite_case(
        &mut self,
        case_id: &SuiteCaseId,
        now: UtcTimestamp,
    ) -> Result<JobRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let case = load_case_tx(&transaction, case_id)?;
        require_running_suite(&transaction, &case.suite_id)?;
        if case.status != SuiteCaseStatus::RetryPending {
            return Err(LabError::Conflict(
                "suite case retry requires explicit resume intent".into(),
            ));
        }
        let job_id = case
            .job_id
            .ok_or_else(|| LabError::Conflict("unadmitted suite case cannot retry".into()))?;
        let pinned = case
            .attempt_id
            .ok_or_else(|| LabError::DataCorrupt("suite case job lacks attempt pin".into()))?;
        let current: String = transaction.query_row(
            "SELECT a.id FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_number=j.current_attempt_number WHERE j.id=?1",
            [job_id.as_str()],
            |row| row.get(0),
        ).map_err(sql_error)?;
        if current != pinned.as_str() {
            return Err(LabError::Conflict(
                "suite case attempt pin is not the job current attempt".into(),
            ));
        }
        let attempt_id = retry_job_tx(&transaction, &job_id, now)?;
        let changed = transaction.execute(
            "UPDATE research_suite_cases SET status='queued',attempt_id=?1,run_id=NULL,failure_json=NULL WHERE id=?2 AND attempt_id=?3 AND status='retry_pending'",
            params![attempt_id.as_str(), case_id.as_str(), pinned.as_str()],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "suite case retry repin CAS failed".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        self.get_job(&job_id)?
            .ok_or_else(|| LabError::Internal("retried suite job disappeared".into()))
    }

    /// Commit an immutable fold winner/digest only after all named selection runs are pinned.
    ///
    /// # Errors
    /// Rejects incomplete/mismatched selection lineage or conflicting repeat.
    pub fn commit_fold_selection(
        &mut self,
        suite_id: &SuiteId,
        fold_index: u32,
        expected_case_runs: &[(SuiteCaseId, RunId)],
        outcome: &SelectionOutcome,
    ) -> Result<WalkForwardFold, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let mut fold = load_fold_tx(&transaction, suite_id, fold_index)?;
        validate_selection_lineage(&transaction, suite_id, fold_index, expected_case_runs)?;
        let unavailable = validate_selection_outcome(&transaction, suite_id, outcome)?;
        if fold.selection_digest.is_some() || fold.winner.is_some() {
            if fold.winner == outcome.winner
                && fold.selection_digest.as_ref() == Some(&outcome.selection_digest)
                && serde_json::to_vec(&fold.unavailable_candidates).map_err(json_error)?
                    == serde_json::to_vec(&unavailable).map_err(json_error)?
            {
                return Ok(fold);
            }
            return Err(LabError::Conflict(
                "walk-forward selection was already committed differently".into(),
            ));
        }
        fold.winner.clone_from(&outcome.winner);
        fold.selection_digest = Some(outcome.selection_digest.clone());
        fold.unavailable_candidates = unavailable;
        let changed = transaction
            .execute(
                "UPDATE research_suite_folds SET fold_json=?1 WHERE suite_id=?2 AND fold_index=?3",
                params![
                    serde_json::to_string(&fold).map_err(json_error)?,
                    suite_id.as_str(),
                    fold_index
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "fold selection commit CAS failed".into(),
            ));
        }
        if fold.winner.is_none() {
            let failure = FailureRecord {
                code: "NO_RANKABLE_CANDIDATE".into(),
                message: format!(
                    "walk-forward fold {fold_index} has no rankable selection candidate"
                ),
            };
            transaction
                .execute(
                    "UPDATE research_suites SET status='blocked',failure_json=?2 WHERE id=?1 AND status='running'",
                    params![suite_id.as_str(), serde_json::to_string(&failure).map_err(json_error)?],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(fold)
    }

    /// Reconcile case projections from their immutable pinned attempts.
    ///
    /// # Errors
    /// Returns an error for missing/corrupt attempt lineage or SQLite failure.
    pub fn reconcile_research_suite(
        &mut self,
        id: &SuiteId,
        now: UtcTimestamp,
    ) -> Result<SuiteRecord, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        require_suite(&transaction, id)?;
        let mut statement = transaction.prepare(
            "SELECT id,attempt_id,status FROM research_suite_cases WHERE suite_id=?1 AND attempt_id IS NOT NULL AND status NOT IN ('completed','retry_pending') ORDER BY case_index",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut pending = Vec::new();
        for row in rows {
            pending.push(row.map_err(sql_error)?);
        }
        drop(statement);
        let mut blocking_failure = None;
        for (case_id, attempt_id, stored_status) in pending {
            let (state_json, run_id): (String, Option<String>) = transaction.query_row(
                "SELECT a.state_json,o.run_id FROM job_attempts a LEFT JOIN job_attempt_run_outputs o ON o.attempt_id=a.id WHERE a.id=?1",
                [&attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional().map_err(sql_error)?.ok_or_else(|| {
                LabError::DataCorrupt("suite case pinned attempt is missing".into())
            })?;
            let state: AttemptState = serde_json::from_str(&state_json).map_err(json_error)?;
            let (status, failure) = case_projection(&state, run_id.as_deref())?;
            if let Some(published_run_id) = run_id.as_deref() {
                validate_case_comparison_digest(&transaction, &case_id, published_run_id)?;
            }
            if matches!(
                status,
                SuiteCaseStatus::Failed | SuiteCaseStatus::Cancelled | SuiteCaseStatus::Interrupted
            ) || (status == SuiteCaseStatus::Blocked && run_id.is_none())
            {
                blocking_failure = blocking_failure.or_else(|| failure.clone());
            }
            if enum_text(&status)? != stored_status || run_id.is_some() || failure.is_some() {
                transaction.execute(
                    "UPDATE research_suite_cases SET status=?1,run_id=COALESCE(run_id,?2),failure_json=?3 WHERE id=?4 AND status<>'completed'",
                    params![enum_text(&status)?, run_id, failure.as_ref().map(serde_json::to_string).transpose().map_err(json_error)?, case_id],
                ).map_err(sql_error)?;
            }
        }
        if let Some(failure) = blocking_failure {
            transaction.execute(
                "UPDATE research_suites SET status='blocked',next_action_at_ms=?2,failure_json=?3 WHERE id=?1 AND status='running'",
                params![id.as_str(), timestamp_ms(now), serde_json::to_string(&failure).map_err(json_error)?],
            ).map_err(sql_error)?;
        }
        let remaining: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM research_suite_cases WHERE suite_id=?1 AND NOT (status='completed' OR (status='blocked' AND run_id IS NOT NULL))",
            [id.as_str()],
            |row| row.get(0),
        ).map_err(sql_error)?;
        if remaining == 0 {
            transaction.execute(
                "UPDATE research_suites SET status='completed',next_action_at_ms=?2,failure_json=NULL WHERE id=?1 AND status='running'",
                params![id.as_str(), timestamp_ms(now)],
            ).map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        self.get_research_suite(id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))
    }

    /// Pause admission and atomically cancel every incomplete owned child.
    ///
    /// # Errors
    /// Rejects completed/missing suites or corrupt child ownership.
    pub fn pause_research_suite(
        &mut self,
        id: &SuiteId,
        now: UtcTimestamp,
    ) -> Result<(SuiteRecord, Vec<JobId>), LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let status: String = transaction
            .query_row(
                "SELECT status FROM research_suites WHERE id=?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?;
        match enum_from_text::<SuiteStatus>(&status)? {
            SuiteStatus::Running | SuiteStatus::Blocked => {
                transaction.execute(
                    "UPDATE research_suites SET status='paused',next_action_at_ms=?2 WHERE id=?1",
                    params![id.as_str(), timestamp_ms(now)],
                ).map_err(sql_error)?;
            }
            SuiteStatus::Paused => {}
            SuiteStatus::Completed => {
                return Err(LabError::Conflict("completed suite cannot pause".into()));
            }
        }
        let mut statement = transaction.prepare(
            "SELECT id,job_id FROM research_suite_cases WHERE suite_id=?1 AND job_id IS NOT NULL AND status IN ('queued','running') ORDER BY case_index",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?;
        let mut children = Vec::new();
        for row in rows {
            children.push(row.map_err(sql_error)?);
        }
        drop(statement);
        let mut jobs = Vec::new();
        for (case_id, job_id) in children {
            let job_id = JobId::new(job_id)?;
            let attempt_id = request_cancel_tx(&transaction, &job_id, now)?;
            let status: String = transaction
                .query_row(
                    "SELECT status FROM job_attempts WHERE id=?1",
                    [attempt_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if status == enum_text(&crate::contracts::JobStatus::Cancelled)? {
                transaction.execute(
                    "UPDATE research_suite_cases SET status='cancelled',failure_json=?1 WHERE id=?2 AND status<>'completed'",
                    params![serde_json::to_string(&FailureRecord { code: "PAUSED".into(), message: "suite paused before child execution".into() }).map_err(json_error)?, case_id],
                ).map_err(sql_error)?;
            } else if status == enum_text(&crate::contracts::JobStatus::Running)? {
                jobs.push(job_id);
            }
        }
        transaction.commit().map_err(sql_error)?;
        let record = self
            .get_research_suite(id)?
            .ok_or_else(|| LabError::Internal("paused suite disappeared".into()))?;
        Ok((record, jobs))
    }

    /// Resume coordinator admission while retaining completed cases and fold winners.
    ///
    /// # Errors
    /// Rejects missing/completed suites or SQLite failure.
    pub fn resume_research_suite(
        &mut self,
        id: &SuiteId,
        now: UtcTimestamp,
    ) -> Result<SuiteRecord, LabError> {
        let current = self
            .get_research_suite(id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?;
        if current.status == SuiteStatus::Running {
            return Ok(current);
        }
        if current.status == SuiteStatus::Completed {
            return Err(LabError::Conflict("completed suite cannot resume".into()));
        }
        if current
            .folds
            .iter()
            .any(|fold| fold.selection_digest.is_some() && fold.winner.is_none())
        {
            return Err(LabError::Conflict(
                "suite has a committed fold with no rankable candidate".into(),
            ));
        }
        let active: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM research_suites WHERE id<>?1 AND status IN ('running','paused')",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if active >= i64::try_from(MAX_ACTIVE_SUITES).unwrap_or(i64::MAX) {
            return Err(LabError::CapacityExceeded(format!(
                "active research suite limit is {MAX_ACTIVE_SUITES}"
            )));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction.execute(
            "UPDATE research_suite_cases SET status='retry_pending' WHERE suite_id=?1 AND (status IN ('failed','interrupted','cancelled') OR (status='blocked' AND run_id IS NULL))",
            [id.as_str()],
        ).map_err(sql_error)?;
        let changed = transaction.execute(
            "UPDATE research_suites SET status='running',next_action_at_ms=?2,failure_json=NULL WHERE id=?1 AND status IN ('paused','blocked')",
            params![id.as_str(), timestamp_ms(now)],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "only paused or blocked suites can resume".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        self.get_research_suite(id)?
            .ok_or_else(|| LabError::Internal("resumed suite disappeared".into()))
    }

    /// Persist an owner-level blocking failure without inventing a child attempt.
    ///
    /// # Errors
    /// Rejects missing/completed suites or SQLite failure.
    pub fn block_research_suite(
        &mut self,
        id: &SuiteId,
        failure: &FailureRecord,
        now: UtcTimestamp,
    ) -> Result<SuiteRecord, LabError> {
        let changed = self.connection.execute(
            "UPDATE research_suites SET status='blocked',next_action_at_ms=?2,failure_json=?3 WHERE id=?1 AND status<>'completed'",
            params![id.as_str(), timestamp_ms(now), serde_json::to_string(failure).map_err(json_error)?],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "completed or missing suite cannot be blocked".into(),
            ));
        }
        self.get_research_suite(id)?
            .ok_or_else(|| LabError::Internal("blocked suite disappeared".into()))
    }

    /// Advance the fairness timestamp after one bounded coordinator decision.
    ///
    /// # Errors
    /// Rejects a missing/nonrunning suite or SQLite failure.
    pub fn defer_research_suite(
        &mut self,
        id: &SuiteId,
        next_action_at: UtcTimestamp,
    ) -> Result<(), LabError> {
        let changed = self
            .connection
            .execute(
                "UPDATE research_suites SET next_action_at_ms=?1 WHERE id=?2 AND status='running'",
                params![timestamp_ms(next_action_at), id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict("research suite is not running".into()));
        }
        Ok(())
    }
}

fn insert_case(transaction: &Transaction<'_>, case: &SuiteCase) -> Result<(), LabError> {
    transaction.execute(
        "INSERT INTO research_suite_cases(id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![case.id.as_str(), case.suite_id.as_str(), case.index, case.fold_index, enum_text(&case.phase)?, case.scenario_index, timestamp_ms(case.range.start()), timestamp_ms(case.range.end()), enum_text(&case.status)?, case.plan_id.as_ref().map(PlanId::as_str), case.job_id.as_ref().map(JobId::as_str), case.attempt_id.as_ref().map(AttemptId::as_str), case.run_id.as_ref().map(RunId::as_str), case.causal_input_digest.as_ref().map(ContentHash::as_str), case.failure.as_ref().map(serde_json::to_string).transpose().map_err(json_error)?],
    ).map_err(sql_error)?;
    Ok(())
}

#[cfg(test)]
fn load_case(connection: &rusqlite::Connection, id: &SuiteCaseId) -> Result<SuiteCase, LabError> {
    let row = connection.query_row(
        "SELECT id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json FROM research_suite_cases WHERE id=?1",
        [id.as_str()],
        raw_case_row,
    ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("unknown suite case".into()))?;
    decode_case(row)
}

fn load_case_tx(transaction: &Transaction<'_>, id: &SuiteCaseId) -> Result<SuiteCase, LabError> {
    let row = transaction.query_row(
        "SELECT id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json FROM research_suite_cases WHERE id=?1",
        [id.as_str()],
        raw_case_row,
    ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("unknown suite case".into()))?;
    decode_case(row)
}

type RawCaseRow = (
    String,
    String,
    u32,
    Option<u32>,
    String,
    u32,
    i64,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn raw_case_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawCaseRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
        row.get(13)?,
        row.get(14)?,
    ))
}

fn decode_case(row: RawCaseRow) -> Result<SuiteCase, LabError> {
    Ok(SuiteCase {
        id: SuiteCaseId::new(row.0)?,
        suite_id: SuiteId::new(row.1)?,
        index: row.2,
        fold_index: row.3,
        phase: enum_from_text(&row.4)?,
        scenario_index: row.5,
        range: UtcRange::new(timestamp_from_ms(row.6)?, timestamp_from_ms(row.7)?)?,
        status: enum_from_text(&row.8)?,
        plan_id: row.9.map(PlanId::new).transpose()?,
        job_id: row.10.map(JobId::new).transpose()?,
        attempt_id: row.11.map(AttemptId::new).transpose()?,
        run_id: row.12.map(RunId::new).transpose()?,
        causal_input_digest: row.13.map(ContentHash::try_from).transpose()?,
        failure: row
            .14
            .map(|json| serde_json::from_str(&json).map_err(json_error))
            .transpose()?,
    })
}

fn load_cases(
    connection: &rusqlite::Connection,
    id: &SuiteId,
    offset: u64,
    limit: u64,
) -> Result<Vec<SuiteCase>, LabError> {
    let mut statement = connection.prepare(
        "SELECT id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json FROM research_suite_cases WHERE suite_id=?1 ORDER BY case_index LIMIT ?2 OFFSET ?3",
    ).map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![id.as_str(), page_i64(limit)?, page_i64(offset)?],
            raw_case_row,
        )
        .map_err(sql_error)?;
    rows.map(|row| decode_case(row.map_err(sql_error)?))
        .collect()
}

fn load_folds(
    connection: &rusqlite::Connection,
    id: &SuiteId,
) -> Result<Vec<WalkForwardFold>, LabError> {
    let mut statement = connection
        .prepare("SELECT fold_json FROM research_suite_folds WHERE suite_id=?1 ORDER BY fold_index")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([id.as_str()], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    rows.map(|row| serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error))
        .collect()
}

fn load_fold_tx(
    transaction: &Transaction<'_>,
    id: &SuiteId,
    index: u32,
) -> Result<WalkForwardFold, LabError> {
    let json = transaction
        .query_row(
            "SELECT fold_json FROM research_suite_folds WHERE suite_id=?1 AND fold_index=?2",
            params![id.as_str(), index],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown walk-forward fold".into()))?;
    serde_json::from_str(&json).map_err(json_error)
}

fn validate_selection_lineage(
    transaction: &Transaction<'_>,
    suite_id: &SuiteId,
    fold_index: u32,
    expected_case_runs: &[(SuiteCaseId, RunId)],
) -> Result<(), LabError> {
    let expected_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM research_suite_cases WHERE suite_id=?1 AND fold_index=?2 AND phase='selection'",
        params![suite_id.as_str(), fold_index],
        |row| row.get(0),
    ).map_err(sql_error)?;
    if nonnegative_u64(expected_count, "selection case count")?
        != u64::try_from(expected_case_runs.len())
            .map_err(|_| LabError::ResourceLimit("selection case count overflow".into()))?
        || expected_case_runs
            .iter()
            .map(|(case, _)| case)
            .collect::<BTreeSet<_>>()
            .len()
            != expected_case_runs.len()
        || expected_case_runs
            .iter()
            .map(|(_, run)| run)
            .collect::<BTreeSet<_>>()
            .len()
            != expected_case_runs.len()
    {
        return Err(LabError::Conflict(
            "selection commit does not name every case and run exactly once".into(),
        ));
    }
    for (case_id, run_id) in expected_case_runs {
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM research_suite_cases WHERE id=?1 AND suite_id=?2 AND fold_index=?3 AND phase='selection' AND status IN ('completed','blocked') AND run_id=?4)",
            params![case_id.as_str(), suite_id.as_str(), fold_index, run_id.as_str()],
            |row| row.get(0),
        ).map_err(sql_error)?;
        if !exists {
            return Err(LabError::Conflict(
                "selection commit references incomplete or unpinned case".into(),
            ));
        }
    }
    Ok(())
}

fn validate_selection_outcome(
    transaction: &Transaction<'_>,
    suite_id: &SuiteId,
    outcome: &SelectionOutcome,
) -> Result<Vec<crate::contracts::UnavailableCandidate>, LabError> {
    let unavailable = outcome
        .unavailable
        .iter()
        .map(|candidate| crate::contracts::UnavailableCandidate {
            candidate: candidate.candidate.clone(),
            reason: candidate.reason.clone(),
        })
        .collect::<Vec<_>>();
    let frozen_json: String = transaction
        .query_row(
            "SELECT frozen_json FROM research_suites WHERE id=?1",
            [suite_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let frozen: FrozenResearchSuite = serde_json::from_str(&frozen_json).map_err(json_error)?;
    let candidates = frozen
        .request
        .template
        .policy_selections
        .iter()
        .collect::<BTreeSet<_>>();
    if outcome.winner.as_ref().is_some_and(|winner| {
        !candidates.contains(winner)
            || unavailable
                .iter()
                .any(|candidate| &candidate.candidate == winner)
    }) || unavailable
        .iter()
        .any(|candidate| !candidates.contains(&candidate.candidate))
        || unavailable
            .iter()
            .map(|candidate| &candidate.candidate)
            .collect::<BTreeSet<_>>()
            .len()
            != unavailable.len()
    {
        return Err(LabError::InputHashMismatch(
            "fold selection references candidates outside the frozen suite".into(),
        ));
    }
    Ok(unavailable)
}

fn validate_case_submission(
    transaction: &Transaction<'_>,
    case: &SuiteCase,
    submission: &JobSubmission,
) -> Result<(), LabError> {
    let plan_id = case
        .plan_id
        .as_ref()
        .ok_or_else(|| LabError::Conflict("suite case plan is not frozen".into()))?;
    if case.causal_input_digest.is_none() {
        return Err(LabError::Conflict(
            "suite case causal input is not frozen".into(),
        ));
    }
    let plan_input_digest: String = transaction
        .query_row(
            "SELECT input_digest FROM plans WHERE id=?1",
            [plan_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("suite case plan is missing".into()))?;
    match &submission.payload {
        JobPayload::Backtest { request }
            if &request.plan_id == plan_id
                && request.input_digest.as_str() == plan_input_digest => {}
        _ => {
            return Err(LabError::Conflict(
                "suite child must backtest the pinned plan input".into(),
            ));
        }
    }
    if case.phase == SuitePhase::Evaluation {
        let index = case
            .fold_index
            .ok_or_else(|| LabError::DataCorrupt("evaluation case lacks fold".into()))?;
        let fold = load_fold_tx(transaction, &case.suite_id, index)?;
        if fold.winner.is_none() || fold.selection_digest.is_none() {
            return Err(LabError::Conflict(
                "evaluation admission requires a committed winner".into(),
            ));
        }
    }
    Ok(())
}

fn require_suite(connection: &rusqlite::Connection, id: &SuiteId) -> Result<(), LabError> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM research_suites WHERE id=?1)",
            [id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if exists {
        Ok(())
    } else {
        Err(LabError::InvalidConfig("unknown research suite".into()))
    }
}

fn require_running_suite(transaction: &Transaction<'_>, id: &SuiteId) -> Result<(), LabError> {
    let status: String = transaction
        .query_row(
            "SELECT status FROM research_suites WHERE id=?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?;
    if status == enum_text(&SuiteStatus::Running)? {
        Ok(())
    } else {
        Err(LabError::Conflict("research suite is not running".into()))
    }
}

fn case_projection(
    state: &AttemptState,
    run_id: Option<&str>,
) -> Result<(SuiteCaseStatus, Option<FailureRecord>), LabError> {
    match state {
        AttemptState::Queued { .. } => Ok((SuiteCaseStatus::Queued, None)),
        AttemptState::Running { .. } => Ok((SuiteCaseStatus::Running, None)),
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            if !matches!(output, JobOutput::Run { .. }) || run_id.is_none() {
                return Err(LabError::DataCorrupt(
                    "suite child terminal output is not a published run".into(),
                ));
            }
            Ok((SuiteCaseStatus::Completed, None))
        }
        AttemptState::Blocked { reason, output, .. } => {
            if output.is_some() && !matches!(output, Some(JobOutput::Run { .. })) {
                return Err(LabError::DataCorrupt(
                    "blocked suite child output is not a run".into(),
                ));
            }
            if output.is_some() != run_id.is_some() {
                return Err(LabError::DataCorrupt(
                    "blocked suite child output/run link mismatch".into(),
                ));
            }
            Ok((SuiteCaseStatus::Blocked, Some(reason.clone())))
        }
        AttemptState::Failed { error, .. } => Ok((SuiteCaseStatus::Failed, Some(error.clone()))),
        AttemptState::Cancelled { reason, .. } => Ok((
            SuiteCaseStatus::Cancelled,
            Some(FailureRecord {
                code: "CANCELLED".into(),
                message: reason.clone(),
            }),
        )),
        AttemptState::Interrupted { reason, .. } => Ok((
            SuiteCaseStatus::Interrupted,
            Some(FailureRecord {
                code: "INTERRUPTED".into(),
                message: reason.clone(),
            }),
        )),
    }
}

fn validate_case_comparison_digest(
    transaction: &Transaction<'_>,
    case_id: &str,
    run_id: &str,
) -> Result<(), LabError> {
    let expected: String = transaction
        .query_row(
            "SELECT causal_input_digest FROM research_suite_cases WHERE id=?1",
            [case_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let mut statement = transaction
        .prepare(
            "SELECT comparison_json FROM run_model_comparisons WHERE run_id=?1 ORDER BY model_id",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([run_id], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    let mut count = 0_u64;
    for row in rows {
        let comparison: RunModelComparison =
            serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?;
        if comparison.causal_input_digest.as_str() != expected {
            return Err(LabError::InputHashMismatch(
                "suite case causal digest differs from published comparison".into(),
            ));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("comparison count overflow".into()))?;
    }
    if count == 0 {
        return Err(LabError::DataCorrupt(
            "completed suite run has no comparison projection".into(),
        ));
    }
    Ok(())
}

fn enum_from_text<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, LabError> {
    serde_json::from_value(serde_json::Value::String(text.into())).map_err(json_error)
}

fn validate_page(limit: u32) -> Result<(), LabError> {
    if limit == 0 || limit > MAX_RESEARCH_PAGE {
        return Err(LabError::ResourceLimit(format!(
            "research page limit must be 1..={MAX_RESEARCH_PAGE}"
        )));
    }
    Ok(())
}

fn count_rows(connection: &rusqlite::Connection, table: &str) -> Result<u64, LabError> {
    let sql = match table {
        "research_suites" => "SELECT COUNT(*) FROM research_suites",
        _ => return Err(LabError::Internal("unsupported count table".into())),
    };
    nonnegative_u64(
        connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(sql_error)?,
        "row count",
    )
}

fn count_for_suite(
    connection: &rusqlite::Connection,
    table: &str,
    id: &SuiteId,
) -> Result<u64, LabError> {
    let sql = match table {
        "research_suite_cases" => "SELECT COUNT(*) FROM research_suite_cases WHERE suite_id=?1",
        _ => return Err(LabError::Internal("unsupported suite count table".into())),
    };
    nonnegative_u64(
        connection
            .query_row(sql, [id.as_str()], |row| row.get(0))
            .map_err(sql_error)?,
        "suite row count",
    )
}

fn page<T>(records: Vec<T>, offset: u64, total_count: u64) -> ResearchPage<T> {
    let returned = u64::try_from(records.len()).unwrap_or(u64::MAX);
    let next = offset.saturating_add(returned);
    ResearchPage {
        records,
        total_count,
        next_offset: (next < total_count).then_some(next),
    }
}

fn page_i64(value: u64) -> Result<i64, LabError> {
    i64::try_from(value)
        .map_err(|_| LabError::ResourceLimit("page offset exceeds SQLite range".into()))
}

fn nonnegative_u64(value: i64, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt(format!("negative stored {label}")))
}
