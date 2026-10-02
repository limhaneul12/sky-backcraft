use super::ledger_seal::{
    RunLedgerSource, records_of_kind, run_ledger_source, sequenced_from_lines,
};
use super::{
    Store, StoredPlan, canonical_decimal, enum_text, io_error, json_error, reject_symlink_chain,
    sql_error, timestamp_ms, u64_to_i64, usize_to_i64, validate_relative_path,
};
use crate::contracts::{
    AccountMark, ArtifactId, ArtifactRef, AttemptState, EpisodeRecord, FillRecord, JobPayload,
    JobStatus, LabError, LedgerSection, ModelId, ModelLedger, ModelStatus, OrderRecord, Page,
    PlanId, QueryCursor, ResultQuery, RunBundle, RunHeader, RunId, SignalRecord, UtcTimestamp,
    ValidationReport, ValidationStatus, strategy_binding,
};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

const MAX_FACTS_PER_BATCH: usize = 256;
const MAX_RESULT_LIMIT: u32 = 100;

#[derive(Debug, Clone, Default)]
pub struct ModelFactBatch {
    pub signals: Vec<SignalRecord>,
    pub order_events: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub account_marks: Vec<AccountMark>,
}

#[derive(Debug, Clone)]
pub enum ResultPage {
    Signals(Page<SignalRecord>),
    Orders(Page<OrderRecord>),
    OrderEvents(Page<OrderRecord>),
    Fills(Page<FillRecord>),
    Episodes(Page<EpisodeRecord>),
    Equity(Page<AccountMark>),
}

pub(super) struct PreparedRunPublication {
    pub final_state: JobStatus,
    pub validation: ValidationReport,
    pub completed_models: u64,
    pub blocked_models: u64,
}

impl Store {
    /// Begin a durable run only for the currently running backtest attempt and frozen plan.
    ///
    /// # Errors
    /// Rejects stale/mismatched identities, illegal state, or SQLite failure.
    pub fn begin_run(&mut self, header: &RunHeader) -> Result<RunId, LabError> {
        if header.state != JobStatus::Running
            || header.last_committed_event_seq != 0
            || header.semantic_digest.is_some()
        {
            return Err(LabError::InvalidConfig(
                "new run header must be RUNNING with no committed facts/digest".into(),
            ));
        }
        let plan = self.load_plan(&header.plan_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown run plan {}", header.plan_id))
        })?;
        let attempt = self.load_attempt_for_run(&header.manifest.attempt_id)?;
        if attempt.job_id != header.manifest.job_id
            || !matches!(
                attempt.state,
                AttemptState::Running {
                    cancel_requested_at: None,
                    ..
                }
            )
        {
            return Err(LabError::Conflict(
                "run attempt is not the active uncancelled RUNNING attempt".into(),
            ));
        }
        let job = self
            .get_job(&attempt.job_id)?
            .ok_or_else(|| LabError::DataCorrupt("run job missing".into()))?;
        match &job.payload {
            JobPayload::Backtest { request }
                if request.plan_id == header.plan_id
                    && request.input_digest == plan.resolved.input_digest => {}
            _ => {
                return Err(LabError::Conflict(
                    "run job payload does not match frozen plan".into(),
                ));
            }
        }
        if header.manifest.run_id.as_str().is_empty() {
            return Err(LabError::InvalidConfig("run id is required".into()));
        }
        let header_json = serde_json::to_string(&header.manifest).map_err(json_error)?;
        if let Some(existing) = self
            .connection
            .query_row(
                "SELECT manifest_json,plan_id FROM runs WHERE id=?1",
                [header.manifest.run_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
        {
            return if existing == (header_json, header.plan_id.to_string()) {
                Ok(header.manifest.run_id.clone())
            } else {
                Err(LabError::Conflict(
                    "run id already has different content".into(),
                ))
            };
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json) VALUES (?1,?2,?3,?4,?5,?6,0,?7)",
            params![header.manifest.run_id.as_str(), header.manifest.job_id.as_str(), header.manifest.attempt_id.as_str(), header.plan_id.as_str(), enum_text(&JobStatus::Running)?, timestamp_ms(header.manifest.created_at), header_json],
        ).map_err(sql_error)?;
        for admission in &plan.resolved.admissions {
            let strategy = strategy_binding(&plan.resolved, admission)?;
            let policy = strategy.policy_ref();
            transaction.execute(
                "INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,policy_id,policy_revision_id,policy_definition_digest) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![header.manifest.run_id.as_str(), admission.model_id.as_str(), admission.market.code(), enum_text(&admission.strategy)?, serde_json::to_string(&strategy).map_err(json_error)?, policy.map(|reference| reference.policy_id.as_str()), policy.map(|reference| reference.revision_id.as_str()), policy.map(|reference| reference.definition_digest.as_str())],
            ).map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(header.manifest.run_id.clone())
    }

    /// Append one bounded, causally contiguous batch of sequenced model facts.
    ///
    /// # Errors
    /// Rejects wrong run/model/time/sequence, unpaired fills and marks, or SQLite failure.
    pub fn append_model_facts(
        &mut self,
        run_id: &RunId,
        model_id: &ModelId,
        batch: &ModelFactBatch,
    ) -> Result<u64, LabError> {
        let events = batch_events(batch)?;
        if events.is_empty() || events.len() > MAX_FACTS_PER_BATCH {
            return Err(LabError::ResourceLimit(format!(
                "fact batch requires 1..={MAX_FACTS_PER_BATCH} events"
            )));
        }
        let run = self
            .load_run_header(run_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        if run.state != JobStatus::Running {
            return Err(LabError::Conflict(
                "facts can only append to RUNNING run".into(),
            ));
        }
        self.ensure_attempt_uncancelled(&run.manifest.attempt_id)?;
        let plan = self
            .load_plan(&run.plan_id)?
            .ok_or_else(|| LabError::DataCorrupt("run plan missing".into()))?;
        validate_batch(run_id, model_id, batch, &events, &plan)?;
        let expected = run
            .last_committed_event_seq
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("run event sequence overflow".into()))?;
        if events[0].seq != expected || events.windows(2).any(|pair| pair[1].seq != pair[0].seq + 1)
        {
            return Err(LabError::Conflict(
                "fact batch is not globally contiguous".into(),
            ));
        }
        let last = events
            .last()
            .ok_or_else(|| LabError::Internal("validated batch empty".into()))?
            .seq;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for event in &events {
            transaction.execute(
                "INSERT INTO run_events(run_id,event_seq,model_id,event_kind,accounting_event_time_ms,record_id) VALUES (?1,?2,?3,?4,?5,?6)",
                params![run_id.as_str(), u64_to_i64(event.seq)?, model_id.as_str(), event.kind, timestamp_ms(event.at), event.record_id],
            ).map_err(sql_error)?;
        }
        insert_signals(&transaction, run_id, model_id, &batch.signals)?;
        insert_order_events(&transaction, run_id, model_id, &batch.order_events)?;
        insert_fills(&transaction, run_id, model_id, &batch.fills)?;
        insert_marks(&transaction, run_id, model_id, &batch.account_marks)?;
        let model_changed = transaction.execute(
            "UPDATE run_models SET last_committed_event_seq=?1,signal_count=signal_count+?2,order_event_count=order_event_count+?3,fill_count=fill_count+?4,account_mark_count=account_mark_count+?5 WHERE run_id=?6 AND model_id=?7 AND status IS NULL",
            params![u64_to_i64(last)?, usize_to_i64(batch.signals.len())?, usize_to_i64(batch.order_events.len())?, usize_to_i64(batch.fills.len())?, usize_to_i64(batch.account_marks.len())?, run_id.as_str(), model_id.as_str()],
        ).map_err(sql_error)?;
        if model_changed != 1 {
            return Err(LabError::Conflict(
                "run model is unknown or already terminal".into(),
            ));
        }
        let run_changed = transaction.execute(
            "UPDATE runs SET last_committed_event_seq=?1 WHERE id=?2 AND state=?3 AND last_committed_event_seq=?4",
            params![u64_to_i64(last)?, run_id.as_str(), enum_text(&JobStatus::Running)?, u64_to_i64(run.last_committed_event_seq)?],
        ).map_err(sql_error)?;
        if run_changed != 1 {
            return Err(LabError::Conflict("run event append CAS failed".into()));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(last)
    }

    /// Finish one model after proving its final projections match persisted append-only facts.
    ///
    /// # Errors
    /// Rejects mismatched facts/projections/links/state or SQLite failure.
    pub fn finish_model(&mut self, run_id: &RunId, ledger: &ModelLedger) -> Result<(), LabError> {
        let run = self
            .load_run_header(run_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        if run.state != JobStatus::Running {
            return Err(LabError::Conflict("run is not RUNNING".into()));
        }
        self.ensure_attempt_uncancelled(&run.manifest.attempt_id)?;
        let source = run_ledger_source(self, run_id)?;
        let stored = self.load_sequenced_model(run_id, &ledger.model_id, &source)?;
        let empty_facts = ledger.signals.is_empty()
            && ledger.order_events.is_empty()
            && ledger.fills.is_empty()
            && ledger.account_marks.is_empty();
        let expected_last_seq = if empty_facts && ledger.status != ModelStatus::Completed {
            run.last_committed_event_seq
        } else {
            stored.last_seq
        };
        let terminal_sections = [
            (
                "signals",
                &stored.signals,
                serde_json::to_value(&ledger.signals).map_err(json_error)?,
            ),
            (
                "order_events",
                &stored.order_events,
                serde_json::to_value(&ledger.order_events).map_err(json_error)?,
            ),
            (
                "fills",
                &stored.fills,
                serde_json::to_value(&ledger.fills).map_err(json_error)?,
            ),
            (
                "account_marks",
                &stored.marks,
                serde_json::to_value(&ledger.account_marks).map_err(json_error)?,
            ),
        ];
        for (section, stored_section, terminal_section) in terminal_sections {
            if stored_section != &terminal_section {
                return Err(LabError::InputHashMismatch(format!(
                    "model terminal {section} do not match committed sequenced facts"
                )));
            }
        }
        if expected_last_seq != ledger.last_event_seq {
            return Err(LabError::InputHashMismatch(
                "model terminal last_event_seq does not match committed sequence".into(),
            ));
        }
        validate_final_projections(ledger)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for order in &ledger.orders {
            transaction.execute(
                "INSERT INTO orders(run_id,model_id,order_id,final_status,event_seq,accounting_event_time_ms,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![run_id.as_str(), ledger.model_id.as_str(), order.order_id.as_str(), enum_text(&order.status)?, u64_to_i64(order.context.event_seq)?, timestamp_ms(order.context.accounting_event_time), serde_json::to_string(order).map_err(json_error)?],
            ).map_err(sql_error)?;
        }
        insert_episodes(&transaction, run_id, ledger)?;
        let fact_digest = crate::contracts::ContentHash::of_value(&(
            &ledger.signals,
            &ledger.order_events,
            &ledger.fills,
            &ledger.account_marks,
        ))?;
        let final_digest = crate::contracts::ContentHash::of_value(ledger)?;
        let changed = transaction.execute(
            "UPDATE run_models SET status=?1,status_reason=?2,episode_count=?3,fact_digest=?4,final_digest=?5,last_committed_event_seq=?6 WHERE run_id=?7 AND model_id=?8 AND status IS NULL AND market=?9 AND strategy_json=?10",
            params![enum_text(&ledger.status)?, ledger.status_reason, usize_to_i64(ledger.episodes.len())?, fact_digest.as_str(), final_digest.as_str(), u64_to_i64(expected_last_seq)?, run_id.as_str(), ledger.model_id.as_str(), ledger.market.code(), serde_json::to_string(&ledger.strategy).map_err(json_error)?],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "model terminal CAS or identity check failed".into(),
            ));
        }
        transaction.commit().map_err(sql_error)
    }

    pub(super) fn prepare_run_publication(
        &self,
        run_id: &RunId,
        semantic_digest: &crate::contracts::ContentHash,
        expected_models: u64,
    ) -> Result<PreparedRunPublication, LabError> {
        let header = self
            .load_run_header(run_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        if header.state != JobStatus::Running {
            return Err(LabError::Conflict("run is not RUNNING".into()));
        }
        self.ensure_attempt_uncancelled(&header.manifest.attempt_id)?;
        let plan = self
            .load_plan(&header.plan_id)?
            .ok_or_else(|| LabError::DataCorrupt("run plan missing".into()))?;
        let expected: BTreeSet<_> = plan
            .resolved
            .admissions
            .iter()
            .map(|admission| admission.model_id.to_string())
            .collect();
        let actual = self.finished_model_ids(run_id)?;
        let expected_count = u64::try_from(expected.len())
            .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?;
        if expected_count != expected_models || actual != expected {
            return Err(LabError::Conflict(
                "finished model identities differ from frozen admissions".into(),
            ));
        }
        let mut bundle = self.load_run_bundle_internal(run_id, Some(semantic_digest.clone()))?;
        let computed = crate::reporting::semantic_digest(&bundle)?;
        if computed != *semantic_digest {
            return Err(LabError::InputHashMismatch(
                "run semantic digest mismatch".into(),
            ));
        }
        bundle.semantic_digest = computed;
        let verification = crate::reporting::verify_run(&bundle);
        if verification.status != ValidationStatus::Pass {
            return Err(LabError::AccountingInvariant(format!(
                "run verification failed: {}",
                verification.findings.join("; ")
            )));
        }
        let completed_models = u64::try_from(
            bundle
                .models
                .iter()
                .filter(|model| model.status == ModelStatus::Completed)
                .count(),
        )
        .map_err(|_| LabError::ResourceLimit("completed model count overflow".into()))?;
        let total_models = u64::try_from(bundle.models.len())
            .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?;
        Ok(PreparedRunPublication {
            final_state: derive_run_state(&bundle.models),
            validation: verification,
            completed_models,
            blocked_models: total_models.saturating_sub(completed_models),
        })
    }

    /// Load a complete immutable run bundle with all frozen inputs.
    ///
    /// # Errors
    /// Returns an error for incomplete/corrupt references or SQLite failure.
    pub fn load_run_bundle(&self, run_id: &RunId) -> Result<Option<RunBundle>, LabError> {
        let header = self.load_run_header(run_id)?;
        let Some(header) = header else {
            return Ok(None);
        };
        let Some(digest) = header.semantic_digest else {
            return Err(LabError::Conflict("run is not finalized".into()));
        };
        self.load_run_bundle_internal(run_id, Some(digest))
            .map(Some)
    }

    /// Load one persisted model ledger.
    ///
    /// # Errors
    /// Returns an error for incomplete/corrupt state or SQLite failure.
    pub fn load_model(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
    ) -> Result<Option<ModelLedger>, LabError> {
        let Some(row) = query_terminal_model_row(self, run_id, model_id)? else {
            return Ok(None);
        };
        let source = run_ledger_source(self, run_id)?;
        load_model_from_row(self, run_id, model_id, row, &source).map(Some)
    }

    /// Load durable run header/status details.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state or SQLite failure.
    pub fn load_run_header(&self, run_id: &RunId) -> Result<Option<RunHeader>, LabError> {
        self.connection.query_row(
            "SELECT manifest_json,plan_id,state,last_committed_event_seq,semantic_digest FROM runs WHERE id=?1",
            [run_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?,row.get::<_, i64>(3)?,row.get::<_, Option<String>>(4)?)),
        ).optional().map_err(sql_error)?.map(|row| Ok(RunHeader {
            manifest: serde_json::from_str(&row.0).map_err(json_error)?,
            plan_id: PlanId::new(row.1)?,
            state: serde_json::from_value(serde_json::Value::String(row.2)).map_err(json_error)?,
            last_committed_event_seq: nonnegative_u64(row.3, "run last event sequence")?,
            semantic_digest: row.4.map(crate::contracts::ContentHash::try_from).transpose()?,
        })).transpose()
    }

    pub(super) fn validate_artifact_publication(
        &self,
        artifacts: &[ArtifactRef],
    ) -> Result<(), LabError> {
        let mut ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for artifact in artifacts {
            if !ids.insert(artifact.id.as_str()) || !paths.insert(artifact.relative_path.as_str()) {
                return Err(LabError::Conflict(
                    "artifact publication contains duplicate id or path".into(),
                ));
            }
            self.validate_artifact_file(artifact)?;
        }
        Ok(())
    }

    fn validate_artifact_file(&self, artifact: &ArtifactRef) -> Result<(), LabError> {
        validate_relative_path(&artifact.relative_path)?;
        if !artifact.relative_path.starts_with("exports/") {
            return Err(LabError::InvalidConfig(
                "artifact must be below exports/".into(),
            ));
        }
        let path = self.raw_objects().root.join(&artifact.relative_path);
        reject_symlink_chain(&self.raw_objects().root, &path)?;
        let bytes = fs::read(&path).map_err(io_error("read artifact"))?;
        let actual_bytes = u64::try_from(bytes.len()).map_err(|_| {
            LabError::ResourceLimit("artifact size exceeds platform capacity".into())
        })?;
        if actual_bytes != artifact.bytes
            || crate::contracts::ContentHash::of_bytes(&bytes) != artifact.sha256
        {
            return Err(LabError::InputHashMismatch(
                "artifact length/hash mismatch".into(),
            ));
        }
        Ok(())
    }

    /// List immutable artifacts for one run.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state or SQLite failure.
    pub fn list_artifacts(&self, run_id: &RunId) -> Result<Vec<ArtifactRef>, LabError> {
        let mut statement = self
            .connection
            .prepare("SELECT artifact_json FROM run_artifacts WHERE run_id=?1 ORDER BY id")
            .map_err(sql_error)?;
        statement
            .query_map([run_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .map(|row| serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error))
            .collect()
    }

    /// Load one immutable artifact catalog entry.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state or SQLite failure.
    pub fn load_artifact(&self, id: &ArtifactId) -> Result<Option<ArtifactRef>, LabError> {
        self.connection
            .query_row(
                "SELECT artifact_json FROM run_artifacts WHERE id=?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
            .map(|json| serde_json::from_str(&json).map_err(json_error))
            .transpose()
    }

    /// Resolve a catalogued artifact only after root, symlink, size, and hash checks.
    ///
    /// # Errors
    /// Rejects unknown, unsafe, or corrupt artifacts and filesystem failures.
    pub fn resolve_artifact_path(&self, id: &ArtifactId) -> Result<PathBuf, LabError> {
        let artifact = self
            .load_artifact(id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown artifact".into()))?;
        validate_relative_path(&artifact.relative_path)?;
        if !artifact.relative_path.starts_with("exports/") {
            return Err(LabError::DataCorrupt(
                "catalogued artifact escaped exports/".into(),
            ));
        }
        let path = self.raw_objects().root.join(&artifact.relative_path);
        reject_symlink_chain(&self.raw_objects().root, &path)?;
        let bytes = fs::read(&path).map_err(io_error("read resolved artifact"))?;
        let actual_bytes = u64::try_from(bytes.len()).map_err(|_| {
            LabError::ResourceLimit("artifact size exceeds platform capacity".into())
        })?;
        if actual_bytes != artifact.bytes
            || crate::contracts::ContentHash::of_bytes(&bytes) != artifact.sha256
        {
            return Err(LabError::DataCorrupt(
                "catalogued artifact length/hash mismatch".into(),
            ));
        }
        Ok(path)
    }

    /// Query one indexed result section with a typed bounded page.
    ///
    /// # Errors
    /// Rejects invalid cursors/limits/sections or corrupt SQLite state.
    pub fn query_result(&self, query: &ResultQuery) -> Result<ResultPage, LabError> {
        if query.limit == 0 || query.limit > MAX_RESULT_LIMIT {
            return Err(LabError::ResourceLimit(format!(
                "result limit must be 1..={MAX_RESULT_LIMIT}"
            )));
        }
        let model = resolve_query_model(self, query)?;
        let offset = query.cursor.as_ref().map_or(0, |cursor| cursor.offset);
        if query
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.model_id != model)
        {
            return Err(LabError::InvalidConfig(
                "result cursor model mismatch".into(),
            ));
        }
        match query.section {
            LedgerSection::Signals => {
                query_json_page(self, query, &model, offset, QueryTable::Signals)
                    .map(ResultPage::Signals)
            }
            LedgerSection::Orders => {
                query_json_page(self, query, &model, offset, QueryTable::Orders)
                    .map(ResultPage::Orders)
            }
            LedgerSection::OrderEvents => {
                query_json_page(self, query, &model, offset, QueryTable::OrderEvents)
                    .map(ResultPage::OrderEvents)
            }
            LedgerSection::Fills => query_json_page(self, query, &model, offset, QueryTable::Fills)
                .map(ResultPage::Fills),
            LedgerSection::Episodes => {
                query_json_page(self, query, &model, offset, QueryTable::Episodes)
                    .map(ResultPage::Episodes)
            }
            LedgerSection::Equity => {
                query_json_page(self, query, &model, offset, QueryTable::Marks)
                    .map(ResultPage::Equity)
            }
        }
    }
}

pub(super) fn insert_artifact_row(
    transaction: &Transaction<'_>,
    artifact: &ArtifactRef,
) -> Result<(), LabError> {
    let json = serde_json::to_string(artifact).map_err(json_error)?;
    if let Some(existing) = transaction
        .query_row(
            "SELECT artifact_json FROM run_artifacts WHERE id=?1",
            [artifact.id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?
    {
        return if existing == json {
            Ok(())
        } else {
            Err(LabError::Conflict(
                "artifact id already has different content".into(),
            ))
        };
    }
    transaction.execute(
        "INSERT INTO run_artifacts(id,run_id,relative_path,media_type,bytes,sha256,uncompressed_sha256,uncompressed_bytes,complete,artifact_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![artifact.id.as_str(), artifact.run_id.as_str(), artifact.relative_path, artifact.media_type, u64_to_i64(artifact.bytes)?, artifact.sha256.as_str(), artifact.uncompressed_sha256.as_ref().map(crate::contracts::ContentHash::as_str), artifact.uncompressed_bytes.map(u64_to_i64).transpose()?, artifact.complete, json],
    ).map_err(sql_error)?;
    Ok(())
}

pub(super) fn insert_validation_row(
    transaction: &Transaction<'_>,
    report: &ValidationReport,
) -> Result<(), LabError> {
    let json = serde_json::to_string(report).map_err(json_error)?;
    let identity_key = format!(
        "{}:{}",
        report.check_id,
        report.run_id.as_ref().map_or("none", RunId::as_str)
    );
    transaction.execute(
        "INSERT INTO validation_results(identity_key,check_id,run_id,status,input_digest,report_json) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(identity_key) DO UPDATE SET status=excluded.status,input_digest=excluded.input_digest,report_json=excluded.report_json",
        params![identity_key, report.check_id, report.run_id.as_ref().map(RunId::as_str), enum_text(&report.status)?, report.input_digest.as_str(), json],
    ).map_err(sql_error)?;
    Ok(())
}

#[derive(Debug)]
struct BatchEvent {
    seq: u64,
    at: UtcTimestamp,
    kind: &'static str,
    record_id: String,
}

fn batch_events(batch: &ModelFactBatch) -> Result<Vec<BatchEvent>, LabError> {
    let mut events = Vec::new();
    events.extend(batch.signals.iter().map(|record| BatchEvent {
        seq: record.context.event_seq,
        at: record.context.accounting_event_time,
        kind: "SIGNAL",
        record_id: record.signal_id.to_string(),
    }));
    events.extend(batch.order_events.iter().map(|record| BatchEvent {
        seq: record.context.event_seq,
        at: record.context.accounting_event_time,
        kind: "ORDER_EVENT",
        record_id: record.order_id.to_string(),
    }));
    events.extend(batch.fills.iter().map(|record| BatchEvent {
        seq: record.context.event_seq,
        at: record.context.accounting_event_time,
        kind: "FILL",
        record_id: record.fill_id.to_string(),
    }));
    events.extend(batch.account_marks.iter().map(|record| BatchEvent {
        seq: record.context.event_seq,
        at: record.context.accounting_event_time,
        kind: "ACCOUNT_MARK",
        record_id: format!("mark-{}", record.context.event_seq),
    }));
    events.sort_by_key(|event| event.seq);
    if events.windows(2).any(|pair| pair[0].seq == pair[1].seq) {
        return Err(LabError::Conflict(
            "duplicate event sequence in fact batch".into(),
        ));
    }
    Ok(events)
}

fn validate_batch(
    run_id: &RunId,
    model_id: &ModelId,
    batch: &ModelFactBatch,
    events: &[BatchEvent],
    plan: &StoredPlan,
) -> Result<(), LabError> {
    let start = plan.resolved.spec.range.start();
    let end = plan.resolved.spec.range.end();
    if events
        .iter()
        .any(|event| event.at < start || event.at > end)
    {
        return Err(LabError::Conflict(
            "fact time is outside frozen plan range".into(),
        ));
    }
    let contexts = batch
        .signals
        .iter()
        .map(|v| &v.context)
        .chain(batch.order_events.iter().map(|v| &v.context))
        .chain(batch.fills.iter().map(|v| &v.context))
        .chain(batch.account_marks.iter().map(|v| &v.context));
    let admission = plan
        .resolved
        .admissions
        .iter()
        .find(|admission| &admission.model_id == model_id)
        .ok_or_else(|| LabError::Conflict("fact model is absent from frozen admissions".into()))?;
    if contexts.into_iter().any(|context| {
        &context.run_id != run_id
            || &context.model_id != model_id
            || context.market != admission.market
    }) {
        return Err(LabError::Conflict("fact context run/model mismatch".into()));
    }
    let marks: BTreeMap<_, _> = batch
        .account_marks
        .iter()
        .map(|mark| (mark.context.event_seq, mark))
        .collect();
    let mut used_marks = BTreeSet::new();
    for fill in &batch.fills {
        if fill.accounting_mark_seq <= fill.context.event_seq
            || !used_marks.insert(fill.accounting_mark_seq)
        {
            return Err(LabError::AccountingInvariant(
                "each fill requires a distinct later after-fill account mark".into(),
            ));
        }
        let mark = marks.get(&fill.accounting_mark_seq).ok_or_else(|| {
            LabError::AccountingInvariant(
                "fill and exact after-fill account mark must share a fact batch".into(),
            )
        })?;
        if mark.kind != crate::contracts::MarkKind::AfterFill {
            return Err(LabError::AccountingInvariant(
                "fill accounting mark is not AFTER_FILL".into(),
            ));
        }
        if fill.liquidity_source_close_time > fill.context.accounting_event_time {
            return Err(LabError::Conflict(
                "fill liquidity source closes after accounting event time".into(),
            ));
        }
    }
    Ok(())
}

fn insert_signals(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    model_id: &ModelId,
    records: &[SignalRecord],
) -> Result<(), LabError> {
    for record in records {
        transaction.execute(
            "INSERT INTO signals(signal_id,run_id,model_id,event_seq,market,signal_time_ms,decision_available_at_ms,valid_until_ms,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![record.signal_id.as_str(), run_id.as_str(), model_id.as_str(), u64_to_i64(record.context.event_seq)?, record.context.market.code(), timestamp_ms(record.signal_time), timestamp_ms(record.decision_available_at), timestamp_ms(record.valid_until), serde_json::to_string(record).map_err(json_error)?],
        ).map_err(sql_error)?;
        for (position, source) in record.source_bar_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO signal_source_bars(run_id,model_id,signal_id,observation_id,position) VALUES (?1,?2,?3,?4,?5)",
                params![run_id.as_str(), model_id.as_str(), record.signal_id.as_str(), source.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
    }
    Ok(())
}

fn insert_order_events(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    model_id: &ModelId,
    records: &[OrderRecord],
) -> Result<(), LabError> {
    for record in records {
        transaction.execute(
            "INSERT OR IGNORE INTO order_identities(run_id,model_id,order_id,parent_signal_id) VALUES (?1,?2,?3,?4)",
            params![run_id.as_str(), model_id.as_str(), record.order_id.as_str(), record.parent_signal_id.as_str()],
        ).map_err(sql_error)?;
        let identity: (String, String) = transaction.query_row(
            "SELECT model_id,parent_signal_id FROM order_identities WHERE run_id=?1 AND model_id=?2 AND order_id=?3",
            params![run_id.as_str(), model_id.as_str(), record.order_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(sql_error)?;
        if identity != (model_id.to_string(), record.parent_signal_id.to_string()) {
            return Err(LabError::Conflict(
                "order identity changed across lifecycle events".into(),
            ));
        }
        transaction.execute(
            "INSERT INTO order_events(run_id,model_id,event_seq,order_id,status,effective_at_ms,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![run_id.as_str(), model_id.as_str(), u64_to_i64(record.context.event_seq)?, record.order_id.as_str(), enum_text(&record.status)?, timestamp_ms(record.effective_at), serde_json::to_string(record).map_err(json_error)?],
        ).map_err(sql_error)?;
    }
    Ok(())
}

fn insert_fills(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    model_id: &ModelId,
    records: &[FillRecord],
) -> Result<(), LabError> {
    for record in records {
        transaction.execute(
            "INSERT OR IGNORE INTO episode_identities(run_id,model_id,episode_id) VALUES (?1,?2,?3)",
            params![run_id.as_str(), model_id.as_str(), record.episode_id.as_str()],
        ).map_err(sql_error)?;
        let episode_model: String = transaction
            .query_row(
                "SELECT model_id FROM episode_identities WHERE run_id=?1 AND model_id=?2 AND episode_id=?3",
                params![run_id.as_str(), model_id.as_str(), record.episode_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if episode_model != model_id.as_str() {
            return Err(LabError::Conflict("episode identity changed models".into()));
        }
        let source_close: i64 = transaction
            .query_row(
                "SELECT close_time_ms FROM candle_observations WHERE id=?1",
                [record.liquidity_source_bar_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if source_close != timestamp_ms(record.liquidity_source_close_time) {
            return Err(LabError::Conflict(
                "fill liquidity source close time disagrees with observation".into(),
            ));
        }
        transaction.execute(
            "INSERT INTO fills(fill_id,run_id,model_id,event_seq,order_id,episode_id,accounting_mark_seq,price_decimal,qty_decimal,notional_decimal,fee_decimal,source_bar_id,record_json,liquidity_source_bar_id,liquidity_source_close_time_ms) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![record.fill_id.as_str(), run_id.as_str(), model_id.as_str(), u64_to_i64(record.context.event_seq)?, record.order_id.as_str(), record.episode_id.as_str(), u64_to_i64(record.accounting_mark_seq)?, canonical_decimal(record.price.get()), canonical_decimal(record.qty.get()), canonical_decimal(record.notional.get()), canonical_decimal(record.fee.get()), record.source_bar_id.as_str(), serde_json::to_string(record).map_err(json_error)?, record.liquidity_source_bar_id.as_str(), timestamp_ms(record.liquidity_source_close_time)],
        ).map_err(sql_error)?;
    }
    Ok(())
}

fn insert_marks(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    model_id: &ModelId,
    records: &[AccountMark],
) -> Result<(), LabError> {
    for record in records {
        transaction.execute(
            "INSERT INTO account_marks(run_id,model_id,event_seq,kind,cash_total_decimal,cash_free_decimal,cash_reserved_decimal,qty_decimal,price_basis_decimal,cumulative_fees_decimal,equity_decimal,source_bar_id,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![run_id.as_str(), model_id.as_str(), u64_to_i64(record.context.event_seq)?, enum_text(&record.kind)?, canonical_decimal(record.state.cash_total.get()), canonical_decimal(record.state.cash_free.get()), canonical_decimal(record.state.cash_reserved.get()), canonical_decimal(record.state.qty.get()), canonical_decimal(record.state.price_basis.get()), canonical_decimal(record.state.cumulative_fees.get()), canonical_decimal(record.equity.get()), record.source_bar_id.as_str(), serde_json::to_string(record).map_err(json_error)?],
        ).map_err(sql_error)?;
    }
    Ok(())
}

struct StoredSequenced {
    signals: serde_json::Value,
    order_events: serde_json::Value,
    fills: serde_json::Value,
    marks: serde_json::Value,
    last_seq: u64,
}

impl Store {
    fn load_sequenced_model(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
        source: &RunLedgerSource,
    ) -> Result<StoredSequenced, LabError> {
        if let RunLedgerSource::Sealed(lines) = source {
            let crate::storage::ledger_seal::ModelFactVectors {
                signals,
                order_events,
                fills,
                account_marks: marks,
            } = sequenced_from_lines(lines, model_id)?;
            let last: i64 = self
                .connection
                .query_row(
                    "SELECT last_committed_event_seq FROM run_models WHERE run_id=?1 AND model_id=?2",
                    params![run_id.as_str(), model_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| LabError::InvalidConfig("unknown run model".into()))?;
            return Ok(StoredSequenced {
                signals: serde_json::to_value(signals).map_err(json_error)?,
                order_events: serde_json::to_value(order_events).map_err(json_error)?,
                fills: serde_json::to_value(fills).map_err(json_error)?,
                marks: serde_json::to_value(marks).map_err(json_error)?,
                last_seq: nonnegative_u64(last, "model last event sequence")?,
            });
        }
        let signals: Vec<SignalRecord> = load_json_records(
            &self.connection,
            "SELECT record_json FROM signals WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
            run_id,
            model_id,
        )?;
        let order_events: Vec<OrderRecord> = load_json_records(
            &self.connection,
            "SELECT record_json FROM order_events WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
            run_id,
            model_id,
        )?;
        let fills = load_fills(&self.connection, run_id, model_id)?;
        let marks: Vec<AccountMark> = load_json_records(
            &self.connection,
            "SELECT record_json FROM account_marks WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
            run_id,
            model_id,
        )?;
        let last: i64 = self
            .connection
            .query_row(
                "SELECT last_committed_event_seq FROM run_models WHERE run_id=?1 AND model_id=?2",
                params![run_id.as_str(), model_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run model".into()))?;
        Ok(StoredSequenced {
            signals: serde_json::to_value(signals).map_err(json_error)?,
            order_events: serde_json::to_value(order_events).map_err(json_error)?,
            fills: serde_json::to_value(fills).map_err(json_error)?,
            marks: serde_json::to_value(marks).map_err(json_error)?,
            last_seq: nonnegative_u64(last, "model last event sequence")?,
        })
    }

    fn ensure_attempt_uncancelled(
        &self,
        attempt_id: &crate::contracts::AttemptId,
    ) -> Result<(), LabError> {
        let attempt = self.load_attempt_for_run(attempt_id)?;
        match attempt.state {
            AttemptState::Running {
                cancel_requested_at: None,
                ..
            } => Ok(()),
            AttemptState::Running {
                cancel_requested_at: Some(_),
                ..
            } => Err(LabError::Cancelled(
                "attempt cancellation was requested".into(),
            )),
            _ => Err(LabError::Conflict(
                "run attempt is no longer RUNNING".into(),
            )),
        }
    }

    fn load_attempt_for_run(
        &self,
        attempt_id: &crate::contracts::AttemptId,
    ) -> Result<crate::contracts::JobAttempt, LabError> {
        self.load_attempt(attempt_id)
    }

    fn finished_model_ids(&self, run_id: &RunId) -> Result<BTreeSet<String>, LabError> {
        let mut statement = self.connection.prepare(
            "SELECT model_id FROM run_models WHERE run_id=?1 AND status IS NOT NULL ORDER BY model_id",
        ).map_err(sql_error)?;
        statement
            .query_map([run_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(sql_error)
    }
}

fn validate_final_projections(ledger: &ModelLedger) -> Result<(), LabError> {
    let mut final_events = BTreeMap::new();
    for event in &ledger.order_events {
        final_events.insert(event.order_id.as_str(), event);
    }
    if ledger.orders.len() != final_events.len() {
        return Err(LabError::Conflict(
            "final order projections are not one-per-order".into(),
        ));
    }
    for projection in &ledger.orders {
        let event = final_events
            .get(projection.order_id.as_str())
            .ok_or_else(|| {
                LabError::Conflict("final order projection lacks lifecycle event".into())
            })?;
        if serde_json::to_vec(projection).map_err(json_error)?
            != serde_json::to_vec(*event).map_err(json_error)?
        {
            return Err(LabError::Conflict(
                "final order projection is not final lifecycle event".into(),
            ));
        }
    }
    Ok(())
}

fn insert_episodes(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    ledger: &ModelLedger,
) -> Result<(), LabError> {
    for episode in &ledger.episodes {
        if &episode.run_id != run_id || episode.model_id != ledger.model_id {
            return Err(LabError::Conflict("episode run/model mismatch".into()));
        }
        transaction.execute(
            "INSERT OR IGNORE INTO episode_identities(run_id,model_id,episode_id) VALUES (?1,?2,?3)",
            params![run_id.as_str(), ledger.model_id.as_str(), episode.episode_id.as_str()],
        ).map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO episodes(run_id,model_id,episode_id,status,opened_at_ms,closed_at_ms,net_realized_decimal,fees_decimal,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![run_id.as_str(), ledger.model_id.as_str(), episode.episode_id.as_str(), enum_text(&episode.status)?, timestamp_ms(episode.opened_at), episode.closed_at.map(timestamp_ms), canonical_decimal(episode.net_realized.get()), canonical_decimal(episode.fees.get()), serde_json::to_string(episode).map_err(json_error)?],
        ).map_err(sql_error)?;
        for (position, fill) in episode.fill_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO episode_fills(run_id,model_id,episode_id,fill_id,position) VALUES (?1,?2,?3,?4,?5)",
                params![run_id.as_str(), ledger.model_id.as_str(), episode.episode_id.as_str(), fill.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
        for (position, order) in episode.order_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO episode_orders(run_id,model_id,episode_id,order_id,position) VALUES (?1,?2,?3,?4,?5)",
                params![run_id.as_str(), ledger.model_id.as_str(), episode.episode_id.as_str(), order.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
    }
    Ok(())
}

fn load_json_records<T: serde::de::DeserializeOwned>(
    connection: &rusqlite::Connection,
    sql: &str,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Vec<T>, LabError> {
    let mut statement = connection.prepare(sql).map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), model_id.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    rows.map(|row| serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error))
        .collect()
}

fn load_fills(
    connection: &rusqlite::Connection,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Vec<FillRecord>, LabError> {
    let mut statement = connection.prepare(
        "SELECT record_json,liquidity_source_bar_id,liquidity_source_close_time_ms FROM fills WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
    ).map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), model_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })
        .map_err(sql_error)?;
    let mut fills = Vec::new();
    for row in rows {
        let (json, source_id, close_time) = row.map_err(sql_error)?;
        let fill: FillRecord = serde_json::from_str(&json).map_err(json_error)?;
        if source_id.as_deref() != Some(fill.liquidity_source_bar_id.as_str())
            || close_time != Some(timestamp_ms(fill.liquidity_source_close_time))
        {
            return Err(LabError::DataCorrupt(format!(
                "fill liquidity source columns disagree with record: {}",
                fill.fill_id
            )));
        }
        fills.push(fill);
    }
    Ok(fills)
}

fn nonnegative_u64(value: i64, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt(format!("negative stored {label}")))
}

struct StoredModelRow {
    market: String,
    strategy_json: String,
    status: String,
    status_reason: Option<String>,
    last_committed_event_seq: i64,
    final_digest: Option<String>,
}

fn query_terminal_model_row(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Option<StoredModelRow>, LabError> {
    store
        .connection
        .query_row(
            "SELECT market,strategy_json,status,status_reason,last_committed_event_seq,final_digest FROM run_models WHERE run_id=?1 AND model_id=?2 AND status IS NOT NULL",
            params![run_id.as_str(), model_id.as_str()],
            |row| {
                Ok(StoredModelRow {
                    market: row.get(0)?,
                    strategy_json: row.get(1)?,
                    status: row.get(2)?,
                    status_reason: row.get(3)?,
                    last_committed_event_seq: row.get(4)?,
                    final_digest: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

fn load_model_from_row(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
    row: StoredModelRow,
    source: &RunLedgerSource,
) -> Result<ModelLedger, LabError> {
    let sequenced = store.load_sequenced_model(run_id, model_id, source)?;
    let orders: Vec<OrderRecord> = if let RunLedgerSource::Sealed(lines) = source {
        records_of_kind(lines, model_id, crate::contracts::LedgerSection::Orders)?
    } else {
        load_json_records(
            &store.connection,
            "SELECT record_json FROM orders WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
            run_id,
            model_id,
        )?
    };
    let episodes: Vec<EpisodeRecord> = load_json_records(
        &store.connection,
        "SELECT record_json FROM episodes WHERE run_id=?1 AND model_id=?2 ORDER BY opened_at_ms,episode_id",
        run_id,
        model_id,
    )?;
    let ledger = ModelLedger {
        model_id: model_id.clone(),
        market: crate::contracts::MarketId::parse_upbit(&row.market)?,
        strategy: serde_json::from_str(&row.strategy_json).map_err(json_error)?,
        status: serde_json::from_value(serde_json::Value::String(row.status))
            .map_err(json_error)?,
        status_reason: row.status_reason,
        signals: serde_json::from_value(sequenced.signals).map_err(json_error)?,
        orders,
        order_events: serde_json::from_value(sequenced.order_events).map_err(json_error)?,
        fills: serde_json::from_value(sequenced.fills).map_err(json_error)?,
        episodes,
        account_marks: serde_json::from_value(sequenced.marks).map_err(json_error)?,
        last_event_seq: nonnegative_u64(row.last_committed_event_seq, "model last event sequence")?,
    };
    if row.final_digest.as_deref()
        != Some(crate::contracts::ContentHash::of_value(&ledger)?.as_str())
    {
        return Err(LabError::DataCorrupt(format!(
            "model final digest mismatch: {model_id}"
        )));
    }
    Ok(ledger)
}

impl Store {
    fn load_run_bundle_internal(
        &self,
        run_id: &RunId,
        semantic_digest: Option<crate::contracts::ContentHash>,
    ) -> Result<RunBundle, LabError> {
        let header = self
            .load_run_header(run_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        let plan = self
            .load_plan(&header.plan_id)?
            .ok_or_else(|| LabError::DataCorrupt("run plan missing".into()))?;
        let mut datasets = Vec::with_capacity(plan.resolved.dataset_digests.len());
        for (dataset_id, digest) in &plan.resolved.dataset_digests {
            let dataset = self.load_dataset(dataset_id)?.ok_or_else(|| {
                LabError::DataCorrupt(format!("run dataset missing: {dataset_id}"))
            })?;
            if &dataset.manifest.semantic_digest != digest {
                return Err(LabError::DataCorrupt(format!(
                    "run dataset digest mismatch: {dataset_id}"
                )));
            }
            datasets.push(dataset);
        }
        let evidence = match &plan.resolved.spec.evidence_snapshot_id {
            Some(id) => Some(self.load_evidence_snapshot(id)?.ok_or_else(|| {
                LabError::DataCorrupt(format!("run Evidence snapshot missing: {id}"))
            })?),
            None => None,
        };
        let ledger_source = run_ledger_source(self, run_id)?;
        let mut models = Vec::with_capacity(plan.resolved.admissions.len());
        for admission in &plan.resolved.admissions {
            let model_id = admission.model_id.clone();
            let row = query_terminal_model_row(self, run_id, &model_id)?.ok_or_else(|| {
                LabError::Conflict(format!("run model is not terminal: {model_id}"))
            })?;
            let model = load_model_from_row(self, run_id, &model_id, row, &ledger_source)?;
            models.push(model);
        }
        Ok(RunBundle {
            manifest: header.manifest,
            plan: plan.resolved,
            datasets,
            evidence,
            models,
            semantic_digest: semantic_digest.unwrap_or_else(|| {
                crate::contracts::ContentHash::of_bytes(b"pending-run-semantic")
            }),
        })
    }
}

fn derive_run_state(models: &[ModelLedger]) -> JobStatus {
    let completed = models
        .iter()
        .filter(|model| model.status == ModelStatus::Completed)
        .count();
    let failed = models
        .iter()
        .any(|model| model.status == ModelStatus::Failed);
    if failed {
        JobStatus::Failed
    } else if completed == models.len() {
        JobStatus::Completed
    } else if completed > 0 {
        JobStatus::Partial
    } else {
        JobStatus::Blocked
    }
}

#[derive(Clone, Copy)]
enum QueryTable {
    Signals,
    Orders,
    OrderEvents,
    Fills,
    Episodes,
    Marks,
}

impl QueryTable {
    fn source(self) -> &'static str {
        match self {
            Self::Signals => {
                "signals s JOIN run_events e ON e.run_id=s.run_id AND e.event_seq=s.event_seq"
            }
            Self::Orders => "orders s",
            Self::OrderEvents => {
                "order_events s JOIN run_events e ON e.run_id=s.run_id AND e.event_seq=s.event_seq"
            }
            Self::Fills => {
                "fills s JOIN run_events e ON e.run_id=s.run_id AND e.event_seq=s.event_seq"
            }
            Self::Episodes => "episodes s",
            Self::Marks => {
                "account_marks s JOIN run_events e ON e.run_id=s.run_id AND e.event_seq=s.event_seq"
            }
        }
    }
    fn time_column(self) -> &'static str {
        match self {
            Self::Orders => "s.accounting_event_time_ms",
            Self::Episodes => "s.opened_at_ms",
            _ => "e.accounting_event_time_ms",
        }
    }
    fn order_column(self) -> &'static str {
        match self {
            Self::Episodes => "s.opened_at_ms,s.episode_id",
            _ => "s.event_seq",
        }
    }
}

fn resolve_query_model(store: &Store, query: &ResultQuery) -> Result<ModelId, LabError> {
    let selected = query
        .cursor
        .as_ref()
        .map(|cursor| cursor.model_id.clone())
        .or_else(|| query.model_id.clone());
    if let Some(selected) = selected {
        let exists = store
            .connection
            .query_row(
                "SELECT 1 FROM run_models WHERE run_id=?1 AND model_id=?2 AND status IS NOT NULL",
                params![query.run_id.as_str(), selected.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?;
        return exists
            .map(|()| selected)
            .ok_or_else(|| LabError::InvalidConfig("unknown result model".into()));
    }
    let id = store.connection.query_row(
        "SELECT model_id FROM run_models WHERE run_id=?1 AND status IS NOT NULL ORDER BY model_id LIMIT 1",
        [query.run_id.as_str()],
        |row| row.get::<_, String>(0),
    ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig("run has no terminal result model".into()))?;
    ModelId::new(id)
}

fn query_json_page<T: serde::de::DeserializeOwned>(
    store: &Store,
    query: &ResultQuery,
    model: &ModelId,
    offset: u64,
    table: QueryTable,
) -> Result<Page<T>, LabError> {
    if !matches!(table, QueryTable::Episodes)
        && let RunLedgerSource::Sealed(lines) = run_ledger_source(store, &query.run_id)?
    {
        return sealed_json_page(&lines, query, model, offset, table);
    }
    let source = table.source();
    let time = table.time_column();
    let order = table.order_column();
    let filter = if query.range.is_some() {
        format!(" AND {time}>=?3 AND {time}<?4")
    } else {
        String::new()
    };
    let count_sql =
        format!("SELECT COUNT(*) FROM {source} WHERE s.run_id=?1 AND s.model_id=?2{filter}");
    let total: i64 = if let Some(range) = query.range {
        store
            .connection
            .query_row(
                &count_sql,
                params![
                    query.run_id.as_str(),
                    model.as_str(),
                    timestamp_ms(range.start()),
                    timestamp_ms(range.end())
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?
    } else {
        store
            .connection
            .query_row(
                &count_sql,
                params![query.run_id.as_str(), model.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?
    };
    let select_sql = format!(
        "SELECT s.record_json FROM {source} WHERE s.run_id=?1 AND s.model_id=?2{filter} ORDER BY {order} LIMIT ?5 OFFSET ?6"
    );
    let limit = i64::from(query.limit);
    let offset_sql = u64_to_i64(offset)?;
    let mut statement = store.connection.prepare(&select_sql).map_err(sql_error)?;
    let mut records = Vec::new();
    if let Some(range) = query.range {
        let rows = statement
            .query_map(
                params![
                    query.run_id.as_str(),
                    model.as_str(),
                    timestamp_ms(range.start()),
                    timestamp_ms(range.end()),
                    limit,
                    offset_sql
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        for row in rows {
            records.push(serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?);
        }
    } else {
        // Numbered placeholders ?5/?6 require bound slots even without a time filter.
        let select_sql = format!(
            "SELECT s.record_json FROM {source} WHERE s.run_id=?1 AND s.model_id=?2 ORDER BY {order} LIMIT ?3 OFFSET ?4"
        );
        drop(statement);
        let mut statement = store.connection.prepare(&select_sql).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![query.run_id.as_str(), model.as_str(), limit, offset_sql],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        for row in rows {
            records.push(serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?);
        }
    }
    let returned_count = u64::try_from(records.len())
        .map_err(|_| LabError::ResourceLimit("result count overflow".into()))?;
    let total_count = nonnegative_u64(total, "result count")?;
    let next_offset = offset
        .checked_add(returned_count)
        .ok_or_else(|| LabError::ResourceLimit("result cursor overflow".into()))?;
    Ok(Page {
        records,
        returned_count,
        total_count,
        next_cursor: (next_offset < total_count).then(|| QueryCursor {
            model_id: model.clone(),
            offset: next_offset,
        }),
        truncated_reason: (next_offset < total_count).then(|| "LIMIT".into()),
    })
}

fn sealed_json_page<T: serde::de::DeserializeOwned>(
    lines: &[crate::storage::ledger_seal::LedgerLine],
    query: &ResultQuery,
    model: &ModelId,
    offset: u64,
    table: QueryTable,
) -> Result<Page<T>, LabError> {
    let section = match table {
        QueryTable::Signals => LedgerSection::Signals,
        QueryTable::Orders => LedgerSection::Orders,
        QueryTable::OrderEvents => LedgerSection::OrderEvents,
        QueryTable::Fills => LedgerSection::Fills,
        QueryTable::Episodes => LedgerSection::Episodes,
        QueryTable::Marks => LedgerSection::Equity,
    };
    let mut matching: Vec<(u64, &serde_json::Value)> = lines
        .iter()
        .filter(|line| {
            line.section == section
                && line.model_id == *model
                && query.range.is_none_or(|range| {
                    crate::storage::ledger_seal::line_in_range(line, Some(&range))
                })
        })
        .map(|line| (line.event_seq, &line.record))
        .collect();
    matching.sort_by_key(|(seq, _)| *seq);
    let total_count = u64::try_from(matching.len())
        .map_err(|_| LabError::ResourceLimit("result count overflow".into()))?;
    let start = usize::try_from(offset)
        .map_err(|_| LabError::ResourceLimit("result offset overflow".into()))?;
    let end = start
        .saturating_add(query.limit as usize)
        .min(matching.len());
    let records = matching[start..end]
        .iter()
        .map(|(_, record)| serde_json::from_value((*record).clone()).map_err(json_error))
        .collect::<Result<Vec<T>, LabError>>()?;
    let returned_count = u64::try_from(records.len())
        .map_err(|_| LabError::ResourceLimit("result count overflow".into()))?;
    let next_offset = offset
        .checked_add(returned_count)
        .ok_or_else(|| LabError::ResourceLimit("result cursor overflow".into()))?;
    Ok(Page {
        records,
        returned_count,
        total_count,
        next_cursor: (next_offset < total_count).then(|| QueryCursor {
            model_id: model.clone(),
            offset: next_offset,
        }),
        truncated_reason: (next_offset < total_count).then(|| "LIMIT".into()),
    })
}
