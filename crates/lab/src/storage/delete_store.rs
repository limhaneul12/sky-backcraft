//! Common two-step hard deletion over registered resource IDs.
//!
//! Preview computes the exact closure scope (dependents included), discloses
//! blockers/cascade/retained groups and returns a scoped confirmation token.
//! Execution recomputes the scope, refuses stale tokens, then deletes the
//! business rows in one transaction and removes the scope's exclusive files.
//! A minimal journal row drives file-removal recovery after interruption; it
//! never preserves deleted business content. Queued/running jobs always block.

use super::{Store, directory_size, json_error, sql_error, timestamp_ms, validate_relative_path};
use crate::contracts::{
    ContentHash, DELETE_PREVIEW_EXAMPLES, DELETE_PREVIEW_TTL_SECONDS, DeleteBlocker,
    DeleteBlockerClass, DeleteCascadeGroup, DeleteEntryKind, DeleteOutcome, DeletePreview,
    DeleteResource, DeleteRetainedGroup, HardDeleteRequest, JobStatus, LabError, UtcTimestamp,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

/// Guard against unbounded closure expansion on corrupt cyclic state.
const SCOPE_FIXPOINT_LIMIT: usize = 64;
const RAW_PREFIX: &str = "raw/";
const EXPORT_PREFIX: &str = "exports/";
const LEDGER_PREFIX: &str = "ledgers/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScopeFile {
    pub id: String,
    pub relative_path: String,
    pub bytes: u64,
}

/// Exact deletion scope: every business row and file removed by one execution.
#[derive(Debug, Default)]
pub(super) struct DeleteScope {
    policies: Vec<String>,
    policy_revisions: Vec<String>,
    plans: Vec<String>,
    jobs: Vec<String>,
    attempts: Vec<String>,
    runs: Vec<String>,
    datasets: Vec<String>,
    evidence_snapshots: Vec<String>,
    artifacts: Vec<String>,
    suites: Vec<String>,
    suite_cases: Vec<String>,
    schedules: Vec<String>,
    schedule_fires: Vec<String>,
    backups: Vec<String>,
    artifact_files: Vec<ScopeFile>,
    ledger_files: Vec<ScopeFile>,
    raw_files: Vec<ScopeFile>,
    backup_files: Vec<ScopeFile>,
    observations: u64,
    blockers: Vec<DeleteBlocker>,
    counts: Vec<(DeleteEntryKind, u64)>,
    retained_shared: Vec<DeleteRetainedGroup>,
    file_bytes: u64,
}

impl DeleteScope {
    fn total_rows(&self) -> u64 {
        self.counts.iter().map(|(_, count)| count).sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum JournalFileKind {
    RawDir,
    ExportFile,
    LedgerFile,
    ManagedBackup,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct JournalFile {
    kind: JournalFileKind,
    path: String,
    bytes: u64,
}

impl Store {
    /// Compute and disclose the exact hard-delete scope for one resource.
    ///
    /// # Errors
    /// Rejects unknown IDs, corrupt state, or SQLite failure.
    pub fn delete_preview(
        &self,
        resource: &DeleteResource,
        now: UtcTimestamp,
    ) -> Result<DeletePreview, LabError> {
        let scope = self.compute_delete_scope(resource)?;
        let expires_at = UtcTimestamp(
            now.0
                .checked_add_signed(chrono::Duration::seconds(
                    i64::try_from(DELETE_PREVIEW_TTL_SECONDS)
                        .map_err(|_| LabError::Internal("delete TTL overflow".into()))?,
                ))
                .ok_or_else(|| LabError::Internal("delete expiry overflow".into()))?,
        );
        let cascade_required = scope
            .blockers
            .iter()
            .any(|blocker| blocker.class == DeleteBlockerClass::LiveReference);
        let scope_digest = scope_digest(&scope)?;
        let confirmation_token = ContentHash::of_value(&serde_json::json!([
            scope_digest.as_str(),
            timestamp_ms(expires_at),
            cascade_required
        ]))?;
        Ok(DeletePreview {
            resource: resource.clone(),
            cascade_required,
            blockers: scope.blockers.clone(),
            cascade: cascade_groups(&scope),
            retained_shared: scope.retained_shared.clone(),
            reclaimable_file_bytes: Some(scope.file_bytes),
            exclusive_file_count: u64::try_from(
                scope.artifact_files.len()
                    + scope.raw_files.len()
                    + scope.ledger_files.len()
                    + scope.backup_files.len(),
            )
            .map_err(|_| LabError::Internal("file count overflow".into()))?,
            db_row_estimate: scope.total_rows(),
            scope_digest,
            expires_at,
            confirmation_token,
            irreversible: true,
        })
    }

    /// Execute a previewed hard deletion before its confirmation expires.
    ///
    /// The scope is recomputed and must match the preview digest exactly. Live
    /// references refuse without `cascade`; active jobs refuse unconditionally.
    ///
    /// # Errors
    /// Rejects unknown resources, stale/expired previews, live references without
    /// cascade, active jobs, file-removal failure, or SQLite failure.
    pub fn execute_hard_delete(
        &mut self,
        request: &HardDeleteRequest,
        now: UtcTimestamp,
    ) -> Result<DeleteOutcome, LabError> {
        let preview = &request.preview;
        if now >= preview.expires_at {
            return Err(LabError::Conflict(
                "delete preview confirmation expired; request a new preview".into(),
            ));
        }
        self.recover_pending_deletions()?;
        let scope = self.compute_delete_scope(&preview.resource)?;
        let scope_digest = scope_digest(&scope)?;
        if scope_digest != preview.scope_digest {
            return Err(LabError::Conflict(
                "delete scope changed since the preview; request a new preview".into(),
            ));
        }
        let cascade_required = scope
            .blockers
            .iter()
            .any(|blocker| blocker.class == DeleteBlockerClass::LiveReference);
        let expected = ContentHash::of_value(&serde_json::json!([
            scope_digest.as_str(),
            timestamp_ms(preview.expires_at),
            cascade_required
        ]))?;
        if expected != preview.confirmation_token {
            return Err(LabError::Conflict(
                "delete confirmation token does not match the preview".into(),
            ));
        }
        if !request.cascade && cascade_required {
            return Err(LabError::Conflict(
                "live references block this delete; pass cascade after reviewing the preview"
                    .into(),
            ));
        }
        if let Some(blocker) = scope.blockers.iter().find(|blocker| {
            matches!(
                blocker.class,
                DeleteBlockerClass::ActiveJob | DeleteBlockerClass::ProtectedReference
            )
        }) {
            return Err(LabError::Conflict(format!(
                "protected state refuses deletion: {} ({})",
                blocker.reference, blocker.reason
            )));
        }
        let deleted_rows = self.delete_scope_rows(&scope, &scope_digest, &preview.resource, now)?;
        let (files, bytes) = remove_scope_files(&self.raw.root, &scope)?;
        let vacuumed = deleted_rows > 0 && self.compact_after_delete()?;
        Ok(DeleteOutcome {
            resource: preview.resource.clone(),
            deleted_db_rows: deleted_rows,
            deleted_files: u64::try_from(files)
                .map_err(|_| LabError::Internal("deleted file count overflow".into()))?,
            deleted_file_bytes: bytes,
            retained_shared_raw_objects: scope
                .retained_shared
                .iter()
                .find(|group| group.kind == DeleteEntryKind::RawObject)
                .map_or(0, |group| group.count),
            vacuumed,
        })
    }

    /// Finish pending file removals left by an interrupted deletion.
    ///
    /// # Errors
    /// Returns an error when recorded files cannot be removed or are unsafe paths.
    pub fn recover_pending_deletions(&mut self) -> Result<u64, LabError> {
        let rows = pending_journal_rows(&self.connection)?;
        let mut recovered = 0_u64;
        for (id, files_json) in rows {
            remove_journal_files(&self.raw.root, &files_json)?;
            self.connection
                .execute("DELETE FROM deletion_journal WHERE id=?1", [id])
                .map_err(sql_error)?;
            recovered += 1;
        }
        Ok(recovered)
    }

    fn compact_after_delete(&mut self) -> Result<bool, LabError> {
        self.connection
            .execute_batch("VACUUM;")
            .map_err(sql_error)?;
        Ok(true)
    }

    fn init_scope_tables(&self) -> Result<(), LabError> {
        self.connection
            .execute_batch(SCOPE_TABLE_SQL)
            .map_err(sql_error)
    }

    fn seeded_id(
        &self,
        exists_sql: &str,
        insert_sql: &str,
        id: &str,
        kind: &str,
    ) -> Result<(), LabError> {
        let exists = self
            .connection
            .query_row(exists_sql, params![id], |_| Ok(()))
            .optional()
            .map_err(sql_error)?;
        if exists.is_none() {
            return Err(LabError::InvalidConfig(format!(
                "unknown {kind} ID for deletion: {id}"
            )));
        }
        self.connection
            .execute(insert_sql, params![id])
            .map_err(sql_error)?;
        Ok(())
    }

    /// Compute the full deletion closure for one resource on the scope tables.
    fn compute_delete_scope(&self, resource: &DeleteResource) -> Result<DeleteScope, LabError> {
        use DeleteResource as R;
        self.init_scope_tables()?;
        match resource {
            R::Policy { policy_id } => self.seeded_id(
                "SELECT 1 FROM policies WHERE policy_id=?1",
                "INSERT OR IGNORE INTO del_policies(id) VALUES (?1)",
                policy_id.as_str(),
                "policy",
            )?,
            R::Dataset { dataset_id } => self.seeded_id(
                "SELECT 1 FROM datasets WHERE id=?1",
                "INSERT OR IGNORE INTO del_datasets(id) VALUES (?1)",
                dataset_id.as_str(),
                "dataset",
            )?,
            R::Job { job_id } => self.seeded_id(
                "SELECT 1 FROM jobs WHERE id=?1",
                "INSERT OR IGNORE INTO del_jobs(id) VALUES (?1)",
                job_id.as_str(),
                "job",
            )?,
            R::Run { run_id } => self.seeded_id(
                "SELECT 1 FROM runs WHERE id=?1",
                "INSERT OR IGNORE INTO del_runs(id) VALUES (?1)",
                run_id.as_str(),
                "run",
            )?,
            R::EvidenceSnapshot { snapshot_id } => self.seeded_id(
                "SELECT 1 FROM evidence_snapshots WHERE id=?1",
                "INSERT OR IGNORE INTO del_snapshots(id) VALUES (?1)",
                snapshot_id.as_str(),
                "evidence snapshot",
            )?,
            R::Artifact { artifact_id } => self.seeded_id(
                "SELECT 1 FROM run_artifacts WHERE id=?1",
                "INSERT OR IGNORE INTO del_artifacts(id) VALUES (?1)",
                artifact_id.as_str(),
                "artifact",
            )?,
            R::Suite { suite_id } => self.seeded_id(
                "SELECT 1 FROM research_suites WHERE id=?1",
                "INSERT OR IGNORE INTO del_suites(id) VALUES (?1)",
                suite_id.as_str(),
                "suite",
            )?,
            R::Schedule { schedule_id } => self.seeded_id(
                "SELECT 1 FROM collection_schedules WHERE id=?1",
                "INSERT OR IGNORE INTO del_schedules(id) VALUES (?1)",
                schedule_id.as_str(),
                "schedule",
            )?,
            R::Backup { backup_id } => self.seeded_id(
                "SELECT 1 FROM managed_backups WHERE id=?1",
                "INSERT OR IGNORE INTO del_backups(id) VALUES (?1)",
                backup_id.as_str(),
                "backup",
            )?,
        }
        for _ in 0..SCOPE_FIXPOINT_LIMIT {
            let mut added = 0_usize;
            for edge in SCOPE_EDGES {
                added += self.connection.execute(edge, []).map_err(sql_error)?;
            }
            if added == 0 {
                break;
            }
        }
        let mut scope = DeleteScope {
            blockers: self.live_reference_blockers(resource)?,
            ..DeleteScope::default()
        };
        scope.policies = self.ordered_ids("del_policies")?;
        scope.plans = self.ordered_ids("del_plans")?;
        scope.jobs = self.ordered_ids("del_jobs")?;
        scope.attempts = self.ordered_ids("del_attempts")?;
        scope.runs = self.ordered_ids("del_runs")?;
        scope.datasets = self.ordered_ids("del_datasets")?;
        scope.evidence_snapshots = self.ordered_ids("del_snapshots")?;
        scope.artifacts = self.ordered_ids("del_artifacts")?;
        scope.suites = self.ordered_ids("del_suites")?;
        scope.suite_cases = self.ordered_ids("del_suite_cases")?;
        scope.schedules = self.ordered_ids("del_schedules")?;
        scope.schedule_fires = self.ordered_ids("del_schedule_fires")?;
        scope.backups = self.ordered_ids("del_backups")?;
        self.connection
            .execute(
                "INSERT OR IGNORE INTO del_revisions SELECT revision_id FROM policy_revisions WHERE policy_id IN (SELECT id FROM del_policies)",
                [],
            )
            .map_err(sql_error)?;
        scope.policy_revisions = self.ordered_ids("del_revisions")?;
        self.protected_automation_blockers(&mut scope)?;
        self.compute_scope_dependencies(&mut scope)?;
        self.check_active_jobs(&mut scope)?;
        self.compute_scope_counts(&mut scope)?;
        Ok(scope)
    }

    fn ordered_ids(&self, table: &str) -> Result<Vec<String>, LabError> {
        let mut statement = self
            .connection
            .prepare(&format!("SELECT id FROM {table} ORDER BY id"))
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(rows)
    }

    fn scalar(&self, sql: &str) -> Result<u64, LabError> {
        let value: i64 = self
            .connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(sql_error)?;
        u64::try_from(value).map_err(|_| LabError::DataCorrupt("negative scope count".into()))
    }

    /// Direct live references to the seeded target that cascade removes.
    fn live_reference_blockers(
        &self,
        resource: &DeleteResource,
    ) -> Result<Vec<DeleteBlocker>, LabError> {
        use DeleteResource as R;
        let mut blockers = Vec::new();
        let mut push_all = |rows: Vec<String>, reason: &str| {
            for reference in rows.iter().take(DELETE_PREVIEW_EXAMPLES) {
                blockers.push(DeleteBlocker {
                    class: DeleteBlockerClass::LiveReference,
                    reference: reference.clone(),
                    reason: reason.into(),
                });
            }
            if rows.len() > DELETE_PREVIEW_EXAMPLES {
                blockers.push(DeleteBlocker {
                    class: DeleteBlockerClass::LiveReference,
                    reference: format!("…({} more)", rows.len() - DELETE_PREVIEW_EXAMPLES),
                    reason: reason.into(),
                });
            }
        };
        match resource {
            R::Policy { .. } => {
                push_all(
                    self.string_column(
                        "SELECT plan_id FROM plan_policy_revisions WHERE policy_id IN (SELECT id FROM del_policies) ORDER BY plan_id",
                    )?,
                    "a frozen plan uses this policy; cascade removes the plan and its runs",
                );
                push_all(
                    self.string_column(
                        "SELECT run_id FROM run_models WHERE policy_id IN (SELECT id FROM del_policies) ORDER BY run_id",
                    )?,
                    "a run records models of this policy; cascade removes those runs",
                );
            }
            R::Dataset { .. } => {
                push_all(
                    self.string_column(
                        "SELECT derived_dataset_id FROM dataset_derivations WHERE source_dataset_id IN (SELECT id FROM del_datasets) ORDER BY derived_dataset_id",
                    )?,
                    "a derived dataset is built from this snapshot; cascade removes it",
                );
                push_all(
                    self.string_column(
                        "SELECT plan_id FROM plan_datasets WHERE dataset_id IN (SELECT id FROM del_datasets) ORDER BY plan_id",
                    )?,
                    "a frozen plan includes this dataset; cascade removes the plan and its runs",
                );
                push_all(
                    self.string_column(
                        "SELECT a.job_id FROM job_attempts a JOIN job_attempt_dataset_outputs o ON o.attempt_id=a.id WHERE o.dataset_id IN (SELECT id FROM del_datasets) ORDER BY a.job_id",
                    )?,
                    "a job attempt published this dataset; cascade removes the job history",
                );
            }
            R::Job { .. } => {
                push_all(
                    self.string_column(
                        "SELECT o.dataset_id FROM job_attempt_dataset_outputs o WHERE o.attempt_id IN (SELECT id FROM del_attempts) ORDER BY o.dataset_id",
                    )?,
                    "this job published a dataset; cascade removes it",
                );
                push_all(
                    self.string_column(
                        "SELECT o.run_id FROM job_attempt_run_outputs o WHERE o.attempt_id IN (SELECT id FROM del_attempts) ORDER BY o.run_id",
                    )?,
                    "this job published a run; cascade removes it",
                );
                push_all(
                    self.string_column(
                        "SELECT o.artifact_id FROM job_attempt_artifact_outputs o WHERE o.attempt_id IN (SELECT id FROM del_attempts) ORDER BY o.artifact_id",
                    )?,
                    "this job published export artifacts; cascade removes them",
                );
            }
            R::Run { .. } => {
                push_all(
                    self.string_column(
                        "SELECT job_id FROM runs WHERE id IN (SELECT id FROM del_runs) ORDER BY job_id",
                    )?,
                    "the producing job attempt records this run as its output; cascade removes the job history",
                );
            }
            R::EvidenceSnapshot { .. } => {
                push_all(
                    self.string_column(
                        "SELECT id FROM plans WHERE evidence_snapshot_id IN (SELECT id FROM del_snapshots) ORDER BY id",
                    )?,
                    "a frozen plan uses this evidence snapshot; cascade removes the plan and its runs",
                );
            }
            R::Artifact { .. } => {
                push_all(
                    self.string_column(
                        "SELECT a.job_id FROM job_attempts a JOIN job_attempt_artifact_outputs o ON o.attempt_id=a.id WHERE o.artifact_id IN (SELECT id FROM del_artifacts) ORDER BY a.job_id",
                    )?,
                    "a job attempt lists this artifact as its output; cascade removes the job history",
                );
            }
            R::Suite { .. } | R::Schedule { .. } | R::Backup { .. } => {}
        }
        Ok(blockers)
    }

    fn protected_automation_blockers(&self, scope: &mut DeleteScope) -> Result<(), LabError> {
        let queries = [
            (
                "SELECT 'suite:' || c.suite_id FROM research_suite_cases c WHERE c.plan_id IN (SELECT id FROM del_plans) OR c.job_id IN (SELECT id FROM del_jobs) OR c.attempt_id IN (SELECT id FROM del_attempts) OR c.run_id IN (SELECT id FROM del_runs) ORDER BY c.suite_id,c.case_index",
                "a research suite retains this frozen lineage; preview-delete the suite first",
            ),
            (
                "SELECT 'schedule:' || f.schedule_id FROM schedule_fires f WHERE f.job_id IN (SELECT id FROM del_jobs) OR f.attempt_id IN (SELECT id FROM del_attempts) OR f.dataset_id IN (SELECT id FROM del_datasets) ORDER BY f.schedule_id,f.boundary_ms",
                "a collection schedule retains this fire lineage; preview-delete the schedule first",
            ),
            (
                "SELECT 'schedule:' || s.id FROM collection_schedules s WHERE s.last_success_dataset_id IN (SELECT id FROM del_datasets) ORDER BY s.id",
                "a collection schedule retains this last-success dataset; preview-delete the schedule first",
            ),
        ];
        for (sql, reason) in queries {
            let rows = self.string_column(sql)?;
            for reference in rows.iter().take(DELETE_PREVIEW_EXAMPLES) {
                scope.blockers.push(DeleteBlocker {
                    class: DeleteBlockerClass::ProtectedReference,
                    reference: reference.clone(),
                    reason: reason.into(),
                });
            }
            if rows.len() > DELETE_PREVIEW_EXAMPLES {
                scope.blockers.push(DeleteBlocker {
                    class: DeleteBlockerClass::ProtectedReference,
                    reference: format!("…({} more)", rows.len() - DELETE_PREVIEW_EXAMPLES),
                    reason: reason.into(),
                });
            }
        }
        Ok(())
    }

    fn string_column(&self, sql: &str) -> Result<Vec<String>, LabError> {
        let mut statement = self.connection.prepare(sql).map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(rows)
    }

    /// Derive collections, exclusive observations and exclusive raw objects.
    #[expect(
        clippy::too_many_lines,
        reason = "one deterministic exclusivity pass over datasets, observations and raw objects"
    )]
    fn compute_scope_dependencies(&self, scope: &mut DeleteScope) -> Result<(), LabError> {
        let connection = &self.connection;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_requests SELECT request_id FROM datasets WHERE id IN (SELECT id FROM del_datasets)",
                [],
            )
            .map_err(sql_error)?;
        // Candidate observations: dataset members plus this request's page rows.
        connection
            .execute(
                "INSERT OR IGNORE INTO del_obs SELECT observation_id FROM dataset_members WHERE dataset_id IN (SELECT id FROM del_datasets)",
                [],
            )
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_obs SELECT pm.observation_id FROM collection_page_members pm WHERE pm.request_id IN (SELECT id FROM del_requests)",
                [],
            )
            .map_err(sql_error)?;
        let candidate_observations = connection
            .query_row("SELECT COUNT(*) FROM del_obs", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql_error)?;
        // Observations still referenced by survivors are retained, never deleted.
        connection
            .execute_batch(
                "CREATE TEMP TABLE keep_obs AS
                 SELECT m.observation_id AS id FROM dataset_members m WHERE m.dataset_id NOT IN (SELECT id FROM del_datasets)
                 UNION SELECT c.constituent_id FROM observation_constituents c WHERE c.observation_id NOT IN (SELECT id FROM del_obs)
                 UNION SELECT s.observation_id FROM signal_source_bars s JOIN signals sg ON sg.signal_id=s.signal_id WHERE sg.run_id NOT IN (SELECT id FROM del_runs)
                 UNION SELECT f.source_bar_id FROM fills f WHERE f.run_id NOT IN (SELECT id FROM del_runs)
                 UNION SELECT am.source_bar_id FROM account_marks am WHERE am.run_id NOT IN (SELECT id FROM del_runs)
                 UNION SELECT pm.observation_id FROM collection_page_members pm WHERE pm.request_id NOT IN (SELECT id FROM del_requests);
                 DELETE FROM del_obs WHERE id IN (SELECT id FROM keep_obs);
                 DROP TABLE keep_obs;",
            )
            .map_err(sql_error)?;
        scope.observations = self.scalar("SELECT COUNT(*) FROM del_obs")?;
        // Candidate raw objects linked by the scope.
        connection
            .execute(
                "INSERT OR IGNORE INTO del_raws SELECT raw_object_id FROM observation_raw_objects WHERE observation_id IN (SELECT id FROM del_obs)",
                [],
            )
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_raws SELECT raw_object_id FROM dataset_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
                [],
            )
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_raws SELECT raw_object_id FROM quality_issue_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
                [],
            )
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_raws SELECT raw_object_id FROM collection_raw_objects WHERE request_id IN (SELECT id FROM del_requests)",
                [],
            )
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO del_raws SELECT raw_object_id FROM collection_pages WHERE request_id IN (SELECT id FROM del_requests)",
                [],
            )
            .map_err(sql_error)?;
        let candidate_raws = connection
            .query_row("SELECT COUNT(*) FROM del_raws", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql_error)?;
        connection
            .execute_batch(
                "CREATE TEMP TABLE keep_raws AS
                 SELECT r.raw_object_id AS id FROM dataset_raw_objects r WHERE r.dataset_id NOT IN (SELECT id FROM del_datasets)
                 UNION SELECT q.raw_object_id FROM quality_issue_raw_objects q WHERE q.dataset_id NOT IN (SELECT id FROM del_datasets)
                 UNION SELECT o.raw_object_id FROM observation_raw_objects o WHERE o.observation_id NOT IN (SELECT id FROM del_obs)
                 UNION SELECT c.raw_object_id FROM collection_raw_objects c WHERE c.request_id NOT IN (SELECT id FROM del_requests)
                 UNION SELECT p.raw_object_id FROM collection_pages p WHERE p.request_id NOT IN (SELECT id FROM del_requests);
                 DELETE FROM del_raws WHERE id IN (SELECT id FROM keep_raws);
                 DROP TABLE keep_raws;",
            )
            .map_err(sql_error)?;
        let retained_raws = self.scalar("SELECT COUNT(*) FROM del_raws")?;
        let retained_raw_count = u64::try_from(candidate_raws)
            .map_err(|_| LabError::DataCorrupt("negative raw candidate count".into()))?
            .saturating_sub(retained_raws);
        if retained_raw_count > 0 {
            scope.retained_shared.push(DeleteRetainedGroup {
                kind: DeleteEntryKind::RawObject,
                count: retained_raw_count,
            });
        }
        let retained_observations = u64::try_from(candidate_observations)
            .map_err(|_| LabError::DataCorrupt("negative observation candidate count".into()))?
            .saturating_sub(scope.observations);
        if retained_observations > 0 {
            scope.retained_shared.push(DeleteRetainedGroup {
                kind: DeleteEntryKind::Observation,
                count: retained_observations,
            });
        }
        // Exclusive files removed by execution.
        let mut statement = connection.prepare(
            "SELECT id,relative_path,bytes FROM run_artifacts WHERE id IN (SELECT id FROM del_artifacts) ORDER BY id",
        ).map_err(sql_error)?;
        scope.artifact_files = statement
            .query_map([], |row| {
                Ok(ScopeFile {
                    id: row.get(0)?,
                    relative_path: row.get(1)?,
                    bytes: row.get::<_, i64>(2)?.max(0).cast_unsigned(),
                })
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let mut statement = connection.prepare(
            "SELECT id,relative_path,compressed_bytes FROM raw_objects WHERE id IN (SELECT id FROM del_raws) ORDER BY id",
        ).map_err(sql_error)?;
        scope.raw_files = statement
            .query_map([], |row| {
                Ok(ScopeFile {
                    id: row.get(0)?,
                    relative_path: row.get(1)?,
                    bytes: row.get::<_, i64>(2)?.max(0).cast_unsigned(),
                })
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let mut statement = connection.prepare(
            "SELECT sha256,relative_path,compressed_bytes FROM run_ledger_chunks WHERE run_id IN (SELECT id FROM del_runs) ORDER BY run_id,chunk_index",
        ).map_err(sql_error)?;
        scope.ledger_files = statement
            .query_map([], |row| {
                Ok(ScopeFile {
                    id: row.get(0)?,
                    relative_path: row.get(1)?,
                    bytes: row.get::<_, i64>(2)?.max(0).cast_unsigned(),
                })
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let mut statement = connection.prepare(
            "SELECT id,relative_path,bytes FROM managed_backups WHERE id IN (SELECT id FROM del_backups) ORDER BY id",
        ).map_err(sql_error)?;
        scope.backup_files = statement
            .query_map([], |row| {
                Ok(ScopeFile {
                    id: row.get(0)?,
                    relative_path: row.get(1)?,
                    bytes: row.get::<_, i64>(2)?.max(0).cast_unsigned(),
                })
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        // Preview measures the real on-disk size of each exclusive raw directory.
        for file in &mut scope.raw_files {
            file.bytes = directory_size(&self.raw.root.join(&file.relative_path))?;
        }
        let backup_root = super::maintenance_store::managed_backup_root(&self.raw.root)?;
        for file in &mut scope.backup_files {
            crate::contracts::BackupId::new(file.relative_path.clone())?;
            if file.relative_path != file.id {
                return Err(LabError::DataCorrupt(
                    "managed backup path does not match its opaque backup ID".into(),
                ));
            }
            file.bytes = directory_size(&backup_root.join(&file.relative_path))?;
        }
        scope.file_bytes = scope
            .artifact_files
            .iter()
            .chain(scope.raw_files.iter())
            .chain(scope.ledger_files.iter())
            .chain(scope.backup_files.iter())
            .map(|file| file.bytes)
            .fold(0_u64, u64::saturating_add);
        Ok(())
    }

    /// Queued or running jobs anywhere in scope refuse deletion outright.
    fn check_active_jobs(&self, scope: &mut DeleteScope) -> Result<(), LabError> {
        let queued = super::enum_text(&JobStatus::Queued)?;
        let running = super::enum_text(&JobStatus::Running)?;
        let mut statement = self.connection.prepare(
            "SELECT id FROM jobs WHERE current_status IN (?1, ?2) AND (
                id IN (SELECT id FROM del_jobs)
                OR id IN (SELECT job_id FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites) AND job_id IS NOT NULL)
                OR id IN (SELECT job_id FROM schedule_fires WHERE schedule_id IN (SELECT id FROM del_schedules) AND job_id IS NOT NULL)
             ) ORDER BY id",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(params![queued, running], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        for job_id in rows {
            scope.blockers.push(DeleteBlocker {
                class: DeleteBlockerClass::ActiveJob,
                reference: format!("job:{job_id}"),
                reason: "queued/running work refuses deletion; cancel the job first and wait for a terminal state".into(),
            });
        }
        let retained = [
            (
                DeleteEntryKind::Plan,
                "SELECT COUNT(DISTINCT plan_id) FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites) AND plan_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Job,
                "SELECT COUNT(DISTINCT job_id) FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites) AND job_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Attempt,
                "SELECT COUNT(DISTINCT attempt_id) FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites) AND attempt_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Run,
                "SELECT COUNT(DISTINCT run_id) FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites) AND run_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Job,
                "SELECT COUNT(DISTINCT job_id) FROM schedule_fires WHERE schedule_id IN (SELECT id FROM del_schedules) AND job_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Attempt,
                "SELECT COUNT(DISTINCT attempt_id) FROM schedule_fires WHERE schedule_id IN (SELECT id FROM del_schedules) AND attempt_id IS NOT NULL",
            ),
            (
                DeleteEntryKind::Dataset,
                "SELECT COUNT(*) FROM (
                    SELECT dataset_id AS id FROM schedule_fires WHERE schedule_id IN (SELECT id FROM del_schedules) AND dataset_id IS NOT NULL
                    UNION
                    SELECT last_success_dataset_id FROM collection_schedules WHERE id IN (SELECT id FROM del_schedules) AND last_success_dataset_id IS NOT NULL
                 )",
            ),
        ];
        for (kind, sql) in retained {
            let count = self.scalar(sql)?;
            if count > 0 {
                if let Some(group) = scope
                    .retained_shared
                    .iter_mut()
                    .find(|group| group.kind == kind)
                {
                    group.count = group.count.saturating_add(count);
                } else {
                    scope
                        .retained_shared
                        .push(DeleteRetainedGroup { kind, count });
                }
            }
        }
        Ok(())
    }

    fn compute_scope_counts(&self, scope: &mut DeleteScope) -> Result<(), LabError> {
        for sql in SCOPE_COUNTS {
            let count = self.scalar(sql)?;
            scope.counts.push((DeleteEntryKind::Run, count));
        }
        // Retained frozen plans and policy revisions referenced only by scope runs.
        let retained_plans = self.scalar(
            "SELECT COUNT(DISTINCT plan_id) FROM runs WHERE id IN (SELECT id FROM del_runs) AND plan_id NOT IN (SELECT id FROM del_plans)",
        )?;
        if retained_plans > 0 {
            scope.retained_shared.push(DeleteRetainedGroup {
                kind: DeleteEntryKind::Plan,
                count: retained_plans,
            });
        }
        let retained_policies = self.scalar(
            "SELECT COUNT(DISTINCT policy_id) FROM run_models WHERE run_id IN (SELECT id FROM del_runs) AND policy_id IS NOT NULL AND policy_id NOT IN (SELECT id FROM del_policies)",
        )?;
        if retained_policies > 0 {
            scope.retained_shared.push(DeleteRetainedGroup {
                kind: DeleteEntryKind::Policy,
                count: retained_policies,
            });
        }
        let retained_versions = self.scalar(
            "SELECT COUNT(DISTINCT m.revision_id) FROM evidence_snapshot_members m WHERE m.snapshot_id IN (SELECT id FROM del_snapshots) AND m.revision_id NOT IN (SELECT revision_id FROM evidence_snapshot_members WHERE snapshot_id NOT IN (SELECT id FROM del_snapshots))",
        )?;
        if retained_versions > 0 {
            scope.retained_shared.push(DeleteRetainedGroup {
                kind: DeleteEntryKind::PolicyRevision,
                count: retained_versions,
            });
        }
        Ok(())
    }

    fn delete_scope_rows(
        &mut self,
        scope: &DeleteScope,
        digest: &ContentHash,
        resource: &DeleteResource,
        now: UtcTimestamp,
    ) -> Result<u64, LabError> {
        let mut journal_files = Vec::new();
        for file in &scope.raw_files {
            journal_files.push(JournalFile {
                kind: JournalFileKind::RawDir,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            });
        }
        for file in &scope.artifact_files {
            journal_files.push(JournalFile {
                kind: JournalFileKind::ExportFile,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            });
        }
        for file in &scope.ledger_files {
            journal_files.push(JournalFile {
                kind: JournalFileKind::LedgerFile,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            });
        }
        for file in &scope.backup_files {
            journal_files.push(JournalFile {
                kind: JournalFileKind::ManagedBackup,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            });
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let mut deleted = 0_u64;
        for sql in SCOPE_DELETES {
            deleted +=
                u64::try_from(transaction.execute(sql, []).map_err(sql_error)?).unwrap_or(u64::MAX);
        }
        if !journal_files.is_empty() {
            transaction
                .execute(
                    "INSERT INTO deletion_journal(resource_kind,resource_id,scope_digest,pending_files_json,created_at_ms) VALUES (?1,?2,?3,?4,?5)",
                    params![
                        resource.label(),
                        resource.label(),
                        digest.as_str(),
                        serde_json::to_string(&journal_files).map_err(json_error)?,
                        timestamp_ms(now)
                    ],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(deleted)
    }
}

fn remove_scope_files(root: &Path, scope: &DeleteScope) -> Result<(usize, u64), LabError> {
    let mut count = 0_usize;
    let mut bytes = 0_u64;
    for file in &scope.raw_files {
        remove_journal_file(
            root,
            &JournalFile {
                kind: JournalFileKind::RawDir,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            },
        )?;
        count += 1;
        bytes += file.bytes;
    }
    for file in &scope.artifact_files {
        remove_journal_file(
            root,
            &JournalFile {
                kind: JournalFileKind::ExportFile,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            },
        )?;
        count += 1;
        bytes += file.bytes;
    }
    for file in &scope.ledger_files {
        remove_journal_file(
            root,
            &JournalFile {
                kind: JournalFileKind::LedgerFile,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            },
        )?;
        count += 1;
        bytes += file.bytes;
    }
    for file in &scope.backup_files {
        remove_journal_file(
            root,
            &JournalFile {
                kind: JournalFileKind::ManagedBackup,
                path: file.relative_path.clone(),
                bytes: file.bytes,
            },
        )?;
        count += 1;
        bytes += file.bytes;
    }
    Ok((count, bytes))
}

/// Free-function recovery used by `Store::open` before the owner is composed.
///
/// # Errors
/// Returns an error when journal rows cannot be read or recorded files cannot
/// be removed.
pub(super) fn recover_pending_file_removals(
    connection: &mut Connection,
    root: &Path,
) -> Result<(), LabError> {
    let rows = pending_journal_rows(connection)?;
    if rows.is_empty() {
        return Ok(());
    }
    let transaction = connection.transaction().map_err(sql_error)?;
    for (id, files_json) in &rows {
        remove_journal_files(root, files_json)?;
        transaction
            .execute("DELETE FROM deletion_journal WHERE id=?1", [id])
            .map_err(sql_error)?;
    }
    transaction.commit().map_err(sql_error)
}

fn pending_journal_rows(connection: &Connection) -> Result<Vec<(i64, String)>, LabError> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='deletion_journal'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(sql_error)?;
    if exists.is_none() {
        return Ok(Vec::new());
    }
    let mut statement = connection
        .prepare("SELECT id,pending_files_json FROM deletion_journal ORDER BY id")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    Ok(rows)
}

fn remove_journal_files(root: &Path, files_json: &str) -> Result<(), LabError> {
    let files: Vec<JournalFile> = serde_json::from_str(files_json).map_err(json_error)?;
    for file in &files {
        remove_journal_file(root, file)?;
    }
    Ok(())
}

fn remove_journal_file(root: &Path, file: &JournalFile) -> Result<(), LabError> {
    validate_relative_path(&file.path)?;
    let inside_root = match file.kind {
        JournalFileKind::RawDir => file.path.starts_with(RAW_PREFIX),
        JournalFileKind::ExportFile => file.path.starts_with(EXPORT_PREFIX),
        JournalFileKind::LedgerFile => file.path.starts_with(LEDGER_PREFIX),
        JournalFileKind::ManagedBackup => {
            crate::contracts::BackupId::new(file.path.clone()).is_ok()
        }
    };
    if !inside_root {
        return Err(LabError::DataCorrupt(
            "deletion journal names a file outside the server-owned store".into(),
        ));
    }
    let owner_root = if file.kind == JournalFileKind::ManagedBackup {
        super::maintenance_store::managed_backup_root(root)?
    } else {
        root.to_path_buf()
    };
    let path = owner_root.join(&file.path);
    if file.kind == JournalFileKind::ManagedBackup
        && !owner_root
            .try_exists()
            .map_err(super::io_error("inspect managed backup root"))?
    {
        return Ok(());
    }
    super::reject_symlink_chain(&owner_root, &path)?;
    match file.kind {
        JournalFileKind::RawDir => {
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(super::io_error("remove raw object dir"))?;
            }
        }
        JournalFileKind::LedgerFile => {
            if path.is_file() {
                std::fs::remove_file(&path).map_err(super::io_error("remove ledger chunk"))?;
                let mut parent = path.parent();
                while let Some(directory) = parent {
                    if directory == root.join(LEDGER_PREFIX.trim_end_matches('/'))
                        || directory == root
                        || !directory.is_dir()
                    {
                        break;
                    }
                    if std::fs::remove_dir(directory).is_err() {
                        break;
                    }
                    parent = directory.parent();
                }
            }
        }
        JournalFileKind::ExportFile => {
            if path.is_file() {
                std::fs::remove_file(&path).map_err(super::io_error("remove export file"))?;
                // Remove emptied export directories up to the exports root.
                let mut parent = path.parent();
                while let Some(directory) = parent {
                    if directory == root.join(EXPORT_PREFIX.trim_end_matches('/'))
                        || directory == root
                        || !directory.is_dir()
                    {
                        break;
                    }
                    if std::fs::remove_dir(directory).is_err() {
                        break;
                    }
                    parent = directory.parent();
                }
            }
        }
        JournalFileKind::ManagedBackup => {
            if path.exists() {
                std::fs::remove_dir_all(&path)
                    .map_err(super::io_error("remove managed backup directory"))?;
            }
        }
    }
    Ok(())
}

fn scope_digest(scope: &DeleteScope) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&serde_json::json!({
        "v": 1,
        "policies": sorted(scope.policies.iter()),
        "policy_revisions": sorted(scope.policy_revisions.iter()),
        "plans": sorted(scope.plans.iter()),
        "jobs": sorted(scope.jobs.iter()),
        "attempts": sorted(scope.attempts.iter()),
        "runs": sorted(scope.runs.iter()),
        "datasets": sorted(scope.datasets.iter()),
        "evidence_snapshots": sorted(scope.evidence_snapshots.iter()),
        "artifacts": sorted(scope.artifacts.iter()),
        "suites": sorted(scope.suites.iter()),
        "suite_cases": sorted(scope.suite_cases.iter()),
        "schedules": sorted(scope.schedules.iter()),
        "schedule_fires": sorted(scope.schedule_fires.iter()),
        "backups": sorted(scope.backups.iter()),
        "artifact_files": scope.artifact_files.iter().map(|f| (&f.id, &f.relative_path, f.bytes)).collect::<Vec<_>>(),
        "ledger_files": scope.ledger_files.iter().map(|f| (&f.id, &f.relative_path, f.bytes)).collect::<Vec<_>>(),
        "raw_files": scope.raw_files.iter().map(|f| (&f.id, &f.relative_path, f.bytes)).collect::<Vec<_>>(),
        "backup_files": scope.backup_files.iter().map(|f| (&f.id, &f.relative_path, f.bytes)).collect::<Vec<_>>(),
        "observations": scope.observations,
        "counts": scope.counts,
        "retained_shared": scope.retained_shared,
    }))
}

fn sorted<'a, I: Iterator<Item = &'a String>>(values: I) -> Vec<&'a String> {
    let mut values: Vec<_> = values.collect();
    values.sort_unstable();
    values
}

fn cascade_groups(scope: &DeleteScope) -> Vec<DeleteCascadeGroup> {
    let group = |kind, count: usize, examples: &[String]| {
        (count > 0).then(|| DeleteCascadeGroup {
            kind,
            count: u64::try_from(count).unwrap_or(u64::MAX),
            examples: examples
                .iter()
                .take(DELETE_PREVIEW_EXAMPLES)
                .cloned()
                .collect(),
        })
    };
    [
        group(
            DeleteEntryKind::Policy,
            scope.policies.len(),
            &scope.policies,
        ),
        group(
            DeleteEntryKind::PolicyRevision,
            scope.policy_revisions.len(),
            &scope.policy_revisions,
        ),
        group(DeleteEntryKind::Plan, scope.plans.len(), &scope.plans),
        group(DeleteEntryKind::Job, scope.jobs.len(), &scope.jobs),
        group(
            DeleteEntryKind::Attempt,
            scope.attempts.len(),
            &scope.attempts,
        ),
        group(DeleteEntryKind::Run, scope.runs.len(), &scope.runs),
        group(
            DeleteEntryKind::Dataset,
            scope.datasets.len(),
            &scope.datasets,
        ),
        group(
            DeleteEntryKind::EvidenceSnapshot,
            scope.evidence_snapshots.len(),
            &scope.evidence_snapshots,
        ),
        group(
            DeleteEntryKind::Artifact,
            scope.artifacts.len(),
            &scope.artifacts,
        ),
        group(
            DeleteEntryKind::RawObject,
            scope.raw_files.len(),
            &scope
                .raw_files
                .iter()
                .map(|f| f.id.clone())
                .collect::<Vec<_>>(),
        ),
        group(DeleteEntryKind::Suite, scope.suites.len(), &scope.suites),
        group(
            DeleteEntryKind::Schedule,
            scope.schedules.len(),
            &scope.schedules,
        ),
        group(DeleteEntryKind::Backup, scope.backups.len(), &scope.backups),
        group(
            DeleteEntryKind::Artifact,
            scope.ledger_files.len(),
            &scope
                .ledger_files
                .iter()
                .map(|f| f.id.clone())
                .collect::<Vec<_>>(),
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Ordered child-first delete statements over the temp scope tables.
const SCOPE_DELETES: &[&str] = &[
    // automation parents release links but retain ordinary plans/jobs/runs/datasets
    "DELETE FROM research_suite_cases WHERE id IN (SELECT id FROM del_suite_cases)",
    "DELETE FROM research_suite_folds WHERE suite_id IN (SELECT id FROM del_suites)",
    "DELETE FROM research_suites WHERE id IN (SELECT id FROM del_suites)",
    "DELETE FROM schedule_fires WHERE id IN (SELECT id FROM del_schedule_fires)",
    "DELETE FROM collection_schedules WHERE id IN (SELECT id FROM del_schedules)",
    "DELETE FROM managed_backups WHERE id IN (SELECT id FROM del_backups)",
    // run bundles
    "DELETE FROM run_artifacts WHERE id IN (SELECT id FROM del_artifacts) OR run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM validation_results WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM signal_source_bars WHERE signal_id IN (SELECT signal_id FROM signals WHERE run_id IN (SELECT id FROM del_runs))",
    "DELETE FROM episode_fills WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM episode_orders WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM fills WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM episodes WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM order_events WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM orders WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM order_identities WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM signals WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM account_marks WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM episode_identities WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM run_events WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM run_models WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM run_ledger_chunks WHERE run_id IN (SELECT id FROM del_runs)",
    "DELETE FROM runs WHERE id IN (SELECT id FROM del_runs)",
    // job history
    "DELETE FROM job_attempt_dataset_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "DELETE FROM job_attempt_run_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "DELETE FROM job_attempt_artifact_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "DELETE FROM job_attempts WHERE job_id IN (SELECT id FROM del_jobs)",
    "DELETE FROM jobs WHERE id IN (SELECT id FROM del_jobs)",
    // frozen plans
    "DELETE FROM plan_policy_revisions WHERE plan_id IN (SELECT id FROM del_plans)",
    "DELETE FROM plan_datasets WHERE plan_id IN (SELECT id FROM del_plans)",
    "DELETE FROM plans WHERE id IN (SELECT id FROM del_plans)",
    // datasets
    "DELETE FROM quality_issue_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM quality_issues WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM dataset_observation_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM dataset_observation_constituents WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM dataset_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM dataset_members WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM dataset_derivations WHERE derived_dataset_id IN (SELECT id FROM del_datasets) OR source_dataset_id IN (SELECT id FROM del_datasets)",
    "DELETE FROM datasets WHERE id IN (SELECT id FROM del_datasets)",
    // collection provenance
    "DELETE FROM collection_page_members WHERE request_id IN (SELECT id FROM del_requests)",
    "DELETE FROM collection_pages WHERE request_id IN (SELECT id FROM del_requests)",
    "DELETE FROM collection_raw_objects WHERE request_id IN (SELECT id FROM del_requests)",
    "DELETE FROM collections WHERE request_id IN (SELECT id FROM del_requests)",
    // evidence snapshots
    "DELETE FROM evidence_snapshot_members WHERE snapshot_id IN (SELECT id FROM del_snapshots)",
    "DELETE FROM evidence_snapshots WHERE id IN (SELECT id FROM del_snapshots)",
    // policies; the revision parent FK is deferrable, so the whole history
    // deletes inside one transaction regardless of revision order
    "DELETE FROM policy_revisions WHERE policy_id IN (SELECT id FROM del_policies)",
    "DELETE FROM policies WHERE policy_id IN (SELECT id FROM del_policies)",
    // shared-content cleanup
    "DELETE FROM observation_raw_objects WHERE observation_id IN (SELECT id FROM del_obs)",
    "DELETE FROM observation_constituents WHERE observation_id IN (SELECT id FROM del_obs)",
    "DELETE FROM candle_observations WHERE id IN (SELECT id FROM del_obs)",
    "DELETE FROM raw_objects WHERE id IN (SELECT id FROM del_raws)",
];

/// Row-count queries mirroring `SCOPE_DELETES` for exact preview accounting.
const SCOPE_COUNTS: &[&str] = &[
    "SELECT COUNT(*) FROM research_suite_cases WHERE id IN (SELECT id FROM del_suite_cases)",
    "SELECT COUNT(*) FROM research_suite_folds WHERE suite_id IN (SELECT id FROM del_suites)",
    "SELECT COUNT(*) FROM research_suites WHERE id IN (SELECT id FROM del_suites)",
    "SELECT COUNT(*) FROM schedule_fires WHERE id IN (SELECT id FROM del_schedule_fires)",
    "SELECT COUNT(*) FROM collection_schedules WHERE id IN (SELECT id FROM del_schedules)",
    "SELECT COUNT(*) FROM managed_backups WHERE id IN (SELECT id FROM del_backups)",
    "SELECT COUNT(*) FROM run_artifacts WHERE id IN (SELECT id FROM del_artifacts) OR run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM validation_results WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM signal_source_bars WHERE signal_id IN (SELECT signal_id FROM signals WHERE run_id IN (SELECT id FROM del_runs))",
    "SELECT COUNT(*) FROM episode_fills WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM episode_orders WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM fills WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM episodes WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM order_events WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM orders WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM order_identities WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM signals WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM account_marks WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM episode_identities WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM run_events WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM run_models WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM run_ledger_chunks WHERE run_id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM runs WHERE id IN (SELECT id FROM del_runs)",
    "SELECT COUNT(*) FROM job_attempt_dataset_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "SELECT COUNT(*) FROM job_attempt_run_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "SELECT COUNT(*) FROM job_attempt_artifact_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "SELECT COUNT(*) FROM job_attempts WHERE job_id IN (SELECT id FROM del_jobs)",
    "SELECT COUNT(*) FROM jobs WHERE id IN (SELECT id FROM del_jobs)",
    "SELECT COUNT(*) FROM plan_policy_revisions WHERE plan_id IN (SELECT id FROM del_plans)",
    "SELECT COUNT(*) FROM plan_datasets WHERE plan_id IN (SELECT id FROM del_plans)",
    "SELECT COUNT(*) FROM plans WHERE id IN (SELECT id FROM del_plans)",
    "SELECT COUNT(*) FROM quality_issue_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM quality_issues WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM dataset_observation_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM dataset_observation_constituents WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM dataset_raw_objects WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM dataset_members WHERE dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM dataset_derivations WHERE derived_dataset_id IN (SELECT id FROM del_datasets) OR source_dataset_id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM datasets WHERE id IN (SELECT id FROM del_datasets)",
    "SELECT COUNT(*) FROM collection_page_members WHERE request_id IN (SELECT id FROM del_requests)",
    "SELECT COUNT(*) FROM collection_pages WHERE request_id IN (SELECT id FROM del_requests)",
    "SELECT COUNT(*) FROM collection_raw_objects WHERE request_id IN (SELECT id FROM del_requests)",
    "SELECT COUNT(*) FROM collections WHERE request_id IN (SELECT id FROM del_requests)",
    "SELECT COUNT(*) FROM evidence_snapshot_members WHERE snapshot_id IN (SELECT id FROM del_snapshots)",
    "SELECT COUNT(*) FROM evidence_snapshots WHERE id IN (SELECT id FROM del_snapshots)",
    "SELECT COUNT(*) FROM policy_revisions WHERE policy_id IN (SELECT id FROM del_policies)",
    "SELECT COUNT(*) FROM policies WHERE policy_id IN (SELECT id FROM del_policies)",
    "SELECT COUNT(*) FROM observation_raw_objects WHERE observation_id IN (SELECT id FROM del_obs)",
    "SELECT COUNT(*) FROM observation_constituents WHERE observation_id IN (SELECT id FROM del_obs)",
    "SELECT COUNT(*) FROM candle_observations WHERE id IN (SELECT id FROM del_obs)",
    "SELECT COUNT(*) FROM raw_objects WHERE id IN (SELECT id FROM del_raws)",
];

const SCOPE_TABLE_SQL: &str = "\
DROP TABLE IF EXISTS temp.del_policies;
CREATE TEMP TABLE del_policies(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_revisions;
CREATE TEMP TABLE del_revisions(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_plans;
CREATE TEMP TABLE del_plans(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_jobs;
CREATE TEMP TABLE del_jobs(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_attempts;
CREATE TEMP TABLE del_attempts(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_runs;
CREATE TEMP TABLE del_runs(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_datasets;
CREATE TEMP TABLE del_datasets(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_snapshots;
CREATE TEMP TABLE del_snapshots(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_artifacts;
CREATE TEMP TABLE del_artifacts(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_requests;
CREATE TEMP TABLE del_requests(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_obs;
CREATE TEMP TABLE del_obs(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_raws;
CREATE TEMP TABLE del_raws(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_suites;
CREATE TEMP TABLE del_suites(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_suite_cases;
CREATE TEMP TABLE del_suite_cases(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_schedules;
CREATE TEMP TABLE del_schedules(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_schedule_fires;
CREATE TEMP TABLE del_schedule_fires(id TEXT PRIMARY KEY);
DROP TABLE IF EXISTS temp.del_backups;
CREATE TEMP TABLE del_backups(id TEXT PRIMARY KEY);";

/// Closure-expansion edges; each `INSERT..SELECT` returns newly added rows.
const SCOPE_EDGES: &[&str] = &[
    // automation parents own links only; ordinary linked resources survive
    "INSERT OR IGNORE INTO del_suite_cases SELECT id FROM research_suite_cases WHERE suite_id IN (SELECT id FROM del_suites)",
    "INSERT OR IGNORE INTO del_schedule_fires SELECT id FROM schedule_fires WHERE schedule_id IN (SELECT id FROM del_schedules)",
    // runs -> owning jobs; plans -> their runs
    "INSERT OR IGNORE INTO del_runs SELECT id FROM runs WHERE plan_id IN (SELECT id FROM del_plans)",
    "INSERT OR IGNORE INTO del_jobs SELECT job_id FROM runs WHERE id IN (SELECT id FROM del_runs)",
    // jobs -> attempts -> outputs
    "INSERT OR IGNORE INTO del_attempts SELECT id FROM job_attempts WHERE job_id IN (SELECT id FROM del_jobs)",
    "INSERT OR IGNORE INTO del_runs SELECT run_id FROM job_attempt_run_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "INSERT OR IGNORE INTO del_datasets SELECT dataset_id FROM job_attempt_dataset_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    "INSERT OR IGNORE INTO del_artifacts SELECT artifact_id FROM job_attempt_artifact_outputs WHERE attempt_id IN (SELECT id FROM del_attempts)",
    // dataset closure: derived children and their consumers
    "INSERT OR IGNORE INTO del_datasets SELECT derived_dataset_id FROM dataset_derivations WHERE source_dataset_id IN (SELECT id FROM del_datasets)",
    "INSERT OR IGNORE INTO del_plans SELECT plan_id FROM plan_datasets WHERE dataset_id IN (SELECT id FROM del_datasets)",
    // jobs whose collected/verified outputs are in scope
    "INSERT OR IGNORE INTO del_jobs SELECT a.job_id FROM job_attempts a JOIN job_attempt_dataset_outputs o ON o.attempt_id=a.id WHERE o.dataset_id IN (SELECT id FROM del_datasets)",
    "INSERT OR IGNORE INTO del_jobs SELECT a.job_id FROM job_attempts a JOIN job_attempt_artifact_outputs o ON o.attempt_id=a.id WHERE o.artifact_id IN (SELECT id FROM del_artifacts)",
    // artifacts of scope runs
    "INSERT OR IGNORE INTO del_artifacts SELECT id FROM run_artifacts WHERE run_id IN (SELECT id FROM del_runs)",
    // policy consumers
    "INSERT OR IGNORE INTO del_runs SELECT run_id FROM run_models WHERE policy_id IN (SELECT id FROM del_policies)",
    "INSERT OR IGNORE INTO del_plans SELECT plan_id FROM plan_policy_revisions WHERE policy_id IN (SELECT id FROM del_policies)",
    // evidence snapshot consumers
    "INSERT OR IGNORE INTO del_plans SELECT id FROM plans WHERE evidence_snapshot_id IN (SELECT id FROM del_snapshots)",
];
