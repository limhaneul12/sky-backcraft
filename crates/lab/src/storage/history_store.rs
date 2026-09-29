use super::{Store, json_error, sql_error, timestamp_from_ms, timestamp_ms};
use crate::contracts::{
    AttemptId, ContentHash, HistoryJobKind, HistoryPage, JobHistoryCursor, JobHistoryEntry, JobId,
    LabError, PolicyRevisionId, PolicyRevisionRef, RunHistoryCursor, RunHistoryEntry, RunId,
};
use rusqlite::params;

const MAX_HISTORY_LIMIT: u32 = 100;

impl Store {
    /// List runs newest-first using a stable time/id cursor.
    ///
    /// # Errors
    /// Rejects invalid bounds/cursors or corrupt SQLite state.
    pub fn run_history(
        &self,
        cursor: Option<&RunHistoryCursor>,
        limit: u32,
    ) -> Result<HistoryPage<RunHistoryEntry, RunHistoryCursor>, LabError> {
        self.run_history_filtered(None, cursor, limit)
    }

    /// List jobs newest-first using a stable time/id cursor.
    ///
    /// # Errors
    /// Rejects invalid bounds/cursors or corrupt SQLite state.
    pub fn job_history(
        &self,
        cursor: Option<&JobHistoryCursor>,
        limit: u32,
    ) -> Result<HistoryPage<JobHistoryEntry, JobHistoryCursor>, LabError> {
        validate_limit(limit)?;
        let total = count(&self.connection, "SELECT COUNT(*) FROM jobs", [])?;
        let (cursor_time, cursor_id) = cursor.map_or((None, None), |value| {
            (
                Some(timestamp_ms(value.created_at)),
                Some(value.job_id.as_str()),
            )
        });
        let mut statement = self.connection.prepare(
            "SELECT j.id,j.request_id,j.created_at_ms,j.payload_kind,j.current_status,a.id,j.current_attempt_number FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_number=j.current_attempt_number WHERE (?1 IS NULL OR j.created_at_ms<?1 OR (j.created_at_ms=?1 AND j.id<?2)) ORDER BY j.created_at_ms DESC,j.id DESC LIMIT ?3",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![cursor_time, cursor_id, i64::from(limit) + 1],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, u32>(6)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut items = Vec::new();
        for row in rows {
            let row = row.map_err(sql_error)?;
            items.push(JobHistoryEntry {
                job_id: JobId::new(row.0)?,
                request_id: crate::contracts::RequestId::new(row.1)?,
                created_at: timestamp_from_ms(row.2)?,
                kind: job_kind(&row.3)?,
                status: enum_from_text(&row.4)?,
                attempt_count: row.6,
                current_attempt_id: AttemptId::new(row.5)?,
            });
        }
        history_page(items, limit, total, |item| JobHistoryCursor {
            created_at: item.created_at,
            job_id: item.job_id.clone(),
        })
    }

    /// List runs that used one exact frozen policy revision.
    ///
    /// # Errors
    /// Rejects invalid references/bounds or corrupt SQLite state.
    pub fn policy_runs(
        &self,
        reference: &PolicyRevisionRef,
        cursor: Option<&RunHistoryCursor>,
        limit: u32,
    ) -> Result<HistoryPage<RunHistoryEntry, RunHistoryCursor>, LabError> {
        if self.load_policy_revision(reference)?.is_none() {
            return Err(LabError::InvalidConfig("unknown policy revision".into()));
        }
        self.run_history_filtered(Some(reference), cursor, limit)
    }

    fn run_history_filtered(
        &self,
        policy: Option<&PolicyRevisionRef>,
        cursor: Option<&RunHistoryCursor>,
        limit: u32,
    ) -> Result<HistoryPage<RunHistoryEntry, RunHistoryCursor>, LabError> {
        validate_limit(limit)?;
        let query_filter = policy.map_or(String::new(), |_| {
            " AND EXISTS (SELECT 1 FROM run_models m WHERE m.run_id=r.id AND m.policy_id=?3 AND m.policy_revision_id=?4 AND m.policy_definition_digest=?5)".into()
        });
        let total = if let Some(reference) = policy {
            count(
                &self.connection,
                "SELECT COUNT(*) FROM runs r WHERE EXISTS (SELECT 1 FROM run_models m WHERE m.run_id=r.id AND m.policy_id=?1 AND m.policy_revision_id=?2 AND m.policy_definition_digest=?3)",
                params![
                    reference.policy_id.as_str(),
                    reference.revision_id.as_str(),
                    reference.definition_digest.as_str()
                ],
            )?
        } else {
            count(&self.connection, "SELECT COUNT(*) FROM runs", [])?
        };
        let (cursor_time, cursor_id) = cursor.map_or((None, None), |value| {
            (
                Some(timestamp_ms(value.created_at)),
                Some(value.run_id.as_str()),
            )
        });
        let rows = if let Some(reference) = policy {
            let sql = format!(
                "SELECT r.id,r.job_id,r.attempt_id,r.plan_id,r.created_at_ms,r.state,p.input_digest,r.semantic_digest FROM runs r JOIN plans p ON p.id=r.plan_id WHERE (?1 IS NULL OR r.created_at_ms<?1 OR (r.created_at_ms=?1 AND r.id<?2)){query_filter} ORDER BY r.created_at_ms DESC,r.id DESC LIMIT ?6"
            );
            query_run_rows(
                &self.connection,
                &sql,
                params![
                    cursor_time,
                    cursor_id,
                    reference.policy_id.as_str(),
                    reference.revision_id.as_str(),
                    reference.definition_digest.as_str(),
                    i64::from(limit) + 1
                ],
            )?
        } else {
            query_run_rows(
                &self.connection,
                "SELECT r.id,r.job_id,r.attempt_id,r.plan_id,r.created_at_ms,r.state,p.input_digest,r.semantic_digest FROM runs r JOIN plans p ON p.id=r.plan_id WHERE (?1 IS NULL OR r.created_at_ms<?1 OR (r.created_at_ms=?1 AND r.id<?2)) ORDER BY r.created_at_ms DESC,r.id DESC LIMIT ?3",
                params![cursor_time, cursor_id, i64::from(limit) + 1],
            )?
        };
        let mut items = Vec::new();
        for row in rows {
            items.push(self.run_history_entry(row)?);
        }
        history_page(items, limit, total, |item| RunHistoryCursor {
            created_at: item.created_at,
            run_id: item.run_id.clone(),
        })
    }

    fn run_history_entry(&self, row: RunRow) -> Result<RunHistoryEntry, LabError> {
        let run_id = RunId::new(row.id)?;
        let counts = self.connection.query_row(
            "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status IN ('BLOCKED_EVIDENCE','BLOCKED_DATA','SKIPPED') THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END),0) FROM run_models WHERE run_id=?1",
            [run_id.as_str()],
            |row| Ok((row.get::<_, i64>(0)?,row.get::<_, i64>(1)?,row.get::<_, i64>(2)?,row.get::<_, i64>(3)?)),
        ).map_err(sql_error)?;
        let mut statement = self.connection.prepare(
            "SELECT policy_id,revision_id,definition_digest FROM plan_policy_revisions WHERE plan_id=?1 ORDER BY position",
        ).map_err(sql_error)?;
        let policies = statement
            .query_map([&row.plan_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?
            .map(|row| {
                let row = row.map_err(sql_error)?;
                Ok(PolicyRevisionRef {
                    policy_id: crate::contracts::PolicyId::new(row.0)?,
                    revision_id: PolicyRevisionId::new(row.1)?,
                    definition_digest: ContentHash::try_from(row.2)?,
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        Ok(RunHistoryEntry {
            run_id,
            job_id: JobId::new(row.job_id)?,
            attempt_id: AttemptId::new(row.attempt_id)?,
            plan_id: crate::contracts::PlanId::new(row.plan_id)?,
            created_at: timestamp_from_ms(row.created_at_ms)?,
            status: enum_from_text(&row.status)?,
            input_digest: ContentHash::try_from(row.input_digest)?,
            semantic_digest: row.semantic_digest.map(ContentHash::try_from).transpose()?,
            completed_models: nonnegative(counts.1)?,
            blocked_models: nonnegative(counts.2)?,
            failed_models: nonnegative(counts.3)?,
            model_count: nonnegative(counts.0)?,
            policy_revisions: policies,
        })
    }
}

struct RunRow {
    id: String,
    job_id: String,
    attempt_id: String,
    plan_id: String,
    created_at_ms: i64,
    status: String,
    input_digest: String,
    semantic_digest: Option<String>,
}

fn query_run_rows<P: rusqlite::Params>(
    connection: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<Vec<RunRow>, LabError> {
    let mut statement = connection.prepare(sql).map_err(sql_error)?;
    statement
        .query_map(params, |row| {
            Ok(RunRow {
                id: row.get(0)?,
                job_id: row.get(1)?,
                attempt_id: row.get(2)?,
                plan_id: row.get(3)?,
                created_at_ms: row.get(4)?,
                status: row.get(5)?,
                input_digest: row.get(6)?,
                semantic_digest: row.get(7)?,
            })
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)
}

fn history_page<T, C>(
    mut items: Vec<T>,
    limit: u32,
    total_count: u64,
    cursor: impl Fn(&T) -> C,
) -> Result<HistoryPage<T, C>, LabError> {
    let limit = usize::try_from(limit)
        .map_err(|_| LabError::ResourceLimit("history limit exceeds platform capacity".into()))?;
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    let next_cursor = if has_more {
        items.last().map(cursor)
    } else {
        None
    };
    Ok(HistoryPage {
        returned_count: u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("history count overflow".into()))?,
        items,
        total_count,
        next_cursor,
        truncated_reason: has_more.then(|| "LIMIT".into()),
    })
}

fn validate_limit(limit: u32) -> Result<(), LabError> {
    if !(1..=MAX_HISTORY_LIMIT).contains(&limit) {
        return Err(LabError::ResourceLimit(
            "history limit must be 1..=100".into(),
        ));
    }
    Ok(())
}

fn count<P: rusqlite::Params>(
    connection: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<u64, LabError> {
    let value: i64 = connection
        .query_row(sql, params, |row| row.get(0))
        .map_err(sql_error)?;
    nonnegative(value)
}

fn nonnegative(value: i64) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt("negative history count".into()))
}

fn enum_from_text<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, LabError> {
    serde_json::from_value(serde_json::Value::String(text.into())).map_err(json_error)
}

fn job_kind(value: &str) -> Result<HistoryJobKind, LabError> {
    match value {
        "COLLECT" => Ok(HistoryJobKind::Collect),
        "BACKTEST" => Ok(HistoryJobKind::Backtest),
        "EXPORT" => Ok(HistoryJobKind::Export),
        "VERIFY" => Ok(HistoryJobKind::Verify),
        _ => Err(LabError::DataCorrupt(format!("unknown job kind {value}"))),
    }
}
