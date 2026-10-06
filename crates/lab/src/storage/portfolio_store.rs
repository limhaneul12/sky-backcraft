//! Durable shared-capital portfolio runs: one immutable spec row plus
//! append-only fact rows published atomically at completion.

use super::{Store, json_error, sql_error, timestamp_from_ms, timestamp_ms};
use crate::contracts::{
    LabError, PortfolioFactKind, PortfolioLedger, PortfolioRunRequest, RunId, UtcTimestamp,
};
use rusqlite::{OptionalExtension, params};

pub(crate) const MAX_PORTFOLIO_FACT_PAGE: u32 = 500;

/// Summary projection of one published portfolio run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PortfolioRunSummary {
    pub run_id: RunId,
    pub request_id: String,
    pub plan_id: String,
    pub input_digest: String,
    pub status: String,
    pub created_at: UtcTimestamp,
    pub completed_at: Option<UtcTimestamp>,
    pub totals: serde_json::Value,
    pub benchmarks: serde_json::Value,
    pub error: Option<String>,
    pub fact_count: u64,
}

impl Store {
    /// Publish one completed portfolio ledger atomically with its facts.
    ///
    /// # Errors
    /// Rejects duplicate run ids, oversized payloads or SQLite failure.
    pub fn publish_portfolio_run(
        &mut self,
        request: &PortfolioRunRequest,
        ledger: &PortfolioLedger,
        benchmarks: &serde_json::Value,
    ) -> Result<(), LabError> {
        let totals = serde_json::to_value(&ledger.totals).map_err(json_error)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        // Idempotent publication: the same (request, run) pair is a no-op, a
        // reused request id under a different run is a conflict. This keeps
        // job retries safe without multiplying run rows.
        if let Some((existing_request, existing_run)) = transaction
            .query_row(
                "SELECT request_id,id FROM portfolio_runs WHERE id=?1 OR request_id=?2 LIMIT 1",
                params![ledger.run_id.as_str(), request.request_id.as_str()],
                |row| -> rusqlite::Result<(String, String)> { Ok((row.get(0)?, row.get(1)?)) },
            )
            .optional()
            .map_err(sql_error)?
        {
            let same = existing_request == request.request_id.as_str()
                && existing_run == ledger.run_id.as_str();
            return if same {
                Ok(())
            } else {
                Err(LabError::Conflict(
                    "portfolio request id already published for another run".into(),
                ))
            };
        }
        transaction
            .execute(
                "INSERT INTO portfolio_runs(id,request_id,plan_id,input_digest,spec_json,status,\
                 created_at_ms,completed_at_ms,error_json,totals_json,benchmarks_json) \
                 VALUES (?1,?2,?3,?4,?5,'completed',?6,?6,NULL,?7,?8)",
                params![
                    ledger.run_id.as_str(),
                    request.request_id.as_str(),
                    request.plan_id.as_str(),
                    request.input_digest.as_str(),
                    // The full request freezes portfolio AND regime gate for
                    // byte-level reproducibility of the run inputs.
                    serde_json::to_string(request).map_err(json_error)?,
                    timestamp_ms(UtcTimestamp::now()),
                    serde_json::to_value(totals)
                        .map_err(json_error)?
                        .to_string(),
                    serde_json::to_string(benchmarks).map_err(json_error)?,
                ],
            )
            .map_err(sql_error)?;
        for fact in portfolio_fact_rows(ledger)? {
            transaction
                .execute(
                    "INSERT INTO portfolio_facts(run_id,event_seq,kind,fact_json) \
                     VALUES (?1,?2,?3,?4)",
                    params![ledger.run_id.as_str(), fact.0, fact.1, fact.2],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(())
    }

    /// Bounded portfolio run summary without fact bodies.
    ///
    /// # Errors
    /// Rejects unknown runs, corrupt rows or SQLite failure.
    pub fn portfolio_run_summary(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PortfolioRunSummary>, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT request_id,plan_id,input_digest,status,created_at_ms,completed_at_ms,\
                 totals_json,benchmarks_json,error_json \
                 FROM portfolio_runs WHERE id=?1",
                [run_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let Some((
            request_id,
            plan_id,
            input_digest,
            status,
            created,
            completed,
            totals,
            benchmarks,
            error,
        )) = row
        else {
            return Ok(None);
        };
        let fact_count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM portfolio_facts WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        Ok(Some(PortfolioRunSummary {
            run_id: run_id.clone(),
            request_id,
            plan_id,
            input_digest,
            status,
            created_at: timestamp_from_ms(created)?,
            completed_at: completed.map(timestamp_from_ms).transpose()?,
            totals: totals
                .map(|text| serde_json::from_str(&text).map_err(json_error))
                .transpose()?
                .unwrap_or(serde_json::Value::Null),
            benchmarks: benchmarks
                .map(|text| serde_json::from_str(&text).map_err(json_error))
                .transpose()?
                .unwrap_or(serde_json::Value::Null),
            error,
            fact_count: u64::try_from(fact_count)
                .map_err(|_| LabError::DataCorrupt("negative portfolio fact count".into()))?,
        }))
    }

    /// Page one fact kind of one run in event order.
    ///
    /// # Errors
    /// Rejects invalid pages, unknown kinds or SQLite failure.
    pub fn portfolio_facts(
        &self,
        run_id: &RunId,
        kind: PortfolioFactKind,
        offset: u64,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, LabError> {
        if !(1..=MAX_PORTFOLIO_FACT_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "portfolio fact page must be in 1..={MAX_PORTFOLIO_FACT_PAGE}"
            )));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT fact_json FROM portfolio_facts \
                 WHERE run_id=?1 AND kind=?2 ORDER BY event_seq LIMIT ?3 OFFSET ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    run_id.as_str(),
                    kind.as_str(),
                    i64::from(limit),
                    i64::try_from(offset)
                        .map_err(|_| LabError::InvalidConfig("fact offset overflow".into()))?
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut facts = Vec::new();
        for row in rows {
            let text = row.map_err(sql_error)?;
            facts.push(serde_json::from_str(&text).map_err(json_error)?);
        }
        Ok(facts)
    }

    /// List recent portfolio runs (newest first), bounded page.
    ///
    /// # Errors
    /// Rejects invalid pages or SQLite failure.
    pub fn list_portfolio_runs(&self, offset: u64, limit: u32) -> Result<Vec<String>, LabError> {
        if !(1..=MAX_PORTFOLIO_FACT_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "portfolio list page must be in 1..={MAX_PORTFOLIO_FACT_PAGE}"
            )));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT id FROM portfolio_runs ORDER BY created_at_ms DESC, id LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    i64::from(limit),
                    i64::try_from(offset)
                        .map_err(|_| LabError::InvalidConfig("list offset overflow".into()))?
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(sql_error)?);
        }
        Ok(ids)
    }
}

/// Flatten one ledger into (seq, kind, json) rows in event order.
fn portfolio_fact_rows(
    ledger: &PortfolioLedger,
) -> Result<Vec<(i64, &'static str, String)>, LabError> {
    let mut rows = Vec::new();
    for intent in &ledger.intents {
        rows.push((
            i64::try_from(intent.event_seq)
                .map_err(|_| LabError::DataCorrupt("intent seq overflow".into()))?,
            "intent",
            serde_json::to_string(intent).map_err(json_error)?,
        ));
    }
    for fill in &ledger.fills {
        rows.push((
            i64::try_from(fill.event_seq)
                .map_err(|_| LabError::DataCorrupt("fill seq overflow".into()))?,
            "fill",
            serde_json::to_string(fill).map_err(json_error)?,
        ));
    }
    for rejection in &ledger.rejections {
        rows.push((
            i64::try_from(rejection.event_seq)
                .map_err(|_| LabError::DataCorrupt("rejection seq overflow".into()))?,
            "rejection",
            serde_json::to_string(rejection).map_err(json_error)?,
        ));
    }
    for mark in &ledger.marks {
        rows.push((
            i64::try_from(mark.event_seq)
                .map_err(|_| LabError::DataCorrupt("mark seq overflow".into()))?,
            "mark",
            serde_json::to_string(mark).map_err(json_error)?,
        ));
    }
    rows.sort_by_key(|(seq, _, _)| *seq);
    Ok(rows)
}
