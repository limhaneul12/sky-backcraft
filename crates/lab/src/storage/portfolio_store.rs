//! Durable shared-capital portfolio runs: one immutable spec row plus
//! append-only fact rows published atomically at completion.

use super::{Store, json_error, sql_error, timestamp_from_ms, timestamp_ms};
use crate::contracts::{
    ContentHash, LabError, PortfolioAllocationPoint, PortfolioContributions, PortfolioEquityPoint,
    PortfolioFactKind, PortfolioLedger, PortfolioProjection, PortfolioProjectionPage,
    PortfolioProjectionUnavailableReason, PortfolioRebalancePoint, PortfolioRegimeResult,
    PortfolioRegimeTimelinePoint, PortfolioResultSummary, PortfolioRunRequest, RunId, UtcTimestamp,
};
use rusqlite::{OptionalExtension, params};
use serde::de::DeserializeOwned;

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

struct PreparedPortfolioPublication {
    request_json: String,
    ledger_digest: ContentHash,
    request_digest: ContentHash,
    benchmarks_json: String,
    benchmarks_digest: ContentHash,
    totals_json: String,
    projections: crate::reporting::portfolio_projection::PortfolioProjectionSet,
    regime_timeline: Vec<PortfolioRegimeTimelinePoint>,
    regime_summary: Option<PortfolioRegimeResult>,
    completed_at: UtcTimestamp,
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
        validate_ledger_request(request, ledger)?;
        let prepared = self.prepare_portfolio_publication(request, ledger, benchmarks)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        if let Some(same_payload) =
            existing_publication_matches(&transaction, request, ledger, &prepared)?
        {
            return if same_payload {
                Ok(())
            } else {
                Err(LabError::Conflict(
                    "portfolio publication identity already exists with different or unprovable content".into(),
                ))
            };
        }
        insert_portfolio_run(&transaction, request, ledger, &prepared)?;
        for fact in portfolio_fact_rows(ledger)? {
            transaction
                .execute(
                    "INSERT INTO portfolio_facts(run_id,event_seq,kind,fact_json) \
                     VALUES (?1,?2,?3,?4)",
                    params![ledger.run_id.as_str(), fact.0, fact.1, fact.2],
                )
                .map_err(sql_error)?;
        }
        insert_projection_meta(&transaction, request, ledger, &prepared)?;
        insert_projection_rows(
            &transaction,
            &ledger.run_id,
            "equity",
            &prepared.projections.equity,
        )?;
        insert_projection_rows(
            &transaction,
            &ledger.run_id,
            "allocation",
            &prepared.projections.allocations,
        )?;
        insert_projection_rows(
            &transaction,
            &ledger.run_id,
            "rebalance",
            &prepared.projections.rebalances,
        )?;
        insert_projection_rows(
            &transaction,
            &ledger.run_id,
            "regime",
            &prepared.regime_timeline,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(())
    }

    fn prepare_portfolio_publication(
        &self,
        request: &PortfolioRunRequest,
        ledger: &PortfolioLedger,
        benchmarks: &serde_json::Value,
    ) -> Result<PreparedPortfolioPublication, LabError> {
        let stored_plan = self
            .load_plan(&request.plan_id)?
            .ok_or_else(|| LabError::InvalidConfig("portfolio plan is missing".into()))?;
        if stored_plan.resolved.input_digest != request.input_digest {
            return Err(LabError::InputHashMismatch(
                "portfolio request input digest differs from frozen plan".into(),
            ));
        }
        let projections =
            crate::reporting::portfolio_projection::build(ledger, &stored_plan.resolved)?;
        let regime_timeline = crate::reporting::regime_projection::timeline(ledger);
        let regime_summary = request
            .regime
            .as_ref()
            .map(|_| crate::reporting::regime_projection::summary(ledger))
            .transpose()?;
        let request_json = serde_json::to_string(request).map_err(json_error)?;
        let ledger_json = serde_json::to_string(ledger).map_err(json_error)?;
        let benchmarks_json = serde_json::to_string(benchmarks).map_err(json_error)?;
        Ok(PreparedPortfolioPublication {
            request_digest: ContentHash::of_bytes(request_json.as_bytes()),
            ledger_digest: ContentHash::of_bytes(ledger_json.as_bytes()),
            benchmarks_digest: ContentHash::of_bytes(benchmarks_json.as_bytes()),
            totals_json: serde_json::to_string(&ledger.totals).map_err(json_error)?,
            request_json,
            benchmarks_json,
            projections,
            regime_timeline,
            regime_summary,
            completed_at: UtcTimestamp::now(),
        })
    }

    /// Exact shared-pool summary, or explicit legacy unavailability.
    ///
    /// # Errors
    /// Returns stored-contract, JSON or SQLite failures.
    pub fn portfolio_summary_projection(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PortfolioProjection<PortfolioResultSummary>>, LabError> {
        self.projection_meta(run_id, "summary_json")
    }

    /// Bounded equity page in mark order.
    ///
    /// # Errors
    /// Rejects invalid page bounds and returns stored-contract or SQLite failures.
    pub fn portfolio_equity(
        &self,
        run_id: &RunId,
        offset: u64,
        limit: u32,
    ) -> Result<Option<PortfolioProjection<PortfolioProjectionPage<PortfolioEquityPoint>>>, LabError>
    {
        self.projection_page(run_id, "equity", offset, limit)
    }

    /// Bounded allocation page in mark order.
    ///
    /// # Errors
    /// Rejects invalid page bounds and returns stored-contract or SQLite failures.
    pub fn portfolio_allocations(
        &self,
        run_id: &RunId,
        offset: u64,
        limit: u32,
    ) -> Result<
        Option<PortfolioProjection<PortfolioProjectionPage<PortfolioAllocationPoint>>>,
        LabError,
    > {
        self.projection_page(run_id, "allocation", offset, limit)
    }

    /// Bounded actual rebalance page in execution order.
    ///
    /// # Errors
    /// Rejects invalid page bounds and returns stored-contract or SQLite failures.
    pub fn portfolio_rebalances(
        &self,
        run_id: &RunId,
        offset: u64,
        limit: u32,
    ) -> Result<
        Option<PortfolioProjection<PortfolioProjectionPage<PortfolioRebalancePoint>>>,
        LabError,
    > {
        self.projection_page(run_id, "rebalance", offset, limit)
    }

    /// Exact market/policy attribution and reconciliation.
    ///
    /// # Errors
    /// Returns stored-contract, JSON or SQLite failures.
    pub fn portfolio_contributions(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PortfolioProjection<PortfolioContributions>>, LabError> {
        self.projection_meta(run_id, "contributions_json")
    }

    /// Bounded causal regime timeline.
    ///
    /// # Errors
    /// Rejects invalid page bounds and returns stored-contract or SQLite failures.
    pub fn portfolio_regime_timeline(
        &self,
        run_id: &RunId,
        offset: u64,
        limit: u32,
    ) -> Result<
        Option<PortfolioProjection<PortfolioProjectionPage<PortfolioRegimeTimelinePoint>>>,
        LabError,
    > {
        if !self.portfolio_run_exists(run_id)? {
            return Ok(None);
        }
        if let Some(unavailable) = self.regime_unavailable(run_id)? {
            return Ok(Some(PortfolioProjection::Unavailable {
                reason: unavailable,
            }));
        }
        self.projection_page(run_id, "regime", offset, limit)
    }

    /// Whole-run regime metrics and transition counts.
    ///
    /// # Errors
    /// Returns stored-contract, JSON or SQLite failures.
    pub fn portfolio_regime_summary(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PortfolioProjection<PortfolioRegimeResult>>, LabError> {
        if !self.portfolio_run_exists(run_id)? {
            return Ok(None);
        }
        if let Some(unavailable) = self.regime_unavailable(run_id)? {
            return Ok(Some(PortfolioProjection::Unavailable {
                reason: unavailable,
            }));
        }
        self.projection_meta(run_id, "regime_summary_json")
    }

    fn projection_meta<T: DeserializeOwned>(
        &self,
        run_id: &RunId,
        column: &'static str,
    ) -> Result<Option<PortfolioProjection<T>>, LabError> {
        if !self.portfolio_run_exists(run_id)? {
            return Ok(None);
        }
        let sql = format!("SELECT {column} FROM portfolio_projection_meta WHERE run_id=?1");
        let json = self
            .connection
            .query_row(&sql, [run_id.as_str()], |row| row.get::<_, String>(0))
            .optional()
            .map_err(sql_error)?;
        Ok(Some(match json {
            Some(json) => PortfolioProjection::Available {
                data: serde_json::from_str(&json).map_err(json_error)?,
            },
            None => PortfolioProjection::Unavailable {
                reason: PortfolioProjectionUnavailableReason::LegacyProjectionMissing,
            },
        }))
    }

    fn projection_page<T: DeserializeOwned>(
        &self,
        run_id: &RunId,
        kind: &'static str,
        offset: u64,
        limit: u32,
    ) -> Result<Option<PortfolioProjection<PortfolioProjectionPage<T>>>, LabError> {
        validate_projection_page(offset, limit)?;
        if !self.portfolio_run_exists(run_id)? {
            return Ok(None);
        }
        let projected: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM portfolio_projection_meta WHERE run_id=?1)",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !projected {
            return Ok(Some(PortfolioProjection::Unavailable {
                reason: PortfolioProjectionUnavailableReason::LegacyProjectionMissing,
            }));
        }
        let total: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM portfolio_projection_rows WHERE run_id=?1 AND kind=?2",
                params![run_id.as_str(), kind],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT row_json FROM portfolio_projection_rows \
                 WHERE run_id=?1 AND kind=?2 ORDER BY ordinal LIMIT ?3 OFFSET ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    run_id.as_str(),
                    kind,
                    i64::from(limit),
                    i64::try_from(offset).map_err(|_| LabError::InvalidConfig(
                        "projection offset overflow".into()
                    ))?
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?);
        }
        let total_count = u64::try_from(total)
            .map_err(|_| LabError::DataCorrupt("negative projection count".into()))?;
        let returned = u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("projection page overflow".into()))?;
        let next = offset
            .checked_add(returned)
            .ok_or_else(|| LabError::ResourceLimit("projection page offset overflow".into()))?;
        Ok(Some(PortfolioProjection::Available {
            data: PortfolioProjectionPage {
                items,
                total_count,
                next_offset: (next < total_count).then_some(next),
            },
        }))
    }

    fn portfolio_run_exists(&self, run_id: &RunId) -> Result<bool, LabError> {
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM portfolio_runs WHERE id=?1)",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    fn regime_unavailable(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PortfolioProjectionUnavailableReason>, LabError> {
        let configured = self
            .connection
            .query_row(
                "SELECT regime_configured FROM portfolio_projection_meta WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(match configured {
            None => Some(PortfolioProjectionUnavailableReason::LegacyProjectionMissing),
            Some(false) => Some(PortfolioProjectionUnavailableReason::RegimeNotConfigured),
            Some(true) => None,
        })
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

fn existing_publication_matches(
    transaction: &rusqlite::Transaction<'_>,
    request: &PortfolioRunRequest,
    ledger: &PortfolioLedger,
    prepared: &PreparedPortfolioPublication,
) -> Result<Option<bool>, LabError> {
    let existing = transaction
        .query_row(
            "SELECT r.request_id,r.id,m.request_digest,m.ledger_digest,m.benchmarks_digest \
             FROM portfolio_runs r LEFT JOIN portfolio_projection_meta m ON m.run_id=r.id \
             WHERE r.id=?1 OR r.request_id=?2 LIMIT 1",
            params![ledger.run_id.as_str(), request.request_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    Ok(existing.map(
        |(existing_request, existing_run, request_digest, ledger_digest, benchmarks_digest)| {
            existing_request == request.request_id.as_str()
                && existing_run == ledger.run_id.as_str()
                && request_digest.as_deref() == Some(prepared.request_digest.as_str())
                && ledger_digest.as_deref() == Some(prepared.ledger_digest.as_str())
                && benchmarks_digest.as_deref() == Some(prepared.benchmarks_digest.as_str())
        },
    ))
}

fn insert_portfolio_run(
    transaction: &rusqlite::Transaction<'_>,
    request: &PortfolioRunRequest,
    ledger: &PortfolioLedger,
    prepared: &PreparedPortfolioPublication,
) -> Result<(), LabError> {
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
                &prepared.request_json,
                timestamp_ms(prepared.completed_at),
                &prepared.totals_json,
                &prepared.benchmarks_json,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn insert_projection_meta(
    transaction: &rusqlite::Transaction<'_>,
    request: &PortfolioRunRequest,
    ledger: &PortfolioLedger,
    prepared: &PreparedPortfolioPublication,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO portfolio_projection_meta(\
             run_id,request_digest,ledger_digest,benchmarks_digest,summary_json,\
             contributions_json,regime_configured,regime_summary_json) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                ledger.run_id.as_str(),
                prepared.request_digest.as_str(),
                prepared.ledger_digest.as_str(),
                prepared.benchmarks_digest.as_str(),
                serde_json::to_string(&prepared.projections.summary).map_err(json_error)?,
                serde_json::to_string(&prepared.projections.contributions).map_err(json_error)?,
                i64::from(request.regime.is_some()),
                prepared
                    .regime_summary
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(json_error)?,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn validate_ledger_request(
    request: &PortfolioRunRequest,
    ledger: &PortfolioLedger,
) -> Result<(), LabError> {
    if serde_json::to_vec(&request.portfolio).map_err(json_error)?
        != serde_json::to_vec(&ledger.spec).map_err(json_error)?
    {
        return Err(LabError::InputHashMismatch(
            "portfolio ledger spec differs from request".into(),
        ));
    }
    Ok(())
}

fn validate_projection_page(offset: u64, limit: u32) -> Result<(), LabError> {
    if !(1..=crate::contracts::MAX_PORTFOLIO_PROJECTION_PAGE).contains(&limit) {
        return Err(LabError::InvalidConfig(format!(
            "portfolio projection page must be in 1..={}",
            crate::contracts::MAX_PORTFOLIO_PROJECTION_PAGE
        )));
    }
    let _ = i64::try_from(offset)
        .map_err(|_| LabError::InvalidConfig("projection offset overflow".into()))?;
    Ok(())
}

fn insert_projection_rows<T: serde::Serialize>(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    kind: &'static str,
    rows: &[T],
) -> Result<(), LabError> {
    for (ordinal, row) in rows.iter().enumerate() {
        transaction
            .execute(
                "INSERT INTO portfolio_projection_rows(run_id,kind,ordinal,row_json) \
                 VALUES (?1,?2,?3,?4)",
                params![
                    run_id.as_str(),
                    kind,
                    i64::try_from(ordinal).map_err(|_| {
                        LabError::ResourceLimit("projection ordinal overflow".into())
                    })?,
                    serde_json::to_string(row).map_err(json_error)?,
                ],
            )
            .map_err(sql_error)?;
    }
    Ok(())
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
