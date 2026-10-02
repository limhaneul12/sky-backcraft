//! Bounded read projections for MCP status and result inspection.

use super::ledger_seal::{RunLedgerSource, run_ledger_source};
use super::{Store, enum_text, sql_error};
use crate::contracts::{
    AccountMark, ContentHash, FillRecord, JobStatus, LabError, LedgerSection, MAX_MODEL_EVENTS,
    MAX_MODELS, MarkKind, MarketId, ModelId, ModelStatus, PlanId, PolicyId, PolicyRevisionId,
    PolicyRevisionRef, QuoteAmount, RunId, Side, SignedAmount, StrategyKind, ValidationReport,
};
use rusqlite::{OptionalExtension, params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const MAX_VALIDATION_REPORTS: usize = 4;
const MAX_VALIDATION_FINDINGS: usize = 16;
const MAX_PUBLIC_TEXT_CHARS: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobQueueStatus {
    pub queued_attempts: u32,
    pub running_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelResultSummary {
    pub model_id: ModelId,
    pub market: MarketId,
    pub strategy: StrategyKind,
    pub policy_ref: Option<PolicyRevisionRef>,
    pub status: Option<ModelStatus>,
    pub status_reason: Option<String>,
    pub last_event_seq: u64,
    pub signal_count: u64,
    pub order_event_count: u64,
    pub fill_count: u64,
    pub account_mark_count: u64,
    pub episode_count: u64,
    pub fact_digest: Option<ContentHash>,
    pub final_digest: Option<ContentHash>,
    pub initial_cash: QuoteAmount,
    pub terminal_equity: Option<QuoteAmount>,
    pub total_return_ratio: Option<SignedAmount>,
    pub total_fees: Option<QuoteAmount>,
    pub embedded_price_cost_quote: Option<SignedAmount>,
    pub closed_positions: Option<u64>,
    pub open_positions: Option<u64>,
    pub financial_null_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunResultSummary {
    pub run_id: RunId,
    pub job_id: crate::contracts::JobId,
    pub attempt_id: crate::contracts::AttemptId,
    pub plan_id: PlanId,
    pub state: JobStatus,
    pub input_digest: ContentHash,
    pub semantic_digest: Option<ContentHash>,
    pub policy_revisions: Vec<PolicyRevisionRef>,
    pub last_event_seq: u64,
    pub models: Vec<ModelResultSummary>,
    pub validations: Vec<ValidationResultSummary>,
    pub validation_truncated_reason: Option<String>,
    pub artifact_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidationResultSummary {
    pub check_id: String,
    pub status: crate::contracts::ValidationStatus,
    pub input_digest: ContentHash,
    pub checked_models: u64,
    pub checked_fills: u64,
    pub checked_marks: u64,
    pub finding_count: u64,
    pub findings: Vec<String>,
    pub truncated_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelCostSummary {
    pub run_id: RunId,
    pub model_id: ModelId,
    pub policy_ref: Option<PolicyRevisionRef>,
    pub status: Option<ModelStatus>,
    pub fill_count: u64,
    pub buy_notional: QuoteAmount,
    pub sell_notional: QuoteAmount,
    pub total_fees: QuoteAmount,
    /// Signed KRW amount already embedded in prices; never an extra cash debit.
    pub price_cost_attribution: SignedAmount,
    pub price_cost_attribution_unit: crate::contracts::PriceCostAttributionUnit,
    pub terminal_equity: Option<QuoteAmount>,
    pub terminal_mark_seq: Option<u64>,
}

impl Store {
    /// Return scalar durable queue state without exposing job payloads.
    ///
    /// # Errors
    /// Returns an error for corrupt counts or SQLite failure.
    pub fn job_queue_status(&self) -> Result<JobQueueStatus, LabError> {
        let (queued, running): (i64, i64) = self
            .connection
            .query_row(
                "SELECT COUNT(CASE WHEN status=?1 THEN 1 END),COUNT(CASE WHEN status=?2 THEN 1 END) FROM job_attempts WHERE status IN (?1,?2)",
                params![enum_text(&JobStatus::Queued)?, enum_text(&JobStatus::Running)?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sql_error)?;
        Ok(JobQueueStatus {
            queued_attempts: u32::try_from(nonnegative_u64(queued, "queued job count")?)
                .map_err(|_| LabError::DataCorrupt("queued job count exceeds u32".into()))?,
            running_attempts: u32::try_from(nonnegative_u64(running, "running job count")?)
                .map_err(|_| LabError::DataCorrupt("running job count exceeds u32".into()))?,
        })
    }

    /// Load bounded run/model status, independent validation, and artifact-count projection.
    ///
    /// # Errors
    /// Returns an error for corrupt projections, bound excess, or SQLite failure.
    pub fn load_run_summary(&self, run_id: &RunId) -> Result<Option<RunResultSummary>, LabError> {
        let Some(header) = self.load_run_header(run_id)? else {
            return Ok(None);
        };
        let plan = self
            .load_plan(&header.plan_id)?
            .ok_or_else(|| LabError::DataCorrupt("run summary plan is missing".into()))?;
        let ledger_source = run_ledger_source(self, run_id)?;
        let mut models = query_models(self, run_id, plan.resolved.spec.initial_cash)?;
        for model in &mut models {
            self.complete_model_summary(run_id, model, &ledger_source)?;
        }
        if models.len() > MAX_MODELS || models.len() != plan.resolved.admissions.len() {
            return Err(LabError::DataCorrupt(
                "run summary model catalog differs from the frozen plan".into(),
            ));
        }
        let (validations, validation_truncated_reason) = query_validations(self, run_id)?;
        let artifact_count = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM run_artifacts WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_error)
            .and_then(|value| nonnegative_u64(value, "artifact count"))?;
        Ok(Some(RunResultSummary {
            run_id: run_id.clone(),
            job_id: header.manifest.job_id,
            attempt_id: header.manifest.attempt_id,
            plan_id: header.plan_id,
            state: header.state,
            input_digest: plan.resolved.input_digest,
            semantic_digest: header.semantic_digest,
            policy_revisions: plan
                .resolved
                .policy_revisions
                .into_iter()
                .map(|revision| revision.reference)
                .collect(),
            last_event_seq: header.last_committed_event_seq,
            models,
            validations,
            validation_truncated_reason,
            artifact_count,
        }))
    }

    /// Fold exact persisted fill costs for one model without SQL decimal arithmetic.
    ///
    /// # Errors
    /// Returns an error for unknown/corrupt models, arithmetic overflow, bounds, or SQLite failure.
    pub fn load_model_cost_summary(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
    ) -> Result<Option<ModelCostSummary>, LabError> {
        let Some(header) = query_model_cost_header(self, run_id, model_id)? else {
            return Ok(None);
        };
        if header.status.is_none() {
            return self
                .model_cost_summary_from_source(run_id, model_id, header, &RunLedgerSource::Detail)
                .map(Some);
        }
        let source = run_ledger_source(self, run_id)?;
        self.model_cost_summary_from_source(run_id, model_id, header, &source)
            .map(Some)
    }

    fn model_cost_summary_from_source(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
        header: ModelCostHeader,
        source: &RunLedgerSource,
    ) -> Result<ModelCostSummary, LabError> {
        let status = header.status.map(|value| parse_enum(&value)).transpose()?;
        let policy_ref = policy_reference(
            header.policy_id,
            header.policy_revision_id,
            header.policy_definition_digest,
        )?;
        let expected_fill_count = nonnegative_u64(header.fill_count, "model fill count")?;
        let (fills, terminal) = if let RunLedgerSource::Sealed(lines) = source {
            let fills: Vec<FillRecord> = lines
                .iter()
                .filter(|line| line.section == LedgerSection::Fills && line.model_id == *model_id)
                .map(|line| serde_json::from_value(line.record.clone()).map_err(Into::into))
                .collect::<Result<Vec<_>, LabError>>()?;
            let terminal = lines
                .iter()
                .filter(|line| line.section == LedgerSection::Equity && line.model_id == *model_id)
                .max_by_key(|line| line.event_seq)
                .map(|line| serde_json::from_value::<AccountMark>(line.record.clone()))
                .transpose()?;
            (fills, terminal)
        } else {
            let fills = load_cost_fills(self, run_id, model_id)?;
            let terminal = self
                .connection
                .query_row(
                    "SELECT record_json FROM account_marks WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq DESC LIMIT 1",
                    params![run_id.as_str(), model_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_error)?
                .map(|json| serde_json::from_str::<AccountMark>(&json))
                .transpose()?;
            (fills, terminal)
        };
        let costs = fold_model_costs(fills)?;
        let actual_fill_count = u64::try_from(costs.fill_count)
            .map_err(|_| LabError::ResourceLimit("fill count exceeds u64".into()))?;
        if actual_fill_count != expected_fill_count {
            return Err(LabError::DataCorrupt(
                "cost summary fill count differs from model projection".into(),
            ));
        }
        if status == Some(ModelStatus::Completed)
            && terminal
                .as_ref()
                .is_none_or(|mark| mark.kind != MarkKind::Terminal)
        {
            return Err(LabError::DataCorrupt(
                "completed model has no terminal account mark".into(),
            ));
        }
        let terminal = terminal.filter(|mark| mark.kind == MarkKind::Terminal);
        Ok(ModelCostSummary {
            run_id: run_id.clone(),
            model_id: model_id.clone(),
            policy_ref,
            status,
            fill_count: actual_fill_count,
            buy_notional: QuoteAmount::new(costs.buy_notional)?,
            sell_notional: QuoteAmount::new(costs.sell_notional)?,
            total_fees: QuoteAmount::new(costs.total_fees)?,
            price_cost_attribution: SignedAmount::new(costs.price_cost_attribution)?,
            price_cost_attribution_unit: crate::contracts::PriceCostAttributionUnit::Krw,
            terminal_equity: terminal.as_ref().map(|mark| mark.equity),
            terminal_mark_seq: terminal.map(|mark| mark.context.event_seq),
        })
    }
    fn complete_model_summary(
        &self,
        run_id: &RunId,
        model: &mut ModelResultSummary,
        source: &RunLedgerSource,
    ) -> Result<(), LabError> {
        if model.status != Some(ModelStatus::Completed) {
            model.financial_null_reason = Some(
                model
                    .status_reason
                    .clone()
                    .unwrap_or_else(|| "MODEL_NOT_COMPLETED".into()),
            );
            return Ok(());
        }
        let header = query_model_cost_header(self, run_id, &model.model_id)?
            .ok_or_else(|| LabError::DataCorrupt("summary model costs missing".into()))?;
        let costs = self.model_cost_summary_from_source(run_id, &model.model_id, header, source)?;
        let terminal = costs.terminal_equity.ok_or_else(|| {
            LabError::DataCorrupt("completed summary has no terminal equity".into())
        })?;
        let net_return = terminal
            .get()
            .checked_sub(model.initial_cash.get())
            .and_then(|value| value.checked_div(model.initial_cash.get()))
            .ok_or_else(|| {
                LabError::AccountingInvariant("summary return arithmetic failed".into())
            })?;
        let (closed,open):(i64,i64) = self.connection.query_row(
            "SELECT COUNT(CASE WHEN status='CLOSED' THEN 1 END),COUNT(CASE WHEN status='OPEN' THEN 1 END) FROM episodes WHERE run_id=?1 AND model_id=?2",
            params![run_id.as_str(),model.model_id.as_str()], |row| Ok((row.get(0)?,row.get(1)?)),
        ).map_err(sql_error)?;
        let closed = nonnegative_u64(closed, "closed positions")?;
        let open = nonnegative_u64(open, "open positions")?;
        if closed.checked_add(open) != Some(model.episode_count) {
            return Err(LabError::DataCorrupt(
                "summary episode count mismatch".into(),
            ));
        }
        model.terminal_equity = Some(terminal);
        model.total_return_ratio = Some(SignedAmount::new(net_return)?);
        model.total_fees = Some(costs.total_fees);
        model.embedded_price_cost_quote = Some(costs.price_cost_attribution);
        model.closed_positions = Some(closed);
        model.open_positions = Some(open);
        Ok(())
    }

    /// Resolve the exact closing signal from immutable episode/fill/order links.
    /// # Errors
    /// Rejects missing or inconsistent links without inventing a strategy reason.
    pub fn episode_exit_details(
        &self,
        episode: &crate::contracts::EpisodeRecord,
    ) -> Result<crate::contracts::EpisodeExitDetails, LabError> {
        if episode.status == crate::contracts::EpisodeStatus::Open {
            return crate::reporting::episode_exit_details(episode, None);
        }
        let materialized: Option<String> = self.connection.query_row(
            "SELECT exit_details_json FROM episodes WHERE run_id=?1 AND model_id=?2 AND episode_id=?3",
            params![episode.run_id.as_str(), episode.model_id.as_str(), episode.episode_id.as_str()],
            |row| row.get(0),
        ).optional().map_err(sql_error)?.flatten();
        if let Some(json) = materialized {
            return serde_json::from_str(&json).map_err(Into::into);
        }
        let id = episode
            .fill_ids
            .last()
            .ok_or_else(|| LabError::DataCorrupt("closed episode has no final fill".into()))?;
        let fill: FillRecord = self.read_linked_record("fills", "fill_id", id.as_str(), episode)?;
        let order: crate::contracts::OrderRecord =
            self.read_linked_record("orders", "order_id", fill.order_id.as_str(), episode)?;
        let signal: crate::contracts::SignalRecord = self.read_linked_record(
            "signals",
            "signal_id",
            order.parent_signal_id.as_str(),
            episode,
        )?;
        crate::reporting::episode_exit_details(episode, Some((&fill, &order, &signal)))
    }

    fn read_linked_record<T: DeserializeOwned>(
        &self,
        table: &str,
        id_column: &str,
        id: &str,
        episode: &crate::contracts::EpisodeRecord,
    ) -> Result<T, LabError> {
        // Identifiers are private fixed literals above, never request-controlled SQL.
        let json: String = self.connection.query_row(
            &format!("SELECT record_json FROM {table} WHERE run_id=?1 AND model_id=?2 AND {id_column}=?3"),
            params![episode.run_id.as_str(),episode.model_id.as_str(),id], |row| row.get(0),
        ).optional().map_err(sql_error)?.ok_or_else(|| LabError::DataCorrupt("episode closing link missing".into()))?;
        serde_json::from_str(&json).map_err(Into::into)
    }
}

fn query_models(
    store: &Store,
    run_id: &RunId,
    initial_cash: QuoteAmount,
) -> Result<Vec<ModelResultSummary>, LabError> {
    let limit = i64::try_from(MAX_MODELS + 1)
        .map_err(|_| LabError::ResourceLimit("model query limit overflow".into()))?;
    let mut statement = store.connection.prepare(
        "SELECT model_id,market,strategy_kind,status,status_reason,last_committed_event_seq,signal_count,order_event_count,fill_count,account_mark_count,episode_count,fact_digest,final_digest,policy_id,policy_revision_id,policy_definition_digest FROM run_models WHERE run_id=?1 ORDER BY model_id LIMIT ?2",
    ).map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
            ))
        })
        .map_err(sql_error)?;
    let mut models = Vec::new();
    for row in rows {
        let row = row.map_err(sql_error)?;
        let policy_ref = policy_reference(row.13, row.14, row.15)?;
        models.push(ModelResultSummary {
            model_id: ModelId::new(row.0)?,
            market: MarketId::parse_upbit(&row.1)?,
            strategy: parse_enum(&row.2)?,
            policy_ref,
            status: row.3.map(|value| parse_enum(&value)).transpose()?,
            status_reason: row.4.map(|text| bound_text(&text)),
            last_event_seq: nonnegative_u64(row.5, "model event sequence")?,
            signal_count: nonnegative_u64(row.6, "signal count")?,
            order_event_count: nonnegative_u64(row.7, "order event count")?,
            fill_count: nonnegative_u64(row.8, "fill count")?,
            account_mark_count: nonnegative_u64(row.9, "account mark count")?,
            episode_count: nonnegative_u64(row.10, "episode count")?,
            fact_digest: row.11.map(ContentHash::try_from).transpose()?,
            final_digest: row.12.map(ContentHash::try_from).transpose()?,
            initial_cash,
            terminal_equity: None,
            total_return_ratio: None,
            total_fees: None,
            embedded_price_cost_quote: None,
            closed_positions: None,
            open_positions: None,
            financial_null_reason: None,
        });
    }
    Ok(models)
}

struct ModelCostHeader {
    status: Option<String>,
    fill_count: i64,
    policy_id: Option<String>,
    policy_revision_id: Option<String>,
    policy_definition_digest: Option<String>,
}

struct ModelCosts {
    fill_count: usize,
    buy_notional: rust_decimal::Decimal,
    sell_notional: rust_decimal::Decimal,
    total_fees: rust_decimal::Decimal,
    price_cost_attribution: rust_decimal::Decimal,
}

fn query_model_cost_header(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Option<ModelCostHeader>, LabError> {
    store
        .connection
        .query_row(
            "SELECT status,fill_count,policy_id,policy_revision_id,policy_definition_digest FROM run_models WHERE run_id=?1 AND model_id=?2",
            params![run_id.as_str(), model_id.as_str()],
            |row| {
                Ok(ModelCostHeader {
                    status: row.get(0)?,
                    fill_count: row.get(1)?,
                    policy_id: row.get(2)?,
                    policy_revision_id: row.get(3)?,
                    policy_definition_digest: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

fn load_cost_fills(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Vec<FillRecord>, LabError> {
    let mut statement = store
        .connection
        .prepare("SELECT record_json FROM fills WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq")
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), model_id.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    rows.map(|row| {
        let json = row.map_err(sql_error)?;
        serde_json::from_str(&json).map_err(LabError::from)
    })
    .collect()
}

fn fold_model_costs(fills: Vec<FillRecord>) -> Result<ModelCosts, LabError> {
    let mut costs = ModelCosts {
        fill_count: 0,
        buy_notional: rust_decimal::Decimal::ZERO,
        sell_notional: rust_decimal::Decimal::ZERO,
        total_fees: rust_decimal::Decimal::ZERO,
        price_cost_attribution: rust_decimal::Decimal::ZERO,
    };
    for fill in fills {
        costs.fill_count = costs
            .fill_count
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("fill count overflow".into()))?;
        if costs.fill_count > MAX_MODEL_EVENTS {
            return Err(LabError::DataCorrupt(
                "model fill count exceeds event bound".into(),
            ));
        }
        match fill.side {
            Side::Buy => {
                costs.buy_notional =
                    checked_add(costs.buy_notional, fill.notional.get(), "buy notional")?;
            }
            Side::Sell => {
                costs.sell_notional =
                    checked_add(costs.sell_notional, fill.notional.get(), "sell notional")?;
            }
        }
        costs.total_fees = checked_add(costs.total_fees, fill.fee.get(), "fees")?;
        costs.price_cost_attribution = checked_add(
            costs.price_cost_attribution,
            crate::reporting::fill_price_cost_quote(&fill)?.get(),
            "quantity-weighted quote cost attribution",
        )?;
    }
    Ok(costs)
}

fn policy_reference(
    policy_id: Option<String>,
    revision_id: Option<String>,
    definition_digest: Option<String>,
) -> Result<Option<PolicyRevisionRef>, LabError> {
    match (policy_id, revision_id, definition_digest) {
        (None, None, None) => Ok(None),
        (Some(policy_id), Some(revision_id), Some(definition_digest)) => {
            Ok(Some(PolicyRevisionRef {
                policy_id: PolicyId::new(policy_id)?,
                revision_id: PolicyRevisionId::new(revision_id)?,
                definition_digest: ContentHash::try_from(definition_digest)?,
            }))
        }
        _ => Err(LabError::DataCorrupt(
            "run model has incomplete policy revision identity".into(),
        )),
    }
}

fn query_validations(
    store: &Store,
    run_id: &RunId,
) -> Result<(Vec<ValidationResultSummary>, Option<String>), LabError> {
    let query_limit = i64::try_from(MAX_VALIDATION_REPORTS + 1)
        .map_err(|_| LabError::ResourceLimit("validation query limit overflow".into()))?;
    let mut statement = store
        .connection
        .prepare(
            "SELECT report_json FROM validation_results WHERE run_id=?1 ORDER BY check_id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), query_limit], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    let reports = rows
        .map(|row| {
            let json = row.map_err(sql_error)?;
            serde_json::from_str(&json).map_err(Into::into)
        })
        .collect::<Result<Vec<ValidationReport>, LabError>>()?;
    let report_truncated = reports.len() > MAX_VALIDATION_REPORTS;
    let reports = reports
        .into_iter()
        .take(MAX_VALIDATION_REPORTS)
        .map(validation_summary)
        .collect::<Result<Vec<_>, _>>()?;
    let truncated_reason = if report_truncated {
        Some(format!(
            "validation report limit reached; first {MAX_VALIDATION_REPORTS} returned"
        ))
    } else {
        None
    };
    Ok((reports, truncated_reason))
}

fn validation_summary(report: ValidationReport) -> Result<ValidationResultSummary, LabError> {
    let finding_count = u64::try_from(report.findings.len())
        .map_err(|_| LabError::ResourceLimit("validation finding count exceeds u64".into()))?;
    let findings_truncated = report.findings.len() > MAX_VALIDATION_FINDINGS;
    let findings = report
        .findings
        .into_iter()
        .take(MAX_VALIDATION_FINDINGS)
        .map(|finding| bound_text(&finding))
        .collect();
    Ok(ValidationResultSummary {
        check_id: bound_text(&report.check_id),
        status: report.status,
        input_digest: report.input_digest,
        checked_models: report.checked_models,
        checked_fills: report.checked_fills,
        checked_marks: report.checked_marks,
        finding_count,
        findings,
        truncated_reason: findings_truncated
            .then(|| format!("finding limit reached; first {MAX_VALIDATION_FINDINGS} returned")),
    })
}

fn bound_text(value: &str) -> String {
    value.chars().take(MAX_PUBLIC_TEXT_CHARS).collect()
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T, LabError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(Into::into)
}

fn nonnegative_u64(value: i64, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt(format!("negative {label}")))
}

fn checked_add(
    left: rust_decimal::Decimal,
    right: rust_decimal::Decimal,
    label: &str,
) -> Result<rust_decimal::Decimal, LabError> {
    left.checked_add(right)
        .ok_or_else(|| LabError::AccountingInvariant(format!("{label} overflow")))
}
