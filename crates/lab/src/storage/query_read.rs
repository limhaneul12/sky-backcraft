//! Bounded read projections for MCP status and result inspection.

use super::ledger_seal::{RunLedgerState, read_run_ledger_state, visit_run_ledger_source};
use super::{Store, enum_text, json_error, sql_error};
use crate::contracts::{
    AccountMark, ContentHash, FillRecord, JobStatus, LabError, LedgerSection, MAX_MODEL_EVENTS,
    MAX_MODELS, MarkKind, MarketId, ModelId, ModelStatus, PlanId, PolicyId, PolicyRevisionId,
    PolicyRevisionRef, QuoteAmount, RunId, Side, SignedAmount, StrategyKind, ValidationReport,
};
use rusqlite::{OptionalExtension, params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;

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
        let ledger_state = read_run_ledger_state(self, run_id)?;
        let models_result = query_models(self, run_id, plan.resolved.spec.initial_cash);
        let mut cost_facts = BTreeMap::new();
        if ledger_state == RunLedgerState::Compacted {
            if let Ok(models) = &models_result {
                for model in models
                    .iter()
                    .filter(|model| model.status == Some(ModelStatus::Completed))
                {
                    cost_facts.insert(model.model_id.clone(), ModelCostFacts::default());
                }
            }
            if !self.visit_run_ledger_lines(run_id, |line| {
                if let Some(facts) = cost_facts.get_mut(&line.model_id) {
                    facts.observe(line.section, line.event_seq, line.record);
                }
            })? {
                return Err(LabError::DataCorrupt(
                    "compacted run has no ledger chunks".into(),
                ));
            }
        }
        let mut models = models_result?;
        for model in &mut models {
            self.complete_model_summary(run_id, model, cost_facts.get_mut(&model.model_id))?;
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
                .model_cost_summary_from_detail(run_id, model_id, header)
                .map(Some);
        }
        let mut facts = ModelCostFacts::default();
        if visit_run_ledger_source(self, run_id, |line| {
            if line.model_id == *model_id {
                facts.observe(line.section, line.event_seq, line.record);
            }
        })? {
            Self::model_cost_summary_from_facts(run_id, model_id, header, &mut facts).map(Some)
        } else {
            self.model_cost_summary_from_detail(run_id, model_id, header)
                .map(Some)
        }
    }

    fn model_cost_summary_from_detail(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
        header: ModelCostHeader,
    ) -> Result<ModelCostSummary, LabError> {
        let (status, policy_ref, expected_fill_count) = model_cost_header_parts(header)?;
        self.model_cost_summary_from_detail_parts(
            run_id,
            model_id,
            status,
            policy_ref,
            expected_fill_count,
        )
    }

    fn model_cost_summary_from_detail_parts(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
        status: Option<ModelStatus>,
        policy_ref: Option<PolicyRevisionRef>,
        expected_fill_count: u64,
    ) -> Result<ModelCostSummary, LabError> {
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
        Self::finish_model_cost_summary(
            run_id,
            model_id,
            status,
            policy_ref,
            expected_fill_count,
            fills,
            terminal,
        )
    }

    fn model_cost_summary_from_facts(
        run_id: &RunId,
        model_id: &ModelId,
        header: ModelCostHeader,
        facts: &mut ModelCostFacts,
    ) -> Result<ModelCostSummary, LabError> {
        let (status, policy_ref, expected_fill_count) = model_cost_header_parts(header)?;
        Self::model_cost_summary_from_facts_parts(
            run_id,
            model_id,
            status,
            policy_ref,
            expected_fill_count,
            facts,
        )
    }

    fn model_cost_summary_from_facts_parts(
        run_id: &RunId,
        model_id: &ModelId,
        status: Option<ModelStatus>,
        policy_ref: Option<PolicyRevisionRef>,
        expected_fill_count: u64,
        facts: &mut ModelCostFacts,
    ) -> Result<ModelCostSummary, LabError> {
        if let Some(error) = facts.fill_error.take() {
            return Err(error);
        }
        let fills = std::mem::take(&mut facts.fills);
        let terminal = facts
            .terminal
            .take()
            .map(|(_, record)| serde_json::from_value::<AccountMark>(record).map_err(json_error))
            .transpose()?;
        Self::finish_model_cost_summary(
            run_id,
            model_id,
            status,
            policy_ref,
            expected_fill_count,
            fills,
            terminal,
        )
    }

    fn finish_model_cost_summary(
        run_id: &RunId,
        model_id: &ModelId,
        status: Option<ModelStatus>,
        policy_ref: Option<PolicyRevisionRef>,
        expected_fill_count: u64,
        fills: Vec<FillRecord>,
        terminal: Option<AccountMark>,
    ) -> Result<ModelCostSummary, LabError> {
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
        facts: Option<&mut ModelCostFacts>,
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
        let costs = if let Some(facts) = facts {
            Self::model_cost_summary_from_facts_parts(
                run_id,
                &model.model_id,
                model.status,
                model.policy_ref.clone(),
                model.fill_count,
                facts,
            )?
        } else {
            self.model_cost_summary_from_detail_parts(
                run_id,
                &model.model_id,
                model.status,
                model.policy_ref.clone(),
                model.fill_count,
            )?
        };
        self.complete_model_summary_from_costs(run_id, model, &costs)
    }

    fn complete_model_summary_from_costs(
        &self,
        run_id: &RunId,
        model: &mut ModelResultSummary,
        costs: &ModelCostSummary,
    ) -> Result<(), LabError> {
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

#[derive(Default)]
struct ModelCostFacts {
    fills: Vec<FillRecord>,
    fill_error: Option<LabError>,
    terminal: Option<(u64, serde_json::Value)>,
}

impl ModelCostFacts {
    fn observe(&mut self, section: LedgerSection, event_seq: u64, record: serde_json::Value) {
        match section {
            LedgerSection::Fills if self.fill_error.is_none() => {
                match serde_json::from_value(record).map_err(json_error) {
                    Ok(fill) => self.fills.push(fill),
                    Err(error) => self.fill_error = Some(error),
                }
            }
            LedgerSection::Equity
                if self
                    .terminal
                    .as_ref()
                    .is_none_or(|(sequence, _)| event_seq >= *sequence) =>
            {
                // Iterator::max_by_key keeps the last equal maximum.
                self.terminal = Some((event_seq, record));
            }
            _ => {}
        }
    }
}

struct ModelCosts {
    fill_count: usize,
    buy_notional: rust_decimal::Decimal,
    sell_notional: rust_decimal::Decimal,
    total_fees: rust_decimal::Decimal,
    price_cost_attribution: rust_decimal::Decimal,
}

fn model_cost_header_parts(
    header: ModelCostHeader,
) -> Result<(Option<ModelStatus>, Option<PolicyRevisionRef>, u64), LabError> {
    let status = header.status.map(|value| parse_enum(&value)).transpose()?;
    let policy_ref = policy_reference(
        header.policy_id,
        header.policy_revision_id,
        header.policy_definition_digest,
    )?;
    let expected_fill_count = nonnegative_u64(header.fill_count, "model fill count")?;
    Ok((status, policy_ref, expected_fill_count))
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

#[cfg(test)]
mod tests {
    use super::super::ledger_seal::tests::{
        ScaledRunConfig, TamperedChunkGuard, TempRoot, seed_scaled_multi_model_run,
    };
    use super::*;

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "regression test verifies run summary, per-model cost summary, blocked model financial nulls, and hidden-line corruption"
    )]
    fn compacted_summary_and_costs_preserve_mixed_models_and_reject_hidden_corruption() {
        let root = TempRoot::new("query-read-summary");
        let mut store = Store::open(&root.0).expect("open store");
        let seeded = seed_scaled_multi_model_run(
            &mut store,
            &ScaledRunConfig {
                run_id: "run-compacted-summary",
                completed_models: 3,
                signals_per_model: 40,
                roundtrips_per_model: 30,
                marks_per_model: 120,
                include_blocked_model: true,
            },
        );
        let blocked_id = seeded.blocked_model.as_ref().expect("blocked model id");

        let detail_summary = store
            .load_run_summary(&seeded.run_id)
            .expect("detail summary")
            .expect("summary exists");
        let detail_summary_bytes =
            serde_json::to_vec(&detail_summary).expect("encode detail summary");

        let mut all_models = seeded.completed_models.clone();
        all_models.push(blocked_id.clone());
        let detail_costs: Vec<(ModelId, Vec<u8>)> = all_models
            .iter()
            .map(|m| {
                let cost = store
                    .load_model_cost_summary(&seeded.run_id, m)
                    .expect("detail model cost")
                    .expect("model cost exists");
                (m.clone(), serde_json::to_vec(&cost).expect("encode cost"))
            })
            .collect();

        store
            .seal_and_compact_run_ledger(&seeded.run_id)
            .expect("seal and compact run");

        let compacted_summary = store
            .load_run_summary(&seeded.run_id)
            .expect("compacted summary")
            .expect("compacted summary exists");
        assert_eq!(
            serde_json::to_vec(&compacted_summary).expect("encode compacted summary"),
            detail_summary_bytes
        );

        let blocked_entry = compacted_summary
            .models
            .iter()
            .find(|m| m.model_id == *blocked_id)
            .expect("blocked model in summary");
        assert_eq!(blocked_entry.status, Some(ModelStatus::BlockedData));
        assert_eq!(
            blocked_entry.financial_null_reason.as_deref(),
            Some("DATA_QUALITY_BLOCKED")
        );
        assert!(blocked_entry.terminal_equity.is_none());
        assert!(blocked_entry.total_return_ratio.is_none());
        assert!(blocked_entry.total_fees.is_none());
        assert!(blocked_entry.embedded_price_cost_quote.is_none());
        assert!(blocked_entry.closed_positions.is_none());
        assert!(blocked_entry.open_positions.is_none());

        for (model_id, expected_bytes) in &detail_costs {
            let cost = store
                .load_model_cost_summary(&seeded.run_id, model_id)
                .expect("compacted model cost")
                .expect("compacted cost exists");
            assert_eq!(
                &serde_json::to_vec(&cost).expect("encode compacted cost"),
                expected_bytes,
                "cost summary mismatch for {model_id}"
            );
        }

        let selected_model = &seeded.completed_models[0];
        let expected_selected_cost = detail_costs
            .iter()
            .find(|(model_id, _)| model_id == selected_model)
            .map(|(_, bytes)| bytes)
            .expect("selected model detail cost");
        let compatible_scope_guard =
            TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 0, |lines| {
                let nonterminal_mark_index = lines
                    .iter()
                    .position(|line| {
                        let envelope: serde_json::Value =
                            serde_json::from_str(line).expect("ledger envelope JSON");
                        envelope.get("kind").and_then(serde_json::Value::as_str)
                            == Some("ACCOUNT_MARK")
                            && envelope.get("model").and_then(serde_json::Value::as_str)
                                == Some(selected_model.as_str())
                    })
                    .expect("selected model account mark");
                let mut mark_envelope: serde_json::Value =
                    serde_json::from_str(&lines[nonterminal_mark_index])
                        .expect("account mark envelope JSON");
                assert_ne!(
                    mark_envelope["r"]["kind"].as_str(),
                    Some("TERMINAL"),
                    "selected fixture mark is nonterminal"
                );
                mark_envelope["r"] = serde_json::json!("opaque nonterminal mark");
                lines[nonterminal_mark_index] =
                    serde_json::to_string(&mark_envelope).expect("encode opaque account mark");

                let fill_index = lines
                    .iter()
                    .position(|line| {
                        let envelope: serde_json::Value =
                            serde_json::from_str(line).expect("ledger envelope JSON");
                        envelope.get("kind").and_then(serde_json::Value::as_str) == Some("FILL")
                            && envelope.get("model").and_then(serde_json::Value::as_str)
                                == Some(selected_model.as_str())
                    })
                    .expect("selected model fill");
                let mut fill_envelope: serde_json::Value =
                    serde_json::from_str(&lines[fill_index]).expect("fill envelope JSON");
                fill_envelope["lsb"] = serde_json::json!("mismatched-source-bar");
                fill_envelope["lst"] = serde_json::json!(0);
                lines[fill_index] =
                    serde_json::to_string(&fill_envelope).expect("encode mismatched fill columns");
            });
        let compatible_cost = store
            .load_model_cost_summary(&seeded.run_id, selected_model)
            .expect("cost summary preserves old sealed-read validation scope")
            .expect("selected model cost exists");
        assert_eq!(
            &serde_json::to_vec(&compatible_cost).expect("encode compatible cost"),
            expected_selected_cost
        );
        compatible_scope_guard.restore(&store, &seeded.run_id);

        // Corrupt a non-cost line (SIGNAL) in the sealed chunk and verify both summary and cost fail closed.
        let guard = TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 0, |lines| {
            let sig_line = lines
                .iter_mut()
                .find(|line| line.contains("\"kind\":\"SIGNAL\""))
                .expect("signal line");
            let mut envelope: serde_json::Value =
                serde_json::from_str(sig_line).expect("signal envelope JSON");
            envelope["seq"] = serde_json::json!(-1);
            *sig_line = serde_json::to_string(&envelope).expect("encode invalid sequence");
        });
        assert!(matches!(
            store.load_run_summary(&seeded.run_id),
            Err(LabError::DataCorrupt(_))
        ));
        assert!(matches!(
            store.load_model_cost_summary(&seeded.run_id, &seeded.completed_models[0]),
            Err(LabError::DataCorrupt(_))
        ));
        guard.restore(&store, &seeded.run_id);
    }
}
