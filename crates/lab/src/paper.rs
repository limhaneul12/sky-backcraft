//! Persistent causal paper-trading session engine and checkpoint state machine.
//!
//! Reuses [`PolicyEvaluator`] and [`EngineState`] so every indicator, signal,
//! virtual order reservation, fill, cost attribution, mark, and episode follows
//! the exact spot accounting invariants without re-running from bar zero.

use crate::contracts::{
    AccountMark, AdmissionStatus, AssetQuantity, BasisPoints, CandleInterval, CandleObservation,
    CapitalMode, CausalExecutionPolicy, ContentHash, CostPolicy, DatasetId, ENGINE_VERSION,
    EpisodeRecord, EvidenceSnapshot, EvidenceUnavailablePolicy, ExecutionPolicy, ExperimentSpec,
    FillRecord, FrozenPolicyRevision, LabError, MarkKind, MarketId, MarketRuleSnapshot,
    MarketTradingState, ModelAdmission, ModelId, ObservationId, OrderRecord, OrderStatus,
    PaperBenchmarkKind, PaperBenchmarkSnapshot, PaperCandleReference, PaperEventKind,
    PaperFreshness, PaperMarketRuleScenario, PaperMarketSnapshot, PaperNullReason,
    PaperPolicyOption, PaperRecoveryCause, PaperScenarioPreset, PaperSessionEvent, PaperSessionId,
    PaperSessionLimits, PaperSessionStartOptions, PaperVerification, PaperVerificationStatus,
    PitPolicy, PlanId, PolicyRevisionRef, PriceKrw, QuoteAmount, ReasonCode, ReportClock,
    RequestId, ResolvedPlan, RuleProvenance, RuleSnapshotId, RunId, Side, SignalOutcome,
    SignalRecord, SignedAmount, SimulatedOrderType, StrategyBinding, TerminalPolicy, TickBand,
    UtcRange, UtcTimestamp, Weight, checked, observation_digest,
};
use crate::engine::{
    EngineState, PendingExecution,
    accounting::Account,
    add_millis,
    execution::{plan_market_arrival, plan_market_request},
};
use crate::evidence::EvidenceEvaluator;
use crate::policy_engine::PolicyEvaluator;
use crate::strategy::PositionView;
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Schema version for persisted per-market paper checkpoints.
pub const PAPER_CHECKPOINT_SCHEMA_VERSION: &str = "paper-market-checkpoint-v1";

/// Maximum active (`RUNNING` or `PAUSED`) paper sessions.
pub const MAX_ACTIVE_PAPER_SESSIONS: u32 = 8;
/// Maximum independent markets per paper session.
pub const MAX_PAPER_MARKETS_PER_SESSION: u32 = 3;
/// Maximum completed candles fetched per market/interval recovery pass.
pub const MAX_PAPER_RECOVERY_BARS: u32 = 199;

/// Source interval cursor stored in a market checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperSourceCursor {
    pub interval: CandleInterval,
    pub last_bar_close_utc: Option<UtcTimestamp>,
    pub last_bar_digest: Option<ContentHash>,
    pub completed_bars: u64,
}

struct PlannedPaperSourceBar<'a> {
    bar: &'a CandleObservation,
    digest: ContentHash,
}

struct PlannedPaperSourceFeed<'a> {
    interval: CandleInterval,
    bars: Vec<PlannedPaperSourceBar<'a>>,
}

struct PreparedPaperSourceRow<'a> {
    bar: &'a CandleObservation,
    original_index: usize,
    digest: Option<ContentHash>,
}

struct PreparedPaperSourceStream<'a> {
    indicator_ids: Vec<String>,
    rows_by_close: Vec<PreparedPaperSourceRow<'a>>,
    validated_prefix_len: usize,
    ordered_prefix: Vec<usize>,
}

/// Per-operation view over one immutable source map; never persisted in a checkpoint.
pub(crate) struct PreparedPaperSourceFrame<'a> {
    market_code: String,
    requirements: Vec<(CandleInterval, usize)>,
    streams: BTreeMap<CandleInterval, PreparedPaperSourceStream<'a>>,
}

impl<'a> PreparedPaperSourceFrame<'a> {
    pub(crate) fn new(
        evaluator: &PolicyEvaluator,
        market: &MarketId,
        source_bars: &'a BTreeMap<CandleInterval, Vec<CandleObservation>>,
    ) -> Self {
        let indicators = evaluator.source_indicators();
        let requirements = evaluator.source_requirements();
        let mut streams = BTreeMap::new();
        for (interval, _) in &requirements {
            let indicator_ids = indicators
                .iter()
                .filter(|(_, source_interval, _)| source_interval == interval)
                .map(|(indicator_id, _, _)| indicator_id.clone())
                .collect();
            let mut rows_by_close = source_bars
                .get(interval)
                .into_iter()
                .flatten()
                .enumerate()
                .filter(|(_, bar)| bar.candle.completed)
                .map(|(original_index, bar)| PreparedPaperSourceRow {
                    bar,
                    original_index,
                    digest: None,
                })
                .collect::<Vec<_>>();
            rows_by_close.sort_by(|left, right| {
                left.bar
                    .candle
                    .close_time_utc
                    .cmp(&right.bar.candle.close_time_utc)
                    .then_with(|| left.original_index.cmp(&right.original_index))
            });
            streams.insert(
                *interval,
                PreparedPaperSourceStream {
                    indicator_ids,
                    rows_by_close,
                    validated_prefix_len: 0,
                    ordered_prefix: Vec::new(),
                },
            );
        }
        Self {
            market_code: market.code(),
            requirements,
            streams,
        }
    }
}

impl<'a> PreparedPaperSourceStream<'a> {
    fn prefix_until(
        &mut self,
        market_code: &str,
        interval: CandleInterval,
        decision_close: UtcTimestamp,
    ) -> Result<usize, LabError> {
        let eligible_end = self
            .rows_by_close
            .partition_point(|row| row.bar.candle.close_time_utc <= decision_close);
        if eligible_end > self.validated_prefix_len {
            let mut newly_eligible = (self.validated_prefix_len..eligible_end).collect::<Vec<_>>();
            newly_eligible.sort_by_key(|row_index| self.rows_by_close[*row_index].original_index);
            for row_index in newly_eligible {
                let bar = self.rows_by_close[row_index].bar;
                if bar.candle.interval != interval || bar.candle.market != market_code {
                    return Err(LabError::DataCorrupt(format!(
                        "paper source row identity mismatch for {interval:?}"
                    )));
                }
                let expected_close = bar
                    .candle
                    .open_time_utc
                    .0
                    .checked_add_signed(interval.duration())
                    .map(UtcTimestamp)
                    .ok_or_else(|| {
                        LabError::DataCorrupt("paper source boundary overflow".into())
                    })?;
                if bar.candle.close_time_utc != expected_close
                    || crate::scheduling::latest_completed_boundary(
                        bar.candle.close_time_utc,
                        interval,
                    )? != bar.candle.close_time_utc
                {
                    return Err(LabError::DataCorrupt(format!(
                        "paper source row is off the {interval:?} boundary grid"
                    )));
                }
                let digest = observation_digest(&bar.candle)?;
                if bar.content_digest != digest {
                    return Err(LabError::InputHashMismatch(
                        "paper source candle content digest mismatch".into(),
                    ));
                }
                self.rows_by_close[row_index].digest = Some(digest);
            }

            for row_index in self.validated_prefix_len..eligible_end {
                if let Some(previous_index) = self.ordered_prefix.last().copied() {
                    let previous = &self.rows_by_close[previous_index];
                    let current = &self.rows_by_close[row_index];
                    if previous.bar.candle.close_time_utc == current.bar.candle.close_time_utc {
                        let previous_digest = previous.digest.as_ref().ok_or_else(|| {
                            LabError::Internal("validated paper source digest missing".into())
                        })?;
                        let current_digest = current.digest.as_ref().ok_or_else(|| {
                            LabError::Internal("validated paper source digest missing".into())
                        })?;
                        if previous_digest != current_digest {
                            return Err(LabError::DataCorrupt(format!(
                                "conflicting paper source rows at {}",
                                current.bar.candle.close_time_utc
                            )));
                        }
                        continue;
                    }
                }
                self.ordered_prefix.push(row_index);
            }
            self.validated_prefix_len = eligible_end;
        }

        let ordered_end = self.ordered_prefix.partition_point(|row_index| {
            self.rows_by_close[*row_index].bar.candle.close_time_utc <= decision_close
        });
        Ok(ordered_end)
    }

    fn planned_row(&self, row_index: usize) -> Result<PlannedPaperSourceBar<'a>, LabError> {
        let row = self
            .rows_by_close
            .get(row_index)
            .ok_or_else(|| LabError::Internal("prepared paper source row missing".into()))?;
        let digest = row
            .digest
            .clone()
            .ok_or_else(|| LabError::Internal("validated paper source digest missing".into()))?;
        Ok(PlannedPaperSourceBar {
            bar: row.bar,
            digest,
        })
    }
}

enum PausedSourceFeed {
    Ready(bool),
    Recovery(PaperStepOutcome),
}

/// Serializable per-market continuation checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperMarketCheckpoint {
    pub schema_version: String,
    pub engine_version: String,
    pub session_id: PaperSessionId,
    pub market: MarketId,
    pub model_id: ModelId,
    pub policy_ref: PolicyRevisionRef,
    pub initial_cash: QuoteAmount,
    pub costs: CostPolicy,
    pub cost_policy_digest: ContentHash,
    pub market_rules: PaperMarketRuleScenario,
    pub rules_digest: ContentHash,
    pub plan: ResolvedPlan,
    pub(crate) evaluator: PolicyEvaluator,
    pub(crate) engine_state: EngineState,
    pub(crate) pending: Option<PendingExecution>,
    pub prior_execution_bar: Option<CandleObservation>,
    pub prior_passive_liquidity_bar: Option<CandleObservation>,
    pub prior_bar_evaluated: bool,
    pub last_finalized_candle_at: Option<UtcTimestamp>,
    pub last_finalized_candle_digest: Option<ContentHash>,
    pub last_source_event_at: Option<UtcTimestamp>,
    pub last_heartbeat_at: Option<UtcTimestamp>,
    pub source_cursors: BTreeMap<CandleInterval, PaperSourceCursor>,
    pub warmup_completed_bars: u32,
    pub warmup_required_bars: u32,
    pub initial_mark: Option<AccountMark>,
    pub latest_mark: Option<AccountMark>,
    pub signal_count: u64,
    pub fill_count: u64,
    pub market_event_count: u64,
    pub benchmark_base_observation_id: Option<ObservationId>,
    pub(crate) benchmark_account: Account,
    pub benchmark: Option<PaperBenchmarkSnapshot>,
    pub benchmark_null_reason: Option<PaperNullReason>,
    pub freshness: PaperFreshness,
    pub recovery_reason: Option<String>,
}

/// Unassigned market event produced by a checkpoint transition.
#[derive(Debug, Clone)]
pub struct PendingPaperEvent {
    pub kind: PaperEventKind,
    pub market: Option<MarketId>,
    pub candle: Option<PaperCandleReference>,
    pub source_event_at: Option<UtcTimestamp>,
    pub finalized_at: Option<UtcTimestamp>,
    pub signals: Vec<SignalRecord>,
    pub orders: Vec<OrderRecord>,
    pub order_events: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub account_marks: Vec<AccountMark>,
    pub episodes: Vec<EpisodeRecord>,
}

/// Output of one causal bar step or pause catch-up on a market checkpoint.
#[derive(Debug, Clone)]
pub struct PaperStepOutcome {
    pub events: Vec<PendingPaperEvent>,
    pub did_advance: bool,
    pub recovery_required: Option<String>,
    pub recovery_cause: Option<PaperRecoveryCause>,
}

/// Build the canonical start options for the paper session creation form.
///
/// # Errors
/// Returns an error if default scenario parameters fail validation.
pub fn default_start_options(
    policy_options: Vec<PaperPolicyOption>,
) -> Result<PaperSessionStartOptions, LabError> {
    let preset_costs = CostPolicy {
        buy_fee_bps: BasisPoints::new(Decimal::from(10))?,
        sell_fee_bps: BasisPoints::new(Decimal::from(10))?,
        maker_fee_bps: BasisPoints::new(Decimal::from(5))?,
        half_spread_bps: BasisPoints::new(Decimal::ZERO)?,
        slippage_bps: BasisPoints::new(Decimal::ZERO)?,
        impact_bps: BasisPoints::new(Decimal::ZERO)?,
        assumption_label: "SIMULATED_SCENARIO: repository synthetic fixture values; editable"
            .into(),
        dynamic: None,
    };
    let preset_rules = PaperMarketRuleScenario::SimulatedScenario {
        min_notional: QuoteAmount::new(Decimal::ONE)?,
        quantity_step: AssetQuantity::new(Decimal::new(1, 2))?,
        ticks: vec![TickBand {
            lower_bound: QuoteAmount::new(Decimal::ZERO)?,
            tick: PriceKrw::new(Decimal::ONE)?,
        }],
        assumption_label: "SIMULATED_SCENARIO: repository synthetic fixture values; editable"
            .into(),
    };
    let preset = PaperScenarioPreset {
        preset_id: "repository-synthetic-baseline".into(),
        name: "Repository synthetic baseline (editable)".into(),
        initial_cash: QuoteAmount::new(Decimal::from(10_000))?,
        costs: preset_costs,
        market_rules: preset_rules,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        latency_ms: 0,
        editable: true,
        source_note: "Derived from the existing valid contracts/experiment.rs synthetic test fixture. These values are scenario assumptions, not Upbit fees or venue rules.".into(),
    };
    let intervals = vec![
        CandleInterval::H1,
        CandleInterval::M15,
        CandleInterval::M5,
        CandleInterval::M1,
        CandleInterval::H4,
        CandleInterval::D1,
    ];
    Ok(PaperSessionStartOptions {
        policy_options,
        scenario_presets: vec![preset],
        decision_intervals: intervals.clone(),
        execution_resolutions: intervals,
        limits: PaperSessionLimits {
            active_sessions: MAX_ACTIVE_PAPER_SESSIONS,
            markets_per_session: MAX_PAPER_MARKETS_PER_SESSION,
            recovery_bars_per_market_interval: MAX_PAPER_RECOVERY_BARS,
        },
    })
}

/// Build a [`PaperPolicyOption`] from a stored [`FrozenPolicyRevision`].
#[must_use]
pub fn policy_option_from_revision(revision: &FrozenPolicyRevision) -> PaperPolicyOption {
    let probe_result = (|| -> Result<(u32, Vec<CandleInterval>), LabError> {
        let session_id = PaperSessionId::new("paper-option-probe")?;
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let initial_cash = QuoteAmount::new(Decimal::from(10_000))?;
        let costs = CostPolicy {
            buy_fee_bps: BasisPoints::new(Decimal::from(10))?,
            sell_fee_bps: BasisPoints::new(Decimal::from(10))?,
            maker_fee_bps: BasisPoints::new(Decimal::from(5))?,
            half_spread_bps: BasisPoints::new(Decimal::ZERO)?,
            slippage_bps: BasisPoints::new(Decimal::ZERO)?,
            impact_bps: BasisPoints::new(Decimal::ZERO)?,
            assumption_label: "probe".into(),
            dynamic: None,
        };
        let rules = PaperMarketRuleScenario::SimulatedScenario {
            min_notional: QuoteAmount::new(Decimal::ONE)?,
            quantity_step: AssetQuantity::new(Decimal::new(1, 2))?,
            ticks: vec![TickBand {
                lower_bound: QuoteAmount::new(Decimal::ZERO)?,
                tick: PriceKrw::new(Decimal::ONE)?,
            }],
            assumption_label: "probe".into(),
        };
        let started_at = UtcTimestamp(
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| LabError::Internal("invalid probe timestamp".into()))?,
        );
        let cp = PaperMarketCheckpoint::new(
            &session_id,
            market,
            revision,
            CandleInterval::H1,
            CandleInterval::H1,
            0,
            ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            initial_cash,
            costs,
            rules,
            started_at,
        )?;
        let source_intervals = cp.source_cursors.keys().copied().collect();
        Ok((cp.warmup_required_bars, source_intervals))
    })();

    match probe_result {
        Ok((decision_warmup_bars, source_intervals)) => PaperPolicyOption {
            reference: revision.reference.clone(),
            family: revision.family,
            name: revision.definition.name.clone(),
            supported: true,
            unavailable_reason: None,
            decision_warmup_bars,
            source_intervals,
        },
        Err(err) => PaperPolicyOption {
            reference: revision.reference.clone(),
            family: revision.family,
            name: revision.definition.name.clone(),
            supported: false,
            unavailable_reason: Some(err.to_string()),
            decision_warmup_bars: 1,
            source_intervals: Vec::new(),
        },
    }
}

/// Default verification block for persistent paper sessions.
#[must_use]
pub fn default_paper_verification() -> PaperVerification {
    PaperVerification {
        status: PaperVerificationStatus::NotAvailable,
        reason: "INDEPENDENT_PAPER_VERIFIER_NOT_IMPLEMENTED".into(),
    }
}

pub(crate) fn semantic_digest<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&(domain, value))
}

/// Compute a deterministic digest over a request payload for idempotent replay checking.
///
/// # Errors
/// Returns an error if the request cannot be serialized.
pub fn request_payload_digest<T: Serialize>(
    action_label: &str,
    request_id: &RequestId,
    payload: &T,
) -> Result<ContentHash, LabError> {
    semantic_digest(
        "paper_request_v1",
        &(action_label, request_id.as_str(), payload),
    )
}

impl PaperMarketCheckpoint {
    /// Create an initial checkpoint for one market at session creation time.
    ///
    /// # Errors
    /// Rejects invalid policy definitions, unsupported intervals, or invalid cost/rule parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: &PaperSessionId,
        market: MarketId,
        policy_revision: &FrozenPolicyRevision,
        decision_interval: CandleInterval,
        execution_resolution: CandleInterval,
        latency_ms: u64,
        execution: ExecutionPolicy,
        initial_cash: QuoteAmount,
        costs: CostPolicy,
        market_rules: PaperMarketRuleScenario,
        started_at: UtcTimestamp,
    ) -> Result<Self, LabError> {
        if decision_interval != execution_resolution {
            return Err(LabError::InvalidConfig(
                "paper sessions currently require execution_resolution equal to decision_interval"
                    .into(),
            ));
        }
        let cost_policy_digest = semantic_digest("paper_cost_policy_v1", &costs)?;
        let rules_digest = semantic_digest("paper_market_rules_v1", &market_rules)?;
        let model_id = ModelId::new(format!(
            "model-{}-{}",
            session_id.as_str(),
            market.code().to_ascii_lowercase()
        ))?;
        let plan = build_single_market_plan(
            session_id,
            &market,
            &model_id,
            policy_revision,
            decision_interval,
            execution_resolution,
            latency_ms,
            execution,
            initial_cash,
            costs.clone(),
            &market_rules,
            started_at,
        )?;
        let binding = StrategyBinding::Policy {
            reference: policy_revision.reference.clone(),
            family: policy_revision.family,
        };
        let evaluator =
            PolicyEvaluator::compile(binding.clone(), &plan, market.clone(), decision_interval)?;
        let admission = plan
            .admissions
            .first()
            .ok_or_else(|| LabError::Internal("paper plan missing admission".into()))?;
        let run_id = RunId::new(format!("run-{}", session_id.as_str()))?;
        let engine_state = EngineState::new(run_id, admission, binding, initial_cash, 0)?;
        let warmup_required_bars = u32::try_from(evaluator.decision_warmup_bars().max(1))
            .map_err(|_| LabError::ResourceLimit("warmup bar count exceeds u32".into()))?;
        let mut source_cursors = BTreeMap::new();
        for (source_interval, _) in evaluator.source_requirements() {
            source_cursors.insert(
                source_interval,
                PaperSourceCursor {
                    interval: source_interval,
                    last_bar_close_utc: None,
                    last_bar_digest: None,
                    completed_bars: 0,
                },
            );
        }
        let benchmark_account = Account::new(initial_cash);
        Ok(Self {
            schema_version: PAPER_CHECKPOINT_SCHEMA_VERSION.to_owned(),
            engine_version: ENGINE_VERSION.to_owned(),
            session_id: session_id.clone(),
            market,
            model_id,
            policy_ref: policy_revision.reference.clone(),
            initial_cash,
            costs,
            cost_policy_digest,
            market_rules,
            rules_digest,
            plan,
            evaluator,
            engine_state,
            pending: None,
            prior_execution_bar: None,
            prior_passive_liquidity_bar: None,
            prior_bar_evaluated: false,
            last_finalized_candle_at: None,
            last_finalized_candle_digest: None,
            last_source_event_at: None,
            last_heartbeat_at: None,
            source_cursors,
            warmup_completed_bars: 0,
            warmup_required_bars,
            initial_mark: None,
            latest_mark: None,
            signal_count: 0,
            fill_count: 0,
            market_event_count: 0,
            benchmark_base_observation_id: None,
            benchmark_account,
            benchmark: None,
            benchmark_null_reason: Some(PaperNullReason::InsufficientHistory),
            freshness: PaperFreshness::Offline,
            recovery_reason: None,
        })
    }

    /// Compute the deterministic digest of this checkpoint for CAS and corruption detection.
    ///
    /// # Errors
    /// Propagates serialization failures.
    pub fn checkpoint_digest(&self) -> Result<ContentHash, LabError> {
        semantic_digest("paper_market_checkpoint_v1", self)
    }

    /// Validate checkpoint integrity against the session's immutable identity and invariants.
    ///
    /// # Errors
    /// Returns [`LabError::DataCorrupt`] when any identity digest, version, or accounting
    /// invariant fails verification.
    pub fn verify_integrity(
        &self,
        expected_session_id: &PaperSessionId,
        expected_market: &MarketId,
        expected_policy_ref: &PolicyRevisionRef,
        expected_initial_cash: QuoteAmount,
    ) -> Result<(), LabError> {
        if self.schema_version != PAPER_CHECKPOINT_SCHEMA_VERSION {
            return Err(LabError::DataCorrupt(format!(
                "unsupported paper checkpoint schema version {}",
                self.schema_version
            )));
        }
        if self.engine_version != ENGINE_VERSION {
            return Err(LabError::DataCorrupt(format!(
                "paper checkpoint engine version mismatch: expected {ENGINE_VERSION}, got {}",
                self.engine_version
            )));
        }
        let expected_cost_digest = semantic_digest("paper_cost_policy_v1", &self.costs)?;
        let expected_rules_digest = semantic_digest("paper_market_rules_v1", &self.market_rules)?;
        if &self.session_id != expected_session_id
            || &self.market != expected_market
            || &self.policy_ref != expected_policy_ref
            || self.initial_cash != expected_initial_cash
            || self.cost_policy_digest != expected_cost_digest
            || self.rules_digest != expected_rules_digest
        {
            return Err(LabError::DataCorrupt(
                "paper checkpoint immutable identity mismatch".into(),
            ));
        }
        self.engine_state.account.validate()?;
        self.benchmark_account.validate()?;
        if let Some(pending) = &self.pending
            && pending.order.status != OrderStatus::Created
        {
            return Err(LabError::DataCorrupt(
                "paper checkpoint pending order is not in CREATED state".into(),
            ));
        }
        Ok(())
    }

    /// Feed a contiguous pre-start seed ending at the immediately preceding completed boundary
    /// without creating pre-start signals, orders, fills, or marks. An incomplete or stale seed
    /// remains untouched so a later owner-published poll can supply the missing history.
    ///
    /// # Errors
    /// Returns an error if warmup bars violate causal ordering.
    pub fn warm_up_pre_start(
        &mut self,
        decision_bars: &[CandleObservation],
        source_bars: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
        evidence: Option<&EvidenceSnapshot>,
        started_at: UtcTimestamp,
    ) -> Result<(), LabError> {
        if self.last_finalized_candle_at.is_some() || self.evaluator.completed_bars_seen() > 0 {
            return Ok(());
        }
        let max_pre_start = (self.warmup_required_bars as usize).saturating_sub(1);
        if max_pre_start == 0 {
            // Decision indicators need no pre-start bars, but cross-interval
            // indicators still need their own completed source history. Seed
            // those streams without observing a decision candle or emitting
            // any pre-start economic effects.
            let mut source_frame =
                PreparedPaperSourceFrame::new(&self.evaluator, &self.market, source_bars);
            let _ = self.feed_sources_until_prepared(&mut source_frame, started_at)?;
            return Ok(());
        }
        let eligible = decision_bars
            .iter()
            .filter(|bar| bar.candle.completed && bar.candle.close_time_utc <= started_at)
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return Ok(());
        }
        let mut contiguous_start = eligible.len() - 1;
        while contiguous_start > 0 {
            let prev = eligible[contiguous_start - 1];
            let curr = eligible[contiguous_start];
            if prev.candle.close_time_utc == curr.candle.open_time_utc {
                contiguous_start -= 1;
            } else {
                break;
            }
        }
        let contiguous = &eligible[contiguous_start..];
        let expected_last_close = crate::scheduling::latest_completed_boundary(
            started_at,
            self.plan.spec.decision_interval,
        )?;
        if contiguous.len() < max_pre_start
            || contiguous
                .last()
                .is_none_or(|bar| bar.candle.close_time_utc != expected_last_close)
        {
            return Ok(());
        }
        let warmup_slice = &contiguous[contiguous.len() - max_pre_start..];
        let evidence_evaluator = EvidenceEvaluator::new(evidence, self.plan.spec.pit_policy)?;
        let cash = PositionView {
            actual_qty: AssetQuantity::new(Decimal::ZERO)?,
            actual_weight: Weight::new(Decimal::ZERO)?,
            episode_opened_at: None,
            held_decision_bars: None,
        };
        let mut source_frame =
            PreparedPaperSourceFrame::new(&self.evaluator, &self.market, source_bars);
        for bar in warmup_slice {
            if !self.feed_sources_until_prepared(&mut source_frame, bar.candle.close_time_utc)? {
                return Ok(());
            }
            self.evaluator
                .observe(bar, cash, Some(&evidence_evaluator))?;
        }
        let seen = u32::try_from(self.evaluator.completed_bars_seen()).unwrap_or(u32::MAX);
        self.warmup_completed_bars = seen.min(self.warmup_required_bars);
        Ok(())
    }

    /// Fast-forward indicator and cross-interval state across bars that closed while the
    /// session was `PAUSED` without emitting retroactive signals, orders, or fills.
    /// Marks any held inventory at the latest finalized close.
    ///
    /// # Errors
    /// Propagates bar validation or accounting errors.
    pub fn catch_up_paused_bars(
        &mut self,
        decision_bars: &[CandleObservation],
        source_bars: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
        evidence: Option<&EvidenceSnapshot>,
        now_utc: UtcTimestamp,
    ) -> Result<PaperStepOutcome, LabError> {
        let mut staged = self.clone();
        let mut source_frame =
            PreparedPaperSourceFrame::new(&staged.evaluator, &staged.market, source_bars);
        let outcome = staged.catch_up_paused_bars_in_place(
            decision_bars,
            &mut source_frame,
            evidence,
            now_utc,
        )?;
        if outcome.recovery_required.is_none() && staged.recovery_reason.is_none() {
            *self = staged;
        } else if let Some(reason) = staged.recovery_reason {
            self.recovery_reason = Some(reason);
            self.freshness = PaperFreshness::Offline;
        }
        Ok(outcome)
    }

    fn catch_up_paused_bars_in_place(
        &mut self,
        decision_bars: &[CandleObservation],
        source_frame: &mut PreparedPaperSourceFrame<'_>,
        evidence: Option<&EvidenceSnapshot>,
        now_utc: UtcTimestamp,
    ) -> Result<PaperStepOutcome, LabError> {
        let evidence_evaluator = EvidenceEvaluator::new(evidence, self.plan.spec.pit_policy)?;
        if let Some(outcome) =
            self.observe_previous_paused_bar(source_frame, &evidence_evaluator, now_utc)?
        {
            return Ok(outcome);
        }

        let mut events = Vec::new();
        let mut did_advance = false;
        for bar in decision_bars {
            if !bar.candle.completed || bar.candle.close_time_utc > now_utc {
                continue;
            }
            let bar_digest = observation_digest(&bar.candle)?;
            if bar.content_digest != bar_digest {
                return Err(LabError::InputHashMismatch(
                    "paper candle content digest mismatch".into(),
                ));
            }
            if let Some(last_close) = self.last_finalized_candle_at {
                if bar.candle.close_time_utc < last_close {
                    continue;
                }
                if bar.candle.close_time_utc == last_close {
                    if self.last_finalized_candle_digest.as_ref() == Some(&bar_digest) {
                        continue;
                    }
                    return Ok(self.fail_checkpoint(
                        bar,
                        &bar_digest,
                        now_utc,
                        None,
                        format!(
                            "conflicting bar digest at {} for {}",
                            bar.candle.close_time_utc, self.market
                        ),
                    ));
                }
            }
            if let Some(prior) = &self.prior_execution_bar
                && prior.candle.close_time_utc != bar.candle.open_time_utc
            {
                return Ok(self.fail_checkpoint(
                    bar,
                    &bar_digest,
                    now_utc,
                    Some(PaperRecoveryCause::FeedGap),
                    format!(
                        "paused catch-up bar gap on {} between {} and {}",
                        self.market, prior.candle.close_time_utc, bar.candle.open_time_utc
                    ),
                ));
            }
            let source_ready = match self.feed_sources_for_paused_bar(source_frame, bar, now_utc)? {
                PausedSourceFeed::Ready(ready) => ready,
                PausedSourceFeed::Recovery(outcome) => return Ok(outcome),
            };
            let marked = self.engine_state.account.mark(bar.candle.close)?;
            let position = self
                .engine_state
                .position_view(marked.actual_weight, bar.candle.close_time_utc)?;
            if source_ready {
                let _ = self
                    .evaluator
                    .observe(bar, position, Some(&evidence_evaluator))?;
                let seen = u32::try_from(self.evaluator.completed_bars_seen()).unwrap_or(u32::MAX);
                self.warmup_completed_bars = seen.min(self.warmup_required_bars);
            }
            if self.initial_mark.is_some() {
                self.engine_state.push_mark(
                    MarkKind::ExecutionClose,
                    bar.candle.close,
                    bar.id.clone(),
                    self.engine_state.last_target,
                    bar.candle.close_time_utc,
                )?;
                self.mark_benchmark(bar.candle.close)?;
            }
            self.last_finalized_candle_at = Some(bar.candle.close_time_utc);
            self.last_finalized_candle_digest = Some(bar_digest.clone());
            self.last_source_event_at = Some(now_utc);
            self.last_heartbeat_at = Some(now_utc);
            self.prior_passive_liquidity_bar = self.prior_execution_bar.take();
            self.prior_execution_bar = Some(bar.clone());
            // A paused catch-up consumes the candle without producing a
            // retroactive decision, even when source history is not ready.
            self.prior_bar_evaluated = true;
            let event = self.drain_candle_event(bar, bar_digest, now_utc);
            events.push(event);
            did_advance = true;
        }
        self.refresh_freshness(now_utc);
        Ok(PaperStepOutcome {
            events,
            did_advance,
            recovery_required: None,
            recovery_cause: None,
        })
    }

    fn observe_previous_paused_bar(
        &mut self,
        source_frame: &mut PreparedPaperSourceFrame<'_>,
        evidence_evaluator: &EvidenceEvaluator,
        now_utc: UtcTimestamp,
    ) -> Result<Option<PaperStepOutcome>, LabError> {
        if self.prior_bar_evaluated {
            return Ok(None);
        }
        let Some(previous) = self.prior_execution_bar.clone() else {
            return Ok(None);
        };
        let source_ready =
            match self.feed_sources_for_paused_bar(source_frame, &previous, now_utc)? {
                PausedSourceFeed::Ready(ready) => ready,
                PausedSourceFeed::Recovery(outcome) => return Ok(Some(outcome)),
            };
        if source_ready {
            let marked = self.engine_state.account.mark(previous.candle.close)?;
            let position = self
                .engine_state
                .position_view(marked.actual_weight, previous.candle.close_time_utc)?;
            let _ = self
                .evaluator
                .observe(&previous, position, Some(evidence_evaluator))?;
            let seen = u32::try_from(self.evaluator.completed_bars_seen()).unwrap_or(u32::MAX);
            self.warmup_completed_bars = seen.min(self.warmup_required_bars);
        }
        self.prior_bar_evaluated = true;
        Ok(None)
    }

    /// Process one finalized bar in causal order.
    ///
    /// # Errors
    /// Rejects incomplete, future, or out-of-order observations and transitions to
    /// `RecoveryRequired` on conflicting bar digests or non-contiguous gaps.
    pub fn step_bar(
        &mut self,
        bar: &CandleObservation,
        source_bars: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
        evidence: Option<&EvidenceSnapshot>,
        now_utc: UtcTimestamp,
    ) -> Result<PaperStepOutcome, LabError> {
        let mut source_frame =
            PreparedPaperSourceFrame::new(&self.evaluator, &self.market, source_bars);
        self.step_bar_with_prepared_sources(bar, &mut source_frame, evidence, now_utc)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn step_bar_with_prepared_sources(
        &mut self,
        bar: &CandleObservation,
        source_frame: &mut PreparedPaperSourceFrame<'_>,
        evidence: Option<&EvidenceSnapshot>,
        now_utc: UtcTimestamp,
    ) -> Result<PaperStepOutcome, LabError> {
        if !bar.candle.completed {
            return Err(LabError::InvalidConfig(
                "paper session only admits completed candles".into(),
            ));
        }
        if bar.candle.close_time_utc > now_utc {
            return Err(LabError::InvalidConfig(format!(
                "paper session rejected future candle closing at {} > {}",
                bar.candle.close_time_utc, now_utc
            )));
        }
        if bar.candle.market != self.market.code()
            || bar.candle.interval != self.plan.spec.decision_interval
        {
            return Err(LabError::InvalidConfig(format!(
                "paper bar market/interval mismatch for {}",
                self.market
            )));
        }
        let bar_digest = observation_digest(&bar.candle)?;
        if bar.content_digest != bar_digest {
            return Err(LabError::InputHashMismatch(
                "paper candle content digest mismatch".into(),
            ));
        }
        if let Some(last_close) = self.last_finalized_candle_at {
            if bar.candle.close_time_utc < last_close {
                return Err(LabError::Conflict(format!(
                    "out-of-order paper bar close {} < cursor {}",
                    bar.candle.close_time_utc, last_close
                )));
            }
            if bar.candle.close_time_utc == last_close {
                if self.last_finalized_candle_digest.as_ref() == Some(&bar_digest) {
                    self.last_heartbeat_at = Some(now_utc);
                    self.refresh_freshness(now_utc);
                    return Ok(PaperStepOutcome {
                        events: Vec::new(),
                        did_advance: false,
                        recovery_required: None,
                        recovery_cause: None,
                    });
                }
                return Ok(self.fail_checkpoint(
                    bar,
                    &bar_digest,
                    now_utc,
                    None,
                    format!(
                        "conflicting bar digest at {} for {}",
                        bar.candle.close_time_utc, self.market
                    ),
                ));
            }
        }
        if let Some(prior) = &self.prior_execution_bar
            && prior.candle.close_time_utc != bar.candle.open_time_utc
        {
            return Ok(self.fail_checkpoint(
                bar,
                &bar_digest,
                now_utc,
                Some(PaperRecoveryCause::FeedGap),
                format!(
                    "missing finalized bar on {} between {} and {}",
                    self.market, prior.candle.close_time_utc, bar.candle.open_time_utc
                ),
            ));
        }

        let evidence_evaluator = EvidenceEvaluator::new(evidence, self.plan.spec.pit_policy)?;
        let participation_cap = match self.plan.spec.execution {
            ExecutionPolicy::NextBarOpen { participation_cap }
            | ExecutionPolicy::PassiveBuy {
                participation_cap, ..
            } => participation_cap,
        };

        if let Some(prev) = self.prior_execution_bar.clone() {
            let now = bar.candle.open_time_utc;
            if self
                .pending
                .as_ref()
                .is_some_and(|order| order.order.expires_at < now)
            {
                let expired = self
                    .pending
                    .take()
                    .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
                self.engine_state.expire_pending(&expired, now)?;
            }
            if self
                .pending
                .as_ref()
                .is_some_and(|order| order.order.order_type == SimulatedOrderType::PassiveBuyLimit)
                && let Some(passive_liq) = &self.prior_passive_liquidity_bar
            {
                self.engine_state.process_passive_close(
                    &mut self.pending,
                    &prev,
                    passive_liq,
                    participation_cap,
                    &self.plan,
                )?;
            }
            if self.evaluator.completed_bars_seen() >= u64::from(self.warmup_required_bars) {
                self.engine_state.push_mark(
                    MarkKind::ExecutionClose,
                    prev.candle.close,
                    prev.id.clone(),
                    self.engine_state.last_target,
                    now,
                )?;
                self.mark_benchmark(prev.candle.close)?;
            }
            self.engine_state.process_pending_at(
                &mut self.pending,
                bar,
                &prev,
                participation_cap,
                &self.plan,
                false,
            )?;

            if !self.prior_bar_evaluated {
                let source_ready =
                    self.feed_sources_until_prepared(source_frame, prev.candle.close_time_utc)?;
                if source_ready {
                    let marked = self.engine_state.account.mark(prev.candle.close)?;
                    let position = self.engine_state.position_view(marked.actual_weight, now)?;
                    let evaluation =
                        self.evaluator
                            .observe(&prev, position, Some(&evidence_evaluator))?;
                    let seen =
                        u32::try_from(self.evaluator.completed_bars_seen()).unwrap_or(u32::MAX);
                    self.warmup_completed_bars = seen.min(self.warmup_required_bars);

                    if let Some(evaluation) = evaluation {
                        let expiry = self.evaluator.signal_expiry(
                            now,
                            self.plan.spec.decision_interval,
                            self.plan.spec.range.end(),
                        )?;
                        let signal_index = self.engine_state.push_signal(
                            &prev,
                            &evaluation,
                            position.actual_weight,
                            expiry,
                        )?;
                        let target = evaluation.constrained_target_weight;
                        if self
                            .pending
                            .as_ref()
                            .is_some_and(|order| order.target != target)
                        {
                            let cancelled = self.pending.take().ok_or_else(|| {
                                LabError::Internal("pending order disappeared".into())
                            })?;
                            self.engine_state.cancel_pending(
                                &cancelled,
                                now,
                                ReasonCode::Cancelled,
                            )?;
                        }
                        if self.pending.is_none() && target != position.actual_weight {
                            let tradable = self.plan.spec.rules_at(now)?.tradable_at(now);
                            let effective_at = add_millis(now, self.plan.spec.latency_ms)?;
                            let order_expiry = self.compute_order_expiry(
                                target > position.actual_weight,
                                now,
                                effective_at,
                                expiry,
                            )?;
                            if tradable {
                                let position_value = checked::mul(
                                    self.engine_state.account.qty(),
                                    prev.candle.close.get(),
                                    "reference position",
                                )?;
                                let reference_equity = checked::add(
                                    self.engine_state.account.cash_free()?,
                                    position_value,
                                    "reference equity",
                                )?;
                                let target_value =
                                    checked::mul(reference_equity, target.get(), "target value")?;
                                let requested_notional =
                                    checked::sub(target_value, position_value, "target gap")?.abs();
                                self.pending = self.engine_state.create_pending(
                                    signal_index,
                                    target,
                                    &prev,
                                    &prev,
                                    requested_notional,
                                    effective_at,
                                    order_expiry,
                                    &self.plan,
                                )?;
                            } else {
                                self.engine_state.push_rejected_order(
                                    signal_index,
                                    now,
                                    effective_at,
                                    order_expiry,
                                    ReasonCode::RuleBlocked,
                                    &self.plan,
                                )?;
                                self.engine_state.signals[signal_index].outcome =
                                    SignalOutcome::RuleBlocked;
                            }
                        } else if self.pending.is_some() {
                            self.engine_state.signals[signal_index].outcome =
                                SignalOutcome::OrderUnfilled;
                        }
                        self.engine_state.last_target = target;
                        self.engine_state.process_pending_at(
                            &mut self.pending,
                            bar,
                            &prev,
                            participation_cap,
                            &self.plan,
                            true,
                        )?;
                        self.init_or_mark_benchmark(bar, &prev, participation_cap)?;
                    }
                }
            }
        } else if self.initial_mark.is_none() {
            self.engine_state.push_mark(
                MarkKind::Initial,
                bar.candle.open,
                bar.id.clone(),
                Weight::new(Decimal::ZERO)?,
                bar.candle.open_time_utc,
            )?;
            if let Some(mark) = self.engine_state.marks.pop() {
                self.initial_mark = Some(mark.clone());
                self.latest_mark = Some(mark);
            }
            self.engine_state.record_count = 0;
        }

        // Determine whether `bar` itself is still a pre-evaluation warmup bar or an eligible decision bar.
        let next_seen = self.evaluator.completed_bars_seen().saturating_add(1);
        let bar_evaluated = if next_seen < u64::from(self.warmup_required_bars) {
            let cash = PositionView {
                actual_qty: AssetQuantity::new(Decimal::ZERO)?,
                actual_weight: Weight::new(Decimal::ZERO)?,
                episode_opened_at: None,
                held_decision_bars: None,
            };
            if self.feed_sources_until_prepared(source_frame, bar.candle.close_time_utc)? {
                self.evaluator
                    .observe(bar, cash, Some(&evidence_evaluator))?;
                let seen = u32::try_from(self.evaluator.completed_bars_seen()).unwrap_or(u32::MAX);
                self.warmup_completed_bars = seen.min(self.warmup_required_bars);
                true
            } else {
                false
            }
        } else {
            false
        };

        self.last_finalized_candle_at = Some(bar.candle.close_time_utc);
        self.last_finalized_candle_digest = Some(bar_digest.clone());
        self.last_source_event_at = Some(now_utc);
        self.last_heartbeat_at = Some(now_utc);
        self.prior_passive_liquidity_bar = self.prior_execution_bar.take();
        self.prior_execution_bar = Some(bar.clone());
        self.prior_bar_evaluated = bar_evaluated;

        let event = self.drain_candle_event(bar, bar_digest, now_utc);
        self.refresh_freshness(now_utc);
        Ok(PaperStepOutcome {
            events: vec![event],
            did_advance: true,
            recovery_required: None,
            recovery_cause: None,
        })
    }

    /// Cancel any pending virtual order on `PAUSE` or `STOP`, releasing reserved cash,
    /// and return the cancelled order records.
    ///
    /// # Errors
    /// Propagates accounting or order-transition errors.
    pub fn cancel_pending_for_lifecycle(
        &mut self,
        at: UtcTimestamp,
    ) -> Result<Vec<OrderRecord>, LabError> {
        if let Some(pending) = self.pending.take() {
            self.engine_state
                .cancel_pending(&pending, at, ReasonCode::Cancelled)?;
        }
        self.engine_state.orders.clear();
        let cancelled = std::mem::take(&mut self.engine_state.order_events);
        self.engine_state.record_count = 0;
        Ok(cancelled)
    }

    /// Update the market freshness status relative to `now_utc`.
    pub fn refresh_freshness(&mut self, now_utc: UtcTimestamp) {
        if self.recovery_reason.is_some() {
            self.freshness = PaperFreshness::Offline;
            return;
        }
        let Some(last_close) = self.last_finalized_candle_at else {
            self.freshness = PaperFreshness::Offline;
            return;
        };
        let age = now_utc.0 - last_close.0;
        let interval = self.plan.spec.decision_interval.duration();
        if age <= interval + interval / 2 {
            self.freshness = PaperFreshness::Live;
        } else if age <= interval * 2 {
            self.freshness = PaperFreshness::Delayed;
        } else if age <= interval * 4 {
            self.freshness = PaperFreshness::Stale;
        } else {
            self.freshness = PaperFreshness::Offline;
        }
    }

    /// Project the checkpoint into the wire [`PaperMarketSnapshot`].
    ///
    /// # Errors
    /// Propagates accounting state or decimal arithmetic errors.
    pub fn to_market_snapshot(&self) -> Result<PaperMarketSnapshot, LabError> {
        let account = self.engine_state.account.state()?;
        let (mark_price, equity, unrealized_pnl, drawdown_amount, drawdown_fraction, last_mark_at) =
            if let Some(mark) = &self.latest_mark {
                let dd_amount = QuoteAmount::new(checked::sub(
                    mark.peak_equity.get(),
                    mark.equity.get(),
                    "paper market drawdown amount",
                )?)?;
                (
                    Some(mark.mark_price),
                    Some(mark.equity),
                    Some(mark.gross_unrealized),
                    Some(dd_amount),
                    Some(mark.drawdown),
                    Some(mark.context.accounting_event_time),
                )
            } else {
                (None, None, None, None, None, None)
            };
        Ok(PaperMarketSnapshot {
            market: self.market.clone(),
            model_id: self.model_id.clone(),
            initial_cash: self.initial_cash,
            cash_total: account.cash_total,
            cash_free: account.cash_free,
            cash_reserved: account.cash_reserved,
            quantity: account.qty,
            mark_price,
            equity,
            realized_pnl: account.gross_realized,
            unrealized_pnl,
            total_fees: account.cumulative_fees,
            drawdown_amount,
            drawdown_fraction,
            signal_count: self.signal_count,
            fill_count: self.fill_count,
            event_count: self.market_event_count,
            last_source_event_at: self.last_source_event_at,
            last_finalized_candle_at: self.last_finalized_candle_at,
            last_account_mark_at: last_mark_at,
            last_heartbeat_at: self.last_heartbeat_at,
            freshness: self.freshness,
            benchmark: self.benchmark.clone(),
            benchmark_null_reason: self.benchmark_null_reason,
            verification: default_paper_verification(),
            costs: self.costs.clone(),
            market_rules: self.market_rules.clone(),
        })
    }

    fn feed_sources_for_paused_bar(
        &mut self,
        source_frame: &mut PreparedPaperSourceFrame<'_>,
        bar: &CandleObservation,
        now_utc: UtcTimestamp,
    ) -> Result<PausedSourceFeed, LabError> {
        match self.feed_sources_until_prepared(source_frame, bar.candle.close_time_utc) {
            Ok(ready) => Ok(PausedSourceFeed::Ready(ready)),
            Err(LabError::DataGap(reason)) => {
                let digest = observation_digest(&bar.candle)?;
                Ok(PausedSourceFeed::Recovery(self.fail_checkpoint(
                    bar,
                    &digest,
                    now_utc,
                    Some(PaperRecoveryCause::FeedGap),
                    format!("DATA_GAP: {reason}"),
                )))
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn feed_sources_until(
        &mut self,
        source_bars: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
        decision_close: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let mut source_frame =
            PreparedPaperSourceFrame::new(&self.evaluator, &self.market, source_bars);
        self.feed_sources_until_prepared(&mut source_frame, decision_close)
    }

    fn feed_sources_until_prepared(
        &mut self,
        source_frame: &mut PreparedPaperSourceFrame<'_>,
        decision_close: UtcTimestamp,
    ) -> Result<bool, LabError> {
        if source_frame.requirements.is_empty() {
            return Ok(true);
        }

        let mut plans = Vec::with_capacity(source_frame.requirements.len());
        let mut cold_deferred = false;
        for (interval, required_bars) in &source_frame.requirements {
            let market_code = source_frame.market_code.as_str();
            let stream = source_frame.streams.get_mut(interval).ok_or_else(|| {
                LabError::DataCorrupt(format!(
                    "prepared paper source stream missing for {interval:?}"
                ))
            })?;
            if stream.indicator_ids.is_empty() {
                return Err(LabError::DataCorrupt(format!(
                    "source requirement {interval:?} has no declared indicators"
                )));
            }
            let ordered_end = stream.prefix_until(market_code, *interval, decision_close)?;
            let ordered_prefix = &stream.ordered_prefix[..ordered_end];
            if let Some(plan) = self.plan_source_interval(
                *interval,
                *required_bars,
                stream,
                ordered_prefix,
                decision_close,
            )? {
                plans.push(plan);
            } else {
                cold_deferred = true;
            }
        }
        if cold_deferred {
            return Ok(false);
        }

        for plan in plans {
            let indicator_ids = &source_frame
                .streams
                .get(&plan.interval)
                .ok_or_else(|| {
                    LabError::DataCorrupt(format!(
                        "prepared paper source stream missing for {:?}",
                        plan.interval
                    ))
                })?
                .indicator_ids;
            for planned_bar in plan.bars {
                for indicator_id in indicator_ids {
                    self.evaluator
                        .observe_source(indicator_id, planned_bar.bar)?;
                }
                let cursor =
                    self.source_cursors
                        .entry(plan.interval)
                        .or_insert_with(|| PaperSourceCursor {
                            interval: plan.interval,
                            last_bar_close_utc: None,
                            last_bar_digest: None,
                            completed_bars: 0,
                        });
                cursor.last_bar_close_utc = Some(planned_bar.bar.candle.close_time_utc);
                cursor.last_bar_digest = Some(planned_bar.digest);
                cursor.completed_bars = cursor.completed_bars.saturating_add(1);
            }
        }

        for (interval, required_bars) in &source_frame.requirements {
            let expected = crate::scheduling::latest_completed_boundary(decision_close, *interval)?;
            let Some(cursor) = self.source_cursors.get(interval) else {
                return Ok(false);
            };
            if cursor.last_bar_close_utc != Some(expected) {
                return Err(LabError::DataGap(format!(
                    "paper source cursor for {interval:?} does not reach completed boundary {expected}"
                )));
            }
            let required_bars = u64::try_from(*required_bars)
                .map_err(|_| LabError::ResourceLimit("source warmup count overflow".into()))?;
            if cursor.completed_bars < required_bars {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one source interval plan keeps cursor, warmup and successor checks in their original error order"
    )]
    fn plan_source_interval<'a>(
        &self,
        interval: CandleInterval,
        required_bars: usize,
        stream: &PreparedPaperSourceStream<'a>,
        ordered_prefix: &[usize],
        decision_close: UtcTimestamp,
    ) -> Result<Option<PlannedPaperSourceFeed<'a>>, LabError> {
        let expected = crate::scheduling::latest_completed_boundary(decision_close, interval)?;
        let cursor = self.source_cursors.get(&interval);
        let last_close = cursor.and_then(|cursor| cursor.last_bar_close_utc);
        if let Some(cursor) = cursor
            && (cursor.interval != interval
                || (cursor.last_bar_close_utc.is_none() != cursor.last_bar_digest.is_none())
                || (cursor.last_bar_close_utc.is_none() && cursor.completed_bars != 0)
                || (cursor.last_bar_close_utc.is_some() && cursor.completed_bars == 0))
        {
            return Err(LabError::DataCorrupt(format!(
                "paper source cursor integrity mismatch for {interval:?}"
            )));
        }
        let last_fed = self.evaluator.cross_last_close();
        if stream
            .indicator_ids
            .iter()
            .any(|indicator_id| last_fed.get(indicator_id).copied() != last_close)
        {
            return Err(LabError::DataCorrupt(format!(
                "paper source evaluator/cursor mismatch for {interval:?}"
            )));
        }

        let Some(last_close) = last_close else {
            let Some(last_index) = ordered_prefix.last().copied() else {
                return Ok(None);
            };
            if stream.rows_by_close[last_index].bar.candle.close_time_utc != expected {
                return Ok(None);
            }
            let mut contiguous_start = ordered_prefix.len() - 1;
            while contiguous_start > 0 {
                let previous = &stream.rows_by_close[ordered_prefix[contiguous_start - 1]].bar;
                let current = &stream.rows_by_close[ordered_prefix[contiguous_start]].bar;
                if previous.candle.close_time_utc == current.candle.open_time_utc {
                    contiguous_start -= 1;
                } else {
                    break;
                }
            }
            let suffix = &ordered_prefix[contiguous_start..];
            if suffix.len() < required_bars {
                return Ok(None);
            }
            let bars = suffix[suffix.len() - required_bars..]
                .iter()
                .map(|row_index| stream.planned_row(*row_index))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Some(PlannedPaperSourceFeed { interval, bars }));
        };

        if last_close > expected {
            return Err(LabError::DataCorrupt(format!(
                "paper source cursor for {interval:?} is ahead of completed boundary {expected}"
            )));
        }
        if let Some(cursor) = cursor {
            let cursor_position = ordered_prefix.partition_point(|row_index| {
                stream.rows_by_close[*row_index].bar.candle.close_time_utc < last_close
            });
            if let Some(row_index) = ordered_prefix.get(cursor_position)
                && stream.rows_by_close[*row_index].bar.candle.close_time_utc == last_close
            {
                let digest = stream.rows_by_close[*row_index]
                    .digest
                    .as_ref()
                    .ok_or_else(|| {
                        LabError::Internal("validated paper source digest missing".into())
                    })?;
                if Some(digest) != cursor.last_bar_digest.as_ref() {
                    return Err(LabError::InputHashMismatch(format!(
                        "paper source row conflicts with persisted {interval:?} cursor"
                    )));
                }
            }
        }

        let mut expected_open = last_close;
        let successor_start = ordered_prefix.partition_point(|row_index| {
            stream.rows_by_close[*row_index].bar.candle.close_time_utc <= last_close
        });
        let mut successors = Vec::with_capacity(ordered_prefix.len() - successor_start);
        for row_index in &ordered_prefix[successor_start..] {
            let source_row = &stream.rows_by_close[*row_index];
            let bar = source_row.bar;
            if bar.candle.close_time_utc > expected {
                return Err(LabError::DataCorrupt(format!(
                    "paper source row for {interval:?} exceeds completed boundary {expected}"
                )));
            }
            if bar.candle.open_time_utc != expected_open {
                return Err(LabError::DataGap(format!(
                    "paper source gap for {interval:?} after {expected_open}"
                )));
            }
            expected_open = bar.candle.close_time_utc;
            successors.push(stream.planned_row(*row_index)?);
        }
        if expected_open != expected {
            return Err(LabError::DataGap(format!(
                "paper source stream {interval:?} does not contain completed boundary {expected}"
            )));
        }
        Ok(Some(PlannedPaperSourceFeed {
            interval,
            bars: successors,
        }))
    }

    fn compute_order_expiry(
        &self,
        is_buy: bool,
        decision_close: UtcTimestamp,
        effective_at: UtcTimestamp,
        signal_expiry: UtcTimestamp,
    ) -> Result<UtcTimestamp, LabError> {
        if !is_buy || !matches!(self.plan.spec.execution, ExecutionPolicy::PassiveBuy { .. }) {
            return Ok(signal_expiry);
        }
        let duration = self.plan.spec.execution_resolution.duration();
        let next_close = if effective_at <= decision_close {
            decision_close
                .0
                .checked_add_signed(duration)
                .map(UtcTimestamp)
                .ok_or_else(|| LabError::InvalidConfig("passive expiry overflow".into()))?
        } else {
            decision_close
                .0
                .checked_add_signed(duration * 2)
                .map(UtcTimestamp)
                .ok_or_else(|| LabError::InvalidConfig("passive expiry overflow".into()))?
        };
        Ok(next_close.min(signal_expiry))
    }

    fn init_or_mark_benchmark(
        &mut self,
        execution_bar: &CandleObservation,
        liquidity_source: &CandleObservation,
        participation_cap: Weight,
    ) -> Result<(), LabError> {
        if self.benchmark_base_observation_id.is_none() {
            self.benchmark_base_observation_id = Some(execution_bar.id.clone());
            let now = execution_bar.candle.open_time_utc;
            let rules = self.plan.spec.rules_at(now)?;
            let req = plan_market_request(
                &self.benchmark_account,
                Weight::new(Decimal::ONE)?,
                liquidity_source.candle.close,
                &self.costs,
                rules,
            )?;
            if let Some(planned_req) = req.fill {
                self.benchmark_account.reserve(planned_req.reserved_cash)?;
                let arrival = plan_market_arrival(
                    &self.benchmark_account,
                    &planned_req,
                    execution_bar.candle.open,
                    &self.costs,
                    rules,
                    liquidity_source.candle.volume,
                    participation_cap,
                )?;
                self.benchmark_account.release(planned_req.reserved_cash)?;
                if let Some(fill) = arrival.fill {
                    self.benchmark_account.reserve(fill.reserved_cash)?;
                    self.benchmark_account.apply_fill(
                        Side::Buy,
                        fill.price,
                        fill.qty,
                        fill.fee,
                        fill.reserved_cash,
                    )?;
                }
            }
            self.mark_benchmark(execution_bar.candle.open)?;
        } else if let Some(last_mark) = self.engine_state.marks.last() {
            self.mark_benchmark(last_mark.mark_price)?;
        }
        Ok(())
    }

    fn mark_benchmark(&mut self, mark_price: PriceKrw) -> Result<(), LabError> {
        let Some(base_id) = &self.benchmark_base_observation_id else {
            self.benchmark = None;
            self.benchmark_null_reason = Some(PaperNullReason::InsufficientHistory);
            return Ok(());
        };
        let marked = self.benchmark_account.mark(mark_price)?;
        let pnl = checked::sub(
            marked.equity,
            self.initial_cash.get(),
            "paper benchmark pnl",
        )?;
        let return_fraction = checked::div(
            pnl,
            self.initial_cash.get(),
            "paper benchmark return fraction",
        )?;
        self.benchmark = Some(PaperBenchmarkSnapshot {
            kind: PaperBenchmarkKind::BuyAndHold,
            initial_cash: self.initial_cash,
            equity: QuoteAmount::new(marked.equity)?,
            pnl: SignedAmount::new(pnl)?,
            return_fraction: SignedAmount::new(return_fraction)?,
            base_observation_id: base_id.clone(),
        });
        self.benchmark_null_reason = None;
        Ok(())
    }

    fn drain_candle_event(
        &mut self,
        bar: &CandleObservation,
        content_digest: ContentHash,
        source_event_at: UtcTimestamp,
    ) -> PendingPaperEvent {
        let signals = std::mem::take(&mut self.engine_state.signals);
        let orders = std::mem::take(&mut self.engine_state.orders);
        let order_events = std::mem::take(&mut self.engine_state.order_events);
        let fills = std::mem::take(&mut self.engine_state.fills);
        let account_marks = std::mem::take(&mut self.engine_state.marks);
        let episodes = std::mem::take(&mut self.engine_state.episodes);
        self.engine_state.record_count = 0;

        self.signal_count = self
            .signal_count
            .saturating_add(u64::try_from(signals.len()).unwrap_or(0));
        self.fill_count = self
            .fill_count
            .saturating_add(u64::try_from(fills.len()).unwrap_or(0));
        self.market_event_count = self.market_event_count.saturating_add(1);
        for mark in &account_marks {
            if mark.kind == MarkKind::Initial && self.initial_mark.is_none() {
                self.initial_mark = Some(mark.clone());
            }
            self.latest_mark = Some(mark.clone());
        }

        PendingPaperEvent {
            kind: PaperEventKind::CandleProcessed,
            market: Some(self.market.clone()),
            candle: Some(PaperCandleReference {
                observation_id: bar.id.clone(),
                interval: bar.candle.interval,
                open_time: bar.candle.open_time_utc,
                close_time: bar.candle.close_time_utc,
                content_digest,
            }),
            source_event_at: Some(source_event_at),
            finalized_at: Some(bar.candle.close_time_utc),
            signals,
            orders,
            order_events,
            fills,
            account_marks,
            episodes,
        }
    }

    fn fail_checkpoint(
        &mut self,
        bar: &CandleObservation,
        content_digest: &ContentHash,
        source_event_at: UtcTimestamp,
        recovery_cause: Option<PaperRecoveryCause>,
        reason: String,
    ) -> PaperStepOutcome {
        self.freshness = PaperFreshness::Offline;
        self.recovery_reason = Some(reason.clone());
        self.market_event_count = self.market_event_count.saturating_add(1);
        PaperStepOutcome {
            events: vec![PendingPaperEvent {
                kind: PaperEventKind::RecoveryRequired,
                market: Some(self.market.clone()),
                candle: Some(PaperCandleReference {
                    observation_id: bar.id.clone(),
                    interval: bar.candle.interval,
                    open_time: bar.candle.open_time_utc,
                    close_time: bar.candle.close_time_utc,
                    content_digest: content_digest.clone(),
                }),
                source_event_at: Some(source_event_at),
                finalized_at: Some(bar.candle.close_time_utc),
                signals: Vec::new(),
                orders: Vec::new(),
                order_events: Vec::new(),
                fills: Vec::new(),
                account_marks: Vec::new(),
                episodes: Vec::new(),
            }],
            did_advance: true,
            recovery_required: Some(reason),
            recovery_cause,
        }
    }
}

/// Materialize a numbered [`PaperSessionEvent`] from a [`PendingPaperEvent`].
#[must_use]
pub fn materialize_candle_event(sequence: u64, pending: PendingPaperEvent) -> PaperSessionEvent {
    PaperSessionEvent {
        sequence,
        kind: pending.kind,
        market: pending.market,
        candle: pending.candle,
        source_event_at: pending.source_event_at,
        finalized_at: pending.finalized_at,
        control: None,
        signals: pending.signals,
        orders: pending.orders,
        order_events: pending.order_events,
        fills: pending.fills,
        account_marks: pending.account_marks,
        episodes: pending.episodes,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_single_market_plan(
    session_id: &PaperSessionId,
    market: &MarketId,
    model_id: &ModelId,
    policy_revision: &FrozenPolicyRevision,
    decision_interval: CandleInterval,
    execution_resolution: CandleInterval,
    latency_ms: u64,
    execution: ExecutionPolicy,
    initial_cash: QuoteAmount,
    costs: CostPolicy,
    market_rules: &PaperMarketRuleScenario,
    started_at: UtcTimestamp,
) -> Result<ResolvedPlan, LabError> {
    let aligned_start_secs = (started_at.0.timestamp() - 365 * 86_400)
        .div_euclid(decision_interval.duration().num_seconds())
        * decision_interval.duration().num_seconds();
    let aligned_start = UtcTimestamp(
        Utc.timestamp_opt(aligned_start_secs, 0)
            .single()
            .ok_or_else(|| LabError::InvalidConfig("invalid paper start timestamp".into()))?,
    );
    let far_end = UtcTimestamp(
        Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0)
            .single()
            .ok_or_else(|| LabError::Internal("invalid paper horizon timestamp".into()))?,
    );
    let range = UtcRange::new(aligned_start, far_end)?;
    let PaperMarketRuleScenario::SimulatedScenario {
        min_notional,
        quantity_step,
        ticks,
        assumption_label,
    } = market_rules.clone();
    let rules_snapshot = MarketRuleSnapshot {
        id: RuleSnapshotId::new("rule-paper-simulated-scenario")?,
        provenance: RuleProvenance::ExplicitScenario,
        valid_range: range,
        observed_at: started_at,
        source_refs: vec!["paper_session_write:simulated_scenario".into()],
        assumption_label,
        min_notional,
        quantity_step,
        ticks,
        fee_schedule: None,
        trading_state: Some(MarketTradingState::Active),
        maintenance_windows: Vec::new(),
    };
    let spec = ExperimentSpec {
        schema_version: "3.0".into(),
        dataset_ids: vec![DatasetId::new("dataset-paper-live")?],
        markets: vec![market.clone()],
        range,
        strategies: Vec::new(),
        policy_selections: vec![policy_revision.reference.clone()],
        causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
        capital_mode: Some(CapitalMode::IndependentModels),
        decision_interval,
        execution_resolution,
        latency_ms,
        initial_cash,
        costs,
        execution,
        market_rules: rules_snapshot,
        market_rules_history: Vec::new(),
        terminal_policy: TerminalPolicy::MarkToMarket,
        evidence_snapshot_id: None,
        pit_policy: PitPolicy::StrictPit,
        evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 30,
            risk_free_annual: 0.0,
        },
        seed: 0,
    };
    spec.validate()?;
    let config_digest = semantic_digest("paper_plan_config_v1", &spec)?;
    let input_digest = semantic_digest(
        "paper_plan_input_v1",
        &(session_id.as_str(), market.code(), &config_digest),
    )?;
    let plan_id = PlanId::new(format!(
        "plan-{}-{}",
        session_id.as_str(),
        market.code().to_ascii_lowercase()
    ))?;
    let admission = ModelAdmission {
        model_id: model_id.clone(),
        market: market.clone(),
        strategy: policy_revision.family,
        policy_ref: Some(policy_revision.reference.clone()),
        status: AdmissionStatus::Eligible,
        reasons: Vec::new(),
    };
    Ok(ResolvedPlan {
        id: plan_id,
        spec,
        config_digest,
        input_digest,
        dataset_digests: Vec::new(),
        evidence_digest: None,
        admissions: vec![admission],
        policy_revisions: vec![policy_revision.clone()],
        warnings: Vec::new(),
        estimated_events: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        BoolExpr, CandleRecord, CompareOp, DeleteBlockerClass, DeleteResource, NumericExpr,
        PaperCommandOutcome, PaperMarketStart, PaperSessionState, PaperSessionView,
        PaperSessionWrite, PolicyDefinition, PolicyId, PolicyIndicator, PolicyIndicatorKind,
        PolicyOrigin, PolicyProgram, PolicyRevisionId, PolicyRule, PolicySignalExpiry,
        PolicyTarget, PolicyWrite, RulesProgram, StateParameters, StrategyKind, StrategySpec,
    };

    fn dec(value: &str) -> Decimal {
        value.parse().expect("valid decimal")
    }

    fn ts(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("valid timestamp")
    }

    fn candle_obs(
        _label: &str,
        market: &str,
        interval: CandleInterval,
        open_at: &str,
        open: &str,
        close: &str,
    ) -> CandleObservation {
        let open_time = ts(open_at);
        let close_time = UtcTimestamp(open_time.0 + interval.duration());
        let open_price = PriceKrw::new(dec(open)).expect("valid open");
        let close_price = PriceKrw::new(dec(close)).expect("valid close");
        let candle = CandleRecord {
            market: market.into(),
            interval,
            open_time_utc: open_time,
            close_time_utc: close_time,
            open: open_price,
            high: PriceKrw::new(open_price.get().max(close_price.get())).expect("valid high"),
            low: PriceKrw::new(open_price.get().min(close_price.get())).expect("valid low"),
            close: close_price,
            volume: AssetQuantity::new(dec("100")).expect("valid volume"),
            quote_turnover: QuoteAmount::new(dec("10000000")).expect("valid turnover"),
            completed: true,
        };
        let digest = observation_digest(&candle).expect("valid digest");
        CandleObservation {
            id: ObservationId::from_seed(digest.as_str()),
            candle,
            content_digest: digest,
            raw_object_ids: Vec::new(),
            constituent_ids: Vec::new(),
        }
    }

    fn candle_obs_at(
        id: &str,
        market: &str,
        interval: CandleInterval,
        open_at: UtcTimestamp,
        open: &str,
        close: &str,
    ) -> CandleObservation {
        let open_at = open_at.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        candle_obs(id, market, interval, &open_at, open, close)
    }

    fn sample_s1_revision() -> FrozenPolicyRevision {
        let strategy = StrategySpec::S1 {
            state: StateParameters {
                ema_length: 2,
                vol_length: 2,
                k: 0.0,
            },
        };
        let definition = PolicyDefinition {
            schema_version: "1.0".into(),
            name: "s1_paper_test".into(),
            description: "SYNTHETIC_PAPER_CONTINUATION_TEST".into(),
            program: PolicyProgram::Builtin { strategy },
        };
        let digest = ContentHash::of_value(&definition).expect("definition digest");
        FrozenPolicyRevision {
            reference: PolicyRevisionRef {
                policy_id: PolicyId::new("policy-s1-paper").expect("policy id"),
                revision_id: PolicyRevisionId::new("policy-rev-s1-paper-r1").expect("rev id"),
                definition_digest: digest,
            },
            revision_number: 1,
            parent_revision_id: None,
            family: StrategyKind::S1,
            origin: PolicyOrigin::Builtin,
            definition,
        }
    }

    fn sample_s1_revision_with_warmup(
        ema_length: usize,
        vol_length: usize,
    ) -> FrozenPolicyRevision {
        let mut revision = sample_s1_revision();
        let PolicyProgram::Builtin {
            strategy: StrategySpec::S1 { state },
        } = &mut revision.definition.program
        else {
            unreachable!("sample policy is S1")
        };
        state.ema_length = ema_length;
        state.vol_length = vol_length;
        revision.reference.definition_digest =
            ContentHash::of_value(&revision.definition).expect("updated definition digest");
        revision
    }

    fn constant_bars(
        prefix: &str,
        start_open: UtcTimestamp,
        count: usize,
        interval: CandleInterval,
        price: &str,
    ) -> Vec<CandleObservation> {
        (0..count)
            .map(|index| {
                candle_obs_at(
                    &format!("{prefix}-{index}"),
                    "KRW-BTC",
                    interval,
                    UtcTimestamp(
                        start_open.0
                            + interval.duration()
                                * i32::try_from(index).expect("fixture count fits i32"),
                    ),
                    price,
                    price,
                )
            })
            .collect()
    }

    fn synthetic_store_batch(
        store: &crate::storage::Store,
        mut bars: Vec<CandleObservation>,
        recorded_at: UtcTimestamp,
        label: &str,
    ) -> Result<(crate::contracts::RawObjectRef, Vec<CandleObservation>), LabError> {
        let published = store.publish_raw(crate::storage::RawObjectInput {
            source_url: "http://127.0.0.1/synthetic-paper-test".into(),
            fetched_at: recorded_at,
            persisted_at: recorded_at,
            http_status: 200,
            remaining_req: Some("synthetic fixture".into()),
            origin: crate::contracts::MarketDataOrigin::SyntheticTestOnly,
            body: format!("PAPER_SESSION_TEST_ONLY {label}").into_bytes(),
        })?;
        for bar in &mut bars {
            bar.raw_object_ids = vec![published.object.id.clone()];
        }
        Ok((published.object, bars))
    }

    fn multi_market_start_write(
        request_id: &str,
        policy_ref: PolicyRevisionRef,
    ) -> Result<PaperSessionWrite, LabError> {
        let markets = ["KRW-BTC", "KRW-XRP"]
            .into_iter()
            .map(|market| {
                Ok(PaperMarketStart {
                    market: MarketId::parse_upbit(market)?,
                    initial_cash: QuoteAmount::new(dec("1000000"))?,
                    costs: zero_cost_policy(),
                    market_rules: simple_rules(),
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        Ok(PaperSessionWrite::Start {
            request_id: RequestId::new(request_id)?,
            policy_ref,
            decision_interval: CandleInterval::M1,
            execution_resolution: CandleInterval::M1,
            latency_ms: 0,
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            markets,
        })
    }

    fn multi_market_h1_history(
        market: &str,
        prefix: &str,
        first_open: UtcTimestamp,
        base_price: i64,
    ) -> Vec<CandleObservation> {
        (0..4_i64)
            .map(|index| {
                let open_at = UtcTimestamp(first_open.0 + chrono::Duration::hours(index));
                let price = (base_price + index * 100).to_string();
                candle_obs_at(
                    &format!("{prefix}-{index}"),
                    market,
                    CandleInterval::H1,
                    open_at,
                    &price,
                    &price,
                )
            })
            .collect()
    }

    fn multi_market_m1(
        label: &str,
        market: &str,
        open_at: UtcTimestamp,
        open_price: i64,
    ) -> CandleObservation {
        let open = open_price.to_string();
        let close = (open_price + 100).to_string();
        candle_obs_at(label, market, CandleInterval::M1, open_at, &open, &close)
    }

    struct MultiMarketRecoveryFetch {
        requirements: Vec<crate::storage::PaperPollRequirement>,
        batches: Vec<(crate::contracts::RawObjectRef, Vec<CandleObservation>)>,
    }

    fn multi_market_feed_gap_batches(
        store: &mut crate::storage::Store,
        session_id: &PaperSessionId,
        expected_sequence: u64,
        now: UtcTimestamp,
        leave_xrp_gap: bool,
        label: &str,
    ) -> Result<MultiMarketRecoveryFetch, LabError> {
        let requirements = store
            .paper_recovery_poll_requirements(session_id, expected_sequence)?
            .ok_or_else(|| LabError::Internal("feed-gap recovery requirements missing".into()))?;
        let started_at = ts("2024-01-01T04:00:00Z");
        let first_h1_open = UtcTimestamp(started_at.0 - chrono::Duration::hours(4));
        let m1_first_missing = UtcTimestamp(started_at.0 + chrono::Duration::minutes(3));
        let m1_after_gap = UtcTimestamp(started_at.0 + chrono::Duration::minutes(4));
        let m1_latest = UtcTimestamp(started_at.0 + chrono::Duration::minutes(5));
        let mut batches = Vec::with_capacity(requirements.len());
        for requirement in &requirements {
            let market = requirement.market.code();
            let bars = match (market.as_str(), requirement.interval) {
                ("KRW-BTC", CandleInterval::M1) => vec![
                    multi_market_m1("btc-recovery-1", market.as_str(), m1_first_missing, 100_300),
                    multi_market_m1("btc-recovery-2", market.as_str(), m1_after_gap, 100_400),
                    multi_market_m1("btc-recovery-3", market.as_str(), m1_latest, 100_500),
                ],
                ("KRW-XRP", CandleInterval::M1) if leave_xrp_gap => vec![
                    multi_market_m1("xrp-recovery-2", market.as_str(), m1_after_gap, 200_400),
                    multi_market_m1("xrp-recovery-3", market.as_str(), m1_latest, 200_500),
                ],
                ("KRW-XRP", CandleInterval::M1) => vec![
                    multi_market_m1("xrp-recovery-1", market.as_str(), m1_first_missing, 200_300),
                    multi_market_m1("xrp-recovery-2", market.as_str(), m1_after_gap, 200_400),
                    multi_market_m1("xrp-recovery-3", market.as_str(), m1_latest, 200_500),
                ],
                ("KRW-BTC", CandleInterval::H1) => multi_market_h1_history(
                    market.as_str(),
                    "btc-recovery-h1",
                    first_h1_open,
                    100_000,
                ),
                ("KRW-XRP", CandleInterval::H1) => multi_market_h1_history(
                    market.as_str(),
                    "xrp-recovery-h1",
                    first_h1_open,
                    200_000,
                ),
                _ => {
                    return Err(LabError::Internal(format!(
                        "unexpected multi-market paper recovery requirement: {market} {:?}",
                        requirement.interval
                    )));
                }
            };
            let batch_label = format!(
                "{label}-{}-{:?}",
                requirement.market.code(),
                requirement.interval
            );
            batches.push(synthetic_store_batch(store, bars, now, &batch_label)?);
        }
        Ok(MultiMarketRecoveryFetch {
            requirements,
            batches,
        })
    }

    struct PaperStoreTestSnapshot {
        view: crate::contracts::PaperSessionView,
        events: Vec<crate::contracts::PaperSessionEvent>,
        checkpoint_digests: Vec<ContentHash>,
    }

    fn paper_store_test_snapshot(
        store: &crate::storage::Store,
        session_id: &PaperSessionId,
        now: UtcTimestamp,
    ) -> Result<PaperStoreTestSnapshot, LabError> {
        let view = store.paper_get_session(session_id, now)?.session;
        let events = store.paper_list_events(session_id, None, 100)?.items;
        let checkpoint_digests = store.paper_checkpoint_digests_for_test(session_id)?;
        Ok(PaperStoreTestSnapshot {
            view,
            events,
            checkpoint_digests,
        })
    }

    fn zero_cost_policy() -> CostPolicy {
        let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
        CostPolicy {
            buy_fee_bps: zero,
            sell_fee_bps: zero,
            maker_fee_bps: zero,
            half_spread_bps: zero,
            slippage_bps: zero,
            impact_bps: zero,
            assumption_label: "zero cost test".into(),
            dynamic: None,
        }
    }

    fn simple_rules() -> PaperMarketRuleScenario {
        PaperMarketRuleScenario::SimulatedScenario {
            min_notional: QuoteAmount::new(dec("10")).expect("min notional"),
            quantity_step: AssetQuantity::new(dec("0.1")).expect("step"),
            ticks: vec![TickBand {
                lower_bound: QuoteAmount::new(dec("0")).expect("lower"),
                tick: PriceKrw::new(dec("1")).expect("tick"),
            }],
            assumption_label: "SIMULATED_SCENARIO test rules".into(),
        }
    }

    fn s1_checkpoint(
        session_id: &PaperSessionId,
        started_at: UtcTimestamp,
    ) -> Result<PaperMarketCheckpoint, LabError> {
        PaperMarketCheckpoint::new(
            session_id,
            MarketId::parse_upbit("KRW-BTC")?,
            &sample_s1_revision_with_warmup(100, 20),
            CandleInterval::M1,
            CandleInterval::M1,
            0,
            ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            QuoteAmount::new(dec("1000000"))?,
            zero_cost_policy(),
            simple_rules(),
            started_at,
        )
    }

    fn mtf_rules_revision() -> FrozenPolicyRevision {
        let definition = PolicyDefinition {
            schema_version: "1.0".into(),
            name: "paper_mtf_source_gate".into(),
            description: "SYNTHETIC_TEST_ONLY H1 source gate for M1 paper decisions".into(),
            program: PolicyProgram::Rules {
                program: RulesProgram {
                    indicators: vec![
                        PolicyIndicator {
                            id: "h1_fast".into(),
                            indicator: PolicyIndicatorKind::Ema { window: 2 },
                            source_interval: Some(CandleInterval::H1),
                        },
                        PolicyIndicator {
                            id: "h1_slow".into(),
                            indicator: PolicyIndicatorKind::Ema { window: 3 },
                            source_interval: Some(CandleInterval::H1),
                        },
                    ],
                    states: Vec::new(),
                    rules: vec![PolicyRule {
                        id: "h1_gate".into(),
                        condition: BoolExpr::Compare {
                            comparison: CompareOp::Gt,
                            left: NumericExpr::Indicator {
                                id: "h1_fast".into(),
                            },
                            right: NumericExpr::Indicator {
                                id: "h1_slow".into(),
                            },
                        },
                        target: PolicyTarget::Hold,
                    }],
                    fallback: PolicyTarget::Hold,
                    signal_expiry: PolicySignalExpiry::EndOfRange,
                },
            },
        };
        let definition_digest = ContentHash::of_value(&definition).expect("definition digest");
        FrozenPolicyRevision {
            reference: PolicyRevisionRef {
                policy_id: PolicyId::new("policy-paper-mtf").expect("policy id"),
                revision_id: PolicyRevisionId::new("policy-paper-mtf-r1").expect("revision id"),
                definition_digest,
            },
            revision_number: 1,
            parent_revision_id: None,
            family: StrategyKind::S1,
            origin: PolicyOrigin::User,
            definition,
        }
    }

    fn mtf_buy_rules_revision() -> FrozenPolicyRevision {
        let mut revision = mtf_rules_revision();
        let PolicyProgram::Rules { program } = &mut revision.definition.program else {
            unreachable!("MTF fixture has a rules program");
        };
        program.rules[0].target = PolicyTarget::Weight {
            value: NumericExpr::Constant { value: 1.0 },
        };
        revision.reference.policy_id = PolicyId::new("policy-paper-mtf-buy").expect("policy id");
        revision.reference.revision_id =
            PolicyRevisionId::new("policy-paper-mtf-buy-r1").expect("revision id");
        revision.reference.definition_digest =
            ContentHash::of_value(&revision.definition).expect("definition digest");
        revision
    }

    fn mtf_checkpoint(
        session_id: &PaperSessionId,
        started_at: UtcTimestamp,
    ) -> Result<PaperMarketCheckpoint, LabError> {
        PaperMarketCheckpoint::new(
            session_id,
            MarketId::parse_upbit("KRW-BTC")?,
            &mtf_rules_revision(),
            CandleInterval::M1,
            CandleInterval::M1,
            0,
            ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            QuoteAmount::new(dec("1000000"))?,
            zero_cost_policy(),
            simple_rules(),
            started_at,
        )
    }

    fn rules_revision_with_indicators(
        policy_id: &str,
        revision_id: &str,
        name: &str,
        indicators: Vec<PolicyIndicator>,
    ) -> FrozenPolicyRevision {
        let definition = PolicyDefinition {
            schema_version: "1.0".into(),
            name: name.into(),
            description: "SYNTHETIC_TEST_ONLY paper source-feed fixture".into(),
            program: PolicyProgram::Rules {
                program: RulesProgram {
                    indicators,
                    states: Vec::new(),
                    rules: Vec::new(),
                    fallback: PolicyTarget::Hold,
                    signal_expiry: PolicySignalExpiry::EndOfRange,
                },
            },
        };
        FrozenPolicyRevision {
            reference: PolicyRevisionRef {
                policy_id: PolicyId::new(policy_id).expect("policy id"),
                revision_id: PolicyRevisionId::new(revision_id).expect("revision id"),
                definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
            },
            revision_number: 1,
            parent_revision_id: None,
            family: StrategyKind::S1,
            origin: PolicyOrigin::User,
            definition,
        }
    }

    fn paper_checkpoint_for_rules_revision(
        session_id: &PaperSessionId,
        started_at: UtcTimestamp,
        revision: &FrozenPolicyRevision,
    ) -> Result<PaperMarketCheckpoint, LabError> {
        PaperMarketCheckpoint::new(
            session_id,
            MarketId::parse_upbit("KRW-BTC")?,
            revision,
            CandleInterval::M1,
            CandleInterval::M1,
            0,
            ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            QuoteAmount::new(dec("1000000"))?,
            zero_cost_policy(),
            simple_rules(),
            started_at,
        )
    }

    fn mtf_start_write(
        request_id: &str,
        policy_ref: PolicyRevisionRef,
    ) -> Result<PaperSessionWrite, LabError> {
        paper_start_write_for_interval(request_id, policy_ref, CandleInterval::M1, "KRW-BTC")
    }

    fn paper_start_write_for_interval(
        request_id: &str,
        policy_ref: PolicyRevisionRef,
        interval: CandleInterval,
        market: &str,
    ) -> Result<PaperSessionWrite, LabError> {
        Ok(PaperSessionWrite::Start {
            request_id: RequestId::new(request_id)?,
            policy_ref,
            decision_interval: interval,
            execution_resolution: interval,
            latency_ms: 0,
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE)?,
            },
            markets: vec![PaperMarketStart {
                market: MarketId::parse_upbit(market)?,
                initial_cash: QuoteAmount::new(dec("1000000"))?,
                costs: zero_cost_policy(),
                market_rules: simple_rules(),
            }],
        })
    }

    fn h1_history(
        prefix: &str,
        first_open: UtcTimestamp,
        closes: &[&str],
    ) -> Vec<CandleObservation> {
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| {
                candle_obs_at(
                    &format!("{prefix}-{index}"),
                    "KRW-BTC",
                    CandleInterval::H1,
                    UtcTimestamp(
                        first_open.0
                            + CandleInterval::H1.duration()
                                * i32::try_from(index).expect("fixture count fits i32"),
                    ),
                    close,
                    close,
                )
            })
            .collect()
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn paper_continuation_warmup_restart_equivalence_and_causal_guards()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-test-continuation")?;
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let revision = sample_s1_revision();
        let started_at = ts("2025-01-02T00:00:00Z");
        let execution = ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        };
        let initial_cash = QuoteAmount::new(dec("1000"))?;

        let mut continuous = PaperMarketCheckpoint::new(
            &session_id,
            market.clone(),
            &revision,
            CandleInterval::H1,
            CandleInterval::H1,
            0,
            execution.clone(),
            initial_cash,
            zero_cost_policy(),
            simple_rules(),
            started_at,
        )?;
        let mut restarted = continuous.clone();

        let pre_bars = [
            candle_obs(
                "pre-1",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-01T21:00:00Z",
                "100",
                "100",
            ),
            candle_obs(
                "pre-2",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-01T22:00:00Z",
                "100",
                "100",
            ),
            candle_obs(
                "pre-3",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-01T23:00:00Z",
                "100",
                "110",
            ),
        ];
        let empty_sources = BTreeMap::new();
        continuous.warm_up_pre_start(&pre_bars, &empty_sources, None, started_at)?;
        restarted.warm_up_pre_start(&pre_bars, &empty_sources, None, started_at)?;

        // Warmup must never emit pre-start signals, fills, or marks.
        assert_eq!(continuous.signal_count, 0);
        assert_eq!(continuous.fill_count, 0);
        assert_eq!(continuous.market_event_count, 0);
        assert!(continuous.last_finalized_candle_at.is_none());
        assert_eq!(
            continuous.warmup_completed_bars,
            continuous.warmup_required_bars - 1
        );

        let live_bars = [
            candle_obs(
                "live-1",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-02T00:00:00Z",
                "110",
                "115",
            ),
            candle_obs(
                "live-2",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-02T01:00:00Z",
                "115",
                "120",
            ),
            candle_obs(
                "live-3",
                "KRW-BTC",
                CandleInterval::H1,
                "2025-01-02T02:00:00Z",
                "120",
                "125",
            ),
        ];

        for (idx, bar) in live_bars.iter().enumerate() {
            let now = bar.candle.close_time_utc;
            let out_cont = continuous.step_bar(bar, &empty_sources, None, now)?;
            assert!(out_cont.did_advance);
            assert!(out_cont.recovery_required.is_none());

            // Simulate process restart by round-tripping `restarted` through JSON before & after each step.
            let encoded = serde_json::to_string(&restarted)?;
            restarted = serde_json::from_str(&encoded)?;
            let out_rest = restarted.step_bar(bar, &empty_sources, None, now)?;
            let seq = (idx as u64) + 1;
            let ev_cont = serde_json::to_value(materialize_candle_event(
                seq,
                out_cont.events.into_iter().next().expect("cont event"),
            ))?;
            let ev_rest = serde_json::to_value(materialize_candle_event(
                seq,
                out_rest.events.into_iter().next().expect("rest event"),
            ))?;
            assert_eq!(ev_cont, ev_rest);
            assert_eq!(
                continuous.checkpoint_digest()?,
                restarted.checkpoint_digest()?
            );
            assert_eq!(
                serde_json::to_value(continuous.to_market_snapshot()?)?,
                serde_json::to_value(restarted.to_market_snapshot()?)?
            );
        }
        assert_eq!(
            continuous.warmup_completed_bars,
            continuous.warmup_required_bars
        );

        let snap = continuous.to_market_snapshot()?;
        assert_eq!(snap.fill_count, 1);
        assert!(snap.quantity.get() > Decimal::ZERO);
        assert!(snap.equity.expect("marked equity").get() > dec("1000"));
        assert!(snap.benchmark.is_some());
        assert_eq!(
            snap.verification.status,
            PaperVerificationStatus::NotAvailable
        );
        assert_eq!(
            snap.verification.reason,
            "INDEPENDENT_PAPER_VERIFIER_NOT_IMPLEMENTED"
        );

        // Duplicate bar idempotency: stepping the same last bar again does not advance or duplicate fills.
        let last_bar = &live_bars[2];
        let dup_now = UtcTimestamp(last_bar.candle.close_time_utc.0 + chrono::Duration::minutes(5));
        let dup = continuous.step_bar(last_bar, &empty_sources, None, dup_now)?;
        assert!(!dup.did_advance);
        assert!(dup.events.is_empty());
        assert_eq!(continuous.fill_count, 1);

        // Incomplete candle must be rejected.
        let mut incomplete = candle_obs(
            "live-4",
            "KRW-BTC",
            CandleInterval::H1,
            "2025-01-02T03:00:00Z",
            "125",
            "130",
        );
        incomplete.candle.completed = false;
        assert!(
            continuous
                .step_bar(
                    &incomplete,
                    &empty_sources,
                    None,
                    incomplete.candle.close_time_utc
                )
                .is_err()
        );

        // Future candle (close_time > now_utc) must be rejected.
        let future_bar = candle_obs(
            "live-4",
            "KRW-BTC",
            CandleInterval::H1,
            "2025-01-02T03:00:00Z",
            "125",
            "130",
        );
        assert!(
            continuous
                .step_bar(
                    &future_bar,
                    &empty_sources,
                    None,
                    ts("2025-01-02T03:30:00Z")
                )
                .is_err()
        );

        // Out-of-order candle (close_time < cursor) must be rejected.
        assert!(
            continuous
                .step_bar(&live_bars[0], &empty_sources, None, dup_now)
                .is_err()
        );

        // Conflicting digest at same close_time transitions to RecoveryRequired.
        let mut conflicting = last_bar.clone();
        conflicting.candle.close = PriceKrw::new(dec("999"))?;
        conflicting.content_digest = observation_digest(&conflicting.candle)?;
        let mut conflict_cp = continuous.clone();
        let conflict_out = conflict_cp.step_bar(&conflicting, &empty_sources, None, dup_now)?;
        assert!(conflict_out.recovery_required.is_some());
        assert_eq!(
            conflict_out.events[0].kind,
            PaperEventKind::RecoveryRequired
        );

        // Missing intermediate bar transitions to RecoveryRequired.
        let gapped = candle_obs(
            "live-gap",
            "KRW-BTC",
            CandleInterval::H1,
            "2025-01-02T04:00:00Z",
            "125",
            "130",
        );
        let mut gap_cp = continuous;
        let gap_out =
            gap_cp.step_bar(&gapped, &empty_sources, None, gapped.candle.close_time_utc)?;
        assert!(gap_out.recovery_required.is_some());
        assert_eq!(gap_out.events[0].kind, PaperEventKind::RecoveryRequired);

        Ok(())
    }

    #[test]
    fn stale_pre_start_warmup_waits_for_fresh_seed_before_s1_evaluation()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-stale-warmup-seed")?;
        let started_at = ts("2025-01-02T12:35:30Z");
        let boundary =
            crate::scheduling::latest_completed_boundary(started_at, CandleInterval::M1)?;
        let mut checkpoint = s1_checkpoint(&session_id, started_at)?;
        assert_eq!(checkpoint.warmup_required_bars, 100);

        let off_grid = constant_bars(
            "off-grid",
            UtcTimestamp(boundary.0 - chrono::Duration::minutes(99) + chrono::Duration::seconds(1)),
            99,
            CandleInterval::M1,
            "100",
        );
        checkpoint.warm_up_pre_start(&off_grid, &BTreeMap::new(), None, started_at)?;
        assert_eq!(checkpoint.warmup_completed_bars, 0);

        let stale_end = UtcTimestamp(boundary.0 - chrono::Duration::minutes(35));
        let stale = constant_bars(
            "stale",
            UtcTimestamp(stale_end.0 - chrono::Duration::minutes(99)),
            99,
            CandleInterval::M1,
            "100",
        );
        checkpoint.warm_up_pre_start(&stale, &BTreeMap::new(), None, started_at)?;
        assert_eq!(checkpoint.warmup_completed_bars, 0);
        assert_eq!(checkpoint.evaluator.completed_bars_seen(), 0);

        let fresh = constant_bars(
            "fresh",
            UtcTimestamp(boundary.0 - chrono::Duration::minutes(99)),
            99,
            CandleInterval::M1,
            "100",
        );
        checkpoint.warm_up_pre_start(&fresh, &BTreeMap::new(), None, started_at)?;
        assert_eq!(checkpoint.warmup_completed_bars, 99);
        assert_eq!(checkpoint.evaluator.completed_bars_seen(), 99);
        assert_eq!(checkpoint.signal_count, 0);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.signals.is_empty());
        assert!(checkpoint.engine_state.orders.is_empty());
        assert!(checkpoint.engine_state.fills.is_empty());
        assert!(checkpoint.last_finalized_candle_at.is_none());

        let first_live = candle_obs_at(
            "first-live",
            "KRW-BTC",
            CandleInterval::M1,
            boundary,
            "100",
            "101",
        );
        let second_live = candle_obs_at(
            "second-live",
            "KRW-BTC",
            CandleInterval::M1,
            UtcTimestamp(boundary.0 + chrono::Duration::minutes(1)),
            "101",
            "102",
        );
        let _ = checkpoint.step_bar(
            &first_live,
            &BTreeMap::new(),
            None,
            first_live.candle.close_time_utc,
        )?;
        let outcome = checkpoint.step_bar(
            &second_live,
            &BTreeMap::new(),
            None,
            second_live.candle.close_time_utc,
        )?;

        assert!(outcome.recovery_required.is_none());
        assert_eq!(checkpoint.warmup_completed_bars, 100);
        assert_eq!(checkpoint.signal_count, 1);
        assert!(
            outcome
                .events
                .iter()
                .flat_map(|event| &event.signals)
                .any(|signal| {
                    signal.signal_time > started_at
                        && signal.signal_time == first_live.candle.close_time_utc
                })
        );
        assert!(checkpoint.fill_count <= 1);
        assert!(
            checkpoint
                .engine_state
                .fills
                .iter()
                .all(|fill| match &fill.timing {
                    crate::contracts::FillTiming::Exact { at } => *at > started_at,
                    crate::contracts::FillTiming::Interval { accounting_at, .. } => {
                        *accounting_at > started_at
                    }
                })
        );
        Ok(())
    }

    #[test]
    fn paper_mtf_cold_source_defers_without_partial_evaluation_or_cursor_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-mtf-cold-source-defer")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let boundary =
            crate::scheduling::latest_completed_boundary(started_at, CandleInterval::M1)?;
        let mut checkpoint = mtf_checkpoint(&session_id, started_at)?;
        assert_eq!(checkpoint.warmup_required_bars, 1);

        let no_sources = BTreeMap::new();
        assert!(!checkpoint.feed_sources_until(&no_sources, ts("2025-01-02T10:01:00Z"))?);
        let incomplete_source =
            h1_history("mtf-incomplete-source", ts("2025-01-02T10:00:00Z"), &["82"]);
        let incomplete_sources = BTreeMap::from([(CandleInterval::H1, incomplete_source)]);

        let no_source_id = PaperSessionId::new("paper-mtf-no-source-evaluation")?;
        let mut no_source_checkpoint = mtf_checkpoint(&no_source_id, started_at)?;
        let first_incomplete_live = candle_obs_at(
            "mtf-incomplete-live-1",
            "KRW-BTC",
            CandleInterval::M1,
            boundary,
            "100",
            "100",
        );
        let second_incomplete_live = candle_obs_at(
            "mtf-incomplete-live-2",
            "KRW-BTC",
            CandleInterval::M1,
            UtcTimestamp(boundary.0 + chrono::Duration::minutes(1)),
            "100",
            "100",
        );
        no_source_checkpoint.step_bar(
            &first_incomplete_live,
            &incomplete_sources,
            None,
            first_incomplete_live.candle.close_time_utc,
        )?;
        no_source_checkpoint.step_bar(
            &second_incomplete_live,
            &incomplete_sources,
            None,
            second_incomplete_live.candle.close_time_utc,
        )?;
        assert_eq!(no_source_checkpoint.signal_count, 0);
        assert_eq!(no_source_checkpoint.fill_count, 0);
        assert_eq!(no_source_checkpoint.evaluator.completed_bars_seen(), 0);
        assert!(no_source_checkpoint.engine_state.orders.is_empty());

        assert!(!checkpoint.feed_sources_until(&incomplete_sources, ts("2025-01-02T11:00:00Z"),)?);

        assert_eq!(checkpoint.signal_count, 0);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());
        assert!(checkpoint.evaluator.cross_last_close().is_empty());
        assert_eq!(
            checkpoint.source_cursors[&CandleInterval::H1].last_bar_close_utc,
            None
        );
        assert_eq!(
            checkpoint.source_cursors[&CandleInterval::H1].completed_bars,
            0
        );
        Ok(())
    }

    #[test]
    fn paper_mtf_bootstraps_from_fresh_contiguous_suffix_before_first_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-mtf-cold-source-bootstrap")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let mut checkpoint = mtf_checkpoint(&session_id, started_at)?;
        let sufficient_sources = BTreeMap::from([(
            CandleInterval::H1,
            h1_history(
                "mtf-fresh-source",
                ts("2025-01-02T10:00:00Z"),
                &["82", "84", "86"],
            ),
        )]);
        assert!(checkpoint.feed_sources_until(&sufficient_sources, ts("2025-01-02T13:00:00Z"))?);

        assert_eq!(checkpoint.signal_count, 0);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());
        assert_eq!(
            checkpoint.source_cursors[&CandleInterval::H1].last_bar_close_utc,
            Some(ts("2025-01-02T13:00:00Z"))
        );
        assert_eq!(
            checkpoint.source_cursors[&CandleInterval::H1].completed_bars,
            3
        );
        assert!(
            checkpoint
                .evaluator
                .cross_last_close()
                .values()
                .all(|close| *close == ts("2025-01-02T13:00:00Z"))
        );
        let first_live = candle_obs_at(
            "mtf-live-after-bootstrap-1",
            "KRW-BTC",
            CandleInterval::M1,
            ts("2025-01-02T13:00:00Z"),
            "100",
            "100",
        );
        let second_live = candle_obs_at(
            "mtf-live-after-bootstrap-2",
            "KRW-BTC",
            CandleInterval::M1,
            ts("2025-01-02T13:01:00Z"),
            "100",
            "100",
        );
        checkpoint.step_bar(
            &first_live,
            &sufficient_sources,
            None,
            first_live.candle.close_time_utc,
        )?;
        let outcome = checkpoint.step_bar(
            &second_live,
            &sufficient_sources,
            None,
            second_live.candle.close_time_utc,
        )?;
        assert!(outcome.recovery_required.is_none());
        assert_eq!(checkpoint.signal_count, 1);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());
        assert!(
            outcome
                .events
                .iter()
                .flat_map(|event| &event.signals)
                .any(|signal| { signal.signal_time == first_live.candle.close_time_utc })
        );
        Ok(())
    }

    #[test]
    fn paper_mtf_reuses_source_within_interval_then_rejects_missing_completed_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-mtf-source-boundary-gap")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let boundary =
            crate::scheduling::latest_completed_boundary(started_at, CandleInterval::M1)?;
        let mut checkpoint = mtf_checkpoint(&session_id, started_at)?;
        let h1_sources = BTreeMap::from([(
            CandleInterval::H1,
            h1_history(
                "mtf-boundary-source",
                ts("2025-01-02T07:00:00Z"),
                &["80", "82", "84"],
            ),
        )]);
        checkpoint.feed_sources_until(&h1_sources, boundary)?;
        assert_eq!(
            checkpoint.source_cursors[&CandleInterval::H1].last_bar_close_utc,
            Some(boundary)
        );

        let no_new_h1 = BTreeMap::new();
        let first_live = candle_obs_at(
            "mtf-boundary-live-1",
            "KRW-BTC",
            CandleInterval::M1,
            boundary,
            "100",
            "100",
        );
        checkpoint.step_bar(
            &first_live,
            &no_new_h1,
            None,
            first_live.candle.close_time_utc,
        )?;
        let second_live = candle_obs_at(
            "mtf-boundary-live-2",
            "KRW-BTC",
            CandleInterval::M1,
            UtcTimestamp(boundary.0 + chrono::Duration::minutes(1)),
            "100",
            "100",
        );
        let valid_intrahour = checkpoint.step_bar(
            &second_live,
            &no_new_h1,
            None,
            second_live.candle.close_time_utc,
        )?;
        assert!(valid_intrahour.recovery_required.is_none());
        assert_eq!(checkpoint.signal_count, 1);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());

        for minute in 2..60_i64 {
            let open_at = UtcTimestamp(boundary.0 + chrono::Duration::minutes(minute));
            let bar = candle_obs_at(
                &format!("mtf-valid-intrahour-{minute}"),
                "KRW-BTC",
                CandleInterval::M1,
                open_at,
                "100",
                "100",
            );
            let outcome = checkpoint.step_bar(&bar, &no_new_h1, None, bar.candle.close_time_utc)?;
            assert!(outcome.recovery_required.is_none());
        }

        let signals_before_gap = checkpoint.signal_count;
        let missing_h1_boundary = candle_obs_at(
            "mtf-boundary-gap-decision",
            "KRW-BTC",
            CandleInterval::M1,
            UtcTimestamp(boundary.0 + chrono::Duration::hours(1)),
            "100",
            "100",
        );
        let gap = checkpoint.step_bar(
            &missing_h1_boundary,
            &no_new_h1,
            None,
            missing_h1_boundary.candle.close_time_utc,
        );
        assert!(matches!(gap, Err(LabError::DataGap(_))));
        assert_eq!(checkpoint.signal_count, signals_before_gap);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());
        Ok(())
    }

    #[test]
    fn paper_mtf_source_rows_validate_lazily_and_preserve_duplicate_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-mtf-source-validation")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let base = mtf_checkpoint(&session_id, started_at)?;
        let source_boundary = ts("2025-01-02T10:00:00Z");
        let valid_history = h1_history(
            "mtf-source-validation",
            ts("2025-01-02T07:00:00Z"),
            &["80", "82", "84"],
        );

        let mut future_row = candle_obs(
            "mtf-future-source-with-bad-digest",
            "KRW-BTC",
            CandleInterval::H1,
            "2025-01-02T10:00:00Z",
            "84",
            "86",
        );
        future_row.content_digest = ContentHash::of_bytes(b"intentionally incorrect future digest");
        let mut with_future_rows = valid_history.clone();
        with_future_rows.push(future_row);

        let mut lazy = base.clone();
        let mut without_future = base.clone();
        let future_sources = BTreeMap::from([(CandleInterval::H1, with_future_rows)]);
        let historical_sources = BTreeMap::from([(CandleInterval::H1, valid_history.clone())]);
        assert!(lazy.feed_sources_until(&future_sources, source_boundary)?);
        assert!(without_future.feed_sources_until(&historical_sources, source_boundary)?);
        assert_eq!(
            lazy.checkpoint_digest()?,
            without_future.checkpoint_digest()?
        );
        assert_eq!(
            lazy.source_cursors[&CandleInterval::H1].last_bar_close_utc,
            Some(source_boundary)
        );

        let before_future_becomes_eligible = lazy.checkpoint_digest()?;
        assert!(matches!(
            lazy.feed_sources_until(&future_sources, ts("2025-01-02T11:00:00Z")),
            Err(LabError::InputHashMismatch(_))
        ));
        assert_eq!(lazy.checkpoint_digest()?, before_future_becomes_eligible);

        let mut identical_rows = valid_history.clone();
        identical_rows.push(
            identical_rows
                .last()
                .expect("history has a final source row")
                .clone(),
        );
        let mut identical_checkpoint = base.clone();
        let identical_sources = BTreeMap::from([(CandleInterval::H1, identical_rows)]);
        assert!(identical_checkpoint.feed_sources_until(&identical_sources, source_boundary)?);
        assert_eq!(
            identical_checkpoint.source_cursors[&CandleInterval::H1].completed_bars,
            3
        );

        let mut conflicting_rows = valid_history;
        let mut conflicting_duplicate = conflicting_rows
            .last()
            .expect("history has a final source row")
            .clone();
        conflicting_duplicate.candle.close = PriceKrw::new(dec("85"))?;
        conflicting_duplicate.candle.high = PriceKrw::new(dec("85"))?;
        conflicting_duplicate.content_digest = observation_digest(&conflicting_duplicate.candle)?;
        conflicting_duplicate.id = crate::contracts::ObservationId::from_seed(
            conflicting_duplicate.content_digest.as_str(),
        );
        conflicting_rows.push(conflicting_duplicate);
        let mut conflicting_checkpoint = base;
        let before_conflicting_duplicate = conflicting_checkpoint.checkpoint_digest()?;
        let conflicting_sources = BTreeMap::from([(CandleInterval::H1, conflicting_rows)]);
        assert!(matches!(
            conflicting_checkpoint.feed_sources_until(&conflicting_sources, source_boundary),
            Err(LabError::DataCorrupt(_))
        ));
        assert_eq!(
            conflicting_checkpoint.checkpoint_digest()?,
            before_conflicting_duplicate
        );
        Ok(())
    }

    #[test]
    fn paper_mtf_multi_interval_cold_source_defers_after_ready_plan_without_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let revision = rules_revision_with_indicators(
            "policy-paper-multi-source-cold",
            "policy-paper-multi-source-cold-r1",
            "paper_mtf_multi_source_cold",
            vec![
                PolicyIndicator {
                    id: "m5_close".into(),
                    indicator: PolicyIndicatorKind::Close,
                    source_interval: Some(CandleInterval::M5),
                },
                PolicyIndicator {
                    id: "h1_close".into(),
                    indicator: PolicyIndicatorKind::Close,
                    source_interval: Some(CandleInterval::H1),
                },
            ],
        );
        let session_id = PaperSessionId::new("paper-mtf-two-source-cold")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let mut checkpoint =
            paper_checkpoint_for_rules_revision(&session_id, started_at, &revision)?;
        let decision_close = ts("2025-01-02T11:00:00Z");
        let ready_m5 = candle_obs(
            "paper-mtf-ready-m5",
            "KRW-BTC",
            CandleInterval::M5,
            "2025-01-02T10:55:00Z",
            "100",
            "100",
        );
        let sources = BTreeMap::from([(CandleInterval::M5, vec![ready_m5])]);
        let before = checkpoint.checkpoint_digest()?;

        assert!(!checkpoint.feed_sources_until(&sources, decision_close)?);
        assert_eq!(checkpoint.checkpoint_digest()?, before);
        assert!(checkpoint.evaluator.cross_last_close().is_empty());
        for interval in [CandleInterval::M5, CandleInterval::H1] {
            let cursor = &checkpoint.source_cursors[&interval];
            assert_eq!(cursor.last_bar_close_utc, None);
            assert_eq!(cursor.completed_bars, 0);
        }
        Ok(())
    }

    #[test]
    fn paused_mtf_source_gap_returns_existing_recovery_outcome()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = PaperSessionId::new("paper-mtf-paused-source-gap")?;
        let started_at = ts("2025-01-02T10:00:30Z");
        let mut checkpoint = mtf_checkpoint(&session_id, started_at)?;
        let source_history = BTreeMap::from([(
            CandleInterval::H1,
            h1_history(
                "mtf-paused-source",
                ts("2025-01-02T07:00:00Z"),
                &["80", "82", "84"],
            ),
        )]);
        assert!(checkpoint.feed_sources_until(&source_history, ts("2025-01-02T10:00:00Z"))?);

        let prior = candle_obs_at(
            "mtf-paused-gap-prior",
            "KRW-BTC",
            CandleInterval::M1,
            ts("2025-01-02T10:59:00Z"),
            "100",
            "100",
        );
        checkpoint.prior_execution_bar = Some(prior);
        checkpoint.prior_bar_evaluated = false;
        let outcome = checkpoint.catch_up_paused_bars(
            &[],
            &BTreeMap::new(),
            None,
            ts("2025-01-02T11:00:30Z"),
        )?;

        assert!(
            outcome
                .recovery_required
                .as_deref()
                .is_some_and(|reason| reason.starts_with("DATA_GAP:"))
        );
        assert_eq!(checkpoint.recovery_reason, outcome.recovery_required);
        assert_eq!(checkpoint.signal_count, 0);
        assert_eq!(checkpoint.fill_count, 0);
        assert!(checkpoint.engine_state.orders.is_empty());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one DatabaseOwner path proves prestart H1 source bootstrap from the first published M1/H1 batch"
    )]
    fn first_owner_batch_uses_prestart_h1_source_for_first_m1_rules_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "spot-lab-paper-mtf-owner-bootstrap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let database = owner.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(async {
            let started_at = ts("2025-01-02T10:00:30Z");
            let source_boundary = ts("2025-01-02T10:00:00Z");
            let definition = mtf_rules_revision().definition;
            let policy_request = PolicyWrite::Create {
                request_id: RequestId::new("paper-mtf-owner-policy")?,
                definition,
            };
            let policy_ref = database
                .call("paper_test_create_mtf_policy", move |store| {
                    let revision = store.write_policy(&policy_request, started_at)?;
                    let options = store.paper_start_options()?;
                    let option = options
                        .policy_options
                        .iter()
                        .find(|option| option.reference == revision.snapshot.reference)
                        .ok_or_else(|| {
                            LabError::DataCorrupt("MTF paper option disappeared".into())
                        })?;
                    if !option.supported {
                        return Err(LabError::InvalidConfig(
                            "test RulesProgram is not supported for M1/H1 paper".into(),
                        ));
                    }
                    Ok(revision.snapshot.reference)
                })
                .await?;
            let started = database
                .call("paper_test_start_mtf_session", move |store| {
                    store.paper_write(
                        &mtf_start_write("paper-mtf-owner-start", policy_ref)?,
                        started_at,
                    )
                })
                .await?;
            assert_eq!(started.session.state, PaperSessionState::Running);
            assert_eq!(started.session.event_count, 0);
            assert_eq!(started.session.markets[0].signal_count, 0);

            let mut first_batch = h1_history(
                "owner-prestart-h1",
                ts("2025-01-02T07:00:00Z"),
                &["80", "82", "84"],
            );
            first_batch.extend([
                candle_obs_at(
                    "owner-poststart-m1-1",
                    "KRW-BTC",
                    CandleInterval::M1,
                    source_boundary,
                    "100",
                    "100",
                ),
                candle_obs_at(
                    "owner-poststart-m1-2",
                    "KRW-BTC",
                    CandleInterval::M1,
                    UtcTimestamp(source_boundary.0 + chrono::Duration::minutes(1)),
                    "100",
                    "100",
                ),
            ]);
            let reconciled_at = ts("2025-01-02T10:02:00Z");
            let session_id = started.session.session_id.clone();
            database
                .call("paper_test_publish_first_mtf_batch", move |store| {
                    let batch =
                        synthetic_store_batch(store, first_batch, reconciled_at, "M1 and H1")?;
                    store.reconcile_running_paper_sessions(&[batch], reconciled_at)
                })
                .await?;
            let (view, events) = database
                .call("paper_test_read_first_mtf_result", move |store| {
                    Ok((
                        store.paper_get_session(&session_id, reconciled_at)?.session,
                        store.paper_list_events(&session_id, None, 100)?,
                    ))
                })
                .await?;
            assert_eq!(view.state, PaperSessionState::Running);
            assert_eq!(view.markets[0].signal_count, 1);
            assert_eq!(view.markets[0].fill_count, 0);
            assert!(events.items.iter().all(|event| {
                event.fills.is_empty()
                    && event.orders.is_empty()
                    && event
                        .signals
                        .iter()
                        .all(|signal| signal.signal_time > started_at)
            }));
            assert!(
                events
                    .items
                    .iter()
                    .flat_map(|event| &event.signals)
                    .any(|signal| {
                        signal.signal_time == ts("2025-01-02T10:01:00Z")
                            && signal.policy_trace.as_ref().is_some_and(|trace| {
                                trace.matched_rule_id.as_deref() == Some("h1_gate")
                            })
                            && signal.indicators.policy_values.iter().any(|value| {
                                value.id == "h1_fast" && (value.value - 83.0).abs() < 1e-9
                            })
                    })
            );
            Ok::<(), LabError>(())
        });
        drop(runtime);
        let closed = owner.shutdown();
        std::fs::remove_dir_all(&root)?;
        result?;
        closed?;
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one storage-owner fixture follows stale start through fresh poll and first S1 decision"
    )]
    fn first_fresh_database_owner_batch_bootstraps_s1_before_post_start_evaluation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "spot-lab-paper-fresh-bootstrap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let database = owner.handle();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(async {
            let started_at = ts("2025-01-02T12:35:30Z");
            let boundary =
                crate::scheduling::latest_completed_boundary(started_at, CandleInterval::M1)?;
            let stale_end = UtcTimestamp(boundary.0 - chrono::Duration::minutes(35));
            let stale = constant_bars(
                "stale-owner",
                UtcTimestamp(stale_end.0 - chrono::Duration::minutes(99)),
                99,
                CandleInterval::M1,
                "100",
            );
            database
                .call("paper_test_insert_stale_owner_batch", move |store| {
                    let stale_batch = synthetic_store_batch(store, stale, boundary, "stale bars")?;
                    store.reconcile_running_paper_sessions(&[stale_batch], boundary)
                })
                .await?;

            let session = database
                .call("paper_test_start_stale_s1", move |store| {
                    let options = store.paper_start_options()?;
                    let s1 = options
                        .policy_options
                        .iter()
                        .find(|option| option.family == StrategyKind::S1 && option.supported)
                        .ok_or_else(|| {
                            LabError::InvalidConfig("S1 test policy unavailable".into())
                        })?;
                    if s1.decision_warmup_bars != 100 {
                        return Err(LabError::Internal(format!(
                            "fixture expects S1 100-bar warmup, got {}",
                            s1.decision_warmup_bars
                        )));
                    }
                    store.paper_write(
                        &PaperSessionWrite::Start {
                            request_id: RequestId::new("fresh-poll-s1-start")?,
                            policy_ref: s1.reference.clone(),
                            decision_interval: CandleInterval::M1,
                            execution_resolution: CandleInterval::M1,
                            latency_ms: 0,
                            execution: ExecutionPolicy::NextBarOpen {
                                participation_cap: Weight::new(Decimal::ONE)?,
                            },
                            markets: vec![PaperMarketStart {
                                market: MarketId::parse_upbit("KRW-BTC")?,
                                initial_cash: QuoteAmount::new(dec("1000000"))?,
                                costs: zero_cost_policy(),
                                market_rules: simple_rules(),
                            }],
                        },
                        started_at,
                    )
                })
                .await?;
            assert_eq!(session.session.markets[0].signal_count, 0);
            assert_eq!(session.session.markets[0].fill_count, 0);
            assert_eq!(session.session.markets[0].last_finalized_candle_at, None);

            let mut owner_batch = constant_bars(
                "fresh-owner",
                UtcTimestamp(boundary.0 - chrono::Duration::minutes(99)),
                99,
                CandleInterval::M1,
                "100",
            );
            owner_batch.extend([
                candle_obs_at(
                    "post-start-1",
                    "KRW-BTC",
                    CandleInterval::M1,
                    boundary,
                    "100",
                    "101",
                ),
                candle_obs_at(
                    "post-start-2",
                    "KRW-BTC",
                    CandleInterval::M1,
                    UtcTimestamp(boundary.0 + chrono::Duration::minutes(1)),
                    "101",
                    "102",
                ),
                candle_obs_at(
                    "post-start-3",
                    "KRW-BTC",
                    CandleInterval::M1,
                    UtcTimestamp(boundary.0 + chrono::Duration::minutes(2)),
                    "102",
                    "103",
                ),
            ]);
            let reconcile_at = owner_batch
                .last()
                .expect("post-start bars")
                .candle
                .close_time_utc;
            let session_id = session.session.session_id.clone();
            database
                .call("paper_test_reconcile_fresh_owner_batch", move |store| {
                    let batch =
                        synthetic_store_batch(store, owner_batch, reconcile_at, "fresh poll")?;
                    store.reconcile_running_paper_sessions(&[batch], reconcile_at)
                })
                .await?;

            let session_id_for_read = session_id.clone();
            let (view, events) = database
                .call("paper_test_read_fresh_owner_result", move |store| {
                    Ok((
                        store
                            .paper_get_session(&session_id_for_read, reconcile_at)?
                            .session,
                        store.paper_list_events(&session_id, None, 100)?,
                    ))
                })
                .await?;
            assert_eq!(view.state, PaperSessionState::Running);
            assert!(view.markets[0].signal_count >= 1);
            let signals = events
                .items
                .iter()
                .flat_map(|event| &event.signals)
                .collect::<Vec<_>>();
            assert!(signals.iter().any(|signal| {
                signal.signal_time > started_at
                    && signal.signal_time == UtcTimestamp(boundary.0 + chrono::Duration::minutes(1))
                    && signal.indicators.completed_bars_seen == 100
            }));
            assert!(events.items.iter().all(|event| {
                event
                    .signals
                    .iter()
                    .all(|signal| signal.signal_time > started_at)
                    && event.fills.iter().all(|fill| match &fill.timing {
                        crate::contracts::FillTiming::Exact { at } => *at > started_at,
                        crate::contracts::FillTiming::Interval { accounting_at, .. } => {
                            *accounting_at > started_at
                        }
                    })
            }));
            Ok::<(), LabError>(())
        });
        drop(runtime);
        let closed = owner.shutdown();
        std::fs::remove_dir_all(&root)?;
        result?;
        closed?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn paper_store_lifecycle_cas_idempotency_pause_resume_stop_and_delete_protection()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-paper-store-{}-{nonce}",
            std::process::id()
        ));
        let mut store = crate::storage::Store::open(&root)?;
        let outcome = (|| {
            let builtins = crate::contracts::builtin_policy_definitions()?;
            store.seed_builtin_policies_once(&builtins, ts("2025-01-01T00:00:00Z"))?;
            let options = store.paper_start_options()?;
            assert!(!options.policy_options.is_empty());
            let s1_option = options
                .policy_options
                .iter()
                .find(|p| p.family == StrategyKind::S1)
                .unwrap_or(&options.policy_options[0])
                .clone();

            let start_now = ts("2025-01-02T00:00:00Z");
            let start_cmd = PaperSessionWrite::Start {
                request_id: RequestId::new("req-paper-start-001")?,
                policy_ref: s1_option.reference.clone(),
                decision_interval: CandleInterval::H1,
                execution_resolution: CandleInterval::H1,
                latency_ms: 0,
                execution: ExecutionPolicy::NextBarOpen {
                    participation_cap: Weight::new(Decimal::ONE)?,
                },
                markets: vec![
                    PaperMarketStart {
                        market: MarketId::parse_upbit("KRW-BTC")?,
                        initial_cash: QuoteAmount::new(dec("1000000"))?,
                        costs: zero_cost_policy(),
                        market_rules: simple_rules(),
                    },
                    PaperMarketStart {
                        market: MarketId::parse_upbit("KRW-ETH")?,
                        initial_cash: QuoteAmount::new(dec("500000"))?,
                        costs: zero_cost_policy(),
                        market_rules: simple_rules(),
                    },
                ],
            };

            let created = store.paper_write(&start_cmd, start_now)?;
            assert_eq!(created.receipt.outcome, PaperCommandOutcome::Applied);
            assert_eq!(created.session.state, PaperSessionState::Running);
            assert_eq!(created.session.sequence, 0);
            assert_eq!(created.session.markets.len(), 2);

            // Idempotent replay with identical request_id and payload returns Replayed without mutating sequence.
            let replayed = store.paper_write(&start_cmd, start_now)?;
            assert_eq!(replayed.receipt.outcome, PaperCommandOutcome::Replayed);
            assert_eq!(replayed.session.session_id, created.session.session_id);
            assert_eq!(replayed.session.sequence, 0);

            // Unknown-outcome readback returns the durable receipt and current session.
            let readback =
                store.paper_readback_request(&RequestId::new("req-paper-start-001")?, start_now)?;
            assert!(readback.found);
            assert_eq!(
                readback.receipt.expect("receipt present").session_id,
                created.session.session_id
            );

            // Reusing request_id with a different payload must fail with Conflict.
            let mut mutated_cmd = start_cmd.clone();
            if let PaperSessionWrite::Start { latency_ms, .. } = &mut mutated_cmd {
                *latency_ms = 500;
            }
            assert!(matches!(
                store.paper_write(&mutated_cmd, start_now),
                Err(LabError::Conflict(_))
            ));

            // Policy deletion must be blocked while referenced by a paper session.
            let preview = store.delete_preview(
                &DeleteResource::Policy {
                    policy_id: s1_option.reference.policy_id.clone(),
                },
                start_now,
            )?;
            let expected_blocker = format!("paper:{}", created.session.session_id.as_str());
            assert!(preview.blockers.iter().any(|b| {
                b.class == DeleteBlockerClass::ProtectedReference && b.reference == expected_blocker
            }));

            let session_id = created.session.session_id.clone();
            let btc = MarketId::parse_upbit("KRW-BTC")?;

            // Step bars until a buy signal creates a pending order or fill.
            let bars = (0..35_i64)
                .map(|idx| {
                    let open_dt = start_now.0 + chrono::Duration::hours(idx);
                    let open_str = open_dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    let p = (100 + idx * 5).to_string();
                    let next_p = (100 + (idx + 1) * 5).to_string();
                    candle_obs(
                        &format!("btc-bar-{idx}"),
                        "KRW-BTC",
                        CandleInterval::H1,
                        &open_str,
                        &p,
                        &next_p,
                    )
                })
                .collect::<Vec<_>>();
            let step_now = bars.last().expect("last bar").candle.close_time_utc;
            let stepped = store.step_paper_session_bars(
                &session_id,
                &btc,
                &bars,
                &BTreeMap::new(),
                step_now,
            )?;
            assert_eq!(stepped.state, PaperSessionState::Running);
            assert!(stepped.sequence > 0);

            // Wrong expected_sequence on Pause must fail with Conflict.
            let bad_pause = PaperSessionWrite::Pause {
                request_id: RequestId::new("req-paper-pause-bad")?,
                session_id: session_id.clone(),
                expected_sequence: stepped.sequence + 99,
            };
            assert!(matches!(
                store.paper_write(&bad_pause, step_now),
                Err(LabError::Conflict(_))
            ));

            // Valid Pause transitions to Paused and increments sequence.
            let pause_cmd = PaperSessionWrite::Pause {
                request_id: RequestId::new("req-paper-pause-001")?,
                session_id: session_id.clone(),
                expected_sequence: stepped.sequence,
            };
            let paused = store.paper_write(&pause_cmd, step_now)?;
            assert_eq!(paused.session.state, PaperSessionState::Paused);
            assert_eq!(paused.session.sequence, stepped.sequence + 1);

            // Paused session must not appear in active_running_paper_polls or advance on reconcile.
            assert!(store.active_running_paper_polls()?.is_empty());
            assert_eq!(store.reconcile_running_paper_sessions(&[], step_now)?, 0);

            // Resume catches up paused bars without creating retroactive fills and returns to Running.
            let resume_now = UtcTimestamp(step_now.0 + chrono::Duration::hours(2));
            let resume_cmd = PaperSessionWrite::Resume {
                request_id: RequestId::new("req-paper-resume-001")?,
                session_id: session_id.clone(),
                expected_sequence: paused.session.sequence,
            };
            let resumed = store.paper_write(&resume_cmd, resume_now)?;
            assert_eq!(resumed.session.state, PaperSessionState::Running);
            assert_eq!(resumed.session.sequence, paused.session.sequence + 1);

            // Stop preserves held positions and ledger and transitions to Stopped.
            let halt_now = UtcTimestamp(resume_now.0 + chrono::Duration::minutes(10));
            let stop_cmd = PaperSessionWrite::Stop {
                request_id: RequestId::new("req-paper-stop-001")?,
                session_id: session_id.clone(),
                expected_sequence: resumed.session.sequence,
            };
            let halted = store.paper_write(&stop_cmd, halt_now)?;
            assert_eq!(halted.session.state, PaperSessionState::Stopped);
            assert_eq!(halted.session.sequence, resumed.session.sequence + 1);

            let events_page = store.paper_list_events(&session_id, None, 100)?;
            assert!(!events_page.items.is_empty());
            assert!(
                events_page
                    .items
                    .iter()
                    .any(|ev| ev.kind == PaperEventKind::Pause)
            );
            assert!(
                events_page
                    .items
                    .iter()
                    .any(|ev| ev.kind == PaperEventKind::Resume)
            );
            assert!(
                events_page
                    .items
                    .iter()
                    .any(|ev| ev.kind == PaperEventKind::Stop)
            );

            Ok::<(), LabError>(())
        })();
        store.close()?;
        std::fs::remove_dir_all(root)?;
        outcome?;
        Ok(())
    }

    struct PaperFeedTestRoot(std::path::PathBuf);

    impl PaperFeedTestRoot {
        fn new() -> Result<Self, std::io::Error> {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(std::io::Error::other)?
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "spot-lab-paper-feed-recovery-{}-{nonce}",
                std::process::id()
            ));
            std::fs::create_dir(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for PaperFeedTestRoot {
        fn drop(&mut self) {
            let _ignored = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Debug, Clone)]
    struct SyntheticPaperFeedRequest {
        stage: String,
        path: String,
        served_at: UtcTimestamp,
        authorization_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SyntheticPaperFeedPublication {
        stage: String,
        candle_close: UtcTimestamp,
        available_at: UtcTimestamp,
    }

    #[derive(Default)]
    struct SyntheticPaperFeedData {
        stage: String,
        rows: Vec<serde_json::Value>,
        h1_rows: Vec<serde_json::Value>,
        requests: Vec<SyntheticPaperFeedRequest>,
        publications: Vec<SyntheticPaperFeedPublication>,
    }

    #[derive(Clone)]
    struct SyntheticPaperFeed {
        inner: std::sync::Arc<std::sync::Mutex<SyntheticPaperFeedData>>,
    }

    impl SyntheticPaperFeed {
        fn new(
            rows: Vec<serde_json::Value>,
            h1_rows: Vec<serde_json::Value>,
            initial_available_at: UtcTimestamp,
        ) -> Self {
            let mut data = SyntheticPaperFeedData {
                stage: "GAP_WITHHELD".into(),
                rows,
                h1_rows,
                ..SyntheticPaperFeedData::default()
            };
            data.publications.push(SyntheticPaperFeedPublication {
                stage: "GAP_WITHHELD".into(),
                candle_close: UtcTimestamp(initial_available_at.0 - chrono::Duration::minutes(30)),
                available_at: initial_available_at,
            });
            Self {
                inner: std::sync::Arc::new(std::sync::Mutex::new(data)),
            }
        }

        fn publish_late_bar(
            &self,
            row: serde_json::Value,
            candle_close: UtcTimestamp,
            available_at: UtcTimestamp,
        ) {
            let mut data = self.inner.lock().expect("paper feed state lock");
            let stage = "LATE_BAR_PUBLISHED".to_owned();
            data.stage = stage.clone();
            data.rows.push(row);
            data.rows.sort_by(|left, right| {
                right["candle_date_time_utc"]
                    .as_str()
                    .cmp(&left["candle_date_time_utc"].as_str())
            });
            data.publications.push(SyntheticPaperFeedPublication {
                stage,
                candle_close,
                available_at,
            });
        }

        fn request_count(&self) -> usize {
            self.inner
                .lock()
                .expect("paper feed state lock")
                .requests
                .len()
        }

        fn requests(&self) -> Vec<SyntheticPaperFeedRequest> {
            self.inner
                .lock()
                .expect("paper feed state lock")
                .requests
                .clone()
        }

        fn publications(&self) -> Vec<SyntheticPaperFeedPublication> {
            self.inner
                .lock()
                .expect("paper feed state lock")
                .publications
                .clone()
        }
    }

    async fn synthetic_paper_feed_response(
        axum::extract::State(feed): axum::extract::State<SyntheticPaperFeed>,
        uri: axum::http::Uri,
        headers: axum::http::HeaderMap,
    ) -> (
        axum::http::StatusCode,
        [(&'static str, &'static str); 1],
        Vec<u8>,
    ) {
        let body = {
            let mut data = feed.inner.lock().expect("paper feed state lock");
            let stage = data.stage.clone();
            data.requests.push(SyntheticPaperFeedRequest {
                stage,
                path: uri.path().to_owned(),
                served_at: UtcTimestamp::now(),
                authorization_present: headers.contains_key(axum::http::header::AUTHORIZATION),
            });
            let rows = if uri.path().ends_with("/minutes/60") {
                &data.h1_rows
            } else {
                &data.rows
            };
            serde_json::to_vec(rows).expect("synthetic candle wire rows")
        };
        (
            axum::http::StatusCode::OK,
            [("Remaining-Req", "group=candle; min=1800; sec=9")],
            body,
        )
    }

    struct SyntheticPaperFeedServer {
        base_url: String,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
    }

    impl SyntheticPaperFeedServer {
        async fn start(feed: SyntheticPaperFeed) -> Result<Self, std::io::Error> {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let app = axum::Router::new()
                .fallback(synthetic_paper_feed_response)
                .with_state(feed);
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ignored = stopped.await;
                    })
                    .await
            });
            Ok(Self {
                base_url: format!("http://{address}"),
                stop: Some(stop),
                task: Some(task),
            })
        }

        async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
            if let Some(stop) = self.stop.take() {
                let _ignored = stop.send(());
            }
            if let Some(task) = self.task.take() {
                task.await??;
            }
            Ok(())
        }
    }

    impl Drop for SyntheticPaperFeedServer {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ignored = stop.send(());
            }
        }
    }

    struct SyntheticPaperWireFixture {
        initial_rows: Vec<serde_json::Value>,
        h1_rows: Vec<serde_json::Value>,
        late_rows: Vec<serde_json::Value>,
        missing_close: UtcTimestamp,
        second_missing_close: UtcTimestamp,
        latest_close: UtcTimestamp,
        started_at: UtcTimestamp,
        initial_available_at: UtcTimestamp,
    }

    fn synthetic_paper_wire_candle(open_at: UtcTimestamp, index: i64) -> serde_json::Value {
        let price = 100_000_i64 + index * 25;
        serde_json::json!({
            "market": "KRW-BTC",
            "candle_date_time_utc": open_at.0.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
            "opening_price": price,
            "high_price": price + 2,
            "low_price": price - 2,
            "trade_price": price + 1,
            "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": (price + 1) * 10,
        })
    }

    fn synthetic_paper_wire_fixture(
        now: UtcTimestamp,
    ) -> Result<SyntheticPaperWireFixture, LabError> {
        let latest_close = crate::scheduling::latest_completed_boundary(now, CandleInterval::M1)?;
        let first_open = UtcTimestamp(latest_close.0 - chrono::Duration::minutes(199));
        let started_at = UtcTimestamp(first_open.0 + chrono::Duration::minutes(100));
        let missing_index = 138_i64;
        let second_missing_index = missing_index + 1;
        let missing_open = UtcTimestamp(first_open.0 + chrono::Duration::minutes(missing_index));
        let missing_close = UtcTimestamp(missing_open.0 + CandleInterval::M1.duration());
        let second_missing_open =
            UtcTimestamp(first_open.0 + chrono::Duration::minutes(second_missing_index));
        let second_missing_close =
            UtcTimestamp(second_missing_open.0 + CandleInterval::M1.duration());
        let mut initial_rows = Vec::with_capacity(197);
        let mut late_rows = Vec::with_capacity(2);
        for index in 0..199_i64 {
            let open_at = UtcTimestamp(first_open.0 + chrono::Duration::minutes(index));
            let row = synthetic_paper_wire_candle(open_at, index);
            if index == missing_index || index == second_missing_index {
                late_rows.push(row);
            } else {
                initial_rows.push(row);
            }
        }
        initial_rows.sort_by(|left, right| {
            right["candle_date_time_utc"]
                .as_str()
                .cmp(&left["candle_date_time_utc"].as_str())
        });
        let latest_h1_close =
            crate::scheduling::latest_completed_boundary(now, CandleInterval::H1)?;
        let first_h1_open = UtcTimestamp(latest_h1_close.0 - chrono::Duration::hours(6));
        let mut h1_rows = (0..6_i64)
            .map(|index| {
                let open_at = UtcTimestamp(first_h1_open.0 + chrono::Duration::hours(index));
                synthetic_paper_wire_candle(open_at, 5_000 + index * 500)
            })
            .collect::<Vec<_>>();
        h1_rows.sort_by(|left, right| {
            right["candle_date_time_utc"]
                .as_str()
                .cmp(&left["candle_date_time_utc"].as_str())
        });
        Ok(SyntheticPaperWireFixture {
            initial_rows,
            h1_rows,
            late_rows,
            missing_close,
            second_missing_close,
            latest_close,
            started_at,
            initial_available_at: UtcTimestamp(missing_close.0 + chrono::Duration::minutes(30)),
        })
    }

    fn test_require(
        condition: bool,
        message: impl Into<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message.into()).into())
        }
    }

    fn test_require_eq<T: std::fmt::Debug + PartialEq>(
        expected: &T,
        actual: &T,
        label: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if expected == actual {
            Ok(())
        } else {
            Err(
                std::io::Error::other(format!("{label}: expected {expected:?}, got {actual:?}"))
                    .into(),
            )
        }
    }

    fn paper_economic_snapshot(
        market: &PaperMarketSnapshot,
    ) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value((
            market.cash_total,
            market.cash_free,
            market.cash_reserved,
            market.quantity,
            market.realized_pnl,
            market.total_fees,
            market.signal_count,
            market.fill_count,
        ))
    }

    struct PaperDurableTestSnapshot {
        view: serde_json::Value,
        receipt: Option<serde_json::Value>,
        events: serde_json::Value,
        checkpoint_digests: Vec<ContentHash>,
        observation_count: u64,
    }

    async fn paper_durable_test_snapshot(
        database: &crate::database::DatabaseHandle,
        session_id: PaperSessionId,
        request_id: RequestId,
    ) -> Result<PaperDurableTestSnapshot, LabError> {
        database
            .call("paper_recovery_durable_snapshot", move |store| {
                let view = store
                    .paper_get_session(&session_id, UtcTimestamp::now())?
                    .session;
                let readback = store.paper_readback_request(&request_id, UtcTimestamp::now())?;
                let mut events = Vec::new();
                let mut after_sequence = None;
                loop {
                    let page = store.paper_list_events(&session_id, after_sequence, 100)?;
                    events.extend(page.items);
                    after_sequence = page.next_cursor.map(|cursor| cursor.sequence);
                    if after_sequence.is_none() {
                        break;
                    }
                }
                Ok(PaperDurableTestSnapshot {
                    view: serde_json::to_value(view)?,
                    receipt: readback.receipt.map(serde_json::to_value).transpose()?,
                    events: serde_json::to_value(events)?,
                    checkpoint_digests: store.paper_checkpoint_digests_for_test(&session_id)?,
                    observation_count: store.paper_candle_observation_count_for_test(
                        &MarketId::parse_upbit("KRW-BTC")?,
                        CandleInterval::M1,
                    )?,
                })
            })
            .await
    }

    fn export_paper_recovery_projections_if_requested(
        export_root: Option<&std::path::Path>,
        before: &PaperSessionView,
        after: &PaperSessionView,
        fixture: &SyntheticPaperWireFixture,
        feed: &SyntheticPaperFeed,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(export_root) = export_root else {
            return Ok(());
        };
        if !export_root.is_absolute()
            || !export_root.is_dir()
            || std::fs::read_dir(export_root)?
                .next()
                .transpose()?
                .is_some()
        {
            return Err(
                "paper recovery projection export root must be an existing empty directory".into(),
            );
        }
        let before_bytes = serde_json::to_vec_pretty(before)?;
        let after_bytes = serde_json::to_vec_pretty(after)?;
        let before_path = export_root.join("paper-session-before-resume.json");
        let after_path = export_root.join("paper-session-after-resume.json");
        if !before_path.starts_with(export_root) || !after_path.starts_with(export_root) {
            return Err("paper recovery projection path escaped its export root".into());
        }
        std::fs::write(&before_path, &before_bytes)?;
        std::fs::write(&after_path, &after_bytes)?;
        let requests = feed.requests();
        let publications = feed.publications();
        let late = publications
            .get(1)
            .ok_or("synthetic late-bar publication receipt missing")?;
        let initial = publications
            .first()
            .ok_or("synthetic initial feed publication receipt missing")?;
        let receipt = serde_json::json!({
            "fixture_input_class": "SYNTHETIC_TEST_ONLY",
            "feed_endpoint": "loopback http via UpbitClient::synthetic_local",
            "request_count": requests.len(),
            "request_paths": requests.iter().map(|request| &request.path).collect::<Vec<_>>(),
            "request_stages": requests.iter().map(|request| &request.stage).collect::<Vec<_>>(),
            "request_served_at_utc": requests.iter().map(|request| request.served_at).collect::<Vec<_>>(),
            "authorization_header_present": requests.iter().any(|request| request.authorization_present),
            "candle_model_missing_close_utc": fixture.missing_close,
            "fixture_initial_publication_at_utc": initial.available_at,
            "fixture_late_available_at_utc": late.available_at,
            "fixture_late_available_lag_ms": (late.available_at.0 - late.candle_close.0).num_milliseconds(),
            "fixture_latest_close_utc": fixture.latest_close,
            "synthetic_first_availability_only": true,
            "not_actual_upbit_first_seen_or_fillability": true,
            "before_projection_sha256": ContentHash::of_bytes(&before_bytes),
            "after_projection_sha256": ContentHash::of_bytes(&after_bytes),
        });
        let receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
        std::fs::write(export_root.join("fixture-receipt.json"), &receipt_bytes)?;
        let files = [
            before_path,
            after_path,
            export_root.join("fixture-receipt.json"),
        ];
        for path in files {
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(format!(
                    "paper recovery projection is not a regular file: {}",
                    path.display()
                )
                .into());
            }
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one synthetic runner journey binds late feed, partial refusal, transaction rollback, CAS races, replay and reopen"
    )]
    fn paper_feed_gap_resume_is_explicit_idempotent_nonretroactive_and_restart_safe()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PaperFeedTestRoot::new()?;
        let fixture = synthetic_paper_wire_fixture(UtcTimestamp::now())?;
        let feed = SyntheticPaperFeed::new(
            fixture.initial_rows.clone(),
            fixture.h1_rows.clone(),
            fixture.initial_available_at,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let server = runtime.block_on(SyntheticPaperFeedServer::start(feed.clone()))?;
        let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
        let database = owner.handle();
        let started_at = fixture.started_at;
        let (session_id, start_response) = runtime.block_on(database.call(
            "paper_feed_recovery_test_start",
            move |store| {
                let policy_write = PolicyWrite::Create {
                    request_id: RequestId::new("paper-feed-gap-policy")?,
                    definition: mtf_rules_revision().definition,
                };
                let revision = store.write_policy(&policy_write, started_at)?;
                let write =
                    mtf_start_write("paper-feed-gap-start", revision.snapshot.reference.clone())?;
                let response = store.paper_write(&write, started_at)?;
                Ok((response.session.session_id.clone(), response))
            },
        ))?;

        let mut legacy_session_json = serde_json::to_value(&start_response.session)?;
        legacy_session_json
            .as_object_mut()
            .ok_or("serialized session view was not an object")?
            .remove("recovery_cause");
        let legacy_session: crate::contracts::PaperSessionView =
            serde_json::from_value(legacy_session_json)?;
        test_require_eq(
            &None,
            &legacy_session.recovery_cause,
            "old PaperSessionView without recovery_cause remains readable",
        )?;

        let client = crate::market_data::UpbitClient::synthetic_local(&server.base_url)?;
        let jobs = runtime.block_on(crate::jobs::JobRuntime::start(
            database.clone(),
            client.clone(),
            root.0.clone(),
            None,
        ))?;
        let service = jobs.service();
        let journey: Result<
            (PaperSessionId, RequestId, PaperDurableTestSnapshot),
            Box<dyn std::error::Error>,
        > = runtime.block_on(async {
            let initial_value = service
                .paper_session_query(crate::contracts::PaperSessionQuery::Get {
                    session_id: session_id.clone(),
                })
                .await?;
            let initial: crate::contracts::PaperSessionGetResponse =
                serde_json::from_value(initial_value)?;
            test_require_eq(
                &PaperSessionState::RecoveryRequired,
                &initial.session.state,
                "missing finalized boundary moves RUNNING session into recovery",
            )?;
            test_require_eq(
                &Some(PaperRecoveryCause::FeedGap),
                &initial.session.recovery_cause,
                "only the typed feed-gap cause is recoverable",
            )?;
            test_require(!feed.requests().is_empty(), "runner used synthetic feed")?;

            let initial_request = RequestId::new("paper-feed-gap-start")?;
            let before_resume = paper_durable_test_snapshot(
                &database,
                session_id.clone(),
                initial_request,
            )
            .await?;
            let before_view: crate::contracts::PaperSessionView =
                serde_json::from_value(before_resume.view.clone())?;
            let before_market = before_view
                .markets
                .first()
                .ok_or("paper market snapshot missing before resume")?;
            let before_economic = paper_economic_snapshot(before_market)?;
            test_require(
                before_market.last_finalized_candle_at.is_some_and(|last| {
                    last < fixture.missing_close
                }),
                "checkpoint remained before the withheld candle",
            )?;
            let before_events: Vec<crate::contracts::PaperSessionEvent> =
                serde_json::from_value(before_resume.events.clone())?;
            test_require(
                before_events.iter().any(|event| {
                    event.kind == PaperEventKind::CandleProcessed
                        && event.finalized_at.is_some_and(|at| at < fixture.missing_close)
                }),
                "contiguous prefix was durably processed before the gap",
            )?;
            test_require(
                !before_events.iter().any(|event| {
                    event.kind == PaperEventKind::CandleProcessed
                        && event.finalized_at.is_some_and(|at| at >= fixture.missing_close)
                }),
                "no candle after the gap mutated the prefix",
            )?;
            let partial_request = RequestId::new("paper-feed-gap-partial-resume")?;
            let partial_write = PaperSessionWrite::Resume {
                request_id: partial_request.clone(),
                session_id: session_id.clone(),
                expected_sequence: initial.session.sequence,
            };
            let partial = service.paper_session_write(partial_write).await?;
            test_require_eq(
                &PaperSessionState::RecoveryRequired,
                &partial.session.state,
                "a still-incomplete bounded batch stays recovery-required",
            )?;
            test_require_eq(
                &Some(PaperRecoveryCause::FeedGap),
                &partial.session.recovery_cause,
                "remaining feed gap retains its typed cause",
            )?;
            let after_partial = paper_durable_test_snapshot(
                &database,
                session_id.clone(),
                partial_request.clone(),
            )
            .await?;
            let partial_view: crate::contracts::PaperSessionView =
                serde_json::from_value(after_partial.view.clone())?;
            let partial_market = partial_view
                .markets
                .first()
                .ok_or("paper market snapshot missing after partial recovery")?;
            test_require_eq(
                &before_economic,
                &paper_economic_snapshot(partial_market)?,
                "partial gap retry does not advance economic state",
            )?;
            test_require_eq(
                &before_market.last_finalized_candle_at,
                &partial_market.last_finalized_candle_at,
                "partial gap retry does not advance the candle cursor",
            )?;
            test_require_eq(
                &before_resume.observation_count,
                &after_partial.observation_count,
                "duplicate prefix rows remain idempotent while the gap is absent",
            )?;
            let partial_events: Vec<crate::contracts::PaperSessionEvent> =
                serde_json::from_value(after_partial.events.clone())?;
            test_require(
                !partial_events.iter().any(|event| {
                    event.kind == PaperEventKind::CandleProcessed
                        && event.finalized_at.is_some_and(|at| at >= fixture.missing_close)
                }),
                "partial recovery emits no economic candle event beyond the gap",
            )?;

            let late_available_at = fixture.latest_close;
            feed.publish_late_bar(
                fixture.late_rows[0].clone(),
                fixture.missing_close,
                late_available_at,
            );
            test_require_eq(
                &3_600_000_i64,
                &(late_available_at.0 - fixture.missing_close.0).num_milliseconds(),
                "synthetic fixture withholds the bar for exactly one hour",
            )?;

            let intermediate_request = RequestId::new("paper-feed-gap-second-boundary-resume")?;
            let intermediate_write = PaperSessionWrite::Resume {
                request_id: intermediate_request.clone(),
                session_id: session_id.clone(),
                expected_sequence: partial.session.sequence,
            };
            let intermediate = service.paper_session_write(intermediate_write).await?;
            test_require_eq(
                &PaperSessionState::RecoveryRequired,
                &intermediate.session.state,
                "a later gap after a contiguous prefix still blocks the whole market catch-up",
            )?;
            let after_intermediate = paper_durable_test_snapshot(
                &database,
                session_id.clone(),
                intermediate_request,
            )
            .await?;
            let intermediate_view: crate::contracts::PaperSessionView =
                serde_json::from_value(after_intermediate.view.clone())?;
            let intermediate_market = intermediate_view
                .markets
                .first()
                .ok_or("paper market snapshot missing after second-boundary refusal")?;
            test_require_eq(
                &after_partial.checkpoint_digests,
                &after_intermediate.checkpoint_digests,
                "same-market later gap preserves the exact pre-resume checkpoint digest",
            )?;
            test_require_eq(
                &serde_json::to_value(partial_market)?,
                &serde_json::to_value(intermediate_market)?,
                "same-market later gap preserves every market projection",
            )?;
            test_require_eq(
                &(partial_view.sequence + 1),
                &intermediate_view.sequence,
                "failed catch-up records only the new session control sequence",
            )?;
            test_require_eq(
                &(partial_view.event_count + 1),
                &intermediate_view.event_count,
                "failed catch-up records only its truthful control event",
            )?;
            let intermediate_events: Vec<crate::contracts::PaperSessionEvent> =
                serde_json::from_value(after_intermediate.events.clone())?;
            test_require(
                !intermediate_events.iter().any(|event| {
                    event.kind == PaperEventKind::CandleProcessed
                        && event.finalized_at.is_some_and(|at| at >= fixture.missing_close)
                }),
                "same-market later gap emits no prefix CandleProcessed event",
            )?;

            feed.publish_late_bar(
                fixture.late_rows[1].clone(),
                fixture.second_missing_close,
                late_available_at,
            );
            test_require_eq(
                &3_540_000_i64,
                &(late_available_at.0 - fixture.second_missing_close.0).num_milliseconds(),
                "second synthetic boundary becomes available after its modeled 59-minute lag",
            )?;

            database
                .call("install_paper_resume_fault", |store| {
                    store.paper_resume_event_fault_for_test(true)
                })
                .await?;
            let retry_request = RequestId::new("paper-feed-gap-transaction-retry")?;
            let retry_write = PaperSessionWrite::Resume {
                request_id: retry_request.clone(),
                session_id: session_id.clone(),
                expected_sequence: intermediate.session.sequence,
            };
            let faulted = service.paper_session_write(retry_write.clone()).await;
            test_require(faulted.is_err(), "synthetic event trigger aborts recovery transaction")?;
            database
                .call("remove_paper_resume_fault", |store| {
                    store.paper_resume_event_fault_for_test(false)
                })
                .await?;
            let after_fault = paper_durable_test_snapshot(
                &database,
                session_id.clone(),
                retry_request.clone(),
            )
            .await?;
            test_require(after_fault.receipt.is_none(), "rolled back recovery wrote no receipt")?;
            test_require_eq(
                &after_intermediate.view,
                &after_fault.view,
                "transaction fault preserved prior session view",
            )?;
            test_require_eq(
                &after_intermediate.events,
                &after_fault.events,
                "transaction fault preserved event rows and sequences",
            )?;
            test_require_eq(
                &after_intermediate.checkpoint_digests,
                &after_fault.checkpoint_digests,
                "transaction fault rolled back checkpoint changes",
            )?;
            test_require_eq(
                &after_intermediate.observation_count,
                &after_fault.observation_count,
                "transaction fault rolled back newly inserted observations",
            )?;

            let distinct_request = RequestId::new("paper-feed-gap-distinct-racer")?;
            let distinct_write = PaperSessionWrite::Resume {
                request_id: distinct_request.clone(),
                session_id: session_id.clone(),
                expected_sequence: intermediate.session.sequence,
            };
            let same_one = retry_write.clone();
            let same_two = retry_write.clone();
            let (same_result_one, same_result_two, distinct_result) = tokio::join!(
                service.paper_session_write(same_one),
                service.paper_session_write(same_two),
                service.paper_session_write(distinct_write),
            );
            let same_results = [&same_result_one, &same_result_two];
            let applied_count = same_results
                .iter()
                .filter(|result| {
                    matches!(result, Ok(response) if response.receipt.outcome == crate::contracts::PaperCommandOutcome::Applied)
                })
                .count();
            let replayed_count = same_results
                .iter()
                .filter(|result| {
                    matches!(result, Ok(response) if response.receipt.outcome == crate::contracts::PaperCommandOutcome::Replayed)
                })
                .count();
            test_require_eq(&1, &applied_count, "same RequestId has one applied recovery")?;
            test_require_eq(&1, &replayed_count, "concurrent same RequestId reads the winning receipt")?;
            test_require(
                matches!(distinct_result, Err(LabError::Conflict(_))),
                "distinct stale RequestId loses the expected-sequence CAS",
            )?;

            let applied_response = same_results
                .iter()
                .find_map(|result| {
                    result.as_ref().ok().filter(|response| {
                        response.receipt.outcome == crate::contracts::PaperCommandOutcome::Applied
                    })
                })
                .ok_or("applied same-RequestId response missing")?;
            test_require_eq(
                &PaperSessionState::Running,
                &applied_response.session.state,
                "explicit feed-gap resume returns to RUNNING after contiguous catch-up",
            )?;
            test_require_eq(
                &None,
                &applied_response.session.recovery_cause,
                "successful recovery clears typed cause",
            )?;

            let request_count_before_retry = feed.request_count();
            let replay = service.paper_session_write(retry_write.clone()).await?;
            let request_count_after_retry = feed.request_count();
            test_require_eq(
                &crate::contracts::PaperCommandOutcome::Replayed,
                &replay.receipt.outcome,
                "committed recovery retry returns its durable receipt",
            )?;
            test_require_eq(
                &request_count_before_retry,
                &request_count_after_retry,
                "committed RequestId retry performs no additional feed GET",
            )?;

            let recovered_snapshot = paper_durable_test_snapshot(
                &database,
                session_id.clone(),
                retry_request.clone(),
            )
            .await?;
            let recovered_view: crate::contracts::PaperSessionView =
                serde_json::from_value(recovered_snapshot.view.clone())?;
            let recovered_market = recovered_view
                .markets
                .first()
                .ok_or("paper market snapshot missing after recovery")?;
            test_require_eq(
                &Some(fixture.latest_close),
                &recovered_market.last_finalized_candle_at,
                "recovery cursor reaches the fixture's newest finalized boundary",
            )?;
            test_require_eq(
                &before_economic,
                &paper_economic_snapshot(recovered_market)?,
                "historical catch-up creates no retroactive signals, fills or orders",
            )?;
            test_require(
                recovered_market.last_account_mark_at.is_some(),
                "catch-up projection contains a current account mark",
            )?;

            let recovered_events: Vec<crate::contracts::PaperSessionEvent> =
                serde_json::from_value(recovered_snapshot.events.clone())?;
            let sequences = recovered_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>();
            let unique_sequences = sequences.iter().copied().collect::<std::collections::BTreeSet<_>>();
            test_require_eq(
                &sequences.len(),
                &unique_sequences.len(),
                "durable event sequences are unique",
            )?;
            test_require(
                sequences.windows(2).all(|pair| pair[0] < pair[1]),
                "durable event sequences are monotone",
            )?;
            test_require_eq(
                &u64::try_from(recovered_events.len()).unwrap_or(u64::MAX),
                &recovered_view.event_count,
                "event rows reconcile with session event_count",
            )?;
            let market_events = recovered_events
                .iter()
                .filter(|event| event.market.as_ref().is_some_and(|market| market == &recovered_market.market))
                .count();
            test_require_eq(
                &u64::try_from(market_events).unwrap_or(u64::MAX),
                &recovered_market.event_count,
                "market event rows reconcile with checkpoint event_count",
            )?;
            let catch_up_events = recovered_events
                .iter()
                .filter(|event| {
                    event.kind == PaperEventKind::CandleProcessed
                        && event.candle.as_ref().is_some_and(|candle| {
                            candle.close_time >= fixture.missing_close
                        })
                })
                .collect::<Vec<_>>();
            let expected_catch_up_count = usize::try_from(
                (fixture.latest_close.0 - fixture.missing_close.0).num_minutes() + 1,
            )
            .map_err(std::io::Error::other)?;
            test_require_eq(
                &expected_catch_up_count,
                &catch_up_events.len(),
                "catch-up records each late and successor candle exactly once",
            )?;
            test_require(
                catch_up_events.iter().all(|event| {
                    event.signals.is_empty()
                        && event.orders.is_empty()
                        && event.order_events.is_empty()
                        && event.fills.is_empty()
                        && event.episodes.is_empty()
                }),
                "catch-up candle rows contain marks but no retroactive economic effects",
            )?;
            let latest_mark_time = catch_up_events
                .iter()
                .flat_map(|event| event.account_marks.iter())
                .map(|mark| mark.context.accounting_event_time)
                .max();
            test_require_eq(
                &latest_mark_time,
                &recovered_market.last_account_mark_at,
                "catch-up account-mark rows reconcile with the market projection",
            )?;
            let successful_receipt = recovered_snapshot
                .receipt
                .as_ref()
                .ok_or("successful recovery receipt missing")?;
            test_require_eq(
                &request_count_before_retry,
                &request_count_after_retry,
                "readback kept fetch count unchanged",
            )?;
            let requests = feed.requests();
            let paths = requests
                .iter()
                .map(|request| request.path.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            test_require(
                requests.iter().all(|request| {
                    matches!(request.path.as_str(), "/v1/candles/minutes/1" | "/v1/candles/minutes/60")
                        && !request.authorization_present
                }) && paths.contains("/v1/candles/minutes/1")
                    && paths.contains("/v1/candles/minutes/60"),
                "synthetic feed used both unauthenticated M1/H1 public candle paths",
            )?;
            let publications = feed.publications();
            let late_publication = publications
                .get(1)
                .ok_or("first late fixture publication receipt missing")?;
            test_require_eq(
                &fixture.missing_close,
                &late_publication.candle_close,
                "late fixture publication is bound to the missing candle",
            )?;
            test_require_eq(
                &fixture.latest_close,
                &late_publication.available_at,
                "fixture first-availability time remains separate from candle model time",
            )?;
            test_require(
                late_publication.stage == "LATE_BAR_PUBLISHED",
                "fixture records its late-bar publication stage",
            )?;
            let second_late_publication = publications
                .get(2)
                .ok_or("second late fixture publication receipt missing")?;
            test_require_eq(
                &fixture.second_missing_close,
                &second_late_publication.candle_close,
                "second late fixture publication is bound to the later gap",
            )?;
            let export_root = std::env::var_os("BACKCRAFT_PAPER_RECOVERY_EXPORT_ROOT")
                .map(std::path::PathBuf::from);
            export_paper_recovery_projections_if_requested(
                export_root.as_deref(),
                &initial.session,
                &recovered_view,
                &fixture,
                &feed,
            )?;
            let _ = successful_receipt;
            Ok((session_id.clone(), retry_request.clone(), recovered_snapshot))
        });
        drop(service);
        let job_shutdown = runtime.block_on(jobs.shutdown());
        let server_shutdown = runtime.block_on(server.shutdown());
        drop(runtime);
        let owner_shutdown = owner.shutdown();
        let (session_id, receipt_id, before_reopen) = journey?;
        job_shutdown?;
        server_shutdown?;
        owner_shutdown?;

        let reopened_owner = crate::database::DatabaseOwner::open(root.0.clone())?;
        let reopened_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let reopened = reopened_runtime.block_on(paper_durable_test_snapshot(
            &reopened_owner.handle(),
            session_id,
            receipt_id,
        ))?;
        test_require_eq(
            &before_reopen.view,
            &reopened.view,
            "session projection survived reopen",
        )?;
        test_require_eq(
            &before_reopen.receipt,
            &reopened.receipt,
            "receipt survived reopen",
        )?;
        test_require_eq(
            &before_reopen.events,
            &reopened.events,
            "event rows survived reopen",
        )?;
        test_require_eq(
            &before_reopen.checkpoint_digests,
            &reopened.checkpoint_digests,
            "checkpoint digest survived reopen",
        )?;
        test_require_eq(
            &before_reopen.observation_count,
            &reopened.observation_count,
            "observation rows survived reopen",
        )?;
        drop(reopened_runtime);
        reopened_owner.shutdown()?;
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the two-market regression binds atomic refusal, rollback, success, and receipt replay"
    )]
    fn paper_feed_gap_resume_is_atomic_across_markets() -> Result<(), Box<dyn std::error::Error>> {
        let root = PaperFeedTestRoot::new()?;
        let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
        let database = owner.handle();
        let started_at = ts("2024-01-01T04:00:00Z");
        let session_id = database.call_blocking("paper_multi_market_start", move |store| {
            let policy = PolicyWrite::Create {
                request_id: RequestId::new("paper-multi-market-policy")?,
                definition: mtf_buy_rules_revision().definition,
            };
            let revision = store.write_policy(&policy, started_at)?;
            let write =
                multi_market_start_write("paper-multi-market-start", revision.snapshot.reference)?;
            Ok(store.paper_write(&write, started_at)?.session.session_id)
        })?;

        let seed_now = ts("2024-01-01T04:06:00Z");
        let seed_session_id = session_id.clone();
        database.call_blocking("paper_multi_market_seed_gap", move |store| {
            let first_h1_open = UtcTimestamp(started_at.0 - chrono::Duration::hours(4));
            let mut btc = multi_market_h1_history(
                "KRW-BTC",
                "paper-multi-btc-seed-h1",
                first_h1_open,
                100_000,
            );
            btc.extend([
                multi_market_m1("paper-multi-btc-m1-0", "KRW-BTC", started_at, 100_000),
                multi_market_m1(
                    "paper-multi-btc-m1-1",
                    "KRW-BTC",
                    UtcTimestamp(started_at.0 + chrono::Duration::minutes(1)),
                    100_100,
                ),
                multi_market_m1(
                    "paper-multi-btc-m1-2",
                    "KRW-BTC",
                    UtcTimestamp(started_at.0 + chrono::Duration::minutes(2)),
                    100_200,
                ),
            ]);
            let btc_batch = synthetic_store_batch(store, btc, seed_now, "multi BTC prefix")?;

            let mut xrp = multi_market_h1_history(
                "KRW-XRP",
                "paper-multi-xrp-seed-h1",
                first_h1_open,
                200_000,
            );
            xrp.extend([
                multi_market_m1("paper-multi-xrp-m1-0", "KRW-XRP", started_at, 200_000),
                multi_market_m1(
                    "paper-multi-xrp-m1-1",
                    "KRW-XRP",
                    UtcTimestamp(started_at.0 + chrono::Duration::minutes(1)),
                    200_100,
                ),
                multi_market_m1(
                    "paper-multi-xrp-m1-2",
                    "KRW-XRP",
                    UtcTimestamp(started_at.0 + chrono::Duration::minutes(2)),
                    200_200,
                ),
                multi_market_m1(
                    "paper-multi-xrp-after-gap",
                    "KRW-XRP",
                    UtcTimestamp(started_at.0 + chrono::Duration::minutes(4)),
                    200_400,
                ),
            ]);
            let xrp_batch = synthetic_store_batch(store, xrp, seed_now, "multi XRP gap")?;
            store.reconcile_running_paper_sessions(&[btc_batch, xrp_batch], seed_now)?;
            let session = store.paper_get_session(&seed_session_id, seed_now)?.session;
            if session.state != PaperSessionState::RecoveryRequired
                || session.recovery_cause != Some(PaperRecoveryCause::FeedGap)
                || session.markets.len() != 2
                || session.markets.iter().any(|market| market.fill_count == 0)
            {
                return Err(LabError::DataCorrupt(
                    "multi-market fixture did not create a filled FEED_GAP session".into(),
                ));
            }
            Ok(())
        })?;

        let before_session_id = session_id.clone();
        let before = database.call_blocking("paper_multi_market_before_resume", move |store| {
            paper_store_test_snapshot(store, &before_session_id, seed_now)
        })?;
        test_require_eq(
            &PaperSessionState::RecoveryRequired,
            &before.view.state,
            "second market gap blocks the session before explicit recovery",
        )?;
        test_require_eq(&2, &before.view.markets.len(), "fixture has both markets")?;
        test_require(
            before
                .view
                .markets
                .iter()
                .all(|market| market.fill_count > 0),
            "distinct-price markets hold pre-gap positions so mark and equity changes are measurable",
        )?;

        let batch_session_id = session_id.clone();
        let expected_sequence = before.view.sequence;
        let partial_fetch =
            database.call_blocking("paper_multi_market_partial_batches", move |store| {
                multi_market_feed_gap_batches(
                    store,
                    &batch_session_id,
                    expected_sequence,
                    seed_now,
                    true,
                    "partial-multi-market",
                )
            })?;
        let partial_write = PaperSessionWrite::Resume {
            request_id: RequestId::new("paper-multi-market-partial")?,
            session_id: session_id.clone(),
            expected_sequence: before.view.sequence,
        };
        let partial_write_for_store = partial_write.clone();
        let partial =
            database.call_blocking("paper_multi_market_partial_resume", move |store| {
                store.paper_write_for_feed_gap_resume(
                    &partial_write_for_store,
                    &partial_fetch.requirements,
                    &partial_fetch.batches,
                    seed_now,
                )
            })?;
        let after_partial_session_id = session_id.clone();
        let after_partial = database
            .call_blocking("paper_multi_market_after_partial", move |store| {
                paper_store_test_snapshot(store, &after_partial_session_id, seed_now)
            })?;

        test_require_eq(
            &PaperSessionState::RecoveryRequired,
            &partial.session.state,
            "a missing later-market boundary remains blocked",
        )?;
        test_require_eq(
            &Some(PaperRecoveryCause::FeedGap),
            &partial.session.recovery_cause,
            "partial multi-market recovery retains the typed feed-gap cause",
        )?;
        test_require_eq(
            &before.checkpoint_digests,
            &after_partial.checkpoint_digests,
            "later-market gap preserves every original checkpoint digest",
        )?;
        test_require_eq(
            &serde_json::to_value(&before.view.markets)?,
            &serde_json::to_value(&after_partial.view.markets)?,
            "later-market gap leaves all market cursors and economics unchanged",
        )?;
        test_require_eq(
            &(before.view.sequence + 1),
            &after_partial.view.sequence,
            "failed multi-market recovery commits only its control sequence",
        )?;
        test_require_eq(
            &(before.view.event_count + 1),
            &after_partial.view.event_count,
            "failed multi-market recovery commits only its control event",
        )?;
        let new_partial_events = after_partial
            .events
            .iter()
            .filter(|event| event.sequence > before.view.sequence)
            .collect::<Vec<_>>();
        test_require_eq(
            &1,
            &new_partial_events.len(),
            "failed multi-market recovery emits one session-level acknowledgement",
        )?;
        test_require(
            new_partial_events.iter().all(|event| {
                event.kind == PaperEventKind::RecoveryRequired
                    && event.market.is_none()
                    && event.control.as_ref().is_some_and(|control| {
                        control.action == crate::contracts::PaperSessionCommandAction::Resume
                    })
                    && event.signals.is_empty()
                    && event.orders.is_empty()
                    && event.order_events.is_empty()
                    && event.fills.is_empty()
                    && event.account_marks.is_empty()
            }),
            "failed multi-market recovery stores no partial market event",
        )?;

        let complete_session_id = session_id.clone();
        let complete_sequence = after_partial.view.sequence;
        let complete_fetch =
            database.call_blocking("paper_multi_market_complete_batches", move |store| {
                multi_market_feed_gap_batches(
                    store,
                    &complete_session_id,
                    complete_sequence,
                    seed_now,
                    false,
                    "complete-multi-market",
                )
            })?;
        let successful_write = PaperSessionWrite::Resume {
            request_id: RequestId::new("paper-multi-market-success")?,
            session_id: session_id.clone(),
            expected_sequence: after_partial.view.sequence,
        };
        let fault_write = successful_write.clone();
        let fault_requirements = complete_fetch.requirements.clone();
        let fault_batches = complete_fetch.batches.clone();
        let fault_message =
            database.call_blocking("paper_multi_market_resume_fault", move |store| {
                store.paper_resume_event_fault_for_test(true)?;
                let result = store.paper_write_for_feed_gap_resume(
                    &fault_write,
                    &fault_requirements,
                    &fault_batches,
                    seed_now,
                );
                store.paper_resume_event_fault_for_test(false)?;
                match result {
                    Ok(_) => Err(LabError::Internal(
                        "synthetic resume event fault unexpectedly committed".into(),
                    )),
                    Err(error) => Ok(error.to_string()),
                }
            })?;
        test_require(
            fault_message.contains("synthetic paper resume transaction fault"),
            "success transaction fault is the injected SQLite event abort",
        )?;
        let after_fault_session_id = session_id.clone();
        let after_fault = database
            .call_blocking("paper_multi_market_after_fault", move |store| {
                paper_store_test_snapshot(store, &after_fault_session_id, seed_now)
            })?;
        test_require_eq(
            &serde_json::to_value(&after_partial.view)?,
            &serde_json::to_value(&after_fault.view)?,
            "multi-market transaction fault preserves session projection",
        )?;
        test_require_eq(
            &serde_json::to_value(&after_partial.events)?,
            &serde_json::to_value(&after_fault.events)?,
            "multi-market transaction fault preserves event rows",
        )?;
        test_require_eq(
            &after_partial.checkpoint_digests,
            &after_fault.checkpoint_digests,
            "multi-market transaction fault rolls back every checkpoint",
        )?;

        let replay_write = successful_write.clone();
        let successful =
            database.call_blocking("paper_multi_market_complete_resume", move |store| {
                store.paper_write_for_feed_gap_resume(
                    &successful_write,
                    &complete_fetch.requirements,
                    &complete_fetch.batches,
                    seed_now,
                )
            })?;
        let after_success = database
            .call_blocking("paper_multi_market_after_success", move |store| {
                paper_store_test_snapshot(store, &session_id, seed_now)
            })?;
        test_require_eq(
            &PaperSessionState::Running,
            &successful.session.state,
            "all-market complete catch-up resumes the session",
        )?;
        test_require_eq(
            &None,
            &successful.session.recovery_cause,
            "complete catch-up clears the typed recovery cause",
        )?;
        test_require_eq(
            &(after_partial.view.event_count + 7),
            &after_success.view.event_count,
            "complete catch-up commits six candle events and one control event",
        )?;
        let catch_up_events = after_success
            .events
            .iter()
            .filter(|event| {
                event.sequence > after_partial.view.sequence
                    && event.kind == PaperEventKind::CandleProcessed
            })
            .collect::<Vec<_>>();
        test_require_eq(
            &6,
            &catch_up_events.len(),
            "complete catch-up emits all six market candles after the gap",
        )?;
        test_require(
            catch_up_events.iter().all(|event| {
                event.signals.is_empty()
                    && event.orders.is_empty()
                    && event.order_events.is_empty()
                    && event.fills.is_empty()
                    && event.episodes.is_empty()
            }),
            "successful historical catch-up has no retroactive economic effects",
        )?;
        for (market, expected_mark) in [("KRW-BTC", "100600"), ("KRW-XRP", "200600")] {
            let before_market = before
                .view
                .markets
                .iter()
                .find(|snapshot| snapshot.market.code() == market)
                .ok_or("before market snapshot missing")?;
            let after_market = after_success
                .view
                .markets
                .iter()
                .find(|snapshot| snapshot.market.code() == market)
                .ok_or("after market snapshot missing")?;
            let before_projection = serde_json::to_value(before_market)?;
            let after_projection = serde_json::to_value(after_market)?;
            test_require(
                before_projection["mark_price"] != after_projection["mark_price"]
                    && before_projection["equity"] != after_projection["equity"],
                format!("{market} held-position mark and equity advance only after full catch-up"),
            )?;
            test_require_eq(
                &serde_json::Value::String(expected_mark.into()),
                &after_projection["mark_price"],
                "successful full catch-up reaches each distinct market mark",
            )?;
            test_require_eq(
                &paper_economic_snapshot(before_market)?,
                &paper_economic_snapshot(after_market)?,
                "successful catch-up preserves cash, quantity, signals, and fills",
            )?;
            test_require_eq(
                &before_market.fill_count,
                &after_market.fill_count,
                "catch-up does not add a retroactive fill",
            )?;
            test_require_eq(
                &before_market.signal_count,
                &after_market.signal_count,
                "catch-up does not add a retroactive signal",
            )?;
        }

        let replayed = database.call_blocking("paper_multi_market_replay", move |store| {
            store.paper_write(&replay_write, seed_now)
        })?;
        test_require_eq(
            &crate::contracts::PaperCommandOutcome::Replayed,
            &replayed.receipt.outcome,
            "committed multi-market request replays its durable receipt",
        )?;
        let after_replay = database
            .call_blocking("paper_multi_market_after_replay", move |store| {
                paper_store_test_snapshot(store, &replayed.session.session_id, seed_now)
            })?;
        test_require_eq(
            &serde_json::to_value(&after_success.view)?,
            &serde_json::to_value(&after_replay.view)?,
            "successful request replay does not change the session projection",
        )?;
        test_require_eq(
            &serde_json::to_value(&after_success.events)?,
            &serde_json::to_value(&after_replay.events)?,
            "successful request replay does not duplicate catch-up events",
        )?;
        test_require_eq(
            &after_success.checkpoint_digests,
            &after_replay.checkpoint_digests,
            "successful request replay does not change checkpoints",
        )?;

        owner.shutdown()?;
        Ok(())
    }

    struct P13GeneratedSourceRoot(std::path::PathBuf);

    impl Drop for P13GeneratedSourceRoot {
        fn drop(&mut self) {
            let _ignored = std::fs::remove_dir_all(&self.0);
        }
    }

    fn p13_fixture_destination() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        use std::path::Path;

        let destination =
            std::path::PathBuf::from(std::env::var("BACKCRAFT_P13_FIXTURE_EXPORT_ROOT")?);
        if !destination.is_absolute()
            || destination.parent() != Some(Path::new("/private/tmp"))
            || !destination
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|name| name.starts_with("backcraft-p13-fixture-"))
        {
            return Err("export root must be /private/tmp/backcraft-p13-fixture-*".into());
        }
        let mut ancestor = Some(destination.as_path());
        while let Some(path) = ancestor {
            if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
                return Err(
                    format!("fixture export path contains a symlink: {}", path.display()).into(),
                );
            }
            ancestor = path.parent();
        }
        if !std::fs::symlink_metadata(&destination)?.is_dir()
            || std::fs::read_dir(&destination)?
                .next()
                .transpose()?
                .is_some()
        {
            return Err("fixture export root must be an existing empty directory".into());
        }
        Ok(destination)
    }

    fn p13_fixture_as_of() -> Result<UtcTimestamp, Box<dyn std::error::Error>> {
        let requested = match std::env::var("BACKCRAFT_P13_FIXTURE_AS_OF") {
            Ok(value) => UtcTimestamp::parse_rfc3339(&value)?,
            Err(std::env::VarError::NotPresent) => UtcTimestamp::now(),
            Err(error) => return Err(error.into()),
        };
        Ok(crate::scheduling::latest_completed_boundary(
            requested,
            CandleInterval::H1,
        )?)
    }

    fn copy_new_regular_file(
        source: &std::path::Path,
        destination: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::io;
        use std::io::Write;

        let metadata = std::fs::symlink_metadata(source)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(
                format!("fixture source is not a regular file: {}", source.display()).into(),
            );
        }
        let mut input = std::fs::File::open(source)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        io::copy(&mut input, &mut output)?;
        output.flush()?;
        output.sync_all()?;
        Ok(())
    }

    fn copy_new_regular_tree(
        source: &std::path::Path,
        destination: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let metadata = std::fs::symlink_metadata(source)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(format!(
                "fixture source is not a regular directory: {}",
                source.display()
            )
            .into());
        }
        std::fs::create_dir(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let entry_metadata = std::fs::symlink_metadata(&source_path)?;
            if entry_metadata.file_type().is_symlink() {
                return Err(format!(
                    "fixture raw tree contains a symlink: {}",
                    source_path.display()
                )
                .into());
            }
            if entry_metadata.is_dir() {
                copy_new_regular_tree(&source_path, &destination_path)?;
            } else if entry_metadata.is_file() {
                copy_new_regular_file(&source_path, &destination_path)?;
            } else {
                return Err(format!(
                    "fixture raw tree contains a special file: {}",
                    source_path.display()
                )
                .into());
            }
        }
        Ok(())
    }

    fn collect_p13_fixture_hashes(
        root: &std::path::Path,
    ) -> Result<Vec<(String, ContentHash)>, Box<dyn std::error::Error>> {
        fn collect_files(
            root: &std::path::Path,
            current: &std::path::Path,
            paths: &mut Vec<std::path::PathBuf>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            for entry in std::fs::read_dir(current)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = std::fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    return Err(
                        format!("fixture export contains a symlink: {}", path.display()).into(),
                    );
                }
                if metadata.is_dir() {
                    collect_files(root, &path, paths)?;
                } else if metadata.is_file() {
                    paths.push(path);
                } else {
                    return Err(format!(
                        "fixture export contains a special file: {}",
                        path.display()
                    )
                    .into());
                }
            }
            let _ = root;
            Ok(())
        }

        let mut paths = Vec::new();
        collect_files(root, root, &mut paths)?;
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let relative = path.strip_prefix(root)?.to_string_lossy().into_owned();
                let digest = ContentHash::of_bytes(&std::fs::read(&path)?);
                Ok((relative, digest))
            })
            .collect()
    }

    fn p13_state_label(state: PaperSessionState) -> &'static str {
        match state {
            PaperSessionState::Running => "RUNNING",
            PaperSessionState::Paused => "PAUSED",
            PaperSessionState::Stopped => "STOPPED",
            PaperSessionState::RecoveryRequired => "RECOVERY_REQUIRED",
        }
    }

    fn p13_freshness_label(freshness: PaperFreshness) -> &'static str {
        match freshness {
            PaperFreshness::Live => "LIVE",
            PaperFreshness::Delayed => "DELAYED",
            PaperFreshness::Stale => "STALE",
            PaperFreshness::Offline => "OFFLINE",
        }
    }

    fn p13_start_mtf_pair_and_seed(
        database: &crate::database::DatabaseHandle,
        policy_ref: &PolicyRevisionRef,
        as_of: UtcTimestamp,
    ) -> Result<(PaperSessionId, PaperSessionId, UtcTimestamp), LabError> {
        let gap_boundary = UtcTimestamp(as_of.0 - chrono::Duration::hours(2));
        let first_m1_open = UtcTimestamp(gap_boundary.0 - chrono::Duration::minutes(1));
        let started_at = first_m1_open;
        let prior_h1_boundary = UtcTimestamp(gap_boundary.0 - chrono::Duration::hours(1));
        let source_first_open = UtcTimestamp(prior_h1_boundary.0 - chrono::Duration::hours(3));
        let recovery_policy = policy_ref.clone();
        let sibling_policy = policy_ref.clone();
        let (recovery_id, sibling_id) =
            database.call_blocking("p13_fixture_start_mtf_sessions", move |store| {
                let recovery = store.paper_write(
                    &mtf_start_write("p13-mtf-recovery-start", recovery_policy)?,
                    started_at,
                )?;
                let sibling = store.paper_write(
                    &mtf_start_write("p13-mtf-paused-sibling-start", sibling_policy)?,
                    started_at,
                )?;
                Ok((recovery.session.session_id, sibling.session.session_id))
            })?;

        let mut first_batch = h1_history(
            "p13-mtf-prestart-h1",
            source_first_open,
            &["100", "100", "100"],
        );
        first_batch.push(candle_obs_at(
            "p13-mtf-first-m1",
            "KRW-BTC",
            CandleInterval::M1,
            first_m1_open,
            "100",
            "100",
        ));
        database.call_blocking("p13_fixture_publish_mtf_seed", move |store| {
            let batch = synthetic_store_batch(store, first_batch, gap_boundary, "M1 H1 seed")?;
            store.reconcile_running_paper_sessions(&[batch], gap_boundary)?;
            Ok(())
        })?;
        Ok((recovery_id, sibling_id, gap_boundary))
    }

    fn p13_create_mtf_gap_and_paused_sibling(
        database: &crate::database::DatabaseHandle,
        policy_ref: &PolicyRevisionRef,
        as_of: UtcTimestamp,
    ) -> Result<serde_json::Value, LabError> {
        let (recovery_id, sibling_id, gap_boundary) =
            p13_start_mtf_pair_and_seed(database, policy_ref, as_of)?;
        let sibling_id_for_pause = sibling_id.clone();
        let sibling_pause =
            database.call_blocking("p13_fixture_pause_mtf_sibling", move |store| {
                let view = store
                    .paper_get_session(&sibling_id_for_pause, gap_boundary)?
                    .session;
                store.paper_write(
                    &PaperSessionWrite::Pause {
                        request_id: RequestId::new("p13-mtf-paused-sibling-pause")?,
                        session_id: sibling_id_for_pause,
                        expected_sequence: view.sequence,
                    },
                    gap_boundary,
                )
            })?;
        if sibling_pause.session.state != PaperSessionState::Paused {
            return Err(LabError::Internal(
                "P13 fixture sibling did not persist PAUSED".into(),
            ));
        }

        let second_m1 = candle_obs_at(
            "p13-mtf-missing-h1-successor",
            "KRW-BTC",
            CandleInterval::M1,
            gap_boundary,
            "100",
            "100",
        );
        let gap_observed_at = second_m1.candle.close_time_utc;
        database.call_blocking("p13_fixture_publish_missing_mtf_successor", move |store| {
            let batch = synthetic_store_batch(
                store,
                vec![second_m1],
                gap_observed_at,
                "missing H1 successor",
            )?;
            store.reconcile_running_paper_sessions(&[batch], gap_observed_at)?;
            Ok(())
        })?;

        let recovery_id_for_read = recovery_id.clone();
        let sibling_id_for_read = sibling_id.clone();
        database.call_blocking("p13_fixture_verify_mtf_recovery", move |store| {
            let recovery = store
                .paper_get_session(&recovery_id_for_read, as_of)?
                .session;
            let sibling = store
                .paper_get_session(&sibling_id_for_read, as_of)?
                .session;
            let recovery_events = store.paper_list_events(&recovery_id_for_read, None, 100)?;
            if recovery.state != PaperSessionState::RecoveryRequired
                || recovery.markets[0].signal_count != 0
                || recovery.markets[0].fill_count != 0
                || sibling.state != PaperSessionState::Paused
                || sibling.markets[0].signal_count != 0
                || sibling.markets[0].fill_count != 0
                || recovery_events
                    .items
                    .iter()
                    .all(|event| event.kind != crate::contracts::PaperEventKind::RecoveryRequired)
                || recovery_events.items.iter().any(|event| {
                    !event.signals.is_empty()
                        || !event.orders.is_empty()
                        || !event.order_events.is_empty()
                        || !event.fills.is_empty()
                })
            {
                return Err(LabError::DataCorrupt(
                    "P13 MTF recovery fixture failed its durable flat-state contract".into(),
                ));
            }
            Ok(serde_json::json!({
                "recovery_session_id": recovery_id_for_read.as_str(),
                "recovery_state": p13_state_label(recovery.state),
                "recovery_signals": recovery.markets[0].signal_count,
                "recovery_fills": recovery.markets[0].fill_count,
                "paused_sibling_session_id": sibling_id_for_read.as_str(),
                "paused_sibling_state": p13_state_label(sibling.state),
                "paused_sibling_signals": sibling.markets[0].signal_count,
                "paused_sibling_fills": sibling.markets[0].fill_count,
                "source_boundary_missing": gap_observed_at,
                "fixture_input_class": "SYNTHETIC_TEST_ONLY"
            }))
        })
    }

    fn p13_create_paused_freshness_snapshot(
        database: &crate::database::DatabaseHandle,
        buy_hold_policy_ref: &PolicyRevisionRef,
        as_of: UtcTimestamp,
        label: &'static str,
        age_hours: i64,
        market: &'static str,
        expected_freshness: PaperFreshness,
    ) -> Result<serde_json::Value, LabError> {
        let finalized_at = UtcTimestamp(as_of.0 - chrono::Duration::hours(age_hours));
        let opened_at = UtcTimestamp(finalized_at.0 - CandleInterval::H1.duration());
        let observed_at = UtcTimestamp(finalized_at.0 + chrono::Duration::hours(1));
        let request_id = format!("p13-{label}-start");
        let policy_ref = buy_hold_policy_ref.clone();
        database.call_blocking("p13_fixture_create_paused_snapshot", move |store| {
            let started = store.paper_write(
                &paper_start_write_for_interval(
                    &request_id,
                    policy_ref,
                    CandleInterval::H1,
                    market,
                )?,
                opened_at,
            )?;
            let session_id = started.session.session_id;
            if started.session.state != PaperSessionState::Running {
                return Err(LabError::DataCorrupt(format!(
                    "P13 {label} start returned {}",
                    p13_state_label(started.session.state)
                )));
            }
            let bar = candle_obs_at(
                &format!("p13-{label}-flat-h1"),
                market,
                CandleInterval::H1,
                opened_at,
                "100",
                "100",
            );
            let batch = synthetic_store_batch(store, vec![bar], observed_at, "flat H1 marker")?;
            let advanced = store.reconcile_running_paper_sessions(&[batch], observed_at)?;
            let current = store.paper_get_session(&session_id, as_of)?.session;
            if current.markets[0].last_finalized_candle_at != Some(finalized_at) {
                return Err(LabError::DataCorrupt(format!(
                    "P13 {label} candle was not reconciled: advanced={advanced}, state={}, sequence={}, events={}, last_finalized={:?}, started_at={opened_at}, observed_at={observed_at}, as_of={as_of}",
                    p13_state_label(current.state),
                    current.sequence,
                    current.event_count,
                    current.markets[0].last_finalized_candle_at,
                )));
            }
            let paused = store.paper_write(
                &PaperSessionWrite::Pause {
                    request_id: RequestId::new(format!("p13-{label}-pause"))?,
                    session_id: session_id.clone(),
                    expected_sequence: current.sequence,
                },
                as_of,
            )?;
            let events = store.paper_list_events(&session_id, None, 100)?;
            let market_view = &paused.session.markets[0];
            if paused.session.state != PaperSessionState::Paused
                || market_view.freshness != expected_freshness
                || market_view.last_finalized_candle_at != Some(finalized_at)
                || market_view.last_source_event_at != Some(observed_at)
                || observed_at.0 - finalized_at.0 != chrono::Duration::hours(1)
                || market_view.signal_count != 0
                || market_view.fill_count != 0
                || events.items.iter().any(|event| {
                    !event.signals.is_empty()
                        || !event.orders.is_empty()
                        || !event.order_events.is_empty()
                        || !event.fills.is_empty()
                })
            {
                return Err(LabError::DataCorrupt(format!(
                    "P13 {label} snapshot mismatch: state={}, freshness={}, expected_freshness={}, finalized={:?}/{finalized_at:?}, source={:?}/{observed_at:?}, signals={}, fills={}, age_seconds={}",
                    p13_state_label(paused.session.state),
                    p13_freshness_label(market_view.freshness),
                    p13_freshness_label(expected_freshness),
                    market_view.last_finalized_candle_at,
                    market_view.last_source_event_at,
                    market_view.signal_count,
                    market_view.fill_count,
                    (as_of.0 - finalized_at.0).num_seconds(),
                )));
            }
            Ok(serde_json::json!({
                "label": label,
                "session_id": session_id.as_str(),
                "state": p13_state_label(paused.session.state),
                "freshness": p13_freshness_label(market_view.freshness),
                "decision_interval": "H1",
                "age_seconds_at_as_of": (as_of.0 - finalized_at.0).num_seconds(),
                "last_finalized_candle_at": finalized_at,
                "last_source_event_at": observed_at,
                "source_lag_seconds": 3600,
                "signals": market_view.signal_count,
                "fills": market_view.fill_count,
                "fixture_input_class": "SYNTHETIC_TEST_ONLY"
            }))
        })
    }

    fn export_p13_paper_fixture(
        destination: &std::path::Path,
        as_of: UtcTimestamp,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source_root = std::env::temp_dir().join(format!(
            "spot-lab-p13-fixture-source-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&source_root)?;
        let _source_guard = P13GeneratedSourceRoot(source_root.clone());
        let scenarios = p13_generate_closed_source_root(&source_root, as_of)?;
        p13_copy_closed_source_root(&source_root, destination, as_of, &scenarios)
    }

    fn p13_generate_closed_source_root(
        source_root: &std::path::Path,
        as_of: UtcTimestamp,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let owner = crate::database::DatabaseOwner::open(source_root.to_path_buf())?;
        let database = owner.handle();
        let policy_created_at = UtcTimestamp(as_of.0 - chrono::Duration::hours(10));
        let policy_request = PolicyWrite::Create {
            request_id: RequestId::new("p13-export-mtf-policy")?,
            definition: mtf_rules_revision().definition,
        };
        let policy_ref = database.call_blocking("p13_fixture_create_policy", move |store| {
            let revision = store.write_policy(&policy_request, policy_created_at)?;
            let option = store
                .paper_start_options()?
                .policy_options
                .into_iter()
                .find(|option| option.reference == revision.snapshot.reference)
                .ok_or_else(|| LabError::DataCorrupt("P13 MTF policy option missing".into()))?;
            if !option.supported {
                return Err(LabError::InvalidConfig(
                    "P13 MTF fixture policy is not supported".into(),
                ));
            }
            Ok(revision.snapshot.reference)
        })?;
        let mtf = p13_create_mtf_gap_and_paused_sibling(&database, &policy_ref, as_of)?;
        let buy_hold_policy_ref = database.call_blocking("p13_fixture_find_buy_hold", |store| {
            store
                .paper_start_options()?
                .policy_options
                .into_iter()
                .find(|option| option.family == StrategyKind::BuyAndHold && option.supported)
                .map(|option| option.reference)
                .ok_or_else(|| {
                    LabError::InvalidConfig("P13 Buy-and-hold fixture policy unavailable".into())
                })
        })?;
        let freshness = [
            ("live", 1, "KRW-ETH", PaperFreshness::Live),
            ("delayed", 2, "KRW-XRP", PaperFreshness::Delayed),
            ("stale", 3, "KRW-ADA", PaperFreshness::Stale),
            ("offline", 5, "KRW-DOGE", PaperFreshness::Offline),
        ]
        .into_iter()
        .map(|(label, age_hours, market, expected)| {
            p13_create_paused_freshness_snapshot(
                &database,
                &buy_hold_policy_ref,
                as_of,
                label,
                age_hours,
                market,
                expected,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

        drop(database);
        owner.shutdown()?;
        let wal_path = source_root.join("lab.sqlite-wal");
        if wal_path.exists() && std::fs::metadata(&wal_path)?.len() != 0 {
            return Err(
                "DatabaseOwner shutdown left a nonempty WAL; refusing incomplete export".into(),
            );
        }
        Ok(serde_json::json!({
            "mtf": mtf,
            "paused_freshness_sessions": freshness
        }))
    }

    fn p13_copy_closed_source_root(
        source_root: &std::path::Path,
        destination: &std::path::Path,
        as_of: UtcTimestamp,
        scenarios: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if std::fs::read_dir(destination)?
            .next()
            .transpose()?
            .is_some()
        {
            return Err("fixture export destination changed after validation".into());
        }
        copy_new_regular_file(
            &source_root.join("lab.sqlite"),
            &destination.join("lab.sqlite"),
        )?;
        copy_new_regular_tree(&source_root.join("raw"), &destination.join("raw"))?;
        let file_hashes = collect_p13_fixture_hashes(destination)?;
        let database_sha256 = file_hashes
            .iter()
            .find(|(path, _)| path == "lab.sqlite")
            .map(|(_, digest)| digest.clone())
            .ok_or("fixture export omitted lab.sqlite")?;
        let fixture_sha256 = ContentHash::of_value(&file_hashes)?;
        let raw_object_count = file_hashes
            .iter()
            .filter(|(path, _)| path.ends_with("metadata.json"))
            .count();
        let mtf = scenarios
            .get("mtf")
            .cloned()
            .ok_or("fixture scenario receipt omitted MTF status")?;
        let freshness = scenarios
            .get("paused_freshness_sessions")
            .cloned()
            .ok_or("fixture scenario receipt omitted freshness snapshots")?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "fixture": "P13_SYNTHETIC_PAPER_MTF_ACCEPTANCE",
                "fixture_input_class": "SYNTHETIC_TEST_ONLY",
                "as_of_utc": as_of,
                "owner_shutdown_before_copy": true,
                "destination": destination,
                "mtf": mtf,
                "paused_freshness_sessions": freshness,
                "raw_object_count": raw_object_count,
                "database_sha256": database_sha256,
                "fixture_files_sha256": fixture_sha256,
                "fixture_file_count": file_hashes.len()
            }))?
        );
        Ok(())
    }

    #[test]
    #[ignore = "explicit synthetic P13 fixture export; requires BACKCRAFT_P13_FIXTURE_EXPORT_ROOT"]
    fn export_p13_paper_mtf_fixture_for_native_acceptance() -> Result<(), Box<dyn std::error::Error>>
    {
        let destination = p13_fixture_destination()?;
        let as_of = p13_fixture_as_of()?;
        export_p13_paper_fixture(&destination, as_of)
    }
}
