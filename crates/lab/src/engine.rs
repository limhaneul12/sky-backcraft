//! Pure deterministic model execution and exact spot-account accounting.

pub(crate) mod accounting;
pub(crate) mod execution;

use crate::contracts::{
    AccountMark, AdmissionStatus, AssetQuantity, CandleObservation, CausalExecutionPolicy,
    CostFeatureInputs, CostPolicy, CostProvenance, CostProxyInputs, DatasetSnapshot,
    ENGINE_VERSION, EpisodeId, EpisodeRecord, EpisodeStatus, EventContext, EvidenceSnapshot,
    ExecutionPolicy, FillId, FillRecord, FillTiming, LabError, MAX_DATASET_ROWS, MAX_MODEL_EVENTS,
    MarkKind, ModelAdmission, ModelLedger, ModelStatus, ObservationId, OrderId, OrderRecord,
    OrderStatus, PriceKrw, QuoteAmount, ReasonCode, ResolvedPlan, RunId, Side, SignalId,
    SignalOutcome, SignalRecord, SimulatedOrderType, StrategyBinding, TerminalPolicy, UtcRange,
    UtcTimestamp, Weight,
};
use crate::evidence::EvidenceEvaluator;
use crate::policy_engine::PolicyEvaluator;
use crate::strategy::{PositionView, StrategyEvaluation};
use accounting::{
    Account, aggregate_identity_within_tolerance, checked_add, checked_div, checked_mul,
    checked_sub,
};
use execution::{
    ExecutionDecision, PlannedFill, plan_market_arrival, plan_market_request, plan_passive_arrival,
    plan_passive_buy_request,
};
use rust_decimal::Decimal;

const STRATEGY_VERSION: &str = "spot-lab-strategy-v1";
const EXECUTION_POLICY_VERSION: &str = "next-bar-open-v2-decision-mark-fixed-base-quantity";
const PASSIVE_POLICY_VERSION: &str = "passive-buy-v1-strict-penetration-partial-once-ttl1";
const EPISODE_SAMPLING: &str = "EXECUTION_CLOSE_AND_AFTER_FILL_NET_PNL_V1";

/// Execute one admitted model against immutable inputs without clocks, I/O, or global state.
///
/// The caller owns cross-model ordering and passes the first sequence number reserved for this
/// model. Returned fact vectors are bounded by [`MAX_MODEL_EVENTS`].
///
/// # Errors
/// Rejects mismatched frozen inputs, unsupported G1 execution modes, invalid arithmetic,
/// event-capacity excess, or explicit cancellation.
#[allow(
    clippy::too_many_lines,
    reason = "the causal execution-grid loop is one ordered state-machine phase"
)]
pub fn run_model(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    evidence: Option<&EvidenceSnapshot>,
    run_id: &RunId,
    admission: &ModelAdmission,
    seq_start: u64,
    cancelled: &dyn Fn() -> bool,
) -> Result<ModelLedger, LabError> {
    plan.spec.validate()?;
    if plan.spec.capital_mode == Some(crate::contracts::CapitalMode::SharedPortfolio) {
        return Err(LabError::InvalidConfig(
            "SHARED_PORTFOLIO plans execute through the portfolio runner, not per-model backtests"
                .into(),
        ));
    }
    validate_admission(plan, admission)?;
    let strategy = crate::contracts::strategy_binding(plan, admission)?;
    if admission.status != AdmissionStatus::Eligible {
        return Ok(blocked_ledger(admission, strategy, seq_start));
    }
    let participation_cap = match plan.spec.execution {
        ExecutionPolicy::NextBarOpen { participation_cap }
        | ExecutionPolicy::PassiveBuy {
            participation_cap, ..
        } => participation_cap,
    };
    validate_frozen_inputs(plan, datasets, evidence)?;
    let (decision_bars, execution_bars) = match plan.spec.causal_execution {
        Some(CausalExecutionPolicy::DeclaredPolicyWarmup) => {
            let decision_warmup = u32::try_from(strategy.warmup_bars(plan)?).map_err(|_| {
                LabError::ResourceLimit("policy warmup exceeds range arithmetic".into())
            })?;
            let decision_range = plan
                .spec
                .range
                .with_warmup(decision_warmup, plan.spec.decision_interval)?;
            let execution_range = plan
                .spec
                .range
                .with_warmup(1, plan.spec.execution_resolution)?;
            (
                observations_for_range(
                    datasets,
                    &admission.market,
                    plan.spec.decision_interval,
                    Some(decision_range),
                )?,
                observations_for_range(
                    datasets,
                    &admission.market,
                    plan.spec.execution_resolution,
                    Some(execution_range),
                )?,
            )
        }
        None => (
            observations_for_range(
                datasets,
                &admission.market,
                plan.spec.decision_interval,
                None,
            )?,
            observations_for_range(
                datasets,
                &admission.market,
                plan.spec.execution_resolution,
                None,
            )?,
        ),
    };
    let initial_bar = execution_bars
        .iter()
        .copied()
        .find(|bar| bar.candle.open_time_utc == plan.spec.range.start())
        .ok_or_else(|| LabError::DataGap("missing execution bar at evaluation start".into()))?;
    let evidence_evaluator = Some(EvidenceEvaluator::new(evidence, plan.spec.pit_policy)?);
    let mut evaluator = PolicyEvaluator::compile(
        strategy.clone(),
        plan,
        admission.market.clone(),
        plan.spec.decision_interval,
    )?;
    let mut state = EngineState::new(
        run_id.clone(),
        admission,
        strategy.clone(),
        plan.spec.initial_cash,
        seq_start,
    )?;
    state.push_mark(
        MarkKind::Initial,
        initial_bar.candle.open,
        initial_bar.id.clone(),
        Weight::new(Decimal::ZERO)?,
        plan.spec.range.start(),
    )?;

    let start_execution = execution_bars
        .iter()
        .position(|bar| bar.candle.open_time_utc == plan.spec.range.start())
        .ok_or_else(|| LabError::DataGap("missing execution start grid point".into()))?;
    if start_execution == 0 {
        return Err(LabError::InsufficientWarmup(
            "execution needs one prior completed liquidity bar".into(),
        ));
    }
    let mut decision_index = 0usize;
    while decision_bars
        .get(decision_index)
        .is_some_and(|bar| bar.candle.close_time_utc < plan.spec.range.start())
    {
        let cash = PositionView {
            actual_qty: AssetQuantity::new(Decimal::ZERO)?,
            actual_weight: Weight::new(Decimal::ZERO)?,
            episode_opened_at: None,
            held_decision_bars: None,
        };
        evaluator.observe(
            decision_bars[decision_index],
            cash,
            evidence_evaluator.as_ref(),
        )?;
        decision_index += 1;
    }

    let mut pending: Option<PendingExecution> = None;
    for execution_index in start_execution..execution_bars.len() {
        check_cancelled(cancelled)?;
        let execution_bar = execution_bars[execution_index];
        let now = execution_bar.candle.open_time_utc;
        if now >= plan.spec.range.end() {
            break;
        }
        let liquidity_source = execution_bars[execution_index - 1];
        if liquidity_source.candle.close_time_utc != now {
            return Err(LabError::DataGap(
                "execution bar is not aligned with its prior liquidity source".into(),
            ));
        }
        if pending
            .as_ref()
            .is_some_and(|order| order.order.expires_at < now)
        {
            let expired = pending
                .take()
                .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
            state.expire_pending(&expired, now)?;
        }
        if pending
            .as_ref()
            .is_some_and(|order| order.order.order_type == SimulatedOrderType::PassiveBuyLimit)
        {
            let passive_liquidity = execution_index
                .checked_sub(2)
                .and_then(|index| execution_bars.get(index))
                .copied()
                .ok_or_else(|| {
                    LabError::InsufficientWarmup(
                        "passive execution needs a prior completed liquidity bar".into(),
                    )
                })?;
            state.process_passive_close(
                &mut pending,
                liquidity_source,
                passive_liquidity,
                participation_cap,
                plan,
            )?;
        }
        state.push_mark(
            MarkKind::ExecutionClose,
            liquidity_source.candle.close,
            liquidity_source.id.clone(),
            state.last_target,
            now,
        )?;
        state.process_pending_at(
            &mut pending,
            execution_bar,
            liquidity_source,
            participation_cap,
            plan,
            false,
        )?;

        if let Some(decision_bar) = decision_bars.get(decision_index).copied() {
            if decision_bar.candle.close_time_utc == now {
                let marked = state.account.mark(decision_bar.candle.close)?;
                let position = state.position_view(marked.actual_weight, now)?;
                let evaluation = evaluator
                    .observe(decision_bar, position, evidence_evaluator.as_ref())?
                    .ok_or_else(|| {
                        LabError::InsufficientWarmup(
                            "strategy warmup did not complete by evaluation start".into(),
                        )
                    })?;
                let expiry = evaluator.signal_expiry(
                    now,
                    plan.spec.decision_interval,
                    plan.spec.range.end(),
                )?;
                let signal_index =
                    state.push_signal(decision_bar, &evaluation, position.actual_weight, expiry)?;
                let target = evaluation.constrained_target_weight;
                if pending.as_ref().is_some_and(|order| order.target != target) {
                    let cancelled = pending
                        .take()
                        .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
                    state.cancel_pending(&cancelled, now, ReasonCode::Cancelled)?;
                }
                if pending.is_none() && target != position.actual_weight {
                    let decision_at = decision_bar.candle.close_time_utc;
                    let tradable = plan.spec.rules_at(decision_at)?.tradable_at(decision_at);
                    let effective_at = add_millis(now, plan.spec.latency_ms)?;
                    let order_expiry = passive_order_expiry(
                        &plan.spec.execution,
                        target > position.actual_weight,
                        &execution_bars,
                        execution_index,
                        effective_at,
                        expiry,
                    );
                    if tradable {
                        let position_value = checked_mul(
                            state.account.qty(),
                            decision_bar.candle.close.get(),
                            "reference position",
                        )?;
                        let reference_equity = checked_add(
                            state.account.cash_free()?,
                            position_value,
                            "reference equity",
                        )?;
                        let target_value =
                            checked_mul(reference_equity, target.get(), "target value")?;
                        let requested_notional =
                            checked_sub(target_value, position_value, "target gap")?.abs();
                        pending = state.create_pending(
                            signal_index,
                            target,
                            decision_bar,
                            liquidity_source,
                            requested_notional,
                            effective_at,
                            order_expiry,
                            plan,
                        )?;
                    } else {
                        state.push_rejected_order(
                            signal_index,
                            decision_at,
                            effective_at,
                            order_expiry,
                            ReasonCode::RuleBlocked,
                            plan,
                        )?;
                        state.signals[signal_index].outcome = SignalOutcome::RuleBlocked;
                    }
                } else if pending.is_some() {
                    state.signals[signal_index].outcome = SignalOutcome::OrderUnfilled;
                }
                state.last_target = target;
                decision_index += 1;
                state.process_pending_at(
                    &mut pending,
                    execution_bar,
                    liquidity_source,
                    participation_cap,
                    plan,
                    true,
                )?;
            } else if decision_bar.candle.close_time_utc < now {
                return Err(LabError::DataGap(
                    "decision close is not aligned to the execution grid".into(),
                ));
            }
        }
    }

    let terminal_bar = execution_bars
        .iter()
        .copied()
        .rev()
        .find(|bar| bar.candle.close_time_utc == plan.spec.range.end())
        .ok_or_else(|| LabError::DataGap("missing terminal execution mark".into()))?;
    if pending
        .as_ref()
        .is_some_and(|order| order.order.order_type == SimulatedOrderType::PassiveBuyLimit)
    {
        let terminal_index = execution_bars
            .iter()
            .position(|bar| bar.id == terminal_bar.id)
            .ok_or_else(|| LabError::Internal("terminal execution bar disappeared".into()))?;
        let passive_liquidity = terminal_index
            .checked_sub(1)
            .and_then(|index| execution_bars.get(index))
            .copied()
            .ok_or_else(|| {
                LabError::InsufficientWarmup(
                    "terminal passive bar lacks prior liquidity source".into(),
                )
            })?;
        state.process_passive_close(
            &mut pending,
            terminal_bar,
            passive_liquidity,
            participation_cap,
            plan,
        )?;
    }
    if pending
        .as_ref()
        .is_some_and(|order| order.order.expires_at < plan.spec.range.end())
    {
        let expired = pending
            .take()
            .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
        state.expire_pending(&expired, plan.spec.range.end())?;
    }
    state.push_mark(
        MarkKind::ExecutionClose,
        terminal_bar.candle.close,
        terminal_bar.id.clone(),
        state.last_target,
        plan.spec.range.end(),
    )?;
    if let Some(item) = pending.take() {
        state.expire_pending(&item, plan.spec.range.end())?;
    }

    if plan.spec.terminal_policy == TerminalPolicy::LiquidateScenario
        && state.account.qty() > Decimal::ZERO
    {
        state.execute_terminal_liquidation(terminal_bar, participation_cap, plan)?;
    }
    state.push_mark(
        MarkKind::Terminal,
        terminal_bar.candle.close,
        terminal_bar.id.clone(),
        state.last_target,
        terminal_bar.candle.close_time_utc,
    )?;
    state.finish_episode(
        terminal_bar.candle.close,
        terminal_bar.candle.close_time_utc,
    )?;
    Ok(state.into_ledger())
}

#[derive(Debug)]
struct PendingExecution {
    signal_index: usize,
    target: Weight,
    request: PlannedFill,
    order: OrderRecord,
    decision_reference: PriceKrw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMoment {
    MarketOpen,
    PassiveClose,
    TerminalClose,
}

#[derive(Debug)]
struct EngineState {
    run_id: RunId,
    model_id: crate::contracts::ModelId,
    market: crate::contracts::MarketId,
    strategy: StrategyBinding,
    account: Account,
    seq: u64,
    record_count: usize,
    signals: Vec<SignalRecord>,
    orders: Vec<OrderRecord>,
    order_events: Vec<OrderRecord>,
    fills: Vec<FillRecord>,
    episodes: Vec<EpisodeRecord>,
    active_episode: Option<ActiveEpisode>,
    marks: Vec<AccountMark>,
    last_target: Weight,
}

impl EngineState {
    fn new(
        run_id: RunId,
        admission: &ModelAdmission,
        strategy: StrategyBinding,
        initial_cash: QuoteAmount,
        seq_start: u64,
    ) -> Result<Self, LabError> {
        Ok(Self {
            run_id,
            model_id: admission.model_id.clone(),
            market: admission.market.clone(),
            strategy,
            account: Account::new(initial_cash),
            seq: seq_start,
            record_count: 0,
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            episodes: Vec::new(),
            active_episode: None,
            marks: Vec::new(),
            last_target: Weight::new(Decimal::ZERO)?,
        })
    }

    fn position_view(
        &mut self,
        actual_weight: Decimal,
        at: UtcTimestamp,
    ) -> Result<PositionView, LabError> {
        if let Some(episode) = &mut self.active_episode
            && at >= episode.opened_at
        {
            episode.decision_marks_since_open = episode
                .decision_marks_since_open
                .checked_add(1)
                .ok_or_else(|| {
                    LabError::ResourceLimit("episode holding counter overflow".into())
                })?;
        }
        let (opened_at, held) = match &self.active_episode {
            Some(episode) => (
                Some(episode.opened_at),
                Some(episode.decision_marks_since_open),
            ),
            None => (None, None),
        };
        Ok(PositionView {
            actual_qty: AssetQuantity::new(self.account.qty())?,
            actual_weight: Weight::new(actual_weight)?,
            episode_opened_at: opened_at,
            held_decision_bars: held,
        })
    }

    fn push_signal(
        &mut self,
        bar: &CandleObservation,
        evaluation: &StrategyEvaluation,
        actual_weight: Weight,
        valid_until: UtcTimestamp,
    ) -> Result<usize, LabError> {
        self.ensure_capacity(1)?;
        let context = self.next_context(evaluation.decision_time)?;
        let signal_id =
            SignalId::from_seed(&format!("model:{}:signal:bar:{}", self.model_id, bar.id));
        let intended_side = match evaluation.constrained_target_weight.cmp(&actual_weight) {
            std::cmp::Ordering::Greater => Some(Side::Buy),
            std::cmp::Ordering::Less => Some(Side::Sell),
            std::cmp::Ordering::Equal => None,
        };
        let no_action = intended_side.is_none();
        self.signals.push(SignalRecord {
            context,
            signal_id,
            strategy: self.strategy.kind(),
            strategy_version: STRATEGY_VERSION.into(),
            policy_ref: evaluation.policy_ref.clone(),
            policy_trace: evaluation.policy_trace.clone(),
            source_bar_ids: vec![bar.id.clone()],
            signal_time: evaluation.decision_time,
            decision_available_at: evaluation.decision_time,
            valid_until,
            state_before: evaluation.state_before,
            state_after: evaluation.state_after,
            raw_target_weight: evaluation.raw_target_weight,
            constrained_target_weight: evaluation.constrained_target_weight,
            intended_side,
            reasons: evaluation.reasons.clone(),
            indicators: evaluation.indicators.clone(),
            evidence_effect: evaluation.evidence_effect.clone(),
            outcome: if no_action {
                SignalOutcome::NoAction
            } else {
                SignalOutcome::OrderUnfilled
            },
        });
        Ok(self.signals.len() - 1)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one ordered request-planning phase needs signal, liquidity, sizing and timing inputs"
    )]
    fn create_pending(
        &mut self,
        signal_index: usize,
        target: Weight,
        decision_bar: &CandleObservation,
        liquidity_source: &CandleObservation,
        requested_notional: Decimal,
        effective_at: UtcTimestamp,
        expires_at: UtcTimestamp,
        plan: &ResolvedPlan,
    ) -> Result<Option<PendingExecution>, LabError> {
        let decision_at = decision_bar.candle.close_time_utc;
        let (costs, provenance) =
            effective_costs(plan, decision_at, liquidity_source, requested_notional)?;
        let rules = plan.spec.rules_at(decision_at)?;
        let passive_buy = matches!(plan.spec.execution, ExecutionPolicy::PassiveBuy { .. })
            && self.signals[signal_index].intended_side == Some(Side::Buy);
        let mut decision = if passive_buy {
            let ExecutionPolicy::PassiveBuy { offset_bps, .. } = &plan.spec.execution else {
                return Err(LabError::Internal(
                    "passive execution policy disappeared".into(),
                ));
            };
            plan_passive_buy_request(
                &self.account,
                target,
                decision_bar.candle.close,
                *offset_bps,
                costs.maker_fee_bps,
                rules,
            )?
        } else {
            plan_market_request(
                &self.account,
                target,
                decision_bar.candle.close,
                &costs,
                rules,
            )?
        };
        if let Some(fill) = decision.fill.as_mut() {
            fill.cost_provenance = provenance;
        }
        let Some(request) = decision.fill else {
            self.push_rejected_order(
                signal_index,
                decision_bar.candle.close_time_utc,
                effective_at,
                expires_at,
                decision.reason,
                plan,
            )?;
            self.signals[signal_index].outcome = match decision.reason {
                ReasonCode::CapitalBlocked => SignalOutcome::CapitalBlocked,
                _ => SignalOutcome::RuleBlocked,
            };
            return Ok(None);
        };
        if request.side == Side::Buy {
            self.account.reserve(request.reserved_cash)?;
        }
        self.ensure_capacity(1)?;
        let context = self.next_context(decision_bar.candle.close_time_utc)?;
        let order = OrderRecord {
            order_id: OrderId::from_seed(&format!(
                "model:{}:order:event:{}",
                self.model_id, context.event_seq
            )),
            context,
            parent_signal_id: self.signals[signal_index].signal_id.clone(),
            episode_id: self
                .active_episode
                .as_ref()
                .map(|episode| episode.id.clone()),
            side: request.side,
            order_type: if passive_buy {
                SimulatedOrderType::PassiveBuyLimit
            } else {
                SimulatedOrderType::Market
            },
            requested_price: passive_buy.then_some(request.price),
            requested_qty: request.qty,
            created_at: decision_bar.candle.close_time_utc,
            effective_at,
            expires_at,
            rule_snapshot_id: rules.id.clone(),
            policy_version: if passive_buy {
                PASSIVE_POLICY_VERSION.into()
            } else {
                EXECUTION_POLICY_VERSION.into()
            },
            reserved_cash: request.reserved_cash,
            cumulative_filled_qty: AssetQuantity::new(Decimal::ZERO)?,
            status: OrderStatus::Created,
            reason: self.signals[signal_index]
                .reasons
                .iter()
                .copied()
                .find(|reason| *reason != ReasonCode::NoAction)
                .unwrap_or(ReasonCode::HoldState),
            order_origin: "SIMULATED".into(),
        };
        self.order_events.push(order.clone());
        Ok(Some(PendingExecution {
            signal_index,
            target,
            request,
            order,
            decision_reference: decision_bar.candle.close,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn process_pending_at(
        &mut self,
        pending: &mut Option<PendingExecution>,
        execution_bar: &CandleObservation,
        liquidity_source: &CandleObservation,
        participation_cap: Weight,
        plan: &ResolvedPlan,
        allow_created_now: bool,
    ) -> Result<(), LabError> {
        let Some(order) = pending.as_ref() else {
            return Ok(());
        };
        if order.order.order_type == SimulatedOrderType::PassiveBuyLimit {
            return Ok(());
        }
        let now = execution_bar.candle.open_time_utc;
        if now >= order.order.expires_at {
            let order = pending
                .take()
                .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
            return self.expire_pending(&order, now);
        }
        if now < order.order.effective_at || (!allow_created_now && order.order.created_at == now) {
            return Ok(());
        }
        let order = pending
            .take()
            .ok_or_else(|| LabError::Internal("pending order disappeared".into()))?;
        if order.request.side == Side::Buy {
            self.account.release(order.request.reserved_cash)?;
        }
        let rules = plan.spec.rules_at(now)?;
        let decision = if rules.tradable_at(now) {
            let (arrival_costs, arrival_provenance) =
                effective_costs(plan, now, liquidity_source, order.request.notional.get())?;
            let mut arrival = plan_market_arrival(
                &self.account,
                &order.request,
                execution_bar.candle.open,
                &arrival_costs,
                rules,
                liquidity_source.candle.volume,
                participation_cap,
            )?;
            if let Some(fill) = arrival.fill.as_mut() {
                fill.cost_provenance = arrival_provenance;
            }
            arrival
        } else {
            ExecutionDecision {
                fill: None,
                reason: ReasonCode::RuleBlocked,
            }
        };
        self.record_execution(
            &order,
            decision,
            execution_bar,
            liquidity_source,
            ExecutionMoment::MarketOpen,
        )
    }

    fn process_passive_close(
        &mut self,
        pending: &mut Option<PendingExecution>,
        completed_bar: &CandleObservation,
        liquidity_source: &CandleObservation,
        participation_cap: Weight,
        plan: &ResolvedPlan,
    ) -> Result<(), LabError> {
        let Some(order) = pending.as_ref() else {
            return Ok(());
        };
        if order.order.order_type != SimulatedOrderType::PassiveBuyLimit
            || completed_bar.candle.open_time_utc < order.order.effective_at
        {
            return Ok(());
        }
        if completed_bar.candle.close_time_utc > order.order.expires_at {
            let expires_at = order.order.expires_at;
            let expired = pending
                .take()
                .ok_or_else(|| LabError::Internal("pending passive order disappeared".into()))?;
            return self.expire_pending(&expired, expires_at);
        }
        let rules = plan.spec.rules_at(completed_bar.candle.close_time_utc)?;
        if !rules.tradable_at(completed_bar.candle.close_time_utc) {
            // A suspended or maintained market leaves the passive order pending;
            // the bounded TTL still expires it deterministically.
            return Ok(());
        }
        let order = pending
            .take()
            .ok_or_else(|| LabError::Internal("pending passive order disappeared".into()))?;
        self.account.release(order.request.reserved_cash)?;
        let ExecutionPolicy::PassiveBuy {
            penetration_ticks,
            fill_fraction,
            ..
        } = &plan.spec.execution
        else {
            return Err(LabError::Internal(
                "passive execution policy disappeared".into(),
            ));
        };
        let decision = plan_passive_arrival(
            &order.request,
            completed_bar.candle.open,
            completed_bar.candle.low,
            liquidity_source.candle.volume,
            participation_cap,
            *fill_fraction,
            *penetration_ticks,
            rules,
        )?;
        self.record_execution(
            &order,
            decision,
            completed_bar,
            liquidity_source,
            ExecutionMoment::PassiveClose,
        )
    }

    fn cancel_pending(
        &mut self,
        pending: &PendingExecution,
        at: UtcTimestamp,
        reason: ReasonCode,
    ) -> Result<(), LabError> {
        if pending.request.side == Side::Buy {
            self.account.release(pending.request.reserved_cash)?;
        }
        let projection = self.transition_order(
            &pending.order,
            at,
            OrderStatus::Cancelled,
            AssetQuantity::new(Decimal::ZERO)?,
            reason,
            None,
        )?;
        self.push_order_projection(projection)?;
        self.signals[pending.signal_index].outcome = SignalOutcome::OrderUnfilled;
        self.signals[pending.signal_index].reasons.push(reason);
        Ok(())
    }

    fn expire_pending(
        &mut self,
        pending: &PendingExecution,
        at: UtcTimestamp,
    ) -> Result<(), LabError> {
        if pending.request.side == Side::Buy {
            self.account.release(pending.request.reserved_cash)?;
        }
        let projection = self.transition_order(
            &pending.order,
            at.min(pending.order.expires_at),
            OrderStatus::Expired,
            AssetQuantity::new(Decimal::ZERO)?,
            ReasonCode::SignalExpired,
            None,
        )?;
        self.push_order_projection(projection)?;
        self.signals[pending.signal_index].outcome = SignalOutcome::SignalExpired;
        self.signals[pending.signal_index]
            .reasons
            .push(ReasonCode::SignalExpired);
        Ok(())
    }

    fn execute_terminal_liquidation(
        &mut self,
        bar: &CandleObservation,
        participation_cap: Weight,
        plan: &ResolvedPlan,
    ) -> Result<(), LabError> {
        if plan.spec.latency_ms != 0 {
            return Err(LabError::InvalidConfig(
                "terminal close liquidation requires zero configured latency".into(),
            ));
        }
        let zero = Weight::new(Decimal::ZERO)?;
        let signal_index = self.push_terminal_signal(bar, zero)?;
        let expiry = add_millis(bar.candle.close_time_utc, 1)?;
        let pending = self
            .create_pending(
                signal_index,
                zero,
                bar,
                bar,
                Decimal::ZERO,
                bar.candle.close_time_utc,
                expiry,
                plan,
            )?
            .ok_or_else(|| {
                LabError::AccountingInvariant("terminal liquidation request was rejected".into())
            })?;
        if pending.request.side == Side::Buy {
            return Err(LabError::AccountingInvariant(
                "terminal liquidation produced a buy request".into(),
            ));
        }
        let terminal_at = bar.candle.close_time_utc;
        let (terminal_costs, provenance) =
            effective_costs(plan, terminal_at, bar, pending.request.notional.get())?;
        let rules = plan.spec.rules_at(terminal_at)?;
        let mut decision = plan_market_arrival(
            &self.account,
            &pending.request,
            bar.candle.close,
            &terminal_costs,
            rules,
            bar.candle.volume,
            participation_cap,
        )?;
        if let Some(fill) = decision.fill.as_mut() {
            fill.cost_provenance = provenance;
        }
        self.last_target = zero;
        self.record_execution(&pending, decision, bar, bar, ExecutionMoment::TerminalClose)
    }

    fn push_terminal_signal(
        &mut self,
        bar: &CandleObservation,
        zero: Weight,
    ) -> Result<usize, LabError> {
        self.ensure_capacity(1)?;
        let (indicators, policy_trace) = self
            .signals
            .last()
            .map(|signal| (signal.indicators.clone(), signal.policy_trace.clone()))
            .ok_or_else(|| {
                LabError::AccountingInvariant(
                    "terminal liquidation has no strategy decision".into(),
                )
            })?;
        let context = self.next_context(bar.candle.close_time_utc)?;
        self.signals.push(SignalRecord {
            signal_id: SignalId::from_seed(&format!(
                "model:{}:terminal-signal:bar:{}",
                self.model_id, bar.id
            )),
            context,
            strategy: self.strategy.kind(),
            strategy_version: STRATEGY_VERSION.into(),
            policy_ref: self.strategy.policy_ref().cloned(),
            policy_trace,
            source_bar_ids: vec![bar.id.clone()],
            signal_time: bar.candle.close_time_utc,
            decision_available_at: bar.candle.close_time_utc,
            valid_until: bar.candle.close_time_utc,
            state_before: crate::contracts::PositionState::Long,
            state_after: crate::contracts::PositionState::Cash,
            raw_target_weight: zero,
            constrained_target_weight: zero,
            intended_side: Some(Side::Sell),
            reasons: vec![ReasonCode::ArtificialTerminalExit],
            indicators,
            evidence_effect: None,
            outcome: SignalOutcome::OrderUnfilled,
        });
        Ok(self.signals.len() - 1)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "fill, accounting mark, episode, and terminal order transition are one atomic phase"
    )]
    fn record_execution(
        &mut self,
        pending: &PendingExecution,
        decision: ExecutionDecision,
        bar: &CandleObservation,
        liquidity_source: &CandleObservation,
        moment: ExecutionMoment,
    ) -> Result<(), LabError> {
        let execution_at = match moment {
            ExecutionMoment::MarketOpen => bar.candle.open_time_utc,
            ExecutionMoment::PassiveClose | ExecutionMoment::TerminalClose => {
                bar.candle.close_time_utc
            }
        };
        let artificial_terminal_exit = moment == ExecutionMoment::TerminalClose;
        let Some(fill) = decision.fill else {
            let status = if moment == ExecutionMoment::PassiveClose {
                OrderStatus::Expired
            } else if decision.reason == ReasonCode::VolumeCap {
                OrderStatus::Cancelled
            } else {
                OrderStatus::Rejected
            };
            let projection = self.transition_order(
                &pending.order,
                execution_at,
                status,
                AssetQuantity::new(Decimal::ZERO)?,
                decision.reason,
                None,
            )?;
            self.push_order_projection(projection)?;
            self.signals[pending.signal_index].outcome = match decision.reason {
                ReasonCode::CapitalBlocked => SignalOutcome::CapitalBlocked,
                ReasonCode::RuleBlocked | ReasonCode::QuantityRoundedToZero => {
                    SignalOutcome::RuleBlocked
                }
                _ => SignalOutcome::OrderUnfilled,
            };
            self.signals[pending.signal_index]
                .reasons
                .push(decision.reason);
            return Ok(());
        };
        if fill.side == Side::Buy {
            self.account.reserve(fill.reserved_cash)?;
        }
        let episode_id = self.ensure_episode(&fill, bar, execution_at)?;
        let partial = fill.qty < pending.order.requested_qty;
        let transition = self.transition_order(
            &pending.order,
            execution_at,
            if partial {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Filled
            },
            fill.qty,
            fill.reason,
            Some(episode_id.clone()),
        )?;
        let fill_context = self.next_context(execution_at)?;
        let accounting_mark_seq = self.seq;
        let fill_id = FillId::from_seed(&format!(
            "model:{}:fill:event:{}",
            self.model_id, fill_context.event_seq
        ));
        let before_qty = self.account.qty();
        let applied = self.account.apply_fill(
            fill.side,
            fill.price,
            fill.qty,
            fill.fee,
            fill.reserved_cash,
        )?;
        let remaining_reservation = if partial && fill.side == Side::Buy {
            QuoteAmount::new(checked_sub(
                pending.order.reserved_cash.get(),
                fill.reserved_cash.get(),
                "partial-fill remaining reservation",
            )?)?
        } else {
            QuoteAmount::new(Decimal::ZERO)?
        };
        if !remaining_reservation.get().is_zero() {
            self.account.reserve(remaining_reservation)?;
        }
        self.fills.push(FillRecord {
            context: fill_context,
            fill_id: fill_id.clone(),
            order_id: pending.order.order_id.clone(),
            episode_id: episode_id.clone(),
            side: fill.side,
            price: fill.price,
            qty: fill.qty,
            notional: fill.notional,
            fee: fill.fee,
            fee_bps: fill.fee_bps,
            fee_currency: "KRW".into(),
            timing: if moment == ExecutionMoment::PassiveClose {
                FillTiming::Interval {
                    start: bar.candle.open_time_utc,
                    end: bar.candle.close_time_utc,
                    accounting_at: execution_at,
                }
            } else {
                FillTiming::Exact { at: execution_at }
            },
            source_bar_id: bar.id.clone(),
            decision_reference: pending.decision_reference,
            bar_open_proxy: bar.candle.open,
            liquidity_source_bar_id: liquidity_source.id.clone(),
            liquidity_source_close_time: liquidity_source.candle.close_time_utc,
            arrival_mid: None,
            arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
            adverse_slippage_bps: crate::contracts::SignedAmount::new(slippage_bps(
                fill.side,
                fill.price.get(),
                pending.decision_reference.get(),
            )?)?,
            price_cost_attribution: crate::contracts::SignedAmount::new(
                fill.price_cost_attribution,
            )?,
            liquidity_role_assumption: if moment == ExecutionMoment::PassiveClose {
                "MAKER_SCENARIO_STRICT_PENETRATION".into()
            } else {
                "TAKER_SCENARIO_PRIOR_COMPLETED_VOLUME".into()
            },
            execution_origin: "SIMULATED_ONLY".into(),
            fill_observed: false,
            model_version: ENGINE_VERSION.into(),
            artificial_terminal_exit,
            cost_provenance: fill.cost_provenance.clone(),
            accounting_mark_seq,
        });
        self.update_episode(
            &fill,
            fill_id,
            pending.order.order_id.clone(),
            applied.removed_basis,
            applied.realized_price_pnl,
            before_qty,
            if artificial_terminal_exit {
                ReasonCode::ArtificialTerminalExit
            } else {
                ReasonCode::MarketFilled
            },
            execution_at,
        )?;
        self.push_mark(
            MarkKind::AfterFill,
            fill.price,
            bar.id.clone(),
            if artificial_terminal_exit {
                Weight::new(Decimal::ZERO)?
            } else {
                self.last_target
            },
            execution_at,
        )?;
        if partial {
            if !remaining_reservation.get().is_zero() {
                self.account.release(remaining_reservation)?;
            }
            let cancelled = self.transition_order(
                &transition,
                execution_at,
                OrderStatus::Cancelled,
                fill.qty,
                ReasonCode::Cancelled,
                Some(episode_id),
            )?;
            self.push_order_projection(cancelled)?;
        } else {
            self.push_order_projection(transition)?;
        }
        self.signals[pending.signal_index].outcome = SignalOutcome::Executed;
        Ok(())
    }

    fn push_rejected_order(
        &mut self,
        signal_index: usize,
        created_at: UtcTimestamp,
        effective_at: UtcTimestamp,
        expires_at: UtcTimestamp,
        reason: ReasonCode,
        plan: &ResolvedPlan,
    ) -> Result<(), LabError> {
        self.ensure_capacity(3)?;
        let rule_snapshot_id = plan.spec.rules_at(created_at)?.id.clone();
        let created_context = self.next_context(created_at)?;
        let side = self.signals[signal_index].intended_side.ok_or_else(|| {
            LabError::AccountingInvariant("rejected order has no intended side".into())
        })?;
        let created = OrderRecord {
            order_id: OrderId::from_seed(&format!(
                "model:{}:order:event:{}",
                self.model_id, created_context.event_seq
            )),
            context: created_context,
            parent_signal_id: self.signals[signal_index].signal_id.clone(),
            episode_id: self
                .active_episode
                .as_ref()
                .map(|episode| episode.id.clone()),
            side,
            order_type: SimulatedOrderType::Market,
            requested_price: None,
            requested_qty: AssetQuantity::new(Decimal::ZERO)?,
            created_at,
            effective_at,
            expires_at,
            rule_snapshot_id,
            policy_version: EXECUTION_POLICY_VERSION.into(),
            reserved_cash: QuoteAmount::new(Decimal::ZERO)?,
            cumulative_filled_qty: AssetQuantity::new(Decimal::ZERO)?,
            status: OrderStatus::Created,
            reason: self.signals[signal_index]
                .reasons
                .iter()
                .copied()
                .find(|candidate| *candidate != ReasonCode::NoAction)
                .unwrap_or(ReasonCode::HoldState),
            order_origin: "SIMULATED".into(),
        };
        self.order_events.push(created.clone());
        let mut rejected = created;
        rejected.context = self.next_context(created_at)?;
        rejected.status = OrderStatus::Rejected;
        rejected.reason = reason;
        self.order_events.push(rejected.clone());
        self.push_order_projection(rejected)?;
        Ok(())
    }

    fn transition_order(
        &mut self,
        order: &OrderRecord,
        at: UtcTimestamp,
        status: OrderStatus,
        cumulative: AssetQuantity,
        reason: ReasonCode,
        episode_id: Option<EpisodeId>,
    ) -> Result<OrderRecord, LabError> {
        self.ensure_capacity(1)?;
        let mut event = order.clone();
        event.context = self.next_context(at)?;
        event.status = status;
        event.cumulative_filled_qty = cumulative;
        if episode_id.is_some() {
            event.episode_id = episode_id;
        }
        event.reason = reason;
        self.order_events.push(event.clone());
        Ok(event)
    }

    fn push_order_projection(&mut self, projection: OrderRecord) -> Result<(), LabError> {
        self.ensure_capacity(1)?;
        self.record_count += 1;
        self.orders.push(projection);
        Ok(())
    }

    fn ensure_episode(
        &mut self,
        fill: &PlannedFill,
        bar: &CandleObservation,
        opened_at: UtcTimestamp,
    ) -> Result<EpisodeId, LabError> {
        if let Some(episode) = &self.active_episode {
            return Ok(episode.id.clone());
        }
        if fill.side != Side::Buy || !self.account.qty().is_zero() {
            return Err(LabError::AccountingInvariant(
                "sell fill has no open episode".into(),
            ));
        }
        let start_equity = self.account.mark(fill.price)?.equity;
        let id = EpisodeId::from_seed(&format!(
            "model:{}:episode:entry-bar:{}",
            self.model_id, bar.id
        ));
        self.active_episode = Some(ActiveEpisode::new(id.clone(), opened_at, start_equity));
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    fn update_episode(
        &mut self,
        fill: &PlannedFill,
        fill_id: FillId,
        order_id: OrderId,
        removed_basis: Decimal,
        realized: Decimal,
        before_qty: Decimal,
        exit_reason: ReasonCode,
        at: UtcTimestamp,
    ) -> Result<(), LabError> {
        let episode = self.active_episode.as_mut().ok_or_else(|| {
            LabError::AccountingInvariant("fill did not resolve an episode".into())
        })?;
        episode.sample_qty(before_qty, at)?;
        episode.fill_ids.push(fill_id);
        episode.order_ids.push(order_id);
        episode.fees = checked_add(episode.fees, fill.fee.get(), "episode fees")?;
        episode.realized = checked_add(episode.realized, realized, "episode realized")?;
        match fill.side {
            Side::Buy => {
                episode.buy_notional =
                    checked_add(episode.buy_notional, fill.notional.get(), "episode buys")?;
                episode.buy_qty =
                    checked_add(episode.buy_qty, fill.qty.get(), "episode buy quantity")?;
            }
            Side::Sell => {
                episode.sell_notional =
                    checked_add(episode.sell_notional, fill.notional.get(), "episode sells")?;
                episode.sell_qty =
                    checked_add(episode.sell_qty, fill.qty.get(), "episode sell quantity")?;
                episode.removed_basis = checked_add(
                    episode.removed_basis,
                    removed_basis,
                    "episode removed basis",
                )?;
            }
        }
        episode.max_qty = episode.max_qty.max(self.account.qty());
        episode.sample_qty(self.account.qty(), at)?;
        let marked = self.account.mark(fill.price)?;
        episode.sample_pnl(
            checked_sub(
                checked_add(
                    episode.realized,
                    marked.gross_unrealized,
                    "episode gross path",
                )?,
                episode.fees,
                "episode net path",
            )?,
            at,
        )?;
        if self.account.qty().is_zero() {
            let mut closed = self.active_episode.take().ok_or_else(|| {
                LabError::AccountingInvariant("closed episode disappeared".into())
            })?;
            closed.closed_at = Some(at);
            closed.exit_reason = Some(exit_reason);
            let record = closed.record(
                &self.run_id,
                &self.model_id,
                &self.market,
                &self.account,
                fill.price,
            )?;
            self.push_episode_record(record)?;
        }
        Ok(())
    }

    fn push_mark(
        &mut self,
        kind: MarkKind,
        price: PriceKrw,
        source_bar_id: ObservationId,
        target: Weight,
        at: UtcTimestamp,
    ) -> Result<(), LabError> {
        self.ensure_capacity(1)?;
        let marked = self.account.mark(price)?;
        if let Some(episode) = &mut self.active_episode {
            episode.sample_qty(self.account.qty(), at)?;
            episode.sample_pnl(
                checked_sub(
                    checked_add(
                        episode.realized,
                        marked.gross_unrealized,
                        "episode gross path",
                    )?,
                    episode.fees,
                    "episode net path",
                )?,
                at,
            )?;
        }
        let context = self.next_context(at)?;
        self.marks.push(AccountMark {
            context,
            kind,
            state: self.account.state()?,
            mark_price: price,
            source_bar_id,
            position_value: QuoteAmount::new(marked.position_value)?,
            gross_unrealized: crate::contracts::SignedAmount::new(marked.gross_unrealized)?,
            equity: QuoteAmount::new(marked.equity)?,
            target_weight: target,
            actual_weight: Weight::new(marked.actual_weight)?,
            peak_equity: QuoteAmount::new(marked.peak_equity)?,
            drawdown: Weight::new(marked.drawdown)?,
        });
        Ok(())
    }

    fn finish_episode(&mut self, price: PriceKrw, at: UtcTimestamp) -> Result<(), LabError> {
        if let Some(mut episode) = self.active_episode.take() {
            episode.sample_qty(self.account.qty(), at)?;
            let record = episode.record(
                &self.run_id,
                &self.model_id,
                &self.market,
                &self.account,
                price,
            )?;
            self.push_episode_record(record)?;
        }
        Ok(())
    }

    fn push_episode_record(&mut self, record: EpisodeRecord) -> Result<(), LabError> {
        self.ensure_capacity(1)?;
        self.record_count += 1;
        self.episodes.push(record);
        Ok(())
    }

    fn next_context(&mut self, at: UtcTimestamp) -> Result<EventContext, LabError> {
        let event_seq = self.seq;
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("event sequence overflow".into()))?;
        self.record_count += 1;
        Ok(EventContext {
            run_id: self.run_id.clone(),
            model_id: self.model_id.clone(),
            market: self.market.clone(),
            event_seq,
            accounting_event_time: at,
        })
    }

    fn ensure_capacity(&self, additional: usize) -> Result<(), LabError> {
        if self
            .record_count
            .checked_add(additional)
            .is_none_or(|count| count > MAX_MODEL_EVENTS)
        {
            return Err(LabError::ResourceLimit(format!(
                "model event count exceeds {MAX_MODEL_EVENTS}"
            )));
        }
        Ok(())
    }

    fn into_ledger(self) -> ModelLedger {
        ModelLedger {
            model_id: self.model_id,
            market: self.market,
            strategy: self.strategy,
            status: ModelStatus::Completed,
            status_reason: None,
            signals: self.signals,
            orders: self.orders,
            order_events: self.order_events,
            fills: self.fills,
            episodes: self.episodes,
            account_marks: self.marks,
            last_event_seq: self.seq.saturating_sub(1),
        }
    }
}

#[derive(Debug)]
struct ActiveEpisode {
    id: EpisodeId,
    opened_at: UtcTimestamp,
    closed_at: Option<UtcTimestamp>,
    fill_ids: Vec<FillId>,
    order_ids: Vec<OrderId>,
    buy_notional: Decimal,
    buy_qty: Decimal,
    sell_notional: Decimal,
    sell_qty: Decimal,
    removed_basis: Decimal,
    max_qty: Decimal,
    quantity_seconds: Decimal,
    last_qty: Decimal,
    last_sample_at: UtcTimestamp,
    realized: Decimal,
    fees: Decimal,
    start_equity: Decimal,
    mae: Decimal,
    mfe: Decimal,
    mae_at: UtcTimestamp,
    mfe_at: UtcTimestamp,
    exit_reason: Option<ReasonCode>,
    decision_marks_since_open: u64,
}

impl ActiveEpisode {
    fn new(id: EpisodeId, opened_at: UtcTimestamp, start_equity: Decimal) -> Self {
        Self {
            id,
            opened_at,
            closed_at: None,
            fill_ids: Vec::new(),
            order_ids: Vec::new(),
            buy_notional: Decimal::ZERO,
            buy_qty: Decimal::ZERO,
            sell_notional: Decimal::ZERO,
            sell_qty: Decimal::ZERO,
            removed_basis: Decimal::ZERO,
            max_qty: Decimal::ZERO,
            quantity_seconds: Decimal::ZERO,
            last_qty: Decimal::ZERO,
            last_sample_at: opened_at,
            realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            start_equity,
            mae: Decimal::ZERO,
            mfe: Decimal::ZERO,
            mae_at: opened_at,
            mfe_at: opened_at,
            exit_reason: None,
            decision_marks_since_open: 0,
        }
    }

    fn sample_qty(&mut self, qty: Decimal, at: UtcTimestamp) -> Result<(), LabError> {
        let seconds = (at.0 - self.last_sample_at.0).num_seconds();
        if seconds < 0 {
            return Err(LabError::AccountingInvariant(
                "episode samples moved backward in time".into(),
            ));
        }
        self.quantity_seconds = checked_add(
            self.quantity_seconds,
            checked_mul(self.last_qty, Decimal::from(seconds), "quantity seconds")?,
            "episode quantity seconds",
        )?;
        self.last_qty = qty;
        self.last_sample_at = at;
        self.max_qty = self.max_qty.max(qty);
        Ok(())
    }

    fn sample_pnl(&mut self, pnl: Decimal, at: UtcTimestamp) -> Result<(), LabError> {
        if at < self.opened_at {
            return Err(LabError::AccountingInvariant(
                "episode pnl sample predates entry".into(),
            ));
        }
        if pnl < self.mae {
            self.mae = pnl;
            self.mae_at = at;
        }
        if pnl > self.mfe {
            self.mfe = pnl;
            self.mfe_at = at;
        }
        Ok(())
    }

    fn record(
        self,
        run_id: &RunId,
        model_id: &crate::contracts::ModelId,
        market: &crate::contracts::MarketId,
        account: &Account,
        mark_price: PriceKrw,
    ) -> Result<EpisodeRecord, LabError> {
        let end = self.closed_at.unwrap_or(self.last_sample_at);
        let holding_seconds = u64::try_from((end.0 - self.opened_at.0).num_seconds())
            .map_err(|_| LabError::AccountingInvariant("negative episode duration".into()))?;
        let average_qty = if holding_seconds == 0 {
            account.qty()
        } else {
            checked_div(
                self.quantity_seconds,
                Decimal::from(holding_seconds),
                "average quantity",
            )?
        };
        let marked_unrealized = checked_sub(
            checked_mul(account.qty(), mark_price.get(), "episode terminal value")?,
            account.state()?.price_basis.get(),
            "episode marked unrealized",
        )?;
        let net = checked_sub(self.realized, self.fees, "episode net realized")?;
        let recomputed_realized = checked_sub(
            self.sell_notional,
            self.removed_basis,
            "episode recomputed realized",
        )?;
        let realized_residual = checked_sub(
            recomputed_realized,
            self.realized,
            "episode realized residual",
        )?;
        if !aggregate_identity_within_tolerance(recomputed_realized, self.realized)? {
            return Err(LabError::AccountingInvariant(format!(
                "episode sell proceeds and removed basis do not reconcile: sell_notional={}, removed_basis={}, recomputed_realized={recomputed_realized}, accumulated_realized={}, residual={realized_residual}, fees={}",
                self.sell_notional, self.removed_basis, self.realized, self.fees,
            )));
        }
        let adverse_fraction = checked_div(self.mae, self.start_equity, "episode MAE percent")?;
        let favorable_fraction = checked_div(self.mfe, self.start_equity, "episode MFE percent")?;
        let final_path = checked_sub(
            checked_add(self.realized, marked_unrealized, "episode final gross")?,
            self.fees,
            "episode final net",
        )?;
        Ok(EpisodeRecord {
            episode_id: self.id,
            run_id: run_id.clone(),
            model_id: model_id.clone(),
            market: market.clone(),
            status: if self.closed_at.is_some() {
                EpisodeStatus::Closed
            } else {
                EpisodeStatus::Open
            },
            opened_at: self.opened_at,
            closed_at: self.closed_at,
            fill_ids: self.fill_ids,
            order_ids: self.order_ids,
            buy_vwap: PriceKrw::new(checked_div(self.buy_notional, self.buy_qty, "buy VWAP")?)?,
            sell_vwap: if self.sell_qty.is_zero() {
                None
            } else {
                Some(PriceKrw::new(checked_div(
                    self.sell_notional,
                    self.sell_qty,
                    "sell VWAP",
                )?)?)
            },
            max_qty: AssetQuantity::new(self.max_qty)?,
            time_weighted_avg_qty: AssetQuantity::new(average_qty)?,
            holding_seconds,
            realized_price_pnl: crate::contracts::SignedAmount::new(self.realized)?,
            fees: QuoteAmount::new(self.fees)?,
            net_realized: crate::contracts::SignedAmount::new(net)?,
            residual_basis: account.state()?.price_basis,
            residual_qty: AssetQuantity::new(account.qty())?,
            marked_unrealized: crate::contracts::SignedAmount::new(marked_unrealized)?,
            exit_reason: self.exit_reason,
            start_equity: QuoteAmount::new(self.start_equity)?,
            mae_amount: crate::contracts::SignedAmount::new(self.mae)?,
            mfe_amount: crate::contracts::SignedAmount::new(self.mfe)?,
            mae_pct_of_start_equity: crate::contracts::SignedAmount::new(adverse_fraction)?,
            mfe_pct_of_start_equity: crate::contracts::SignedAmount::new(favorable_fraction)?,
            time_to_mae_seconds: u64::try_from((self.mae_at.0 - self.opened_at.0).num_seconds())
                .map_err(|_| LabError::AccountingInvariant("negative MAE time".into()))?,
            time_to_mfe_seconds: u64::try_from((self.mfe_at.0 - self.opened_at.0).num_seconds())
                .map_err(|_| LabError::AccountingInvariant("negative MFE time".into()))?,
            sampling_definition: EPISODE_SAMPLING.into(),
            exit_peak_giveback: QuoteAmount::new((self.mfe - final_path).max(Decimal::ZERO))?,
        })
    }
}

fn validate_admission(plan: &ResolvedPlan, admission: &ModelAdmission) -> Result<(), LabError> {
    let matches = plan.admissions.iter().any(|candidate| {
        candidate.model_id == admission.model_id
            && candidate.market == admission.market
            && candidate.strategy == admission.strategy
            && candidate.policy_ref == admission.policy_ref
            && candidate.status == admission.status
    });
    if !matches {
        return Err(LabError::InvalidConfig(
            "model admission is not frozen in plan".into(),
        ));
    }
    Ok(())
}

fn validate_frozen_inputs(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    evidence: Option<&EvidenceSnapshot>,
) -> Result<(), LabError> {
    if datasets.len() != plan.spec.dataset_ids.len()
        || datasets.len() != plan.dataset_digests.len()
        || datasets
            .iter()
            .any(|snapshot| snapshot.observations.len() > MAX_DATASET_ROWS)
    {
        return Err(LabError::ResourceLimit(
            "dataset inputs differ from the bounded frozen plan".into(),
        ));
    }
    for (dataset_id, digest) in &plan.dataset_digests {
        let snapshot = datasets
            .iter()
            .find(|candidate| &candidate.manifest.id == dataset_id)
            .ok_or_else(|| LabError::InputHashMismatch(format!("missing dataset {dataset_id}")))?;
        if &snapshot.manifest.semantic_digest != digest {
            return Err(LabError::InputHashMismatch(format!(
                "dataset digest mismatch: {dataset_id}"
            )));
        }
    }
    match (&plan.evidence_digest, evidence) {
        (Some(expected), Some(snapshot)) if expected == &snapshot.digest => {}
        (None, None) => {}
        (Some(_), _) | (None, Some(_)) => {
            return Err(LabError::InputHashMismatch(
                "Evidence snapshot/digest mismatch".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn observations_for_range<'a>(
    datasets: &'a [DatasetSnapshot],
    market: &crate::contracts::MarketId,
    interval: crate::contracts::CandleInterval,
    causal_range: Option<UtcRange>,
) -> Result<Vec<&'a CandleObservation>, LabError> {
    let mut observations = datasets
        .iter()
        .flat_map(|dataset| dataset.observations.iter())
        .filter(|observation| {
            observation.candle.market == market.code() && observation.candle.interval == interval
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| {
        left.candle
            .open_time_utc
            .cmp(&right.candle.open_time_utc)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    if let Some(range) = causal_range {
        observations.retain(|observation| range.contains(observation.candle.open_time_utc));
    }
    if observations.is_empty() {
        return Err(LabError::DataGap(format!(
            "no {interval:?} observations for {market}"
        )));
    }
    if observations
        .windows(2)
        .any(|pair| pair[0].candle.open_time_utc == pair[1].candle.open_time_utc)
    {
        return Err(LabError::Conflict(
            "duplicate market/interval candle open time".into(),
        ));
    }
    if observations.iter().any(|observation| {
        !observation.candle.completed
            || observation.candle.close_time_utc.0 - observation.candle.open_time_utc.0
                != interval.duration()
    }) || observations
        .windows(2)
        .any(|pair| pair[0].candle.close_time_utc != pair[1].candle.open_time_utc)
    {
        return Err(LabError::DataGap(
            "observation source window is incomplete or misaligned".into(),
        ));
    }
    Ok(observations)
}

fn add_millis(time: UtcTimestamp, millis: u64) -> Result<UtcTimestamp, LabError> {
    let millis = i64::try_from(millis)
        .map_err(|_| LabError::InvalidConfig("latency does not fit timestamp arithmetic".into()))?;
    time.0
        .checked_add_signed(chrono::Duration::milliseconds(millis))
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("latency timestamp overflow".into()))
}

fn passive_order_expiry(
    policy: &ExecutionPolicy,
    is_buy: bool,
    execution_bars: &[&CandleObservation],
    current_index: usize,
    effective_at: UtcTimestamp,
    signal_expiry: UtcTimestamp,
) -> UtcTimestamp {
    if !is_buy || !matches!(policy, ExecutionPolicy::PassiveBuy { .. }) {
        return signal_expiry;
    }
    execution_bars
        .iter()
        .skip(current_index)
        .find(|bar| {
            bar.candle.open_time_utc >= effective_at && bar.candle.close_time_utc <= signal_expiry
        })
        .map_or(signal_expiry, |bar| bar.candle.close_time_utc)
}

fn slippage_bps(side: Side, fill: Decimal, reference: Decimal) -> Result<Decimal, LabError> {
    let ratio = checked_sub(
        checked_div(fill, reference, "slippage ratio")?,
        Decimal::ONE,
        "slippage",
    )?;
    let signed = match side {
        Side::Buy => ratio,
        Side::Sell => -ratio,
    };
    checked_mul(signed, Decimal::from(10_000), "slippage bps")
}

/// Proxy liquidity features of one completed bar: `(high-low)/close` range
/// fraction and the observed `quote_turnover`. Zero-close bars are flat.
fn liquidity_features(liquidity: &CandleObservation) -> Result<(Decimal, Decimal), LabError> {
    let close = liquidity.candle.close.get();
    let turnover = liquidity.candle.quote_turnover.get();
    if close.is_zero() {
        return Ok((Decimal::ZERO, turnover));
    }
    let range = checked_sub(
        liquidity.candle.high.get(),
        liquidity.candle.low.get(),
        "bar range",
    )?;
    let fraction = checked_div(range, close, "bar range fraction")?;
    Ok((fraction, turnover))
}

/// Fees and slippage in effect at `at`, resolved from the point-in-time rule
/// snapshot and the optional dynamic OHLCV-proxy cost model. The dynamic model
/// only reads the completed liquidity-source bar, never the execution bar.
fn effective_costs(
    plan: &ResolvedPlan,
    at: UtcTimestamp,
    liquidity: &CandleObservation,
    requested_notional: Decimal,
) -> Result<(CostPolicy, Option<CostProvenance>), LabError> {
    let mut costs = plan.spec.costs.clone();
    let fees = plan.spec.fee_policy_at(at)?;
    costs.buy_fee_bps = fees.buy;
    costs.sell_fee_bps = fees.sell;
    costs.maker_fee_bps = fees.maker;
    let Some(dynamic) = &plan.spec.costs.dynamic else {
        return Ok((costs, None));
    };
    let (range_fraction, turnover) = liquidity_features(liquidity)?;
    let effective = dynamic.effective_slippage_bps(CostFeatureInputs {
        bar_range_fraction: range_fraction,
        quote_turnover: turnover,
        requested_notional,
    })?;
    costs.slippage_bps = effective;
    let participation_rate = if turnover.is_zero() {
        Decimal::ZERO
    } else {
        checked_div(requested_notional, turnover, "participation rate")?
    };
    let provenance = CostProvenance {
        model_kind: dynamic.kind(),
        effective_slippage_bps: effective,
        proxy_inputs: CostProxyInputs {
            liquidity_bar_id: liquidity.id.clone(),
            bar_range_bps: checked_mul(range_fraction, Decimal::from(10_000), "bar range bps")?,
            quote_turnover: QuoteAmount::new(turnover)?,
            requested_notional: QuoteAmount::new(requested_notional)?,
            participation_rate,
        },
    };
    Ok((costs, Some(provenance)))
}

fn check_cancelled(cancelled: &dyn Fn() -> bool) -> Result<(), LabError> {
    if cancelled() {
        Err(LabError::Cancelled(
            "model execution cancelled at deterministic checkpoint".into(),
        ))
    } else {
        Ok(())
    }
}

fn blocked_ledger(
    admission: &ModelAdmission,
    strategy: StrategyBinding,
    seq_start: u64,
) -> ModelLedger {
    let status = match admission.status {
        AdmissionStatus::BlockedEvidence => ModelStatus::BlockedEvidence,
        AdmissionStatus::BlockedData => ModelStatus::BlockedData,
        AdmissionStatus::Eligible => ModelStatus::Skipped,
    };
    ModelLedger {
        model_id: admission.model_id.clone(),
        market: admission.market.clone(),
        strategy,
        status,
        status_reason: Some(admission.reasons.join("; ")),
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        episodes: Vec::new(),
        account_marks: Vec::new(),
        last_event_seq: seq_start.saturating_sub(1),
    }
}

#[cfg(test)]
mod tests;
