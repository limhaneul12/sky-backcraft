//! Shared-capital portfolio execution over frozen single-strategy plans.
//!
//! One cash pool funds every asset slot. Signals at one decision time compete
//! for the same cash under a deterministic arbitration policy, every rejected
//! signal keeps a typed reason, and the accounting identity
//! `equity - initial == realized + unrealized - fees` is re-checked at every
//! completed mark. Long/cash only; no leverage, short or margin.

use crate::contracts::checked::{
    add as checked_add, div as checked_div, mul as checked_mul, sub as checked_sub,
};
use crate::contracts::{
    AdmissionStatus, ArbitrationPolicy, AssetQuantity, BasisPoints, BenchmarkKind, BenchmarkResult,
    CandleObservation, ContentHash, DatasetSnapshot, LabError, MAX_PORTFOLIO_EVENTS, MarketId,
    ModelId, PortfolioAttribution, PortfolioFillRecord, PortfolioIntentRecord, PortfolioLedger,
    PortfolioMarkRecord, PortfolioRejectionReason, PortfolioRejectionRecord, PortfolioSpec,
    PortfolioStatus, PortfolioTotals, PriceKrw, QuoteAmount, ReasonCode, RegimeGateSpec,
    RegimeLabel, RegimeObservation, ResolvedPlan, RunId, Side, StrategyBinding, StrategyKind,
    TerminalPolicy, UtcTimestamp, Weight,
};
use crate::engine::execution::{bps_rate, floor_to_step, tick_for};
use crate::policy_engine::{CrossIntervalFeed, PolicyEvaluator};
use crate::regime::{RegimeClassifier, gate_action};
use crate::strategy::PositionView;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// Aggregate equity identity tolerance; mirrors the single-model engine.
const NUMERIC_TOLERANCE: Decimal = crate::contracts::NUMERIC_TOLERANCE;

#[cfg(test)]
mod tests;

/// One per-market execution view: strategy evaluator plus its frozen bars.
struct MarketSlot {
    market: MarketId,
    model_id: ModelId,
    strategy_kind: StrategyKind,
    evaluator: PolicyEvaluator,
    decision_bars: Vec<CandleObservation>,
    execution_bars: Vec<CandleObservation>,
    qty: Decimal,
    price_basis: Decimal,
    realized: Decimal,
    fees: Decimal,
    buy_notional: Decimal,
    sell_notional: Decimal,
    closed_trades: u64,
    max_weight_seen: Decimal,
    exposure_seconds: u64,
    episode_opened_at: Option<UtcTimestamp>,
    decision_marks_since_open: u64,
    gate: Option<(RegimeClassifier, Vec<CandleObservation>)>,
    /// Bounded causal cross-interval feed; empty for single-interval policies.
    source_feed: CrossIntervalFeed,
}

/// Shared cash ledger for the whole portfolio; the single spend authority.
struct SharedCash {
    initial: Decimal,
    total: Decimal,
    reserved: Decimal,
}

impl SharedCash {
    fn free(&self) -> Result<Decimal, LabError> {
        checked_sub(self.total, self.reserved, "portfolio free cash")
    }

    fn reserve(&mut self, amount: Decimal) -> Result<(), LabError> {
        let free = self.free()?;
        if amount > free {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio reservation exceeds free cash: required {amount}, free {free}"
            )));
        }
        self.reserved = checked_add(self.reserved, amount, "portfolio reservation")?;
        Ok(())
    }

    fn release(&mut self, amount: Decimal) -> Result<(), LabError> {
        if amount > self.reserved {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio release exceeds reserved cash: requested {amount}, reserved {}",
                self.reserved
            )));
        }
        self.reserved = checked_sub(self.reserved, amount, "portfolio release")?;
        Ok(())
    }

    fn settle_buy(&mut self, debit: Decimal) -> Result<(), LabError> {
        if debit > self.total {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio buy debit exceeds total cash: debit {debit}, total {}",
                self.total
            )));
        }
        self.total = checked_sub(self.total, debit, "portfolio buy cash")?;
        if self.total < Decimal::ZERO {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio cash went negative ({}); fees may never overdraw the pool",
                self.total
            )));
        }
        Ok(())
    }

    fn settle_sell(&mut self, proceeds: Decimal) -> Result<(), LabError> {
        self.total = checked_add(self.total, proceeds, "portfolio sell cash")?;
        Ok(())
    }
}

/// Append-only runner state with a bounded event budget.
struct RunnerState {
    seq: u64,
    spec: PortfolioSpec,
    intents: Vec<PortfolioIntentRecord>,
    fills: Vec<PortfolioFillRecord>,
    rejections: Vec<PortfolioRejectionRecord>,
    marks: Vec<PortfolioMarkRecord>,
    observations: Vec<RegimeObservation>,
    peak_equity: Decimal,
    record_count: usize,
    portfolio_exposure_seconds: u64,
}

impl RunnerState {
    fn next_seq(&mut self) -> Result<u64, LabError> {
        let seq = self.seq;
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("portfolio event overflow".into()))?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("portfolio event overflow".into()))?;
        if self.record_count > MAX_PORTFOLIO_EVENTS {
            return Err(LabError::ResourceLimit(format!(
                "portfolio event count exceeds {MAX_PORTFOLIO_EVENTS}"
            )));
        }
        Ok(seq)
    }

    fn push_intent(
        &mut self,
        time: UtcTimestamp,
        market: &MarketId,
        side: Side,
        target: Decimal,
        requested: Decimal,
        rank: u32,
    ) -> Result<(), LabError> {
        let seq = self.next_seq()?;
        self.intents.push(PortfolioIntentRecord {
            event_seq: seq,
            decision_time: time,
            market: market.clone(),
            side,
            target_weight: Weight::new(target)?,
            requested_notional: QuoteAmount::new(requested)?,
            arbitration_rank: rank,
        });
        Ok(())
    }

    fn push_rejection(
        &mut self,
        time: UtcTimestamp,
        market: &MarketId,
        side: Side,
        target: Decimal,
        requested: Decimal,
        reason: PortfolioRejectionReason,
    ) -> Result<(), LabError> {
        let seq = self.next_seq()?;
        self.rejections.push(PortfolioRejectionRecord {
            event_seq: seq,
            decision_time: time,
            market: market.clone(),
            side,
            target_weight: Weight::new(target)?,
            requested_notional: QuoteAmount::new(requested)?,
            reason,
        });
        Ok(())
    }

    fn push_mark(
        &mut self,
        marked: &PortfolioMark,
        slots: &[MarketSlot],
        cash: &SharedCash,
        time: UtcTimestamp,
    ) -> Result<(), LabError> {
        // Completed-mark identity: equity - initial == realized + unrealized - fees.
        let unrealized = unrealized_at(slots, time)?;
        let fees = slots.iter().map(|slot| slot.fees).sum::<Decimal>();
        let realized = slots.iter().map(|slot| slot.realized).sum::<Decimal>();
        let expected = checked_sub(
            checked_add(realized, unrealized, "gross path")?,
            fees,
            "net path",
        )?;
        let actual = checked_sub(marked.equity, cash.initial, "equity change")?;
        let residual = checked_sub(actual, expected, "identity residual")?;
        if residual.abs() > NUMERIC_TOLERANCE {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio equity identity does not reconcile at {time}: actual_change={actual}, expected_change={expected}, residual={residual}"
            )));
        }
        let seq = self.next_seq()?;
        self.peak_equity = self.peak_equity.max(marked.equity);
        let peak = self.peak_equity;
        let drawdown = if peak.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(
                checked_sub(peak, marked.equity, "drawdown")?,
                peak,
                "drawdown",
            )?
        };
        let stop = self.spec.risk.drawdown_stop.get();
        self.marks.push(PortfolioMarkRecord {
            event_seq: seq,
            time,
            cash: QuoteAmount::new(marked.cash)?,
            position_value: QuoteAmount::new(marked.position_value)?,
            gross_exposure_weight: Weight::new(marked.gross_weight)?,
            equity: QuoteAmount::new(marked.equity)?,
            peak_equity: QuoteAmount::new(peak)?,
            drawdown: Weight::new(drawdown)?,
            stopped: stop > Decimal::ZERO && drawdown >= stop,
            weights: marked.weights.clone(),
        });
        Ok(())
    }
}

/// Shared-account snapshot at one completed decision mark.
struct PortfolioMark {
    cash: Decimal,
    position_value: Decimal,
    gross_weight: Decimal,
    equity: Decimal,
    weights: BTreeMap<String, Decimal>,
}

/// One strategy signal at a shared decision time.
struct Signal {
    slot_index: usize,
    target: Decimal,
    raw_target: Decimal,
}

/// Execute one shared-capital portfolio run against frozen inputs.
///
/// # Errors
/// Rejects invalid specs, mismatched frozen inputs, misaligned market grids,
/// nonzero latency, unsupported terminal policies, arithmetic or invariant
/// failures, and explicit cancellation.
#[expect(
    clippy::too_many_lines,
    reason = "the shared-capital decision loop is one ordered state-machine phase"
)]
#[allow(
    clippy::needless_range_loop,
    reason = "the decision loop mutates and reads several market slots by index"
)]
pub fn run_portfolio(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    spec: &PortfolioSpec,
    regime: Option<&RegimeGateSpec>,
    run_id: &RunId,
    seq_start: u64,
    cancelled: &dyn Fn() -> bool,
) -> Result<PortfolioLedger, LabError> {
    plan.spec.validate()?;
    spec.validate()?;
    if let Some(regime) = regime {
        regime.validate()?;
    }
    if plan.spec.latency_ms != 0 {
        return Err(LabError::InvalidConfig(
            "portfolio v1 requires zero configured latency".into(),
        ));
    }
    if plan.spec.capital_mode != Some(crate::contracts::CapitalMode::SharedPortfolio) {
        return Err(LabError::InvalidConfig(
            "the portfolio runner requires SHARED_PORTFOLIO capital mode on the frozen plan".into(),
        ));
    }
    if matches!(
        plan.spec.execution,
        crate::contracts::ExecutionPolicy::PassiveBuy { .. }
    ) {
        return Err(LabError::InvalidConfig(
            "portfolio v1 executes every fill as a next-bar-open taker; passive execution is not supported"
                .into(),
        ));
    }
    validate_frozen_inputs(plan, datasets)?;
    let participation_cap = match plan.spec.execution {
        crate::contracts::ExecutionPolicy::NextBarOpen { participation_cap }
        | crate::contracts::ExecutionPolicy::PassiveBuy {
            participation_cap, ..
        } => participation_cap,
    };
    let mut slots = market_slots(plan, datasets, spec, regime, run_id)?;
    // One shared decision timeline inside the evaluation range; every market
    // must close a decision bar at the same instants or arbitration is
    // undefined and the run fails closed.
    let timeline = shared_timeline(&slots, plan)?;
    for slot in &mut slots {
        for bar in &slot.decision_bars {
            if bar.candle.close_time_utc >= plan.spec.range.start() {
                break;
            }
            let view = PositionView {
                actual_qty: AssetQuantity::new(Decimal::ZERO)?,
                actual_weight: Weight::new(Decimal::ZERO)?,
                episode_opened_at: None,
                held_decision_bars: None,
            };
            slot.source_feed
                .feed_until(&mut slot.evaluator, bar.candle.close_time_utc)?;
            slot.evaluator.observe(bar, view, None)?;
        }
        if let Some((gate, gate_bars)) = &mut slot.gate {
            for bar in gate_bars {
                if bar.candle.close_time_utc >= plan.spec.range.start() {
                    break;
                }
                gate.observe(bar, plan.spec.decision_interval)?;
            }
        }
    }
    let mut cash = SharedCash {
        initial: spec.initial_cash.get(),
        total: spec.initial_cash.get(),
        reserved: Decimal::ZERO,
    };
    let mut state = RunnerState {
        seq: seq_start,
        spec: spec.clone(),
        intents: Vec::new(),
        fills: Vec::new(),
        rejections: Vec::new(),
        marks: Vec::new(),
        observations: Vec::new(),
        peak_equity: spec.initial_cash.get(),
        record_count: 0,
        portfolio_exposure_seconds: 0,
    };
    for (step_index, time) in timeline.iter().enumerate() {
        check_cancelled(cancelled)?;
        let marked = mark_state(&slots, cash.total, *time)?;
        state.push_mark(&marked, &slots, &cash, *time)?;
        let mut signals = Vec::new();
        #[allow(
            clippy::needless_range_loop,
            reason = "the loop mutates and reads several market slots by index"
        )]
        for slot_index in 0..slots.len() {
            let Some(bar) = slots[slot_index]
                .decision_bars
                .iter()
                .find(|bar| bar.candle.close_time_utc == *time)
                .cloned()
            else {
                return Err(LabError::DataGap(format!(
                    "market {} lacks a decision bar at {time}",
                    slots[slot_index].market
                )));
            };
            let position_value = checked_mul(
                slots[slot_index].qty,
                bar.candle.close.get(),
                "position value",
            )?;
            let actual_weight = if marked.equity.is_zero() {
                Decimal::ZERO
            } else {
                checked_div(position_value, marked.equity, "actual weight")?
            };
            {
                let slot = &mut slots[slot_index];
                if slot.qty > Decimal::ZERO {
                    if slot.episode_opened_at.is_none() {
                        slot.episode_opened_at = Some(bar.candle.close_time_utc);
                        slot.decision_marks_since_open = 0;
                    }
                    slot.decision_marks_since_open = slot
                        .decision_marks_since_open
                        .checked_add(1)
                        .ok_or_else(|| {
                            LabError::ResourceLimit("episode counter overflow".into())
                        })?;
                } else {
                    slot.episode_opened_at = None;
                    slot.decision_marks_since_open = 0;
                }
            }
            let holding = slots[slot_index].qty > Decimal::ZERO;
            let view = PositionView {
                actual_qty: AssetQuantity::new(slots[slot_index].qty)?,
                actual_weight: Weight::new(actual_weight)?,
                episode_opened_at: holding
                    .then_some(slots[slot_index].episode_opened_at)
                    .flatten(),
                held_decision_bars: holding.then_some(slots[slot_index].decision_marks_since_open),
            };
            let evaluation = {
                let slot = &mut slots[slot_index];
                slot.source_feed
                    .feed_until(&mut slot.evaluator, bar.candle.close_time_utc)?;
                slot.evaluator.observe(&bar, view, None)?
            }
            .ok_or_else(|| {
                LabError::InsufficientWarmup(
                    "strategy warmup did not complete by portfolio start".into(),
                )
            })?;
            let mut target = evaluation.constrained_target_weight.get();
            if let (Some(regime_spec), Some((gate, _))) = (regime, &mut slots[slot_index].gate) {
                // Warmup-incomplete classification applies the frozen UNKNOWN
                // rule: the gate never silently bypasses itself.
                let observation = gate.observe(&bar, plan.spec.decision_interval)?;
                let action = match &observation {
                    Some(observation) => gate_action(regime_spec, observation.regime),
                    None => gate_action(regime_spec, RegimeLabel::Unknown),
                };
                if !action.allows_entries() && target > Decimal::ZERO {
                    target = Decimal::ZERO;
                } else if let Some(cap) = action.exposure_cap() {
                    target = target.min(cap.get());
                }
                if let Some(observation) = observation {
                    state.observations.push(observation);
                }
            }
            signals.push(Signal {
                slot_index,
                target,
                raw_target: evaluation.raw_target_weight.get(),
            });
        }
        arbitrate_and_fill(
            &mut slots,
            &mut cash,
            &mut state,
            &signals,
            *time,
            plan,
            participation_cap,
        )?;
        let next_time = if let Some(next) = timeline.get(step_index + 1) {
            *next
        } else {
            plan.spec.range.end()
        };
        let step_seconds = u64::try_from((next_time.0 - time.0).num_seconds()).unwrap_or(0);
        if step_seconds > 0 {
            let mut portfolio_has_position = false;
            for slot in &mut slots {
                if slot.qty > Decimal::ZERO {
                    slot.exposure_seconds = slot.exposure_seconds.saturating_add(step_seconds);
                    portfolio_has_position = true;
                }
            }
            if portfolio_has_position {
                state.portfolio_exposure_seconds = state
                    .portfolio_exposure_seconds
                    .saturating_add(step_seconds);
            }
        }
    }
    // Terminal handling: optional liquidation at the terminal execution close.
    if plan.spec.terminal_policy == TerminalPolicy::LiquidateScenario {
        let terminal_time = plan.spec.range.end();
        #[allow(
            clippy::needless_range_loop,
            reason = "terminal liquidation mutates each slot by index after reads"
        )]
        for slot_index in 0..slots.len() {
            if slots[slot_index].qty.is_zero() {
                continue;
            }
            let bar = slots[slot_index]
                .execution_bars
                .last()
                .cloned()
                .ok_or_else(|| LabError::DataGap("terminal execution bar missing".into()))?;
            if bar.candle.close_time_utc != terminal_time {
                return Err(LabError::DataGap(
                    "terminal execution bar does not close at the range end".into(),
                ));
            }
            let liquidity_index = slots[slot_index]
                .execution_bars
                .len()
                .checked_sub(2)
                .ok_or_else(|| {
                    LabError::InsufficientWarmup(
                        "terminal liquidation needs a prior liquidity bar".into(),
                    )
                })?;
            let liquidity_source = slots[slot_index].execution_bars[liquidity_index].clone();
            let terminal_qty = slots[slot_index].qty;
            execute_fill(
                &mut slots[slot_index],
                &mut cash,
                &mut state,
                Side::Sell,
                terminal_qty,
                None,
                bar.candle.close.get(),
                bar.candle.close.get(),
                &bar,
                &liquidity_source,
                participation_cap,
                plan,
                ReasonCode::ArtificialTerminalExit,
                terminal_time,
                terminal_time,
            )?;
        }
    }
    let terminal_time = plan.spec.range.end();
    let terminal_mark = mark_state(&slots, cash.total, terminal_time)?;
    state.push_mark(&terminal_mark, &slots, &cash, terminal_time)?;
    let attribution = attribution(&slots, terminal_time)?;
    let totals = totals(&slots, &cash, &state, terminal_time)?;
    reconcile(&slots, &cash)?;
    // Attribution reconciliation: the per-asset contributions must explain the
    // whole portfolio PnL inside the accounting tolerance (doc §16).
    {
        let portfolio_pnl =
            checked_sub(totals.terminal_equity.get(), cash.initial, "portfolio pnl")?;
        let explained = attribution
            .iter()
            .map(|item| {
                item.realized_pnl
                    .get()
                    .checked_add(item.unrealized_pnl.get())
                    .and_then(|value| value.checked_sub(item.fees.get()))
            })
            .sum::<Option<Decimal>>()
            .ok_or_else(|| LabError::AccountingInvariant("attribution sum overflow".into()))?;
        let residual = checked_sub(portfolio_pnl, explained, "attribution residual")?;
        if residual.abs() > NUMERIC_TOLERANCE {
            return Err(LabError::AccountingInvariant(format!(
                "attribution does not reconcile: portfolio_pnl={portfolio_pnl}, explained={explained}, residual={residual}"
            )));
        }
    }
    Ok(PortfolioLedger {
        run_id: run_id.clone(),
        model_ids: slots.iter().map(|slot| slot.model_id.clone()).collect(),
        spec: spec.clone(),
        decision_interval: plan.spec.decision_interval,
        execution_resolution: plan.spec.execution_resolution,
        status: PortfolioStatus::Completed,
        status_reason: None,
        intents: state.intents,
        fills: state.fills,
        rejections: state.rejections,
        marks: state.marks,
        attribution,
        totals,
        regime_observations: state.observations,
        last_event_seq: state.seq.saturating_sub(1),
    })
}

fn check_cancelled(cancelled: &dyn Fn() -> bool) -> Result<(), LabError> {
    if cancelled() {
        Err(LabError::Cancelled(
            "portfolio execution cancelled at a deterministic checkpoint".into(),
        ))
    } else {
        Ok(())
    }
}

fn validate_frozen_inputs(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
) -> Result<(), LabError> {
    if datasets.len() != plan.spec.dataset_ids.len() {
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
    Ok(())
}

/// One strategy binding per market, matching the portfolio asset list.
#[expect(
    clippy::too_many_lines,
    reason = "slot construction binds frozen bars, policies and causal gates in one pass"
)]
fn market_slots(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    spec: &PortfolioSpec,
    regime: Option<&RegimeGateSpec>,
    run_id: &RunId,
) -> Result<Vec<MarketSlot>, LabError> {
    if plan.spec.markets.len() != spec.assets.len() {
        return Err(LabError::InvalidConfig(
            "portfolio assets must cover exactly the plan markets".into(),
        ));
    }
    let mut slots = Vec::new();
    for asset in &spec.assets {
        let admissions: Vec<_> = plan
            .admissions
            .iter()
            .filter(|admission| admission.market == asset.market)
            .collect();
        if admissions.len() != 1 {
            return Err(LabError::InvalidConfig(format!(
                "portfolio requires exactly one admitted strategy per market; {} has {}",
                asset.market,
                admissions.len()
            )));
        }
        let admission = admissions[0];
        if admission.status != AdmissionStatus::Eligible {
            return Err(LabError::InvalidConfig(format!(
                "portfolio market {} is not eligible",
                asset.market
            )));
        }
        let binding: StrategyBinding = crate::contracts::strategy_binding(plan, admission)?;
        let strategy_kind = binding.kind();
        let evaluator = PolicyEvaluator::compile(
            binding,
            plan,
            asset.market.clone(),
            plan.spec.decision_interval,
        )?;
        let warmup = u32::try_from(evaluator.decision_warmup_bars())
            .map_err(|_| LabError::ResourceLimit("policy warmup overflow".into()))?;
        let decision_range = plan
            .spec
            .range
            .with_warmup(warmup, plan.spec.decision_interval)?;
        let execution_range = plan
            .spec
            .range
            .with_warmup(1, plan.spec.execution_resolution)?;
        let decision_bars = crate::policy_engine::prepare_interval_bars(
            datasets,
            &asset.market,
            plan.spec.decision_interval,
            crate::policy_engine::SourceWindow::declared_warmup(
                decision_range,
                plan.spec.range.start(),
            ),
        )?;
        let execution_bars = crate::policy_engine::prepare_interval_bars(
            datasets,
            &asset.market,
            plan.spec.execution_resolution,
            execution_range,
        )?;
        // Bounded causal cross-interval stream per declared source interval.
        let source_feed =
            CrossIntervalFeed::new(&evaluator, datasets, &asset.market, plan.spec.range)?;
        // The classifier keeps its own causal stream: its warmup is usually
        // longer than the strategy's, and the strategy stream must stay at
        // the declared policy warmup for reproducibility.
        let gate = match regime {
            Some(regime_spec) => {
                let gate_warmup = u32::try_from(regime_spec.classifier.warmup_bars())
                    .map_err(|_| LabError::ResourceLimit("classifier warmup overflow".into()))?;
                let gate_range = plan
                    .spec
                    .range
                    .with_warmup(gate_warmup, plan.spec.decision_interval)?;
                let gate_bars = crate::engine::observations_for_range(
                    datasets,
                    &asset.market,
                    plan.spec.decision_interval,
                    Some(gate_range),
                )?
                .into_iter()
                .cloned()
                .collect();
                Some((
                    RegimeClassifier::new(
                        regime_spec.classifier.clone(),
                        asset.market.clone(),
                        plan_dataset_digest(plan),
                    ),
                    gate_bars,
                ))
            }
            None => None,
        };
        slots.push(MarketSlot {
            market: asset.market.clone(),
            model_id: ModelId::from_seed(&format!("{run_id}:{}", asset.market.code())),
            strategy_kind,
            evaluator,
            decision_bars,
            execution_bars,
            qty: Decimal::ZERO,
            price_basis: Decimal::ZERO,
            realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            buy_notional: Decimal::ZERO,
            sell_notional: Decimal::ZERO,
            closed_trades: 0,
            max_weight_seen: Decimal::ZERO,
            exposure_seconds: 0,
            episode_opened_at: None,
            decision_marks_since_open: 0,
            source_feed,
            gate,
        });
    }
    Ok(slots)
}

/// The plan-level dataset digest binds regime observation inputs.
fn plan_dataset_digest(plan: &ResolvedPlan) -> ContentHash {
    let mut combined = String::new();
    for (id, digest) in &plan.dataset_digests {
        combined.push_str(id.as_str());
        combined.push(':');
        combined.push_str(digest.as_str());
        combined.push(';');
    }
    ContentHash::of_bytes(combined.as_bytes())
}

/// Shared decision closes inside the evaluation range; misalignment fails closed.
fn shared_timeline(
    slots: &[MarketSlot],
    plan: &ResolvedPlan,
) -> Result<Vec<UtcTimestamp>, LabError> {
    let first = slots
        .first()
        .ok_or_else(|| LabError::InvalidConfig("no portfolio markets".into()))?;
    let in_range =
        |time: &UtcTimestamp| *time >= plan.spec.range.start() && *time < plan.spec.range.end();
    let timeline: Vec<UtcTimestamp> = first
        .decision_bars
        .iter()
        .map(|bar| bar.candle.close_time_utc)
        .filter(in_range)
        .collect();
    for slot in slots {
        let closes: Vec<UtcTimestamp> = slot
            .decision_bars
            .iter()
            .map(|bar| bar.candle.close_time_utc)
            .filter(in_range)
            .collect();
        if closes != timeline {
            return Err(LabError::DataGap(
                "market decision grids are not aligned; shared-capital arbitration requires identical decision closes".into(),
            ));
        }
    }
    Ok(timeline)
}

/// Mark every market at the latest decision close at or before `time`.
fn mark_state(
    slots: &[MarketSlot],
    cash_total: Decimal,
    time: UtcTimestamp,
) -> Result<PortfolioMark, LabError> {
    let mut position_value = Decimal::ZERO;
    let mut weights = BTreeMap::new();
    for slot in slots {
        let close = close_at(slot, time)?;
        let value = checked_mul(slot.qty, close, "marked position")?;
        position_value = checked_add(position_value, value, "position sum")?;
    }
    let equity = checked_add(cash_total, position_value, "portfolio equity")?;
    for slot in slots {
        let close = close_at(slot, time)?;
        let value = checked_mul(slot.qty, close, "marked position")?;
        let weight = if equity.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(value, equity, "asset weight")?
        };
        weights.insert(slot.market.code(), weight);
    }
    let gross_weight = if equity.is_zero() {
        Decimal::ZERO
    } else {
        checked_div(position_value, equity, "gross weight")?
    };
    Ok(PortfolioMark {
        cash: cash_total,
        position_value,
        gross_weight,
        equity,
        weights,
    })
}

/// Latest decision close at or before `time`; the terminal instant prefers the
/// execution bar that closes exactly at the range end.
fn close_at(slot: &MarketSlot, time: UtcTimestamp) -> Result<Decimal, LabError> {
    if let Ok(index) = slot
        .execution_bars
        .binary_search_by_key(&time, |bar| bar.candle.close_time_utc)
    {
        return Ok(slot.execution_bars[index].candle.close.get());
    }
    slot.decision_bars
        .partition_point(|bar| bar.candle.close_time_utc <= time)
        .checked_sub(1)
        .and_then(|index| slot.decision_bars.get(index))
        .map(|bar| bar.candle.close.get())
        .ok_or_else(|| {
            LabError::DataGap(format!(
                "portfolio market {} has no causal close at or before {time}",
                slot.market
            ))
        })
}

/// Unrealized profit of every position at `time` (mark value minus basis).
fn unrealized_at(slots: &[MarketSlot], time: UtcTimestamp) -> Result<Decimal, LabError> {
    let mut total = Decimal::ZERO;
    for slot in slots {
        let close = close_at(slot, time)?;
        let value = checked_mul(slot.qty, close, "marked value")?;
        total = checked_add(
            total,
            checked_sub(value, slot.price_basis, "unrealized")?,
            "total unrealized",
        )?;
    }
    Ok(total)
}

/// Deterministic arbitration across simultaneous signals, then immediate
/// next-bar-open fills at the shared execution instant.
#[expect(
    clippy::too_many_lines,
    reason = "arbitration, reservation and fill settlement form one atomic allocation round"
)]
#[allow(clippy::too_many_arguments)]
fn arbitrate_and_fill(
    slots: &mut [MarketSlot],
    cash: &mut SharedCash,
    state: &mut RunnerState,
    signals: &[Signal],
    time: UtcTimestamp,
    plan: &ResolvedPlan,
    participation_cap: Weight,
) -> Result<(), LabError> {
    if signals.is_empty() {
        return Ok(());
    }
    let marked = mark_state(slots, cash.total, time)?;
    let equity = marked.equity;
    let mut ordered: Vec<&Signal> = signals.iter().collect();
    match state.spec.arbitration {
        ArbitrationPolicy::ScoreRanked => {
            ordered.sort_by(|left, right| {
                right
                    .raw_target
                    .partial_cmp(&left.raw_target)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        priority_of(state, slots, left).cmp(&priority_of(state, slots, right))
                    })
                    .then_with(|| {
                        slots[left.slot_index]
                            .market
                            .code()
                            .cmp(&slots[right.slot_index].market.code())
                    })
            });
        }
        ArbitrationPolicy::Priority | ArbitrationPolicy::ProRata => {
            ordered.sort_by(|left, right| {
                priority_of(state, slots, left)
                    .cmp(&priority_of(state, slots, right))
                    .then_with(|| {
                        slots[left.slot_index]
                            .market
                            .code()
                            .cmp(&slots[right.slot_index].market.code())
                    })
            });
        }
    }
    // Sells first: they release cash for the competing buys.
    let mut sells = Vec::new();
    let mut buys = Vec::new();
    for signal in &ordered {
        let weight = current_weight(slots, signal.slot_index, equity, time)?;
        if signal.target < weight {
            sells.push(*signal);
        } else if signal.target > weight {
            buys.push(*signal);
        }
    }
    for (rank, signal) in sells.iter().enumerate() {
        let slot = &slots[signal.slot_index];
        let bar = slot
            .decision_bars
            .iter()
            .find(|bar| bar.candle.close_time_utc == time)
            .cloned()
            .ok_or_else(|| LabError::Internal("sell signal lost its decision bar".into()))?;
        let position_value = checked_mul(slot.qty, bar.candle.close.get(), "position value")?;
        let target_value = checked_mul(signal.target, equity, "target value")?;
        let requested = checked_sub(position_value, target_value.max(Decimal::ZERO), "sell gap")?;
        state.push_intent(
            time,
            &slot.market,
            Side::Sell,
            signal.target,
            requested,
            u32::try_from(rank).map_err(|_| LabError::ResourceLimit("rank overflow".into()))?,
        )?;
        let qty = checked_div(requested, bar.candle.close.get(), "sell quantity")?;
        let slot_index = signal.slot_index;
        let execution_bar = slots[slot_index]
            .execution_bars
            .iter()
            .find(|bar| bar.candle.open_time_utc == time)
            .cloned()
            .ok_or_else(|| {
                LabError::DataGap(format!("execution bar missing at decision time {time}"))
            })?;
        let liquidity_index = slots[slot_index]
            .execution_bars
            .iter()
            .position(|bar| bar.candle.open_time_utc == time)
            .and_then(|index| index.checked_sub(1))
            .ok_or_else(|| {
                LabError::InsufficientWarmup(
                    "portfolio execution needs one prior liquidity bar".into(),
                )
            })?;
        let liquidity_source = slots[slot_index].execution_bars[liquidity_index].clone();
        execute_fill(
            &mut slots[slot_index],
            cash,
            state,
            Side::Sell,
            qty,
            None,
            bar.candle.close.get(),
            execution_bar.candle.open.get(),
            &execution_bar,
            &liquidity_source,
            participation_cap,
            plan,
            ReasonCode::MarketFilled,
            time,
            time,
        )?;
    }
    if state.spec.arbitration == ArbitrationPolicy::ProRata && buys.len() > 1 {
        allocate_pro_rata(
            slots,
            cash,
            state,
            &buys,
            time,
            plan,
            equity,
            participation_cap,
        )?;
    } else {
        for (rank, signal) in buys.iter().enumerate() {
            allocate_buy(
                slots,
                cash,
                state,
                signal,
                u32::try_from(rank).map_err(|_| LabError::ResourceLimit("rank overflow".into()))?,
                time,
                plan,
                equity,
                participation_cap,
            )?;
        }
    }
    Ok(())
}

fn priority_of(state: &RunnerState, slots: &[MarketSlot], signal: &Signal) -> usize {
    state.spec.priority(&slots[signal.slot_index].market)
}

fn current_weight(
    slots: &[MarketSlot],
    slot_index: usize,
    equity: Decimal,
    time: UtcTimestamp,
) -> Result<Decimal, LabError> {
    if equity.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let slot = &slots[slot_index];
    let close = close_at(slot, time)?;
    let value = checked_mul(slot.qty, close, "current value")?;
    checked_div(value, equity, "current weight")
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::too_many_lines,
    reason = "pro rata arbitration coordinates cash scaling, allocation bounds and execution"
)]
fn allocate_pro_rata(
    slots: &mut [MarketSlot],
    cash: &mut SharedCash,
    state: &mut RunnerState,
    buys: &[&Signal],
    time: UtcTimestamp,
    plan: &ResolvedPlan,
    equity: Decimal,
    participation_cap: Weight,
) -> Result<(), LabError> {
    // Pro-rata scales every competing buy by one common factor so the shared
    // pool is never double-spent and no buy is silently preferred.
    let mut targets = Vec::with_capacity(buys.len());
    let mut multipliers = Vec::with_capacity(buys.len());
    let mut cash_demands = Vec::with_capacity(buys.len());
    let mut total_desired = Decimal::ZERO;
    let mut total_cash_demand = Decimal::ZERO;

    for signal in buys {
        let slot = &slots[signal.slot_index];
        let desired = desired_notional(slots, signal, equity, time)?;
        let close = close_at(slot, time)?;
        let position_value = checked_mul(slot.qty, close, "position value")?;
        let asset_headroom = checked_sub(
            checked_mul(state.spec.asset_cap(&slot.market), equity, "asset budget")?,
            position_value,
            "asset headroom",
        )?
        .max(Decimal::ZERO);
        let target = desired.min(asset_headroom);

        let fees = plan.spec.fee_policy_at(time)?;
        let fee_rate = bps_rate(fees.buy)?;
        let price_cost_rate = plan_price_cost_rate(plan)?;
        let multiplier = checked_add(
            Decimal::ONE,
            checked_add(fee_rate, price_cost_rate, "reservation costs")?,
            "reservation multiplier",
        )?;
        let cash_demand = checked_mul(target, multiplier, "buy cash demand")?;

        total_desired = checked_add(total_desired, target, "pro-rata target sum")?;
        total_cash_demand =
            checked_add(total_cash_demand, cash_demand, "pro-rata cash demand sum")?;

        targets.push(target);
        multipliers.push(multiplier);
        cash_demands.push(cash_demand);
    }

    let gross = state.spec.risk.max_gross_exposure.get();
    let gross_headroom = checked_sub(
        checked_mul(gross, equity, "gross budget")?,
        gross_value(slots, time)?,
        "gross headroom",
    )?
    .max(Decimal::ZERO);
    let reserve = state.spec.risk.min_cash_weight.get();
    let cash_headroom = checked_sub(
        cash.free()?,
        checked_mul(reserve, equity, "reserve budget")?,
        "cash headroom",
    )?
    .max(Decimal::ZERO);

    let gross_factor = if total_desired > gross_headroom && !total_desired.is_zero() {
        checked_div(gross_headroom, total_desired, "gross pro-rata factor")?
    } else {
        Decimal::ONE
    };
    let cash_factor = if total_cash_demand > cash_headroom && !total_cash_demand.is_zero() {
        checked_div(cash_headroom, total_cash_demand, "cash pro-rata factor")?
    } else {
        Decimal::ONE
    };

    let factor = if total_desired.is_zero() || total_cash_demand.is_zero() {
        Decimal::ZERO
    } else {
        Decimal::ONE.min(gross_factor).min(cash_factor)
    };

    let min_notional = plan.spec.rules_at(time)?.min_notional.get();

    for (rank, ((signal, target_notional), multiplier)) in
        buys.iter().zip(&targets).zip(&multipliers).enumerate()
    {
        let desired = desired_notional(slots, signal, equity, time)?;
        let scaled = checked_mul(*target_notional, factor, "pro-rata scaling")?;
        let max_safe = checked_div(cash.free()?, *multiplier, "safe notional")?;
        let allocation = scaled.min(max_safe);

        if allocation < min_notional || allocation.is_zero() {
            let binding = if allocation.is_zero() {
                binding_reason(slots, cash, state, signal, equity, time)?
            } else {
                PortfolioRejectionReason::MinNotional
            };
            state.push_intent(
                time,
                &slots[signal.slot_index].market,
                Side::Buy,
                signal.target,
                desired,
                u32::try_from(rank).map_err(|_| LabError::ResourceLimit("rank overflow".into()))?,
            )?;
            state.push_rejection(
                time,
                &slots[signal.slot_index].market,
                Side::Buy,
                signal.target,
                desired,
                binding,
            )?;
            continue;
        }

        state.push_intent(
            time,
            &slots[signal.slot_index].market,
            Side::Buy,
            signal.target,
            desired,
            u32::try_from(rank).map_err(|_| LabError::ResourceLimit("rank overflow".into()))?,
        )?;
        execute_buy(
            slots,
            cash,
            state,
            signal,
            allocation,
            time,
            plan,
            participation_cap,
        )?;
    }
    Ok(())
}

fn desired_notional(
    slots: &[MarketSlot],
    signal: &Signal,
    equity: Decimal,
    time: UtcTimestamp,
) -> Result<Decimal, LabError> {
    let slot = &slots[signal.slot_index];
    let close = close_at(slot, time)?;
    let position_value = checked_mul(slot.qty, close, "position value")?;
    let target_value = checked_mul(signal.target, equity, "target value")?;
    Ok(checked_sub(target_value, position_value, "buy gap")?.max(Decimal::ZERO))
}

/// Combined headroom for one buy: asset cap, gross cap and cash reserve.
fn buy_headroom(
    slots: &[MarketSlot],
    cash: &SharedCash,
    state: &RunnerState,
    equity: Decimal,
    time: UtcTimestamp,
) -> Result<Decimal, LabError> {
    let gross = state.spec.risk.max_gross_exposure.get();
    let gross_headroom = checked_sub(
        checked_mul(gross, equity, "gross budget")?,
        gross_value(slots, time)?,
        "gross headroom",
    )?
    .max(Decimal::ZERO);
    let reserve = state.spec.risk.min_cash_weight.get();
    let cash_headroom = checked_sub(
        cash.free()?,
        checked_mul(reserve, equity, "reserve budget")?,
        "cash headroom",
    )?
    .max(Decimal::ZERO);
    Ok(gross_headroom.min(cash_headroom))
}

/// The typed reason that would bind a zero allocation right now.
fn binding_reason(
    slots: &[MarketSlot],
    cash: &SharedCash,
    state: &RunnerState,
    signal: &Signal,
    equity: Decimal,
    time: UtcTimestamp,
) -> Result<PortfolioRejectionReason, LabError> {
    let slot = &slots[signal.slot_index];
    let close = close_at(slot, time)?;
    let position_value = checked_mul(slot.qty, close, "position value")?;
    let asset_headroom = checked_sub(
        checked_mul(state.spec.asset_cap(&slot.market), equity, "asset budget")?,
        position_value,
        "asset headroom",
    )?
    .max(Decimal::ZERO);
    let stop = state.spec.risk.drawdown_stop.get();
    let peak = state.peak_equity;
    let drawdown = if peak.is_zero() {
        Decimal::ZERO
    } else {
        checked_div(checked_sub(peak, equity, "drawdown")?, peak, "drawdown")?
    };
    let gross_headroom = buy_headroom(slots, cash, state, equity, time)?;
    if stop > Decimal::ZERO && drawdown >= stop {
        Ok(PortfolioRejectionReason::PortfolioStop)
    } else if asset_headroom.is_zero() {
        Ok(PortfolioRejectionReason::AssetWeightCap)
    } else if gross_headroom.is_zero() {
        Ok(PortfolioRejectionReason::GrossExposureCap)
    } else {
        Ok(PortfolioRejectionReason::CashReserve)
    }
}

#[allow(clippy::too_many_arguments)]
fn allocate_buy(
    slots: &mut [MarketSlot],
    cash: &mut SharedCash,
    state: &mut RunnerState,
    signal: &Signal,
    rank: u32,
    time: UtcTimestamp,
    plan: &ResolvedPlan,
    equity: Decimal,
    participation_cap: Weight,
) -> Result<(), LabError> {
    let market = slots[signal.slot_index].market.clone();
    let requested = desired_notional(slots, signal, equity, time)?;
    state.push_intent(time, &market, Side::Buy, signal.target, requested, rank)?;
    let stop = state.spec.risk.drawdown_stop.get();
    let drawdown = if state.peak_equity.is_zero() {
        Decimal::ZERO
    } else {
        checked_div(
            checked_sub(state.peak_equity, equity, "drawdown")?,
            state.peak_equity,
            "drawdown",
        )?
    };
    if stop > Decimal::ZERO && drawdown >= stop {
        state.push_rejection(
            time,
            &market,
            Side::Buy,
            signal.target,
            requested,
            PortfolioRejectionReason::PortfolioStop,
        )?;
        return Ok(());
    }
    let slot = &slots[signal.slot_index];
    let close = close_at(slot, time)?;
    let position_value = checked_mul(slot.qty, close, "position value")?;
    let asset_headroom = checked_sub(
        checked_mul(state.spec.asset_cap(&slot.market), equity, "asset budget")?,
        position_value,
        "asset headroom",
    )?
    .max(Decimal::ZERO);
    let gross = state.spec.risk.max_gross_exposure.get();
    let gross_headroom = checked_sub(
        checked_mul(gross, equity, "gross budget")?,
        gross_value(slots, time)?,
        "gross headroom",
    )?
    .max(Decimal::ZERO);
    let reserve = state.spec.risk.min_cash_weight.get();
    let cash_headroom = checked_sub(
        cash.free()?,
        checked_mul(reserve, equity, "reserve budget")?,
        "cash headroom",
    )?
    .max(Decimal::ZERO);
    let fees = plan.spec.fee_policy_at(time)?;
    let fee_rate = bps_rate(fees.buy)?;
    let price_cost_rate = plan_price_cost_rate(plan)?;
    let multiplier = checked_add(
        Decimal::ONE,
        checked_add(fee_rate, price_cost_rate, "reservation costs")?,
        "reservation multiplier",
    )?;
    let cash_notional_headroom = checked_div(cash_headroom, multiplier, "cash notional headroom")?;
    // The binding cap clips the allocation; a clipped amount at or above the
    // minimum notional still trades (a partial fill), anything smaller is a
    // typed rejection that names the binding cap.
    let allocation = requested
        .min(asset_headroom)
        .min(gross_headroom)
        .min(cash_notional_headroom);
    let min_notional = plan.spec.rules_at(time)?.min_notional.get();
    if allocation >= min_notional && !allocation.is_zero() {
        execute_buy(
            slots,
            cash,
            state,
            signal,
            allocation,
            time,
            plan,
            participation_cap,
        )?;
        return Ok(());
    }
    let reason = if asset_headroom <= gross_headroom && asset_headroom <= cash_headroom {
        PortfolioRejectionReason::AssetWeightCap
    } else if gross_headroom <= cash_headroom {
        PortfolioRejectionReason::GrossExposureCap
    } else if cash.free()?.is_zero() {
        PortfolioRejectionReason::InsufficientCash
    } else {
        PortfolioRejectionReason::CashReserve
    };
    state.push_rejection(time, &market, Side::Buy, signal.target, requested, reason)
}

#[allow(clippy::too_many_arguments)]
fn execute_buy(
    slots: &mut [MarketSlot],
    cash: &mut SharedCash,
    state: &mut RunnerState,
    signal: &Signal,
    notional: Decimal,
    time: UtcTimestamp,
    plan: &ResolvedPlan,
    participation_cap: Weight,
) -> Result<(), LabError> {
    let slot = &slots[signal.slot_index];
    let bar = slot
        .decision_bars
        .iter()
        .find(|bar| bar.candle.close_time_utc == time)
        .cloned()
        .ok_or_else(|| LabError::Internal("buy allocation lost its decision bar".into()))?;
    let fees = plan.spec.fee_policy_at(time)?;
    let fee_rate = bps_rate(fees.buy)?;
    // The reservation covers the worst-case debit: notional uplifted by the
    // price-cost rate and the fee, so the pool can never be double-spent.
    let price_cost_rate = plan_price_cost_rate(plan)?;
    let multiplier = checked_add(
        Decimal::ONE,
        checked_add(fee_rate, price_cost_rate, "reservation costs")?,
        "reservation multiplier",
    )?;
    let raw_reserved = checked_mul(notional, multiplier, "buy reservation")?;
    let free = cash.free()?;
    let reserved = raw_reserved.min(free);
    cash.reserve(reserved)?;
    let requested_qty = checked_div(notional, bar.candle.close.get(), "buy quantity")?;
    let slot_index = signal.slot_index;
    let execution_bar = slots[slot_index]
        .execution_bars
        .iter()
        .find(|bar| bar.candle.open_time_utc == time)
        .cloned();
    let Some(execution_bar) = execution_bar else {
        cash.release(reserved)?;
        return Err(LabError::DataGap(format!(
            "execution bar missing at decision time {time}"
        )));
    };
    let liquidity_index = slots[slot_index]
        .execution_bars
        .iter()
        .position(|bar| bar.candle.open_time_utc == time)
        .and_then(|index| index.checked_sub(1))
        .ok_or_else(|| {
            LabError::InsufficientWarmup("portfolio execution needs one prior liquidity bar".into())
        })?;
    let liquidity_source = slots[slot_index].execution_bars[liquidity_index].clone();
    let qty = fill_quantity(
        requested_qty,
        Some(reserved),
        fee_rate,
        &execution_bar,
        &liquidity_source,
        participation_cap,
        plan,
        time,
    )?;
    execute_fill(
        &mut slots[slot_index],
        cash,
        state,
        Side::Buy,
        qty,
        Some(reserved),
        bar.candle.close.get(),
        execution_bar.candle.open.get(),
        &execution_bar,
        &liquidity_source,
        participation_cap,
        plan,
        ReasonCode::MarketFilled,
        time,
        time,
    )?;
    Ok(())
}

/// Reservation- and volume-capped fill quantity at one execution open.
#[allow(clippy::too_many_arguments)]
fn fill_quantity(
    requested_qty: Decimal,
    reserved: Option<Decimal>,
    fee_rate: Decimal,
    execution_bar: &CandleObservation,
    liquidity_source: &CandleObservation,
    participation_cap: Weight,
    plan: &ResolvedPlan,
    time: UtcTimestamp,
) -> Result<Decimal, LabError> {
    let rules = plan.spec.rules_at(time)?;
    // The cap must anticipate the exact price execute_fill will settle:
    // open uplifted by the price-cost rate and rounded up to the tick.
    let price_cost_rate = plan_price_cost_rate(plan)?;
    let multiplier = checked_add(Decimal::ONE, price_cost_rate, "worst-case multiplier")?;
    let worst_price = round_adversarial(
        checked_mul(
            execution_bar.candle.open.get(),
            multiplier,
            "worst-case price",
        )?,
        tick_for(execution_bar.candle.open.get(), rules)?,
        Side::Buy,
    )?;
    let volume_cap = checked_mul(
        liquidity_source.candle.volume.get(),
        participation_cap.get(),
        "volume cap",
    )?;
    let mut cap = volume_cap.min(requested_qty);
    if let Some(reserved) = reserved {
        let per_unit_debit = checked_mul(
            worst_price,
            checked_add(Decimal::ONE, fee_rate, "fee multiplier")?,
            "debit",
        )?;
        let reservation_cap = checked_div(reserved, per_unit_debit, "reservation cap")?;
        cap = cap.min(reservation_cap);
    }
    floor_to_step(cap, rules.quantity_step.get())
}

fn round_adversarial(value: Decimal, tick: Decimal, side: Side) -> Result<Decimal, LabError> {
    use rust_decimal::RoundingStrategy;
    if tick <= Decimal::ZERO {
        return Err(LabError::InvalidConfig("tick must be positive".into()));
    }
    let units = checked_div(value, tick, "tick units")?.round_dp_with_strategy(
        0,
        match side {
            Side::Buy => RoundingStrategy::ToPositiveInfinity,
            Side::Sell => RoundingStrategy::ToNegativeInfinity,
        },
    );
    checked_mul(units, tick, "tick-rounded price")
}

#[expect(
    clippy::too_many_lines,
    reason = "one fill settles price costs, reservations, cash, basis and facts atomically"
)]
#[allow(clippy::too_many_arguments)]
fn execute_fill(
    slot: &mut MarketSlot,
    cash: &mut SharedCash,
    state: &mut RunnerState,
    side: Side,
    requested_qty: Decimal,
    reserved: Option<Decimal>,
    decision_close: Decimal,
    execution_open: Decimal,
    execution_bar: &CandleObservation,
    liquidity_source: &CandleObservation,
    participation_cap: Weight,
    plan: &ResolvedPlan,
    reason: ReasonCode,
    decision_time: UtcTimestamp,
    execution_time: UtcTimestamp,
) -> Result<(), LabError> {
    let _ = reason;
    let fees = plan.spec.fee_policy_at(execution_time)?;
    let fee_bps: BasisPoints = match side {
        Side::Buy => fees.buy,
        Side::Sell => fees.sell,
    };
    let fee_rate = bps_rate(fee_bps)?;
    let price_cost_rate = plan_price_cost_rate(plan)?;
    let multiplier = match side {
        Side::Buy => checked_add(Decimal::ONE, price_cost_rate, "buy price multiplier")?,
        Side::Sell => checked_sub(Decimal::ONE, price_cost_rate, "sell price multiplier")?,
    };
    let rules = plan.spec.rules_at(execution_time)?;
    let unrounded = checked_mul(execution_open, multiplier, "cost-adjusted price")?;
    let rounded = round_adversarial(unrounded, tick_for(execution_open, rules)?, side)?;
    let price_cost = match side {
        Side::Buy => checked_sub(rounded, decision_close, "buy price cost")?,
        Side::Sell => checked_sub(decision_close, rounded, "sell price cost")?,
    };
    let volume_cap = checked_mul(
        liquidity_source.candle.volume.get(),
        participation_cap.get(),
        "volume cap",
    )?;
    let qty = floor_to_step(requested_qty.min(volume_cap), rules.quantity_step.get())?;
    let notional = checked_mul(qty, rounded, "fill notional")?;
    let price_cost_quote = checked_mul(price_cost, qty, "fill price cost quote")?;
    if qty.is_zero() || notional < rules.min_notional.get() {
        if let Some(reserved) = reserved {
            cash.release(reserved)?;
        }
        // A clipped or dust fill is a typed rejection, never a silent skip.
        let reason = if qty.is_zero() {
            PortfolioRejectionReason::RuleViolation
        } else {
            PortfolioRejectionReason::MinNotional
        };
        state.push_rejection(
            decision_time,
            &slot.market,
            side,
            Decimal::ZERO,
            checked_mul(requested_qty, rounded, "rejected notional")?,
            reason,
        )?;
        return Ok(());
    }
    let fee = checked_mul(notional, fee_rate, "fill fee")?;
    match side {
        Side::Buy => {
            if let Some(reserved) = reserved {
                cash.release(reserved)?;
            }
            let debit = checked_add(notional, fee, "buy debit")?;
            let available_cash = cash.free()?;
            if debit > available_cash {
                return Err(LabError::AccountingInvariant(format!(
                    "portfolio buy debit exceeds free cash: market={}, debit={debit}, free={available_cash}",
                    slot.market
                )));
            }
            cash.settle_buy(debit)?;
            slot.qty = checked_add(slot.qty, qty, "buy quantity")?;
            slot.price_basis = checked_add(slot.price_basis, notional, "buy basis")?;
            slot.buy_notional = checked_add(slot.buy_notional, notional, "buy notional")?;
        }
        Side::Sell => {
            if qty > slot.qty {
                return Err(LabError::AccountingInvariant(format!(
                    "portfolio sell exceeds inventory: market={}, sell_qty={qty}, slot_qty={}",
                    slot.market, slot.qty
                )));
            }
            let removed_basis = if qty == slot.qty {
                slot.price_basis
            } else {
                checked_mul(
                    slot.price_basis,
                    checked_div(qty, slot.qty, "sold fraction")?,
                    "removed basis",
                )?
            };
            let proceeds = checked_sub(notional, fee, "net proceeds")?;
            let realized = checked_sub(notional, removed_basis, "realized pnl")?;
            cash.settle_sell(proceeds)?;
            slot.qty = checked_sub(slot.qty, qty, "sell quantity")?;
            slot.price_basis = checked_sub(slot.price_basis, removed_basis, "remaining basis")?;
            if slot.qty.is_zero() {
                slot.price_basis = Decimal::ZERO;
            }
            slot.realized = checked_add(slot.realized, realized, "cumulative realized")?;
            slot.sell_notional = checked_add(slot.sell_notional, notional, "sell notional")?;
            slot.closed_trades = slot.closed_trades.saturating_add(1);
        }
    }
    slot.fees = checked_add(slot.fees, fee, "slot fees")?;
    let seq = state.next_seq()?;
    state.fills.push(PortfolioFillRecord {
        event_seq: seq,
        market: slot.market.clone(),
        side,
        price: PriceKrw::new(rounded)?,
        qty: AssetQuantity::new(qty)?,
        notional: QuoteAmount::new(notional)?,
        fee: QuoteAmount::new(fee)?,
        fee_bps,
        price_cost: crate::contracts::SignedAmount::new(price_cost_quote)?,
        price_difference_per_unit: Some(crate::contracts::SignedAmount::new(price_cost)?),
        reserved_cash: QuoteAmount::new(Decimal::ZERO)?,
        decision_time,
        execution_time,
        source_bar_id: execution_bar.id.as_str().to_string(),
        liquidity_source_bar_id: liquidity_source.id.as_str().to_string(),
        cash_after: QuoteAmount::new(cash.total)?,
    });
    Ok(())
}

fn plan_price_cost_rate(plan: &ResolvedPlan) -> Result<Decimal, LabError> {
    let costs = &plan.spec.costs;
    checked_div(
        checked_add(
            checked_add(
                costs.half_spread_bps.get(),
                costs.slippage_bps.get(),
                "price costs",
            )?,
            costs.impact_bps.get(),
            "price costs",
        )?,
        Decimal::from_parts(10_000, 0, 0, false, 0),
        "price cost rate",
    )
}

fn gross_value(slots: &[MarketSlot], time: UtcTimestamp) -> Result<Decimal, LabError> {
    let mut total = Decimal::ZERO;
    for slot in slots {
        let close = close_at(slot, time)?;
        total = checked_add(
            total,
            checked_mul(slot.qty, close, "position value")?,
            "gross value",
        )?;
    }
    Ok(total)
}

fn attribution(
    slots: &[MarketSlot],
    terminal_time: UtcTimestamp,
) -> Result<Vec<PortfolioAttribution>, LabError> {
    slots
        .iter()
        .map(|slot| {
            let close = close_at(slot, terminal_time)?;
            let unrealized = checked_sub(
                checked_mul(slot.qty, close, "terminal value")?,
                slot.price_basis,
                "terminal unrealized",
            )?;
            Ok(PortfolioAttribution {
                market: slot.market.clone(),
                strategy: slot.strategy_kind,
                buy_notional: QuoteAmount::new(slot.buy_notional)?,
                sell_notional: QuoteAmount::new(slot.sell_notional)?,
                fees: QuoteAmount::new(slot.fees)?,
                realized_pnl: crate::contracts::SignedAmount::new(slot.realized)?,
                unrealized_pnl: crate::contracts::SignedAmount::new(unrealized)?,
                closed_trades: slot.closed_trades,
                max_weight_seen: Weight::new(slot.max_weight_seen)?,
                exposure_seconds: slot.exposure_seconds,
            })
        })
        .collect()
}

fn totals(
    slots: &[MarketSlot],
    cash: &SharedCash,
    state: &RunnerState,
    terminal_time: UtcTimestamp,
) -> Result<PortfolioTotals, LabError> {
    let marked = mark_state(slots, cash.total, terminal_time)?;
    let max_drawdown = maximum_drawdown(
        state
            .marks
            .iter()
            .map(|mark| mark.equity.get())
            .chain(std::iter::once(marked.equity)),
    )?;
    let gross = slots
        .iter()
        .map(|slot| slot.buy_notional + slot.sell_notional)
        .sum::<Decimal>();
    let mut rejection_reasons = BTreeMap::new();
    for rejection in &state.rejections {
        let key = serde_json::to_value(rejection.reason)?
            .as_str()
            .unwrap_or_default()
            .to_string();
        rejection_reasons
            .entry(key)
            .and_modify(|count: &mut u64| *count = count.saturating_add(1))
            .or_insert(1_u64);
    }
    Ok(PortfolioTotals {
        terminal_equity: QuoteAmount::new(marked.equity)?,
        total_return: if cash.initial.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(
                checked_sub(marked.equity, cash.initial, "total return")?,
                cash.initial,
                "total return",
            )?
        },
        max_drawdown: Weight::new(max_drawdown)?,
        turnover: if cash.initial.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(gross, cash.initial, "turnover")?
        },
        total_fees: QuoteAmount::new(slots.iter().map(|slot| slot.fees).sum::<Decimal>())?,
        price_cost_drag: crate::contracts::SignedAmount::new(
            state
                .fills
                .iter()
                .map(|fill| fill.price_cost.get())
                .sum::<Decimal>(),
        )?,
        exposure_seconds: state.portfolio_exposure_seconds,
        rejected_signals: u64::try_from(state.rejections.len())
            .map_err(|_| LabError::ResourceLimit("rejection count overflow".into()))?,
        rejection_reasons,
    })
}

fn maximum_drawdown(equities: impl IntoIterator<Item = Decimal>) -> Result<Decimal, LabError> {
    let mut peak = Decimal::ZERO;
    let mut maximum = Decimal::ZERO;
    for equity in equities {
        if equity >= peak {
            peak = equity;
            continue;
        }
        if peak.is_zero() {
            continue;
        }
        let drawdown = checked_div(
            checked_sub(peak, equity, "max drawdown")?,
            peak,
            "max drawdown",
        )?;
        maximum = maximum.max(drawdown);
    }
    Ok(maximum)
}

/// Final reconciliation: cash bounds, position bounds and the aggregate
/// identity between the terminal equity and the accumulated facts.
fn reconcile(slots: &[MarketSlot], cash: &SharedCash) -> Result<(), LabError> {
    if cash.total < Decimal::ZERO || cash.reserved < Decimal::ZERO || cash.reserved > cash.total {
        return Err(LabError::AccountingInvariant(format!(
            "portfolio cash invariants violated at reconciliation: total={}, reserved={}",
            cash.total, cash.reserved
        )));
    }
    for slot in slots {
        if slot.qty < Decimal::ZERO || slot.price_basis < Decimal::ZERO {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio position invariants violated for {}: qty={}, basis={}",
                slot.market, slot.qty, slot.price_basis
            )));
        }
        if slot.qty.is_zero() != slot.price_basis.is_zero() {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio quantity and basis disagree for {}: qty={}, basis={}",
                slot.market, slot.qty, slot.price_basis
            )));
        }
    }
    Ok(())
}

/// Same-period benchmarks for one portfolio plan: cash, per-market buy-and-hold
/// and an equal-weight static allocation. Gross price paths, no cost model.
#[expect(
    clippy::too_many_lines,
    reason = "three benchmark kinds share one bounded projection pass"
)]
///
/// # Errors
/// Rejects missing boundary bars or arithmetic failures.
pub fn portfolio_benchmarks(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    spec: &PortfolioSpec,
) -> Result<Vec<BenchmarkResult>, LabError> {
    let initial = spec.initial_cash.get();
    let mut results = vec![BenchmarkResult {
        kind: BenchmarkKind::Cash,
        market: None,
        terminal_equity: QuoteAmount::new(initial)?,
        total_return: Decimal::ZERO,
        max_drawdown: Weight::new(Decimal::ZERO)?,
    }];
    for asset in &spec.assets {
        let bars = crate::engine::observations_for_range(
            datasets,
            &asset.market,
            plan.spec.execution_resolution,
            None,
        )?;
        let first_open = bars
            .iter()
            .find(|bar| bar.candle.open_time_utc >= plan.spec.range.start())
            .map(|bar| bar.candle.open.get())
            .ok_or_else(|| LabError::DataGap("benchmark first open missing".into()))?;
        let terminal_close = bars
            .iter()
            .rev()
            .find(|bar| bar.candle.close_time_utc == plan.spec.range.end())
            .map(|bar| bar.candle.close.get())
            .ok_or_else(|| LabError::DataGap("benchmark terminal close missing".into()))?;
        let qty = checked_div(initial, first_open, "benchmark quantity")?;
        let terminal = checked_mul(qty, terminal_close, "benchmark terminal")?;
        let peak = bars
            .iter()
            .map(|bar| checked_mul(qty, bar.candle.high.get(), "benchmark peak"))
            .collect::<Result<Vec<_>, LabError>>()?
            .into_iter()
            .fold(Decimal::ZERO, Decimal::max);
        let drawdown = if peak.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(
                checked_sub(peak, terminal.min(peak), "benchmark drawdown")?,
                peak,
                "benchmark drawdown",
            )?
        };
        results.push(BenchmarkResult {
            kind: BenchmarkKind::BuyAndHold,
            market: Some(asset.market.clone()),
            terminal_equity: QuoteAmount::new(terminal)?,
            total_return: checked_div(
                checked_sub(terminal, initial, "benchmark return")?,
                initial,
                "benchmark return",
            )?,
            max_drawdown: Weight::new(drawdown)?,
        });
    }
    let asset_count = u64::try_from(spec.assets.len())
        .map_err(|_| LabError::ResourceLimit("asset count overflow".into()))?;
    if asset_count > 0 {
        let per_asset = checked_div(initial, Decimal::from(asset_count), "static split")?;
        let mut terminal = Decimal::ZERO;
        let mut peak = Decimal::ZERO;
        for asset in &spec.assets {
            let bars = crate::engine::observations_for_range(
                datasets,
                &asset.market,
                plan.spec.execution_resolution,
                None,
            )?;
            let first_open = bars
                .iter()
                .find(|bar| bar.candle.open_time_utc >= plan.spec.range.start())
                .map(|bar| bar.candle.open.get())
                .ok_or_else(|| LabError::DataGap("static benchmark open missing".into()))?;
            let close = bars
                .iter()
                .rev()
                .find(|bar| bar.candle.close_time_utc == plan.spec.range.end())
                .map(|bar| bar.candle.close.get())
                .ok_or_else(|| LabError::DataGap("static benchmark close missing".into()))?;
            let qty = checked_div(per_asset, first_open, "static quantity")?;
            terminal = checked_add(terminal, checked_mul(qty, close, "static terminal")?, "sum")?;
            peak = checked_add(
                peak,
                checked_mul(
                    qty,
                    bars.iter()
                        .map(|bar| bar.candle.high.get())
                        .fold(Decimal::ZERO, Decimal::max),
                    "static peak",
                )?,
                "sum",
            )?;
        }
        results.push(BenchmarkResult {
            kind: BenchmarkKind::StaticAllocation,
            market: None,
            terminal_equity: QuoteAmount::new(terminal)?,
            total_return: checked_div(
                checked_sub(terminal, initial, "static return")?,
                initial,
                "static return",
            )?,
            max_drawdown: Weight::new(if peak.is_zero() {
                Decimal::ZERO
            } else {
                checked_div(
                    checked_sub(peak, terminal.min(peak), "static drawdown")?,
                    peak,
                    "static drawdown",
                )?
            })?,
        });
    }
    Ok(results)
}
