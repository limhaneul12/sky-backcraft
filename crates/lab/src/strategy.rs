//! Pure causal strategy evaluation over completed candle observations.
//!
//! Indicator floats are confined to this module and rejected when nonfinite.
//! Every financial target crosses the checked exact [`Weight`] boundary.

pub(crate) mod indicators;

use crate::contracts::{
    AssetQuantity, CandleInterval, CandleObservation, CompareOp, EvidenceDecisionEffect,
    FinitePolicyNumber, INDICATOR_VERSION, IndicatorSnapshot, LabError, MarketId,
    PolicyConditionEvidence, PolicyConditionOutcome, PolicyConditionReason, PolicyConditionRole,
    PolicyConditionScope, PolicyConditionTrace, PositionState, PriceKrw, ReasonCode,
    StateParameters, StrategyKind, StrategySpec, UtcTimestamp, VerifiedProviderCoverage, Weight,
};
use crate::evidence::EvidenceEvaluator;
use indicators::{Ema, RollingExtreme, RollingVol, Rsi, finite, finite_positive};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Actual filled holding context supplied by execution/accounting.
///
/// Strategy evaluation never converts an unfilled signal into inventory. S3's
/// holding timeout therefore uses only `held_decision_bars` from a real episode.
#[derive(Debug, Clone, Copy)]
pub struct PositionView {
    pub actual_qty: AssetQuantity,
    pub actual_weight: Weight,
    pub episode_opened_at: Option<UtcTimestamp>,
    pub held_decision_bars: Option<u64>,
}

impl PositionView {
    pub(crate) fn state(self, decision_time: UtcTimestamp) -> Result<PositionState, LabError> {
        let holding = self.actual_qty.get() > Decimal::ZERO;
        if holding {
            if self.actual_weight.get() == Decimal::ZERO
                || self.episode_opened_at.is_none()
                || self.held_decision_bars.is_none()
            {
                return Err(LabError::InvalidConfig(
                    "filled position requires positive actual weight and episode context".into(),
                ));
            }
            if self
                .episode_opened_at
                .is_some_and(|opened| opened > decision_time)
            {
                return Err(LabError::InvalidConfig(
                    "episode opens after decision time".into(),
                ));
            }
            Ok(PositionState::Long)
        } else {
            if self.actual_weight.get() != Decimal::ZERO
                || self.episode_opened_at.is_some()
                || self.held_decision_bars.is_some()
            {
                return Err(LabError::InvalidConfig(
                    "cash position cannot carry weight or episode context".into(),
                ));
            }
            Ok(PositionState::Cash)
        }
    }
}

/// Complete pure output for one post-warmup decision bar.
#[derive(Debug, Clone)]
pub struct StrategyEvaluation {
    pub decision_time: UtcTimestamp,
    pub state_before: PositionState,
    pub state_after: PositionState,
    pub raw_target_weight: Weight,
    pub constrained_target_weight: Weight,
    pub reasons: Vec<ReasonCode>,
    pub indicators: IndicatorSnapshot,
    pub evidence_effect: Option<EvidenceDecisionEffect>,
    pub policy_ref: Option<crate::contracts::PolicyRevisionRef>,
    pub policy_trace: Option<crate::contracts::PolicyTrace>,
    /// Actual branch-local conditions evaluated by builtin strategies.
    /// Rules policies continue to use `policy_trace.condition_trace`.
    pub(crate) decision_trace: Option<Vec<PolicyConditionTrace>>,
    pub(crate) base_state_before: Option<PositionState>,
    pub(crate) base_state_after: Option<PositionState>,
}

/// Stateful, prefix-causal evaluator. EMA, Wilder RSI, rolling sample
/// volatility, and Donchian extremes are updated incrementally per bar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyEvaluator {
    spec: StrategySpec,
    market: MarketId,
    interval: CandleInterval,
    warmup_bars: usize,
    completed_bars_seen: u64,
    last_close_time: Option<UtcTimestamp>,
    ema: Option<Ema>,
    volatility: Option<RollingVol>,
    rsi: Option<Rsi>,
    entry_extreme: Option<RollingExtreme>,
    exit_extreme: Option<RollingExtreme>,
    base_signal_state: PositionState,
}

impl StrategyEvaluator {
    /// Construct a validated evaluator for one market/decision interval.
    /// # Errors
    /// Rejects invalid strategy parameters.
    pub fn new(
        spec: StrategySpec,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        let warmup_bars = spec.warmup_bars()?;
        let (ema, volatility, rsi, entry_extreme, exit_extreme) = match &spec {
            StrategySpec::S1 { state }
            | StrategySpec::S4 { state, .. }
            | StrategySpec::S5 { state }
            | StrategySpec::S1CoverageControl { state } => (
                Some(Ema::new(state.ema_length)),
                Some(RollingVol::new(state.vol_length)),
                None,
                None,
                None,
            ),
            StrategySpec::S2 {
                entry_length,
                exit_length,
            } => (
                None,
                None,
                None,
                Some(RollingExtreme::new(*entry_length)),
                Some(RollingExtreme::new(*exit_length)),
            ),
            StrategySpec::S3 {
                trend_length,
                rsi_length,
                ..
            } => (
                Some(Ema::new(*trend_length)),
                None,
                Some(Rsi::new(*rsi_length)),
                None,
                None,
            ),
            StrategySpec::BuyAndHold => (None, None, None, None, None),
        };
        Ok(Self {
            spec,
            market,
            interval,
            warmup_bars,
            completed_bars_seen: 0,
            last_close_time: None,
            ema,
            volatility,
            rsi,
            entry_extreme,
            exit_extreme,
            base_signal_state: PositionState::Cash,
        })
    }

    pub(crate) const fn completed_bars_seen(&self) -> u64 {
        self.completed_bars_seen
    }

    /// Admit one completed bar and, after warmup, produce one deterministic decision.
    ///
    /// Warmup updates indicator state but uses no phantom holding state. Donchian
    /// thresholds are captured before the current bar is admitted.
    ///
    /// # Errors
    /// Rejects wrong-market/interval, incomplete/noncontiguous bars, inconsistent
    /// OHLC, nonfinite indicator arithmetic, invalid position context, and missing
    /// Evidence evaluator for S5/control evaluation.
    pub fn observe(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
    ) -> Result<Option<StrategyEvaluation>, LabError> {
        self.observe_with_provider_coverage(
            observation,
            position,
            evidence,
            &VerifiedProviderCoverage::empty(),
        )
    }

    /// Admit one actual completed bar when verified provider coverage accounts
    /// for any missing open times since the prior actual bar.
    ///
    /// Coverage exempts only the continuity check. Indicators and counters
    /// still advance once, using this actual observation only.
    ///
    /// # Errors
    /// Preserves all existing bar, market, OHLC, evidence and position guards.
    pub(crate) fn observe_with_provider_coverage(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<Option<StrategyEvaluation>, LabError> {
        self.validate_observation(observation, coverage)?;
        let candle = &observation.candle;
        let close = decimal_f64(candle.close.get(), "close")?;
        let high = decimal_f64(candle.high.get(), "high")?;
        let low = decimal_f64(candle.low.get(), "low")?;

        let entry_high = if let Some(extreme) = &mut self.entry_extreme {
            extreme.prior_then_update(high, low)?.0
        } else {
            None
        };
        let exit_low = if let Some(extreme) = &mut self.exit_extreme {
            extreme.prior_then_update(high, low)?.1
        } else {
            None
        };
        let ema = self
            .ema
            .as_mut()
            .map(|state| state.update(close))
            .transpose()?
            .flatten();
        let bar_sigma = self
            .volatility
            .as_mut()
            .map(|state| state.update(close))
            .transpose()?
            .flatten();
        let rsi = self
            .rsi
            .as_mut()
            .map(|state| state.update(close))
            .transpose()?
            .flatten();
        let annual_vol = bar_sigma.map(|sigma| self.annualize(sigma)).transpose()?;
        self.completed_bars_seen += 1;
        self.last_close_time = Some(candle.close_time_utc);

        let indicators = IndicatorSnapshot {
            version: INDICATOR_VERSION.into(),
            ema,
            bar_sigma,
            annual_vol,
            rsi,
            entry_high: entry_high.map(price_from_f64).transpose()?,
            exit_low: exit_low.map(price_from_f64).transpose()?,
            completed_bars_seen: self.completed_bars_seen,
            policy_values: Vec::new(),
        };
        if self.completed_bars_seen < self.warmup_bars as u64 {
            return Ok(None);
        }

        let actual_state = position.state(candle.close_time_utc)?;
        let mut decision = self.evaluate(
            close,
            actual_state,
            position,
            &indicators,
            evidence,
            candle.close_time_utc,
        )?;
        if decision.reasons.is_empty() {
            decision.reasons.push(ReasonCode::HoldState);
            decision.reasons.push(ReasonCode::NoAction);
        }
        Ok(Some(decision))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive strategy transition keeps state and evidence overlays in their evaluation order"
    )]
    fn evaluate(
        &mut self,
        close: f64,
        actual_state: PositionState,
        position: PositionView,
        indicators: &IndicatorSnapshot,
        evidence: Option<&EvidenceEvaluator>,
        decision_time: UtcTimestamp,
    ) -> Result<StrategyEvaluation, LabError> {
        let base_before = self.base_signal_state;
        let mut decision_state_before = actual_state;
        let strategy_kind = self.spec.kind();
        let mut decision_trace = BuiltinConditionRecorder::new(strategy_kind);
        let (state_after, raw_target, constrained_target, mut reasons) = match &self.spec {
            StrategySpec::S1 { state } => {
                decision_state_before = base_before;
                let decision =
                    state_decision(close, base_before, state, indicators, &mut decision_trace)?;
                self.base_signal_state = decision.0;
                decision
            }
            StrategySpec::S2 { .. } => {
                donchian_decision(close, actual_state, indicators, &mut decision_trace)?
            }
            StrategySpec::S3 { .. } => trend_pullback_decision(
                close,
                actual_state,
                position,
                indicators,
                &self.spec,
                &mut decision_trace,
            )?,
            StrategySpec::S4 {
                state,
                target_annual_vol,
                vol_floor,
                rebalance_band,
            } => {
                let (state_after, _, _, reasons) =
                    state_decision(close, base_before, state, indicators, &mut decision_trace)?;
                decision_state_before = base_before;
                self.base_signal_state = state_after;
                let annual_vol = indicators.annual_vol.ok_or_else(missing_indicator)?;
                let target = if state_after == PositionState::Cash {
                    zero_weight()?
                } else {
                    weight_from_f64((target_annual_vol / annual_vol.max(*vol_floor)).min(1.0))?
                };
                let difference = (target.get() - position.actual_weight.get()).abs();
                let rebalance_allowed = difference <= rebalance_band.get();
                decision_trace.weight_band(
                    "builtin:rebalance:target_distance_within_band",
                    target,
                    position.actual_weight,
                    Weight::new(difference)?,
                    *rebalance_band,
                    rebalance_allowed,
                );
                if rebalance_allowed {
                    let mut reasons = reasons;
                    reasons.push(ReasonCode::RebalanceBand);
                    reasons.push(ReasonCode::NoAction);
                    (state_after, target, position.actual_weight, reasons)
                } else {
                    (state_after, target, target, reasons)
                }
            }
            StrategySpec::S5 { state } | StrategySpec::S1CoverageControl { state } => {
                decision_state_before = base_before;
                let (base_state_after, base_target, _, mut reasons) =
                    state_decision(close, base_before, state, indicators, &mut decision_trace)?;
                self.base_signal_state = base_state_after;
                let evaluator = evidence.ok_or_else(|| {
                    LabError::BlockedEvidence("S5 evaluation requires an EvidenceEvaluator".into())
                })?;
                let coverage_control = matches!(self.spec, StrategySpec::S1CoverageControl { .. });
                let effect =
                    evaluator.effect(&self.market, decision_time, base_target, coverage_control)?;
                reasons.push(if effect.coverage_available {
                    ReasonCode::EvidenceOverlay
                } else {
                    ReasonCode::EvidenceUnavailable
                });
                let state_after = state_from_weight(effect.final_target);
                return Ok(StrategyEvaluation {
                    decision_time,
                    state_before: decision_state_before,
                    state_after,
                    raw_target_weight: base_target,
                    constrained_target_weight: effect.final_target,
                    reasons,
                    indicators: indicators.clone(),
                    evidence_effect: Some(effect),
                    policy_ref: None,
                    policy_trace: None,
                    decision_trace: Some(decision_trace.into_rows()),
                    base_state_before: Some(base_before),
                    base_state_after: Some(base_state_after),
                });
            }
            StrategySpec::BuyAndHold => {
                let starts_from_cash = actual_state == PositionState::Cash;
                decision_trace.position(
                    "builtin:entry:starts_from_cash",
                    PositionState::Cash,
                    actual_state,
                    starts_from_cash,
                );
                (
                    PositionState::Long,
                    one_weight()?,
                    one_weight()?,
                    if starts_from_cash {
                        vec![ReasonCode::StartFromCash]
                    } else {
                        Vec::new()
                    },
                )
            }
        };
        let target_matches_position = constrained_target == position.actual_weight;
        decision_trace.weight_comparison(
            "builtin:action:target_equals_actual_weight",
            constrained_target,
            CompareOp::Eq,
            position.actual_weight,
            target_matches_position,
        );
        if target_matches_position && reasons.is_empty() {
            reasons.extend([ReasonCode::HoldState, ReasonCode::NoAction]);
        }
        Ok(StrategyEvaluation {
            decision_time,
            state_before: decision_state_before,
            state_after,
            raw_target_weight: raw_target,
            constrained_target_weight: constrained_target,
            reasons,
            indicators: indicators.clone(),
            evidence_effect: None,
            policy_ref: None,
            policy_trace: None,
            decision_trace: Some(decision_trace.into_rows()),
            base_state_before: matches!(
                self.spec,
                StrategySpec::S1 { .. } | StrategySpec::S4 { .. }
            )
            .then_some(base_before),
            base_state_after: matches!(
                self.spec,
                StrategySpec::S1 { .. } | StrategySpec::S4 { .. }
            )
            .then_some(self.base_signal_state),
        })
    }

    pub(crate) fn signal_expiry_bars(&self) -> Option<u32> {
        match self.spec {
            StrategySpec::S3 {
                signal_expiry_bars, ..
            } => u32::try_from(signal_expiry_bars).ok(),
            _ => None,
        }
    }

    fn validate_observation(
        &self,
        observation: &CandleObservation,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<(), LabError> {
        let candle = &observation.candle;
        if candle.market != self.market.code() || candle.interval != self.interval {
            return Err(LabError::InvalidConfig(
                "strategy observation market/interval mismatch".into(),
            ));
        }
        if !candle.completed
            || candle.close_time_utc.0 - candle.open_time_utc.0 != self.interval.duration()
            || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
            || candle
                .open_time_utc
                .0
                .timestamp()
                .rem_euclid(self.interval.duration().num_seconds())
                != 0
        {
            return Err(LabError::DataGap(
                "strategy requires one complete aligned candle".into(),
            ));
        }
        if self.last_close_time.is_some_and(|previous| {
            !coverage.covers(&self.market, self.interval, previous, candle.open_time_utc)
        }) {
            return Err(LabError::DataGap(
                "strategy candle sequence is not contiguous".into(),
            ));
        }
        if candle.high < candle.open
            || candle.high < candle.close
            || candle.low > candle.open
            || candle.low > candle.close
            || candle.high < candle.low
        {
            return Err(LabError::DataCorrupt(
                "invalid strategy OHLC relation".into(),
            ));
        }
        Ok(())
    }

    fn annualize(&self, sigma: f64) -> Result<f64, LabError> {
        let interval_seconds =
            i32::try_from(self.interval.duration().num_seconds()).map_err(|_| {
                LabError::InvalidConfig("strategy interval seconds exceed i32 range".to_owned())
            })?;
        let bars_per_year = 365.25 * 24.0 * 60.0 * 60.0 / f64::from(interval_seconds);
        let annual = sigma * bars_per_year.sqrt();
        finite(annual, "annualized volatility")?;
        Ok(annual)
    }
}

fn state_decision(
    close: f64,
    state_before: PositionState,
    parameters: &StateParameters,
    indicators: &IndicatorSnapshot,
    trace: &mut BuiltinConditionRecorder,
) -> Result<(PositionState, Weight, Weight, Vec<ReasonCode>), LabError> {
    let ema = indicators.ema.ok_or_else(missing_indicator)?;
    let sigma = indicators.bar_sigma.ok_or_else(missing_indicator)?;
    let upper = ema * (1.0 + parameters.k * sigma);
    let lower = ema * (1.0 - parameters.k * sigma);
    finite(upper, "upper state band")?;
    finite(lower, "lower state band")?;
    let (state_after, reasons) = match state_before {
        PositionState::Cash => {
            let entered = close > upper;
            trace.comparison(
                "builtin:entry:close_gt_upper_band",
                close,
                CompareOp::Gt,
                upper,
                entered,
            )?;
            if entered {
                (PositionState::Long, vec![ReasonCode::BandEntry])
            } else {
                (PositionState::Cash, Vec::new())
            }
        }
        PositionState::Long => {
            let exited = close < lower;
            trace.comparison(
                "builtin:exit:close_lt_lower_band",
                close,
                CompareOp::Lt,
                lower,
                exited,
            )?;
            if exited {
                (PositionState::Cash, vec![ReasonCode::BandExit])
            } else {
                (PositionState::Long, Vec::new())
            }
        }
    };
    let target = weight_for_state(state_after)?;
    Ok((state_after, target, target, reasons))
}

fn donchian_decision(
    close: f64,
    state_before: PositionState,
    indicators: &IndicatorSnapshot,
    trace: &mut BuiltinConditionRecorder,
) -> Result<(PositionState, Weight, Weight, Vec<ReasonCode>), LabError> {
    let entry = decimal_f64(
        indicators.entry_high.ok_or_else(missing_indicator)?.get(),
        "entry high",
    )?;
    let exit = decimal_f64(
        indicators.exit_low.ok_or_else(missing_indicator)?.get(),
        "exit low",
    )?;
    let (state_after, reasons) = match state_before {
        PositionState::Cash => {
            let breakout = close > entry;
            trace.comparison(
                "builtin:entry:close_gt_prior_entry_high",
                close,
                CompareOp::Gt,
                entry,
                breakout,
            )?;
            if breakout {
                (PositionState::Long, vec![ReasonCode::Breakout])
            } else {
                (PositionState::Cash, Vec::new())
            }
        }
        PositionState::Long => {
            let breakdown = close < exit;
            trace.comparison(
                "builtin:exit:close_lt_prior_exit_low",
                close,
                CompareOp::Lt,
                exit,
                breakdown,
            )?;
            if breakdown {
                (PositionState::Cash, vec![ReasonCode::Breakdown])
            } else {
                (PositionState::Long, Vec::new())
            }
        }
    };
    let target = weight_for_state(state_after)?;
    Ok((state_after, target, target, reasons))
}

fn trend_pullback_decision(
    close: f64,
    state_before: PositionState,
    position: PositionView,
    indicators: &IndicatorSnapshot,
    strategy_spec: &StrategySpec,
    trace: &mut BuiltinConditionRecorder,
) -> Result<(PositionState, Weight, Weight, Vec<ReasonCode>), LabError> {
    let StrategySpec::S3 {
        entry_threshold,
        exit_threshold,
        max_holding_bars,
        ..
    } = strategy_spec
    else {
        return Err(LabError::Internal(
            "trend-pullback decision requires its frozen S3 specification".into(),
        ));
    };
    let ema = indicators.ema.ok_or_else(missing_indicator)?;
    let rsi = indicators.rsi.ok_or_else(missing_indicator)?;
    let mut reasons = Vec::new();
    let state_after = match state_before {
        PositionState::Cash => {
            let trend_confirmed = close > ema;
            trace.comparison(
                "builtin:entry:close_gt_ema",
                close,
                CompareOp::Gt,
                ema,
                trend_confirmed,
            )?;
            let pullback_confirmed = if trend_confirmed {
                let confirmed = rsi < *entry_threshold;
                trace.comparison(
                    "builtin:entry:rsi_lt_entry_threshold",
                    rsi,
                    CompareOp::Lt,
                    *entry_threshold,
                    confirmed,
                )?;
                confirmed
            } else {
                trace.not_evaluated(
                    "builtin:entry:rsi_lt_entry_threshold",
                    PolicyConditionReason::AndShortCircuit,
                );
                false
            };
            if trend_confirmed && pullback_confirmed {
                reasons.push(ReasonCode::TrendPullbackEntry);
                PositionState::Long
            } else {
                PositionState::Cash
            }
        }
        PositionState::Long => {
            let trend_invalidated = close < ema;
            trace.comparison(
                "builtin:exit:close_lt_ema",
                close,
                CompareOp::Lt,
                ema,
                trend_invalidated,
            )?;
            if trend_invalidated {
                reasons.push(ReasonCode::TrendInvalidated);
            }
            let rsi_recovered = rsi > *exit_threshold;
            trace.comparison(
                "builtin:exit:rsi_gt_exit_threshold",
                rsi,
                CompareOp::Gt,
                *exit_threshold,
                rsi_recovered,
            )?;
            if rsi_recovered {
                reasons.push(ReasonCode::RsiRecovery);
            }
            let holding_timed_out = position.held_decision_bars.is_some_and(|bars| {
                let limit = *max_holding_bars as u64;
                let timed_out = bars >= limit;
                trace.count_comparison(
                    "builtin:exit:held_bars_gte_maximum",
                    bars,
                    CompareOp::Gte,
                    limit,
                    timed_out,
                );
                timed_out
            });
            if holding_timed_out {
                reasons.push(ReasonCode::HoldingTimeout);
            }
            if reasons.is_empty() {
                PositionState::Long
            } else {
                PositionState::Cash
            }
        }
    };
    let target = weight_for_state(state_after)?;
    Ok((state_after, target, target, reasons))
}

fn builtin_condition_trace(
    strategy: StrategyKind,
    path: &str,
    result: bool,
    evidence: PolicyConditionEvidence,
) -> PolicyConditionTrace {
    PolicyConditionTrace {
        scope: PolicyConditionScope::BuiltinStrategy { strategy },
        ast_path: path.into(),
        role: PolicyConditionRole::Unspecified,
        outcome: if result {
            PolicyConditionOutcome::True
        } else {
            PolicyConditionOutcome::False
        },
        engine_result: Some(result),
        evidence: Some(evidence),
    }
}

struct BuiltinConditionRecorder {
    strategy: StrategyKind,
    rows: Vec<PolicyConditionTrace>,
}

impl BuiltinConditionRecorder {
    fn new(strategy: StrategyKind) -> Self {
        Self {
            strategy,
            rows: Vec::new(),
        }
    }

    fn into_rows(self) -> Vec<PolicyConditionTrace> {
        self.rows
    }

    fn comparison(
        &mut self,
        path: &str,
        left: f64,
        comparison: CompareOp,
        right: f64,
        result: bool,
    ) -> Result<(), LabError> {
        self.rows.push(builtin_condition_trace(
            self.strategy,
            path,
            result,
            PolicyConditionEvidence::Comparison {
                left: FinitePolicyNumber::new(left)?,
                comparison,
                right: FinitePolicyNumber::new(right)?,
            },
        ));
        Ok(())
    }

    fn count_comparison(
        &mut self,
        path: &str,
        left: u64,
        comparison: CompareOp,
        right: u64,
        result: bool,
    ) {
        self.rows.push(builtin_condition_trace(
            self.strategy,
            path,
            result,
            PolicyConditionEvidence::CountComparison {
                left,
                comparison,
                right,
            },
        ));
    }

    fn position(
        &mut self,
        path: &str,
        expected: PositionState,
        actual: PositionState,
        result: bool,
    ) {
        self.rows.push(builtin_condition_trace(
            self.strategy,
            path,
            result,
            PolicyConditionEvidence::Position { expected, actual },
        ));
    }

    fn weight_band(
        &mut self,
        path: &str,
        target: Weight,
        actual: Weight,
        difference: Weight,
        band: Weight,
        result: bool,
    ) {
        self.rows.push(builtin_condition_trace(
            self.strategy,
            path,
            result,
            PolicyConditionEvidence::WeightBand {
                target,
                actual,
                difference,
                band,
            },
        ));
    }

    fn weight_comparison(
        &mut self,
        path: &str,
        left: Weight,
        comparison: CompareOp,
        right: Weight,
        result: bool,
    ) {
        self.rows.push(builtin_condition_trace(
            self.strategy,
            path,
            result,
            PolicyConditionEvidence::WeightComparison {
                left,
                comparison,
                right,
            },
        ));
    }

    fn not_evaluated(&mut self, path: &str, reason: PolicyConditionReason) {
        self.rows.push(PolicyConditionTrace {
            scope: PolicyConditionScope::BuiltinStrategy {
                strategy: self.strategy,
            },
            ast_path: path.into(),
            role: PolicyConditionRole::Unspecified,
            outcome: PolicyConditionOutcome::NotEvaluated(reason),
            engine_result: None,
            evidence: None,
        });
    }
}

fn missing_indicator() -> LabError {
    LabError::InsufficientWarmup("required strategy indicator is not initialized".into())
}

fn state_from_weight(weight: Weight) -> PositionState {
    if weight.get() == Decimal::ZERO {
        PositionState::Cash
    } else {
        PositionState::Long
    }
}

fn weight_for_state(state: PositionState) -> Result<Weight, LabError> {
    match state {
        PositionState::Cash => zero_weight(),
        PositionState::Long => one_weight(),
    }
}

fn zero_weight() -> Result<Weight, LabError> {
    Weight::new(Decimal::ZERO)
}
fn one_weight() -> Result<Weight, LabError> {
    Weight::new(Decimal::ONE)
}

fn weight_from_f64(value: f64) -> Result<Weight, LabError> {
    finite(value, "target weight")?;
    let decimal = Decimal::from_str(&value.to_string())
        .map_err(|error| LabError::InvalidConfig(format!("target weight conversion: {error}")))?;
    Weight::new(decimal)
}

fn price_from_f64(value: f64) -> Result<PriceKrw, LabError> {
    finite_positive(value, "indicator price")?;
    let decimal = Decimal::from_str(&value.to_string())
        .map_err(|error| LabError::DataCorrupt(format!("indicator price conversion: {error}")))?;
    PriceKrw::new(decimal)
}

pub(crate) fn decimal_f64(value: Decimal, label: &str) -> Result<f64, LabError> {
    let value = value.to_f64().ok_or_else(|| {
        LabError::DataCorrupt(format!("{label} cannot be represented as finite f64"))
    })?;
    finite_positive(value, label)?;
    Ok(value)
}

#[cfg(test)]
mod tests;
