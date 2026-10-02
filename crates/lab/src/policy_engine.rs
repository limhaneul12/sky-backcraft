//! Bounded deterministic evaluator for frozen custom policy rule programs.

#[cfg(test)]
mod tests;

use crate::contracts::policy::{
    BoolExpr, CompareOp, NamedPolicyValue, NumericExpr, PolicyDefinition, PolicyEvidenceTrace,
    PolicyIndicator, PolicyIndicatorKind, PolicyProgram, PolicyTarget, PolicyTrace, RulesProgram,
};
use crate::contracts::{
    CandleInterval, CandleObservation, IndicatorSnapshot, LabError, MarketId, PositionState,
    ReasonCode, ResolvedPlan, StrategyBinding, UtcTimestamp, Weight,
};
use crate::evidence::EvidenceEvaluator;
use crate::strategy::indicators::{Ema, RollingExtreme, RollingVol, Rsi, Sma, finite};
use crate::strategy::{PositionView, StrategyEvaluation, StrategyEvaluator, decimal_f64};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub(crate) struct RuleEvaluation {
    pub(crate) decision_time: UtcTimestamp,
    pub(crate) target: Weight,
    pub(crate) trace: PolicyTrace,
    pub(crate) indicator_values: Vec<NamedPolicyValue>,
    pub(crate) completed_bars: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PolicyEvaluator {
    binding: StrategyBinding,
    inner: CompiledPolicy,
}

#[derive(Debug, Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "one inline evaluator per active model avoids a heap allocation; no collection of this enum"
)]
enum CompiledPolicy {
    Builtin(StrategyEvaluator),
    Rules(RulesEvaluator),
}

impl PolicyEvaluator {
    pub(crate) fn compile(
        binding: StrategyBinding,
        plan: &ResolvedPlan,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        let program = binding.program(plan)?;
        let inner = match program {
            PolicyProgram::Builtin { strategy } => {
                CompiledPolicy::Builtin(StrategyEvaluator::new(strategy, market, interval)?)
            }
            PolicyProgram::Rules { .. } => {
                let reference = binding.policy_ref().ok_or_else(|| {
                    LabError::InputHashMismatch("RULES policy lacks a frozen revision".into())
                })?;
                let definition = plan
                    .policy_revisions
                    .iter()
                    .find(|revision| revision.reference == *reference)
                    .map(|revision| &revision.definition)
                    .ok_or_else(|| {
                        LabError::InputHashMismatch("RULES policy revision is not frozen".into())
                    })?;
                CompiledPolicy::Rules(RulesEvaluator::new(definition, market, interval)?)
            }
        };
        Ok(Self { binding, inner })
    }

    pub(crate) fn observe(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
    ) -> Result<Option<StrategyEvaluation>, LabError> {
        let policy_ref = self.binding.policy_ref().cloned();
        match &mut self.inner {
            CompiledPolicy::Builtin(evaluator) => {
                let mut evaluation = evaluator.observe(observation, position, evidence)?;
                if let Some(evaluation) = &mut evaluation
                    && let Some(reference) = policy_ref
                {
                    evaluation.policy_ref = Some(reference);
                    evaluation.policy_trace = Some(builtin_trace(evaluation)?);
                }
                Ok(evaluation)
            }
            CompiledPolicy::Rules(evaluator) => evaluator
                .observe(observation, position, evidence)?
                .map(|evaluation| rule_strategy_evaluation(policy_ref, evaluation, position))
                .transpose(),
        }
    }

    pub(crate) fn signal_expiry(
        &self,
        decision_time: UtcTimestamp,
        interval: CandleInterval,
        range_end: UtcTimestamp,
    ) -> Result<UtcTimestamp, LabError> {
        let bars = match &self.inner {
            CompiledPolicy::Builtin(evaluator) => evaluator.signal_expiry_bars(),
            CompiledPolicy::Rules(evaluator) => evaluator.signal_expiry_bars(),
        };
        let Some(bars) = bars else {
            return Ok(range_end);
        };
        let seconds = interval
            .duration()
            .num_seconds()
            .checked_mul(i64::from(bars))
            .ok_or_else(|| LabError::InvalidConfig("policy signal expiry overflow".into()))?;
        let expiry = decision_time
            .0
            .checked_add_signed(chrono::Duration::seconds(seconds))
            .map(UtcTimestamp)
            .ok_or_else(|| {
                LabError::InvalidConfig("policy signal expiry timestamp overflow".into())
            })?;
        Ok(expiry.min(range_end))
    }
}

fn rule_strategy_evaluation(
    policy_ref: Option<crate::contracts::PolicyRevisionRef>,
    evaluation: RuleEvaluation,
    position: PositionView,
) -> Result<StrategyEvaluation, LabError> {
    let state_before = position.state(evaluation.decision_time)?;
    let state_after = if evaluation.target.get().is_zero() {
        PositionState::Cash
    } else {
        PositionState::Long
    };
    let reason = if evaluation.trace.matched_rule_id.is_some() {
        ReasonCode::PolicyRule
    } else {
        ReasonCode::PolicyFallback
    };
    Ok(StrategyEvaluation {
        decision_time: evaluation.decision_time,
        state_before,
        state_after,
        raw_target_weight: evaluation.target,
        constrained_target_weight: evaluation.target,
        reasons: vec![reason],
        indicators: IndicatorSnapshot {
            version: "policy-rules-v1".into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: evaluation.completed_bars,
            policy_values: evaluation.indicator_values,
        },
        evidence_effect: None,
        policy_ref,
        policy_trace: Some(evaluation.trace),
        base_state_before: None,
        base_state_after: None,
    })
}

fn builtin_trace(evaluation: &StrategyEvaluation) -> Result<PolicyTrace, LabError> {
    let value = |state| match state {
        PositionState::Cash => 0.0,
        PositionState::Long => 1.0,
    };
    let evidence = if let Some(effect) = &evaluation.evidence_effect {
        let multiplier = effect
            .eligible
            .iter()
            .map(|eligible| eligible.multiplier)
            .min()
            .unwrap_or(Weight::new(Decimal::ZERO)?);
        Some(PolicyEvidenceTrace {
            policy: effect.policy,
            snapshot_id: effect.snapshot_id.clone(),
            used_at: effect.used_at,
            eligible: effect.eligible.clone(),
            coverage_available: effect.coverage_available,
            multiplier,
        })
    } else {
        None
    };
    let state_values = |state: Option<PositionState>| {
        state
            .map(|state| NamedPolicyValue {
                id: "base_signal_state".into(),
                value: value(state),
            })
            .into_iter()
            .collect()
    };
    Ok(PolicyTrace {
        matched_rule_id: None,
        state_before: state_values(evaluation.base_state_before),
        state_after: state_values(evaluation.base_state_after),
        evidence,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct RulesEvaluator {
    program: RulesProgram,
    market: MarketId,
    interval: CandleInterval,
    warmup_bars: usize,
    completed_bars: u64,
    last_close: Option<UtcTimestamp>,
    indicators: Vec<IndicatorRuntime>,
    state: BTreeMap<String, f64>,
    crosses: BTreeMap<String, CrossSample>,
    requires_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
struct CrossSample {
    previous: Option<(f64, f64)>,
    current: (f64, f64),
}

#[derive(Debug, Clone)]
struct EvalContext<'a> {
    indicators: &'a BTreeMap<String, f64>,
    state: &'a BTreeMap<String, f64>,
    actual_state: PositionState,
    actual_weight: f64,
    held_bars: f64,
    evidence_multiplier: f64,
    evidence_coverage: f64,
    crosses: &'a BTreeMap<String, CrossSample>,
}

#[derive(Debug, Clone)]
struct IndicatorRuntime {
    id: String,
    kind: IndicatorState,
}

#[derive(Debug, Clone, Copy)]
struct CandlePrices {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Debug, Clone)]
enum IndicatorState {
    Open,
    High,
    Low,
    Close,
    Volume,
    QuoteTurnover,
    Sma(Sma),
    Ema(Ema),
    Rsi(Rsi),
    SampleVol(RollingVol),
    AnnualVol(RollingVol),
    PriorHigh(RollingExtreme),
    PriorLow(RollingExtreme),
}

impl RulesEvaluator {
    pub(crate) fn new(
        definition: &PolicyDefinition,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        let warmup_bars = definition.warmup_bars()?;
        let PolicyProgram::Rules { program } = &definition.program else {
            return Err(LabError::InvalidConfig(
                "rules evaluator requires a RULES policy definition".into(),
            ));
        };
        let indicators = program
            .indicators
            .iter()
            .map(IndicatorRuntime::new)
            .collect::<Vec<_>>();
        let state = program
            .states
            .iter()
            .map(|slot| (slot.id.clone(), slot.initial))
            .collect();
        Ok(Self {
            program: program.clone(),
            market,
            interval,
            warmup_bars,
            completed_bars: 0,
            last_close: None,
            indicators,
            state,
            crosses: BTreeMap::new(),
            requires_evidence: definition.requires_evidence(),
        })
    }

    pub(crate) fn observe(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
    ) -> Result<Option<RuleEvaluation>, LabError> {
        self.validate_observation(observation)?;
        let mut values = BTreeMap::new();
        if !self.indicators.is_empty() {
            let prices = CandlePrices::from_observation(observation)?;
            for indicator in &mut self.indicators {
                let value = indicator.update(observation, &prices, self.interval)?;
                if let Some(value) = value {
                    values.insert(indicator.id.clone(), value);
                }
            }
        }
        self.completed_bars = self.completed_bars.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("policy completed-bar counter overflow".into())
        })?;
        self.last_close = Some(observation.candle.close_time_utc);
        if values.len() != self.indicators.len() {
            return Ok(None);
        }

        let (multiplier, coverage, evidence_trace) =
            self.evidence_values(observation.candle.close_time_utc, evidence)?;
        let actual_state = position.state(observation.candle.close_time_utc)?;
        let actual_weight =
            nonnegative_decimal_f64(position.actual_weight.get(), "policy actual weight")?;
        let held_bars = u32::try_from(position.held_decision_bars.unwrap_or(0))
            .map(f64::from)
            .map_err(|_| LabError::ResourceLimit("policy held bars exceed u32".into()))?;
        finite(held_bars, "policy held bars")?;

        let before = self.state.clone();
        let base = EvalContext {
            indicators: &values,
            state: &before,
            actual_state,
            actual_weight,
            held_bars,
            evidence_multiplier: multiplier,
            evidence_coverage: coverage,
            crosses: &self.crosses,
        };
        let next_crosses = collect_all_crosses(&self.program, &base, &self.crosses)?;
        let context = EvalContext {
            crosses: &next_crosses,
            ..base
        };
        let mut after = BTreeMap::new();
        for slot in &self.program.states {
            let value = eval_numeric(&slot.next, &context, &format!("state:{}:next", slot.id))?;
            finite(value, "policy next state")?;
            after.insert(slot.id.clone(), value);
        }

        let mut matched_rule_id = None;
        let mut selected = None;
        for rule in &self.program.rules {
            let path = format!("rule:{}", rule.id);
            if eval_bool(&rule.condition, &context, &format!("{path}:when"))? {
                matched_rule_id = Some(rule.id.clone());
                selected = Some(eval_target(
                    &rule.target,
                    &context,
                    &format!("{path}:target"),
                )?);
                break;
            }
        }
        let target_value = match selected {
            Some(value) => value,
            None => eval_target(&self.program.fallback, &context, "fallback")?,
        };
        finite(target_value, "policy target")?;
        let target_decimal = Decimal::from_str(&target_value.to_string()).map_err(|error| {
            LabError::InvalidConfig(format!("policy target conversion: {error}"))
        })?;
        let target = Weight::new(target_decimal)?;
        if self.completed_bars < self.warmup_bars as u64 {
            self.state = after;
            self.crosses = next_crosses;
            return Ok(None);
        }
        let state_after = named_values(&after);
        self.state = after;
        self.crosses = next_crosses;
        Ok(Some(RuleEvaluation {
            decision_time: observation.candle.close_time_utc,
            target,
            trace: PolicyTrace {
                matched_rule_id,
                state_before: named_values(&before),
                state_after,
                evidence: evidence_trace,
            },
            indicator_values: named_values(&values),
            completed_bars: self.completed_bars,
        }))
    }

    fn signal_expiry_bars(&self) -> Option<u32> {
        match self.program.signal_expiry {
            crate::contracts::policy::PolicySignalExpiry::EndOfRange => None,
            crate::contracts::policy::PolicySignalExpiry::DecisionBars { bars } => Some(bars),
        }
    }

    fn evidence_values(
        &self,
        at: UtcTimestamp,
        evidence: Option<&EvidenceEvaluator>,
    ) -> Result<(f64, f64, Option<PolicyEvidenceTrace>), LabError> {
        if !self.requires_evidence {
            return Ok((0.0, 0.0, None));
        }
        let evaluator = evidence.ok_or_else(|| {
            LabError::BlockedEvidence("custom policy requires frozen Evidence input".into())
        })?;
        let one = Weight::new(Decimal::ONE)?;
        let effect = evaluator.effect(&self.market, at, one, false)?;
        let multiplier = nonnegative_decimal_f64(effect.final_target.get(), "evidence multiplier")?;
        let coverage = if effect.coverage_available { 1.0 } else { 0.0 };
        Ok((
            multiplier,
            coverage,
            Some(PolicyEvidenceTrace {
                policy: effect.policy,
                snapshot_id: effect.snapshot_id,
                used_at: effect.used_at,
                eligible: effect.eligible,
                coverage_available: effect.coverage_available,
                multiplier: effect.final_target,
            }),
        ))
    }

    fn validate_observation(&self, observation: &CandleObservation) -> Result<(), LabError> {
        let candle = &observation.candle;
        if candle.market != self.market.code()
            || candle.interval != self.interval
            || !candle.completed
            || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
            || candle.close_time_utc.0.timestamp_subsec_nanos() != 0
            || candle
                .open_time_utc
                .0
                .timestamp()
                .rem_euclid(self.interval.duration().num_seconds())
                != 0
            || candle.close_time_utc.0 - candle.open_time_utc.0 != self.interval.duration()
            || self
                .last_close
                .is_some_and(|previous| previous != candle.open_time_utc)
        {
            return Err(LabError::DataGap(
                "policy requires contiguous completed bars for its market and interval".into(),
            ));
        }
        if candle.high < candle.open
            || candle.high < candle.close
            || candle.low > candle.open
            || candle.low > candle.close
            || candle.high < candle.low
        {
            return Err(LabError::DataCorrupt("invalid policy OHLC relation".into()));
        }
        Ok(())
    }
}

impl IndicatorRuntime {
    fn new(spec: &PolicyIndicator) -> Self {
        let kind = match &spec.indicator {
            PolicyIndicatorKind::Open => IndicatorState::Open,
            PolicyIndicatorKind::High => IndicatorState::High,
            PolicyIndicatorKind::Low => IndicatorState::Low,
            PolicyIndicatorKind::Close => IndicatorState::Close,
            PolicyIndicatorKind::Volume => IndicatorState::Volume,
            PolicyIndicatorKind::QuoteTurnover => IndicatorState::QuoteTurnover,
            PolicyIndicatorKind::Sma { window } => IndicatorState::Sma(Sma::new(*window)),
            PolicyIndicatorKind::Ema { window } => IndicatorState::Ema(Ema::new(*window)),
            PolicyIndicatorKind::Rsi { window } => IndicatorState::Rsi(Rsi::new(*window)),
            PolicyIndicatorKind::SampleVol { window } => {
                IndicatorState::SampleVol(RollingVol::new(*window))
            }
            PolicyIndicatorKind::AnnualVol { window } => {
                IndicatorState::AnnualVol(RollingVol::new(*window))
            }
            PolicyIndicatorKind::PriorHigh { window } => {
                IndicatorState::PriorHigh(RollingExtreme::new(*window))
            }
            PolicyIndicatorKind::PriorLow { window } => {
                IndicatorState::PriorLow(RollingExtreme::new(*window))
            }
        };
        Self {
            id: spec.id.clone(),
            kind,
        }
    }

    fn update(
        &mut self,
        observation: &CandleObservation,
        prices: &CandlePrices,
        interval: CandleInterval,
    ) -> Result<Option<f64>, LabError> {
        let candle = &observation.candle;
        match &mut self.kind {
            IndicatorState::Open => Ok(Some(prices.open)),
            IndicatorState::High => Ok(Some(prices.high)),
            IndicatorState::Low => Ok(Some(prices.low)),
            IndicatorState::Close => Ok(Some(prices.close)),
            IndicatorState::Volume => Ok(Some(nonnegative_decimal_f64(
                candle.volume.get(),
                "policy volume",
            )?)),
            IndicatorState::QuoteTurnover => Ok(Some(nonnegative_decimal_f64(
                candle.quote_turnover.get(),
                "policy quote turnover",
            )?)),
            IndicatorState::Sma(state) => state.update(prices.close),
            IndicatorState::Ema(state) => state.update(prices.close),
            IndicatorState::Rsi(state) => state.update(prices.close),
            IndicatorState::SampleVol(state) => state.update(prices.close),
            IndicatorState::AnnualVol(state) => state
                .update(prices.close)?
                .map(|sigma| annualize(sigma, interval))
                .transpose(),
            IndicatorState::PriorHigh(state) => {
                Ok(state.prior_then_update(prices.high, prices.low)?.0)
            }
            IndicatorState::PriorLow(state) => {
                Ok(state.prior_then_update(prices.high, prices.low)?.1)
            }
        }
    }
}

impl CandlePrices {
    fn from_observation(observation: &CandleObservation) -> Result<Self, LabError> {
        let candle = &observation.candle;
        Ok(Self {
            open: decimal_f64(candle.open.get(), "policy open")?,
            high: decimal_f64(candle.high.get(), "policy high")?,
            low: decimal_f64(candle.low.get(), "policy low")?,
            close: decimal_f64(candle.close.get(), "policy close")?,
        })
    }
}

fn nonnegative_decimal_f64(value: Decimal, label: &str) -> Result<f64, LabError> {
    let value = value.to_f64().ok_or_else(|| {
        LabError::DataCorrupt(format!("{label} cannot be represented as finite f64"))
    })?;
    finite(value, label)?;
    if value < 0.0 {
        return Err(LabError::DataCorrupt(format!(
            "{label} must be nonnegative"
        )));
    }
    Ok(value)
}

fn annualize(sigma: f64, interval: CandleInterval) -> Result<f64, LabError> {
    let seconds = i32::try_from(interval.duration().num_seconds())
        .map_err(|_| LabError::InvalidConfig("policy interval exceeds i32 seconds".into()))?;
    let result = sigma * (365.25 * 24.0 * 60.0 * 60.0 / f64::from(seconds)).sqrt();
    finite(result, "policy annual volatility")?;
    Ok(result)
}

fn named_values(values: &BTreeMap<String, f64>) -> Vec<NamedPolicyValue> {
    values
        .iter()
        .map(|(id, value)| NamedPolicyValue {
            id: id.clone(),
            value: *value,
        })
        .collect()
}

fn eval_target(
    target: &PolicyTarget,
    context: &EvalContext<'_>,
    path: &str,
) -> Result<f64, LabError> {
    match target {
        PolicyTarget::Hold => Ok(context.actual_weight),
        PolicyTarget::Weight { value } => eval_numeric(value, context, path),
    }
}

fn eval_numeric(
    expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
) -> Result<f64, LabError> {
    let value = match expr {
        NumericExpr::Constant { value } => *value,
        NumericExpr::Indicator { id } => *context.indicators.get(id).ok_or_else(|| {
            LabError::InsufficientWarmup(format!("policy indicator is not ready: {id}"))
        })?,
        NumericExpr::State { id } => *context
            .state
            .get(id)
            .ok_or_else(|| LabError::InvalidConfig(format!("policy state is absent: {id}")))?,
        NumericExpr::ActualWeight => context.actual_weight,
        NumericExpr::HeldBars => context.held_bars,
        NumericExpr::EvidenceMultiplier => context.evidence_multiplier,
        NumericExpr::EvidenceCoverage => context.evidence_coverage,
        NumericExpr::Add { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                + eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Sub { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                - eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Mul { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                * eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Div { left, right } => {
            let numerator = eval_numeric(left, context, &format!("{path}:left"))?;
            let denominator = eval_numeric(right, context, &format!("{path}:right"))?;
            if denominator == 0.0 {
                return Err(LabError::InvalidConfig("policy division by zero".into()));
            }
            numerator / denominator
        }
        NumericExpr::Min { left, right } => eval_numeric(left, context, &format!("{path}:left"))?
            .min(eval_numeric(right, context, &format!("{path}:right"))?),
        NumericExpr::Max { left, right } => eval_numeric(left, context, &format!("{path}:left"))?
            .max(eval_numeric(right, context, &format!("{path}:right"))?),
        NumericExpr::Abs { value } => eval_numeric(value, context, &format!("{path}:value"))?.abs(),
        NumericExpr::Clamp { value, min, max } => {
            let value = eval_numeric(value, context, &format!("{path}:value"))?;
            let min = eval_numeric(min, context, &format!("{path}:min"))?;
            let max = eval_numeric(max, context, &format!("{path}:max"))?;
            if min > max {
                return Err(LabError::InvalidConfig(
                    "policy clamp min exceeds max".into(),
                ));
            }
            value.clamp(min, max)
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => {
            if eval_bool(condition, context, &format!("{path}:if"))? {
                eval_numeric(then_value, context, &format!("{path}:then"))?
            } else {
                eval_numeric(else_value, context, &format!("{path}:else"))?
            }
        }
    };
    finite(value, "policy numeric expression")?;
    Ok(value)
}

#[expect(
    clippy::float_cmp,
    reason = "EQ is the explicit exact numeric DSL operation; adding an epsilon changes user policy"
)]
fn eval_bool(expr: &BoolExpr, context: &EvalContext<'_>, path: &str) -> Result<bool, LabError> {
    match expr {
        BoolExpr::Compare {
            comparison,
            left,
            right,
        } => {
            let left = eval_numeric(left, context, &format!("{path}:left"))?;
            let right = eval_numeric(right, context, &format!("{path}:right"))?;
            Ok(match comparison {
                CompareOp::Gt => left > right,
                CompareOp::Gte => left >= right,
                CompareOp::Lt => left < right,
                CompareOp::Lte => left <= right,
                CompareOp::Eq => left == right,
            })
        }
        BoolExpr::And { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                if !eval_bool(condition, context, &format!("{path}:and:{index}"))? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoolExpr::Or { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                if eval_bool(condition, context, &format!("{path}:or:{index}"))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        BoolExpr::Not { condition } => Ok(!eval_bool(condition, context, &format!("{path}:not"))?),
        BoolExpr::CrossesAbove { .. } => cross_value(context, path, true),
        BoolExpr::CrossesBelow { .. } => cross_value(context, path, false),
        BoolExpr::PositionIs { state } => Ok(context.actual_state == *state),
    }
}

fn cross_value(context: &EvalContext<'_>, path: &str, above: bool) -> Result<bool, LabError> {
    let sample = context
        .crosses
        .get(path)
        .ok_or_else(|| LabError::Internal(format!("policy cross sample absent: {path}")))?;
    let Some(previous) = sample.previous else {
        return Ok(false);
    };
    Ok(if above {
        previous.0 <= previous.1 && sample.current.0 > sample.current.1
    } else {
        previous.0 >= previous.1 && sample.current.0 < sample.current.1
    })
}

fn collect_all_crosses(
    program: &RulesProgram,
    context: &EvalContext<'_>,
    prior: &BTreeMap<String, CrossSample>,
) -> Result<BTreeMap<String, CrossSample>, LabError> {
    let mut output = prior.clone();
    for state in &program.states {
        collect_crosses_numeric(
            &state.next,
            context,
            &format!("state:{}:next", state.id),
            &mut output,
        )?;
    }
    for rule in &program.rules {
        let root = format!("rule:{}", rule.id);
        collect_crosses_bool(
            &rule.condition,
            context,
            &format!("{root}:when"),
            &mut output,
        )?;
        collect_crosses_target(
            &rule.target,
            context,
            &format!("{root}:target"),
            &mut output,
        )?;
    }
    collect_crosses_target(&program.fallback, context, "fallback", &mut output)?;
    Ok(output)
}

fn collect_crosses_target(
    target: &PolicyTarget,
    context: &EvalContext<'_>,
    path: &str,
    output: &mut BTreeMap<String, CrossSample>,
) -> Result<(), LabError> {
    if let PolicyTarget::Weight { value } = target {
        collect_crosses_numeric(value, context, path, output)?;
    }
    Ok(())
}

fn collect_crosses_bool(
    expr: &BoolExpr,
    context: &EvalContext<'_>,
    path: &str,
    output: &mut BTreeMap<String, CrossSample>,
) -> Result<(), LabError> {
    match expr {
        BoolExpr::Compare { left, right, .. } => {
            collect_crosses_numeric(left, context, &format!("{path}:left"), output)?;
            collect_crosses_numeric(right, context, &format!("{path}:right"), output)?;
        }
        BoolExpr::And { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                collect_crosses_bool(condition, context, &format!("{path}:and:{index}"), output)?;
            }
        }
        BoolExpr::Or { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                collect_crosses_bool(condition, context, &format!("{path}:or:{index}"), output)?;
            }
        }
        BoolExpr::Not { condition } => {
            collect_crosses_bool(condition, context, &format!("{path}:not"), output)?;
        }
        BoolExpr::CrossesAbove { left, right } | BoolExpr::CrossesBelow { left, right } => {
            collect_crosses_numeric(left, context, &format!("{path}:left"), output)?;
            collect_crosses_numeric(right, context, &format!("{path}:right"), output)?;
            let current = {
                let current_context = EvalContext {
                    crosses: output,
                    ..context.clone()
                };
                (
                    eval_numeric(left, &current_context, &format!("{path}:left"))?,
                    eval_numeric(right, &current_context, &format!("{path}:right"))?,
                )
            };
            let previous = output.get(path).map(|sample| sample.current);
            output.insert(path.into(), CrossSample { previous, current });
        }
        BoolExpr::PositionIs { .. } => {}
    }
    Ok(())
}

fn collect_crosses_numeric(
    expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    output: &mut BTreeMap<String, CrossSample>,
) -> Result<(), LabError> {
    match expr {
        NumericExpr::Add { left, right }
        | NumericExpr::Sub { left, right }
        | NumericExpr::Mul { left, right }
        | NumericExpr::Div { left, right }
        | NumericExpr::Min { left, right }
        | NumericExpr::Max { left, right } => {
            collect_crosses_numeric(left, context, &format!("{path}:left"), output)?;
            collect_crosses_numeric(right, context, &format!("{path}:right"), output)?;
        }
        NumericExpr::Abs { value } => {
            collect_crosses_numeric(value, context, &format!("{path}:value"), output)?;
        }
        NumericExpr::Clamp { value, min, max } => {
            collect_crosses_numeric(value, context, &format!("{path}:value"), output)?;
            collect_crosses_numeric(min, context, &format!("{path}:min"), output)?;
            collect_crosses_numeric(max, context, &format!("{path}:max"), output)?;
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => {
            collect_crosses_bool(condition, context, &format!("{path}:if"), output)?;
            collect_crosses_numeric(then_value, context, &format!("{path}:then"), output)?;
            collect_crosses_numeric(else_value, context, &format!("{path}:else"), output)?;
        }
        NumericExpr::Constant { .. }
        | NumericExpr::Indicator { .. }
        | NumericExpr::State { .. }
        | NumericExpr::ActualWeight
        | NumericExpr::HeldBars
        | NumericExpr::EvidenceMultiplier
        | NumericExpr::EvidenceCoverage => {}
    }
    Ok(())
}
