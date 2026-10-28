//! Bounded deterministic evaluator for frozen custom policy rule programs.

#[cfg(test)]
mod tests;

use crate::contracts::policy::{
    BoolExpr, CompareOp, FinitePolicyNumber, NamedPolicyValue, NumericExpr,
    PolicyConditionEvidence, PolicyConditionOutcome, PolicyConditionReason, PolicyConditionRole,
    PolicyConditionRoleDeclaration, PolicyConditionScope, PolicyConditionTrace, PolicyDefinition,
    PolicyEvidenceTrace, PolicyIndicator, PolicyIndicatorKind, PolicyIndicatorSourceTrace,
    PolicyOperandPair, PolicyProgram, PolicySourceCaptureOrigin, PolicySourceObservationRef,
    PolicyTarget, PolicyTrace, RulesProgram,
};
use crate::contracts::{
    AdmissionStatus, CandleInterval, CandleObservation, CausalExecutionPolicy, ContentHash,
    DatasetSnapshot, FrozenPolicyRevision, IndicatorSnapshot, LabError, MarketId, ObservationId,
    PositionState, ReasonCode, ResolvedPlan, StrategyBinding, UtcRange, UtcTimestamp,
    VerifiedProviderCoverage, Weight, strategy_binding, validate_frozen_policy,
};
use crate::evidence::EvidenceEvaluator;
use crate::strategy::indicators::{
    Atr, Bollinger, BollingerOutput, DrawdownFromHigh, Ema, Macd, MacdOutput, Roc, RollingExtreme,
    RollingVol, Rsi, Sma, ZScore, finite,
};
use crate::strategy::{
    PositionView, StrategyEvaluation, StrategyEvaluator, annualize, decimal_f64,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

const POLICY_SOURCE_PROVENANCE_REF_LIMIT: usize = 8;
const MAX_PARALLEL_WORKERS: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct RuleEvaluation {
    pub(crate) decision_time: UtcTimestamp,
    pub(crate) target: Weight,
    pub(crate) trace: PolicyTrace,
    pub(crate) indicator_values: Vec<NamedPolicyValue>,
    pub(crate) completed_bars: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PolicyEvaluator {
    binding: StrategyBinding,
    inner: CompiledPolicy,
    decision_interval: CandleInterval,
    decision_warmup_bars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CompiledPolicy {
    Builtin(StrategyEvaluator),
    Rules(RulesEvaluator),
}

impl PolicyEvaluator {
    /// Compile directly from one immutable policy revision for a live causal stream.
    /// This path shares the same builtin and Rules evaluators as replay and paper.
    /// # Errors
    /// Rejects a corrupt revision or unsupported policy program.
    pub(crate) fn compile_frozen_revision(
        revision: &FrozenPolicyRevision,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        validate_frozen_policy(revision)?;
        let binding = StrategyBinding::Policy {
            reference: revision.reference.clone(),
            family: revision.family,
        };
        let program = revision.definition.program.clone();
        let decision_warmup_bars = match &program {
            PolicyProgram::Builtin { strategy } => strategy.warmup_bars()?,
            PolicyProgram::Rules { .. } => revision.definition.decision_warmup_bars(interval)?,
        };
        let inner = match program {
            PolicyProgram::Builtin { strategy } => {
                CompiledPolicy::Builtin(StrategyEvaluator::new(strategy, market, interval)?)
            }
            PolicyProgram::Rules { .. } => {
                CompiledPolicy::Rules(RulesEvaluator::new(&revision.definition, market, interval)?)
            }
        };
        Ok(Self {
            binding,
            inner,
            decision_interval: interval,
            decision_warmup_bars,
        })
    }

    pub(crate) fn compile(
        binding: StrategyBinding,
        plan: &ResolvedPlan,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        let program = binding.program(plan)?;
        let decision_warmup_bars = match &program {
            PolicyProgram::Builtin { strategy } => strategy.warmup_bars()?,
            PolicyProgram::Rules { .. } => {
                let reference = binding.policy_ref().ok_or_else(|| {
                    LabError::InputHashMismatch("RULES policy lacks a frozen revision".into())
                })?;
                plan.policy_revisions
                    .iter()
                    .find(|revision| revision.reference == *reference)
                    .ok_or_else(|| {
                        LabError::InputHashMismatch("RULES policy revision is not frozen".into())
                    })?
                    .definition
                    .decision_warmup_bars(interval)?
            }
        };
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
        Ok(Self {
            binding,
            inner,
            decision_interval: interval,
            decision_warmup_bars,
        })
    }

    pub(crate) fn observe(
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

    /// Evaluate one actual completed bar, allowing only time gaps covered by
    /// raw-response absence evidence already verified by the storage owner.
    ///
    /// # Errors
    /// Preserves all dense-observation validation and fails closed outside exact coverage.
    pub(crate) fn observe_with_provider_coverage(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<Option<StrategyEvaluation>, LabError> {
        let policy_ref = self.binding.policy_ref().cloned();
        match &mut self.inner {
            CompiledPolicy::Builtin(evaluator) => {
                let mut evaluation = evaluator.observe_with_provider_coverage(
                    observation,
                    position,
                    evidence,
                    coverage,
                )?;
                if let Some(evaluation) = &mut evaluation
                    && let Some(reference) = policy_ref
                {
                    evaluation.policy_ref = Some(reference);
                    evaluation.policy_trace = Some(builtin_trace(evaluation)?);
                }
                Ok(evaluation)
            }
            CompiledPolicy::Rules(evaluator) => {
                let result = if coverage.spans().is_empty() {
                    evaluator.observe(observation, position, evidence)?
                } else {
                    evaluator.observe_with_provider_coverage(
                        observation,
                        position,
                        evidence,
                        coverage,
                    )?
                };
                result
                    .map(|evaluation| rule_strategy_evaluation(policy_ref, evaluation, position))
                    .transpose()
            }
        }
    }

    /// Cross-interval indicator sources: `(indicator id, interval, kind
    /// warmup)` triples whose stream differs from the decision interval.
    pub(crate) fn source_indicators(&self) -> Vec<(String, CandleInterval, usize)> {
        match &self.inner {
            CompiledPolicy::Builtin(_) => Vec::new(),
            CompiledPolicy::Rules(evaluator) => evaluator.source_indicators(),
        }
    }

    pub(crate) fn source_requirements(&self) -> Vec<(CandleInterval, usize)> {
        let mut requirements = Vec::<(CandleInterval, usize)>::new();
        for (_, interval, warmup) in self.source_indicators() {
            if let Some((_, current)) = requirements
                .iter_mut()
                .find(|(candidate, _)| *candidate == interval)
            {
                *current = (*current).max(warmup);
            } else {
                requirements.push((interval, warmup));
            }
        }
        requirements.sort_by_key(|(interval, _)| interval.duration().num_seconds());
        requirements
    }

    pub(crate) fn decision_warmup_bars(&self) -> usize {
        self.decision_warmup_bars
    }

    pub(crate) fn completed_bars_seen(&self) -> u64 {
        match &self.inner {
            CompiledPolicy::Builtin(evaluator) => evaluator.completed_bars_seen(),
            CompiledPolicy::Rules(evaluator) => evaluator.completed_bars,
        }
    }

    pub(crate) fn cross_last_close(&self) -> BTreeMap<String, UtcTimestamp> {
        match &self.inner {
            CompiledPolicy::Builtin(_) => BTreeMap::new(),
            CompiledPolicy::Rules(evaluator) => evaluator.cross_last_close.clone(),
        }
    }

    /// Feed one completed source bar to one cross-interval indicator.
    /// # Errors
    /// Propagates evaluator validation failures; builtin strategies never
    /// declare source intervals, so feeding them is a contract error.
    pub(crate) fn observe_source(
        &mut self,
        id: &str,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        self.observe_source_with_provider_coverage(
            id,
            observation,
            &VerifiedProviderCoverage::empty(),
        )
    }

    /// Feed one actual completed source bar under verified coverage for any gap
    /// between this source's previously observed close and the new open.
    ///
    /// # Errors
    /// Rejects undeclared sources, invalid source bars, and uncovered gaps.
    pub(crate) fn observe_source_with_provider_coverage(
        &mut self,
        id: &str,
        observation: &CandleObservation,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<(), LabError> {
        match &mut self.inner {
            CompiledPolicy::Builtin(_) => Err(LabError::InvalidConfig(
                "builtin strategies have no cross-interval indicator sources".into(),
            )),
            CompiledPolicy::Rules(evaluator) => {
                if coverage.spans().is_empty() {
                    evaluator.observe_source(id, observation)
                } else {
                    evaluator.observe_source_with_provider_coverage(id, observation, coverage)
                }
            }
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

/// Bounded incremental cross-interval feed: per-interval cursors over the
/// frozen causal source bars. Feeding is O(new bars per decision), never a
/// full history reaggregation.
#[derive(Debug)]
pub(crate) struct CrossIntervalFeed {
    /// One bounded causal bar list per declared source interval (at most a
    /// handful of intervals, hence a small association vec, not a map).
    bars: Vec<(CandleInterval, Arc<[CandleObservation]>)>,
    /// Cached indicator order and independent next-bar cursor per source.
    sources: Vec<(String, CandleInterval, usize)>,
}

impl CrossIntervalFeed {
    /// Load the causal source bars for every cross-interval indicator of one
    /// compiled policy. Absent when the policy is single-interval.
    /// # Errors
    /// Rejects missing, gapped or incomplete source bars for a declared
    /// source interval (fail closed; never interpolated).
    pub(crate) fn new_with_cache(
        evaluator: &PolicyEvaluator,
        datasets: &[DatasetSnapshot],
        market: &MarketId,
        range: crate::contracts::UtcRange,
        cache: Option<&PreparedBarCache>,
    ) -> Result<Self, LabError> {
        let mut bars: Vec<(CandleInterval, Arc<[CandleObservation]>)> = Vec::new();
        for (interval, kind_warmup) in evaluator.source_requirements() {
            let causal_range =
                source_causal_range(range, evaluator.decision_interval, interval, kind_warmup)?;
            let loaded = prepare_interval_bars_cached(
                datasets,
                market,
                interval,
                SourceWindow::declared_warmup(causal_range, range.start()),
                cache,
            )?;
            bars.push((interval, loaded));
        }
        let sources = evaluator
            .source_indicators()
            .into_iter()
            .map(|(id, interval, _)| (id, interval, 0_usize))
            .collect();
        Ok(Self { bars, sources })
    }

    /// Feed every source bar that closed at or before `decision_time` to its
    /// indicator. `source_bar.close_time <= decision_time` is the strict
    /// causal alignment invariant.
    /// # Errors
    /// Propagates evaluator validation failures.
    pub(crate) fn feed_until(
        &mut self,
        evaluator: &mut PolicyEvaluator,
        decision_time: crate::contracts::UtcTimestamp,
    ) -> Result<(), LabError> {
        for (id, interval, cursor) in &mut self.sources {
            let Some((_, bars)) = self
                .bars
                .iter()
                .find(|(declared, _)| *declared == *interval)
            else {
                continue;
            };
            for bar in bars.iter().skip(*cursor) {
                if bar.candle.close_time_utc > decision_time {
                    break;
                }
                evaluator.observe_source(id, bar)?;
                *cursor += 1;
            }
        }
        Ok(())
    }
}

pub(crate) fn source_causal_range(
    range: UtcRange,
    decision_interval: CandleInterval,
    source_interval: CandleInterval,
    warmup_bars: usize,
) -> Result<UtcRange, LabError> {
    let warmup = i64::try_from(warmup_bars)
        .map_err(|_| LabError::ResourceLimit("source warmup overflow".into()))?;
    let last_decision = range
        .end()
        .0
        .checked_sub_signed(decision_interval.duration())
        .ok_or_else(|| LabError::InvalidConfig("decision range underflow".into()))?;
    let source_seconds = source_interval.duration().num_seconds();
    let first_source_close_seconds = range
        .start()
        .0
        .timestamp()
        .div_euclid(source_seconds)
        .checked_mul(source_seconds)
        .ok_or_else(|| LabError::InvalidConfig("source boundary overflow".into()))?;
    let warmup_seconds = source_seconds
        .checked_mul(warmup)
        .ok_or_else(|| LabError::InvalidConfig("source warmup overflow".into()))?;
    let source_start_seconds = first_source_close_seconds
        .checked_sub(warmup_seconds)
        .ok_or_else(|| LabError::InvalidConfig("source warmup underflow".into()))?;
    let source_end_seconds = last_decision
        .timestamp()
        .div_euclid(source_seconds)
        .checked_mul(source_seconds)
        .ok_or_else(|| LabError::InvalidConfig("source boundary overflow".into()))?;
    let source_end = chrono::DateTime::from_timestamp(source_end_seconds, 0)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("source boundary is not representable".into()))?;
    let source_start = chrono::DateTime::from_timestamp(source_start_seconds, 0)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("source warmup is not representable".into()))?;
    UtcRange::new(source_start, source_end).map_err(|error| {
        LabError::InsufficientWarmup(format!(
            "source interval {source_interval:?} has no completed bar before the final decision: {error}"
        ))
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        (*msg).to_owned()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

/// Evaluate `items` in parallel across a bounded scoped worker pool (<= 8 workers),
/// returning outputs in exact input order and deterministically reporting the
/// lowest-index failure when multiple items fail concurrently.
///
/// # Errors
/// Propagates cancellation, worker panics as [`LabError::Internal`], or the
/// lowest-index item failure.
pub(crate) fn ordered_parallel_try_map<T, R, F>(
    items: &[T],
    cancelled: &(dyn Fn() -> bool + Sync),
    f: F,
) -> Result<Vec<R>, LabError>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> Result<R, LabError> + Sync,
{
    let len = items.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let workers = available.min(MAX_PARALLEL_WORKERS).min(len).max(1);

    let run_one = |index: usize| -> Result<R, LabError> {
        if cancelled() {
            return Err(LabError::Cancelled(
                "parallel execution cancelled at deterministic checkpoint".into(),
            ));
        }
        match catch_unwind(AssertUnwindSafe(|| f(&items[index]))) {
            Ok(result) => result,
            Err(panic) => Err(LabError::Internal(format!(
                "parallel worker panicked at index {index}: {}",
                panic_message(&*panic)
            ))),
        }
    };

    if workers == 1 {
        let mut output = Vec::with_capacity(len);
        for index in 0..len {
            output.push(run_one(index)?);
        }
        return Ok(output);
    }

    let next_index = AtomicUsize::new(0);
    let min_failed = AtomicUsize::new(usize::MAX);
    let mut slots: Vec<Option<Result<R, LabError>>> = (0..len).map(|_| None).collect();

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut local: Vec<(usize, Result<R, LabError>)> = Vec::new();
                    loop {
                        let index = next_index.fetch_add(1, Ordering::Relaxed);
                        if index >= len || index > min_failed.load(Ordering::Acquire) {
                            break;
                        }
                        let result = run_one(index);
                        if result.is_err() {
                            min_failed.fetch_min(index, Ordering::AcqRel);
                        }
                        local.push((index, result));
                    }
                    local
                })
            })
            .collect();
        for handle in handles {
            if let Ok(local) = handle.join() {
                for (index, result) in local {
                    slots[index] = Some(result);
                }
            }
        }
    });

    let mut output = Vec::with_capacity(len);
    for (index, slot) in slots.into_iter().enumerate() {
        match slot {
            Some(Ok(value)) => output.push(value),
            Some(Err(error)) => return Err(error),
            None => {
                return Err(LabError::Internal(format!(
                    "parallel worker lost result at index {index}"
                )));
            }
        }
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SourceWindow {
    Complete(UtcRange),
    DeclaredWarmup {
        range: UtcRange,
        evaluation_start: UtcTimestamp,
    },
}

impl SourceWindow {
    pub(crate) fn declared_warmup(range: UtcRange, evaluation_start: UtcTimestamp) -> Self {
        Self::DeclaredWarmup {
            range,
            evaluation_start,
        }
    }

    fn range(self) -> UtcRange {
        match self {
            Self::Complete(range) | Self::DeclaredWarmup { range, .. } => range,
        }
    }

    fn missing_leading_bar(self) -> LabError {
        match self {
            Self::DeclaredWarmup {
                range,
                evaluation_start,
            } if range.start() < evaluation_start => LabError::InsufficientWarmup(
                "source window starts after the declared warmup boundary".into(),
            ),
            Self::Complete(_) | Self::DeclaredWarmup { .. } => {
                LabError::DataGap("source window starts after its required boundary".into())
            }
        }
    }
}

impl From<UtcRange> for SourceWindow {
    fn from(range: UtcRange) -> Self {
        Self::Complete(range)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PreparedBarKey {
    pub(crate) market: MarketId,
    pub(crate) interval: CandleInterval,
    pub(crate) window: SourceWindow,
}

/// Thread-safe cache of validated, resampled interval bar streams shared across
/// multi-policy admissions, cross-interval feeds, and causal input digesting.
#[derive(Debug, Default)]
pub struct PreparedBarCache {
    inner: RwLock<BTreeMap<PreparedBarKey, Arc<[CandleObservation]>>>,
}

impl PreparedBarCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get(&self, key: &PreparedBarKey) -> Option<Arc<[CandleObservation]>> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    pub(crate) fn get_or_prepare(
        &self,
        datasets: &[DatasetSnapshot],
        market: &MarketId,
        target: CandleInterval,
        window: SourceWindow,
    ) -> Result<Arc<[CandleObservation]>, LabError> {
        let key = PreparedBarKey {
            market: market.clone(),
            interval: target,
            window,
        };
        if let Some(hit) = self.get(&key) {
            return Ok(hit);
        }
        let prepared: Arc<[CandleObservation]> =
            prepare_interval_bars(datasets, market, target, window)?.into();
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        Ok(guard.entry(key).or_insert_with(|| prepared.clone()).clone())
    }

    /// Best-effort parallel pre-warming across all distinct `(market, interval, window)`
    /// streams required by eligible admissions in `plan`.
    ///
    /// Failures, panics, or cancellations during pre-warming leave keys cold in the
    /// cache and never surface early, guaranteeing that sequential `run_model`
    /// evaluation preserves exact admission-order error semantics.
    #[must_use]
    pub fn prewarm_for_plan(
        plan: &ResolvedPlan,
        datasets: &[DatasetSnapshot],
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Self {
        let cache = Self::new();
        if plan.spec.causal_execution != Some(CausalExecutionPolicy::DeclaredPolicyWarmup) {
            return cache;
        }
        let mut keys = BTreeSet::new();
        for admission in &plan.admissions {
            if admission.status != AdmissionStatus::Eligible {
                continue;
            }
            let Ok(binding) = strategy_binding(plan, admission) else {
                continue;
            };
            let Ok(evaluator) = PolicyEvaluator::compile(
                binding,
                plan,
                admission.market.clone(),
                plan.spec.decision_interval,
            ) else {
                continue;
            };
            let Ok(decision_warmup) = u32::try_from(evaluator.decision_warmup_bars()) else {
                continue;
            };
            let Ok(decision_range) = plan
                .spec
                .range
                .with_warmup(decision_warmup, plan.spec.decision_interval)
            else {
                continue;
            };
            let Ok(execution_range) = plan
                .spec
                .range
                .with_warmup(1, plan.spec.execution_resolution)
            else {
                continue;
            };
            keys.insert(PreparedBarKey {
                market: admission.market.clone(),
                interval: plan.spec.decision_interval,
                window: SourceWindow::declared_warmup(decision_range, plan.spec.range.start()),
            });
            keys.insert(PreparedBarKey {
                market: admission.market.clone(),
                interval: plan.spec.execution_resolution,
                window: SourceWindow::Complete(execution_range),
            });
            for (source_interval, source_warmup) in evaluator.source_requirements() {
                if let Ok(source_range) = source_causal_range(
                    plan.spec.range,
                    plan.spec.decision_interval,
                    source_interval,
                    source_warmup,
                ) {
                    keys.insert(PreparedBarKey {
                        market: admission.market.clone(),
                        interval: source_interval,
                        window: SourceWindow::declared_warmup(
                            source_range,
                            plan.spec.range.start(),
                        ),
                    });
                }
            }
        }
        let key_list: Vec<PreparedBarKey> = keys.into_iter().collect();
        let _ = ordered_parallel_try_map(&key_list, cancelled, |key| {
            if let Ok(bars) = prepare_interval_bars(datasets, &key.market, key.interval, key.window)
            {
                let arc_bars: Arc<[CandleObservation]> = bars.into();
                let mut guard = cache.inner.write().unwrap_or_else(PoisonError::into_inner);
                guard.entry(key.clone()).or_insert(arc_bars);
            }
            Ok(())
        });
        cache
    }
}

pub(crate) fn prepare_interval_bars_cached(
    datasets: &[DatasetSnapshot],
    market: &MarketId,
    target: CandleInterval,
    window: impl Into<SourceWindow>,
    cache: Option<&PreparedBarCache>,
) -> Result<Arc<[CandleObservation]>, LabError> {
    let window = window.into();
    if let Some(cache) = cache {
        cache.get_or_prepare(datasets, market, target, window)
    } else {
        prepare_interval_bars(datasets, market, target, window).map(Arc::from)
    }
}

/// Resolve one complete immutable interval stream. Exact native observations
/// take precedence. When absent, the closest divisible finer stream is
/// resampled through the collection authority using fixed UTC buckets and all
/// constituents. The selected path is validated against the actual rows, not
/// only manifest coverage.
pub(crate) fn prepare_interval_bars(
    datasets: &[DatasetSnapshot],
    market: &MarketId,
    target: CandleInterval,
    window: impl Into<SourceWindow>,
) -> Result<Vec<CandleObservation>, LabError> {
    let window = window.into();
    let range = window.range();
    let native_declared = datasets.iter().any(|dataset| {
        dataset.manifest.request.data_resolution == target
            && dataset.manifest.request.markets.contains(market)
    });
    if native_declared {
        let exact = collect_interval_rows(datasets, market, target, range);
        reject_quality_issues(datasets, market, target, range)?;
        validate_interval_bars(&exact, market, target, window)?;
        return Ok(exact);
    }

    let target_seconds = target.duration().num_seconds();
    let source = datasets
        .iter()
        .flat_map(|dataset| dataset.observations.iter())
        .filter(|row| row.candle.market == market.code())
        .map(|row| row.candle.interval)
        .filter(|interval| {
            let seconds = interval.duration().num_seconds();
            seconds < target_seconds && target_seconds % seconds == 0
        })
        .max_by_key(|interval| interval.duration().num_seconds())
        .ok_or_else(|| {
            LabError::DataGap(format!(
                "no native {target:?} or divisible finer observations for {market}"
            ))
        })?;
    reject_quality_issues(datasets, market, source, range)?;
    let source_rows = collect_interval_rows(datasets, market, source, range);
    validate_interval_bars(&source_rows, market, source, window)?;
    let derived = crate::collection::resample(&source_rows, target)?;
    validate_interval_bars(&derived, market, target, window)?;
    Ok(derived)
}

fn reject_quality_issues(
    datasets: &[DatasetSnapshot],
    market: &MarketId,
    interval: CandleInterval,
    range: UtcRange,
) -> Result<(), LabError> {
    if datasets.iter().any(|dataset| {
        dataset.manifest.request.data_resolution == interval
            && dataset.manifest.request.markets.contains(market)
            && dataset.manifest.quality_issues.iter().any(|issue| {
                issue.market == *market
                    && issue.severity == crate::contracts::QualitySeverity::Error
                    && issue.start < range.end()
                    && issue.end > range.start()
            })
    }) {
        return Err(LabError::DataGap(format!(
            "{interval:?} source for {market} has an overlapping error-severity quality issue"
        )));
    }
    Ok(())
}

fn collect_interval_rows(
    datasets: &[DatasetSnapshot],
    market: &MarketId,
    interval: CandleInterval,
    range: UtcRange,
) -> Vec<CandleObservation> {
    let mut rows = datasets
        .iter()
        .flat_map(|dataset| dataset.observations.iter())
        .filter(|row| {
            row.candle.market == market.code()
                && row.candle.interval == interval
                && range.contains(row.candle.open_time_utc)
                && row.candle.close_time_utc <= range.end()
        })
        .cloned()
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.candle
            .open_time_utc
            .cmp(&right.candle.open_time_utc)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    rows
}

fn validate_interval_bars(
    rows: &[CandleObservation],
    market: &MarketId,
    interval: CandleInterval,
    window: SourceWindow,
) -> Result<(), LabError> {
    let range = window.range();
    let Some(first) = rows.first() else {
        return Err(LabError::DataGap(format!(
            "no {interval:?} observations for {market}"
        )));
    };
    let Some(last) = rows.last() else {
        return Err(LabError::DataGap(
            "prepared interval unexpectedly empty".into(),
        ));
    };
    if first.candle.open_time_utc > range.start() {
        return Err(window.missing_leading_bar());
    }
    let seconds = interval.duration().num_seconds();
    if first.candle.open_time_utc != range.start()
        || last.candle.close_time_utc != range.end()
        || rows.iter().any(|row| {
            row.candle.market != market.code()
                || row.candle.interval != interval
                || !row.candle.completed
                || row.candle.open_time_utc.0.timestamp_subsec_nanos() != 0
                || row.candle.close_time_utc.0.timestamp_subsec_nanos() != 0
                || row.candle.open_time_utc.0.timestamp().rem_euclid(seconds) != 0
                || row.candle.close_time_utc.0 - row.candle.open_time_utc.0 != interval.duration()
        })
        || rows
            .windows(2)
            .any(|pair| pair[0].candle.close_time_utc != pair[1].candle.open_time_utc)
    {
        return Err(LabError::DataGap(format!(
            "{interval:?} source window for {market} is incomplete, duplicated or misaligned"
        )));
    }
    Ok(())
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
        decision_trace: None,
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
        condition_trace: None,
        source_provenance: None,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Latest warm value of each cross-interval indicator.
    cross_latest: BTreeMap<String, f64>,
    /// Last fed source close per cross-interval indicator (strict ordering).
    cross_last_close: BTreeMap<String, UtcTimestamp>,
    requires_evidence: bool,
    /// Missing means a legacy checkpoint did not capture source provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_provenance: Option<BTreeMap<String, SourceProvenanceState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceProvenanceState {
    capture_origin: PolicySourceCaptureOrigin,
    capture_anchor: Option<PolicySourceObservationRef>,
    captured_count: u64,
    recent_refs: Vec<PolicySourceObservationRef>,
    rolling_digest: Option<ContentHash>,
}

#[derive(Serialize)]
struct SourceDigestStep<'a> {
    version: &'static str,
    previous: Option<&'a ContentHash>,
    observation_id: &'a ObservationId,
    content_digest: &'a ContentHash,
    open_time: UtcTimestamp,
    close_time: UtcTimestamp,
}

/// Recompute the evaluator's rolling source digest from an ordered sequence of
/// captured observation references.
///
/// # Errors
/// Propagates serialization failures from the canonical content hash.
pub(crate) fn policy_source_provenance_digest(
    observations: &[PolicySourceObservationRef],
) -> Result<Option<ContentHash>, LabError> {
    let mut digest = None;
    for observation in observations {
        digest = Some(source_provenance_digest_step(digest.as_ref(), observation)?);
    }
    Ok(digest)
}

fn source_provenance_digest_step(
    previous: Option<&ContentHash>,
    observation: &PolicySourceObservationRef,
) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&SourceDigestStep {
        version: "policy-source-provenance-v1",
        previous,
        observation_id: &observation.observation_id,
        content_digest: &observation.content_digest,
        open_time: observation.open_time,
        close_time: observation.close_time,
    })
}

impl SourceProvenanceState {
    fn captured_from_evaluator_start() -> Self {
        Self {
            capture_origin: PolicySourceCaptureOrigin::CapturedFromEvaluatorStart,
            capture_anchor: None,
            captured_count: 0,
            recent_refs: Vec::new(),
            rolling_digest: None,
        }
    }

    fn not_captured() -> Self {
        Self {
            capture_origin: PolicySourceCaptureOrigin::NotCaptured,
            capture_anchor: None,
            captured_count: 0,
            recent_refs: Vec::new(),
            rolling_digest: None,
        }
    }

    fn record(&mut self, observation: &CandleObservation) -> Result<(), LabError> {
        let reference = PolicySourceObservationRef {
            observation_id: observation.id.clone(),
            content_digest: observation.content_digest.clone(),
            open_time: observation.candle.open_time_utc,
            close_time: observation.candle.close_time_utc,
        };
        let captured_count = self.captured_count.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("policy source provenance count overflow".into())
        })?;
        let rolling_digest =
            source_provenance_digest_step(self.rolling_digest.as_ref(), &reference)?;

        if self.capture_anchor.is_none() {
            self.capture_anchor = Some(reference.clone());
        }
        if self.recent_refs.len() == POLICY_SOURCE_PROVENANCE_REF_LIMIT {
            self.recent_refs.remove(0);
        }
        self.recent_refs.push(reference);
        self.captured_count = captured_count;
        self.rolling_digest = Some(rolling_digest);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndicatorRuntime {
    id: String,
    kind: IndicatorState,
    /// Effective source stream: the declared interval or the decision one.
    source_interval: CandleInterval,
    /// The declared indicator kind's own warmup (window) in source bars.
    kind_warmup: usize,
}

#[derive(Debug, Clone, Copy)]
struct CandlePrices {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    Atr(Atr),
    Bollinger(Bollinger),
    Macd(Macd),
    ZScore(ZScore),
    Roc(Roc),
    DrawdownFromHigh(DrawdownFromHigh),
}

impl RulesEvaluator {
    pub(crate) fn new(
        definition: &PolicyDefinition,
        market: MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        let warmup_bars = definition.decision_warmup_bars(interval)?;
        let PolicyProgram::Rules { program } = &definition.program else {
            return Err(LabError::InvalidConfig(
                "rules evaluator requires a RULES policy definition".into(),
            ));
        };
        let indicators = program
            .indicators
            .iter()
            .map(|spec| IndicatorRuntime::new(spec, interval))
            .collect::<Vec<_>>();
        let source_provenance = Some(
            indicators
                .iter()
                .map(|indicator| {
                    (
                        indicator.id.clone(),
                        SourceProvenanceState::captured_from_evaluator_start(),
                    )
                })
                .collect(),
        );
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
            cross_latest: BTreeMap::new(),
            cross_last_close: BTreeMap::new(),
            requires_evidence: definition.requires_evidence(),
            source_provenance,
        })
    }

    fn ensure_source_provenance(&mut self) {
        if self.source_provenance.is_none() {
            self.source_provenance = Some(
                self.indicators
                    .iter()
                    .map(|indicator| (indicator.id.clone(), SourceProvenanceState::not_captured()))
                    .collect(),
            );
        }
    }

    fn record_source_provenance(
        &mut self,
        indicator_id: &str,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        self.ensure_source_provenance();
        let state = self
            .source_provenance
            .as_mut()
            .and_then(|states| states.get_mut(indicator_id))
            .ok_or_else(|| {
                LabError::Internal(format!(
                    "policy source provenance state is absent: {indicator_id}"
                ))
            })?;
        state.record(observation)
    }

    fn source_provenance_trace(&self) -> Result<Vec<PolicyIndicatorSourceTrace>, LabError> {
        self.indicators
            .iter()
            .map(|indicator| {
                let required_warmup_bars = u64::try_from(indicator.kind_warmup).map_err(|_| {
                    LabError::ResourceLimit(
                        "policy indicator warmup exceeds provenance counter capacity".into(),
                    )
                })?;
                let state = self
                    .source_provenance
                    .as_ref()
                    .and_then(|states| states.get(&indicator.id));
                Ok(PolicyIndicatorSourceTrace {
                    indicator_id: indicator.id.clone(),
                    source_interval: indicator.source_interval,
                    required_warmup_bars,
                    capture_origin: state.map_or(PolicySourceCaptureOrigin::NotCaptured, |state| {
                        state.capture_origin
                    }),
                    capture_anchor: state.and_then(|state| state.capture_anchor.clone()),
                    captured_count: state.map_or(0, |state| state.captured_count),
                    recent_refs: state.map_or_else(Vec::new, |state| state.recent_refs.clone()),
                    rolling_digest: state.and_then(|state| state.rolling_digest.clone()),
                })
            })
            .collect()
    }

    /// Cross-interval indicator sources: `(indicator id, interval, kind
    /// warmup)` triples whose stream differs from the decision interval.
    pub(crate) fn source_indicators(&self) -> Vec<(String, CandleInterval, usize)> {
        self.indicators
            .iter()
            .filter(|indicator| indicator.source_interval != self.interval)
            .map(|indicator| {
                (
                    indicator.id.clone(),
                    indicator.source_interval,
                    indicator.kind_warmup,
                )
            })
            .collect()
    }

    /// Feed one completed source bar to one cross-interval indicator using
    /// the default dense-observation contract.
    /// # Errors
    /// Propagates source validation failures.
    pub(crate) fn observe_source(
        &mut self,
        id: &str,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        self.observe_source_with_provider_coverage(
            id,
            observation,
            &VerifiedProviderCoverage::empty(),
        )
    }

    /// Feed one completed source bar to one cross-interval indicator.
    ///
    /// Causal alignment is enforced here: the bar must belong to the
    /// indicator's declared source interval, be completed, and close strictly
    /// after the previously fed bar unless that gap is covered by verified
    /// provider evidence.
    /// # Errors
    /// Rejects unknown ids, decision-stream ids, foreign bars, incomplete or
    /// misaligned bars, regressions and corrupt OHLC relations.
    pub(crate) fn observe_source_with_provider_coverage(
        &mut self,
        id: &str,
        observation: &CandleObservation,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<(), LabError> {
        let Some(index) = self
            .indicators
            .iter()
            .position(|indicator| indicator.id == id)
        else {
            return Err(LabError::InvalidConfig(format!(
                "unknown policy indicator {id}"
            )));
        };
        let indicator = &self.indicators[index];
        let source = indicator.source_interval;
        if source == self.interval {
            return Err(LabError::InvalidConfig(format!(
                "indicator {id} reads the decision stream; feed it through observe"
            )));
        }
        let candle = &observation.candle;
        if candle.market != self.market.code()
            || candle.interval != source
            || !candle.completed
            || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
            || candle.close_time_utc.0.timestamp_subsec_nanos() != 0
            || candle
                .open_time_utc
                .0
                .timestamp()
                .rem_euclid(source.duration().num_seconds())
                != 0
            || candle.close_time_utc.0 - candle.open_time_utc.0 != source.duration()
            || self.cross_last_close.get(id).is_some_and(|previous| {
                !coverage.covers(&self.market, source, *previous, candle.open_time_utc)
            })
        {
            return Err(LabError::DataGap(format!(
                "policy source {id} requires contiguous completed {source:?} bars closing forward"
            )));
        }
        if candle.high < candle.open
            || candle.high < candle.close
            || candle.low > candle.open
            || candle.low > candle.close
            || candle.high < candle.low
        {
            return Err(LabError::DataCorrupt("invalid policy OHLC relation".into()));
        }
        let prices = CandlePrices::from_observation(observation)?;
        let indicator = &mut self.indicators[index];
        if let Some(value) = indicator.update(observation, &prices, source)? {
            self.cross_latest.insert(id.to_string(), value);
        }
        self.record_source_provenance(id, observation)?;
        self.cross_last_close
            .insert(id.to_string(), candle.close_time_utc);
        Ok(())
    }

    /// Evaluate one actual decision bar under the default dense-observation
    /// contract.
    /// # Errors
    /// Propagates observation validation failures.
    pub(crate) fn observe(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
    ) -> Result<Option<RuleEvaluation>, LabError> {
        self.observe_with_provider_coverage(
            observation,
            position,
            evidence,
            &VerifiedProviderCoverage::empty(),
        )
    }

    /// Evaluate one actual decision bar, allowing time gaps only where
    /// verified provider evidence covers the exact missing open-time range.
    ///
    /// # Errors
    /// Rejects uncovered or malformed observations and invalid policy state.
    pub(crate) fn observe_with_provider_coverage(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<Option<RuleEvaluation>, LabError> {
        self.validate_observation(observation, coverage)?;
        let values = self.update_decision_indicators(observation)?;
        self.completed_bars = self.completed_bars.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("policy completed-bar counter overflow".into())
        })?;
        self.last_close = Some(observation.candle.close_time_utc);
        if values.len() != self.indicators.len() {
            return Ok(None);
        }
        self.evaluate_ready_observation(observation, position, evidence, &values)
    }

    fn update_decision_indicators(
        &mut self,
        observation: &CandleObservation,
    ) -> Result<BTreeMap<String, f64>, LabError> {
        let mut values = BTreeMap::new();
        if !self.indicators.is_empty() {
            self.ensure_source_provenance();
            let prices = CandlePrices::from_observation(observation)?;
            let source_provenance = self.source_provenance.as_mut().ok_or_else(|| {
                LabError::Internal(
                    "policy source provenance map is absent after initialization".into(),
                )
            })?;
            for indicator in &mut self.indicators {
                if indicator.source_interval != self.interval {
                    // Cross-interval indicators hold their latest fed value;
                    // they are updated only through observe_source.
                    if let Some(value) = self.cross_latest.get(&indicator.id) {
                        values.insert(indicator.id.clone(), *value);
                    }
                    continue;
                }
                let value = indicator.update(observation, &prices, self.interval)?;
                if let Some(value) = value {
                    values.insert(indicator.id.clone(), value);
                }
                let provenance = source_provenance.get_mut(&indicator.id).ok_or_else(|| {
                    LabError::Internal(format!(
                        "policy source provenance state is absent: {}",
                        indicator.id
                    ))
                })?;
                provenance.record(observation)?;
            }
        }
        Ok(values)
    }

    fn evaluate_ready_observation(
        &mut self,
        observation: &CandleObservation,
        position: PositionView,
        evidence: Option<&EvidenceEvaluator>,
        values: &BTreeMap<String, f64>,
    ) -> Result<Option<RuleEvaluation>, LabError> {
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
            indicators: values,
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
        let (after, state_trace) = self.evaluate_next_state(&context)?;
        let (matched_rule_id, target_value, rule_trace) =
            self.evaluate_rules_with_trace(&context)?;
        let condition_trace = state_trace
            .into_iter()
            .chain(rule_trace)
            .collect::<Vec<_>>();
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
        let source_provenance = self.source_provenance_trace()?;
        Ok(Some(RuleEvaluation {
            decision_time: observation.candle.close_time_utc,
            target,
            trace: PolicyTrace {
                matched_rule_id,
                state_before: named_values(&before),
                state_after,
                evidence: evidence_trace,
                condition_trace: Some(condition_trace),
                source_provenance: Some(source_provenance),
            },
            indicator_values: named_values(values),
            completed_bars: self.completed_bars,
        }))
    }

    fn evaluate_next_state(
        &self,
        context: &EvalContext<'_>,
    ) -> Result<(BTreeMap<String, f64>, Vec<PolicyConditionTrace>), LabError> {
        let mut after = BTreeMap::new();
        let mut condition_trace = Vec::new();
        for slot in &self.program.states {
            let mut collector = ConditionTraceCollector::new(
                PolicyConditionScope::StateTransition {
                    state_id: slot.id.clone(),
                },
                &[],
            );
            let value = eval_numeric(
                &slot.next,
                context,
                &format!("state:{}:next", slot.id),
                Some(&mut collector),
            )?;
            finite(value, "policy next state")?;
            after.insert(slot.id.clone(), value);
            condition_trace.extend(collector.entries);
        }
        Ok((after, condition_trace))
    }

    fn evaluate_rules_with_trace(
        &self,
        context: &EvalContext<'_>,
    ) -> Result<(Option<String>, f64, Vec<PolicyConditionTrace>), LabError> {
        let mut matched_rule_id = None;
        let mut selected = None;
        let mut selected_rule_index = None;
        let mut condition_trace = Vec::new();
        for (index, rule) in self.program.rules.iter().enumerate() {
            let path = format!("rule:{}", rule.id);
            let condition_path = format!("{path}:when");
            let mut collector = ConditionTraceCollector::new(
                PolicyConditionScope::RuleCondition {
                    rule_id: rule.id.clone(),
                },
                &self.program.condition_roles,
            );
            let matched = eval_bool(
                &rule.condition,
                context,
                &condition_path,
                Some(&mut collector),
            )?;
            condition_trace.extend(collector.entries);
            if matched {
                matched_rule_id = Some(rule.id.clone());
                let mut target_collector = ConditionTraceCollector::new(
                    PolicyConditionScope::RuleTarget {
                        rule_id: rule.id.clone(),
                    },
                    &[],
                );
                selected = Some(eval_target(
                    &rule.target,
                    context,
                    &format!("{path}:target"),
                    Some(&mut target_collector),
                )?);
                condition_trace.extend(target_collector.entries);
                selected_rule_index = Some(index);
                break;
            }
            Self::append_target_guard_not_evaluated(
                &rule.target,
                PolicyConditionScope::RuleTarget {
                    rule_id: rule.id.clone(),
                },
                &format!("{path}:target"),
                PolicyConditionReason::RuleNotMatched,
                &mut condition_trace,
            );
        }
        self.append_traces_skipped_after_rule_match(selected_rule_index, &mut condition_trace);
        let target_value = if let Some(value) = selected {
            value
        } else {
            let mut fallback_collector =
                ConditionTraceCollector::new(PolicyConditionScope::FallbackTarget, &[]);
            let value = eval_target(
                &self.program.fallback,
                context,
                "fallback",
                Some(&mut fallback_collector),
            )?;
            condition_trace.extend(fallback_collector.entries);
            value
        };
        Ok((matched_rule_id, target_value, condition_trace))
    }

    fn append_traces_skipped_after_rule_match(
        &self,
        selected_rule_index: Option<usize>,
        trace: &mut Vec<PolicyConditionTrace>,
    ) {
        let Some(index) = selected_rule_index else {
            return;
        };
        for rule in &self.program.rules[index + 1..] {
            let condition_path = format!("rule:{}:when", rule.id);
            let mut collector = ConditionTraceCollector::new(
                PolicyConditionScope::RuleCondition {
                    rule_id: rule.id.clone(),
                },
                &self.program.condition_roles,
            );
            collector.mark_not_evaluated(
                &rule.condition,
                &condition_path,
                PolicyConditionReason::PriorRuleMatched,
            );
            trace.extend(collector.entries);
            Self::append_target_guard_not_evaluated(
                &rule.target,
                PolicyConditionScope::RuleTarget {
                    rule_id: rule.id.clone(),
                },
                &format!("rule:{}:target", rule.id),
                PolicyConditionReason::PriorRuleMatched,
                trace,
            );
        }
        Self::append_target_guard_not_evaluated(
            &self.program.fallback,
            PolicyConditionScope::FallbackTarget,
            "fallback",
            PolicyConditionReason::PriorRuleMatched,
            trace,
        );
    }

    fn append_target_guard_not_evaluated(
        target: &PolicyTarget,
        scope: PolicyConditionScope,
        ast_path: &str,
        reason: PolicyConditionReason,
        trace: &mut Vec<PolicyConditionTrace>,
    ) {
        let mut collector = ConditionTraceCollector::new(scope, &[]);
        collector.mark_target_not_evaluated(target, ast_path, reason);
        trace.extend(collector.entries);
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

    fn validate_observation(
        &self,
        observation: &CandleObservation,
        coverage: &VerifiedProviderCoverage,
    ) -> Result<(), LabError> {
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
            || self.last_close.is_some_and(|previous| {
                !coverage.covers(&self.market, self.interval, previous, candle.open_time_utc)
            })
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
    fn new(spec: &PolicyIndicator, decision_interval: CandleInterval) -> Self {
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
            PolicyIndicatorKind::Atr { window } => IndicatorState::Atr(Atr::new(*window)),
            PolicyIndicatorKind::BollingerUpper { window, multiplier } => {
                IndicatorState::Bollinger(Bollinger::new(
                    *window,
                    *multiplier,
                    BollingerOutput::Upper,
                ))
            }
            PolicyIndicatorKind::BollingerLower { window, multiplier } => {
                IndicatorState::Bollinger(Bollinger::new(
                    *window,
                    *multiplier,
                    BollingerOutput::Lower,
                ))
            }
            PolicyIndicatorKind::BollingerBandwidth { window, multiplier } => {
                IndicatorState::Bollinger(Bollinger::new(
                    *window,
                    *multiplier,
                    BollingerOutput::Bandwidth,
                ))
            }
            PolicyIndicatorKind::BollingerPercentB { window, multiplier } => {
                IndicatorState::Bollinger(Bollinger::new(
                    *window,
                    *multiplier,
                    BollingerOutput::PercentB,
                ))
            }
            PolicyIndicatorKind::MacdLine {
                fast_window,
                slow_window,
            } => IndicatorState::Macd(Macd::new(
                *fast_window,
                *slow_window,
                None,
                MacdOutput::Line,
            )),
            PolicyIndicatorKind::MacdSignal {
                fast_window,
                slow_window,
                signal_window,
            } => IndicatorState::Macd(Macd::new(
                *fast_window,
                *slow_window,
                Some(*signal_window),
                MacdOutput::Signal,
            )),
            PolicyIndicatorKind::MacdHistogram {
                fast_window,
                slow_window,
                signal_window,
            } => IndicatorState::Macd(Macd::new(
                *fast_window,
                *slow_window,
                Some(*signal_window),
                MacdOutput::Histogram,
            )),
            PolicyIndicatorKind::ZScore { window } => IndicatorState::ZScore(ZScore::new(*window)),
            PolicyIndicatorKind::Roc { window } => IndicatorState::Roc(Roc::new(*window)),
            PolicyIndicatorKind::DrawdownFromHigh { window } => {
                IndicatorState::DrawdownFromHigh(DrawdownFromHigh::new(*window))
            }
        };
        Self {
            id: spec.id.clone(),
            kind,
            source_interval: spec.source_interval.unwrap_or(decision_interval),
            kind_warmup: spec.indicator.warmup_bars(),
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
            IndicatorState::Atr(state) => state.update(prices.high, prices.low, prices.close),
            IndicatorState::Bollinger(state) => state.update(prices.close),
            IndicatorState::Macd(state) => state.update(prices.close),
            IndicatorState::ZScore(state) => state.update(prices.close),
            IndicatorState::Roc(state) => state.update(prices.close),
            IndicatorState::DrawdownFromHigh(state) => state.update(prices.close),
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

fn named_values(values: &BTreeMap<String, f64>) -> Vec<NamedPolicyValue> {
    values
        .iter()
        .map(|(id, value)| NamedPolicyValue {
            id: id.clone(),
            value: *value,
        })
        .collect()
}

struct ConditionTraceCollector<'a> {
    scope: PolicyConditionScope,
    roles: &'a [PolicyConditionRoleDeclaration],
    entries: Vec<PolicyConditionTrace>,
}

impl<'a> ConditionTraceCollector<'a> {
    fn new(scope: PolicyConditionScope, roles: &'a [PolicyConditionRoleDeclaration]) -> Self {
        Self {
            scope,
            roles,
            entries: Vec::new(),
        }
    }

    fn record_evaluated(
        &mut self,
        ast_path: &str,
        engine_result: bool,
        unknown_reason: Option<PolicyConditionReason>,
        evidence: Option<PolicyConditionEvidence>,
    ) {
        let outcome = match unknown_reason {
            Some(reason) => PolicyConditionOutcome::Unknown(reason),
            None if engine_result => PolicyConditionOutcome::True,
            None => PolicyConditionOutcome::False,
        };
        self.record(ast_path, outcome, Some(engine_result), evidence);
    }

    fn mark_not_evaluated(
        &mut self,
        expr: &BoolExpr,
        ast_path: &str,
        reason: PolicyConditionReason,
    ) {
        match expr {
            BoolExpr::Compare { left, right, .. }
            | BoolExpr::CrossesAbove { left, right }
            | BoolExpr::CrossesBelow { left, right } => {
                self.mark_numeric_conditions_not_evaluated(
                    left,
                    &format!("{ast_path}:left"),
                    reason,
                );
                self.mark_numeric_conditions_not_evaluated(
                    right,
                    &format!("{ast_path}:right"),
                    reason,
                );
            }
            BoolExpr::And { conditions } => {
                for (index, condition) in conditions.iter().enumerate() {
                    self.mark_not_evaluated(condition, &format!("{ast_path}:and:{index}"), reason);
                }
            }
            BoolExpr::Or { conditions } => {
                for (index, condition) in conditions.iter().enumerate() {
                    self.mark_not_evaluated(condition, &format!("{ast_path}:or:{index}"), reason);
                }
            }
            BoolExpr::Not { condition } => {
                self.mark_not_evaluated(condition, &format!("{ast_path}:not"), reason);
            }
            BoolExpr::PositionIs { .. } => {}
        }
        self.record(
            ast_path,
            PolicyConditionOutcome::NotEvaluated(reason),
            None,
            None,
        );
    }

    fn mark_numeric_conditions_not_evaluated(
        &mut self,
        expr: &NumericExpr,
        ast_path: &str,
        reason: PolicyConditionReason,
    ) {
        match expr {
            NumericExpr::Add { left, right }
            | NumericExpr::Sub { left, right }
            | NumericExpr::Mul { left, right }
            | NumericExpr::Div { left, right }
            | NumericExpr::Min { left, right }
            | NumericExpr::Max { left, right } => {
                self.mark_numeric_conditions_not_evaluated(
                    left,
                    &format!("{ast_path}:left"),
                    reason,
                );
                self.mark_numeric_conditions_not_evaluated(
                    right,
                    &format!("{ast_path}:right"),
                    reason,
                );
            }
            NumericExpr::Abs { value } => self.mark_numeric_conditions_not_evaluated(
                value,
                &format!("{ast_path}:value"),
                reason,
            ),
            NumericExpr::Clamp { value, min, max } => {
                self.mark_numeric_conditions_not_evaluated(
                    value,
                    &format!("{ast_path}:value"),
                    reason,
                );
                self.mark_numeric_conditions_not_evaluated(min, &format!("{ast_path}:min"), reason);
                self.mark_numeric_conditions_not_evaluated(max, &format!("{ast_path}:max"), reason);
            }
            NumericExpr::If {
                condition,
                then_value,
                else_value,
            } => {
                self.mark_not_evaluated(condition, &format!("{ast_path}:if"), reason);
                self.mark_numeric_conditions_not_evaluated(
                    then_value,
                    &format!("{ast_path}:then"),
                    reason,
                );
                self.mark_numeric_conditions_not_evaluated(
                    else_value,
                    &format!("{ast_path}:else"),
                    reason,
                );
            }
            NumericExpr::Constant { .. }
            | NumericExpr::Indicator { .. }
            | NumericExpr::State { .. }
            | NumericExpr::ActualWeight
            | NumericExpr::HeldBars
            | NumericExpr::EvidenceMultiplier
            | NumericExpr::EvidenceCoverage => {}
        }
    }

    fn mark_target_not_evaluated(
        &mut self,
        target: &PolicyTarget,
        ast_path: &str,
        reason: PolicyConditionReason,
    ) {
        if let PolicyTarget::Weight { value } = target {
            self.mark_numeric_conditions_not_evaluated(value, ast_path, reason);
        }
    }

    fn outcome_at(&self, ast_path: &str) -> Option<&PolicyConditionOutcome> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.ast_path == ast_path)
            .map(|entry| &entry.outcome)
    }

    fn engine_result_at(&self, ast_path: &str) -> Option<bool> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.ast_path == ast_path)
            .and_then(|entry| entry.engine_result)
    }

    fn bool_is_unknown(&self, expr: &BoolExpr, ast_path: &str) -> bool {
        match expr {
            BoolExpr::Compare { left, right, .. } => {
                self.numeric_is_unknown(left, &format!("{ast_path}:left"))
                    || self.numeric_is_unknown(right, &format!("{ast_path}:right"))
            }
            BoolExpr::And { conditions } => {
                let mut saw_unknown = false;
                for index in 0..conditions.len() {
                    let child_path = format!("{ast_path}:and:{index}");
                    match self.outcome_at(&child_path) {
                        Some(PolicyConditionOutcome::False) => return false,
                        Some(PolicyConditionOutcome::Unknown(_)) => saw_unknown = true,
                        _ => {}
                    }
                }
                saw_unknown
            }
            BoolExpr::Or { conditions } => {
                let mut saw_unknown = false;
                for index in 0..conditions.len() {
                    let child_path = format!("{ast_path}:or:{index}");
                    match self.outcome_at(&child_path) {
                        Some(PolicyConditionOutcome::True) => return false,
                        Some(PolicyConditionOutcome::Unknown(_)) => saw_unknown = true,
                        _ => {}
                    }
                }
                saw_unknown
            }
            BoolExpr::Not { .. } => matches!(
                self.outcome_at(&format!("{ast_path}:not")),
                Some(PolicyConditionOutcome::Unknown(_))
            ),
            BoolExpr::CrossesAbove { .. } | BoolExpr::CrossesBelow { .. } => matches!(
                self.outcome_at(ast_path),
                Some(PolicyConditionOutcome::Unknown(_))
            ),
            BoolExpr::PositionIs { .. } => false,
        }
    }

    fn numeric_is_unknown(&self, expr: &NumericExpr, ast_path: &str) -> bool {
        match expr {
            NumericExpr::Add { left, right }
            | NumericExpr::Sub { left, right }
            | NumericExpr::Mul { left, right }
            | NumericExpr::Div { left, right }
            | NumericExpr::Min { left, right }
            | NumericExpr::Max { left, right } => {
                self.numeric_is_unknown(left, &format!("{ast_path}:left"))
                    || self.numeric_is_unknown(right, &format!("{ast_path}:right"))
            }
            NumericExpr::Abs { value } => {
                self.numeric_is_unknown(value, &format!("{ast_path}:value"))
            }
            NumericExpr::Clamp { value, min, max } => {
                self.numeric_is_unknown(value, &format!("{ast_path}:value"))
                    || self.numeric_is_unknown(min, &format!("{ast_path}:min"))
                    || self.numeric_is_unknown(max, &format!("{ast_path}:max"))
            }
            NumericExpr::If {
                then_value,
                else_value,
                ..
            } => {
                let condition_path = format!("{ast_path}:if");
                if matches!(
                    self.outcome_at(&condition_path),
                    Some(PolicyConditionOutcome::Unknown(_))
                ) {
                    return true;
                }
                self.engine_result_at(&condition_path)
                    .is_some_and(|selected| {
                        if selected {
                            self.numeric_is_unknown(then_value, &format!("{ast_path}:then"))
                        } else {
                            self.numeric_is_unknown(else_value, &format!("{ast_path}:else"))
                        }
                    })
            }
            NumericExpr::Constant { .. }
            | NumericExpr::Indicator { .. }
            | NumericExpr::State { .. }
            | NumericExpr::ActualWeight
            | NumericExpr::HeldBars
            | NumericExpr::EvidenceMultiplier
            | NumericExpr::EvidenceCoverage => false,
        }
    }

    fn unknown_reason(&self, expr: &BoolExpr, ast_path: &str) -> Option<PolicyConditionReason> {
        self.bool_is_unknown(expr, ast_path)
            .then_some(PolicyConditionReason::CrossHistoryUnavailable)
    }

    fn record(
        &mut self,
        ast_path: &str,
        outcome: PolicyConditionOutcome,
        engine_result: Option<bool>,
        evidence: Option<PolicyConditionEvidence>,
    ) {
        let role = if matches!(&self.scope, PolicyConditionScope::RuleCondition { .. }) {
            self.roles
                .iter()
                .find(|declaration| declaration.ast_path == ast_path)
                .map_or(PolicyConditionRole::Unspecified, |declaration| {
                    declaration.role
                })
        } else {
            PolicyConditionRole::Unspecified
        };
        self.entries.push(PolicyConditionTrace {
            scope: self.scope.clone(),
            ast_path: ast_path.to_owned(),
            role,
            outcome,
            engine_result,
            evidence,
        });
    }
}

fn eval_target(
    target: &PolicyTarget,
    context: &EvalContext<'_>,
    path: &str,
    trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<f64, LabError> {
    match target {
        PolicyTarget::Hold => Ok(context.actual_weight),
        PolicyTarget::Weight { value } => eval_numeric(value, context, path, trace),
    }
}

fn eval_numeric(
    expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    trace: Option<&mut ConditionTraceCollector<'_>>,
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
        NumericExpr::Add { .. }
        | NumericExpr::Sub { .. }
        | NumericExpr::Mul { .. }
        | NumericExpr::Div { .. }
        | NumericExpr::Min { .. }
        | NumericExpr::Max { .. } => eval_numeric_binary(expr, context, path, trace)?,
        NumericExpr::Abs { value } => {
            eval_numeric(value, context, &format!("{path}:value"), trace)?.abs()
        }
        NumericExpr::Clamp { value, min, max } => {
            eval_numeric_clamp(value, min, max, context, path, trace)?
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => eval_numeric_if(condition, then_value, else_value, context, path, trace)?,
    };
    finite(value, "policy numeric expression")?;
    Ok(value)
}

fn eval_numeric_binary(
    expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<f64, LabError> {
    let (NumericExpr::Add { left, right }
    | NumericExpr::Sub { left, right }
    | NumericExpr::Mul { left, right }
    | NumericExpr::Div { left, right }
    | NumericExpr::Min { left, right }
    | NumericExpr::Max { left, right }) = expr
    else {
        return Err(LabError::Internal(
            "non-binary policy expression reached binary evaluator".into(),
        ));
    };
    let left_value = eval_numeric(left, context, &format!("{path}:left"), trace.as_deref_mut())?;
    let right_value = eval_numeric(right, context, &format!("{path}:right"), trace)?;
    match expr {
        NumericExpr::Add { .. } => Ok(left_value + right_value),
        NumericExpr::Sub { .. } => Ok(left_value - right_value),
        NumericExpr::Mul { .. } => Ok(left_value * right_value),
        NumericExpr::Div { .. } => {
            if right_value == 0.0 {
                return Err(LabError::InvalidConfig("policy division by zero".into()));
            }
            Ok(left_value / right_value)
        }
        NumericExpr::Min { .. } => Ok(left_value.min(right_value)),
        NumericExpr::Max { .. } => Ok(left_value.max(right_value)),
        _ => Err(LabError::Internal(
            "non-binary policy expression reached binary evaluator".into(),
        )),
    }
}

fn eval_numeric_clamp(
    value: &NumericExpr,
    min: &NumericExpr,
    max: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<f64, LabError> {
    let value = eval_numeric(
        value,
        context,
        &format!("{path}:value"),
        trace.as_deref_mut(),
    )?;
    let min = eval_numeric(min, context, &format!("{path}:min"), trace.as_deref_mut())?;
    let max = eval_numeric(max, context, &format!("{path}:max"), trace)?;
    if min > max {
        return Err(LabError::InvalidConfig(
            "policy clamp min exceeds max".into(),
        ));
    }
    Ok(value.clamp(min, max))
}

fn eval_numeric_if(
    condition: &BoolExpr,
    then_value: &NumericExpr,
    else_value: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<f64, LabError> {
    let selected = eval_bool(
        condition,
        context,
        &format!("{path}:if"),
        trace.as_deref_mut(),
    )?;
    if selected {
        if let Some(collector) = trace.as_deref_mut() {
            collector.mark_numeric_conditions_not_evaluated(
                else_value,
                &format!("{path}:else"),
                PolicyConditionReason::IfBranchNotSelected,
            );
        }
        eval_numeric(then_value, context, &format!("{path}:then"), trace)
    } else {
        if let Some(collector) = trace.as_deref_mut() {
            collector.mark_numeric_conditions_not_evaluated(
                then_value,
                &format!("{path}:then"),
                PolicyConditionReason::IfBranchNotSelected,
            );
        }
        eval_numeric(else_value, context, &format!("{path}:else"), trace)
    }
}

struct CrossEvaluation {
    engine_result: bool,
    history_available: bool,
    previous: Option<(f64, f64)>,
    current: (f64, f64),
}

fn eval_bool(
    expr: &BoolExpr,
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<bool, LabError> {
    let mut evidence = None;
    let result = match expr {
        BoolExpr::Compare {
            comparison,
            left,
            right,
        } => {
            let (result, captured) = eval_bool_comparison(
                *comparison,
                left,
                right,
                context,
                path,
                trace.as_deref_mut(),
            )?;
            evidence = captured;
            result
        }
        BoolExpr::And { conditions } => {
            eval_bool_and(conditions, context, path, trace.as_deref_mut())?
        }
        BoolExpr::Or { conditions } => {
            eval_bool_or(conditions, context, path, trace.as_deref_mut())?
        }
        BoolExpr::Not { condition } => !eval_bool(
            condition,
            context,
            &format!("{path}:not"),
            trace.as_deref_mut(),
        )?,
        BoolExpr::CrossesAbove { .. } | BoolExpr::CrossesBelow { .. } => {
            return eval_bool_cross(expr, context, path, trace);
        }
        BoolExpr::PositionIs { state } => {
            let actual = context.actual_state;
            let result = actual == *state;
            if trace.is_some() {
                evidence = Some(PolicyConditionEvidence::Position {
                    expected: *state,
                    actual,
                });
            }
            result
        }
    };
    let unknown_reason = trace
        .as_ref()
        .and_then(|collector| collector.unknown_reason(expr, path));
    if let Some(collector) = trace {
        collector.record_evaluated(path, result, unknown_reason, evidence);
    }
    Ok(result)
}

#[expect(
    clippy::float_cmp,
    reason = "EQ is the explicit exact numeric DSL operation; adding an epsilon changes user policy"
)]
fn eval_bool_comparison(
    comparison: CompareOp,
    left_expr: &NumericExpr,
    right_expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<(bool, Option<PolicyConditionEvidence>), LabError> {
    let left = eval_numeric(
        left_expr,
        context,
        &format!("{path}:left"),
        trace.as_deref_mut(),
    )?;
    let right = eval_numeric(
        right_expr,
        context,
        &format!("{path}:right"),
        trace.as_deref_mut(),
    )?;
    let result = match comparison {
        CompareOp::Gt => left > right,
        CompareOp::Gte => left >= right,
        CompareOp::Lt => left < right,
        CompareOp::Lte => left <= right,
        CompareOp::Eq => left == right,
    };
    let evidence = if trace.is_some() {
        Some(PolicyConditionEvidence::Comparison {
            left: FinitePolicyNumber::new(left)?,
            comparison,
            right: FinitePolicyNumber::new(right)?,
        })
    } else {
        None
    };
    Ok((result, evidence))
}

fn eval_bool_and(
    conditions: &[BoolExpr],
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<bool, LabError> {
    for (index, condition) in conditions.iter().enumerate() {
        let condition_path = format!("{path}:and:{index}");
        if !eval_bool(condition, context, &condition_path, trace.as_deref_mut())? {
            if let Some(collector) = trace.as_deref_mut() {
                for (skipped_index, skipped) in conditions.iter().enumerate().skip(index + 1) {
                    collector.mark_not_evaluated(
                        skipped,
                        &format!("{path}:and:{skipped_index}"),
                        PolicyConditionReason::AndShortCircuit,
                    );
                }
            }
            return Ok(false);
        }
    }
    Ok(true)
}

fn eval_bool_or(
    conditions: &[BoolExpr],
    context: &EvalContext<'_>,
    path: &str,
    mut trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<bool, LabError> {
    for (index, condition) in conditions.iter().enumerate() {
        let condition_path = format!("{path}:or:{index}");
        if eval_bool(condition, context, &condition_path, trace.as_deref_mut())? {
            if let Some(collector) = trace.as_deref_mut() {
                for (skipped_index, skipped) in conditions.iter().enumerate().skip(index + 1) {
                    collector.mark_not_evaluated(
                        skipped,
                        &format!("{path}:or:{skipped_index}"),
                        PolicyConditionReason::OrShortCircuit,
                    );
                }
            }
            return Ok(true);
        }
    }
    Ok(false)
}

fn eval_bool_cross(
    expr: &BoolExpr,
    context: &EvalContext<'_>,
    path: &str,
    trace: Option<&mut ConditionTraceCollector<'_>>,
) -> Result<bool, LabError> {
    let above = matches!(expr, BoolExpr::CrossesAbove { .. });
    let evaluation = cross_value(context, path, above)?;
    if let Some(collector) = trace {
        let unknown_reason = (!evaluation.history_available)
            .then_some(PolicyConditionReason::CrossHistoryUnavailable);
        let evidence = PolicyConditionEvidence::Crossing {
            previous: evaluation.previous.map(policy_operand_pair).transpose()?,
            current: policy_operand_pair(evaluation.current)?,
        };
        collector.record_evaluated(
            path,
            evaluation.engine_result,
            unknown_reason,
            Some(evidence),
        );
    }
    Ok(evaluation.engine_result)
}

fn policy_operand_pair(values: (f64, f64)) -> Result<PolicyOperandPair, LabError> {
    Ok(PolicyOperandPair {
        left: FinitePolicyNumber::new(values.0)?,
        right: FinitePolicyNumber::new(values.1)?,
    })
}

fn cross_value(
    context: &EvalContext<'_>,
    path: &str,
    above: bool,
) -> Result<CrossEvaluation, LabError> {
    let sample = context
        .crosses
        .get(path)
        .ok_or_else(|| LabError::Internal(format!("policy cross sample absent: {path}")))?;
    let Some(previous) = sample.previous else {
        return Ok(CrossEvaluation {
            engine_result: false,
            history_available: false,
            previous: None,
            current: sample.current,
        });
    };
    Ok(CrossEvaluation {
        engine_result: if above {
            previous.0 <= previous.1 && sample.current.0 > sample.current.1
        } else {
            previous.0 >= previous.1 && sample.current.0 < sample.current.1
        },
        history_available: true,
        previous: Some(previous),
        current: sample.current,
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
                    eval_numeric(left, &current_context, &format!("{path}:left"), None)?,
                    eval_numeric(right, &current_context, &format!("{path}:right"), None)?,
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
