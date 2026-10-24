//! Pure event-linked candidate analysis using frozen source facts and variants.

#[cfg(test)]
mod tests;

use crate::contracts::{
    AccountMark, ArbitrationPolicy, AssetQuantity, COUNTERFACTUAL_CONTRACT_VERSION, CandleInterval,
    CandleObservation, ContentHash, CostPolicy, CounterfactualAnalysisRequest,
    CounterfactualCandidateIdentity, CounterfactualCandidateInput, CounterfactualCandidateReport,
    CounterfactualCapitalScope, CounterfactualEvidenceInput, CounterfactualExecution,
    CounterfactualExecutionScope, CounterfactualPnlScope, CounterfactualReport,
    CounterfactualReportMetadata, CounterfactualReportStatus, CounterfactualUnavailableReason,
    CounterfactualValuation, CounterfactualValuationKind, CounterfactualVariant,
    CounterfactualVariantOutcome, CounterfactualVariantStatus, ExecutionPolicy, FillId, LabError,
    MAX_COUNTERFACTUAL_HOLD_BARS, MAX_COUNTERFACTUAL_VALUATION_BARS, MarketId, MarketRuleSnapshot,
    PaperEventKind, PortfolioAssetSpec, PortfolioSpec, PositionState, PriceKrw, QuoteAmount,
    ReasonCode, Side, SignalRecord, SignedAmount, UtcRange, UtcTimestamp, Weight,
};
use crate::engine::accounting::{Account, checked_div, checked_mul};
use crate::engine::execution::{plan_market_arrival, plan_market_request};
use crate::policy_engine::PolicyEvaluator;
use crate::research::EvidenceEvaluator;
use crate::strategy::PositionView;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};

/// Analyze saved candidate events against bounded frozen policy variants.
///
/// Every candidate and policy variant gets an independent virtual account. The
/// only completed bars used for policy decisions end at the source signal time;
/// later bars are limited to next-open execution and the declared ex-post mark.
///
/// # Errors
/// Rejects invalid source identities, inconsistent saved facts, incomparable
/// variant scopes, malformed rules, and arithmetic/evaluator failures.
pub(crate) fn analyze(
    request: &CounterfactualAnalysisRequest,
) -> Result<CounterfactualReport, LabError> {
    request.validate_bounds()?;
    let validation_market = request
        .candidates
        .first()
        .and_then(|candidate| candidate.source_event.market.as_ref())
        .ok_or_else(|| LabError::DataGap("first candidate source event has no market".into()))?;
    if request
        .candidates
        .iter()
        .any(|candidate| candidate.source_event.market.as_ref() != Some(validation_market))
    {
        return Err(LabError::InvalidConfig(
            "counterfactual analysis supports exactly one market per request".into(),
        ));
    }
    validate_variants(&request.variants, validation_market)?;

    let variants = sorted_variants(&request.variants);
    let common_exit = variants
        .first()
        .map(|variant| variant.exit)
        .ok_or_else(|| LabError::InvalidConfig("counterfactual variants are empty".into()))?;
    let mut prepared = request
        .candidates
        .iter()
        .map(prepare_candidate)
        .collect::<Result<Vec<_>, _>>()?;
    prepared.sort_by(|left, right| {
        left.input
            .source_session_id
            .cmp(&right.input.source_session_id)
            .then_with(|| left.market.code().cmp(&right.market.code()))
            .then_with(|| left.signal.signal_time.cmp(&right.signal.signal_time))
            .then_with(|| {
                left.input
                    .source_event
                    .sequence
                    .cmp(&right.input.source_event.sequence)
            })
            .then_with(|| left.signal.signal_id.cmp(&right.signal.signal_id))
    });
    let mut candidate_identities = BTreeSet::new();
    if prepared
        .iter()
        .any(|candidate| !candidate_identities.insert(candidate.identity.digest.clone()))
    {
        return Err(LabError::Conflict(
            "duplicate exact counterfactual candidate identity".into(),
        ));
    }
    let overlapping = overlapping_candidate_windows(&prepared, &variants, common_exit);

    let variant_digests = variants
        .iter()
        .map(variant_digest)
        .collect::<Result<Vec<_>, _>>()?;
    let (candidate_reports, has_unavailable) =
        analyze_candidate_reports(&prepared, &variants, &variant_digests, &overlapping)?;

    let contract_digest = ContentHash::of_value(&(
        COUNTERFACTUAL_CONTRACT_VERSION,
        candidate_reports
            .iter()
            .map(|candidate| candidate.identity.digest.clone())
            .collect::<Vec<_>>(),
        variant_digests,
    ))?;
    Ok(CounterfactualReport {
        status: if has_unavailable {
            CounterfactualReportStatus::Partial
        } else {
            CounterfactualReportStatus::Complete
        },
        metadata: CounterfactualReportMetadata {
            contract_version: COUNTERFACTUAL_CONTRACT_VERSION.into(),
            execution_scope: CounterfactualExecutionScope::VirtualOnly,
            pnl_scope: CounterfactualPnlScope::HypotheticalStandaloneScenario,
            capital_scope: CounterfactualCapitalScope::IndependentPerCandidateAndVariant,
            candidate_roles_are_caller_declared: true,
            source_facts_are_preserved: true,
        },
        contract_digest,
        variants,
        candidates: candidate_reports,
    })
}

fn analyze_candidate_reports(
    prepared: &[PreparedCandidate<'_>],
    variants: &[CounterfactualVariant],
    variant_digests: &[ContentHash],
    overlapping: &BTreeSet<usize>,
) -> Result<(Vec<CounterfactualCandidateReport>, bool), LabError> {
    let mut candidate_reports = Vec::with_capacity(prepared.len());
    let mut has_unavailable = false;
    for (candidate_index, candidate) in prepared.iter().enumerate() {
        let variant_outcomes = variants
            .iter()
            .zip(variant_digests)
            .map(|(variant, digest)| {
                let outcome = if overlapping.contains(&candidate_index) {
                    unavailable_outcome(
                        variant,
                        digest,
                        CounterfactualVariantStatus::Unsupported,
                        CounterfactualUnavailableReason::OverlappingCandidateWindow,
                        "candidate valuation window overlaps another candidate in the same session and market",
                    )
                } else {
                    analyze_variant(candidate, variant, digest)?
                };
                if is_unavailable_status(outcome.status) {
                    has_unavailable = true;
                }
                Ok(outcome)
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        candidate_reports.push(CounterfactualCandidateReport {
            identity: candidate.identity.clone(),
            source_configuration: candidate.input.source_configuration.clone(),
            source_signal: candidate.signal.clone(),
            source_actual_order_ids: candidate.actual_order_ids.clone(),
            source_actual_fill_ids: candidate.actual_fill_ids.clone(),
            variant_outcomes,
        });
    }
    Ok((candidate_reports, has_unavailable))
}

struct PreparedCandidate<'a> {
    input: &'a CounterfactualCandidateInput,
    identity: CounterfactualCandidateIdentity,
    signal: &'a SignalRecord,
    market: MarketId,
    source_state_mark: Option<&'a AccountMark>,
    source_event_candle_available: bool,
    bars_by_interval: IntervalBars<'a>,
    actual_order_ids: Vec<crate::contracts::OrderId>,
    actual_fill_ids: Vec<FillId>,
}

type IntervalBars<'a> = BTreeMap<i64, Vec<&'a CandleObservation>>;

fn prepare_candidate(
    input: &CounterfactualCandidateInput,
) -> Result<PreparedCandidate<'_>, LabError> {
    let market = input
        .source_event
        .market
        .clone()
        .ok_or_else(|| LabError::DataGap("source paper event has no market".into()))?;
    let bar_index = validate_candidate_bars(input, &market)?;
    let signal = select_source_signal(input, &market)?;
    let source_decision_bar = select_source_decision_bar(input, signal, &market, &bar_index)?;

    let source_event_candle_available = event_candle_is_available(input, &market)?;
    let source_state_mark = source_state_mark(input, signal, &market, source_decision_bar)?;
    let identity = candidate_identity(input, signal, source_decision_bar, source_state_mark)?;
    let (actual_order_ids, actual_fill_ids) = actual_source_trade_ids(input);
    Ok(PreparedCandidate {
        input,
        identity,
        signal,
        market,
        source_state_mark,
        source_event_candle_available,
        bars_by_interval: bar_index,
        actual_order_ids,
        actual_fill_ids,
    })
}

fn select_source_signal<'a>(
    input: &'a CounterfactualCandidateInput,
    market: &MarketId,
) -> Result<&'a SignalRecord, LabError> {
    if input.source_event.kind != PaperEventKind::CandleProcessed {
        return Err(LabError::InvalidConfig(
            "counterfactual candidate must reference a saved candle event".into(),
        ));
    }
    let matching: Vec<_> = input
        .source_event
        .signals
        .iter()
        .filter(|signal| signal.signal_id == input.source_signal_id)
        .collect();
    let signal = match matching.as_slice() {
        [signal] => *signal,
        [] => {
            return Err(LabError::DataGap(
                "selected signal is absent from the exact source event".into(),
            ));
        }
        _ => {
            return Err(LabError::DataCorrupt(
                "source event repeats the selected signal identity".into(),
            ));
        }
    };
    validate_source_signal(input, signal, market)?;
    Ok(signal)
}

fn validate_source_signal(
    input: &CounterfactualCandidateInput,
    signal: &SignalRecord,
    market: &MarketId,
) -> Result<(), LabError> {
    if signal.policy_ref.as_ref() != Some(&input.source_configuration.policy_ref) {
        return Err(LabError::InputHashMismatch(
            "source signal policy reference differs from saved session configuration".into(),
        ));
    }
    if &signal.context.market != market {
        return Err(LabError::DataCorrupt(
            "source signal market differs from its paper event".into(),
        ));
    }
    let configured_market_count = input
        .source_configuration
        .markets
        .iter()
        .filter(|configured| &configured.market == market)
        .count();
    if configured_market_count != 1 {
        return Err(LabError::InputHashMismatch(
            "source event market is not uniquely configured in its saved session".into(),
        ));
    }
    if signal.decision_available_at != signal.signal_time {
        return Err(LabError::DataCorrupt(
            "counterfactual decision time must equal the saved signal availability time".into(),
        ));
    }
    Ok(())
}

fn select_source_decision_bar<'a>(
    input: &CounterfactualCandidateInput,
    signal: &SignalRecord,
    market: &MarketId,
    bars_by_interval: &IntervalBars<'a>,
) -> Result<&'a CandleObservation, LabError> {
    let source_bars = bars_by_interval
        .get(&interval_key(input.source_configuration.decision_interval))
        .ok_or_else(|| LabError::DataGap("source decision interval bars are missing".into()))?;
    let source_ids: BTreeSet<_> = signal.source_bar_ids.iter().collect();
    let matching: Vec<_> = source_bars
        .iter()
        .copied()
        .filter(|bar| {
            bar.candle.close_time_utc == signal.signal_time && source_ids.contains(&bar.id)
        })
        .collect();
    let decision_bar = match matching.as_slice() {
        [bar] => *bar,
        [] => {
            return Err(LabError::DataGap(
                "signal source bar identity is missing from the supplied decision bars".into(),
            ));
        }
        _ => {
            return Err(LabError::DataCorrupt(
                "signal source-bar IDs select more than one decision bar".into(),
            ));
        }
    };
    if decision_bar.candle.market != market.code() {
        return Err(LabError::DataCorrupt(
            "source decision bar market differs from the paper event".into(),
        ));
    }
    Ok(decision_bar)
}

fn event_candle_is_available(
    input: &CounterfactualCandidateInput,
    market: &MarketId,
) -> Result<bool, LabError> {
    let Some(reference) = &input.source_event.candle else {
        return Ok(false);
    };
    if reference.interval != input.source_configuration.execution_resolution {
        return Err(LabError::InputHashMismatch(
            "paper event candle interval differs from saved execution resolution".into(),
        ));
    }
    let matching: Vec<_> = input
        .bars
        .iter()
        .filter(|bar| bar.id == reference.observation_id)
        .collect();
    match matching.as_slice() {
        [bar]
            if bar.content_digest == reference.content_digest
                && bar.candle.interval == reference.interval
                && bar.candle.open_time_utc == reference.open_time
                && bar.candle.close_time_utc == reference.close_time
                && bar.candle.market == market.code() =>
        {
            Ok(true)
        }
        [] => Ok(false),
        [_] => Err(LabError::InputHashMismatch(
            "saved paper event candle differs from its supplied observation".into(),
        )),
        _ => Err(LabError::DataCorrupt(
            "supplied bars repeat the paper event candle identity".into(),
        )),
    }
}

fn source_state_mark<'a>(
    input: &'a CounterfactualCandidateInput,
    signal: &SignalRecord,
    market: &MarketId,
    decision_bar: &CandleObservation,
) -> Result<Option<&'a AccountMark>, LabError> {
    let mark = input
        .source_event
        .account_marks
        .iter()
        .filter(|mark| {
            mark.source_bar_id == decision_bar.id
                && mark.context.event_seq < signal.context.event_seq
                && mark.context.run_id == signal.context.run_id
                && mark.context.model_id == signal.context.model_id
                && &mark.context.market == market
        })
        .max_by_key(|mark| mark.context.event_seq);
    if let Some(mark) = mark {
        validate_source_state_mark(mark, signal)?;
    }
    Ok(mark)
}

fn candidate_identity(
    input: &CounterfactualCandidateInput,
    signal: &SignalRecord,
    decision_bar: &CandleObservation,
    state_mark: Option<&AccountMark>,
) -> Result<CounterfactualCandidateIdentity, LabError> {
    let source_event_digest = ContentHash::of_value(&input.source_event)?;
    let source_signal_digest = ContentHash::of_value(signal)?;
    let source_configuration_digest = ContentHash::of_value(&input.source_configuration)?;
    let source_state_mark_digest = ContentHash::of_value(&state_mark)?;
    let evidence_digest = ContentHash::of_value(&input.evidence)?;
    let mut canonical_bars: Vec<_> = input.bars.iter().collect();
    canonical_bars.sort_by(|left, right| {
        interval_key(left.candle.interval)
            .cmp(&interval_key(right.candle.interval))
            .then_with(|| left.candle.open_time_utc.cmp(&right.candle.open_time_utc))
            .then_with(|| left.id.cmp(&right.id))
    });
    let bar_evidence_digest = ContentHash::of_value(&canonical_bars)?;
    let digest = ContentHash::of_value(&(
        &input.source_session_id,
        input.source_event.sequence,
        &input.source_signal_id,
        &input.source_configuration.policy_ref,
        &source_event_digest,
        &source_signal_digest,
        &source_configuration_digest,
        signal.state_before,
        signal.state_after,
        &decision_bar.id,
        &source_state_mark_digest,
        &evidence_digest,
        &bar_evidence_digest,
        &input.declaration,
    ))?;
    Ok(CounterfactualCandidateIdentity {
        digest,
        source_session_id: input.source_session_id.clone(),
        source_event_sequence: input.source_event.sequence,
        source_signal_id: input.source_signal_id.clone(),
        source_policy_ref: input.source_configuration.policy_ref.clone(),
        source_event_digest,
        source_signal_digest,
        source_configuration_digest,
        source_state_before: signal.state_before,
        source_state_after: signal.state_after,
        source_decision_bar_id: decision_bar.id.clone(),
        source_state_mark_digest,
        evidence_digest,
        bar_evidence_digest,
        declaration: input.declaration.clone(),
    })
}

fn validate_source_state_mark(mark: &AccountMark, signal: &SignalRecord) -> Result<(), LabError> {
    let marked_state = if mark.state.qty.get().is_zero() {
        PositionState::Cash
    } else {
        PositionState::Long
    };
    if marked_state != signal.state_before
        || (mark.state.qty.get().is_zero() != mark.actual_weight.get().is_zero())
    {
        return Err(LabError::DataCorrupt(
            "source state mark does not match the saved signal state".into(),
        ));
    }
    Ok(())
}

fn validate_candidate_bars<'a>(
    input: &'a CounterfactualCandidateInput,
    market: &MarketId,
) -> Result<IntervalBars<'a>, LabError> {
    let mut by_interval = IntervalBars::new();
    let mut ids = BTreeSet::new();
    let mut slots = BTreeSet::new();
    for bar in &input.bars {
        let key = interval_key(bar.candle.interval);
        if bar.candle.market != market.code() || !bar.candle.completed {
            return Err(LabError::DataCorrupt(
                "counterfactual bars must be completed and use the exact source market".into(),
            ));
        }
        if !ids.insert(bar.id.clone()) || !slots.insert((key, bar.candle.open_time_utc)) {
            return Err(LabError::Conflict(
                "counterfactual bars repeat an observation identity or interval slot".into(),
            ));
        }
        validate_candle_shape(bar)?;
        by_interval.entry(key).or_default().push(bar);
    }
    for bars in by_interval.values_mut() {
        bars.sort_by_key(|bar| bar.candle.open_time_utc);
        for pair in bars.windows(2) {
            if pair[0].candle.close_time_utc != pair[1].candle.open_time_utc {
                return Err(LabError::DataGap(format!(
                    "counterfactual bars contain a gap in {:?}",
                    pair[0].candle.interval
                )));
            }
        }
    }
    Ok(by_interval)
}

fn validate_candle_shape(bar: &CandleObservation) -> Result<(), LabError> {
    let candle = &bar.candle;
    let duration = candle.interval.duration();
    let open = candle.open.get();
    let high = candle.high.get();
    let low = candle.low.get();
    let close = candle.close.get();
    let duration_seconds = duration.num_seconds();
    if duration_seconds <= 0
        || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
        || candle
            .open_time_utc
            .0
            .timestamp()
            .rem_euclid(duration_seconds)
            != 0
        || candle.close_time_utc.0 != candle.open_time_utc.0 + duration
        || high < open.max(close)
        || low > open.min(close)
        || high < low
        || candle.volume.get() < Decimal::ZERO
        || candle.quote_turnover.get() < Decimal::ZERO
    {
        return Err(LabError::DataCorrupt(
            "counterfactual bar has invalid time grid or OHLCV shape".into(),
        ));
    }
    Ok(())
}

fn validate_variants(
    variants: &[CounterfactualVariant],
    validation_market: &MarketId,
) -> Result<(), LabError> {
    let mut ids = BTreeSet::new();
    let Some(first) = variants.first() else {
        return Err(LabError::InvalidConfig(
            "counterfactual variants are empty".into(),
        ));
    };
    let first_scenario = variant_scenario_digest(first)?;
    for variant in variants {
        if variant.variant_id.is_empty()
            || variant.variant_id.len() > 96
            || !ids.insert(variant.variant_id.as_str())
        {
            return Err(LabError::InvalidConfig(
                "counterfactual variant IDs must be unique and bounded".into(),
            ));
        }
        if variant.exit.hold_decision_bars == 0
            || variant.exit.hold_decision_bars > MAX_COUNTERFACTUAL_HOLD_BARS
            || variant.exit.valuation_bars_after_exit == 0
            || variant.exit.valuation_bars_after_exit > MAX_COUNTERFACTUAL_VALUATION_BARS
        {
            return Err(LabError::InvalidConfig(format!(
                "fixed exit requires hold bars in 1..={MAX_COUNTERFACTUAL_HOLD_BARS} and valuation bars in 1..={MAX_COUNTERFACTUAL_VALUATION_BARS}"
            )));
        }
        if variant_scenario_digest(variant)? != first_scenario {
            return Err(LabError::Conflict(
                "counterfactual variants must share capital, risk, cost, execution, interval, rules, PIT and exit assumptions".into(),
            ));
        }
        if variant.policy.reference.definition_digest
            != ContentHash::of_value(&variant.policy.definition)?
        {
            return Err(LabError::InputHashMismatch(
                "variant policy definition digest differs from its frozen reference".into(),
            ));
        }
        let risk_spec = PortfolioSpec {
            initial_cash: variant.initial_capital,
            assets: vec![PortfolioAssetSpec {
                market: validation_market.clone(),
                max_weight: Weight::new(Decimal::ONE)?,
            }],
            risk: variant.risk.clone(),
            arbitration: ArbitrationPolicy::Priority,
        };
        risk_spec.validate()?;
    }
    Ok(())
}

fn variant_scenario_digest(variant: &CounterfactualVariant) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&(
        &variant.initial_capital,
        variant.capital_mode,
        &variant.risk,
        &variant.costs,
        &variant.execution,
        variant.decision_interval,
        variant.execution_resolution,
        variant.latency_ms,
        &variant.market_rules,
        variant.pit_policy,
        variant.exit,
    ))
}

fn variant_digest(variant: &CounterfactualVariant) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&(
        &variant.variant_id,
        &variant.policy,
        variant_scenario_digest(variant)?,
    ))
}

fn sorted_variants(variants: &[CounterfactualVariant]) -> Vec<CounterfactualVariant> {
    let mut result = variants.to_vec();
    result.sort_by(|left, right| left.variant_id.cmp(&right.variant_id));
    result
}

fn actual_source_trade_ids(
    candidate: &CounterfactualCandidateInput,
) -> (Vec<crate::contracts::OrderId>, Vec<FillId>) {
    let mut order_ids = BTreeSet::new();
    for order in candidate
        .source_event
        .orders
        .iter()
        .chain(candidate.source_event.order_events.iter())
        .filter(|order| order.parent_signal_id == candidate.source_signal_id)
    {
        order_ids.insert(order.order_id.clone());
    }
    let fill_ids = candidate
        .source_event
        .fills
        .iter()
        .filter(|fill| order_ids.contains(&fill.order_id))
        .map(|fill| fill.fill_id.clone())
        .collect::<BTreeSet<_>>();
    (
        order_ids.into_iter().collect(),
        fill_ids.into_iter().collect(),
    )
}

fn overlapping_candidate_windows(
    candidates: &[PreparedCandidate<'_>],
    variants: &[CounterfactualVariant],
    exit: crate::contracts::CounterfactualFixedExit,
) -> BTreeSet<usize> {
    let Some(variant) = variants.first() else {
        return BTreeSet::new();
    };
    let mut groups = BTreeMap::<
        (crate::contracts::PaperSessionId, String),
        Vec<(usize, UtcTimestamp, UtcTimestamp)>,
    >::new();
    for (index, candidate) in candidates.iter().enumerate() {
        let decision_bars = candidate
            .bars_by_interval
            .get(&interval_key(variant.decision_interval));
        let execution_bars = candidate
            .bars_by_interval
            .get(&interval_key(variant.execution_resolution));
        let Some(end) = decision_bars
            .and_then(|bars| {
                unique_close_index(bars, candidate.signal.signal_time)
                    .ok()
                    .map(|idx| (bars, idx))
            })
            .and_then(|(bars, index)| fixed_window_end(bars, execution_bars?, index, exit).ok())
        else {
            continue;
        };
        groups
            .entry((
                candidate.input.source_session_id.clone(),
                candidate.market.code(),
            ))
            .or_default()
            .push((index, candidate.signal.signal_time, end));
    }
    let mut overlapping = BTreeSet::new();
    for windows in groups.values_mut() {
        windows.sort_by_key(|(_, start, end)| (*start, *end));
        for left in 0..windows.len() {
            for right in left + 1..windows.len() {
                let (left_index, left_start, left_end) = windows[left];
                let (right_index, right_start, right_end) = windows[right];
                if left_start < right_end && right_start < left_end {
                    overlapping.insert(left_index);
                    overlapping.insert(right_index);
                }
            }
        }
    }
    overlapping
}

fn fixed_window_end(
    decision_bars: &[&CandleObservation],
    execution_bars: &[&CandleObservation],
    decision_index: usize,
    exit: crate::contracts::CounterfactualFixedExit,
) -> Result<UtcTimestamp, LabError> {
    let exit_index = decision_index
        .checked_add(usize::try_from(exit.hold_decision_bars).map_err(|_| {
            LabError::ResourceLimit("fixed exit hold count exceeds platform capacity".into())
        })?)
        .ok_or_else(|| LabError::ResourceLimit("fixed exit decision index overflow".into()))?;
    let exit_decision = decision_bars
        .get(exit_index)
        .ok_or_else(|| LabError::DataGap("fixed exit decision bar is missing".into()))?;
    let execution_index = unique_open_index(execution_bars, exit_decision.candle.close_time_utc)?;
    let valuation_index = execution_index
        .checked_add(
            usize::try_from(exit.valuation_bars_after_exit - 1).map_err(|_| {
                LabError::ResourceLimit("valuation offset exceeds platform capacity".into())
            })?,
        )
        .ok_or_else(|| LabError::ResourceLimit("valuation index overflow".into()))?;
    execution_bars
        .get(valuation_index)
        .map(|bar| bar.candle.close_time_utc)
        .ok_or_else(|| LabError::DataGap("declared ex-post valuation bar is missing".into()))
}

fn analyze_variant(
    candidate: &PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
    variant_digest: &ContentHash,
) -> Result<CounterfactualVariantOutcome, LabError> {
    if let Some(outcome) = unsupported_candidate_outcome(candidate, variant, variant_digest) {
        return Ok(outcome);
    }
    let window = match scenario_window(candidate, variant)? {
        ScenarioWindowStage::Ready(window) => window,
        ScenarioWindowStage::Insufficient(detail) => {
            return Ok(insufficient_evidence_outcome(
                variant,
                variant_digest,
                detail,
            ));
        }
    };
    let decision = match evaluate_variant_policy(candidate, variant, &window)? {
        PolicyDecisionStage::Ready(decision) => decision,
        PolicyDecisionStage::Unsupported(reason, detail) => {
            return Ok(unavailable_outcome(
                variant,
                variant_digest,
                CounterfactualVariantStatus::Unsupported,
                reason,
                detail,
            ));
        }
        PolicyDecisionStage::EvidenceUnavailable(reason, detail) => {
            return Ok(unavailable_outcome(
                variant,
                variant_digest,
                CounterfactualVariantStatus::EvidenceUnavailable,
                reason,
                detail,
            ));
        }
        PolicyDecisionStage::Insufficient(detail) => {
            return Ok(insufficient_evidence_outcome(
                variant,
                variant_digest,
                detail,
            ));
        }
    };
    if decision.policy_target.get().is_zero() {
        return Ok(empty_execution_outcome(
            variant,
            variant_digest,
            EmptyOutcomeDetails::from_decision(
                CounterfactualVariantStatus::NotTriggered,
                &decision,
                None,
                None,
            ),
        ));
    }
    if decision.risk_target.get().is_zero() {
        return Ok(empty_execution_outcome(
            variant,
            variant_digest,
            EmptyOutcomeDetails::from_decision(
                CounterfactualVariantStatus::RiskBlocked,
                &decision,
                Some(ReasonCode::CapitalBlocked),
                None,
            ),
        ));
    }
    let mut position = match simulate_entry(variant, variant_digest, &decision, &window)? {
        EntryStage::Filled(position) => *position,
        EntryStage::Rejected(outcome) => return Ok(*outcome),
    };
    let exit = simulate_exit(&mut position, variant, &window)?;
    value_scenario(variant, variant_digest, decision, &window, position, exit)
}

fn unsupported_candidate_outcome(
    candidate: &PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
    digest: &ContentHash,
) -> Option<CounterfactualVariantOutcome> {
    if !candidate.source_event_candle_available {
        return Some(unavailable_outcome(
            variant,
            digest,
            CounterfactualVariantStatus::EvidenceUnavailable,
            CounterfactualUnavailableReason::SourceEventCandleUnavailable,
            "the source event candle is not present with its saved identity in the supplied bars",
        ));
    }
    let Some(mark) = candidate.source_state_mark else {
        return Some(unavailable_outcome(
            variant,
            digest,
            CounterfactualVariantStatus::EvidenceUnavailable,
            CounterfactualUnavailableReason::MissingSourceStateMark,
            "the saved event has no account mark for the source decision bar before the signal fact",
        ));
    };
    if candidate.signal.state_before != PositionState::Cash
        || mark.state.qty.get() > Decimal::ZERO
        || !mark.state.cash_reserved.get().is_zero()
    {
        return Some(unavailable_outcome(
            variant,
            digest,
            CounterfactualVariantStatus::Unsupported,
            CounterfactualUnavailableReason::SourceStateNotCash,
            "this bounded entry-opportunity scenario starts from a saved cash state",
        ));
    }
    let unsupported = if variant.capital_mode == crate::contracts::CapitalMode::SharedPortfolio {
        Some((
            CounterfactualUnavailableReason::SharedCapitalUnsupported,
            "candidate variants use independent virtual accounts; shared-capital arbitration is not implemented here",
        ))
    } else if variant.costs.dynamic.is_some() {
        Some((
            CounterfactualUnavailableReason::DynamicCostModelUnsupported,
            "the bounded scenario supports fixed costs and resolved saved fee schedules only",
        ))
    } else if matches!(&variant.execution, ExecutionPolicy::PassiveBuy { .. }) {
        Some((
            CounterfactualUnavailableReason::PassiveExecutionUnsupported,
            "the bounded scenario supports NextBarOpen execution only",
        ))
    } else if variant.latency_ms != 0 {
        Some((
            CounterfactualUnavailableReason::NonzeroLatencyUnsupported,
            "the bounded NextBarOpen scenario requires zero latency",
        ))
    } else if variant.risk.drawdown_stop.get() > Decimal::ZERO {
        Some((
            CounterfactualUnavailableReason::DrawdownStopUnsupported,
            "drawdown-stop behavior needs a causal virtual-account history beyond this standalone candidate window",
        ))
    } else {
        None
    };
    unsupported.map(|(reason, detail)| {
        unavailable_outcome(
            variant,
            digest,
            CounterfactualVariantStatus::Unsupported,
            reason,
            detail,
        )
    })
}

struct ScenarioWindow<'a> {
    decision: &'a CandleObservation,
    entry_execution: &'a CandleObservation,
    entry_liquidity: &'a CandleObservation,
    exit_decision: &'a CandleObservation,
    exit_execution: &'a CandleObservation,
    exit_liquidity: &'a CandleObservation,
    valuation: &'a CandleObservation,
}

enum ScenarioWindowStage<'a> {
    Ready(ScenarioWindow<'a>),
    Insufficient(&'static str),
}

fn scenario_window<'a>(
    candidate: &'a PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
) -> Result<ScenarioWindowStage<'a>, LabError> {
    let decision_bars = candidate
        .bars_by_interval
        .get(&interval_key(variant.decision_interval))
        .ok_or_else(|| LabError::DataGap("variant decision interval bars are missing".into()))?;
    let execution_bars = candidate
        .bars_by_interval
        .get(&interval_key(variant.execution_resolution))
        .ok_or_else(|| LabError::DataGap("variant execution bars are missing".into()))?;
    let Ok(decision_index) = unique_close_index(decision_bars, candidate.signal.signal_time) else {
        return Ok(ScenarioWindowStage::Insufficient(
            "no completed variant decision bar closes at the exact source signal time",
        ));
    };
    let decision_bar = decision_bars[decision_index];
    let Ok(entry_index) = unique_open_index(execution_bars, decision_bar.candle.close_time_utc)
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "the next-open entry bar at the source decision boundary is unavailable",
        ));
    };
    let Some(entry_liquidity_bar) = entry_index
        .checked_sub(1)
        .and_then(|index| execution_bars.get(index).copied())
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "a completed prior execution bar is required for the next-open volume cap",
        ));
    };
    let hold_bars = usize::try_from(variant.exit.hold_decision_bars).map_err(|_| {
        LabError::ResourceLimit("fixed exit hold count exceeds platform capacity".into())
    })?;
    let Some(exit_decision_index) = decision_index
        .checked_add(hold_bars)
        .filter(|index| *index < decision_bars.len())
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "the declared completed fixed-exit decision bar is unavailable",
        ));
    };
    let exit_decision_bar = decision_bars[exit_decision_index];
    let Ok(exit_index) = unique_open_index(execution_bars, exit_decision_bar.candle.close_time_utc)
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "the next-open fixed-exit execution bar is unavailable",
        ));
    };
    let Some(exit_liquidity_bar) = exit_index
        .checked_sub(1)
        .and_then(|index| execution_bars.get(index).copied())
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "a completed prior execution bar is required for the fixed-exit volume cap",
        ));
    };
    let valuation_offset =
        usize::try_from(variant.exit.valuation_bars_after_exit - 1).map_err(|_| {
            LabError::ResourceLimit("valuation offset exceeds platform capacity".into())
        })?;
    let Some(valuation_index) = exit_index
        .checked_add(valuation_offset)
        .filter(|index| *index < execution_bars.len())
    else {
        return Ok(ScenarioWindowStage::Insufficient(
            "the declared ex-post valuation bar is unavailable",
        ));
    };
    let valuation_bar = execution_bars[valuation_index];
    variant.market_rules.validate(UtcRange::new(
        decision_bar.candle.close_time_utc,
        valuation_bar.candle.close_time_utc,
    )?)?;
    Ok(ScenarioWindowStage::Ready(ScenarioWindow {
        decision: decision_bar,
        entry_execution: execution_bars[entry_index],
        entry_liquidity: entry_liquidity_bar,
        exit_decision: exit_decision_bar,
        exit_execution: execution_bars[exit_index],
        exit_liquidity: exit_liquidity_bar,
        valuation: valuation_bar,
    }))
}

enum EvidenceAvailability<'a> {
    Available(Option<&'a crate::contracts::EvidenceSnapshot>),
    Unavailable(CounterfactualUnavailableReason, &'static str),
}

enum PolicyPrefixStage {
    Ready(Box<crate::strategy::StrategyEvaluation>),
    EvidenceUnavailable(&'static str),
    Insufficient(&'static str),
}

enum PolicyDecisionStage {
    Ready(PolicyDecision),
    Unsupported(CounterfactualUnavailableReason, &'static str),
    EvidenceUnavailable(CounterfactualUnavailableReason, &'static str),
    Insufficient(&'static str),
}

struct PolicyDecision {
    policy_target: Weight,
    risk_target: Weight,
    reasons: Vec<ReasonCode>,
}

fn evaluate_variant_policy(
    candidate: &PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
    window: &ScenarioWindow<'_>,
) -> Result<PolicyDecisionStage, LabError> {
    let mut policy = PolicyEvaluator::compile_frozen_revision(
        &variant.policy,
        candidate.market.clone(),
        variant.decision_interval,
    )?;
    if !policy.source_requirements().is_empty() {
        return Ok(PolicyDecisionStage::Unsupported(
            CounterfactualUnavailableReason::CrossIntervalPolicyUnsupported,
            "the bounded candidate input contains one decision interval and cannot supply policy source intervals",
        ));
    }
    let evidence_snapshot = match source_evidence_snapshot(candidate, variant)? {
        EvidenceAvailability::Available(snapshot) => snapshot,
        EvidenceAvailability::Unavailable(reason, detail) => {
            return Ok(PolicyDecisionStage::EvidenceUnavailable(reason, detail));
        }
    };
    let evidence = EvidenceEvaluator::new(evidence_snapshot, variant.pit_policy)?;
    match evaluate_policy_prefix(
        candidate,
        variant,
        window,
        &mut policy,
        evidence_snapshot,
        &evidence,
    )? {
        PolicyPrefixStage::EvidenceUnavailable(detail) => {
            Ok(PolicyDecisionStage::EvidenceUnavailable(
                CounterfactualUnavailableReason::MissingRequiredEvidence,
                detail,
            ))
        }
        PolicyPrefixStage::Insufficient(detail) => Ok(PolicyDecisionStage::Insufficient(detail)),
        PolicyPrefixStage::Ready(evaluation) => {
            let evaluation = *evaluation;
            let policy_target = evaluation.constrained_target_weight;
            let cash_cap = Decimal::ONE
                .checked_sub(variant.risk.min_cash_weight.get())
                .ok_or_else(|| {
                    LabError::AccountingInvariant("counterfactual risk cash cap overflow".into())
                })?;
            let risk_target = Weight::new(
                policy_target
                    .get()
                    .min(variant.risk.max_gross_exposure.get().min(cash_cap)),
            )?;
            Ok(PolicyDecisionStage::Ready(PolicyDecision {
                policy_target,
                risk_target,
                reasons: evaluation.reasons,
            }))
        }
    }
}

fn source_evidence_snapshot<'a>(
    candidate: &'a PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
) -> Result<EvidenceAvailability<'a>, LabError> {
    let evidence_snapshot = match &candidate.input.evidence {
        CounterfactualEvidenceInput::Snapshot { snapshot } => Some(snapshot),
        CounterfactualEvidenceInput::Unavailable { .. } => None,
    };
    let source_trace = candidate
        .signal
        .policy_trace
        .as_ref()
        .and_then(|trace| trace.evidence.as_ref());
    let source_effect = candidate.signal.evidence_effect.as_ref();
    let effect_snapshot_id = source_effect.and_then(|effect| effect.snapshot_id.as_ref());
    let trace_snapshot_id = source_trace.and_then(|trace| trace.snapshot_id.as_ref());
    if effect_snapshot_id
        .zip(trace_snapshot_id)
        .is_some_and(|(effect, trace)| effect != trace)
    {
        return Err(LabError::DataCorrupt(
            "source signal effect and policy trace pin different evidence snapshots".into(),
        ));
    }
    let expected_snapshot_id = effect_snapshot_id.or(trace_snapshot_id);
    match (expected_snapshot_id, evidence_snapshot) {
        (Some(expected), Some(snapshot)) if expected == &snapshot.id => {}
        (Some(_), Some(_)) => {
            return Ok(EvidenceAvailability::Unavailable(
                CounterfactualUnavailableReason::SourceEvidenceIdentityMismatch,
                "the supplied evidence snapshot does not match the source signal identity",
            ));
        }
        (Some(_), None) => {
            return Ok(EvidenceAvailability::Unavailable(
                CounterfactualUnavailableReason::MissingRequiredEvidence,
                "the source signal pins an evidence snapshot that is unavailable in this request",
            ));
        }
        (None, _) => {}
    }
    let effect_policy = source_effect.map(|effect| effect.policy);
    let trace_policy = source_trace.map(|trace| trace.policy);
    if effect_policy
        .zip(trace_policy)
        .is_some_and(|(effect, trace)| effect != trace)
    {
        return Err(LabError::DataCorrupt(
            "source signal effect and policy trace use different PIT policies".into(),
        ));
    }
    if effect_policy
        .or(trace_policy)
        .is_some_and(|policy| policy != variant.pit_policy)
    {
        return Ok(EvidenceAvailability::Unavailable(
            CounterfactualUnavailableReason::SourceEvidenceIdentityMismatch,
            "variant PIT policy differs from the source signal evidence policy",
        ));
    }
    Ok(EvidenceAvailability::Available(evidence_snapshot))
}

fn evaluate_policy_prefix(
    candidate: &PreparedCandidate<'_>,
    variant: &CounterfactualVariant,
    window: &ScenarioWindow<'_>,
    policy: &mut PolicyEvaluator,
    evidence_snapshot: Option<&crate::contracts::EvidenceSnapshot>,
    evidence: &EvidenceEvaluator,
) -> Result<PolicyPrefixStage, LabError> {
    let decision_bars = candidate
        .bars_by_interval
        .get(&interval_key(variant.decision_interval))
        .ok_or_else(|| LabError::DataGap("variant decision interval bars are missing".into()))?;
    let cash_position = PositionView {
        actual_qty: AssetQuantity::new(Decimal::ZERO)?,
        actual_weight: Weight::new(Decimal::ZERO)?,
        episode_opened_at: None,
        held_decision_bars: None,
    };
    let mut selected = None;
    for bar in decision_bars
        .iter()
        .take_while(|bar| bar.candle.close_time_utc <= window.decision.candle.close_time_utc)
    {
        let evaluation = policy.observe(bar, cash_position, Some(evidence))?;
        if evidence_snapshot.is_none()
            && evaluation
                .as_ref()
                .is_some_and(|evaluation| evaluation.evidence_effect.is_some())
        {
            return Ok(PolicyPrefixStage::EvidenceUnavailable(
                "the frozen policy evaluation requires evidence absent from the request",
            ));
        }
        if bar.id == window.decision.id {
            selected = evaluation;
        }
    }
    match selected {
        Some(evaluation) => Ok(PolicyPrefixStage::Ready(Box::new(evaluation))),
        None => Ok(PolicyPrefixStage::Insufficient(
            "variant policy warmup did not complete by the source signal time",
        )),
    }
}

struct EmptyOutcomeDetails {
    status: CounterfactualVariantStatus,
    policy_target: Option<Weight>,
    risk_target: Option<Weight>,
    policy_reasons: Vec<ReasonCode>,
    entry_result: Option<ReasonCode>,
    exit_result: Option<ReasonCode>,
}

impl EmptyOutcomeDetails {
    fn from_decision(
        status: CounterfactualVariantStatus,
        decision: &PolicyDecision,
        entry_result: Option<ReasonCode>,
        exit_result: Option<ReasonCode>,
    ) -> Self {
        Self {
            status,
            policy_target: Some(decision.policy_target),
            risk_target: Some(decision.risk_target),
            policy_reasons: decision.reasons.clone(),
            entry_result,
            exit_result,
        }
    }
}

struct VirtualPosition {
    account: Account,
    executions: Vec<CounterfactualExecution>,
    entry_result: ReasonCode,
    entry_partial: bool,
    participation_cap: Weight,
}

enum EntryStage {
    Filled(Box<VirtualPosition>),
    Rejected(Box<CounterfactualVariantOutcome>),
}

fn simulate_entry(
    variant: &CounterfactualVariant,
    digest: &ContentHash,
    decision: &PolicyDecision,
    window: &ScenarioWindow<'_>,
) -> Result<EntryStage, LabError> {
    let participation_cap = next_open_participation(variant)?;
    let mut account = Account::new(variant.initial_capital);
    let fill = simulate_next_open(
        &mut account,
        variant,
        MarketStep {
            target: decision.risk_target,
            decision_bar: window.decision,
            execution_bar: window.entry_execution,
            liquidity_bar: window.entry_liquidity,
            participation_cap,
            enforce_entry_risk: true,
        },
    )?;
    match fill {
        SimulatedFill::Filled(execution, partial) => {
            let entry_result = execution.reason;
            Ok(EntryStage::Filled(Box::new(VirtualPosition {
                account,
                executions: vec![*execution],
                entry_result,
                entry_partial: partial,
                participation_cap,
            })))
        }
        SimulatedFill::Unfilled(reason) => {
            Ok(EntryStage::Rejected(Box::new(empty_execution_outcome(
                variant,
                digest,
                EmptyOutcomeDetails::from_decision(
                    CounterfactualVariantStatus::EntryUnfilled,
                    decision,
                    Some(reason),
                    None,
                ),
            ))))
        }
        SimulatedFill::RiskBlocked(reason) => {
            Ok(EntryStage::Rejected(Box::new(empty_execution_outcome(
                variant,
                digest,
                EmptyOutcomeDetails::from_decision(
                    CounterfactualVariantStatus::RiskBlocked,
                    decision,
                    Some(reason),
                    None,
                ),
            ))))
        }
    }
}

#[derive(Clone, Copy)]
struct ExitStage {
    result: Option<ReasonCode>,
    partial: bool,
}

fn simulate_exit(
    position: &mut VirtualPosition,
    variant: &CounterfactualVariant,
    window: &ScenarioWindow<'_>,
) -> Result<ExitStage, LabError> {
    let fill = simulate_next_open(
        &mut position.account,
        variant,
        MarketStep {
            target: Weight::new(Decimal::ZERO)?,
            decision_bar: window.exit_decision,
            execution_bar: window.exit_execution,
            liquidity_bar: window.exit_liquidity,
            participation_cap: position.participation_cap,
            enforce_entry_risk: false,
        },
    )?;
    match fill {
        SimulatedFill::Filled(execution, partial) => {
            let result = execution.reason;
            position.executions.push(*execution);
            Ok(ExitStage {
                result: Some(result),
                partial,
            })
        }
        SimulatedFill::Unfilled(reason) | SimulatedFill::RiskBlocked(reason) => Ok(ExitStage {
            result: Some(reason),
            partial: true,
        }),
    }
}

fn value_scenario(
    variant: &CounterfactualVariant,
    digest: &ContentHash,
    decision: PolicyDecision,
    window: &ScenarioWindow<'_>,
    mut position: VirtualPosition,
    exit: ExitStage,
) -> Result<CounterfactualVariantOutcome, LabError> {
    let mark = position.account.mark(window.valuation.candle.close)?;
    let ending_account = position.account.state()?;
    let cumulative_fees = ending_account.cumulative_fees;
    let net_pnl = SignedAmount::new(
        mark.equity
            .checked_sub(variant.initial_capital.get())
            .ok_or_else(|| LabError::AccountingInvariant("counterfactual PnL overflow".into()))?,
    )?;
    let status = if position.entry_partial || exit.partial {
        CounterfactualVariantStatus::PartiallyExecuted
    } else {
        CounterfactualVariantStatus::Completed
    };
    Ok(CounterfactualVariantOutcome {
        variant_id: variant.variant_id.clone(),
        policy_ref: variant.policy.reference.clone(),
        variant_digest: digest.clone(),
        status,
        unavailable_reason: None,
        detail: None,
        policy_target_weight: Some(decision.policy_target),
        risk_adjusted_target_weight: Some(decision.risk_target),
        policy_reasons: decision.reasons,
        entry_result: Some(position.entry_result),
        exit_result: exit.result,
        executions: position.executions,
        initial_capital: variant.initial_capital,
        ending_account: Some(ending_account),
        final_equity: Some(QuoteAmount::new(mark.equity)?),
        net_pnl: Some(net_pnl),
        cumulative_fees: Some(cumulative_fees),
        valuation: Some(CounterfactualValuation {
            kind: CounterfactualValuationKind::ExPostCompletedBarClose,
            observation_id: window.valuation.id.clone(),
            content_digest: window.valuation.content_digest.clone(),
            valued_at: window.valuation.candle.close_time_utc,
            price: window.valuation.candle.close,
        }),
    })
}

#[derive(Clone, Copy)]
struct MarketStep<'a> {
    target: Weight,
    decision_bar: &'a CandleObservation,
    execution_bar: &'a CandleObservation,
    liquidity_bar: &'a CandleObservation,
    participation_cap: Weight,
    enforce_entry_risk: bool,
}

fn next_open_participation(variant: &CounterfactualVariant) -> Result<Weight, LabError> {
    match &variant.execution {
        ExecutionPolicy::NextBarOpen { participation_cap } => Ok(*participation_cap),
        ExecutionPolicy::PassiveBuy { .. } => Err(LabError::InvalidConfig(
            "NextBarOpen participation requested for a passive variant".into(),
        )),
    }
}

enum SimulatedFill {
    Filled(Box<CounterfactualExecution>, bool),
    Unfilled(ReasonCode),
    RiskBlocked(ReasonCode),
}

fn simulate_next_open(
    account: &mut Account,
    variant: &CounterfactualVariant,
    step: MarketStep<'_>,
) -> Result<SimulatedFill, LabError> {
    let MarketStep {
        target,
        decision_bar,
        execution_bar,
        liquidity_bar,
        participation_cap,
        enforce_entry_risk,
    } = step;
    let decision_time = decision_bar.candle.close_time_utc;
    let execution_time = execution_bar.candle.open_time_utc;
    if execution_time != decision_time || liquidity_bar.candle.close_time_utc != execution_time {
        return Err(LabError::DataGap(
            "NextBarOpen execution and completed liquidity source do not align with the decision"
                .into(),
        ));
    }
    if !variant.market_rules.tradable_at(decision_time)
        || !variant.market_rules.tradable_at(execution_time)
    {
        return Ok(SimulatedFill::Unfilled(ReasonCode::RuleBlocked));
    }
    let decision_costs = effective_costs(&variant.costs, &variant.market_rules);
    let request = plan_market_request(
        account,
        target,
        decision_bar.candle.close,
        &decision_costs,
        &variant.market_rules,
    )?;
    let Some(requested) = request.fill else {
        return Ok(SimulatedFill::Unfilled(request.reason));
    };
    let had_buy_reservation = requested.side == Side::Buy;
    if had_buy_reservation {
        account.reserve(requested.reserved_cash)?;
        // The production arrival path releases the request reservation before
        // applying the actual-open price and volume caps.
        account.release(requested.reserved_cash)?;
    }
    let arrival = plan_market_arrival(
        account,
        &requested,
        execution_bar.candle.open,
        &decision_costs,
        &variant.market_rules,
        liquidity_bar.candle.volume,
        participation_cap,
    )?;
    let Some(fill) = arrival.fill else {
        return Ok(SimulatedFill::Unfilled(arrival.reason));
    };
    let mut prospective = account.clone();
    if fill.side == Side::Buy {
        prospective.reserve(fill.reserved_cash)?;
    }
    prospective.apply_fill(
        fill.side,
        fill.price,
        fill.qty,
        fill.fee,
        fill.reserved_cash,
    )?;
    if enforce_entry_risk && !risk_allows_fill(&mut prospective, fill.price, variant)? {
        return Ok(SimulatedFill::RiskBlocked(ReasonCode::CapitalBlocked));
    }
    *account = prospective;
    let price_cost_attribution = SignedAmount::new(checked_mul(
        fill.price_cost_attribution,
        fill.qty.get(),
        "counterfactual price-cost attribution",
    )?)?;
    Ok(SimulatedFill::Filled(
        Box::new(CounterfactualExecution {
            side: fill.side,
            decision_time,
            execution_time,
            decision_bar_id: decision_bar.id.clone(),
            execution_bar_id: execution_bar.id.clone(),
            liquidity_source_bar_id: liquidity_bar.id.clone(),
            liquidity_source_close_time: liquidity_bar.candle.close_time_utc,
            decision_reference: decision_bar.candle.close,
            execution_open_proxy: execution_bar.candle.open,
            price: fill.price,
            quantity: fill.qty,
            notional: fill.notional,
            fee: fill.fee,
            fee_bps: fill.fee_bps,
            price_cost_attribution_krw: price_cost_attribution,
            reason: fill.reason,
        }),
        fill.reason != ReasonCode::MarketFilled,
    ))
}

fn risk_allows_fill(
    account: &mut Account,
    fill_price: PriceKrw,
    variant: &CounterfactualVariant,
) -> Result<bool, LabError> {
    let marked = account.mark(fill_price)?;
    if marked.equity <= Decimal::ZERO
        || marked.actual_weight > variant.risk.max_gross_exposure.get()
    {
        return Ok(false);
    }
    let state = account.state()?;
    let cash_weight = checked_div(
        state.cash_total.get(),
        marked.equity,
        "counterfactual cash weight",
    )?;
    Ok(cash_weight >= variant.risk.min_cash_weight.get())
}

fn effective_costs(costs: &CostPolicy, rules: &MarketRuleSnapshot) -> CostPolicy {
    let mut effective = costs.clone();
    if let Some(schedule) = &rules.fee_schedule {
        effective.buy_fee_bps = schedule.taker_fee_bps;
        effective.sell_fee_bps = schedule.taker_fee_bps;
        effective.maker_fee_bps = schedule.maker_fee_bps;
    }
    effective
}

fn unavailable_outcome(
    variant: &CounterfactualVariant,
    digest: &ContentHash,
    status: CounterfactualVariantStatus,
    reason: CounterfactualUnavailableReason,
    detail: &str,
) -> CounterfactualVariantOutcome {
    CounterfactualVariantOutcome {
        variant_id: variant.variant_id.clone(),
        policy_ref: variant.policy.reference.clone(),
        variant_digest: digest.clone(),
        status,
        unavailable_reason: Some(reason),
        detail: Some(detail.to_owned()),
        policy_target_weight: None,
        risk_adjusted_target_weight: None,
        policy_reasons: Vec::new(),
        entry_result: None,
        exit_result: None,
        executions: Vec::new(),
        initial_capital: variant.initial_capital,
        ending_account: None,
        final_equity: None,
        net_pnl: None,
        cumulative_fees: None,
        valuation: None,
    }
}

fn insufficient_evidence_outcome(
    variant: &CounterfactualVariant,
    digest: &ContentHash,
    detail: &str,
) -> CounterfactualVariantOutcome {
    unavailable_outcome(
        variant,
        digest,
        CounterfactualVariantStatus::InsufficientEvidence,
        CounterfactualUnavailableReason::MissingExitEvidence,
        detail,
    )
}

fn empty_execution_outcome(
    variant: &CounterfactualVariant,
    digest: &ContentHash,
    details: EmptyOutcomeDetails,
) -> CounterfactualVariantOutcome {
    CounterfactualVariantOutcome {
        variant_id: variant.variant_id.clone(),
        policy_ref: variant.policy.reference.clone(),
        variant_digest: digest.clone(),
        status: details.status,
        unavailable_reason: None,
        detail: None,
        policy_target_weight: details.policy_target,
        risk_adjusted_target_weight: details.risk_target,
        policy_reasons: details.policy_reasons,
        entry_result: details.entry_result,
        exit_result: details.exit_result,
        executions: Vec::new(),
        initial_capital: variant.initial_capital,
        ending_account: None,
        final_equity: None,
        net_pnl: None,
        cumulative_fees: None,
        valuation: None,
    }
}

fn is_unavailable_status(status: CounterfactualVariantStatus) -> bool {
    matches!(
        status,
        CounterfactualVariantStatus::Unsupported
            | CounterfactualVariantStatus::EvidenceUnavailable
            | CounterfactualVariantStatus::InsufficientEvidence
    )
}

fn unique_close_index(
    bars: &[&CandleObservation],
    close_time: UtcTimestamp,
) -> Result<usize, LabError> {
    unique_index(
        bars,
        |bar| bar.candle.close_time_utc == close_time,
        "close time",
    )
}

fn unique_open_index(
    bars: &[&CandleObservation],
    open_time: UtcTimestamp,
) -> Result<usize, LabError> {
    unique_index(
        bars,
        |bar| bar.candle.open_time_utc == open_time,
        "open time",
    )
}

fn unique_index(
    bars: &[&CandleObservation],
    predicate: impl Fn(&CandleObservation) -> bool,
    label: &str,
) -> Result<usize, LabError> {
    let mut result = None;
    for (index, bar) in bars.iter().enumerate() {
        if !predicate(bar) {
            continue;
        }
        if result.is_some() {
            return Err(LabError::DataCorrupt(format!(
                "counterfactual bars repeat {label}"
            )));
        }
        result = Some(index);
    }
    let Some(index) = result else {
        return Err(LabError::DataGap(format!(
            "counterfactual bar with matching {label} is missing"
        )));
    };
    Ok(index)
}

fn interval_key(interval: CandleInterval) -> i64 {
    interval.duration().num_seconds()
}
