use crate::contracts::policy::{PolicyIndicator, PolicyIndicatorKind, PolicyProgram};
use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, IndicatorSeedEvidence,
    IndicatorSourceReference, IndicatorSourceSeries, LabError, MarketId, NamedPolicyValue,
    ReviewAuditBasis, ReviewAuditDiagnostic, ReviewAuditDiagnosticCode as Code, ReviewAuditKind,
    ReviewAuditReport, SourceHistoryScope, SourceIndicatorAuditInput, observation_digest,
    validate_frozen_policy,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

use super::common::{
    AuditFindings, MAX_AUDIT_BARS, MAX_SERIES, MAX_VALUES, check_serialized_bound, decimal_to_f64,
    digest_value, tolerance, validate_observation, validate_total_links,
};

pub const SOURCE_INDICATOR_ABSOLUTE_TOLERANCE: f64 = 1.0e-12;
pub const SOURCE_INDICATOR_RELATIVE_TOLERANCE: f64 = 1.0e-10;

#[derive(Debug)]
struct PreparedSeries<'a> {
    source: &'a IndicatorSourceSeries,
    bars: Vec<&'a CandleObservation>,
    gap_free: bool,
    usable_for_indicators: bool,
}

struct SeriesContext<'a> {
    market: &'a MarketId,
    interval: CandleInterval,
    decision_interval: CandleInterval,
    cutoff: crate::contracts::UtcTimestamp,
    decision_observation: &'a CandleObservation,
    indicators: Vec<&'a PolicyIndicator>,
}

#[derive(Default)]
struct SourcePreparationState {
    used_observation_ids: BTreeSet<crate::contracts::ObservationId>,
    diagnostics: Vec<ReviewAuditDiagnostic>,
}

struct PreparedSourceSet<'a> {
    series: BTreeMap<CandleInterval, PreparedSeries<'a>>,
    diagnostics: Vec<ReviewAuditDiagnostic>,
}

struct SourceEvidenceDigests {
    evidence_digest: ContentHash,
    item_digests: Vec<ContentHash>,
}

#[derive(Serialize)]
struct PreparedSeriesProjection<'a> {
    interval: CandleInterval,
    bars: Vec<&'a CandleObservation>,
    seed: &'a IndicatorSeedEvidence,
    closure: &'a Option<crate::contracts::IndicatorSourceClosureEvidence>,
}

#[derive(Serialize)]
struct SourceEvidenceProjection<'a> {
    policy: &'a crate::contracts::FrozenPolicyRevision,
    saved_policy_ref: &'a crate::contracts::PolicyRevisionRef,
    market: &'a MarketId,
    decision_interval: CandleInterval,
    cutoff_utc: crate::contracts::UtcTimestamp,
    decision_observation: &'a CandleObservation,
    source_series: Vec<PreparedSeriesProjection<'a>>,
    saved_values: &'a [NamedPolicyValue],
}

#[derive(Debug)]
enum IndicatorResult {
    Value(f64),
    Insufficient(Code, String),
    Unsupported(String),
}

/// Independently recompute supported Rules-program indicators from retained
/// source bars through the saved decision cutoff.
///
/// Supported indicators are open/high/low/close/volume/quote turnover, SMA,
/// seed-mean EMA, Wilder RSI, rolling sample/annual log-return volatility, and
/// prior-window high/low. EMA and RSI require the original saved seed; the other
/// rolling formulas require only their exact retained finite window.
///
/// Tolerance is `1e-12 + 1e-10 * max(|expected|, |recomputed|)` for indicator
/// values. Future suffix bars are excluded from calculations and evidence digests.
///
/// # Errors
/// Rejects corrupt source identities, malformed bars, and resource overages.
pub fn audit_source_indicators(
    input: &SourceIndicatorAuditInput,
) -> Result<ReviewAuditReport, LabError> {
    validate_source_input(input)?;

    let PolicyProgram::Rules { program } = &input.policy.definition.program else {
        return unsupported_source_audit(input, Code::BuiltinPolicyUnsupported, "policy.program");
    };
    if program.indicators.is_empty() {
        return unsupported_source_audit(
            input,
            Code::UnsupportedAuditShape,
            "policy.program.indicators",
        );
    }
    let expected_intervals = program
        .indicators
        .iter()
        .map(|indicator| indicator.source_interval.unwrap_or(input.decision_interval))
        .collect::<BTreeSet<_>>();
    let prepared = prepare_source_series(input, program, &expected_intervals)?;
    let evidence = source_evidence_digests(input, &prepared.series)?;
    let mut final_findings = AuditFindings::new(
        ReviewAuditKind::SourceIndicatorAudit,
        ReviewAuditBasis::RecomputedFromRetainedSourceBars,
        evidence.evidence_digest,
        evidence.item_digests,
    );
    if input.saved_policy_ref != input.policy.reference {
        final_findings.contradiction(Code::PolicyRevisionMismatch, "saved_policy_ref");
        return final_findings.finish();
    }
    for diagnostic in prepared.diagnostics {
        final_findings.insufficient(diagnostic.code, diagnostic.path);
    }
    compare_saved_indicators(input, program, &prepared.series, &mut final_findings)?;
    final_findings.finish()
}

fn validate_source_input(input: &SourceIndicatorAuditInput) -> Result<(), LabError> {
    validate_frozen_policy(&input.policy)?;
    validate_observation(
        &input.decision_observation,
        &input.market,
        input.decision_interval,
        input.cutoff_utc,
        "decision observation",
    )?;
    if input.decision_observation.candle.close_time_utc != input.cutoff_utc {
        return Err(LabError::DataCorrupt(
            "source indicator audit decision observation does not match cutoff".into(),
        ));
    }
    if observation_digest(&input.decision_observation.candle)?
        != input.decision_observation.content_digest
    {
        return Err(LabError::DataCorrupt(
            "source indicator audit decision content digest mismatch".into(),
        ));
    }
    validate_source_input_bounds(input)?;
    validate_total_links(
        input
            .source_series
            .iter()
            .flat_map(|series| &series.observations),
    )?;
    if input
        .saved_values
        .iter()
        .any(|value| !value.value.is_finite())
    {
        return Err(LabError::DataCorrupt(
            "source indicator audit saved value is nonfinite".into(),
        ));
    }
    check_serialized_bound(input)
}

fn validate_source_input_bounds(input: &SourceIndicatorAuditInput) -> Result<(), LabError> {
    if input.source_series.len() > MAX_SERIES || input.saved_values.len() > MAX_VALUES {
        return Err(LabError::ResourceLimit(
            "source indicator audit series/value bound exceeded".into(),
        ));
    }
    let total_bars = input
        .source_series
        .iter()
        .try_fold(0_usize, |sum, series| {
            sum.checked_add(series.observations.len()).ok_or_else(|| {
                LabError::ResourceLimit("source indicator bar count overflow".into())
            })
        })?;
    if total_bars > MAX_AUDIT_BARS {
        return Err(LabError::ResourceLimit(
            "source indicator audit bar bound exceeded".into(),
        ));
    }
    Ok(())
}

fn unsupported_source_audit(
    input: &SourceIndicatorAuditInput,
    code: Code,
    path: &str,
) -> Result<ReviewAuditReport, LabError> {
    let mut findings = AuditFindings::new(
        ReviewAuditKind::SourceIndicatorAudit,
        ReviewAuditBasis::RecomputedFromRetainedSourceBars,
        digest_value(input)?,
        vec![input.policy.reference.definition_digest.clone()],
    );
    findings.unsupported(code, path);
    findings.finish()
}

fn prepare_source_series<'a>(
    input: &'a SourceIndicatorAuditInput,
    program: &'a crate::contracts::RulesProgram,
    expected_intervals: &BTreeSet<CandleInterval>,
) -> Result<PreparedSourceSet<'a>, LabError> {
    let mut source_by_interval = BTreeMap::new();
    for source in &input.source_series {
        if !expected_intervals.contains(&source.interval)
            || source_by_interval.insert(source.interval, source).is_some()
        {
            return Err(LabError::DataCorrupt(
                "source indicator audit has an unexpected or duplicate source interval".into(),
            ));
        }
    }

    let mut preparation = SourcePreparationState::default();
    let mut series_by_interval = BTreeMap::new();
    for interval in expected_intervals {
        let Some(source) = source_by_interval.get(interval) else {
            preparation.diagnostics.push(diagnostic(
                Code::MissingSourceSeries,
                format!("source_series.{interval:?}"),
            ));
            continue;
        };
        let context = SeriesContext {
            market: &input.market,
            interval: *interval,
            decision_interval: input.decision_interval,
            cutoff: input.cutoff_utc,
            decision_observation: &input.decision_observation,
            indicators: program
                .indicators
                .iter()
                .filter(|indicator| {
                    indicator.source_interval.unwrap_or(input.decision_interval) == *interval
                })
                .collect(),
        };
        let prepared = prepare_series(source, &context, &mut preparation)?;
        series_by_interval.insert(*interval, prepared);
    }
    Ok(PreparedSourceSet {
        series: series_by_interval,
        diagnostics: preparation.diagnostics,
    })
}

fn source_evidence_digests(
    input: &SourceIndicatorAuditInput,
    prepared: &BTreeMap<CandleInterval, PreparedSeries<'_>>,
) -> Result<SourceEvidenceDigests, LabError> {
    let mut projections = prepared
        .values()
        .map(|series| PreparedSeriesProjection {
            interval: series.source.interval,
            bars: series.bars.clone(),
            seed: &series.source.seed,
            closure: &series.source.closure,
        })
        .collect::<Vec<_>>();
    projections.sort_by_key(|series| series.interval.duration().num_seconds());
    let evidence_digest = digest_value(&SourceEvidenceProjection {
        policy: &input.policy,
        saved_policy_ref: &input.saved_policy_ref,
        market: &input.market,
        decision_interval: input.decision_interval,
        cutoff_utc: input.cutoff_utc,
        decision_observation: &input.decision_observation,
        source_series: projections,
        saved_values: &input.saved_values,
    })?;
    let mut item_digests = vec![
        input.policy.reference.definition_digest.clone(),
        input.decision_observation.content_digest.clone(),
    ];
    for series in prepared.values() {
        match &series.source.seed.source {
            IndicatorSourceReference::Dataset { dataset_digest, .. } => {
                item_digests.push(dataset_digest.clone());
            }
            IndicatorSourceReference::CapturedWindow {
                evidence_digest, ..
            } => item_digests.push(evidence_digest.clone()),
        }
        if let Some(closure) = &series.source.closure {
            item_digests.push(closure.evidence_digest.clone());
        }
        item_digests.extend(series.bars.iter().map(|bar| bar.content_digest.clone()));
    }
    Ok(SourceEvidenceDigests {
        evidence_digest,
        item_digests,
    })
}

fn compare_saved_indicators(
    input: &SourceIndicatorAuditInput,
    program: &crate::contracts::RulesProgram,
    prepared: &BTreeMap<CandleInterval, PreparedSeries<'_>>,
    findings: &mut AuditFindings,
) -> Result<(), LabError> {
    let Some(saved_values) = values_map(&input.saved_values, program, findings)? else {
        return Ok(());
    };
    for indicator in &program.indicators {
        let source_interval = indicator.source_interval.unwrap_or(input.decision_interval);
        let Some(series) = prepared.get(&source_interval) else {
            findings.insufficient(
                Code::MissingSourceSeries,
                format!("indicators.{}.source_interval", indicator.id),
            );
            continue;
        };
        if !series.usable_for_indicators {
            continue;
        }
        compare_one_indicator(indicator, series, source_interval, &saved_values, findings)?;
    }
    Ok(())
}

fn compare_one_indicator(
    indicator: &PolicyIndicator,
    series: &PreparedSeries<'_>,
    interval: CandleInterval,
    saved_values: &BTreeMap<String, f64>,
    findings: &mut AuditFindings,
) -> Result<(), LabError> {
    match recompute_indicator(indicator, series, interval) {
        IndicatorResult::Value(recomputed) => {
            let Some(expected) = saved_values.get(&indicator.id).copied() else {
                findings.insufficient(
                    Code::MissingRequiredEvidence,
                    format!("saved_values.{}", indicator.id),
                );
                return Ok(());
            };
            let permitted = tolerance(expected, recomputed);
            if !findings.residual(
                format!("indicators.{}", indicator.id),
                expected,
                recomputed,
                permitted,
            )? {
                findings.contradiction(
                    Code::IndicatorValueMismatch,
                    format!("indicators.{}", indicator.id),
                );
            }
        }
        IndicatorResult::Insufficient(code, path) => findings.insufficient(code, path),
        IndicatorResult::Unsupported(path) => {
            findings.unsupported(Code::UnsupportedAuditShape, path);
        }
    }
    Ok(())
}

fn prepare_series<'a>(
    source: &'a IndicatorSourceSeries,
    context: &SeriesContext<'_>,
    state: &mut SourcePreparationState,
) -> Result<PreparedSeries<'a>, LabError> {
    if source.interval != context.interval {
        return Err(LabError::DataCorrupt(
            "source indicator interval key mismatch".into(),
        ));
    }
    let (bars, gap_free) = retain_cutoff_prefix(source, context, state)?;
    let count = u32::try_from(bars.len())
        .map_err(|_| LabError::ResourceLimit("source indicator count exceeds u32".into()))?;
    if bars.is_empty() {
        state.diagnostics.push(diagnostic(
            Code::MissingSourceSeries,
            format!("source_series.{:?}.bars", context.interval),
        ));
    }
    validate_source_seed(source, &bars, count)?;
    validate_source_warmup(source, &context.indicators, count, &mut state.diagnostics)?;
    let usable_for_indicators =
        validate_series_cutoff_alignment(source, context, &bars, &mut state.diagnostics)?;

    Ok(PreparedSeries {
        source,
        bars,
        gap_free,
        usable_for_indicators,
    })
}

fn retain_cutoff_prefix<'a>(
    source: &'a IndicatorSourceSeries,
    context: &SeriesContext<'_>,
    state: &mut SourcePreparationState,
) -> Result<(Vec<&'a CandleObservation>, bool), LabError> {
    let mut previous_open = None;
    let mut bars = Vec::new();
    let mut gap_free = true;
    for observation in &source.observations {
        if previous_open.is_some_and(|previous| previous >= observation.candle.open_time_utc) {
            return Err(LabError::DataCorrupt(
                "source indicator bars are not strictly ordered".into(),
            ));
        }
        previous_open = Some(observation.candle.open_time_utc);
        if observation.candle.close_time_utc > context.cutoff {
            continue;
        }
        validate_observation(
            observation,
            context.market,
            context.interval,
            context.cutoff,
            "source bar",
        )?;
        if observation_digest(&observation.candle)? != observation.content_digest {
            return Err(LabError::DataCorrupt(
                "source indicator bar content digest mismatch".into(),
            ));
        }
        if !state.used_observation_ids.insert(observation.id.clone()) {
            return Err(LabError::DataCorrupt(
                "source indicator observation id is duplicated".into(),
            ));
        }
        if let Some(previous) = bars.last() {
            let previous: &&CandleObservation = previous;
            if previous.candle.close_time_utc != observation.candle.open_time_utc {
                gap_free = false;
            }
        }
        bars.push(observation);
    }
    Ok((bars, gap_free))
}

fn validate_source_seed(
    source: &IndicatorSourceSeries,
    bars: &[&CandleObservation],
    count: u32,
) -> Result<(), LabError> {
    if source.seed.bars_through_cutoff != count {
        return Err(LabError::DataCorrupt(
            "source indicator seed count does not match retained cutoff prefix".into(),
        ));
    }
    if let Some(first) = bars.first() {
        match &source.seed.source {
            IndicatorSourceReference::CapturedWindow {
                first_observation_id,
                evidence_digest,
                observation_count,
                ..
            } => {
                if first_observation_id != &first.id || *observation_count != count {
                    return Err(LabError::DataCorrupt(
                        "captured source window boundary/count mismatch".into(),
                    ));
                }
                let digest = ContentHash::of_value(&bars)?;
                if &digest != evidence_digest {
                    return Err(LabError::DataCorrupt(
                        "captured source window digest mismatch".into(),
                    ));
                }
            }
            IndicatorSourceReference::Dataset { .. } => {}
        }
        match source.seed.history_scope {
            SourceHistoryScope::CompleteFromSeed => {
                if source
                    .seed
                    .original_seed_observation_id
                    .as_ref()
                    .is_some_and(|id| id != &first.id)
                {
                    return Err(LabError::DataCorrupt(
                        "source seed identity does not match first retained bar".into(),
                    ));
                }
            }
            SourceHistoryScope::TruncatedPrefix => {}
        }
    }
    Ok(())
}

fn validate_source_warmup(
    source: &IndicatorSourceSeries,
    indicators: &[&PolicyIndicator],
    count: u32,
    diagnostics: &mut Vec<ReviewAuditDiagnostic>,
) -> Result<(), LabError> {
    let interval = source.interval;
    let required = indicators
        .iter()
        .map(|indicator| indicator_warmup(&indicator.indicator))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(1);
    let required = u32::try_from(required)
        .map_err(|_| LabError::ResourceLimit("source warmup exceeds u32".into()))?;
    if source.seed.required_warmup_bars != required {
        return Err(LabError::DataCorrupt(
            "source indicator declared warmup does not match policy".into(),
        ));
    }
    if count < required {
        diagnostics.push(diagnostic(
            Code::IncompleteWarmup,
            format!("source_series.{interval:?}.warmup"),
        ));
    }
    Ok(())
}

fn validate_series_cutoff_alignment(
    source: &IndicatorSourceSeries,
    context: &SeriesContext<'_>,
    bars: &[&CandleObservation],
    diagnostics: &mut Vec<ReviewAuditDiagnostic>,
) -> Result<bool, LabError> {
    let interval = context.interval;
    let mut usable_for_indicators = true;
    if interval == context.decision_interval {
        match bars.last() {
            Some(last) if same_observation(last, context.decision_observation) => {}
            Some(last) if last.id == context.decision_observation.id => {
                return Err(LabError::DataCorrupt(
                    "decision observation and source bar content disagree".into(),
                ));
            }
            _ => {
                diagnostics.push(diagnostic(
                    Code::MissingSourceSeries,
                    format!("source_series.{interval:?}.decision_observation"),
                ));
                usable_for_indicators = false;
            }
        }
    } else if let Some(closure) = &source.closure {
        if closure.source != source.seed.source {
            return Err(LabError::DataCorrupt(
                "source closure and seed references disagree".into(),
            ));
        }
        if closure.closed_through_utc != context.cutoff {
            diagnostics.push(diagnostic(
                Code::MissingMultiTimeframeClosure,
                format!("source_series.{interval:?}.closure.cutoff"),
            ));
            usable_for_indicators = false;
        }
        let expected_boundary =
            crate::scheduling::latest_completed_boundary(context.cutoff, interval)?;
        match bars.last() {
            Some(last)
                if closure.latest_observation_id == last.id
                    && closure.latest_close_time_utc == last.candle.close_time_utc =>
            {
                if last.candle.close_time_utc != expected_boundary {
                    diagnostics.push(diagnostic(
                        Code::MissingMultiTimeframeClosure,
                        format!("source_series.{interval:?}.closure.stale_tail"),
                    ));
                    usable_for_indicators = false;
                }
            }
            Some(_) => {
                return Err(LabError::DataCorrupt(
                    "source closure tail identity does not match retained bars".into(),
                ));
            }
            None => {
                diagnostics.push(diagnostic(
                    Code::MissingMultiTimeframeClosure,
                    format!("source_series.{interval:?}.closure.tail"),
                ));
                usable_for_indicators = false;
            }
        }
    } else {
        diagnostics.push(diagnostic(
            Code::MissingMultiTimeframeClosure,
            format!("source_series.{interval:?}.closure"),
        ));
        usable_for_indicators = false;
    }
    Ok(usable_for_indicators)
}

fn same_observation(left: &CandleObservation, right: &CandleObservation) -> bool {
    left.id == right.id
        && left.content_digest == right.content_digest
        && left.candle == right.candle
        && left.raw_object_ids == right.raw_object_ids
        && left.constituent_ids == right.constituent_ids
}

fn indicator_warmup(kind: &PolicyIndicatorKind) -> Result<usize, LabError> {
    Ok(match kind {
        PolicyIndicatorKind::Sma { window } | PolicyIndicatorKind::Ema { window } => *window,
        PolicyIndicatorKind::Rsi { window }
        | PolicyIndicatorKind::SampleVol { window }
        | PolicyIndicatorKind::AnnualVol { window }
        | PolicyIndicatorKind::PriorHigh { window }
        | PolicyIndicatorKind::PriorLow { window } => window
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("source indicator warmup overflow".into()))?,
        PolicyIndicatorKind::Open
        | PolicyIndicatorKind::High
        | PolicyIndicatorKind::Low
        | PolicyIndicatorKind::Close
        | PolicyIndicatorKind::Volume
        | PolicyIndicatorKind::QuoteTurnover => 1,
    })
}

fn values_map(
    values: &[NamedPolicyValue],
    program: &crate::contracts::RulesProgram,
    findings: &mut AuditFindings,
) -> Result<Option<BTreeMap<String, f64>>, LabError> {
    let expected = program
        .indicators
        .iter()
        .map(|indicator| indicator.id.clone())
        .collect::<BTreeSet<_>>();
    let mut result = BTreeMap::new();
    for value in values {
        if !expected.contains(&value.id) || result.insert(value.id.clone(), value.value).is_some() {
            return Err(LabError::DataCorrupt(
                "source indicator audit saved ids are unknown or duplicated".into(),
            ));
        }
    }
    let found = result.keys().cloned().collect::<BTreeSet<_>>();
    for id in expected.difference(&found) {
        findings.insufficient(Code::MissingRequiredEvidence, format!("saved_values.{id}"));
    }
    if found.len() == expected.len() {
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

fn recompute_indicator(
    indicator: &PolicyIndicator,
    series: &PreparedSeries<'_>,
    interval: CandleInterval,
) -> IndicatorResult {
    if series.bars.is_empty() {
        return IndicatorResult::Insufficient(
            Code::MissingSourceSeries,
            format!("indicators.{}.source_bars", indicator.id),
        );
    }
    if matches!(
        &indicator.indicator,
        PolicyIndicatorKind::SampleVol { window: 1 } | PolicyIndicatorKind::AnnualVol { window: 1 }
    ) {
        return IndicatorResult::Unsupported(format!(
            "indicators.{}.sample_variance_requires_two_returns",
            indicator.id
        ));
    }
    let values = match &indicator.indicator {
        PolicyIndicatorKind::Open => current_field(series, "open"),
        PolicyIndicatorKind::High => current_field(series, "high"),
        PolicyIndicatorKind::Low => current_field(series, "low"),
        PolicyIndicatorKind::Close => current_field(series, "close"),
        PolicyIndicatorKind::Volume => current_field(series, "volume"),
        PolicyIndicatorKind::QuoteTurnover => current_field(series, "quote_turnover"),
        PolicyIndicatorKind::Sma { window } => sma(series, *window),
        PolicyIndicatorKind::Ema { window } => ema(series, *window),
        PolicyIndicatorKind::Rsi { window } => rsi(series, *window),
        PolicyIndicatorKind::SampleVol { window } => sample_vol(series, *window),
        PolicyIndicatorKind::AnnualVol { window } => {
            sample_vol(series, *window).and_then(|value| annualize(value, interval))
        }
        PolicyIndicatorKind::PriorHigh { window } => prior_extreme(series, *window, true),
        PolicyIndicatorKind::PriorLow { window } => prior_extreme(series, *window, false),
    };
    match values {
        Some(value) if value.is_finite() => IndicatorResult::Value(value),
        Some(_) => IndicatorResult::Unsupported(format!("indicators.{}.nonfinite", indicator.id)),
        None if uses_first_seed(&indicator.indicator)
            && (series.source.seed.history_scope != SourceHistoryScope::CompleteFromSeed
                || series.source.seed.original_seed_observation_id.as_ref()
                    != series.bars.first().map(|bar| &bar.id)) =>
        {
            IndicatorResult::Insufficient(
                Code::MissingSourceSeed,
                format!("indicators.{}.seed", indicator.id),
            )
        }
        None if !series.gap_free => IndicatorResult::Insufficient(
            Code::SourceHistoryGap,
            format!("indicators.{}.source_bars", indicator.id),
        ),
        None => IndicatorResult::Insufficient(
            Code::IncompleteWarmup,
            format!("indicators.{}.warmup", indicator.id),
        ),
    }
}

fn uses_first_seed(kind: &PolicyIndicatorKind) -> bool {
    matches!(
        kind,
        PolicyIndicatorKind::Ema { .. } | PolicyIndicatorKind::Rsi { .. }
    )
}

fn current_field(series: &PreparedSeries<'_>, field: &str) -> Option<f64> {
    let candle = &series.bars.last()?.candle;
    let value = match field {
        "open" => candle.open.get(),
        "high" => candle.high.get(),
        "low" => candle.low.get(),
        "close" => candle.close.get(),
        "volume" => candle.volume.get(),
        "quote_turnover" => candle.quote_turnover.get(),
        _ => return None,
    };
    decimal_to_f64(value, field).ok()
}

fn closes(series: &PreparedSeries<'_>) -> Option<Vec<f64>> {
    series
        .bars
        .iter()
        .map(|bar| decimal_to_f64(bar.candle.close.get(), "close").ok())
        .collect()
}

fn sma(series: &PreparedSeries<'_>, window: usize) -> Option<f64> {
    let data = closes(series)?;
    let tail = data.get(data.len().checked_sub(window)?..)?;
    if !tail_is_contiguous(series, window) {
        return None;
    }
    let average = tail.iter().sum::<f64>() / f64::from(u32::try_from(window).ok()?);
    average.is_finite().then_some(average)
}

fn ema(series: &PreparedSeries<'_>, window: usize) -> Option<f64> {
    if series.source.seed.history_scope != SourceHistoryScope::CompleteFromSeed
        || series.source.seed.original_seed_observation_id.as_ref()
            != series.bars.first().map(|bar| &bar.id)
        || !series.gap_free
    {
        return None;
    }
    let data = closes(series)?;
    if data.len() < window {
        return None;
    }
    let length = f64::from(u32::try_from(window).ok()?);
    let mut value = data[..window].iter().sum::<f64>() / length;
    let alpha = 2.0 / (length + 1.0);
    for close in &data[window..] {
        value = alpha.mul_add(*close - value, value);
        if !value.is_finite() {
            return None;
        }
    }
    value.is_finite().then_some(value)
}

fn rsi(series: &PreparedSeries<'_>, window: usize) -> Option<f64> {
    if series.source.seed.history_scope != SourceHistoryScope::CompleteFromSeed
        || series.source.seed.original_seed_observation_id.as_ref()
            != series.bars.first().map(|bar| &bar.id)
        || !series.gap_free
    {
        return None;
    }
    let data = closes(series)?;
    if data.len() < window.checked_add(1)? {
        return None;
    }
    let changes = data
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .collect::<Vec<_>>();
    let mut gains = changes[..window]
        .iter()
        .map(|change| change.max(0.0))
        .sum::<f64>();
    let mut losses = changes[..window]
        .iter()
        .map(|change| (-change).max(0.0))
        .sum::<f64>();
    let length = f64::from(u32::try_from(window).ok()?);
    gains /= length;
    losses /= length;
    for change in &changes[window..] {
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        gains = ((length - 1.0) * gains + gain) / length;
        losses = ((length - 1.0) * losses + loss) / length;
    }
    let value = if gains == 0.0 && losses == 0.0 {
        50.0
    } else if losses == 0.0 {
        100.0
    } else if gains == 0.0 {
        0.0
    } else {
        100.0 - 100.0 / (1.0 + gains / losses)
    };
    value.is_finite().then_some(value)
}

fn sample_vol(series: &PreparedSeries<'_>, window: usize) -> Option<f64> {
    let data = closes(series)?;
    let required = window.checked_add(1)?;
    let tail = data.get(data.len().checked_sub(required)?..)?;
    if !tail_is_contiguous(series, required) {
        return None;
    }
    let returns = tail
        .windows(2)
        .map(|pair| (pair[1] / pair[0]).ln())
        .collect::<Vec<_>>();
    if returns.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let count = f64::from(u32::try_from(returns.len()).ok()?);
    let mean = returns.iter().sum::<f64>() / count;
    let variance = returns
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / (count - 1.0);
    let sigma = variance.sqrt();
    sigma.is_finite().then_some(sigma)
}

fn annualize(sigma: f64, interval: CandleInterval) -> Option<f64> {
    let seconds = i32::try_from(interval.duration().num_seconds()).ok()?;
    if seconds <= 0 {
        return None;
    }
    let annual = sigma * (365.25 * 24.0 * 60.0 * 60.0 / f64::from(seconds)).sqrt();
    annual.is_finite().then_some(annual)
}

fn prior_extreme(series: &PreparedSeries<'_>, window: usize, maximum: bool) -> Option<f64> {
    let end = series.bars.len().checked_sub(1)?;
    let start = end.checked_sub(window)?;
    if !tail_is_contiguous(series, window.checked_add(1)?) {
        return None;
    }
    let values = series.bars[start..end]
        .iter()
        .map(|bar| {
            if maximum {
                decimal_to_f64(bar.candle.high.get(), "prior high").ok()
            } else {
                decimal_to_f64(bar.candle.low.get(), "prior low").ok()
            }
        })
        .collect::<Option<Vec<_>>>()?;
    if maximum {
        values.into_iter().reduce(f64::max)
    } else {
        values.into_iter().reduce(f64::min)
    }
}

fn tail_is_contiguous(series: &PreparedSeries<'_>, count: usize) -> bool {
    let Some(start) = series.bars.len().checked_sub(count) else {
        return false;
    };
    series.bars[start..]
        .windows(2)
        .all(|pair| pair[0].candle.close_time_utc == pair[1].candle.open_time_utc)
}

fn diagnostic(code: Code, path: impl Into<String>) -> ReviewAuditDiagnostic {
    ReviewAuditDiagnostic {
        code,
        path: path.into(),
    }
}
