//! Read-only adapters from saved paper facts to bounded audit inputs.

use super::{Store, enum_text, json_error, sql_error};
use crate::contracts::{
    AccountMark, CandleInterval, CandleObservation, ContentHash, IndicatorSeedEvidence,
    IndicatorSourceClosureEvidence, IndicatorSourceReference, IndicatorSourceSeries, LabError,
    MarketId, ObservationId, PaperDecisionReview, PaperReviewSelection, PolicyIndicator,
    PolicyIndicatorSourceTrace, PolicyProgram, PolicySourceCaptureOrigin,
    PolicySourceObservationRef, PolicyTrace, PositionState, ReasonCode, RuleSnapshotContext,
    SavedRuleDecision, SignalRecord, SnapshotRuleAuditInput, SourceCaptureOrigin,
    SourceHistoryScope, SourceIndicatorAuditInput, UtcTimestamp,
};
use rusqlite::params;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const MAX_PRIOR_EVENTS: usize = 64;
const MAX_PRIOR_EVENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_SOURCE_BARS: u32 = 4_096;
const MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;

struct PreviousEventWindow {
    events: Vec<crate::contracts::PaperSessionEvent>,
}

struct SourceGroup {
    interval: CandleInterval,
    required_warmup_bars: u32,
    capture: Option<PolicyIndicatorSourceTrace>,
    limit: usize,
}

struct SourceReadBudget {
    remaining_bars: usize,
    remaining_bytes: usize,
    observation_ids: BTreeSet<ObservationId>,
}

struct SourceIndicatorFacts<'a> {
    signal: &'a SignalRecord,
    decision_source_id: &'a ObservationId,
    decision_observation: CandleObservation,
    cutoff: UtcTimestamp,
}

struct RuleAuditFacts<'a> {
    trace: &'a PolicyTrace,
    decision_observation: CandleObservation,
    actual_state: PositionState,
    actual_weight: crate::contracts::Weight,
}

impl SourceReadBudget {
    fn admit(&mut self, observations: &[CandleObservation]) -> bool {
        if observations.len() > self.remaining_bars {
            return false;
        }
        let mut new_ids = BTreeSet::new();
        if observations.iter().any(|observation| {
            self.observation_ids.contains(&observation.id)
                || !new_ids.insert(observation.id.clone())
        }) {
            return false;
        }
        self.remaining_bars -= observations.len();
        self.observation_ids.extend(new_ids);
        true
    }
}

#[derive(Serialize)]
struct SourceClosureDigestProjection<'a> {
    version: &'static str,
    capture: &'a PolicyIndicatorSourceTrace,
    cutoff_utc: UtcTimestamp,
    source: &'a IndicatorSourceReference,
    latest: &'a PolicySourceObservationRef,
}

impl Store {
    /// Build a snapshot reconstruction input from a saved paper signal.
    ///
    /// # Errors
    /// Rejects corrupt saved identities and database/JSON disagreements.
    pub fn paper_rule_audit_input(
        &self,
        review: &PaperDecisionReview,
    ) -> Result<Option<SnapshotRuleAuditInput>, LabError> {
        self.build_rule_audit_input(review, true)
    }

    fn build_rule_audit_input(
        &self,
        review: &PaperDecisionReview,
        include_prior_cross: bool,
    ) -> Result<Option<SnapshotRuleAuditInput>, LabError> {
        let Some(signal) = selected_review_signal(review)? else {
            return Ok(None);
        };
        if signal.reasons.contains(&ReasonCode::ArtificialTerminalExit)
            || signal.policy_ref.as_ref() != Some(&review.policy.reference)
            || !matches!(
                &review.policy.definition.program,
                PolicyProgram::Rules { .. }
            )
        {
            return Ok(None);
        }
        let history = self.previous_event_window(review)?;
        let Some(facts) = self.rule_audit_facts(review, signal, &history)? else {
            return Ok(None);
        };
        let current = RuleSnapshotContext {
            observation_id: facts.decision_observation.id.clone(),
            decision_time_utc: signal.decision_available_at,
            completed_bars_seen: signal.indicators.completed_bars_seen,
            indicators: signal.indicators.policy_values.clone(),
            state_before: facts.trace.state_before.clone(),
            actual_state: facts.actual_state,
            actual_weight: facts.actual_weight,
            held_bars: None,
            evidence: facts.trace.evidence.clone(),
        };
        let saved = SavedRuleDecision {
            policy_ref: review.policy.reference.clone(),
            observation_id: facts.decision_observation.id.clone(),
            decision_time_utc: signal.decision_available_at,
            matched_rule_id: facts.trace.matched_rule_id.clone(),
            target_weight: signal.raw_target_weight,
            state_after: facts.trace.state_after.clone(),
        };
        let mut input = SnapshotRuleAuditInput {
            policy: review.policy.clone(),
            market: signal.context.market.clone(),
            decision_interval: review.configuration.decision_interval,
            cutoff_utc: facts.decision_observation.candle.close_time_utc,
            decision_observation: facts.decision_observation,
            current,
            saved,
            prior_cross_snapshot: None,
        };
        if include_prior_cross {
            input.prior_cross_snapshot = self.prior_cross_snapshot(review, signal, &history)?;
        }
        Ok(Some(input))
    }

    fn rule_audit_facts<'a>(
        &self,
        review: &PaperDecisionReview,
        signal: &'a SignalRecord,
        history: &PreviousEventWindow,
    ) -> Result<Option<RuleAuditFacts<'a>>, LabError> {
        let Some(trace) = signal.policy_trace.as_ref() else {
            return Ok(None);
        };
        let Some(source_id) = signal.source_bar_ids.last() else {
            return Ok(None);
        };
        if signal.source_bar_ids.iter().collect::<BTreeSet<_>>().len()
            != signal.source_bar_ids.len()
        {
            return Err(LabError::DataCorrupt(
                "paper signal has duplicate source observation ids".into(),
            ));
        }
        let Some(decision_observation) =
            super::review_decision::load_observation(&self.connection, source_id)?
        else {
            return Ok(None);
        };
        if decision_observation.candle.market != signal.context.market.code()
            || decision_observation.candle.interval != review.configuration.decision_interval
            || !decision_observation.candle.completed
            || decision_observation.candle.close_time_utc != signal.decision_available_at
            || signal.signal_time != signal.decision_available_at
        {
            return Err(LabError::DataCorrupt(
                "paper signal decision observation identity/time mismatch".into(),
            ));
        }
        let Some(mark) = find_mark_before_signal(review, signal, history) else {
            return Ok(None);
        };
        let actual_state = position_state_from_mark(&mark)?;
        let actual_weight = mark.actual_weight;
        if actual_state != signal.state_before
            || (actual_weight.get().is_zero() && actual_state != PositionState::Cash)
            || actual_weight.get().is_sign_negative()
            || (!actual_weight.get().is_zero()
                && actual_weight.get().is_sign_positive()
                && actual_state != PositionState::Long)
        {
            return Err(LabError::DataCorrupt(
                "saved account mark does not match paper signal pre-decision position".into(),
            ));
        }
        if review.policy.definition.requires_evidence() && trace.evidence.is_none() {
            return Ok(None);
        }
        Ok(Some(RuleAuditFacts {
            trace,
            decision_observation,
            actual_state,
            actual_weight,
        }))
    }

    fn prior_cross_snapshot(
        &self,
        review: &PaperDecisionReview,
        signal: &SignalRecord,
        history: &PreviousEventWindow,
    ) -> Result<Option<crate::contracts::RuleCrossPriorSnapshot>, LabError> {
        let Some(previous) = find_previous_policy_signal(review, signal, history) else {
            return Ok(None);
        };
        let previous_review = self.paper_review_decision(&PaperReviewSelection {
            session_id: review.selection.session_id.clone(),
            event_sequence: previous.0.sequence,
            as_of_sequence: review.selection.as_of_sequence,
            signal_id: Some(previous.1.signal_id.clone()),
        })?;
        let Some(previous_input) = self.build_rule_audit_input(&previous_review, false)? else {
            return Ok(None);
        };
        crate::reporting::review_audit::reconstruct_cross_prior_snapshot(&previous_input)
    }

    /// Build a cutoff-bounded source-indicator input from saved observations.
    ///
    /// # Errors
    /// Rejects corrupt SQL/JSON identities, duplicate slots and resource overages.
    pub fn paper_indicator_audit_input(
        &self,
        review: &PaperDecisionReview,
        max_source_bars: u32,
    ) -> Result<Option<SourceIndicatorAuditInput>, LabError> {
        if !(1..=MAX_SOURCE_BARS).contains(&max_source_bars) {
            return Err(LabError::InvalidConfig(format!(
                "paper source audit bar limit must be 1..={MAX_SOURCE_BARS}"
            )));
        }
        let Some(facts) = self.source_indicator_facts(review)? else {
            return Ok(None);
        };
        let PolicyProgram::Rules { program } = &review.policy.definition.program else {
            return Ok(None);
        };
        if program.indicators.is_empty() {
            return Ok(None);
        }
        let trace_by_id = source_trace_by_id(facts.signal)?;
        let Some(groups) = build_source_groups(
            program,
            review.configuration.decision_interval,
            &trace_by_id,
            max_source_bars as usize,
        )?
        else {
            return Ok(None);
        };
        let mut budget = SourceReadBudget {
            remaining_bars: max_source_bars as usize,
            remaining_bytes: MAX_SOURCE_BYTES,
            observation_ids: BTreeSet::new(),
        };
        let Some(source_series) = self.load_source_series(
            &facts.signal.context.market,
            review.configuration.decision_interval,
            facts.decision_source_id,
            facts.cutoff,
            &groups,
            &mut budget,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(SourceIndicatorAuditInput {
            policy: review.policy.clone(),
            saved_policy_ref: review.policy.reference.clone(),
            market: facts.signal.context.market.clone(),
            decision_interval: review.configuration.decision_interval,
            cutoff_utc: facts.cutoff,
            decision_observation: facts.decision_observation,
            source_series,
            saved_values: facts.signal.indicators.policy_values.clone(),
        }))
    }

    fn source_indicator_facts<'a>(
        &self,
        review: &'a PaperDecisionReview,
    ) -> Result<Option<SourceIndicatorFacts<'a>>, LabError> {
        let Some(signal) = selected_review_signal(review)? else {
            return Ok(None);
        };
        if signal.reasons.contains(&ReasonCode::ArtificialTerminalExit)
            || signal.policy_ref.as_ref() != Some(&review.policy.reference)
        {
            return Ok(None);
        }
        let Some(source_id) = signal.source_bar_ids.last() else {
            return Ok(None);
        };
        if signal.source_bar_ids.iter().collect::<BTreeSet<_>>().len()
            != signal.source_bar_ids.len()
        {
            return Err(LabError::DataCorrupt(
                "paper signal has duplicate source observation ids".into(),
            ));
        }
        let Some(decision_observation) =
            super::review_decision::load_observation(&self.connection, source_id)?
        else {
            return Ok(None);
        };
        let cutoff = signal.decision_available_at;
        if decision_observation.candle.market != signal.context.market.code()
            || decision_observation.candle.interval != review.configuration.decision_interval
            || !decision_observation.candle.completed
            || decision_observation.candle.close_time_utc != cutoff
            || signal.signal_time != cutoff
        {
            return Err(LabError::DataCorrupt(
                "paper indicator decision observation identity/time mismatch".into(),
            ));
        }
        Ok(Some(SourceIndicatorFacts {
            signal,
            decision_source_id: source_id,
            decision_observation,
            cutoff,
        }))
    }

    fn load_source_series(
        &self,
        market: &MarketId,
        decision_interval: CandleInterval,
        decision_source_id: &ObservationId,
        cutoff: UtcTimestamp,
        groups: &[SourceGroup],
        budget: &mut SourceReadBudget,
    ) -> Result<Option<Vec<IndicatorSourceSeries>>, LabError> {
        let mut source_series = Vec::with_capacity(groups.len());
        for group in groups {
            let Some(series) = self.load_source_group(
                market,
                decision_interval,
                decision_source_id,
                cutoff,
                group,
                budget,
            )?
            else {
                return Ok(None);
            };
            source_series.push(series);
        }
        Ok(Some(source_series))
    }

    fn load_source_group(
        &self,
        market: &MarketId,
        decision_interval: CandleInterval,
        decision_source_id: &ObservationId,
        cutoff: UtcTimestamp,
        group: &SourceGroup,
        budget: &mut SourceReadBudget,
    ) -> Result<Option<IndicatorSourceSeries>, LabError> {
        let Some((observations, complete)) =
            self.source_window_for_group(market, cutoff, group, budget)?
        else {
            return Ok(None);
        };
        if observations.is_empty() || !budget.admit(&observations) {
            return Ok(None);
        }
        if !complete
            && group.capture.as_ref().is_some_and(|capture| {
                !capture.recent_refs.is_empty()
                    && !tail_matches(
                        &capture.recent_refs,
                        &observations,
                        capture.recent_refs.len().min(observations.len()),
                    )
            })
        {
            return Ok(None);
        }
        if group.interval == decision_interval
            && observations.last().map(|bar| &bar.id) != Some(decision_source_id)
        {
            return Ok(None);
        }
        let mut series = source_series_from_window(
            group.interval,
            observations,
            group.required_warmup_bars,
            complete,
        )?;
        if complete
            && group.interval != decision_interval
            && let Some(capture) = group.capture.as_ref()
        {
            series.closure = source_closure_from_capture(
                capture,
                &series.seed.source,
                &series.observations,
                cutoff,
                group.interval,
            )?;
        }
        Ok(Some(series))
    }

    fn source_window_for_group(
        &self,
        market: &MarketId,
        cutoff: UtcTimestamp,
        group: &SourceGroup,
        budget: &mut SourceReadBudget,
    ) -> Result<Option<(Vec<CandleObservation>, bool)>, LabError> {
        if let Some(capture) = group.capture.as_ref()
            && capture.capture_origin == PolicySourceCaptureOrigin::CapturedFromEvaluatorStart
            && usize::try_from(capture.captured_count).is_ok_and(|count| count <= group.limit)
            && capture.capture_anchor.is_some()
            && capture.rolling_digest.is_some()
            && !capture.recent_refs.is_empty()
            && capture.recent_refs.len() <= 8
        {
            let expected = usize::try_from(capture.captured_count).unwrap_or_default();
            let from_open = capture
                .capture_anchor
                .as_ref()
                .map(|reference| reference.open_time);
            let Some(anchored) = self.load_source_window(
                market,
                group.interval,
                cutoff,
                from_open,
                expected
                    .saturating_add(1)
                    .min(group.limit.saturating_add(1)),
                &mut budget.remaining_bytes,
            )?
            else {
                return Ok(None);
            };
            if captured_window_matches(capture, &anchored, expected)? {
                return Ok(Some((anchored, true)));
            }
        }
        let Some(recent) = self.load_source_window(
            market,
            group.interval,
            cutoff,
            None,
            group.limit,
            &mut budget.remaining_bytes,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((recent, false)))
    }

    fn previous_event_window(
        &self,
        review: &PaperDecisionReview,
    ) -> Result<PreviousEventWindow, LabError> {
        let current = review.selection.event_sequence;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence, length(CAST(event_json AS BLOB)), event_json \
             FROM paper_session_events WHERE session_id=?1 AND sequence<?2 \
             ORDER BY sequence DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                review.selection.session_id.as_str(),
                i64::try_from(current).map_err(|_| LabError::ResourceLimit(
                    "paper sequence exceeds SQLite range".into()
                ))?,
                i64::try_from(MAX_PRIOR_EVENTS + 1)
                    .map_err(|_| LabError::ResourceLimit("prior event limit".into()))?,
            ])
            .map_err(sql_error)?;
        let mut events = Vec::with_capacity(MAX_PRIOR_EVENTS);
        let mut bytes = 0_usize;
        let mut expected = current.saturating_sub(1);
        let mut complete = true;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if events.len() == MAX_PRIOR_EVENTS {
                complete = false;
                break;
            }
            let sequence: i64 = row.get(0).map_err(sql_error)?;
            let size: i64 = row.get(1).map_err(sql_error)?;
            bytes =
                bytes.saturating_add(usize::try_from(size).map_err(|_| {
                    LabError::DataCorrupt("negative paper event byte length".into())
                })?);
            if bytes > MAX_PRIOR_EVENT_BYTES {
                complete = false;
                break;
            }
            let event_sequence = u64::try_from(sequence)
                .map_err(|_| LabError::DataCorrupt("negative paper event sequence".into()))?;
            if event_sequence != expected {
                return Err(LabError::DataCorrupt(
                    "paper event history sequence is not contiguous".into(),
                ));
            }
            let raw: String = row.get(2).map_err(sql_error)?;
            let event: crate::contracts::PaperSessionEvent =
                serde_json::from_str(&raw).map_err(json_error)?;
            if event.sequence != event_sequence {
                return Err(LabError::DataCorrupt(
                    "paper event SQL/JSON sequence mismatch".into(),
                ));
            }
            events.push(event);
            expected = expected.saturating_sub(1);
        }
        if complete && events.len() < MAX_PRIOR_EVENTS && expected > 0 {
            return Err(LabError::DataCorrupt(
                "paper event history ended before sequence one".into(),
            ));
        }
        Ok(PreviousEventWindow { events })
    }

    fn load_source_window(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        cutoff: UtcTimestamp,
        from_open: Option<UtcTimestamp>,
        limit: usize,
        bytes_remaining: &mut usize,
    ) -> Result<Option<Vec<CandleObservation>>, LabError> {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let query_limit = limit
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("source bar query limit overflow".into()))?;
        let interval_text = enum_text(&interval)?;
        let mut statement = self.connection.prepare(
            "SELECT length(CAST(observation_json AS BLOB)),id,market,interval,open_time_ms,close_time_ms,content_digest,observation_json \
             FROM candle_observations \
             WHERE market=?1 AND interval=?2 AND close_time_ms<=?3 \
               AND (?4 IS NULL OR open_time_ms>=?4) \
             ORDER BY open_time_ms DESC,id DESC LIMIT ?5",
        ).map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                market.code(),
                interval_text,
                cutoff.0.timestamp_millis(),
                from_open.map(|value| value.0.timestamp_millis()),
                i64::try_from(query_limit).map_err(|_| LabError::ResourceLimit(
                    "source bar limit exceeds SQLite range".into()
                ))?,
            ])
            .map_err(sql_error)?;
        let mut observations = Vec::with_capacity(limit.min(MAX_SOURCE_BARS as usize));
        let mut sql_open_times = Vec::with_capacity(limit.min(MAX_SOURCE_BARS as usize));
        let mut bytes_used = 0_usize;
        while let Some(row) = rows.next().map_err(sql_error)? {
            let size: i64 = row.get(0).map_err(sql_error)?;
            bytes_used = bytes_used.saturating_add(usize::try_from(size).map_err(|_| {
                LabError::DataCorrupt("negative paper source observation byte length".into())
            })?);
            if bytes_used > *bytes_remaining {
                return Ok(None);
            }
            let open_ms: i64 = row.get(4).map_err(sql_error)?;
            if observations.len() == limit {
                if sql_open_times.last().copied() == Some(open_ms) {
                    return Ok(None);
                }
                break;
            }
            if sql_open_times.last().copied() == Some(open_ms) {
                // Multiple immutable revisions can share a market/interval slot.
                return Ok(None);
            }
            let id: String = row.get(1).map_err(sql_error)?;
            let sql_market: String = row.get(2).map_err(sql_error)?;
            let sql_interval: String = row.get(3).map_err(sql_error)?;
            let close_ms: i64 = row.get(5).map_err(sql_error)?;
            let digest: String = row.get(6).map_err(sql_error)?;
            let raw: String = row.get(7).map_err(sql_error)?;
            let observation: CandleObservation = serde_json::from_str(&raw).map_err(json_error)?;
            let expected_id = ObservationId::new(id)?;
            crate::storage::validate_observation_identity(&observation)?;
            if observation.id != expected_id
                || sql_market != market.code()
                || sql_interval != interval_text
                || open_ms != observation.candle.open_time_utc.0.timestamp_millis()
                || close_ms != observation.candle.close_time_utc.0.timestamp_millis()
                || digest != observation.content_digest.as_str()
                || observation.candle.market != market.code()
                || observation.candle.interval != interval
                || observation.candle.close_time_utc > cutoff
                || !observation.candle.completed
            {
                return Err(LabError::DataCorrupt(
                    "paper source observation SQL/JSON identity mismatch".into(),
                ));
            }
            sql_open_times.push(open_ms);
            observations.push(observation);
        }
        *bytes_remaining = (*bytes_remaining).saturating_sub(bytes_used);
        observations.reverse();
        if observations
            .windows(2)
            .any(|pair| pair[0].candle.open_time_utc >= pair[1].candle.open_time_utc)
        {
            return Ok(None);
        }
        Ok(Some(observations))
    }
}

fn selected_review_signal(review: &PaperDecisionReview) -> Result<Option<&SignalRecord>, LabError> {
    if review.event.sequence != review.selection.event_sequence {
        return Err(LabError::DataCorrupt(
            "paper review selection/event sequence mismatch".into(),
        ));
    }
    let selected = super::review_decision::select_signal(&review.event.signals, &review.selection)?;
    if selected.map(|signal| &signal.signal_id) != review.selected_signal_id.as_ref() {
        return Err(LabError::DataCorrupt(
            "paper review selected signal identity mismatch".into(),
        ));
    }
    Ok(selected)
}

fn source_trace_by_id(
    signal: &SignalRecord,
) -> Result<BTreeMap<String, &PolicyIndicatorSourceTrace>, LabError> {
    let mut traces = BTreeMap::new();
    if let Some(items) = signal
        .policy_trace
        .as_ref()
        .and_then(|trace| trace.source_provenance.as_ref())
    {
        for trace in items {
            if traces.insert(trace.indicator_id.clone(), trace).is_some() {
                return Err(LabError::DataCorrupt(
                    "duplicate policy source trace indicator id".into(),
                ));
            }
        }
    }
    Ok(traces)
}

fn build_source_groups(
    program: &crate::contracts::RulesProgram,
    decision_interval: CandleInterval,
    trace_by_id: &BTreeMap<String, &PolicyIndicatorSourceTrace>,
    max_source_bars: usize,
) -> Result<Option<Vec<SourceGroup>>, LabError> {
    let expected_trace_ids = program
        .indicators
        .iter()
        .map(|indicator| indicator.id.as_str())
        .collect::<BTreeSet<_>>();
    if trace_by_id
        .keys()
        .any(|indicator_id| !expected_trace_ids.contains(indicator_id.as_str()))
    {
        return Err(LabError::DataCorrupt(
            "saved policy source trace contains an unknown indicator".into(),
        ));
    }
    let mut grouped: BTreeMap<CandleInterval, Vec<&PolicyIndicator>> = BTreeMap::new();
    for indicator in &program.indicators {
        let interval = indicator.source_interval.unwrap_or(decision_interval);
        grouped.entry(interval).or_default().push(indicator);
    }
    let mut groups = Vec::with_capacity(grouped.len());
    for (interval, indicators) in grouped {
        let mut warmup = 1_u32;
        let mut traces = Vec::with_capacity(indicators.len());
        for indicator in &indicators {
            let kind_warmup = u32::try_from(indicator.indicator.warmup_bars()).map_err(|_| {
                LabError::ResourceLimit("source indicator warmup exceeds u32".into())
            })?;
            warmup = warmup.max(kind_warmup);
            if let Some(source_trace) = trace_by_id.get(&indicator.id) {
                if source_trace.source_interval != interval
                    || source_trace.required_warmup_bars != u64::from(kind_warmup)
                {
                    return Err(LabError::DataCorrupt(
                        "saved policy source trace does not match frozen indicator".into(),
                    ));
                }
                traces.push((*source_trace).clone());
            }
        }
        groups.push(SourceGroup {
            interval,
            required_warmup_bars: warmup,
            capture: (traces.len() == indicators.len())
                .then(|| consistent_capture(&traces))
                .flatten(),
            limit: usize::try_from(warmup).unwrap_or(usize::MAX),
        });
    }
    let minimum = groups
        .iter()
        .map(|group| group.limit)
        .fold(0_usize, usize::saturating_add);
    if minimum > max_source_bars {
        return Ok(None);
    }
    allocate_source_limits(&mut groups, max_source_bars);
    Ok(Some(groups))
}

fn consistent_capture(traces: &[PolicyIndicatorSourceTrace]) -> Option<PolicyIndicatorSourceTrace> {
    let first = traces.first()?.clone();
    traces
        .iter()
        .all(|trace| {
            trace.capture_origin == first.capture_origin
                && trace.capture_anchor == first.capture_anchor
                && trace.captured_count == first.captured_count
                && trace.recent_refs == first.recent_refs
                && trace.rolling_digest == first.rolling_digest
        })
        .then_some(first)
}

fn allocate_source_limits(groups: &mut [SourceGroup], total: usize) {
    let expected = groups
        .iter()
        .map(|group| {
            group
                .capture
                .as_ref()
                .and_then(|capture| usize::try_from(capture.captured_count).ok())
                .map(|count| count.max(group.limit))
        })
        .collect::<Vec<_>>();
    let expected_total = expected
        .iter()
        .flatten()
        .copied()
        .fold(0_usize, usize::saturating_add);
    let all_captures_fit = expected.iter().all(Option::is_some) && expected_total <= total;
    if all_captures_fit {
        for (group, count) in groups.iter_mut().zip(expected.into_iter().flatten()) {
            group.limit = count;
        }
        return;
    }
    let mut remaining = total.saturating_sub(groups.iter().map(|group| group.limit).sum());
    while remaining > 0 {
        let mut advanced = false;
        for (index, group) in groups.iter_mut().enumerate() {
            if remaining == 0 {
                break;
            }
            let desired = expected[index].unwrap_or(usize::MAX);
            if group.limit >= desired {
                continue;
            }
            group.limit += 1;
            remaining -= 1;
            advanced = true;
        }
        if !advanced {
            break;
        }
    }
}

fn captured_window_matches(
    capture: &PolicyIndicatorSourceTrace,
    observations: &[CandleObservation],
    expected_count: usize,
) -> Result<bool, LabError> {
    if observations.len() != expected_count
        || observations.is_empty()
        || !source_rows_contiguous(observations, capture.source_interval)
    {
        return Ok(false);
    }
    let Some(anchor) = capture.capture_anchor.as_ref() else {
        return Ok(false);
    };
    let refs = observations.iter().map(source_ref).collect::<Vec<_>>();
    let tail_count = capture.recent_refs.len().min(refs.len());
    if anchor != &refs[0]
        || capture.captured_count != expected_count as u64
        || tail_count != capture.recent_refs.len()
        || refs[refs.len() - tail_count..] != capture.recent_refs[..]
        || crate::policy_engine::policy_source_provenance_digest(&refs)? != capture.rolling_digest
    {
        return Ok(false);
    }
    Ok(true)
}

fn source_closure_from_capture(
    capture: &PolicyIndicatorSourceTrace,
    source: &IndicatorSourceReference,
    observations: &[CandleObservation],
    cutoff: UtcTimestamp,
    interval: CandleInterval,
) -> Result<Option<IndicatorSourceClosureEvidence>, LabError> {
    if capture.capture_origin != PolicySourceCaptureOrigin::CapturedFromEvaluatorStart
        || capture.source_interval != interval
        || !matches!(
            source,
            IndicatorSourceReference::CapturedWindow {
                capture_origin: SourceCaptureOrigin::SavedObservationWindow,
                ..
            }
        )
        || observations.is_empty()
        || !captured_window_matches(capture, observations, observations.len())?
    {
        return Ok(None);
    }
    let Some(latest) = observations.last() else {
        return Ok(None);
    };
    let expected_boundary = crate::scheduling::latest_completed_boundary(cutoff, interval)?;
    if latest.candle.close_time_utc > cutoff || latest.candle.close_time_utc != expected_boundary {
        return Ok(None);
    }
    let latest_ref = source_ref(latest);
    let evidence_digest = ContentHash::of_value(&SourceClosureDigestProjection {
        version: "paper-evaluator-source-closure-v1",
        capture,
        cutoff_utc: cutoff,
        source,
        latest: &latest_ref,
    })?;
    Ok(Some(IndicatorSourceClosureEvidence {
        source: source.clone(),
        closed_through_utc: cutoff,
        latest_observation_id: latest.id.clone(),
        latest_close_time_utc: latest.candle.close_time_utc,
        evidence_digest,
    }))
}

fn source_series_from_window(
    interval: CandleInterval,
    observations: Vec<CandleObservation>,
    required_warmup_bars: u32,
    complete: bool,
) -> Result<IndicatorSourceSeries, LabError> {
    let first = observations
        .first()
        .ok_or_else(|| LabError::InvalidConfig("paper source window is empty".into()))?;
    let count = u32::try_from(observations.len())
        .map_err(|_| LabError::ResourceLimit("paper source window count exceeds u32".into()))?;
    let first_id = first.id.clone();
    let reference = IndicatorSourceReference::CapturedWindow {
        first_observation_id: first_id.clone(),
        evidence_digest: ContentHash::of_value(&observations)?,
        observation_count: count,
        capture_origin: SourceCaptureOrigin::SavedObservationWindow,
    };
    Ok(IndicatorSourceSeries {
        interval,
        observations,
        seed: IndicatorSeedEvidence {
            source: reference,
            original_seed_observation_id: complete.then_some(first_id),
            history_scope: if complete {
                SourceHistoryScope::CompleteFromSeed
            } else {
                SourceHistoryScope::TruncatedPrefix
            },
            bars_through_cutoff: count,
            required_warmup_bars,
        },
        // A local retained prefix does not prove that a higher-timeframe source was closed.
        closure: None,
    })
}

fn source_rows_contiguous(observations: &[CandleObservation], interval: CandleInterval) -> bool {
    observations.iter().all(|observation| {
        observation.candle.interval == interval
            && observation.candle.completed
            && observation.candle.close_time_utc.0 - observation.candle.open_time_utc.0
                == interval.duration()
            && observation.candle.open_time_utc.0.timestamp_subsec_nanos() == 0
            && observation.candle.close_time_utc.0.timestamp_subsec_nanos() == 0
            && observation
                .candle
                .open_time_utc
                .0
                .timestamp()
                .rem_euclid(interval.duration().num_seconds())
                == 0
    }) && observations
        .windows(2)
        .all(|pair| pair[0].candle.close_time_utc == pair[1].candle.open_time_utc)
}

fn source_ref(observation: &CandleObservation) -> PolicySourceObservationRef {
    PolicySourceObservationRef {
        observation_id: observation.id.clone(),
        content_digest: observation.content_digest.clone(),
        open_time: observation.candle.open_time_utc,
        close_time: observation.candle.close_time_utc,
    }
}

fn tail_matches(
    captured: &[PolicySourceObservationRef],
    observations: &[CandleObservation],
    overlap: usize,
) -> bool {
    overlap > 0
        && captured[captured.len() - overlap..]
            .iter()
            .zip(&observations[observations.len() - overlap..])
            .all(|(reference, observation)| reference == &source_ref(observation))
}

fn find_mark_before_signal(
    review: &PaperDecisionReview,
    signal: &SignalRecord,
    history: &PreviousEventWindow,
) -> Option<AccountMark> {
    let decision_source_id = signal.source_bar_ids.last()?;
    let matches = |mark: &&AccountMark| {
        mark.context.market == signal.context.market
            && mark.context.event_seq < signal.context.event_seq
            && mark.context.accounting_event_time <= signal.decision_available_at
            && &mark.source_bar_id == decision_source_id
    };
    review
        .event
        .account_marks
        .iter()
        .filter(matches)
        .max_by_key(|mark| mark.context.event_seq)
        .cloned()
        .or_else(|| {
            history.events.iter().find_map(|event| {
                event
                    .account_marks
                    .iter()
                    .filter(matches)
                    .max_by_key(|mark| mark.context.event_seq)
                    .cloned()
            })
        })
}

fn position_state_from_mark(mark: &AccountMark) -> Result<PositionState, LabError> {
    if mark.state.qty.get().is_sign_negative() {
        return Err(LabError::DataCorrupt(
            "paper account mark has negative quantity".into(),
        ));
    }
    Ok(if mark.state.qty.get().is_zero() {
        PositionState::Cash
    } else {
        PositionState::Long
    })
}

fn find_previous_policy_signal<'a>(
    review: &'a PaperDecisionReview,
    signal: &SignalRecord,
    history: &'a PreviousEventWindow,
) -> Option<(&'a crate::contracts::PaperSessionEvent, &'a SignalRecord)> {
    let prior_in_event = review
        .event
        .signals
        .iter()
        .filter(|candidate| {
            candidate.context.market == signal.context.market
                && candidate.context.event_seq < signal.context.event_seq
                && candidate.policy_ref.is_some()
                && !candidate
                    .reasons
                    .contains(&ReasonCode::ArtificialTerminalExit)
        })
        .max_by_key(|candidate| candidate.context.event_seq)
        .map(|candidate| (&review.event, candidate));
    let previous = prior_in_event.or_else(|| {
        history.events.iter().find_map(|event| {
            event
                .signals
                .iter()
                .filter(|candidate| {
                    candidate.context.market == signal.context.market
                        && candidate.policy_ref.is_some()
                        && candidate.context.event_seq < signal.context.event_seq
                        && !candidate
                            .reasons
                            .contains(&ReasonCode::ArtificialTerminalExit)
                })
                .max_by_key(|candidate| candidate.context.event_seq)
                .map(|candidate| (event, candidate))
        })
    })?;
    (previous.1.policy_ref.as_ref() == Some(&review.policy.reference)).then_some(previous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::Decimal;

    fn source_observation(index: i64) -> CandleObservation {
        let epoch = Utc
            .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
            .single()
            .expect("fixed fixture time");
        let open_time = UtcTimestamp(epoch + Duration::hours(index));
        let price = crate::contracts::PriceKrw::new(Decimal::from(100 + index))
            .expect("valid fixture price");
        let candle = crate::contracts::CandleRecord {
            market: MarketId::parse_upbit("KRW-BTC")
                .expect("fixture market")
                .code(),
            interval: CandleInterval::H1,
            open_time_utc: open_time,
            close_time_utc: UtcTimestamp(open_time.0 + Duration::hours(1)),
            open: price,
            high: crate::contracts::PriceKrw::new(price.get() + Decimal::ONE)
                .expect("valid fixture high"),
            low: crate::contracts::PriceKrw::new(price.get() - Decimal::ONE)
                .expect("valid fixture low"),
            close: price,
            volume: crate::contracts::AssetQuantity::new(Decimal::ONE)
                .expect("valid fixture volume"),
            quote_turnover: crate::contracts::QuoteAmount::new(Decimal::from(100))
                .expect("valid fixture turnover"),
            completed: true,
        };
        let content_digest =
            crate::contracts::observation_digest(&candle).expect("fixture observation digest");
        CandleObservation {
            id: ObservationId::from_seed(content_digest.as_str()),
            candle,
            content_digest,
            raw_object_ids: Vec::new(),
            constituent_ids: Vec::new(),
        }
    }

    #[test]
    fn mtf_closure_requires_a_complete_capture_at_latest_boundary() -> Result<(), LabError> {
        let observations = (0..3).map(source_observation).collect::<Vec<_>>();
        let references = observations.iter().map(source_ref).collect::<Vec<_>>();
        let capture = PolicyIndicatorSourceTrace {
            indicator_id: "close".into(),
            source_interval: CandleInterval::H1,
            required_warmup_bars: 1,
            capture_origin: PolicySourceCaptureOrigin::CapturedFromEvaluatorStart,
            capture_anchor: references.first().cloned(),
            captured_count: u64::try_from(observations.len())
                .map_err(|_| LabError::ResourceLimit("fixture count".into()))?,
            recent_refs: references.clone(),
            rolling_digest: crate::policy_engine::policy_source_provenance_digest(&references)?,
        };
        let source = IndicatorSourceReference::CapturedWindow {
            first_observation_id: observations[0].id.clone(),
            evidence_digest: ContentHash::of_value(&observations)?,
            observation_count: u32::try_from(observations.len())
                .map_err(|_| LabError::ResourceLimit("fixture count".into()))?,
            capture_origin: SourceCaptureOrigin::SavedObservationWindow,
        };
        let latest_close = observations
            .last()
            .map(|bar| bar.candle.close_time_utc)
            .ok_or_else(|| LabError::InvalidConfig("fixture has no source bars".into()))?;

        let current = source_closure_from_capture(
            &capture,
            &source,
            &observations,
            latest_close,
            CandleInterval::H1,
        )?;
        assert!(current.is_some());

        let stale_cutoff = UtcTimestamp(latest_close.0 + Duration::hours(1));
        let stale = source_closure_from_capture(
            &capture,
            &source,
            &observations,
            stale_cutoff,
            CandleInterval::H1,
        )?;
        assert!(stale.is_none());
        Ok(())
    }
}
