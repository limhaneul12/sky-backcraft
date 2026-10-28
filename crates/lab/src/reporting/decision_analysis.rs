//! Pure projections and complete-population counts for saved decision facts.

use crate::contracts::{
    DECISION_ANALYSIS_SCHEMA_VERSION, DecisionAnalysisActionCounts, DecisionAnalysisCaptureCounts,
    DecisionAnalysisChangedField, DecisionAnalysisComparison, DecisionAnalysisComparisonKind,
    DecisionAnalysisComparisonTarget, DecisionAnalysisComparisonUnavailableReason,
    DecisionAnalysisConditionCount, DecisionAnalysisDecisionDetail, DecisionAnalysisDecisionFilter,
    DecisionAnalysisDecisionId, DecisionAnalysisDecisionRow, DecisionAnalysisExecutionCounts,
    DecisionAnalysisMeasurement, DecisionAnalysisObservationCount, DecisionAnalysisPage,
    DecisionAnalysisPeriodPlacement, DecisionAnalysisQuery, DecisionAnalysisReasonCount,
    DecisionAnalysisResponse, DecisionAnalysisSourceSnapshot, DecisionAnalysisTotals, LabError,
    MAX_DECISION_ANALYSIS_ELAPSED_MILLIS, MAX_DECISION_ANALYSIS_OUTPUT_BYTES,
    RecordedDecisionAction, RecordedDecisionStage, RecordedExecutionState, SavedFactCaptureStatus,
};
use crate::storage::{DecisionAnalysisReadSnapshot, DecisionAnalysisSourceData};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct ReasonAccumulator {
    stage: Option<RecordedDecisionStage>,
    code: String,
    distinct_decisions: BTreeSet<String>,
    occurrences: u64,
}

#[derive(Default)]
struct ConditionAccumulator {
    policy_ref: Option<crate::contracts::PolicyRevisionRef>,
    scope: Option<crate::contracts::PolicyConditionScope>,
    ast_path: String,
    role: Option<crate::contracts::PolicyConditionRole>,
    outcome: Option<crate::contracts::PolicyConditionOutcome>,
    occurrences: u64,
}

#[derive(Default)]
struct ObservationAccumulator {
    kind: String,
    basis_time: Option<crate::contracts::DecisionAnalysisObservationClock>,
    scope: Option<crate::contracts::DecisionAnalysisObservationScope>,
    observations: u64,
    unplaced: u64,
}

pub(crate) fn analyze(
    snapshot: DecisionAnalysisReadSnapshot,
    query: DecisionAnalysisQuery,
) -> Result<DecisionAnalysisResponse, LabError> {
    match query {
        DecisionAnalysisQuery::Overview { .. } => {
            let totals = totals(&snapshot.selected)?;
            Ok(DecisionAnalysisResponse::Overview {
                schema_version: DECISION_ANALYSIS_SCHEMA_VERSION.into(),
                source_snapshot: snapshot.selected.source_snapshot,
                population_complete: true,
                totals,
                measurement: empty_measurement(),
            })
        }
        DecisionAnalysisQuery::Decisions { page, filter, .. } => {
            let page = decisions_page(&snapshot.selected, page, filter.as_ref())?;
            Ok(DecisionAnalysisResponse::Decisions {
                schema_version: DECISION_ANALYSIS_SCHEMA_VERSION.into(),
                source_snapshot: snapshot.selected.source_snapshot,
                filter: filter.clone(),
                page,
                measurement: empty_measurement(),
            })
        }
        DecisionAnalysisQuery::Decision { decision_id, .. } => {
            let decision = find_decision(&snapshot.selected, &decision_id)?.clone();
            Ok(DecisionAnalysisResponse::Decision {
                schema_version: DECISION_ANALYSIS_SCHEMA_VERSION.into(),
                source_snapshot: snapshot.selected.source_snapshot,
                decision,
                measurement: empty_measurement(),
            })
        }
        DecisionAnalysisQuery::Compare {
            decision_id,
            against,
            ..
        } => compare(snapshot, &decision_id, against),
    }
}

#[derive(Debug, Default)]
struct DecisionCountTotals {
    strategy: DecisionAnalysisActionCounts,
    constrained: DecisionAnalysisActionCounts,
    execution: DecisionAnalysisExecutionCounts,
    condition_trace_capture: DecisionAnalysisCaptureCounts,
    decisions_with_time: u64,
    orders: u64,
    decisions_with_orders: u64,
    fills: u64,
    decisions_with_fills: u64,
}

fn totals(source: &DecisionAnalysisSourceData) -> Result<DecisionAnalysisTotals, LabError> {
    let placed = source
        .decisions
        .iter()
        .filter(|decision| {
            decision.row.period_placement == DecisionAnalysisPeriodPlacement::InPeriod
        })
        .collect::<Vec<_>>();
    let unplaced = source
        .decisions
        .iter()
        .filter(|decision| {
            decision.row.period_placement == DecisionAnalysisPeriodPlacement::Unplaced
        })
        .count();
    let counts = placed_decision_counts(&placed)?;
    Ok(DecisionAnalysisTotals {
        decisions: len_u64(placed.len())?,
        decisions_with_time: counts.decisions_with_time,
        unplaced_decisions: len_u64(unplaced)?,
        strategy: counts.strategy,
        constrained: counts.constrained,
        execution: counts.execution,
        orders: counts.orders,
        decisions_with_orders: counts.decisions_with_orders,
        fills: counts.fills,
        decisions_with_fills: counts.decisions_with_fills,
        condition_trace_capture: counts.condition_trace_capture,
        reasons: reason_counts(&placed)?,
        conditions: condition_counts(&placed)?,
        non_decision_observations: observation_counts(source)?,
    })
}

fn placed_decision_counts(
    decisions: &[&DecisionAnalysisDecisionDetail],
) -> Result<DecisionCountTotals, LabError> {
    let mut counts = DecisionCountTotals::default();
    for decision in decisions {
        count_action(&mut counts.strategy, decision.row.strategy_action);
        count_action(&mut counts.constrained, decision.row.constrained_action);
        count_execution(&mut counts.execution, decision.row.execution_state);
        match decision.explanation.condition_trace_capture {
            SavedFactCaptureStatus::Captured => counts.condition_trace_capture.captured += 1,
            SavedFactCaptureStatus::PartialCapture => {
                counts.condition_trace_capture.partial_capture += 1;
            }
            SavedFactCaptureStatus::NotCaptured => {
                counts.condition_trace_capture.not_captured += 1;
            }
            SavedFactCaptureStatus::NotDecisionEvent => {
                counts.condition_trace_capture.not_decision_event += 1;
            }
        }
        if decision.row.decision_time.is_some() {
            counts.decisions_with_time += 1;
        }
        counts.orders = counts
            .orders
            .saturating_add(len_u64(decision.explanation.orders.len())?);
        if !decision.explanation.orders.is_empty() {
            counts.decisions_with_orders += 1;
        }
        counts.fills = counts
            .fills
            .saturating_add(len_u64(decision.explanation.fills.len())?);
        if !decision.explanation.fills.is_empty() {
            counts.decisions_with_fills += 1;
        }
    }
    Ok(counts)
}

fn reason_counts(
    decisions: &[&DecisionAnalysisDecisionDetail],
) -> Result<Vec<DecisionAnalysisReasonCount>, LabError> {
    let mut reasons: BTreeMap<String, ReasonAccumulator> = BTreeMap::new();
    for decision in decisions {
        let decision_key = serde_json::to_string(&decision.row.decision_id)?;
        for reason in &decision.explanation.reasons {
            let key = serde_json::to_string(&(reason.stage, &reason.code))?;
            let entry = reasons.entry(key).or_default();
            entry.stage = Some(reason.stage);
            entry.code.clone_from(&reason.code);
            entry.distinct_decisions.insert(decision_key.clone());
            entry.occurrences += 1;
        }
    }
    reasons
        .into_values()
        .map(|item| {
            Ok(DecisionAnalysisReasonCount {
                stage: item
                    .stage
                    .ok_or_else(|| LabError::Internal("reason aggregate has no stage".into()))?,
                code: item.code,
                distinct_decisions: len_u64(item.distinct_decisions.len())?,
                occurrences: item.occurrences,
            })
        })
        .collect()
}

fn condition_counts(
    decisions: &[&DecisionAnalysisDecisionDetail],
) -> Result<Vec<DecisionAnalysisConditionCount>, LabError> {
    let mut conditions: BTreeMap<String, ConditionAccumulator> = BTreeMap::new();
    for decision in decisions {
        let policy_ref = decision
            .explanation
            .policy_ref
            .clone()
            .unwrap_or_else(|| decision.row.policy_ref.clone());
        for trace in &decision.explanation.condition_trace {
            let key = serde_json::to_string(&(
                &policy_ref,
                trace.scope.clone(),
                &trace.ast_path,
                trace.role,
                trace.outcome.clone(),
            ))?;
            let entry = conditions.entry(key).or_default();
            entry.policy_ref = Some(policy_ref.clone());
            entry.scope = Some(trace.scope.clone());
            entry.ast_path.clone_from(&trace.ast_path);
            entry.role = Some(trace.role);
            entry.outcome = Some(trace.outcome.clone());
            entry.occurrences += 1;
        }
    }
    conditions
        .into_values()
        .map(|item| {
            Ok(DecisionAnalysisConditionCount {
                policy_ref: item.policy_ref.ok_or_else(|| {
                    LabError::Internal("condition aggregate has no policy revision".into())
                })?,
                scope: item
                    .scope
                    .ok_or_else(|| LabError::Internal("condition aggregate has no scope".into()))?,
                ast_path: item.ast_path,
                role: item
                    .role
                    .ok_or_else(|| LabError::Internal("condition aggregate has no role".into()))?,
                outcome: item.outcome.ok_or_else(|| {
                    LabError::Internal("condition aggregate has no outcome".into())
                })?,
                occurrences: item.occurrences,
            })
        })
        .collect()
}

fn observation_counts(
    source: &DecisionAnalysisSourceData,
) -> Result<Vec<DecisionAnalysisObservationCount>, LabError> {
    let mut observations: BTreeMap<String, ObservationAccumulator> = BTreeMap::new();
    for observation in &source.observations {
        if let Some(filter_market) = &source.source_snapshot.selection.population.market
            && matches!(
                &observation.scope,
                crate::contracts::DecisionAnalysisObservationScope::Market { market }
                    if market != filter_market
            )
        {
            continue;
        }
        let key = serde_json::to_string(&(
            &observation.kind,
            observation.basis_time,
            &observation.scope,
        ))?;
        let entry = observations.entry(key).or_default();
        entry.kind.clone_from(&observation.kind);
        entry.basis_time = Some(observation.basis_time);
        entry.scope = Some(observation.scope.clone());
        if observation.at.is_some_and(|at| in_period(source, at)) {
            entry.observations += 1;
        } else if observation.at.is_none() {
            entry.unplaced += 1;
        }
    }
    observations
        .into_values()
        .filter(|item| item.observations > 0 || item.unplaced > 0)
        .map(|item| {
            Ok(DecisionAnalysisObservationCount {
                kind: item.kind,
                basis_time: item.basis_time.ok_or_else(|| {
                    LabError::Internal("observation aggregate has no clock basis".into())
                })?,
                scope: item.scope.ok_or_else(|| {
                    LabError::Internal("observation aggregate has no scope".into())
                })?,
                condition_trace_capture: SavedFactCaptureStatus::NotDecisionEvent,
                observations: item.observations,
                unplaced: item.unplaced,
            })
        })
        .collect()
}

fn decisions_page(
    source: &DecisionAnalysisSourceData,
    page_request: crate::contracts::DecisionAnalysisPageRequest,
    filter: Option<&DecisionAnalysisDecisionFilter>,
) -> Result<DecisionAnalysisPage, LabError> {
    let mut matched = source
        .decisions
        .iter()
        .filter(|decision| filter_matches(&decision.row, filter))
        .collect::<Vec<_>>();
    matched.sort_by(|left, right| compare_rows(&left.row, &right.row));
    let in_period_count = len_u64(
        matched
            .iter()
            .filter(|decision| {
                decision.row.period_placement == DecisionAnalysisPeriodPlacement::InPeriod
            })
            .count(),
    )?;
    let unplaced_count = len_u64(
        matched
            .iter()
            .filter(|decision| {
                decision.row.period_placement == DecisionAnalysisPeriodPlacement::Unplaced
            })
            .count(),
    )?;
    let total_count = len_u64(matched.len())?;
    let start = usize::try_from(page_request.offset).map_err(|_| {
        LabError::InvalidConfig("decision page offset exceeds platform range".into())
    })?;
    let limit = usize::try_from(page_request.limit).map_err(|_| {
        LabError::InvalidConfig("decision page limit exceeds platform range".into())
    })?;
    let end = start.saturating_add(limit).min(matched.len());
    let items = if start < matched.len() {
        matched[start..end]
            .iter()
            .map(|decision| decision.row.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let returned_count = len_u32(items.len())?;
    let next_offset = if u64::from(page_request.offset) + u64::from(returned_count) < total_count {
        Some(
            page_request
                .offset
                .checked_add(returned_count)
                .ok_or_else(|| LabError::ResourceLimit("decision page offset overflow".into()))?,
        )
    } else {
        None
    };
    Ok(DecisionAnalysisPage {
        offset: page_request.offset,
        limit: page_request.limit,
        returned_count,
        total_count,
        in_period_count,
        unplaced_count,
        next_offset,
        items,
    })
}

fn filter_matches(
    row: &DecisionAnalysisDecisionRow,
    filter: Option<&DecisionAnalysisDecisionFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter
        .strategy_action
        .is_some_and(|action| action != row.strategy_action)
        || filter
            .constrained_action
            .is_some_and(|action| action != row.constrained_action)
        || filter
            .execution_state
            .is_some_and(|state| state != row.execution_state)
    {
        return false;
    }
    if filter.stage.is_none() && filter.reason_code.is_none() {
        return true;
    }
    row.reasons.iter().any(|reason| {
        filter.stage.is_none_or(|stage| reason.stage == stage)
            && filter
                .reason_code
                .as_ref()
                .is_none_or(|code| &reason.code == code)
    })
}

fn find_decision<'a>(
    source: &'a DecisionAnalysisSourceData,
    id: &DecisionAnalysisDecisionId,
) -> Result<&'a DecisionAnalysisDecisionDetail, LabError> {
    source
        .decisions
        .iter()
        .find(|decision| &decision.row.decision_id == id)
        .ok_or_else(|| {
            LabError::InvalidConfig(
                "decision is outside the selected period or unavailable at this source fence"
                    .into(),
            )
        })
}

fn compare(
    snapshot: DecisionAnalysisReadSnapshot,
    selected_id: &DecisionAnalysisDecisionId,
    target: DecisionAnalysisComparisonTarget,
) -> Result<DecisionAnalysisResponse, LabError> {
    let selected_source = snapshot.selected;
    let selected = find_decision(&selected_source, selected_id)?.clone();
    let (kind, compared_source_snapshot, compared, unavailable_reason) = match target {
        DecisionAnalysisComparisonTarget::Previous => {
            if selected.row.decision_time.is_none() {
                (
                    DecisionAnalysisComparisonKind::Previous,
                    None,
                    None,
                    Some(DecisionAnalysisComparisonUnavailableReason::MissingDecisionTime),
                )
            } else if let Some(previous) = previous_decision(&selected_source, &selected) {
                (
                    DecisionAnalysisComparisonKind::Previous,
                    Some(selected_source.source_snapshot.clone()),
                    Some(previous.clone()),
                    None,
                )
            } else if let Some(previous) = previous_context_decision(&selected_source, &selected) {
                let compared_snapshot =
                    previous_context_snapshot(&selected_source.source_snapshot, previous)?;
                (
                    DecisionAnalysisComparisonKind::Previous,
                    Some(compared_snapshot),
                    Some(previous.clone()),
                    None,
                )
            } else {
                (
                    DecisionAnalysisComparisonKind::Previous,
                    None,
                    None,
                    Some(DecisionAnalysisComparisonUnavailableReason::NoPreviousDecision),
                )
            }
        }
        DecisionAnalysisComparisonTarget::Selected {
            selection: _,
            decision_id,
        } => {
            let compared_source = snapshot.compared.ok_or_else(|| {
                LabError::Internal("selected comparison source snapshot is missing".into())
            })?;
            let compared = find_decision(&compared_source, &decision_id)?.clone();
            (
                DecisionAnalysisComparisonKind::Selected,
                Some(compared_source.source_snapshot.clone()),
                Some(compared),
                None,
            )
        }
    };
    let changed_fields = match (&compared, &compared_source_snapshot) {
        (Some(compared), Some(compared_source)) => changed_fields(
            &selected_source.source_snapshot,
            &selected,
            compared_source,
            compared,
        ),
        (Some(_), None) => {
            return Err(LabError::Internal(
                "compared record has no source snapshot".into(),
            ));
        }
        (None, _) => Vec::new(),
    };
    Ok(DecisionAnalysisResponse::Compare {
        schema_version: DECISION_ANALYSIS_SCHEMA_VERSION.into(),
        selected_source_snapshot: selected_source.source_snapshot,
        compared_source_snapshot,
        comparison: Box::new(DecisionAnalysisComparison {
            kind,
            selected,
            compared,
            unavailable_reason,
            changed_fields,
        }),
        measurement: empty_measurement(),
    })
}

fn previous_context_decision<'a>(
    source: &'a DecisionAnalysisSourceData,
    selected: &DecisionAnalysisDecisionDetail,
) -> Option<&'a DecisionAnalysisDecisionDetail> {
    let selected_time = selected.row.decision_time?;
    source
        .predecessor_candidates
        .iter()
        .filter(|candidate| {
            candidate.row.market == selected.row.market
                && candidate
                    .row
                    .decision_time
                    .is_some_and(|time| time < selected_time)
        })
        .max_by(|left, right| compare_rows(&left.row, &right.row))
}

fn previous_context_snapshot(
    selected: &DecisionAnalysisSourceSnapshot,
    previous: &DecisionAnalysisDecisionDetail,
) -> Result<DecisionAnalysisSourceSnapshot, LabError> {
    let start = previous.row.decision_time.ok_or_else(|| {
        LabError::DataCorrupt("chronological predecessor has no saved decision time".into())
    })?;
    let end = selected.selection.population.period.start;
    if start >= end {
        return Err(LabError::DataCorrupt(
            "outside-period predecessor does not precede the selected period".into(),
        ));
    }
    let mut context = selected.clone();
    context.selection.population.period = crate::contracts::DecisionAnalysisPeriod { start, end };
    Ok(context)
}

fn previous_decision<'a>(
    source: &'a DecisionAnalysisSourceData,
    selected: &DecisionAnalysisDecisionDetail,
) -> Option<&'a DecisionAnalysisDecisionDetail> {
    let mut eligible = source
        .decisions
        .iter()
        .filter(|candidate| {
            candidate.row.period_placement == DecisionAnalysisPeriodPlacement::InPeriod
                && candidate.row.market == selected.row.market
                && compare_rows(&candidate.row, &selected.row).is_lt()
        })
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| compare_rows(&left.row, &right.row));
    eligible.last().copied()
}

fn compare_rows(
    left: &DecisionAnalysisDecisionRow,
    right: &DecisionAnalysisDecisionRow,
) -> std::cmp::Ordering {
    match (left.decision_time, right.decision_time) {
        (Some(left_time), Some(right_time)) => left_time
            .cmp(&right_time)
            .then_with(|| compare_ids(&left.decision_id, &right.decision_id)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => compare_ids(&left.decision_id, &right.decision_id),
    }
}

fn compare_ids(
    left: &DecisionAnalysisDecisionId,
    right: &DecisionAnalysisDecisionId,
) -> std::cmp::Ordering {
    match (left, right) {
        (
            DecisionAnalysisDecisionId::Paper {
                event_sequence: left_sequence,
                signal_id: left_signal,
            },
            DecisionAnalysisDecisionId::Paper {
                event_sequence: right_sequence,
                signal_id: right_signal,
            },
        ) => left_sequence
            .cmp(right_sequence)
            .then_with(|| left_signal.as_str().cmp(right_signal.as_str())),
        (
            DecisionAnalysisDecisionId::Automation {
                intent_id: left_intent,
            },
            DecisionAnalysisDecisionId::Automation {
                intent_id: right_intent,
            },
        ) => left_intent.as_str().cmp(right_intent.as_str()),
        (
            DecisionAnalysisDecisionId::Paper { .. },
            DecisionAnalysisDecisionId::Automation { .. },
        ) => std::cmp::Ordering::Less,
        (
            DecisionAnalysisDecisionId::Automation { .. },
            DecisionAnalysisDecisionId::Paper { .. },
        ) => std::cmp::Ordering::Greater,
    }
}

fn changed_fields(
    selected_source: &DecisionAnalysisSourceSnapshot,
    selected: &DecisionAnalysisDecisionDetail,
    compared_source: &DecisionAnalysisSourceSnapshot,
    compared: &DecisionAnalysisDecisionDetail,
) -> Vec<DecisionAnalysisChangedField> {
    let mut changes = context_changed_fields(selected_source, selected, compared_source, compared);
    append_explanation_changed_fields(&mut changes, &selected.explanation, &compared.explanation);
    changes
}

fn context_changed_fields(
    selected_source: &DecisionAnalysisSourceSnapshot,
    selected: &DecisionAnalysisDecisionDetail,
    compared_source: &DecisionAnalysisSourceSnapshot,
    compared: &DecisionAnalysisDecisionDetail,
) -> Vec<DecisionAnalysisChangedField> {
    let mut changes = Vec::new();
    if source_family(&selected_source.selection.population.source)
        != source_family(&compared_source.selection.population.source)
    {
        changes.push(DecisionAnalysisChangedField::SourceFamily);
    }
    if source_session_id(&selected_source.selection.population.source)
        != source_session_id(&compared_source.selection.population.source)
    {
        changes.push(DecisionAnalysisChangedField::SessionId);
    }
    if selected_source.mode != compared_source.mode {
        changes.push(DecisionAnalysisChangedField::Mode);
    }
    if selected_source.policy_ref != compared_source.policy_ref
        || selected.row.policy_ref != compared.row.policy_ref
    {
        changes.push(DecisionAnalysisChangedField::PolicyRef);
    }
    if selected.row.market != compared.row.market {
        changes.push(DecisionAnalysisChangedField::Market);
    }
    changes
}

fn append_explanation_changed_fields(
    changes: &mut Vec<DecisionAnalysisChangedField>,
    left: &crate::contracts::RecordedDecisionExplanation,
    right: &crate::contracts::RecordedDecisionExplanation,
) {
    push_if_different(
        changes,
        DecisionAnalysisChangedField::DecisionTime,
        &left.decision_time,
        &right.decision_time,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::StrategyAction,
        &left.strategy_action,
        &right.strategy_action,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ConstrainedAction,
        &left.constrained_action,
        &right.constrained_action,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ExecutionState,
        &left.execution_state,
        &right.execution_state,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ActualWeightAtDecision,
        &left.actual_weight_at_decision,
        &right.actual_weight_at_decision,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::RawTargetWeight,
        &left.raw_target_weight,
        &right.raw_target_weight,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ConstrainedTargetWeight,
        &left.constrained_target_weight,
        &right.constrained_target_weight,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::MatchedRuleId,
        &left.matched_rule_id,
        &right.matched_rule_id,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ConditionTraceCapture,
        &left.condition_trace_capture,
        &right.condition_trace_capture,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::ConditionTrace,
        &left.condition_trace,
        &right.condition_trace,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::Reasons,
        &left.reasons,
        &right.reasons,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::Orders,
        &left.orders,
        &right.orders,
    );
    push_if_different(
        changes,
        DecisionAnalysisChangedField::Fills,
        &left.fills,
        &right.fills,
    );
}

fn source_family(source: &crate::contracts::DecisionAnalysisSource) -> &'static str {
    match source {
        crate::contracts::DecisionAnalysisSource::Paper { .. } => "paper",
        crate::contracts::DecisionAnalysisSource::Automation { .. } => "automation",
    }
}

fn source_session_id(source: &crate::contracts::DecisionAnalysisSource) -> &str {
    match source {
        crate::contracts::DecisionAnalysisSource::Paper { session_id } => session_id.as_str(),
        crate::contracts::DecisionAnalysisSource::Automation { session_id } => session_id.as_str(),
    }
}

fn push_if_different<T: PartialEq>(
    fields: &mut Vec<DecisionAnalysisChangedField>,
    field: DecisionAnalysisChangedField,
    left: &T,
    right: &T,
) {
    if left != right {
        fields.push(field);
    }
}

fn count_action(counts: &mut DecisionAnalysisActionCounts, action: RecordedDecisionAction) {
    match action {
        RecordedDecisionAction::Buy => counts.buy += 1,
        RecordedDecisionAction::Sell => counts.sell += 1,
        RecordedDecisionAction::Hold => counts.hold += 1,
        RecordedDecisionAction::NotEvaluated => counts.not_evaluated += 1,
        RecordedDecisionAction::NotCaptured => counts.not_captured += 1,
    }
}

fn count_execution(counts: &mut DecisionAnalysisExecutionCounts, state: RecordedExecutionState) {
    match state {
        RecordedExecutionState::NotEvaluated => counts.not_evaluated += 1,
        RecordedExecutionState::NoOrder => counts.no_order += 1,
        RecordedExecutionState::Rejected => counts.rejected += 1,
        RecordedExecutionState::Pending => counts.pending += 1,
        RecordedExecutionState::Filled => counts.filled += 1,
        RecordedExecutionState::PartiallyFilled => counts.partially_filled += 1,
        RecordedExecutionState::Cancelled => counts.cancelled += 1,
        RecordedExecutionState::Expired => counts.expired += 1,
        RecordedExecutionState::NotCaptured => counts.not_captured += 1,
    }
}

fn in_period(source: &DecisionAnalysisSourceData, at: crate::contracts::UtcTimestamp) -> bool {
    let period = &source.source_snapshot.selection.population.period;
    at >= period.start && at < period.end
}

fn len_u64(length: usize) -> Result<u64, LabError> {
    u64::try_from(length)
        .map_err(|_| LabError::ResourceLimit("decision analysis count overflow".into()))
}

fn len_u32(length: usize) -> Result<u32, LabError> {
    u32::try_from(length)
        .map_err(|_| LabError::ResourceLimit("decision analysis page count overflow".into()))
}

fn empty_measurement() -> DecisionAnalysisMeasurement {
    DecisionAnalysisMeasurement {
        elapsed_millis: 0,
        response_bytes: 0,
    }
}

pub(crate) fn with_measurement(
    mut response: DecisionAnalysisResponse,
    started: &std::time::Instant,
) -> Result<Value, LabError> {
    let mut value = serde_json::to_value(&response)?;
    for _ in 0..6 {
        let size = len_u64(serde_json::to_vec(&value)?.len())?;
        let elapsed_millis = elapsed_millis(started)?;
        set_measurement(&mut response, elapsed_millis, size);
        let next = serde_json::to_value(&response)?;
        if next == value {
            break;
        }
        value = next;
    }
    let bytes = serde_json::to_vec(&value)?.len();
    if bytes > MAX_DECISION_ANALYSIS_OUTPUT_BYTES {
        return Err(LabError::ResourceLimit(
            "decision analysis response exceeds byte budget; narrow the selected period or refresh the overview".into(),
        ));
    }
    let elapsed_millis = elapsed_millis(started)?;
    set_measurement(&mut response, elapsed_millis, len_u64(bytes)?);
    value = serde_json::to_value(&response)?;
    if serde_json::to_vec(&value)?.len() > MAX_DECISION_ANALYSIS_OUTPUT_BYTES {
        return Err(LabError::ResourceLimit(
            "decision analysis response exceeds byte budget; narrow the selected period or refresh the overview".into(),
        ));
    }
    Ok(value)
}

fn elapsed_millis(started: &std::time::Instant) -> Result<u64, LabError> {
    let elapsed = u64::try_from(started.elapsed().as_millis())
        .map_err(|_| LabError::ResourceLimit("decision analysis elapsed time overflow".into()))?;
    if elapsed > MAX_DECISION_ANALYSIS_ELAPSED_MILLIS {
        return Err(LabError::ResourceLimit(
            "complete saved-source decision analysis exceeded its elapsed-time bound; choose a smaller saved session and request a fresh overview".into(),
        ));
    }
    Ok(elapsed)
}

fn set_measurement(
    response: &mut DecisionAnalysisResponse,
    elapsed_millis: u64,
    response_bytes: u64,
) {
    let measurement = match response {
        DecisionAnalysisResponse::Overview { measurement, .. }
        | DecisionAnalysisResponse::Decisions { measurement, .. }
        | DecisionAnalysisResponse::Decision { measurement, .. }
        | DecisionAnalysisResponse::Compare { measurement, .. } => measurement,
    };
    measurement.elapsed_millis = elapsed_millis;
    measurement.response_bytes = response_bytes;
}

#[cfg(test)]
mod tests;
