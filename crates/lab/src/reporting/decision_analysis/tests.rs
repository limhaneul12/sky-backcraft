use super::*;
use crate::contracts::{
    ContentHash, DecisionAnalysisComparisonTarget, DecisionAnalysisDecisionFilter,
    DecisionAnalysisDecisionId, DecisionAnalysisFence, DecisionAnalysisMode,
    DecisionAnalysisObservationClock, DecisionAnalysisPeriod, DecisionAnalysisPopulation,
    DecisionAnalysisSelection, DecisionAnalysisSource, DecisionAnalysisSourceSnapshot,
    PolicyConditionOutcome, PolicyConditionReason, PolicyConditionRole, PolicyConditionScope,
    PolicyConditionTrace, PolicyId, PolicyRevisionId, PolicyRevisionRef,
    RecordedDecisionExplanation, RecordedDecisionReason, RecordedDecisionStage,
    RecordedExecutionState, SavedFactCaptureStatus, SignalId, UtcTimestamp,
};
use crate::storage::decision_analysis::{
    DecisionAnalysisObservation, DecisionAnalysisReadSnapshot, DecisionAnalysisSourceData,
};
use rust_decimal::Decimal;

#[test]
fn stage_and_reason_filters_must_match_one_original_reason_occurrence() -> Result<(), LabError> {
    let row = row(
        2,
        "filter-signal",
        Some(time("2026-10-14T01:00:00Z")?),
        DecisionAnalysisPeriodPlacement::InPeriod,
        vec![
            RecordedDecisionReason {
                stage: RecordedDecisionStage::Strategy,
                code: "STRATEGY_REASON".into(),
                label_ko: "전략 사유".into(),
            },
            RecordedDecisionReason {
                stage: RecordedDecisionStage::Constraints,
                code: "CONSTRAINT_REASON".into(),
                label_ko: "제약 사유".into(),
            },
        ],
    )?;
    let crossed = DecisionAnalysisDecisionFilter {
        stage: Some(RecordedDecisionStage::Strategy),
        reason_code: Some("CONSTRAINT_REASON".into()),
        ..DecisionAnalysisDecisionFilter::default()
    };
    assert!(!filter_matches(&row, Some(&crossed)));
    let exact = DecisionAnalysisDecisionFilter {
        stage: Some(RecordedDecisionStage::Constraints),
        reason_code: Some("CONSTRAINT_REASON".into()),
        ..DecisionAnalysisDecisionFilter::default()
    };
    assert!(filter_matches(&row, Some(&exact)));
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one synthetic projection fixture checks placed, unplaced, capture, and chronology boundaries together"
)]
fn missing_times_capture_states_and_numeric_tie_identity_stay_distinct() -> Result<(), LabError> {
    let start = time("2026-10-14T01:00:00Z")?;
    let end = time("2026-10-14T02:00:00Z")?;
    let paper_session_id = crate::contracts::PaperSessionId::new("projection-test")?;
    let market = crate::contracts::MarketId::parse_upbit("KRW-BTC")?;
    let policy_ref = policy_reference()?;
    let population = DecisionAnalysisPopulation {
        source: DecisionAnalysisSource::Paper {
            session_id: paper_session_id,
        },
        period: DecisionAnalysisPeriod { start, end },
        market: Some(market.clone()),
        policy_ref: Some(policy_ref.clone()),
    };
    let selection = DecisionAnalysisSelection {
        population: population.clone(),
        fence: DecisionAnalysisFence::Paper { as_of_sequence: 11 },
    };
    let source_snapshot = DecisionAnalysisSourceSnapshot {
        selection,
        mode: DecisionAnalysisMode::Simulated,
        policy_ref: policy_ref.clone(),
    };
    let false_trace = condition_trace("rule/entry/0", PolicyConditionOutcome::False, Some(false));
    let unknown_trace = condition_trace(
        "rule/entry/1",
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable),
        Some(false),
    );
    let skipped_trace = condition_trace(
        "rule/entry/2",
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::PriorRuleMatched),
        None,
    );
    let first = detail(
        2,
        "tie-two",
        Some(start),
        DecisionAnalysisPeriodPlacement::InPeriod,
        policy_ref.clone(),
        RecordedDecisionAction::Buy,
        ConditionFixture {
            capture: SavedFactCaptureStatus::Captured,
            traces: vec![false_trace, unknown_trace, skipped_trace],
        },
    )?;
    let tied = detail(
        10,
        "tie-ten",
        Some(start),
        DecisionAnalysisPeriodPlacement::InPeriod,
        policy_ref.clone(),
        RecordedDecisionAction::Hold,
        ConditionFixture {
            capture: SavedFactCaptureStatus::NotCaptured,
            traces: Vec::new(),
        },
    )?;
    let unplaced = detail(
        11,
        "time-unknown",
        None,
        DecisionAnalysisPeriodPlacement::Unplaced,
        policy_ref,
        RecordedDecisionAction::Sell,
        ConditionFixture {
            capture: SavedFactCaptureStatus::NotCaptured,
            traces: Vec::new(),
        },
    )?;
    let data = DecisionAnalysisSourceData {
        source_snapshot: source_snapshot.clone(),
        decisions: vec![tied, unplaced.clone(), first.clone()],
        predecessor_candidates: Vec::new(),
        observations: vec![
            DecisionAnalysisObservation {
                kind: "PAUSE".into(),
                basis_time: DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
                scope: crate::contracts::DecisionAnalysisObservationScope::SessionWide,
                at: None,
            },
            DecisionAnalysisObservation {
                kind: "PROVIDER_RECOVERY".into(),
                basis_time: DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
                scope: crate::contracts::DecisionAnalysisObservationScope::Unknown,
                at: Some(start),
            },
        ],
    };
    let snapshot = DecisionAnalysisReadSnapshot {
        selected: data,
        compared: None,
    };

    let overview_query = DecisionAnalysisQuery::Overview {
        population: population.clone(),
    };
    let overview = analyze(snapshot.clone(), overview_query)?;
    let totals = match overview {
        DecisionAnalysisResponse::Overview {
            totals,
            population_complete,
            ..
        } => {
            assert!(population_complete);
            totals
        }
        _ => {
            return Err(LabError::Internal(
                "projection test expected overview".into(),
            ));
        }
    };
    assert_eq!(totals.decisions, 2);
    assert_eq!(totals.decisions_with_time, 2);
    assert_eq!(totals.unplaced_decisions, 1);
    assert_eq!(totals.strategy.buy, 1);
    assert_eq!(totals.strategy.hold, 1);
    assert_eq!(totals.strategy.sell, 0);
    assert_eq!(totals.condition_trace_capture.captured, 1);
    assert_eq!(totals.condition_trace_capture.not_captured, 1);
    assert_eq!(totals.conditions.len(), 3);
    assert!(
        totals
            .conditions
            .iter()
            .any(|row| row.outcome == PolicyConditionOutcome::False)
    );
    assert!(
        totals
            .conditions
            .iter()
            .any(|row| matches!(row.outcome, PolicyConditionOutcome::Unknown(_)))
    );
    assert!(
        totals
            .conditions
            .iter()
            .any(|row| matches!(row.outcome, PolicyConditionOutcome::NotEvaluated(_)))
    );
    assert_eq!(totals.non_decision_observations.len(), 2);
    let pause = totals
        .non_decision_observations
        .iter()
        .find(|row| row.kind == "PAUSE")
        .ok_or_else(|| LabError::Internal("session-wide control row missing".into()))?;
    assert_eq!(
        pause.scope,
        crate::contracts::DecisionAnalysisObservationScope::SessionWide
    );
    assert_eq!(
        pause.condition_trace_capture,
        SavedFactCaptureStatus::NotDecisionEvent
    );
    assert_eq!(pause.observations, 0);
    assert_eq!(pause.unplaced, 1);
    let unknown = totals
        .non_decision_observations
        .iter()
        .find(|row| row.kind == "PROVIDER_RECOVERY")
        .ok_or_else(|| LabError::Internal("unknown-scope recovery row missing".into()))?;
    assert_eq!(
        unknown.scope,
        crate::contracts::DecisionAnalysisObservationScope::Unknown
    );
    assert_eq!(unknown.observations, 1);

    let page_query = DecisionAnalysisQuery::Decisions {
        selection: source_snapshot.selection.clone(),
        page: crate::contracts::DecisionAnalysisPageRequest {
            offset: 0,
            limit: 10,
        },
        filter: None,
    };
    let DecisionAnalysisResponse::Decisions { page, .. } = analyze(snapshot.clone(), page_query)?
    else {
        return Err(LabError::Internal(
            "projection test expected decisions".into(),
        ));
    };
    assert_eq!(page.total_count, 3);
    assert_eq!(page.in_period_count, 2);
    assert_eq!(page.unplaced_count, 1);
    assert_eq!(page.returned_count, 3);
    assert_eq!(page.items[0].decision_id, id(2, "tie-two")?);
    assert_eq!(page.items[1].decision_id, id(10, "tie-ten")?);
    assert_eq!(
        page.items[2].period_placement,
        DecisionAnalysisPeriodPlacement::Unplaced
    );

    let unplaced_detail_query = DecisionAnalysisQuery::Decision {
        selection: source_snapshot.selection.clone(),
        decision_id: unplaced.row.decision_id.clone(),
    };
    let DecisionAnalysisResponse::Decision {
        decision: unplaced_detail,
        ..
    } = analyze(snapshot.clone(), unplaced_detail_query)?
    else {
        return Err(LabError::Internal(
            "projection test expected unplaced detail".into(),
        ));
    };
    assert_eq!(
        unplaced_detail.row.period_placement,
        DecisionAnalysisPeriodPlacement::Unplaced
    );
    assert!(unplaced_detail.explanation.decision_time.is_none());

    let tie_query = DecisionAnalysisQuery::Compare {
        selection: source_snapshot.selection.clone(),
        decision_id: id(10, "tie-ten")?,
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let DecisionAnalysisResponse::Compare { comparison, .. } =
        analyze(snapshot.clone(), tie_query)?
    else {
        return Err(LabError::Internal(
            "projection test expected comparison".into(),
        ));
    };
    assert_eq!(
        comparison
            .compared
            .as_ref()
            .map(|item| &item.row.decision_id),
        Some(&id(2, "tie-two")?)
    );

    let missing_time_query = DecisionAnalysisQuery::Compare {
        selection: source_snapshot.selection.clone(),
        decision_id: unplaced.row.decision_id.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let started = std::time::Instant::now();
    let missing_time_response = analyze(snapshot.clone(), missing_time_query)?;
    let DecisionAnalysisResponse::Compare {
        compared_source_snapshot,
        comparison: missing_time,
        ..
    } = &missing_time_response
    else {
        return Err(LabError::Internal(
            "projection test expected missing-time compare".into(),
        ));
    };
    assert_eq!(
        missing_time.unavailable_reason,
        Some(DecisionAnalysisComparisonUnavailableReason::MissingDecisionTime)
    );
    assert!(missing_time.compared.is_none());
    assert!(compared_source_snapshot.is_none());
    let missing_time_wire = with_measurement(missing_time_response, &started)?;
    save_captured_wire(
        "paper-captured-compare-missing-time.json",
        &missing_time_wire,
    )?;

    let mut no_previous_snapshot = snapshot;
    no_previous_snapshot.selected.decisions = vec![first.clone()];
    let no_previous_query = DecisionAnalysisQuery::Compare {
        selection: source_snapshot.selection,
        decision_id: first.row.decision_id.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let started = std::time::Instant::now();
    let no_previous_response = analyze(no_previous_snapshot, no_previous_query)?;
    let DecisionAnalysisResponse::Compare {
        compared_source_snapshot,
        comparison: no_previous,
        ..
    } = &no_previous_response
    else {
        return Err(LabError::Internal(
            "projection test expected no-previous compare".into(),
        ));
    };
    assert_eq!(
        no_previous.unavailable_reason,
        Some(DecisionAnalysisComparisonUnavailableReason::NoPreviousDecision)
    );
    assert!(no_previous.compared.is_none());
    assert!(compared_source_snapshot.is_none());
    let no_previous_wire = with_measurement(no_previous_response, &started)?;
    save_captured_wire("paper-captured-compare-no-previous.json", &no_previous_wire)?;
    Ok(())
}

fn row(
    event_sequence: u64,
    signal_id: &str,
    decision_time: Option<UtcTimestamp>,
    period_placement: DecisionAnalysisPeriodPlacement,
    reasons: Vec<RecordedDecisionReason>,
) -> Result<DecisionAnalysisDecisionRow, LabError> {
    Ok(DecisionAnalysisDecisionRow {
        decision_id: id(event_sequence, signal_id)?,
        market: crate::contracts::MarketId::parse_upbit("KRW-BTC")?,
        decision_time,
        period_placement,
        policy_ref: policy_reference()?,
        saved_fact_digest: ContentHash::of_bytes(b"saved-paper-event"),
        strategy_action: RecordedDecisionAction::Buy,
        constrained_action: RecordedDecisionAction::Hold,
        execution_state: RecordedExecutionState::NoOrder,
        reasons,
    })
}

struct ConditionFixture {
    capture: SavedFactCaptureStatus,
    traces: Vec<PolicyConditionTrace>,
}

fn detail(
    event_sequence: u64,
    signal_id: &str,
    decision_time: Option<UtcTimestamp>,
    period_placement: DecisionAnalysisPeriodPlacement,
    policy_ref: PolicyRevisionRef,
    strategy_action: RecordedDecisionAction,
    conditions: ConditionFixture,
) -> Result<DecisionAnalysisDecisionDetail, LabError> {
    let row = DecisionAnalysisDecisionRow {
        decision_id: id(event_sequence, signal_id)?,
        market: crate::contracts::MarketId::parse_upbit("KRW-BTC")?,
        decision_time,
        period_placement,
        policy_ref: policy_ref.clone(),
        saved_fact_digest: ContentHash::of_bytes(format!("event-{event_sequence}").as_bytes()),
        strategy_action,
        constrained_action: RecordedDecisionAction::Hold,
        execution_state: RecordedExecutionState::NoOrder,
        reasons: Vec::new(),
    };
    let explanation = RecordedDecisionExplanation {
        schema_version: "1.0".into(),
        policy_ref: Some(policy_ref),
        decision_time,
        strategy_action,
        constrained_action: RecordedDecisionAction::Hold,
        execution_state: RecordedExecutionState::NoOrder,
        actual_weight_at_decision: None,
        raw_target_weight: None,
        constrained_target_weight: None,
        matched_rule_id: None,
        condition_trace_capture: conditions.capture,
        condition_trace: conditions.traces,
        reasons: Vec::new(),
        orders: Vec::new(),
        fills: Vec::new(),
    };
    Ok(DecisionAnalysisDecisionDetail { row, explanation })
}

fn condition_trace(
    ast_path: &str,
    outcome: PolicyConditionOutcome,
    engine_result: Option<bool>,
) -> PolicyConditionTrace {
    PolicyConditionTrace {
        scope: PolicyConditionScope::RuleCondition {
            rule_id: "entry-rule".into(),
        },
        ast_path: ast_path.into(),
        role: PolicyConditionRole::Entry,
        outcome,
        engine_result,
        evidence: None,
    }
}

fn id(event_sequence: u64, signal_id: &str) -> Result<DecisionAnalysisDecisionId, LabError> {
    Ok(DecisionAnalysisDecisionId::Paper {
        event_sequence,
        signal_id: SignalId::new(signal_id)?,
    })
}

fn policy_reference() -> Result<PolicyRevisionRef, LabError> {
    Ok(PolicyRevisionRef {
        policy_id: PolicyId::new("projection-policy")?,
        revision_id: PolicyRevisionId::new("projection-revision")?,
        definition_digest: ContentHash::of_bytes(b"projection-definition"),
    })
}

fn time(value: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(value)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one captured wire corpus serializes the real enum and evidence variants used by native detail and comparison views"
)]
fn captured_condition_wire_corpus_preserves_enum_evidence_and_decimal_text() -> Result<(), LabError>
{
    let start = time("2026-10-14T01:00:00Z")?;
    let end = time("2026-10-14T02:00:00Z")?;
    let market = crate::contracts::MarketId::parse_upbit("KRW-BTC")?;
    let policy_ref = policy_reference()?;
    let make_snapshot = |session_id: &str| -> Result<DecisionAnalysisSourceSnapshot, LabError> {
        Ok(DecisionAnalysisSourceSnapshot {
            selection: DecisionAnalysisSelection {
                population: DecisionAnalysisPopulation {
                    source: DecisionAnalysisSource::Paper {
                        session_id: crate::contracts::PaperSessionId::new(session_id)?,
                    },
                    period: DecisionAnalysisPeriod { start, end },
                    market: Some(market.clone()),
                    policy_ref: Some(policy_ref.clone()),
                },
                fence: DecisionAnalysisFence::Paper { as_of_sequence: 2 },
            },
            mode: DecisionAnalysisMode::Simulated,
            policy_ref: policy_ref.clone(),
        })
    };
    let left_source = make_snapshot("captured-left")?;
    let right_source = make_snapshot("captured-right")?;

    let mut true_trace = condition_trace(
        "rule:entry:comparison",
        PolicyConditionOutcome::True,
        Some(true),
    );
    true_trace.evidence = Some(crate::contracts::PolicyConditionEvidence::Comparison {
        left: crate::contracts::FinitePolicyNumber::new(0.25)?,
        comparison: crate::contracts::CompareOp::Gt,
        right: crate::contracts::FinitePolicyNumber::new(0.2)?,
    });
    let mut false_trace = condition_trace(
        "rule:entry:weight",
        PolicyConditionOutcome::False,
        Some(false),
    );
    false_trace.evidence = Some(
        crate::contracts::PolicyConditionEvidence::WeightComparison {
            left: crate::contracts::Weight::new(Decimal::new(1, 1))?,
            comparison: crate::contracts::CompareOp::Gt,
            right: crate::contracts::Weight::new(Decimal::new(3, 1))?,
        },
    );
    let mut unknown_trace = condition_trace(
        "rule:entry:crossing",
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable),
        Some(false),
    );
    unknown_trace.evidence = Some(crate::contracts::PolicyConditionEvidence::Crossing {
        previous: None,
        current: crate::contracts::PolicyOperandPair {
            left: crate::contracts::FinitePolicyNumber::new(0.75)?,
            right: crate::contracts::FinitePolicyNumber::new(0.5)?,
        },
    });
    let not_evaluated_trace = condition_trace(
        "rule:entry:skipped",
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::PriorRuleMatched),
        None,
    );
    let traces = vec![true_trace, false_trace, unknown_trace, not_evaluated_trace];
    let left_id = id(2, "captured-left-signal")?;
    let right_id = id(2, "captured-right-signal")?;
    let left = detail(
        2,
        "captured-left-signal",
        Some(start),
        DecisionAnalysisPeriodPlacement::InPeriod,
        policy_ref.clone(),
        RecordedDecisionAction::Buy,
        ConditionFixture {
            capture: SavedFactCaptureStatus::Captured,
            traces: traces.clone(),
        },
    )?;
    let right = detail(
        2,
        "captured-right-signal",
        Some(start),
        DecisionAnalysisPeriodPlacement::InPeriod,
        policy_ref,
        RecordedDecisionAction::Hold,
        ConditionFixture {
            capture: SavedFactCaptureStatus::Captured,
            traces: traces[..3].to_vec(),
        },
    )?;
    let snapshot = DecisionAnalysisReadSnapshot {
        selected: DecisionAnalysisSourceData {
            source_snapshot: left_source.clone(),
            decisions: vec![left],
            predecessor_candidates: Vec::new(),
            observations: Vec::new(),
        },
        compared: Some(DecisionAnalysisSourceData {
            source_snapshot: right_source.clone(),
            decisions: vec![right],
            predecessor_candidates: Vec::new(),
            observations: Vec::new(),
        }),
    };
    let overview_query = DecisionAnalysisQuery::Overview {
        population: left_source.selection.population.clone(),
    };
    let started = std::time::Instant::now();
    let overview_wire = with_measurement(analyze(snapshot.clone(), overview_query)?, &started)?;
    let overview_conditions = overview_wire["totals"]["conditions"]
        .as_array()
        .ok_or_else(|| {
            LabError::Internal("captured overview conditions are not an array".into())
        })?;
    assert!(overview_conditions.iter().any(|condition| {
        condition["outcome"]["status"] == "UNKNOWN"
            && condition["outcome"]["reason"] == "CROSS_HISTORY_UNAVAILABLE"
    }));
    let decisions_query = DecisionAnalysisQuery::Decisions {
        selection: left_source.selection.clone(),
        page: crate::contracts::DecisionAnalysisPageRequest {
            offset: 0,
            limit: 10,
        },
        filter: None,
    };
    let started = std::time::Instant::now();
    let decisions_wire = with_measurement(analyze(snapshot.clone(), decisions_query)?, &started)?;
    assert_eq!(decisions_wire["page"]["returned_count"], 1);
    let detail_query = DecisionAnalysisQuery::Decision {
        selection: left_source.selection.clone(),
        decision_id: left_id.clone(),
    };
    let started = std::time::Instant::now();
    let detail_wire = with_measurement(analyze(snapshot.clone(), detail_query)?, &started)?;
    assert_eq!(detail_wire["schema_version"], "1.0");
    let condition_rows = detail_wire["decision"]["explanation"]["condition_trace"]
        .as_array()
        .ok_or_else(|| LabError::Internal("captured condition trace is not an array".into()))?;
    assert_eq!(condition_rows.len(), 4);
    assert_eq!(condition_rows[0]["outcome"]["status"], "TRUE");
    assert_eq!(condition_rows[0]["evidence"]["left"], 0.25);
    assert_eq!(condition_rows[1]["outcome"]["status"], "FALSE");
    assert_eq!(condition_rows[1]["evidence"]["left"], "0.1");
    assert_eq!(condition_rows[1]["evidence"]["right"], "0.3");
    assert_eq!(condition_rows[2]["outcome"]["status"], "UNKNOWN");
    assert_eq!(
        condition_rows[2]["outcome"]["reason"],
        "CROSS_HISTORY_UNAVAILABLE"
    );
    assert_eq!(condition_rows[2]["evidence"]["current"]["left"], 0.75);
    assert_eq!(condition_rows[3]["outcome"]["status"], "NOT_EVALUATED");
    assert_eq!(condition_rows[3]["outcome"]["reason"], "PRIOR_RULE_MATCHED");
    assert!(condition_rows[3].get("evidence").is_none());

    let compare_query = DecisionAnalysisQuery::Compare {
        selection: left_source.selection,
        decision_id: left_id,
        against: DecisionAnalysisComparisonTarget::Selected {
            selection: Box::new(right_source.selection),
            decision_id: right_id,
        },
    };
    let started = std::time::Instant::now();
    let compare_wire = with_measurement(analyze(snapshot, compare_query)?, &started)?;
    assert_eq!(compare_wire["comparison"]["kind"], "selected");
    assert!(
        compare_wire["comparison"]["changed_fields"]
            .as_array()
            .is_some_and(|fields| fields.iter().any(|field| field == "condition_trace"))
    );
    save_captured_wire("paper-captured-overview.json", &overview_wire)?;
    save_captured_wire("paper-captured-decisions.json", &decisions_wire)?;
    save_captured_wire("paper-captured-decision.json", &detail_wire)?;
    save_captured_wire("paper-captured-compare.json", &compare_wire)?;
    Ok(())
}

fn save_captured_wire(name: &str, value: &serde_json::Value) -> Result<(), LabError> {
    let Some(directory) = std::env::var_os("SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIR") else {
        return Ok(());
    };
    let directory = std::path::PathBuf::from(directory);
    std::fs::create_dir_all(&directory)
        .map_err(|error| LabError::Internal(format!("create captured wire directory: {error}")))?;
    std::fs::write(directory.join(name), serde_json::to_vec_pretty(value)?)
        .map_err(|error| LabError::Internal(format!("write captured wire: {error}")))
}
