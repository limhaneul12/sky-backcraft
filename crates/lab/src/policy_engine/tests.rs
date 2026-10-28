use super::*;
use crate::contracts::policy::{
    PolicyConditionOutcome, PolicyConditionReason, PolicyConditionRole,
    PolicyConditionRoleDeclaration, PolicyConditionScope, PolicyIndicator, PolicyRule,
    PolicySignalExpiry, PolicySourceCaptureOrigin, PolicyState,
};
use crate::contracts::{
    AssetQuantity, CandleRecord, ContentHash, FrozenPolicyRevision, MarketId, ObservationId,
    PolicyDefinition, PolicyId, PolicyOrigin, PolicyProgram, PolicyRevisionId, PolicyRevisionRef,
    PriceKrw, ProviderAbsenceAnchor, ProviderAbsenceSnapshotRef, ProviderAbsenceSpan, QuoteAmount,
    RawObjectId, StrategyKind, VerifiedProviderCoverage, observation_digest,
};
use chrono::{Duration, TimeZone, Utc};

fn constant(value: f64) -> NumericExpr {
    NumericExpr::Constant { value }
}

fn definition(program: RulesProgram) -> PolicyDefinition {
    PolicyDefinition {
        schema_version: "1.0".into(),
        name: "fixture".into(),
        description: "SYNTHETIC_TEST_ONLY".into(),
        program: PolicyProgram::Rules { program },
    }
}

#[test]
fn legacy_policy_trace_and_empty_role_metadata_roundtrip_without_new_fields() {
    let old_trace = serde_json::json!({
        "matched_rule_id": null,
        "state_before": [],
        "state_after": [],
        "evidence": null
    });
    let decoded_trace: crate::contracts::PolicyTrace =
        serde_json::from_value(old_trace.clone()).expect("legacy policy trace JSON");
    assert_eq!(
        serde_json::to_value(decoded_trace).expect("serialize legacy trace"),
        old_trace
    );

    let old_definition = definition(RulesProgram {
        indicators: Vec::new(),
        states: Vec::new(),
        rules: Vec::new(),
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let old_definition_json =
        serde_json::to_value(&old_definition).expect("serialize legacy policy definition");
    let decoded_definition: PolicyDefinition =
        serde_json::from_value(old_definition_json.clone()).expect("legacy policy definition JSON");
    assert_eq!(
        serde_json::to_value(decoded_definition).expect("serialize roundtripped definition"),
        old_definition_json
    );
    assert!(
        old_definition_json["program"]["program"]
            .get("condition_roles")
            .is_none()
    );
}

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("market")
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

fn observation(index: i64, close: i64) -> CandleObservation {
    let open = UtcTimestamp(
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
            .single()
            .expect("time")
            + Duration::hours(index),
    );
    let price = PriceKrw::new(Decimal::from(close)).expect("price");
    CandleObservation {
        id: ObservationId::new(format!("policy-obs-{index}")).expect("id"),
        candle: CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: open,
            close_time_utc: UtcTimestamp(open.0 + Duration::hours(1)),
            open: price,
            high: PriceKrw::new(Decimal::from(close + 1)).expect("high"),
            low: PriceKrw::new(Decimal::from(close - 1)).expect("low"),
            close: price,
            volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
            quote_turnover: QuoteAmount::new(Decimal::from(close)).expect("turnover"),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(format!("policy-{index}").as_bytes()),
        raw_object_ids: vec![RawObjectId::new(format!("policy-raw-{index}")).expect("raw")],
        constituent_ids: Vec::new(),
    }
}

fn canonical_observation(index: i64, close: i64) -> CandleObservation {
    let mut result = observation(index, close);
    result.content_digest = observation_digest(&result.candle).expect("observation digest");
    result.id = ObservationId::from_seed(result.content_digest.as_str());
    result
}

fn verified_span(
    market: MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
    proof_seed: &str,
) -> ProviderAbsenceSpan {
    let pre_open = UtcTimestamp(start.0 - interval.duration());
    let fetched_at = time("2024-01-02T00:00:01Z");
    ProviderAbsenceSpan {
        proof_id: ContentHash::of_bytes(proof_seed.as_bytes()),
        market,
        interval,
        start_utc: start,
        end_utc: end,
        pre_anchor: ProviderAbsenceAnchor {
            observation_id: ObservationId::from_seed(&format!("{proof_seed}-pre")),
            open_time_utc: pre_open,
            content_digest: ContentHash::of_bytes(format!("{proof_seed}-pre").as_bytes()),
        },
        post_anchor: ProviderAbsenceAnchor {
            observation_id: ObservationId::from_seed(&format!("{proof_seed}-post")),
            open_time_utc: end,
            content_digest: ContentHash::of_bytes(format!("{proof_seed}-post").as_bytes()),
        },
        snapshots: [
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed(&format!("{proof_seed}-raw-1")),
                fetched_at_utc: fetched_at,
                body_digest: ContentHash::of_bytes(b"verified source page"),
            },
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed(&format!("{proof_seed}-raw-2")),
                fetched_at_utc: UtcTimestamp(fetched_at.0 + Duration::seconds(1)),
                // Identical body hashes are valid independent reads.
                body_digest: ContentHash::of_bytes(b"verified source page"),
            },
        ],
    }
}

fn frozen_revision(definition: PolicyDefinition, seed: &str) -> FrozenPolicyRevision {
    let digest = ContentHash::of_value(&definition).expect("definition digest");
    FrozenPolicyRevision {
        reference: PolicyRevisionRef {
            policy_id: PolicyId::new(format!("policy-{seed}")).expect("policy id"),
            revision_id: PolicyRevisionId::new(format!("revision-{seed}")).expect("revision id"),
            definition_digest: digest,
        },
        revision_number: 1,
        parent_revision_id: None,
        family: StrategyKind::Other,
        origin: PolicyOrigin::User,
        definition,
    }
}

fn verified_coverage(
    gap_market: MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
    proof_seed: &str,
) -> VerifiedProviderCoverage {
    VerifiedProviderCoverage::from_verified_spans(vec![verified_span(
        gap_market, interval, start, end, proof_seed,
    )])
    .expect("verified fixture coverage")
}

fn revision_for(program: RulesProgram, seed: &str) -> FrozenPolicyRevision {
    frozen_revision(definition(program), seed)
}

fn cash() -> PositionView {
    PositionView {
        actual_qty: AssetQuantity::new(Decimal::ZERO).expect("qty"),
        actual_weight: Weight::new(Decimal::ZERO).expect("weight"),
        episode_opened_at: None,
        held_decision_bars: None,
    }
}

fn long(at: UtcTimestamp) -> PositionView {
    PositionView {
        actual_qty: AssetQuantity::new(Decimal::ONE).expect("qty"),
        actual_weight: Weight::new(Decimal::ONE).expect("weight"),
        episode_opened_at: Some(at),
        held_decision_bars: Some(1),
    }
}

fn close_indicator() -> PolicyIndicator {
    PolicyIndicator {
        id: "close".into(),
        indicator: PolicyIndicatorKind::Close,
        source_interval: None,
    }
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "exact integer state fixtures prove simultaneous update semantics"
)]
fn state_updates_read_one_prior_snapshot_and_commit_simultaneously() {
    let program = RulesProgram {
        indicators: vec![close_indicator()],
        states: vec![
            PolicyState {
                id: "a".into(),
                initial: 1.0,
                next: NumericExpr::Add {
                    left: Box::new(NumericExpr::State { id: "b".into() }),
                    right: Box::new(constant(1.0)),
                },
            },
            PolicyState {
                id: "b".into(),
                initial: 2.0,
                next: NumericExpr::Add {
                    left: Box::new(NumericExpr::State { id: "a".into() }),
                    right: Box::new(constant(10.0)),
                },
            },
        ],
        rules: Vec::new(),
        fallback: PolicyTarget::Weight {
            value: NumericExpr::State { id: "a".into() },
        },
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    };
    let mut evaluator =
        RulesEvaluator::new(&definition(program), market(), CandleInterval::H1).expect("compile");
    let result = evaluator
        .observe(&observation(0, 100), cash(), None)
        .expect("evaluate")
        .expect("ready");
    assert_eq!(result.target.get(), Decimal::ONE);
    assert_eq!(result.trace.state_before[0].value, 1.0);
    assert_eq!(result.trace.state_before[1].value, 2.0);
    assert_eq!(result.trace.state_after[0].value, 3.0);
    assert_eq!(result.trace.state_after[1].value, 11.0);
}

#[test]
fn cross_history_advances_behind_an_earlier_matching_rule() {
    let program = RulesProgram {
        indicators: vec![close_indicator()],
        states: Vec::new(),
        rules: vec![
            PolicyRule {
                id: "cash_first".into(),
                condition: BoolExpr::PositionIs {
                    state: PositionState::Cash,
                },
                target: PolicyTarget::Weight {
                    value: constant(0.0),
                },
            },
            PolicyRule {
                id: "cross".into(),
                condition: BoolExpr::CrossesBelow {
                    left: NumericExpr::Indicator { id: "close".into() },
                    right: constant(100.0),
                },
                target: PolicyTarget::Weight {
                    value: constant(1.0),
                },
            },
        ],
        fallback: PolicyTarget::Weight {
            value: constant(0.0),
        },
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    };
    let mut evaluator =
        RulesEvaluator::new(&definition(program), market(), CandleInterval::H1).expect("compile");
    assert!(
        evaluator
            .observe(&observation(0, 90), cash(), None)
            .expect("first")
            .is_none()
    );
    let second = evaluator
        .observe(&observation(1, 110), cash(), None)
        .expect("second")
        .expect("decision");
    assert_eq!(second.trace.matched_rule_id.as_deref(), Some("cash_first"));
    let skipped_cross = second
        .trace
        .condition_trace
        .as_ref()
        .expect("new rule condition trace")
        .iter()
        .find(|condition| condition.ast_path == "rule:cross:when")
        .expect("lower-priority cross condition");
    assert_eq!(
        skipped_cross.outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::PriorRuleMatched)
    );
    assert_eq!(skipped_cross.engine_result, None);
    let third_bar = observation(2, 90);
    let result = evaluator
        .observe(&third_bar, long(third_bar.candle.open_time_utc), None)
        .expect("third")
        .expect("ready");
    assert_eq!(result.trace.matched_rule_id.as_deref(), Some("cross"));
    assert_eq!(result.target.get(), Decimal::ONE);
    let evaluated_cross = result
        .trace
        .condition_trace
        .as_ref()
        .expect("new rule condition trace")
        .iter()
        .find(|condition| condition.ast_path == "rule:cross:when")
        .expect("crossing predicate");
    assert_eq!(evaluated_cross.outcome, PolicyConditionOutcome::True);
    assert_eq!(evaluated_cross.engine_result, Some(true));
}

fn nested_trace_policy() -> (PolicyDefinition, String) {
    let rule_id = "gate";
    let root = format!("rule:{rule_id}:when");
    let policy = definition(RulesProgram {
        indicators: vec![close_indicator()],
        states: vec![PolicyState {
            id: "memory".into(),
            initial: 0.25,
            next: NumericExpr::Add {
                left: Box::new(NumericExpr::State {
                    id: "memory".into(),
                }),
                right: Box::new(constant(0.25)),
            },
        }],
        rules: vec![PolicyRule {
            id: rule_id.into(),
            condition: BoolExpr::And {
                conditions: vec![
                    BoolExpr::Compare {
                        comparison: CompareOp::Gt,
                        left: NumericExpr::Indicator { id: "close".into() },
                        right: constant(90.0),
                    },
                    BoolExpr::Or {
                        conditions: vec![
                            BoolExpr::Compare {
                                comparison: CompareOp::Lt,
                                left: NumericExpr::Indicator { id: "close".into() },
                                right: constant(90.0),
                            },
                            BoolExpr::Compare {
                                comparison: CompareOp::Gt,
                                left: NumericExpr::Indicator { id: "close".into() },
                                right: constant(95.0),
                            },
                            BoolExpr::CrossesAbove {
                                left: NumericExpr::Indicator { id: "close".into() },
                                right: constant(95.0),
                            },
                        ],
                    },
                    BoolExpr::Compare {
                        comparison: CompareOp::Gt,
                        left: NumericExpr::Indicator { id: "close".into() },
                        right: constant(1_000.0),
                    },
                    BoolExpr::PositionIs {
                        state: PositionState::Long,
                    },
                ],
            },
            target: PolicyTarget::Weight {
                value: constant(1.0),
            },
        }],
        fallback: PolicyTarget::Weight {
            value: NumericExpr::State {
                id: "memory".into(),
            },
        },
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: vec![
            PolicyConditionRoleDeclaration {
                ast_path: format!("{root}:and:0"),
                role: PolicyConditionRole::Entry,
            },
            PolicyConditionRoleDeclaration {
                ast_path: format!("{root}:and:1"),
                role: PolicyConditionRole::Filter,
            },
            PolicyConditionRoleDeclaration {
                ast_path: format!("{root}:and:2"),
                role: PolicyConditionRole::Filter,
            },
            PolicyConditionRoleDeclaration {
                ast_path: format!("{root}:and:3"),
                role: PolicyConditionRole::Filter,
            },
        ],
    });
    (policy, root)
}

#[expect(
    clippy::float_cmp,
    reason = "binary-fraction state values lock the evaluator's exact warmup update order"
)]
#[test]
fn nested_condition_trace_preserves_short_circuit_roles_and_state_order() {
    let (policy, root) = nested_trace_policy();
    let mut evaluator =
        RulesEvaluator::new(&policy, market(), CandleInterval::H1).expect("valid role paths");
    assert!(
        evaluator
            .observe(&canonical_observation(0, 100), cash(), None)
            .expect("first cross history bar")
            .is_none()
    );
    let result = evaluator
        .observe(&canonical_observation(1, 100), cash(), None)
        .expect("decision")
        .expect("cross warmup completed");

    assert_eq!(result.trace.matched_rule_id, None);
    // The first cross-warmup bar advances state before decision emission.
    assert_eq!(result.target.get(), Decimal::new(5, 1));
    assert_eq!(result.trace.state_before[0].value, 0.5);
    assert_eq!(result.trace.state_after[0].value, 0.75);

    let conditions = result
        .trace
        .condition_trace
        .as_ref()
        .expect("condition trace");
    let find = |ast_path: &str| {
        conditions
            .iter()
            .find(|condition| condition.ast_path == ast_path)
            .unwrap_or_else(|| panic!("missing condition trace at {ast_path}"))
    };
    assert_eq!(find(&root).outcome, PolicyConditionOutcome::False);
    assert_eq!(find(&root).engine_result, Some(false));
    assert_eq!(
        find(&root).scope,
        PolicyConditionScope::RuleCondition {
            rule_id: "gate".into(),
        }
    );
    assert_eq!(
        find(&format!("{root}:and:0")).role,
        PolicyConditionRole::Entry
    );
    assert_eq!(
        find(&format!("{root}:and:0")).outcome,
        PolicyConditionOutcome::True
    );
    assert_eq!(
        find(&format!("{root}:and:1")).role,
        PolicyConditionRole::Filter
    );
    assert_eq!(
        find(&format!("{root}:and:1:or:0")).outcome,
        PolicyConditionOutcome::False
    );
    assert_eq!(
        find(&format!("{root}:and:1:or:1")).outcome,
        PolicyConditionOutcome::True
    );
    assert_eq!(
        find(&format!("{root}:and:1:or:2")).outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::OrShortCircuit)
    );
    assert_eq!(find(&format!("{root}:and:1:or:2")).engine_result, None);
    assert_eq!(
        find(&format!("{root}:and:2")).outcome,
        PolicyConditionOutcome::False
    );
    assert_eq!(
        find(&format!("{root}:and:2")).role,
        PolicyConditionRole::Filter
    );
    assert_eq!(
        find(&format!("{root}:and:3")).role,
        PolicyConditionRole::Filter
    );
    assert_eq!(
        find(&format!("{root}:and:3")).outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::AndShortCircuit)
    );

    let mut invalid_role = policy;
    let PolicyProgram::Rules { program } = &mut invalid_role.program else {
        unreachable!("test policy uses RULES");
    };
    program
        .condition_roles
        .push(PolicyConditionRoleDeclaration {
            ast_path: "rule:missing:when".into(),
            role: PolicyConditionRole::Classification,
        });
    assert!(matches!(
        invalid_role.validate(),
        Err(LabError::InvalidConfig(_))
    ));
}

fn evaluate_condition_trace(
    condition: &BoolExpr,
    crosses: &BTreeMap<String, CrossSample>,
) -> Result<(bool, Vec<crate::contracts::PolicyConditionTrace>), LabError> {
    let indicators = BTreeMap::<String, f64>::new();
    let state = BTreeMap::<String, f64>::new();
    let context = EvalContext {
        indicators: &indicators,
        state: &state,
        actual_state: PositionState::Cash,
        actual_weight: 0.0,
        held_bars: 0.0,
        unrealized_return_bps: 0.0,
        trailing_drawdown_bps: 0.0,
        evidence_multiplier: 0.0,
        evidence_coverage: 0.0,
        crosses,
    };
    let mut collector = ConditionTraceCollector::new(
        PolicyConditionScope::RuleCondition {
            rule_id: "trace-test".into(),
        },
        &[],
    );
    let result = eval_bool(
        condition,
        &context,
        "rule:trace-test:when",
        Some(&mut collector),
    )?;
    Ok((result, collector.entries))
}

fn condition_trace_at<'a>(
    entries: &'a [crate::contracts::PolicyConditionTrace],
    path: &str,
) -> Result<&'a crate::contracts::PolicyConditionTrace, LabError> {
    entries
        .iter()
        .find(|entry| entry.ast_path == path)
        .ok_or_else(|| LabError::Internal(format!("condition trace missing at {path}")))
}

#[test]
fn condition_trace_captures_operands_unknown_crossing_and_legacy_roundtrip() -> Result<(), LabError>
{
    let root = "rule:trace-test:when";
    let condition = BoolExpr::And {
        conditions: vec![
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 90.0 },
            },
            BoolExpr::CrossesAbove {
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 90.0 },
            },
            BoolExpr::Compare {
                comparison: CompareOp::Lt,
                left: NumericExpr::Constant { value: 1.0 },
                right: NumericExpr::Constant { value: 2.0 },
            },
        ],
    };
    let crosses = BTreeMap::from([(
        format!("{root}:and:1"),
        CrossSample {
            previous: None,
            current: (100.0, 90.0),
        },
    )]);
    let (engine_result, trace) = evaluate_condition_trace(&condition, &crosses)?;
    assert!(!engine_result);

    let comparison_path = format!("{root}:and:0");
    let comparison_json = serde_json::to_value(condition_trace_at(&trace, &comparison_path)?)?;
    assert_eq!(
        comparison_json.get("evidence"),
        Some(&serde_json::json!({
            "kind": "COMPARISON",
            "left": 100.0,
            "comparison": "GT",
            "right": 90.0
        }))
    );

    let crossing_path = format!("{root}:and:1");
    let crossing = condition_trace_at(&trace, &crossing_path)?;
    assert_eq!(
        crossing.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );
    assert_eq!(crossing.engine_result, Some(false));
    let crossing_json = serde_json::to_value(crossing)?;
    assert_eq!(
        crossing_json.get("evidence"),
        Some(&serde_json::json!({
            "kind": "CROSSING",
            "previous": null,
            "current": {"left": 100.0, "right": 90.0}
        }))
    );

    let skipped_path = format!("{root}:and:2");
    let skipped = condition_trace_at(&trace, &skipped_path)?;
    assert_eq!(
        skipped.outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::AndShortCircuit)
    );
    let mut legacy_json = serde_json::to_value(skipped)?;
    assert!(legacy_json.get("evidence").is_none());
    legacy_json
        .as_object_mut()
        .ok_or_else(|| LabError::Internal("trace must serialize as an object".into()))?
        .remove("evidence");
    let legacy: crate::contracts::PolicyConditionTrace =
        serde_json::from_value(legacy_json.clone())?;
    assert_eq!(serde_json::to_value(legacy)?, legacy_json);

    let position = BoolExpr::PositionIs {
        state: PositionState::Cash,
    };
    let (_, position_trace) = evaluate_condition_trace(&position, &BTreeMap::new())?;
    let position_json =
        serde_json::to_value(condition_trace_at(&position_trace, "rule:trace-test:when")?)?;
    assert_eq!(
        position_json.get("evidence"),
        Some(&serde_json::json!({
            "kind": "POSITION",
            "expected": "CASH",
            "actual": "CASH"
        }))
    );
    Ok(())
}

#[test]
fn nested_unknown_propagates_but_known_and_or_results_dominate() -> Result<(), LabError> {
    let crosses = |path: &str| {
        BTreeMap::from([(
            path.to_owned(),
            CrossSample {
                previous: None,
                current: (100.0, 90.0),
            },
        )])
    };
    let root = "rule:trace-test:when";

    let not = BoolExpr::Not {
        condition: Box::new(BoolExpr::CrossesAbove {
            left: NumericExpr::Constant { value: 100.0 },
            right: NumericExpr::Constant { value: 90.0 },
        }),
    };
    let (not_engine, not_trace) =
        evaluate_condition_trace(&not, &crosses("rule:trace-test:when:not"))?;
    assert!(not_engine);
    assert_eq!(
        condition_trace_at(&not_trace, root)?.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );
    assert_eq!(
        condition_trace_at(&not_trace, root)?.engine_result,
        Some(true)
    );

    let and = BoolExpr::And {
        conditions: vec![
            not.clone(),
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 1_000.0 },
            },
        ],
    };
    let (and_engine, and_trace) =
        evaluate_condition_trace(&and, &crosses("rule:trace-test:when:and:0:not"))?;
    assert!(!and_engine);
    assert_eq!(
        condition_trace_at(&and_trace, root)?.outcome,
        PolicyConditionOutcome::False
    );
    assert_eq!(
        condition_trace_at(&and_trace, root)?.engine_result,
        Some(false)
    );
    assert_eq!(
        condition_trace_at(&and_trace, "rule:trace-test:when:and:0")?.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );

    let or = BoolExpr::Or {
        conditions: vec![
            BoolExpr::CrossesAbove {
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 90.0 },
            },
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 90.0 },
            },
        ],
    };
    let (or_engine, or_trace) =
        evaluate_condition_trace(&or, &crosses("rule:trace-test:when:or:0"))?;
    assert!(or_engine);
    assert_eq!(
        condition_trace_at(&or_trace, root)?.outcome,
        PolicyConditionOutcome::True
    );
    assert_eq!(
        condition_trace_at(&or_trace, root)?.engine_result,
        Some(true)
    );

    let numeric_if = BoolExpr::Compare {
        comparison: CompareOp::Gt,
        left: NumericExpr::If {
            condition: Box::new(BoolExpr::CrossesAbove {
                left: NumericExpr::Constant { value: 100.0 },
                right: NumericExpr::Constant { value: 90.0 },
            }),
            then_value: Box::new(NumericExpr::Constant { value: 50.0 }),
            else_value: Box::new(NumericExpr::Constant { value: 120.0 }),
        },
        right: NumericExpr::Constant { value: 90.0 },
    };
    let (if_engine, if_trace) =
        evaluate_condition_trace(&numeric_if, &crosses("rule:trace-test:when:left:if"))?;
    assert!(if_engine);
    assert_eq!(
        condition_trace_at(&if_trace, root)?.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );
    assert_eq!(
        condition_trace_at(&if_trace, root)?.engine_result,
        Some(true)
    );
    Ok(())
}

fn scoped_guard_policy() -> PolicyDefinition {
    let numeric_guard = |then_value, else_value| NumericExpr::If {
        condition: Box::new(BoolExpr::CrossesAbove {
            left: NumericExpr::Indicator { id: "close".into() },
            right: constant(90.0),
        }),
        then_value: Box::new(constant(then_value)),
        else_value: Box::new(constant(else_value)),
    };
    definition(RulesProgram {
        indicators: vec![close_indicator()],
        states: vec![PolicyState {
            id: "memory".into(),
            initial: 0.0,
            next: numeric_guard(1.0, 0.25),
        }],
        rules: vec![PolicyRule {
            id: "entry".into(),
            condition: BoolExpr::PositionIs {
                state: PositionState::Cash,
            },
            target: PolicyTarget::Weight {
                value: numeric_guard(1.0, 0.5),
            },
        }],
        fallback: PolicyTarget::Weight {
            value: numeric_guard(1.0, 0.75),
        },
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    })
}

fn scoped_guard_crosses() -> BTreeMap<String, CrossSample> {
    [
        "state:memory:next:if",
        "rule:entry:target:if",
        "fallback:if",
    ]
    .into_iter()
    .map(|path| {
        (
            path.to_owned(),
            CrossSample {
                previous: None,
                current: (100.0, 90.0),
            },
        )
    })
    .collect()
}

#[test]
fn state_and_selected_target_guards_keep_scope_and_unknown_result() -> Result<(), LabError> {
    let policy = scoped_guard_policy();
    let crosses = scoped_guard_crosses();
    let indicators = BTreeMap::from([("close".to_owned(), 100.0)]);
    let state = BTreeMap::from([("memory".to_owned(), 0.0)]);
    let context = EvalContext {
        indicators: &indicators,
        state: &state,
        actual_state: PositionState::Cash,
        actual_weight: 0.0,
        held_bars: 0.0,
        unrealized_return_bps: 0.0,
        trailing_drawdown_bps: 0.0,
        evidence_multiplier: 0.0,
        evidence_coverage: 0.0,
        crosses: &crosses,
    };
    let evaluator = RulesEvaluator::new(&policy, market(), CandleInterval::H1)?;
    let (_, state_trace) = evaluator.evaluate_next_state(&context)?;
    assert_eq!(state_trace.len(), 1);
    assert_eq!(
        state_trace[0].scope,
        PolicyConditionScope::StateTransition {
            state_id: "memory".into(),
        }
    );
    assert_eq!(state_trace[0].role, PolicyConditionRole::Unspecified);
    assert_eq!(
        state_trace[0].outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );
    assert_eq!(state_trace[0].engine_result, Some(false));

    let (matched, target, target_trace) = evaluator.evaluate_rules_with_trace(&context)?;
    assert_eq!(matched.as_deref(), Some("entry"));
    assert_eq!(target.to_bits(), 0.5_f64.to_bits());
    let target_guard = condition_trace_at(&target_trace, "rule:entry:target:if")?;
    assert_eq!(
        target_guard.scope,
        PolicyConditionScope::RuleTarget {
            rule_id: "entry".into(),
        }
    );
    assert_eq!(target_guard.role, PolicyConditionRole::Unspecified);
    assert_eq!(
        target_guard.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable,)
    );
    assert_eq!(target_guard.engine_result, Some(false));
    Ok(())
}

#[test]
fn skipped_target_and_fallback_guards_are_not_evaluated() -> Result<(), LabError> {
    let policy = scoped_guard_policy();
    let crosses = scoped_guard_crosses();
    let indicators = BTreeMap::from([("close".to_owned(), 100.0)]);
    let state = BTreeMap::from([("memory".to_owned(), 0.0)]);
    let context = EvalContext {
        indicators: &indicators,
        state: &state,
        actual_state: PositionState::Cash,
        actual_weight: 0.0,
        held_bars: 0.0,
        unrealized_return_bps: 0.0,
        trailing_drawdown_bps: 0.0,
        evidence_multiplier: 0.0,
        evidence_coverage: 0.0,
        crosses: &crosses,
    };

    let selected_evaluator = RulesEvaluator::new(&policy, market(), CandleInterval::H1)?;
    let (_, _, selected_trace) = selected_evaluator.evaluate_rules_with_trace(&context)?;
    let skipped_fallback = condition_trace_at(&selected_trace, "fallback:if")?;
    assert_eq!(skipped_fallback.scope, PolicyConditionScope::FallbackTarget);
    assert_eq!(
        skipped_fallback.outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::PriorRuleMatched)
    );
    assert_eq!(skipped_fallback.engine_result, None);

    let mut nonmatching_policy = policy.clone();
    let PolicyProgram::Rules { program } = &mut nonmatching_policy.program else {
        return Err(LabError::Internal("fixture must be a Rules policy".into()));
    };
    program.rules[0].condition = BoolExpr::PositionIs {
        state: PositionState::Long,
    };
    let nonmatching_evaluator =
        RulesEvaluator::new(&nonmatching_policy, market(), CandleInterval::H1)?;
    let (_, _, nonmatching_trace) = nonmatching_evaluator.evaluate_rules_with_trace(&context)?;
    let skipped_target = condition_trace_at(&nonmatching_trace, "rule:entry:target:if")?;
    assert_eq!(
        skipped_target.scope,
        PolicyConditionScope::RuleTarget {
            rule_id: "entry".into(),
        }
    );
    assert_eq!(
        skipped_target.outcome,
        PolicyConditionOutcome::NotEvaluated(PolicyConditionReason::RuleNotMatched)
    );
    assert_eq!(skipped_target.engine_result, None);

    let mut fallback_policy = policy;
    let PolicyProgram::Rules { program } = &mut fallback_policy.program else {
        return Err(LabError::Internal("fixture must be a Rules policy".into()));
    };
    program.rules.clear();
    let fallback_evaluator = RulesEvaluator::new(&fallback_policy, market(), CandleInterval::H1)?;
    let (matched, target, fallback_trace) =
        fallback_evaluator.evaluate_rules_with_trace(&context)?;
    assert_eq!(matched, None);
    assert_eq!(target.to_bits(), 0.75_f64.to_bits());
    let fallback_guard = condition_trace_at(&fallback_trace, "fallback:if")?;
    assert_eq!(fallback_guard.scope, PolicyConditionScope::FallbackTarget);
    assert_eq!(fallback_guard.role, PolicyConditionRole::Unspecified);
    assert_eq!(
        fallback_guard.outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable,)
    );
    assert_eq!(fallback_guard.engine_result, Some(false));
    Ok(())
}

#[test]
fn unknown_cross_condition_keeps_legacy_false_result() {
    let indicators = std::collections::BTreeMap::<String, f64>::new();
    let state = std::collections::BTreeMap::<String, f64>::new();
    let crosses = std::collections::BTreeMap::from([(
        "rule:cross:when".to_owned(),
        CrossSample {
            previous: None,
            current: (100.0, 50.0),
        },
    )]);
    let context = EvalContext {
        indicators: &indicators,
        state: &state,
        actual_state: PositionState::Cash,
        actual_weight: 0.0,
        held_bars: 0.0,
        unrealized_return_bps: 0.0,
        trailing_drawdown_bps: 0.0,
        evidence_multiplier: 0.0,
        evidence_coverage: 0.0,
        crosses: &crosses,
    };
    let condition = BoolExpr::CrossesAbove {
        left: constant(100.0),
        right: constant(50.0),
    };
    let mut collector = ConditionTraceCollector::new(
        PolicyConditionScope::RuleCondition {
            rule_id: "cross".into(),
        },
        &[],
    );
    let engine_result = eval_bool(
        &condition,
        &context,
        "rule:cross:when",
        Some(&mut collector),
    )
    .expect("known cross sample entry");

    assert!(!engine_result);
    assert_eq!(collector.entries.len(), 1);
    assert_eq!(
        collector.entries[0].outcome,
        PolicyConditionOutcome::Unknown(PolicyConditionReason::CrossHistoryUnavailable)
    );
    assert_eq!(collector.entries[0].engine_result, Some(false));
}

#[test]
fn source_provenance_is_bounded_and_legacy_checkpoint_origin_stays_unknown() -> Result<(), LabError>
{
    let policy = definition(RulesProgram {
        indicators: vec![close_indicator()],
        states: Vec::new(),
        rules: Vec::new(),
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let mut evaluator = RulesEvaluator::new(&policy, market(), CandleInterval::H1)?;
    let first = canonical_observation(0, 100);
    let mut latest = evaluator
        .observe(&first, cash(), None)?
        .expect("close source is warm on its first bar");
    for index in 1..12_i64 {
        let bar = canonical_observation(index, 100 + index);
        latest = evaluator
            .observe(&bar, cash(), None)?
            .expect("close source remains ready");
    }

    let source = &latest
        .trace
        .source_provenance
        .as_ref()
        .expect("source trace")[0];
    assert_eq!(
        source.capture_origin,
        PolicySourceCaptureOrigin::CapturedFromEvaluatorStart
    );
    assert_eq!(source.required_warmup_bars, 1);
    assert_eq!(source.captured_count, 12);
    assert_eq!(
        source
            .capture_anchor
            .as_ref()
            .expect("capture anchor")
            .observation_id,
        first.id
    );
    assert_eq!(source.recent_refs.len(), 8);
    assert_eq!(
        source.recent_refs[0].observation_id,
        canonical_observation(4, 104).id
    );
    let expected_refs = (0..12_i64)
        .map(|index| {
            let observation = canonical_observation(index, 100 + index);
            PolicySourceObservationRef {
                observation_id: observation.id.clone(),
                content_digest: observation.content_digest.clone(),
                open_time: observation.candle.open_time_utc,
                close_time: observation.candle.close_time_utc,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        source.rolling_digest.as_ref(),
        policy_source_provenance_digest(&expected_refs)?.as_ref()
    );

    let checkpoint = serde_json::to_value(&evaluator)?;
    assert_eq!(
        checkpoint["source_provenance"]["close"]["recent_refs"]
            .as_array()
            .expect("bounded checkpoint refs")
            .len(),
        8
    );

    let mut legacy_checkpoint = checkpoint;
    let legacy_fields = legacy_checkpoint
        .as_object_mut()
        .expect("serialized evaluator object");
    assert!(legacy_fields.remove("source_provenance").is_some());
    let mut resumed: RulesEvaluator = serde_json::from_value(legacy_checkpoint)?;
    let resumed_observation = canonical_observation(12, 112);
    let resumed_result = resumed
        .observe(&resumed_observation, cash(), None)?
        .expect("resumed close source is ready");
    let resumed_source = &resumed_result
        .trace
        .source_provenance
        .as_ref()
        .expect("source trace")[0];
    assert_eq!(
        resumed_source.capture_origin,
        PolicySourceCaptureOrigin::NotCaptured
    );
    assert_eq!(resumed_source.captured_count, 1);
    assert_eq!(
        resumed_source
            .capture_anchor
            .as_ref()
            .expect("captured suffix anchor")
            .observation_id,
        resumed_observation.id
    );
    Ok(())
}

#[test]
fn numeric_if_guards_division_and_target_bounds_fail_closed() {
    let guarded = NumericExpr::If {
        condition: Box::new(BoolExpr::Compare {
            comparison: CompareOp::Eq,
            left: constant(0.0),
            right: constant(0.0),
        }),
        then_value: Box::new(constant(0.0)),
        else_value: Box::new(NumericExpr::Div {
            left: Box::new(constant(1.0)),
            right: Box::new(constant(0.0)),
        }),
    };
    let make = |target| RulesProgram {
        indicators: vec![close_indicator()],
        states: Vec::new(),
        rules: Vec::new(),
        fallback: PolicyTarget::Weight { value: target },
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    };
    let mut safe = RulesEvaluator::new(&definition(make(guarded)), market(), CandleInterval::H1)
        .expect("compile");
    assert_eq!(
        safe.observe(&observation(0, 100), cash(), None)
            .expect("guarded")
            .expect("ready")
            .target
            .get(),
        Decimal::ZERO
    );
    let mut unsafe_div = RulesEvaluator::new(
        &definition(make(NumericExpr::Div {
            left: Box::new(constant(1.0)),
            right: Box::new(constant(0.0)),
        })),
        market(),
        CandleInterval::H1,
    )
    .expect("compile");
    assert!(
        unsafe_div
            .observe(&observation(0, 100), cash(), None)
            .is_err()
    );
    let mut out_of_range = RulesEvaluator::new(
        &definition(make(constant(1.1))),
        market(),
        CandleInterval::H1,
    )
    .expect("compile");
    assert!(
        out_of_range
            .observe(&observation(0, 100), cash(), None)
            .is_err()
    );
}

#[test]
fn zero_volume_and_cash_are_valid_but_unanchored_bars_are_rejected() {
    let policy = definition(RulesProgram {
        indicators: vec![
            PolicyIndicator {
                id: "volume".into(),
                indicator: PolicyIndicatorKind::Volume,
                source_interval: None,
            },
            PolicyIndicator {
                id: "turnover".into(),
                indicator: PolicyIndicatorKind::QuoteTurnover,
                source_interval: None,
            },
        ],
        states: vec![],
        rules: vec![],
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let mut evaluator = RulesEvaluator::new(&policy, market(), CandleInterval::H1).expect("policy");
    let mut bar = observation(0, 100);
    bar.candle.volume = AssetQuantity::new(Decimal::ZERO).expect("zero volume");
    bar.candle.quote_turnover = QuoteAmount::new(Decimal::ZERO).expect("zero turnover");
    let result = evaluator
        .observe(&bar, cash(), None)
        .expect("zero values accepted")
        .expect("ready");
    assert_eq!(result.target.get(), Decimal::ZERO);
    assert!(
        result
            .indicator_values
            .iter()
            .all(|value| value.value == 0.0)
    );
    for shift in [Duration::milliseconds(1), Duration::seconds(1)] {
        let mut evaluator =
            RulesEvaluator::new(&policy, market(), CandleInterval::H1).expect("policy");
        let mut shifted = bar.clone();
        shifted.candle.open_time_utc.0 += shift;
        shifted.candle.close_time_utc.0 += shift;
        assert!(matches!(
            evaluator.observe(&shifted, cash(), None),
            Err(LabError::DataGap(_))
        ));
    }
}

#[test]
fn cross_interval_indicator_sources_fail_closed_until_multi_timeframe_ships() {
    let policy = definition(RulesProgram {
        indicators: vec![PolicyIndicator {
            id: "regime_ema".into(),
            indicator: PolicyIndicatorKind::Ema { window: 20 },
            source_interval: Some(CandleInterval::H4),
        }],
        states: vec![],
        rules: vec![],
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    // The evaluator accepts cross-interval declarations and runs them from a
    // separate causal source stream (fed via observe_source).
    let mut evaluator =
        RulesEvaluator::new(&policy, market(), CandleInterval::H1).expect("policy compiles");
    assert_eq!(evaluator.source_indicators().len(), 1);
    assert_eq!(evaluator.warmup_bars, 1);
    assert_eq!(
        policy.source_requirements(CandleInterval::H1),
        vec![(CandleInterval::H4, 20)]
    );
    // Same-interval declaration is an explicit decision-stream indicator.
    let same = definition(RulesProgram {
        indicators: vec![PolicyIndicator {
            id: "ema".into(),
            indicator: PolicyIndicatorKind::Ema { window: 20 },
            source_interval: Some(CandleInterval::H1),
        }],
        states: vec![],
        rules: vec![],
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let same_evaluator = RulesEvaluator::new(&same, market(), CandleInterval::H1)
        .expect("same-interval source is the decision stream");
    assert!(same_evaluator.source_indicators().is_empty());

    // Unknown ids and decision-stream ids are rejected on the source feed.
    let bar = observation(0, 80);
    assert!(matches!(
        evaluator.observe_source("missing", &bar),
        Err(LabError::InvalidConfig(_))
    ));
    assert!(matches!(
        same_evaluator.clone().observe_source("ema", &bar),
        Err(LabError::InvalidConfig(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one hand-computed multi-timeframe journey proves causal feed, values and gating"
)]
fn cross_interval_source_feed_is_causal_deterministic_and_leak_free()
-> Result<(), Box<dyn std::error::Error>> {
    // Multi-timeframe policy: an M1 decision stream gates on H1 EMAs fed
    // from a separate causal source stream.
    let policy = definition(RulesProgram {
        indicators: vec![
            PolicyIndicator {
                id: "h1_ema_fast".into(),
                indicator: PolicyIndicatorKind::Ema { window: 2 },
                source_interval: Some(CandleInterval::H1),
            },
            PolicyIndicator {
                id: "h1_ema_slow".into(),
                indicator: PolicyIndicatorKind::Ema { window: 4 },
                source_interval: Some(CandleInterval::H1),
            },
        ],
        states: vec![],
        rules: vec![PolicyRule {
            id: "regime_long".into(),
            condition: BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Indicator {
                    id: "h1_ema_fast".into(),
                },
                right: NumericExpr::Indicator {
                    id: "h1_ema_slow".into(),
                },
            },
            target: PolicyTarget::Weight {
                value: NumericExpr::Constant { value: 1.0 },
            },
        }],
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let mut evaluator =
        RulesEvaluator::new(&policy, market(), CandleInterval::M1).expect("policy compiles");
    assert_eq!(evaluator.source_indicators().len(), 2);

    // H1 source bars: a ramp then a drop. The decision stream is contiguous
    // M1 bars; decision k closes at minute k+1, so the H1 bar covering hour h
    // closes exactly at decision k = 60*(h+1)-1.
    let path = [80_i64, 82, 84, 86, 88, 90, 70, 66];
    let h1_bars: Vec<CandleObservation> = path
        .iter()
        .enumerate()
        .map(|(index, close)| {
            let mut bar = observation(index.try_into().expect("index"), *close);
            bar.candle.interval = CandleInterval::H1;
            bar
        })
        .collect();
    let decision_count = path.len() * 60;
    let decisions: Vec<CandleObservation> = (0..decision_count)
        .map(|minute| {
            let mut bar = observation(
                i64::try_from(minute).expect("minute"),
                i64::try_from(100 + minute % 7).expect("close"),
            );
            bar.candle.interval = CandleInterval::M1;
            bar.candle.open_time_utc = UtcTimestamp(
                Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
                    .single()
                    .expect("time")
                    + Duration::minutes(i64::try_from(minute).expect("minute")),
            );
            bar.candle.close_time_utc =
                UtcTimestamp(bar.candle.open_time_utc.0 + Duration::minutes(1));
            bar
        })
        .collect();

    let mut source_index = 0_usize;
    let mut targets: Vec<Option<Decimal>> = Vec::new();
    let mut fast_values: Vec<f64> = Vec::new();
    for bar in &decisions {
        while source_index < h1_bars.len()
            && h1_bars[source_index].candle.close_time_utc <= bar.candle.close_time_utc
        {
            evaluator.observe_source("h1_ema_fast", &h1_bars[source_index])?;
            evaluator.observe_source("h1_ema_slow", &h1_bars[source_index])?;
            source_index += 1;
        }
        if let Some(evaluation) = evaluator.observe(bar, cash(), None)? {
            targets.push(Some(evaluation.target.get()));
            fast_values.push(
                evaluation
                    .indicator_values
                    .iter()
                    .find(|value| value.id == "h1_ema_fast")
                    .expect("fast value present")
                    .value,
            );
        } else {
            targets.push(none_target());
            fast_values.push(f64::NAN);
        }
    }
    // Hand-computed boundaries: EMA2 over the H1 closes seeds at bar 1
    // (values 81, 83, 85, 87, 89, 76.33, 69.44); EMA4 seeds at bar 3 (83,
    // 85, 87, 80.2, 74.52). The H1 bar for hour h is fed at decision
    // k = 60*(h+1)-1, so decisions before k=239 are unready.
    for (k, target) in targets.iter().enumerate().take(239) {
        assert_eq!(*target, none_target(), "decision {k} awaits source warmup");
    }
    // Fast 85 > slow 83 at the first warm decision.
    assert_eq!(targets[239], Some(Decimal::ONE));
    assert!((fast_values[239] - 85.0).abs() < 1e-9);
    for target in targets.iter().take(419).skip(240) {
        assert_eq!(*target, Some(Decimal::ONE), "ramp keeps the gate on");
    }
    // The drop bar (70) feeds at k=419: fast 76.33 < slow 80.2 -> fallback 0.
    assert_eq!(targets[419], Some(Decimal::ZERO));
    assert!((fast_values[419] - 76.333_333_333).abs() < 1e-6);
    for target in targets.iter().skip(420) {
        assert_eq!(*target, Some(Decimal::ZERO));
    }
    // Strict causal feed: all eight H1 bars were consumed by the last minute.
    assert_eq!(source_index, h1_bars.len());
    // After the final drop bar (66): fast 69.44 < slow 74.52.
    assert!((fast_values[479] - 69.444_444_444).abs() < 1e-6);

    // Misfed bars fail closed: regressions, foreign intervals, incomplete bars.
    assert!(matches!(
        evaluator.observe_source("h1_ema_fast", &h1_bars[3]),
        Err(LabError::DataGap(_))
    ));
    let mut foreign = h1_bars[3].clone();
    foreign.candle.interval = CandleInterval::M5;
    assert!(matches!(
        evaluator.observe_source("h1_ema_fast", &foreign),
        Err(LabError::DataGap(_))
    ));
    let mut incomplete = h1_bars[3].clone();
    incomplete.candle.completed = false;
    assert!(matches!(
        evaluator.observe_source("h1_ema_fast", &incomplete),
        Err(LabError::DataGap(_))
    ));

    // Determinism: an identical replay produces identical values.
    let mut replay = RulesEvaluator::new(&policy, market(), CandleInterval::M1)?;
    let mut replay_source = 0_usize;
    for bar in &decisions {
        while replay_source < h1_bars.len()
            && h1_bars[replay_source].candle.close_time_utc <= bar.candle.close_time_utc
        {
            replay.observe_source("h1_ema_fast", &h1_bars[replay_source])?;
            replay.observe_source("h1_ema_slow", &h1_bars[replay_source])?;
            replay_source += 1;
        }
    }
    assert_eq!(replay.cross_latest, evaluator.cross_latest);
    Ok(())
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "one deterministic policy journey proves dense defaults, fail-closed mutation, actual-bar count, and carried SMA state"
)]
fn verified_decision_gap_uses_only_actual_bars_and_preserves_dense_defaults() -> Result<(), LabError>
{
    let definition = definition(RulesProgram {
        indicators: vec![PolicyIndicator {
            id: "close_sma".into(),
            indicator: PolicyIndicatorKind::Sma { window: 2 },
            source_interval: None,
        }],
        states: Vec::new(),
        rules: Vec::new(),
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    });
    let revision = frozen_revision(definition, "verified-decision-gap");
    let mut evaluator =
        PolicyEvaluator::compile_frozen_revision(&revision, market(), CandleInterval::H1)?;
    let previous = canonical_observation(17, 100);
    let next = canonical_observation(22, 110);
    assert!(evaluator.observe(&previous, cash(), None)?.is_none());
    let before_gap = serde_json::to_value(&evaluator)?;

    assert!(matches!(
        evaluator.observe(&next, cash(), None),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(serde_json::to_value(&evaluator)?, before_gap);

    let one_slot_short = verified_coverage(
        market(),
        CandleInterval::H1,
        time("2024-01-01T18:00:00Z"),
        time("2024-01-01T21:00:00Z"),
        "one-uncovered-h1-slot",
    );
    assert!(matches!(
        evaluator.observe_with_provider_coverage(&next, cash(), None, &one_slot_short),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(serde_json::to_value(&evaluator)?, before_gap);

    let wrong_market_coverage = verified_coverage(
        MarketId::parse_upbit("KRW-ETH")?,
        CandleInterval::H1,
        time("2024-01-01T18:00:00Z"),
        time("2024-01-01T22:00:00Z"),
        "wrong-market-gap",
    );
    assert!(matches!(
        evaluator.observe_with_provider_coverage(&next, cash(), None, &wrong_market_coverage),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(serde_json::to_value(&evaluator)?, before_gap);

    let coverage = verified_coverage(
        market(),
        CandleInterval::H1,
        time("2024-01-01T18:00:00Z"),
        time("2024-01-01T22:00:00Z"),
        "verified-h1-gap",
    );
    let result = evaluator
        .observe_with_provider_coverage(&next, cash(), None, &coverage)?
        .expect("second actual bar meets SMA warmup");
    assert_eq!(result.indicators.completed_bars_seen, 2);
    assert_eq!(evaluator.completed_bars_seen(), 2);
    let sma = result
        .indicators
        .policy_values
        .iter()
        .find(|value| value.id == "close_sma")
        .expect("SMA value from two actual closes");
    assert_eq!(sma.value, 105.0);
    Ok(())
}

#[test]
fn verified_source_gap_keeps_the_actual_source_cursor_and_indicator_history() -> Result<(), LabError>
{
    let revision = revision_for(
        RulesProgram {
            indicators: vec![PolicyIndicator {
                id: "h1_sma".into(),
                indicator: PolicyIndicatorKind::Sma { window: 2 },
                source_interval: Some(CandleInterval::H1),
            }],
            states: Vec::new(),
            rules: Vec::new(),
            fallback: PolicyTarget::Hold,
            signal_expiry: PolicySignalExpiry::EndOfRange,
            condition_roles: Vec::new(),
        },
        "verified-source-gap",
    );
    let mut evaluator =
        PolicyEvaluator::compile_frozen_revision(&revision, market(), CandleInterval::M1)?;
    let previous = canonical_observation(17, 100);
    let next = canonical_observation(22, 110);
    evaluator.observe_source("h1_sma", &previous)?;
    let before_gap = serde_json::to_value(&evaluator)?;

    assert!(matches!(
        evaluator.observe_source("h1_sma", &next),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(serde_json::to_value(&evaluator)?, before_gap);

    let wrong_scope = verified_coverage(
        market(),
        CandleInterval::M5,
        time("2024-01-01T18:00:00Z"),
        time("2024-01-01T22:00:00Z"),
        "wrong-source-interval",
    );
    assert!(matches!(
        evaluator.observe_source_with_provider_coverage("h1_sma", &next, &wrong_scope),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(serde_json::to_value(&evaluator)?, before_gap);

    let coverage = verified_coverage(
        market(),
        CandleInterval::H1,
        time("2024-01-01T18:00:00Z"),
        time("2024-01-01T22:00:00Z"),
        "verified-source-gap",
    );
    evaluator.observe_source_with_provider_coverage("h1_sma", &next, &coverage)?;
    assert_eq!(
        evaluator.cross_last_close()["h1_sma"],
        next.candle.close_time_utc
    );
    assert_eq!(evaluator.completed_bars_seen(), 0);
    let CompiledPolicy::Rules(rules) = &evaluator.inner else {
        return Err(LabError::Internal(
            "fixture compiled as builtin policy".into(),
        ));
    };
    assert_eq!(rules.cross_latest.get("h1_sma"), Some(&105.0));
    Ok(())
}

#[test]
fn prepare_interval_bars_uses_largest_divisible_finer_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let start = UtcTimestamp(
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
            .single()
            .expect("start time"),
    );
    let end = UtcTimestamp(start.0 + Duration::hours(1));
    let range = crate::contracts::UtcRange::new(start, end)?;
    let market = market();
    let rows = |interval: CandleInterval, count: usize, id_start: i64, close_start: i64| {
        (0..count)
            .map(|index| {
                let index = i64::try_from(index).expect("source index");
                let mut row = observation(id_start + index, close_start + index);
                let open = UtcTimestamp(
                    start.0 + Duration::minutes(index * interval.duration().num_minutes()),
                );
                row.candle.interval = interval;
                row.candle.open_time_utc = open;
                row.candle.close_time_utc = UtcTimestamp(open.0 + interval.duration());
                row
            })
            .collect::<Vec<_>>()
    };
    let snapshot = |id: &str, interval: CandleInterval, observations: Vec<CandleObservation>| {
        crate::contracts::DatasetSnapshot {
            manifest: crate::contracts::DatasetManifest {
                schema_version: "1.0".into(),
                id: crate::contracts::DatasetId::new(id).expect("dataset id"),
                request: crate::contracts::CollectRequest {
                    request_id: crate::contracts::RequestId::new(format!("request-{id}"))
                        .expect("request id"),
                    markets: vec![market.clone()],
                    range,
                    data_resolution: interval,
                    warmup_bars: 0,
                    completed_only: true,
                },
                coverage: range,
                status: crate::contracts::DatasetStatus::Ready,
                row_count: observations.len() as u64,
                normalizer_version: "fixture-v1".into(),
                gap_policy: "FAIL_CLOSED".into(),
                semantic_digest: ContentHash::of_bytes(format!("semantic-{id}").as_bytes()),
                provenance_digest: ContentHash::of_bytes(format!("provenance-{id}").as_bytes()),
                origin: crate::contracts::MarketDataOrigin::SyntheticTestOnly,
                raw_objects: Vec::new(),
                quality_issues: Vec::new(),
                reuse: None,
            },
            observations,
        }
    };
    let datasets = vec![
        snapshot(
            "dataset-m5",
            CandleInterval::M5,
            rows(CandleInterval::M5, 12, 100, 100),
        ),
        snapshot(
            "dataset-m15",
            CandleInterval::M15,
            rows(CandleInterval::M15, 4, 200, 200),
        ),
    ];

    let prepared = prepare_interval_bars(&datasets, &market, CandleInterval::H1, range)?;
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].candle.close.get(), Decimal::from(203));
    assert_eq!(prepared[0].constituent_ids.len(), 4);
    Ok(())
}

fn none_target() -> Option<Decimal> {
    None
}

#[test]
fn ordered_parallel_try_map_preserves_order_and_lowest_index_error_determinism() {
    let items: Vec<usize> = (0..32).collect();
    let squares = ordered_parallel_try_map(&items, &|| false, |value| Ok(value * value))
        .expect("parallel map should succeed");
    assert_eq!(
        squares,
        items.iter().map(|v| v * v).collect::<Vec<_>>(),
        "parallel output order must match input order"
    );

    // Even if index 7 fails immediately and index 3 fails after a brief delay,
    // the lowest failing index (3) must deterministically win.
    let err = ordered_parallel_try_map(&items, &|| false, |value| {
        if *value == 3 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            return Err(LabError::InvalidConfig("fail-at-3".into()));
        }
        if *value == 7 {
            return Err(LabError::DataGap("fail-at-7".into()));
        }
        Ok(*value)
    })
    .expect_err("parallel map should fail");
    assert!(
        matches!(&err, LabError::InvalidConfig(msg) if msg == "fail-at-3"),
        "expected lowest-index failure fail-at-3, got {err:?}"
    );
}

#[test]
fn ordered_parallel_try_map_catches_worker_panics_and_honors_cancellation() {
    let items: Vec<usize> = (0..16).collect();
    let panic_err = ordered_parallel_try_map(&items, &|| false, |value| {
        assert!(*value != 5, "synthetic worker panic");
        Ok(*value)
    })
    .expect_err("worker panic must convert to LabError::Internal");
    assert!(
        matches!(&panic_err, LabError::Internal(msg) if msg.contains("synthetic worker panic")),
        "expected Internal panic error, got {panic_err:?}"
    );

    let cancel_err = ordered_parallel_try_map(&items, &|| true, |value| Ok(*value))
        .expect_err("cancelled map must return LabError::Cancelled");
    assert!(
        matches!(cancel_err, LabError::Cancelled(_)),
        "expected Cancelled, got {cancel_err:?}"
    );
}

#[test]
fn prepared_bar_cache_reuses_identical_arc_slices() -> Result<(), LabError> {
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let start = UtcTimestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;
    let end = UtcTimestamp::parse_rfc3339("2026-01-01T01:00:00Z")?;
    let range = UtcRange::new(start, end)?;
    let observations = (0..4)
        .map(|index| {
            let mut row = observation(300 + index, 500 + index);
            let open = UtcTimestamp(start.0 + Duration::minutes(index * 15));
            row.candle.interval = CandleInterval::M15;
            row.candle.open_time_utc = open;
            row.candle.close_time_utc = UtcTimestamp(open.0 + Duration::minutes(15));
            row
        })
        .collect::<Vec<_>>();
    let datasets = vec![crate::contracts::DatasetSnapshot {
        manifest: crate::contracts::DatasetManifest {
            schema_version: "1.0".into(),
            id: crate::contracts::DatasetId::new("dataset-cache-m15")?,
            request: crate::contracts::CollectRequest {
                request_id: crate::contracts::RequestId::new("req-cache-m15")?,
                markets: vec![market.clone()],
                range,
                data_resolution: CandleInterval::M15,
                warmup_bars: 0,
                completed_only: true,
            },
            coverage: range,
            status: crate::contracts::DatasetStatus::Ready,
            row_count: 4,
            normalizer_version: "fixture-v1".into(),
            gap_policy: "FAIL_CLOSED".into(),
            semantic_digest: ContentHash::of_bytes(b"sem-cache-m15"),
            provenance_digest: ContentHash::of_bytes(b"prov-cache-m15"),
            origin: crate::contracts::MarketDataOrigin::SyntheticTestOnly,
            raw_objects: Vec::new(),
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations,
    }];
    let cache = PreparedBarCache::new();
    let first =
        prepare_interval_bars_cached(&datasets, &market, CandleInterval::H1, range, Some(&cache))?;
    let second =
        prepare_interval_bars_cached(&datasets, &market, CandleInterval::H1, range, Some(&cache))?;
    assert!(
        std::sync::Arc::ptr_eq(&first, &second),
        "cache hit must return the exact shared Arc slice without reallocating"
    );
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].candle.close.get(), Decimal::from(503));
    Ok(())
}

fn position_aware_test_program() -> RulesProgram {
    RulesProgram {
        indicators: vec![close_indicator()],
        states: vec![
            PolicyState {
                id: "seen_return_bps".into(),
                initial: 0.0,
                next: NumericExpr::UnrealizedReturnBps,
            },
            PolicyState {
                id: "seen_drawdown_bps".into(),
                initial: 0.0,
                next: NumericExpr::TrailingDrawdownBps,
            },
        ],
        rules: vec![
            PolicyRule {
                id: "trailing_stop_exit".into(),
                condition: BoolExpr::And {
                    conditions: vec![
                        BoolExpr::PositionIs {
                            state: PositionState::Long,
                        },
                        BoolExpr::Compare {
                            comparison: CompareOp::Gte,
                            left: NumericExpr::TrailingDrawdownBps,
                            right: constant(400.0),
                        },
                    ],
                },
                target: PolicyTarget::Weight {
                    value: constant(0.0),
                },
            },
            PolicyRule {
                id: "enter_long".into(),
                condition: BoolExpr::PositionIs {
                    state: PositionState::Cash,
                },
                target: PolicyTarget::Weight {
                    value: constant(1.0),
                },
            },
        ],
        fallback: PolicyTarget::Hold,
        signal_expiry: PolicySignalExpiry::EndOfRange,
        condition_roles: Vec::new(),
    }
}

fn state_value(eval: &RuleEvaluation, id: &str) -> f64 {
    eval.trace
        .state_after
        .iter()
        .find(|v| v.id == id)
        .expect(id)
        .value
}

#[test]
fn position_aware_unrealized_return_and_trailing_drawdown_operands_evaluate_deterministically() {
    let mut evaluator = RulesEvaluator::new(
        &definition(position_aware_test_program()),
        market(),
        CandleInterval::H1,
    )
    .expect("compile");

    let bar0 = observation(0, 100);
    let eval0 = evaluator
        .observe(&bar0, cash(), None)
        .expect("bar0")
        .expect("ready");
    assert_eq!(eval0.trace.matched_rule_id.as_deref(), Some("enter_long"));
    assert_eq!(eval0.target.get(), Decimal::ONE);

    let bar1 = observation(1, 110);
    let episode_open = bar1.candle.open_time_utc;
    let eval1 = evaluator
        .observe(&bar1, long(episode_open), None)
        .expect("bar1")
        .expect("ready");
    assert_eq!(eval1.trace.matched_rule_id, None);
    assert_eq!(eval1.target.get(), Decimal::ONE);
    let ret1 = state_value(&eval1, "seen_return_bps");
    let dd1 = state_value(&eval1, "seen_drawdown_bps");
    assert!(
        (ret1 - 1_000.0).abs() < 1e-9,
        "expected +1000 bps, got {ret1}"
    );
    assert!(
        dd1.abs() < 1e-9,
        "expected 0 bps drawdown at peak, got {dd1}"
    );

    let bar2 = observation(2, 104);
    let eval2 = evaluator
        .observe(&bar2, long(episode_open), None)
        .expect("bar2")
        .expect("ready");
    assert_eq!(
        eval2.trace.matched_rule_id.as_deref(),
        Some("trailing_stop_exit")
    );
    assert_eq!(eval2.target.get(), Decimal::ZERO);
    let ret2 = state_value(&eval2, "seen_return_bps");
    let dd2 = state_value(&eval2, "seen_drawdown_bps");
    assert!((ret2 - 400.0).abs() < 1e-9, "expected +400 bps, got {ret2}");
    assert!(
        (dd2 - (6.0 / 110.0) * 10_000.0).abs() < 1e-9,
        "expected ~545.45 bps trailing drawdown, got {dd2}"
    );
}
