use super::{
    audit_snapshot_rules, audit_source_indicators, compare_machine_contracts,
    insufficient_evidence_report,
};
use crate::contracts::policy::{
    BoolExpr, CompareOp, FrozenPolicyRevision, NamedPolicyValue, NumericExpr, PolicyDefinition,
    PolicyIndicator, PolicyIndicatorKind, PolicyOrigin, PolicyProgram, PolicyRevisionRef,
    PolicyRule, PolicySignalExpiry, PolicyState, PolicyTarget, RulesProgram,
};
use crate::contracts::{
    AssetQuantity, CandleInterval, CandleObservation, CandleRecord, ContentHash,
    IndicatorSeedEvidence, IndicatorSourceClosureEvidence, IndicatorSourceReference,
    IndicatorSourceSeries, LabError, MachineContractApprovalOrigin, MachineContractComparisonInput,
    MachineContractFact, MarketId, ObservationId, PolicyId, PolicyRevisionId, PositionState,
    QuoteAmount, ReviewAuditBasis, ReviewAuditDiagnosticCode, ReviewAuditKind, ReviewAuditStatus,
    SavedRuleDecision, SnapshotRuleAuditInput, SourceCaptureOrigin, SourceHistoryScope,
    SourceIndicatorAuditInput, StrategyKind, UtcTimestamp, Weight, observation_digest,
};
use chrono::{Duration, TimeZone, Utc};
use rust_decimal::Decimal;

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("fixture market")
}

fn observation(
    interval: CandleInterval,
    index: i64,
    close_value: i64,
    high_value: i64,
    low_value: i64,
) -> CandleObservation {
    let epoch = Utc
        .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
        .single()
        .expect("fixture epoch");
    let open_time =
        UtcTimestamp(epoch + Duration::seconds(index * interval.duration().num_seconds()));
    let candle = CandleRecord {
        market: market().code(),
        interval,
        open_time_utc: open_time,
        close_time_utc: UtcTimestamp(open_time.0 + interval.duration()),
        open: crate::contracts::PriceKrw::new(Decimal::from(close_value)).expect("open"),
        high: crate::contracts::PriceKrw::new(Decimal::from(high_value)).expect("high"),
        low: crate::contracts::PriceKrw::new(Decimal::from(low_value)).expect("low"),
        close: crate::contracts::PriceKrw::new(Decimal::from(close_value)).expect("close"),
        volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
        quote_turnover: QuoteAmount::new(Decimal::from(close_value)).expect("turnover"),
        completed: true,
    };
    CandleObservation {
        id: ObservationId::from_seed(&format!("audit-{}-{index}", interval_name(interval))),
        content_digest: observation_digest(&candle).expect("candle digest"),
        candle,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn interval_name(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "m1",
        CandleInterval::M5 => "m5",
        CandleInterval::M15 => "m15",
        CandleInterval::H1 => "h1",
        CandleInterval::H4 => "h4",
        CandleInterval::D1 => "d1",
    }
}

fn frozen(program: PolicyProgram) -> FrozenPolicyRevision {
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "review_audit_fixture".into(),
        description: "synthetic audit input".into(),
        program,
    };
    let digest = ContentHash::of_value(&definition).expect("definition digest");
    FrozenPolicyRevision {
        reference: PolicyRevisionRef {
            policy_id: PolicyId::new("policy-review-audit").expect("policy id"),
            revision_id: PolicyRevisionId::new("revision-review-audit").expect("revision id"),
            definition_digest: digest,
        },
        revision_number: 1,
        parent_revision_id: None,
        family: StrategyKind::S1,
        origin: PolicyOrigin::User,
        definition,
    }
}

fn simple_rules_program(
    indicator_interval: Option<CandleInterval>,
    condition: BoolExpr,
    target: PolicyTarget,
    fallback: PolicyTarget,
    states: Vec<PolicyState>,
) -> RulesProgram {
    RulesProgram {
        indicators: vec![PolicyIndicator {
            id: "close".into(),
            indicator: PolicyIndicatorKind::Close,
            source_interval: indicator_interval,
        }],
        states,
        rules: vec![PolicyRule {
            id: "enter".into(),
            condition,
            target,
        }],
        fallback,
        signal_expiry: PolicySignalExpiry::DecisionBars { bars: 1 },
        condition_roles: Vec::new(),
    }
}

fn indicator(
    id: &str,
    kind: PolicyIndicatorKind,
    source_interval: Option<CandleInterval>,
) -> PolicyIndicator {
    PolicyIndicator {
        id: id.into(),
        indicator: kind,
        source_interval,
    }
}

fn rule_definition(indicators: Vec<PolicyIndicator>) -> PolicyProgram {
    PolicyProgram::Rules {
        program: RulesProgram {
            indicators,
            states: Vec::new(),
            rules: Vec::new(),
            fallback: PolicyTarget::Hold,
            signal_expiry: PolicySignalExpiry::DecisionBars { bars: 1 },
            condition_roles: Vec::new(),
        },
    }
}

fn named(id: &str, value: f64) -> NamedPolicyValue {
    NamedPolicyValue {
        id: id.into(),
        value,
    }
}

fn snapshot_input(
    policy: FrozenPolicyRevision,
    observation: CandleObservation,
    completed_bars_seen: u64,
    indicators: Vec<NamedPolicyValue>,
    state_before: Vec<NamedPolicyValue>,
    saved: SavedRuleDecision,
    prior_cross_snapshot: Option<crate::contracts::RuleCrossPriorSnapshot>,
) -> SnapshotRuleAuditInput {
    let cutoff = observation.candle.close_time_utc;
    SnapshotRuleAuditInput {
        market: market(),
        decision_interval: observation.candle.interval,
        cutoff_utc: cutoff,
        current: crate::contracts::RuleSnapshotContext {
            observation_id: observation.id.clone(),
            decision_time_utc: cutoff,
            completed_bars_seen,
            indicators,
            state_before,
            actual_state: PositionState::Cash,
            actual_weight: Weight::new(Decimal::ZERO).expect("cash weight"),
            held_bars: None,
            evidence: None,
        },
        decision_observation: observation,
        saved,
        policy,
        prior_cross_snapshot,
    }
}

fn audit_source_fixture() -> SourceIndicatorAuditInput {
    let observations = vec![
        observation(CandleInterval::H1, 0, 10, 11, 9),
        observation(CandleInterval::H1, 1, 12, 13, 11),
        observation(CandleInterval::H1, 2, 14, 15, 13),
    ];
    let reference = IndicatorSourceReference::CapturedWindow {
        first_observation_id: observations[0].id.clone(),
        evidence_digest: ContentHash::of_value(&observations).expect("window digest"),
        observation_count: 3,
        capture_origin: SourceCaptureOrigin::SavedObservationWindow,
    };
    let policy = frozen(rule_definition(vec![
        indicator("close", PolicyIndicatorKind::Close, None),
        indicator("sma", PolicyIndicatorKind::Sma { window: 2 }, None),
        indicator("ema", PolicyIndicatorKind::Ema { window: 2 }, None),
        indicator("rsi", PolicyIndicatorKind::Rsi { window: 2 }, None),
        indicator(
            "prior_high",
            PolicyIndicatorKind::PriorHigh { window: 2 },
            None,
        ),
    ]));
    SourceIndicatorAuditInput {
        saved_policy_ref: policy.reference.clone(),
        market: market(),
        decision_interval: CandleInterval::H1,
        cutoff_utc: observations[2].candle.close_time_utc,
        decision_observation: observations[2].clone(),
        source_series: vec![IndicatorSourceSeries {
            interval: CandleInterval::H1,
            observations: observations.clone(),
            seed: IndicatorSeedEvidence {
                source: reference,
                original_seed_observation_id: Some(observations[0].id.clone()),
                history_scope: SourceHistoryScope::CompleteFromSeed,
                bars_through_cutoff: 3,
                required_warmup_bars: 3,
            },
            closure: None,
        }],
        saved_values: vec![
            named("close", 14.0),
            named("sma", 13.0),
            named("ema", 13.0),
            named("rsi", 100.0),
            named("prior_high", 13.0),
        ],
        policy,
    }
}

#[test]
fn snapshot_rules_reconstruct_target_state_and_detect_mismatch() -> Result<(), LabError> {
    let state = PolicyState {
        id: "memory".into(),
        initial: 0.0,
        next: NumericExpr::Add {
            left: Box::new(NumericExpr::State {
                id: "memory".into(),
            }),
            right: Box::new(NumericExpr::Constant { value: 1.0 }),
        },
    };
    let policy = frozen(PolicyProgram::Rules {
        program: simple_rules_program(
            None,
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Indicator { id: "close".into() },
                right: NumericExpr::Constant { value: 100.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::Constant { value: 1.0 },
            },
            // The unused fallback may refer to unknown held-bar context. The
            // first matching rule prevents this branch from being evaluated.
            PolicyTarget::Weight {
                value: NumericExpr::HeldBars,
            },
            vec![state],
        ),
    });
    let decision_observation = observation(CandleInterval::H1, 0, 101, 102, 100);
    let saved = SavedRuleDecision {
        policy_ref: policy.reference.clone(),
        observation_id: decision_observation.id.clone(),
        decision_time_utc: decision_observation.candle.close_time_utc,
        matched_rule_id: Some("enter".into()),
        target_weight: Weight::new(Decimal::ONE).expect("full weight"),
        state_after: vec![named("memory", 1.0)],
    };
    let input = snapshot_input(
        policy.clone(),
        decision_observation.clone(),
        1,
        vec![named("close", 101.0)],
        vec![named("memory", 0.0)],
        saved.clone(),
        None,
    );
    let first = audit_snapshot_rules(&input)?;
    let repeated = audit_snapshot_rules(&input)?;
    assert_eq!(first.status, ReviewAuditStatus::Verified);
    assert_eq!(
        first.basis,
        ReviewAuditBasis::ReconstructedFromSavedSnapshot
    );
    assert_eq!(first.result_digest, repeated.result_digest);
    assert_eq!(first.residuals.len(), 1);
    assert_eq!(first.exact_residuals.len(), 1);
    assert_eq!(first.exact_residuals[0].absolute_difference, "0");
    first.validate_digest()?;

    let mut contradicted = input.clone();
    contradicted.saved.matched_rule_id = None;
    contradicted.saved.target_weight = Weight::new(Decimal::ZERO).expect("zero weight");
    contradicted.saved.state_after = vec![named("memory", 0.0)];
    let result = audit_snapshot_rules(&contradicted)?;
    assert_eq!(result.status, ReviewAuditStatus::Contradiction);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::MatchedRuleMismatch)
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::StateTransitionMismatch)
    );
    let mut wrong_revision = input.clone();
    wrong_revision.saved.policy_ref.revision_id =
        PolicyRevisionId::new("different-saved-revision").expect("alternate revision");
    let identity = audit_snapshot_rules(&wrong_revision)?;
    assert_eq!(identity.status, ReviewAuditStatus::Contradiction);
    assert!(
        identity
            .diagnostics
            .iter()
            .any(|item| { item.code == ReviewAuditDiagnosticCode::PolicyRevisionMismatch })
    );
    Ok(())
}

#[test]
fn snapshot_rule_audit_requires_context_only_for_reached_branch() -> Result<(), LabError> {
    let decision_observation = observation(CandleInterval::H1, 0, 101, 102, 100);
    let missing_context = frozen(PolicyProgram::Rules {
        program: simple_rules_program(
            None,
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Indicator { id: "close".into() },
                right: NumericExpr::Constant { value: 100.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::Constant { value: 1.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::HeldBars,
            },
            Vec::new(),
        ),
    });
    let missing_saved = SavedRuleDecision {
        policy_ref: missing_context.reference.clone(),
        observation_id: decision_observation.id.clone(),
        decision_time_utc: decision_observation.candle.close_time_utc,
        matched_rule_id: None,
        target_weight: Weight::new(Decimal::ZERO).expect("saved fallback"),
        state_after: Vec::new(),
    };
    let mut missing_input = snapshot_input(
        missing_context,
        decision_observation,
        1,
        vec![named("close", 99.0)],
        Vec::new(),
        missing_saved,
        None,
    );
    // The saved bar's close is 101; the context is separate saved evidence.
    // Rebuild the decision bar at 99 to isolate lazy HELD_BARS behavior.
    let low_bar = observation(CandleInterval::H1, 0, 99, 100, 98);
    missing_input.decision_observation = low_bar.clone();
    missing_input.cutoff_utc = low_bar.candle.close_time_utc;
    missing_input.current.observation_id = low_bar.id.clone();
    missing_input.current.decision_time_utc = low_bar.candle.close_time_utc;
    missing_input.saved.observation_id = low_bar.id;
    missing_input.saved.decision_time_utc = low_bar.candle.close_time_utc;
    let result = audit_snapshot_rules(&missing_input)?;
    assert_eq!(result.status, ReviewAuditStatus::InsufficientEvidence);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|item| item.path.contains("held_bars"))
    );
    Ok(())
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "the saved prior operand is an exact integer-valued fixture"
)]
fn snapshot_cross_requires_an_adjacent_prior() -> Result<(), LabError> {
    let program = simple_rules_program(
        None,
        BoolExpr::CrossesAbove {
            left: NumericExpr::Indicator { id: "close".into() },
            right: NumericExpr::Constant { value: 100.0 },
        },
        PolicyTarget::Weight {
            value: NumericExpr::Constant { value: 1.0 },
        },
        PolicyTarget::Hold,
        Vec::new(),
    );
    let policy = frozen(PolicyProgram::Rules { program });
    let previous = observation(CandleInterval::H1, 1, 95, 96, 94);
    let current = observation(CandleInterval::H1, 2, 105, 106, 104);
    let previous_saved = SavedRuleDecision {
        policy_ref: policy.reference.clone(),
        observation_id: previous.id.clone(),
        decision_time_utc: previous.candle.close_time_utc,
        matched_rule_id: None,
        target_weight: Weight::new(Decimal::ZERO).expect("hold weight"),
        state_after: Vec::new(),
    };
    let previous_input = snapshot_input(
        policy.clone(),
        previous.clone(),
        2,
        vec![named("close", 95.0)],
        Vec::new(),
        previous_saved,
        None,
    );
    let prior = super::reconstruct_cross_prior_snapshot(&previous_input)?
        .expect("simple cross operands reconstruct from saved values");
    assert_eq!(prior.completed_bars_seen, 2);
    // The independently reconstructed saved operand is an exact integer value.
    assert_eq!(prior.values[0].left, 95.0);
    let saved = SavedRuleDecision {
        policy_ref: policy.reference.clone(),
        observation_id: current.id.clone(),
        decision_time_utc: current.candle.close_time_utc,
        matched_rule_id: Some("enter".into()),
        target_weight: Weight::new(Decimal::ONE).expect("full weight"),
        state_after: Vec::new(),
    };
    let missing = snapshot_input(
        policy.clone(),
        current.clone(),
        3,
        vec![named("close", 105.0)],
        Vec::new(),
        saved.clone(),
        None,
    );
    let missing_report = audit_snapshot_rules(&missing)?;
    assert_eq!(
        missing_report.status,
        ReviewAuditStatus::InsufficientEvidence
    );
    assert!(
        missing_report
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::MissingCrossPriorSnapshot)
    );

    let with_prior = snapshot_input(
        policy.clone(),
        current.clone(),
        3,
        vec![named("close", 105.0)],
        Vec::new(),
        saved,
        Some(prior),
    );
    assert_eq!(
        audit_snapshot_rules(&with_prior)?.status,
        ReviewAuditStatus::Verified
    );
    let mut nonadjacent = with_prior.clone();
    nonadjacent
        .prior_cross_snapshot
        .as_mut()
        .expect("prior cross snapshot")
        .completed_bars_seen = 1;
    assert_eq!(
        audit_snapshot_rules(&nonadjacent)?.status,
        ReviewAuditStatus::InsufficientEvidence
    );
    Ok(())
}

#[test]
fn nested_cross_operands_and_builtin_programs_fail_closed() -> Result<(), LabError> {
    let previous = observation(CandleInterval::H1, 1, 95, 96, 94);
    let current = observation(CandleInterval::H1, 2, 105, 106, 104);
    let nested = frozen(PolicyProgram::Rules {
        program: simple_rules_program(
            None,
            BoolExpr::CrossesAbove {
                left: NumericExpr::If {
                    condition: Box::new(BoolExpr::CrossesAbove {
                        left: NumericExpr::Indicator { id: "close".into() },
                        right: NumericExpr::Constant { value: 100.0 },
                    }),
                    then_value: Box::new(NumericExpr::Constant { value: 200.0 }),
                    else_value: Box::new(NumericExpr::Constant { value: 50.0 }),
                },
                right: NumericExpr::Constant { value: 100.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::Constant { value: 1.0 },
            },
            PolicyTarget::Hold,
            Vec::new(),
        ),
    });
    let nested_saved = SavedRuleDecision {
        policy_ref: nested.reference.clone(),
        observation_id: previous.id.clone(),
        decision_time_utc: previous.candle.close_time_utc,
        matched_rule_id: None,
        target_weight: Weight::new(Decimal::ZERO).expect("hold weight"),
        state_after: Vec::new(),
    };
    let nested_previous = snapshot_input(
        nested,
        previous.clone(),
        2,
        vec![named("close", 95.0)],
        Vec::new(),
        nested_saved,
        None,
    );
    assert!(super::reconstruct_cross_prior_snapshot(&nested_previous)?.is_none());

    let builtin = frozen(PolicyProgram::Builtin {
        strategy: crate::contracts::StrategySpec::BuyAndHold,
    });
    let builtin_saved = SavedRuleDecision {
        policy_ref: builtin.reference.clone(),
        observation_id: current.id.clone(),
        decision_time_utc: current.candle.close_time_utc,
        matched_rule_id: None,
        target_weight: Weight::new(Decimal::ONE).expect("full weight"),
        state_after: Vec::new(),
    };
    let builtin_input = snapshot_input(
        builtin,
        current,
        3,
        vec![named("close", 105.0)],
        Vec::new(),
        builtin_saved,
        None,
    );
    assert_eq!(
        audit_snapshot_rules(&builtin_input)?.status,
        ReviewAuditStatus::Unsupported
    );
    Ok(())
}

#[test]
fn unsupported_rule_arithmetic_never_becomes_a_passing_decision() -> Result<(), LabError> {
    let policy = frozen(PolicyProgram::Rules {
        program: simple_rules_program(
            None,
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Indicator { id: "close".into() },
                right: NumericExpr::Constant { value: 100.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::Div {
                    left: Box::new(NumericExpr::Constant { value: 1.0 }),
                    right: Box::new(NumericExpr::Constant { value: 0.0 }),
                },
            },
            PolicyTarget::Hold,
            Vec::new(),
        ),
    });
    let bar = observation(CandleInterval::H1, 0, 101, 102, 100);
    let saved = SavedRuleDecision {
        policy_ref: policy.reference.clone(),
        observation_id: bar.id.clone(),
        decision_time_utc: bar.candle.close_time_utc,
        matched_rule_id: None,
        target_weight: Weight::new(Decimal::ZERO).expect("saved weight"),
        state_after: Vec::new(),
    };
    let input = snapshot_input(
        policy,
        bar,
        1,
        vec![named("close", 101.0)],
        Vec::new(),
        saved,
        None,
    );
    let report = audit_snapshot_rules(&input)?;
    assert_eq!(report.status, ReviewAuditStatus::Unsupported);
    assert!(
        report
            .diagnostics
            .iter()
            .any(|item| { item.code == ReviewAuditDiagnosticCode::UnsupportedArithmetic })
    );
    Ok(())
}

#[test]
fn snapshot_target_residual_preserves_exact_decimal_difference() -> Result<(), LabError> {
    let policy = frozen(PolicyProgram::Rules {
        program: simple_rules_program(
            None,
            BoolExpr::Compare {
                comparison: CompareOp::Gt,
                left: NumericExpr::Indicator { id: "close".into() },
                right: NumericExpr::Constant { value: 100.0 },
            },
            PolicyTarget::Weight {
                value: NumericExpr::Constant { value: 0.5 },
            },
            PolicyTarget::Hold,
            Vec::new(),
        ),
    });
    let bar = observation(CandleInterval::H1, 0, 101, 102, 100);
    let saved = SavedRuleDecision {
        policy_ref: policy.reference.clone(),
        observation_id: bar.id.clone(),
        decision_time_utc: bar.candle.close_time_utc,
        matched_rule_id: Some("enter".into()),
        target_weight: Weight::new(Decimal::new(500_000_000_000_000_001, 18))
            .expect("near-half weight"),
        state_after: Vec::new(),
    };
    let input = snapshot_input(
        policy,
        bar,
        1,
        vec![named("close", 101.0)],
        Vec::new(),
        saved,
        None,
    );
    let report = audit_snapshot_rules(&input)?;
    assert_eq!(report.status, ReviewAuditStatus::Contradiction);
    assert_eq!(
        report.exact_residuals[0].absolute_difference,
        "0.000000000000000001"
    );
    Ok(())
}

#[test]
fn source_indicators_recompute_hand_values_and_ignore_future_suffix() -> Result<(), LabError> {
    let input = audit_source_fixture();
    let first = audit_source_indicators(&input)?;
    let repeated = audit_source_indicators(&input)?;
    assert_eq!(first.status, ReviewAuditStatus::Verified);
    assert_eq!(first.result_digest, repeated.result_digest);
    assert_eq!(first.residuals.len(), 5);
    first.validate_digest()?;

    let encoded = serde_json::to_value(&input.policy)?;
    let old_frozen_policy: FrozenPolicyRevision = serde_json::from_value(encoded.clone())?;
    assert_eq!(serde_json::to_value(old_frozen_policy)?, encoded);

    let mut with_future = input.clone();
    with_future.source_series[0]
        .observations
        .push(observation(CandleInterval::H1, 3, 16, 17, 15));
    let future_report = audit_source_indicators(&with_future)?;
    assert_eq!(first.status, future_report.status);
    assert_eq!(first.evidence_digest, future_report.evidence_digest);
    assert_eq!(first.result_digest, future_report.result_digest);
    assert_eq!(
        serde_json::to_value(&first)?,
        serde_json::to_value(&future_report)?
    );

    let mut wrong_revision = input;
    wrong_revision.saved_policy_ref.revision_id =
        PolicyRevisionId::new("different-source-revision").expect("alternate revision");
    let identity = audit_source_indicators(&wrong_revision)?;
    assert_eq!(identity.status, ReviewAuditStatus::Contradiction);
    assert!(
        identity
            .diagnostics
            .iter()
            .any(|item| { item.code == ReviewAuditDiagnosticCode::PolicyRevisionMismatch })
    );
    Ok(())
}

#[test]
fn source_seed_and_warmup_gaps_remain_insufficient() -> Result<(), LabError> {
    let mut input = audit_source_fixture();
    input.source_series[0].seed.history_scope = SourceHistoryScope::TruncatedPrefix;
    input.source_series[0].seed.original_seed_observation_id = None;
    let truncated = audit_source_indicators(&input)?;
    assert_eq!(truncated.status, ReviewAuditStatus::InsufficientEvidence);
    assert!(
        truncated
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::MissingSourceSeed)
    );
    assert_eq!(truncated.residuals.len(), 3);

    let prefix = input.source_series[0].observations[..2].to_vec();
    input.decision_observation = prefix[1].clone();
    input.cutoff_utc = prefix[1].candle.close_time_utc;
    input.source_series[0].observations = prefix.clone();
    input.source_series[0].seed.source = IndicatorSourceReference::CapturedWindow {
        first_observation_id: prefix[0].id.clone(),
        evidence_digest: ContentHash::of_value(&prefix)?,
        observation_count: 2,
        capture_origin: SourceCaptureOrigin::SavedObservationWindow,
    };
    input.source_series[0].seed.bars_through_cutoff = 2;
    input.saved_values = vec![
        named("close", 12.0),
        named("sma", 11.0),
        named("ema", 11.0),
        named("rsi", 100.0),
        named("prior_high", 13.0),
    ];
    let warmup = audit_source_indicators(&input)?;
    assert_eq!(warmup.status, ReviewAuditStatus::InsufficientEvidence);
    assert!(
        warmup
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::IncompleteWarmup)
    );
    assert!(
        warmup
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::MissingSourceSeed)
    );
    Ok(())
}

#[test]
fn multi_timeframe_indicator_requires_closed_interval_bound_evidence() -> Result<(), LabError> {
    let decision = observation(CandleInterval::H1, 0, 20, 21, 19);
    let source_bars = vec![
        observation(CandleInterval::M15, 0, 10, 11, 9),
        observation(CandleInterval::M15, 1, 11, 12, 10),
        observation(CandleInterval::M15, 2, 12, 13, 11),
        observation(CandleInterval::M15, 3, 13, 14, 12),
    ];
    let source = IndicatorSourceReference::CapturedWindow {
        first_observation_id: source_bars[0].id.clone(),
        evidence_digest: ContentHash::of_value(&source_bars)?,
        observation_count: 4,
        capture_origin: SourceCaptureOrigin::SavedObservationWindow,
    };
    let policy = frozen(rule_definition(vec![indicator(
        "close",
        PolicyIndicatorKind::Close,
        Some(CandleInterval::M15),
    )]));
    let closure = IndicatorSourceClosureEvidence {
        source: source.clone(),
        closed_through_utc: decision.candle.close_time_utc,
        latest_observation_id: source_bars[3].id.clone(),
        latest_close_time_utc: source_bars[3].candle.close_time_utc,
        evidence_digest: ContentHash::of_bytes(b"saved M15 closure through H1 decision"),
    };
    let input = SourceIndicatorAuditInput {
        saved_policy_ref: policy.reference.clone(),
        policy,
        market: market(),
        decision_interval: CandleInterval::H1,
        cutoff_utc: decision.candle.close_time_utc,
        decision_observation: decision,
        source_series: vec![IndicatorSourceSeries {
            interval: CandleInterval::M15,
            observations: source_bars.clone(),
            seed: IndicatorSeedEvidence {
                source,
                original_seed_observation_id: None,
                history_scope: SourceHistoryScope::TruncatedPrefix,
                bars_through_cutoff: 4,
                required_warmup_bars: 1,
            },
            closure: Some(closure),
        }],
        saved_values: vec![named("close", 13.0)],
    };
    assert_eq!(
        audit_source_indicators(&input)?.status,
        ReviewAuditStatus::Verified
    );

    let mut no_closure = input.clone();
    no_closure.source_series[0].closure = None;
    let result = audit_source_indicators(&no_closure)?;
    assert_eq!(result.status, ReviewAuditStatus::InsufficientEvidence);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|item| item.code == ReviewAuditDiagnosticCode::MissingMultiTimeframeClosure)
    );

    let stale_decision = observation(CandleInterval::H1, 8, 20, 21, 19);
    let stale_bars = vec![
        observation(CandleInterval::M15, 29, 11, 12, 10),
        observation(CandleInterval::M15, 30, 12, 13, 11),
        observation(CandleInterval::M15, 31, 13, 14, 12),
    ];
    let stale_source = IndicatorSourceReference::CapturedWindow {
        first_observation_id: stale_bars[0].id.clone(),
        evidence_digest: ContentHash::of_value(&stale_bars)?,
        observation_count: 3,
        capture_origin: SourceCaptureOrigin::SavedObservationWindow,
    };
    let mut stale = input.clone();
    stale.cutoff_utc = stale_decision.candle.close_time_utc;
    stale.decision_observation = stale_decision.clone();
    stale.source_series[0].observations = stale_bars.clone();
    stale.source_series[0].seed.source = stale_source.clone();
    stale.source_series[0].seed.bars_through_cutoff = 3;
    stale.source_series[0].closure = Some(IndicatorSourceClosureEvidence {
        source: stale_source,
        closed_through_utc: stale_decision.candle.close_time_utc,
        latest_observation_id: stale_bars[2].id.clone(),
        latest_close_time_utc: stale_bars[2].candle.close_time_utc,
        evidence_digest: ContentHash::of_bytes(b"stale M15 closure through H1 decision"),
    });
    let stale_report = audit_source_indicators(&stale)?;
    assert_eq!(stale_report.status, ReviewAuditStatus::InsufficientEvidence);
    assert!(
        stale_report
            .diagnostics
            .iter()
            .any(|item| { item.path == "source_series.M15.closure.stale_tail" })
    );

    let mut wrong_interval = input;
    wrong_interval.source_series[0].interval = CandleInterval::H1;
    assert!(matches!(
        audit_source_indicators(&wrong_interval),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

#[test]
fn exact_machine_fact_comparison_keeps_collection_cadence_distinct() -> Result<(), LabError> {
    let policy = frozen(rule_definition(vec![indicator(
        "close",
        PolicyIndicatorKind::Close,
        None,
    )]));
    let expected = vec![
        MachineContractFact::PolicyRevision {
            reference: policy.reference.clone(),
        },
        MachineContractFact::DecisionInterval {
            interval: CandleInterval::H1,
        },
        MachineContractFact::CollectionCadence {
            interval: CandleInterval::H1,
            cadence_seconds: 300,
        },
    ];
    let mut input = MachineContractComparisonInput {
        approval_evidence_origin: MachineContractApprovalOrigin::CallerDeclared,
        expected: expected.clone(),
        observed: expected[..2].to_vec(),
    };
    let partial = compare_machine_contracts(&input)?;
    assert_eq!(partial.status, ReviewAuditStatus::InsufficientEvidence);
    assert_eq!(
        partial.approval_evidence_origin,
        Some(MachineContractApprovalOrigin::CallerDeclared)
    );
    assert!(
        partial
            .diagnostics
            .iter()
            .any(|item| item.path == "collection_cadence")
    );

    input.observed.push(expected[2].clone());
    let verified = compare_machine_contracts(&input)?;
    assert_eq!(verified.status, ReviewAuditStatus::Verified);
    assert_eq!(verified.kind, ReviewAuditKind::MachineContractComparison);
    verified.validate_digest()?;

    let mut contradiction = input;
    contradiction.observed[2] = MachineContractFact::CollectionCadence {
        interval: CandleInterval::H1,
        cadence_seconds: 301,
    };
    assert_eq!(
        compare_machine_contracts(&contradiction)?.status,
        ReviewAuditStatus::Contradiction
    );
    Ok(())
}

#[test]
fn adapter_can_return_typed_insufficient_without_placeholder_domain_values() -> Result<(), LabError>
{
    let report = insufficient_evidence_report(
        ReviewAuditKind::SourceIndicatorAudit,
        ReviewAuditBasis::RecomputedFromRetainedSourceBars,
        ContentHash::of_bytes(b"missing saved source"),
        Vec::new(),
        vec![crate::contracts::ReviewAuditDiagnostic {
            code: ReviewAuditDiagnosticCode::MissingSourceSeries,
            path: "signal.source_bar_ids".into(),
        }],
    )?;
    assert_eq!(report.status, ReviewAuditStatus::InsufficientEvidence);
    report.validate_digest()?;
    Ok(())
}

struct SerializeErrorAfterOversizedElement<'a>(&'a str);

impl serde::Serialize for SerializeErrorAfterOversizedElement<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(1))?;
        serde::ser::SerializeSeq::serialize_element(&mut sequence, self.0)?;
        Err(<S::Error as serde::ser::Error>::custom(
            "intentional failure after oversized element",
        ))
    }
}

#[test]
fn serialized_bound_is_strict_and_preserves_serialization_error_precedence() {
    let exact = "x".repeat(super::common::MAX_AUDIT_BYTES - 2);
    assert!(super::common::check_serialized_bound(&exact).is_ok());
    drop(exact);

    let oversized = "x".repeat(super::common::MAX_AUDIT_BYTES - 1);
    assert!(matches!(
        super::common::check_serialized_bound(&oversized),
        Err(LabError::ResourceLimit(message))
            if message == "review audit supplied evidence exceeds byte bound"
    ));
    drop(oversized);

    let escaped_pattern = "é\"\\\n\u{0001}";
    let escaped_pattern_bytes = serde_json::to_vec(escaped_pattern)
        .expect("serialize escaped pattern")
        .len()
        - 2;
    let escaped_repetitions = (super::common::MAX_AUDIT_BYTES - 2) / escaped_pattern_bytes;
    let below_limit = escaped_pattern.repeat(escaped_repetitions);
    assert!(super::common::check_serialized_bound(&below_limit).is_ok());
    drop(below_limit);
    let above_limit = escaped_pattern.repeat(escaped_repetitions + 1);
    assert!(matches!(
        super::common::check_serialized_bound(&above_limit),
        Err(LabError::ResourceLimit(message))
            if message == "review audit supplied evidence exceeds byte bound"
    ));
    drop(above_limit);

    let oversized_before_error = "x".repeat(super::common::MAX_AUDIT_BYTES + 1);
    assert!(matches!(
        super::common::check_serialized_bound(&SerializeErrorAfterOversizedElement(
            &oversized_before_error,
        )),
        Err(LabError::ContractParse(message))
            if message.contains("intentional failure after oversized element")
    ));
}
