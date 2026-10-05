use super::accounting::{Account, aggregate_identity_within_tolerance};
use super::execution::{
    plan_market_arrival, plan_market_request, plan_passive_arrival, plan_passive_buy_request,
};
use super::run_model;
use crate::contracts::policy::{
    BoolExpr, CompareOp, FrozenPolicyRevision, NumericExpr, PolicyDefinition, PolicyIndicator,
    PolicyIndicatorKind, PolicyOrigin, PolicyProgram, PolicyRevisionRef, PolicyRule,
    PolicySignalExpiry, PolicyTarget, RulesProgram,
};
use crate::contracts::{
    AdmissionStatus, AssetQuantity, BasisPoints, CandleInterval, CandleObservation, CandleRecord,
    CausalExecutionPolicy, CollectRequest, ContentHash, CostPolicy, DatasetId, DatasetManifest,
    DatasetSnapshot, DatasetStatus, DynamicCostKind, DynamicCostModel, EvidenceId, EvidenceImport,
    EvidenceProvenance, EvidencePurpose, EvidenceRevisionId, EvidenceUnavailablePolicy,
    EvidenceVersion, ExecutionPolicy, HistoricalFeeSchedule, MarkKind, MarketDataOrigin, MarketId,
    MarketRuleSnapshot, ModelAdmission, ModelId, ModelStatus, OrderStatus, PitPolicy, PlanId,
    PriceKrw, QuoteAmount, ReasonCode, ReportClock, RequestId, ResolvedPlan, RuleProvenance,
    RuleSnapshotId, RunId, SCHEMA_VERSION, Side, StateParameters, StrategyKind, StrategySpec,
    TerminalPolicy, TickBand, UtcRange, UtcTimestamp, Weight, experiment_config_digest,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().expect("fixture decimal must be exact")
}

fn price(value: &str) -> PriceKrw {
    PriceKrw::new(decimal(value)).expect("fixture price must be valid")
}

fn amount(value: &str) -> QuoteAmount {
    QuoteAmount::new(decimal(value)).expect("fixture amount must be valid")
}

fn qty(value: &str) -> AssetQuantity {
    AssetQuantity::new(decimal(value)).expect("fixture quantity must be valid")
}

#[test]
fn hand_calculated_round_trip_reconciles_2000_to_2077_90() {
    let mut account = Account::new(amount("2000"));
    let buy_fee = amount("1.01");
    let buy_reservation = amount("1011.01");
    account
        .reserve(buy_reservation)
        .expect("buy cash is available");
    account
        .apply_fill(Side::Buy, price("101"), qty("10"), buy_fee, buy_reservation)
        .expect("buy applies");
    let after_buy = account.state().expect("account state is valid");
    assert_eq!(after_buy.cash_total.get(), decimal("988.99"));
    assert_eq!(after_buy.qty.get(), decimal("10"));
    assert_eq!(after_buy.price_basis.get(), decimal("1010"));
    assert_eq!(after_buy.cumulative_fees.get(), decimal("1.01"));

    let sell = account
        .apply_fill(
            Side::Sell,
            price("109"),
            qty("10"),
            amount("1.09"),
            amount("0"),
        )
        .expect("sell applies");
    assert_eq!(sell.removed_basis, decimal("1010"));
    assert_eq!(sell.realized_price_pnl, decimal("80"));
    let final_state = account.state().expect("account state is valid");
    assert_eq!(final_state.cash_total.get(), decimal("2077.90"));
    assert_eq!(final_state.gross_realized.get(), decimal("80"));
    assert_eq!(final_state.cumulative_fees.get(), decimal("2.10"));
    assert_eq!(
        account.mark(price("109")).expect("mark reconciles").equity,
        decimal("2077.90")
    );
}

#[test]
fn aggregate_pnl_identities_accept_only_declared_subquantum_residual() {
    let actual = decimal("223298.596484170000");
    let measured_expected = decimal("223298.5964841700000000000005");
    assert!(
        aggregate_identity_within_tolerance(actual, measured_expected)
            .expect("measured residual is representable")
    );
    assert!(
        !aggregate_identity_within_tolerance(actual, actual + decimal("0.00000002"))
            .expect("material residual is representable")
    );
}

#[test]
fn reservations_prevent_double_spending_and_release_exactly() {
    let mut account = Account::new(amount("1000"));
    account
        .reserve(amount("600"))
        .expect("first reservation fits");
    assert_eq!(
        account.cash_free().expect("free cash is valid"),
        decimal("400")
    );
    assert!(account.reserve(amount("500")).is_err());
    account
        .release(amount("600"))
        .expect("reservation releases");
    assert_eq!(
        account.cash_free().expect("free cash is valid"),
        decimal("1000")
    );
}

#[test]
fn partial_sell_removes_moving_average_basis_without_fees_in_basis() {
    let mut account = Account::new(amount("5000"));
    for (fill_price, reservation) in [("100", "1000"), ("200", "2000")] {
        let reserved = amount(reservation);
        account.reserve(reserved).expect("buy reservation fits");
        account
            .apply_fill(
                Side::Buy,
                price(fill_price),
                qty("10"),
                amount("0"),
                reserved,
            )
            .expect("buy applies");
    }
    let sale = account
        .apply_fill(Side::Sell, price("180"), qty("5"), amount("0"), amount("0"))
        .expect("partial sell applies");
    assert_eq!(sale.removed_basis, decimal("750"));
    assert_eq!(sale.realized_price_pnl, decimal("150"));
    let state = account.state().expect("account state is valid");
    assert_eq!(state.qty.get(), decimal("15"));
    assert_eq!(state.price_basis.get(), decimal("2250"));
    assert_eq!(state.gross_realized.get(), decimal("150"));
}

#[test]
fn market_sizing_keeps_post_fee_cash_and_applies_price_cost_once() {
    let account = Account::new(amount("1000"));
    let costs = CostPolicy {
        buy_fee_bps: BasisPoints::new(decimal("10")).expect("valid bps"),
        sell_fee_bps: BasisPoints::new(decimal("10")).expect("valid bps"),
        maker_fee_bps: BasisPoints::new(decimal("5")).expect("valid bps"),
        half_spread_bps: BasisPoints::new(decimal("5")).expect("valid bps"),
        slippage_bps: BasisPoints::new(decimal("5")).expect("valid bps"),
        impact_bps: BasisPoints::new(decimal("0")).expect("valid bps"),
        assumption_label: "synthetic fixture".into(),
        dynamic: None,
    };
    let market_rules = rules("0.01", "0.1", "1");
    let decision = plan_market_request(
        &account,
        Weight::new(Decimal::ONE).expect("valid weight"),
        price("100"),
        &costs,
        &market_rules,
    )
    .expect("sizing succeeds");
    let fill = decision.fill.expect("100 percent target creates fill");
    assert_eq!(fill.price.get(), decimal("101"));
    assert_eq!(fill.price_cost_attribution, decimal("1"));
    assert_eq!(fill.qty.get(), decimal("9.8"));
    assert_eq!(fill.notional.get(), decimal("989.8"));
    assert_eq!(fill.fee.get(), decimal("0.9898"));
    assert_eq!(fill.reserved_cash.get(), decimal("990.7898"));
    assert!(fill.reserved_cash.get() <= decimal("1000"));
}

#[test]
fn market_sizing_floors_quantity_to_prior_volume_cap_and_blocks_min_notional() {
    let account = Account::new(amount("1000"));
    let costs = zero_costs();
    let market_rules = rules("10", "0.1", "1");
    let request = plan_market_request(
        &account,
        Weight::new(Decimal::ONE).expect("valid weight"),
        price("10"),
        &costs,
        &market_rules,
    )
    .expect("request sizing succeeds")
    .fill
    .expect("request is valid");
    let capped = plan_market_arrival(
        &account,
        &request,
        price("10"),
        &costs,
        &market_rules,
        qty("3.09"),
        Weight::new(decimal("0.5")).expect("valid cap"),
    )
    .expect("sizing succeeds")
    .fill
    .expect("cap still clears minimum");
    assert_eq!(capped.qty.get(), decimal("1.5"));

    let blocked_rules = rules("100", "0.1", "1");
    let blocked = plan_market_arrival(
        &account,
        &request,
        price("10"),
        &costs,
        &blocked_rules,
        qty("3.09"),
        Weight::new(decimal("0.5")).expect("valid cap"),
    )
    .expect("rule block is an economic outcome");
    assert!(blocked.fill.is_none());
}

#[test]
fn buy_and_hold_runs_from_cash_at_next_eligible_open_with_bounded_unique_events() {
    let (plan, dataset, admission, run_id, evaluation_start) = buy_and_hold_fixture();
    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 100, &|| false)
        .expect("fixture model executes");

    assert_eq!(ledger.status, ModelStatus::Completed);
    assert_eq!(ledger.fills.len(), 1);
    assert_eq!(ledger.orders.len(), 1);
    assert_eq!(ledger.order_events.len(), 2);
    assert_eq!(
        ledger.order_events[0].status,
        crate::contracts::OrderStatus::Created
    );
    assert_eq!(
        ledger.order_events[1].status,
        crate::contracts::OrderStatus::Filled
    );
    assert_eq!(ledger.orders[0].order_id, ledger.order_events[1].order_id);
    assert_eq!(
        ledger.orders[0].context.event_seq,
        ledger.order_events[1].context.event_seq
    );
    assert_eq!(
        ledger.orders[0].status,
        crate::contracts::OrderStatus::Filled
    );
    assert_eq!(ledger.fills[0].side, Side::Buy);
    assert_eq!(ledger.fills[0].price.get(), decimal("100"));
    assert_eq!(ledger.fills[0].qty.get(), decimal("10"));
    assert_eq!(
        match ledger.fills[0].timing {
            crate::contracts::FillTiming::Exact { at } => at,
            crate::contracts::FillTiming::Interval { .. } => panic!("market fixture is exact"),
        },
        evaluation_start
    );
    assert_eq!(ledger.episodes.len(), 1);
    assert_eq!(
        ledger.episodes[0].status,
        crate::contracts::EpisodeStatus::Open
    );
    assert_eq!(ledger.episodes[0].marked_unrealized.get(), decimal("100"));
    assert!(
        ledger
            .signals
            .iter()
            .any(|signal| signal.outcome == crate::contracts::SignalOutcome::NoAction)
    );
    assert_eq!(
        ledger
            .account_marks
            .last()
            .expect("terminal mark exists")
            .equity
            .get(),
        decimal("1100")
    );

    let mut sequences = ledger
        .signals
        .iter()
        .map(|fact| fact.context.event_seq)
        .chain(
            ledger
                .order_events
                .iter()
                .map(|fact| fact.context.event_seq),
        )
        .chain(ledger.fills.iter().map(|fact| fact.context.event_seq))
        .chain(
            ledger
                .account_marks
                .iter()
                .map(|fact| fact.context.event_seq),
        )
        .collect::<Vec<_>>();
    let fact_count = sequences.len();
    sequences.sort_unstable();
    sequences.dedup();
    assert_eq!(sequences.len(), fact_count);
    assert_eq!(sequences.first().copied(), Some(100));
    assert_eq!(sequences.last().copied(), Some(ledger.last_event_seq));
}

#[test]
fn s1_warmup_state_enters_from_cash_without_phantom_inventory() {
    let (mut plan, mut dataset, mut admission, run_id, evaluation_start) = buy_and_hold_fixture();
    admission.strategy = StrategyKind::S1;
    plan.spec.strategies = vec![StrategySpec::S1 {
        state: StateParameters {
            ema_length: 2,
            vol_length: 2,
            k: 0.0,
        },
    }];
    plan.admissions = vec![admission.clone()];
    dataset.observations = vec![
        observation("warmup-1", "2025-01-01T21:00:00Z", "100", "100"),
        observation("warmup-2", "2025-01-01T22:00:00Z", "100", "100"),
        observation("warmup-3", "2025-01-01T23:00:00Z", "100", "110"),
        observation("s1-first", "2025-01-02T00:00:00Z", "110", "111"),
        observation("s1-second", "2025-01-02T01:00:00Z", "111", "112"),
    ];
    dataset.manifest.request.warmup_bars = 3;
    dataset.manifest.coverage =
        UtcRange::new(time("2025-01-01T21:00:00Z"), time("2025-01-02T02:00:00Z"))
            .expect("fixture coverage");
    dataset.manifest.row_count = dataset.observations.len() as u64;
    let digest = ContentHash::of_bytes(b"s1-fixture");
    dataset.manifest.semantic_digest = digest.clone();
    plan.dataset_digests[0].1 = digest;

    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 500, &|| false)
        .expect("S1 fixture executes");
    assert_eq!(ledger.fills.len(), 1);
    assert_eq!(ledger.fills[0].price.get(), decimal("110"));
    assert_eq!(ledger.fills[0].qty.get(), decimal("9"));
    assert_eq!(
        match ledger.fills[0].timing {
            crate::contracts::FillTiming::Exact { at } => at,
            crate::contracts::FillTiming::Interval { .. } => panic!("S1 market fixture is exact"),
        },
        evaluation_start
    );
    assert_eq!(ledger.episodes.len(), 1);
    assert_eq!(
        ledger
            .account_marks
            .last()
            .expect("terminal mark exists")
            .equity
            .get(),
        decimal("1018")
    );
}

#[test]
fn frozen_custom_rule_policy_runs_and_records_exact_trace() {
    let (mut plan, dataset, mut admission, run_id, _) = buy_and_hold_fixture();
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "always_long".into(),
        description: "SYNTHETIC_TEST_ONLY".into(),
        program: PolicyProgram::Rules {
            program: RulesProgram {
                indicators: vec![PolicyIndicator {
                    id: "close".into(),
                    indicator: PolicyIndicatorKind::Close,
                }],
                states: Vec::new(),
                rules: vec![PolicyRule {
                    id: "positive_close".into(),
                    condition: BoolExpr::Compare {
                        comparison: CompareOp::Gt,
                        left: NumericExpr::Indicator { id: "close".into() },
                        right: NumericExpr::Constant { value: 0.0 },
                    },
                    target: PolicyTarget::Weight {
                        value: NumericExpr::Constant { value: 1.0 },
                    },
                }],
                fallback: PolicyTarget::Weight {
                    value: NumericExpr::Constant { value: 0.0 },
                },
                signal_expiry: PolicySignalExpiry::DecisionBars { bars: 1 },
            },
        },
    };
    let reference = PolicyRevisionRef {
        policy_id: crate::contracts::PolicyId::new("policy-custom").expect("policy id"),
        revision_id: crate::contracts::PolicyRevisionId::new("policy-revision-custom")
            .expect("revision id"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    };
    plan.spec.schema_version = "2.0".into();
    plan.spec.strategies.clear();
    plan.spec.policy_selections = vec![reference.clone()];
    plan.policy_revisions = vec![FrozenPolicyRevision {
        reference: reference.clone(),
        revision_number: 1,
        parent_revision_id: None,
        family: StrategyKind::Other,
        origin: PolicyOrigin::User,
        definition,
    }];
    admission.strategy = StrategyKind::Other;
    admission.policy_ref = Some(reference.clone());
    plan.admissions = vec![admission.clone()];

    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 750, &|| false)
        .expect("custom policy executes from frozen definition");
    assert_eq!(ledger.fills.len(), 1);
    let signal = ledger.signals.first().expect("custom signal");
    assert_eq!(signal.policy_ref.as_ref(), Some(&reference));
    assert_eq!(
        signal
            .policy_trace
            .as_ref()
            .and_then(|trace| trace.matched_rule_id.as_deref()),
        Some("positive_close")
    );
    assert_eq!(signal.indicators.policy_values[0].id, "close");
}

#[test]
fn causal_execution_ignores_a_gapped_future_suffix() {
    let (plan, baseline, admission, run_id, _) = causal_s1_fixture();
    let expected = run_model(
        &plan,
        std::slice::from_ref(&baseline),
        None,
        &run_id,
        &admission,
        820,
        &|| false,
    )
    .expect("bounded causal fixture executes");
    let mut with_future_gap = baseline;
    with_future_gap.observations.push(observation(
        "irrelevant-future",
        "2025-01-02T04:00:00Z",
        "999999",
        "1",
    ));
    refresh_dataset_identity(&mut with_future_gap, b"causal-future-gap");
    let mut future_plan = plan;
    future_plan.dataset_digests[0].1 = with_future_gap.manifest.semantic_digest.clone();

    let actual = run_model(
        &future_plan,
        &[with_future_gap],
        None,
        &run_id,
        &admission,
        820,
        &|| false,
    )
    .expect("observations after the causal range cannot invalidate training");
    assert_eq!(
        serde_json::to_value(actual).expect("actual ledger serializes"),
        serde_json::to_value(expected).expect("expected ledger serializes")
    );
}

#[test]
fn legacy_v1_and_v2_keep_whole_vector_continuity_validation() {
    let (mut v1_plan, mut v1_dataset, v1_admission, v1_run_id, _) = buy_and_hold_fixture();
    v1_dataset.observations.push(observation(
        "legacy-v1-future-gap",
        "2025-01-02T04:00:00Z",
        "999999",
        "1",
    ));
    refresh_dataset_identity(&mut v1_dataset, b"legacy-v1-future-gap");
    v1_plan.dataset_digests[0].1 = v1_dataset.manifest.semantic_digest.clone();
    assert!(matches!(
        run_model(
            &v1_plan,
            &[v1_dataset],
            None,
            &v1_run_id,
            &v1_admission,
            830,
            &|| false,
        ),
        Err(crate::contracts::LabError::DataGap(_))
    ));

    let (mut v2_plan, mut v2_dataset, v2_admission, v2_run_id, _) = causal_s1_fixture();
    v2_plan.spec.schema_version = "2.0".into();
    v2_plan.spec.causal_execution = None;
    v2_dataset.observations.push(observation(
        "legacy-v2-future-gap",
        "2025-01-02T04:00:00Z",
        "999999",
        "1",
    ));
    refresh_dataset_identity(&mut v2_dataset, b"legacy-v2-future-gap");
    v2_plan.dataset_digests[0].1 = v2_dataset.manifest.semantic_digest.clone();
    assert!(matches!(
        run_model(
            &v2_plan,
            &[v2_dataset],
            None,
            &v2_run_id,
            &v2_admission,
            830,
            &|| false,
        ),
        Err(crate::contracts::LabError::DataGap(_))
    ));
}

#[test]
fn causal_execution_ignores_history_before_declared_policy_warmup() {
    let (plan, baseline, admission, run_id, _) = causal_s1_fixture();
    let mut altered_past = baseline.clone();
    altered_past.observations[0] =
        observation("past-too-old", "2025-01-01T20:00:00Z", "1000000", "1000000");
    refresh_dataset_identity(&mut altered_past, b"causal-altered-past");
    let mut altered_plan = plan.clone();
    altered_plan.dataset_digests[0].1 = altered_past.manifest.semantic_digest.clone();

    let expected = run_model(&plan, &[baseline], None, &run_id, &admission, 840, &|| {
        false
    })
    .expect("baseline causal fixture executes");
    let actual = run_model(
        &altered_plan,
        &[altered_past],
        None,
        &run_id,
        &admission,
        840,
        &|| false,
    )
    .expect("past outside declared warmup remains irrelevant");
    assert_eq!(
        serde_json::to_value(actual).expect("actual ledger serializes"),
        serde_json::to_value(expected).expect("expected ledger serializes")
    );
}

#[test]
fn causal_execution_requires_the_declared_warmup_start_boundary() {
    let (mut plan, mut dataset, admission, run_id, _) = causal_s1_fixture();
    dataset
        .observations
        .retain(|observation| observation.id.as_str() != "warmup-1");
    refresh_dataset_identity(&mut dataset, b"causal-missing-warmup-start");
    plan.dataset_digests[0].1 = dataset.manifest.semantic_digest.clone();

    assert!(matches!(
        run_model(&plan, &[dataset], None, &run_id, &admission, 850, &|| false),
        Err(crate::contracts::LabError::InsufficientWarmup(_))
    ));
}

#[test]
fn causal_execution_ignores_evidence_available_only_after_the_range() {
    let (mut plan, dataset, mut admission, run_id, _) = causal_s1_fixture();
    install_causal_policy(
        &mut plan,
        &mut admission,
        &StrategySpec::S5 {
            state: StateParameters {
                ema_length: 2,
                vol_length: 2,
                k: 0.0,
            },
        },
        "s5",
    );
    let base_version = evidence_version(
        "evidence-base",
        "evidence-event",
        "2025-01-01T23:00:00Z",
        "1",
        None,
    );
    let base = crate::evidence::build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions: vec![base_version.clone()],
    })
    .expect("base evidence snapshot");
    let with_future = crate::evidence::build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions: vec![
            base_version,
            evidence_version(
                "evidence-future",
                "evidence-event",
                "2025-01-02T04:00:00Z",
                "0.1",
                Some("evidence-base"),
            ),
        ],
    })
    .expect("future evidence snapshot");

    let mut base_plan = plan.clone();
    base_plan.evidence_digest = Some(base.digest.clone());
    base_plan.spec.evidence_snapshot_id = Some(base.id.clone());
    let mut future_plan = plan;
    future_plan.evidence_digest = Some(with_future.digest.clone());
    future_plan.spec.evidence_snapshot_id = Some(with_future.id.clone());
    let base_ledger = run_model(
        &base_plan,
        std::slice::from_ref(&dataset),
        Some(&base),
        &run_id,
        &admission,
        860,
        &|| false,
    )
    .expect("base evidence executes");
    let future_ledger = run_model(
        &future_plan,
        &[dataset],
        Some(&with_future),
        &run_id,
        &admission,
        860,
        &|| false,
    )
    .expect("future evidence suffix executes");
    assert_eq!(base_ledger.signals.len(), future_ledger.signals.len());
    for (base_signal, future_signal) in base_ledger.signals.iter().zip(&future_ledger.signals) {
        assert_eq!(
            base_signal.constrained_target_weight,
            future_signal.constrained_target_weight
        );
        assert_eq!(base_signal.reasons, future_signal.reasons);
        let eligible_revisions = |signal: &crate::contracts::SignalRecord| {
            signal
                .evidence_effect
                .as_ref()
                .expect("S5 records evidence")
                .eligible
                .iter()
                .map(|item| item.revision_id.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            eligible_revisions(base_signal),
            eligible_revisions(future_signal)
        );
    }
}

#[test]
fn causal_schema_is_exact_and_legacy_serialization_stays_omitted() {
    let (mut v1_plan, v1_dataset, v1_admission, v1_run_id, _) = buy_and_hold_fixture();
    let v1_bytes = serde_json::to_vec(&v1_plan.spec).expect("v1 spec serializes");
    assert!(!String::from_utf8_lossy(&v1_bytes).contains("causal_execution"));
    let v1_digest = experiment_config_digest(&v1_plan.spec, &[]).expect("v1 digest");
    let v1_replay = run_model(
        &v1_plan,
        &[v1_dataset],
        None,
        &v1_run_id,
        &v1_admission,
        890,
        &|| false,
    )
    .expect("v1 replay executes");

    v1_plan.spec.causal_execution = Some(CausalExecutionPolicy::DeclaredPolicyWarmup);
    assert!(v1_plan.spec.validate().is_err());

    let (mut v2_plan, v2_dataset, v2_admission_exact, v2_run_id, _) = causal_s1_fixture();
    let mut v2_admission = v2_admission_exact.clone();
    v2_plan.spec.schema_version = "2.0".into();
    v2_plan.spec.causal_execution = None;
    assert!(v2_plan.spec.validate().is_ok());
    assert!(
        !serde_json::to_string(&v2_plan.spec)
            .expect("v2 spec serializes")
            .contains("causal_execution")
    );
    let v2_digest =
        experiment_config_digest(&v2_plan.spec, &v2_plan.policy_revisions).expect("v2 digest");
    let v2_replay = run_model(
        &v2_plan,
        &[v2_dataset],
        None,
        &v2_run_id,
        &v2_admission_exact,
        900,
        &|| false,
    )
    .expect("v2 replay executes");
    assert_eq!(
        ContentHash::of_value(&v1_replay)
            .expect("v1 replay digest")
            .to_string(),
        "2b2ce4fec6467c186dcb60e2ee09fedb85f785a2508d16595d445b2a3dcb6e4a"
    );
    assert_eq!(
        ContentHash::of_value(&v2_replay)
            .expect("v2 replay digest")
            .to_string(),
        "b1f531071dab53900a3fe281f1189996c8e67628b785c1ee40ef72e331e3fdcd"
    );

    v2_plan.spec.schema_version = "3.0".into();
    assert!(v2_plan.spec.validate().is_err());
    v2_plan.spec.causal_execution = Some(CausalExecutionPolicy::DeclaredPolicyWarmup);
    assert!(v2_plan.spec.validate().is_ok());
    assert!(experiment_config_digest(&v2_plan.spec, &v2_plan.policy_revisions).is_ok());
    assert!(experiment_config_digest(&v2_plan.spec, &[]).is_err());
    v2_plan.spec.pit_policy = PitPolicy::LatestVersionProxy;
    assert!(v2_plan.spec.validate().is_err());
    v2_plan.spec.pit_policy = PitPolicy::StrictPit;
    v2_admission.policy_ref = None;
    assert!(crate::contracts::strategy_binding(&v2_plan, &v2_admission).is_err());

    let mut unknown = serde_json::to_value(&v2_plan.spec).expect("v3 spec serializes");
    unknown["causal_execution"] = serde_json::Value::String("UNKNOWN".into());
    assert!(serde_json::from_value::<crate::contracts::ExperimentSpec>(unknown).is_err());

    assert_eq!(v1_digest, ContentHash::of_bytes(&v1_bytes));
    assert_eq!(
        v1_digest.to_string(),
        "f8e9c7b4a02a489414a4ce5dc3970e9473409fb2176fca20d3ba5051aa398ffa"
    );
    assert_eq!(
        v2_digest.to_string(),
        "3f202d94fd13b0f53b56eeac411406504a178bbde2300fbfcce26e55218846cd"
    );
}

#[test]
fn causal_passive_execution_needs_only_one_pre_range_execution_bar() {
    let (mut plan, mut dataset, mut admission, run_id, _) = causal_s1_fixture();
    install_causal_policy(
        &mut plan,
        &mut admission,
        &StrategySpec::BuyAndHold,
        "passive-buy-hold",
    );
    plan.spec.execution = ExecutionPolicy::PassiveBuy {
        offset_bps: BasisPoints::new(Decimal::ZERO).expect("offset"),
        penetration_ticks: 0,
        fill_fraction: crate::contracts::PassiveFraction::Half,
        ttl_execution_bars: 1,
        participation_cap: Weight::new(Decimal::ONE).expect("cap"),
    };
    dataset.observations = vec![
        observation("prior", "2025-01-01T23:00:00Z", "100", "100"),
        observation("first", "2025-01-02T00:00:00Z", "101", "101"),
        observation("second", "2025-01-02T01:00:00Z", "101", "110"),
    ];
    dataset.observations[1].candle.low = price("99");
    dataset.manifest.coverage =
        UtcRange::new(time("2025-01-01T23:00:00Z"), time("2025-01-02T02:00:00Z"))
            .expect("fixture coverage");
    dataset.manifest.request.warmup_bars = 1;
    refresh_dataset_identity(&mut dataset, b"causal-passive-one-prior");
    plan.dataset_digests[0].1 = dataset.manifest.semantic_digest.clone();

    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 880, &|| false)
        .expect("one pre-range execution bar supports passive prior-two-bar indexing");
    assert_eq!(ledger.fills.len(), 1);
    assert!(matches!(
        ledger.fills[0].timing,
        crate::contracts::FillTiming::Interval { .. }
    ));
}

#[test]
fn rejected_order_keeps_created_and_rejected_events_with_one_final_projection() {
    let (mut plan, dataset, admission, run_id, _) = buy_and_hold_fixture();
    plan.spec.market_rules.min_notional = amount("2000");
    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 900, &|| false)
        .expect("rule rejection is a completed economic outcome");
    assert!(ledger.fills.is_empty());
    assert_eq!(ledger.order_events.len(), ledger.orders.len() * 2);
    for (projection, events) in ledger
        .orders
        .iter()
        .zip(ledger.order_events.as_chunks::<2>().0.iter())
    {
        assert_eq!(events[0].status, crate::contracts::OrderStatus::Created);
        assert_eq!(events[1].status, crate::contracts::OrderStatus::Rejected);
        assert_eq!(projection.status, crate::contracts::OrderStatus::Rejected);
        assert_eq!(projection.order_id, events[1].order_id);
        assert_eq!(projection.context.event_seq, events[1].context.event_seq);
        assert!(events[0].context.event_seq < events[1].context.event_seq);
    }
}

#[test]
fn replay_graph_and_economics_are_stable_across_run_occurrences() {
    let (plan, dataset, admission, first_run, _) = buy_and_hold_fixture();
    let second_run = RunId::new("run-bh-retry").expect("fixture retry run id");
    let first = run_model(
        &plan,
        std::slice::from_ref(&dataset),
        None,
        &first_run,
        &admission,
        1200,
        &|| false,
    )
    .expect("first occurrence executes");
    let second = run_model(
        &plan,
        &[dataset],
        None,
        &second_run,
        &admission,
        1200,
        &|| false,
    )
    .expect("retry occurrence executes");

    assert_ne!(
        first.signals[0].context.run_id,
        second.signals[0].context.run_id
    );
    assert_eq!(first.signals[0].signal_id, second.signals[0].signal_id);
    assert_eq!(first.orders[0].order_id, second.orders[0].order_id);
    assert_eq!(first.fills[0].fill_id, second.fills[0].fill_id);
    assert_eq!(first.episodes[0].episode_id, second.episodes[0].episode_id);

    let mut first_graph = serde_json::to_value(first).expect("ledger serializes");
    let mut second_graph = serde_json::to_value(second).expect("ledger serializes");
    remove_occurrence_run_ids(&mut first_graph);
    remove_occurrence_run_ids(&mut second_graph);
    assert_eq!(first_graph, second_graph);
}

#[test]
fn delayed_fill_at_shared_time_precedes_the_next_decision() {
    let (mut plan, dataset, admission, run_id, _) = buy_and_hold_fixture();
    plan.spec.latency_ms = 3_600_000;
    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 1600, &|| {
        false
    })
    .expect("delayed fixture executes");
    let fill = ledger.fills.first().expect("delayed fill exists");
    let same_time_later_signal = ledger
        .signals
        .iter()
        .find(|signal| {
            signal.context.accounting_event_time == fill.context.accounting_event_time
                && signal.signal_id != ledger.order_events[0].parent_signal_id
        })
        .expect("next decision shares the fill timestamp");
    assert!(fill.context.event_seq < same_time_later_signal.context.event_seq);
    assert_eq!(
        same_time_later_signal.state_before,
        crate::contracts::PositionState::Long
    );
    assert_eq!(
        ledger.order_events[0].effective_at,
        fill.context.accounting_event_time
    );
}

#[test]
fn arrival_volume_clip_emits_partial_fill_then_cancels_remainder() {
    let (mut plan, mut dataset, admission, run_id, _) = buy_and_hold_fixture();
    let ExecutionPolicy::NextBarOpen { participation_cap } = &mut plan.spec.execution else {
        panic!("fixture uses next-bar-open");
    };
    *participation_cap = Weight::new(decimal("0.5")).expect("fixture cap");
    dataset.observations[0].candle.volume = qty("5");
    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 1900, &|| {
        false
    })
    .expect("partial fixture executes");
    assert_eq!(ledger.fills[0].qty.get(), decimal("2.5"));
    let history = ledger
        .order_events
        .iter()
        .filter(|event| event.order_id == ledger.orders[0].order_id)
        .collect::<Vec<_>>();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].status, crate::contracts::OrderStatus::Created);
    assert_eq!(
        history[1].status,
        crate::contracts::OrderStatus::PartiallyFilled
    );
    assert_eq!(history[2].status, crate::contracts::OrderStatus::Cancelled);
    assert_eq!(ledger.orders[0].cumulative_filled_qty.get(), decimal("2.5"));
    let fill = &ledger.fills[0];
    let after_fill = ledger
        .account_marks
        .iter()
        .find(|mark| mark.context.event_seq == fill.accounting_mark_seq)
        .expect("partial fill has its exact after-fill mark");
    let fill_debit = fill
        .notional
        .get()
        .checked_add(fill.fee.get())
        .expect("fixture debit is exact");
    let expected_remainder = history[0]
        .reserved_cash
        .get()
        .checked_sub(fill_debit)
        .expect("partial debit does not exceed reservation");
    assert_eq!(after_fill.state.cash_reserved.get(), expected_remainder);
    assert_eq!(after_fill.state.cash_free.get(), Decimal::ZERO);
    let post_cancel = ledger
        .account_marks
        .iter()
        .find(|mark| mark.context.event_seq > history[2].context.event_seq)
        .expect("later execution-close mark observes cancellation release");
    assert_eq!(post_cancel.state.cash_reserved.get(), Decimal::ZERO);
}

#[test]
fn passive_cross_touch_and_strict_penetration_are_distinct() {
    let account = Account::new(amount("1000"));
    let market_rules = rules("10", "0.1", "1");
    let request = plan_passive_buy_request(
        &account,
        Weight::new(Decimal::ONE).expect("target"),
        price("100"),
        BasisPoints::new(Decimal::ZERO).expect("offset"),
        BasisPoints::new(Decimal::ZERO).expect("maker fee"),
        &market_rules,
    )
    .expect("passive request sizes")
    .fill
    .expect("passive request exists");
    let cap = Weight::new(Decimal::ONE).expect("cap");
    let crossing = plan_passive_arrival(
        &request,
        price("100"),
        price("90"),
        qty("100"),
        cap,
        crate::contracts::PassiveFraction::Half,
        0,
        &market_rules,
    )
    .expect("crossing is modeled outcome");
    assert!(crossing.fill.is_none());
    assert_eq!(
        crossing.reason,
        crate::contracts::ReasonCode::WouldTakeOrUnknown
    );
    let touch = plan_passive_arrival(
        &request,
        price("101"),
        price("100"),
        qty("100"),
        cap,
        crate::contracts::PassiveFraction::Half,
        0,
        &market_rules,
    )
    .expect("touch is modeled outcome");
    assert!(touch.fill.is_none());
    assert_eq!(
        touch.reason,
        crate::contracts::ReasonCode::PassiveNotPenetrated
    );
    let penetrated = plan_passive_arrival(
        &request,
        price("101"),
        price("99"),
        qty("100"),
        cap,
        crate::contracts::PassiveFraction::Half,
        0,
        &market_rules,
    )
    .expect("penetration evaluates")
    .fill
    .expect("strict penetration fills once");
    assert_eq!(penetrated.qty.get(), decimal("5"));
    assert_eq!(
        penetrated.reason,
        crate::contracts::ReasonCode::PassivePartial
    );
}

#[test]
fn passive_partial_accounts_at_bar_end_before_close_mark_and_next_decision() {
    let (mut plan, mut dataset, admission, run_id, _) = buy_and_hold_fixture();
    plan.spec.execution = ExecutionPolicy::PassiveBuy {
        offset_bps: BasisPoints::new(Decimal::ZERO).expect("offset"),
        penetration_ticks: 0,
        fill_fraction: crate::contracts::PassiveFraction::Half,
        ttl_execution_bars: 1,
        participation_cap: Weight::new(Decimal::ONE).expect("cap"),
    };
    dataset.observations[1].candle.open = price("101");
    dataset.observations[1].candle.high = price("102");
    dataset.observations[1].candle.low = price("99");
    dataset.observations[1].candle.close = price("101");
    let ledger = run_model(&plan, &[dataset], None, &run_id, &admission, 2200, &|| {
        false
    })
    .expect("passive fixture executes");
    let fill = ledger.fills.first().expect("passive fill exists");
    assert_eq!(fill.qty.get(), decimal("5"));
    assert!(matches!(
        fill.timing,
        crate::contracts::FillTiming::Interval { .. }
    ));
    let history = ledger
        .order_events
        .iter()
        .filter(|event| event.order_id == fill.order_id)
        .collect::<Vec<_>>();
    assert_eq!(history.len(), 3);
    assert_eq!(
        history[1].status,
        crate::contracts::OrderStatus::PartiallyFilled
    );
    assert_eq!(history[2].status, crate::contracts::OrderStatus::Cancelled);
    let after_fill = ledger
        .account_marks
        .iter()
        .find(|mark| mark.context.event_seq == fill.accounting_mark_seq)
        .expect("passive fill mark");
    let close_mark = ledger
        .account_marks
        .iter()
        .find(|mark| {
            mark.kind == crate::contracts::MarkKind::ExecutionClose
                && mark.context.accounting_event_time == fill.context.accounting_event_time
                && mark.context.event_seq > history[2].context.event_seq
        })
        .expect("execution close follows passive cancellation");
    let next_signal = ledger
        .signals
        .iter()
        .find(|signal| {
            signal.context.accounting_event_time == fill.context.accounting_event_time
                && signal.context.event_seq > close_mark.context.event_seq
        })
        .expect("same-time next decision follows close mark");
    assert!(fill.context.event_seq < after_fill.context.event_seq);
    assert!(after_fill.context.event_seq < history[2].context.event_seq);
    assert!(history[2].context.event_seq < close_mark.context.event_seq);
    assert!(close_mark.context.event_seq < next_signal.context.event_seq);
}

fn zero_costs() -> CostPolicy {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps is valid");
    CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "synthetic zero-cost fixture".into(),
        dynamic: None,
    }
}

fn rules(min_notional: &str, quantity_step: &str, tick: &str) -> MarketRuleSnapshot {
    let start = UtcTimestamp::parse_rfc3339("2025-01-01T00:00:00Z").expect("valid time");
    let end = UtcTimestamp::parse_rfc3339("2025-01-03T00:00:00Z").expect("valid time");
    MarketRuleSnapshot {
        id: RuleSnapshotId::new("rule-fixture").expect("valid id"),
        provenance: RuleProvenance::ExplicitScenario,
        valid_range: UtcRange::new(start, end).expect("valid range"),
        observed_at: start,
        source_refs: vec!["synthetic://fixture".into()],
        assumption_label: "synthetic fixture".into(),
        min_notional: amount(min_notional),
        quantity_step: qty(quantity_step),
        ticks: vec![TickBand {
            lower_bound: amount("0"),
            tick: price(tick),
        }],
        fee_schedule: None,
        trading_state: None,
        maintenance_windows: Vec::new(),
    }
}

fn buy_and_hold_fixture() -> (
    ResolvedPlan,
    DatasetSnapshot,
    ModelAdmission,
    RunId,
    UtcTimestamp,
) {
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let evaluation_start = time("2025-01-02T00:00:00Z");
    let evaluation_end = time("2025-01-02T02:00:00Z");
    let range = UtcRange::new(evaluation_start, evaluation_end).expect("fixture range");
    let dataset_id = DatasetId::new("dataset-bh").expect("fixture dataset id");
    let semantic_digest = ContentHash::of_bytes(b"buy-and-hold-fixture");
    let observations = vec![
        observation("prior", "2025-01-01T23:00:00Z", "100", "100"),
        observation("first", "2025-01-02T00:00:00Z", "100", "101"),
        observation("second", "2025-01-02T01:00:00Z", "101", "110"),
    ];
    let dataset = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: SCHEMA_VERSION.into(),
            id: dataset_id.clone(),
            request: CollectRequest {
                request_id: RequestId::new("request-bh").expect("fixture request id"),
                markets: vec![market.clone()],
                range,
                data_resolution: CandleInterval::H1,
                warmup_bars: 1,
                completed_only: true,
            },
            coverage: UtcRange::new(time("2025-01-01T23:00:00Z"), evaluation_end)
                .expect("fixture coverage"),
            status: DatasetStatus::Ready,
            row_count: observations.len() as u64,
            normalizer_version: "fixture-v1".into(),
            gap_policy: "FAIL_CLOSED".into(),
            semantic_digest: semantic_digest.clone(),
            provenance_digest: ContentHash::of_bytes(b"fixture-provenance"),
            origin: MarketDataOrigin::SyntheticTestOnly,
            raw_objects: Vec::new(),
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations,
    };
    let model_id = ModelId::new("model-bh").expect("fixture model id");
    let admission = ModelAdmission {
        model_id: model_id.clone(),
        market: market.clone(),
        strategy: StrategyKind::BuyAndHold,
        policy_ref: None,
        status: AdmissionStatus::Eligible,
        reasons: Vec::new(),
    };
    let plan = ResolvedPlan {
        id: PlanId::new("plan-bh").expect("fixture plan id"),
        spec: crate::contracts::ExperimentSpec {
            schema_version: SCHEMA_VERSION.into(),
            dataset_ids: vec![dataset_id.clone()],
            markets: vec![market],
            range,
            strategies: vec![StrategySpec::BuyAndHold],
            policy_selections: Vec::new(),
            causal_execution: None,
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            latency_ms: 0,
            initial_cash: amount("1000"),
            costs: zero_costs(),
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE).expect("fixture cap"),
            },
            market_rules: rules("1", "0.1", "1"),
            market_rules_history: Vec::new(),
            terminal_policy: TerminalPolicy::MarkToMarket,
            evidence_snapshot_id: None,
            pit_policy: PitPolicy::StrictPit,
            evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
            report_clock: ReportClock {
                timezone: "UTC".into(),
                min_annualization_days: 1,
                risk_free_annual: 0.0,
            },
            seed: 7,
        },
        config_digest: ContentHash::of_bytes(b"fixture-config"),
        input_digest: ContentHash::of_bytes(b"fixture-input"),
        dataset_digests: vec![(dataset_id, semantic_digest)],
        evidence_digest: None,
        admissions: vec![admission.clone()],
        policy_revisions: Vec::new(),
        warnings: Vec::new(),
        estimated_events: 32,
    };
    (
        plan,
        dataset,
        admission,
        RunId::new("run-bh").expect("fixture run id"),
        evaluation_start,
    )
}

fn causal_s1_fixture() -> (
    ResolvedPlan,
    DatasetSnapshot,
    ModelAdmission,
    RunId,
    UtcTimestamp,
) {
    let (mut plan, mut dataset, mut admission, run_id, evaluation_start) = buy_and_hold_fixture();
    let strategy = StrategySpec::S1 {
        state: StateParameters {
            ema_length: 2,
            vol_length: 2,
            k: 0.0,
        },
    };
    install_causal_policy(&mut plan, &mut admission, &strategy, "s1");
    dataset.observations = vec![
        observation("too-old", "2025-01-01T20:00:00Z", "1", "1"),
        observation("warmup-1", "2025-01-01T21:00:00Z", "100", "100"),
        observation("warmup-2", "2025-01-01T22:00:00Z", "100", "100"),
        observation("warmup-3", "2025-01-01T23:00:00Z", "100", "110"),
        observation("oos-1", "2025-01-02T00:00:00Z", "110", "111"),
        observation("oos-2", "2025-01-02T01:00:00Z", "111", "112"),
    ];
    dataset.manifest.request.warmup_bars = 4;
    dataset.manifest.coverage =
        UtcRange::new(time("2025-01-01T20:00:00Z"), time("2025-01-02T02:00:00Z"))
            .expect("fixture coverage");
    refresh_dataset_identity(&mut dataset, b"causal-s1");
    plan.dataset_digests[0].1 = dataset.manifest.semantic_digest.clone();
    (plan, dataset, admission, run_id, evaluation_start)
}

fn install_causal_policy(
    plan: &mut ResolvedPlan,
    admission: &mut ModelAdmission,
    strategy: &StrategySpec,
    suffix: &str,
) {
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: format!("causal_{suffix}"),
        description: "SYNTHETIC_TEST_ONLY".into(),
        program: PolicyProgram::Builtin {
            strategy: strategy.clone(),
        },
    };
    let reference = PolicyRevisionRef {
        policy_id: crate::contracts::PolicyId::new(format!("policy-causal-{suffix}"))
            .expect("policy id"),
        revision_id: crate::contracts::PolicyRevisionId::new(format!(
            "policy-revision-causal-{suffix}"
        ))
        .expect("revision id"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    };
    plan.spec.schema_version = "3.0".into();
    plan.spec.strategies.clear();
    plan.spec.policy_selections = vec![reference.clone()];
    plan.spec.causal_execution = Some(CausalExecutionPolicy::DeclaredPolicyWarmup);
    plan.policy_revisions = vec![FrozenPolicyRevision {
        reference: reference.clone(),
        revision_number: 1,
        parent_revision_id: None,
        family: strategy.kind(),
        origin: PolicyOrigin::Builtin,
        definition,
    }];
    admission.strategy = strategy.kind();
    admission.policy_ref = Some(reference);
    plan.admissions = vec![admission.clone()];
}

fn refresh_dataset_identity(dataset: &mut DatasetSnapshot, seed: &[u8]) {
    dataset.manifest.row_count = dataset.observations.len() as u64;
    dataset.manifest.semantic_digest = ContentHash::of_bytes(seed);
}

fn evidence_version(
    revision: &str,
    event: &str,
    available_at: &str,
    multiplier: &str,
    parent: Option<&str>,
) -> EvidenceVersion {
    let body = format!("synthetic body {revision}");
    let body_hash = ContentHash::of_bytes(body.as_bytes());
    let available_at = time(available_at);
    EvidenceVersion {
        evidence_id: EvidenceId::new(format!("evidence-{event}")).expect("evidence id"),
        revision_id: EvidenceRevisionId::new(revision).expect("revision id"),
        event_id: event.into(),
        purpose: EvidencePurpose::StrategyInput,
        category: "synthetic-boundary".into(),
        regime_label: None,
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        source_refs: vec!["https://example.invalid/synthetic".into()],
        body,
        body_hash: body_hash.clone(),
        event_time: Some(available_at),
        published_at: Some(available_at),
        first_seen_at: Some(available_at),
        content_updated_at: None,
        declared_available_at: available_at,
        registered_at: available_at,
        valid_until: time("2025-01-03T00:00:00Z"),
        supersedes_revision_id: parent
            .map(|id| EvidenceRevisionId::new(id).expect("parent revision id")),
        provenance: EvidenceProvenance::ForwardCaptured {
            captured_at: available_at,
            captured_body_hash: body_hash,
        },
        mapping_version: "synthetic-v1".into(),
        weight_multiplier: Weight::new(decimal(multiplier)).expect("evidence multiplier"),
    }
}

fn observation(id: &str, open_at: &str, open: &str, close: &str) -> CandleObservation {
    let open_time = time(open_at);
    let close_time = UtcTimestamp(open_time.0 + chrono::Duration::hours(1));
    let open_price = price(open);
    let close_price = price(close);
    CandleObservation {
        id: crate::contracts::ObservationId::new(id).expect("fixture observation id"),
        candle: CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: open_time,
            close_time_utc: close_time,
            open: open_price,
            high: PriceKrw::new(open_price.get().max(close_price.get())).expect("fixture high"),
            low: PriceKrw::new(open_price.get().min(close_price.get())).expect("fixture low"),
            close: close_price,
            volume: qty("100"),
            quote_turnover: amount("10000"),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(id.as_bytes()),
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

fn remove_occurrence_run_ids(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            fields.remove("run_id");
            for child in fields.values_mut() {
                remove_occurrence_run_ids(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                remove_occurrence_run_ids(item);
            }
        }
        _ => {}
    }
}

#[test]
fn verified_historical_fee_boundary_switches_execution_costs_pit() {
    let (mut plan, dataset, admission, run_id, _start) = buy_and_hold_fixture();
    let full = plan.spec.range;
    let mid = time("2025-01-02T01:00:00Z");
    let verified = |id: &str, start: UtcTimestamp, end: UtcTimestamp, taker: &str| {
        let mut snapshot = rules("1", "0.1", "1");
        snapshot.id = RuleSnapshotId::new(id).expect("valid rule id");
        snapshot.valid_range = UtcRange::new(start, end).expect("segment range");
        snapshot.provenance = RuleProvenance::VerifiedHistorical;
        snapshot.source_refs = vec!["exchange-notice://2025-01".into()];
        snapshot.fee_schedule = Some(HistoricalFeeSchedule {
            taker_fee_bps: BasisPoints::new(decimal(taker)).expect("valid fee"),
            maker_fee_bps: BasisPoints::new(decimal("1")).expect("valid fee"),
            assumption_label: "verified historical fee schedule".into(),
        });
        snapshot
    };
    let segment_a = verified("rule-a", full.start(), mid, "10");
    let segment_b = verified("rule-b", mid, full.end(), "100");
    plan.spec.costs.buy_fee_bps = BasisPoints::new(decimal("500")).expect("scenario fee");
    plan.spec.costs.sell_fee_bps = BasisPoints::new(decimal("500")).expect("scenario fee");
    plan.spec.terminal_policy = TerminalPolicy::LiquidateScenario;
    plan.spec.market_rules = segment_a.clone();
    plan.spec.market_rules_history = vec![segment_a.clone(), segment_b.clone()];
    let ledger = run_model(
        &plan,
        std::slice::from_ref(&dataset),
        None,
        &run_id,
        &admission,
        10,
        &|| false,
    )
    .expect("pit run completes");
    let buy = ledger
        .fills
        .iter()
        .find(|fill| fill.side == Side::Buy)
        .expect("entry buy fill exists");
    let sell = ledger
        .fills
        .iter()
        .find(|fill| fill.artificial_terminal_exit)
        .expect("terminal sell fill exists");
    assert_eq!(
        buy.fee_bps.get(),
        decimal("10"),
        "segment A taker fee applies"
    );
    assert_eq!(
        sell.fee_bps.get(),
        decimal("100"),
        "segment B verified fee overrides the 500bps scenario proxy"
    );
    assert_eq!(buy.context.market, ledger.market);

    // Control: without history the scenario fee applies to every fill.
    let mut control_plan = plan.clone();
    control_plan.spec.market_rules_history = Vec::new();
    control_plan.spec.market_rules = rules("1", "0.1", "1");
    let control = run_model(
        &control_plan,
        std::slice::from_ref(&dataset),
        None,
        &run_id,
        &admission,
        10,
        &|| false,
    )
    .expect("control run completes");
    for fill in &control.fills {
        assert_eq!(fill.fee_bps.get(), decimal("500"), "current-rule proxy fee");
    }
}

#[test]
fn suspended_market_rejects_the_order_and_never_fills() {
    let (mut plan, dataset, admission, run_id, _start) = buy_and_hold_fixture();
    let full = plan.spec.range;
    plan.spec.market_rules.maintenance_windows =
        vec![UtcRange::new(full.start(), full.end()).expect("maintenance window")];
    let ledger = run_model(
        &plan,
        std::slice::from_ref(&dataset),
        None,
        &run_id,
        &admission,
        10,
        &|| false,
    )
    .expect("suspended run completes");
    assert!(
        ledger.fills.is_empty(),
        "no fill may occur during maintenance"
    );
    let rejected = ledger
        .order_events
        .iter()
        .find(|event| event.status == OrderStatus::Rejected)
        .expect("order is explicitly rejected");
    assert_eq!(rejected.reason, ReasonCode::RuleBlocked);
    let terminal = ledger
        .account_marks
        .iter()
        .find(|mark| mark.kind == MarkKind::Terminal)
        .expect("terminal mark exists");
    assert_eq!(terminal.state.cash_total.get(), decimal("1000"));
}

#[test]
fn dynamic_volatility_cost_uses_only_the_completed_liquidity_bar() {
    let (mut plan, mut dataset, admission, run_id, _start) = buy_and_hold_fixture();
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    plan.spec.costs = CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "dynamic causality fixture".into(),
        dynamic: Some(DynamicCostModel::VolatilityAware {
            base_slippage_bps: zero,
            range_weight: Decimal::from(10_000),
            max_slippage_bps: BasisPoints::new(decimal("5000")).expect("bound"),
        }),
    };
    // The liquidity source bar is flat (range 0); the execution bar itself has a
    // huge range that must never enter the next-bar-open cost.
    let mut prior = observation("prior", "2025-01-01T23:00:00Z", "100", "100");
    prior.candle.high = price("100");
    prior.candle.low = price("100");
    let mut first = observation("first", "2025-01-02T00:00:00Z", "100", "101");
    first.candle.high = price("150");
    first.candle.low = price("50");
    dataset.observations = vec![
        prior,
        first,
        observation("second", "2025-01-02T01:00:00Z", "101", "110"),
    ];
    let ledger = run_model(
        &plan,
        std::slice::from_ref(&dataset),
        None,
        &run_id,
        &admission,
        10,
        &|| false,
    )
    .expect("dynamic run completes");
    let fill = ledger
        .fills
        .iter()
        .find(|fill| fill.side == Side::Buy)
        .expect("entry fill exists");
    assert_eq!(
        fill.price.get(),
        decimal("100"),
        "flat liquidity bar means zero dynamic slippage despite the wide execution bar"
    );
    let provenance = fill
        .cost_provenance
        .as_ref()
        .expect("proxy provenance recorded");
    assert_eq!(provenance.model_kind, DynamicCostKind::VolatilityAware);
    assert_eq!(provenance.effective_slippage_bps.get(), Decimal::ZERO);
    assert_eq!(provenance.proxy_inputs.bar_range_bps, Decimal::ZERO);
    assert_eq!(
        provenance.proxy_inputs.quote_turnover.get(),
        decimal("10000")
    );

    // A wide liquidity bar raises the fill price by the bounded range component.
    let mut wide_prior = observation("prior", "2025-01-01T23:00:00Z", "100", "100");
    wide_prior.candle.high = price("101");
    wide_prior.candle.low = price("99");
    dataset.observations = vec![
        wide_prior,
        observation("first", "2025-01-02T00:00:00Z", "100", "101"),
        observation("second", "2025-01-02T01:00:00Z", "101", "110"),
    ];
    let wide = run_model(
        &plan,
        std::slice::from_ref(&dataset),
        None,
        &run_id,
        &admission,
        10,
        &|| false,
    )
    .expect("wide-liquidity run completes");
    let wide_fill = wide
        .fills
        .iter()
        .find(|fill| fill.side == Side::Buy)
        .expect("wide entry fill exists");
    let wide_provenance = wide_fill
        .cost_provenance
        .as_ref()
        .expect("wide provenance recorded");
    assert_eq!(
        wide_provenance.proxy_inputs.bar_range_bps,
        decimal("200"),
        "2% range of the completed liquidity bar"
    );
    // 2% range fraction x 10000 weight = 200bps, buy price rounds up to the tick.
    assert!(
        wide_fill.price.get() > decimal("100"),
        "prior-bar volatility raises the buy price"
    );
}
