use super::*;
use crate::contracts::{
    AccountMark, AccountState, BasisPoints, CandleRecord, ContentHash, CostPolicy,
    CounterfactualAnalysisRequest, CounterfactualCandidateDeclaration,
    CounterfactualCandidateInput, CounterfactualCandidateRole, CounterfactualEvidenceInput,
    CounterfactualFixedExit, CounterfactualVariant, EventContext, ExecutionPolicy,
    FrozenPolicyRevision, IndicatorSnapshot, MarkKind, MarketRuleSnapshot, PaperCandleReference,
    PaperEventKind, PaperMarketRuleScenario, PaperMarketStart, PaperNullReason, PaperSessionEvent,
    PaperSessionId, PaperSessionStartRequest, PassiveFraction, PitPolicy, PolicyDefinition,
    PolicyId, PolicyOrigin, PolicyRevisionId, PolicyRevisionRef, PortfolioRiskPolicy,
    PositionState, PriceKrw, ReasonCode, RuleProvenance, SignalOutcome, SignalRecord, SignedAmount,
    StateParameters, StrategyKind, StrategySpec, TickBand, UtcRange, UtcTimestamp, Weight,
};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;

fn dec(value: &str) -> Decimal {
    value.parse().expect("fixture decimal")
}

fn time(hour: u32) -> UtcTimestamp {
    UtcTimestamp(
        Utc.with_ymd_and_hms(2024, 1, 1, hour, 0, 0)
            .single()
            .expect("fixture time"),
    )
}

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("fixture market")
}

fn quote(value: &str) -> QuoteAmount {
    QuoteAmount::new(dec(value)).expect("fixture quote")
}

fn signed(value: &str) -> SignedAmount {
    SignedAmount::new(dec(value)).expect("fixture signed amount")
}

fn qty(value: &str) -> AssetQuantity {
    AssetQuantity::new(dec(value)).expect("fixture quantity")
}

fn weight(value: &str) -> Weight {
    Weight::new(dec(value)).expect("fixture weight")
}

fn bps(value: &str) -> BasisPoints {
    BasisPoints::new(dec(value)).expect("fixture basis points")
}

fn observation(index: usize, open: &str, high: &str, low: &str, close: &str) -> CandleObservation {
    let open_time = time(u32::try_from(index).expect("bar index"));
    let close_time = UtcTimestamp(open_time.0 + chrono::Duration::hours(1));
    let id = crate::contracts::ObservationId::new(format!("counterfactual-bar-{index}"))
        .expect("observation id");
    let open_price = PriceKrw::new(dec(open)).expect("open price");
    let volume = if index == 2 { qty("200") } else { qty("1000") };
    CandleObservation {
        content_digest: ContentHash::of_bytes(id.as_str().as_bytes()),
        id,
        candle: CandleRecord {
            market: market().code(),
            interval: CandleInterval::H1,
            open_time_utc: open_time,
            close_time_utc: close_time,
            open: open_price,
            high: PriceKrw::new(dec(high)).expect("high price"),
            low: PriceKrw::new(dec(low)).expect("low price"),
            close: PriceKrw::new(dec(close)).expect("close price"),
            volume,
            quote_turnover: quote("100000"),
            completed: true,
        },
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn bars() -> Vec<CandleObservation> {
    vec![
        observation(0, "100", "101", "99", "100"),
        observation(1, "100", "101", "99", "100"),
        observation(2, "100", "110", "99", "110"),
        observation(3, "120", "130", "119", "125"),
        observation(4, "125", "131", "124", "127"),
        observation(5, "127", "132", "126", "128"),
    ]
}

fn costs() -> CostPolicy {
    CostPolicy {
        buy_fee_bps: bps("100"),
        sell_fee_bps: bps("100"),
        maker_fee_bps: bps("50"),
        half_spread_bps: bps("50"),
        slippage_bps: bps("0"),
        impact_bps: bps("0"),
        assumption_label: "hand-computed fixed scenario".into(),
        dynamic: None,
    }
}

fn rules() -> MarketRuleSnapshot {
    MarketRuleSnapshot {
        id: crate::contracts::RuleSnapshotId::new("counterfactual-rules").expect("rule ID"),
        provenance: RuleProvenance::ExplicitScenario,
        valid_range: UtcRange::new(time(0), time(6)).expect("rule range"),
        observed_at: time(0),
        source_refs: vec!["synthetic://counterfactual-fixture".into()],
        assumption_label: "hand-computed tick and fee assumptions".into(),
        min_notional: quote("1"),
        quantity_step: qty("0.1"),
        ticks: vec![TickBand {
            lower_bound: quote("0"),
            tick: PriceKrw::new(Decimal::ONE).expect("tick"),
        }],
        fee_schedule: None,
        trading_state: None,
        maintenance_windows: Vec::new(),
    }
}

fn frozen_policy(id: &str, strategy: &StrategySpec) -> FrozenPolicyRevision {
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: id.into(),
        description: "synthetic counterfactual fixture".into(),
        program: crate::contracts::PolicyProgram::Builtin {
            strategy: strategy.clone(),
        },
    };
    let reference = PolicyRevisionRef {
        policy_id: PolicyId::new(format!("policy-{id}")).expect("policy ID"),
        revision_id: PolicyRevisionId::new(format!("revision-{id}")).expect("policy revision ID"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    };
    FrozenPolicyRevision {
        reference,
        revision_number: 1,
        parent_revision_id: None,
        family: strategy.kind(),
        origin: PolicyOrigin::User,
        definition,
    }
}

fn risk() -> PortfolioRiskPolicy {
    PortfolioRiskPolicy {
        max_gross_exposure: weight("1"),
        min_cash_weight: weight("0"),
        drawdown_stop: weight("0"),
    }
}

fn paper_start(policy_ref: PolicyRevisionRef) -> PaperSessionStartRequest {
    let costs = costs();
    let ticks = vec![TickBand {
        lower_bound: quote("0"),
        tick: PriceKrw::new(Decimal::ONE).expect("tick"),
    }];
    PaperSessionStartRequest {
        request_id: crate::contracts::RequestId::new("counterfactual-source").expect("request ID"),
        policy_ref,
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: weight("1"),
        },
        markets: vec![PaperMarketStart {
            market: market(),
            initial_cash: quote("1000"),
            costs,
            market_rules: PaperMarketRuleScenario::SimulatedScenario {
                min_notional: quote("1"),
                quantity_step: qty("0.1"),
                ticks,
                assumption_label: "source-session scenario".into(),
            },
        }],
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the synthetic source signal, state mark, event and bars form one linked identity fixture"
)]
fn candidate() -> CounterfactualCandidateInput {
    let bars = bars();
    let source_policy = frozen_policy("source", &StrategySpec::BuyAndHold);
    let source_signal_id =
        crate::contracts::SignalId::new("source-signal").expect("source signal ID");
    let context = EventContext {
        run_id: crate::contracts::RunId::new("source-run").expect("run ID"),
        model_id: crate::contracts::ModelId::new("source-model").expect("model ID"),
        market: market(),
        event_seq: 2,
        accounting_event_time: time(2),
    };
    let source_signal = SignalRecord {
        decision_trace: None,
        actual_weight_at_decision: None,
        context: context.clone(),
        signal_id: source_signal_id.clone(),
        strategy: StrategyKind::BuyAndHold,
        strategy_version: "fixture-v1".into(),
        policy_ref: Some(source_policy.reference.clone()),
        policy_trace: None,
        source_bar_ids: vec![bars[1].id.clone()],
        signal_time: time(2),
        decision_available_at: time(2),
        valid_until: time(3),
        state_before: PositionState::Cash,
        state_after: PositionState::Cash,
        raw_target_weight: weight("0"),
        constrained_target_weight: weight("0"),
        intended_side: None,
        reasons: vec![ReasonCode::NoAction],
        indicators: IndicatorSnapshot {
            version: "fixture-indicators".into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: 2,
            policy_values: Vec::new(),
        },
        evidence_effect: None,
        outcome: SignalOutcome::NoAction,
    };
    let source_mark = AccountMark {
        context: EventContext {
            event_seq: 1,
            ..context
        },
        kind: MarkKind::ExecutionClose,
        state: AccountState {
            cash_total: quote("1000"),
            cash_free: quote("1000"),
            cash_reserved: quote("0"),
            qty: qty("0"),
            price_basis: quote("0"),
            gross_realized: signed("0"),
            cumulative_fees: quote("0"),
        },
        mark_price: bars[1].candle.close,
        source_bar_id: bars[1].id.clone(),
        position_value: quote("0"),
        gross_unrealized: signed("0"),
        equity: quote("1000"),
        target_weight: weight("0"),
        actual_weight: weight("0"),
        peak_equity: quote("1000"),
        drawdown: weight("0"),
    };
    let event_bar = &bars[2];
    let source_event = PaperSessionEvent {
        preparation: None,
        sequence: 9,
        kind: PaperEventKind::CandleProcessed,
        market: Some(market()),
        candle: Some(PaperCandleReference {
            observation_id: event_bar.id.clone(),
            interval: event_bar.candle.interval,
            open_time: event_bar.candle.open_time_utc,
            close_time: event_bar.candle.close_time_utc,
            content_digest: event_bar.content_digest.clone(),
        }),
        source_event_at: Some(event_bar.candle.close_time_utc),
        finalized_at: Some(event_bar.candle.close_time_utc),
        control: None,
        provider_recovery: None,
        signals: vec![source_signal],
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: vec![source_mark],
        episodes: Vec::new(),
    };
    CounterfactualCandidateInput {
        source_session_id: PaperSessionId::new("source-session").expect("session ID"),
        source_configuration: paper_start(source_policy.reference),
        source_event,
        source_signal_id,
        declaration: CounterfactualCandidateDeclaration::CallerDeclaredScenario {
            scenario_id: "manual-opportunity".into(),
            role: CounterfactualCandidateRole::EntryOpportunity,
            declared_by: "synthetic-fixture".into(),
            rationale: "test a declared entry opportunity without changing source facts".into(),
        },
        bars,
        evidence: CounterfactualEvidenceInput::Unavailable {
            reason: PaperNullReason::DataNotCaptured,
        },
    }
}

fn variant(id: &str, policy: FrozenPolicyRevision) -> CounterfactualVariant {
    CounterfactualVariant {
        variant_id: id.into(),
        policy,
        initial_capital: quote("1000"),
        capital_mode: crate::contracts::CapitalMode::IndependentModels,
        risk: risk(),
        costs: costs(),
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: weight("1"),
        },
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        market_rules: rules(),
        pit_policy: PitPolicy::StrictPit,
        exit: CounterfactualFixedExit {
            hold_decision_bars: 1,
            valuation_bars_after_exit: 1,
        },
    }
}

fn fixture_request() -> CounterfactualAnalysisRequest {
    CounterfactualAnalysisRequest {
        candidates: vec![candidate()],
        variants: vec![
            variant(
                "buy-and-hold",
                frozen_policy("buy-and-hold", &StrategySpec::BuyAndHold),
            ),
            variant(
                "breakout",
                frozen_policy(
                    "breakout",
                    &StrategySpec::S2 {
                        entry_length: 1,
                        exit_length: 1,
                    },
                ),
            ),
        ],
    }
}

#[test]
fn next_open_scenario_reuses_exact_fee_tick_and_accounting_conventions() {
    // 100 bps = 0.01 and 50 bps price cost rounds a 100 buy up to 101.
    let report = analyze(&fixture_request()).expect("counterfactual report");
    assert_eq!(report.status, CounterfactualReportStatus::Complete);
    assert_eq!(
        report.metadata.execution_scope,
        CounterfactualExecutionScope::VirtualOnly
    );
    assert_eq!(
        report.metadata.pnl_scope,
        CounterfactualPnlScope::HypotheticalStandaloneScenario
    );
    assert_eq!(report.candidates.len(), 1);
    let candidate = &report.candidates[0];
    assert_eq!(candidate.source_signal.outcome, SignalOutcome::NoAction);
    assert_eq!(candidate.source_signal.reasons, vec![ReasonCode::NoAction]);
    assert!(candidate.source_actual_order_ids.is_empty());
    assert!(candidate.source_actual_fill_ids.is_empty());

    let buy = candidate
        .variant_outcomes
        .iter()
        .find(|outcome| outcome.variant_id == "buy-and-hold")
        .expect("buy-and-hold variant");
    assert_eq!(buy.status, CounterfactualVariantStatus::Completed);
    assert_eq!(buy.executions.len(), 2);
    assert_eq!(buy.executions[0].price.get(), dec("101"));
    assert_eq!(buy.executions[0].quantity.get(), dec("9.8"));
    assert_eq!(buy.executions[0].notional.get(), dec("989.8"));
    assert_eq!(buy.executions[0].fee.get(), dec("9.898"));
    assert_eq!(
        buy.executions[0].price_cost_attribution_krw.get(),
        dec("9.8")
    );
    assert_eq!(buy.executions[1].price.get(), dec("119"));
    assert_eq!(buy.executions[1].quantity.get(), dec("9.8"));
    assert_eq!(buy.executions[1].notional.get(), dec("1166.2"));
    assert_eq!(buy.executions[1].fee.get(), dec("11.662"));
    assert_eq!(
        buy.executions[1].price_cost_attribution_krw.get(),
        dec("9.8")
    );
    assert_eq!(
        buy.cumulative_fees.as_ref().expect("fees").get(),
        dec("21.56")
    );
    assert_eq!(
        buy.final_equity.as_ref().expect("equity").get(),
        dec("1154.84")
    );
    assert_eq!(buy.net_pnl.as_ref().expect("PnL").get(), dec("154.84"));
    let end_state = buy.ending_account.as_ref().expect("ending account");
    assert_eq!(end_state.cash_total.get(), dec("1154.84"));
    assert_eq!(end_state.qty.get(), Decimal::ZERO);

    let breakout = candidate
        .variant_outcomes
        .iter()
        .find(|outcome| outcome.variant_id == "breakout")
        .expect("breakout variant");
    assert_eq!(breakout.status, CounterfactualVariantStatus::NotTriggered);
    assert_eq!(breakout.policy_target_weight, Some(weight("0")));
    assert!(breakout.executions.is_empty());
}

#[test]
fn partial_entry_and_exit_keep_residual_inventory_in_virtual_account() {
    let mut request = fixture_request();
    request.variants.truncate(1);
    request.variants[0].initial_capital = quote("100000");
    let ExecutionPolicy::NextBarOpen { participation_cap } = &mut request.variants[0].execution
    else {
        panic!("fixture uses NextBarOpen");
    };
    *participation_cap = weight("0.5");

    let report = analyze(&request).expect("partial virtual fills");
    let outcome = &report.candidates[0].variant_outcomes[0];
    assert_eq!(
        outcome.status,
        CounterfactualVariantStatus::PartiallyExecuted
    );
    assert_eq!(outcome.entry_result, Some(ReasonCode::VolumeCap));
    assert_eq!(outcome.exit_result, Some(ReasonCode::VolumeCap));
    assert_eq!(outcome.executions.len(), 2);
    assert_eq!(outcome.executions[0].quantity.get(), dec("500"));
    assert_eq!(outcome.executions[1].quantity.get(), dec("100"));
    let ending = outcome.ending_account.as_ref().expect("residual account");
    assert_eq!(ending.cash_total.get(), dec("60776"));
    assert_eq!(ending.qty.get(), dec("400"));
    assert_eq!(
        outcome.final_equity.as_ref().expect("valuation").get(),
        dec("110776")
    );
    assert_eq!(outcome.net_pnl.as_ref().expect("PnL").get(), dec("10776"));
    assert_eq!(
        outcome.cumulative_fees.as_ref().expect("fees").get(),
        dec("624")
    );
}

#[test]
fn additional_future_suffix_does_not_change_candidate_decision_or_valuation() {
    let base = analyze(&fixture_request()).expect("base report");
    let mut extended_request = fixture_request();
    extended_request.candidates[0]
        .bars
        .push(observation(6, "128", "2000", "127", "1999"));
    let extended = analyze(&extended_request).expect("extended report");

    let base_outcome = base.candidates[0]
        .variant_outcomes
        .iter()
        .find(|outcome| outcome.variant_id == "buy-and-hold")
        .expect("buy-and-hold base outcome");
    let extended_outcome = extended.candidates[0]
        .variant_outcomes
        .iter()
        .find(|outcome| outcome.variant_id == "buy-and-hold")
        .expect("buy-and-hold extended outcome");
    assert_eq!(
        base_outcome.policy_target_weight,
        extended_outcome.policy_target_weight
    );
    assert_eq!(base_outcome.status, extended_outcome.status);
    assert_eq!(
        base_outcome.executions.len(),
        extended_outcome.executions.len()
    );
    assert_eq!(
        base_outcome.executions[0].price,
        extended_outcome.executions[0].price
    );
    assert_eq!(base_outcome.final_equity, extended_outcome.final_equity);
    assert_eq!(base_outcome.net_pnl, extended_outcome.net_pnl);
    assert_ne!(
        base.candidates[0].identity.digest, extended.candidates[0].identity.digest,
        "the candidate identity records the distinct explicit bar evidence input"
    );
}

#[test]
fn same_time_signals_keep_distinct_identity_and_overlaps_are_unavailable() {
    let mut first = candidate();
    let exact_duplicate = CounterfactualAnalysisRequest {
        candidates: vec![first.clone(), first.clone()],
        variants: fixture_request().variants,
    };
    assert!(matches!(
        analyze(&exact_duplicate),
        Err(LabError::Conflict(message)) if message.contains("duplicate exact")
    ));
    let mut second = first.clone();
    let mut second_signal = second.source_event.signals[0].clone();
    second_signal.signal_id =
        crate::contracts::SignalId::new("source-signal-two").expect("second signal ID");
    second_signal.context.event_seq = 3;
    second_signal.state_after = PositionState::Long;
    second.source_event.signals.push(second_signal.clone());
    second.source_signal_id = second_signal.signal_id;
    let CounterfactualCandidateDeclaration::CallerDeclaredScenario { scenario_id, .. } =
        &mut second.declaration;
    *scenario_id = "manual-opportunity-two".into();
    // A distinct state and signal at the same timestamp remains a separate
    // identity even though the overlapping virtual horizon is unavailable.
    let request = CounterfactualAnalysisRequest {
        candidates: vec![first.clone(), second],
        variants: fixture_request().variants,
    };
    let report = analyze(&request).expect("overlap report");
    assert_eq!(report.candidates.len(), 2);
    assert_ne!(
        report.candidates[0].identity.digest,
        report.candidates[1].identity.digest
    );
    let mut changed_bar_lineage = first.clone();
    changed_bar_lineage.bars[0]
        .raw_object_ids
        .push(crate::contracts::RawObjectId::new("additional-raw-evidence").expect("raw ID"));
    let changed_evidence = analyze(&CounterfactualAnalysisRequest {
        candidates: vec![changed_bar_lineage],
        variants: fixture_request().variants,
    })
    .expect("changed bar lineage");
    assert_ne!(
        changed_evidence.candidates[0].identity.bar_evidence_digest,
        analyze(&CounterfactualAnalysisRequest {
            candidates: vec![first.clone()],
            variants: fixture_request().variants,
        })
        .expect("original bar lineage")
        .candidates[0]
            .identity
            .bar_evidence_digest
    );
    assert!(report.candidates.iter().all(|candidate| {
        candidate.source_signal.outcome == SignalOutcome::NoAction
            && candidate.variant_outcomes[0].unavailable_reason
                == Some(CounterfactualUnavailableReason::OverlappingCandidateWindow)
    }));
    first.source_event.sequence += 1;
    assert_ne!(
        analyze(&CounterfactualAnalysisRequest {
            candidates: vec![first],
            variants: fixture_request().variants,
        })
        .expect("changed event identity")
        .candidates[0]
            .identity
            .digest,
        report.candidates[0].identity.digest
    );
}

#[test]
fn required_evidence_is_unavailable_and_unsupported_costs_fail_closed() {
    let mut evidence_request = fixture_request();
    let evidence_policy = frozen_policy(
        "evidence-overlay",
        &StrategySpec::S5 {
            state: StateParameters {
                ema_length: 2,
                vol_length: 2,
                k: 0.0,
            },
        },
    );
    evidence_request.variants = vec![variant("evidence", evidence_policy)];
    move_candidate_to_second_decision(&mut evidence_request.candidates[0]);
    let evidence_report = analyze(&evidence_request).expect("evidence availability report");
    assert_eq!(
        evidence_report.candidates[0].variant_outcomes[0].status,
        CounterfactualVariantStatus::EvidenceUnavailable
    );
    assert_eq!(
        evidence_report.candidates[0].variant_outcomes[0].unavailable_reason,
        Some(CounterfactualUnavailableReason::MissingRequiredEvidence)
    );

    let mut dynamic_request = fixture_request();
    dynamic_request.variants.truncate(1);
    dynamic_request.variants[0].costs.dynamic =
        Some(crate::contracts::DynamicCostModel::VolatilityAware {
            base_slippage_bps: bps("0"),
            range_weight: Decimal::ONE,
            max_slippage_bps: bps("10"),
        });
    let dynamic_report = analyze(&dynamic_request).expect("unsupported dynamic cost report");
    assert_eq!(
        dynamic_report.candidates[0].variant_outcomes[0].status,
        CounterfactualVariantStatus::Unsupported
    );
    assert_eq!(
        dynamic_report.candidates[0].variant_outcomes[0].unavailable_reason,
        Some(CounterfactualUnavailableReason::DynamicCostModelUnsupported)
    );

    let mut passive_request = fixture_request();
    passive_request.variants.truncate(1);
    passive_request.variants[0].execution = ExecutionPolicy::PassiveBuy {
        offset_bps: bps("10"),
        penetration_ticks: 1,
        fill_fraction: PassiveFraction::Full,
        ttl_execution_bars: 2,
        participation_cap: weight("1"),
    };
    let passive_report = analyze(&passive_request).expect("unsupported passive report");
    assert_eq!(
        passive_report.candidates[0].variant_outcomes[0].unavailable_reason,
        Some(CounterfactualUnavailableReason::PassiveExecutionUnsupported)
    );

    let mut shared_request = fixture_request();
    shared_request.variants.truncate(1);
    shared_request.variants[0].capital_mode = crate::contracts::CapitalMode::SharedPortfolio;
    let shared_report = analyze(&shared_request).expect("unsupported shared capital report");
    assert_eq!(
        shared_report.candidates[0].variant_outcomes[0].unavailable_reason,
        Some(CounterfactualUnavailableReason::SharedCapitalUnsupported)
    );

    let mut drawdown_request = fixture_request();
    drawdown_request.variants.truncate(1);
    drawdown_request.variants[0].risk.drawdown_stop = weight("0.1");
    let drawdown_report = analyze(&drawdown_request).expect("unsupported drawdown-stop report");
    assert_eq!(
        drawdown_report.candidates[0].variant_outcomes[0].unavailable_reason,
        Some(CounterfactualUnavailableReason::DrawdownStopUnsupported)
    );
}

#[test]
fn incomparable_variant_capital_and_missing_exit_bars_are_rejected_explicitly() {
    let mut incomparable = fixture_request();
    incomparable.variants[1].initial_capital = quote("2000");
    assert!(matches!(
        analyze(&incomparable),
        Err(LabError::Conflict(message)) if message.contains("share capital")
    ));

    let mut missing_exit = fixture_request();
    missing_exit.candidates[0].bars.truncate(3);
    let report = analyze(&missing_exit).expect("missing exit report");
    assert_eq!(
        report.candidates[0].variant_outcomes[0].status,
        CounterfactualVariantStatus::InsufficientEvidence
    );
    assert_eq!(report.status, CounterfactualReportStatus::Partial);
}

#[test]
fn delayed_saved_signal_availability_cannot_execute_before_its_decision_exists() {
    let mut request = fixture_request();
    request.candidates[0].source_event.signals[0].decision_available_at = time(3);
    assert!(matches!(
        analyze(&request),
        Err(LabError::DataCorrupt(message)) if message.contains("availability time")
    ));
}

#[test]
fn mixed_market_candidates_are_rejected_before_reusing_one_market_rules_snapshot() {
    let mut request = fixture_request();
    let mut other_market = candidate();
    other_market.source_event.market =
        Some(MarketId::parse_upbit("KRW-ETH").expect("other fixture market"));
    request.candidates.push(other_market);
    assert!(matches!(
        analyze(&request),
        Err(LabError::InvalidConfig(message)) if message.contains("exactly one market")
    ));
}

#[test]
fn identical_input_has_deterministic_contract_and_outcome_digests() {
    let request = fixture_request();
    let first = analyze(&request).expect("first report");
    let second = analyze(&request).expect("second report");
    assert_eq!(first.contract_digest, second.contract_digest);
    assert_eq!(
        serde_json::to_value(first).expect("first JSON"),
        serde_json::to_value(second).expect("second JSON")
    );
}

fn move_candidate_to_second_decision(candidate: &mut CounterfactualCandidateInput) {
    let decision_bar = candidate.bars[2].clone();
    let event_bar = candidate.bars[3].clone();
    let signal = &mut candidate.source_event.signals[0];
    signal.source_bar_ids = vec![decision_bar.id.clone()];
    signal.signal_time = time(3);
    signal.decision_available_at = time(3);
    signal.valid_until = time(4);
    signal.context.accounting_event_time = time(3);
    candidate.source_event.candle = Some(PaperCandleReference {
        observation_id: event_bar.id.clone(),
        interval: event_bar.candle.interval,
        open_time: event_bar.candle.open_time_utc,
        close_time: event_bar.candle.close_time_utc,
        content_digest: event_bar.content_digest.clone(),
    });
    candidate.source_event.source_event_at = Some(event_bar.candle.close_time_utc);
    candidate.source_event.finalized_at = Some(event_bar.candle.close_time_utc);
    candidate.source_event.account_marks[0].source_bar_id = decision_bar.id;
    candidate.source_event.account_marks[0].mark_price = candidate.bars[2].candle.close;
    candidate.source_event.account_marks[0]
        .context
        .accounting_event_time = time(3);
}
