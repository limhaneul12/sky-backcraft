use super::*;
use crate::contracts::{
    AccountMark, AccountState, AssetQuantity, BasisPoints, CandleInterval, CandleObservation,
    CandleRecord, CapitalMode, CostPolicy, CounterfactualAnalysisRequest,
    CounterfactualCandidateDeclaration, CounterfactualCandidateInput, CounterfactualCandidateRole,
    CounterfactualEvidenceInput, CounterfactualFixedExit, CounterfactualVariant, EventContext,
    ExecutionPolicy, IndicatorSnapshot, MarkKind, MarketId, MarketRuleSnapshot, ModelId,
    ObservationId, PaperCandleReference, PaperEventKind, PaperMarketRuleScenario, PaperMarketStart,
    PaperNullReason, PaperSessionEvent, PaperSessionWrite, PitPolicy, PolicyId, PolicyOrigin,
    PolicyRevisionId, PortfolioRiskPolicy, PositionState, PriceKrw, QuoteAmount, ReasonCode,
    RuleProvenance, RunId, SignalId, SignalOutcome, SignalRecord, SignedAmount, StrategyKind,
    TickBand, UtcRange, UtcTimestamp, Weight, builtin_policy_definitions, observation_digest,
};
use rust_decimal::Decimal;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-counterfactual-review-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create isolated test root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

fn time(hour: u32) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(&format!("2024-01-01T{hour:02}:00:00Z")).expect("fixture time")
}

fn decimal(text: &str) -> Decimal {
    text.parse().expect("fixture decimal")
}

fn amount(text: &str) -> QuoteAmount {
    QuoteAmount::new(decimal(text)).expect("fixture quote")
}

fn quantity(text: &str) -> AssetQuantity {
    AssetQuantity::new(decimal(text)).expect("fixture quantity")
}

fn weight(text: &str) -> Weight {
    Weight::new(decimal(text)).expect("fixture weight")
}

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("fixture market")
}

fn costs() -> CostPolicy {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "SYNTHETIC_TEST_ONLY zero costs".into(),
        dynamic: None,
    }
}

fn observation(index: u32, open_price: &str) -> CandleObservation {
    let open_time = time(index);
    let close_time = UtcTimestamp(open_time.0 + chrono::Duration::hours(1));
    let price = decimal(open_price);
    let candle = CandleRecord {
        market: market().code(),
        interval: CandleInterval::H1,
        open_time_utc: open_time,
        close_time_utc: close_time,
        open: PriceKrw::new(price).expect("open"),
        high: PriceKrw::new(price + Decimal::ONE).expect("high"),
        low: PriceKrw::new(price - Decimal::ONE).expect("low"),
        close: PriceKrw::new(price).expect("close"),
        volume: quantity("10"),
        quote_turnover: amount("1000"),
        completed: true,
    };
    let content_digest = observation_digest(&candle).expect("canonical observation digest");
    CandleObservation {
        id: ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn market_rules() -> MarketRuleSnapshot {
    MarketRuleSnapshot {
        id: crate::contracts::RuleSnapshotId::new("counterfactual-store-rules")
            .expect("rule snapshot ID"),
        provenance: RuleProvenance::ExplicitScenario,
        valid_range: UtcRange::new(time(0), time(9)).expect("rule range"),
        observed_at: time(0),
        source_refs: vec!["synthetic://counterfactual-store".into()],
        assumption_label: "SYNTHETIC_TEST_ONLY market rules".into(),
        min_notional: amount("1"),
        quantity_step: quantity("0.1"),
        ticks: vec![TickBand {
            lower_bound: amount("0"),
            tick: PriceKrw::new(Decimal::ONE).expect("tick"),
        }],
        fee_schedule: None,
        trading_state: None,
        maintenance_windows: Vec::new(),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the isolated temporary Store fixture commits the exact saved policy, observations, event and review request as one lifecycle"
)]
fn build_store_fixture() -> Result<
    (
        TempRoot,
        crate::storage::Store,
        CounterfactualAnalysisRequest,
        crate::contracts::PaperReviewSelection,
    ),
    LabError,
> {
    let root = TempRoot::new();
    let mut store = crate::storage::Store::open(root.path())?;
    store.seed_builtin_policies_once(&builtin_policy_definitions()?, time(4))?;
    let source_policy = store
        .paper_start_options()?
        .policy_options
        .into_iter()
        .find(|option| option.reference.policy_id.as_str() == "builtin-buy-and-hold")
        .expect("built-in buy-and-hold policy")
        .reference;
    let observations = vec![
        observation(0, "100"),
        observation(1, "100"),
        observation(2, "101"),
    ];
    {
        let transaction = store
            .connection
            .transaction()
            .map_err(crate::storage::sql_error)?;
        for item in &observations {
            crate::storage::insert_observation(&transaction, item)?;
        }
        transaction.commit().map_err(crate::storage::sql_error)?;
    }
    let execution = ExecutionPolicy::NextBarOpen {
        participation_cap: weight("1"),
    };
    let start = store.paper_write(
        &PaperSessionWrite::Start {
            request_id: crate::contracts::RequestId::new("counterfactual-source-start")?,
            policy_ref: source_policy.clone(),
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            latency_ms: 0,
            execution: execution.clone(),
            markets: vec![PaperMarketStart {
                market: market(),
                initial_cash: amount("1000"),
                costs: costs(),
                market_rules: PaperMarketRuleScenario::SimulatedScenario {
                    min_notional: amount("1"),
                    quantity_step: quantity("0.1"),
                    ticks: vec![TickBand {
                        lower_bound: amount("0"),
                        tick: PriceKrw::new(Decimal::ONE).expect("tick"),
                    }],
                    assumption_label: "SYNTHETIC_TEST_ONLY source rules".into(),
                },
            }],
        },
        time(4),
    )?;

    let decision_bar = &observations[1];
    let event_bar = &observations[2];
    let signal_time = decision_bar.candle.close_time_utc;
    let context = EventContext {
        run_id: RunId::new("counterfactual-source-run")?,
        model_id: ModelId::new("counterfactual-source-model")?,
        market: market(),
        event_seq: 2,
        accounting_event_time: signal_time,
    };
    let mark_context = EventContext {
        event_seq: 1,
        ..context.clone()
    };
    let signal_id = SignalId::new("counterfactual-source-signal")?;
    let signal = SignalRecord {
        decision_trace: None,
        actual_weight_at_decision: None,
        context,
        signal_id: signal_id.clone(),
        strategy: StrategyKind::BuyAndHold,
        strategy_version: "saved-fixture".into(),
        policy_ref: Some(source_policy.clone()),
        policy_trace: None,
        source_bar_ids: vec![decision_bar.id.clone()],
        signal_time,
        decision_available_at: signal_time,
        valid_until: event_bar.candle.open_time_utc,
        state_before: PositionState::Cash,
        state_after: PositionState::Cash,
        raw_target_weight: weight("0"),
        constrained_target_weight: weight("0"),
        intended_side: None,
        reasons: vec![ReasonCode::NoAction],
        indicators: IndicatorSnapshot {
            version: "saved-fixture-indicators".into(),
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
    let event = PaperSessionEvent {
        preparation: None,
        sequence: 1,
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
        signals: vec![signal],
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: vec![AccountMark {
            context: mark_context,
            kind: MarkKind::ExecutionClose,
            state: AccountState {
                cash_total: amount("1000"),
                cash_free: amount("1000"),
                cash_reserved: amount("0"),
                qty: quantity("0"),
                price_basis: amount("0"),
                gross_realized: SignedAmount::new(Decimal::ZERO)?,
                cumulative_fees: amount("0"),
            },
            mark_price: decision_bar.candle.close,
            source_bar_id: decision_bar.id.clone(),
            position_value: amount("0"),
            gross_unrealized: SignedAmount::new(Decimal::ZERO)?,
            equity: amount("1000"),
            target_weight: weight("0"),
            actual_weight: weight("0"),
            peak_equity: amount("1000"),
            drawdown: weight("0"),
        }],
        episodes: Vec::new(),
    };

    let mut view = start.session;
    view.sequence = event.sequence;
    view.event_count = 1;
    let view_json = serde_json::to_string(&view).map_err(crate::storage::json_error)?;
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET sequence=?1,event_count=?2,view_json=?3 WHERE session_id=?4",
            rusqlite::params![
                i64::try_from(event.sequence).expect("test sequence fits i64"),
                1_i64,
                view_json,
                view.session_id.as_str(),
            ],
        )
        .map_err(crate::storage::sql_error)?;
    {
        let transaction = store
            .connection
            .transaction()
            .map_err(crate::storage::sql_error)?;
        crate::storage::paper_store::insert_event_row(
            &transaction,
            &view.session_id,
            &event,
            time(4),
        )?;
        transaction.commit().map_err(crate::storage::sql_error)?;
    }
    let selection = crate::contracts::PaperReviewSelection {
        session_id: view.session_id.clone(),
        event_sequence: event.sequence,
        as_of_sequence: event.sequence,
        signal_id: Some(signal_id.clone()),
    };
    let source = store.paper_review_decision(&selection)?;
    let policy = store
        .load_policy_revision(&source_policy)?
        .expect("saved source policy")
        .snapshot;
    let request = CounterfactualAnalysisRequest {
        candidates: vec![CounterfactualCandidateInput {
            source_session_id: view.session_id,
            source_configuration: source.configuration,
            source_event: source.event,
            source_signal_id: signal_id,
            declaration: CounterfactualCandidateDeclaration::CallerDeclaredScenario {
                scenario_id: "test-entry-opportunity".into(),
                role: CounterfactualCandidateRole::EntryOpportunity,
                declared_by: "storage-regression".into(),
                rationale: "verify that the request remains bound to saved facts".into(),
            },
            bars: observations,
            evidence: CounterfactualEvidenceInput::Unavailable {
                reason: PaperNullReason::DataNotCaptured,
            },
        }],
        variants: vec![CounterfactualVariant {
            variant_id: "stored-baseline".into(),
            policy,
            initial_capital: amount("1000"),
            capital_mode: CapitalMode::IndependentModels,
            risk: PortfolioRiskPolicy {
                max_gross_exposure: weight("1"),
                min_cash_weight: weight("0"),
                drawdown_stop: weight("0"),
            },
            costs: costs(),
            execution,
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            latency_ms: 0,
            market_rules: market_rules(),
            pit_policy: PitPolicy::StrictPit,
            exit: CounterfactualFixedExit {
                hold_decision_bars: 1,
                valuation_bars_after_exit: 1,
            },
        }],
    };
    Ok((root, store, request, selection))
}

#[test]
fn counterfactual_validation_binds_saved_facts_and_does_not_mutate_them() {
    let (_root, store, request, selection) = build_store_fixture().expect("saved fixture");
    let before = serde_json::to_value(
        store
            .paper_review_decision(&selection)
            .expect("source review before validation"),
    )
    .expect("source review JSON");

    store
        .validate_paper_counterfactual(&request)
        .expect("exact saved context, event, and bars validate");

    let mut forged_configuration = request.clone();
    forged_configuration.candidates[0]
        .source_configuration
        .latency_ms += 1;
    assert!(matches!(
        store.validate_paper_counterfactual(&forged_configuration),
        Err(LabError::InputHashMismatch(_))
    ));

    let mut forged_event = request.clone();
    forged_event.candidates[0].source_event.signals[0]
        .reasons
        .push(ReasonCode::MarketFilled);
    assert!(matches!(
        store.validate_paper_counterfactual(&forged_event),
        Err(LabError::InputHashMismatch(_))
    ));

    let mut forged_bar = request.clone();
    forged_bar.candidates[0].bars[0].candle.close =
        PriceKrw::new(decimal("999")).expect("forged close");
    assert!(matches!(
        store.validate_paper_counterfactual(&forged_bar),
        Err(LabError::InputHashMismatch(_))
    ));

    let mut aliased_policy = request.clone();
    aliased_policy.variants[0].policy.origin = PolicyOrigin::User;
    assert!(matches!(
        store.validate_paper_counterfactual(&aliased_policy),
        Err(LabError::InputHashMismatch(_))
    ));

    let after = serde_json::to_value(
        store
            .paper_review_decision(&selection)
            .expect("source review after validation"),
    )
    .expect("source review JSON");
    assert_eq!(before, after, "validation and rejection are read-only");
}

#[test]
fn unknown_valid_variant_is_analysis_only_and_not_registered() {
    let (_root, store, mut request, _) = build_store_fixture().expect("saved fixture");
    let snapshot = &mut request.variants[0].policy;
    snapshot.reference.policy_id = PolicyId::new("counterfactual-only-policy").expect("policy ID");
    snapshot.reference.revision_id =
        PolicyRevisionId::new("counterfactual-only-revision").expect("revision ID");
    store
        .validate_paper_counterfactual(&request)
        .expect("valid unknown frozen variant is analysis-only");
    assert!(
        store
            .load_policy_revision(&request.variants[0].policy.reference)
            .expect("read unknown variant")
            .is_none()
    );
}
