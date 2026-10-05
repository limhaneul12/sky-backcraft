use super::*;
use crate::contracts::{
    CandleInterval, CandleRecord, CollectRequest, DatasetId, DatasetManifest, DatasetStatus,
    ExecutionPolicy, MarketDataOrigin, MarketRuleSnapshot, ObservationId, PlanId,
    PortfolioAssetSpec, RegimeLabel, ReportClock, RequestId, RuleProvenance, RuleSnapshotId,
    SCHEMA_VERSION, StrategySpec, TickBand, UtcRange,
};
use std::str::FromStr;

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture time")
}

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("fixture decimal")
}

fn price(value: &str) -> PriceKrw {
    PriceKrw::new(decimal(value)).expect("fixture price")
}

fn quote(value: &str) -> QuoteAmount {
    QuoteAmount::new(decimal(value)).expect("fixture quote")
}

fn rules() -> MarketRuleSnapshot {
    MarketRuleSnapshot {
        id: RuleSnapshotId::new("portfolio-rules").expect("rule id"),
        provenance: RuleProvenance::ExplicitScenario,
        valid_range: UtcRange::new(time("2025-01-01T00:00:00Z"), time("2025-01-03T00:00:00Z"))
            .expect("range"),
        observed_at: time("2025-01-01T00:00:00Z"),
        source_refs: vec!["synthetic://portfolio".into()],
        assumption_label: "portfolio fixture rules".into(),
        min_notional: quote("1"),
        quantity_step: AssetQuantity::new(decimal("0.0001")).expect("step"),
        ticks: vec![TickBand {
            lower_bound: quote("0"),
            tick: price("0.01"),
        }],
        fee_schedule: None,
        trading_state: None,
        maintenance_windows: Vec::new(),
    }
}

fn zero_costs() -> crate::contracts::CostPolicy {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    crate::contracts::CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "portfolio zero-cost fixture".into(),
        dynamic: None,
    }
}

/// One market dataset with flat `volume=100` H1 bars over the given closes.
fn dataset(code: &str, prices: &[&str]) -> DatasetSnapshot {
    let market = MarketId::parse_upbit(code).expect("fixture market");
    let observations: Vec<CandleObservation> = prices
        .iter()
        .enumerate()
        .map(|(index, close)| {
            let open_time = time("2025-01-01T00:00:00Z").0
                + chrono::Duration::hours(i64::try_from(index).expect("index"));
            let close_price = price(close);
            CandleObservation {
                id: ObservationId::new(format!("{code}-obs-{index}")).expect("observation id"),
                candle: CandleRecord {
                    market: code.to_string(),
                    interval: CandleInterval::H1,
                    open_time_utc: UtcTimestamp(open_time),
                    close_time_utc: UtcTimestamp(open_time + chrono::Duration::hours(1)),
                    open: close_price,
                    high: close_price,
                    low: close_price,
                    close: close_price,
                    volume: AssetQuantity::new(Decimal::from(100)).expect("volume"),
                    quote_turnover: QuoteAmount::new(Decimal::from(100) * close_price.get())
                        .expect("turnover"),
                    completed: true,
                },
                content_digest: ContentHash::of_bytes(format!("{code}-{index}").as_bytes()),
                raw_object_ids: Vec::new(),
                constituent_ids: Vec::new(),
            }
        })
        .collect();
    let id = DatasetId::new(format!("dataset-{code}")).expect("dataset id");
    let start = time("2025-01-01T00:00:00Z");
    let end = UtcTimestamp(
        time("2025-01-01T00:00:00Z").0
            + chrono::Duration::hours(i64::try_from(prices.len()).expect("len")),
    );
    DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: SCHEMA_VERSION.into(),
            id: id.clone(),
            request: CollectRequest {
                request_id: RequestId::new(format!("request-{code}")).expect("request"),
                markets: vec![market],
                range: UtcRange::new(start, end).expect("range"),
                data_resolution: CandleInterval::H1,
                warmup_bars: 0,
                completed_only: true,
            },
            coverage: UtcRange::new(start, end).expect("coverage"),
            status: DatasetStatus::Ready,
            row_count: u64::try_from(observations.len()).unwrap_or(u64::MAX),
            normalizer_version: "fixture-v1".into(),
            gap_policy: "FAIL_CLOSED".into(),
            semantic_digest: ContentHash::of_bytes(code.as_bytes()),
            provenance_digest: ContentHash::of_bytes(format!("{code}-prov").as_bytes()),
            origin: MarketDataOrigin::SyntheticTestOnly,
            raw_objects: Vec::new(),
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations,
    }
}

/// A v1 plan with one strategy family across the given markets.
fn plan(markets: &[&str], strategy: StrategySpec) -> ResolvedPlan {
    let market_ids: Vec<MarketId> = markets
        .iter()
        .map(|code| MarketId::parse_upbit(code).expect("market"))
        .collect();
    let start = time("2025-01-01T02:00:00Z");
    let end = time("2025-01-01T10:00:00Z");
    let range = UtcRange::new(start, end).expect("range");
    let mut admissions = Vec::new();
    for market in &market_ids {
        admissions.push(crate::contracts::ModelAdmission {
            model_id: ModelId::from_seed(&format!("portfolio:{}", market.code())),
            market: market.clone(),
            strategy: strategy.kind(),
            policy_ref: None,
            status: crate::contracts::AdmissionStatus::Eligible,
            reasons: Vec::new(),
        });
    }
    ResolvedPlan {
        id: PlanId::new("plan-portfolio").expect("plan id"),
        spec: crate::contracts::ExperimentSpec {
            schema_version: SCHEMA_VERSION.into(),
            dataset_ids: markets
                .iter()
                .map(|code| DatasetId::new(format!("dataset-{code}")).expect("dataset"))
                .collect(),
            markets: market_ids,
            range,
            strategies: vec![strategy],
            policy_selections: Vec::new(),
            causal_execution: None,
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            latency_ms: 0,
            initial_cash: quote("300000"),
            costs: zero_costs(),
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE).expect("weight"),
            },
            market_rules: rules(),
            market_rules_history: Vec::new(),
            terminal_policy: TerminalPolicy::LiquidateScenario,
            evidence_snapshot_id: None,
            pit_policy: crate::contracts::PitPolicy::StrictPit,
            evidence_unavailable:
                crate::contracts::EvidenceUnavailablePolicy::CashWithMatchedControl,
            report_clock: ReportClock {
                timezone: "UTC".into(),
                min_annualization_days: 1,
                risk_free_annual: 0.0,
            },
            seed: 11,
        },
        config_digest: ContentHash::of_bytes(b"portfolio-config"),
        input_digest: ContentHash::of_bytes(b"portfolio-input"),
        dataset_digests: markets
            .iter()
            .map(|code| {
                (
                    DatasetId::new(format!("dataset-{code}")).expect("dataset"),
                    ContentHash::of_bytes(code.as_bytes()),
                )
            })
            .collect(),
        evidence_digest: None,
        admissions,
        policy_revisions: Vec::new(),
        warnings: Vec::new(),
        estimated_events: 128,
    }
}

fn portfolio_spec(
    initial: &str,
    assets: &[(&str, &str)],
    gross: &str,
    reserve: &str,
    stop: &str,
    arbitration: ArbitrationPolicy,
) -> PortfolioSpec {
    PortfolioSpec {
        initial_cash: quote(initial),
        assets: assets
            .iter()
            .map(|(code, cap)| PortfolioAssetSpec {
                market: MarketId::parse_upbit(code).expect("asset"),
                max_weight: Weight::new(decimal(cap)).expect("cap"),
            })
            .collect(),
        risk: crate::contracts::PortfolioRiskPolicy {
            max_gross_exposure: Weight::new(decimal(gross)).expect("gross"),
            min_cash_weight: Weight::new(decimal(reserve)).expect("reserve"),
            drawdown_stop: Weight::new(decimal(stop)).expect("stop"),
        },
        arbitration,
    }
}

fn run(plan: &ResolvedPlan, datasets: &[DatasetSnapshot], spec: &PortfolioSpec) -> PortfolioLedger {
    run_portfolio(
        plan,
        datasets,
        spec,
        None,
        &RunId::new("run-portfolio").expect("run id"),
        100,
        &|| false,
    )
    .expect("portfolio run completes")
}

const FLAT_PRICES: [&str; 10] = [
    "100", "100", "100", "100", "100", "100", "100", "100", "100", "100",
];

#[test]
fn shared_pool_never_double_spends_and_reconciles() {
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "300000",
        &[("KRW-BTC", "0.45"), ("KRW-XRP", "0.30")],
        "0.70",
        "0.20",
        "0.015",
        ArbitrationPolicy::Priority,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    // Every mark reconciles (enforced internally) and cash never goes negative.
    for mark in &ledger.marks {
        assert!(
            mark.cash.get() >= Decimal::ZERO,
            "cash must stay nonnegative"
        );
        assert!(
            mark.gross_exposure_weight.get() <= decimal("0.7") + decimal("0.0001"),
            "gross exposure respects the cap: {}",
            mark.gross_exposure_weight.get()
        );
        for (code, weight) in &mark.weights {
            let cap = if code == "KRW-BTC" {
                decimal("0.45")
            } else {
                decimal("0.30")
            };
            assert!(
                *weight <= cap + decimal("0.0001"),
                "asset weight {code}={weight} exceeds its cap"
            );
        }
    }
    // The terminal equity identity: initial + realized + unrealized - fees.
    let terminal = ledger.marks.last().expect("terminal mark");
    let realized: Decimal = ledger
        .attribution
        .iter()
        .map(|a| a.realized_pnl.get())
        .sum();
    let unrealized: Decimal = ledger
        .attribution
        .iter()
        .map(|a| a.unrealized_pnl.get())
        .sum();
    let fees: Decimal = ledger.attribution.iter().map(|a| a.fees.get()).sum();
    let expected = Decimal::from(300_000) + realized + unrealized - fees;
    assert!(
        (terminal.equity.get() - expected).abs() <= decimal("0.00000001"),
        "terminal reconciliation: equity={} expected={expected}",
        terminal.equity.get()
    );
    // With flat prices and full liquidation, equity returns to initial.
    assert_eq!(terminal.equity.get(), Decimal::from(300_000));
    // Attribution sums to the shared-pool activity; both assets traded.
    assert_eq!(ledger.attribution.len(), 2);
    let buys: Decimal = ledger
        .fills
        .iter()
        .filter(|fill| fill.side == Side::Buy)
        .map(|fill| fill.notional.get())
        .sum();
    let sells: Decimal = ledger
        .fills
        .iter()
        .filter(|fill| fill.side == Side::Sell)
        .map(|fill| fill.notional.get())
        .sum();
    assert!(
        buys > Decimal::ZERO && sells > Decimal::ZERO,
        "round trip completed"
    );
    // Turnover reflects both sides on the shared pool.
    assert_eq!(
        ledger.totals.turnover,
        (buys + sells) / Decimal::from(300_000)
    );
}

#[test]
fn simultaneous_buys_share_one_pool_without_double_spending() {
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.40"), ("KRW-XRP", "0.40")],
        "0.60",
        "0.00",
        "0",
        ArbitrationPolicy::Priority,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    let first_decisions: Vec<_> = ledger
        .fills
        .iter()
        .filter(|fill| fill.side == Side::Buy)
        .filter(|fill| fill.decision_time == time("2025-01-01T02:00:00Z"))
        .collect();
    assert_eq!(
        first_decisions.len(),
        2,
        "both assets buy at the first decision"
    );
    let total: Decimal = first_decisions
        .iter()
        .map(|fill| fill.notional.get() + fill.fee.get())
        .sum();
    assert!(
        total <= Decimal::from(1000),
        "simultaneous buys must not exceed the shared pool: {total}"
    );
    // Priority order: BTC is served first and receives its full asset cap.
    let btc_fill = first_decisions
        .iter()
        .find(|fill| fill.market.code() == "KRW-BTC")
        .expect("btc fill");
    assert_eq!(btc_fill.notional.get(), Decimal::from(400));
    // XRP is capped by the gross exposure headroom that BTC left.
    let xrp_fill = first_decisions
        .iter()
        .find(|fill| fill.market.code() == "KRW-XRP")
        .expect("xrp fill");
    assert_eq!(xrp_fill.notional.get(), Decimal::from(200));
}

#[test]
fn rejected_signals_keep_typed_reasons() {
    // Gross cap 0.45 with BTC max 0.45 leaves zero headroom for XRP.
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.45"), ("KRW-XRP", "0.30")],
        "0.45",
        "0.00",
        "0",
        ArbitrationPolicy::Priority,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    assert!(
        ledger
            .rejections
            .iter()
            .any(|rejection| rejection.market.code() == "KRW-XRP"
                && rejection.reason == PortfolioRejectionReason::GrossExposureCap),
        "gross cap rejection is recorded with its typed reason: {:?}",
        ledger.rejections
    );
    assert!(ledger.totals.rejected_signals >= 1);
    assert!(
        ledger
            .totals
            .rejection_reasons
            .contains_key("GROSS_EXPOSURE_CAP")
    );
}

#[test]
fn cash_reserve_blocks_the_last_buy_before_the_pool_runs_dry() {
    // Reserve 0.20 keeps 200 KRW free; BTC max 0.80 would want 800, but the
    // reserve headroom (1000-200=800) is exactly consumed by the cap.
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.80"), ("KRW-XRP", "0.30")],
        "0.80",
        "0.20",
        "0",
        ArbitrationPolicy::Priority,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    let btc_buy = ledger
        .fills
        .iter()
        .find(|fill| fill.side == Side::Buy && fill.market.code() == "KRW-BTC")
        .expect("btc buys");
    assert_eq!(btc_buy.notional.get(), Decimal::from(800));
    let after = ledger
        .marks
        .iter()
        .find(|mark| mark.time == btc_buy.execution_time)
        .expect("post-fill mark");
    assert!(
        after.cash.get() >= Decimal::from(200) - decimal("0.0001"),
        "cash reserve holds after the buy: {}",
        after.cash.get()
    );
    assert!(
        ledger
            .rejections
            .iter()
            .any(|rejection| rejection.market.code() == "KRW-XRP"
                && rejection.reason == PortfolioRejectionReason::GrossExposureCap),
        "xrp cannot buy inside the gross cap"
    );
}

#[test]
fn drawdown_stop_blocks_new_buys_after_a_loss() {
    // BTC falls 10 percent after entry; the stop blocks the XRP entry that
    // was previously blocked only by the gross cap.
    let btc_prices = [
        "100", "100", "100", "100", "100", "90", "90", "90", "90", "90",
    ];
    let btc = dataset("KRW-BTC", &btc_prices);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.80"), ("KRW-XRP", "0.30")],
        "0.80",
        "0.20",
        "0.05",
        ArbitrationPolicy::Priority,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    // The drawdown triggers when BTC marks 10 percent below the equity peak;
    // weight-restoring rebuys (BTC or XRP) are then blocked with the typed reason.
    let first_stop_time = ledger
        .rejections
        .iter()
        .find(|rejection| rejection.reason == PortfolioRejectionReason::PortfolioStop)
        .expect("drawdown stop rejects new buys")
        .decision_time;
    assert!(
        ledger
            .marks
            .iter()
            .any(|mark| mark.stopped && mark.drawdown.get() >= decimal("0.05")),
        "stopped marks expose the active drawdown state"
    );
    assert!(
        ledger
            .fills
            .iter()
            .filter(|fill| fill.side == Side::Buy)
            .all(|fill| fill.decision_time < first_stop_time),
        "no buy fills after the stop activates"
    );
}

#[test]
fn pro_rata_scaling_is_deterministic_and_never_overdraws() {
    // Two equal-cap assets compete for one constrained pool; pro-rata gives
    // each the same share of the available headroom.
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.45"), ("KRW-XRP", "0.45")],
        "0.60",
        "0.00",
        "0",
        ArbitrationPolicy::ProRata,
    );
    let ledger = run(&plan, &[btc, xrp], &spec);
    let first: Vec<_> = ledger
        .fills
        .iter()
        .filter(|fill| fill.side == Side::Buy && fill.decision_time == time("2025-01-01T02:00:00Z"))
        .collect();
    assert_eq!(first.len(), 2, "both buys scale into the pool");
    let btc_notional = first
        .iter()
        .find(|fill| fill.market.code() == "KRW-BTC")
        .expect("btc")
        .notional
        .get();
    let xrp_notional = first
        .iter()
        .find(|fill| fill.market.code() == "KRW-XRP")
        .expect("xrp")
        .notional
        .get();
    assert_eq!(
        btc_notional, xrp_notional,
        "equal caps and equal targets scale to equal shares"
    );
    let total: Decimal = first
        .iter()
        .map(|fill| fill.notional.get() + fill.fee.get())
        .sum();
    assert!(total <= Decimal::from(1000));
}

#[test]
fn arbitration_is_reproducible_across_runs() {
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.40"), ("KRW-XRP", "0.40")],
        "0.60",
        "0.00",
        "0",
        ArbitrationPolicy::ScoreRanked,
    );
    let first = run(&plan, &[btc.clone(), xrp.clone()], &spec);
    let second = run(&plan, &[btc, xrp], &spec);
    assert_eq!(first.fills.len(), second.fills.len());
    for (left, right) in first.fills.iter().zip(&second.fills) {
        assert_eq!(left.market, right.market);
        assert_eq!(left.notional, right.notional);
        assert_eq!(left.price, right.price);
    }
}

#[test]
fn fee_reservation_never_overdraws_the_pool() {
    let btc_prices = [
        "100", "100", "100", "100", "100", "100", "100", "100", "100", "100",
    ];
    let mut btc = dataset("KRW-BTC", &btc_prices);
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let mut plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    // 100 bps buy fee and 50 bps price cost: the reservation must absorb both.
    plan.spec.costs.buy_fee_bps = BasisPoints::new(decimal("100")).expect("fee");
    plan.spec.costs.sell_fee_bps = BasisPoints::new(decimal("100")).expect("fee");
    plan.spec.costs.half_spread_bps = BasisPoints::new(decimal("50")).expect("cost");
    plan.spec.costs.assumption_label = "fee reservation fixture".into();
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.80"), ("KRW-XRP", "0.30")],
        "0.80",
        "0.00",
        "0",
        ArbitrationPolicy::Priority,
    );
    let _ = &mut btc;
    let ledger = run(&plan, &[btc, xrp], &spec);
    for mark in &ledger.marks {
        assert!(mark.cash.get() >= Decimal::ZERO, "fee never overdraws cash");
    }
    let btc_buy = ledger
        .fills
        .iter()
        .find(|fill| fill.side == Side::Buy)
        .expect("buy with fees");
    assert_eq!(btc_buy.fee_bps.get(), decimal("100"), "PIT fee applies");
    assert!(
        btc_buy.price.get() > Decimal::from(100),
        "price cost raises the buy price: {}",
        btc_buy.price.get()
    );
    assert!(
        btc_buy.cash_after.get() >= Decimal::ZERO,
        "cash after the fee-laden buy stays nonnegative"
    );
}

#[test]
fn benchmarks_are_comparable_on_the_same_period() {
    let btc = dataset(
        "KRW-BTC",
        &[
            "100", "100", "100", "110", "110", "110", "110", "110", "110", "110",
        ],
    );
    let xrp = dataset("KRW-XRP", &FLAT_PRICES);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "300000",
        &[("KRW-BTC", "0.45"), ("KRW-XRP", "0.30")],
        "0.70",
        "0.20",
        "0.015",
        ArbitrationPolicy::Priority,
    );
    let benchmarks = portfolio_benchmarks(&plan, &[btc, xrp], &spec).expect("benchmarks");
    assert_eq!(benchmarks.len(), 4, "cash, BTC B&H, XRP B&H, static");
    let cash = benchmarks
        .iter()
        .find(|benchmark| benchmark.kind == BenchmarkKind::Cash)
        .expect("cash benchmark");
    assert_eq!(cash.terminal_equity.get(), Decimal::from(300_000));
    let btc_bh = benchmarks
        .iter()
        .find(|benchmark| {
            benchmark.kind == BenchmarkKind::BuyAndHold
                && benchmark
                    .market
                    .as_ref()
                    .is_some_and(|m| m.code() == "KRW-BTC")
        })
        .expect("btc benchmark");
    assert_eq!(
        btc_bh.total_return,
        decimal("0.1"),
        "10 percent price rise on the shared range"
    );
    let static_bench = benchmarks
        .iter()
        .find(|benchmark| benchmark.kind == BenchmarkKind::StaticAllocation)
        .expect("static benchmark");
    assert_eq!(
        static_bench.total_return,
        decimal("0.05"),
        "equal-weight half of 10 percent"
    );
}

#[test]
fn misaligned_market_grids_fail_closed() {
    // XRP misses one decision close: shared arbitration is undefined.
    let mut xrp_prices = FLAT_PRICES.to_vec();
    xrp_prices[4] = "100";
    let btc = dataset("KRW-BTC", &FLAT_PRICES);
    let mut xrp = dataset("KRW-XRP", &xrp_prices);
    xrp.observations.remove(4);
    xrp.manifest.row_count = u64::try_from(xrp.observations.len()).unwrap_or(u64::MAX);
    let plan = plan(&["KRW-BTC", "KRW-XRP"], StrategySpec::BuyAndHold);
    let spec = portfolio_spec(
        "1000",
        &[("KRW-BTC", "0.45"), ("KRW-XRP", "0.30")],
        "0.70",
        "0.20",
        "0",
        ArbitrationPolicy::Priority,
    );
    let result = run_portfolio(
        &plan,
        &[btc, xrp],
        &spec,
        None,
        &RunId::new("run-misaligned").expect("run id"),
        100,
        &|| false,
    );
    assert!(matches!(result, Err(LabError::DataGap(_))));
}

#[test]
fn regime_gating_changes_the_executed_path() {
    // A steady rise: always-on S2 enters at the first decision while the
    // TREND_UP-only gate waits for the classifier warmup, so the executed
    // paths differ (entry price and marked equity).
    let path = [
        "100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111", "112",
        "113", "114", "115", "116", "117", "118", "119",
    ];
    let xrp = dataset("KRW-XRP", &path);
    let mut plan = plan(
        &["KRW-XRP"],
        StrategySpec::S2 {
            entry_length: 2,
            exit_length: 2,
        },
    );
    // Four decision closes of warmup exist before the evaluation start.
    plan.spec.range = UtcRange::new(time("2025-01-01T06:00:00Z"), time("2025-01-01T10:00:00Z"))
        .expect("gated range");
    plan.spec.terminal_policy = TerminalPolicy::MarkToMarket;
    let always_on = portfolio_spec(
        "300000",
        &[("KRW-XRP", "1")],
        "1",
        "0",
        "0",
        ArbitrationPolicy::Priority,
    );
    let gated_spec = RegimeGateSpec {
        classifier: crate::contracts::RegimeClassifierSpec {
            revision: "test-gate-v1".into(),
            sma_long: 6,
            sma_mid: 2,
            sma_short: 3,
            slope_lookback: 2,
            vol_lookback: 4,
            atr_lookback: 3,
            high_vol_annualized: None,
            low_vol_annualized: None,
        },
        rules: crate::contracts::RegimeGateRule {
            trend_up: crate::contracts::RegimeGateAction::Enabled,
            trend_down: crate::contracts::RegimeGateAction::Disabled,
            chop: crate::contracts::RegimeGateAction::Disabled,
            unknown: crate::contracts::RegimeGateAction::Disabled,
        },
    };
    let always = run(&plan, std::slice::from_ref(&xrp), &always_on);
    let gated = run_portfolio(
        &plan,
        std::slice::from_ref(&xrp),
        &always_on,
        Some(&gated_spec),
        &RunId::new("run-gated").expect("run id"),
        100,
        &|| false,
    )
    .expect("gated run completes");
    assert!(
        !gated.regime_observations.is_empty(),
        "gating records regime observations"
    );
    assert!(
        gated
            .regime_observations
            .iter()
            .any(|observation| observation.regime == RegimeLabel::TrendUp),
        "the rising segment labels TREND_UP"
    );
    // The gated run must not buy while the label is not TREND_UP.
    let up_times: std::collections::BTreeSet<_> = gated
        .regime_observations
        .iter()
        .filter(|observation| observation.regime == RegimeLabel::TrendUp)
        .map(|observation| observation.decision_time)
        .collect();
    for fill in &gated.fills {
        if fill.side == Side::Buy {
            assert!(
                up_times.contains(&fill.decision_time),
                "gated buys happen only in TREND_UP (fill at {} not in {up_times:?}; labels {:?})",
                fill.decision_time,
                gated
                    .regime_observations
                    .iter()
                    .map(|observation| (observation.decision_time, observation.regime))
                    .collect::<Vec<_>>()
            );
        }
    }
    // Turnover drops when the gate exits at the trend transition.
    assert!(
        gated.totals.turnover <= always.totals.turnover,
        "gating reduces churn: gated={} always={}",
        gated.totals.turnover,
        always.totals.turnover
    );
    // Both runs reconcile internally; results differ.
    assert_eq!(always.status, PortfolioStatus::Completed);
    assert_eq!(gated.status, PortfolioStatus::Completed);
    assert_ne!(
        always.totals.terminal_equity, gated.totals.terminal_equity,
        "gating changes the executed path on a trending-then-falling market"
    );
}
