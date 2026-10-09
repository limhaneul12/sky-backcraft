use super::*;
use crate::contracts::policy::{PolicyIndicator, PolicyRule, PolicySignalExpiry, PolicyState};
use crate::contracts::{
    AssetQuantity, CandleRecord, ContentHash, ObservationId, PriceKrw, QuoteAmount, RawObjectId,
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

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("market")
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
    };
    let mut evaluator =
        RulesEvaluator::new(&definition(program), market(), CandleInterval::H1).expect("compile");
    evaluator
        .observe(&observation(0, 90), cash(), None)
        .expect("first");
    evaluator
        .observe(&observation(1, 110), cash(), None)
        .expect("second");
    let third_bar = observation(2, 90);
    let result = evaluator
        .observe(&third_bar, long(third_bar.candle.open_time_utc), None)
        .expect("third")
        .expect("ready");
    assert_eq!(result.trace.matched_rule_id.as_deref(), Some("cross"));
    assert_eq!(result.target.get(), Decimal::ONE);
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

fn none_target() -> Option<Decimal> {
    None
}
