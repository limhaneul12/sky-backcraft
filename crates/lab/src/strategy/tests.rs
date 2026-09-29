use super::*;
use crate::contracts::{ContentHash, ObservationId, RawObjectId, StateParameters};
use chrono::{Duration, TimeZone, Utc};

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("synthetic market")
}
fn time(hour: i64) -> UtcTimestamp {
    let hour = u32::try_from(hour).expect("fixture hour fits u32");
    UtcTimestamp(Utc.with_ymd_and_hms(2024, 1, 1, hour, 0, 0).unwrap())
}

fn fixture_index(index: usize) -> i64 {
    i64::try_from(index).expect("fixture index fits i64")
}
fn decimal(value: i64) -> Decimal {
    Decimal::from(value)
}
fn price(value: i64) -> PriceKrw {
    PriceKrw::new(decimal(value)).expect("synthetic price")
}
fn cash() -> PositionView {
    PositionView {
        actual_qty: AssetQuantity::new(Decimal::ZERO).unwrap(),
        actual_weight: Weight::new(Decimal::ZERO).unwrap(),
        episode_opened_at: None,
        held_decision_bars: None,
    }
}
fn long(opened: UtcTimestamp, held: u64, weight: &str) -> PositionView {
    PositionView {
        actual_qty: AssetQuantity::new(Decimal::ONE).unwrap(),
        actual_weight: Weight::new(Decimal::from_str(weight).unwrap()).unwrap(),
        episode_opened_at: Some(opened),
        held_decision_bars: Some(held),
    }
}

/// `SYNTHETIC_TEST_ONLY`: hand-authored OHLC, never market-performance evidence.
fn observation(index: i64, close: i64, high: i64, low: i64) -> CandleObservation {
    let open_time = time(index);
    CandleObservation {
        id: ObservationId::new(format!("obs-{index}")).unwrap(),
        candle: crate::contracts::CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: open_time,
            close_time_utc: UtcTimestamp(open_time.0 + Duration::hours(1)),
            open: price(close),
            high: price(high),
            low: price(low),
            close: price(close),
            volume: AssetQuantity::new(Decimal::ONE).unwrap(),
            quote_turnover: crate::contracts::QuoteAmount::new(decimal(close)).unwrap(),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(format!("synthetic-{index}").as_bytes()),
        raw_object_ids: vec![RawObjectId::new(format!("raw-{index}")).unwrap()],
        constituent_ids: Vec::new(),
    }
}

fn run(
    spec: StrategySpec,
    closes: &[(i64, i64, i64)],
    final_position: PositionView,
) -> StrategyEvaluation {
    let mut evaluator = StrategyEvaluator::new(spec, market(), CandleInterval::H1).unwrap();
    let last = closes.len() - 1;
    let mut result = None;
    for (index, &(close, high, low)) in closes.iter().enumerate() {
        result = evaluator
            .observe(
                &observation(fixture_index(index), close, high, low),
                if index == last {
                    final_position
                } else {
                    cash()
                },
                None,
            )
            .unwrap();
    }
    result.expect("post-warmup strategy decision")
}

#[test]
fn hand_traced_strategies_obey_strict_boundaries_and_real_holding_context() {
    assert_state_and_breakout_boundaries();
    assert_pullback_holding_context();
    assert_volatility_and_buy_hold_boundaries();
}

fn state_parameters() -> StateParameters {
    StateParameters {
        ema_length: 2,
        vol_length: 2,
        k: 0.0,
    }
}

fn assert_state_and_breakout_boundaries() {
    let state = state_parameters();
    let s1 = run(
        StrategySpec::S1 {
            state: state.clone(),
        },
        &[(100, 101, 99), (100, 101, 99), (110, 111, 109)],
        cash(),
    );
    assert_eq!(s1.state_after, PositionState::Long);
    assert_eq!(s1.reasons, vec![ReasonCode::BandEntry]);

    let s1_equality = run(
        StrategySpec::S1 {
            state: state.clone(),
        },
        &[(100, 101, 99), (100, 101, 99), (100, 101, 99)],
        cash(),
    );
    assert_eq!(s1_equality.state_after, PositionState::Cash);
    assert_eq!(
        s1_equality.reasons,
        vec![ReasonCode::HoldState, ReasonCode::NoAction]
    );

    let s2 = run(
        StrategySpec::S2 {
            entry_length: 2,
            exit_length: 1,
        },
        &[(100, 101, 99), (101, 102, 100), (103, 104, 102)],
        cash(),
    );
    assert_eq!(s2.indicators.entry_high.unwrap(), price(102));
    assert_eq!(s2.reasons, vec![ReasonCode::Breakout]);
    let s2_equality = run(
        StrategySpec::S2 {
            entry_length: 2,
            exit_length: 1,
        },
        &[(100, 101, 99), (101, 102, 100), (102, 200, 101)],
        cash(),
    );
    assert_eq!(s2_equality.state_after, PositionState::Cash);
}

fn assert_pullback_holding_context() {
    let s3_entry = run(
        StrategySpec::S3 {
            trend_length: 2,
            rsi_length: 2,
            entry_threshold: 36.0,
            exit_threshold: 55.0,
            max_holding_bars: 2,
            signal_expiry_bars: 2,
        },
        &[(100, 101, 99), (80, 81, 79), (91, 92, 90)],
        cash(),
    );
    assert_eq!(s3_entry.state_after, PositionState::Long);
    assert_eq!(s3_entry.reasons, vec![ReasonCode::TrendPullbackEntry]);

    let s3_spec = StrategySpec::S3 {
        trend_length: 2,
        rsi_length: 2,
        entry_threshold: 10.0,
        exit_threshold: 20.0,
        max_holding_bars: 2,
        signal_expiry_bars: 2,
    };
    let s3 = run(
        s3_spec,
        &[
            (200, 201, 199),
            (100, 101, 99),
            (120, 121, 119),
            (129, 130, 128),
        ],
        long(time(2), 2, "1"),
    );
    assert_eq!(
        s3.reasons,
        vec![
            ReasonCode::TrendInvalidated,
            ReasonCode::RsiRecovery,
            ReasonCode::HoldingTimeout
        ]
    );
    assert_eq!(s3.state_after, PositionState::Cash);

    // The same bar with an unfilled prior signal stays cash and cannot timeout.
    let unfilled = run(
        StrategySpec::S3 {
            trend_length: 2,
            rsi_length: 2,
            entry_threshold: 10.0,
            exit_threshold: 20.0,
            max_holding_bars: 2,
            signal_expiry_bars: 2,
        },
        &[
            (200, 201, 199),
            (100, 101, 99),
            (120, 121, 119),
            (129, 130, 128),
        ],
        cash(),
    );
    assert!(!unfilled.reasons.contains(&ReasonCode::HoldingTimeout));
}

fn assert_volatility_and_buy_hold_boundaries() {
    let state = state_parameters();
    let s4 = run(
        StrategySpec::S4 {
            state: state.clone(),
            target_annual_vol: 0.4,
            vol_floor: 0.1,
            rebalance_band: Weight::new(Decimal::ONE).unwrap(),
        },
        &[(100, 101, 99), (100, 101, 99), (110, 111, 109)],
        cash(),
    );
    assert_eq!(s4.constrained_target_weight.get(), Decimal::ZERO);
    assert!(s4.reasons.contains(&ReasonCode::RebalanceBand));

    let buy_hold = run(StrategySpec::BuyAndHold, &[(100, 101, 99)], cash());
    assert_eq!(buy_hold.raw_target_weight.get(), Decimal::ONE);
    assert_eq!(buy_hold.reasons, vec![ReasonCode::StartFromCash]);
}

#[test]
fn s5_and_matched_control_share_unavailable_cash_boundary() {
    let state = StateParameters {
        ema_length: 2,
        vol_length: 2,
        k: 0.0,
    };
    let evidence = EvidenceEvaluator::new(None, crate::contracts::PitPolicy::StrictPit).unwrap();
    for spec in [
        StrategySpec::S5 {
            state: state.clone(),
        },
        StrategySpec::S1CoverageControl {
            state: state.clone(),
        },
    ] {
        let mut evaluator = StrategyEvaluator::new(spec, market(), CandleInterval::H1).unwrap();
        let mut result = None;
        for (index, close) in [100, 100, 110].into_iter().enumerate() {
            result = evaluator
                .observe(
                    &observation(fixture_index(index), close, close + 1, close - 1),
                    cash(),
                    Some(&evidence),
                )
                .unwrap();
        }
        let decision = result.unwrap();
        assert_eq!(decision.raw_target_weight.get(), Decimal::ONE);
        assert_eq!(decision.constrained_target_weight.get(), Decimal::ZERO);
        assert_eq!(decision.state_after, PositionState::Cash);
        assert!(decision.reasons.contains(&ReasonCode::EvidenceUnavailable));
        assert!(!decision.evidence_effect.unwrap().coverage_available);
    }
}

#[test]
fn evidence_zero_does_not_reset_the_latent_base_signal_state() {
    let evidence = EvidenceEvaluator::new(None, crate::contracts::PitPolicy::StrictPit).unwrap();
    let mut evaluator = StrategyEvaluator::new(
        StrategySpec::S5 {
            state: StateParameters {
                ema_length: 2,
                vol_length: 2,
                k: 0.1,
            },
        },
        market(),
        CandleInterval::H1,
    )
    .unwrap();
    let mut decisions = Vec::new();
    for (index, close) in [100, 100, 120, 115].into_iter().enumerate() {
        if let Some(decision) = evaluator
            .observe(
                &observation(fixture_index(index), close, close + 1, close - 1),
                cash(),
                Some(&evidence),
            )
            .unwrap()
        {
            decisions.push(decision);
        }
    }
    assert_eq!(decisions[0].raw_target_weight.get(), Decimal::ONE);
    assert_eq!(decisions[0].constrained_target_weight.get(), Decimal::ZERO);
    assert_eq!(decisions[1].raw_target_weight.get(), Decimal::ONE);
    assert_eq!(decisions[1].constrained_target_weight.get(), Decimal::ZERO);
    assert_eq!(decisions[1].base_state_before, Some(PositionState::Long));
    assert_eq!(decisions[1].base_state_after, Some(PositionState::Long));
}

#[test]
fn future_suffix_does_not_change_existing_prefix_decisions() {
    let spec = StrategySpec::S1 {
        state: StateParameters {
            ema_length: 2,
            vol_length: 2,
            k: 0.5,
        },
    };
    let prefix = [
        (100, 101, 99),
        (105, 106, 104),
        (90, 91, 89),
        (110, 111, 109),
    ];
    let mut first = StrategyEvaluator::new(spec.clone(), market(), CandleInterval::H1).unwrap();
    let mut second = StrategyEvaluator::new(spec, market(), CandleInterval::H1).unwrap();
    let mut first_outputs = Vec::new();
    let mut second_outputs = Vec::new();
    for (index, &(close, high, low)) in prefix.iter().enumerate() {
        if let Some(value) = first
            .observe(
                &observation(fixture_index(index), close, high, low),
                cash(),
                None,
            )
            .unwrap()
        {
            first_outputs.push((value.state_after, value.raw_target_weight, value.reasons));
        }
        if let Some(value) = second
            .observe(
                &observation(fixture_index(index), close, high, low),
                cash(),
                None,
            )
            .unwrap()
        {
            second_outputs.push((value.state_after, value.raw_target_weight, value.reasons));
        }
    }
    second
        .observe(&observation(4, 1_000, 1_001, 999), cash(), None)
        .unwrap();
    assert_eq!(first_outputs, second_outputs);
}

#[test]
fn state_policy_hysteresis_advances_without_a_phantom_filled_position() {
    let mut evaluator = StrategyEvaluator::new(
        StrategySpec::S1 {
            state: StateParameters {
                ema_length: 2,
                vol_length: 2,
                k: 0.0,
            },
        },
        market(),
        CandleInterval::H1,
    )
    .unwrap();
    let mut decisions = Vec::new();
    for (index, close) in [100, 100, 110, 90].into_iter().enumerate() {
        if let Some(decision) = evaluator
            .observe(
                &observation(fixture_index(index), close, close + 1, close - 1),
                cash(),
                None,
            )
            .unwrap()
        {
            decisions.push(decision);
        }
    }
    assert_eq!(decisions[0].reasons, vec![ReasonCode::BandEntry]);
    assert_eq!(decisions[0].state_before, PositionState::Cash);
    assert_eq!(decisions[0].state_after, PositionState::Long);
    assert_eq!(decisions[1].reasons, vec![ReasonCode::BandExit]);
    assert_eq!(decisions[1].state_before, PositionState::Long);
    assert_eq!(decisions[1].state_after, PositionState::Cash);
}

#[test]
fn noncontiguous_input_and_inconsistent_position_fail_closed() {
    let mut evaluator =
        StrategyEvaluator::new(StrategySpec::BuyAndHold, market(), CandleInterval::H1).unwrap();
    evaluator
        .observe(&observation(0, 100, 101, 99), cash(), None)
        .unwrap();
    assert!(matches!(
        evaluator.observe(&observation(2, 100, 101, 99), cash(), None),
        Err(LabError::DataGap(_))
    ));
    let mut evaluator =
        StrategyEvaluator::new(StrategySpec::BuyAndHold, market(), CandleInterval::H1).unwrap();
    let invalid = PositionView {
        actual_qty: AssetQuantity::new(Decimal::ONE).unwrap(),
        ..cash()
    };
    assert!(matches!(
        evaluator.observe(&observation(0, 100, 101, 99), invalid, None),
        Err(LabError::InvalidConfig(_))
    ));
}
