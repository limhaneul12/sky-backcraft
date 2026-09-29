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
            },
            PolicyIndicator {
                id: "turnover".into(),
                indicator: PolicyIndicatorKind::QuoteTurnover,
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
