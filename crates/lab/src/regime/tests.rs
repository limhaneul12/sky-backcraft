use super::*;
use crate::contracts::{
    AssetQuantity, CandleRecord, ObservationId, PriceKrw, QuoteAmount, RegimeClassifierSpec,
    RegimeGateAction, RegimeGateRule, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use std::str::FromStr;

fn market() -> MarketId {
    MarketId::parse_upbit("KRW-BTC").expect("fixture market")
}

fn spec() -> RegimeClassifierSpec {
    RegimeClassifierSpec {
        revision: "test-rule-v1".into(),
        sma_long: 6,
        sma_mid: 2,
        sma_short: 3,
        slope_lookback: 2,
        vol_lookback: 4,
        atr_lookback: 3,
        high_vol_annualized: Some(4.0),
        low_vol_annualized: Some(0.5),
    }
}

fn classifier() -> RegimeClassifier {
    RegimeClassifier::new(
        spec(),
        market(),
        ContentHash::of_bytes(b"regime-test-dataset"),
    )
}

fn bar(index: usize, close: &str) -> CandleObservation {
    let open_time = UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z")
        .expect("base time")
        .0
        + chrono::Duration::hours(i64::try_from(index).unwrap_or(0));
    let close_price = PriceKrw::new(Decimal::from_str(close).expect("price")).expect("price");
    CandleObservation {
        id: ObservationId::new(format!("regime-{index}")).expect("observation id"),
        candle: CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: UtcTimestamp(open_time),
            close_time_utc: UtcTimestamp(open_time + chrono::Duration::hours(1)),
            open: close_price,
            high: close_price,
            low: close_price,
            close: close_price,
            volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
            quote_turnover: QuoteAmount::new(Decimal::ONE).expect("turnover"),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(format!("regime-{index}").as_bytes()),
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

#[test]
fn warmup_produces_no_label_and_replay_is_deterministic() {
    let mut rising = classifier();
    let mut labels = Vec::new();
    for index in 0..8 {
        let observation = rising
            .observe(&bar(index, &(100 + index).to_string()), CandleInterval::H1)
            .expect("bars observe");
        if index < 7 {
            assert!(observation.is_none(), "bar {index} is inside warmup");
        } else {
            let observation = observation.expect("warmup complete");
            assert_eq!(observation.regime, RegimeLabel::TrendUp);
            assert_eq!(observation.classifier_revision, "test-rule-v1");
            labels.push(observation);
        }
    }
    // Identical input replay produces identical observations.
    let mut replay = classifier();
    for index in 0..8 {
        if let Some(observation) = replay
            .observe(&bar(index, &(100 + index).to_string()), CandleInterval::H1)
            .expect("bars observe")
        {
            assert_eq!(observation, labels[0]);
        }
    }
}

#[test]
fn trends_and_chop_transition_deterministically() {
    let path = [
        "100", "101", "102", "103", "104", "105", "106", "107", "100", "95", "90", "85", "80",
        "75", "70", "65", "65", "65", "65", "65", "65", "65", "65", "65",
    ];
    let run = |labels: &mut Vec<RegimeLabel>| {
        let mut classifier = classifier();
        for (index, close) in path.iter().enumerate() {
            if let Some(observation) = classifier
                .observe(&bar(index, close), CandleInterval::H1)
                .expect("bars observe")
            {
                labels.push(observation.regime);
            }
        }
    };
    let mut labels = Vec::new();
    run(&mut labels);
    assert!(
        labels.contains(&RegimeLabel::TrendUp),
        "ramp labels up: {labels:?}"
    );
    assert!(
        labels.contains(&RegimeLabel::TrendDown),
        "fall labels down: {labels:?}"
    );
    assert!(
        labels.contains(&RegimeLabel::Chop),
        "flat labels chop: {labels:?}"
    );
    let mut replayed = Vec::new();
    run(&mut replayed);
    assert_eq!(labels, replayed, "label sequence is deterministic");
}

#[test]
fn gating_rules_map_labels_to_actions() {
    let spec = RegimeGateSpec {
        classifier: RegimeClassifierSpec {
            revision: "test-rule-v1".into(),
            sma_long: 6,
            sma_mid: 2,
            sma_short: 3,
            slope_lookback: 2,
            vol_lookback: 4,
            atr_lookback: 3,
            high_vol_annualized: None,
            low_vol_annualized: None,
        },
        rules: RegimeGateRule {
            trend_up: RegimeGateAction::Enabled,
            trend_down: RegimeGateAction::ReducedExposure {
                max_weight: Weight::new(Decimal::new(3, 1)).expect("weight"),
            },
            chop: RegimeGateAction::Disabled,
            unknown: RegimeGateAction::Disabled,
        },
    };
    spec.validate().expect("gate spec validates");
    assert!(gate_action(&spec, RegimeLabel::TrendUp).allows_entries());
    assert!(!gate_action(&spec, RegimeLabel::Chop).allows_entries());
    assert_eq!(
        gate_action(&spec, RegimeLabel::TrendDown)
            .exposure_cap()
            .map(Weight::get),
        Some(Decimal::new(3, 1))
    );
    // Unknown warmup defaults to the frozen unknown rule, never an entry.
    assert!(!gate_action(&spec, RegimeLabel::Unknown).allows_entries());
    // Vol thresholds must come in pairs.
    let mut broken = spec.classifier.clone();
    broken.high_vol_annualized = Some(4.0);
    assert!(broken.validate().is_err());
}

#[test]
fn future_suffix_never_changes_prefix_labels() {
    // The classifier is streaming: appending bars after the evaluation window
    // cannot alter labels inside it (the PIT guarantee behind
    // selection/evaluation isolation).
    let prefix = [
        "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
    ];
    let run_prefix = |suffix: &[&str]| {
        let mut classifier = classifier();
        let mut labels = Vec::new();
        for (index, close) in prefix.iter().chain(suffix.iter()).enumerate() {
            if let Some(observation) = classifier
                .observe(&bar(index, close), CandleInterval::H1)
                .expect("bars observe")
                && observation.decision_time.0
                    < UtcTimestamp::parse_rfc3339("2024-01-01T09:00:00Z")
                        .expect("cut time")
                        .0
            {
                labels.push(observation);
            }
        }
        labels
    };
    let alone = run_prefix(&[]);
    let with_future = run_prefix(&["50", "40", "30", "20", "10"]);
    assert_eq!(alone, with_future);
}
