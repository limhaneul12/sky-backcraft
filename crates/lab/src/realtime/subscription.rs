use std::collections::BTreeMap;

use crate::contracts::{CandleInterval, LabError, MarketId, Venue};

/// One canonical market/timeframe requirement contributed by any active runtime session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicMarketSubscription {
    pub market: MarketId,
    pub interval: CandleInterval,
}

impl PublicMarketSubscription {
    #[must_use]
    pub fn new(market: MarketId, interval: CandleInterval) -> Self {
        Self { market, interval }
    }
}

pub(super) fn canonical_subscriptions(
    subscriptions: impl IntoIterator<Item = PublicMarketSubscription>,
    maximum: usize,
) -> Result<Vec<PublicMarketSubscription>, LabError> {
    let mut canonical = Vec::new();
    for subscription in subscriptions {
        if subscription.market.venue != Venue::Upbit {
            return Err(LabError::InvalidConfig(
                "public market feed accepts Upbit market identities only".into(),
            ));
        }
        let code = subscription.market.code();
        let parsed = MarketId::parse_upbit(&code)?;
        if parsed != subscription.market {
            return Err(LabError::InvalidConfig(
                "public feed market identity is not canonical".into(),
            ));
        }
        canonical.push(subscription);
    }
    canonical.sort_by(|left, right| {
        left.market
            .code()
            .cmp(&right.market.code())
            .then_with(|| left.interval.duration().cmp(&right.interval.duration()))
    });
    canonical
        .dedup_by(|left, right| left.market == right.market && left.interval == right.interval);
    if canonical.len() > maximum {
        return Err(LabError::CapacityExceeded(format!(
            "public market subscription union exceeds configured limit {maximum}"
        )));
    }
    Ok(canonical)
}

pub(super) const fn websocket_type_for_interval(interval: CandleInterval) -> Option<&'static str> {
    match interval {
        CandleInterval::M1 => Some("candle.1m"),
        CandleInterval::M5 => Some("candle.5m"),
        CandleInterval::M15 => Some("candle.15m"),
        CandleInterval::H1 => Some("candle.60m"),
        CandleInterval::H4 => Some("candle.240m"),
        CandleInterval::D1 => None,
    }
}

pub(super) fn interval_for_websocket_type(value: &str) -> Option<CandleInterval> {
    match value {
        "candle.1m" => Some(CandleInterval::M1),
        "candle.5m" => Some(CandleInterval::M5),
        "candle.15m" => Some(CandleInterval::M15),
        "candle.60m" => Some(CandleInterval::H1),
        "candle.240m" => Some(CandleInterval::H4),
        _ => None,
    }
}

pub(super) fn websocket_subscription_message(
    subscriptions: &[PublicMarketSubscription],
    ticket: &str,
) -> Result<String, LabError> {
    let mut groups: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for subscription in subscriptions {
        let Some(stream_type) = websocket_type_for_interval(subscription.interval) else {
            continue;
        };
        groups
            .entry(stream_type)
            .or_default()
            .push(subscription.market.code());
    }
    if groups.is_empty() {
        return Err(LabError::InvalidConfig(
            "WebSocket subscription requires at least one supported timeframe".into(),
        ));
    }
    let mut objects = vec![serde_json::json!({"ticket": ticket})];
    for (stream_type, mut codes) in groups {
        codes.sort();
        codes.dedup();
        if codes.len() > 200 {
            return Err(LabError::CapacityExceeded(
                "Upbit accepts at most 200 market codes per candle type".into(),
            ));
        }
        objects.push(serde_json::json!({
            "type": stream_type,
            "codes": codes,
            "is_only_realtime": false
        }));
    }
    objects.push(serde_json::json!({"format": "DEFAULT"}));
    serde_json::to_string(&objects)
        .map_err(|_| LabError::Internal("public feed subscription encoding failed".into()))
}
