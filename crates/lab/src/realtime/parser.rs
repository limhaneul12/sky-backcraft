use chrono::NaiveDateTime;
use serde::Deserialize;
use serde_json::Number;

use crate::contracts::{
    AssetQuantity, CandleRecord, LabError, MarketId, PriceKrw, QuoteAmount, UtcTimestamp,
};

use super::subscription::{PublicMarketSubscription, interval_for_websocket_type};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CandleStreamKind {
    Snapshot,
    Realtime,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedCandleFrame {
    pub subscription: PublicMarketSubscription,
    pub candle: CandleRecord,
    pub provider_at: UtcTimestamp,
    pub received_at: UtcTimestamp,
    pub stream_kind: CandleStreamKind,
}

#[derive(Debug, Deserialize)]
struct UpbitCandleFrameWire {
    #[serde(rename = "type")]
    stream_type: String,
    code: String,
    candle_date_time_utc: String,
    opening_price: Number,
    high_price: Number,
    low_price: Number,
    trade_price: Number,
    candle_acc_trade_volume: Number,
    candle_acc_trade_price: Number,
    timestamp: i64,
    #[serde(rename = "stream_type")]
    stream_type_label: String,
}

pub(super) fn decode_candle_frame(
    raw: &[u8],
    received_at: UtcTimestamp,
    maximum_bytes: usize,
) -> Result<ValidatedCandleFrame, LabError> {
    if raw.len() > maximum_bytes {
        return Err(LabError::CapacityExceeded(format!(
            "public WebSocket message exceeds {maximum_bytes} bytes"
        )));
    }
    let wire: UpbitCandleFrameWire = serde_json::from_slice(raw)?;
    let interval = interval_for_websocket_type(&wire.stream_type)
        .ok_or_else(|| LabError::ContractParse("unsupported Upbit candle WebSocket type".into()))?;
    let stream_kind = match wire.stream_type_label.as_str() {
        "SNAPSHOT" => CandleStreamKind::Snapshot,
        "REALTIME" => CandleStreamKind::Realtime,
        _ => {
            return Err(LabError::ContractParse(
                "unsupported Upbit candle stream kind".into(),
            ));
        }
    };
    let market = MarketId::parse_upbit(&wire.code)?;
    let open = NaiveDateTime::parse_from_str(&wire.candle_date_time_utc, "%Y-%m-%dT%H:%M:%S")
        .map_err(|error| {
            LabError::ContractParse(format!(
                "candle_date_time_utc {:?}: {error}",
                wire.candle_date_time_utc
            ))
        })?
        .and_utc();
    let step = interval.duration();
    if open.timestamp().rem_euclid(step.num_seconds()) != 0 {
        return Err(LabError::ContractParse(
            "unaligned Upbit WebSocket candle start".into(),
        ));
    }
    let provider_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(wire.timestamp)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::ContractParse("provider timestamp is out of range".into()))?;
    if provider_at.0 < open {
        return Err(LabError::ContractParse(
            "provider timestamp precedes the candle open".into(),
        ));
    }
    let close_time = open
        .checked_add_signed(step)
        .ok_or_else(|| LabError::ContractParse("candle close timestamp out of range".into()))?;
    let open_price = PriceKrw::from_json_number(&wire.opening_price)?;
    let high = PriceKrw::from_json_number(&wire.high_price)?;
    let low = PriceKrw::from_json_number(&wire.low_price)?;
    let close = PriceKrw::from_json_number(&wire.trade_price)?;
    let volume = AssetQuantity::from_json_number(&wire.candle_acc_trade_volume)?;
    let quote_turnover = QuoteAmount::from_json_number(&wire.candle_acc_trade_price)?;
    if low.get() > open_price.get()
        || low.get() > close.get()
        || high.get() < open_price.get()
        || high.get() < close.get()
    {
        return Err(LabError::ContractParse(
            "Upbit WebSocket OHLC values are inconsistent".into(),
        ));
    }
    Ok(ValidatedCandleFrame {
        subscription: PublicMarketSubscription::new(market.clone(), interval),
        candle: CandleRecord {
            market: market.code(),
            interval,
            open_time_utc: UtcTimestamp(open),
            close_time_utc: UtcTimestamp(close_time),
            open: open_price,
            high,
            low,
            close,
            volume,
            quote_turnover,
            // A WebSocket timestamp cannot establish interval finality.
            completed: false,
        },
        provider_at,
        received_at,
        stream_kind,
    })
}
