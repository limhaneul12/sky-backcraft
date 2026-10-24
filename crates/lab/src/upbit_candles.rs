//! Pure normalization of public Upbit candle pages shared by the REST adapter
//! and durable raw-evidence validation.

use crate::contracts::{
    AssetQuantity, CandleInterval, CandleRecord, LabError, MarketId, PriceKrw, QuoteAmount,
    UtcTimestamp,
};
use serde::Deserialize;

/// Wire DTO for Upbit candle endpoints. Unknown exchange fields are ignored;
/// JSON numbers remain exact through the repository's arbitrary-precision setting.
#[derive(Debug, Deserialize)]
struct UpbitCandleWire {
    market: String,
    candle_date_time_utc: String,
    opening_price: serde_json::Number,
    high_price: serde_json::Number,
    low_price: serde_json::Number,
    trade_price: serde_json::Number,
    candle_acc_trade_volume: serde_json::Number,
    candle_acc_trade_price: serde_json::Number,
}

/// Decode one bounded public REST page, newest candle first.
///
/// # Errors
/// Rejects malformed JSON, more than 200 records, mismatched markets,
/// unaligned or repeated timestamps, out-of-order rows, and invalid decimals.
pub(crate) fn decode_candles(
    raw: &[u8],
    market: &MarketId,
    interval: CandleInterval,
    fetched_at: UtcTimestamp,
) -> Result<Vec<CandleRecord>, LabError> {
    let wire: Vec<UpbitCandleWire> = serde_json::from_slice(raw)?;
    if wire.len() > 200 {
        return Err(LabError::ContractParse(
            "Upbit returned more than 200 candles".into(),
        ));
    }
    let step = interval.duration();
    let expected_market = market.code();
    let mut records = Vec::with_capacity(wire.len());
    for candle in wire {
        if candle.market != expected_market {
            return Err(LabError::ContractParse(format!(
                "candle market {} does not match requested {expected_market}",
                candle.market
            )));
        }
        let open = chrono::NaiveDateTime::parse_from_str(
            &candle.candle_date_time_utc,
            "%Y-%m-%dT%H:%M:%S",
        )
        .map_err(|error| {
            LabError::ContractParse(format!(
                "candle_date_time_utc {:?}: {error}",
                candle.candle_date_time_utc
            ))
        })?
        .and_utc();
        if open.timestamp().rem_euclid(step.num_seconds()) != 0 {
            return Err(LabError::ContractParse(format!(
                "unaligned candle start: {open}"
            )));
        }
        let close_time = open
            .checked_add_signed(step)
            .ok_or_else(|| LabError::ContractParse("candle close timestamp out of range".into()))?;
        records.push(CandleRecord {
            market: expected_market.clone(),
            interval,
            open_time_utc: UtcTimestamp(open),
            close_time_utc: UtcTimestamp(close_time),
            open: PriceKrw::from_json_number(&candle.opening_price)?,
            high: PriceKrw::from_json_number(&candle.high_price)?,
            low: PriceKrw::from_json_number(&candle.low_price)?,
            close: PriceKrw::from_json_number(&candle.trade_price)?,
            volume: AssetQuantity::from_json_number(&candle.candle_acc_trade_volume)?,
            quote_turnover: QuoteAmount::from_json_number(&candle.candle_acc_trade_price)?,
            completed: close_time <= fetched_at.0,
        });
    }
    if records
        .windows(2)
        .any(|pair| pair[0].open_time_utc <= pair[1].open_time_utc)
    {
        return Err(LabError::ContractParse(
            "candles must be distinct and newest first".into(),
        ));
    }
    Ok(records)
}
