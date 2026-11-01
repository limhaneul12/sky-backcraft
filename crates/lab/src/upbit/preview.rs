//! Upbit Order Preview Engine.
//!
//! Validates:
//! - Market allowlist (`KRW-BTC`, `KRW-ETH`, `KRW-XRP`).
//! - Order side (`bid`, `ask`).
//! - Order types (`limit`, `price`, `market`).
//! - Upbit market buy parameter semantics:
//!   - `price` order type (market buy) requires `price` (KRW total amount) and no `volume`.
//!   - `market` order type (market sell) requires `volume` (coin amount) and no `price`.
//!   - `limit` order type requires both `price` and `volume`.
//! - Minimum notional: 5,000 KRW threshold enforcement.
//! - Upbit KRW tick size quantization and alignment.
//! - Fee estimation (0.05% = 5 bps).
//! - Balance sufficiency against available snapshot.
//! - Cryptographic intent digest generation (SHA-256) and short TTL (60s).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Minimum notional required by Upbit KRW pairs: 5,000 KRW.
pub const MIN_NOTIONAL_KRW: Decimal = Decimal::from_parts(5000, 0, 0, false, 0);
/// Standard Upbit taker/maker fee rate: 0.05% (5 bps).
pub const DEFAULT_UPBIT_FEE_RATE: Decimal = Decimal::from_parts(5, 0, 0, false, 4);
/// Default preview time-to-live: 60 seconds.
pub const PREVIEW_TTL_SECONDS: i64 = 60;
/// Minimum stop distance ratio (`15 bps` = `0.15%`, greater than 3x round-trip 10 bps fee).
pub const MIN_STOP_DISTANCE_RATIO: Decimal = Decimal::from_parts(15, 0, 0, false, 4);
/// Upbit coin volume decimal scale (`0.00000001`).
pub const UPBIT_VOLUME_SCALE: u32 = 8;

/// Preview errors.
#[derive(Debug, thiserror::Error)]
pub enum PreviewError {
    #[error("Disallowed market: '{0}' (allowed: KRW-BTC, KRW-ETH, KRW-XRP)")]
    DisallowedMarket(String),
    #[error("Invalid order side: '{0}' (expected 'bid' or 'ask')")]
    InvalidSide(String),
    #[error("Invalid order type: '{0}' (expected 'limit', 'price', or 'market')")]
    InvalidOrderType(String),
    #[error("Missing price for limit or market-buy order")]
    MissingPrice,
    #[error("Missing volume for limit or market-sell order")]
    MissingVolume,
    #[error("Unexpected volume provided for market-buy ('price') order")]
    UnexpectedVolume,
    #[error("Unexpected price provided for market-sell ('market') order")]
    UnexpectedPrice,
    #[error("Invalid stop-distance risk sizing: {0}")]
    InvalidStopSizing(String),
    #[error("Order notional {notional_krw} KRW is below Upbit minimum {min_krw} KRW")]
    UnderMinimumNotional {
        notional_krw: Decimal,
        min_krw: Decimal,
    },
    #[error("Price {price} does not align with Upbit tick size {tick_size} KRW")]
    InvalidTickSize { price: Decimal, tick_size: Decimal },
    #[error("Insufficient KRW balance: available {available} KRW, required {required} KRW")]
    InsufficientKrw {
        available: Decimal,
        required: Decimal,
    },
    #[error("Insufficient coin balance: available {available}, required {required}")]
    InsufficientCoin {
        available: Decimal,
        required: Decimal,
    },
    #[error("Preview expired at {expired_at}")]
    Expired { expired_at: DateTime<Utc> },
    #[error("Digest mismatch: expected '{expected}', got '{actual}'")]
    DigestMismatch { expected: String, actual: String },
}

/// Request to preview an order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPreviewRequest {
    pub market: String,
    pub side: String,
    pub ord_type: String,
    #[serde(default)]
    pub price: Option<Decimal>,
    #[serde(default)]
    pub volume: Option<Decimal>,
    #[serde(default)]
    pub current_krw_balance: Option<Decimal>,
    #[serde(default)]
    pub current_coin_balance: Option<Decimal>,
    #[serde(default)]
    pub stop_price: Option<Decimal>,
    #[serde(default)]
    pub max_loss_krw: Option<Decimal>,
}

/// Validated order preview output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPreview {
    pub status: String,
    pub preview_digest: String,
    pub created_at_utc: DateTime<Utc>,
    pub valid_until_utc: DateTime<Utc>,
    pub market: String,
    pub side: String,
    pub ord_type: String,
    pub price: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub calculated_notional_krw: Decimal,
    pub estimated_fee_krw: Decimal,
    pub total_required_krw: Decimal,
    pub balance_verified: bool,
}

impl OrderPreview {
    /// Validate input parameters, verify rules, and generate an immutable order preview.
    ///
    /// # Errors
    /// Returns `PreviewError` on any validation failure.
    pub fn build(req: OrderPreviewRequest, now: DateTime<Utc>) -> Result<Self, PreviewError> {
        // 1. Canonical Upbit KRW market check (KRW-<SYMBOL>)
        let market = req.market.to_uppercase();
        if crate::contracts::MarketId::parse_upbit(&market).is_err() {
            return Err(PreviewError::DisallowedMarket(req.market));
        }

        // 2. Side validation
        let side = req.side.to_lowercase();
        if !matches!(side.as_str(), "bid" | "ask") {
            return Err(PreviewError::InvalidSide(req.side));
        }

        // 3. Order type validation and parameter consistency
        let ord_type = req.ord_type.to_lowercase();
        let (price, volume, notional) = match ord_type.as_str() {
            "limit" => {
                let p = req.price.ok_or(PreviewError::MissingPrice)?;
                if p <= Decimal::ZERO {
                    return Err(PreviewError::MissingPrice);
                }
                validate_krw_tick_size(p)?;
                let v =
                    resolve_limit_volume(&side, p, req.volume, req.stop_price, req.max_loss_krw)?;
                let notional = p * v;
                (Some(p), Some(v), notional)
            }
            "price" => {
                if req.stop_price.is_some() || req.max_loss_krw.is_some() {
                    return Err(PreviewError::InvalidStopSizing(
                        "stop_price and max_loss_krw are only supported for limit bid orders"
                            .into(),
                    ));
                }
                // Market buy: price is total KRW amount, volume must be None
                if req.volume.is_some() {
                    return Err(PreviewError::UnexpectedVolume);
                }
                let p = req.price.ok_or(PreviewError::MissingPrice)?;
                if p <= Decimal::ZERO {
                    return Err(PreviewError::MissingPrice);
                }
                (Some(p), None, p)
            }
            "market" => {
                if req.stop_price.is_some() || req.max_loss_krw.is_some() {
                    return Err(PreviewError::InvalidStopSizing(
                        "stop_price and max_loss_krw are only supported for limit bid orders"
                            .into(),
                    ));
                }
                // Market sell: volume is coin quantity, price must be None
                if req.price.is_some() {
                    return Err(PreviewError::UnexpectedPrice);
                }
                let v = req.volume.ok_or(PreviewError::MissingVolume)?;
                if v <= Decimal::ZERO {
                    return Err(PreviewError::MissingVolume);
                }
                // For market sell, notional depends on live price; assume estimate or min threshold
                (None, Some(v), MIN_NOTIONAL_KRW)
            }
            _ => return Err(PreviewError::InvalidOrderType(req.ord_type)),
        };

        // 4. Minimum notional check (5,000 KRW)
        if notional < MIN_NOTIONAL_KRW {
            return Err(PreviewError::UnderMinimumNotional {
                notional_krw: notional,
                min_krw: MIN_NOTIONAL_KRW,
            });
        }

        // 5. Fee calculation
        let estimated_fee = notional * DEFAULT_UPBIT_FEE_RATE;
        let total_required = if side == "bid" {
            notional + estimated_fee
        } else {
            notional
        };

        // 6. Optional balance verification
        let balance_verified = check_balances(
            &side,
            total_required,
            volume,
            req.current_krw_balance,
            req.current_coin_balance,
        )?;

        // 7. Expiration
        let valid_until = now + chrono::Duration::seconds(PREVIEW_TTL_SECONDS);

        // 8. Cryptographic digest
        let digest = compute_preview_digest(&DigestParams {
            market: &market,
            side: &side,
            ord_type: &ord_type,
            price,
            volume,
            notional,
            fee: estimated_fee,
            valid_until,
        });

        Ok(Self {
            status: "PREVIEW_VALIDATED".to_string(),
            preview_digest: digest,
            created_at_utc: now,
            valid_until_utc: valid_until,
            market,
            side,
            ord_type,
            price,
            volume,
            calculated_notional_krw: notional,
            estimated_fee_krw: estimated_fee,
            total_required_krw: total_required,
            balance_verified,
        })
    }

    /// Check if this preview has expired relative to reference time.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now > self.valid_until_utc
    }

    /// Re-verify that digest matches the contents.
    ///
    /// # Errors
    /// Returns `PreviewError::DigestMismatch` if corrupted.
    pub fn verify_digest(&self) -> Result<(), PreviewError> {
        let expected = compute_preview_digest(&DigestParams {
            market: &self.market,
            side: &self.side,
            ord_type: &self.ord_type,
            price: self.price,
            volume: self.volume,
            notional: self.calculated_notional_krw,
            fee: self.estimated_fee_krw,
            valid_until: self.valid_until_utc,
        });
        if self.preview_digest == expected {
            Ok(())
        } else {
            Err(PreviewError::DigestMismatch {
                expected,
                actual: self.preview_digest.clone(),
            })
        }
    }
}

fn resolve_limit_volume(
    side: &str,
    price: Decimal,
    explicit_volume: Option<Decimal>,
    stop_price: Option<Decimal>,
    max_loss_krw: Option<Decimal>,
) -> Result<Decimal, PreviewError> {
    if stop_price.is_none() && max_loss_krw.is_none() {
        let volume = explicit_volume.ok_or(PreviewError::MissingVolume)?;
        if volume <= Decimal::ZERO {
            return Err(PreviewError::MissingVolume);
        }
        return Ok(volume);
    }

    if side != "bid" {
        return Err(PreviewError::InvalidStopSizing(
            "stop_price and max_loss_krw are only supported for limit bid orders".into(),
        ));
    }
    let (Some(stop), Some(max_loss)) = (stop_price, max_loss_krw) else {
        return Err(PreviewError::InvalidStopSizing(
            "both stop_price and max_loss_krw are required for stop-distance sizing".into(),
        ));
    };
    if stop <= Decimal::ZERO {
        return Err(PreviewError::InvalidStopSizing(
            "stop_price must be positive".into(),
        ));
    }
    if stop >= price {
        return Err(PreviewError::InvalidStopSizing(format!(
            "stop_price ({stop}) must be strictly below limit bid price ({price})"
        )));
    }
    validate_krw_tick_size(stop)?;
    if max_loss <= Decimal::ZERO {
        return Err(PreviewError::InvalidStopSizing(
            "max_loss_krw must be positive".into(),
        ));
    }

    let stop_distance = price - stop;
    if stop_distance < price * MIN_STOP_DISTANCE_RATIO {
        return Err(PreviewError::InvalidStopSizing(format!(
            "stop distance ({stop_distance} KRW) is tighter than minimum 15 bps of entry price ({price} KRW)"
        )));
    }

    // Fee-inclusive loss per 1.0 coin unit stopped out at stop_price (entry + exit 5 bps fee).
    let unit_loss_krw = stop_distance + ((price + stop) * DEFAULT_UPBIT_FEE_RATE);
    let sized_volume = (max_loss / unit_loss_krw)
        .round_dp_with_strategy(UPBIT_VOLUME_SCALE, rust_decimal::RoundingStrategy::ToZero);
    if sized_volume <= Decimal::ZERO {
        return Err(PreviewError::InvalidStopSizing(
            "max_loss_krw is too small to size a positive 8-decimal coin volume".into(),
        ));
    }

    if let Some(volume) = explicit_volume {
        if volume <= Decimal::ZERO {
            return Err(PreviewError::MissingVolume);
        }
        if volume > sized_volume {
            return Err(PreviewError::InvalidStopSizing(format!(
                "explicit volume ({volume}) exceeds max_loss_krw risk-sized cap ({sized_volume})"
            )));
        }
        Ok(volume)
    } else {
        Ok(sized_volume)
    }
}

fn check_balances(
    side: &str,
    total_required: Decimal,
    volume: Option<Decimal>,
    current_krw: Option<Decimal>,
    current_coin: Option<Decimal>,
) -> Result<bool, PreviewError> {
    if side == "bid" {
        if let Some(avail_krw) = current_krw {
            if avail_krw < total_required {
                return Err(PreviewError::InsufficientKrw {
                    available: avail_krw,
                    required: total_required,
                });
            }
            return Ok(true);
        }
    } else if let Some(avail_coin) = current_coin
        && let Some(v) = volume
    {
        if avail_coin < v {
            return Err(PreviewError::InsufficientCoin {
                available: avail_coin,
                required: v,
            });
        }
        return Ok(true);
    }
    Ok(false)
}

struct DigestParams<'a> {
    market: &'a str,
    side: &'a str,
    ord_type: &'a str,
    price: Option<Decimal>,
    volume: Option<Decimal>,
    notional: Decimal,
    fee: Decimal,
    valid_until: DateTime<Utc>,
}

/// Compute SHA-256 preview digest over canonical string representation.
fn compute_preview_digest(params: &DigestParams<'_>) -> String {
    let p_str = params
        .price
        .map_or_else(|| "NONE".into(), |p| p.to_string());
    let v_str = params
        .volume
        .map_or_else(|| "NONE".into(), |v| v.to_string());
    let canonical = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        params.market,
        params.side,
        params.ord_type,
        p_str,
        v_str,
        params.notional,
        params.fee,
        params.valid_until.to_rfc3339()
    );
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    crate::upbit::auth::hex_encode(&result)
}

/// Validate price against Upbit KRW order tick sizes.
fn validate_krw_tick_size(price: Decimal) -> Result<(), PreviewError> {
    let tick_size = get_upbit_krw_tick_size(price);
    let remainder = price % tick_size;
    if remainder.is_zero() {
        Ok(())
    } else {
        Err(PreviewError::InvalidTickSize { price, tick_size })
    }
}

/// Upbit KRW market tick size rules.
#[must_use]
pub fn get_upbit_krw_tick_size(price: Decimal) -> Decimal {
    // Official KRW price bands effective 2025-07-31. Live automation additionally
    // requires the current instrument response and never relies on this preview table.
    let bands = [
        (Decimal::from(1_000_000), Decimal::from(1_000)),
        (Decimal::from(500_000), Decimal::from(500)),
        (Decimal::from(100_000), Decimal::from(100)),
        (Decimal::from(50_000), Decimal::from(50)),
        (Decimal::from(10_000), Decimal::from(10)),
        (Decimal::from(5_000), Decimal::from(5)),
        (Decimal::from(100), Decimal::ONE),
        (Decimal::from(10), Decimal::new(1, 1)),
        (Decimal::ONE, Decimal::new(1, 2)),
        (Decimal::new(1, 1), Decimal::new(1, 3)),
        (Decimal::new(1, 2), Decimal::new(1, 4)),
        (Decimal::new(1, 3), Decimal::new(1, 5)),
        (Decimal::new(1, 4), Decimal::new(1, 6)),
        (Decimal::new(1, 5), Decimal::new(1, 7)),
    ];
    bands
        .into_iter()
        .find(|(minimum, _)| price >= *minimum)
        .map_or(Decimal::new(1, 8), |(_, tick)| tick)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revised_krw_ticks_use_exact_decimal_boundaries() {
        for (price, expected) in [
            ("1000000", "1000"),
            ("999999.9", "500"),
            ("500000", "500"),
            ("499999.9", "100"),
            ("50000", "50"),
            ("5000", "5"),
            ("100", "1"),
            ("10", "0.1"),
            ("1", "0.01"),
            ("0.1", "0.001"),
            ("0.00001", "0.0000001"),
            ("0.00000999", "0.00000001"),
        ] {
            assert_eq!(
                get_upbit_krw_tick_size(Decimal::from_str_exact(price).unwrap()),
                Decimal::from_str_exact(expected).unwrap()
            );
        }
    }

    #[test]
    fn test_preview_valid_limit_bid() {
        let req = OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(95_000_000, 0)),
            volume: Some(Decimal::new(1, 2)), // 0.01 BTC
            current_krw_balance: Some(Decimal::new(2_000_000, 0)),
            current_coin_balance: None,
            stop_price: None,
            max_loss_krw: None,
        };
        let preview = OrderPreview::build(req, Utc::now()).unwrap();
        assert_eq!(preview.status, "PREVIEW_VALIDATED");
        assert_eq!(preview.calculated_notional_krw, Decimal::new(950_000, 0));
        assert!(preview.balance_verified);
        assert!(preview.verify_digest().is_ok());
    }

    #[test]
    fn test_preview_reject_under_min_notional() {
        let req = OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(95_000_000, 0)),
            volume: Some(Decimal::new(1, 5)), // 0.00001 BTC = 950 KRW < 5,000 KRW
            current_krw_balance: None,
            current_coin_balance: None,
            stop_price: None,
            max_loss_krw: None,
        };
        assert!(OrderPreview::build(req, Utc::now()).is_err());
    }

    #[test]
    fn test_preview_reject_disallowed_market() {
        let disallowed = OrderPreviewRequest {
            market: "USDT-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(150, 0)),
            volume: Some(Decimal::new(100, 0)),
            current_krw_balance: None,
            current_coin_balance: None,
            stop_price: None,
            max_loss_krw: None,
        };
        assert!(matches!(
            OrderPreview::build(disallowed, Utc::now()),
            Err(PreviewError::DisallowedMarket(_))
        ));

        let valid_altcoin = OrderPreviewRequest {
            market: "KRW-DOGE".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(150, 0)),
            volume: Some(Decimal::new(100, 0)),
            current_krw_balance: None,
            current_coin_balance: None,
            stop_price: None,
            max_loss_krw: None,
        };
        assert!(OrderPreview::build(valid_altcoin, Utc::now()).is_ok());
    }

    #[test]
    fn test_preview_insufficient_krw() {
        let req = OrderPreviewRequest {
            market: "KRW-ETH".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(3_500_000, 0)),
            volume: Some(Decimal::new(1, 0)), // 1 ETH = 3,500,000 KRW
            current_krw_balance: Some(Decimal::new(100_000, 0)), // only 100k
            current_coin_balance: None,
            stop_price: None,
            max_loss_krw: None,
        };
        assert!(matches!(
            OrderPreview::build(req, Utc::now()),
            Err(PreviewError::InsufficientKrw { .. })
        ));
    }

    #[test]
    fn test_preview_stop_distance_risk_sizing_derives_fee_inclusive_volume() {
        // Entry = 100,000,000 KRW, Stop = 99,000,000 KRW (1% = 100 bps stop distance).
        // Unit loss = (100M - 99M) + (100M + 99M) * 0.0005 = 1,099,500 KRW / BTC.
        // Max loss = 50,000 KRW -> 50,000 / 1,099,500 = 0.04547521 BTC (rounded down to 8 dp).
        let req = OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(100_000_000, 0)),
            volume: None,
            current_krw_balance: Some(Decimal::new(10_000_000, 0)),
            current_coin_balance: None,
            stop_price: Some(Decimal::new(99_000_000, 0)),
            max_loss_krw: Some(Decimal::new(50_000, 0)),
        };
        let preview = OrderPreview::build(req, Utc::now()).unwrap();
        let expected_volume = Decimal::from_str_exact("0.04547521").unwrap();
        assert_eq!(preview.volume, Some(expected_volume));
        assert_eq!(
            preview.calculated_notional_krw,
            Decimal::from_str_exact("4547521.00000000").unwrap()
        );
        let realized_loss_if_stopped = expected_volume * Decimal::new(1_099_500, 0);
        assert!(realized_loss_if_stopped <= Decimal::new(50_000, 0));
        assert!(preview.verify_digest().is_ok());
    }

    #[test]
    fn test_preview_stop_distance_rejects_tight_stop_and_excess_explicit_volume() {
        // 10 bps stop (< 15 bps minimum) must be rejected.
        let tight_stop = OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(100_000_000, 0)),
            volume: None,
            current_krw_balance: None,
            current_coin_balance: None,
            stop_price: Some(Decimal::new(99_900_000, 0)),
            max_loss_krw: Some(Decimal::new(50_000, 0)),
        };
        assert!(matches!(
            OrderPreview::build(tight_stop, Utc::now()),
            Err(PreviewError::InvalidStopSizing(_))
        ));

        // Explicit volume exceeding risk-sized cap must be rejected.
        let excess_volume = OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(100_000_000, 0)),
            volume: Some(Decimal::new(1, 1)), // 0.1 BTC > 0.04547521 BTC cap
            current_krw_balance: None,
            current_coin_balance: None,
            stop_price: Some(Decimal::new(99_000_000, 0)),
            max_loss_krw: Some(Decimal::new(50_000, 0)),
        };
        assert!(matches!(
            OrderPreview::build(excess_volume, Utc::now()),
            Err(PreviewError::InvalidStopSizing(_))
        ));
    }

    #[test]
    fn test_daytrader_rules_example_validates_against_policy_contract() {
        let example_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/examples/policy-create-daytrader-rules.json");
        let raw = std::fs::read_to_string(&example_path).unwrap();
        let write: crate::contracts::PolicyWrite = serde_json::from_str(&raw).unwrap();
        match write {
            crate::contracts::PolicyWrite::Create { definition, .. } => {
                definition.validate().unwrap();
            }
            crate::contracts::PolicyWrite::Revise { .. }
            | crate::contracts::PolicyWrite::Sweep { .. }
            | crate::contracts::PolicyWrite::Preflight { .. } => {
                panic!("expected PolicyWrite::Create in policy-create-daytrader-rules.json");
            }
        }
    }
}
