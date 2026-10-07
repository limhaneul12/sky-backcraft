//! Typed research contracts for identities, data, experiments, ledgers and jobs.
//!
//! The single source of contract truth is these Rust types plus explicit
//! validating constructors. Ledger numbers are exact decimals serialized as
//! decimal strings; indicator parameters and statistical values use finite floats.

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub mod identity;
pub use identity::*;
pub mod dataset;
pub use dataset::*;
mod dataset_identity;
pub use dataset_identity::*;
pub mod policy;
pub use policy::*;
pub mod policy_seeds;
pub use policy_seeds::*;
pub mod policy_api;
pub use policy_api::*;
pub mod policy_binding;
pub use policy_binding::*;
pub mod delete;
pub use delete::*;
pub mod experiment;
pub use experiment::*;
pub mod evidence;
pub use evidence::*;
pub mod ledger;
pub use ledger::*;
pub mod jobs;
pub use jobs::*;
mod artifact_api;
pub use artifact_api::*;
mod presentation;
pub use presentation::*;
mod metrics;
pub use metrics::*;
pub mod schedule;
pub use schedule::*;
pub mod sweep;
pub use sweep::*;
pub mod portfolio;
pub use portfolio::*;
pub mod regime;
pub use regime::*;
pub mod maintenance;
pub use maintenance::*;
pub mod research;
pub use research::*;
mod limits;
pub use limits::*;

/// Domain error taxonomy per F02. Transport failures stay distinct from
/// economic outcomes; unknown values must surface as errors, never as zero.
#[derive(Debug, Error)]
pub enum LabError {
    #[error("INVALID_CONFIG: {0}")]
    InvalidConfig(String),
    #[error("INSUFFICIENT_WARMUP: {0}")]
    InsufficientWarmup(String),
    #[error("DATA_GAP: {0}")]
    DataGap(String),
    #[error("INPUT_HASH_MISMATCH: {0}")]
    InputHashMismatch(String),
    #[error("BLOCKED_EVIDENCE: {0}")]
    BlockedEvidence(String),
    #[error("NETWORK_UNAVAILABLE: {0}")]
    NetworkUnavailable(String),
    #[error("RATE_LIMITED: {0}")]
    RateLimited(String),
    #[error("TEMPORARILY_BLOCKED: {0}")]
    TemporarilyBlocked(String),
    #[error("CAPACITY_EXCEEDED: {0}")]
    CapacityExceeded(String),
    #[error("CONFLICT: {0}")]
    Conflict(String),
    #[error("DATA_CORRUPT: {0}")]
    DataCorrupt(String),
    #[error("RESOURCE_LIMIT: {0}")]
    ResourceLimit(String),
    /// Persistent-storage pressure with a stable sub-reason in the payload
    /// (`DB_STORAGE_PRESSURE` / `WAL_STORAGE_PRESSURE`); remediable only by
    /// explicit maintenance, never by automatic destructive cleanup.
    #[error("STORAGE_PRESSURE: {0}")]
    StoragePressure(String),
    #[error("RESOURCE_LIMIT: {0}")]
    RequestLimit(Box<LimitReport>),
    #[error("CANCELLED: {0}")]
    Cancelled(String),
    #[error("UNVERIFIED_MARKET_RULES: {0}")]
    UnverifiedMarketRules(String),
    #[error("ACCOUNTING_INVARIANT_FAILURE: {0}")]
    AccountingInvariant(String),
    #[error("OUTCOME_UNKNOWN: {0}")]
    OutcomeUnknown(String),
    #[error("CONTRACT_PARSE: {0}")]
    ContractParse(String),
    /// Runtime/plumbing failure that is not one of the F02 domain errors.
    #[error("INTERNAL: {0}")]
    Internal(String),
}

/// Completed-candle count. Upbit's limit is 200; reserve one forming candle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ProbeCount(u32);

impl schemars::JsonSchema for ProbeCount {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProbeCount".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "integer", "minimum": 1, "maximum": 199})
    }
}

impl ProbeCount {
    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl Default for ProbeCount {
    fn default() -> Self {
        Self(2)
    }
}

impl TryFrom<u32> for ProbeCount {
    type Error = LabError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if !(1..=199).contains(&value) {
            return Err(LabError::InvalidConfig(format!(
                "count must be in 1..=199, got {value}"
            )));
        }
        Ok(Self(value))
    }
}

impl From<ProbeCount> for u32 {
    fn from(value: ProbeCount) -> Self {
        value.get()
    }
}

impl From<serde_json::Error> for LabError {
    fn from(error: serde_json::Error) -> Self {
        Self::ContractParse(error.to_string())
    }
}

impl From<rust_decimal::Error> for LabError {
    fn from(error: rust_decimal::Error) -> Self {
        Self::ContractParse(error.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Upbit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuoteCurrency {
    Krw,
}

/// Upbit `KRW` base-asset symbol (for example `BTC`, `ETH`, `SAND`). Every
/// listed Upbit `KRW` market is representable; symbol existence is validated
/// against the live market catalog at collection admission, so this type
/// enforces only the exchange symbol shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(with = "String")]
pub struct Asset(String);

impl Asset {
    /// Validate an Upbit base-asset symbol: 1..=15 uppercase ASCII
    /// letters/digits (for example `BTC`, `1INCH`).
    ///
    /// # Errors
    ///
    /// [`LabError::InvalidConfig`] when the symbol is not in the Upbit
    /// symbol shape.
    pub fn new(symbol: impl AsRef<str>) -> Result<Self, LabError> {
        let symbol = symbol.as_ref();
        let in_shape = (1..=15).contains(&symbol.len())
            && symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
        if !in_shape {
            return Err(LabError::InvalidConfig(format!(
                "unsupported asset symbol (expected 1..=15 uppercase letters/digits): {symbol}"
            )));
        }
        Ok(Self(symbol.to_owned()))
    }

    /// Exchange symbol of the base asset, e.g. `BTC`.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<Asset> for String {
    fn from(asset: Asset) -> Self {
        asset.0
    }
}

impl TryFrom<String> for Asset {
    type Error = LabError;

    fn try_from(symbol: String) -> Result<Self, Self::Error> {
        Self::new(symbol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(into = "String", try_from = "String")]
#[schemars(with = "String")]
pub struct MarketId {
    pub venue: Venue,
    pub base: Asset,
    pub quote: QuoteCurrency,
}

impl MarketId {
    /// Parse an `Upbit` market code such as `KRW-BTC`. Any listed Upbit
    /// `KRW` market is representable; symbol existence is validated against
    /// the live market catalog at collection admission.
    ///
    /// # Errors
    ///
    /// [`LabError::InvalidConfig`] when the code is not a `KRW`-quoted spot
    /// market code in the Upbit symbol shape.
    pub fn parse_upbit(code: &str) -> Result<Self, LabError> {
        let invalid = || {
            LabError::InvalidConfig(format!(
                "unsupported market code (expected KRW-<ASSET> on Upbit): {code}"
            ))
        };
        let Some((quote, base_code)) = code.split_once('-') else {
            return Err(invalid());
        };
        if !quote.eq_ignore_ascii_case("KRW") {
            return Err(invalid());
        }
        Ok(Self {
            venue: Venue::Upbit,
            base: Asset::new(base_code)?,
            quote: QuoteCurrency::Krw,
        })
    }

    /// Market code in exchange notation, e.g. `KRW-BTC`.
    #[must_use]
    pub fn code(&self) -> String {
        format!("KRW-{}", self.base.0)
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.code())
    }
}

impl From<MarketId> for String {
    fn from(market: MarketId) -> Self {
        market.code().clone()
    }
}

impl TryFrom<String> for MarketId {
    type Error = LabError;

    fn try_from(code: String) -> Result<Self, Self::Error> {
        Self::parse_upbit(&code)
    }
}

/// Candle width of raw observation data. Batch A probes `H1`; `M1`/`M5` stay
/// opt-in for fine execution research, per the three-resolution contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CandleInterval {
    M1,
    M5,
    H1,
    H4,
    D1,
}

impl CandleInterval {
    /// Parse an interval code (`h1`, `4h`, `d1`, ...).
    ///
    /// # Errors
    ///
    /// [`LabError::InvalidConfig`] for unsupported intervals; 1s/1ms-style
    /// granularities are rejected by contract.
    pub fn parse_code(code: &str) -> Result<Self, LabError> {
        match code.to_ascii_lowercase().as_str() {
            "m1" | "1m" => Ok(Self::M1),
            "m5" | "5m" => Ok(Self::M5),
            "h1" | "1h" => Ok(Self::H1),
            "h4" | "4h" => Ok(Self::H4),
            "d1" | "1d" => Ok(Self::D1),
            other => Err(LabError::InvalidConfig(format!(
                "unsupported candle interval: {other}"
            ))),
        }
    }

    /// Candle half-open interval is `[open_time, close_time)`. `Upbit` UTC
    /// daily candles open at `00:00Z`, so `D1` spans 24 hours.
    #[must_use]
    pub fn duration(self) -> Duration {
        match self {
            Self::M1 => Duration::minutes(1),
            Self::M5 => Duration::minutes(5),
            Self::H1 => Duration::hours(1),
            Self::H4 => Duration::hours(4),
            Self::D1 => Duration::hours(24),
        }
    }

    /// `Upbit` public REST path relative to the API base, including the
    /// `/v1` prefix and the minute unit as a path segment.
    #[must_use]
    pub fn upbit_endpoint(self) -> &'static str {
        match self {
            Self::M1 => "v1/candles/minutes/1",
            Self::M5 => "v1/candles/minutes/5",
            Self::H1 => "v1/candles/minutes/60",
            Self::H4 => "v1/candles/minutes/240",
            Self::D1 => "v1/candles/days",
        }
    }
}

/// UTC instant, serialized as RFC 3339.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct UtcTimestamp(pub DateTime<Utc>);

impl UtcTimestamp {
    /// Current wall-clock instant (UTC).
    #[must_use]
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Parse an RFC 3339 timestamp and normalize to UTC.
    ///
    /// # Errors
    ///
    /// [`LabError::ContractParse`] when the input is not valid RFC 3339.
    pub fn parse_rfc3339(text: &str) -> Result<Self, LabError> {
        DateTime::parse_from_rfc3339(text)
            .map(|parsed| Self(parsed.with_timezone(&Utc)))
            .map_err(|error| LabError::ContractParse(format!("rfc3339 timestamp: {error}")))
    }

    /// Render as RFC 3339 with second precision.
    #[must_use]
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }
}

impl fmt::Display for UtcTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_rfc3339())
    }
}

/// Exact decimal from a JSON number literal. With `serde_json`
/// `arbitrary_precision` the literal round-trips byte-exact, so `Upbit`
/// numbers never pass through `f64`.
///
/// # Errors
///
/// [`LabError::ContractParse`] when the literal is not a finite decimal.
pub fn decimal_from_json_number(value: &serde_json::Number) -> Result<Decimal, LabError> {
    parse_decimal_exact(value.as_str())
        .map_err(|error| LabError::ContractParse(format!("decimal from json number: {error}")))
}

fn parse_decimal_exact(text: &str) -> Result<Decimal, rust_decimal::Error> {
    if let Some((mantissa, _)) = text.split_once(['e', 'E']) {
        // from_scientific checks exponent/scale but uses rounding FromStr for
        // its mantissa; validate that component exactly before delegating.
        Decimal::from_str_exact(mantissa)?;
        Decimal::from_scientific(text)
    } else {
        Decimal::from_str_exact(text)
    }
}

/// Defines a unit newtype over `Decimal` that serializes as an exact decimal
/// string and validates through its constructor on deserialization.
macro_rules! decimal_unit {
    ($name:ident, $doc:expr, $validate:expr) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, schemars::JsonSchema)]
        #[schemars(with = "String")]
        pub struct $name(Decimal);

        impl $name {
            /// Construct from an exact decimal, enforcing the unit range.
            ///
            /// # Errors
            ///
            /// [`LabError::ContractParse`] when the value violates the unit
            /// contract.
            pub fn new(value: Decimal) -> Result<Self, LabError> {
                #[allow(clippy::redundant_closure_call)]
                let validated: Decimal = ($validate)(value)?;
                Ok(Self(validated))
            }

            /// Construct from a JSON number literal without `f64` loss.
            ///
            /// # Errors
            ///
            /// [`LabError::ContractParse`] when the literal is not a valid
            /// decimal or violates the unit contract.
            pub fn from_json_number(value: &serde_json::Number) -> Result<Self, LabError> {
                Self::new(decimal_from_json_number(value)?)
            }

            /// Exact decimal value.
            #[must_use]
            pub fn get(self) -> Decimal {
                self.0
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0.to_string())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                let value = parse_decimal_exact(&text).map_err(serde::de::Error::custom)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

decimal_unit!(
    PriceKrw,
    "Price in `KRW` quote currency. Strictly positive.",
    |value: Decimal| if value <= Decimal::ZERO {
        Err(LabError::ContractParse(format!(
            "price must be > 0, got {value}"
        )))
    } else {
        Ok(value)
    }
);

decimal_unit!(
    AssetQuantity,
    "Base asset quantity. Non-negative.",
    |value: Decimal| if value < Decimal::ZERO {
        Err(LabError::ContractParse(format!(
            "quantity must be >= 0, got {value}"
        )))
    } else {
        Ok(value)
    }
);

decimal_unit!(
    QuoteAmount,
    "`KRW` quote amount (cash, notional, fees). Non-negative.",
    |value: Decimal| if value < Decimal::ZERO {
        Err(LabError::ContractParse(format!(
            "quote amount must be >= 0, got {value}"
        )))
    } else {
        Ok(value)
    }
);

decimal_unit!(
    Weight,
    "Portfolio target weight in `[0, 1]`. Long/cash only.",
    |value: Decimal| if value < Decimal::ZERO || value > Decimal::ONE {
        Err(LabError::ContractParse(format!(
            "weight must be in [0, 1], got {value}"
        )))
    } else {
        Ok(value)
    }
);

decimal_unit!(
    SignedAmount,
    "Signed exact decimal amount used for profit/loss and residuals.",
    |value: Decimal| Ok::<Decimal, LabError>(value)
);

decimal_unit!(
    BasisPoints,
    "Nonnegative rate in basis points; one basis point is 1/10000.",
    |value: Decimal| if value < Decimal::ZERO {
        Err(LabError::InvalidConfig(
            "basis points must be nonnegative".into(),
        ))
    } else {
        Ok(value)
    }
);

/// Origin of market data observations (F02 state vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataOrigin {
    ExchangeObserved,
    SyntheticTestOnly,
}

/// One normalized completed-candle record (Batch A probe shape).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CandleRecord {
    pub market: String,
    pub interval: CandleInterval,
    /// Candle half-open interval `[open_time, close_time)`.
    pub open_time_utc: UtcTimestamp,
    pub close_time_utc: UtcTimestamp,
    #[schemars(with = "String")]
    pub open: PriceKrw,
    #[schemars(with = "String")]
    pub high: PriceKrw,
    #[schemars(with = "String")]
    pub low: PriceKrw,
    #[schemars(with = "String")]
    pub close: PriceKrw,
    #[schemars(with = "String")]
    pub volume: AssetQuantity,
    pub quote_turnover: QuoteAmount,
    /// True only when `close_time` <= `fetched_at` at parse time.
    pub completed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F02 acceptance: out-of-contract inputs are rejected, never coerced.
    #[test]
    fn contract_rejections() {
        // Any listed Upbit KRW symbol is representable (catalog validates
        // existence at collection admission); shape is still enforced.
        assert_eq!(
            MarketId::parse_upbit("KRW-DOGE").unwrap().code(),
            "KRW-DOGE"
        );
        assert_eq!(
            MarketId::parse_upbit("KRW-1INCH").unwrap().code(),
            "KRW-1INCH"
        );
        assert!(MarketId::parse_upbit("BTC-KRW").is_err());
        assert!(MarketId::parse_upbit("KRW-BTC-X").is_err());
        assert!(MarketId::parse_upbit("KRW-").is_err());
        assert!(MarketId::parse_upbit("krw-btc").is_err());
        assert!(Asset::new("sol").is_err());
        assert!(Asset::new("DO GE").is_err());
        assert!(CandleInterval::parse_code("1s").is_err());
        assert!(CandleInterval::parse_code("1ms").is_err());
        assert!(PriceKrw::new(Decimal::ZERO).is_err());
        assert!(AssetQuantity::new(Decimal::from(-1)).is_err());
        assert!(Weight::new(Decimal::from(2)).is_err());
        for count in [0, 200, 500, u32::MAX] {
            assert!(ProbeCount::try_from(count).is_err());
        }
        assert_eq!(ProbeCount::try_from(199).unwrap().get(), 199);
        // Exact decimal-string round trip preserves wire digits.
        let price = PriceKrw::new("113539000.00000000".parse().unwrap()).unwrap();
        assert_eq!(
            serde_json::to_string(&price).unwrap(),
            "\"113539000.00000000\""
        );
        // Precision outside Decimal's range must fail instead of rounding.
        let excessive = "0.12345678901234567890123456789";
        let number = serde_json::from_str::<serde_json::Number>(excessive).unwrap();
        assert!(decimal_from_json_number(&number).is_err());
        let text = format!("\"{excessive}\"");
        assert!(serde_json::from_str::<AssetQuantity>(&text).is_err());
        assert!(parse_decimal_exact("2.5e-28").is_err());
        assert_eq!(
            parse_decimal_exact("1.25e-3").unwrap().to_string(),
            "0.00125"
        );
    }
}

/// Result of one actual `Upbit` public API probe.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProbeReport {
    pub market: String,
    pub interval: CandleInterval,
    pub requested_completed_count: u32,
    pub fetched_at: UtcTimestamp,
    pub source_url: String,
    pub http_status: u16,
    pub raw_sha256: String,
    /// Path of the preserved raw response (absolute when `data_root` is absolute).
    pub raw_file: Option<String>,
    /// `Upbit` rate-limit accounting header, when present.
    pub remaining_req: Option<String>,
    pub origin: MarketDataOrigin,
    pub candles: Vec<CandleRecord>,
}
