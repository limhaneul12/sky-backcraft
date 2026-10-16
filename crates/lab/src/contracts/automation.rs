//! Durable strategy automation commands, identities, and read projections.
//!
//! Automation is an execution lifecycle over one frozen policy revision. It is
//! distinct from historical replay and persistent simulated-paper sessions.

use super::{
    AssetQuantity, CandleInterval, ContentHash, LabError, MarketId, ObservationId,
    PolicyRevisionRef, PriceKrw, QuoteAmount, RequestId, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A client identifier that Upbit accepts for durable order readback.
///
/// Values are account-wide and must never be reused. Only the storage owner
/// allocates identifiers for accepted automation intents.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct AutomationOrderIdentifier(String);

impl AutomationOrderIdentifier {
    /// Validate an exchange identifier.
    ///
    /// # Errors
    /// Rejects empty, oversized, or unsafe identifiers.
    pub fn new(value: impl Into<String>) -> Result<Self, LabError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(LabError::InvalidConfig(
                "automation order identifier must contain 1..64 ASCII letters, digits, hyphens or underscores".into(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn from_intent(intent_id: &AutomationIntentId) -> Self {
        let digest = ContentHash::of_bytes(intent_id.as_str().as_bytes());
        // `auto-` plus 59 hexadecimal characters is stable, safe, and <=64.
        Self(format!("auto-{}", &digest.as_str()[..59]))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AutomationOrderIdentifier {
    type Error = LabError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<AutomationOrderIdentifier> for String {
    fn from(value: AutomationOrderIdentifier) -> Self {
        value.0
    }
}

impl fmt::Display for AutomationOrderIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! automation_id {
    ($name:ident, $prefix:literal) => {
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
        )]
        #[serde(try_from = "String", into = "String")]
        #[schemars(with = "String")]
        pub struct $name(String);

        impl $name {
            /// Construct a bounded opaque automation identity.
            /// # Errors
            /// Rejects empty, oversized, or unsafe identities.
            pub fn new(value: impl Into<String>) -> Result<Self, LabError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > 96
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                {
                    return Err(LabError::InvalidConfig(
                        concat!(
                            stringify!($name),
                            " must contain 1..96 ASCII letters, digits, hyphens or underscores"
                        )
                        .into(),
                    ));
                }
                Ok(Self(value))
            }

            #[must_use]
            pub fn from_seed(seed: &str) -> Self {
                Self(format!(
                    "{}-{}",
                    $prefix,
                    ContentHash::of_bytes(seed.as_bytes())
                ))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = LabError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

automation_id!(AutomationSessionId, "automation");
automation_id!(AutomationIntentId, "intent");
automation_id!(AutomationBaselineId, "baseline");

/// Provenance of an account daily starting baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationBaselineSource {
    OperatorAttested,
    ExchangeObservedDayStart,
    PriorDayVerifiedLedgerRollover,
    DurableMockVenueSeed,
}

/// Verification status of an account daily starting baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationBaselineVerificationState {
    Verified,
    BlockedInsufficientEvidence,
}

/// Valuation basis for a single currency in a daily starting baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationAssetValuationBasis {
    QuoteCurrencyParity,
    ZeroBalance,
    FinalizedCandleClose,
    OperatorAttestedPrice,
}

/// Per-asset entry submitted in an account daily starting baseline request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationBaselineAssetInput {
    pub currency: String,
    pub free: AssetQuantity,
    pub locked: AssetQuantity,
    pub valuation_basis: AutomationAssetValuationBasis,
    #[serde(default)]
    pub mark_price_krw: Option<PriceKrw>,
}

impl AutomationBaselineAssetInput {
    /// Validate currency symbol, non-negative quantities, and valuation basis compatibility.
    ///
    /// # Errors
    /// Rejects malformed currency codes or incompatible valuation basis and mark price fields.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.currency.len() < 2
            || self.currency.len() > 16
            || !self
                .currency
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err(LabError::InvalidConfig(format!(
                "invalid baseline asset currency '{}'",
                self.currency
            )));
        }
        let total = self
            .free
            .get()
            .checked_add(self.locked.get())
            .ok_or_else(|| {
                LabError::InvalidConfig("baseline asset free + locked overflows Decimal".into())
            })?;
        if self.currency == "KRW" {
            if self.valuation_basis != AutomationAssetValuationBasis::QuoteCurrencyParity {
                return Err(LabError::InvalidConfig(
                    "KRW baseline asset must use QUOTE_CURRENCY_PARITY valuation_basis".into(),
                ));
            }
            if let Some(mark) = self.mark_price_krw
                && mark.get() != rust_decimal::Decimal::ONE
            {
                return Err(LabError::InvalidConfig(
                    "KRW baseline asset mark_price_krw must be omitted or equal to 1".into(),
                ));
            }
        } else {
            if self.valuation_basis == AutomationAssetValuationBasis::QuoteCurrencyParity {
                return Err(LabError::InvalidConfig(format!(
                    "non-KRW asset {} cannot use QUOTE_CURRENCY_PARITY",
                    self.currency
                )));
            }
            if total == rust_decimal::Decimal::ZERO {
                if self.valuation_basis == AutomationAssetValuationBasis::OperatorAttestedPrice
                    && self.mark_price_krw.is_none()
                {
                    return Err(LabError::InvalidConfig(format!(
                        "zero-balance asset {} with OPERATOR_ATTESTED_PRICE requires mark_price_krw",
                        self.currency
                    )));
                }
            } else {
                match self.valuation_basis {
                    AutomationAssetValuationBasis::ZeroBalance => {
                        return Err(LabError::InvalidConfig(format!(
                            "non-zero asset {} cannot use ZERO_BALANCE valuation_basis",
                            self.currency
                        )));
                    }
                    AutomationAssetValuationBasis::OperatorAttestedPrice => {
                        if self.mark_price_krw.is_none() {
                            return Err(LabError::InvalidConfig(format!(
                                "non-zero asset {} with OPERATOR_ATTESTED_PRICE requires mark_price_krw",
                                self.currency
                            )));
                        }
                    }
                    AutomationAssetValuationBasis::FinalizedCandleClose
                    | AutomationAssetValuationBasis::QuoteCurrencyParity => {}
                }
            }
        }
        Ok(())
    }
}

/// Operator or recovery input for an account daily starting baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationAccountDayBaselineInput {
    pub account_identity: String,
    pub day_utc: String,
    pub reference_time_utc: UtcTimestamp,
    pub source: AutomationBaselineSource,
    pub assets: Vec<AutomationBaselineAssetInput>,
    #[serde(default)]
    pub operator_note: Option<String>,
}

impl AutomationAccountDayBaselineInput {
    /// Validate canonical date format, timestamp date alignment, and asset list uniqueness.
    ///
    /// # Errors
    /// Rejects empty, duplicate, or date-mismatched baseline inputs before DB mutations.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.account_identity.is_empty() || self.account_identity.len() > 128 {
            return Err(LabError::InvalidConfig(
                "baseline account_identity must be non-empty".into(),
            ));
        }
        let _ = parse_day_start_utc(&self.day_utc)?;
        let reference_day = self.reference_time_utc.0.format("%Y-%m-%d").to_string();
        if reference_day != self.day_utc {
            return Err(LabError::InvalidConfig(format!(
                "baseline reference_time_utc ({}) does not belong to day_utc ({})",
                self.reference_time_utc.to_rfc3339(),
                self.day_utc
            )));
        }
        if self.assets.is_empty() || self.assets.len() > 64 {
            return Err(LabError::InvalidConfig(
                "baseline assets must contain 1..=64 currency entries".into(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut has_krw = false;
        for asset in &self.assets {
            asset.validate()?;
            if !seen.insert(asset.currency.as_str()) {
                return Err(LabError::InvalidConfig(format!(
                    "duplicate currency '{}' in baseline input",
                    asset.currency
                )));
            }
            if asset.currency == "KRW" {
                has_krw = true;
            }
        }
        if !has_krw {
            return Err(LabError::InvalidConfig(
                "baseline assets must explicitly include KRW quote currency".into(),
            ));
        }
        if let Some(note) = &self.operator_note
            && note.len() > 512
        {
            return Err(LabError::InvalidConfig(
                "baseline operator_note must not exceed 512 bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Evaluated per-asset entry persisted in an account daily starting baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationBaselineAssetEntry {
    pub currency: String,
    pub free: AssetQuantity,
    pub locked: AssetQuantity,
    pub total: AssetQuantity,
    pub valuation_basis: AutomationAssetValuationBasis,
    pub mark_price_krw: Option<PriceKrw>,
    pub valuation_krw: Option<QuoteAmount>,
    pub candle_observation_id: Option<ObservationId>,
    pub candle_content_digest: Option<ContentHash>,
}

/// Exchange-proven evidence paired against an account daily starting baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationBaselineExchangeEvidence {
    pub snapshot_sequence: Option<u64>,
    pub snapshot_observed_at_utc: Option<UtcTimestamp>,
    pub snapshot_received_at_utc: Option<UtcTimestamp>,
    pub snapshot_balances_digest: Option<ContentHash>,
    pub prior_day_baseline_digest: Option<ContentHash>,
}

/// Durable read projection for an account daily starting baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationAccountDayBaselineView {
    pub baseline_id: AutomationBaselineId,
    pub account_identity: String,
    pub mode: AutomationSessionMode,
    pub day_utc: String,
    pub reference_time_utc: UtcTimestamp,
    pub recorded_at_utc: UtcTimestamp,
    pub source: AutomationBaselineSource,
    pub verification_state: AutomationBaselineVerificationState,
    pub sufficient_for_risk_admission: bool,
    pub total_equity_krw: Option<QuoteAmount>,
    pub assets: Vec<AutomationBaselineAssetEntry>,
    pub exchange_evidence: Option<AutomationBaselineExchangeEvidence>,
    pub block_reasons: Vec<String>,
    pub baseline_digest: ContentHash,
    pub request_id: Option<RequestId>,
    pub operator_note: Option<String>,
}

/// Explicit aggregate readback for the `LEGACY_LOCAL_MOCK_LEDGER` compatibility scope.
/// This carries no per-asset valuation proof, and `recorded_at_utc` is the local SQLite
/// record time rather than provider or exchange as-of evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationLegacyMockBaselineView {
    pub account_identity: String,
    pub day_utc: String,
    pub baseline_equity_krw: QuoteAmount,
    pub balances_digest: ContentHash,
    pub recorded_at_utc: UtcTimestamp,
}

/// Readback response for the `baseline` session query action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationAccountDayBaselineStatus {
    pub session_id: AutomationSessionId,
    pub account_identity: String,
    pub mode: AutomationSessionMode,
    pub day_utc: String,
    pub admission_blocked: bool,
    pub block_reasons: Vec<String>,
    pub baseline: Option<AutomationAccountDayBaselineView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_mock_baseline: Option<AutomationLegacyMockBaselineView>,
}

/// Buy/sell direction before conversion to Upbit's `bid` / `ask` wire values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationSide {
    Bid,
    Ask,
}

/// Exact order shape accepted by the Upbit venue adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum AutomationOrderKind {
    Limit {
        volume: AssetQuantity,
        price: PriceKrw,
    },
    MarketBuy {
        budget: QuoteAmount,
    },
    MarketSell {
        volume: AssetQuantity,
    },
}

/// Durable order request passed to the venue boundary after intent admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationOrderRequest {
    pub client_identifier: AutomationOrderIdentifier,
    pub market: MarketId,
    pub side: AutomationSide,
    pub order: AutomationOrderKind,
}

impl AutomationOrderRequest {
    /// Validate side/order compatibility and strictly positive economics.
    /// # Errors
    /// Rejects malformed request combinations before any venue effect.
    pub fn validate(&self) -> Result<(), LabError> {
        let valid = match (&self.side, &self.order) {
            (
                AutomationSide::Bid | AutomationSide::Ask,
                AutomationOrderKind::Limit { volume, price },
            ) => {
                volume.get() > rust_decimal::Decimal::ZERO
                    && price.get() > rust_decimal::Decimal::ZERO
            }
            (AutomationSide::Bid, AutomationOrderKind::MarketBuy { budget }) => {
                budget.get() > rust_decimal::Decimal::ZERO
            }
            (AutomationSide::Ask, AutomationOrderKind::MarketSell { volume }) => {
                volume.get() > rust_decimal::Decimal::ZERO
            }
            (AutomationSide::Bid, AutomationOrderKind::MarketSell { .. })
            | (AutomationSide::Ask, AutomationOrderKind::MarketBuy { .. }) => false,
        };
        if valid {
            Ok(())
        } else {
            Err(LabError::InvalidConfig(
                "automation order side and positive order amounts do not agree".into(),
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationSessionMode {
    Mock,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationSessionState {
    Running,
    Paused,
    Stopped,
    NeedsReconciliation,
    BlockedApproval,
    Faulted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationAction {
    Start,
    Pause,
    Resume,
    Stop,
    CancelOrder,
    RecordBaseline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationCommandOutcome {
    Applied,
    Replayed,
    NoOp,
    Pending,
    Denied,
}

/// Durable session lifecycle commands with request idempotency and sequence CAS.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum AutomationSessionWrite {
    Start {
        request_id: RequestId,
        policy_ref: PolicyRevisionRef,
        market: MarketId,
        decision_interval: CandleInterval,
        mode: AutomationSessionMode,
        risk: AutomationRiskPolicy,
    },
    Pause {
        request_id: RequestId,
        session_id: AutomationSessionId,
        expected_sequence: u64,
    },
    Resume {
        request_id: RequestId,
        session_id: AutomationSessionId,
        expected_sequence: u64,
    },
    Stop {
        request_id: RequestId,
        session_id: AutomationSessionId,
        expected_sequence: u64,
    },
    CancelOrder {
        request_id: RequestId,
        session_id: AutomationSessionId,
        expected_sequence: u64,
        client_identifier: AutomationOrderIdentifier,
    },
    RecordBaseline {
        request_id: RequestId,
        session_id: AutomationSessionId,
        expected_sequence: u64,
        baseline: AutomationAccountDayBaselineInput,
    },
}

/// Shared account risk bounds frozen into every automation session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationRiskPolicy {
    pub max_order_notional_krw: QuoteAmount,
    pub max_daily_notional_krw: QuoteAmount,
    pub max_asset_exposure_krw: QuoteAmount,
    pub max_daily_loss_krw: QuoteAmount,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_loss_limit_krw: Option<QuoteAmount>,
    pub max_open_orders: u32,
    pub max_orders_per_interval: u32,
    pub max_balance_age_seconds: u32,
    pub max_feed_age_seconds: u32,
}

impl AutomationRiskPolicy {
    /// Reject zero, negative, or impractically large limits at the public boundary.
    /// # Errors
    /// Returns a typed configuration error for limits the runtime cannot enforce.
    pub fn validate(&self) -> Result<(), LabError> {
        let monetary = [
            self.max_order_notional_krw.get(),
            self.max_daily_notional_krw.get(),
            self.max_asset_exposure_krw.get(),
            self.max_daily_loss_krw.get(),
        ];
        if monetary
            .iter()
            .any(|value| *value <= rust_decimal::Decimal::ZERO)
            || self
                .activation_loss_limit_krw
                .is_some_and(|value| value.get() <= rust_decimal::Decimal::ZERO)
            || self.max_order_notional_krw > self.max_daily_notional_krw
            || self.max_open_orders == 0
            || self.max_open_orders > 100
            || self.max_orders_per_interval == 0
            || self.max_orders_per_interval > 10
            || self.max_balance_age_seconds == 0
            || self.max_balance_age_seconds > 3_600
            || self.max_feed_age_seconds == 0
            || self.max_feed_age_seconds > 86_400
        {
            return Err(LabError::InvalidConfig(
                "automation risk limits are outside their supported positive bounds".into(),
            ));
        }
        Ok(())
    }

    /// Validate mode-specific risk choices without changing UTC-day defaults.
    /// # Errors
    /// Rejects an activation-observed reference limit on MOCK sessions.
    pub fn validate_for_mode(&self, mode: AutomationSessionMode) -> Result<(), LabError> {
        self.validate()?;
        if self.activation_loss_limit_krw.is_some() && mode == AutomationSessionMode::Mock {
            return Err(LabError::InvalidConfig(
                "activation_loss_limit_krw is supported only for LIVE sessions".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum AutomationSessionQuery {
    List {
        offset: u32,
        limit: u32,
    },
    Get {
        session_id: AutomationSessionId,
    },
    Request {
        request_id: RequestId,
    },
    Events {
        session_id: AutomationSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    },
    Ledger {
        session_id: AutomationSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    },
    Baseline {
        session_id: AutomationSessionId,
        #[serde(default)]
        day_utc: Option<String>,
    },
    Decisions {
        session_id: AutomationSessionId,
        offset: u32,
        limit: u32,
    },
    Decision {
        session_id: AutomationSessionId,
        intent_id: AutomationIntentId,
    },
    Performance {
        session_id: AutomationSessionId,
    },
    ExportManifest {
        session_id: AutomationSessionId,
    },
    ExportRead {
        session_id: AutomationSessionId,
        file: crate::contracts::AutomationReviewFile,
        offset: u64,
        expected_sha256: ContentHash,
        expected_snapshot_digest: ContentHash,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationSessionRequestReadback {
    pub request_id: RequestId,
    pub state: AutomationRequestReadbackState,
    pub response: Option<AutomationSessionCommandResponse>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationRequestReadbackState {
    Pending,
    Applied,
    Rejected,
    NotRecorded,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationSessionReceipt {
    pub request_id: RequestId,
    pub action: AutomationAction,
    pub outcome: AutomationCommandOutcome,
    pub sequence: u64,
    pub observed_at_utc: UtcTimestamp,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationSessionView {
    pub session_id: AutomationSessionId,
    pub account_identity: String,
    pub mode: AutomationSessionMode,
    pub state: AutomationSessionState,
    pub sequence: u64,
    pub policy_ref: PolicyRevisionRef,
    pub market: MarketId,
    pub decision_interval: CandleInterval,
    pub risk: AutomationRiskPolicy,
    pub created_at_utc: UtcTimestamp,
    pub updated_at_utc: UtcTimestamp,
    pub last_finalized_candle_at: Option<UtcTimestamp>,
    pub last_balance_observed_at: Option<UtcTimestamp>,
    pub unresolved_order_count: u32,
    pub fill_count: u64,
    pub recovery_reason: Option<String>,
    pub account_risk: Option<AutomationAccountRiskProjection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationFreshness {
    Fresh,
    Delayed,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationDataSource {
    UpbitLiveRead,
    UpbitPublicFeed,
    DurableMockVenue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationBalanceProjection {
    pub source: AutomationDataSource,
    pub observed_at_utc: Option<UtcTimestamp>,
    pub received_at_utc: Option<UtcTimestamp>,
    pub freshness: AutomationFreshness,
    pub quote_currency: String,
    pub quote_total: Option<QuoteAmount>,
    pub quote_free: Option<QuoteAmount>,
    pub quote_exchange_locked: Option<QuoteAmount>,
    pub quote_local_reserved: Option<QuoteAmount>,
    pub base_currency: String,
    pub base_total: Option<AssetQuantity>,
    pub base_free: Option<AssetQuantity>,
    pub base_exchange_locked: Option<AssetQuantity>,
    pub base_local_reserved: Option<AssetQuantity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationRiskUsageProjection {
    pub daily_notional_used_krw: Option<QuoteAmount>,
    pub daily_notional_limit_krw: QuoteAmount,
    pub daily_loss_used_krw: Option<QuoteAmount>,
    pub daily_loss_limit_krw: QuoteAmount,
    pub asset_exposure_krw: Option<QuoteAmount>,
    pub asset_exposure_limit_krw: QuoteAmount,
    pub open_order_count: u32,
    pub open_order_limit: u32,
    pub orders_in_interval: Option<u32>,
    pub orders_per_interval_limit: u32,
    pub emergency_latched: bool,
    pub live_grant_active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationLossReferenceBasis {
    UtcDayBaseline,
    LiveActivationObserved,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationEffectiveLossProjection {
    pub basis: AutomationLossReferenceBasis,
    pub limit_krw: Option<QuoteAmount>,
    pub used_krw: Option<QuoteAmount>,
    pub admission_blocked: bool,
    #[serde(default)]
    pub block_reasons: Vec<String>,
}

impl Default for AutomationEffectiveLossProjection {
    fn default() -> Self {
        Self {
            basis: AutomationLossReferenceBasis::Unavailable,
            limit_krw: None,
            used_krw: None,
            admission_blocked: true,
            block_reasons: vec!["LOSS_REFERENCE_UNAVAILABLE".into()],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationActivationReferenceSource {
    CurrentLocalObserved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationActivationValuationState {
    PendingMarks,
    Verified,
}

/// Immutable account-scoped equity anchor captured from an eligible current
/// private account observation and finalized-candle valuation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationLiveActivationReferenceView {
    pub account_identity: String,
    pub mode: AutomationSessionMode,
    pub source: AutomationActivationReferenceSource,
    pub reference_observed_at_utc: UtcTimestamp,
    pub reference_received_at_utc: UtcTimestamp,
    pub valuation_recorded_at_utc: UtcTimestamp,
    pub snapshot_sequence: u64,
    pub control_sequence: u64,
    pub reference_max_feed_age_seconds: u32,
    pub balances_digest: ContentHash,
    pub valuation_state: AutomationActivationValuationState,
    pub valuation_digest: ContentHash,
    pub reference_equity_krw: Option<QuoteAmount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationFeedProjection {
    pub source: AutomationDataSource,
    pub observation_close_at_utc: Option<UtcTimestamp>,
    pub received_at_utc: Option<UtcTimestamp>,
    pub freshness: AutomationFreshness,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)] // Capabilities are independent wire permissions consumed individually by the native client.
pub struct AutomationCapabilities {
    pub can_pause: bool,
    pub can_resume: bool,
    pub can_stop: bool,
    pub can_cancel_order: bool,
    pub can_submit_orders: bool,
    pub live_effects_enabled: bool,
    pub explicit_approval_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationMockAssumptions {
    pub initial_quote_balance_krw: QuoteAmount,
    pub initial_base_balance: AssetQuantity,
    pub fee_rate: String,
    pub fill_model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationAccountRiskProjection {
    pub balance: AutomationBalanceProjection,
    pub risk: AutomationRiskUsageProjection,
    #[serde(default)]
    pub effective_loss: AutomationEffectiveLossProjection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_reference: Option<AutomationLiveActivationReferenceView>,
    pub feed: AutomationFeedProjection,
    pub capabilities: AutomationCapabilities,
    pub mock_assumptions: Option<AutomationMockAssumptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_mock_baseline: Option<AutomationLegacyMockBaselineView>,
    #[serde(default)]
    pub day_baseline: Option<AutomationAccountDayBaselineView>,
    #[serde(default)]
    pub admission_block_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationSessionCommandResponse {
    pub session: AutomationSessionView,
    pub receipt: AutomationSessionReceipt,
    pub order: Option<AutomationOrderView>,
    #[serde(default)]
    pub baseline: Option<AutomationAccountDayBaselineView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationSessionPage {
    pub items: Vec<AutomationSessionView>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationEventKind {
    Started,
    Paused,
    Resumed,
    Stopped,
    CandleObserved,
    SignalObserved,
    IntentRejected,
    OrderAdmitted,
    OrderSendClaimed,
    OrderAcknowledged,
    OrderCancelRequested,
    OrderReconciled,
    FillRecorded,
    NeedsReconciliation,
    RecoveryBlocked,
    BaselineRecorded,
    BaselineBlocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationEvent {
    pub session_id: AutomationSessionId,
    pub sequence: u64,
    pub kind: AutomationEventKind,
    pub at_utc: UtcTimestamp,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationEventPage {
    pub items: Vec<AutomationEvent>,
    pub next_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationOrderState {
    Reserved,
    SubmittedUnknown,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    NeedsReconciliation,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationOrderView {
    pub client_identifier: AutomationOrderIdentifier,
    pub intent_id: AutomationIntentId,
    pub market: MarketId,
    pub side: AutomationSide,
    pub state: AutomationOrderState,
    pub venue_order_uuid: Option<String>,
    pub requested_notional_krw: QuoteAmount,
    pub reserved_fee_krw: QuoteAmount,
    pub executed_volume: AssetQuantity,
    pub paid_fee_krw: QuoteAmount,
    pub locked_quote_krw: Option<QuoteAmount>,
    pub locked_base: Option<AssetQuantity>,
    pub trade_count: u64,
    pub created_at_utc: UtcTimestamp,
    pub updated_at_utc: UtcTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationFillView {
    pub trade_uuid: String,
    pub client_identifier: AutomationOrderIdentifier,
    pub market: MarketId,
    pub side: AutomationSide,
    pub price_krw: PriceKrw,
    pub volume: AssetQuantity,
    pub funds_krw: QuoteAmount,
    pub observed_at_utc: UtcTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationLedgerPage {
    pub orders: Vec<AutomationOrderView>,
    pub fills: Vec<AutomationFillView>,
    pub next_sequence: Option<u64>,
}

/// Parse a canonical `YYYY-MM-DD` UTC day string into its `00:00:00Z` timestamp.
///
/// # Errors
/// Rejects malformed or non-zero-padded dates.
pub fn parse_day_start_utc(day_utc: &str) -> Result<UtcTimestamp, LabError> {
    let parsed_date = chrono::NaiveDate::parse_from_str(day_utc, "%Y-%m-%d").map_err(|error| {
        LabError::InvalidConfig(format!(
            "baseline day_utc '{day_utc}' must be YYYY-MM-DD: {error}"
        ))
    })?;
    if parsed_date.format("%Y-%m-%d").to_string() != day_utc {
        return Err(LabError::InvalidConfig(
            "baseline day_utc must use canonical zero-padded YYYY-MM-DD".into(),
        ));
    }
    let naive_dt = parsed_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| LabError::InvalidConfig(format!("invalid day start for '{day_utc}'")))?;
    Ok(UtcTimestamp(
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive_dt, chrono::Utc),
    ))
}

#[cfg(test)]
mod risk_policy_boundary_tests {
    use super::{AutomationRiskPolicy, AutomationSessionMode};
    use crate::contracts::LabError;

    fn valid_risk() -> serde_json::Value {
        serde_json::json!({
            "max_order_notional_krw":"10000.01", "max_daily_notional_krw":"100000.02",
            "max_asset_exposure_krw":"20000.03", "max_daily_loss_krw":"5000.04",
            "max_open_orders":2, "max_orders_per_interval":3,
            "max_balance_age_seconds":60, "max_feed_age_seconds":120
        })
    }

    #[test]
    fn rejects_invalid_monetary_and_count_bounds() -> Result<(), LabError> {
        let valid: AutomationRiskPolicy = serde_json::from_value(valid_risk())?;
        valid.validate()?;
        let invalid = [
            ("max_order_notional_krw", serde_json::json!("0")),
            ("max_daily_notional_krw", serde_json::json!("0")),
            ("max_asset_exposure_krw", serde_json::json!("0")),
            ("max_daily_loss_krw", serde_json::json!("0")),
            ("activation_loss_limit_krw", serde_json::json!("0")),
            ("max_daily_notional_krw", serde_json::json!("1")),
            ("max_order_notional_krw", serde_json::json!("-1")),
            ("max_open_orders", serde_json::json!(0)),
            ("max_open_orders", serde_json::json!(101)),
            ("max_orders_per_interval", serde_json::json!(0)),
            ("max_orders_per_interval", serde_json::json!(11)),
            ("max_balance_age_seconds", serde_json::json!(0)),
            ("max_balance_age_seconds", serde_json::json!(3601)),
            ("max_feed_age_seconds", serde_json::json!(0)),
            ("max_feed_age_seconds", serde_json::json!(86401)),
        ];
        for (field, value) in invalid {
            let mut input = valid_risk();
            input[field] = value;
            let rejected = serde_json::from_value::<AutomationRiskPolicy>(input)
                .map_or(true, |policy| policy.validate().is_err());
            assert!(rejected, "invalid {field} was accepted");
        }
        Ok(())
    }

    #[test]
    fn activation_limit_is_live_only_and_decimal_fields_roundtrip() -> Result<(), LabError> {
        let mut input = valid_risk();
        input["activation_loss_limit_krw"] = serde_json::json!("1234.56");
        let policy: AutomationRiskPolicy = serde_json::from_value(input.clone())?;
        policy.validate_for_mode(AutomationSessionMode::Live)?;
        assert!(
            policy
                .validate_for_mode(AutomationSessionMode::Mock)
                .is_err()
        );
        assert_eq!(serde_json::to_value(&policy)?, input);
        Ok(())
    }
}
