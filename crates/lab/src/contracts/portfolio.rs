//! Shared-capital portfolio contracts: one cash pool, many assets, typed
//! rejection reasons and append-only ledger facts.

use super::{
    AssetQuantity, BasisPoints, CandleInterval, LabError, MarketId, ModelId, PriceKrw, QuoteAmount,
    RequestId, RunId, SignedAmount, StrategyKind, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_PORTFOLIO_MARKETS: usize = 3;
/// Bounded ledger facts per portfolio run (fills + rejections + marks + intents).
pub const MAX_PORTFOLIO_EVENTS: usize = 100_000;

/// Deterministic arbitration of simultaneous capital demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArbitrationPolicy {
    /// Asset declaration order wins; simple and explainable (default).
    Priority,
    /// Competing buys scale proportionally to fit the shared pool.
    ProRata,
    /// Higher declared target weight wins; ties fall back to declaration order.
    ScoreRanked,
}

/// One asset slot in the shared-capital portfolio.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAssetSpec {
    pub market: MarketId,
    /// Maximum position value weight relative to portfolio equity.
    pub max_weight: Weight,
}

/// Portfolio-level risk bounds; every limit is enforced at allocation time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRiskPolicy {
    /// Maximum combined position value weight relative to equity.
    pub max_gross_exposure: Weight,
    /// Minimum cash weight relative to equity after any allocation.
    pub min_cash_weight: Weight,
    /// Completed-mark drawdown that blocks new buys; zero disables the stop.
    pub drawdown_stop: Weight,
}

/// Shared-capital portfolio specification; exactly one cash pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioSpec {
    pub initial_cash: QuoteAmount,
    pub assets: Vec<PortfolioAssetSpec>,
    pub risk: PortfolioRiskPolicy,
    pub arbitration: ArbitrationPolicy,
}

impl PortfolioSpec {
    /// # Errors
    /// Rejects empty/oversized asset lists, duplicates, crossed weights and
    /// impossible reserve/exposure combinations.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.assets.is_empty() || self.assets.len() > MAX_PORTFOLIO_MARKETS {
            return Err(LabError::InvalidConfig(format!(
                "portfolio requires 1..={MAX_PORTFOLIO_MARKETS} assets"
            )));
        }
        let mut markets = BTreeSet::new();
        for asset in &self.assets {
            if !markets.insert(asset.market.code()) {
                return Err(LabError::InvalidConfig("duplicate portfolio asset".into()));
            }
            if asset.max_weight.get() <= Decimal::ZERO || asset.max_weight.get() > Decimal::ONE {
                return Err(LabError::InvalidConfig(
                    "asset max_weight must be in (0,1]".into(),
                ));
            }
        }
        let gross = self.risk.max_gross_exposure.get();
        let reserve = self.risk.min_cash_weight.get();
        if gross <= Decimal::ZERO || gross > Decimal::ONE {
            return Err(LabError::InvalidConfig(
                "max_gross_exposure must be in (0,1]".into(),
            ));
        }
        if reserve < Decimal::ZERO || reserve > Decimal::ONE {
            return Err(LabError::InvalidConfig(
                "min_cash_weight must be in [0,1]".into(),
            ));
        }
        let stop = self.risk.drawdown_stop.get();
        if stop < Decimal::ZERO || stop >= Decimal::ONE {
            return Err(LabError::InvalidConfig(
                "drawdown_stop must be in [0,1)".into(),
            ));
        }
        if gross + reserve > Decimal::ONE {
            return Err(LabError::InvalidConfig(
                "max_gross_exposure plus min_cash_weight must not exceed one".into(),
            ));
        }
        if self.initial_cash.get() <= Decimal::ZERO {
            return Err(LabError::InvalidConfig(
                "portfolio initial cash must be positive".into(),
            ));
        }
        Ok(())
    }

    /// Declaration priority of a market; unknown markets sort last.
    #[must_use]
    pub fn priority(&self, market: &MarketId) -> usize {
        self.assets
            .iter()
            .position(|asset| asset.market == *market)
            .unwrap_or(self.assets.len())
    }

    /// Asset cap for one market; unknown markets get zero.
    #[must_use]
    pub fn asset_cap(&self, market: &MarketId) -> Decimal {
        self.assets
            .iter()
            .find(|asset| asset.market == *market)
            .map_or(Decimal::ZERO, |asset| asset.max_weight.get())
    }
}

/// Why one portfolio signal could not allocate capital.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortfolioRejectionReason {
    InsufficientCash,
    AssetWeightCap,
    GrossExposureCap,
    CashReserve,
    PortfolioStop,
    MinNotional,
    RuleViolation,
}

/// One allocation decision recorded before its fill outcome.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioIntentRecord {
    pub event_seq: u64,
    pub decision_time: UtcTimestamp,
    pub market: MarketId,
    pub side: super::Side,
    pub target_weight: Weight,
    pub requested_notional: QuoteAmount,
    /// Arbitration rank at this decision time (0 = first served).
    pub arbitration_rank: u32,
}

/// One executed portfolio fill against the shared cash pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioFillRecord {
    pub event_seq: u64,
    pub market: MarketId,
    pub side: super::Side,
    pub price: PriceKrw,
    pub qty: AssetQuantity,
    pub notional: QuoteAmount,
    pub fee: QuoteAmount,
    pub fee_bps: BasisPoints,
    /// Slippage/impact cost embedded in the fill price; never debited twice (in KRW).
    pub price_cost: SignedAmount,
    /// Per-unit price difference: rounded executed price minus decision close price (in KRW per base unit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_difference_per_unit: Option<SignedAmount>,
    pub reserved_cash: QuoteAmount,
    pub decision_time: UtcTimestamp,
    pub execution_time: UtcTimestamp,
    pub source_bar_id: String,
    pub liquidity_source_bar_id: String,
    /// Cash right after this fill settled.
    pub cash_after: QuoteAmount,
}

/// One rejected signal with its typed reason; never silently dropped.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRejectionRecord {
    pub event_seq: u64,
    pub decision_time: UtcTimestamp,
    pub market: MarketId,
    pub side: super::Side,
    pub target_weight: Weight,
    pub requested_notional: QuoteAmount,
    pub reason: PortfolioRejectionReason,
}

/// Shared-account snapshot at one completed decision mark.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioMarkRecord {
    pub event_seq: u64,
    pub time: UtcTimestamp,
    pub cash: QuoteAmount,
    pub position_value: QuoteAmount,
    pub gross_exposure_weight: Weight,
    pub equity: QuoteAmount,
    pub peak_equity: QuoteAmount,
    pub drawdown: Weight,
    pub stopped: bool,
    /// Position value weights per market code at this mark.
    #[schemars(with = "std::collections::BTreeMap<String, String>")]
    pub weights: BTreeMap<String, Decimal>,
}

/// Per-asset contribution over the whole run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAttribution {
    pub market: MarketId,
    pub strategy: StrategyKind,
    pub buy_notional: QuoteAmount,
    pub sell_notional: QuoteAmount,
    pub fees: QuoteAmount,
    pub realized_pnl: SignedAmount,
    /// Mark-to-market value of the residual position at the terminal mark.
    pub unrealized_pnl: SignedAmount,
    pub closed_trades: u64,
    pub max_weight_seen: Weight,
    pub exposure_seconds: u64,
}

/// Aggregate portfolio metrics; the report projection of one ledger.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioTotals {
    pub terminal_equity: QuoteAmount,
    #[schemars(with = "String")]
    pub total_return: Decimal,
    pub max_drawdown: Weight,
    /// Combined buy+sell notional relative to initial cash.
    #[schemars(with = "String")]
    pub turnover: Decimal,
    pub total_fees: QuoteAmount,
    /// Slippage/impact cost embedded in fill prices (proxy cost drag).
    pub price_cost_drag: SignedAmount,
    pub exposure_seconds: u64,
    pub rejected_signals: u64,
    pub rejection_reasons: BTreeMap<String, u64>,
}

/// Terminal status of a portfolio run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortfolioStatus {
    Completed,
    Failed,
}

/// Complete append-only result of one shared-capital backtest.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioLedger {
    pub run_id: RunId,
    pub model_ids: Vec<ModelId>,
    pub spec: PortfolioSpec,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub status: PortfolioStatus,
    pub status_reason: Option<String>,
    pub intents: Vec<PortfolioIntentRecord>,
    pub fills: Vec<PortfolioFillRecord>,
    pub rejections: Vec<PortfolioRejectionRecord>,
    pub marks: Vec<PortfolioMarkRecord>,
    pub attribution: Vec<PortfolioAttribution>,
    pub totals: PortfolioTotals,
    /// Regime observations when the run was gated; sorted by (market, time).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regime_observations: Vec<super::RegimeObservation>,
    pub last_event_seq: u64,
}

/// Request to run one shared-capital portfolio backtest against a frozen plan.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRunRequest {
    pub request_id: RequestId,
    pub plan_id: super::PlanId,
    pub input_digest: super::ContentHash,
    pub portfolio: PortfolioSpec,
    /// Optional causal regime gating; absent keeps every strategy enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime: Option<super::RegimeGateSpec>,
}

/// Closed ledger fact kinds exposed by the facts query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioFactKind {
    Intent,
    Fill,
    Rejection,
    Mark,
}

impl PortfolioFactKind {
    /// Persisted column spelling for this fact kind.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Intent => "intent",
            Self::Fill => "fill",
            Self::Rejection => "rejection",
            Self::Mark => "mark",
        }
    }
}

/// Bounded actions over durable portfolio runs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PortfolioAction {
    Create {
        request: Box<PortfolioRunRequest>,
    },
    Get {
        run_id: RunId,
    },
    Facts {
        run_id: RunId,
        kind: PortfolioFactKind,
        offset: u64,
        limit: u32,
    },
    List {
        offset: u64,
        limit: u32,
    },
}

/// Benchmark kinds comparable on the same period and cost model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BenchmarkKind {
    Cash,
    BuyAndHold,
    /// Equal-weight buy-and-hold across all portfolio assets, rebalanced never.
    StaticAllocation,
}

/// One benchmark equity path summary.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkResult {
    pub kind: BenchmarkKind,
    pub market: Option<MarketId>,
    pub terminal_equity: QuoteAmount,
    #[schemars(with = "String")]
    pub total_return: Decimal,
    pub max_drawdown: Weight,
}
