//! Typed, bounded projections for durable shared-portfolio results.

use super::{
    ContentHash, MarketId, ModelId, PolicyRevisionRef, QuoteAmount, RunId, Side, SignedAmount,
    StrategyKind, UtcTimestamp, VolState, Weight,
};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum rows returned by one portfolio projection query.
pub const MAX_PORTFOLIO_PROJECTION_PAGE: u32 = 500;

/// Why a durable projection cannot be returned for a known run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortfolioProjectionUnavailableReason {
    /// The run predates the projection migration. Its legacy facts remain
    /// queryable, but missing research facts are never reconstructed.
    LegacyProjectionMissing,
    /// This run did not freeze a regime classifier/gate.
    RegimeNotConfigured,
}

/// Availability is explicit so old rows never acquire invented projections.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortfolioProjection<T> {
    Available {
        data: T,
    },
    Unavailable {
        reason: PortfolioProjectionUnavailableReason,
    },
}

/// One bounded page with an exact total and continuation offset.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioProjectionPage<T> {
    pub items: Vec<T>,
    pub total_count: u64,
    pub next_offset: Option<u64>,
}

/// Frozen strategy source used by one portfolio asset.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioPolicySource {
    pub market: MarketId,
    pub model_id: ModelId,
    pub strategy: StrategyKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<PolicyRevisionRef>,
}

/// Exact terminal projection of one shared cash pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioResultSummary {
    pub run_id: RunId,
    pub initial_cash: QuoteAmount,
    pub terminal_equity: QuoteAmount,
    #[schemars(with = "String")]
    pub total_return: Decimal,
    pub max_drawdown: Weight,
    #[schemars(with = "String")]
    pub cash_weight: Decimal,
    #[schemars(with = "String")]
    pub turnover: Decimal,
    pub fees: QuoteAmount,
    /// Price impact/slippage already embedded in fills and portfolio `PnL`.
    pub embedded_cost: SignedAmount,
    pub portfolio_pnl: SignedAmount,
    pub policy_sources: Vec<PortfolioPolicySource>,
}

/// One completed shared-account mark.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioEquityPoint {
    pub timestamp: UtcTimestamp,
    pub equity: QuoteAmount,
    pub cash: QuoteAmount,
    pub position_value: QuoteAmount,
    pub drawdown: Weight,
}

/// One market weight and its frozen policy source at an allocation mark.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAssetAllocation {
    pub market: MarketId,
    #[schemars(with = "String")]
    pub weight: Decimal,
    pub source: PortfolioPolicySource,
}

/// Allocation state after one completed mark.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAllocationPoint {
    pub timestamp: UtcTimestamp,
    #[schemars(with = "String")]
    pub cash_weight: Decimal,
    pub assets: Vec<PortfolioAssetAllocation>,
}

/// One actual rebalance fill against the shared pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRebalancePoint {
    pub decision_time: UtcTimestamp,
    pub execution_time: UtcTimestamp,
    pub market: MarketId,
    pub side: Side,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_weight: Option<Weight>,
    pub notional: QuoteAmount,
    pub fee: QuoteAmount,
    /// Diagnostic cost already included in the fill price (in KRW).
    pub embedded_cost: SignedAmount,
    /// Per-unit price difference: rounded executed price minus decision close price (in KRW per base unit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_difference_per_unit: Option<SignedAmount>,
    pub cash_after: QuoteAmount,
}

/// Exact whole-run contribution for one market.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioMarketContribution {
    pub market: MarketId,
    pub strategy: StrategyKind,
    pub gross_pnl: SignedAmount,
    pub fees: QuoteAmount,
    pub net_pnl: SignedAmount,
}

/// The same economic contribution projected through the frozen policy axis.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioPolicyContribution {
    pub source: PortfolioPolicySource,
    pub gross_pnl: SignedAmount,
    pub fees: QuoteAmount,
    pub net_pnl: SignedAmount,
}

/// Reconciled attribution. Market and policy vectors are alternate views;
/// only market net `PnL` plus residual forms the additive reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioContributions {
    pub run_id: RunId,
    pub market: Vec<PortfolioMarketContribution>,
    pub policy: Vec<PortfolioPolicyContribution>,
    /// Negative fee contribution, exposed separately for review.
    pub fees: SignedAmount,
    /// Embedded execution-price cost; diagnostic only, never subtracted twice.
    pub embedded_cost: SignedAmount,
    /// Nominal no-yield cash has exactly zero `PnL`. No opportunity-cost
    /// benchmark is fabricated.
    pub cash_drag: SignedAmount,
    pub portfolio_pnl: SignedAmount,
    pub residual: SignedAmount,
    /// Absolute Decimal tolerance used by publication reconciliation.
    #[schemars(with = "String")]
    pub tolerance: Decimal,
}

/// Why one regime label was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RegimeObservationReason {
    WarmupIncomplete,
    Classified,
}

/// Durable causal regime timeline entry and transition edge.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRegimeTimelinePoint {
    pub market: MarketId,
    pub timestamp: UtcTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_regime: Option<super::RegimeLabel>,
    pub current_regime: super::RegimeLabel,
    pub transitioned: bool,
    pub vol_state: VolState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<super::RegimeFeatures>,
    pub reason: RegimeObservationReason,
    pub classifier_revision: String,
    pub feature_reference: ContentHash,
}

/// Exact bounded metrics for one market/regime bucket.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRegimeMetrics {
    pub market: MarketId,
    pub regime: super::RegimeLabel,
    pub time_seconds: u64,
    pub pnl: SignedAmount,
    /// Largest whole-portfolio drawdown observed while this market carried
    /// the label; it is descriptive and is not summed across markets.
    pub max_drawdown: Weight,
    pub trade_count: u64,
    #[schemars(with = "String")]
    pub turnover: Decimal,
    pub fees: QuoteAmount,
    pub embedded_cost: SignedAmount,
    pub transition_count: u64,
}

/// Whole-run regime research projection.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRegimeResult {
    pub run_id: RunId,
    pub buckets: Vec<PortfolioRegimeMetrics>,
    /// Counts by stable `MARKET:PREVIOUS->CURRENT` key.
    pub transitions: BTreeMap<String, u64>,
}
