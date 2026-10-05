//! Append-only simulated facts and portable reconstruction inputs.

use super::{
    ArtifactId, AssetQuantity, AttemptId, BasisPoints, ContentHash, DatasetSnapshot, EpisodeId,
    EvidenceDecisionEffect, EvidenceSnapshot, FillId, JobId, MarketDataOrigin, MarketId, ModelId,
    ModelStatus, ObservationId, OrderId, PriceKrw, QuoteAmount, ResolvedPlan, RuleSnapshotId,
    RunId, SignalId, SignedAmount, StrategyBinding, StrategyKind, UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PositionState {
    Cash,
    Long,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReasonCode {
    StartFromCash,
    BandEntry,
    BandExit,
    Breakout,
    Breakdown,
    HoldState,
    TrendPullbackEntry,
    TrendInvalidated,
    RsiRecovery,
    HoldingTimeout,
    EvidenceUnavailable,
    EvidenceOverlay,
    RebalanceBand,
    NoAction,
    CapitalBlocked,
    RuleBlocked,
    QuantityRoundedToZero,
    SignalExpired,
    WouldTakeOrUnknown,
    PassiveNotPenetrated,
    PassivePartial,
    PassiveRemainderCancelled,
    MarketFilled,
    VolumeCap,
    TerminalMark,
    ArtificialTerminalExit,
    Cancelled,
    PolicyRule,
    PolicyFallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSnapshot {
    pub version: String,
    pub ema: Option<f64>,
    pub bar_sigma: Option<f64>,
    pub annual_vol: Option<f64>,
    pub rsi: Option<f64>,
    pub entry_high: Option<PriceKrw>,
    pub exit_low: Option<PriceKrw>,
    pub completed_bars_seen: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_values: Vec<super::NamedPolicyValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventContext {
    pub run_id: RunId,
    pub model_id: ModelId,
    pub market: MarketId,
    /// Unique across a run; deterministic model order followed by causal model order.
    pub event_seq: u64,
    pub accounting_event_time: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalOutcome {
    Executed,
    NoAction,
    CapitalBlocked,
    RuleBlocked,
    EvidenceUnavailable,
    OrderUnfilled,
    SignalExpired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignalRecord {
    pub context: EventContext,
    pub signal_id: SignalId,
    pub strategy: StrategyKind,
    pub strategy_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<super::PolicyRevisionRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_trace: Option<super::PolicyTrace>,
    pub source_bar_ids: Vec<ObservationId>,
    pub signal_time: UtcTimestamp,
    pub decision_available_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub state_before: PositionState,
    pub state_after: PositionState,
    pub raw_target_weight: Weight,
    pub constrained_target_weight: Weight,
    pub intended_side: Option<Side>,
    pub reasons: Vec<ReasonCode>,
    pub indicators: IndicatorSnapshot,
    pub evidence_effect: Option<EvidenceDecisionEffect>,
    pub outcome: SignalOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulatedOrderType {
    Market,
    PassiveBuyLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Created,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrderRecord {
    pub context: EventContext,
    pub order_id: OrderId,
    pub parent_signal_id: SignalId,
    pub episode_id: Option<EpisodeId>,
    pub side: Side,
    pub order_type: SimulatedOrderType,
    pub requested_price: Option<PriceKrw>,
    pub requested_qty: AssetQuantity,
    pub created_at: UtcTimestamp,
    pub effective_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
    pub rule_snapshot_id: RuleSnapshotId,
    pub policy_version: String,
    pub reserved_cash: QuoteAmount,
    pub cumulative_filled_qty: AssetQuantity,
    pub status: OrderStatus,
    pub reason: ReasonCode,
    pub order_origin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum FillTiming {
    Exact {
        at: UtcTimestamp,
    },
    Interval {
        start: UtcTimestamp,
        end: UtcTimestamp,
        accounting_at: UtcTimestamp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FillRecord {
    pub context: EventContext,
    pub fill_id: FillId,
    pub order_id: OrderId,
    pub episode_id: EpisodeId,
    pub side: Side,
    pub price: PriceKrw,
    pub qty: AssetQuantity,
    pub notional: QuoteAmount,
    pub fee: QuoteAmount,
    pub fee_bps: BasisPoints,
    pub fee_currency: String,
    pub timing: FillTiming,
    pub source_bar_id: ObservationId,
    pub liquidity_source_bar_id: ObservationId,
    pub liquidity_source_close_time: UtcTimestamp,
    pub decision_reference: PriceKrw,
    pub bar_open_proxy: PriceKrw,
    /// No L2/mid observation is captured in this MVP.
    pub arrival_mid: Option<PriceKrw>,
    pub arrival_mid_null_reason: String,
    pub adverse_slippage_bps: SignedAmount,
    /// Diagnostic already embedded in the price; never debit it again.
    pub price_cost_attribution: SignedAmount,
    pub liquidity_role_assumption: String,
    pub execution_origin: String,
    pub fill_observed: bool,
    pub model_version: String,
    pub artificial_terminal_exit: bool,
    /// Proxy liquidity inputs behind a dynamic cost model; absent for fixed bps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_provenance: Option<super::CostProvenance>,
    /// The exact after-fill account mark is committed in the same fact batch.
    pub accounting_mark_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountState {
    pub cash_total: QuoteAmount,
    pub cash_free: QuoteAmount,
    pub cash_reserved: QuoteAmount,
    pub qty: AssetQuantity,
    pub price_basis: QuoteAmount,
    pub gross_realized: SignedAmount,
    pub cumulative_fees: QuoteAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarkKind {
    Initial,
    AfterFill,
    ExecutionClose,
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountMark {
    pub context: EventContext,
    pub kind: MarkKind,
    pub state: AccountState,
    pub mark_price: PriceKrw,
    pub source_bar_id: ObservationId,
    pub position_value: QuoteAmount,
    pub gross_unrealized: SignedAmount,
    pub equity: QuoteAmount,
    pub target_weight: Weight,
    pub actual_weight: Weight,
    pub peak_equity: QuoteAmount,
    pub drawdown: Weight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EpisodeStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EpisodeRecord {
    pub episode_id: EpisodeId,
    pub run_id: RunId,
    pub model_id: ModelId,
    pub market: MarketId,
    pub status: EpisodeStatus,
    pub opened_at: UtcTimestamp,
    pub closed_at: Option<UtcTimestamp>,
    pub fill_ids: Vec<FillId>,
    pub order_ids: Vec<OrderId>,
    pub buy_vwap: PriceKrw,
    pub sell_vwap: Option<PriceKrw>,
    pub max_qty: AssetQuantity,
    pub time_weighted_avg_qty: AssetQuantity,
    pub holding_seconds: u64,
    pub realized_price_pnl: SignedAmount,
    pub fees: QuoteAmount,
    pub net_realized: SignedAmount,
    pub residual_basis: QuoteAmount,
    pub residual_qty: AssetQuantity,
    pub marked_unrealized: SignedAmount,
    pub exit_reason: Option<ReasonCode>,
    pub start_equity: QuoteAmount,
    pub mae_amount: SignedAmount,
    pub mfe_amount: SignedAmount,
    pub mae_pct_of_start_equity: SignedAmount,
    pub mfe_pct_of_start_equity: SignedAmount,
    pub time_to_mae_seconds: u64,
    pub time_to_mfe_seconds: u64,
    pub sampling_definition: String,
    pub exit_peak_giveback: QuoteAmount,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelLedger {
    pub model_id: ModelId,
    pub market: MarketId,
    pub strategy: StrategyBinding,
    pub status: ModelStatus,
    pub status_reason: Option<String>,
    pub signals: Vec<SignalRecord>,
    /// One final projection per order ID; lifecycle facts are in `order_events`.
    pub orders: Vec<OrderRecord>,
    pub order_events: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub episodes: Vec<EpisodeRecord>,
    pub account_marks: Vec<AccountMark>,
    pub last_event_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub schema_version: String,
    pub run_id: RunId,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub created_at: UtcTimestamp,
    pub code_revision: Option<String>,
    pub source_digest: ContentHash,
    pub lockfile_digest: ContentHash,
    pub toolchain: String,
    pub engine_version: String,
    pub indicator_version: String,
    pub rounding_version: String,
    pub mode: String,
    pub historical_availability_model: String,
    pub execution_origin: String,
    pub fill_observed: bool,
    pub origin: MarketDataOrigin,
    pub numeric_tolerance: String,
    pub metric_tolerance: f64,
    pub start_policy: String,
    pub account_mode: String,
    pub assumptions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunBundle {
    pub manifest: RunManifest,
    pub plan: ResolvedPlan,
    pub datasets: Vec<DatasetSnapshot>,
    pub evidence: Option<EvidenceSnapshot>,
    pub models: Vec<ModelLedger>,
    pub semantic_digest: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationStatus {
    Pass,
    Fail,
    NotRun,
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidationReport {
    pub check_id: String,
    pub status: ValidationStatus,
    pub run_id: Option<RunId>,
    pub input_digest: ContentHash,
    pub checked_models: u64,
    pub checked_fills: u64,
    pub checked_marks: u64,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Catalog lookup ID in export v4; earlier manifests carry package-local IDs.
    pub id: ArtifactId,
    pub run_id: RunId,
    pub relative_path: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: ContentHash,
    pub uncompressed_sha256: Option<ContentHash>,
    pub uncompressed_bytes: Option<u64>,
    pub complete: bool,
}
