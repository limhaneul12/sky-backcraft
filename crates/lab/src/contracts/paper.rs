//! Durable simulated-paper session requests and read projections.

use super::{
    AccountMark, AssetQuantity, CandleInterval, ContentHash, CostPolicy, EpisodeRecord,
    ExecutionPolicy, FillRecord, MarketId, ModelId, OrderRecord, PolicyRevisionRef, PriceKrw,
    QuoteAmount, RequestId, SignalRecord, StrategyKind, TickBand, UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PaperSessionWrite {
    Start {
        request_id: RequestId,
        policy_ref: PolicyRevisionRef,
        decision_interval: CandleInterval,
        execution_resolution: CandleInterval,
        latency_ms: u64,
        execution: ExecutionPolicy,
        markets: Vec<PaperMarketStart>,
    },
    Pause {
        request_id: RequestId,
        session_id: super::PaperSessionId,
        expected_sequence: u64,
    },
    Resume {
        request_id: RequestId,
        session_id: super::PaperSessionId,
        expected_sequence: u64,
    },
    Stop {
        request_id: RequestId,
        session_id: super::PaperSessionId,
        expected_sequence: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperMarketStart {
    pub market: MarketId,
    pub initial_cash: QuoteAmount,
    pub costs: CostPolicy,
    pub market_rules: PaperMarketRuleScenario,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PaperMarketRuleScenario {
    SimulatedScenario {
        min_notional: QuoteAmount,
        quantity_step: AssetQuantity,
        ticks: Vec<TickBand>,
        assumption_label: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PaperSessionQuery {
    StartOptions,
    List {
        after_session_id: Option<super::PaperSessionId>,
        limit: u32,
    },
    Get {
        session_id: super::PaperSessionId,
    },
    Events {
        session_id: super::PaperSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    },
    Request {
        request_id: RequestId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperSessionState {
    Running,
    Paused,
    Stopped,
    RecoveryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperRecoveryCause {
    FeedGap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperSessionMode {
    Simulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperFreshness {
    Live,
    Delayed,
    Stale,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperVerificationStatus {
    NotAvailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperNullReason {
    DataNotCaptured,
    NotApplicable,
    InsufficientHistory,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperBenchmarkKind {
    BuyAndHold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperEventKind {
    CandleProcessed,
    Pause,
    Resume,
    Stop,
    RecoveryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PaperSessionCommandAction {
    Start,
    Pause,
    Resume,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperCommandOutcome {
    Applied,
    Replayed,
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperPolicyOption {
    pub reference: PolicyRevisionRef,
    pub family: StrategyKind,
    pub name: String,
    pub supported: bool,
    pub unavailable_reason: Option<String>,
    pub decision_warmup_bars: u32,
    pub source_intervals: Vec<CandleInterval>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperScenarioPreset {
    pub preset_id: String,
    pub name: String,
    pub initial_cash: QuoteAmount,
    pub costs: CostPolicy,
    pub market_rules: PaperMarketRuleScenario,
    pub execution: ExecutionPolicy,
    pub latency_ms: u64,
    pub editable: bool,
    pub source_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionLimits {
    pub active_sessions: u32,
    pub markets_per_session: u32,
    pub recovery_bars_per_market_interval: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionStartOptions {
    pub policy_options: Vec<PaperPolicyOption>,
    pub scenario_presets: Vec<PaperScenarioPreset>,
    pub decision_intervals: Vec<CandleInterval>,
    pub execution_resolutions: Vec<CandleInterval>,
    pub limits: PaperSessionLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperBenchmarkSnapshot {
    pub kind: PaperBenchmarkKind,
    pub initial_cash: QuoteAmount,
    pub equity: QuoteAmount,
    pub pnl: super::SignedAmount,
    pub return_fraction: super::SignedAmount,
    pub base_observation_id: super::ObservationId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperVerification {
    pub status: PaperVerificationStatus,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperCandleReference {
    pub observation_id: super::ObservationId,
    pub interval: CandleInterval,
    pub open_time: UtcTimestamp,
    pub close_time: UtcTimestamp,
    pub content_digest: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperMarketSnapshot {
    pub market: MarketId,
    pub model_id: ModelId,
    pub initial_cash: QuoteAmount,
    pub cash_total: QuoteAmount,
    pub cash_free: QuoteAmount,
    pub cash_reserved: QuoteAmount,
    pub quantity: AssetQuantity,
    pub mark_price: Option<PriceKrw>,
    pub equity: Option<QuoteAmount>,
    pub realized_pnl: super::SignedAmount,
    pub unrealized_pnl: Option<super::SignedAmount>,
    pub total_fees: QuoteAmount,
    pub drawdown_amount: Option<QuoteAmount>,
    pub drawdown_fraction: Option<Weight>,
    pub signal_count: u64,
    pub fill_count: u64,
    pub event_count: u64,
    pub last_source_event_at: Option<UtcTimestamp>,
    pub last_finalized_candle_at: Option<UtcTimestamp>,
    pub last_account_mark_at: Option<UtcTimestamp>,
    pub last_heartbeat_at: Option<UtcTimestamp>,
    pub freshness: PaperFreshness,
    pub benchmark: Option<PaperBenchmarkSnapshot>,
    pub benchmark_null_reason: Option<PaperNullReason>,
    pub verification: PaperVerification,
    pub costs: CostPolicy,
    pub market_rules: PaperMarketRuleScenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionView {
    pub session_id: super::PaperSessionId,
    pub origin_request_id: RequestId,
    pub state: PaperSessionState,
    pub sequence: u64,
    pub policy_ref: PolicyRevisionRef,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub mode: PaperSessionMode,
    pub created_at: UtcTimestamp,
    pub started_at: UtcTimestamp,
    pub paused_at: Option<UtcTimestamp>,
    pub stopped_at: Option<UtcTimestamp>,
    pub last_evaluated_at: Option<UtcTimestamp>,
    pub event_count: u64,
    pub markets: Vec<PaperMarketSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_cause: Option<PaperRecoveryCause>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionGetResponse {
    pub session: PaperSessionView,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionCursor {
    pub session_id: super::PaperSessionId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionPage {
    pub items: Vec<PaperSessionView>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_cursor: Option<PaperSessionCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperControlEvent {
    pub action: PaperSessionCommandAction,
    pub cancelled_pending_orders: Vec<OrderRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionEvent {
    pub sequence: u64,
    pub kind: PaperEventKind,
    pub market: Option<MarketId>,
    pub candle: Option<PaperCandleReference>,
    pub source_event_at: Option<UtcTimestamp>,
    pub finalized_at: Option<UtcTimestamp>,
    pub control: Option<PaperControlEvent>,
    pub signals: Vec<SignalRecord>,
    pub orders: Vec<OrderRecord>,
    pub order_events: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub account_marks: Vec<AccountMark>,
    pub episodes: Vec<EpisodeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperEventCursor {
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionEventPage {
    pub session_id: super::PaperSessionId,
    pub items: Vec<PaperSessionEvent>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_cursor: Option<PaperEventCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionReceipt {
    pub request_id: RequestId,
    pub session_id: super::PaperSessionId,
    pub action: PaperSessionCommandAction,
    pub outcome: PaperCommandOutcome,
    pub expected_sequence: Option<u64>,
    pub sequence: u64,
    pub created_at: UtcTimestamp,
    pub cancelled_pending_orders: Vec<OrderRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionCommandResponse {
    pub receipt: PaperSessionReceipt,
    pub session: PaperSessionView,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionRequestReadback {
    pub request_id: RequestId,
    pub found: bool,
    pub action: Option<PaperSessionCommandAction>,
    pub session_id: Option<super::PaperSessionId>,
    pub receipt: Option<PaperSessionReceipt>,
    pub session: Option<PaperSessionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperSessionStartRequest {
    pub request_id: RequestId,
    pub policy_ref: PolicyRevisionRef,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub latency_ms: u64,
    pub execution: ExecutionPolicy,
    pub markets: Vec<PaperMarketStart>,
}

impl PaperSessionWrite {
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::Start { request_id, .. }
            | Self::Pause { request_id, .. }
            | Self::Resume { request_id, .. }
            | Self::Stop { request_id, .. } => request_id,
        }
    }

    #[must_use]
    pub const fn action(&self) -> PaperSessionCommandAction {
        match self {
            Self::Start { .. } => PaperSessionCommandAction::Start,
            Self::Pause { .. } => PaperSessionCommandAction::Pause,
            Self::Resume { .. } => PaperSessionCommandAction::Resume,
            Self::Stop { .. } => PaperSessionCommandAction::Stop,
        }
    }
}
