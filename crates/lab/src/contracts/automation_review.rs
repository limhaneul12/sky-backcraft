//! Private, persisted-fact automation review and portable export contracts.

use super::{
    AssetQuantity, AutomationEvent, AutomationFillView, AutomationIntentId,
    AutomationOrderIdentifier, AutomationOrderView, AutomationRiskPolicy, AutomationSessionId,
    AutomationSessionMode, AutomationSessionState, CandleInterval, CandleObservation, ContentHash,
    FrozenPolicyRevision, MarketId, ObservationId, PolicyRevisionRef, PriceKrw, QuoteAmount,
    SignedAmount, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Hard bounded whole-session snapshot, never a silently truncated export.
pub const MAX_AUTOMATION_REVIEW_FACTS: usize = 50_000;
/// Encoded snapshot/export file ceiling.
pub const MAX_AUTOMATION_REVIEW_BYTES: usize = 32 * 1024 * 1024;
/// HEX transport expansion remains below the existing MCP response cap.
pub const AUTOMATION_REVIEW_CHUNK_BYTES: usize = 65_536;

/// Frozen metadata without time-dependent account/authorization projections.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewSession {
    pub session_id: AutomationSessionId,
    pub mode: AutomationSessionMode,
    pub state: AutomationSessionState,
    pub policy_ref: PolicyRevisionRef,
    pub market: MarketId,
    pub decision_interval: CandleInterval,
    pub risk: AutomationRiskPolicy,
    pub created_at_utc: UtcTimestamp,
    pub updated_at_utc: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AutomationReviewAction {
    Buy,
    Sell,
    Hold,
}

/// Exact saved evaluation. Never reruns an evaluator against today's data.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationDecisionRecord {
    pub session_id: AutomationSessionId,
    pub intent_id: AutomationIntentId,
    pub observation_id: ObservationId,
    pub decision_time_utc: UtcTimestamp,
    pub recorded_at_utc: UtcTimestamp,
    pub action: AutomationReviewAction,
    pub state: String,
    pub candle: CandleObservation,
    pub evaluation: serde_json::Value,
    pub evaluation_digest: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationDecisionSummary {
    pub intent_id: AutomationIntentId,
    pub observation_id: ObservationId,
    pub decision_time_utc: UtcTimestamp,
    pub action: AutomationReviewAction,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationDecisionPage {
    pub session_id: AutomationSessionId,
    pub mode: AutomationSessionMode,
    pub policy_ref: PolicyRevisionRef,
    pub items: Vec<AutomationDecisionSummary>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationDecisionDetail {
    pub session: AutomationReviewSession,
    pub frozen_policy: FrozenPolicyRevision,
    pub decision: AutomationDecisionRecord,
    pub context_candles: Vec<CandleObservation>,
    pub orders: Vec<AutomationOrderView>,
    pub events: Vec<AutomationEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewOrder {
    pub order: AutomationOrderView,
    pub decision_mark_krw: PriceKrw,
    pub venue_created_at_utc: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewFill {
    pub fill: AutomationFillView,
    pub stored_trade_time_utc: UtcTimestamp,
    pub recorded_at_utc: UtcTimestamp,
    pub source_digest: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewFee {
    pub client_identifier: AutomationOrderIdentifier,
    pub sequence: u64,
    pub delta_paid_fee_krw: QuoteAmount,
    pub cumulative_paid_fee_krw: QuoteAmount,
    pub observed_at_utc: UtcTimestamp,
}

/// Account evidence observed before the first owned order, not a current balance guess.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewOpeningInventory {
    pub snapshot_sequence: u64,
    pub source_mode: AutomationSessionMode,
    pub observed_at_utc: UtcTimestamp,
    pub base_quantity: AssetQuantity,
    pub balances_digest: ContentHash,
}

/// The sole Store owner reads this consistent bounded snapshot in one command.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewSnapshot {
    pub session: AutomationReviewSession,
    pub frozen_policy: FrozenPolicyRevision,
    /// Includes warmup/catchup observations even when no decision was emitted.
    pub candles: Vec<CandleObservation>,
    pub decisions: Vec<AutomationDecisionRecord>,
    pub orders: Vec<AutomationReviewOrder>,
    pub fills: Vec<AutomationReviewFill>,
    pub fee_accruals: Vec<AutomationReviewFee>,
    pub events: Vec<AutomationEvent>,
    #[serde(default)]
    pub opening_inventory: Option<AutomationReviewOpeningInventory>,
}

/// Nullable numeric metric with a mandatory explanation when unavailable.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewMetric {
    pub value: Option<SignedAmount>,
    pub unit: String,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationPerformanceReport {
    pub schema_version: String,
    pub session: AutomationReviewSession,
    pub snapshot_digest: ContentHash,
    pub snapshot_at_utc: UtcTimestamp,
    pub decision_count: u64,
    /// Persisted replay warmup records, excluded from policy signal counts.
    pub warmup_record_count: u64,
    pub buy_signal_count: u64,
    pub sell_signal_count: u64,
    pub hold_signal_count: u64,
    pub rejected_intent_count: u64,
    pub order_count: u64,
    pub fill_count: u64,
    pub buy_funds_krw: QuoteAmount,
    pub sell_funds_krw: QuoteAmount,
    pub paid_fees_krw: QuoteAmount,
    pub net_quote_cashflow_krw: SignedAmount,
    pub realized_pnl_krw: AutomationReviewMetric,
    pub unrealized_pnl_krw: AutomationReviewMetric,
    pub total_pnl_krw: AutomationReviewMetric,
    pub return_ratio: AutomationReviewMetric,
    pub max_drawdown_ratio: AutomationReviewMetric,
    pub remaining_volume: Option<AssetQuantity>,
    pub remaining_cost_krw: Option<QuoteAmount>,
    pub mark_price_krw: Option<PriceKrw>,
    pub mark_at_utc: Option<UtcTimestamp>,
    pub mark_observation_id: Option<ObservationId>,
    pub mark_content_digest: Option<ContentHash>,
    pub opening_inventory: Option<AutomationReviewOpeningInventory>,
    pub methodology: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AutomationReviewFile {
    DecisionsCsv,
    OrdersCsv,
    FillsCsv,
    ReportJson,
    ReportMarkdown,
    SnapshotJson,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewExportFile {
    pub file: AutomationReviewFile,
    pub file_name: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewExportManifest {
    pub schema_version: String,
    pub session_id: AutomationSessionId,
    pub mode: AutomationSessionMode,
    pub snapshot_digest: ContentHash,
    pub files: Vec<AutomationReviewExportFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutomationReviewExportChunk {
    pub session_id: AutomationSessionId,
    pub mode: AutomationSessionMode,
    pub snapshot_digest: ContentHash,
    pub file: AutomationReviewFile,
    pub file_name: String,
    pub media_type: String,
    pub total_bytes: u64,
    pub sha256: ContentHash,
    pub offset: u64,
    pub bytes: u64,
    pub chunk_sha256: ContentHash,
    pub encoding: String,
    pub data_hex: String,
    pub next_offset: Option<u64>,
}
