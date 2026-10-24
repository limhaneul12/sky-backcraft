//! Safe connection projections; durable market and financial facts live elsewhere.

use super::{
    CandleInterval, MarketId, ProviderAbsenceSpan, ProviderFeedRecoveryProgress, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Provenance of one received WebSocket data message, excluding auth/control frames.
/// Epoch and message sequence are scoped to the managed connection owner; they
/// are evidence coordinates, not a durable business-event or SSE replay cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RawWebSocketTransport {
    pub connection_epoch: u64,
    pub message_sequence: u64,
    pub provider_at: Option<UtcTimestamp>,
    pub received_at: UtcTimestamp,
}

/// Latest connection state, never proof that a market observation is finalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RealtimeConnectionState {
    Unavailable,
    Idle,
    Connecting,
    Recovering,
    Ready,
    Stale,
    Stopped,
    Error,
}

/// Safe, bounded reason that a feed or one market/timeframe is not ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RealtimeReadinessBlocker {
    ConnectionMissing,
    HeartbeatUnconfirmed,
    SubscriptionsNotReady,
    FeedStateNotReady,
    RestBootstrapRequired,
    RecoveryRequired,
    RestOnlyInputUnavailable,
    WebsocketCandleMissing,
    WebsocketCandleStale,
    ProviderClockInvalid,
    StreamNotConfirmed,
    ReconciliationPending,
    RestConfirmationMissing,
    CacheUnavailable,
}

/// Compact public view of one durable provider-absence proof.
/// Full raw witnesses and anchor identities remain in the evidence store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAbsenceSummary {
    pub proof_id: super::ContentHash,
    pub start_utc: UtcTimestamp,
    pub end_utc: UtcTimestamp,
}

impl From<&ProviderAbsenceSpan> for ProviderAbsenceSummary {
    fn from(span: &ProviderAbsenceSpan) -> Self {
        Self {
            proof_id: span.proof_id.clone(),
            start_utc: span.start_utc,
            end_utc: span.end_utc,
        }
    }
}

/// Compact public view of bounded backwards recovery progress.
/// The accepted anchor and its full identity stay in the durable cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderRecoveryProgressSummary {
    pub next_page_to_utc: Option<UtcTimestamp>,
    pub pages_scanned: u32,
    pub rows_scanned: u64,
    pub page_limit: u32,
    pub row_limit: u64,
    pub pending_absence_confirmation: bool,
}

impl From<&ProviderFeedRecoveryProgress> for ProviderRecoveryProgressSummary {
    fn from(progress: &ProviderFeedRecoveryProgress) -> Self {
        Self {
            next_page_to_utc: progress.next_page_to_utc,
            pages_scanned: progress.pages_scanned,
            rows_scanned: progress.rows_scanned,
            page_limit: progress.page_limit,
            row_limit: progress.row_limit,
            pending_absence_confirmation: progress.pending_absence_confirmation,
        }
    }
}

/// Read-only readiness facts for one active public market/timeframe pair.
/// Economic prices and raw provider payloads are intentionally excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RealtimeSubscriptionReadiness {
    pub market: MarketId,
    pub interval: CandleInterval,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<RealtimeReadinessBlocker>,
    /// A new REST tip is awaiting a second identical economic snapshot.
    #[serde(default, skip_serializing_if = "is_false")]
    pub pending_rest_confirmation: bool,
    /// Number of distinct provider-revision snapshot shapes pinned in this process.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub revision_snapshot_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_revision_open_time_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_revision_detected_at_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_received_at_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_at_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_websocket_open_time_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_rest_close_time_utc: Option<UtcTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rest_fetched_at_utc: Option<UtcTimestamp>,
    /// Confirmed provider-response omissions for this market/timeframe. Spans
    /// refer to raw evidence and never represent synthetic candle rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub confirmed_provider_absence_spans: Vec<ProviderAbsenceSummary>,
    /// Bounded progress across backwards REST pages, when recovery is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_recovery_progress: Option<ProviderRecoveryProgressSummary>,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde's skip_serializing_if predicate requires a borrowed argument"
)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde's skip_serializing_if predicate requires a borrowed argument"
)]
const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Bounded aggregate health. This contains no prices, account identifiers or secrets.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RealtimeFeedStatus {
    pub state: RealtimeConnectionState,
    pub connection_count: u32,
    pub subscription_count: u32,
    pub last_received_at_utc: Option<UtcTimestamp>,
    pub last_reconciled_at_utc: Option<UtcTimestamp>,
    pub reconnect_count: u64,
    pub dropped_messages: u64,
    pub ready_for_decisions: bool,
    /// Empty for absent/private feeds. The public list is bounded by the active
    /// feed union limit and may be coalesced in SSE latest-state snapshots.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readiness_blockers: Vec<RealtimeReadinessBlocker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscription_readiness: Vec<RealtimeSubscriptionReadiness>,
}

impl RealtimeFeedStatus {
    /// An absent feed is explicitly unavailable, never a healthy empty stream.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            state: RealtimeConnectionState::Unavailable,
            connection_count: 0,
            subscription_count: 0,
            last_received_at_utc: None,
            last_reconciled_at_utc: None,
            reconnect_count: 0,
            dropped_messages: 0,
            ready_for_decisions: false,
            readiness_blockers: Vec::new(),
            subscription_readiness: Vec::new(),
        }
    }
}

/// A latest-state snapshot. SSE may coalesce snapshots; history uses durable APIs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RealtimeStatus {
    pub observed_at_utc: UtcTimestamp,
    pub market: RealtimeFeedStatus,
    pub private: RealtimeFeedStatus,
}

/// Audit observations do not themselves settle balances, orders or fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrivateStreamObservationKind {
    Baseline,
    OrderObserved,
    AssetObserved,
    BootstrapComplete,
    RestReconciled,
    RestOrderObserved,
}

/// Provider facts are separate from connection/receipt metadata. A repeated
/// `account/event_key` binds the same kind, provider clock and canonical payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrivateStreamObservation {
    pub account_identity: String,
    pub generation: u64,
    pub kind: PrivateStreamObservationKind,
    pub event_key: String,
    pub provider_at_utc: Option<UtcTimestamp>,
    pub received_at_utc: UtcTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<RawWebSocketTransport>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StoredPrivateStreamObservation {
    pub sequence: u64,
    pub observation: PrivateStreamObservation,
}
