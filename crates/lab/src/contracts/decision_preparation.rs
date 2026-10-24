//! Original preparation facts for candidates that have not reached evaluation.

use super::{CandleInterval, MarketId, ObservationId, UtcTimestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionPreparationStatus {
    PolicyWarmup,
    AwaitingPolicySources,
    AwaitingExecutionBar,
    PrivateReadinessBlocked,
    RecoveryRequired,
    CatchUpStateOnly,
}

/// These are input/readiness observations, never fabricated strategy HOLDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionPreparation {
    pub status: DecisionPreparationStatus,
    pub market: MarketId,
    pub interval: CandleInterval,
    pub observation_id: ObservationId,
    pub decision_time: UtcTimestamp,
    pub completed_bars_seen: u64,
    pub required_bars: u64,
}

/// The exact already-evaluated phase at which a safety gate deferred progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionDeferredPhase {
    BeforeAdmission,
    BeforeSendClaim,
    AfterSendClaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionDeferredReason {
    PrivateAccountRecoveryRequired,
    EffectAuthorizationBlocked,
}

/// Original gate observation; it never replaces the frozen strategy evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdmissionDeferredFact {
    pub phase: AdmissionDeferredPhase,
    pub reason: AdmissionDeferredReason,
    pub intent_id: super::AutomationIntentId,
    pub observation_id: ObservationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_identifier: Option<super::AutomationOrderIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_detail: Option<String>,
}

/// Storage resolves the immutable intent/observation links from this durable key.
#[derive(Debug, Clone)]
pub(crate) enum AdmissionDeferredTarget {
    PendingIntent(super::AutomationIntentId),
    ReservedOrder(super::AutomationOrderIdentifier),
    ClaimedOrder(super::AutomationOrderIdentifier),
}
