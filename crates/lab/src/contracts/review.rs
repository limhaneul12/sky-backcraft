//! Passive paper evidence selection and additive observation projections.

use super::{
    CandleInterval, ContentHash, CounterfactualAnalysisRequest, FrozenPolicyRevision, LabError,
    MachineContractComparisonInput, ObservationId, PaperSessionEvent, PaperSessionId,
    PaperSessionStartRequest, PolicyConditionTrace, PositionState, RawObjectId, ReasonCode,
    SignalId, SignalOutcome, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_PAPER_REVIEW_SOURCE_BARS: u32 = 4_096;
pub const MAX_PAPER_REVIEW_INPUT_BYTES: usize = 512 * 1_024;
pub const MAX_PAPER_REVIEW_OUTPUT_BYTES: usize = 512 * 1_024;

/// An immutable event inside an explicitly selected saved prefix.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(inline)]
pub struct PaperReviewSelection {
    pub session_id: PaperSessionId,
    pub event_sequence: u64,
    pub as_of_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_id: Option<SignalId>,
}

impl PaperReviewSelection {
    /// Validate the event boundary before any storage read.
    /// # Errors
    /// Rejects empty or reversed event-prefix boundaries.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.event_sequence == 0 || self.event_sequence > self.as_of_sequence {
            return Err(LabError::InvalidConfig(
                "paper review requires 1 <= event_sequence <= as_of_sequence".into(),
            ));
        }
        Ok(())
    }
}

/// New calculations remain explicit read-only opt-ins, separate from polling.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PaperReviewQuery {
    Decision {
        selection: PaperReviewSelection,
    },
    AuditRules {
        selection: PaperReviewSelection,
    },
    AuditIndicators {
        selection: PaperReviewSelection,
        max_source_bars: u32,
    },
    CompareContract {
        selection: PaperReviewSelection,
        contract: MachineContractComparisonInput,
    },
    Counterfactual {
        request: CounterfactualAnalysisRequest,
    },
}

impl PaperReviewQuery {
    /// Validate a bounded, additive review operation.
    /// # Errors
    /// Rejects invalid selection, source limits and oversized request bodies.
    pub fn validate(&self) -> Result<(), LabError> {
        match self {
            Self::Decision { selection }
            | Self::AuditRules { selection }
            | Self::CompareContract { selection, .. } => selection.validate()?,
            Self::AuditIndicators {
                selection,
                max_source_bars,
            } => {
                selection.validate()?;
                if !(1..=MAX_PAPER_REVIEW_SOURCE_BARS).contains(max_source_bars) {
                    return Err(LabError::InvalidConfig(
                        "source review limit must be in 1..=4096".into(),
                    ));
                }
            }
            Self::Counterfactual { request } => request.validate_bounds()?,
        }
        if serde_json::to_vec(self)?.len() > MAX_PAPER_REVIEW_INPUT_BYTES {
            return Err(LabError::ResourceLimit(
                "paper review request exceeds byte budget".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SavedFactCaptureStatus {
    Captured,
    PartialCapture,
    NotCaptured,
    NotDecisionEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaperReviewUnavailableReason {
    ConditionTraceNotCaptured,
    SourceProvenanceNotCaptured,
    SourceObservationMissing,
    ReceiptNotRetainedBeforeEvent,
    ProviderFirstSeenNotReported,
    PaperScenarioDoesNotAttestLiveTradeability,
}

/// Receipt clocks describe retained HTTP/WS evidence, not provider first-ever visibility.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperRetainedReceipt {
    pub raw_object_id: RawObjectId,
    pub fetched_at_utc: UtcTimestamp,
    pub persisted_at_utc: UtcTimestamp,
}

/// Source identity and the earliest receipt retained before this event was recorded.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperObservationProvenance {
    pub observation_id: ObservationId,
    pub interval: CandleInterval,
    pub content_digest: Option<ContentHash>,
    pub open_time: Option<UtcTimestamp>,
    pub close_time: Option<UtcTimestamp>,
    pub raw_object_ids: Vec<RawObjectId>,
    pub constituent_ids: Vec<ObservationId>,
    pub first_retained_receipt: Option<PaperRetainedReceipt>,
    /// Saved `source_event_at` processing clock used to bound eligible local receipts.
    pub receipt_cutoff_utc: Option<UtcTimestamp>,
    /// No provider API in this contract supplies a first-ever visibility attestation.
    pub provider_first_seen_at_utc: Option<UtcTimestamp>,
    pub unavailable_reasons: Vec<PaperReviewUnavailableReason>,
}

/// Descriptive data observations never grant live-order admission.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperDecisionDataQuality {
    pub condition_trace: SavedFactCaptureStatus,
    pub source_provenance: SavedFactCaptureStatus,
    pub decision_bar_completed_at_logical_time: Option<bool>,
    pub realtime_tradable: Option<bool>,
    pub unavailable_reasons: Vec<PaperReviewUnavailableReason>,
}

/// Observations retain explicit roles and actual outcomes. A false leaf inside
/// an OR is not, by itself, proof that it caused the final non-entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperNonEntryObservations {
    pub saved_outcome: SignalOutcome,
    pub saved_reasons: Vec<ReasonCode>,
    pub state_before: PositionState,
    pub declared_conditions: Vec<PolicyConditionTrace>,
    pub roles_not_declared: bool,
    pub causal_rejection_attribution_available: bool,
}

/// Original facts plus explicitly separated, read-time provenance metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperDecisionReview {
    pub schema_version: String,
    pub selection: PaperReviewSelection,
    pub configuration: PaperSessionStartRequest,
    pub event: PaperSessionEvent,
    pub original_event_digest: ContentHash,
    pub policy: FrozenPolicyRevision,
    pub selected_signal_id: Option<SignalId>,
    pub source_observations: Vec<PaperObservationProvenance>,
    pub data_quality: PaperDecisionDataQuality,
    pub non_entry_observations: Option<PaperNonEntryObservations>,
    /// Read-time rendering of the recorded path, separate from independent audits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_decision: Option<super::RecordedDecisionExplanation>,
}

/// Bind an independent calculation to the selected immutable saved event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PaperReviewAuditResponse {
    pub schema_version: String,
    pub selection: PaperReviewSelection,
    pub original_event_digest: ContentHash,
    pub report: super::ReviewAuditReport,
}
