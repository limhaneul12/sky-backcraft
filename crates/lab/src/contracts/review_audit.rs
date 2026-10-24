//! Bounded evidence DTOs for independent read-only research audits.

use super::{
    AutomationRiskPolicy, CandleInterval, CandleObservation, ContentHash, DatasetId,
    FrozenPolicyRevision, LabError, MarketId, NamedPolicyValue, ObservationId, PolicyEvidenceTrace,
    PolicyRevisionRef, PositionState, UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_REVIEW_AUDIT_SOURCE_SERIES: usize = 16;
pub const MAX_REVIEW_AUDIT_SOURCE_BARS: usize = 20_000;
pub const MAX_REVIEW_AUDIT_CROSS_VALUES: usize = 128;
pub const MAX_REVIEW_AUDIT_NAMED_VALUES: usize = 16;
pub const MAX_REVIEW_AUDIT_EVIDENCE_ITEMS: usize = 256;
pub const MAX_REVIEW_AUDIT_OBSERVATION_LINKS_PER_BAR: usize = 64;
pub const MAX_REVIEW_AUDIT_TOTAL_OBSERVATION_LINKS: usize = 40_000;
pub const MAX_REVIEW_AUDIT_FACTS: usize = 5;
pub const MAX_REVIEW_AUDIT_EVIDENCE_BYTES: usize = 8 * 1024 * 1024;

/// Audit kind is explicit in every returned report and wire payload.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewAuditKind {
    SnapshotRuleAudit,
    SourceIndicatorAudit,
    MachineContractComparison,
}

/// A passing result means only that the supplied bounded evidence agrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewAuditStatus {
    Verified,
    Contradiction,
    Unsupported,
    InsufficientEvidence,
}

/// Limits the claim made by an audit, especially for reconstructed rule state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewAuditBasis {
    ReconstructedFromSavedSnapshot,
    RecomputedFromRetainedSourceBars,
    ComparedExplicitMachineFacts,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewAuditDiagnosticCode {
    BuiltinPolicyUnsupported,
    UnsupportedAuditShape,
    UnsupportedArithmetic,
    UnsupportedMachineFact,
    MissingRequiredEvidence,
    MissingCrossPriorSnapshot,
    MissingSourceSeries,
    MissingSourceSeed,
    SourceHistoryGap,
    TruncatedSourceHistory,
    IncompleteWarmup,
    MissingMultiTimeframeClosure,
    MissingMachineFact,
    ExplicitApprovalRequired,
    PolicyRevisionMismatch,
    MatchedRuleMismatch,
    TargetWeightMismatch,
    StateTransitionMismatch,
    IndicatorValueMismatch,
    MachineFactMismatch,
}

/// Saved context required to reconstruct a single immutable Rules-program decision.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuleSnapshotContext {
    pub observation_id: ObservationId,
    pub decision_time_utc: UtcTimestamp,
    pub completed_bars_seen: u64,
    pub indicators: Vec<NamedPolicyValue>,
    pub state_before: Vec<NamedPolicyValue>,
    pub actual_state: PositionState,
    pub actual_weight: Weight,
    /// Absent when the saved event did not retain this context. It is required
    /// only if the evaluated expression path uses `HELD_BARS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_bars: Option<u64>,
    /// Required only for policies that reference EvidenceMultiplier/Coverage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<PolicyEvidenceTrace>,
}

/// A prior cross sample is evidence input, never the result being audited.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuleCrossPriorValue {
    /// Stable Rules AST path (for example `rule:entry:when`).
    pub path: String,
    pub left: f64,
    pub right: f64,
}

/// Identity and path-complete operands from the immediately preceding saved rule snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuleCrossPriorSnapshot {
    pub policy_ref: PolicyRevisionRef,
    pub market: MarketId,
    pub decision_interval: CandleInterval,
    pub observation_id: ObservationId,
    pub decision_time_utc: UtcTimestamp,
    pub completed_bars_seen: u64,
    pub values: Vec<RuleCrossPriorValue>,
}

/// Persisted rule outcome against which the independent reconstruction is compared.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedRuleDecision {
    pub policy_ref: PolicyRevisionRef,
    pub observation_id: ObservationId,
    pub decision_time_utc: UtcTimestamp,
    pub matched_rule_id: Option<String>,
    pub target_weight: Weight,
    pub state_after: Vec<NamedPolicyValue>,
}

/// Inputs used to reconstruct one saved `SNAPSHOT_RULE_AUDIT` observation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRuleAuditInput {
    pub policy: FrozenPolicyRevision,
    pub market: MarketId,
    pub decision_interval: CandleInterval,
    pub cutoff_utc: UtcTimestamp,
    pub decision_observation: CandleObservation,
    pub current: RuleSnapshotContext,
    pub saved: SavedRuleDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_cross_snapshot: Option<RuleCrossPriorSnapshot>,
}

/// Explicit classification of how the retained source prefix relates to its seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceHistoryScope {
    CompleteFromSeed,
    TruncatedPrefix,
}

/// Origin class for a saved, non-dataset source observation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceCaptureOrigin {
    SavedRunBundle,
    SavedAutomationReview,
    PersistentPaperCheckpoint,
    SavedObservationWindow,
}

/// Immutable source reference available to a pure audit adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum IndicatorSourceReference {
    Dataset {
        dataset_id: DatasetId,
        dataset_digest: ContentHash,
    },
    CapturedWindow {
        first_observation_id: ObservationId,
        evidence_digest: ContentHash,
        observation_count: u32,
        capture_origin: SourceCaptureOrigin,
    },
}

/// Explicit source seed and warmup counts resolved by the storage adapter.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSeedEvidence {
    pub source: IndicatorSourceReference,
    /// Present only when retained bars begin at the evaluator's original seed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_seed_observation_id: Option<ObservationId>,
    pub history_scope: SourceHistoryScope,
    /// Number of retained bars through the audit cutoff, excluding future suffixes.
    pub bars_through_cutoff: u32,
    /// Maximum warmup required by indicators reading this source interval.
    pub required_warmup_bars: u32,
}

/// Explicit proof reference that the source query is closed through the decision cutoff.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSourceClosureEvidence {
    pub source: IndicatorSourceReference,
    pub closed_through_utc: UtcTimestamp,
    pub latest_observation_id: ObservationId,
    pub latest_close_time_utc: UtcTimestamp,
    pub evidence_digest: ContentHash,
}

/// Ordered completed observations from one source interval.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSourceSeries {
    pub interval: CandleInterval,
    pub observations: Vec<CandleObservation>,
    pub seed: IndicatorSeedEvidence,
    /// Required for a non-decision (multi-timeframe) source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure: Option<IndicatorSourceClosureEvidence>,
}

/// Inputs for `SOURCE_INDICATOR_AUDIT` over retained, cutoff-scoped source bars.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceIndicatorAuditInput {
    pub policy: FrozenPolicyRevision,
    pub saved_policy_ref: PolicyRevisionRef,
    pub market: MarketId,
    pub decision_interval: CandleInterval,
    pub cutoff_utc: UtcTimestamp,
    pub decision_observation: CandleObservation,
    pub source_series: Vec<IndicatorSourceSeries>,
    pub saved_values: Vec<NamedPolicyValue>,
}

/// Explicit exact machine facts. No value is inferred from policy prose or labels.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum MachineContractFact {
    PolicyRevision {
        reference: PolicyRevisionRef,
    },
    DecisionInterval {
        interval: CandleInterval,
    },
    ExecutionInterval {
        interval: CandleInterval,
    },
    CollectionCadence {
        interval: CandleInterval,
        cadence_seconds: u32,
    },
    AutomationRisk {
        policy: AutomationRiskPolicy,
    },
}

/// Origin recorded separately from the comparison result; caller-declared
/// consent is never elevated into a durable approval fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MachineContractApprovalOrigin {
    CallerDeclared,
}

/// Bounded comparison between explicitly selected expected and observed facts.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MachineContractComparisonInput {
    pub approval_evidence_origin: MachineContractApprovalOrigin,
    pub expected: Vec<MachineContractFact>,
    pub observed: Vec<MachineContractFact>,
}

/// One numeric reconstruction residual with the tolerance that governed it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewAuditResidual {
    pub path: String,
    pub expected: f64,
    pub recomputed: f64,
    pub absolute_difference: f64,
    pub tolerance: f64,
}

/// Exact decimal comparison used for target weights and other decimal facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewAuditExactResidual {
    pub path: String,
    pub expected: String,
    pub recomputed: String,
    pub absolute_difference: String,
}

/// Stable diagnostics and immutable evidence/result digests for one pure audit.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewAuditReport {
    pub kind: ReviewAuditKind,
    pub basis: ReviewAuditBasis,
    pub status: ReviewAuditStatus,
    pub evidence_digest: ContentHash,
    pub evidence_digests: Vec<ContentHash>,
    /// Provenance of the comparison request, separate from its fact-match status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_evidence_origin: Option<MachineContractApprovalOrigin>,
    pub diagnostics: Vec<ReviewAuditDiagnostic>,
    pub residuals: Vec<ReviewAuditResidual>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exact_residuals: Vec<ReviewAuditExactResidual>,
    pub result_digest: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewAuditDiagnostic {
    pub code: ReviewAuditDiagnosticCode,
    pub path: String,
}

impl ReviewAuditReport {
    /// Verify that the returned report has the digest of its immutable projection.
    ///
    /// # Errors
    /// Returns a typed data-corruption error for a changed report projection.
    pub fn validate_digest(&self) -> Result<(), LabError> {
        let expected = super::ContentHash::of_value(&ReviewAuditReportDigestProjection {
            kind: self.kind,
            basis: self.basis,
            status: self.status,
            evidence_digest: &self.evidence_digest,
            evidence_digests: &self.evidence_digests,
            approval_evidence_origin: self.approval_evidence_origin.as_ref(),
            diagnostics: &self.diagnostics,
            residuals: &self.residuals,
            exact_residuals: (!self.exact_residuals.is_empty())
                .then_some(self.exact_residuals.as_slice()),
        })?;
        if expected != self.result_digest {
            return Err(LabError::DataCorrupt(
                "review audit result digest mismatch".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
pub(crate) struct ReviewAuditReportDigestProjection<'a> {
    pub kind: ReviewAuditKind,
    pub basis: ReviewAuditBasis,
    pub status: ReviewAuditStatus,
    pub evidence_digest: &'a ContentHash,
    pub evidence_digests: &'a [ContentHash],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_evidence_origin: Option<&'a MachineContractApprovalOrigin>,
    pub diagnostics: &'a [ReviewAuditDiagnostic],
    pub residuals: &'a [ReviewAuditResidual],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exact_residuals: Option<&'a [ReviewAuditExactResidual]>,
}
