//! Explicit human-supplied public evidence; no interpretation or URL fetching.

use super::{
    ContentHash, EvidenceId, EvidenceRevisionId, EvidenceSnapshotId, LabError, MarketId, PitPolicy,
    UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidencePurpose {
    PosthocContext,
    StrategyInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum EvidenceProvenance {
    ForwardCaptured {
        captured_at: UtcTimestamp,
        captured_body_hash: ContentHash,
    },
    ArchivedPublication {
        archived_at: UtcTimestamp,
        archive_ref: String,
        archived_body_hash: ContentHash,
    },
    /// Does not establish historical PIT; only explicit proxy mode can use this.
    DeclaredLatest,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceVersion {
    pub evidence_id: EvidenceId,
    pub revision_id: EvidenceRevisionId,
    pub event_id: String,
    pub purpose: EvidencePurpose,
    pub category: String,
    pub regime_label: Option<String>,
    pub markets: Vec<MarketId>,
    pub source_refs: Vec<String>,
    pub body: String,
    pub body_hash: ContentHash,
    pub event_time: Option<UtcTimestamp>,
    pub published_at: Option<UtcTimestamp>,
    pub first_seen_at: Option<UtcTimestamp>,
    pub content_updated_at: Option<UtcTimestamp>,
    pub declared_available_at: UtcTimestamp,
    pub registered_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub supersedes_revision_id: Option<EvidenceRevisionId>,
    pub provenance: EvidenceProvenance,
    pub mapping_version: String,
    pub weight_multiplier: Weight,
}

impl EvidenceVersion {
    /// Validate bounded content and preserved-body provenance without certifying history.
    /// # Errors
    /// Rejects missing context, invalid times or a body/proof hash mismatch.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.event_id.is_empty()
            || self.event_id.len() > 96
            || self.body.len() > 65_536
            || self.category.len() > 128
            || self.mapping_version.is_empty()
            || self.mapping_version.len() > 96
            || self
                .regime_label
                .as_ref()
                .is_some_and(|label| label.len() > 128)
            || self.markets.is_empty()
            || self.markets.len() > 3
            || self.source_refs.is_empty()
            || self.source_refs.len() > 16
            || self.source_refs.iter().any(|s| s.len() > 2048)
        {
            return Err(LabError::InvalidConfig(
                "evidence fields exceed bounds or lack scope/source/mapping".into(),
            ));
        }
        if self.declared_available_at >= self.valid_until {
            return Err(LabError::InvalidConfig(
                "evidence availability must precede expiry".into(),
            ));
        }
        if ContentHash::of_bytes(self.body.as_bytes()) != self.body_hash {
            return Err(LabError::InputHashMismatch(
                "evidence body hash mismatch".into(),
            ));
        }
        let proof_hash = match &self.provenance {
            EvidenceProvenance::ForwardCaptured {
                captured_body_hash, ..
            } => Some(captured_body_hash),
            EvidenceProvenance::ArchivedPublication {
                archived_body_hash,
                archive_ref,
                ..
            } => {
                if archive_ref.is_empty() || archive_ref.len() > 2048 {
                    return Err(LabError::InvalidConfig(
                        "archive provenance reference is required".into(),
                    ));
                }
                Some(archived_body_hash)
            }
            EvidenceProvenance::DeclaredLatest => None,
        };
        if proof_hash.is_some_and(|hash| hash != &self.body_hash) {
            return Err(LabError::InputHashMismatch(
                "evidence preserved proof/body mismatch".into(),
            ));
        }
        if self.supersedes_revision_id.as_ref() == Some(&self.revision_id) {
            return Err(LabError::InvalidConfig(
                "evidence revision cannot supersede itself".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSnapshot {
    pub id: EvidenceSnapshotId,
    pub digest: ContentHash,
    pub versions: Vec<EvidenceVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceImport {
    pub public_non_sensitive_ack: bool,
    pub versions: Vec<EvidenceVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EligibleEvidence {
    pub revision_id: EvidenceRevisionId,
    pub available_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub multiplier: Weight,
    pub eligibility_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceDecisionEffect {
    pub policy: PitPolicy,
    pub snapshot_id: Option<EvidenceSnapshotId>,
    pub used_at: UtcTimestamp,
    pub eligible: Vec<EligibleEvidence>,
    pub base_target: Weight,
    pub final_target: Weight,
    pub coverage_available: bool,
    pub effect_changed_action: bool,
    pub unavailable_policy: String,
}
