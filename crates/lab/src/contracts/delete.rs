//! Common two-step hard-delete contracts shared by every caller of the lab.
//!
//! Deletion is not owner-scoped: any caller of the same instance may preview and
//! execute the same contract. Inputs are registered resource IDs only; no SQL,
//! paths, or shell fragments are accepted. Execution removes business rows and
//! the deleted scope's exclusive files for real; it never hides rows behind
//! `deleted_at`/`archived` flags.

use super::{
    ArtifactId, BackupId, ContentHash, DatasetId, EvidenceSnapshotId, JobId, PolicyId, RunId,
    ScheduleId, SuiteId, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Confirmation tokens from an expired preview are refused.
pub const DELETE_PREVIEW_TTL_SECONDS: u64 = 600;
/// Upper bound of example IDs rendered per preview group.
pub const DELETE_PREVIEW_EXAMPLES: usize = 20;

/// Registered resource kinds eligible for hard deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeleteResource {
    Policy { policy_id: PolicyId },
    Dataset { dataset_id: DatasetId },
    Job { job_id: JobId },
    Run { run_id: RunId },
    EvidenceSnapshot { snapshot_id: EvidenceSnapshotId },
    Artifact { artifact_id: ArtifactId },
    Suite { suite_id: SuiteId },
    Schedule { schedule_id: ScheduleId },
    Backup { backup_id: BackupId },
}

impl DeleteResource {
    /// Opaque `kind:id` label used in blocker/cascade listings.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Policy { policy_id } => format!("policy:{}", policy_id.as_str()),
            Self::Dataset { dataset_id } => format!("dataset:{}", dataset_id.as_str()),
            Self::Job { job_id } => format!("job:{}", job_id.as_str()),
            Self::Run { run_id } => format!("run:{}", run_id.as_str()),
            Self::EvidenceSnapshot { snapshot_id } => {
                format!("evidence_snapshot:{}", snapshot_id.as_str())
            }
            Self::Artifact { artifact_id } => format!("artifact:{}", artifact_id.as_str()),
            Self::Suite { suite_id } => format!("suite:{}", suite_id.as_str()),
            Self::Schedule { schedule_id } => format!("schedule:{}", schedule_id.as_str()),
            Self::Backup { backup_id } => format!("backup:{}", backup_id.as_str()),
        }
    }
}

/// Kinds of rows counted in preview/execution scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeleteEntryKind {
    Policy,
    PolicyRevision,
    Plan,
    Job,
    Attempt,
    Run,
    Dataset,
    EvidenceSnapshot,
    Artifact,
    RawObject,
    Observation,
    Collection,
    CollectionPage,
    Validation,
    Suite,
    Schedule,
    Backup,
}

/// One live reference that makes a non-cascade delete impossible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteBlocker {
    /// Active/protected references refuse even cascade; live references allow cascade.
    pub class: DeleteBlockerClass,
    /// Opaque label of the referencing resource.
    pub reference: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeleteBlockerClass {
    /// A queued/running job in scope; execution never kills live work.
    ActiveJob,
    /// A surviving resource still references the scope.
    LiveReference,
    /// Durable automation lineage must be released by deleting its parent first.
    ProtectedReference,
}

/// Counted group of resources removed together under explicit cascade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteCascadeGroup {
    pub kind: DeleteEntryKind,
    pub count: u64,
    pub examples: Vec<String>,
}

/// Counted group of shared resources that survive the deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteRetainedGroup {
    pub kind: DeleteEntryKind,
    pub count: u64,
}

/// First step of the two-step contract: compute and disclose the exact scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeletePreview {
    pub resource: DeleteResource,
    /// True when the scope only deletes with `cascade=true`.
    pub cascade_required: bool,
    pub blockers: Vec<DeleteBlocker>,
    /// Resources removed together when cascade is requested (empty when blocked).
    pub cascade: Vec<DeleteCascadeGroup>,
    /// Shared resources kept after deletion.
    pub retained_shared: Vec<DeleteRetainedGroup>,
    /// Bytes of exclusive files removed by execution; `None` when unmeasured.
    pub reclaimable_file_bytes: Option<u64>,
    pub exclusive_file_count: u64,
    /// Business rows inside the computed scope.
    pub db_row_estimate: u64,
    /// Digest over the full computed scope (not the truncated examples).
    pub scope_digest: ContentHash,
    pub expires_at: UtcTimestamp,
    /// Binding token over `(scope_digest, expires_at, cascade_required)`; the
    /// echoed preview is valid only while this token still matches.
    pub confirmation_token: ContentHash,
    /// Always true: hard deletion cannot be undone through this API.
    pub irreversible: bool,
}

/// Second step: execute a previously previewed scope before it expires.
///
/// The caller echoes the exact `DeletePreview` it is confirming. Callers that
/// cannot compute digests (LLM clients) can return the preview verbatim; the
/// server recomputes the binding from the echo, so a stale, expired, or altered
/// preview is refused exactly like an invalid token.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HardDeleteRequest {
    /// Verbatim copy of the preview being confirmed.
    pub preview: DeletePreview,
    /// Remove live-reference dependents disclosed by the preview.
    pub cascade: bool,
}

/// Result of one executed hard deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteOutcome {
    pub resource: DeleteResource,
    pub deleted_db_rows: u64,
    pub deleted_files: u64,
    pub deleted_file_bytes: u64,
    pub retained_shared_raw_objects: u64,
    /// True when SQLite file compaction ran after the committed deletion.
    pub vacuumed: bool,
}
