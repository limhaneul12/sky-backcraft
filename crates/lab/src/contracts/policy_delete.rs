//! Desktop-only, policy-scoped delete requests and durable readback.

use super::{ContentHash, PolicyId, RequestId, UtcTimestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum number of policy previews admitted by one explicit confirmation.
pub const MAX_POLICY_DELETE_ITEMS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeletePreviewEcho {
    #[serde(default)]
    pub scope_digest: Option<ContentHash>,
    #[serde(default)]
    pub expires_at: Option<UtcTimestamp>,
    #[serde(default)]
    pub confirmation_token: Option<ContentHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteItemRequest {
    pub policy_id: PolicyId,
    #[serde(default)]
    pub preview: Option<PolicyDeletePreviewEcho>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteWriteRequest {
    pub request_id: RequestId,
    pub items: Vec<PolicyDeleteItemRequest>,
    pub confirmation_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteQueryRequest {
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteItemStatus {
    Pending,
    Deleted,
    Blocked,
    Stale,
    Failed,
    InterruptedNotApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteRequestState {
    Pending,
    Complete,
    Partial,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteReasonCode {
    BuiltinPolicy,
    UnknownOrigin,
    PolicyNotFound,
    ReferencedHistorical,
    ReferencedPaper,
    ReferencedAutomation,
    ActiveJob,
    OtherResourceScope,
    PreviewMissing,
    PreviewIncomplete,
    PreviewExpired,
    ScopeChanged,
    TokenMismatch,
    ProcessInterrupted,
    StorageFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteRemediationCode {
    ArchiveDeactivateUnavailable,
    BuiltinReadOnly,
    FreshPreviewRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteEffect {
    Applied,
    NotApplied,
    NoNewEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDeleteWriteErrorCode {
    InvalidSelection,
    SelectionLimit,
    DuplicatePolicyId,
    ConfirmationMismatch,
    RequestIdConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteWriteError {
    pub error: PolicyDeleteWriteErrorDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteWriteErrorDetail {
    pub request_id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submitted_request_digest: Option<ContentHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_request_digest: Option<ContentHash>,
    pub code: PolicyDeleteWriteErrorCode,
    pub item_index: Option<u32>,
    pub policy_id: Option<PolicyId>,
    pub effect: PolicyDeleteEffect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteItemReadback {
    pub policy_id: PolicyId,
    pub preview_scope_digest: Option<ContentHash>,
    pub preview_digest: ContentHash,
    pub status: PolicyDeleteItemStatus,
    pub reason_code: Option<PolicyDeleteReasonCode>,
    pub remediation_code: Option<PolicyDeleteRemediationCode>,
    pub deleted_revision_count: u64,
    pub retained: bool,
    pub effect: PolicyDeleteEffect,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteCounts {
    pub pending: u64,
    pub deleted: u64,
    pub blocked: u64,
    pub stale: u64,
    pub failed: u64,
    pub interrupted_not_applied: u64,
    /// Overlapping count of rows verified present; not part of status partition.
    pub retained: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeleteReadback {
    pub request_id: RequestId,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_digest: Option<ContentHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<PolicyDeleteRequestState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<PolicyDeleteItemReadback>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counts: Option<PolicyDeleteCounts>,
}
