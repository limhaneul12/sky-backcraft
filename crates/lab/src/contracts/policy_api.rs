//! Bounded policy authoring and immutable execution-history JSON interfaces.
use super::{
    AttemptId, ContentHash, JobId, JobStatus, PlanId, PolicyDefinition, PolicyId, PolicyOrigin,
    PolicyRevisionId, PolicyRevisionRef, RequestId, RunId, StrategyKind, StrategySpec,
    UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyWrite {
    Create {
        request_id: RequestId,
        definition: PolicyDefinition,
    },
    Revise {
        request_id: RequestId,
        policy_id: PolicyId,
        expected_parent_revision_id: PolicyRevisionId,
        definition: PolicyDefinition,
    },
    /// Expand a deterministic parameter sweep into one immutable policy per
    /// candidate; identical parameter sets reuse the existing policy.
    Sweep {
        request_id: RequestId,
        family: StrategyKind,
        template: StrategySpec,
        mode: super::ParameterSweepMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        research: Option<super::SweepResearchContext>,
    },
    /// Calculate candidate and optional research-suite resource counts without
    /// creating policy revisions.
    Preflight {
        request_id: RequestId,
        family: StrategyKind,
        template: StrategySpec,
        mode: super::ParameterSweepMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        research: Option<super::SweepResearchContext>,
    },
}

/// Sweep plan plus the exact policy revisions it froze or reused.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicySweepResult {
    pub plan: super::SweepPlan,
    pub revisions: Vec<PolicyRevisionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyQuery {
    List {
        after_policy_id: Option<PolicyId>,
        limit: u32,
    },
    Get {
        reference: PolicyRevisionRef,
    },
    History {
        policy_id: PolicyId,
        before_revision_number: Option<u32>,
        limit: u32,
    },
    Runs {
        reference: PolicyRevisionRef,
        cursor: Option<RunHistoryCursor>,
        limit: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyHeader {
    pub policy_id: PolicyId,
    pub family: StrategyKind,
    pub origin: PolicyOrigin,
    pub created_at: UtcTimestamp,
    pub name: String,
    pub head: PolicyRevisionRef,
    pub revision_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRevisionSummary {
    pub reference: PolicyRevisionRef,
    pub revision_number: u32,
    pub parent_revision_id: Option<PolicyRevisionId>,
    pub name: String,
    pub created_at: UtcTimestamp,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage<T, C> {
    pub items: Vec<T>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_cursor: Option<C>,
    pub truncated_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunHistoryCursor {
    pub created_at: UtcTimestamp,
    pub run_id: RunId,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobHistoryCursor {
    pub created_at: UtcTimestamp,
    pub job_id: JobId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryQuery {
    Runs {
        cursor: Option<RunHistoryCursor>,
        limit: u32,
    },
    Jobs {
        cursor: Option<JobHistoryCursor>,
        limit: u32,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HistoryJobKind {
    Collect,
    Backtest,
    Export,
    Verify,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunHistoryEntry {
    pub run_id: RunId,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub plan_id: PlanId,
    pub created_at: UtcTimestamp,
    pub status: JobStatus,
    pub input_digest: ContentHash,
    pub semantic_digest: Option<ContentHash>,
    pub completed_models: u64,
    pub blocked_models: u64,
    pub failed_models: u64,
    pub model_count: u64,
    pub policy_revisions: Vec<PolicyRevisionRef>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobHistoryEntry {
    pub job_id: JobId,
    pub request_id: RequestId,
    pub created_at: UtcTimestamp,
    pub kind: HistoryJobKind,
    pub status: JobStatus,
    pub attempt_count: u32,
    pub current_attempt_id: AttemptId,
}
