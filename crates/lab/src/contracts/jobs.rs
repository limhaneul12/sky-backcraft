//! Durable request and immutable attempt identities; terminal state is explicit.

use super::{
    ArtifactId, AttemptId, CollectRequest, ContentHash, DatasetId, JobId, MarketId, PlanId,
    RequestId, RunId, RunRequest, UtcTimestamp, ValidationReport,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
    Interrupted,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum JobPayload {
    Collect {
        request: CollectRequest,
    },
    Backtest {
        request: RunRequest,
    },
    Export {
        run_id: RunId,
        market: Option<MarketId>,
    },
    Verify {
        artifact_id: ArtifactId,
        replay: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProgressCountUnit {
    DatasetRows,
    LedgerFacts,
    Artifacts,
    CheckedModels,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobProgress {
    pub stage: String,
    #[serde(default)]
    pub committed_records: Option<u64>,
    #[serde(default)]
    pub count_unit: Option<ProgressCountUnit>,
    #[serde(default)]
    pub last_committed_event_seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FailureRecord {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum JobOutput {
    Dataset {
        dataset_id: DatasetId,
    },
    Run {
        run_id: RunId,
        completed_models: u64,
        blocked_models: u64,
    },
    Artifacts {
        artifact_ids: Vec<ArtifactId>,
    },
    Validation {
        report: ValidationReport,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "state",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum AttemptState {
    Queued {
        queued_at: UtcTimestamp,
    },
    Running {
        started_at: UtcTimestamp,
        cancel_requested_at: Option<UtcTimestamp>,
    },
    Completed {
        ended_at: UtcTimestamp,
        output: JobOutput,
    },
    Partial {
        ended_at: UtcTimestamp,
        output: JobOutput,
        warnings: Vec<String>,
    },
    Failed {
        ended_at: UtcTimestamp,
        error: FailureRecord,
    },
    Cancelled {
        ended_at: UtcTimestamp,
        reason: String,
    },
    Interrupted {
        ended_at: UtcTimestamp,
        reason: String,
    },
    Blocked {
        ended_at: UtcTimestamp,
        reason: FailureRecord,
        output: Option<JobOutput>,
    },
}

impl AttemptState {
    #[must_use]
    pub fn status(&self) -> JobStatus {
        match self {
            Self::Queued { .. } => JobStatus::Queued,
            Self::Running { .. } => JobStatus::Running,
            Self::Completed { .. } => JobStatus::Completed,
            Self::Partial { .. } => JobStatus::Partial,
            Self::Failed { .. } => JobStatus::Failed,
            Self::Cancelled { .. } => JobStatus::Cancelled,
            Self::Interrupted { .. } => JobStatus::Interrupted,
            Self::Blocked { .. } => JobStatus::Blocked,
        }
    }
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Queued { .. } | Self::Running { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobAttempt {
    pub id: AttemptId,
    pub job_id: JobId,
    pub number: u32,
    pub input_digest: ContentHash,
    pub state: AttemptState,
    pub progress: JobProgress,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobRecord {
    pub id: JobId,
    pub request_id: RequestId,
    pub normalized_input_digest: ContentHash,
    pub payload: JobPayload,
    pub attempts: Vec<JobAttempt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlanRequest {
    pub request_id: RequestId,
    pub spec: super::ExperimentSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobSubmission {
    pub request_id: RequestId,
    pub payload: JobPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobControl {
    Get { job_id: JobId },
    Cancel { job_id: JobId },
    Retry { job_id: JobId },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunHeader {
    pub manifest: super::RunManifest,
    pub plan_id: PlanId,
    pub state: JobStatus,
    pub last_committed_event_seq: u64,
    pub semantic_digest: Option<ContentHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LedgerSection {
    Signals,
    Orders,
    OrderEvents,
    Fills,
    Episodes,
    Equity,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryCursor {
    pub model_id: super::ModelId,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResultQuery {
    pub run_id: RunId,
    pub model_id: Option<super::ModelId>,
    pub section: LedgerSection,
    pub range: Option<super::UtcRange>,
    pub cursor: Option<QueryCursor>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Page<T> {
    pub records: Vec<T>,
    pub returned_count: u64,
    pub total_count: u64,
    pub next_cursor: Option<QueryCursor>,
    pub truncated_reason: Option<String>,
}
