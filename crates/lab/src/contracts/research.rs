//! Durable research suites reuse frozen plans and ordinary job attempts.

use super::{
    AttemptId, BasisPoints, ContentHash, CostPolicy, ExperimentSpec, FailureRecord,
    FrozenPolicyRevision, JobId, MarketId, MetricValue, ModelId, ModelStatus, PlanId,
    PolicyRevisionRef, QuoteAmount, RequestId, RunId, SignedAmount, SuiteCaseId, SuiteId, UtcRange,
    UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_SUITE_RUNS: usize = 64;
pub const MAX_SUITE_CELLS: usize = 256;
pub const MAX_SUITE_FOLDS: usize = 12;
pub const MAX_SUITE_SCENARIOS: usize = 4;
pub const MAX_ACTIVE_SUITES: usize = 8;
pub const MAX_RESEARCH_PAGE: u32 = 100;

/// Validate the common bounded page contract at service and protocol boundaries.
/// # Errors
/// Rejects zero or oversized pages without accessing storage.
pub fn validate_research_page(limit: u32) -> Result<(), super::LabError> {
    if (1..=MAX_RESEARCH_PAGE).contains(&limit) {
        Ok(())
    } else {
        Err(super::LabError::InvalidConfig(
            "research page limit must be in 1..=100".into(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchDesign {
    Batch,
    WalkForward {
        selection_bars: u32,
        evaluation_bars: u32,
        step_bars: u32,
        embargo_bars: u32,
    },
}

/// Exact frozen parameter tuples cross with these fee/slippage axes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostSweep {
    pub fee_bps: Vec<BasisPoints>,
    pub slippage_bps: Vec<BasisPoints>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResearchSuiteRequest {
    pub request_id: RequestId,
    pub template: ExperimentSpec,
    pub design: ResearchDesign,
    pub cost_sweep: CostSweep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuitePhase {
    Batch,
    Selection,
    Evaluation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuiteStatus {
    Running,
    Paused,
    Completed,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuiteCaseStatus {
    Planned,
    Queued,
    Running,
    Completed,
    Blocked,
    Failed,
    Cancelled,
    Interrupted,
    RetryPending,
}

/// A predeclared candidate omitted from selection with its bounded explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UnavailableCandidate {
    pub candidate: PolicyRevisionRef,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WalkForwardFold {
    pub index: u32,
    pub selection_range: UtcRange,
    pub evaluation_range: UtcRange,
    pub winner: Option<PolicyRevisionRef>,
    pub selection_digest: Option<ContentHash>,
    pub unavailable_candidates: Vec<UnavailableCandidate>,
}

/// Immutable request, policy bodies and scenario geometry; never a worker owner.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrozenResearchSuite {
    pub id: SuiteId,
    pub request: ResearchSuiteRequest,
    pub input_digest: ContentHash,
    pub policy_revisions: Vec<FrozenPolicyRevision>,
    pub scenarios: Vec<CostPolicy>,
    pub folds: Vec<WalkForwardFold>,
    pub planned_runs: u32,
    pub planned_comparison_cells: u32,
    pub estimated_events: u64,
    pub unused_tail: Option<UtcRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteCase {
    pub id: SuiteCaseId,
    pub suite_id: SuiteId,
    pub index: u32,
    pub fold_index: Option<u32>,
    pub phase: SuitePhase,
    pub scenario_index: u32,
    pub range: UtcRange,
    pub status: SuiteCaseStatus,
    pub plan_id: Option<PlanId>,
    pub job_id: Option<JobId>,
    pub attempt_id: Option<AttemptId>,
    pub run_id: Option<RunId>,
    pub causal_input_digest: Option<ContentHash>,
    pub failure: Option<FailureRecord>,
}

/// Computed by the existing execution worker and atomically published with its run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunModelComparison {
    pub run_id: RunId,
    pub model_id: ModelId,
    pub market: MarketId,
    pub policy_ref: Option<PolicyRevisionRef>,
    pub status: ModelStatus,
    pub initial_equity: Option<QuoteAmount>,
    pub final_equity: Option<QuoteAmount>,
    pub net_pnl: Option<SignedAmount>,
    pub net_return: MetricValue,
    pub max_drawdown: MetricValue,
    pub turnover: MetricValue,
    pub cumulative_fees: Option<QuoteAmount>,
    pub closed_episodes: Option<u64>,
    pub semantic_digest: ContentHash,
    pub causal_input_digest: ContentHash,
}

/// One combined comparison row carries the scenario/fold alongside its metrics.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteComparisonRow {
    pub case_id: SuiteCaseId,
    pub fold_index: Option<u32>,
    pub phase: SuitePhase,
    pub scenario_index: u32,
    pub range: UtcRange,
    pub costs: CostPolicy,
    pub comparison: RunModelComparison,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteRecord {
    pub frozen: FrozenResearchSuite,
    pub status: SuiteStatus,
    pub created_at: UtcTimestamp,
    pub next_action_at: UtcTimestamp,
    pub failure: Option<FailureRecord>,
    pub folds: Vec<WalkForwardFold>,
    pub cases: Vec<SuiteCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteSummary {
    pub id: SuiteId,
    pub request_id: RequestId,
    pub input_digest: ContentHash,
    pub design: ResearchDesign,
    pub candidate_refs: Vec<PolicyRevisionRef>,
    pub cost_scenarios: Vec<CostPolicy>,
    pub status: SuiteStatus,
    pub failure: Option<FailureRecord>,
    pub created_at: UtcTimestamp,
    pub planned_runs: u32,
    pub completed_runs: u32,
    pub blocked_or_failed_runs: u32,
    pub selected_folds: Vec<WalkForwardFold>,
    pub unused_tail: Option<UtcRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResearchPage<T> {
    pub records: Vec<T>,
    pub total_count: u64,
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchSuiteAction {
    /// Read-only admission preview; never creates state.
    Plan {
        request: Box<ResearchSuiteRequest>,
    },
    Create {
        request: Box<ResearchSuiteRequest>,
    },
    Get {
        suite_id: SuiteId,
    },
    List {
        offset: u64,
        limit: u32,
    },
    Cases {
        suite_id: SuiteId,
        offset: u64,
        limit: u32,
    },
    Comparisons {
        suite_id: SuiteId,
        offset: u64,
        limit: u32,
    },
    Pause {
        suite_id: SuiteId,
    },
    Resume {
        suite_id: SuiteId,
    },
}

/// Bounded suite resource axis the planner reports against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuitePlanLimitKind {
    SuiteRuns,
    SuiteCells,
    SuiteFolds,
    SuiteScenarios,
    RunEvents,
}

/// One numeric admission violation with the requested and allowed magnitude.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuitePlanViolation {
    pub item: SuitePlanLimitKind,
    pub requested: u64,
    pub allowed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum SuitePlanAdmission {
    Admitted,
    Rejected {
        violations: Vec<SuitePlanViolation>,
    },
    /// The template/design itself is invalid; `create` would fail the same way.
    Invalid {
        message: String,
    },
}

/// Read-only resource estimate for one research suite request. The planner
/// shares the create-time formulas, so an `Admitted` plan only fails create
/// on mutable state such as unknown policy revisions or duplicate suites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuitePlanReport {
    pub candidates: u32,
    pub markets: u32,
    pub folds: u32,
    pub cost_scenarios: u32,
    pub planned_runs: u64,
    pub comparison_cells: u64,
    /// Estimated events of the largest single run (selection case).
    pub estimated_model_events: u64,
    /// Estimated aggregate events across all planned runs.
    pub estimated_run_events: u64,
    /// Conservative byte estimate; ledger facts compress below this.
    pub estimated_storage_bytes: u64,
    pub admission: SuitePlanAdmission,
    pub suggestions: Vec<String>,
    pub notes: Vec<String>,
}
