//! Deterministic views of recorded decisions; never another strategy evaluator.

use super::{
    PolicyConditionTrace, PolicyRevisionRef, SavedFactCaptureStatus, UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordedDecisionAction {
    Buy,
    Sell,
    Hold,
    NotEvaluated,
    NotCaptured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordedDecisionStage {
    DataReadiness,
    Strategy,
    Constraints,
    Admission,
    Execution,
    Lifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordedExecutionState {
    NotEvaluated,
    NoOrder,
    Rejected,
    Pending,
    Filled,
    PartiallyFilled,
    Cancelled,
    Expired,
    NotCaptured,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedDecisionReason {
    pub stage: RecordedDecisionStage,
    /// The original engine/storage reason, not a model-generated diagnosis.
    pub code: String,
    /// A fixed label for the code; values remain in the accompanying facts.
    pub label_ko: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedDecisionOrder {
    pub order_id: String,
    pub state: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedDecisionFill {
    pub fill_id: String,
    pub order_id: String,
}

/// A projection of saved engine and linked lifecycle facts. No policy is replayed.
/// Parent responses own session/intent/signal identities and the selected cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedDecisionExplanation {
    pub schema_version: String,
    pub policy_ref: Option<PolicyRevisionRef>,
    pub decision_time: Option<UtcTimestamp>,
    pub strategy_action: RecordedDecisionAction,
    pub constrained_action: RecordedDecisionAction,
    pub execution_state: RecordedExecutionState,
    pub actual_weight_at_decision: Option<Weight>,
    pub raw_target_weight: Option<Weight>,
    pub constrained_target_weight: Option<Weight>,
    pub matched_rule_id: Option<String>,
    pub condition_trace_capture: SavedFactCaptureStatus,
    /// Rows retain FALSE, UNKNOWN and `NOT_EVALUATED`. Missing operands stay absent.
    pub condition_trace: Vec<PolicyConditionTrace>,
    pub reasons: Vec<RecordedDecisionReason>,
    pub orders: Vec<RecordedDecisionOrder>,
    pub fills: Vec<RecordedDecisionFill>,
}
