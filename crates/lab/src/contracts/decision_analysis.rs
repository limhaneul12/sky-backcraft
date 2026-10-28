//! Shared, saved-fact decision-analysis wire contracts.

use super::{
    AutomationIntentId, AutomationSessionId, ContentHash, LabError, MarketId, PaperSessionId,
    PolicyConditionOutcome, PolicyConditionRole, PolicyConditionScope, PolicyRevisionRef,
    RecordedDecisionAction, RecordedDecisionExplanation, RecordedDecisionReason,
    RecordedDecisionStage, RecordedExecutionState, SavedFactCaptureStatus, SignalId, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_DECISION_ANALYSIS_PAGE_SIZE: u32 = 100;
pub const MAX_DECISION_ANALYSIS_QUERY_BYTES: usize = 32 * 1024;
pub const MAX_DECISION_ANALYSIS_FACTS: usize = 50_000;
pub const MAX_DECISION_ANALYSIS_INPUT_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_DECISION_ANALYSIS_OUTPUT_BYTES: usize = 512 * 1024;
pub const MAX_DECISION_ANALYSIS_ELAPSED_MILLIS: u64 = 18_000;
pub const DECISION_ANALYSIS_SCHEMA_VERSION: &str = "1.0";

/// One explicitly selected saved source family and session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisSource {
    Paper { session_id: PaperSessionId },
    Automation { session_id: AutomationSessionId },
}

impl DecisionAnalysisSource {
    #[must_use]
    pub fn is_automation(&self) -> bool {
        matches!(self, Self::Automation { .. })
    }

    fn family_matches(&self, fence: &DecisionAnalysisFence) -> bool {
        matches!(
            (self, fence),
            (Self::Paper { .. }, DecisionAnalysisFence::Paper { .. })
                | (
                    Self::Automation { .. },
                    DecisionAnalysisFence::Automation { .. }
                )
        )
    }
}

/// Half-open UTC decision-time range `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisPeriod {
    pub start: UtcTimestamp,
    pub end: UtcTimestamp,
}

impl DecisionAnalysisPeriod {
    fn validate(&self) -> Result<(), LabError> {
        if self.start >= self.end {
            return Err(LabError::InvalidConfig(
                "decision analysis period requires start < end (UTC [start, end))".into(),
            ));
        }
        Ok(())
    }
}

/// Stable population definition. Optional filters are checked against this saved session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisPopulation {
    pub source: DecisionAnalysisSource,
    pub period: DecisionAnalysisPeriod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market: Option<MarketId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<PolicyRevisionRef>,
}

impl DecisionAnalysisPopulation {
    fn validate(&self) -> Result<(), LabError> {
        self.period.validate()
    }
}

/// Durable fact boundary returned by `overview` and required for every follow-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisFence {
    Paper { as_of_sequence: u64 },
    Automation { snapshot_digest: ContentHash },
}

/// A population bound to the exact saved facts used by an overview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisSelection {
    pub population: DecisionAnalysisPopulation,
    pub fence: DecisionAnalysisFence,
}

impl DecisionAnalysisSelection {
    fn validate(&self) -> Result<(), LabError> {
        self.population.validate()?;
        if !self.population.source.family_matches(&self.fence) {
            return Err(LabError::InvalidConfig(
                "decision analysis source family and snapshot fence differ".into(),
            ));
        }
        Ok(())
    }
}

/// True saved decision identity at the source's own grain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisDecisionId {
    Paper {
        event_sequence: u64,
        signal_id: SignalId,
    },
    Automation {
        intent_id: AutomationIntentId,
    },
}

impl DecisionAnalysisDecisionId {
    fn matches_source(&self, source: &DecisionAnalysisSource) -> bool {
        matches!(
            (self, source),
            (Self::Paper { .. }, DecisionAnalysisSource::Paper { .. })
                | (
                    Self::Automation { .. },
                    DecisionAnalysisSource::Automation { .. }
                )
        )
    }

    fn validate(&self) -> Result<(), LabError> {
        if matches!(
            self,
            Self::Paper {
                event_sequence: 0,
                ..
            }
        ) {
            return Err(LabError::InvalidConfig(
                "paper decision event_sequence must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Bounded offset pagination over the stable, identity-ordered decision list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisPageRequest {
    pub offset: u32,
    pub limit: u32,
}

impl DecisionAnalysisPageRequest {
    fn validate(self) -> Result<(), LabError> {
        if !(1..=MAX_DECISION_ANALYSIS_PAGE_SIZE).contains(&self.limit) {
            return Err(LabError::InvalidConfig(format!(
                "decision analysis page limit must be in 1..={MAX_DECISION_ANALYSIS_PAGE_SIZE}"
            )));
        }
        Ok(())
    }
}

/// Optional list-only filters; all selected fields use AND semantics and period totals always
/// describe the full population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisDecisionFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_action: Option<RecordedDecisionAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constrained_action: Option<RecordedDecisionAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_state: Option<RecordedExecutionState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<RecordedDecisionStage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
}

impl DecisionAnalysisDecisionFilter {
    fn validate(&self) -> Result<(), LabError> {
        if self
            .reason_code
            .as_ref()
            .is_some_and(|code| code.is_empty() || code.len() > 128)
        {
            return Err(LabError::InvalidConfig(
                "decision analysis reason_code must contain 1..=128 bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Previous decision ignores the list-only stage/reason filter. A selected target carries its
/// own independently fenced source and period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisComparisonTarget {
    Previous,
    Selected {
        selection: Box<DecisionAnalysisSelection>,
        decision_id: DecisionAnalysisDecisionId,
    },
}

/// One query contract shared by the MCP and native read clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisQuery {
    Overview {
        population: DecisionAnalysisPopulation,
    },
    Decisions {
        selection: DecisionAnalysisSelection,
        page: DecisionAnalysisPageRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<DecisionAnalysisDecisionFilter>,
    },
    Decision {
        selection: DecisionAnalysisSelection,
        decision_id: DecisionAnalysisDecisionId,
    },
    Compare {
        selection: DecisionAnalysisSelection,
        decision_id: DecisionAnalysisDecisionId,
        against: DecisionAnalysisComparisonTarget,
    },
}

impl DecisionAnalysisQuery {
    /// Validate identities, family-specific fences, period, filters and request size.
    /// # Errors
    /// Rejects malformed or oversized query contracts before storage admission.
    pub fn validate(&self) -> Result<(), LabError> {
        match self {
            Self::Overview { population } => population.validate()?,
            Self::Decisions {
                selection,
                page,
                filter,
            } => {
                selection.validate()?;
                page.validate()?;
                if let Some(filter) = filter {
                    filter.validate()?;
                }
            }
            Self::Decision {
                selection,
                decision_id,
            } => {
                selection.validate()?;
                validate_decision_id(decision_id, &selection.population.source)?;
            }
            Self::Compare {
                selection,
                decision_id,
                against,
            } => {
                selection.validate()?;
                validate_decision_id(decision_id, &selection.population.source)?;
                if let DecisionAnalysisComparisonTarget::Selected {
                    selection: other_selection,
                    decision_id: other_id,
                } = against
                {
                    other_selection.validate()?;
                    if selection.population.source.is_automation()
                        != other_selection.population.source.is_automation()
                    {
                        return Err(LabError::InvalidConfig(
                            "selected comparison requires the same decision source family".into(),
                        ));
                    }
                    validate_decision_id(other_id, &other_selection.population.source)?;
                }
            }
        }
        if serde_json::to_vec(self)?.len() > MAX_DECISION_ANALYSIS_QUERY_BYTES {
            return Err(LabError::ResourceLimit(
                "decision analysis request exceeds byte budget".into(),
            ));
        }
        Ok(())
    }

    /// Whether any selected side requires the existing automation read authorization.
    #[must_use]
    pub fn requires_trading_read(&self) -> bool {
        match self {
            Self::Overview { population } => population.source.is_automation(),
            Self::Decisions { selection, .. } | Self::Decision { selection, .. } => {
                selection.population.source.is_automation()
            }
            Self::Compare {
                selection, against, ..
            } => {
                selection.population.source.is_automation()
                    || matches!(
                        against,
                        DecisionAnalysisComparisonTarget::Selected { selection, .. }
                            if selection.population.source.is_automation()
                    )
            }
        }
    }
}

fn validate_decision_id(
    decision_id: &DecisionAnalysisDecisionId,
    source: &DecisionAnalysisSource,
) -> Result<(), LabError> {
    decision_id.validate()?;
    if !decision_id.matches_source(source) {
        return Err(LabError::InvalidConfig(
            "decision identity and selected source family differ".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionAnalysisMode {
    Simulated,
    Mock,
    Live,
}

/// Resolved source identity returned with every response and reused by follow-up queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisSourceSnapshot {
    pub selection: DecisionAnalysisSelection,
    pub mode: DecisionAnalysisMode,
    pub policy_ref: PolicyRevisionRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisActionCounts {
    pub buy: u64,
    pub sell: u64,
    pub hold: u64,
    pub not_evaluated: u64,
    pub not_captured: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisExecutionCounts {
    pub not_evaluated: u64,
    pub no_order: u64,
    pub rejected: u64,
    pub pending: u64,
    pub filled: u64,
    pub partially_filled: u64,
    pub cancelled: u64,
    pub expired: u64,
    pub not_captured: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisCaptureCounts {
    pub captured: u64,
    pub partial_capture: u64,
    pub not_captured: u64,
    pub not_decision_event: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisReasonCount {
    pub stage: RecordedDecisionStage,
    pub code: String,
    /// Nonexclusive distinct-decision count; reason groups may overlap.
    pub distinct_decisions: u64,
    pub occurrences: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisConditionCount {
    pub policy_ref: PolicyRevisionRef,
    pub scope: PolicyConditionScope,
    /// Original condition identity within the exact policy revision.
    pub ast_path: String,
    pub role: PolicyConditionRole,
    pub outcome: PolicyConditionOutcome,
    /// Occurrence count only; this projection is not a clickable decision selector.
    pub occurrences: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAnalysisObservationClock {
    PreparationDecisionTime,
    PaperSourceOrFinalizedTime,
    AutomationEventTime,
}

/// Explicit market association for a saved preparation/control observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisObservationScope {
    Market { market: MarketId },
    SessionWide,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisObservationCount {
    /// The persisted observation kind; not inferred from missing policy/time.
    pub kind: String,
    pub basis_time: DecisionAnalysisObservationClock,
    /// Explicit association; `unknown` is never attributed to a selected market.
    pub scope: DecisionAnalysisObservationScope,
    pub condition_trace_capture: SavedFactCaptureStatus,
    pub observations: u64,
    /// Same-kind observations with no original clock; not assigned to the selected period.
    pub unplaced: u64,
}

/// Full selected population totals, independent of the current decision-list page/filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisTotals {
    /// Only decisions with saved times inside `[period.start, period.end)`.
    pub decisions: u64,
    /// Descriptive count; equals `decisions` because missing-time rows are unplaced.
    pub decisions_with_time: u64,
    /// Missing-time decisions stay outside the period and remain coverage-unknown.
    pub unplaced_decisions: u64,
    /// Strategy, constrained and execution buckets use only period-placed decisions.
    pub strategy: DecisionAnalysisActionCounts,
    pub constrained: DecisionAnalysisActionCounts,
    pub execution: DecisionAnalysisExecutionCounts,
    pub orders: u64,
    pub decisions_with_orders: u64,
    pub fills: u64,
    pub decisions_with_fills: u64,
    pub condition_trace_capture: DecisionAnalysisCaptureCounts,
    /// Nonexclusive reasons; distinct-decision counts may overlap across rows.
    pub reasons: Vec<DecisionAnalysisReasonCount>,
    pub conditions: Vec<DecisionAnalysisConditionCount>,
    pub non_decision_observations: Vec<DecisionAnalysisObservationCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisDecisionRow {
    pub decision_id: DecisionAnalysisDecisionId,
    pub market: MarketId,
    pub decision_time: Option<UtcTimestamp>,
    pub period_placement: DecisionAnalysisPeriodPlacement,
    pub policy_ref: PolicyRevisionRef,
    pub saved_fact_digest: ContentHash,
    pub strategy_action: RecordedDecisionAction,
    pub constrained_action: RecordedDecisionAction,
    pub execution_state: RecordedExecutionState,
    pub reasons: Vec<RecordedDecisionReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAnalysisPeriodPlacement {
    InPeriod,
    Unplaced,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisDecisionDetail {
    pub row: DecisionAnalysisDecisionRow,
    pub explanation: RecordedDecisionExplanation,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisPage {
    pub offset: u32,
    pub limit: u32,
    /// Number of rows in `items`, never the full filtered population.
    pub returned_count: u32,
    /// Filtered population size before pagination; equals `in_period_count + unplaced_count`.
    pub total_count: u64,
    /// Filtered rows whose saved decision time falls inside the selected period.
    pub in_period_count: u64,
    /// Filtered rows with no saved decision time; not assigned to the period.
    pub unplaced_count: u64,
    pub next_offset: Option<u32>,
    pub items: Vec<DecisionAnalysisDecisionRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAnalysisChangedField {
    SourceFamily,
    SessionId,
    Mode,
    PolicyRef,
    Market,
    DecisionTime,
    StrategyAction,
    ConstrainedAction,
    ExecutionState,
    ActualWeightAtDecision,
    RawTargetWeight,
    ConstrainedTargetWeight,
    MatchedRuleId,
    ConditionTraceCapture,
    ConditionTrace,
    Reasons,
    Orders,
    Fills,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAnalysisComparisonKind {
    Previous,
    Selected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAnalysisComparisonUnavailableReason {
    NoPreviousDecision,
    MissingDecisionTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisComparison {
    pub kind: DecisionAnalysisComparisonKind,
    pub selected: DecisionAnalysisDecisionDetail,
    pub compared: Option<DecisionAnalysisDecisionDetail>,
    pub unavailable_reason: Option<DecisionAnalysisComparisonUnavailableReason>,
    pub changed_fields: Vec<DecisionAnalysisChangedField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnalysisMeasurement {
    pub elapsed_millis: u64,
    pub response_bytes: u64,
}

/// Typed responses keep wire keys shared by MCP and the native GUI.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnalysisResponse {
    Overview {
        schema_version: String,
        source_snapshot: DecisionAnalysisSourceSnapshot,
        population_complete: bool,
        totals: DecisionAnalysisTotals,
        measurement: DecisionAnalysisMeasurement,
    },
    Decisions {
        schema_version: String,
        source_snapshot: DecisionAnalysisSourceSnapshot,
        filter: Option<DecisionAnalysisDecisionFilter>,
        page: DecisionAnalysisPage,
        measurement: DecisionAnalysisMeasurement,
    },
    Decision {
        schema_version: String,
        source_snapshot: DecisionAnalysisSourceSnapshot,
        decision: DecisionAnalysisDecisionDetail,
        measurement: DecisionAnalysisMeasurement,
    },
    Compare {
        schema_version: String,
        selected_source_snapshot: DecisionAnalysisSourceSnapshot,
        compared_source_snapshot: Option<DecisionAnalysisSourceSnapshot>,
        comparison: Box<DecisionAnalysisComparison>,
        measurement: DecisionAnalysisMeasurement,
    },
}
