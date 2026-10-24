//! Explicit event-linked virtual candidate scenarios.

use super::{
    AccountState, AssetQuantity, BasisPoints, CandleInterval, CapitalMode, ContentHash,
    EvidenceSnapshot, ExecutionPolicy, FillId, FrozenPolicyRevision, LabError, MarketRuleSnapshot,
    OrderId, PaperNullReason, PaperSessionEvent, PaperSessionId, PaperSessionStartRequest,
    PitPolicy, PolicyRevisionRef, PositionState, PriceKrw, QuoteAmount, ReasonCode, Side, SignalId,
    SignedAmount, UtcTimestamp, Weight,
};
use crate::contracts::{CandleObservation, CostPolicy, PortfolioRiskPolicy};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const COUNTERFACTUAL_CONTRACT_VERSION: &str = "1.0";
pub const MAX_COUNTERFACTUAL_CANDIDATES: usize = 32;
pub const MAX_COUNTERFACTUAL_VARIANTS: usize = 8;
pub const MAX_COUNTERFACTUAL_BARS_PER_CANDIDATE: usize = 4_096;
pub const MAX_COUNTERFACTUAL_HOLD_BARS: u32 = 256;
pub const MAX_COUNTERFACTUAL_VALUATION_BARS: u32 = 256;

/// Candidate roles are declarations supplied by the caller because saved paper
/// events do not assign authoritative candidate/filter roles.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum CounterfactualCandidateDeclaration {
    CallerDeclaredScenario {
        scenario_id: String,
        role: CounterfactualCandidateRole,
        declared_by: String,
        rationale: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualCandidateRole {
    EntryOpportunity,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "availability",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum CounterfactualEvidenceInput {
    Snapshot { snapshot: EvidenceSnapshot },
    Unavailable { reason: PaperNullReason },
}

/// One saved paper event and signal, with the exact session configuration and
/// explicit market/evidence bars needed for a caller-declared scenario.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualCandidateInput {
    pub source_session_id: PaperSessionId,
    pub source_configuration: PaperSessionStartRequest,
    pub source_event: PaperSessionEvent,
    pub source_signal_id: SignalId,
    pub declaration: CounterfactualCandidateDeclaration,
    /// Same-market completed decision and execution bars. Each interval stream
    /// is validated independently; the vector includes the source event candle,
    /// decision prefix, fixed exit and declared post-exit valuation horizon.
    pub bars: Vec<CandleObservation>,
    pub evidence: CounterfactualEvidenceInput,
}

/// A fixed exit after N completed decision bars, followed by an explicitly
/// declared completed-bar valuation point. A value of one marks at the close of
/// the first bar after the exit's next-open arrival.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualFixedExit {
    pub hold_decision_bars: u32,
    pub valuation_bars_after_exit: u32,
}

/// Frozen independent virtual account and execution assumptions for one policy
/// variant. Variant comparisons require all scenario fields except policy to
/// match exactly.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualVariant {
    pub variant_id: String,
    pub policy: FrozenPolicyRevision,
    pub initial_capital: QuoteAmount,
    pub capital_mode: CapitalMode,
    pub risk: PortfolioRiskPolicy,
    pub costs: CostPolicy,
    pub execution: ExecutionPolicy,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub latency_ms: u64,
    pub market_rules: MarketRuleSnapshot,
    pub pit_policy: PitPolicy,
    pub exit: CounterfactualFixedExit,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualAnalysisRequest {
    pub candidates: Vec<CounterfactualCandidateInput>,
    pub variants: Vec<CounterfactualVariant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualReportStatus {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualExecutionScope {
    VirtualOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualPnlScope {
    HypotheticalStandaloneScenario,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualCapitalScope {
    IndependentPerCandidateAndVariant,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualReportMetadata {
    pub contract_version: String,
    pub execution_scope: CounterfactualExecutionScope,
    pub pnl_scope: CounterfactualPnlScope,
    pub capital_scope: CounterfactualCapitalScope,
    pub candidate_roles_are_caller_declared: bool,
    pub source_facts_are_preserved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualReport {
    pub status: CounterfactualReportStatus,
    pub metadata: CounterfactualReportMetadata,
    pub contract_digest: ContentHash,
    pub variants: Vec<CounterfactualVariant>,
    pub candidates: Vec<CounterfactualCandidateReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualCandidateIdentity {
    pub digest: ContentHash,
    pub source_session_id: PaperSessionId,
    pub source_event_sequence: u64,
    pub source_signal_id: SignalId,
    pub source_policy_ref: PolicyRevisionRef,
    pub source_event_digest: ContentHash,
    pub source_signal_digest: ContentHash,
    pub source_configuration_digest: ContentHash,
    pub source_state_before: PositionState,
    pub source_state_after: PositionState,
    pub source_decision_bar_id: super::ObservationId,
    pub source_state_mark_digest: ContentHash,
    pub evidence_digest: ContentHash,
    pub bar_evidence_digest: ContentHash,
    pub declaration: CounterfactualCandidateDeclaration,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualCandidateReport {
    pub identity: CounterfactualCandidateIdentity,
    pub source_configuration: PaperSessionStartRequest,
    /// The original source signal, including its actual outcome and reasons.
    pub source_signal: super::SignalRecord,
    pub source_actual_order_ids: Vec<OrderId>,
    pub source_actual_fill_ids: Vec<FillId>,
    pub variant_outcomes: Vec<CounterfactualVariantOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualVariantStatus {
    Completed,
    PartiallyExecuted,
    NotTriggered,
    EntryUnfilled,
    RiskBlocked,
    Unsupported,
    EvidenceUnavailable,
    InsufficientEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualUnavailableReason {
    SharedCapitalUnsupported,
    DynamicCostModelUnsupported,
    PassiveExecutionUnsupported,
    NonzeroLatencyUnsupported,
    CrossIntervalPolicyUnsupported,
    SourceStateNotCash,
    MissingRequiredEvidence,
    SourceEvidenceIdentityMismatch,
    SourceEventCandleUnavailable,
    MissingSourceStateMark,
    MissingExitEvidence,
    OverlappingCandidateWindow,
    DrawdownStopUnsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualVariantOutcome {
    pub variant_id: String,
    pub policy_ref: PolicyRevisionRef,
    pub variant_digest: ContentHash,
    pub status: CounterfactualVariantStatus,
    pub unavailable_reason: Option<CounterfactualUnavailableReason>,
    pub detail: Option<String>,
    pub policy_target_weight: Option<Weight>,
    pub risk_adjusted_target_weight: Option<Weight>,
    pub policy_reasons: Vec<ReasonCode>,
    pub entry_result: Option<ReasonCode>,
    pub exit_result: Option<ReasonCode>,
    pub executions: Vec<CounterfactualExecution>,
    pub initial_capital: QuoteAmount,
    pub ending_account: Option<AccountState>,
    pub final_equity: Option<QuoteAmount>,
    pub net_pnl: Option<SignedAmount>,
    pub cumulative_fees: Option<QuoteAmount>,
    pub valuation: Option<CounterfactualValuation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualExecution {
    pub side: Side,
    pub decision_time: UtcTimestamp,
    pub execution_time: UtcTimestamp,
    pub decision_bar_id: super::ObservationId,
    pub execution_bar_id: super::ObservationId,
    pub liquidity_source_bar_id: super::ObservationId,
    pub liquidity_source_close_time: UtcTimestamp,
    pub decision_reference: PriceKrw,
    pub execution_open_proxy: PriceKrw,
    pub price: PriceKrw,
    pub quantity: AssetQuantity,
    pub notional: QuoteAmount,
    pub fee: QuoteAmount,
    pub fee_bps: BasisPoints,
    pub price_cost_attribution_krw: SignedAmount,
    pub reason: ReasonCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CounterfactualValuationKind {
    ExPostCompletedBarClose,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualValuation {
    pub kind: CounterfactualValuationKind,
    pub observation_id: super::ObservationId,
    pub content_digest: ContentHash,
    pub valued_at: UtcTimestamp,
    pub price: PriceKrw,
}

impl CounterfactualAnalysisRequest {
    /// Validate only the top-level finite input bounds before source resolution.
    ///
    /// # Errors
    /// Returns a resource error for empty or oversized candidate/variant/bar
    /// sets and malformed declaration bounds.
    pub fn validate_bounds(&self) -> Result<(), LabError> {
        if self.candidates.is_empty() || self.candidates.len() > MAX_COUNTERFACTUAL_CANDIDATES {
            return Err(LabError::ResourceLimit(format!(
                "counterfactual candidates require 1..={MAX_COUNTERFACTUAL_CANDIDATES}"
            )));
        }
        if self.variants.is_empty() || self.variants.len() > MAX_COUNTERFACTUAL_VARIANTS {
            return Err(LabError::ResourceLimit(format!(
                "counterfactual variants require 1..={MAX_COUNTERFACTUAL_VARIANTS}"
            )));
        }
        for candidate in &self.candidates {
            if candidate.bars.is_empty()
                || candidate.bars.len() > MAX_COUNTERFACTUAL_BARS_PER_CANDIDATE
            {
                return Err(LabError::ResourceLimit(format!(
                    "counterfactual bars require 1..={MAX_COUNTERFACTUAL_BARS_PER_CANDIDATE} per candidate"
                )));
            }
            match &candidate.declaration {
                CounterfactualCandidateDeclaration::CallerDeclaredScenario {
                    scenario_id,
                    declared_by,
                    rationale,
                    ..
                } if scenario_id.is_empty()
                    || scenario_id.len() > 96
                    || declared_by.is_empty()
                    || declared_by.len() > 96
                    || rationale.is_empty()
                    || rationale.len() > 512 =>
                {
                    return Err(LabError::InvalidConfig(
                        "caller-declared candidate scenario needs bounded identity and rationale"
                            .into(),
                    ));
                }
                CounterfactualCandidateDeclaration::CallerDeclaredScenario { .. } => {}
            }
        }
        Ok(())
    }
}
