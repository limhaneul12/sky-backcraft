//! Immutable policy definitions, revisions and deterministic evaluation traces.

use super::experiment::finite_statistic;
use super::{
    ContentHash, EligibleEvidence, EvidenceSnapshotId, LabError, PitPolicy, PolicyId,
    PolicyRevisionId, PositionState, RequestId, StrategyKind, StrategySpec, UtcTimestamp, Weight,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const POLICY_SCHEMA_VERSION: &str = "1.0";
const MAX_DEFINITION_BYTES: usize = 64 * 1024;
const MAX_AST_NODES: usize = 128;
const MAX_AST_DEPTH: usize = 12;
const MAX_INDICATORS: usize = 16;
const MAX_RULES: usize = 16;
const MAX_STATES: usize = 8;
const MAX_WINDOW: usize = 5_000;
const MAX_WINDOW_SLOTS: usize = 20_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyOrigin {
    Builtin,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRevisionRef {
    pub policy_id: PolicyId,
    pub revision_id: PolicyRevisionId,
    pub definition_digest: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrozenPolicyRevision {
    pub reference: PolicyRevisionRef,
    pub revision_number: u32,
    pub parent_revision_id: Option<PolicyRevisionId>,
    pub family: StrategyKind,
    pub origin: PolicyOrigin,
    pub definition: PolicyDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRevision {
    pub snapshot: FrozenPolicyRevision,
    pub request_id: RequestId,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyDefinition {
    pub schema_version: String,
    pub name: String,
    pub description: String,
    pub program: PolicyProgram,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PolicyProgram {
    Builtin { strategy: StrategySpec },
    Rules { program: RulesProgram },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RulesProgram {
    pub indicators: Vec<PolicyIndicator>,
    pub states: Vec<PolicyState>,
    pub rules: Vec<PolicyRule>,
    pub fallback: PolicyTarget,
    pub signal_expiry: PolicySignalExpiry,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyIndicator {
    pub id: String,
    pub indicator: PolicyIndicatorKind,
    /// Source candle interval for this indicator. Absent (or equal to the
    /// decision interval) uses the decision stream; a different interval
    /// declares a multi-timeframe source fed from completed bars of that
    /// interval whose close time is at or before each decision time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_interval: Option<super::CandleInterval>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PolicyIndicatorKind {
    Open,
    High,
    Low,
    Close,
    Volume,
    QuoteTurnover,
    Sma { window: usize },
    Ema { window: usize },
    Rsi { window: usize },
    SampleVol { window: usize },
    AnnualVol { window: usize },
    PriorHigh { window: usize },
    PriorLow { window: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyState {
    pub id: String,
    #[serde(deserialize_with = "finite_statistic")]
    pub initial: f64,
    pub next: NumericExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum NumericExpr {
    Constant {
        #[serde(deserialize_with = "finite_statistic")]
        value: f64,
    },
    Indicator {
        id: String,
    },
    State {
        id: String,
    },
    ActualWeight,
    HeldBars,
    EvidenceMultiplier,
    EvidenceCoverage,
    Add {
        left: Box<Self>,
        right: Box<Self>,
    },
    Sub {
        left: Box<Self>,
        right: Box<Self>,
    },
    Mul {
        left: Box<Self>,
        right: Box<Self>,
    },
    Div {
        left: Box<Self>,
        right: Box<Self>,
    },
    Min {
        left: Box<Self>,
        right: Box<Self>,
    },
    Max {
        left: Box<Self>,
        right: Box<Self>,
    },
    Abs {
        value: Box<Self>,
    },
    Clamp {
        value: Box<Self>,
        min: Box<Self>,
        max: Box<Self>,
    },
    If {
        condition: Box<BoolExpr>,
        then_value: Box<Self>,
        else_value: Box<Self>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompareOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum BoolExpr {
    Compare {
        comparison: CompareOp,
        left: NumericExpr,
        right: NumericExpr,
    },
    And {
        conditions: Vec<Self>,
    },
    Or {
        conditions: Vec<Self>,
    },
    Not {
        condition: Box<Self>,
    },
    CrossesAbove {
        left: NumericExpr,
        right: NumericExpr,
    },
    CrossesBelow {
        left: NumericExpr,
        right: NumericExpr,
    },
    PositionIs {
        state: PositionState,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRule {
    pub id: String,
    pub condition: BoolExpr,
    pub target: PolicyTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PolicyTarget {
    Hold,
    Weight { value: NumericExpr },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PolicySignalExpiry {
    EndOfRange,
    DecisionBars { bars: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NamedPolicyValue {
    pub id: String,
    #[serde(deserialize_with = "finite_statistic")]
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyEvidenceTrace {
    pub policy: PitPolicy,
    pub snapshot_id: Option<EvidenceSnapshotId>,
    pub used_at: UtcTimestamp,
    pub eligible: Vec<EligibleEvidence>,
    pub coverage_available: bool,
    pub multiplier: Weight,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyTrace {
    pub matched_rule_id: Option<String>,
    pub state_before: Vec<NamedPolicyValue>,
    pub state_after: Vec<NamedPolicyValue>,
    pub evidence: Option<PolicyEvidenceTrace>,
}

impl PolicyDefinition {
    /// Validate the complete persisted policy definition and all bounded references.
    ///
    /// # Errors
    /// Rejects invalid schemas, names, programs, references, finite values or resource bounds.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.schema_version != POLICY_SCHEMA_VERSION
            || !(1..=64).contains(&self.name.chars().count())
            || self.description.len() > 512
        {
            return Err(LabError::InvalidConfig(
                "policy requires schema 1.0, a 1..=64 character name and <=512 byte description"
                    .into(),
            ));
        }

        match &self.program {
            PolicyProgram::Builtin { strategy } => strategy.validate()?,
            PolicyProgram::Rules { program } => program.validate()?,
        }

        let serialized = serde_json::to_vec(self)?;
        if serialized.len() > MAX_DEFINITION_BYTES {
            return Err(LabError::ResourceLimit(
                "serialized policy definition exceeds 64 KiB".into(),
            ));
        }
        Ok(())
    }

    /// Every declared cross-interval indicator source of this definition.
    #[must_use]
    pub fn declared_source_intervals(&self) -> Vec<crate::contracts::CandleInterval> {
        match &self.program {
            PolicyProgram::Builtin { .. } => Vec::new(),
            PolicyProgram::Rules { program } => program
                .indicators
                .iter()
                .filter_map(|indicator| indicator.source_interval)
                .collect(),
        }
    }

    /// Exact causal source requirements for indicators whose stream differs
    /// from `decision_interval`. Duplicate interval declarations collapse to
    /// the largest indicator warmup and the result is stably ordered by width.
    #[must_use]
    pub(crate) fn source_requirements(
        &self,
        decision_interval: crate::contracts::CandleInterval,
    ) -> Vec<(crate::contracts::CandleInterval, usize)> {
        let PolicyProgram::Rules { program } = &self.program else {
            return Vec::new();
        };
        program.source_requirements(decision_interval)
    }

    /// Required completed decision-stream bars before policy evaluation.
    /// Cross-interval indicator windows are warmed on their own streams and
    /// therefore do not inflate this count.
    ///
    /// # Errors
    /// Rejects an invalid definition or warmup arithmetic overflow.
    pub(crate) fn decision_warmup_bars(
        &self,
        decision_interval: crate::contracts::CandleInterval,
    ) -> Result<usize, LabError> {
        self.validate()?;
        match &self.program {
            PolicyProgram::Builtin { strategy } => strategy.warmup_bars(),
            PolicyProgram::Rules { program } => program.decision_warmup_bars(decision_interval),
        }
    }

    /// Required completed decision bars before evaluating this policy.
    ///
    /// # Errors
    /// Rejects invalid policy definitions or a warmup calculation overflow.
    pub fn warmup_bars(&self) -> Result<usize, LabError> {
        self.validate()?;
        match &self.program {
            PolicyProgram::Builtin { strategy } => strategy.warmup_bars(),
            PolicyProgram::Rules { program } => program.warmup_bars(),
        }
    }

    #[must_use]
    pub fn requires_evidence(&self) -> bool {
        match &self.program {
            PolicyProgram::Builtin { strategy } => matches!(
                strategy,
                StrategySpec::S5 { .. } | StrategySpec::S1CoverageControl { .. }
            ),
            PolicyProgram::Rules { program } => program.requires_evidence(),
        }
    }
}

impl RulesProgram {
    fn validate(&self) -> Result<(), LabError> {
        if self.indicators.len() > MAX_INDICATORS
            || self.rules.len() > MAX_RULES
            || self.states.len() > MAX_STATES
        {
            return Err(LabError::ResourceLimit(
                "policy exceeds 16 indicators, 16 rules or 8 states".into(),
            ));
        }
        if let PolicySignalExpiry::DecisionBars { bars } = self.signal_expiry
            && !(1..=MAX_WINDOW).contains(
                &usize::try_from(bars).map_err(|_| {
                    LabError::InvalidConfig("expiry exceeds platform capacity".into())
                })?,
            )
        {
            return Err(LabError::InvalidConfig(
                "decision-bar expiry must be in 1..=5000".into(),
            ));
        }

        let indicator_ids = unique_ids(self.indicators.iter().map(|item| item.id.as_str()))?;
        let state_ids = unique_ids(self.states.iter().map(|item| item.id.as_str()))?;
        unique_ids(self.rules.iter().map(|item| item.id.as_str()))?;

        let mut window_slots = 0_usize;
        for indicator in &self.indicators {
            if let Some(window) = indicator.indicator.window() {
                if !(1..=MAX_WINDOW).contains(&window) {
                    return Err(LabError::InvalidConfig(
                        "indicator window must be in 1..=5000".into(),
                    ));
                }
                window_slots = window_slots.checked_add(window).ok_or_else(|| {
                    LabError::ResourceLimit("aggregate policy window slots overflow".into())
                })?;
            }
        }
        if window_slots > MAX_WINDOW_SLOTS {
            return Err(LabError::ResourceLimit(
                "aggregate policy window slots exceed 20000".into(),
            ));
        }

        let mut limits = AstLimits::default();
        for state in &self.states {
            if !state.initial.is_finite() {
                return Err(LabError::InvalidConfig(
                    "policy state initial value must be finite".into(),
                ));
            }
            state
                .next
                .validate(&indicator_ids, &state_ids, &mut limits, 1)?;
        }
        for rule in &self.rules {
            rule.condition
                .validate(&indicator_ids, &state_ids, &mut limits, 1)?;
            rule.target
                .validate(&indicator_ids, &state_ids, &mut limits, 1)?;
        }
        self.fallback
            .validate(&indicator_ids, &state_ids, &mut limits, 1)
    }

    fn warmup_bars(&self) -> Result<usize, LabError> {
        let base = self
            .indicators
            .iter()
            .map(|indicator| indicator.indicator.warmup_bars())
            .max()
            .unwrap_or(1);
        base.checked_add(usize::from(self.has_cross()))
            .ok_or_else(|| LabError::ResourceLimit("policy warmup calculation overflow".into()))
    }

    fn decision_warmup_bars(
        &self,
        decision_interval: crate::contracts::CandleInterval,
    ) -> Result<usize, LabError> {
        let base = self
            .indicators
            .iter()
            .filter(|indicator| {
                indicator.source_interval.unwrap_or(decision_interval) == decision_interval
            })
            .map(|indicator| indicator.indicator.warmup_bars())
            .max()
            .unwrap_or(1);
        base.checked_add(usize::from(self.has_cross()))
            .ok_or_else(|| LabError::ResourceLimit("policy warmup calculation overflow".into()))
    }

    fn source_requirements(
        &self,
        decision_interval: crate::contracts::CandleInterval,
    ) -> Vec<(crate::contracts::CandleInterval, usize)> {
        let mut requirements = Vec::<(crate::contracts::CandleInterval, usize)>::new();
        for indicator in &self.indicators {
            let source = indicator.source_interval.unwrap_or(decision_interval);
            if source == decision_interval {
                continue;
            }
            let warmup = indicator.indicator.warmup_bars();
            if let Some((_, current)) = requirements
                .iter_mut()
                .find(|(interval, _)| *interval == source)
            {
                *current = (*current).max(warmup);
            } else {
                requirements.push((source, warmup));
            }
        }
        requirements.sort_by_key(|(interval, _)| interval.duration().num_seconds());
        requirements
    }

    fn has_cross(&self) -> bool {
        self.states.iter().any(|state| state.next.has_cross())
            || self
                .rules
                .iter()
                .any(|rule| rule.condition.has_cross() || rule.target.has_cross())
            || self.fallback.has_cross()
    }

    fn requires_evidence(&self) -> bool {
        self.states
            .iter()
            .any(|state| state.next.requires_evidence())
            || self
                .rules
                .iter()
                .any(|rule| rule.condition.requires_evidence() || rule.target.requires_evidence())
            || self.fallback.requires_evidence()
    }
}

impl PolicyIndicatorKind {
    fn window(&self) -> Option<usize> {
        match self {
            Self::Sma { window }
            | Self::Ema { window }
            | Self::Rsi { window }
            | Self::SampleVol { window }
            | Self::AnnualVol { window }
            | Self::PriorHigh { window }
            | Self::PriorLow { window } => Some(*window),
            Self::Open
            | Self::High
            | Self::Low
            | Self::Close
            | Self::Volume
            | Self::QuoteTurnover => None,
        }
    }

    pub(crate) fn warmup_bars(&self) -> usize {
        match self {
            Self::Sma { window } | Self::Ema { window } => *window,
            Self::Rsi { window }
            | Self::SampleVol { window }
            | Self::AnnualVol { window }
            | Self::PriorHigh { window }
            | Self::PriorLow { window } => window + 1,
            Self::Open
            | Self::High
            | Self::Low
            | Self::Close
            | Self::Volume
            | Self::QuoteTurnover => 1,
        }
    }
}

#[derive(Default)]
struct AstLimits {
    nodes: usize,
}

impl AstLimits {
    fn admit(&mut self, depth: usize) -> Result<(), LabError> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("policy AST node count overflow".into()))?;
        if self.nodes > MAX_AST_NODES || depth > MAX_AST_DEPTH {
            return Err(LabError::ResourceLimit(
                "policy AST exceeds 128 nodes or depth 12".into(),
            ));
        }
        Ok(())
    }
}

impl NumericExpr {
    fn validate(
        &self,
        indicators: &BTreeSet<String>,
        states: &BTreeSet<String>,
        limits: &mut AstLimits,
        depth: usize,
    ) -> Result<(), LabError> {
        limits.admit(depth)?;
        match self {
            Self::Constant { value } if !value.is_finite() => Err(LabError::InvalidConfig(
                "policy numeric constants must be finite".into(),
            )),
            Self::Indicator { id } if !indicators.contains(id) => Err(LabError::InvalidConfig(
                format!("unknown policy indicator reference: {id}"),
            )),
            Self::State { id } if !states.contains(id) => Err(LabError::InvalidConfig(format!(
                "unknown policy state reference: {id}"
            ))),
            Self::Add { left, right }
            | Self::Sub { left, right }
            | Self::Mul { left, right }
            | Self::Div { left, right }
            | Self::Min { left, right }
            | Self::Max { left, right } => {
                left.validate(indicators, states, limits, depth + 1)?;
                right.validate(indicators, states, limits, depth + 1)
            }
            Self::Abs { value } => value.validate(indicators, states, limits, depth + 1),
            Self::Clamp { value, min, max } => {
                value.validate(indicators, states, limits, depth + 1)?;
                min.validate(indicators, states, limits, depth + 1)?;
                max.validate(indicators, states, limits, depth + 1)
            }
            Self::If {
                condition,
                then_value,
                else_value,
            } => {
                condition.validate(indicators, states, limits, depth + 1)?;
                then_value.validate(indicators, states, limits, depth + 1)?;
                else_value.validate(indicators, states, limits, depth + 1)
            }
            Self::Constant { .. }
            | Self::Indicator { .. }
            | Self::State { .. }
            | Self::ActualWeight
            | Self::HeldBars
            | Self::EvidenceMultiplier
            | Self::EvidenceCoverage => Ok(()),
        }
    }

    fn has_cross(&self) -> bool {
        match self {
            Self::Add { left, right }
            | Self::Sub { left, right }
            | Self::Mul { left, right }
            | Self::Div { left, right }
            | Self::Min { left, right }
            | Self::Max { left, right } => left.has_cross() || right.has_cross(),
            Self::Abs { value } => value.has_cross(),
            Self::Clamp { value, min, max } => {
                value.has_cross() || min.has_cross() || max.has_cross()
            }
            Self::If {
                condition,
                then_value,
                else_value,
            } => condition.has_cross() || then_value.has_cross() || else_value.has_cross(),
            Self::Constant { .. }
            | Self::Indicator { .. }
            | Self::State { .. }
            | Self::ActualWeight
            | Self::HeldBars
            | Self::EvidenceMultiplier
            | Self::EvidenceCoverage => false,
        }
    }

    fn requires_evidence(&self) -> bool {
        match self {
            Self::EvidenceMultiplier | Self::EvidenceCoverage => true,
            Self::Add { left, right }
            | Self::Sub { left, right }
            | Self::Mul { left, right }
            | Self::Div { left, right }
            | Self::Min { left, right }
            | Self::Max { left, right } => left.requires_evidence() || right.requires_evidence(),
            Self::Abs { value } => value.requires_evidence(),
            Self::Clamp { value, min, max } => {
                value.requires_evidence() || min.requires_evidence() || max.requires_evidence()
            }
            Self::If {
                condition,
                then_value,
                else_value,
            } => {
                condition.requires_evidence()
                    || then_value.requires_evidence()
                    || else_value.requires_evidence()
            }
            Self::Constant { .. }
            | Self::Indicator { .. }
            | Self::State { .. }
            | Self::ActualWeight
            | Self::HeldBars => false,
        }
    }
}

impl BoolExpr {
    fn validate(
        &self,
        indicators: &BTreeSet<String>,
        states: &BTreeSet<String>,
        limits: &mut AstLimits,
        depth: usize,
    ) -> Result<(), LabError> {
        limits.admit(depth)?;
        match self {
            Self::Compare { left, right, .. }
            | Self::CrossesAbove { left, right }
            | Self::CrossesBelow { left, right } => {
                left.validate(indicators, states, limits, depth + 1)?;
                right.validate(indicators, states, limits, depth + 1)
            }
            Self::And { conditions } | Self::Or { conditions } => {
                for condition in conditions {
                    condition.validate(indicators, states, limits, depth + 1)?;
                }
                Ok(())
            }
            Self::Not { condition } => condition.validate(indicators, states, limits, depth + 1),
            Self::PositionIs { .. } => Ok(()),
        }
    }

    fn has_cross(&self) -> bool {
        match self {
            Self::CrossesAbove { .. } | Self::CrossesBelow { .. } => true,
            Self::Compare { left, right, .. } => left.has_cross() || right.has_cross(),
            Self::And { conditions } | Self::Or { conditions } => {
                conditions.iter().any(Self::has_cross)
            }
            Self::Not { condition } => condition.has_cross(),
            Self::PositionIs { .. } => false,
        }
    }

    fn requires_evidence(&self) -> bool {
        match self {
            Self::Compare { left, right, .. }
            | Self::CrossesAbove { left, right }
            | Self::CrossesBelow { left, right } => {
                left.requires_evidence() || right.requires_evidence()
            }
            Self::And { conditions } | Self::Or { conditions } => {
                conditions.iter().any(Self::requires_evidence)
            }
            Self::Not { condition } => condition.requires_evidence(),
            Self::PositionIs { .. } => false,
        }
    }
}

impl PolicyTarget {
    fn validate(
        &self,
        indicators: &BTreeSet<String>,
        states: &BTreeSet<String>,
        limits: &mut AstLimits,
        depth: usize,
    ) -> Result<(), LabError> {
        match self {
            Self::Hold => Ok(()),
            Self::Weight { value } => value.validate(indicators, states, limits, depth),
        }
    }

    fn has_cross(&self) -> bool {
        match self {
            Self::Hold => false,
            Self::Weight { value } => value.has_cross(),
        }
    }

    fn requires_evidence(&self) -> bool {
        match self {
            Self::Hold => false,
            Self::Weight { value } => value.requires_evidence(),
        }
    }
}

fn unique_ids<'a>(ids: impl Iterator<Item = &'a str>) -> Result<BTreeSet<String>, LabError> {
    let mut unique = BTreeSet::new();
    for id in ids {
        if !valid_local_id(id) || !unique.insert(id.to_owned()) {
            return Err(LabError::InvalidConfig(
                "policy IDs must be unique 1..=64 character ASCII alphanumeric, hyphen or underscore names"
                    .into(),
            ));
        }
    }
    Ok(unique)
}

fn valid_local_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constant(value: f64) -> NumericExpr {
        NumericExpr::Constant { value }
    }

    fn definition(program: RulesProgram) -> PolicyDefinition {
        PolicyDefinition {
            schema_version: POLICY_SCHEMA_VERSION.into(),
            name: "custom_policy".into(),
            description: String::new(),
            program: PolicyProgram::Rules { program },
        }
    }

    fn valid_program() -> RulesProgram {
        RulesProgram {
            indicators: vec![PolicyIndicator {
                id: "close".into(),
                indicator: PolicyIndicatorKind::Close,
                source_interval: None,
            }],
            states: vec![PolicyState {
                id: "memory".into(),
                initial: 0.0,
                next: NumericExpr::State {
                    id: "memory".into(),
                },
            }],
            rules: vec![PolicyRule {
                id: "enter".into(),
                condition: BoolExpr::Compare {
                    comparison: CompareOp::Gt,
                    left: NumericExpr::Indicator { id: "close".into() },
                    right: constant(0.0),
                },
                target: PolicyTarget::Weight {
                    value: constant(1.0),
                },
            }],
            fallback: PolicyTarget::Hold,
            signal_expiry: PolicySignalExpiry::DecisionBars { bars: 1 },
        }
    }

    #[test]
    fn validator_enforces_limits_references_and_finite_values() {
        assert!(definition(valid_program()).validate().is_ok());

        let mut invalid_reference = valid_program();
        invalid_reference.rules[0].condition = BoolExpr::PositionIs {
            state: PositionState::Cash,
        };
        invalid_reference.states[0].next = NumericExpr::Indicator {
            id: "missing".into(),
        };
        assert!(definition(invalid_reference).validate().is_err());

        let mut invalid_window = valid_program();
        invalid_window.indicators[0].indicator = PolicyIndicatorKind::Sma { window: 5_001 };
        assert!(definition(invalid_window).validate().is_err());

        let mut nonfinite = valid_program();
        nonfinite.states[0].initial = f64::NAN;
        assert!(definition(nonfinite).validate().is_err());

        let mut oversized_ast = constant(0.0);
        for _ in 0..MAX_AST_DEPTH {
            oversized_ast = NumericExpr::Abs {
                value: Box::new(oversized_ast),
            };
        }
        let mut excessive_depth = valid_program();
        excessive_depth.rules[0].target = PolicyTarget::Weight {
            value: oversized_ast,
        };
        assert!(definition(excessive_depth).validate().is_err());
    }
}
