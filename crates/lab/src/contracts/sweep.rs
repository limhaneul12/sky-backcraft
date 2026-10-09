//! Deterministic parameter-sweep expansion into exact immutable policies.
//!
//! Every expanded candidate becomes a normalized parameter map with a stable
//! digest and a full [`PolicyDefinition`]; no anonymous parameter tuple ever
//! reaches execution. Expansion is pure: identical requests expand to
//! byte-identical candidates regardless of tuple order.

use super::{
    ContentHash, CostSweep, ExperimentSpec, LabError, PolicyDefinition, PolicyId, PolicyProgram,
    PolicyRevisionId, PolicyRevisionRef, ResearchDesign, ResearchSuiteRequest, StrategyKind,
    StrategySpec, SuitePlanAdmission,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Bounded candidate count; larger studies split into multiple sweep requests
/// and cross-check the resource planner before admission.
pub const MAX_SWEEP_CANDIDATES: usize = 32;

/// One parameter value in a sweep axis. Lookback-style parameters take
/// integers, statistical thresholds take finite floats; decimal weights are
/// not sweepable in this first version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SweepValue {
    Integer(u32),
    Float(#[serde(deserialize_with = "super::finite_statistic")] f64),
}

impl SweepValue {
    fn normalized(&self) -> String {
        match self {
            Self::Integer(value) => value.to_string(),
            Self::Float(value) => serde_json::to_string(value).unwrap_or_default(),
        }
    }
}

/// Explicit tuple or cartesian grid sweep mode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParameterSweepMode {
    Tuples {
        tuples: Vec<BTreeMap<String, SweepValue>>,
    },
    Grid {
        axes: BTreeMap<String, Vec<SweepValue>>,
    },
}

/// Optional suite geometry evaluated together with a policy sweep. The
/// experiment must be structurally valid; its policy selections are replaced
/// by one deterministic nonpersisted placeholder while the shared planner uses
/// the separately supplied candidate count.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepResearchContext {
    pub experiment: ExperimentSpec,
    pub design: ResearchDesign,
    pub cost_sweep: CostSweep,
}

/// Read-only sweep admission result. Counts are calculated before any policy
/// revision is materialized in SQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepPreflightReport {
    /// Distinct fully-applied policy bodies after semantic deduplication, or a
    /// declared upper bound when `candidate_count_exact` is false.
    pub candidate_count: u64,
    /// False only when an oversized grid was bounded from its unique axes
    /// without constructing the full semantic Cartesian product.
    pub candidate_count_exact: bool,
    /// Raw tuple count, or the exact Cartesian size before deduplication.
    pub exact_tuple_count: u64,
    pub market_count: u64,
    pub fold_count: u64,
    pub cost_scenario_count: u64,
    pub estimated_model_count: u64,
    pub estimated_event_count: u64,
    pub estimates_saturated: bool,
    pub resource_admissible: bool,
    pub reject_reason: Option<String>,
    pub notes: Vec<String>,
}

/// One expanded, immutable candidate with its normalized identity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepCandidate {
    pub index: u32,
    /// Normalized parameter map; keys sort, values render canonically.
    pub parameters: BTreeMap<String, String>,
    /// Digest of the exact immutable policy body; semantically identical
    /// candidates share it even when their partial assignments differ.
    pub definition_digest: ContentHash,
    /// Exact immutable policy body the candidate freezes into.
    pub definition: PolicyDefinition,
}

/// Result of a pure sweep expansion before any persistence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepPlan {
    pub family: StrategyKind,
    pub mode: ParameterSweepMode,
    pub candidates: Vec<SweepCandidate>,
    pub duplicates_suppressed: usize,
}

pub(crate) struct PreparedSweep {
    pub report: SweepPreflightReport,
    pub plan: Option<SweepPlan>,
}

#[derive(Debug, Clone, Copy)]
struct SweepShape {
    exact_tuple_count: u64,
    candidate_count: u64,
    candidate_count_exact: bool,
}

/// Calculate exact sweep and optional research-suite resource counts without
/// writing policy rows.
///
/// # Errors
/// Rejects invalid templates, axes, tuple values, or count overflow. Resource
/// excess is returned as a non-admissible report rather than an error.
pub fn preflight_parameter_sweep(
    family: StrategyKind,
    template: &StrategySpec,
    mode: &ParameterSweepMode,
    research: Option<&SweepResearchContext>,
) -> Result<SweepPreflightReport, LabError> {
    Ok(prepare_parameter_sweep(family, template, mode, research)?.report)
}

/// Expand a sweep request into deduplicated immutable policy candidates.
///
/// # Errors
/// Rejects invalid templates, unsupported families or parameters, mistyped
/// values, duplicate tuple keys and grids beyond [`MAX_SWEEP_CANDIDATES`].
pub fn expand_parameter_sweep(
    family: StrategyKind,
    template: &StrategySpec,
    mode: &ParameterSweepMode,
) -> Result<SweepPlan, LabError> {
    let prepared = prepare_parameter_sweep(family, template, mode, None)?;
    if let Some(plan) = prepared.plan {
        return Ok(plan);
    }
    Err(LabError::ResourceLimit(
        prepared
            .report
            .reject_reason
            .unwrap_or_else(|| "parameter sweep was not admitted".into()),
    ))
}

pub(crate) fn prepare_parameter_sweep(
    family: StrategyKind,
    template: &StrategySpec,
    mode: &ParameterSweepMode,
    research: Option<&SweepResearchContext>,
) -> Result<PreparedSweep, LabError> {
    template.validate()?;
    if template.kind() != family || family == StrategyKind::BuyAndHold {
        return Err(LabError::InvalidConfig(
            "sweep family must match the template strategy and support parameters".into(),
        ));
    }
    let allowed = parameter_names(template);
    let mut shape = analyze_shape(template, mode, &allowed)?;
    let sweep_limit = u64::try_from(MAX_SWEEP_CANDIDATES).unwrap_or(u64::MAX);
    let mut report = research_report(family, template, shape, research)?;
    if shape.candidate_count > sweep_limit {
        report.resource_admissible = false;
        let sweep_reason = format!(
            "sweep would produce {} distinct-candidate upper bound; limit is {MAX_SWEEP_CANDIDATES}",
            shape.candidate_count
        );
        report.reject_reason = Some(match report.reject_reason.take() {
            Some(reason) => format!("{sweep_reason}; {reason}"),
            None => sweep_reason,
        });
        if !shape.candidate_count_exact {
            report.notes.push(
                "candidate_count is an upper bound from unique grid axes; the oversized semantic Cartesian product was not materialized"
                    .into(),
            );
        }
        return Ok(PreparedSweep { report, plan: None });
    }
    let plan = materialize_plan(family, template, mode, shape)?;
    shape.candidate_count = u64_count(plan.candidates.len(), "candidate count")?;
    shape.candidate_count_exact = true;
    let report = research_report(family, template, shape, research)?;
    Ok(PreparedSweep {
        report,
        plan: Some(plan),
    })
}

fn analyze_shape(
    template: &StrategySpec,
    mode: &ParameterSweepMode,
    allowed: &BTreeSet<String>,
) -> Result<SweepShape, LabError> {
    match mode {
        ParameterSweepMode::Tuples { tuples } => {
            if tuples.is_empty() {
                return Err(LabError::InvalidConfig(
                    "parameter sweep requires at least one tuple".into(),
                ));
            }
            let mut semantic_candidates = BTreeSet::new();
            for tuple in tuples {
                validate_assignment_names(tuple, allowed, template.kind())?;
                let applied = apply_assignment(template, tuple)?;
                applied.validate()?;
                semantic_candidates.insert(candidate_semantic_digest(template.kind(), &applied)?);
            }
            Ok(SweepShape {
                exact_tuple_count: u64_count(tuples.len(), "tuple count")?,
                candidate_count: u64_count(semantic_candidates.len(), "candidate count")?,
                candidate_count_exact: true,
            })
        }
        ParameterSweepMode::Grid { axes } => {
            let mut exact_tuple_count = 1_u64;
            let mut candidate_count = 1_u64;
            for (name, values) in axes {
                if !allowed.contains(name) {
                    return Err(LabError::InvalidConfig(format!(
                        "unsupported sweep axis {name} for {:?}",
                        template.kind()
                    )));
                }
                if values.is_empty() {
                    return Err(LabError::InvalidConfig(format!(
                        "sweep axis {name} must have at least one value"
                    )));
                }
                let mut unique = BTreeSet::new();
                for value in values {
                    let assignment = BTreeMap::from([(name.clone(), value.clone())]);
                    apply_assignment(template, &assignment)?.validate()?;
                    unique.insert(value.normalized());
                }
                exact_tuple_count = checked_count_product(
                    exact_tuple_count,
                    u64_count(values.len(), "axis value count")?,
                )?;
                candidate_count = checked_count_product(
                    candidate_count,
                    u64_count(unique.len(), "unique axis value count")?,
                )?;
            }
            Ok(SweepShape {
                exact_tuple_count,
                candidate_count,
                candidate_count_exact: false,
            })
        }
    }
}

fn materialize_plan(
    family: StrategyKind,
    template: &StrategySpec,
    mode: &ParameterSweepMode,
    shape: SweepShape,
) -> Result<SweepPlan, LabError> {
    let allowed = parameter_names(template);
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    match mode {
        ParameterSweepMode::Tuples { tuples } => {
            for assignment in tuples {
                validate_assignment_names(assignment, &allowed, family)?;
                push_candidate(family, template, assignment, &mut seen, &mut candidates)?;
            }
        }
        ParameterSweepMode::Grid { axes } => {
            let unique_axes = unique_grid_axes(axes);
            for assignment in cartesian(&unique_axes) {
                push_candidate(family, template, &assignment, &mut seen, &mut candidates)?;
            }
        }
    }
    if candidates.is_empty() {
        return Err(LabError::InvalidConfig(
            "parameter sweep requires at least one candidate".into(),
        ));
    }
    let duplicates_suppressed = shape
        .exact_tuple_count
        .checked_sub(u64_count(candidates.len(), "materialized candidate count")?)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| LabError::ResourceLimit("duplicate count overflow".into()))?;
    Ok(SweepPlan {
        family,
        mode: mode.clone(),
        candidates,
        duplicates_suppressed,
    })
}

/// Cartesian product over sorted axes; deterministic by construction.
fn cartesian(axes: &[(String, Vec<SweepValue>)]) -> Vec<BTreeMap<String, SweepValue>> {
    let mut products = vec![BTreeMap::new()];
    for (name, values) in axes {
        let mut next = Vec::new();
        for product in &products {
            for value in values {
                let mut candidate = product.clone();
                candidate.insert(name.clone(), value.clone());
                next.push(candidate);
            }
        }
        products = next;
    }
    products
}

fn unique_grid_axes(axes: &BTreeMap<String, Vec<SweepValue>>) -> Vec<(String, Vec<SweepValue>)> {
    axes.iter()
        .map(|(name, values)| {
            let mut unique = BTreeMap::new();
            for value in values {
                unique
                    .entry(value.normalized())
                    .or_insert_with(|| value.clone());
            }
            (name.clone(), unique.into_values().collect())
        })
        .collect()
}

fn push_candidate(
    family: StrategyKind,
    template: &StrategySpec,
    assignment: &BTreeMap<String, SweepValue>,
    seen: &mut BTreeSet<ContentHash>,
    candidates: &mut Vec<SweepCandidate>,
) -> Result<(), LabError> {
    let applied = apply_assignment(template, assignment)?;
    applied.validate()?;
    let semantic_digest = candidate_semantic_digest(family, &applied)?;
    if !seen.insert(semantic_digest.clone()) {
        return Ok(());
    }
    let definition = PolicyDefinition {
        schema_version: super::POLICY_SCHEMA_VERSION.into(),
        name: format!("sweep-{family:?}-{}", &semantic_digest.as_str()[..12]),
        description: "parameter sweep candidate; deterministic expansion".into(),
        program: PolicyProgram::Builtin {
            strategy: applied.clone(),
        },
    };
    definition.validate()?;
    let definition_digest = ContentHash::of_value(&definition)?;
    let index = u32::try_from(candidates.len())
        .map_err(|_| LabError::ResourceLimit("candidate index overflow".into()))?;
    candidates.push(SweepCandidate {
        index,
        parameters: strategy_parameters(&applied),
        definition_digest,
        definition,
    });
    Ok(())
}

fn candidate_semantic_digest(
    family: StrategyKind,
    applied: &StrategySpec,
) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&("policy-sweep-candidate-v2", family, applied))
}

fn research_report(
    family: StrategyKind,
    template: &StrategySpec,
    shape: SweepShape,
    research: Option<&SweepResearchContext>,
) -> Result<SweepPreflightReport, LabError> {
    let Some(research) = research else {
        return Ok(SweepPreflightReport {
            candidate_count: shape.candidate_count,
            candidate_count_exact: shape.candidate_count_exact,
            exact_tuple_count: shape.exact_tuple_count,
            market_count: 0,
            fold_count: 0,
            cost_scenario_count: 0,
            estimated_model_count: 0,
            estimated_event_count: 0,
            estimates_saturated: false,
            resource_admissible: true,
            reject_reason: None,
            notes: Vec::new(),
        });
    };
    let mut experiment = research.experiment.clone();
    experiment.strategies.clear();
    let placeholder_digest = candidate_semantic_digest(family, template)?;
    experiment.policy_selections = vec![PolicyRevisionRef {
        policy_id: PolicyId::from_seed(&format!("sweep-preflight:{placeholder_digest}")),
        revision_id: PolicyRevisionId::from_seed(&format!("sweep-preflight:{placeholder_digest}")),
        definition_digest: placeholder_digest,
    }];
    let suite = crate::research::plan_suite_shape(
        &ResearchSuiteRequest {
            request_id: super::RequestId::from_seed("sweep-preflight"),
            template: experiment,
            design: research.design.clone(),
            cost_sweep: research.cost_sweep.clone(),
        },
        shape.candidate_count,
    )?;
    let (resource_admissible, reject_reason) = match &suite.admission {
        SuitePlanAdmission::Admitted => (true, None),
        SuitePlanAdmission::Invalid { message } => (false, Some(message.clone())),
        SuitePlanAdmission::Rejected { violations } => (
            false,
            Some(
                violations
                    .iter()
                    .map(|violation| {
                        format!(
                            "{:?} requested {} exceeds {}",
                            violation.item, violation.requested, violation.allowed
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
        ),
    };
    Ok(SweepPreflightReport {
        candidate_count: shape.candidate_count,
        candidate_count_exact: shape.candidate_count_exact,
        exact_tuple_count: shape.exact_tuple_count,
        market_count: suite.markets,
        fold_count: suite.folds,
        cost_scenario_count: suite.cost_scenarios,
        estimated_model_count: suite.comparison_cells,
        estimated_event_count: suite.estimated_run_events,
        estimates_saturated: suite.estimates_saturated,
        resource_admissible,
        reject_reason,
        notes: suite.notes,
    })
}

fn validate_assignment_names(
    assignment: &BTreeMap<String, SweepValue>,
    allowed: &BTreeSet<String>,
    family: StrategyKind,
) -> Result<(), LabError> {
    for name in assignment.keys() {
        if !allowed.contains(name) {
            return Err(LabError::InvalidConfig(format!(
                "unsupported sweep parameter {name} for {family:?}"
            )));
        }
    }
    Ok(())
}

fn checked_count_product(left: u64, right: u64) -> Result<u64, LabError> {
    left.checked_mul(right)
        .ok_or_else(|| LabError::ResourceLimit("sweep tuple count exceeds u64".into()))
}

fn u64_count(count: usize, label: &str) -> Result<u64, LabError> {
    u64::try_from(count).map_err(|_| LabError::ResourceLimit(format!("{label} exceeds u64")))
}

fn strategy_parameters(spec: &StrategySpec) -> BTreeMap<String, String> {
    match spec {
        StrategySpec::S1 { state }
        | StrategySpec::S5 { state }
        | StrategySpec::S1CoverageControl { state } => BTreeMap::from([
            ("state.ema_length".into(), state.ema_length.to_string()),
            ("state.vol_length".into(), state.vol_length.to_string()),
            (
                "state.k".into(),
                serde_json::to_string(&state.k).unwrap_or_default(),
            ),
        ]),
        StrategySpec::S2 {
            entry_length,
            exit_length,
        } => BTreeMap::from([
            ("entry_length".into(), entry_length.to_string()),
            ("exit_length".into(), exit_length.to_string()),
        ]),
        StrategySpec::S3 { .. } | StrategySpec::S4 { .. } | StrategySpec::BuyAndHold => {
            BTreeMap::new()
        }
    }
}

/// Sweepable parameter names for one strategy family; dotted paths address
/// nested state parameters. Decimal weights are excluded on purpose.
#[must_use]
fn parameter_names(spec: &StrategySpec) -> BTreeSet<String> {
    let names: &[&str] = match spec {
        StrategySpec::S1 { .. }
        | StrategySpec::S5 { .. }
        | StrategySpec::S1CoverageControl { .. } => {
            &["state.ema_length", "state.vol_length", "state.k"]
        }
        StrategySpec::S2 { .. } => &["entry_length", "exit_length"],
        // S3/S4 stay unsweepable in this first version; the family list grows
        // with an explicit typed apply path per parameter.
        StrategySpec::S3 { .. } | StrategySpec::S4 { .. } | StrategySpec::BuyAndHold => &[],
    };
    names.iter().map(|name| (*name).to_string()).collect()
}

/// Apply one parameter assignment to a template clone; typed per parameter.
/// # Errors
/// Rejects mistyped values and parameters outside the family's allowed set.
fn apply_assignment(
    template: &StrategySpec,
    assignment: &BTreeMap<String, SweepValue>,
) -> Result<StrategySpec, LabError> {
    let mut spec = template.clone();
    let integer = |value: &SweepValue, name: &str| -> Result<u32, LabError> {
        match value {
            SweepValue::Integer(value) if *value <= 5000 => Ok(*value),
            _ => Err(LabError::InvalidConfig(format!(
                "sweep parameter {name} requires an integer lookback in 0..=5000"
            ))),
        }
    };
    let float = |value: &SweepValue, name: &str| -> Result<f64, LabError> {
        match value {
            SweepValue::Float(value) if value.is_finite() => Ok(*value),
            _ => Err(LabError::InvalidConfig(format!(
                "sweep parameter {name} requires a finite float"
            ))),
        }
    };
    for (name, value) in assignment {
        let name = name.as_str();
        match (&mut spec, name) {
            (
                StrategySpec::S1 { state }
                | StrategySpec::S5 { state }
                | StrategySpec::S1CoverageControl { state },
                "state.ema_length",
            ) => {
                state.ema_length = usize::try_from(integer(value, name)?)
                    .map_err(|_| LabError::InvalidConfig("lookback overflow".into()))?;
            }
            (
                StrategySpec::S1 { state }
                | StrategySpec::S5 { state }
                | StrategySpec::S1CoverageControl { state },
                "state.vol_length",
            ) => {
                state.vol_length = usize::try_from(integer(value, name)?)
                    .map_err(|_| LabError::InvalidConfig("lookback overflow".into()))?;
            }
            (
                StrategySpec::S1 { state }
                | StrategySpec::S5 { state }
                | StrategySpec::S1CoverageControl { state },
                "state.k",
            ) => {
                state.k = float(value, name)?;
            }
            (StrategySpec::S2 { entry_length, .. }, "entry_length") => {
                *entry_length = usize::try_from(integer(value, name)?)
                    .map_err(|_| LabError::InvalidConfig("lookback overflow".into()))?;
            }
            (StrategySpec::S2 { exit_length, .. }, "exit_length") => {
                *exit_length = usize::try_from(integer(value, name)?)
                    .map_err(|_| LabError::InvalidConfig("lookback overflow".into()))?;
            }
            _ => {
                return Err(LabError::InvalidConfig(format!(
                    "sweep parameter {name} does not exist on this strategy"
                )));
            }
        }
    }
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        AssetQuantity, BasisPoints, CandleInterval, CausalExecutionPolicy, CostPolicy, DatasetId,
        EvidenceUnavailablePolicy, ExecutionPolicy, MarketId, MarketRuleSnapshot, PitPolicy,
        PriceKrw, QuoteAmount, ReportClock, RuleProvenance, RuleSnapshotId, TerminalPolicy,
        TickBand, UtcRange, UtcTimestamp, Weight,
    };
    use rust_decimal::Decimal;

    fn s2(entry: u32, exit: u32) -> StrategySpec {
        StrategySpec::S2 {
            entry_length: usize::try_from(entry).unwrap_or(0),
            exit_length: usize::try_from(exit).unwrap_or(0),
        }
    }

    fn int(value: u32) -> SweepValue {
        SweepValue::Integer(value)
    }

    fn research_context() -> SweepResearchContext {
        let start = UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z").expect("start");
        let end = UtcTimestamp::parse_rfc3339("2024-01-01T06:00:00Z").expect("end");
        let range = UtcRange::new(start, end).expect("range");
        let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
        let placeholder_digest = ContentHash::of_bytes(b"sweep-test-placeholder");
        SweepResearchContext {
            experiment: ExperimentSpec {
                schema_version: "3.0".into(),
                dataset_ids: vec![DatasetId::new("sweep-preflight-dataset").expect("dataset")],
                markets: ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
                    .into_iter()
                    .map(|market| MarketId::parse_upbit(market).expect("market"))
                    .collect(),
                range,
                strategies: Vec::new(),
                policy_selections: vec![PolicyRevisionRef {
                    policy_id: PolicyId::from_seed("sweep-test-placeholder"),
                    revision_id: PolicyRevisionId::from_seed("sweep-test-placeholder"),
                    definition_digest: placeholder_digest,
                }],
                causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
                capital_mode: None,
                decision_interval: CandleInterval::H1,
                execution_resolution: CandleInterval::H1,
                latency_ms: 0,
                initial_cash: QuoteAmount::new(Decimal::from(1_000_000)).expect("cash"),
                costs: CostPolicy {
                    buy_fee_bps: zero,
                    sell_fee_bps: zero,
                    maker_fee_bps: zero,
                    half_spread_bps: zero,
                    slippage_bps: zero,
                    impact_bps: zero,
                    assumption_label: "sweep preflight fixture".into(),
                    dynamic: None,
                },
                execution: ExecutionPolicy::NextBarOpen {
                    participation_cap: Weight::new(Decimal::ONE).expect("weight"),
                },
                market_rules: MarketRuleSnapshot {
                    id: RuleSnapshotId::new("sweep-preflight-rules").expect("rules"),
                    provenance: RuleProvenance::ExplicitScenario,
                    valid_range: range,
                    observed_at: start,
                    source_refs: vec!["synthetic-sweep-preflight".into()],
                    assumption_label: "sweep preflight fixture".into(),
                    min_notional: QuoteAmount::new(Decimal::ONE).expect("notional"),
                    quantity_step: AssetQuantity::new(Decimal::new(1, 4)).expect("step"),
                    ticks: vec![TickBand {
                        lower_bound: QuoteAmount::new(Decimal::ZERO).expect("lower"),
                        tick: PriceKrw::new(Decimal::ONE).expect("tick"),
                    }],
                    fee_schedule: None,
                    trading_state: None,
                    maintenance_windows: Vec::new(),
                },
                market_rules_history: Vec::new(),
                terminal_policy: TerminalPolicy::MarkToMarket,
                evidence_snapshot_id: None,
                pit_policy: PitPolicy::StrictPit,
                evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
                report_clock: ReportClock {
                    timezone: "UTC".into(),
                    min_annualization_days: 1,
                    risk_free_annual: 0.0,
                },
                seed: 17,
            },
            design: ResearchDesign::WalkForward {
                selection_bars: 2,
                evaluation_bars: 2,
                step_bars: 2,
                embargo_bars: 0,
            },
            cost_sweep: CostSweep {
                fee_bps: vec![
                    BasisPoints::new(Decimal::ONE).expect("fee one"),
                    BasisPoints::new(Decimal::from(2)).expect("fee two"),
                ],
                slippage_bps: vec![BasisPoints::new(Decimal::ONE).expect("slippage")],
            },
        }
    }

    #[test]
    fn tuple_expansion_is_deterministic_and_deduplicates() {
        let template = s2(20, 10);
        let tuple = |entry: u32, exit: u32| {
            BTreeMap::from([
                ("entry_length".to_string(), int(entry)),
                ("exit_length".to_string(), int(exit)),
            ])
        };
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![tuple(20, 10), tuple(55, 20), tuple(20, 10)],
        };
        let plan = expand_parameter_sweep(StrategyKind::S2, &template, &mode)
            .expect("valid sweep expands");
        assert_eq!(plan.candidates.len(), 2);
        assert_eq!(plan.duplicates_suppressed, 1);
        // Order independence: reversed tuple list produces identical digests.
        let reversed = ParameterSweepMode::Tuples {
            tuples: vec![tuple(20, 10), tuple(55, 20), tuple(20, 10)],
        };
        let mut reversed = expand_parameter_sweep(StrategyKind::S2, &template, &reversed)
            .expect("valid sweep expands");
        reversed.candidates.reverse();
        let digests = |plan: &SweepPlan| {
            plan.candidates
                .iter()
                .map(|candidate| candidate.definition_digest.clone())
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(digests(&plan), digests(&reversed));
        // The frozen definition carries the exact applied parameters.
        let candidate = &plan.candidates[0];
        let PolicyDefinition {
            program: PolicyProgram::Builtin { strategy },
            ..
        } = &candidate.definition
        else {
            panic!("sweep candidates freeze builtin strategies");
        };
        let StrategySpec::S2 {
            entry_length,
            exit_length,
        } = strategy
        else {
            panic!("s2 sweep freezes an S2 strategy");
        };
        assert_eq!(
            candidate.parameters.get("entry_length").map(String::as_str),
            Some(entry_length.to_string().as_str())
        );
        assert_eq!(
            candidate.parameters.get("exit_length").map(String::as_str),
            Some(exit_length.to_string().as_str())
        );
    }

    #[test]
    fn semantic_dedupe_uses_the_fully_applied_template() {
        let template = s2(20, 10);
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![
                BTreeMap::new(),
                BTreeMap::from([("entry_length".to_string(), int(20))]),
                BTreeMap::from([
                    ("entry_length".to_string(), int(20)),
                    ("exit_length".to_string(), int(10)),
                ]),
            ],
        };
        let report = preflight_parameter_sweep(StrategyKind::S2, &template, &mode, None)
            .expect("preflight succeeds");
        assert_eq!(report.exact_tuple_count, 3);
        assert_eq!(report.candidate_count, 1);
        assert!(report.resource_admissible);
        let plan = expand_parameter_sweep(StrategyKind::S2, &template, &mode)
            .expect("semantic duplicate sweep expands");
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.duplicates_suppressed, 2);
        assert_eq!(
            plan.candidates[0]
                .parameters
                .get("exit_length")
                .map(String::as_str),
            Some("10")
        );
    }

    #[test]
    fn untouched_template_parameters_are_part_of_candidate_identity() {
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![BTreeMap::from([("entry_length".to_string(), int(55))])],
        };
        let first = expand_parameter_sweep(StrategyKind::S2, &s2(20, 10), &mode)
            .expect("first template expands");
        let second = expand_parameter_sweep(StrategyKind::S2, &s2(20, 20), &mode)
            .expect("second template expands");
        assert_ne!(
            first.candidates[0].definition_digest, second.candidates[0].definition_digest,
            "an untouched template parameter must separate policy identity"
        );
    }

    #[test]
    fn grid_expands_cartesian_and_rejects_explosions() {
        let template = s2(20, 10);
        let mode = ParameterSweepMode::Grid {
            axes: BTreeMap::from([
                ("entry_length".to_string(), vec![int(20), int(40), int(55)]),
                ("exit_length".to_string(), vec![int(10), int(20)]),
            ]),
        };
        let plan =
            expand_parameter_sweep(StrategyKind::S2, &template, &mode).expect("small grid expands");
        assert_eq!(plan.candidates.len(), 6);
        let report = preflight_parameter_sweep(StrategyKind::S2, &template, &mode, None)
            .expect("small grid preflights");
        assert_eq!(report.exact_tuple_count, 6);
        assert_eq!(report.candidate_count, 6);
        let explosion = ParameterSweepMode::Grid {
            axes: BTreeMap::from([
                (
                    "entry_length".to_string(),
                    (1..=100).map(int).collect::<Vec<_>>(),
                ),
                (
                    "exit_length".to_string(),
                    (1..=100).map(int).collect::<Vec<_>>(),
                ),
            ]),
        };
        assert!(matches!(
            expand_parameter_sweep(StrategyKind::S2, &template, &explosion),
            Err(LabError::ResourceLimit(_))
        ));
        let report = preflight_parameter_sweep(StrategyKind::S2, &template, &explosion, None)
            .expect("oversized grid returns a report");
        assert_eq!(report.exact_tuple_count, 10_000);
        assert_eq!(report.candidate_count, 10_000);
        assert!(!report.resource_admissible);
        assert!(report.reject_reason.is_some());
    }

    #[test]
    fn oversized_grid_keeps_joint_research_counts_without_materialization() {
        let template = s2(20, 10);
        let mode = ParameterSweepMode::Grid {
            axes: BTreeMap::from([
                (
                    "entry_length".to_string(),
                    (1..=100).map(int).collect::<Vec<_>>(),
                ),
                (
                    "exit_length".to_string(),
                    (1..=100).map(int).collect::<Vec<_>>(),
                ),
            ]),
        };
        let context = research_context();
        let prepared = prepare_parameter_sweep(StrategyKind::S2, &template, &mode, Some(&context))
            .expect("oversized preflight returns a bounded report");
        assert!(
            prepared.plan.is_none(),
            "oversized candidates stay unmaterialized"
        );
        assert_eq!(prepared.report.exact_tuple_count, 10_000);
        assert_eq!(prepared.report.candidate_count, 10_000);
        assert!(!prepared.report.candidate_count_exact);
        assert_eq!(prepared.report.market_count, 3);
        assert_eq!(prepared.report.fold_count, 2);
        assert_eq!(prepared.report.cost_scenario_count, 2);
        assert_eq!(prepared.report.estimated_model_count, 120_012);
        assert_eq!(prepared.report.estimated_event_count, 3_120_312);
        assert!(!prepared.report.estimates_saturated);
        assert!(!prepared.report.resource_admissible);
        assert!(prepared.report.reject_reason.is_some());
    }

    #[test]
    fn unsupported_parameters_and_mistyped_values_are_rejected() {
        let template = s2(20, 10);
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![BTreeMap::from([("window".to_string(), int(5))])],
        };
        assert!(matches!(
            expand_parameter_sweep(StrategyKind::S2, &template, &mode),
            Err(LabError::InvalidConfig(_))
        ));
        let mistyped = ParameterSweepMode::Tuples {
            tuples: vec![BTreeMap::from([(
                "entry_length".to_string(),
                SweepValue::Float(20.0),
            )])],
        };
        assert!(matches!(
            expand_parameter_sweep(StrategyKind::S2, &template, &mistyped),
            Err(LabError::InvalidConfig(_))
        ));
        // The template itself must stay valid.
        let mut invalid = s2(20, 10);
        if let StrategySpec::S2 { entry_length, .. } = &mut invalid {
            *entry_length = 0;
        }
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![BTreeMap::new()],
        };
        assert!(matches!(
            expand_parameter_sweep(StrategyKind::S2, &invalid, &mode),
            Err(LabError::InvalidConfig(_))
        ));
    }

    #[test]
    fn float_sweep_targets_apply_and_validate() {
        let template = StrategySpec::S1 {
            state: super::super::StateParameters {
                ema_length: 20,
                vol_length: 10,
                k: 0.5,
            },
        };
        let mode = ParameterSweepMode::Tuples {
            tuples: vec![
                BTreeMap::from([("state.k".to_string(), SweepValue::Float(1.5))]),
                BTreeMap::from([("state.k".to_string(), SweepValue::Float(2.0))]),
            ],
        };
        let plan = expand_parameter_sweep(StrategyKind::S1, &template, &mode)
            .expect("float sweep expands");
        assert_eq!(plan.candidates.len(), 2);
        assert_eq!(
            plan.candidates[0]
                .parameters
                .get("state.k")
                .map(String::as_str),
            Some("1.5")
        );
        // A negative k fails template validation after application.
        let negative = ParameterSweepMode::Tuples {
            tuples: vec![BTreeMap::from([(
                "state.k".to_string(),
                SweepValue::Float(-1.0),
            )])],
        };
        assert!(matches!(
            expand_parameter_sweep(StrategyKind::S1, &template, &negative),
            Err(LabError::InvalidConfig(_))
        ));
    }
}
