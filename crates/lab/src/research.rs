//! Pure deterministic research-suite geometry and selection decisions.

use crate::contracts::{
    ContentHash, CostPolicy, DatasetSnapshot, EligibleEvidence, EvidenceSnapshot, ExperimentSpec,
    FrozenPolicyRevision, FrozenResearchSuite, LabError, MAX_RUN_EVENTS, MAX_SUITE_CELLS,
    MAX_SUITE_FOLDS, MAX_SUITE_RUNS, MAX_SUITE_SCENARIOS, ModelStatus, ObservationId,
    PolicyRevisionRef, ResearchDesign, ResearchSuiteRequest, ResolvedPlan, RunId,
    RunModelComparison, SuiteCase, SuiteCaseId, SuiteCaseStatus, SuiteId, SuitePhase, SuiteRecord,
    SuiteSummary, UtcRange, Weight, experiment_config_digest, strategy_binding,
    validate_frozen_policy,
};
use crate::evidence::EvidenceEvaluator;
use chrono::Duration;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub use crate::contracts::UnavailableCandidate;

/// Conservative uncompressed byte estimate per planned ledger fact; sealed
/// chunks compress well below it. Storage stays an estimate, never a cap.
pub const PLANNED_BYTES_PER_EVENT: u64 = 400;

#[cfg(test)]
mod tests;

/// Fully expanded, effect-free suite geometry used during preflight.
#[derive(Debug, Clone, Serialize)]
pub struct ResearchGeometry {
    pub scenarios: Vec<CostPolicy>,
    pub folds: Vec<crate::contracts::WalkForwardFold>,
    pub planned_runs: u32,
    pub planned_comparison_cells: u32,
    pub estimated_events: u64,
    pub unused_tail: Option<UtcRange>,
}

/// Read-only resource plan for one suite request. Shared formulas with
/// [`expand_geometry`] keep planner/create estimate drift minimal.
///
/// # Errors
/// Returns the create-time validation error for structurally invalid requests.
#[expect(
    clippy::too_many_lines,
    reason = "one read-only admission pass mirrors every create-time limit formula"
)]
pub fn plan_suite(
    request: &ResearchSuiteRequest,
) -> Result<crate::contracts::SuitePlanReport, LabError> {
    use crate::contracts::{SuitePlanAdmission, SuitePlanLimitKind, SuitePlanViolation};
    let invalid = |message: String| {
        Ok(crate::contracts::SuitePlanReport {
            candidates: 0,
            markets: 0,
            folds: 0,
            cost_scenarios: 0,
            planned_runs: 0,
            comparison_cells: 0,
            estimated_model_events: 0,
            estimated_run_events: 0,
            estimated_storage_bytes: 0,
            admission: SuitePlanAdmission::Invalid { message },
            suggestions: Vec::new(),
            notes: Vec::new(),
        })
    };
    if let Err(error) = request.template.validate() {
        return invalid(failure_message(&error));
    }
    if request.template.costs.dynamic.is_some() {
        return invalid(
            "cost sweep is not supported with a dynamic cost model; sweep the fixed model or omit the sweep"
                .into(),
        );
    }
    if request.template.schema_version != "3.0"
        || request.template.causal_execution
            != Some(crate::contracts::CausalExecutionPolicy::DeclaredPolicyWarmup)
        || request.template.pit_policy != crate::contracts::PitPolicy::StrictPit
    {
        return invalid("research suites require v3 declared-policy warmup and STRICT_PIT".into());
    }
    let fees = &request.cost_sweep.fee_bps;
    let slippages = &request.cost_sweep.slippage_bps;
    if fees.is_empty() || slippages.is_empty() {
        return invalid("cost sweep axes must both be nonempty".into());
    }
    if fees
        .iter()
        .map(|value| value.get())
        .collect::<BTreeSet<_>>()
        .len()
        != fees.len()
        || slippages
            .iter()
            .map(|value| value.get())
            .collect::<BTreeSet<_>>()
            .len()
            != slippages.len()
    {
        return invalid("cost sweep axes must not contain duplicates".into());
    }
    let scenario_count = checked_product(&[fees.len(), slippages.len()], "suite scenarios")?;
    let candidates = request.template.policy_selections.len();
    let markets = request.template.markets.len();
    let mut violations = Vec::new();
    if scenario_count > MAX_SUITE_SCENARIOS {
        violations.push(SuitePlanViolation {
            item: SuitePlanLimitKind::SuiteScenarios,
            requested: u64::try_from(scenario_count).unwrap_or(u64::MAX),
            allowed: u64::try_from(MAX_SUITE_SCENARIOS).unwrap_or(u64::MAX),
        });
    }
    let folds = match request.design {
        crate::contracts::ResearchDesign::Batch => 0_u64,
        crate::contracts::ResearchDesign::WalkForward {
            selection_bars,
            evaluation_bars,
            step_bars,
            embargo_bars,
        } => {
            if selection_bars == 0 || evaluation_bars == 0 || step_bars < evaluation_bars {
                return invalid(
                    "walk-forward requires positive selection/evaluation and step >= evaluation bars"
                        .into(),
                );
            }
            let bars = u64::try_from(
                request
                    .template
                    .range
                    .bars(request.template.decision_interval)
                    .map_err(|error| {
                        LabError::InvalidConfig(format!("decision bar count: {error}"))
                    })?,
            )
            .map_err(|_| LabError::ResourceLimit("decision bar count overflow".into()))?;
            let span =
                u64::from(selection_bars) + u64::from(embargo_bars) + u64::from(evaluation_bars);
            if bars < span {
                return invalid("walk-forward range yields no complete fold".into());
            }
            let step = u64::from(step_bars);
            let count = (bars - span) / step + 1;
            if count > u64::try_from(MAX_SUITE_FOLDS).unwrap_or(u64::MAX) {
                violations.push(SuitePlanViolation {
                    item: SuitePlanLimitKind::SuiteFolds,
                    requested: count,
                    allowed: u64::try_from(MAX_SUITE_FOLDS).unwrap_or(u64::MAX),
                });
            }
            count
        }
    };
    let (runs, cells) = match request.design {
        crate::contracts::ResearchDesign::Batch => (
            u64::try_from(scenario_count).unwrap_or(u64::MAX),
            u64::try_from(
                checked_product(&[scenario_count, candidates, markets], "suite cells")
                    .unwrap_or(usize::MAX),
            )
            .unwrap_or(u64::MAX),
        ),
        crate::contracts::ResearchDesign::WalkForward { .. } => {
            let candidate_and_winner = candidates.saturating_add(1);
            (
                folds
                    .checked_mul(u64::try_from(scenario_count).unwrap_or(u64::MAX))
                    .and_then(|value| value.checked_mul(2))
                    .unwrap_or(u64::MAX),
                folds
                    .checked_mul(u64::try_from(scenario_count).unwrap_or(u64::MAX))
                    .and_then(|value| value.checked_mul(u64::try_from(markets).unwrap_or(u64::MAX)))
                    .and_then(|value| {
                        value.checked_mul(u64::try_from(candidate_and_winner).unwrap_or(u64::MAX))
                    })
                    .unwrap_or(u64::MAX),
            )
        }
    };
    let runs_limit = u64::try_from(MAX_SUITE_RUNS).unwrap_or(u64::MAX);
    let cells_limit = u64::try_from(MAX_SUITE_CELLS).unwrap_or(u64::MAX);
    if runs > runs_limit {
        violations.push(SuitePlanViolation {
            item: SuitePlanLimitKind::SuiteRuns,
            requested: runs,
            allowed: runs_limit,
        });
    }
    if cells > cells_limit {
        violations.push(SuitePlanViolation {
            item: SuitePlanLimitKind::SuiteCells,
            requested: cells,
            allowed: cells_limit,
        });
    }
    // Event estimate mirrors estimate_suite_events without failing on excess.
    let per_model_events = |range: UtcRange| -> Result<u64, LabError> {
        let decision_bars = u64::try_from(
            range
                .bars(request.template.decision_interval)
                .map_err(|error| LabError::InvalidConfig(format!("decision bar count: {error}")))?,
        )
        .map_err(|_| LabError::ResourceLimit("decision bar count overflow".into()))?;
        let execution_bars = u64::try_from(
            range
                .bars(request.template.execution_resolution)
                .map_err(|error| {
                    LabError::InvalidConfig(format!("execution bar count: {error}"))
                })?,
        )
        .map_err(|_| LabError::ResourceLimit("execution bar count overflow".into()))?;
        decision_bars
            .checked_mul(12)
            .and_then(|value| value.checked_add(execution_bars))
            .ok_or_else(|| LabError::ResourceLimit("model event estimate overflow".into()))
    };
    let event_limit = u64::try_from(MAX_RUN_EVENTS).unwrap_or(u64::MAX);
    let event_result = match request.design {
        crate::contracts::ResearchDesign::Batch => per_model_events(request.template.range)?
            .checked_mul(
                u64::try_from(
                    checked_product(&[candidates, markets, scenario_count], "batch models")
                        .unwrap_or(usize::MAX),
                )
                .unwrap_or(u64::MAX),
            ),
        crate::contracts::ResearchDesign::WalkForward {
            selection_bars,
            evaluation_bars,
            ..
        } => {
            // Every fold repeats the same selection/evaluation shape.
            let start = request.template.range.start();
            let interval = request.template.decision_interval.duration();
            let selection_events = per_model_events(sub_range(start, selection_bars, interval)?)?
                .checked_mul(
                    u64::try_from(
                        checked_product(&[candidates, markets, scenario_count], "selection models")
                            .unwrap_or(usize::MAX),
                    )
                    .unwrap_or(u64::MAX),
                );
            let evaluation_events = per_model_events(sub_range(start, evaluation_bars, interval)?)?
                .checked_mul(
                    u64::try_from(
                        checked_product(&[markets, scenario_count], "evaluation models")
                            .unwrap_or(usize::MAX),
                    )
                    .unwrap_or(u64::MAX),
                );
            selection_events
                .and_then(|selection| {
                    evaluation_events.and_then(|evaluation| selection.checked_add(evaluation))
                })
                .and_then(|per_fold| per_fold.checked_mul(folds))
        }
    }
    .unwrap_or(u64::MAX);
    if event_result > event_limit {
        violations.push(SuitePlanViolation {
            item: SuitePlanLimitKind::RunEvents,
            requested: event_result,
            allowed: event_limit,
        });
    }
    let admission = if violations.is_empty() {
        SuitePlanAdmission::Admitted
    } else {
        SuitePlanAdmission::Rejected {
            violations: violations.clone(),
        }
    };
    let suggestions = reduction_suggestions(request, &violations);
    let mut notes = vec![
        "planner is read-only; create additionally checks policy revisions exist and stay unchanged"
            .to_string(),
    ];
    if event_result >= event_limit {
        notes.push("event estimate saturates the aggregate bound; treat the number as an order of magnitude".into());
    }
    Ok(crate::contracts::SuitePlanReport {
        candidates: u32_count(candidates, "candidates")?,
        markets: u32_count(markets, "markets")?,
        folds: u32::try_from(folds)
            .map_err(|_| LabError::ResourceLimit("fold count overflow".into()))?,
        cost_scenarios: u32_count(scenario_count, "scenarios")?,
        planned_runs: runs,
        comparison_cells: cells,
        estimated_model_events: per_model_events(request.template.range)?,
        estimated_run_events: event_result,
        estimated_storage_bytes: event_result.saturating_mul(PLANNED_BYTES_PER_EVENT),
        admission,
        suggestions,
        notes,
    })
}

/// Range covering `bars` decision bars from `start`.
/// # Errors
/// Rejects range construction failures.
fn sub_range(
    start: crate::contracts::UtcTimestamp,
    bars: u32,
    interval: chrono::Duration,
) -> Result<UtcRange, LabError> {
    let end = add_bars(start, bars, interval)?;
    UtcRange::new(start, end)
}

fn failure_message(error: &LabError) -> String {
    error.to_string().chars().take(300).collect()
}

/// Deterministic reduction proposals for a rejected plan; every suggestion
/// states whether it changes the research meaning.
#[must_use]
fn reduction_suggestions(
    request: &ResearchSuiteRequest,
    violations: &[crate::contracts::SuitePlanViolation],
) -> Vec<String> {
    use crate::contracts::SuitePlanLimitKind;
    let mut suggestions = Vec::new();
    let markets = request.template.markets.len().max(1);
    let candidates = request.template.policy_selections.len().max(1);
    for violation in violations {
        match violation.item {
            SuitePlanLimitKind::SuiteScenarios => {
                suggestions.push(format!(
                    "reduce the cost sweep to fee_bps x slippage_bps <= {MAX_SUITE_SCENARIOS} scenarios"
                ));
                suggestions.push(
                    "warning: splitting cost scenarios changes per-candidate selection aggregates"
                        .to_string(),
                );
            }
            SuitePlanLimitKind::SuiteFolds => {
                suggestions
                    .push("increase step_bars so fewer folds cover the evaluation range".into());
                suggestions.push(
                    "warning: changing folds changes the out-of-sample partition; keep fold windows fixed once selected"
                        .to_string(),
                );
            }
            SuitePlanLimitKind::SuiteRuns => {
                suggestions.push(
                    "reduce cost scenarios or increase step_bars; runs = folds x scenarios x 2"
                        .to_string(),
                );
            }
            SuitePlanLimitKind::SuiteCells => {
                let per_market = violation
                    .requested
                    .checked_div(u64::try_from(markets).unwrap_or(u64::MAX))
                    .unwrap_or(u64::MAX);
                let max_markets = violation
                    .allowed
                    .checked_div(per_market.max(1))
                    .unwrap_or(1);
                if max_markets >= 1
                    && markets > 1
                    && max_markets < u64::try_from(markets).unwrap_or(u64::MAX)
                {
                    suggestions.push(format!(
                        "split markets into separate suites (at most {max_markets} market(s) per suite); per-market selection semantics are preserved"
                    ));
                }
                let per_candidate = violation
                    .requested
                    .checked_div(u64::try_from(candidates).unwrap_or(u64::MAX))
                    .unwrap_or(u64::MAX);
                let max_candidates = violation
                    .allowed
                    .checked_div(per_candidate.max(1))
                    .unwrap_or(1);
                if max_candidates < u64::try_from(candidates).unwrap_or(u64::MAX)
                    && max_candidates >= 1
                {
                    suggestions.push(format!(
                        "split candidates into batches of at most {max_candidates}; compare batches only on identical evaluation windows"
                    ));
                    if matches!(
                        request.design,
                        crate::contracts::ResearchDesign::WalkForward { .. }
                    ) {
                        suggestions.push(
                            "warning: candidate batches select winners independently; cross-batch ranking changes the research meaning"
                                .to_string(),
                        );
                    }
                }
                suggestions.push(
                    "reduce cost scenarios; warning: scenario splits change selection aggregates"
                        .to_string(),
                );
            }
            SuitePlanLimitKind::RunEvents => {
                suggestions.push(format!(
                    "shorten the evaluation range or coarsen decision/execution intervals; allowed aggregate events = {MAX_RUN_EVENTS}"
                ));
                suggestions.push("reduce markets, candidates or cost scenarios".into());
            }
        }
    }
    suggestions
}

/// Deterministic fold-selection decision committed before evaluation admission.
#[derive(Debug, Clone, Serialize)]
pub struct SelectionOutcome {
    pub winner: Option<PolicyRevisionRef>,
    pub selection_digest: ContentHash,
    pub unavailable: Vec<UnavailableCandidate>,
}

#[derive(Serialize)]
struct FrozenProjection<'a> {
    version: &'static str,
    request: &'a ResearchSuiteRequest,
    policy_revisions: &'a [FrozenPolicyRevision],
    geometry: &'a ResearchGeometry,
}

#[derive(Serialize)]
struct SelectionProjection<'a> {
    version: &'static str,
    suite_id: &'a SuiteId,
    fold_index: u32,
    selection_cases: Vec<(&'a SuiteCaseId, &'a RunId, u32)>,
    winner: &'a Option<PolicyRevisionRef>,
    unavailable: &'a [UnavailableCandidate],
}

#[derive(Serialize)]
struct CausalInputProjection<'a> {
    version: &'static str,
    config: CausalConfigProjection<'a>,
    observations: Vec<CausalObservation<'a>>,
    evidence: Vec<CausalEvidence>,
}

#[derive(Serialize)]
struct CausalConfigProjection<'a> {
    spec_without_input_ids: ExperimentSpec,
    policy_revisions: &'a [FrozenPolicyRevision],
}

#[derive(Serialize)]
struct CausalObservation<'a> {
    id: &'a ObservationId,
    content_digest: &'a ContentHash,
}

#[derive(Serialize)]
struct CausalEvidence {
    market: String,
    decision_time: crate::contracts::UtcTimestamp,
    eligible: Vec<CausalEligibleEvidence>,
}

#[derive(Serialize)]
struct CausalEligibleEvidence {
    effect: EligibleEvidence,
    version_digest: ContentHash,
}

#[derive(Debug)]
struct CandidateScore {
    reference: PolicyRevisionRef,
    exact_pnl_sum: Decimal,
    drawdown_sum: f64,
}

type ComparisonIndex<'a> = BTreeMap<(RunId, PolicyRevisionRef, String), &'a RunModelComparison>;

/// Validate and expand all cost scenarios, folds and joint resource bounds.
///
/// # Errors
/// Rejects non-v3/non-PIT templates, duplicate axes, invalid windows, overflow and joint limits.
pub fn expand_geometry(request: &ResearchSuiteRequest) -> Result<ResearchGeometry, LabError> {
    request.template.validate()?;
    if request.template.schema_version != "3.0"
        || request.template.causal_execution
            != Some(crate::contracts::CausalExecutionPolicy::DeclaredPolicyWarmup)
        || request.template.pit_policy != crate::contracts::PitPolicy::StrictPit
    {
        return Err(LabError::InvalidConfig(
            "research suites require v3 declared-policy warmup and STRICT_PIT".into(),
        ));
    }
    let scenarios = scenarios(request)?;
    let candidate_count = request.template.policy_selections.len();
    let market_count = request.template.markets.len();
    let scenario_count = scenarios.len();

    let (folds, runs, cells, unused_tail) = match request.design {
        ResearchDesign::Batch => (
            Vec::new(),
            scenario_count,
            checked_product(
                &[scenario_count, candidate_count, market_count],
                "suite cells",
            )?,
            None,
        ),
        ResearchDesign::WalkForward {
            selection_bars,
            evaluation_bars,
            step_bars,
            embargo_bars,
        } => {
            let (folds, unused_tail) = walk_forward_folds(
                &request.template,
                selection_bars,
                evaluation_bars,
                step_bars,
                embargo_bars,
            )?;
            let fold_count = folds.len();
            let runs = checked_product(&[2, fold_count, scenario_count], "suite runs")?;
            let candidate_and_winner = candidate_count.checked_add(1).ok_or_else(|| {
                LabError::ResourceLimit("suite comparison cell count overflow".into())
            })?;
            let cells = checked_product(
                &[
                    fold_count,
                    scenario_count,
                    market_count,
                    candidate_and_winner,
                ],
                "suite cells",
            )?;
            (folds, runs, cells, unused_tail)
        }
    };
    if runs > MAX_SUITE_RUNS {
        return Err(LabError::ResourceLimit(format!(
            "suite run count {runs} exceeds {MAX_SUITE_RUNS}"
        )));
    }
    if cells > MAX_SUITE_CELLS {
        return Err(LabError::ResourceLimit(format!(
            "suite comparison cell count {cells} exceeds {MAX_SUITE_CELLS}"
        )));
    }
    let estimated_events = estimate_suite_events(request, &folds, scenario_count)?;
    let event_limit = u64::try_from(MAX_RUN_EVENTS)
        .map_err(|_| LabError::ResourceLimit("run event limit is not representable".into()))?;
    if estimated_events > event_limit {
        return Err(LabError::ResourceLimit(format!(
            "aggregate suite event estimate {estimated_events} exceeds {event_limit}"
        )));
    }
    Ok(ResearchGeometry {
        scenarios,
        folds,
        planned_runs: u32_count(runs, "suite runs")?,
        planned_comparison_cells: u32_count(cells, "suite cells")?,
        estimated_events,
        unused_tail,
    })
}

/// Freeze request, exact policy bodies and expanded geometry under one stable identity.
///
/// # Errors
/// Rejects missing/reordered/altered policy bodies and any geometry preflight failure.
pub fn freeze(
    request: ResearchSuiteRequest,
    policy_revisions: Vec<FrozenPolicyRevision>,
) -> Result<FrozenResearchSuite, LabError> {
    let geometry = expand_geometry(&request)?;
    let _config_digest = experiment_config_digest(&request.template, &policy_revisions)?;
    if policy_revisions.len() != request.template.policy_selections.len() {
        return Err(LabError::InputHashMismatch(
            "suite policy body count differs from candidate count".into(),
        ));
    }
    for (reference, policy) in request
        .template
        .policy_selections
        .iter()
        .zip(&policy_revisions)
    {
        validate_frozen_policy(policy)?;
        if reference != &policy.reference {
            return Err(LabError::InputHashMismatch(
                "suite policy bodies must match candidate order exactly".into(),
            ));
        }
    }
    let input_digest = ContentHash::of_value(&FrozenProjection {
        version: "research-suite-v1",
        request: &request,
        policy_revisions: &policy_revisions,
        geometry: &geometry,
    })?;
    Ok(FrozenResearchSuite {
        id: SuiteId::from_seed(input_digest.as_str()),
        request,
        input_digest,
        policy_revisions,
        scenarios: geometry.scenarios,
        folds: geometry.folds,
        planned_runs: geometry.planned_runs,
        planned_comparison_cells: geometry.planned_comparison_cells,
        estimated_events: geometry.estimated_events,
        unused_tail: geometry.unused_tail,
    })
}

/// Hash only the v3 causal observation windows and PIT-eligible evidence inputs.
///
/// Appending or changing observations/evidence outside those windows cannot alter this digest.
/// Legacy v1/v2 plans retain their existing full-plan input digest unchanged.
/// # Errors
/// Rejects non-v3 plans, missing frozen policies, corrupt evidence or conflicting observations.
#[expect(
    clippy::too_many_lines,
    reason = "one audit path binds declared windows and PIT evidence into a single causal identity"
)]
pub fn causal_input_digest(
    plan: &ResolvedPlan,
    datasets: &[DatasetSnapshot],
    evidence: Option<&EvidenceSnapshot>,
) -> Result<ContentHash, LabError> {
    if plan.spec.schema_version != "3.0" && plan.spec.causal_execution.is_none() {
        return Ok(plan.input_digest.clone());
    }
    if plan.spec.schema_version != "3.0"
        || plan.spec.causal_execution
            != Some(crate::contracts::CausalExecutionPolicy::DeclaredPolicyWarmup)
        || plan.spec.pit_policy != crate::contracts::PitPolicy::StrictPit
    {
        return Err(LabError::InvalidConfig(
            "causal input digest requires a v3 STRICT_PIT plan".into(),
        ));
    }
    let mut causal_spec = plan.spec.clone();
    causal_spec.dataset_ids.clear();
    causal_spec.evidence_snapshot_id = None;
    let evaluator = EvidenceEvaluator::new(evidence, plan.spec.pit_policy)?;
    let zero_weight = Weight::new(Decimal::ZERO)?;
    let mut observations = BTreeMap::<&ObservationId, &ContentHash>::new();
    let mut evidence_points = BTreeMap::new();
    for admission in &plan.admissions {
        let market_code = admission.market.code();
        let binding = strategy_binding(plan, admission)?;
        let warmup = u32::try_from(binding.warmup_bars(plan)?)
            .map_err(|_| LabError::ResourceLimit("policy warmup exceeds u32".into()))?;
        let decision_range = plan
            .spec
            .range
            .with_warmup(warmup, plan.spec.decision_interval)?;
        let execution_range = plan
            .spec
            .range
            .with_warmup(1, plan.spec.execution_resolution)?;
        let requires_evidence = binding
            .policy_ref()
            .and_then(|reference| {
                plan.policy_revisions
                    .iter()
                    .find(|policy| policy.reference == *reference)
            })
            .is_some_and(|policy| policy.definition.requires_evidence());
        for dataset in datasets {
            for observation in &dataset.observations {
                if observation.candle.market != market_code.as_str() {
                    continue;
                }
                let in_decision = observation.candle.interval == plan.spec.decision_interval
                    && decision_range.contains(observation.candle.open_time_utc)
                    && observation.candle.close_time_utc < plan.spec.range.end();
                let in_execution = observation.candle.interval == plan.spec.execution_resolution
                    && execution_range.contains(observation.candle.open_time_utc);
                if !in_decision && !in_execution {
                    continue;
                }
                match observations.insert(&observation.id, &observation.content_digest) {
                    Some(existing) if existing != &observation.content_digest => {
                        return Err(LabError::InputHashMismatch(
                            "causal observation ID has conflicting content".into(),
                        ));
                    }
                    _ => {}
                }
                if requires_evidence && in_decision {
                    evidence_points.insert(
                        (market_code.clone(), observation.candle.close_time_utc),
                        admission.market.clone(),
                    );
                }
            }
        }
    }
    let observations = observations
        .iter()
        .map(|(id, content_digest)| CausalObservation { id, content_digest })
        .collect();
    let versions = evidence
        .map(|snapshot| {
            snapshot
                .versions
                .iter()
                .map(|version| (&version.revision_id, version))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut causal_evidence = Vec::with_capacity(evidence_points.len());
    for ((market_code, decision_time), market) in evidence_points {
        let effect = evaluator.effect(&market, decision_time, zero_weight, false)?;
        let eligible = effect
            .eligible
            .into_iter()
            .map(|eligible| {
                let version = versions.get(&eligible.revision_id).ok_or_else(|| {
                    LabError::InputHashMismatch(
                        "eligible evidence revision is absent from frozen snapshot".into(),
                    )
                })?;
                Ok(CausalEligibleEvidence {
                    version_digest: ContentHash::of_value(*version)?,
                    effect: eligible,
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        causal_evidence.push(CausalEvidence {
            market: market_code,
            decision_time,
            eligible,
        });
    }
    ContentHash::of_value(&CausalInputProjection {
        version: "causal-input-v1",
        config: CausalConfigProjection {
            spec_without_input_ids: causal_spec,
            policy_revisions: &plan.policy_revisions,
        },
        observations,
        evidence: causal_evidence,
    })
}

/// Materialize every planned run with stable IDs; evaluation cases remain gated by a winner.
///
/// # Errors
/// Rejects corrupt frozen counts or unrepresentable indexes.
pub fn initial_cases(frozen: &FrozenResearchSuite) -> Result<Vec<SuiteCase>, LabError> {
    let mut cases =
        Vec::with_capacity(usize::try_from(frozen.planned_runs).map_err(|_| {
            LabError::ResourceLimit("planned run count is not representable".into())
        })?);
    match frozen.request.design {
        ResearchDesign::Batch => {
            for scenario_index in 0..frozen.scenarios.len() {
                push_case(
                    &mut cases,
                    frozen,
                    None,
                    SuitePhase::Batch,
                    scenario_index,
                    frozen.request.template.range,
                )?;
            }
        }
        ResearchDesign::WalkForward { .. } => {
            for fold in &frozen.folds {
                for scenario_index in 0..frozen.scenarios.len() {
                    push_case(
                        &mut cases,
                        frozen,
                        Some(fold.index),
                        SuitePhase::Selection,
                        scenario_index,
                        fold.selection_range,
                    )?;
                }
                for scenario_index in 0..frozen.scenarios.len() {
                    push_case(
                        &mut cases,
                        frozen,
                        Some(fold.index),
                        SuitePhase::Evaluation,
                        scenario_index,
                        fold.evaluation_range,
                    )?;
                }
            }
        }
    }
    if cases.len()
        != usize::try_from(frozen.planned_runs)
            .map_err(|_| LabError::ResourceLimit("planned run count is not representable".into()))?
    {
        return Err(LabError::InputHashMismatch(
            "frozen suite run count disagrees with case geometry".into(),
        ));
    }
    Ok(cases)
}

/// Derive an ordinary experiment spec for a planned case.
///
/// Selection cases retain every frozen candidate; evaluation cases use only the committed winner.
/// # Errors
/// Rejects foreign/corrupt cases, absent winners and invalid derived specs.
pub fn case_spec(record: &SuiteRecord, case: &SuiteCase) -> Result<ExperimentSpec, LabError> {
    if case.suite_id != record.frozen.id {
        return Err(LabError::Conflict(
            "suite case belongs to another suite".into(),
        ));
    }
    let scenario =
        record
            .frozen
            .scenarios
            .get(usize::try_from(case.scenario_index).map_err(|_| {
                LabError::ResourceLimit("scenario index is not representable".into())
            })?)
            .ok_or_else(|| LabError::InputHashMismatch("suite case scenario is missing".into()))?;
    validate_case_geometry(record, case)?;
    let mut spec = record.frozen.request.template.clone();
    spec.range = case.range;
    spec.costs = scenario.clone();
    match case.phase {
        SuitePhase::Batch | SuitePhase::Selection => {}
        SuitePhase::Evaluation => {
            let fold = fold(record, case.fold_index)?;
            let winner = fold.winner.clone().ok_or_else(|| {
                LabError::Conflict("evaluation cannot be admitted before winner commit".into())
            })?;
            if fold.selection_digest.is_none() {
                return Err(LabError::Conflict(
                    "evaluation cannot be admitted before selection digest commit".into(),
                ));
            }
            spec.policy_selections = vec![winner];
        }
    }
    spec.validate()?;
    Ok(spec)
}

/// Rank one fold from its completed training comparisons.
///
/// # Errors
/// Rejects incomplete/foreign/duplicate cells and exact financial inconsistencies.
pub fn select_winner(
    record: &SuiteRecord,
    fold_index: u32,
    rows: &[RunModelComparison],
) -> Result<SelectionOutcome, LabError> {
    let fold = record
        .folds
        .iter()
        .find(|fold| fold.index == fold_index)
        .ok_or_else(|| LabError::InvalidConfig("unknown walk-forward fold".into()))?;
    if fold.winner.is_some() || fold.selection_digest.is_some() {
        return Err(LabError::Conflict(
            "walk-forward fold selection is already committed".into(),
        ));
    }
    let selection_cases = selection_cases(record, fold_index)?;
    let indexed = index_training_rows(record, &selection_cases, rows)?;

    let mut scores = Vec::new();
    let mut unavailable = Vec::new();
    for candidate in &record.frozen.request.template.policy_selections {
        match candidate_score(record, candidate, &selection_cases, &indexed) {
            Ok(score) => scores.push(score),
            Err(reason) => unavailable.push(UnavailableCandidate {
                candidate: candidate.clone(),
                reason,
            }),
        }
    }
    scores.sort_by(|left, right| {
        right
            .exact_pnl_sum
            .cmp(&left.exact_pnl_sum)
            .then_with(|| left.drawdown_sum.total_cmp(&right.drawdown_sum))
            .then_with(|| left.reference.policy_id.cmp(&right.reference.policy_id))
            .then_with(|| left.reference.revision_id.cmp(&right.reference.revision_id))
    });
    let winner = scores.first().map(|score| score.reference.clone());
    let selection_cases = selection_cases
        .iter()
        .map(|case| {
            Ok((
                &case.id,
                case.run_id.as_ref().ok_or_else(|| {
                    LabError::Conflict("completed selection case lacks run".into())
                })?,
                case.scenario_index,
            ))
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let selection_digest = ContentHash::of_value(&SelectionProjection {
        version: "research-selection-v1",
        suite_id: &record.frozen.id,
        fold_index,
        selection_cases,
        winner: &winner,
        unavailable: &unavailable,
    })?;
    Ok(SelectionOutcome {
        winner,
        selection_digest,
        unavailable,
    })
}

/// Present bounded suite progress without changing durable state.
#[must_use]
pub fn summarize(record: &SuiteRecord) -> SuiteSummary {
    let completed_runs = record
        .cases
        .iter()
        .filter(|case| case.status == SuiteCaseStatus::Completed)
        .count();
    let blocked_or_failed_runs = record
        .cases
        .iter()
        .filter(|case| {
            matches!(
                case.status,
                SuiteCaseStatus::Blocked
                    | SuiteCaseStatus::Failed
                    | SuiteCaseStatus::Cancelled
                    | SuiteCaseStatus::Interrupted
            )
        })
        .count();
    SuiteSummary {
        id: record.frozen.id.clone(),
        request_id: record.frozen.request.request_id.clone(),
        input_digest: record.frozen.input_digest.clone(),
        design: record.frozen.request.design.clone(),
        status: record.status,
        failure: record.failure.clone(),
        created_at: record.created_at,
        planned_runs: record.frozen.planned_runs,
        candidate_refs: record.frozen.request.template.policy_selections.clone(),
        cost_scenarios: record.frozen.scenarios.clone(),
        completed_runs: u32::try_from(completed_runs).unwrap_or(u32::MAX),
        blocked_or_failed_runs: u32::try_from(blocked_or_failed_runs).unwrap_or(u32::MAX),
        selected_folds: record
            .folds
            .iter()
            .filter(|fold| fold.selection_digest.is_some())
            .cloned()
            .collect(),
        unused_tail: record.frozen.unused_tail,
    }
}

fn scenarios(request: &ResearchSuiteRequest) -> Result<Vec<CostPolicy>, LabError> {
    if request.template.costs.dynamic.is_some() {
        // The engine derives slippage from the dynamic model, so swept
        // slippage_bps values would produce identical scenarios that only
        // differ in their label — a sweep must not pretend to vary cost.
        return Err(LabError::InvalidConfig(
            "cost sweep is not supported with a dynamic cost model; sweep the fixed model or omit the sweep"
                .into(),
        ));
    }
    let fees = &request.cost_sweep.fee_bps;
    let slippages = &request.cost_sweep.slippage_bps;
    if fees.is_empty() || slippages.is_empty() {
        return Err(LabError::InvalidConfig(
            "cost sweep axes must both be nonempty".into(),
        ));
    }
    if fees
        .iter()
        .map(|value| value.get())
        .collect::<BTreeSet<_>>()
        .len()
        != fees.len()
        || slippages
            .iter()
            .map(|value| value.get())
            .collect::<BTreeSet<_>>()
            .len()
            != slippages.len()
    {
        return Err(LabError::InvalidConfig(
            "cost sweep axes must not contain duplicates".into(),
        ));
    }
    let count = checked_product(&[fees.len(), slippages.len()], "suite scenarios")?;
    if count > MAX_SUITE_SCENARIOS {
        return Err(LabError::ResourceLimit(format!(
            "suite scenario count {count} exceeds {MAX_SUITE_SCENARIOS}"
        )));
    }
    let mut scenarios = Vec::with_capacity(count);
    for fee in fees {
        for slippage in slippages {
            let mut costs = request.template.costs.clone();
            costs.buy_fee_bps = *fee;
            costs.sell_fee_bps = *fee;
            costs.maker_fee_bps = *fee;
            costs.slippage_bps = *slippage;
            costs.assumption_label = format!(
                "research_suite fee_bps={} slippage_bps={}; {}",
                fee.get(),
                slippage.get(),
                request.template.costs.assumption_label
            );
            let mut spec = request.template.clone();
            spec.costs = costs.clone();
            spec.validate()?;
            scenarios.push(costs);
        }
    }
    Ok(scenarios)
}

fn walk_forward_folds(
    template: &ExperimentSpec,
    selection_bars: u32,
    evaluation_bars: u32,
    step_bars: u32,
    embargo_bars: u32,
) -> Result<(Vec<crate::contracts::WalkForwardFold>, Option<UtcRange>), LabError> {
    if selection_bars == 0 || evaluation_bars == 0 || step_bars < evaluation_bars {
        return Err(LabError::InvalidConfig(
            "walk-forward requires positive selection/evaluation and step >= evaluation bars"
                .into(),
        ));
    }
    let interval = template.decision_interval;
    let duration = interval.duration();
    let mut folds = Vec::new();
    let mut selection_start = template.range.start();
    loop {
        let selection_end = add_bars(selection_start, selection_bars, duration)?;
        let evaluation_start = add_bars(selection_end, embargo_bars, duration)?;
        let evaluation_end = add_bars(evaluation_start, evaluation_bars, duration)?;
        if evaluation_end > template.range.end() {
            break;
        }
        let index = u32_count(folds.len(), "walk-forward folds")?;
        folds.push(crate::contracts::WalkForwardFold {
            index,
            selection_range: UtcRange::new(selection_start, selection_end)?,
            evaluation_range: UtcRange::new(evaluation_start, evaluation_end)?,
            winner: None,
            selection_digest: None,
            unavailable_candidates: Vec::new(),
        });
        if folds.len() > MAX_SUITE_FOLDS {
            return Err(LabError::ResourceLimit(format!(
                "walk-forward fold count exceeds {MAX_SUITE_FOLDS}"
            )));
        }
        selection_start = add_bars(selection_start, step_bars, duration)?;
    }
    let last_end = folds
        .last()
        .map(|fold| fold.evaluation_range.end())
        .ok_or_else(|| {
            LabError::InvalidConfig("walk-forward range yields no complete fold".into())
        })?;
    let unused_tail = (last_end < template.range.end())
        .then(|| UtcRange::new(last_end, template.range.end()))
        .transpose()?;
    Ok((folds, unused_tail))
}

fn estimate_suite_events(
    request: &ResearchSuiteRequest,
    folds: &[crate::contracts::WalkForwardFold],
    scenario_count: usize,
) -> Result<u64, LabError> {
    let candidates = request.template.policy_selections.len();
    let markets = request.template.markets.len();
    match request.design {
        ResearchDesign::Batch => run_events(
            request.template.range,
            &request.template,
            checked_product(&[candidates, markets, scenario_count], "batch models")?,
        ),
        ResearchDesign::WalkForward { .. } => folds.iter().try_fold(0_u64, |total, fold| {
            let selection_models =
                checked_product(&[candidates, markets, scenario_count], "selection models")?;
            let evaluation_models =
                checked_product(&[markets, scenario_count], "evaluation models")?;
            let selection_events =
                run_events(fold.selection_range, &request.template, selection_models)?;
            let evaluation_events =
                run_events(fold.evaluation_range, &request.template, evaluation_models)?;
            total
                .checked_add(selection_events)
                .and_then(|value| value.checked_add(evaluation_events))
                .ok_or_else(|| LabError::ResourceLimit("suite event estimate overflow".into()))
        }),
    }
}

fn run_events(
    range: UtcRange,
    template: &ExperimentSpec,
    model_runs: usize,
) -> Result<u64, LabError> {
    let decision_bars = u64::try_from(range.bars(template.decision_interval)?)
        .map_err(|_| LabError::ResourceLimit("decision bar count overflow".into()))?;
    let execution_bars = u64::try_from(range.bars(template.execution_resolution)?)
        .map_err(|_| LabError::ResourceLimit("execution bar count overflow".into()))?;
    let per_model = decision_bars
        .checked_mul(12)
        .and_then(|value| value.checked_add(execution_bars))
        .ok_or_else(|| LabError::ResourceLimit("model event estimate overflow".into()))?;
    per_model
        .checked_mul(
            u64::try_from(model_runs)
                .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?,
        )
        .ok_or_else(|| LabError::ResourceLimit("suite event estimate overflow".into()))
}

fn add_bars(
    start: crate::contracts::UtcTimestamp,
    bars: u32,
    duration: Duration,
) -> Result<crate::contracts::UtcTimestamp, LabError> {
    let seconds = duration
        .num_seconds()
        .checked_mul(i64::from(bars))
        .ok_or_else(|| LabError::ResourceLimit("walk-forward duration overflow".into()))?;
    start
        .0
        .checked_add_signed(Duration::seconds(seconds))
        .map(crate::contracts::UtcTimestamp)
        .ok_or_else(|| LabError::ResourceLimit("walk-forward timestamp overflow".into()))
}

fn push_case(
    cases: &mut Vec<SuiteCase>,
    frozen: &FrozenResearchSuite,
    fold_index: Option<u32>,
    phase: SuitePhase,
    scenario_index: usize,
    range: UtcRange,
) -> Result<(), LabError> {
    let index = u32_count(cases.len(), "suite case index")?;
    let scenario_index = u32_count(scenario_index, "scenario index")?;
    let phase_name = match phase {
        SuitePhase::Batch => "batch",
        SuitePhase::Selection => "selection",
        SuitePhase::Evaluation => "evaluation",
    };
    let id = SuiteCaseId::from_seed(&format!(
        "{}:{index}:{fold_index:?}:{phase_name}:{scenario_index}:{}:{}",
        frozen.id,
        range.start(),
        range.end()
    ));
    cases.push(SuiteCase {
        id,
        suite_id: frozen.id.clone(),
        index,
        fold_index,
        phase,
        scenario_index,
        range,
        status: SuiteCaseStatus::Planned,
        plan_id: None,
        job_id: None,
        attempt_id: None,
        run_id: None,
        causal_input_digest: None,
        failure: None,
    });
    Ok(())
}

fn validate_case_geometry(record: &SuiteRecord, case: &SuiteCase) -> Result<(), LabError> {
    let expected = match case.phase {
        SuitePhase::Batch => {
            if case.fold_index.is_some() {
                return Err(LabError::InputHashMismatch(
                    "batch suite case must not reference a fold".into(),
                ));
            }
            record.frozen.request.template.range
        }
        SuitePhase::Selection => fold(record, case.fold_index)?.selection_range,
        SuitePhase::Evaluation => fold(record, case.fold_index)?.evaluation_range,
    };
    if case.range != expected {
        return Err(LabError::InputHashMismatch(
            "suite case range differs from frozen geometry".into(),
        ));
    }
    Ok(())
}

fn fold(
    record: &SuiteRecord,
    fold_index: Option<u32>,
) -> Result<&crate::contracts::WalkForwardFold, LabError> {
    let index = fold_index
        .ok_or_else(|| LabError::InputHashMismatch("walk-forward case lacks fold index".into()))?;
    record
        .folds
        .iter()
        .find(|fold| fold.index == index)
        .ok_or_else(|| {
            LabError::InputHashMismatch("walk-forward case references unknown fold".into())
        })
}

fn candidate_score(
    record: &SuiteRecord,
    candidate: &PolicyRevisionRef,
    cases: &[&SuiteCase],
    rows: &ComparisonIndex<'_>,
) -> Result<CandidateScore, String> {
    let mut exact_pnl_sum = Decimal::ZERO;
    let mut drawdown_sum = 0.0_f64;
    for case in cases {
        let run_id = case
            .run_id
            .as_ref()
            .ok_or_else(|| "selection case has no run".to_owned())?;
        for market in &record.frozen.request.template.markets {
            let row = rows
                .get(&(run_id.clone(), candidate.clone(), market.code()))
                .ok_or_else(|| format!("missing completed cell for {market}"))?;
            if row.status != ModelStatus::Completed {
                return Err(format!("cell status is {:?}", row.status));
            }
            let _reported_return = row
                .net_return
                .value
                .filter(|value| value.is_finite())
                .ok_or_else(|| "net return is unavailable".to_owned())?;
            let drawdown = row
                .max_drawdown
                .value
                .filter(|value| value.is_finite())
                .ok_or_else(|| "drawdown is unavailable".to_owned())?;
            let initial = row
                .initial_equity
                .ok_or_else(|| "initial equity is unavailable".to_owned())?;
            let final_equity = row
                .final_equity
                .ok_or_else(|| "final equity is unavailable".to_owned())?;
            let net_pnl = row
                .net_pnl
                .ok_or_else(|| "net PnL is unavailable".to_owned())?;
            if initial != record.frozen.request.template.initial_cash
                || initial.get() <= Decimal::ZERO
            {
                return Err("cell initial equity differs from frozen initial cash".into());
            }
            let expected_final = initial
                .get()
                .checked_add(net_pnl.get())
                .ok_or_else(|| "final equity arithmetic overflow".to_owned())?;
            if expected_final != final_equity.get() {
                return Err("exact final equity does not equal initial equity plus net PnL".into());
            }
            exact_pnl_sum = exact_pnl_sum
                .checked_add(net_pnl.get())
                .ok_or_else(|| "exact PnL sum overflow".to_owned())?;
            drawdown_sum += drawdown;
            if !drawdown_sum.is_finite() {
                return Err("drawdown sum is non-finite".into());
            }
        }
    }
    Ok(CandidateScore {
        reference: candidate.clone(),
        exact_pnl_sum,
        drawdown_sum,
    })
}

fn selection_cases(record: &SuiteRecord, fold_index: u32) -> Result<Vec<&SuiteCase>, LabError> {
    let mut cases = record
        .cases
        .iter()
        .filter(|case| case.fold_index == Some(fold_index) && case.phase == SuitePhase::Selection)
        .collect::<Vec<_>>();
    cases.sort_by_key(|case| (case.scenario_index, case.index));
    if cases.len() != record.frozen.scenarios.len()
        || cases.iter().any(|case| {
            !matches!(
                case.status,
                SuiteCaseStatus::Completed | SuiteCaseStatus::Blocked
            ) || case.run_id.is_none()
        })
    {
        return Err(LabError::Conflict(
            "all fold selection cases must have a terminal published run before ranking".into(),
        ));
    }
    let scenario_indexes = cases
        .iter()
        .map(|case| case.scenario_index)
        .collect::<BTreeSet<_>>();
    if scenario_indexes.len() != record.frozen.scenarios.len()
        || scenario_indexes
            .iter()
            .copied()
            .enumerate()
            .any(|(expected, actual)| u32::try_from(expected).ok() != Some(actual))
    {
        return Err(LabError::InputHashMismatch(
            "fold selection cases do not cover each frozen scenario exactly once".into(),
        ));
    }
    let run_ids = cases
        .iter()
        .filter_map(|case| case.run_id.as_ref())
        .collect::<BTreeSet<_>>();
    if run_ids.len() != cases.len() {
        return Err(LabError::InputHashMismatch(
            "fold selection cases must reference distinct runs".into(),
        ));
    }
    Ok(cases)
}

fn index_training_rows<'a>(
    record: &SuiteRecord,
    cases: &[&SuiteCase],
    rows: &'a [RunModelComparison],
) -> Result<ComparisonIndex<'a>, LabError> {
    let expected_runs = cases
        .iter()
        .filter_map(|case| case.run_id.as_ref())
        .collect::<BTreeSet<_>>();
    let candidate_set = record
        .frozen
        .request
        .template
        .policy_selections
        .iter()
        .collect::<BTreeSet<_>>();
    let market_set = record
        .frozen
        .request
        .template
        .markets
        .iter()
        .map(crate::contracts::MarketId::code)
        .collect::<BTreeSet<_>>();
    let mut indexed = BTreeMap::new();
    for row in rows {
        let Some(reference) = row.policy_ref.as_ref() else {
            return Err(LabError::InputHashMismatch(
                "training comparison lacks exact policy reference".into(),
            ));
        };
        if !expected_runs.contains(&row.run_id)
            || !candidate_set.contains(reference)
            || !market_set.contains(&row.market.code())
        {
            return Err(LabError::InputHashMismatch(
                "training comparison lies outside the frozen fold geometry".into(),
            ));
        }
        let key = (row.run_id.clone(), reference.clone(), row.market.code());
        if indexed.insert(key, row).is_some() {
            return Err(LabError::InputHashMismatch(
                "duplicate training comparison cell".into(),
            ));
        }
    }
    Ok(indexed)
}

fn checked_product(values: &[usize], label: &str) -> Result<usize, LabError> {
    values.iter().try_fold(1_usize, |product, value| {
        product
            .checked_mul(*value)
            .ok_or_else(|| LabError::ResourceLimit(format!("{label} overflow")))
    })
}

fn u32_count(value: usize, label: &str) -> Result<u32, LabError> {
    u32::try_from(value).map_err(|_| LabError::ResourceLimit(format!("{label} exceeds u32")))
}
