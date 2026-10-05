//! Typed dataset admission and frozen input planning; no strategy interpretation.

use crate::contracts::{
    AdmissionStatus, ContentHash, DatasetId, DatasetSnapshot, EvidenceSnapshot, ExperimentSpec,
    FrozenPolicyRevision, LabError, LimitItem, LimitReport, MAX_DATASET_ROWS, MAX_MODEL_EVENTS,
    MAX_RUN_EVENTS, MarketId, ModelAdmission, ModelId, PlanId, PlanRequest, PolicyRevisionRef,
    QualitySeverity, RequestSize, ResolvedPlan, RuleProvenance, StrategyKind, StrategySpec,
    UtcRange, UtcTimestamp, Weight, active_limits, evaluation_days_ceil, experiment_config_digest,
};
use crate::database::DatabaseHandle;
use crate::evidence::EvidenceEvaluator;
use crate::storage::{dataset_digests, dataset_id};
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Serialize)]
struct InputProjection<'a> {
    config_digest: &'a ContentHash,
    dataset_digests: &'a [(DatasetId, ContentHash)],
    evidence_digest: &'a Option<ContentHash>,
}

/// Read and verify frozen input objects before exposing an executable plan.
/// # Errors
/// Rejects missing/corrupt snapshots or invalid configuration without altering inputs.
pub async fn prepare(
    database: &DatabaseHandle,
    request: PlanRequest,
) -> Result<ResolvedPlan, LabError> {
    Ok(prepare_with_causal_digest(database, request).await?.0)
}

/// Freeze the plan and its actual causal-use witness from one verified input load.
/// # Errors
/// Preserves ordinary planning errors and rejects invalid causal input contracts.
pub(crate) async fn prepare_with_causal_digest(
    database: &DatabaseHandle,
    request: PlanRequest,
) -> Result<(ResolvedPlan, ContentHash), LabError> {
    request.spec.validate()?;
    let (datasets, evidence) = load_inputs(database, &request.spec).await?;
    let references = request.spec.policy_selections.clone();
    let policies = database
        .call("load_policy_selections", move |store| {
            references
                .iter()
                .map(|reference| {
                    store
                        .load_policy_revision(reference)?
                        .map(|revision| revision.snapshot)
                        .ok_or_else(|| LabError::InvalidConfig("unknown policy revision".into()))
                })
                .collect::<Result<Vec<_>, LabError>>()
        })
        .await?;
    let plan = resolve_with_policies(&request.spec, &datasets, evidence.as_ref(), &policies)?;
    let causal_digest = crate::research::causal_input_digest(&plan, &datasets, evidence.as_ref())?;
    let saved = plan.clone();
    database
        .call("save_plan", move |store| store.save_plan(&request, &saved))
        .await?;
    Ok((plan, causal_digest))
}

/// Load immutable datasets and evidence, verifying bytes and economic identities.
/// # Errors
/// Returns invalid-input, corrupt-data or persistence errors.
pub async fn load_inputs(
    database: &DatabaseHandle,
    spec: &ExperimentSpec,
) -> Result<(Vec<DatasetSnapshot>, Option<EvidenceSnapshot>), LabError> {
    let ids = spec.dataset_ids.clone();
    let evidence_id = spec.evidence_snapshot_id.clone();
    database
        .call("load_frozen_inputs", move |store| {
            let mut datasets = Vec::with_capacity(ids.len());
            for id in ids {
                let dataset = store
                    .load_dataset(&id)?
                    .ok_or_else(|| LabError::InvalidConfig(format!("unknown dataset {id}")))?;
                let digests = dataset_digests(&dataset)?;
                if dataset.manifest.semantic_digest != digests.semantic
                    || dataset.manifest.provenance_digest != digests.provenance
                    || dataset.manifest.id != dataset_id(&digests)
                {
                    return Err(LabError::InputHashMismatch(format!(
                        "dataset {id} identity mismatch"
                    )));
                }
                for raw in &dataset.manifest.raw_objects {
                    let _verified = store.raw_objects().read_verified(raw)?;
                }
                datasets.push(dataset);
            }
            let evidence = evidence_id
                .map(|id| {
                    store.load_evidence_snapshot(&id)?.ok_or_else(|| {
                        LabError::InvalidConfig(format!("unknown evidence snapshot {id}"))
                    })
                })
                .transpose()?;
            Ok((datasets, evidence))
        })
        .await
}

/// Resolve exact frozen policy bodies and inputs without accessing mutable registry heads.
/// # Errors
/// Rejects altered definitions, missing selections, or invalid data closure.
pub fn resolve_with_policies(
    spec: &ExperimentSpec,
    datasets: &[DatasetSnapshot],
    evidence: Option<&EvidenceSnapshot>,
    policies: &[FrozenPolicyRevision],
) -> Result<ResolvedPlan, LabError> {
    validate_input_closure(spec, datasets, evidence)?;
    let config_digest = experiment_config_digest(spec, policies)?;
    let selections = planning_selections(spec, policies)?;
    let dataset_hashes = datasets
        .iter()
        .map(|d| (d.manifest.id.clone(), d.manifest.semantic_digest.clone()))
        .collect::<Vec<_>>();
    let evidence_digest = evidence.map(|e| e.digest.clone());
    let input_digest = ContentHash::of_value(&InputProjection {
        config_digest: &config_digest,
        dataset_digests: &dataset_hashes,
        evidence_digest: &evidence_digest,
    })?;
    let evaluator = EvidenceEvaluator::new(evidence, spec.pit_policy)?;
    let mut admissions = Vec::new();
    let mut estimated_events = 0_u64;
    let mut max_model_events = 0_u64;
    for market in &spec.markets {
        for selection in &selections {
            let model_id = match &selection.reference {
                Some(reference) => ModelId::from_seed(&serde_json::to_string(&(
                    market,
                    reference,
                    &config_digest,
                ))?),
                None => ModelId::from_seed(&serde_json::to_string(&(
                    market,
                    selection.legacy,
                    &config_digest,
                ))?),
            };
            let mut reasons = data_reasons(spec, selection.warmup, market, datasets)?;
            let evidence_strategy = selection.requires_evidence;
            let mut status = if reasons.is_empty() {
                AdmissionStatus::Eligible
            } else {
                AdmissionStatus::BlockedData
            };
            if status == AdmissionStatus::Eligible
                && evidence_strategy
                && !has_coverage(spec, market, &evaluator)?
            {
                status = AdmissionStatus::BlockedEvidence;
                reasons.push(
                    "no time-eligible STRATEGY_INPUT Evidence; performance remains null".into(),
                );
            }
            if status == AdmissionStatus::Eligible {
                let estimated = estimate_model_events(spec)?;
                max_model_events = max_model_events.max(estimated);
                estimated_events = estimated_events
                    .checked_add(estimated)
                    .ok_or_else(|| LabError::ResourceLimit("run event estimate overflow".into()))?;
            }
            admissions.push(ModelAdmission {
                model_id,
                market: market.clone(),
                strategy: selection.family,
                policy_ref: selection.reference.cloned(),
                status,
                reasons,
            });
        }
    }
    validate_run_budget(spec, admissions.len(), max_model_events, estimated_events)?;
    let warnings = base_plan_warnings(spec);
    Ok(ResolvedPlan {
        id: PlanId::from_seed(input_digest.as_str()),
        spec: spec.clone(),
        config_digest,
        input_digest,
        dataset_digests: dataset_hashes,
        evidence_digest,
        admissions,
        policy_revisions: policies.to_vec(),
        warnings,
        estimated_events,
    })
}

/// Bounded, deterministic plan warnings; the shared-capital note stays until a
/// native portfolio surface replaces the independent-account default.
fn base_plan_warnings(spec: &ExperimentSpec) -> Vec<String> {
    let mut warnings = vec![
        "HISTORICAL_REPLAY / BAR_CLOSE_ASSUMED; all fills are SIMULATED_ONLY".into(),
        "independent cash account for each asset-strategy; not a shared-capital portfolio".into(),
    ];
    if spec.market_rules_history.is_empty() {
        if spec.market_rules.provenance == RuleProvenance::ExplicitScenario {
            warnings.push(
                "UNVERIFIED_HISTORICAL_RULES: explicit rule scenario, not verified historical rules"
                    .into(),
            );
        }
    } else {
        let segments = spec.market_rules_history.len();
        let explicit = spec
            .market_rules_history
            .iter()
            .filter(|segment| segment.provenance == RuleProvenance::ExplicitScenario)
            .count();
        if explicit > 0 {
            warnings.push(format!(
                "UNVERIFIED_HISTORICAL_RULES: {explicit}/{segments} rule segments are explicit scenarios, not verified historical rules"
            ));
        }
    }
    warnings
}

fn validate_run_budget(
    spec: &ExperimentSpec,
    model_count: usize,
    max_model_events: u64,
    estimated_events: u64,
) -> Result<(), LabError> {
    let allowed = u64::try_from(MAX_RUN_EVENTS)
        .map_err(|_| LabError::ResourceLimit("run limit overflow".into()))?;
    if estimated_events <= allowed {
        return Ok(());
    }
    Err(LabError::RequestLimit(Box::new(LimitReport::exceeded(
        request_size(spec, model_count, max_model_events, Some(estimated_events))?,
        LimitItem::RunEvents,
        estimated_events,
        allowed,
        &[
            "reduce markets or policy/strategy selections",
            "shorten the evaluation range",
            "use coarser decision or execution intervals",
        ],
    ))))
}

fn estimate_model_events(spec: &ExperimentSpec) -> Result<u64, LabError> {
    let estimated = model_event_count(spec)?;
    if estimated
        > u64::try_from(MAX_MODEL_EVENTS)
            .map_err(|_| LabError::ResourceLimit("event limit overflow".into()))?
    {
        let selection_count = spec.strategies.len() + spec.policy_selections.len();
        let model_count = spec
            .markets
            .len()
            .checked_mul(selection_count)
            .ok_or_else(|| LabError::ResourceLimit("model count overflow".into()))?;
        return Err(LabError::RequestLimit(Box::new(LimitReport::exceeded(
            request_size(spec, model_count, estimated, None)?,
            LimitItem::ModelEvents,
            estimated,
            active_limits().model_events,
            &[
                "shorten the evaluation range",
                "use coarser decision or execution intervals",
            ],
        ))));
    }
    Ok(estimated)
}

fn model_event_count(spec: &ExperimentSpec) -> Result<u64, LabError> {
    let decision_bars = u64::try_from(spec.range.bars(spec.decision_interval)?)
        .map_err(|_| LabError::ResourceLimit("decision count overflow".into()))?;
    let execution_bars = u64::try_from(spec.range.bars(spec.execution_resolution)?)
        .map_err(|_| LabError::ResourceLimit("execution count overflow".into()))?;
    let estimated = decision_bars
        .checked_mul(12)
        .and_then(|n| n.checked_add(execution_bars))
        .ok_or_else(|| LabError::ResourceLimit("event estimate overflow".into()))?;
    Ok(estimated)
}

/// Calculate bounded presentation metadata without changing the immutable plan.
/// # Errors
/// Rejects unrepresentable counts in corrupt or unsupported plan values.
pub fn plan_size(plan: &ResolvedPlan) -> Result<RequestSize, LabError> {
    request_size(
        &plan.spec,
        plan.admissions.len(),
        model_event_count(&plan.spec)?,
        Some(plan.estimated_events),
    )
}

fn request_size(
    spec: &ExperimentSpec,
    model_count: usize,
    max_model_events: u64,
    run_events: Option<u64>,
) -> Result<RequestSize, LabError> {
    let evaluation_seconds =
        u64::try_from((spec.range.end().0 - spec.range.start().0).num_seconds())
            .map_err(|_| LabError::InvalidConfig("plan range duration is invalid".into()))?;
    Ok(RequestSize {
        evaluation_days: Some(evaluation_days_ceil(evaluation_seconds)),
        evaluation_seconds: Some(evaluation_seconds),
        collection_rows: None,
        model_count: Some(
            u64::try_from(model_count)
                .map_err(|_| LabError::ResourceLimit("model count exceeds u64".into()))?,
        ),
        max_model_events: Some(max_model_events),
        run_events,
    })
}

struct PlanningSelection<'a> {
    family: StrategyKind,
    reference: Option<&'a PolicyRevisionRef>,
    legacy: Option<&'a StrategySpec>,
    warmup: usize,
    requires_evidence: bool,
}

fn planning_selections<'a>(
    spec: &'a ExperimentSpec,
    policies: &'a [FrozenPolicyRevision],
) -> Result<Vec<PlanningSelection<'a>>, LabError> {
    if spec.schema_version == "1.0" {
        spec.strategies
            .iter()
            .map(|strategy| {
                Ok(PlanningSelection {
                    family: strategy.kind(),
                    reference: None,
                    legacy: Some(strategy),
                    warmup: strategy.warmup_bars()?,
                    requires_evidence: matches!(
                        strategy,
                        StrategySpec::S5 { .. } | StrategySpec::S1CoverageControl { .. }
                    ),
                })
            })
            .collect()
    } else {
        policies
            .iter()
            .map(|policy| {
                Ok(PlanningSelection {
                    family: policy.family,
                    reference: Some(&policy.reference),
                    legacy: None,
                    warmup: policy.definition.warmup_bars()?,
                    requires_evidence: policy.definition.requires_evidence(),
                })
            })
            .collect()
    }
}

fn validate_input_closure(
    spec: &ExperimentSpec,
    datasets: &[DatasetSnapshot],
    evidence: Option<&EvidenceSnapshot>,
) -> Result<(), LabError> {
    spec.validate()?;
    let requested_ids: BTreeSet<_> = spec.dataset_ids.iter().collect();
    let actual_ids: BTreeSet<_> = datasets.iter().map(|d| &d.manifest.id).collect();
    if requested_ids != actual_ids || datasets.len() != spec.dataset_ids.len() {
        return Err(LabError::InputHashMismatch(
            "resolved dataset identities differ from request".into(),
        ));
    }
    if spec.evidence_snapshot_id.as_ref() != evidence.map(|e| &e.id) {
        return Err(LabError::InputHashMismatch(
            "resolved Evidence identity differs from request".into(),
        ));
    }
    let origin = datasets
        .first()
        .ok_or_else(|| LabError::InvalidConfig("no datasets".into()))?
        .manifest
        .origin;
    if datasets.iter().any(|d| d.manifest.origin != origin) {
        return Err(LabError::InvalidConfig(
            "cannot mix synthetic and observed inputs".into(),
        ));
    }
    let mut frozen_rows = BTreeSet::new();
    let mut observation_ids = BTreeSet::new();
    let mut input_rows = 0usize;
    for dataset in datasets {
        input_rows = input_rows
            .checked_add(dataset.observations.len())
            .ok_or_else(|| LabError::ResourceLimit("frozen input count overflow".into()))?;
        if input_rows > MAX_DATASET_ROWS {
            return Err(LabError::ResourceLimit(
                "combined frozen input closure exceeds 30000 rows".into(),
            ));
        }
        for row in &dataset.observations {
            if !frozen_rows.insert((
                &row.candle.market,
                row.candle.interval.duration().num_seconds(),
                row.candle.open_time_utc,
            )) || !observation_ids.insert(&row.id)
            {
                return Err(LabError::Conflict(
                    "frozen datasets overlap; select one snapshot for each market/resolution/time"
                        .into(),
                ));
            }
        }
    }
    if datasets
        .iter()
        .flat_map(|dataset| &dataset.observations)
        .flat_map(|row| &row.constituent_ids)
        .any(|id| !observation_ids.contains(id))
    {
        return Err(LabError::InvalidConfig(
            "derived dataset requires its complete source dataset closure in dataset_ids".into(),
        ));
    }
    Ok(())
}

fn data_reasons(
    spec: &ExperimentSpec,
    warmup: usize,
    market: &MarketId,
    datasets: &[DatasetSnapshot],
) -> Result<Vec<String>, LabError> {
    let mut reasons = Vec::new();
    let warmup = u32::try_from(warmup)
        .map_err(|_| LabError::InvalidConfig("warmup count overflow".into()))?;
    let needed = spec.range.with_warmup(warmup, spec.decision_interval)?;
    let execution_needed = spec.range.with_warmup(1, spec.execution_resolution)?;
    let mut decision_usable = false;
    let mut execution_usable = false;
    for dataset in datasets {
        if !dataset.manifest.request.markets.contains(market) {
            continue;
        }
        let source = dataset
            .manifest
            .request
            .data_resolution
            .duration()
            .num_seconds();
        let covers = |range: UtcRange| {
            dataset.manifest.coverage.start() <= range.start()
                && dataset.manifest.coverage.end() >= range.end()
                && !dataset.manifest.quality_issues.iter().any(|i| {
                    i.market == *market
                        && i.severity == QualitySeverity::Error
                        && i.start < range.end()
                        && i.end > range.start()
                })
        };
        if source == spec.decision_interval.duration().num_seconds() && covers(needed) {
            decision_usable = true;
        }
        if source == spec.execution_resolution.duration().num_seconds() && covers(execution_needed)
        {
            execution_usable = true;
        }
    }
    if !decision_usable {
        reasons.push(
            "INSUFFICIENT_WARMUP / DATA_GAP: complete decision grid and lookback required".into(),
        );
    }
    if !execution_usable {
        reasons.push(
            "DATA_GAP: complete divisible execution grid including preceding volume bar required"
                .into(),
        );
    }
    Ok(reasons)
}

fn has_coverage(
    spec: &ExperimentSpec,
    market: &MarketId,
    evaluator: &EvidenceEvaluator,
) -> Result<bool, LabError> {
    let mut time = spec.range.start();
    let base = Weight::new(rust_decimal::Decimal::ONE)?;
    while time < spec.range.end() {
        if evaluator
            .effect(market, time, base, false)?
            .coverage_available
        {
            return Ok(true);
        }
        time = UtcTimestamp(
            time.0
                .checked_add_signed(spec.decision_interval.duration())
                .ok_or_else(|| LabError::InvalidConfig("decision timestamp overflow".into()))?,
        );
    }
    Ok(false)
}
