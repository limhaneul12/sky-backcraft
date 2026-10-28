use crate::contracts::policy::{
    BoolExpr, CompareOp, NumericExpr, PolicyProgram, PolicyTarget, RulesProgram,
};
use crate::contracts::{
    LabError, NamedPolicyValue, PositionState, ReviewAuditBasis, ReviewAuditDiagnosticCode as Code,
    ReviewAuditKind, ReviewAuditReport, SavedRuleDecision, SnapshotRuleAuditInput, Weight,
    observation_digest, validate_frozen_policy,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use super::common::{
    AuditFindings, MAX_CROSS_VALUES, MAX_VALUES, check_serialized_bound, decimal_to_f64,
    digest_value, validate_observation, validate_total_links,
};

#[derive(Debug, Clone, Copy)]
struct CrossSample {
    previous: Option<(f64, f64)>,
    current: (f64, f64),
}

#[derive(Clone, Copy)]
struct EvalContext<'a> {
    indicators: &'a BTreeMap<String, f64>,
    state: &'a BTreeMap<String, f64>,
    actual_state: PositionState,
    actual_weight: f64,
    held_bars: Option<u64>,
    evidence_multiplier: f64,
    evidence_coverage: f64,
    crosses: &'a BTreeMap<String, CrossSample>,
}

struct PreparedRuleContext {
    indicators: BTreeMap<String, f64>,
    state_before: BTreeMap<String, f64>,
    actual_state: PositionState,
    actual_weight: f64,
    held_bars: Option<u64>,
    evidence_multiplier: f64,
    evidence_coverage: f64,
}

impl PreparedRuleContext {
    fn evaluation<'a>(&'a self, crosses: &'a BTreeMap<String, CrossSample>) -> EvalContext<'a> {
        EvalContext {
            indicators: &self.indicators,
            state: &self.state_before,
            actual_state: self.actual_state,
            actual_weight: self.actual_weight,
            held_bars: self.held_bars,
            evidence_multiplier: self.evidence_multiplier,
            evidence_coverage: self.evidence_coverage,
            crosses,
        }
    }
}

#[derive(Debug)]
enum EvalIssue {
    MissingEvidence(String),
    UnsupportedArithmetic(String),
}

/// Independently reconstruct one saved Rules-program result from its saved
/// snapshot inputs. This does not claim an actual historical execution trace.
/// It supports the current Rules numeric/boolean AST, state transitions, first
/// matching target selection, and explicit cross operators when the adjacent
/// prior cross snapshot is supplied. Builtin strategy programs are unsupported.
/// The comparison covers matched rule, target, and state-after values; diagnostic
/// condition-role/source-provenance trace vectors are outside this audit's claim.
///
/// # Errors
/// Rejects malformed identities, corrupt saved evidence, and resource overages.
pub fn audit_snapshot_rules(input: &SnapshotRuleAuditInput) -> Result<ReviewAuditReport, LabError> {
    validate_snapshot_evidence(input)?;

    let mut evidence_digests = vec![
        input.policy.reference.definition_digest.clone(),
        input.decision_observation.content_digest.clone(),
    ];
    if let Some(prior) = &input.prior_cross_snapshot {
        evidence_digests.push(digest_value(prior)?);
    }
    if let Some(evidence) = &input.current.evidence {
        evidence_digests.push(digest_value(evidence)?);
    }
    let evidence_digest = digest_value(input)?;
    let mut findings = AuditFindings::new(
        ReviewAuditKind::SnapshotRuleAudit,
        ReviewAuditBasis::ReconstructedFromSavedSnapshot,
        evidence_digest,
        evidence_digests,
    );

    if input.saved.policy_ref != input.policy.reference {
        findings.contradiction(Code::PolicyRevisionMismatch, "saved.policy_ref");
        return findings.finish();
    }
    let PolicyProgram::Rules { program } = &input.policy.definition.program else {
        findings.unsupported(Code::BuiltinPolicyUnsupported, "policy.program");
        return findings.finish();
    };
    let Some(context_values) = prepare_rule_context(input, program, &mut findings)? else {
        return findings.finish();
    };
    let Some(mut crosses) = prior_cross_samples(input, program, &mut findings)? else {
        return findings.finish();
    };
    let no_crosses = BTreeMap::new();
    let base_context = context_values.evaluation(&no_crosses);
    if let Err(issue) = collect_current_crosses(program, &base_context, &mut crosses) {
        record_eval_issue(issue, &mut findings);
        return findings.finish();
    }
    let context = context_values.evaluation(&crosses);
    evaluate_and_compare(input, program, &context, &mut findings)?;
    findings.finish()
}

fn prepare_rule_context(
    input: &SnapshotRuleAuditInput,
    program: &RulesProgram,
    findings: &mut AuditFindings,
) -> Result<Option<PreparedRuleContext>, LabError> {
    if input.current.completed_bars_seen == 0 {
        findings.insufficient(Code::IncompleteWarmup, "current.completed_bars_seen");
        return Ok(None);
    }
    let required_bars = input
        .policy
        .definition
        .decision_warmup_bars(input.decision_interval)?;
    if usize::try_from(input.current.completed_bars_seen)
        .ok()
        .is_none_or(|seen| seen < required_bars)
    {
        findings.insufficient(Code::IncompleteWarmup, "current.completed_bars_seen");
        return Ok(None);
    }

    let indicator_ids = program
        .indicators
        .iter()
        .map(|value| value.id.clone())
        .collect::<BTreeSet<_>>();
    let state_ids = program
        .states
        .iter()
        .map(|value| value.id.clone())
        .collect::<BTreeSet<_>>();
    let Some(indicators) = saved_values_map(
        &input.current.indicators,
        &indicator_ids,
        "current.indicators",
        findings,
    )?
    else {
        return Ok(None);
    };
    let Some(state_before) = saved_values_map(
        &input.current.state_before,
        &state_ids,
        "current.state_before",
        findings,
    )?
    else {
        return Ok(None);
    };
    if state_before.len() != program.states.len() || indicators.len() != program.indicators.len() {
        findings.insufficient(Code::MissingRequiredEvidence, "current.rule_context");
        return Ok(None);
    }
    if (input.current.actual_weight.get() == Decimal::ZERO
        && input.current.actual_state != PositionState::Cash)
        || (input.current.actual_weight.get() > Decimal::ZERO
            && input.current.actual_state != PositionState::Long)
    {
        return Err(LabError::DataCorrupt(
            "snapshot rule audit actual position state/weight mismatch".into(),
        ));
    }
    let actual_weight = decimal_to_f64(input.current.actual_weight.get(), "actual weight")?;
    let Some((evidence_multiplier, evidence_coverage)) = snapshot_evidence_values(input, findings)?
    else {
        return Ok(None);
    };
    Ok(Some(PreparedRuleContext {
        indicators,
        state_before,
        actual_state: input.current.actual_state,
        actual_weight,
        held_bars: input.current.held_bars,
        evidence_multiplier,
        evidence_coverage,
    }))
}

fn snapshot_evidence_values(
    input: &SnapshotRuleAuditInput,
    findings: &mut AuditFindings,
) -> Result<Option<(f64, f64)>, LabError> {
    if input.policy.definition.requires_evidence() {
        let Some(evidence) = &input.current.evidence else {
            findings.insufficient(Code::MissingRequiredEvidence, "current.evidence");
            return Ok(None);
        };
        if evidence.used_at != input.cutoff_utc {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit evidence timestamp mismatch".into(),
            ));
        }
        Ok(Some((
            decimal_to_f64(evidence.multiplier.get(), "evidence multiplier")?,
            f64::from(u8::from(evidence.coverage_available)),
        )))
    } else if input.current.evidence.is_some() {
        Err(LabError::DataCorrupt(
            "snapshot rule audit has unexpected evidence trace".into(),
        ))
    } else {
        Ok(Some((0.0, 0.0)))
    }
}

fn evaluate_and_compare(
    input: &SnapshotRuleAuditInput,
    program: &RulesProgram,
    context: &EvalContext<'_>,
    findings: &mut AuditFindings,
) -> Result<(), LabError> {
    let mut state_after = BTreeMap::new();
    for slot in &program.states {
        let path = format!("state:{}:next", slot.id);
        match eval_numeric(&slot.next, context, &path) {
            Ok(value) => {
                state_after.insert(slot.id.clone(), value);
            }
            Err(issue) => {
                record_eval_issue(issue, findings);
                return Ok(());
            }
        }
    }
    let (matched_rule, target_value) = match select_target(program, context) {
        Ok(value) => value,
        Err(issue) => {
            record_eval_issue(issue, findings);
            return Ok(());
        }
    };
    let recomputed_target = match weight_from_f64(target_value) {
        Ok(value) => value,
        Err(path) => {
            findings.unsupported(Code::UnsupportedArithmetic, path);
            return Ok(());
        }
    };
    let state_ids = program
        .states
        .iter()
        .map(|state| state.id.clone())
        .collect::<BTreeSet<_>>();
    compare_saved_decision(
        &input.saved,
        matched_rule.as_deref(),
        recomputed_target,
        &state_after,
        &state_ids,
        findings,
    )
}

/// Reconstruct the current cross operands from one saved Rules snapshot so a
/// read-only adapter can bind them as the next adjacent snapshot's prior input.
/// This is derived evidence, not a historical execution trace.
///
/// Returns `None` when the saved snapshot is builtin, has no cross expressions,
/// or lacks a context value needed to independently reconstruct nested operands.
///
/// # Errors
/// Rejects corrupt identities, saved bars, and resource overages.
pub fn reconstruct_cross_prior_snapshot(
    previous: &SnapshotRuleAuditInput,
) -> Result<Option<crate::contracts::RuleCrossPriorSnapshot>, LabError> {
    validate_snapshot_evidence(previous)?;
    if previous.saved.policy_ref != previous.policy.reference {
        return Ok(None);
    }
    let PolicyProgram::Rules { program } = &previous.policy.definition.program else {
        return Ok(None);
    };
    let paths = cross_paths(program);
    if paths.is_empty() {
        return Ok(None);
    }
    let mut findings = AuditFindings::new(
        ReviewAuditKind::SnapshotRuleAudit,
        ReviewAuditBasis::ReconstructedFromSavedSnapshot,
        digest_value(previous)?,
        Vec::new(),
    );
    let Some(context_values) = prepare_rule_context(previous, program, &mut findings)? else {
        return Ok(None);
    };
    let mut crosses = if previous.prior_cross_snapshot.is_some() {
        let Some(crosses) = prior_cross_samples(previous, program, &mut findings)? else {
            return Ok(None);
        };
        crosses
    } else {
        // Simple cross operands can be reconstructed without an older cross
        // sample. Nested cross operands fail closed in collect_current_crosses.
        BTreeMap::new()
    };
    let no_crosses = BTreeMap::new();
    let context = context_values.evaluation(&no_crosses);
    if collect_current_crosses(program, &context, &mut crosses).is_err() {
        return Ok(None);
    }
    let values = paths
        .into_iter()
        .map(|path| {
            crosses
                .get(&path)
                .map(|sample| crate::contracts::RuleCrossPriorValue {
                    path,
                    left: sample.current.0,
                    right: sample.current.1,
                })
        })
        .collect::<Option<Vec<_>>>();
    let Some(values) = values else {
        return Ok(None);
    };
    Ok(Some(crate::contracts::RuleCrossPriorSnapshot {
        policy_ref: previous.policy.reference.clone(),
        market: previous.market.clone(),
        decision_interval: previous.decision_interval,
        observation_id: previous.current.observation_id.clone(),
        decision_time_utc: previous.current.decision_time_utc,
        completed_bars_seen: previous.current.completed_bars_seen,
        values,
    }))
}

fn validate_snapshot_evidence(input: &SnapshotRuleAuditInput) -> Result<(), LabError> {
    validate_frozen_policy(&input.policy)?;
    validate_observation(
        &input.decision_observation,
        &input.market,
        input.decision_interval,
        input.cutoff_utc,
        "decision observation",
    )?;
    if input.decision_observation.candle.close_time_utc != input.cutoff_utc
        || input.current.observation_id != input.decision_observation.id
        || input.current.decision_time_utc != input.cutoff_utc
        || input.saved.observation_id != input.decision_observation.id
        || input.saved.decision_time_utc != input.cutoff_utc
    {
        return Err(LabError::DataCorrupt(
            "snapshot rule audit observation identity/cutoff mismatch".into(),
        ));
    }
    if input.current.indicators.len() > MAX_VALUES
        || input.current.state_before.len() > MAX_VALUES
        || input.saved.state_after.len() > MAX_VALUES
    {
        return Err(LabError::ResourceLimit(
            "snapshot rule audit named-value bound exceeded".into(),
        ));
    }
    validate_finite_values(&input.current.indicators, "current.indicators")?;
    validate_finite_values(&input.current.state_before, "current.state_before")?;
    validate_finite_values(&input.saved.state_after, "saved.state_after")?;
    if input.current.evidence.as_ref().is_some_and(|evidence| {
        evidence.eligible.len() > crate::contracts::MAX_REVIEW_AUDIT_EVIDENCE_ITEMS
    }) {
        return Err(LabError::ResourceLimit(
            "snapshot rule audit evidence-item bound exceeded".into(),
        ));
    }
    if let Some(prior) = &input.prior_cross_snapshot
        && prior.values.len() > MAX_CROSS_VALUES
    {
        return Err(LabError::ResourceLimit(
            "snapshot rule audit cross-history bound exceeded".into(),
        ));
    }
    if input.prior_cross_snapshot.as_ref().is_some_and(|prior| {
        prior
            .values
            .iter()
            .any(|value| !value.left.is_finite() || !value.right.is_finite())
    }) {
        return Err(LabError::DataCorrupt(
            "snapshot rule audit prior cross value is nonfinite".into(),
        ));
    }
    validate_total_links([&input.decision_observation])?;
    if observation_digest(&input.decision_observation.candle)?
        != input.decision_observation.content_digest
    {
        return Err(LabError::DataCorrupt(
            "snapshot rule audit candle content digest mismatch".into(),
        ));
    }
    check_serialized_bound(input)
}

fn saved_values_map(
    values: &[NamedPolicyValue],
    expected_ids: &BTreeSet<String>,
    path: &str,
    findings: &mut AuditFindings,
) -> Result<Option<BTreeMap<String, f64>>, LabError> {
    let mut result = BTreeMap::new();
    for value in values {
        if !value.value.is_finite() {
            return Err(LabError::DataCorrupt(format!(
                "snapshot rule audit {path} contains nonfinite value"
            )));
        }
        if !expected_ids.contains(&value.id)
            || result.insert(value.id.clone(), value.value).is_some()
        {
            return Err(LabError::DataCorrupt(format!(
                "snapshot rule audit {path} has an unknown or duplicate id"
            )));
        }
    }
    let found_ids = result.keys().cloned().collect::<BTreeSet<_>>();
    for missing in expected_ids.difference(&found_ids) {
        findings.insufficient(Code::MissingRequiredEvidence, format!("{path}.{missing}"));
    }
    if result.len() == expected_ids.len() {
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

fn validate_finite_values(values: &[NamedPolicyValue], path: &str) -> Result<(), LabError> {
    if values.iter().any(|value| !value.value.is_finite()) {
        return Err(LabError::DataCorrupt(format!(
            "snapshot rule audit {path} contains nonfinite value"
        )));
    }
    Ok(())
}

fn prior_cross_samples(
    input: &SnapshotRuleAuditInput,
    program: &RulesProgram,
    findings: &mut AuditFindings,
) -> Result<Option<BTreeMap<String, CrossSample>>, LabError> {
    let paths = cross_paths(program);
    if paths.is_empty() {
        if input.prior_cross_snapshot.is_some() {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit has cross history for a policy without crosses".into(),
            ));
        }
        return Ok(Some(BTreeMap::new()));
    }
    let Some(prior) = &input.prior_cross_snapshot else {
        findings.insufficient(Code::MissingCrossPriorSnapshot, "prior_cross_snapshot");
        return Ok(None);
    };
    if prior.policy_ref != input.policy.reference {
        findings.insufficient(
            Code::MissingCrossPriorSnapshot,
            "prior_cross_snapshot.policy_ref",
        );
        return Ok(None);
    }
    let adjacent = prior
        .completed_bars_seen
        .checked_add(1)
        .is_some_and(|count| count == input.current.completed_bars_seen);
    if prior.market != input.market
        || prior.decision_interval != input.decision_interval
        || prior.observation_id == input.current.observation_id
        || prior.decision_time_utc >= input.cutoff_utc
        || !adjacent
    {
        findings.insufficient(
            Code::MissingCrossPriorSnapshot,
            "prior_cross_snapshot.identity",
        );
        return Ok(None);
    }
    let mut result = BTreeMap::new();
    for sample in &prior.values {
        if !sample.left.is_finite() || !sample.right.is_finite() {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit prior cross value is nonfinite".into(),
            ));
        }
        if !paths.contains(&sample.path) || result.insert(sample.path.clone(), sample).is_some() {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit prior cross path is unknown or duplicated".into(),
            ));
        }
    }
    for path in &paths {
        if !result.contains_key(path) {
            findings.insufficient(
                Code::MissingCrossPriorSnapshot,
                format!("prior_cross_snapshot.{path}"),
            );
        }
    }
    if result.len() == paths.len() {
        Ok(Some(
            result
                .into_iter()
                .map(|(path, value)| {
                    (
                        path,
                        CrossSample {
                            previous: None,
                            current: (value.left, value.right),
                        },
                    )
                })
                .collect(),
        ))
    } else {
        Ok(None)
    }
}

fn cross_paths(program: &RulesProgram) -> Vec<String> {
    let mut paths = Vec::new();
    for state in &program.states {
        collect_cross_paths_numeric(&state.next, &format!("state:{}:next", state.id), &mut paths);
    }
    for rule in &program.rules {
        let path = format!("rule:{}:when", rule.id);
        collect_cross_paths_bool(&rule.condition, &path, &mut paths);
        collect_cross_paths_target(
            &rule.target,
            &format!("rule:{}:target", rule.id),
            &mut paths,
        );
    }
    collect_cross_paths_target(&program.fallback, "fallback", &mut paths);
    paths
}

fn collect_cross_paths_target(target: &PolicyTarget, path: &str, output: &mut Vec<String>) {
    if let PolicyTarget::Weight { value } = target {
        collect_cross_paths_numeric(value, path, output);
    }
}

fn collect_cross_paths_numeric(expr: &NumericExpr, path: &str, output: &mut Vec<String>) {
    match expr {
        NumericExpr::Add { left, right }
        | NumericExpr::Sub { left, right }
        | NumericExpr::Mul { left, right }
        | NumericExpr::Div { left, right }
        | NumericExpr::Min { left, right }
        | NumericExpr::Max { left, right } => {
            collect_cross_paths_numeric(left, &format!("{path}:left"), output);
            collect_cross_paths_numeric(right, &format!("{path}:right"), output);
        }
        NumericExpr::Abs { value } => {
            collect_cross_paths_numeric(value, &format!("{path}:value"), output);
        }
        NumericExpr::Clamp { value, min, max } => {
            collect_cross_paths_numeric(value, &format!("{path}:value"), output);
            collect_cross_paths_numeric(min, &format!("{path}:min"), output);
            collect_cross_paths_numeric(max, &format!("{path}:max"), output);
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => {
            collect_cross_paths_bool(condition, &format!("{path}:if"), output);
            collect_cross_paths_numeric(then_value, &format!("{path}:then"), output);
            collect_cross_paths_numeric(else_value, &format!("{path}:else"), output);
        }
        NumericExpr::Constant { .. }
        | NumericExpr::Indicator { .. }
        | NumericExpr::State { .. }
        | NumericExpr::ActualWeight
        | NumericExpr::HeldBars
        | NumericExpr::UnrealizedReturnBps
        | NumericExpr::TrailingDrawdownBps
        | NumericExpr::EvidenceMultiplier
        | NumericExpr::EvidenceCoverage => {}
    }
}

fn collect_cross_paths_bool(expr: &BoolExpr, path: &str, output: &mut Vec<String>) {
    match expr {
        BoolExpr::Compare { left, right, .. } => {
            collect_cross_paths_numeric(left, &format!("{path}:left"), output);
            collect_cross_paths_numeric(right, &format!("{path}:right"), output);
        }
        BoolExpr::And { conditions } | BoolExpr::Or { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                let conjunction = matches!(expr, BoolExpr::And { .. });
                let kind = if conjunction { "and" } else { "or" };
                collect_cross_paths_bool(condition, &format!("{path}:{kind}:{index}"), output);
            }
        }
        BoolExpr::Not { condition } => {
            collect_cross_paths_bool(condition, &format!("{path}:not"), output);
        }
        BoolExpr::CrossesAbove { left, right } | BoolExpr::CrossesBelow { left, right } => {
            collect_cross_paths_numeric(left, &format!("{path}:left"), output);
            collect_cross_paths_numeric(right, &format!("{path}:right"), output);
            output.push(path.into());
        }
        BoolExpr::PositionIs { .. } => {}
    }
}

fn collect_current_crosses(
    program: &RulesProgram,
    context: &EvalContext<'_>,
    samples: &mut BTreeMap<String, CrossSample>,
) -> Result<(), EvalIssue> {
    for state in &program.states {
        collect_current_crosses_numeric(
            &state.next,
            &format!("state:{}:next", state.id),
            context,
            samples,
        )?;
    }
    for rule in &program.rules {
        let path = format!("rule:{}:when", rule.id);
        collect_current_crosses_bool(&rule.condition, &path, context, samples)?;
        if let PolicyTarget::Weight { value } = &rule.target {
            collect_current_crosses_numeric(
                value,
                &format!("rule:{}:target", rule.id),
                context,
                samples,
            )?;
        }
    }
    if let PolicyTarget::Weight { value } = &program.fallback {
        collect_current_crosses_numeric(value, "fallback", context, samples)?;
    }
    Ok(())
}

fn collect_current_crosses_numeric(
    expr: &NumericExpr,
    path: &str,
    context: &EvalContext<'_>,
    samples: &mut BTreeMap<String, CrossSample>,
) -> Result<(), EvalIssue> {
    match expr {
        NumericExpr::Add { left, right }
        | NumericExpr::Sub { left, right }
        | NumericExpr::Mul { left, right }
        | NumericExpr::Div { left, right }
        | NumericExpr::Min { left, right }
        | NumericExpr::Max { left, right } => {
            collect_current_crosses_numeric(left, &format!("{path}:left"), context, samples)?;
            collect_current_crosses_numeric(right, &format!("{path}:right"), context, samples)?;
        }
        NumericExpr::Abs { value } => {
            collect_current_crosses_numeric(value, &format!("{path}:value"), context, samples)?;
        }
        NumericExpr::Clamp { value, min, max } => {
            collect_current_crosses_numeric(value, &format!("{path}:value"), context, samples)?;
            collect_current_crosses_numeric(min, &format!("{path}:min"), context, samples)?;
            collect_current_crosses_numeric(max, &format!("{path}:max"), context, samples)?;
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => {
            collect_current_crosses_bool(condition, &format!("{path}:if"), context, samples)?;
            collect_current_crosses_numeric(then_value, &format!("{path}:then"), context, samples)?;
            collect_current_crosses_numeric(else_value, &format!("{path}:else"), context, samples)?;
        }
        NumericExpr::Constant { .. }
        | NumericExpr::Indicator { .. }
        | NumericExpr::State { .. }
        | NumericExpr::ActualWeight
        | NumericExpr::HeldBars
        | NumericExpr::UnrealizedReturnBps
        | NumericExpr::TrailingDrawdownBps
        | NumericExpr::EvidenceMultiplier
        | NumericExpr::EvidenceCoverage => {}
    }
    Ok(())
}

fn collect_current_crosses_bool(
    expr: &BoolExpr,
    path: &str,
    context: &EvalContext<'_>,
    samples: &mut BTreeMap<String, CrossSample>,
) -> Result<(), EvalIssue> {
    match expr {
        BoolExpr::Compare { left, right, .. } => {
            collect_current_crosses_numeric(left, &format!("{path}:left"), context, samples)?;
            collect_current_crosses_numeric(right, &format!("{path}:right"), context, samples)?;
        }
        BoolExpr::And { conditions } | BoolExpr::Or { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                let conjunction = matches!(expr, BoolExpr::And { .. });
                let kind = if conjunction { "and" } else { "or" };
                collect_current_crosses_bool(
                    condition,
                    &format!("{path}:{kind}:{index}"),
                    context,
                    samples,
                )?;
            }
        }
        BoolExpr::Not { condition } => {
            collect_current_crosses_bool(condition, &format!("{path}:not"), context, samples)?;
        }
        BoolExpr::CrossesAbove { left, right } | BoolExpr::CrossesBelow { left, right } => {
            collect_current_crosses_numeric(left, &format!("{path}:left"), context, samples)?;
            collect_current_crosses_numeric(right, &format!("{path}:right"), context, samples)?;
            let current_context = EvalContext {
                crosses: samples,
                ..*context
            };
            let current = (
                eval_numeric(left, &current_context, &format!("{path}:left"))?,
                eval_numeric(right, &current_context, &format!("{path}:right"))?,
            );
            let previous = samples.get(path).map(|sample| sample.current);
            samples.insert(path.into(), CrossSample { previous, current });
        }
        BoolExpr::PositionIs { .. } => {}
    }
    Ok(())
}

fn eval_numeric(
    expr: &NumericExpr,
    context: &EvalContext<'_>,
    path: &str,
) -> Result<f64, EvalIssue> {
    let value = match expr {
        NumericExpr::Constant { value } => *value,
        NumericExpr::Indicator { id } => *context
            .indicators
            .get(id)
            .ok_or_else(|| EvalIssue::MissingEvidence(format!("{path}.indicator.{id}")))?,
        NumericExpr::State { id } => *context
            .state
            .get(id)
            .ok_or_else(|| EvalIssue::MissingEvidence(format!("{path}.state.{id}")))?,
        NumericExpr::ActualWeight => context.actual_weight,
        NumericExpr::HeldBars => {
            let value = context
                .held_bars
                .ok_or_else(|| EvalIssue::MissingEvidence(format!("{path}.held_bars")))?;
            f64::from(
                u32::try_from(value).map_err(|_| {
                    EvalIssue::UnsupportedArithmetic(format!("{path}.held_bars_range"))
                })?,
            )
        }
        NumericExpr::UnrealizedReturnBps => {
            if context.actual_state == PositionState::Cash {
                0.0
            } else {
                return Err(EvalIssue::MissingEvidence(format!(
                    "{path}.unrealized_return_bps"
                )));
            }
        }
        NumericExpr::TrailingDrawdownBps => {
            if context.actual_state == PositionState::Cash {
                0.0
            } else {
                return Err(EvalIssue::MissingEvidence(format!(
                    "{path}.trailing_drawdown_bps"
                )));
            }
        }
        NumericExpr::EvidenceMultiplier => context.evidence_multiplier,
        NumericExpr::EvidenceCoverage => context.evidence_coverage,
        NumericExpr::Add { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                + eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Sub { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                - eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Mul { left, right } => {
            eval_numeric(left, context, &format!("{path}:left"))?
                * eval_numeric(right, context, &format!("{path}:right"))?
        }
        NumericExpr::Div { left, right } => {
            let numerator = eval_numeric(left, context, &format!("{path}:left"))?;
            let denominator = eval_numeric(right, context, &format!("{path}:right"))?;
            if denominator == 0.0 {
                return Err(EvalIssue::UnsupportedArithmetic(format!(
                    "{path}.division_by_zero"
                )));
            }
            numerator / denominator
        }
        NumericExpr::Min { left, right } => eval_numeric(left, context, &format!("{path}:left"))?
            .min(eval_numeric(right, context, &format!("{path}:right"))?),
        NumericExpr::Max { left, right } => eval_numeric(left, context, &format!("{path}:left"))?
            .max(eval_numeric(right, context, &format!("{path}:right"))?),
        NumericExpr::Abs { value } => eval_numeric(value, context, &format!("{path}:value"))?.abs(),
        NumericExpr::Clamp { value, min, max } => {
            let value = eval_numeric(value, context, &format!("{path}:value"))?;
            let min = eval_numeric(min, context, &format!("{path}:min"))?;
            let max = eval_numeric(max, context, &format!("{path}:max"))?;
            if min > max {
                return Err(EvalIssue::UnsupportedArithmetic(format!(
                    "{path}.clamp_bounds"
                )));
            }
            value.clamp(min, max)
        }
        NumericExpr::If {
            condition,
            then_value,
            else_value,
        } => {
            if eval_bool(condition, context, &format!("{path}:if"))? {
                eval_numeric(then_value, context, &format!("{path}:then"))?
            } else {
                eval_numeric(else_value, context, &format!("{path}:else"))?
            }
        }
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(EvalIssue::UnsupportedArithmetic(format!(
            "{path}.nonfinite"
        )))
    }
}

#[expect(
    clippy::float_cmp,
    reason = "EQ is an explicit exact comparison in the saved Rules DSL"
)]
fn eval_bool(expr: &BoolExpr, context: &EvalContext<'_>, path: &str) -> Result<bool, EvalIssue> {
    match expr {
        BoolExpr::Compare {
            comparison,
            left,
            right,
        } => {
            let left = eval_numeric(left, context, &format!("{path}:left"))?;
            let right = eval_numeric(right, context, &format!("{path}:right"))?;
            Ok(match comparison {
                CompareOp::Gt => left > right,
                CompareOp::Gte => left >= right,
                CompareOp::Lt => left < right,
                CompareOp::Lte => left <= right,
                CompareOp::Eq => left == right,
            })
        }
        BoolExpr::And { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                if !eval_bool(condition, context, &format!("{path}:and:{index}"))? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoolExpr::Or { conditions } => {
            for (index, condition) in conditions.iter().enumerate() {
                if eval_bool(condition, context, &format!("{path}:or:{index}"))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        BoolExpr::Not { condition } => Ok(!eval_bool(condition, context, &format!("{path}:not"))?),
        BoolExpr::CrossesAbove { .. } => cross_value(context, path, true),
        BoolExpr::CrossesBelow { .. } => cross_value(context, path, false),
        BoolExpr::PositionIs { state } => Ok(context.actual_state == *state),
    }
}

fn cross_value(context: &EvalContext<'_>, path: &str, above: bool) -> Result<bool, EvalIssue> {
    let sample = context
        .crosses
        .get(path)
        .ok_or_else(|| EvalIssue::MissingEvidence(format!("{path}.cross_sample")))?;
    let Some(previous) = sample.previous else {
        return Err(EvalIssue::MissingEvidence(format!("{path}.cross_prior")));
    };
    Ok(if above {
        previous.0 <= previous.1 && sample.current.0 > sample.current.1
    } else {
        previous.0 >= previous.1 && sample.current.0 < sample.current.1
    })
}

fn select_target(
    program: &RulesProgram,
    context: &EvalContext<'_>,
) -> Result<(Option<String>, f64), EvalIssue> {
    for rule in &program.rules {
        let path = format!("rule:{}", rule.id);
        if eval_bool(&rule.condition, context, &format!("{path}:when"))? {
            let target = eval_target(&rule.target, context, &format!("{path}:target"))?;
            return Ok((Some(rule.id.clone()), target));
        }
    }
    Ok((None, eval_target(&program.fallback, context, "fallback")?))
}

fn eval_target(
    target: &PolicyTarget,
    context: &EvalContext<'_>,
    path: &str,
) -> Result<f64, EvalIssue> {
    match target {
        PolicyTarget::Hold => Ok(context.actual_weight),
        PolicyTarget::Weight { value } => eval_numeric(value, context, path),
    }
}

fn weight_from_f64(value: f64) -> Result<Weight, String> {
    if !value.is_finite() {
        return Err("target.nonfinite".into());
    }
    let decimal = Decimal::from_str(&value.to_string()).map_err(|_| "target.decimal_conversion")?;
    Weight::new(decimal).map_err(|_| "target.outside_weight_range".into())
}

fn compare_saved_decision(
    saved: &SavedRuleDecision,
    matched_rule: Option<&str>,
    target: Weight,
    state_after: &BTreeMap<String, f64>,
    state_ids: &BTreeSet<String>,
    findings: &mut AuditFindings,
) -> Result<(), LabError> {
    if saved.matched_rule_id.as_deref() != matched_rule {
        findings.contradiction(Code::MatchedRuleMismatch, "saved.matched_rule_id");
    }
    findings.exact_decimal_residual(
        "saved.target_weight",
        saved.target_weight.get(),
        target.get(),
    )?;
    if saved.target_weight != target {
        findings.contradiction(Code::TargetWeightMismatch, "saved.target_weight");
    }

    if saved.state_after.len() > MAX_VALUES {
        return Err(LabError::ResourceLimit(
            "snapshot rule audit saved state bound exceeded".into(),
        ));
    }
    let mut observed = BTreeMap::new();
    for value in &saved.state_after {
        if !value.value.is_finite() {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit saved state contains nonfinite value".into(),
            ));
        }
        if !state_ids.contains(&value.id)
            || observed.insert(value.id.clone(), value.value).is_some()
        {
            return Err(LabError::DataCorrupt(
                "snapshot rule audit saved state id is unknown or duplicated".into(),
            ));
        }
    }
    for (id, actual) in state_after {
        let Some(expected) = observed.get(id) else {
            findings.insufficient(
                Code::MissingRequiredEvidence,
                format!("saved.state_after.{id}"),
            );
            continue;
        };
        #[expect(
            clippy::float_cmp,
            reason = "state expressions are reconstructed with the same ordered f64 semantics"
        )]
        let matches = *expected == *actual;
        let _ = findings.residual(format!("saved.state_after.{id}"), *expected, *actual, 0.0)?;
        if !matches {
            findings.contradiction(
                Code::StateTransitionMismatch,
                format!("saved.state_after.{id}"),
            );
        }
    }
    Ok(())
}

fn record_eval_issue(issue: EvalIssue, findings: &mut AuditFindings) {
    match issue {
        EvalIssue::MissingEvidence(path) => {
            findings.insufficient(Code::MissingRequiredEvidence, path);
        }
        EvalIssue::UnsupportedArithmetic(path) => {
            findings.unsupported(Code::UnsupportedArithmetic, path);
        }
    }
}
