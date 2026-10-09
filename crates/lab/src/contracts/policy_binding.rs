//! Frozen strategy selection identity; family labels never select execution code.

use super::{
    ContentHash, ExperimentSpec, FrozenPolicyRevision, LabError, ModelAdmission, PolicyProgram,
    PolicyRevisionRef, ResolvedPlan, StrategyKind, StrategySpec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum StrategyBinding {
    Legacy(StrategySpec),
    Policy {
        reference: PolicyRevisionRef,
        family: StrategyKind,
    },
}

impl From<StrategySpec> for StrategyBinding {
    fn from(spec: StrategySpec) -> Self {
        Self::Legacy(spec)
    }
}

impl StrategyBinding {
    #[must_use]
    pub fn kind(&self) -> StrategyKind {
        match self {
            Self::Legacy(spec) => spec.kind(),
            Self::Policy { family, .. } => *family,
        }
    }
    #[must_use]
    pub fn policy_ref(&self) -> Option<&PolicyRevisionRef> {
        match self {
            Self::Legacy(_) => None,
            Self::Policy { reference, .. } => Some(reference),
        }
    }
    /// Resolve only the immutable body embedded in this plan, never a registry head.
    /// # Errors
    /// Rejects missing, mismatched or altered frozen definitions.
    pub fn program(&self, plan: &ResolvedPlan) -> Result<PolicyProgram, LabError> {
        match self {
            Self::Legacy(spec) => Ok(PolicyProgram::Builtin {
                strategy: spec.clone(),
            }),
            Self::Policy { reference, family } => Ok(frozen_policy(plan, reference, *family)?
                .definition
                .program
                .clone()),
        }
    }
}

fn frozen_policy<'a>(
    plan: &'a ResolvedPlan,
    reference: &PolicyRevisionRef,
    family: StrategyKind,
) -> Result<&'a FrozenPolicyRevision, LabError> {
    let frozen = plan
        .policy_revisions
        .iter()
        .find(|revision| revision.reference == *reference)
        .ok_or_else(|| {
            LabError::InputHashMismatch("policy revision is not frozen in this plan".into())
        })?;
    validate_frozen_policy(frozen)?;
    if frozen.family != family {
        return Err(LabError::InputHashMismatch(
            "policy family differs from frozen revision".into(),
        ));
    }
    Ok(frozen)
}

/// Find one exact selected definition for a model admission.
/// # Errors
/// Rejects a missing or version-incompatible selection.
pub fn strategy_binding(
    plan: &ResolvedPlan,
    admission: &ModelAdmission,
) -> Result<StrategyBinding, LabError> {
    match (
        &admission.policy_ref,
        plan.spec.schema_version.as_str(),
        plan.spec.causal_execution,
    ) {
        (None, "1.0", None) => plan
            .spec
            .strategies
            .iter()
            .find(|strategy| strategy.kind() == admission.strategy)
            .cloned()
            .map(StrategyBinding::Legacy)
            .ok_or_else(|| {
                LabError::InputHashMismatch("legacy admission has no matching strategy".into())
            }),
        (Some(reference), "2.0", None)
        | (Some(reference), "3.0", Some(super::CausalExecutionPolicy::DeclaredPolicyWarmup)) => {
            let binding = StrategyBinding::Policy {
                reference: reference.clone(),
                family: admission.strategy,
            };
            let _program = binding.program(plan)?;
            Ok(binding)
        }
        _ => Err(LabError::InputHashMismatch(
            "admission selection differs from experiment schema".into(),
        )),
    }
}

#[derive(Serialize)]
struct ConfigProjection<'a> {
    spec: &'a ExperimentSpec,
    policy_revisions: &'a [FrozenPolicyRevision],
}

/// Canonical configuration identity; v1/v2 bytes remain unchanged and v3 commits its causal mode.
/// # Errors
/// Rejects version/body disagreement or invalid frozen revisions.
pub fn experiment_config_digest(
    spec: &ExperimentSpec,
    policies: &[FrozenPolicyRevision],
) -> Result<ContentHash, LabError> {
    spec.validate()?;
    match spec.schema_version.as_str() {
        "1.0" if policies.is_empty() => ContentHash::of_value(spec),
        "2.0" | "3.0" if policies.len() == spec.policy_selections.len() => {
            for (reference, frozen) in spec.policy_selections.iter().zip(policies) {
                if *reference != frozen.reference {
                    return Err(LabError::InputHashMismatch(
                        "frozen policy order or reference mismatch".into(),
                    ));
                }
                validate_frozen_policy(frozen)?;
            }
            ContentHash::of_value(&ConfigProjection {
                spec,
                policy_revisions: policies,
            })
        }
        _ => Err(LabError::InputHashMismatch(
            "policy bodies disagree with experiment version".into(),
        )),
    }
}

/// # Errors
/// Rejects malformed revision lineage and altered definition bytes.
pub fn validate_frozen_policy(policy: &FrozenPolicyRevision) -> Result<(), LabError> {
    policy.definition.validate()?;
    if policy.revision_number == 0
        || (policy.revision_number == 1) != policy.parent_revision_id.is_none()
        || policy.parent_revision_id.as_ref() == Some(&policy.reference.revision_id)
    {
        return Err(LabError::InputHashMismatch(
            "invalid frozen policy lineage".into(),
        ));
    }
    if ContentHash::of_value(&policy.definition)? != policy.reference.definition_digest {
        return Err(LabError::InputHashMismatch(
            "frozen policy definition digest mismatch".into(),
        ));
    }
    Ok(())
}
