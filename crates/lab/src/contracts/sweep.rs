//! Deterministic parameter-sweep expansion into exact immutable policies.
//!
//! Every expanded candidate becomes a normalized parameter map with a stable
//! digest and a full [`PolicyDefinition`]; no anonymous parameter tuple ever
//! reaches execution. Expansion is pure: identical requests expand to
//! byte-identical candidates regardless of tuple order.

use super::{ContentHash, LabError, PolicyDefinition, PolicyProgram, StrategyKind, StrategySpec};
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

/// One expanded, immutable candidate with its normalized identity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepCandidate {
    pub index: u32,
    /// Normalized parameter map; keys sort, values render canonically.
    pub parameters: BTreeMap<String, String>,
    /// Digest of `(family, parameters)`; identical parameter sets share it.
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
    template.validate()?;
    if template.kind() != family || family == StrategyKind::BuyAndHold {
        return Err(LabError::InvalidConfig(
            "sweep family must match the template strategy and support parameters".into(),
        ));
    }
    let allowed = parameter_names(template);
    let assignments = match mode {
        ParameterSweepMode::Tuples { tuples } => {
            let mut expanded = Vec::new();
            for tuple in tuples {
                let mut assignment = BTreeMap::new();
                for (name, value) in tuple {
                    if !allowed.contains(name) {
                        return Err(LabError::InvalidConfig(format!(
                            "unsupported sweep parameter {name} for {family:?}"
                        )));
                    }
                    if assignment.insert(name.clone(), value.clone()).is_some() {
                        return Err(LabError::InvalidConfig(format!(
                            "duplicate sweep parameter {name} in one tuple"
                        )));
                    }
                }
                expanded.push(assignment);
            }
            expanded
        }
        ParameterSweepMode::Grid { axes } => {
            for name in axes.keys() {
                if !allowed.contains(name) {
                    return Err(LabError::InvalidConfig(format!(
                        "unsupported sweep axis {name} for {family:?}"
                    )));
                }
                if axes[name].is_empty() {
                    return Err(LabError::InvalidConfig(format!(
                        "sweep axis {name} must have at least one value"
                    )));
                }
            }
            let total = axes
                .values()
                .try_fold(1_usize, |product, values| product.checked_mul(values.len()))
                .unwrap_or(usize::MAX);
            if total > MAX_SWEEP_CANDIDATES {
                return Err(LabError::ResourceLimit(format!(
                    "grid expansion would produce {total} candidates; limit is {MAX_SWEEP_CANDIDATES}"
                )));
            }
            cartesian(&axes.iter().collect::<Vec<_>>())
        }
    };
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    let mut duplicates_suppressed = 0_usize;
    for assignment in assignments {
        let normalized: BTreeMap<String, String> = assignment
            .iter()
            .map(|(name, value)| (name.clone(), value.normalized()))
            .collect();
        let applied = apply_assignment(template, &assignment)?;
        applied.validate()?;
        let definition_digest = ContentHash::of_value(&(family, &normalized))?;
        if !seen.insert(definition_digest.clone()) {
            duplicates_suppressed = duplicates_suppressed
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("duplicate count overflow".into()))?;
            continue;
        }
        let definition = PolicyDefinition {
            schema_version: super::POLICY_SCHEMA_VERSION.into(),
            name: format!("sweep-{family:?}-{}", &definition_digest.as_str()[..12]),
            description: "parameter sweep candidate; deterministic expansion".into(),
            program: PolicyProgram::Builtin { strategy: applied },
        };
        definition.validate()?;
        let index = u32::try_from(candidates.len())
            .map_err(|_| LabError::ResourceLimit("candidate index overflow".into()))?;
        candidates.push(SweepCandidate {
            index,
            parameters: normalized,
            definition_digest,
            definition,
        });
    }
    if candidates.is_empty() {
        return Err(LabError::InvalidConfig(
            "parameter sweep requires at least one candidate".into(),
        ));
    }
    Ok(SweepPlan {
        family,
        mode: mode.clone(),
        candidates,
        duplicates_suppressed,
    })
}

/// Cartesian product over sorted axes; deterministic by construction.
fn cartesian(axes: &[(&String, &Vec<SweepValue>)]) -> Vec<BTreeMap<String, SweepValue>> {
    let mut products = vec![BTreeMap::new()];
    for (name, values) in axes {
        let mut next = Vec::new();
        for product in &products {
            for value in *values {
                let mut candidate = product.clone();
                candidate.insert((*name).clone(), value.clone());
                next.push(candidate);
            }
        }
        products = next;
    }
    products
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

    fn s2(entry: u32, exit: u32) -> StrategySpec {
        StrategySpec::S2 {
            entry_length: usize::try_from(entry).unwrap_or(0),
            exit_length: usize::try_from(exit).unwrap_or(0),
        }
    }

    fn int(value: u32) -> SweepValue {
        SweepValue::Integer(value)
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
