use crate::contracts::{
    AutomationRiskPolicy, LabError, MAX_SCHEDULE_CADENCE_SECONDS, MIN_SCHEDULE_CADENCE_SECONDS,
    MachineContractComparisonInput, MachineContractFact, ReviewAuditBasis,
    ReviewAuditDiagnosticCode as Code, ReviewAuditKind, ReviewAuditReport,
};
use std::collections::BTreeMap;

use super::common::{AuditFindings, MAX_FACTS, check_serialized_bound, digest_value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FactKind {
    PolicyRevision,
    DecisionInterval,
    ExecutionInterval,
    CollectionCadence,
    AutomationRisk,
}

/// Compare only explicit, typed machine facts. Caller-declared approval is
/// recorded as provenance and never promoted to durable approval authority.
///
/// # Errors
/// Rejects duplicate, invalid, or over-bounded fact sets.
pub fn compare_machine_contracts(
    input: &MachineContractComparisonInput,
) -> Result<ReviewAuditReport, LabError> {
    if input.expected.len() > MAX_FACTS || input.observed.len() > MAX_FACTS {
        return Err(LabError::ResourceLimit(
            "machine contract comparison fact bound exceeded".into(),
        ));
    }
    check_serialized_bound(input)?;
    let expected = index_facts(&input.expected, "expected")?;
    let observed = index_facts(&input.observed, "observed")?;
    validate_fact_values(&input.expected)?;
    validate_fact_values(&input.observed)?;

    let evidence_digest = digest_value(input)?;
    let evidence_digests = vec![
        digest_value(&input.expected)?,
        digest_value(&input.observed)?,
    ];
    let mut findings = AuditFindings::new(
        ReviewAuditKind::MachineContractComparison,
        ReviewAuditBasis::ComparedExplicitMachineFacts,
        evidence_digest,
        evidence_digests,
    );
    findings.set_approval_origin(input.approval_evidence_origin);
    if expected.is_empty() {
        findings.insufficient(Code::MissingMachineFact, "expected");
        return findings.finish();
    }

    for (kind, expected_fact) in expected {
        let path = fact_path(kind);
        let Some(observed_fact) = observed.get(&kind) else {
            findings.insufficient(Code::MissingMachineFact, path);
            continue;
        };
        if !facts_match(expected_fact, observed_fact) {
            findings.contradiction(Code::MachineFactMismatch, path);
        }
    }
    findings.finish()
}

fn index_facts<'a>(
    facts: &'a [MachineContractFact],
    side: &str,
) -> Result<BTreeMap<FactKind, &'a MachineContractFact>, LabError> {
    let mut indexed = BTreeMap::new();
    for fact in facts {
        let kind = fact_kind(fact);
        if indexed.insert(kind, fact).is_some() {
            return Err(LabError::DataCorrupt(format!(
                "machine contract comparison has duplicate {side} fact"
            )));
        }
    }
    Ok(indexed)
}

fn fact_kind(fact: &MachineContractFact) -> FactKind {
    match fact {
        MachineContractFact::PolicyRevision { .. } => FactKind::PolicyRevision,
        MachineContractFact::DecisionInterval { .. } => FactKind::DecisionInterval,
        MachineContractFact::ExecutionInterval { .. } => FactKind::ExecutionInterval,
        MachineContractFact::CollectionCadence { .. } => FactKind::CollectionCadence,
        MachineContractFact::AutomationRisk { .. } => FactKind::AutomationRisk,
    }
}

fn validate_fact_values(facts: &[MachineContractFact]) -> Result<(), LabError> {
    for fact in facts {
        match fact {
            MachineContractFact::PolicyRevision { .. }
            | MachineContractFact::DecisionInterval { .. }
            | MachineContractFact::ExecutionInterval { .. } => {}
            MachineContractFact::CollectionCadence {
                cadence_seconds, ..
            } => {
                if !(MIN_SCHEDULE_CADENCE_SECONDS..=MAX_SCHEDULE_CADENCE_SECONDS)
                    .contains(cadence_seconds)
                {
                    return Err(LabError::DataCorrupt(
                        "machine contract cadence is outside the saved schedule contract".into(),
                    ));
                }
            }
            MachineContractFact::AutomationRisk { policy } => {
                validate_risk_policy(policy)?;
            }
        }
    }
    Ok(())
}

fn validate_risk_policy(policy: &AutomationRiskPolicy) -> Result<(), LabError> {
    if policy.validate().is_err() {
        return Err(LabError::DataCorrupt(
            "machine contract automation-risk fact is invalid".into(),
        ));
    }
    Ok(())
}

fn facts_match(expected: &MachineContractFact, observed: &MachineContractFact) -> bool {
    match (expected, observed) {
        (
            MachineContractFact::PolicyRevision { reference: left },
            MachineContractFact::PolicyRevision { reference: right },
        ) => left == right,
        (
            MachineContractFact::DecisionInterval { interval: left },
            MachineContractFact::DecisionInterval { interval: right },
        )
        | (
            MachineContractFact::ExecutionInterval { interval: left },
            MachineContractFact::ExecutionInterval { interval: right },
        ) => left == right,
        (
            MachineContractFact::CollectionCadence {
                interval: left_interval,
                cadence_seconds: left_seconds,
            },
            MachineContractFact::CollectionCadence {
                interval: right_interval,
                cadence_seconds: right_seconds,
            },
        ) => left_interval == right_interval && left_seconds == right_seconds,
        (
            MachineContractFact::AutomationRisk { policy: left },
            MachineContractFact::AutomationRisk { policy: right },
        ) => risk_policies_match(left, right),
        _ => false,
    }
}

fn risk_policies_match(left: &AutomationRiskPolicy, right: &AutomationRiskPolicy) -> bool {
    left.max_order_notional_krw == right.max_order_notional_krw
        && left.max_daily_notional_krw == right.max_daily_notional_krw
        && left.max_asset_exposure_krw == right.max_asset_exposure_krw
        && left.max_daily_loss_krw == right.max_daily_loss_krw
        && left.activation_loss_limit_krw == right.activation_loss_limit_krw
        && left.max_open_orders == right.max_open_orders
        && left.max_orders_per_interval == right.max_orders_per_interval
        && left.max_balance_age_seconds == right.max_balance_age_seconds
        && left.max_feed_age_seconds == right.max_feed_age_seconds
}

fn fact_path(kind: FactKind) -> &'static str {
    match kind {
        FactKind::PolicyRevision => "policy_revision",
        FactKind::DecisionInterval => "decision_interval",
        FactKind::ExecutionInterval => "execution_interval",
        FactKind::CollectionCadence => "collection_cadence",
        FactKind::AutomationRisk => "automation_risk",
    }
}
