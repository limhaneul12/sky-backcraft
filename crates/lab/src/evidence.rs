//! Pure immutable Evidence snapshot construction and point-in-time evaluation.

use crate::contracts::{
    ContentHash, EligibleEvidence, EvidenceDecisionEffect, EvidenceId, EvidenceImport,
    EvidenceProvenance, EvidencePurpose, EvidenceRevisionId, EvidenceSnapshot, EvidenceSnapshotId,
    EvidenceVersion, LabError, MarketId, PitPolicy, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};

const MAX_EVIDENCE_VERSIONS: usize = 1_024;
const UNAVAILABLE_POLICY: &str = "CASH_WITH_MATCHED_CONTROL";

#[derive(Debug, Clone)]
struct PreparedVersion {
    version: EvidenceVersion,
    available_at: UtcTimestamp,
    eligibility_reason: String,
    lineage_depth: usize,
}

/// Validated, immutable point-in-time projection of one optional Evidence snapshot.
#[derive(Debug, Clone)]
pub struct EvidenceEvaluator {
    policy: PitPolicy,
    snapshot_id: Option<EvidenceSnapshotId>,
    versions: Vec<PreparedVersion>,
}

impl EvidenceEvaluator {
    /// Validate a snapshot once and prepare policy-specific availability clocks.
    ///
    /// `None` is a deliberate unavailable input. It produces cash targets for both
    /// S5 and its matched coverage control rather than silently treating missing
    /// Evidence as risk-on.
    ///
    /// # Errors
    /// Rejects non-canonical snapshots, invalid hashes, ambiguous/cyclic revision
    /// lineages, and time claims unsupported by the selected PIT policy.
    pub fn new(snapshot: Option<&EvidenceSnapshot>, policy: PitPolicy) -> Result<Self, LabError> {
        let Some(snapshot) = snapshot else {
            return Ok(Self {
                policy,
                snapshot_id: None,
                versions: Vec::new(),
            });
        };
        validate_snapshot(snapshot)?;
        let versions = snapshot
            .versions
            .iter()
            .cloned()
            .map(|version| {
                let (available_at, eligibility_reason) =
                    if version.purpose == EvidencePurpose::PosthocContext {
                        (
                            version.declared_available_at,
                            "POSTHOC_CONTEXT_EXCLUDED".into(),
                        )
                    } else {
                        availability(&version, policy)?
                    };
                if available_at >= version.valid_until {
                    return Err(LabError::InvalidConfig(format!(
                        "effective Evidence availability must precede expiry: {}",
                        version.revision_id
                    )));
                }
                Ok(PreparedVersion {
                    version,
                    available_at,
                    eligibility_reason,
                    lineage_depth: 0,
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        let prepared_by_revision: BTreeMap<_, _> = versions
            .iter()
            .map(|prepared| (&prepared.version.revision_id, prepared))
            .collect();
        for prepared in &versions {
            if let Some(parent_id) = &prepared.version.supersedes_revision_id {
                let parent = prepared_by_revision.get(parent_id).ok_or_else(|| {
                    LabError::Internal("validated Evidence parent disappeared".into())
                })?;
                if prepared.available_at < parent.available_at {
                    return Err(LabError::Conflict(
                        "new Evidence revision cannot become available before its parent".into(),
                    ));
                }
            }
        }
        drop(prepared_by_revision);
        let parent_by_revision: BTreeMap<_, _> = versions
            .iter()
            .map(|prepared| {
                (
                    prepared.version.revision_id.clone(),
                    prepared.version.supersedes_revision_id.clone(),
                )
            })
            .collect();
        let mut versions = versions;
        for prepared in &mut versions {
            let mut cursor = prepared.version.supersedes_revision_id.as_ref();
            while let Some(parent_id) = cursor {
                prepared.lineage_depth += 1;
                cursor = parent_by_revision
                    .get(parent_id)
                    .ok_or_else(|| {
                        LabError::Internal("validated Evidence parent disappeared".into())
                    })?
                    .as_ref();
            }
        }
        Ok(Self {
            policy,
            snapshot_id: Some(snapshot.id.clone()),
            versions,
        })
    }

    /// Apply the eligible latest revision of each independent event.
    ///
    /// S5 uses the minimum multiplier. The matched coverage control uses the
    /// identical availability boundary but leaves the base target unchanged while
    /// coverage is available. Both use cash when coverage is unavailable.
    ///
    /// # Errors
    /// Returns a contract error only if checked decimal multiplication overflows.
    pub fn effect(
        &self,
        market: &MarketId,
        decision_time: UtcTimestamp,
        base_target: Weight,
        coverage_control: bool,
    ) -> Result<EvidenceDecisionEffect, LabError> {
        let mut latest_by_event: BTreeMap<&str, &PreparedVersion> = BTreeMap::new();
        for candidate in &self.versions {
            let version = &candidate.version;
            if version.purpose != EvidencePurpose::StrategyInput
                || !version.markets.iter().any(|scope| scope == market)
                || candidate.available_at > decision_time
                || decision_time >= version.valid_until
            {
                continue;
            }
            match latest_by_event.get(version.event_id.as_str()) {
                Some(current) if current.lineage_depth > candidate.lineage_depth => {}
                Some(current)
                    if current.lineage_depth == candidate.lineage_depth
                        && current.available_at > candidate.available_at => {}
                Some(current)
                    if current.lineage_depth == candidate.lineage_depth
                        && current.available_at == candidate.available_at
                        && current.version.revision_id > version.revision_id => {}
                _ => {
                    latest_by_event.insert(version.event_id.as_str(), candidate);
                }
            }
        }

        let coverage_available = !latest_by_event.is_empty();
        let multiplier = latest_by_event
            .values()
            .map(|candidate| candidate.version.weight_multiplier.get())
            .min()
            .unwrap_or(Decimal::ZERO);
        let final_decimal = if !coverage_available {
            Decimal::ZERO
        } else if coverage_control {
            base_target.get()
        } else {
            base_target.get().checked_mul(multiplier).ok_or_else(|| {
                LabError::InvalidConfig("Evidence target multiplication overflow".into())
            })?
        };
        let final_target = Weight::new(final_decimal)?;
        let eligible = latest_by_event
            .values()
            .map(|candidate| EligibleEvidence {
                revision_id: candidate.version.revision_id.clone(),
                available_at: candidate.available_at,
                valid_until: candidate.version.valid_until,
                multiplier: candidate.version.weight_multiplier,
                eligibility_reason: candidate.eligibility_reason.clone(),
            })
            .collect();
        Ok(EvidenceDecisionEffect {
            policy: self.policy,
            snapshot_id: self.snapshot_id.clone(),
            used_at: decision_time,
            eligible,
            base_target,
            final_target,
            coverage_available,
            effect_changed_action: final_target != base_target,
            unavailable_policy: UNAVAILABLE_POLICY.into(),
        })
    }
}

/// Build the sole canonical snapshot representation: unique versions sorted by
/// revision ID, with the digest and snapshot ID derived from that exact vector.
///
/// Re-registering a byte-identical revision is idempotent. Reusing a revision ID
/// for different content is a conflict.
///
/// # Errors
/// Rejects unacknowledged/private imports, resource excess, invalid versions, or
/// conflicting revision identity/lineage.
pub fn build_snapshot(import: EvidenceImport) -> Result<EvidenceSnapshot, LabError> {
    if !import.public_non_sensitive_ack {
        return Err(LabError::InvalidConfig(
            "Evidence import requires public_non_sensitive_ack=true".into(),
        ));
    }
    if import.versions.is_empty() || import.versions.len() > MAX_EVIDENCE_VERSIONS {
        return Err(LabError::ResourceLimit(format!(
            "Evidence import requires 1..={MAX_EVIDENCE_VERSIONS} versions"
        )));
    }
    let mut unique: BTreeMap<EvidenceRevisionId, (ContentHash, EvidenceVersion)> = BTreeMap::new();
    for version in import.versions {
        version.validate()?;
        let version_hash = ContentHash::of_value(&version)?;
        match unique.get(&version.revision_id) {
            Some((existing_hash, _)) if existing_hash == &version_hash => {}
            Some(_) => {
                return Err(LabError::Conflict(format!(
                    "revision ID reused with different content: {}",
                    version.revision_id
                )));
            }
            None => {
                unique.insert(version.revision_id.clone(), (version_hash, version));
            }
        }
    }
    let versions = unique
        .into_values()
        .map(|(_, version)| version)
        .collect::<Vec<_>>();
    validate_lineages(&versions)?;
    let digest = ContentHash::of_value(&versions)?;
    let id = EvidenceSnapshotId::from_seed(digest.as_str());
    Ok(EvidenceSnapshot {
        id,
        digest,
        versions,
    })
}

fn validate_snapshot(snapshot: &EvidenceSnapshot) -> Result<(), LabError> {
    if snapshot.versions.is_empty() || snapshot.versions.len() > MAX_EVIDENCE_VERSIONS {
        return Err(LabError::ResourceLimit(format!(
            "Evidence snapshot requires 1..={MAX_EVIDENCE_VERSIONS} versions"
        )));
    }
    let mut previous: Option<&EvidenceRevisionId> = None;
    for version in &snapshot.versions {
        version.validate()?;
        if previous.is_some_and(|id| id >= &version.revision_id) {
            return Err(LabError::DataCorrupt(
                "Evidence snapshot revisions are not in canonical unique ID order".into(),
            ));
        }
        previous = Some(&version.revision_id);
    }
    validate_lineages(&snapshot.versions)?;
    let digest = ContentHash::of_value(&snapshot.versions)?;
    if digest != snapshot.digest {
        return Err(LabError::InputHashMismatch(
            "Evidence snapshot digest mismatch".into(),
        ));
    }
    let expected_id = EvidenceSnapshotId::from_seed(digest.as_str());
    if snapshot.id != expected_id {
        return Err(LabError::InputHashMismatch(
            "Evidence snapshot ID/digest mismatch".into(),
        ));
    }
    Ok(())
}

fn validate_lineages(versions: &[EvidenceVersion]) -> Result<(), LabError> {
    let by_revision: BTreeMap<_, _> = versions.iter().map(|v| (&v.revision_id, v)).collect();
    if by_revision.len() != versions.len() {
        return Err(LabError::Conflict(
            "duplicate revision ID in Evidence snapshot".into(),
        ));
    }
    let mut child_by_parent: BTreeMap<&EvidenceRevisionId, &EvidenceRevisionId> = BTreeMap::new();
    let mut roots_by_event: BTreeMap<&str, usize> = BTreeMap::new();
    let mut event_by_evidence: BTreeMap<&EvidenceId, &str> = BTreeMap::new();
    for version in versions {
        if event_by_evidence
            .insert(&version.evidence_id, version.event_id.as_str())
            .is_some_and(|event| event != version.event_id.as_str())
        {
            return Err(LabError::Conflict(
                "Evidence ID cannot span multiple event lineages".into(),
            ));
        }
        let unique_markets: BTreeSet<_> = version.markets.iter().map(MarketId::code).collect();
        if unique_markets.len() != version.markets.len() {
            return Err(LabError::InvalidConfig(
                "Evidence market scope contains duplicates".into(),
            ));
        }
        if let Some(parent_id) = &version.supersedes_revision_id {
            let parent = by_revision.get(parent_id).ok_or_else(|| {
                LabError::InvalidConfig(format!("missing superseded revision: {parent_id}"))
            })?;
            if parent.event_id != version.event_id
                || parent.evidence_id != version.evidence_id
                || parent.purpose != version.purpose
            {
                return Err(LabError::Conflict(
                    "Evidence revision may supersede only its own event lineage".into(),
                ));
            }
            if child_by_parent
                .insert(parent_id, &version.revision_id)
                .is_some()
            {
                return Err(LabError::Conflict(
                    "branching Evidence revisions require explicit human resolution".into(),
                ));
            }
        } else {
            *roots_by_event.entry(version.event_id.as_str()).or_default() += 1;
        }
    }
    if roots_by_event.values().any(|roots| *roots != 1) {
        return Err(LabError::Conflict(
            "each Evidence event must have one resolved revision lineage".into(),
        ));
    }
    for version in versions {
        let mut seen = BTreeSet::new();
        let mut cursor = Some(&version.revision_id);
        while let Some(revision_id) = cursor {
            if !seen.insert(revision_id) {
                return Err(LabError::Conflict(
                    "cyclic Evidence revision lineage".into(),
                ));
            }
            cursor = by_revision
                .get(revision_id)
                .and_then(|record| record.supersedes_revision_id.as_ref());
        }
    }
    Ok(())
}

fn availability(
    version: &EvidenceVersion,
    policy: PitPolicy,
) -> Result<(UtcTimestamp, String), LabError> {
    match (policy, &version.provenance) {
        (PitPolicy::StrictPit, EvidenceProvenance::ForwardCaptured { captured_at, .. }) => {
            let latest = latest_clock(
                version.declared_available_at,
                [
                    version.published_at,
                    version.first_seen_at,
                    version.content_updated_at,
                    Some(*captured_at),
                ],
            );
            Ok((latest, "STRICT_PIT_FORWARD_CAPTURE".into()))
        }
        (PitPolicy::StrictPit, EvidenceProvenance::ArchivedPublication { archived_at, .. }) => {
            let published_at = version.published_at.ok_or_else(|| {
                LabError::InvalidConfig(
                    "STRICT_PIT archived publication requires published_at".into(),
                )
            })?;
            if *archived_at < published_at {
                return Err(LabError::InvalidConfig(
                    "archive capture cannot precede claimed publication".into(),
                ));
            }
            let latest = latest_clock(
                version.declared_available_at,
                [version.published_at, version.content_updated_at],
            );
            Ok((latest, "STRICT_PIT_ARCHIVED_PUBLICATION".into()))
        }
        (PitPolicy::StrictPit, EvidenceProvenance::DeclaredLatest) => {
            Err(LabError::BlockedEvidence(
                "DECLARED_LATEST cannot establish STRICT_PIT availability".into(),
            ))
        }
        (PitPolicy::LatestVersionProxy, _) => {
            let latest = latest_clock(
                version.declared_available_at,
                [
                    version.published_at,
                    version.first_seen_at,
                    version.content_updated_at,
                    Some(version.registered_at),
                ],
            );
            Ok((latest, "LATEST_VERSION_PROXY_LATEST_KNOWN_CLOCK".into()))
        }
    }
}

fn latest_clock<const N: usize>(
    initial: UtcTimestamp,
    candidates: [Option<UtcTimestamp>; N],
) -> UtcTimestamp {
    candidates
        .into_iter()
        .flatten()
        .fold(initial, UtcTimestamp::max)
}

#[cfg(test)]
mod tests;
