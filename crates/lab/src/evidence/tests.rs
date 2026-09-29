use super::*;
use crate::contracts::{EvidenceId, EvidencePurpose};
use chrono::{TimeZone, Utc};

fn time(hour: u32) -> UtcTimestamp {
    UtcTimestamp(Utc.with_ymd_and_hms(2024, 1, 1, hour, 0, 0).unwrap())
}
fn market(code: &str) -> MarketId {
    MarketId::parse_upbit(code).unwrap()
}
fn weight(value: &str) -> Weight {
    Weight::new(value.parse().unwrap()).unwrap()
}

/// `SYNTHETIC_TEST_ONLY`: explicit hand-authored public-Evidence shape; no
/// performance or historical provenance claim.
fn version(
    revision: &str,
    event: &str,
    available_hour: u32,
    multiplier: &str,
    parent: Option<&str>,
) -> EvidenceVersion {
    let body = format!("synthetic body {revision}");
    let body_hash = ContentHash::of_bytes(body.as_bytes());
    EvidenceVersion {
        evidence_id: EvidenceId::new(format!("evidence-{event}")).unwrap(),
        revision_id: EvidenceRevisionId::new(revision).unwrap(),
        event_id: event.into(),
        purpose: EvidencePurpose::StrategyInput,
        category: "synthetic-boundary".into(),
        regime_label: None,
        markets: vec![market("KRW-BTC")],
        source_refs: vec!["https://example.invalid/synthetic".into()],
        body,
        body_hash: body_hash.clone(),
        event_time: Some(time(0)),
        published_at: Some(time(available_hour)),
        first_seen_at: Some(time(available_hour)),
        content_updated_at: None,
        declared_available_at: time(available_hour),
        registered_at: time(available_hour),
        valid_until: time(10),
        supersedes_revision_id: parent.map(|id| EvidenceRevisionId::new(id).unwrap()),
        provenance: EvidenceProvenance::ForwardCaptured {
            captured_at: time(available_hour),
            captured_body_hash: body_hash,
        },
        mapping_version: "synthetic-v1".into(),
        weight_multiplier: weight(multiplier),
    }
}

fn snapshot(versions: Vec<EvidenceVersion>) -> EvidenceSnapshot {
    build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions,
    })
    .unwrap()
}

#[test]
fn pit_boundaries_latest_lineage_minimum_and_scope_are_exact() {
    let first = version("revision-a1", "event-a", 1, "0.8", None);
    let second = version("revision-a2", "event-a", 3, "0.5", Some("revision-a1"));
    let independent = version("revision-b1", "event-b", 1, "0.7", None);
    let snapshot = snapshot(vec![second, independent, first]);
    assert!(
        snapshot
            .versions
            .windows(2)
            .all(|pair| pair[0].revision_id < pair[1].revision_id)
    );
    let evaluator = EvidenceEvaluator::new(Some(&snapshot), PitPolicy::StrictPit).unwrap();

    // available_at is inclusive and the later revision is not back-applied.
    let at_first = evaluator
        .effect(&market("KRW-BTC"), time(1), weight("1"), false)
        .unwrap();
    assert_eq!(at_first.final_target, weight("0.7"));
    assert_eq!(at_first.eligible.len(), 2);
    let at_second = evaluator
        .effect(&market("KRW-BTC"), time(3), weight("1"), false)
        .unwrap();
    assert_eq!(at_second.final_target, weight("0.5"));
    assert_eq!(at_second.eligible.len(), 2);

    // expiry is exclusive; unavailable and scope mismatch both map to cash.
    let expired = evaluator
        .effect(&market("KRW-BTC"), time(10), weight("1"), false)
        .unwrap();
    assert!(!expired.coverage_available);
    assert_eq!(expired.final_target, weight("0"));
    let wrong_scope = evaluator
        .effect(&market("KRW-ETH"), time(3), weight("1"), false)
        .unwrap();
    assert!(!wrong_scope.coverage_available);
    assert_eq!(wrong_scope.final_target, weight("0"));

    let control = evaluator
        .effect(&market("KRW-BTC"), time(3), weight("0.9"), true)
        .unwrap();
    assert_eq!(control.final_target, weight("0.9"));
    assert!(control.coverage_available);
}

#[test]
fn future_suffix_and_posthoc_context_cannot_rewrite_past_decisions() {
    let base = version("revision-a1", "event-a", 1, "0.8", None);
    let early_snapshot = snapshot(vec![base.clone()]);
    let future = version("revision-a2", "event-a", 5, "0.2", Some("revision-a1"));
    let mut posthoc = version("revision-review", "event-review", 2, "0.01", None);
    posthoc.purpose = EvidencePurpose::PosthocContext;
    posthoc.provenance = EvidenceProvenance::DeclaredLatest;
    let later_snapshot = snapshot(vec![base, future, posthoc]);
    let early = EvidenceEvaluator::new(Some(&early_snapshot), PitPolicy::StrictPit).unwrap();
    let later = EvidenceEvaluator::new(Some(&later_snapshot), PitPolicy::StrictPit).unwrap();
    let early_effect = early
        .effect(&market("KRW-BTC"), time(2), weight("1"), false)
        .unwrap();
    let later_effect = later
        .effect(&market("KRW-BTC"), time(2), weight("1"), false)
        .unwrap();
    assert_eq!(early_effect.final_target, later_effect.final_target);
    assert_eq!(early_effect.eligible.len(), later_effect.eligible.len());
}

#[test]
fn proxy_clock_strict_proof_and_lineage_conflicts_fail_closed() {
    let mut latest = version("revision-latest", "event-latest", 1, "0.4", None);
    latest.provenance = EvidenceProvenance::DeclaredLatest;
    latest.registered_at = time(4);
    let latest_snapshot = snapshot(vec![latest]);
    assert!(matches!(
        EvidenceEvaluator::new(Some(&latest_snapshot), PitPolicy::StrictPit),
        Err(LabError::BlockedEvidence(_))
    ));
    let proxy =
        EvidenceEvaluator::new(Some(&latest_snapshot), PitPolicy::LatestVersionProxy).unwrap();
    assert!(
        !proxy
            .effect(&market("KRW-BTC"), time(3), weight("1"), false)
            .unwrap()
            .coverage_available
    );
    assert!(
        proxy
            .effect(&market("KRW-BTC"), time(4), weight("1"), false)
            .unwrap()
            .coverage_available
    );

    let root = version("revision-root", "event-conflict", 1, "1", None);
    let left = version(
        "revision-left",
        "event-conflict",
        2,
        "0.8",
        Some("revision-root"),
    );
    let right = version(
        "revision-right",
        "event-conflict",
        2,
        "0.7",
        Some("revision-root"),
    );
    assert!(matches!(
        build_snapshot(EvidenceImport {
            public_non_sensitive_ack: true,
            versions: vec![root, left, right],
        }),
        Err(LabError::Conflict(_))
    ));
}

#[test]
fn archived_strict_clock_does_not_mix_later_forward_first_seen() {
    let mut archived = version("revision-archive", "event-archive", 1, "0.6", None);
    archived.first_seen_at = Some(time(9));
    archived.provenance = EvidenceProvenance::ArchivedPublication {
        archived_at: time(5),
        archive_ref: "https://example.invalid/archive/synthetic".into(),
        archived_body_hash: archived.body_hash.clone(),
    };
    let snapshot = snapshot(vec![archived]);
    let evaluator = EvidenceEvaluator::new(Some(&snapshot), PitPolicy::StrictPit).unwrap();
    let effect = evaluator
        .effect(&market("KRW-BTC"), time(2), weight("1"), false)
        .unwrap();
    assert!(effect.coverage_available);
    assert_eq!(effect.final_target, weight("0.6"));
    assert_eq!(effect.eligible[0].available_at, time(1));
}

#[test]
fn snapshot_body_proof_digest_and_duplicate_identity_are_validated_once() {
    let record = version("revision-one", "event-one", 1, "1", None);
    let idempotent = snapshot(vec![record.clone(), record.clone()]);
    assert_eq!(idempotent.versions.len(), 1);

    let mut conflicting = record.clone();
    conflicting.weight_multiplier = weight("0.5");
    assert!(matches!(
        build_snapshot(EvidenceImport {
            public_non_sensitive_ack: true,
            versions: vec![record.clone(), conflicting],
        }),
        Err(LabError::Conflict(_))
    ));

    let mut bad_body = record;
    bad_body.body.push_str(" changed");
    assert!(matches!(
        build_snapshot(EvidenceImport {
            public_non_sensitive_ack: true,
            versions: vec![bad_body]
        }),
        Err(LabError::InputHashMismatch(_))
    ));

    let mut bad_digest = idempotent;
    bad_digest.digest = ContentHash::of_bytes(b"wrong");
    assert!(matches!(
        EvidenceEvaluator::new(Some(&bad_digest), PitPolicy::StrictPit),
        Err(LabError::InputHashMismatch(_))
    ));
}
