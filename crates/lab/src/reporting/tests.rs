use super::{
    ReplayQualification, build_review, export_run, read_export_package, semantic_digest, verify_run,
};
use crate::contracts::*;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn f07_hand_accounting_fixture_passes_exact_independent_verification() {
    let bundle = fixture();
    let report = verify_run(&bundle);
    assert_eq!(
        report.status,
        ValidationStatus::Pass,
        "{:?}",
        report.findings
    );
    let final_mark = bundle.models[0]
        .account_marks
        .last()
        .expect("fixture final mark");
    assert_eq!(final_mark.state.cash_total.get(), dec("2077.90"));
    assert_eq!(final_mark.state.gross_realized.get(), dec("80"));
    assert_eq!(final_mark.state.cumulative_fees.get(), dec("2.10"));
    assert_eq!(final_mark.equity.get(), dec("2077.90"));
}

#[test]
fn price_cost_units_and_linked_episode_exit_reasons_are_explicit() {
    let bundle = fixture();
    let mut example = bundle.models[0].fills[0].clone();
    example.price_cost_attribution = signed("43000");
    example.qty = qty("0.07028686");
    assert_eq!(
        super::fill_price_cost_quote(&example)
            .expect("quantity-weighted quote cost")
            .get(),
        dec("3022.33498")
    );

    let review = build_review(&bundle).expect("review with typed cost and exit presentation");
    let model = &review.models[0];
    assert_eq!(
        model.costs.embedded_price_cost_attribution.get(),
        dec("200")
    );
    assert_eq!(
        model.costs.embedded_price_cost_attribution_unit,
        PriceCostAttributionUnit::Krw
    );
    assert_eq!(
        model.episode_summaries[0].exit_details.execution_result,
        Some(ReasonCode::MarketFilled)
    );
    assert_eq!(
        model.episode_summaries[0]
            .exit_details
            .strategy_exit_reasons,
        vec![ReasonCode::BandExit]
    );

    let mut terminal_episode = bundle.models[0].episodes[0].clone();
    terminal_episode.exit_reason = Some(ReasonCode::ArtificialTerminalExit);
    let mut terminal_fill = bundle.models[0].fills[1].clone();
    terminal_fill.artificial_terminal_exit = true;
    let terminal_order = bundle.models[0]
        .orders
        .iter()
        .find(|order| order.order_id == terminal_fill.order_id)
        .expect("terminal order");
    let mut terminal_signal = bundle.models[0]
        .signals
        .iter()
        .find(|signal| signal.signal_id == terminal_order.parent_signal_id)
        .expect("terminal signal")
        .clone();
    terminal_signal.reasons = vec![ReasonCode::ArtificialTerminalExit];
    let terminal_details = super::episode_exit_details(
        &terminal_episode,
        Some((&terminal_fill, terminal_order, &terminal_signal)),
    )
    .expect("terminal exit details");
    assert_eq!(
        terminal_details.execution_result,
        Some(ReasonCode::ArtificialTerminalExit)
    );
    assert_eq!(
        terminal_details.closing_signal_id,
        Some(terminal_signal.signal_id)
    );
    assert!(terminal_details.strategy_exit_reasons.is_empty());

    let mut legacy_json = serde_json::to_value(&review).expect("review JSON");
    let legacy_model = &mut legacy_json["models"][0];
    legacy_model["costs"]
        .as_object_mut()
        .expect("cost object")
        .remove("price_cost_schema_version");
    legacy_model["costs"]
        .as_object_mut()
        .expect("cost object")
        .remove("embedded_price_cost_attribution_unit");
    legacy_model["episode_summaries"][0]
        .as_object_mut()
        .expect("episode summary object")
        .remove("exit_details");
    let legacy: super::ReviewPayload =
        serde_json::from_value(legacy_json).expect("legacy review remains readable");
    assert_eq!(
        legacy.models[0].costs.embedded_price_cost_attribution_unit,
        PriceCostAttributionUnit::LegacySumKrwPerBaseUnit
    );
}

#[test]
fn passive_last_bar_fixture_requires_strict_penetration_and_frozen_lifecycle() {
    let bundle = passive_fixture();
    let report = verify_run(&bundle);
    assert_eq!(
        report.status,
        ValidationStatus::Pass,
        "{:?}",
        report.findings
    );
    let fill = &bundle.models[0].fills[0];
    assert!(
        matches!(&fill.timing, FillTiming::Interval { end, accounting_at, .. }
        if *end == bundle.plan.spec.range.end() && accounting_at == end)
    );

    let mut touched_only = bundle.clone();
    let source_id = touched_only.models[0].fills[0].source_bar_id.clone();
    let source = touched_only.datasets[0]
        .observations
        .iter_mut()
        .find(|row| row.id == source_id)
        .expect("passive source");
    source.candle.low = price("107.90");
    assert_failed_with(
        &touched_only,
        "passive fill fill-passive eligibility/size/lifecycle mismatch",
    );

    let mut crossing_open = bundle;
    let source_id = crossing_open.models[0].fills[0].source_bar_id.clone();
    let source = crossing_open.datasets[0]
        .observations
        .iter_mut()
        .find(|row| row.id == source_id)
        .expect("passive source");
    source.candle.open = price("107.91");
    assert_failed_with(
        &crossing_open,
        "passive fill fill-passive eligibility/size/lifecycle mismatch",
    );
}

#[test]
fn verifier_rejects_orphan_references_input_corruption_and_double_fee_debit() {
    let mut orphan = fixture();
    orphan.models[0].fills[0].order_id = OrderId::new("order-missing").expect("test ID");
    assert_failed_with(&orphan, "orphan order or episode link");

    let mut input = fixture();
    input.plan.dataset_digests[0].1 = ContentHash::of_bytes(b"corrupt");
    assert_failed_with(&input, "dataset digest/reference mismatch");

    let mut tolerance = fixture();
    tolerance.manifest.numeric_tolerance = "0.00000002".into();
    assert_failed_with(&tolerance, "numeric tolerance differs from the contract");

    let mut double_debit = fixture();
    let final_mark = double_debit.models[0]
        .account_marks
        .last_mut()
        .expect("fixture final mark");
    final_mark.equity = quote("2075.80");
    assert_failed_with(&double_debit, "equity identity / fee debit exactly once");

    let mut episode_corruption = fixture();
    episode_corruption.models[0].episodes[0].realized_price_pnl = signed("80.00000002");
    episode_corruption.models[0].episodes[0].net_realized = signed("77.90000002");
    assert_failed_with(&episode_corruption, "fill/basis/PnL conservation mismatch");

    let mut lifecycle = fixture();
    lifecycle.models[0].order_events[1].status = OrderStatus::Created;
    assert_failed_with(&lifecycle, "illegal or inconsistent lifecycle transition");

    let mut resized_order = fixture();
    resized_order.models[0].order_events[1].requested_qty = qty("11");
    assert_failed_with(
        &resized_order,
        "illegal or inconsistent lifecycle transition",
    );

    let mut retrospective_creation = fixture();
    retrospective_creation.models[0].order_events[0]
        .context
        .accounting_event_time = ts("2024-01-01T02:00:00Z");
    assert_failed_with(
        &retrospective_creation,
        "creation/latency does not follow its parent decision",
    );

    let mut sequence_gap = fixture();
    sequence_gap.models[0].signals[0].context.event_seq = 12;
    assert_failed_with(&sequence_gap, "event sequence has gaps");

    let mut wrong_execution_source = fixture();
    let wrong_source = wrong_execution_source.datasets[0].observations[2]
        .id
        .clone();
    wrong_execution_source.models[0].fills[0].source_bar_id = wrong_source;
    assert_failed_with(
        &wrong_execution_source,
        "timing does not match its execution source interval",
    );

    let mut future_liquidity = fixture();
    let future_source = future_liquidity.datasets[0].observations[5].id.clone();
    future_liquidity.models[0].fills[0].liquidity_source_bar_id = future_source;
    future_liquidity.models[0].fills[0].liquidity_source_close_time = ts("2024-01-01T05:00:00Z");
    assert_failed_with(&future_liquidity, "causal source/order/reference mismatch");

    let mut wrong_decision_reference = fixture();
    wrong_decision_reference.models[0].fills[0].decision_reference = price("101");
    assert_failed_with(
        &wrong_decision_reference,
        "causal source/order/reference mismatch",
    );
}

#[test]
fn frozen_policy_lineage_is_exported_and_trace_corruption_is_rejected() {
    let bundle = policy_fixture();
    let report = verify_run(&bundle);
    assert_eq!(
        report.status,
        ValidationStatus::Pass,
        "{:?}",
        report.findings
    );

    let root = unique_temp_root();
    fs::create_dir(&root).expect("create policy export root");
    let exported = export_run(&bundle, &root, None).expect("export frozen policy run");
    let package = read_export_package(&exported.directory).expect("read frozen policy export");
    assert_eq!(
        package.manifest.policy_revisions,
        bundle.plan.spec.policy_selections
    );

    let mut rejected = policy_fixture();
    rejected.plan.spec.market_rules.min_notional = quote("1000000");
    refresh_bundle_hashes(&mut rejected);
    let admission = rejected.plan.admissions[0].clone();
    rejected.models = vec![
        crate::engine::run_model(
            &rejected.plan,
            &rejected.datasets,
            rejected.evidence.as_ref(),
            &rejected.manifest.run_id,
            &admission,
            1,
            &|| false,
        )
        .expect("run frozen builtin with rejected orders"),
    ];
    refresh_bundle_hashes(&mut rejected);
    assert!(rejected.models[0].signals.iter().any(|signal| matches!(
        signal.outcome,
        SignalOutcome::CapitalBlocked | SignalOutcome::RuleBlocked | SignalOutcome::OrderUnfilled
    )));
    let rejected_report = verify_run(&rejected);
    assert_eq!(
        rejected_report.status,
        ValidationStatus::Pass,
        "{:?}",
        rejected_report.findings
    );

    let mut corrupt = bundle;
    corrupt.models[0].signals[0]
        .policy_trace
        .as_mut()
        .expect("policy trace")
        .state_after
        .push(NamedPolicyValue {
            id: "unfrozen_state".into(),
            value: 1.0,
        });
    assert_failed_with(&corrupt, "invalid builtin policy trace");
    fs::remove_dir_all(&root).expect("remove policy export root");
}

#[test]
fn semantic_digest_excludes_occurrence_metadata_but_retains_economic_timing() {
    let bundle = fixture();
    let expected = semantic_digest(&bundle).expect("semantic digest");
    assert_eq!(
        expected,
        super::verify::semantic_digest_from_full_clone(&bundle)
            .expect("full-clone semantic digest")
    );
    let mut occurrence = bundle.clone();
    occurrence.manifest.run_id = RunId::new("run-other").expect("test ID");
    occurrence.manifest.job_id = JobId::new("job-other").expect("test ID");
    occurrence.manifest.attempt_id = AttemptId::new("attempt-other").expect("test ID");
    occurrence.manifest.created_at = ts("2025-01-01T00:00:00Z");
    for signal in &mut occurrence.models[0].signals {
        signal.context.run_id = occurrence.manifest.run_id.clone();
    }
    for order in &mut occurrence.models[0].orders {
        order.context.run_id = occurrence.manifest.run_id.clone();
    }
    for order_event in &mut occurrence.models[0].order_events {
        order_event.context.run_id = occurrence.manifest.run_id.clone();
    }
    for fill in &mut occurrence.models[0].fills {
        fill.context.run_id = occurrence.manifest.run_id.clone();
    }
    for mark in &mut occurrence.models[0].account_marks {
        mark.context.run_id = occurrence.manifest.run_id.clone();
    }
    occurrence.models[0].episodes[0].run_id = occurrence.manifest.run_id.clone();
    assert_eq!(
        semantic_digest(&occurrence).expect("semantic digest"),
        expected
    );

    occurrence.models[0].fills[0].context.accounting_event_time = ts("2024-01-01T02:00:01Z");
    assert_ne!(
        semantic_digest(&occurrence).expect("semantic digest"),
        expected
    );
}

#[test]
fn review_separates_closed_open_and_emits_only_finite_or_typed_null_metrics() {
    let bundle = fixture();
    let review = build_review(&bundle).expect("review");
    assert_eq!(review.models[0].episodes.closed_count, 1);
    assert_eq!(review.models[0].episodes.open_count, 0);
    let json = serde_json::to_string(&review).expect("review JSON");
    assert!(!json.contains("NaN"));
    assert!(!json.contains("Infinity"));
    assert!(json.contains("NO_LOSING_EPISODES"));
}

#[test]
fn mixed_completed_and_blocked_models_require_empty_blocked_facts_and_null_metrics() {
    let mut bundle = fixture();
    let blocked_id = ModelId::new("model-btc-s5-blocked").expect("model ID");
    let blocked_strategy = StrategySpec::S5 {
        state: StateParameters {
            ema_length: 2,
            vol_length: 2,
            k: 1.0,
        },
    };
    let blocked = ModelLedger {
        model_id: blocked_id.clone(),
        market: bundle.models[0].market.clone(),
        strategy: blocked_strategy.clone().into(),
        status: ModelStatus::BlockedEvidence,
        status_reason: Some("eligible Evidence unavailable".into()),
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        episodes: Vec::new(),
        account_marks: Vec::new(),
        last_event_seq: bundle.models[0].last_event_seq,
    };
    bundle.plan.spec.strategies.push(blocked_strategy);
    bundle.plan.admissions.push(ModelAdmission {
        model_id: blocked_id,
        market: blocked.market.clone(),
        strategy: StrategyKind::S5,
        policy_ref: None,
        status: AdmissionStatus::BlockedEvidence,
        reasons: vec!["eligible Evidence unavailable".into()],
    });
    bundle.models.push(blocked);
    refresh_bundle_hashes(&mut bundle);

    let report = verify_run(&bundle);
    assert_eq!(
        report.status,
        ValidationStatus::Pass,
        "{:?}",
        report.findings
    );
    let review = build_review(&bundle).expect("review mixed model states");
    let blocked_review = review
        .models
        .iter()
        .find(|model| model.status == ModelStatus::BlockedEvidence)
        .expect("blocked review");
    assert!(blocked_review.equity.initial_equity.is_none());
    assert!(blocked_review.equity.total_return.value.is_none());

    let mut corrupt = bundle;
    let mut leaked_signal = corrupt.models[0].signals[0].clone();
    let blocked = corrupt.models.last_mut().expect("blocked model");
    leaked_signal.context.model_id = blocked.model_id.clone();
    blocked.signals.push(leaked_signal);
    assert_failed_with(
        &corrupt,
        "blocked/skipped ledger must contain only a bounded status reason",
    );
}

#[test]
fn review_groups_account_returns_by_recorded_active_regime_labels() {
    let mut bundle = fixture();
    let snapshot_id = EvidenceSnapshotId::new("evidence-snapshot-fixture").expect("test ID");
    let revision_id = EvidenceRevisionId::new("revision-bull").expect("test ID");
    let body = "synthetic bull regime";
    let version = EvidenceVersion {
        evidence_id: EvidenceId::new("evidence-bull").expect("test ID"),
        revision_id: revision_id.clone(),
        event_id: "event-bull".into(),
        purpose: EvidencePurpose::StrategyInput,
        category: "REGIME".into(),
        regime_label: Some("BULL".into()),
        markets: vec![bundle.models[0].market.clone()],
        source_refs: vec!["synthetic-fixture".into()],
        body: body.into(),
        body_hash: ContentHash::of_bytes(body.as_bytes()),
        event_time: Some(ts("2023-12-31T00:00:00Z")),
        published_at: Some(ts("2023-12-31T00:00:00Z")),
        first_seen_at: Some(ts("2023-12-31T00:00:00Z")),
        content_updated_at: None,
        declared_available_at: ts("2023-12-31T00:00:00Z"),
        registered_at: ts("2023-12-31T00:00:00Z"),
        valid_until: ts("2024-02-01T00:00:00Z"),
        supersedes_revision_id: None,
        provenance: EvidenceProvenance::DeclaredLatest,
        mapping_version: "mapping-v1".into(),
        weight_multiplier: weight("1"),
    };
    let snapshot = EvidenceSnapshot {
        id: snapshot_id.clone(),
        digest: ContentHash::of_value(&vec![version.clone()]).expect("evidence digest"),
        versions: vec![version],
    };
    bundle.plan.spec.evidence_snapshot_id = Some(snapshot_id.clone());
    bundle.plan.config_digest = ContentHash::of_value(&bundle.plan.spec).expect("config digest");
    bundle.plan.evidence_digest = Some(snapshot.digest.clone());
    bundle.evidence = Some(snapshot);
    bundle.models[0].signals[0].evidence_effect = Some(EvidenceDecisionEffect {
        policy: PitPolicy::LatestVersionProxy,
        snapshot_id: Some(snapshot_id),
        used_at: ts("2024-01-01T01:00:00Z"),
        eligible: vec![EligibleEvidence {
            revision_id,
            available_at: ts("2023-12-31T00:00:00Z"),
            valid_until: ts("2024-02-01T00:00:00Z"),
            multiplier: weight("1"),
            eligibility_reason: "DECLARED_LATEST_PROXY".into(),
        }],
        base_target: weight("1"),
        final_target: weight("1"),
        coverage_available: true,
        effect_changed_action: false,
        unavailable_policy: "CASH_WITH_MATCHED_CONTROL".into(),
    });
    bundle.plan.input_digest = super::verify::input_digest(&bundle).expect("input digest");
    bundle.semantic_digest = semantic_digest(&bundle).expect("semantic digest");

    let review = build_review(&bundle).expect("review");
    let regimes = &review.models[0].active_regime_returns;
    assert_eq!(regimes.len(), 1);
    assert_eq!(regimes[0].regime_label, "BULL");
    assert!(regimes[0].sampled_intervals >= 1);
    assert!(regimes[0].compounded_account_return.value.is_some());
    assert_eq!(review.models[0].active_regime_null_reason, None);

    let mut earlier = bundle.models[0].signals[0].clone();
    earlier.context.event_seq = 0;
    earlier.evidence_effect = None;
    bundle.models[0].signals.push(earlier);
    let out_of_order = build_review(&bundle).expect("out-of-order signal review");
    assert_eq!(
        out_of_order.models[0].active_regime_returns[0].regime_label,
        "BULL"
    );

    let mut same_sequence = bundle.models[0].signals[0].clone();
    same_sequence.evidence_effect = None;
    bundle.models[0].signals.push(same_sequence);
    assert!(
        build_review(&bundle)
            .expect("same-sequence signal review")
            .models[0]
            .active_regime_returns
            .is_empty(),
        "the last signal at an equal event sequence remains authoritative"
    );
}

#[test]
fn verifier_requires_complete_ordered_source_and_derived_dataset_closure() {
    let (bundle, source_id, derived_id) = derived_bundle_fixture();
    let report = verify_run(&bundle);
    assert_eq!(
        report.status,
        ValidationStatus::Pass,
        "{:?}",
        report.findings
    );

    let mut missing_source = bundle.clone();
    missing_source
        .datasets
        .retain(|dataset| dataset.manifest.id != source_id);
    missing_source
        .plan
        .spec
        .dataset_ids
        .retain(|id| id != &source_id);
    missing_source
        .plan
        .dataset_digests
        .retain(|(id, _)| id != &source_id);
    refresh_bundle_hashes(&mut missing_source);
    assert_failed_with(&missing_source, "constituent");

    let mut reordered = bundle.clone();
    let derived = reordered
        .datasets
        .iter_mut()
        .find(|dataset| dataset.manifest.id == derived_id)
        .expect("derived dataset");
    derived.observations[0].constituent_ids.swap(0, 1);
    assert_failed_with(
        &reordered,
        "ordered partition/aggregation/raw closure mismatch",
    );

    let mut altered = bundle;
    let derived = altered
        .datasets
        .iter_mut()
        .find(|dataset| dataset.manifest.id == derived_id)
        .expect("derived dataset");
    derived.observations[0].candle.close = price("199");
    assert_failed_with(
        &altered,
        "ordered partition/aggregation/raw closure mismatch",
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one export boundary test covers atomicity, qualification, hashes and corruption"
)]
fn export_is_deterministic_bounded_and_rejects_trailing_gzip_bytes() {
    let bundle = multi_market_fixture();
    let root = unique_temp_root();
    fs::create_dir(&root).expect("create test root");
    let legacy_export = root.join(format!("{}-full", bundle.manifest.run_id));
    fs::create_dir(&legacy_export).expect("create immutable legacy export fixture");
    let result = export_run(&bundle, &root, None).expect("export");
    assert!(legacy_export.exists());
    assert_ne!(result.directory, legacy_export);
    let round_trip = read_export_package(&result.directory).expect("read export");
    assert_eq!(round_trip.bundle.semantic_digest, bundle.semantic_digest);
    assert_eq!(result.manifest.artifacts.len(), 2);
    assert_eq!(result.artifacts.len(), 3);
    // Every packaged reference must already be the exact public lookup identity.
    let directory_name = result
        .directory
        .file_name()
        .expect("export name")
        .to_str()
        .expect("UTF-8");
    for reference in &result.manifest.artifacts {
        let catalog_path = format!("exports/{directory_name}/{}", reference.relative_path);
        assert_eq!(
            reference.id,
            ArtifactId::from_seed(&format!(
                "{}:{catalog_path}:{}",
                reference.run_id, reference.sha256
            ))
        );
    }
    let catalog = result
        .clone()
        .into_catalog_artifacts()
        .expect("qualify catalog paths");
    for reference in &result.manifest.artifacts {
        let published = catalog
            .iter()
            .find(|item| item.id == reference.id)
            .expect("same public ID");
        assert_eq!(published.sha256, reference.sha256);
        assert_eq!(published.bytes, reference.bytes);
    }
    let stale_staging = root.join(format!(
        ".{}-full-export-v4.attempt-{}",
        bundle.manifest.run_id, bundle.manifest.attempt_id
    ));
    fs::create_dir(&stale_staging).expect("create stale staging fixture");
    let retried = export_run(&bundle, &root, None).expect("idempotent export readback");
    assert_eq!(retried.directory, result.directory);
    assert_eq!(
        retried.manifest.semantic_digest,
        result.manifest.semantic_digest
    );
    assert_eq!(
        retried
            .artifacts
            .iter()
            .map(|artifact| (&artifact.relative_path, &artifact.sha256))
            .collect::<Vec<_>>(),
        result
            .artifacts
            .iter()
            .map(|artifact| (&artifact.relative_path, &artifact.sha256))
            .collect::<Vec<_>>()
    );
    assert!(stale_staging.exists());

    let mut incompatible = bundle.clone();
    incompatible
        .manifest
        .assumptions
        .push("different immutable export identity".into());
    incompatible.semantic_digest = semantic_digest(&incompatible).expect("semantic digest");
    assert!(matches!(
        export_run(&incompatible, &root, None),
        Err(LabError::Conflict(message)) if message.contains("immutable identity")
    ));
    let asset_result = export_run(
        &bundle,
        &root,
        Some(Asset::new("BTC").expect("asset symbol")),
    )
    .expect("asset export");
    let asset_package = read_export_package(&asset_result.directory).expect("read asset export");
    let full_ids: std::collections::BTreeSet<_> =
        result.artifacts.iter().map(|item| &item.id).collect();
    assert!(
        asset_result
            .artifacts
            .iter()
            .all(|item| !full_ids.contains(&item.id))
    );
    let mut mismatched = result.clone();
    mismatched.artifacts[0].id = asset_result.artifacts[0].id.clone();
    assert!(mismatched.into_catalog_artifacts().is_err());
    let asset_bundle = asset_package.bundle;
    assert_eq!(asset_bundle.models.len(), 1);
    assert_eq!(asset_bundle.datasets.len(), bundle.datasets.len());
    assert_eq!(asset_bundle.plan.config_digest, bundle.plan.config_digest);
    assert_eq!(asset_bundle.plan.input_digest, bundle.plan.input_digest);
    assert_eq!(asset_bundle.plan.spec.markets.len(), 2);
    assert_eq!(
        asset_package.manifest.replay_qualification,
        ReplayQualification::QualifiedAssetSubset
    );
    assert_eq!(asset_package.manifest.included_model_ids.len(), 1);
    assert_eq!(asset_package.manifest.excluded_model_ids.len(), 1);
    assert_eq!(
        asset_package.manifest.source_run_semantic_digest,
        bundle.semantic_digest
    );
    assert_ne!(asset_bundle.semantic_digest, bundle.semantic_digest);
    assert_eq!(verify_run(&asset_bundle).status, ValidationStatus::Fail);
    assert_eq!(
        asset_result.manifest.dataset_ids,
        result.manifest.dataset_ids
    );
    let manifest_path = result.directory.join("manifest.json");
    let mut versioned_manifest: super::ExportManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    let mut legacy_v3 = versioned_manifest.clone();
    legacy_v3.schema_version = "spot-lab-export-v3".into();
    for reference in &mut legacy_v3.artifacts {
        reference.id = ArtifactId::for_file(
            &reference.run_id,
            &reference.relative_path,
            &reference.sha256,
        );
    }
    fs::write(
        &manifest_path,
        serde_json::to_vec(&legacy_v3).expect("v3 JSON"),
    )
    .expect("v3 fixture");
    read_export_package(&result.directory).expect("immutable v3 remains readable");
    let review_path = result.directory.join("review.json");
    let original_review = fs::read(&review_path).expect("read review");
    let mut legacy_review: serde_json::Value =
        serde_json::from_slice(&original_review).expect("parse review");
    for model in legacy_review["models"]
        .as_array_mut()
        .expect("review models")
    {
        let costs = model["costs"].as_object_mut().expect("review costs");
        costs.remove("price_cost_schema_version");
        costs.remove("embedded_price_cost_attribution_unit");
        for episode in model["episode_summaries"]
            .as_array_mut()
            .expect("episode summaries")
        {
            episode
                .as_object_mut()
                .expect("episode summary")
                .remove("exit_details");
        }
    }
    let legacy_review = serde_json::to_vec(&legacy_review).expect("encode legacy review");
    fs::write(&review_path, &legacy_review).expect("write legacy review");
    let legacy_review_ref = versioned_manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.relative_path == "review.json")
        .expect("review artifact");
    legacy_review_ref.bytes = u64::try_from(legacy_review.len()).expect("legacy review length");
    legacy_review_ref.sha256 = ContentHash::of_bytes(&legacy_review);
    for version in ["spot-lab-export-v2", "spot-lab-export-v1"] {
        versioned_manifest.schema_version = version.into();
        let mut manifest_json =
            serde_json::to_value(&versioned_manifest).expect("serialize legacy manifest");
        let manifest_object = manifest_json.as_object_mut().expect("manifest object");
        manifest_object.remove("raw_fill_price_cost_unit");
        manifest_object.remove("review_price_cost_unit");
        if version == "spot-lab-export-v1" {
            manifest_object.remove("policy_revisions");
        }
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest_json).expect("encode legacy manifest"),
        )
        .expect("write legacy manifest");
        read_export_package(&result.directory).expect("read immutable legacy export version");
    }
    fs::write(&review_path, &original_review).expect("restore current review");
    let current_review_ref = versioned_manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.relative_path == "review.json")
        .expect("review artifact");
    current_review_ref.bytes = u64::try_from(original_review.len()).expect("review length");
    current_review_ref.sha256 = ContentHash::of_bytes(&original_review);
    versioned_manifest.schema_version = "spot-lab-export-v4".into();
    fs::write(
        &manifest_path,
        serde_json::to_vec(&versioned_manifest).expect("serialize current manifest"),
    )
    .expect("restore current manifest");
    let mut tampered_review: serde_json::Value =
        serde_json::from_slice(&original_review).expect("parse review");
    tampered_review["models"][0]["costs"]["embedded_price_cost_attribution"] =
        serde_json::json!("999");
    let tampered_review = serde_json::to_vec(&tampered_review).expect("encode tampered review");
    fs::write(&review_path, &tampered_review).expect("write tampered review");
    let mut review_manifest: super::ExportManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    let review_ref = review_manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.relative_path == "review.json")
        .expect("review artifact");
    review_ref.bytes = u64::try_from(tampered_review.len()).expect("tampered review length");
    review_ref.sha256 = ContentHash::of_bytes(&tampered_review);
    review_ref.id = ArtifactId::for_file(
        &review_ref.run_id,
        &format!("exports/{directory_name}/{}", review_ref.relative_path),
        &review_ref.sha256,
    );
    fs::write(
        &manifest_path,
        serde_json::to_vec(&review_manifest).expect("encode tampered manifest"),
    )
    .expect("write tampered manifest");
    assert!(matches!(
        read_export_package(&result.directory),
        Err(LabError::InputHashMismatch(message)) if message.contains("review differs")
    ));
    fs::write(&review_path, &original_review).expect("restore review");
    let review_ref = review_manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.relative_path == "review.json")
        .expect("review artifact");
    review_ref.bytes = u64::try_from(original_review.len()).expect("review length");
    review_ref.sha256 = ContentHash::of_bytes(&original_review);
    review_ref.id = ArtifactId::for_file(
        &review_ref.run_id,
        &format!("exports/{directory_name}/{}", review_ref.relative_path),
        &review_ref.sha256,
    );
    fs::write(
        &manifest_path,
        serde_json::to_vec(&review_manifest).expect("restore manifest"),
    )
    .expect("write restored manifest");
    let gzip_path = result.directory.join("ledger.json.gz");
    let mut header = [0_u8; 10];
    fs::File::open(&gzip_path)
        .expect("open gzip")
        .read_exact(&mut header)
        .expect("read gzip header");
    assert_eq!(&header[4..8], &[0, 0, 0, 0]);
    assert_eq!(header[9], 255);
    OpenOptions::new()
        .append(true)
        .open(&gzip_path)
        .expect("open gzip append")
        .write_all(b"trailing")
        .expect("append trailing bytes");
    let compressed = fs::read(&gzip_path).expect("read corrupted gzip");
    let mut manifest: super::ExportManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    let ledger_ref = manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.relative_path == "ledger.json.gz")
        .expect("ledger artifact");
    ledger_ref.bytes = u64::try_from(compressed.len()).expect("test length");
    ledger_ref.sha256 = ContentHash::of_bytes(&compressed);
    ledger_ref.id = ArtifactId::for_file(
        &ledger_ref.run_id,
        &format!("exports/{directory_name}/{}", ledger_ref.relative_path),
        &ledger_ref.sha256,
    );
    fs::write(
        manifest_path,
        serde_json::to_vec(&manifest).expect("serialize manifest"),
    )
    .expect("rewrite manifest");
    let error = read_export_package(&result.directory).expect_err("trailing gzip must fail");
    assert!(matches!(error, LabError::DataCorrupt(message) if message.contains("trailing")));
    assert!(matches!(
        export_run(&bundle, &root, None),
        Err(LabError::DataCorrupt(message)) if message.contains("trailing")
    ));

    let staging_only_root = unique_temp_root();
    fs::create_dir(&staging_only_root).expect("create staging-only root");
    let staging_only = staging_only_root.join(format!(
        ".{}-full-export-v4.attempt-{}",
        bundle.manifest.run_id, bundle.manifest.attempt_id
    ));
    fs::create_dir(&staging_only).expect("create staging-only fixture");
    assert!(matches!(
        export_run(&bundle, &staging_only_root, None),
        Err(LabError::Conflict(message)) if message.contains("already exists")
    ));
    assert!(staging_only.exists());
    fs::remove_dir_all(&staging_only_root).expect("remove staging-only root");
    fs::remove_dir_all(&root).expect("remove test root");
}

fn assert_failed_with(bundle: &RunBundle, expected: &str) {
    let report = verify_run(bundle);
    assert_eq!(report.status, ValidationStatus::Fail);
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.contains(expected)),
        "{:?}",
        report.findings
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "the hand-computed fixture keeps every linked F08 fact explicit"
)]
fn fixture() -> RunBundle {
    let run_id = RunId::new("run-f07").expect("test ID");
    let model_id = ModelId::new("model-btc-bh").expect("test ID");
    let market = MarketId::parse_upbit("KRW-BTC").expect("test market");
    let episode_id = EpisodeId::new("episode-roundtrip").expect("test ID");
    let buy_signal = SignalId::new("signal-buy").expect("test ID");
    let sell_signal = SignalId::new("signal-sell").expect("test ID");
    let buy_order = OrderId::new("order-buy").expect("test ID");
    let sell_order = OrderId::new("order-sell").expect("test ID");
    let buy_fill = FillId::new("fill-buy").expect("test ID");
    let sell_fill = FillId::new("fill-sell").expect("test ID");
    let rule_id = RuleSnapshotId::new("rule-fixture").expect("test ID");
    let range =
        UtcRange::new(ts("2024-01-01T00:00:00Z"), ts("2024-01-01T05:00:00Z")).expect("test range");
    let coverage = range
        .with_warmup(1, CandleInterval::H1)
        .expect("fixture coverage");
    let raw_object = fixture_raw_object("fixture-btc-raw");
    let placeholder = ContentHash::of_bytes(b"unpublished-fixture");
    let mut dataset = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: "1.0".into(),
            id: DatasetId::from_seed("unpublished-fixture"),
            request: CollectRequest {
                request_id: RequestId::new("request-collect").expect("test ID"),
                markets: vec![market.clone()],
                range,
                data_resolution: CandleInterval::H1,
                warmup_bars: 1,
                completed_only: true,
            },
            coverage,
            status: DatasetStatus::Ready,
            row_count: 6,
            normalizer_version: NORMALIZER_VERSION.into(),
            gap_policy: "REJECT".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: MarketDataOrigin::SyntheticTestOnly,
            raw_objects: vec![raw_object.clone()],
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations: vec![
            candle_observation(
                &market,
                "2023-12-31T23:00:00Z",
                "2024-01-01T00:00:00Z",
                "99",
                "99",
                "900",
                &raw_object.id,
            ),
            candle_observation(
                &market,
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "99",
                "100",
                "800",
                &raw_object.id,
            ),
            candle_observation(
                &market,
                "2024-01-01T01:00:00Z",
                "2024-01-01T02:00:00Z",
                "100",
                "101",
                "500",
                &raw_object.id,
            ),
            candle_observation(
                &market,
                "2024-01-01T02:00:00Z",
                "2024-01-01T03:00:00Z",
                "101",
                "110",
                "600",
                &raw_object.id,
            ),
            candle_observation(
                &market,
                "2024-01-01T03:00:00Z",
                "2024-01-01T04:00:00Z",
                "110",
                "109",
                "400",
                &raw_object.id,
            ),
            candle_observation(
                &market,
                "2024-01-01T04:00:00Z",
                "2024-01-01T05:00:00Z",
                "109",
                "109",
                "700",
                &raw_object.id,
            ),
        ],
    };
    let buy_decision_bar = dataset.observations[1].id.clone();
    let buy_liquidity_bar = dataset.observations[2].id.clone();
    let buy_execution_bar = dataset.observations[3].id.clone();
    let sell_liquidity_bar = dataset.observations[4].id.clone();
    let sell_execution_bar = dataset.observations[5].id.clone();
    let dataset_digests = crate::contracts::dataset_digests(&dataset).expect("dataset digests");
    let dataset_id = crate::contracts::dataset_id(&dataset_digests);
    dataset.manifest.id = dataset_id.clone();
    dataset.manifest.semantic_digest = dataset_digests.semantic.clone();
    dataset.manifest.provenance_digest = dataset_digests.provenance;
    let spec = ExperimentSpec {
        schema_version: SCHEMA_VERSION.into(),
        causal_execution: None,
        dataset_ids: vec![dataset_id.clone()],
        markets: vec![market.clone()],
        range,
        strategies: vec![StrategySpec::BuyAndHold],
        policy_selections: Vec::new(),
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 3_600_000,
        initial_cash: quote("2000"),
        costs: CostPolicy {
            buy_fee_bps: bps("10"),
            sell_fee_bps: bps("10"),
            maker_fee_bps: bps("10"),
            half_spread_bps: bps("0"),
            slippage_bps: bps("100"),
            impact_bps: bps("0"),
            assumption_label: "SYNTHETIC_TEST_ONLY F07 fixture".into(),
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: weight("1"),
        },
        market_rules: MarketRuleSnapshot {
            id: rule_id.clone(),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: range,
            observed_at: ts("2024-01-01T00:00:00Z"),
            source_refs: vec!["synthetic-fixture".into()],
            assumption_label: "SYNTHETIC_TEST_ONLY".into(),
            min_notional: quote("1"),
            quantity_step: qty("0.01"),
            ticks: vec![TickBand {
                lower_bound: quote("0"),
                tick: price("0.01"),
            }],
        },
        terminal_policy: TerminalPolicy::MarkToMarket,
        evidence_snapshot_id: None,
        pit_policy: PitPolicy::StrictPit,
        evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 1,
            risk_free_annual: 0.0,
        },
        seed: 7,
    };
    let config_digest = ContentHash::of_value(&spec).expect("config digest");
    let mut plan = ResolvedPlan {
        id: PlanId::new("plan-fixture").expect("test ID"),
        spec,
        config_digest,
        input_digest: ContentHash::of_bytes(b"pending"),
        dataset_digests: vec![(dataset_id, dataset_digests.semantic)],
        evidence_digest: None,
        admissions: vec![ModelAdmission {
            model_id: model_id.clone(),
            market: market.clone(),
            strategy: StrategyKind::BuyAndHold,
            policy_ref: None,
            status: AdmissionStatus::Eligible,
            reasons: Vec::new(),
        }],
        policy_revisions: Vec::new(),
        warnings: vec!["SYNTHETIC_TEST_ONLY".into()],
        estimated_events: 16,
    };
    plan.input_digest = ContentHash::of_value(&serde_json::json!({
        "config_digest": plan.config_digest,
        "dataset_digests": plan.dataset_digests,
        "evidence_digest": plan.evidence_digest,
    }))
    .expect("input digest");
    let context = |event_seq: u64, at: &str| -> EventContext {
        EventContext {
            run_id: run_id.clone(),
            model_id: model_id.clone(),
            market: market.clone(),
            event_seq,
            accounting_event_time: ts(at),
        }
    };
    let indicator = || IndicatorSnapshot {
        version: INDICATOR_VERSION.into(),
        ema: None,
        bar_sigma: None,
        annual_vol: None,
        rsi: None,
        entry_high: None,
        exit_low: None,
        completed_bars_seen: 1,
        policy_values: Vec::new(),
    };
    let signals = vec![
        SignalRecord {
            context: context(3, "2024-01-01T01:00:00Z"),
            signal_id: buy_signal.clone(),
            strategy: StrategyKind::BuyAndHold,
            strategy_version: "bh-v1".into(),
            policy_ref: None,
            policy_trace: None,
            source_bar_ids: vec![buy_decision_bar.clone()],
            signal_time: ts("2024-01-01T01:00:00Z"),
            decision_available_at: ts("2024-01-01T01:00:00Z"),
            valid_until: ts("2024-01-01T03:00:00Z"),
            state_before: PositionState::Cash,
            state_after: PositionState::Long,
            raw_target_weight: weight("1"),
            constrained_target_weight: weight("1"),
            intended_side: Some(Side::Buy),
            reasons: vec![ReasonCode::StartFromCash],
            indicators: indicator(),
            evidence_effect: None,
            outcome: SignalOutcome::Executed,
        },
        SignalRecord {
            context: context(10, "2024-01-01T03:00:00Z"),
            signal_id: sell_signal.clone(),
            strategy: StrategyKind::BuyAndHold,
            strategy_version: "bh-v1".into(),
            policy_ref: None,
            policy_trace: None,
            source_bar_ids: vec![buy_execution_bar.clone()],
            signal_time: ts("2024-01-01T03:00:00Z"),
            decision_available_at: ts("2024-01-01T03:00:00Z"),
            valid_until: ts("2024-01-01T05:00:00Z"),
            state_before: PositionState::Long,
            state_after: PositionState::Cash,
            raw_target_weight: weight("0"),
            constrained_target_weight: weight("0"),
            intended_side: Some(Side::Sell),
            reasons: vec![ReasonCode::BandExit],
            indicators: indicator(),
            evidence_effect: None,
            outcome: SignalOutcome::Executed,
        },
    ];
    let buy_created = OrderRecord {
        context: context(4, "2024-01-01T01:00:00Z"),
        order_id: buy_order.clone(),
        parent_signal_id: buy_signal,
        episode_id: Some(episode_id.clone()),
        side: Side::Buy,
        order_type: SimulatedOrderType::Market,
        requested_price: None,
        requested_qty: qty("10"),
        created_at: ts("2024-01-01T01:00:00Z"),
        effective_at: ts("2024-01-01T02:00:00Z"),
        expires_at: ts("2024-01-01T03:00:00Z"),
        rule_snapshot_id: rule_id.clone(),
        policy_version: "next-open-v1".into(),
        reserved_cash: quote("1011.01"),
        cumulative_filled_qty: qty("0"),
        status: OrderStatus::Created,
        reason: ReasonCode::StartFromCash,
        order_origin: "SIMULATED".into(),
    };
    let mut buy_final = buy_created.clone();
    buy_final.context = context(6, "2024-01-01T02:00:00Z");
    buy_final.cumulative_filled_qty = qty("10");
    buy_final.status = OrderStatus::Filled;
    buy_final.reason = ReasonCode::MarketFilled;
    let sell_created = OrderRecord {
        context: context(11, "2024-01-01T03:00:00Z"),
        order_id: sell_order.clone(),
        parent_signal_id: sell_signal,
        episode_id: Some(episode_id.clone()),
        side: Side::Sell,
        order_type: SimulatedOrderType::Market,
        requested_price: None,
        requested_qty: qty("10"),
        created_at: ts("2024-01-01T03:00:00Z"),
        effective_at: ts("2024-01-01T04:00:00Z"),
        expires_at: ts("2024-01-01T05:00:00Z"),
        rule_snapshot_id: rule_id,
        policy_version: "next-open-v1".into(),
        reserved_cash: quote("0"),
        cumulative_filled_qty: qty("0"),
        status: OrderStatus::Created,
        reason: ReasonCode::BandExit,
        order_origin: "SIMULATED".into(),
    };
    let mut sell_final = sell_created.clone();
    sell_final.context = context(13, "2024-01-01T04:00:00Z");
    sell_final.cumulative_filled_qty = qty("10");
    sell_final.status = OrderStatus::Filled;
    sell_final.reason = ReasonCode::MarketFilled;
    let orders = vec![buy_final.clone(), sell_final.clone()];
    let order_events = vec![buy_created, buy_final, sell_created, sell_final];
    let fills = vec![
        FillRecord {
            context: context(7, "2024-01-01T02:00:00Z"),
            fill_id: buy_fill.clone(),
            order_id: buy_order.clone(),
            episode_id: episode_id.clone(),
            side: Side::Buy,
            price: price("101"),
            qty: qty("10"),
            notional: quote("1010"),
            fee: quote("1.01"),
            fee_bps: bps("10"),
            fee_currency: "KRW".into(),
            timing: FillTiming::Exact {
                at: ts("2024-01-01T02:00:00Z"),
            },
            source_bar_id: buy_execution_bar.clone(),
            liquidity_source_bar_id: buy_liquidity_bar.clone(),
            liquidity_source_close_time: ts("2024-01-01T02:00:00Z"),
            decision_reference: price("100"),
            bar_open_proxy: price("101"),
            arrival_mid: None,
            arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
            adverse_slippage_bps: signed("100"),
            price_cost_attribution: signed("10"),
            liquidity_role_assumption: "TAKER_SCENARIO".into(),
            execution_origin: "SIMULATED_ONLY".into(),
            fill_observed: false,
            model_version: "execution-v1".into(),
            artificial_terminal_exit: false,
            accounting_mark_seq: 8,
        },
        FillRecord {
            context: context(14, "2024-01-01T04:00:00Z"),
            fill_id: sell_fill.clone(),
            order_id: sell_order.clone(),
            episode_id: episode_id.clone(),
            side: Side::Sell,
            price: price("109"),
            qty: qty("10"),
            notional: quote("1090"),
            fee: quote("1.09"),
            fee_bps: bps("10"),
            fee_currency: "KRW".into(),
            timing: FillTiming::Exact {
                at: ts("2024-01-01T04:00:00Z"),
            },
            source_bar_id: sell_execution_bar.clone(),
            liquidity_source_bar_id: sell_liquidity_bar.clone(),
            liquidity_source_close_time: ts("2024-01-01T04:00:00Z"),
            decision_reference: price("110"),
            bar_open_proxy: price("109"),
            arrival_mid: None,
            arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
            adverse_slippage_bps: signed("90.9090909091"),
            price_cost_attribution: signed("10"),
            liquidity_role_assumption: "TAKER_SCENARIO".into(),
            execution_origin: "SIMULATED_ONLY".into(),
            fill_observed: false,
            model_version: "execution-v1".into(),
            artificial_terminal_exit: false,
            accounting_mark_seq: 15,
        },
    ];
    let state = |cash, qty_value, basis, gross, fees| AccountState {
        cash_total: quote(cash),
        cash_free: quote(cash),
        cash_reserved: quote("0"),
        qty: qty(qty_value),
        price_basis: quote(basis),
        gross_realized: signed(gross),
        cumulative_fees: quote(fees),
    };
    let marks = vec![
        AccountMark {
            context: context(1, "2024-01-01T00:00:00Z"),
            kind: MarkKind::Initial,
            state: state("2000", "0", "0", "0", "0"),
            mark_price: price("100"),
            source_bar_id: buy_decision_bar.clone(),
            position_value: quote("0"),
            gross_unrealized: signed("0"),
            equity: quote("2000"),
            target_weight: weight("0"),
            actual_weight: weight("0"),
            peak_equity: quote("2000"),
            drawdown: weight("0"),
        },
        AccountMark {
            context: context(2, "2024-01-01T01:00:00Z"),
            kind: MarkKind::ExecutionClose,
            state: state("2000", "0", "0", "0", "0"),
            mark_price: price("100"),
            source_bar_id: buy_decision_bar,
            position_value: quote("0"),
            gross_unrealized: signed("0"),
            equity: quote("2000"),
            target_weight: weight("0"),
            actual_weight: weight("0"),
            peak_equity: quote("2000"),
            drawdown: weight("0"),
        },
        AccountMark {
            context: context(5, "2024-01-01T02:00:00Z"),
            kind: MarkKind::ExecutionClose,
            state: AccountState {
                cash_total: quote("2000"),
                cash_free: quote("988.99"),
                cash_reserved: quote("1011.01"),
                qty: qty("0"),
                price_basis: quote("0"),
                gross_realized: signed("0"),
                cumulative_fees: quote("0"),
            },
            mark_price: price("101"),
            source_bar_id: buy_liquidity_bar,
            position_value: quote("0"),
            gross_unrealized: signed("0"),
            equity: quote("2000"),
            target_weight: weight("1"),
            actual_weight: weight("0"),
            peak_equity: quote("2000"),
            drawdown: weight("0"),
        },
        AccountMark {
            context: context(8, "2024-01-01T02:00:00Z"),
            kind: MarkKind::AfterFill,
            state: state("988.99", "10", "1010", "0", "1.01"),
            mark_price: price("101"),
            source_bar_id: buy_execution_bar.clone(),
            position_value: quote("1010"),
            gross_unrealized: signed("0"),
            equity: quote("1998.99"),
            target_weight: weight("1"),
            actual_weight: weight("0.5052551538526956112836982676"),
            peak_equity: quote("2000"),
            drawdown: weight("0.000505"),
        },
        AccountMark {
            context: context(9, "2024-01-01T03:00:00Z"),
            kind: MarkKind::ExecutionClose,
            state: state("988.99", "10", "1010", "0", "1.01"),
            mark_price: price("110"),
            source_bar_id: buy_execution_bar,
            position_value: quote("1100"),
            gross_unrealized: signed("90"),
            equity: quote("2088.99"),
            target_weight: weight("1"),
            actual_weight: weight_ratio("1100", "2088.99"),
            peak_equity: quote("2088.99"),
            drawdown: weight("0"),
        },
        AccountMark {
            context: context(12, "2024-01-01T04:00:00Z"),
            kind: MarkKind::ExecutionClose,
            state: state("988.99", "10", "1010", "0", "1.01"),
            mark_price: price("109"),
            source_bar_id: sell_liquidity_bar,
            position_value: quote("1090"),
            gross_unrealized: signed("80"),
            equity: quote("2078.99"),
            target_weight: weight("0"),
            actual_weight: weight_ratio("1090", "2078.99"),
            peak_equity: quote("2088.99"),
            drawdown: weight_ratio("10", "2088.99"),
        },
        AccountMark {
            context: context(15, "2024-01-01T04:00:00Z"),
            kind: MarkKind::AfterFill,
            state: state("2077.90", "0", "0", "80", "2.10"),
            mark_price: price("109"),
            source_bar_id: sell_execution_bar.clone(),
            position_value: quote("0"),
            gross_unrealized: signed("0"),
            equity: quote("2077.90"),
            target_weight: weight("0"),
            actual_weight: weight("0"),
            peak_equity: quote("2088.99"),
            drawdown: weight_ratio("11.09", "2088.99"),
        },
        AccountMark {
            context: context(16, "2024-01-01T05:00:00Z"),
            kind: MarkKind::ExecutionClose,
            state: state("2077.90", "0", "0", "80", "2.10"),
            mark_price: price("109"),
            source_bar_id: sell_execution_bar,
            position_value: quote("0"),
            gross_unrealized: signed("0"),
            equity: quote("2077.90"),
            target_weight: weight("0"),
            actual_weight: weight("0"),
            peak_equity: quote("2088.99"),
            drawdown: weight_ratio("11.09", "2088.99"),
        },
    ];
    let episodes = vec![EpisodeRecord {
        episode_id,
        run_id: run_id.clone(),
        model_id: model_id.clone(),
        market: market.clone(),
        status: EpisodeStatus::Closed,
        opened_at: ts("2024-01-01T02:00:00Z"),
        closed_at: Some(ts("2024-01-01T04:00:00Z")),
        fill_ids: vec![buy_fill, sell_fill],
        order_ids: vec![buy_order, sell_order],
        buy_vwap: price("101"),
        sell_vwap: Some(price("109")),
        max_qty: qty("10"),
        time_weighted_avg_qty: qty("10"),
        holding_seconds: 7200,
        realized_price_pnl: signed("80"),
        fees: quote("2.10"),
        net_realized: signed("77.90"),
        residual_basis: quote("0"),
        residual_qty: qty("0"),
        marked_unrealized: signed("0"),
        exit_reason: Some(ReasonCode::MarketFilled),
        start_equity: quote("2000"),
        mae_amount: signed("-1.01"),
        mfe_amount: signed("88.99"),
        mae_pct_of_start_equity: signed("-0.000505"),
        mfe_pct_of_start_equity: signed("0.044495"),
        time_to_mae_seconds: 0,
        time_to_mfe_seconds: 3600,
        sampling_definition: "execution-close net PnL path".into(),
        exit_peak_giveback: quote("11.09"),
    }];
    let model = ModelLedger {
        model_id,
        market,
        strategy: StrategySpec::BuyAndHold.into(),
        status: ModelStatus::Completed,
        status_reason: None,
        signals,
        orders,
        order_events,
        fills,
        episodes,
        account_marks: marks,
        last_event_seq: 16,
    };
    let manifest = RunManifest {
        schema_version: SCHEMA_VERSION.into(),
        run_id,
        job_id: JobId::new("job-f07").expect("test ID"),
        attempt_id: AttemptId::new("attempt-f07").expect("test ID"),
        created_at: ts("2024-01-02T00:00:00Z"),
        code_revision: Some("fixture".into()),
        source_digest: ContentHash::of_bytes(b"source"),
        lockfile_digest: ContentHash::of_bytes(b"lock"),
        toolchain: "1.98.0".into(),
        engine_version: ENGINE_VERSION.into(),
        indicator_version: INDICATOR_VERSION.into(),
        rounding_version: ROUNDING_VERSION.into(),
        mode: "HISTORICAL_REPLAY".into(),
        historical_availability_model: "BAR_CLOSE_ASSUMED".into(),
        execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false,
        origin: MarketDataOrigin::SyntheticTestOnly,
        numeric_tolerance: crate::contracts::NUMERIC_TOLERANCE.normalize().to_string(),
        metric_tolerance: 1e-12,
        start_policy: "START_FROM_CASH".into(),
        account_mode: "INDEPENDENT_ASSET_STRATEGY".into(),
        assumptions: vec!["SYNTHETIC_TEST_ONLY".into()],
    };
    let mut bundle = RunBundle {
        manifest,
        plan,
        datasets: vec![dataset],
        evidence: None,
        models: vec![model],
        semantic_digest: ContentHash::of_bytes(b"pending-semantic"),
    };
    bundle.semantic_digest = semantic_digest(&bundle).expect("semantic digest");
    bundle
}

pub(crate) fn policy_fixture() -> RunBundle {
    let mut bundle = fixture();
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "Editable S1".into(),
        description: "S1 family metadata with a frozen buy-and-hold program".into(),
        program: PolicyProgram::Builtin {
            strategy: StrategySpec::BuyAndHold,
        },
    };
    let reference = PolicyRevisionRef {
        policy_id: PolicyId::new("policy-editable-s1").expect("policy ID"),
        revision_id: PolicyRevisionId::new("policy-editable-s1-r1").expect("revision ID"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    };
    bundle.plan.spec.schema_version = "2.0".into();
    bundle.plan.spec.strategies.clear();
    bundle.plan.spec.policy_selections = vec![reference.clone()];
    bundle.plan.policy_revisions = vec![FrozenPolicyRevision {
        reference: reference.clone(),
        revision_number: 1,
        parent_revision_id: None,
        family: StrategyKind::S1,
        origin: PolicyOrigin::User,
        definition,
    }];
    bundle.plan.admissions[0].strategy = StrategyKind::S1;
    bundle.plan.admissions[0].policy_ref = Some(reference.clone());
    bundle.models[0].strategy = StrategyBinding::Policy {
        reference: reference.clone(),
        family: StrategyKind::S1,
    };
    for signal in &mut bundle.models[0].signals {
        signal.strategy = StrategyKind::S1;
        signal.policy_ref = Some(reference.clone());
        signal.policy_trace = Some(PolicyTrace {
            matched_rule_id: None,
            state_before: Vec::new(),
            state_after: Vec::new(),
            evidence: None,
        });
    }
    refresh_bundle_hashes(&mut bundle);
    bundle
}

#[allow(
    clippy::too_many_lines,
    reason = "one engine-free passive fixture keeps timing, cash and lifecycle facts explicit"
)]
fn passive_fixture() -> RunBundle {
    let mut bundle = fixture();
    bundle.plan.spec.latency_ms = 0;
    bundle.plan.spec.execution = ExecutionPolicy::PassiveBuy {
        offset_bps: bps("100"),
        penetration_ticks: 1,
        fill_fraction: PassiveFraction::Half,
        ttl_execution_bars: 1,
        participation_cap: weight("1"),
    };
    bundle.plan.spec.costs.maker_fee_bps = bps("10");
    let run_id = bundle.manifest.run_id.clone();
    let model_id = bundle.models[0].model_id.clone();
    let market = bundle.models[0].market.clone();
    let decision_bar = bundle.datasets[0].observations[4].id.clone();
    let execution_bar = bundle.datasets[0].observations[5].id.clone();
    let signal_id = SignalId::new("signal-passive").expect("signal ID");
    let order_id = OrderId::new("order-passive").expect("order ID");
    let fill_id = FillId::new("fill-passive").expect("fill ID");
    let episode_id = EpisodeId::new("episode-passive").expect("episode ID");
    let context = |event_seq: u64, at: &str| -> EventContext {
        EventContext {
            run_id: run_id.clone(),
            model_id: model_id.clone(),
            market: market.clone(),
            event_seq,
            accounting_event_time: ts(at),
        }
    };
    let signal = SignalRecord {
        context: context(6, "2024-01-01T04:00:00Z"),
        signal_id: signal_id.clone(),
        strategy: StrategyKind::BuyAndHold,
        strategy_version: "bh-v1".into(),
        policy_ref: None,
        policy_trace: None,
        source_bar_ids: vec![decision_bar.clone()],
        signal_time: ts("2024-01-01T04:00:00Z"),
        decision_available_at: ts("2024-01-01T04:00:00Z"),
        valid_until: ts("2024-01-01T05:00:00Z"),
        state_before: PositionState::Cash,
        state_after: PositionState::Long,
        raw_target_weight: weight("0.5"),
        constrained_target_weight: weight("0.5"),
        intended_side: Some(Side::Buy),
        reasons: vec![ReasonCode::BandEntry],
        indicators: IndicatorSnapshot {
            version: INDICATOR_VERSION.into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: 4,
            policy_values: Vec::new(),
        },
        evidence_effect: None,
        outcome: SignalOutcome::Executed,
    };
    let created = OrderRecord {
        context: context(7, "2024-01-01T04:00:00Z"),
        order_id: order_id.clone(),
        parent_signal_id: signal_id,
        episode_id: Some(episode_id.clone()),
        side: Side::Buy,
        order_type: SimulatedOrderType::PassiveBuyLimit,
        requested_price: Some(price("107.91")),
        requested_qty: qty("10"),
        created_at: ts("2024-01-01T04:00:00Z"),
        effective_at: ts("2024-01-01T04:00:00Z"),
        expires_at: ts("2024-01-01T05:00:00Z"),
        rule_snapshot_id: bundle.plan.spec.market_rules.id.clone(),
        policy_version: "passive-buy-v1".into(),
        reserved_cash: quote("1080.17910"),
        cumulative_filled_qty: qty("0"),
        status: OrderStatus::Created,
        reason: ReasonCode::BandEntry,
        order_origin: "SIMULATED".into(),
    };
    let mut partial = created.clone();
    partial.context = context(8, "2024-01-01T05:00:00Z");
    partial.cumulative_filled_qty = qty("5");
    partial.status = OrderStatus::PartiallyFilled;
    partial.reason = ReasonCode::PassivePartial;
    let mut cancelled = partial.clone();
    cancelled.context = context(11, "2024-01-01T05:00:00Z");
    cancelled.status = OrderStatus::Cancelled;
    cancelled.reason = ReasonCode::PassiveRemainderCancelled;
    let fill = FillRecord {
        context: context(9, "2024-01-01T05:00:00Z"),
        fill_id: fill_id.clone(),
        order_id: order_id.clone(),
        episode_id: episode_id.clone(),
        side: Side::Buy,
        price: price("107.91"),
        qty: qty("5"),
        notional: quote("539.55"),
        fee: quote("0.53955"),
        fee_bps: bps("10"),
        fee_currency: "KRW".into(),
        timing: FillTiming::Interval {
            start: ts("2024-01-01T04:00:00Z"),
            end: ts("2024-01-01T05:00:00Z"),
            accounting_at: ts("2024-01-01T05:00:00Z"),
        },
        source_bar_id: execution_bar.clone(),
        liquidity_source_bar_id: decision_bar.clone(),
        liquidity_source_close_time: ts("2024-01-01T04:00:00Z"),
        decision_reference: price("109"),
        bar_open_proxy: price("109"),
        arrival_mid: None,
        arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
        adverse_slippage_bps: signed("-100"),
        price_cost_attribution: signed("-5.45"),
        liquidity_role_assumption: "MAKER_SCENARIO".into(),
        execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false,
        model_version: "passive-fill-v1".into(),
        artificial_terminal_exit: false,
        accounting_mark_seq: 10,
    };
    let state = |cash, free, reserved, quantity, basis, fees| AccountState {
        cash_total: quote(cash),
        cash_free: quote(free),
        cash_reserved: quote(reserved),
        qty: qty(quantity),
        price_basis: quote(basis),
        gross_realized: signed("0"),
        cumulative_fees: quote(fees),
    };
    let source_ids: Vec<_> = bundle.datasets[0]
        .observations
        .iter()
        .skip(1)
        .map(|row| row.id.clone())
        .collect();
    let marks = vec![
        passive_mark(
            &context,
            1,
            "2024-01-01T00:00:00Z",
            MarkKind::Initial,
            state("2000", "2000", "0", "0", "0", "0"),
            "100",
            &source_ids[0],
            "0",
            "0",
            "2000",
            "0",
            "0",
        ),
        passive_mark(
            &context,
            2,
            "2024-01-01T01:00:00Z",
            MarkKind::ExecutionClose,
            state("2000", "2000", "0", "0", "0", "0"),
            "100",
            &source_ids[0],
            "0",
            "0",
            "2000",
            "0",
            "0",
        ),
        passive_mark(
            &context,
            3,
            "2024-01-01T02:00:00Z",
            MarkKind::ExecutionClose,
            state("2000", "2000", "0", "0", "0", "0"),
            "101",
            &source_ids[1],
            "0",
            "0",
            "2000",
            "0",
            "0",
        ),
        passive_mark(
            &context,
            4,
            "2024-01-01T03:00:00Z",
            MarkKind::ExecutionClose,
            state("2000", "2000", "0", "0", "0", "0"),
            "110",
            &source_ids[2],
            "0",
            "0",
            "2000",
            "0",
            "0",
        ),
        passive_mark(
            &context,
            5,
            "2024-01-01T04:00:00Z",
            MarkKind::ExecutionClose,
            state("2000", "2000", "0", "0", "0", "0"),
            "109",
            &source_ids[3],
            "0",
            "0",
            "2000",
            "0",
            "0",
        ),
        passive_mark(
            &context,
            10,
            "2024-01-01T05:00:00Z",
            MarkKind::AfterFill,
            state(
                "1459.91045",
                "919.82090",
                "540.08955",
                "5",
                "539.55",
                "0.53955",
            ),
            "109",
            &execution_bar,
            "545",
            "5.45",
            "2004.91045",
            "0.5",
            "0",
        ),
        passive_mark(
            &context,
            12,
            "2024-01-01T05:00:00Z",
            MarkKind::ExecutionClose,
            state("1459.91045", "1459.91045", "0", "5", "539.55", "0.53955"),
            "109",
            &execution_bar,
            "545",
            "5.45",
            "2004.91045",
            "0.5",
            "0",
        ),
    ];
    let episode = EpisodeRecord {
        episode_id,
        run_id,
        model_id: model_id.clone(),
        market: market.clone(),
        status: EpisodeStatus::Open,
        opened_at: ts("2024-01-01T05:00:00Z"),
        closed_at: None,
        fill_ids: vec![fill_id],
        order_ids: vec![order_id],
        buy_vwap: price("107.91"),
        sell_vwap: None,
        max_qty: qty("5"),
        time_weighted_avg_qty: qty("5"),
        holding_seconds: 0,
        realized_price_pnl: signed("0"),
        fees: quote("0.53955"),
        net_realized: signed("-0.53955"),
        residual_basis: quote("539.55"),
        residual_qty: qty("5"),
        marked_unrealized: signed("5.45"),
        exit_reason: None,
        start_equity: quote("2000"),
        mae_amount: signed("-0.53955"),
        mfe_amount: signed("4.91045"),
        mae_pct_of_start_equity: signed("-0.000269775"),
        mfe_pct_of_start_equity: signed("0.002455225"),
        time_to_mae_seconds: 0,
        time_to_mfe_seconds: 0,
        sampling_definition: "after-fill and execution-close net PnL path".into(),
        exit_peak_giveback: quote("0"),
    };
    bundle.models[0] = ModelLedger {
        model_id,
        market,
        strategy: StrategySpec::BuyAndHold.into(),
        status: ModelStatus::Completed,
        status_reason: None,
        signals: vec![signal],
        orders: vec![cancelled.clone()],
        order_events: vec![created, partial, cancelled],
        fills: vec![fill],
        episodes: vec![episode],
        account_marks: marks,
        last_event_seq: 12,
    };
    bundle.plan.estimated_events = 12;
    refresh_bundle_hashes(&mut bundle);
    bundle
}

#[allow(
    clippy::too_many_arguments,
    reason = "passive fixture mark fields remain explicit for independent arithmetic review"
)]
fn passive_mark(
    context: &impl Fn(u64, &str) -> EventContext,
    event_seq: u64,
    at: &str,
    kind: MarkKind,
    state: AccountState,
    mark_price: &str,
    source_bar_id: &ObservationId,
    position_value: &str,
    gross_unrealized: &str,
    equity: &str,
    target_weight: &str,
    drawdown: &str,
) -> AccountMark {
    AccountMark {
        context: context(event_seq, at),
        kind,
        state,
        mark_price: price(mark_price),
        source_bar_id: source_bar_id.clone(),
        position_value: quote(position_value),
        gross_unrealized: signed(gross_unrealized),
        equity: quote(equity),
        target_weight: weight(target_weight),
        actual_weight: if position_value == "0" {
            weight("0")
        } else {
            weight_ratio(position_value, equity)
        },
        peak_equity: quote(if dec(equity) > dec("2000") {
            equity
        } else {
            "2000"
        }),
        drawdown: weight(drawdown),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one linked multi-market fixture keeps every remapped identity explicit"
)]
pub(crate) fn multi_market_fixture() -> RunBundle {
    let mut bundle = fixture();
    let eth_market = MarketId::parse_upbit("KRW-ETH").expect("test market");
    let eth_model_id = ModelId::new("model-eth-bh").expect("test ID");
    let eth_episode_id = EpisodeId::new("episode-eth-roundtrip").expect("test ID");
    let eth_buy_signal = SignalId::new("signal-eth-buy").expect("test ID");
    let eth_sell_signal = SignalId::new("signal-eth-sell").expect("test ID");
    let eth_buy_order = OrderId::new("order-eth-buy").expect("test ID");
    let eth_sell_order = OrderId::new("order-eth-sell").expect("test ID");
    let eth_buy_fill = FillId::new("fill-eth-buy").expect("test ID");
    let eth_sell_fill = FillId::new("fill-eth-sell").expect("test ID");
    let eth_raw = fixture_raw_object("fixture-eth-raw");

    let mut eth_model = bundle.models[0].clone();
    eth_model.model_id = eth_model_id.clone();
    eth_model.market = eth_market.clone();
    eth_model.last_event_seq = 32;
    for context in eth_model
        .signals
        .iter_mut()
        .map(|record| &mut record.context)
        .chain(
            eth_model
                .order_events
                .iter_mut()
                .map(|record| &mut record.context),
        )
        .chain(
            eth_model
                .orders
                .iter_mut()
                .map(|record| &mut record.context),
        )
        .chain(eth_model.fills.iter_mut().map(|record| &mut record.context))
        .chain(
            eth_model
                .account_marks
                .iter_mut()
                .map(|record| &mut record.context),
        )
    {
        context.model_id = eth_model_id.clone();
        context.market = eth_market.clone();
        context.event_seq += 16;
    }
    eth_model.signals[0].signal_id = eth_buy_signal.clone();
    eth_model.signals[1].signal_id = eth_sell_signal.clone();
    let mut observation_ids = BTreeMap::new();
    let eth_observations: Vec<_> = bundle.datasets[0]
        .observations
        .iter()
        .cloned()
        .map(|mut observation| {
            let original = observation.id.as_str().to_owned();
            observation.candle.market = eth_market.code().clone();
            observation.content_digest = crate::contracts::observation_digest(&observation.candle)
                .expect("ETH observation digest");
            observation.id = ObservationId::from_seed(observation.content_digest.as_str());
            observation.raw_object_ids = vec![eth_raw.id.clone()];
            observation_ids.insert(original, observation.id.clone());
            observation
        })
        .collect();
    for signal in &mut eth_model.signals {
        signal.source_bar_ids = signal
            .source_bar_ids
            .iter()
            .map(|id| observation_ids[id.as_str()].clone())
            .collect();
    }
    for order in eth_model
        .orders
        .iter_mut()
        .chain(eth_model.order_events.iter_mut())
    {
        let buy = order.side == Side::Buy;
        order.order_id = if buy {
            eth_buy_order.clone()
        } else {
            eth_sell_order.clone()
        };
        order.parent_signal_id = if buy {
            eth_buy_signal.clone()
        } else {
            eth_sell_signal.clone()
        };
        order.episode_id = Some(eth_episode_id.clone());
    }
    for fill in &mut eth_model.fills {
        let buy = fill.side == Side::Buy;
        fill.fill_id = if buy {
            eth_buy_fill.clone()
        } else {
            eth_sell_fill.clone()
        };
        fill.order_id = if buy {
            eth_buy_order.clone()
        } else {
            eth_sell_order.clone()
        };
        fill.episode_id = eth_episode_id.clone();
        fill.source_bar_id = observation_ids[fill.source_bar_id.as_str()].clone();
        fill.liquidity_source_bar_id =
            observation_ids[fill.liquidity_source_bar_id.as_str()].clone();
        fill.accounting_mark_seq += 16;
    }
    for mark in &mut eth_model.account_marks {
        mark.source_bar_id = observation_ids[mark.source_bar_id.as_str()].clone();
    }
    let episode = &mut eth_model.episodes[0];
    episode.episode_id = eth_episode_id;
    episode.model_id = eth_model_id.clone();
    episode.market = eth_market.clone();
    episode.fill_ids = vec![eth_buy_fill, eth_sell_fill];
    episode.order_ids = vec![eth_buy_order, eth_sell_order];

    bundle.datasets[0].observations.extend(eth_observations);
    bundle.datasets[0].manifest.raw_objects.push(eth_raw);
    bundle.datasets[0]
        .manifest
        .request
        .markets
        .push(eth_market.clone());
    bundle.datasets[0].manifest.row_count = 12;
    let dataset_digests = crate::contracts::dataset_digests(&bundle.datasets[0])
        .expect("multi-market dataset digests");
    let dataset_id = crate::contracts::dataset_id(&dataset_digests);
    bundle.datasets[0].manifest.id = dataset_id.clone();
    bundle.datasets[0].manifest.semantic_digest = dataset_digests.semantic.clone();
    bundle.datasets[0].manifest.provenance_digest = dataset_digests.provenance;
    bundle.plan.spec.markets.push(eth_market.clone());
    bundle.plan.spec.dataset_ids[0] = dataset_id.clone();
    bundle.plan.admissions.push(ModelAdmission {
        model_id: eth_model_id,
        market: eth_market,
        strategy: StrategyKind::BuyAndHold,
        policy_ref: None,
        status: AdmissionStatus::Eligible,
        reasons: Vec::new(),
    });
    bundle.plan.estimated_events = 32;
    bundle.plan.config_digest = ContentHash::of_value(&bundle.plan.spec).expect("config digest");
    bundle.plan.dataset_digests[0] = (dataset_id, dataset_digests.semantic);
    bundle.models.push(eth_model);
    bundle.plan.input_digest = super::verify::input_digest(&bundle).expect("input digest");
    bundle.semantic_digest = semantic_digest(&bundle).expect("semantic digest");
    bundle
}

fn derived_bundle_fixture() -> (RunBundle, DatasetId, DatasetId) {
    let mut bundle = fixture();
    let market = MarketId::parse_upbit("KRW-BTC").expect("market");
    let range = UtcRange::new(ts("2024-01-01T00:00:00Z"), ts("2024-01-01T01:00:00Z"))
        .expect("derived source range");
    let raw = fixture_raw_object("fixture-m5-source");
    let request = CollectRequest {
        request_id: RequestId::new("request-m5-source").expect("request ID"),
        markets: vec![market.clone()],
        range,
        data_resolution: CandleInterval::M5,
        warmup_bars: 0,
        completed_only: true,
    };
    let observations: Vec<_> = (0..12)
        .map(|index| {
            let opened = UtcTimestamp(
                range
                    .start()
                    .0
                    .checked_add_signed(chrono::Duration::minutes(index * 5))
                    .expect("M5 open"),
            );
            let candle = CandleRecord {
                market: market.code().clone(),
                interval: CandleInterval::M5,
                open_time_utc: opened,
                close_time_utc: UtcTimestamp(
                    opened
                        .0
                        .checked_add_signed(CandleInterval::M5.duration())
                        .expect("M5 close"),
                ),
                open: price(&format!("{}", 100 + index)),
                high: price("200"),
                low: price("50"),
                close: price(&format!("{}", 101 + index)),
                volume: qty("2"),
                quote_turnover: quote("210"),
                completed: true,
            };
            let digest =
                crate::contracts::observation_digest(&candle).expect("M5 observation digest");
            CandleObservation {
                id: ObservationId::from_seed(digest.as_str()),
                candle,
                content_digest: digest,
                raw_object_ids: vec![raw.id.clone()],
                constituent_ids: Vec::new(),
            }
        })
        .collect();
    let placeholder = ContentHash::of_bytes(b"unpublished-m5");
    let mut source = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: SCHEMA_VERSION.into(),
            id: DatasetId::from_seed("unpublished-m5"),
            request,
            coverage: range,
            status: DatasetStatus::Ready,
            row_count: 12,
            normalizer_version: NORMALIZER_VERSION.into(),
            gap_policy: "REJECT_UNRESOLVED_GAPS".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: MarketDataOrigin::SyntheticTestOnly,
            raw_objects: vec![raw],
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations,
    };
    let source_digests = crate::contracts::dataset_digests(&source).expect("source digests");
    source.manifest.id = crate::contracts::dataset_id(&source_digests);
    source.manifest.semantic_digest = source_digests.semantic.clone();
    source.manifest.provenance_digest = source_digests.provenance;
    let derived = crate::collection::derive_snapshot(&source, CandleInterval::H1)
        .expect("derive H1 snapshot");
    let source_id = source.manifest.id.clone();
    let derived_id = derived.manifest.id.clone();
    bundle
        .plan
        .spec
        .dataset_ids
        .extend([source_id.clone(), derived_id.clone()]);
    bundle.plan.dataset_digests.extend([
        (source_id.clone(), source.manifest.semantic_digest.clone()),
        (derived_id.clone(), derived.manifest.semantic_digest.clone()),
    ]);
    bundle.datasets.extend([source, derived]);
    refresh_bundle_hashes(&mut bundle);
    (bundle, source_id, derived_id)
}

fn refresh_bundle_hashes(bundle: &mut RunBundle) {
    bundle.plan.config_digest =
        experiment_config_digest(&bundle.plan.spec, &bundle.plan.policy_revisions)
            .expect("config digest");
    bundle.plan.input_digest = super::verify::input_digest(bundle).expect("input digest");
    bundle.semantic_digest = semantic_digest(bundle).expect("semantic digest");
}

fn unique_temp_root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "spot-lab-reporting-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn candle_observation(
    market: &MarketId,
    open_time: &str,
    close_time: &str,
    open: &str,
    close: &str,
    volume: &str,
    raw_object_id: &RawObjectId,
) -> CandleObservation {
    let candle = CandleRecord {
        market: market.code().clone(),
        interval: CandleInterval::H1,
        open_time_utc: ts(open_time),
        close_time_utc: ts(close_time),
        open: price(open),
        high: price("111"),
        low: price("98"),
        close: price(close),
        volume: qty(volume),
        quote_turnover: quote("100000"),
        completed: true,
    };
    let content_digest =
        crate::contracts::observation_digest(&candle).expect("canonical observation digest");
    CandleObservation {
        id: ObservationId::from_seed(content_digest.as_str()),
        content_digest,
        candle,
        raw_object_ids: vec![raw_object_id.clone()],
        constituent_ids: Vec::new(),
    }
}

fn fixture_raw_object(seed: &str) -> RawObjectRef {
    RawObjectRef {
        id: RawObjectId::from_seed(seed),
        relative_path: format!("raw/{seed}"),
        source_url: format!("https://example.invalid/{seed}"),
        fetched_at: ts("2024-01-01T06:00:00Z"),
        persisted_at: ts("2024-01-01T06:00:00Z"),
        http_status: 200,
        remaining_req: None,
        raw_sha256: ContentHash::of_bytes(seed.as_bytes()),
        compressed_sha256: ContentHash::of_bytes(format!("gzip-{seed}").as_bytes()),
        raw_bytes: 1,
        compressed_bytes: 1,
        origin: MarketDataOrigin::SyntheticTestOnly,
    }
}

fn ts(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("test timestamp")
}
fn dec(value: &str) -> Decimal {
    Decimal::from_str_exact(value).expect("test decimal")
}
fn price(value: &str) -> PriceKrw {
    PriceKrw::new(dec(value)).expect("test price")
}
fn quote(value: &str) -> QuoteAmount {
    QuoteAmount::new(dec(value)).expect("test quote")
}
fn qty(value: &str) -> AssetQuantity {
    AssetQuantity::new(dec(value)).expect("test quantity")
}
fn weight(value: &str) -> Weight {
    Weight::new(dec(value)).expect("test weight")
}
fn weight_ratio(numerator: &str, denominator: &str) -> Weight {
    Weight::new(dec(numerator) / dec(denominator)).expect("test weight ratio")
}
fn bps(value: &str) -> BasisPoints {
    BasisPoints::new(dec(value)).expect("test bps")
}
fn signed(value: &str) -> SignedAmount {
    SignedAmount::new(dec(value)).expect("test signed amount")
}
