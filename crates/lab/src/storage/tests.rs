use super::*;
use crate::contracts::{
    AdmissionStatus, ArtifactId, AssetQuantity, AttemptState, BasisPoints, CandleInterval,
    CandleRecord, CostPolicy, DatasetManifest, DatasetStatus, DeleteBlockerClass, DeleteEntryKind,
    DeletePreview, DeleteResource, EvidenceUnavailablePolicy, ExecutionPolicy, ExperimentSpec,
    HardDeleteRequest, JobOutput, JobPayload, JobProgress, JobStatus, JobSubmission,
    MarketRuleSnapshot, ModelAdmission, ModelId, PitPolicy, PlanId, PlanRequest, PolicyDefinition,
    PolicyProgram, PolicyRevision, PolicyWrite, PriceKrw, ProgressCountUnit, QualityIssue,
    QualityKind, QualitySeverity, QuoteAmount, ReportClock, ResolvedPlan, RuleProvenance,
    RuleSnapshotId, RunHeader, RunId, RunRequest, StrategyKind, StrategySpec, TerminalPolicy,
    TickBand, UtcRange, ValidationReport, ValidationStatus, Weight, builtin_policy_definitions,
    experiment_config_digest,
};
use rust_decimal::Decimal;
use std::str::FromStr;

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

fn time(text: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(text).expect("valid fixture time")
}

fn publish(store: &Store, body: &[u8], fetched: UtcTimestamp) -> PublishedRaw {
    store
        .publish_raw(RawObjectInput {
            source_url: "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC".into(),
            fetched_at: fetched,
            persisted_at: fetched,
            http_status: 200,
            remaining_req: Some("group=candle; sec=9".into()),
            origin: MarketDataOrigin::ExchangeObserved,
            body: body.to_vec(),
        })
        .expect("publish fixture raw")
}

fn observation(raw: &RawObjectRef, open: &str) -> CandleObservation {
    observation_at(raw, "2024-01-01T00:00:00Z", open)
}

fn observation_at(raw: &RawObjectRef, open_at: &str, open: &str) -> CandleObservation {
    let open_time = time(open_at);
    let candle = CandleRecord {
        market: "KRW-BTC".into(),
        interval: CandleInterval::H1,
        open_time_utc: open_time,
        close_time_utc: UtcTimestamp(open_time.0 + chrono::Duration::hours(1)),
        open: PriceKrw::new(Decimal::from_str(open).expect("decimal")).expect("price"),
        high: PriceKrw::new(Decimal::from_str("110").expect("decimal")).expect("price"),
        low: PriceKrw::new(Decimal::from_str("90").expect("decimal")).expect("price"),
        close: PriceKrw::new(Decimal::from_str("105").expect("decimal")).expect("price"),
        volume: AssetQuantity::new(Decimal::from_str("2.5").expect("decimal")).expect("quantity"),
        quote_turnover: QuoteAmount::new(Decimal::from_str("250").expect("decimal"))
            .expect("amount"),
        completed: true,
    };
    let placeholder = ContentHash::of_bytes(b"placeholder");
    let mut observation = CandleObservation {
        id: crate::contracts::ObservationId::new("placeholder").expect("observation id"),
        content_digest: placeholder,
        candle,
        raw_object_ids: vec![raw.id.clone()],
        constituent_ids: Vec::new(),
    };
    let digest = observation_digest(&observation.candle).expect("observation digest");
    observation.id = crate::contracts::ObservationId::from_seed(digest.as_str());
    observation.content_digest = digest;
    observation
}

fn request(id: &str) -> CollectRequest {
    CollectRequest {
        request_id: RequestId::new(id).expect("request id"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(time("2024-01-01T00:00:00Z"), time("2024-01-01T01:00:00Z"))
            .expect("range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    }
}

fn page(
    request: &CollectRequest,
    raw: RawObjectRef,
    observation: CandleObservation,
) -> CollectionPage {
    CollectionPage {
        request_id: request.request_id.clone(),
        market: request.markets[0].clone(),
        requested_to: time("2024-01-01T01:00:00Z"),
        next_to: Some(time("2024-01-01T00:00:00Z")),
        raw_object: raw,
        observations: vec![observation],
        page_index: 0,
    }
}

fn snapshot(
    request: CollectRequest,
    raw: RawObjectRef,
    observation: CandleObservation,
) -> DatasetSnapshot {
    let placeholder = ContentHash::of_bytes(b"placeholder");
    let mut snapshot = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: "1".into(),
            id: DatasetId::new("placeholder").expect("dataset id"),
            request,
            coverage: UtcRange::new(time("2024-01-01T00:00:00Z"), time("2024-01-01T01:00:00Z"))
                .expect("range"),
            status: DatasetStatus::Ready,
            row_count: 1,
            normalizer_version: crate::contracts::NORMALIZER_VERSION.into(),
            gap_policy: "reject".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: MarketDataOrigin::ExchangeObserved,
            raw_objects: vec![raw],
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations: vec![observation],
    };
    let digests = dataset_digests(&snapshot).expect("dataset digests");
    snapshot.manifest.id = dataset_id(&digests);
    snapshot.manifest.semantic_digest = digests.semantic;
    snapshot.manifest.provenance_digest = digests.provenance;
    snapshot
}

#[expect(
    clippy::too_many_lines,
    reason = "complete validated v2 frozen-policy plan fixture"
)]
fn save_v2_policy_plan_fixture(store: &mut Store, policy: &PolicyRevision) -> PlanId {
    #[derive(serde::Serialize)]
    struct Input<'a> {
        config_digest: &'a ContentHash,
        dataset_digests: &'a [(DatasetId, ContentHash)],
        evidence_digest: &'a Option<ContentHash>,
    }

    let published = publish(store, b"policy plan dataset", time("2024-01-01T02:00:00Z"));
    let dataset_request = request("policy-plan-dataset");
    let dataset = snapshot(
        dataset_request.clone(),
        published.object.clone(),
        observation(&published.object, "100"),
    );
    let dataset_id = store
        .finish_dataset(&dataset)
        .expect("finish policy-plan dataset");
    let market = dataset_request.markets[0].clone();
    let zero_bps = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    let spec = ExperimentSpec {
        schema_version: "2.0".into(),
        causal_execution: None,
        dataset_ids: vec![dataset_id.clone()],
        markets: vec![market.clone()],
        range: dataset_request.range,
        strategies: Vec::new(),
        policy_selections: vec![policy.snapshot.reference.clone()],
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: QuoteAmount::new(Decimal::from(1_000)).expect("cash"),
        costs: CostPolicy {
            buy_fee_bps: zero_bps,
            sell_fee_bps: zero_bps,
            maker_fee_bps: zero_bps,
            half_spread_bps: zero_bps,
            slippage_bps: zero_bps,
            impact_bps: zero_bps,
            assumption_label: "policy fixture".into(),
            dynamic: None,
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE).expect("weight"),
        },
        market_rules: MarketRuleSnapshot {
            id: RuleSnapshotId::new("policy-rule").expect("rule id"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: dataset_request.range,
            observed_at: dataset_request.range.start(),
            source_refs: vec!["synthetic-policy-fixture".into()],
            assumption_label: "policy fixture".into(),
            min_notional: QuoteAmount::new(Decimal::ONE).expect("notional"),
            quantity_step: AssetQuantity::new(Decimal::new(1, 1)).expect("quantity step"),
            ticks: vec![TickBand {
                lower_bound: QuoteAmount::new(Decimal::ZERO).expect("lower bound"),
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
        seed: 7,
    };
    let frozen = vec![policy.snapshot.clone()];
    let config_digest = experiment_config_digest(&spec, &frozen).expect("config digest");
    let dataset_digests = vec![(dataset_id, dataset.manifest.semantic_digest.clone())];
    let evidence_digest = None;
    let input_digest = ContentHash::of_value(&Input {
        config_digest: &config_digest,
        dataset_digests: &dataset_digests,
        evidence_digest: &evidence_digest,
    })
    .expect("input digest");
    let plan_id = PlanId::from_seed(input_digest.as_str());
    let plan = ResolvedPlan {
        id: plan_id.clone(),
        spec: spec.clone(),
        config_digest,
        input_digest,
        dataset_digests,
        evidence_digest,
        admissions: vec![ModelAdmission {
            model_id: ModelId::new("policy-model").expect("model id"),
            market,
            strategy: StrategyKind::Other,
            policy_ref: Some(policy.snapshot.reference.clone()),
            status: AdmissionStatus::Eligible,
            reasons: Vec::new(),
        }],
        policy_revisions: frozen,
        warnings: Vec::new(),
        estimated_events: 10,
    };
    store
        .save_plan(
            &PlanRequest {
                request_id: RequestId::new("policy-plan-request").expect("request id"),
                spec,
            },
            &plan,
        )
        .expect("save v2 plan");
    plan_id
}

#[test]
fn reopen_idempotency_conflict_and_snapshot_immutability() {
    let root = TempRoot::new("reopen");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(
        &store,
        br#"[{"trade_price":105}]"#,
        time("2024-01-01T02:00:00Z"),
    );
    let first_request = request("request-a");
    store
        .begin_collection(&first_request)
        .expect("begin collection");
    assert_eq!(
        store
            .begin_collection(&first_request)
            .expect("idempotent begin"),
        normalized_request_digest(&first_request).expect("request digest")
    );
    store
        .catalog_collection_raw(&first_request.request_id, &published.object)
        .expect("catalog first raw response");
    store
        .catalog_collection_raw(&first_request.request_id, &published.object)
        .expect("idempotent raw response catalog");
    let rejected = publish(&store, b"rejected response", time("2024-01-01T02:01:00Z"));
    store
        .catalog_collection_raw(&first_request.request_id, &rejected.object)
        .expect("catalog rejected raw response");
    let mut conflicting_request = first_request.clone();
    conflicting_request.warmup_bars = 1;
    assert!(matches!(
        store.begin_collection(&conflicting_request),
        Err(LabError::Conflict(_))
    ));
    let first_observation = observation(&published.object, "100");
    let first_page = page(
        &first_request,
        published.object.clone(),
        first_observation.clone(),
    );
    store.commit_page(&first_page).expect("commit page");
    store.commit_page(&first_page).expect("idempotent page");
    let all_raw = store
        .load_collection_raw_objects(&first_request.request_id)
        .expect("load all collection responses");
    assert_eq!(all_raw.len(), 2);
    assert_eq!(all_raw[0].id, published.object.id);
    assert_eq!(all_raw[1].id, rejected.object.id);
    assert_eq!(
        store
            .collection_raw_bytes(&first_request.request_id)
            .expect("load collection byte count"),
        all_raw.iter().map(|raw| raw.raw_bytes).sum::<u64>()
    );
    let resumed = store
        .load_collection_pages(&first_request.request_id, &first_request.markets[0])
        .expect("load committed pages");
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0].observations[0].content_digest,
        first_observation.content_digest
    );

    let mut conflict = first_page.clone();
    conflict.requested_to = time("2024-01-01T02:00:00Z");
    assert!(matches!(
        store.commit_page(&conflict),
        Err(LabError::Conflict(_))
    ));

    let snapshot = snapshot(first_request, published.object, first_observation);
    let id = store.finish_dataset(&snapshot).expect("finish dataset");
    assert_eq!(
        store.finish_dataset(&snapshot).expect("idempotent dataset"),
        id
    );
    let second = publish(&store, b"second source", time("2024-01-02T02:00:00Z"));
    let second_request = request("request-b");
    store
        .begin_collection(&second_request)
        .expect("begin second collection");
    let second_observation = observation(&second.object, "101");
    store
        .commit_page(&page(&second_request, second.object, second_observation))
        .expect("commit later observation");
    store.close().expect("close store");

    let store = Store::open(&root.0).expect("reopen store");
    let loaded = store
        .load_dataset(&id)
        .expect("load dataset")
        .expect("dataset exists");
    assert_eq!(
        loaded.manifest.semantic_digest,
        snapshot.manifest.semantic_digest
    );
    assert_eq!(
        loaded.observations[0].content_digest,
        snapshot.observations[0].content_digest
    );
}

#[test]
fn ready_dataset_rejects_error_quality_issue() {
    let root = TempRoot::new("ready-quality");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"quality", time("2024-01-01T02:00:00Z"));
    let request = request("request-quality");
    let observation = observation(&published.object, "100");
    let mut invalid = snapshot(request, published.object, observation);
    invalid.manifest.quality_issues.push(QualityIssue {
        kind: QualityKind::UnknownSourceGap,
        severity: QualitySeverity::Error,
        market: invalid.manifest.request.markets[0].clone(),
        start: time("2024-01-01T00:00:00Z"),
        end: time("2024-01-01T01:00:00Z"),
        count: 1,
        raw_object_ids: vec![invalid.manifest.raw_objects[0].id.clone()],
        detail: "fixture error".into(),
    });
    assert!(matches!(
        store.finish_dataset(&invalid),
        Err(LabError::DataCorrupt(_))
    ));
}

#[test]
fn benign_duplicate_quality_changes_provenance_not_economic_semantic() {
    let root = TempRoot::new("duplicate-quality");
    let store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"duplicate", time("2024-01-01T02:00:00Z"));
    let base = snapshot(
        request("request-duplicate-quality"),
        published.object.clone(),
        observation(&published.object, "100"),
    );
    let mut overlap = base.clone();
    overlap.manifest.quality_issues.push(QualityIssue {
        kind: QualityKind::DuplicateIdentical,
        severity: QualitySeverity::Info,
        market: overlap.manifest.request.markets[0].clone(),
        start: time("2024-01-01T00:00:00Z"),
        end: time("2024-01-01T01:00:00Z"),
        count: 1,
        raw_object_ids: vec![published.object.id],
        detail: "overlapping retrieval page".into(),
    });
    let base_digests = dataset_digests(&base).expect("base digests");
    let overlap_digests = dataset_digests(&overlap).expect("overlap digests");
    assert_eq!(base_digests.semantic, overlap_digests.semantic);
    assert_ne!(base_digests.provenance, overlap_digests.provenance);
}

#[test]
fn failed_transaction_leaves_only_identifiable_orphan() {
    let root = TempRoot::new("transaction-failure");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"[]", time("2024-01-01T02:00:00Z"));
    let request = request("request-fail");
    store.begin_collection(&request).expect("begin collection");
    let page = page(
        &request,
        published.object.clone(),
        observation(&published.object, "100"),
    );
    store.connection.execute_batch(
        "CREATE TRIGGER fail_page BEFORE INSERT ON collection_pages BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    ).expect("install failure trigger");
    assert!(store.commit_page(&page).is_err());
    let resumed_pages = store
        .load_collection_pages(&request.request_id, &request.markets[0])
        .expect("load pages after failed commit");
    assert!(resumed_pages.is_empty());
    let orphans = store.scan_orphans().expect("orphan scan");
    assert_eq!(orphans, vec![published.object.relative_path]);

    let oversized = RawObjectInput {
        source_url: "https://api.upbit.com/".into(),
        fetched_at: time("2024-01-01T02:00:00Z"),
        persisted_at: time("2024-01-01T02:00:00Z"),
        http_status: 200,
        remaining_req: None,
        origin: MarketDataOrigin::ExchangeObserved,
        body: vec![0; MAX_RAW_OBJECT_BYTES + 1],
    };
    assert!(matches!(
        store.publish_raw(oversized),
        Err(LabError::ResourceLimit(_))
    ));

    let failing_body = b"filesystem failure";
    let raw_hash = ContentHash::of_bytes(failing_body);
    let shard_path = root.0.join(RAW_DIR).join(&raw_hash.as_str()[..2]);
    fs::write(&shard_path, b"not a directory").expect("inject path failure");
    assert!(
        store
            .publish_raw(RawObjectInput {
                source_url: "https://api.upbit.com/".into(),
                fetched_at: time("2024-01-01T03:00:00Z"),
                persisted_at: time("2024-01-01T03:00:00Z"),
                http_status: 200,
                remaining_req: None,
                origin: MarketDataOrigin::ExchangeObserved,
                body: failing_body.to_vec(),
            })
            .is_err()
    );
}

#[test]
fn migration_checksum_and_newer_version_fail_closed() {
    let checksum_root = TempRoot::new("checksum");
    {
        let store = Store::open(&checksum_root.0).expect("open store");
        store
            .connection
            .execute(
                "UPDATE schema_migrations SET checksum='bad' WHERE version=1",
                [],
            )
            .expect("corrupt checksum");
    }
    assert!(matches!(
        Store::open(&checksum_root.0),
        Err(LabError::DataCorrupt(_))
    ));

    let version_root = TempRoot::new("newer-version");
    {
        let store = Store::open(&version_root.0).expect("open store");
        store.connection.execute("INSERT INTO schema_migrations(version,name,checksum) VALUES (99,'future','future')", []).expect("insert future migration");
    }
    assert!(matches!(
        Store::open(&version_root.0),
        Err(LabError::DataCorrupt(_))
    ));
}

#[test]
fn repeated_economic_rows_create_distinct_immutable_provenance_snapshots() {
    let root = TempRoot::new("provenance-snapshots");
    let mut store = Store::open(&root.0).expect("open store");

    let first_raw = publish(&store, b"source A", time("2024-01-01T02:00:00Z"));
    let first_request = request("request-provenance-a");
    store
        .begin_collection(&first_request)
        .expect("begin first collection");
    let first_observation = observation(&first_raw.object, "100");
    store
        .commit_page(&page(
            &first_request,
            first_raw.object.clone(),
            first_observation.clone(),
        ))
        .expect("commit first page");
    let first_snapshot = snapshot(first_request, first_raw.object.clone(), first_observation);
    let first_id = store
        .finish_dataset(&first_snapshot)
        .expect("finish first snapshot");

    let second_raw = publish(&store, b"source B", time("2024-01-02T02:00:00Z"));
    let second_request = request("request-provenance-b");
    store
        .begin_collection(&second_request)
        .expect("begin second collection");
    let second_observation = observation(&second_raw.object, "100.0");
    store
        .commit_page(&page(
            &second_request,
            second_raw.object.clone(),
            second_observation.clone(),
        ))
        .expect("commit second page");
    let second_snapshot = snapshot(
        second_request,
        second_raw.object.clone(),
        second_observation,
    );
    let second_id = store
        .finish_dataset(&second_snapshot)
        .expect("finish second snapshot");

    assert_ne!(first_id, second_id);
    assert_eq!(
        first_snapshot.manifest.semantic_digest,
        second_snapshot.manifest.semantic_digest
    );
    assert_ne!(
        first_snapshot.manifest.provenance_digest,
        second_snapshot.manifest.provenance_digest
    );
    let first_loaded = store
        .load_dataset(&first_id)
        .expect("load first snapshot")
        .expect("first snapshot exists");
    let second_loaded = store
        .load_dataset(&second_id)
        .expect("load second snapshot")
        .expect("second snapshot exists");
    assert_eq!(
        first_loaded.observations[0].raw_object_ids,
        vec![first_raw.object.id]
    );
    assert_eq!(
        second_loaded.observations[0].raw_object_ids,
        vec![second_raw.object.id]
    );
    assert_eq!(
        dataset_digests(&first_loaded)
            .expect("first recheck")
            .provenance,
        first_loaded.manifest.provenance_digest
    );
    assert_eq!(
        dataset_digests(&second_loaded)
            .expect("second recheck")
            .provenance,
        second_loaded.manifest.provenance_digest
    );
}

#[test]
fn backup_restore_rechecks_raw_and_dataset_hashes() {
    let root = TempRoot::new("backup-source");
    let backup_parent = TempRoot::new("backup-parent");
    let restore_parent = TempRoot::new("restore-parent");
    let backup_path = backup_parent.0.join("snapshot");
    let restore_path = restore_parent.0.join("restored");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"backup payload", time("2024-01-01T02:00:00Z"));
    let request = request("request-backup");
    store.begin_collection(&request).expect("begin collection");
    let observation = observation(&published.object, "100");
    store
        .commit_page(&page(
            &request,
            published.object.clone(),
            observation.clone(),
        ))
        .expect("commit page");
    let snapshot = snapshot(request, published.object, observation);
    let id = store.finish_dataset(&snapshot).expect("finish dataset");
    store.backup(&backup_path).expect("create backup");

    let restored = Store::restore(&backup_path, &restore_path).expect("restore backup");
    let loaded = restored
        .load_dataset(&id)
        .expect("load restored dataset")
        .expect("restored dataset");
    assert_eq!(
        loaded.manifest.semantic_digest,
        snapshot.manifest.semantic_digest
    );
    assert_eq!(
        loaded.manifest.provenance_digest,
        snapshot.manifest.provenance_digest
    );
    assert!(
        restored
            .scan_orphans()
            .expect("restored orphan scan")
            .is_empty()
    );

    let locked_restore = restore_parent.0.join("locked-restored");
    fs::create_dir(&locked_restore).expect("create locked restore root");
    fs::write(locked_restore.join(OWNER_LOCK_FILE), b"held by fixture").expect("create owner lock");
    assert!(matches!(
        Store::restore(&backup_path, &locked_restore),
        Err(LabError::Conflict(_))
    ));
    let locked_store =
        Store::restore_locked(&backup_path, &locked_restore).expect("restore under held lock");
    assert!(locked_restore.join(OWNER_LOCK_FILE).is_file());
    assert!(
        locked_store
            .load_dataset(&id)
            .expect("load locked restored dataset")
            .is_some()
    );

    fs::write(
        backup_path
            .join(&snapshot.manifest.raw_objects[0].relative_path)
            .join(BODY_FILE),
        b"tampered compressed body",
    )
    .expect("tamper backup raw body");
    assert!(matches!(
        Store::restore(&backup_path, restore_parent.0.join("tampered-restore")),
        Err(LabError::DataCorrupt(_))
    ));
}

#[test]
fn repeated_raw_publication_returns_original_persisted_metadata() {
    let root = TempRoot::new("raw-reuse");
    let mut store = Store::open(&root.0).expect("open store");
    let first = publish(&store, b"same body", time("2024-01-01T02:00:00Z"));
    store.catalog_raw(&first.object).expect("catalog probe raw");
    store
        .catalog_raw(&first.object)
        .expect("idempotent catalog");
    assert_eq!(
        store
            .raw_objects()
            .read_verified(&first.object)
            .expect("read verified raw"),
        b"same body"
    );
    let second = store
        .publish_raw(RawObjectInput {
            source_url: first.object.source_url.clone(),
            fetched_at: first.object.fetched_at,
            persisted_at: time("2024-01-02T02:00:00Z"),
            http_status: first.object.http_status,
            remaining_req: first.object.remaining_req.clone(),
            origin: first.object.origin,
            body: b"same body".to_vec(),
        })
        .expect("idempotent raw publication");
    assert_eq!(second.object.id, first.object.id);
    assert_eq!(second.object.persisted_at, first.object.persisted_at);
    let canonical = observation(&first.object, "100");
    let scaled = observation(&first.object, "100.0");
    assert_eq!(canonical.content_digest, scaled.content_digest);
    assert_eq!(canonical.id, scaled.id);
}

#[test]
fn durable_job_attempts_enforce_idempotency_cancel_retry_and_recovery() {
    let root = TempRoot::new("job-lifecycle");
    let mut store = Store::open(&root.0).expect("open store");
    let submission = JobSubmission {
        request_id: RequestId::new("job-request").expect("request id"),
        payload: JobPayload::Collect {
            request: request("collection-request"),
        },
    };
    let queued = store
        .submit_job(&submission, time("2024-01-01T00:00:00Z"))
        .expect("submit job");
    assert_eq!(
        store
            .submit_job(&submission, time("2024-01-01T00:01:00Z"))
            .expect("idempotent submit")
            .id,
        queued.id
    );
    let claimed = store
        .claim_next(time("2024-01-01T00:02:00Z"))
        .expect("claim query")
        .expect("claimed attempt");
    assert_eq!(claimed.state.status(), JobStatus::Running);
    store
        .update_progress(
            &claimed.id,
            &JobProgress {
                stage: "collecting".into(),
                committed_records: Some(2),
                count_unit: Some(ProgressCountUnit::DatasetRows),
                last_committed_event_seq: None,
            },
        )
        .expect("update progress");
    store
        .request_cancel(&queued.id, time("2024-01-01T00:03:00Z"))
        .expect("request cancellation");
    assert!(matches!(
        store.finish_attempt(
            &claimed.id,
            AttemptState::Interrupted {
                ended_at: time("2024-01-01T00:04:00Z"),
                reason: "wrong terminal after cancel".into(),
            },
        ),
        Err(LabError::Cancelled(_))
    ));
    store
        .finish_attempt(
            &claimed.id,
            AttemptState::Cancelled {
                ended_at: time("2024-01-01T00:04:00Z"),
                reason: "cancelled".into(),
            },
        )
        .expect("finish cancelled");
    let retried = store
        .retry_job(&queued.id, time("2024-01-01T00:05:00Z"))
        .expect("retry job");
    assert_eq!(retried.attempts.len(), 2);
    store
        .claim_next(time("2024-01-01T00:06:00Z"))
        .expect("claim retry")
        .expect("retried attempt");
    assert_eq!(
        store
            .recover_interrupted(time("2024-01-01T00:07:00Z"))
            .expect("recover running"),
        1
    );
    let recovered = store.get_job(&queued.id).expect("get job").expect("job");
    assert_eq!(recovered.attempts[1].state.status(), JobStatus::Interrupted);
    store
        .connection
        .execute(
            "UPDATE job_attempts SET attempt_number=32 WHERE id=?1",
            [recovered.attempts[1].id.as_str()],
        )
        .expect("move fixture to attempt cap");
    store
        .connection
        .execute(
            "UPDATE jobs SET current_attempt_number=32 WHERE id=?1",
            [queued.id.as_str()],
        )
        .expect("move job to attempt cap");
    assert!(matches!(
        store.retry_job(&queued.id, time("2024-01-01T00:08:00Z")),
        Err(LabError::ResourceLimit(_))
    ));
}

#[test]
fn durable_job_queue_rejects_ninth_queued_request() {
    let root = TempRoot::new("job-capacity");
    let mut store = Store::open(&root.0).expect("open store");
    for index in 0..8 {
        let submission = JobSubmission {
            request_id: RequestId::new(format!("job-{index}")).expect("job request id"),
            payload: JobPayload::Collect {
                request: request(&format!("collect-{index}")),
            },
        };
        store
            .submit_job(&submission, time("2024-01-01T00:00:00Z"))
            .expect("queue within limit");
    }
    let ninth = JobSubmission {
        request_id: RequestId::new("job-9").expect("job request id"),
        payload: JobPayload::Collect {
            request: request("collect-9"),
        },
    };
    assert!(matches!(
        store.submit_job(&ninth, time("2024-01-01T00:00:00Z")),
        Err(LabError::CapacityExceeded(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single cancellation race and transaction rollback publication fixture"
)]
fn attempt_publication_loses_cancel_race_and_rolls_back_injected_failure() {
    let root = TempRoot::new("attempt-publication");
    let mut store = Store::open(&root.0).expect("open store");
    let submit = |request_id: &str, artifact_id: &str| JobSubmission {
        request_id: RequestId::new(request_id).expect("request id"),
        payload: JobPayload::Verify {
            artifact_id: ArtifactId::new(artifact_id).expect("artifact id"),
            replay: false,
        },
    };
    let report = |check_id: &str| ValidationReport {
        check_id: check_id.into(),
        status: ValidationStatus::Pass,
        run_id: None,
        input_digest: ContentHash::of_bytes(check_id.as_bytes()),
        checked_models: 0,
        checked_fills: 0,
        checked_marks: 0,
        findings: Vec::new(),
    };

    let cancelled_job = store
        .submit_job(
            &submit("publish-cancel", "artifact-cancel"),
            time("2024-01-01T00:00:00Z"),
        )
        .expect("submit cancel-race job");
    let cancelled_attempt = store
        .claim_next(time("2024-01-01T00:01:00Z"))
        .expect("claim cancel-race job")
        .expect("attempt");
    store
        .request_cancel(&cancelled_job.id, time("2024-01-01T00:02:00Z"))
        .expect("request cancellation");
    let cancelled_report = report("cancel-race-report");
    let cancelled_terminal = AttemptState::Completed {
        ended_at: time("2024-01-01T00:03:00Z"),
        output: JobOutput::Validation {
            report: cancelled_report.clone(),
        },
    };
    assert!(matches!(
        store.publish_attempt(
            &cancelled_attempt.id,
            cancelled_terminal,
            AttemptPublication::Validation(cancelled_report),
        ),
        Err(LabError::Cancelled(_))
    ));
    let cancelled_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM validation_results WHERE check_id='cancel-race-report'",
            [],
            |row| row.get(0),
        )
        .expect("count cancel-race reports");
    assert_eq!(cancelled_count, 0);

    assert_eq!(
        store
            .recover_interrupted(time("2024-01-01T00:04:00Z"))
            .expect("recover cancel-requested attempt"),
        1
    );
    assert_eq!(
        store
            .load_attempt(&cancelled_attempt.id)
            .expect("load recovered cancellation")
            .state
            .status(),
        JobStatus::Cancelled
    );
    let rollback_job = store
        .submit_job(
            &submit("publish-rollback", "artifact-rollback"),
            time("2024-01-01T00:05:00Z"),
        )
        .expect("submit rollback job");
    let rollback_attempt = store
        .claim_next(time("2024-01-01T00:06:00Z"))
        .expect("claim rollback job")
        .expect("attempt");
    store.connection.execute_batch(
        "CREATE TRIGGER fail_attempt_publication BEFORE UPDATE OF status ON job_attempts WHEN NEW.status='COMPLETED' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    ).expect("inject publication failure");
    let rollback_report = report("rollback-report");
    let rollback_terminal = AttemptState::Completed {
        ended_at: time("2024-01-01T00:07:00Z"),
        output: JobOutput::Validation {
            report: rollback_report.clone(),
        },
    };
    assert!(
        store
            .publish_attempt(
                &rollback_attempt.id,
                rollback_terminal,
                AttemptPublication::Validation(rollback_report),
            )
            .is_err()
    );
    let rollback_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM validation_results WHERE check_id='rollback-report'",
            [],
            |row| row.get(0),
        )
        .expect("count rollback reports");
    assert_eq!(rollback_count, 0);
    assert_eq!(
        store
            .load_attempt(&rollback_attempt.id)
            .expect("load rollback attempt")
            .state
            .status(),
        JobStatus::Running
    );
    store
        .connection
        .execute_batch("DROP TRIGGER fail_attempt_publication;")
        .expect("remove publication failure");
    let completed_report = report("rollback-report");
    store
        .publish_attempt(
            &rollback_attempt.id,
            AttemptState::Completed {
                ended_at: time("2024-01-01T00:08:00Z"),
                output: JobOutput::Validation {
                    report: completed_report.clone(),
                },
            },
            AttemptPublication::Validation(completed_report),
        )
        .expect("publish validation after rollback");
    store
        .connection
        .execute(
            "UPDATE job_attempts SET progress_stage='queued',committed_records=0,last_committed_event_seq=0 WHERE id=?1",
            [rollback_attempt.id.as_str()],
        )
        .expect("simulate historical stale terminal progress");
    let projected = store
        .get_job(&rollback_job.id)
        .expect("load historical projection")
        .expect("job exists");
    assert_eq!(projected.attempts[0].progress.stage, "validation_completed");
    assert_eq!(projected.attempts[0].progress.committed_records, Some(0));
    assert_eq!(
        projected.attempts[0].progress.count_unit,
        Some(ProgressCountUnit::CheckedModels)
    );
    assert_eq!(
        projected.attempts[0].progress.last_committed_event_seq,
        None
    );
}

#[test]
fn prepared_dataset_is_not_frozen_after_cancellation_wins() {
    let root = TempRoot::new("dataset-publication-cancel");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"prepared dataset", time("2024-01-01T02:00:00Z"));
    let collect_request = request("prepared-collection");
    store
        .begin_collection(&collect_request)
        .expect("begin collection");
    let row = observation(&published.object, "100");
    store
        .commit_page(&page(
            &collect_request,
            published.object.clone(),
            row.clone(),
        ))
        .expect("commit prepared page");
    let prepared = snapshot(collect_request.clone(), published.object, row);
    let job = store
        .submit_job(
            &JobSubmission {
                request_id: RequestId::new("prepared-job").expect("job request id"),
                payload: JobPayload::Collect {
                    request: collect_request,
                },
            },
            time("2024-01-01T02:01:00Z"),
        )
        .expect("submit collection job");
    let attempt = store
        .claim_next(time("2024-01-01T02:02:00Z"))
        .expect("claim collection job")
        .expect("attempt");
    store
        .request_cancel(&job.id, time("2024-01-01T02:03:00Z"))
        .expect("request cancellation");
    let terminal = AttemptState::Completed {
        ended_at: time("2024-01-01T02:04:00Z"),
        output: JobOutput::Dataset {
            dataset_id: prepared.manifest.id.clone(),
        },
    };
    assert!(matches!(
        store.publish_attempt(
            &attempt.id,
            terminal,
            AttemptPublication::Dataset(Box::new(prepared.clone())),
        ),
        Err(LabError::Cancelled(_))
    ));
    assert!(
        store
            .load_dataset(&prepared.manifest.id)
            .expect("load cancelled dataset")
            .is_none()
    );
}

#[test]
fn root_reservations_share_capacity_with_raw_publication() {
    let root = TempRoot::new("root-reservation");
    let store = Store::open(&root.0).expect("open store");
    let guard = store
        .reserve_root_bytes(MAX_EXPORT_RESERVATION_BYTES)
        .expect("reserve bounded export");
    publish(
        &store,
        b"raw while export reserved",
        time("2024-01-01T02:00:00Z"),
    );
    assert!(matches!(
        store.reserve_root_bytes(MAX_DATA_ROOT_BYTES),
        Err(LabError::ResourceLimit(_))
    ));
    drop(guard);
    assert!(matches!(
        store.reserve_root_bytes(0),
        Err(LabError::InvalidConfig(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single derived publication and atomic rollback fixture"
)]
fn derived_dataset_and_lineage_commit_atomically() {
    const TRANSFORM: &str = "utc-ohlcv-sum-v1";
    let root = TempRoot::new("derived-lineage");
    let mut store = Store::open(&root.0).expect("open store");
    let published = publish(&store, b"four source bars", time("2024-01-01T05:00:00Z"));
    let source_request = CollectRequest {
        request_id: RequestId::new("source-four-hours").expect("request id"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(time("2024-01-01T00:00:00Z"), time("2024-01-01T04:00:00Z"))
            .expect("range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    };
    let source_rows = (0..4)
        .map(|hour| {
            observation_at(
                &published.object,
                &format!("2024-01-01T{hour:02}:00:00Z"),
                &format!("{}", 100 + hour),
            )
        })
        .collect::<Vec<_>>();
    let placeholder = ContentHash::of_bytes(b"placeholder");
    let mut source = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: "1".into(),
            id: DatasetId::new("source-placeholder").expect("dataset id"),
            request: source_request.clone(),
            coverage: source_request.range,
            status: DatasetStatus::Ready,
            row_count: 4,
            normalizer_version: crate::contracts::NORMALIZER_VERSION.into(),
            gap_policy: "reject".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder.clone(),
            origin: MarketDataOrigin::ExchangeObserved,
            raw_objects: vec![published.object.clone()],
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations: source_rows,
    };
    let source_digests = dataset_digests(&source).expect("source digests");
    source.manifest.id = dataset_id(&source_digests);
    source.manifest.semantic_digest = source_digests.semantic;
    source.manifest.provenance_digest = source_digests.provenance;
    let source_id = store
        .finish_dataset(&source)
        .expect("finish source dataset");

    let target = CandleInterval::H4;
    let request_seed =
        serde_json::to_string(&(source_id.clone(), target, TRANSFORM)).expect("request seed");
    let derived_request = CollectRequest {
        request_id: RequestId::from_seed(&request_seed),
        markets: source_request.markets.clone(),
        range: source_request.range,
        data_resolution: target,
        warmup_bars: 0,
        completed_only: true,
    };
    let derived_rows =
        crate::collection::resample(&source.observations, target).expect("resample source rows");
    let mut derived = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: source.manifest.schema_version.clone(),
            id: DatasetId::new("derived-placeholder").expect("dataset id"),
            request: derived_request.clone(),
            coverage: derived_request.range,
            status: DatasetStatus::Ready,
            row_count: 1,
            normalizer_version: format!("{}+{TRANSFORM}", crate::contracts::NORMALIZER_VERSION),
            gap_policy: source.manifest.gap_policy.clone(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: source.manifest.origin,
            raw_objects: source.manifest.raw_objects.clone(),
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations: derived_rows,
    };
    let derived_digests = dataset_digests(&derived).expect("derived digests");
    derived.manifest.id = dataset_id(&derived_digests);
    derived.manifest.semantic_digest = derived_digests.semantic;
    derived.manifest.provenance_digest = derived_digests.provenance;

    store.connection.execute_batch(
        "CREATE TRIGGER fail_derived_lineage BEFORE INSERT ON dataset_derivations BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    ).expect("inject lineage failure");
    assert!(
        store
            .finish_derived_dataset(&source_id, &derived, TRANSFORM)
            .is_err()
    );
    assert!(
        store
            .load_dataset(&derived.manifest.id)
            .expect("load after rollback")
            .is_none()
    );
    store
        .connection
        .execute_batch("DROP TRIGGER fail_derived_lineage;")
        .expect("remove failure trigger");
    let derived_id = store
        .finish_derived_dataset(&source_id, &derived, TRANSFORM)
        .expect("finish derived dataset");
    let lineage = store
        .load_dataset_derivation(&derived_id)
        .expect("load lineage")
        .expect("lineage exists");
    assert_eq!(lineage.source_dataset_id, source_id);
    assert_eq!(lineage.target_interval, target);
    assert_eq!(lineage.transform_version, TRANSFORM);
}

#[test]
fn semantic_fact_ids_are_isolated_by_run_and_model() {
    let root = TempRoot::new("run-scoped-fact-ids");
    let store = Store::open(&root.0).expect("open store");
    store.connection.execute_batch(
        r"
        INSERT INTO plans(id,request_id,config_digest,input_digest,original_request_json,resolved_plan_json)
        VALUES ('plan-a','plan-request-a','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','{}','{}');
        INSERT INTO plans(id,request_id,config_digest,input_digest,original_request_json,resolved_plan_json)
        VALUES ('plan-b','plan-request-b','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','{}','{}');
        INSERT INTO jobs VALUES ('job-a','job-request-a','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','BACKTEST','{}',1,'RUNNING',0);
        INSERT INTO jobs VALUES ('job-b','job-request-b','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','BACKTEST','{}',1,'RUNNING',0);
        INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms)
        VALUES ('attempt-a','job-a',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','RUNNING','{}','running',0,0,0,0);
        INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms)
        VALUES ('attempt-b','job-b',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','RUNNING','{}','running',0,0,0,0);
        INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json)
        VALUES ('run-a','job-a','attempt-a','plan-a','RUNNING',0,5,'{}');
        INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json)
        VALUES ('run-b','job-b','attempt-b','plan-b','RUNNING',0,5,'{}');
        INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json)
        VALUES ('run-a','model-x','KRW-BTC','S1','{}'),('run-b','model-x','KRW-BTC','S1','{}');
        INSERT INTO candle_observations VALUES ('obs-x','KRW-BTC','h1',0,3600000,'1','1','1','1','1','1',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{}');
        INSERT INTO run_events VALUES
          ('run-a',1,'model-x','SIGNAL',0,'signal-x'),('run-a',2,'model-x','ORDER_EVENT',0,'order-x'),
          ('run-a',3,'model-x','FILL',0,'fill-x'),('run-a',4,'model-x','ACCOUNT_MARK',0,'mark-4'),
          ('run-b',1,'model-x','SIGNAL',0,'signal-x'),('run-b',2,'model-x','ORDER_EVENT',0,'order-x'),
          ('run-b',3,'model-x','FILL',0,'fill-x'),('run-b',4,'model-x','ACCOUNT_MARK',0,'mark-4'),
          ('run-b',5,'model-x','SIGNAL',0,'signal-only-b');
        INSERT INTO signals VALUES
          ('signal-x','run-a','model-x',1,'KRW-BTC',0,0,1,'{}'),
          ('signal-x','run-b','model-x',1,'KRW-BTC',0,0,1,'{}'),
          ('signal-only-b','run-b','model-x',5,'KRW-BTC',0,0,1,'{}');
        INSERT INTO order_identities VALUES
          ('run-a','model-x','order-x','signal-x'),('run-b','model-x','order-x','signal-x');
        INSERT INTO order_events VALUES
          ('run-a','model-x',2,'order-x','CREATED',0,'{}'),
          ('run-b','model-x',2,'order-x','CREATED',0,'{}');
        INSERT INTO episode_identities VALUES
          ('run-a','model-x','episode-x'),('run-b','model-x','episode-x');
        INSERT INTO fills(fill_id,run_id,model_id,event_seq,order_id,episode_id,accounting_mark_seq,price_decimal,qty_decimal,notional_decimal,fee_decimal,source_bar_id,record_json,liquidity_source_bar_id,liquidity_source_close_time_ms)
        VALUES
          ('fill-x','run-a','model-x',3,'order-x','episode-x',4,'1','1','1','0','obs-x','{}','obs-x',3600000),
          ('fill-x','run-b','model-x',3,'order-x','episode-x',4,'1','1','1','0','obs-x','{}','obs-x',3600000);
        ",
    ).expect("insert identical semantic facts in two runs");
    let count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM signals WHERE signal_id='signal-x'",
            [],
            |row| row.get(0),
        )
        .expect("count signals");
    assert_eq!(count, 2);
    assert!(store.connection.execute(
        "INSERT INTO order_identities(run_id,model_id,order_id,parent_signal_id) VALUES ('run-a','model-x','cross-order','signal-only-b')",
        [],
    ).is_err());
    assert!(
        store
            .connection
            .execute(
                "INSERT INTO signals VALUES ('signal-x','run-a','model-x',5,'KRW-BTC',0,0,1,'{}')",
                [],
            )
            .is_err()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "single immutable policy CAS, history, freeze and isolation regression"
)]
fn policy_revisions_are_idempotent_cas_frozen_and_isolated() {
    let root = TempRoot::new("policy-registry");
    let mut store = Store::open(&root.0).expect("open store");
    let definition = |name: &str, description: &str| PolicyDefinition {
        schema_version: "1.0".into(),
        name: name.into(),
        description: description.into(),
        program: PolicyProgram::Builtin {
            strategy: StrategySpec::BuyAndHold,
        },
    };
    let create_a = PolicyWrite::Create {
        request_id: RequestId::new("policy-create-a").expect("request id"),
        definition: definition("Policy A", "first"),
    };
    let first = store
        .write_policy(&create_a, time("2024-01-01T00:00:00.123456789Z"))
        .expect("create policy A");
    assert_eq!(
        serde_json::to_value(
            store
                .write_policy(&create_a, time("2024-01-02T00:00:00Z"))
                .expect("idempotent create")
        )
        .expect("retry JSON"),
        serde_json::to_value(&first).expect("original JSON")
    );
    let frozen_plan_id = save_v2_policy_plan_fixture(&mut store, &first);
    let second_policy = store
        .write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new("policy-create-b").expect("request id"),
                definition: definition("Policy B", "isolated"),
            },
            time("2024-01-01T00:01:00Z"),
        )
        .expect("create policy B");
    assert_ne!(
        first.snapshot.reference.policy_id,
        second_policy.snapshot.reference.policy_id
    );
    let revise = PolicyWrite::Revise {
        request_id: RequestId::new("policy-revise-a").expect("request id"),
        policy_id: first.snapshot.reference.policy_id.clone(),
        expected_parent_revision_id: first.snapshot.reference.revision_id.clone(),
        definition: definition("Policy A", "second"),
    };
    let second = store
        .write_policy(&revise, time("2024-01-01T01:00:00Z"))
        .expect("revise policy A");
    assert_eq!(second.snapshot.revision_number, 2);
    assert!(matches!(
        store.write_policy(
            &PolicyWrite::Revise {
                request_id: RequestId::new("policy-stale-a").expect("request id"),
                policy_id: first.snapshot.reference.policy_id.clone(),
                expected_parent_revision_id: first.snapshot.reference.revision_id.clone(),
                definition: definition("Policy A", "stale"),
            },
            time("2024-01-01T02:00:00Z"),
        ),
        Err(LabError::Conflict(_))
    ));
    let loaded_first = store
        .load_policy_revision(&first.snapshot.reference)
        .expect("load first revision")
        .expect("first revision exists");
    assert_eq!(loaded_first.snapshot.definition.description, "first");
    let history = store
        .policy_history(&first.snapshot.reference.policy_id, None, 100)
        .expect("policy history");
    assert_eq!(history.items.len(), 2);
    assert_eq!(history.items[0].revision_number, 2);
    let listed = store.list_policies(None, 1).expect("list policies");
    assert_eq!(listed.items.len(), 1);
    assert!(listed.next_cursor.is_some());

    let builtins = crate::contracts::builtin_policy_definitions().expect("builtin definitions");
    store
        .seed_builtin_policies(&builtins, time("2024-01-01T03:00:00Z"))
        .expect("seed builtins");
    let (builtin_id, _, builtin_definition) = &builtins[0];
    let builtin_head = store
        .policy_history(builtin_id, None, 1)
        .expect("builtin history")
        .items[0]
        .reference
        .clone();
    let mut edited_builtin = builtin_definition.clone();
    edited_builtin.description = "edited builtin head".into();
    let edited = store
        .write_policy(
            &PolicyWrite::Revise {
                request_id: RequestId::new("edit-builtin").expect("request id"),
                policy_id: builtin_id.clone(),
                expected_parent_revision_id: builtin_head.revision_id,
                definition: edited_builtin,
            },
            time("2024-01-01T04:00:00Z"),
        )
        .expect("edit builtin");
    store
        .seed_builtin_policies(&builtins, time("2024-01-01T05:00:00Z"))
        .expect("reopen seed skips existing builtins");
    let after_reseed = store
        .policy_history(builtin_id, None, 1)
        .expect("builtin history after reseed");
    assert_eq!(after_reseed.items[0].reference, edited.snapshot.reference);

    let frozen_plan = store
        .load_plan(&frozen_plan_id)
        .expect("load frozen plan")
        .expect("frozen plan exists");
    assert_eq!(
        frozen_plan.resolved.policy_revisions[0].reference,
        first.snapshot.reference
    );
    let original_json = serde_json::to_string(&frozen_plan.resolved).expect("plan json");
    let mut tampered = frozen_plan.resolved.clone();
    tampered.policy_revisions[0].definition.description = "tampered frozen body".into();
    let tampered_digest =
        ContentHash::of_value(&tampered.policy_revisions[0].definition).expect("tampered digest");
    tampered.policy_revisions[0].reference.definition_digest = tampered_digest;
    let tampered_reference = tampered.policy_revisions[0].reference.clone();
    tampered.spec.policy_selections[0] = tampered_reference.clone();
    tampered.admissions[0].policy_ref = Some(tampered_reference);
    store
        .connection
        .execute(
            "UPDATE plans SET resolved_plan_json=?1 WHERE id=?2",
            params![
                serde_json::to_string(&tampered).expect("tampered plan json"),
                frozen_plan_id.as_str()
            ],
        )
        .expect("tamper resolved plan body");
    assert!(store.load_plan(&frozen_plan_id).is_err());
    store
        .connection
        .execute(
            "UPDATE plans SET resolved_plan_json=?1 WHERE id=?2",
            params![original_json, frozen_plan_id.as_str()],
        )
        .expect("restore resolved plan json");
    store
        .connection
        .execute(
            "UPDATE plan_policy_revisions SET frozen_json=?1 WHERE plan_id=?2",
            params![
                serde_json::to_string(&second.snapshot).expect("second frozen json"),
                frozen_plan_id.as_str()
            ],
        )
        .expect("tamper frozen policy row");
    assert!(store.load_plan(&frozen_plan_id).is_err());
}

fn hard_delete_request(preview: &DeletePreview, cascade: bool) -> HardDeleteRequest {
    HardDeleteRequest {
        preview: preview.clone(),
        cascade,
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers cascade scope, restrict refusal, restart recovery and reseeding"
)]
fn policy_cascade_delete_removes_frozen_plans_and_reseeding_stays_explicit() {
    let root = TempRoot::new("delete-policy-cascade");
    let mut store = Store::open(&root.0).expect("open store");
    let builtins = builtin_policy_definitions().expect("builtin policy definitions");
    assert!(
        store
            .seed_builtin_policies_once(&builtins, time("2024-01-01T00:00:00Z"))
            .expect("first seed")
    );
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "Research Policy".into(),
        description: "user research policy".into(),
        program: PolicyProgram::Builtin {
            strategy: StrategySpec::BuyAndHold,
        },
    };
    let policy = store
        .write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new("delete-policy-create").expect("request id"),
                definition,
            },
            time("2024-01-01T00:01:00Z"),
        )
        .expect("create policy");
    let frozen_plan_id = save_v2_policy_plan_fixture(&mut store, &policy);
    let plan_dataset_ids = store
        .load_plan(&frozen_plan_id)
        .expect("load frozen plan")
        .expect("plan present")
        .resolved
        .spec
        .dataset_ids
        .clone();

    let resource = DeleteResource::Policy {
        policy_id: policy.snapshot.reference.policy_id.clone(),
    };
    let preview = store
        .delete_preview(&resource, time("2024-01-02T00:00:00Z"))
        .expect("preview");
    assert!(preview.cascade_required);
    assert!(
        preview
            .blockers
            .iter()
            .any(|blocker| blocker.class == DeleteBlockerClass::LiveReference)
    );
    assert!(
        preview
            .cascade
            .iter()
            .any(|group| group.kind == DeleteEntryKind::Plan)
    );
    // Restrict refuses without cascade.
    assert!(matches!(
        store.execute_hard_delete(
            &hard_delete_request(&preview, false),
            time("2024-01-02T00:01:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    let outcome = store
        .execute_hard_delete(
            &hard_delete_request(&preview, true),
            time("2024-01-02T00:01:00Z"),
        )
        .expect("cascade delete");
    assert!(outcome.vacuumed);
    assert!(
        store
            .load_policy_revision(&policy.snapshot.reference)
            .expect("load")
            .is_none()
    );
    assert!(
        store
            .load_plan(&frozen_plan_id)
            .expect("load plan")
            .is_none()
    );
    // Frozen plans reference datasets but never own them: the shared snapshots
    // survive and only the plan (and its runs) go with the policy.
    for dataset_id in &plan_dataset_ids {
        assert!(store.load_dataset(dataset_id).expect("load").is_some());
    }
    assert!(matches!(
        store.delete_preview(&resource, time("2024-01-02T00:02:00Z")),
        Err(LabError::InvalidConfig(_))
    ));

    // A deleted built-in is never re-created by a plain reopen or reseed-once.
    assert!(store.list_policies(None, 100).is_ok());
    store.close().expect("close");
    let mut reopened = Store::open(&root.0).expect("reopen");
    assert!(
        !reopened
            .seed_builtin_policies_once(&builtins, time("2024-01-03T00:00:00Z"))
            .expect("seed once after reopen")
    );
    assert!(
        reopened
            .load_policy_revision(&policy.snapshot.reference)
            .expect("load")
            .is_none()
    );
    // Explicit reinstallation is the only path back, and it restores only
    // builtins: the deleted user research policy stays gone.
    reopened
        .seed_builtin_policies(&builtins, time("2024-01-04T00:00:00Z"))
        .expect("explicit reinstall");
    let reinstalled = reopened
        .list_policies(None, 100)
        .expect("list after reinstall");
    let ids: Vec<_> = reinstalled
        .items
        .iter()
        .map(|header| header.policy_id.clone())
        .collect();
    assert!(ids.contains(&builtins[0].0));
    assert!(!ids.contains(&policy.snapshot.reference.policy_id));
    reopened.close().expect("close");
}

#[test]
fn dataset_hard_delete_removes_exclusive_files_refuses_stale_and_active_jobs() {
    let root = TempRoot::new("delete-dataset-files");
    let mut store = Store::open(&root.0).expect("open store");
    let first = publish(&store, b"delete-dataset-a", time("2024-01-01T02:00:00Z"));
    let second = publish(&store, b"delete-dataset-b", time("2024-01-01T02:01:00Z"));
    let request_a = request("delete-a");
    let request_b = request("delete-b");
    let dataset_a = store
        .finish_dataset(&snapshot(
            request_a.clone(),
            first.object.clone(),
            observation(&first.object, "100"),
        ))
        .expect("finish dataset a");
    let dataset_b = store
        .finish_dataset(&snapshot(
            request_b.clone(),
            second.object.clone(),
            observation(&second.object, "200"),
        ))
        .expect("finish dataset b");
    let raw_a_path = root.0.join(&first.object.relative_path);
    let raw_b_path = root.0.join(&second.object.relative_path);
    assert!(raw_a_path.exists() && raw_b_path.exists());

    let resource = DeleteResource::Dataset {
        dataset_id: dataset_a.clone(),
    };
    let preview = store
        .delete_preview(&resource, time("2024-01-02T00:00:00Z"))
        .expect("preview");
    assert!(!preview.cascade_required);
    assert_eq!(preview.exclusive_file_count, 1);
    assert_eq!(preview.blockers, Vec::new());
    // A stale digest cannot execute even inside the validity window.
    let mut stale = hard_delete_request(&preview, false);
    stale.preview.scope_digest = ContentHash::of_bytes(b"stale-scope");
    assert!(matches!(
        store.execute_hard_delete(&stale, time("2024-01-02T00:01:00Z")),
        Err(LabError::Conflict(_))
    ));
    // An expired preview is refused even with a valid token.
    let expired = hard_delete_request(&preview, false);
    assert!(matches!(
        store.execute_hard_delete(&expired, preview.expires_at),
        Err(LabError::Conflict(_))
    ));

    let outcome = store
        .execute_hard_delete(
            &hard_delete_request(&preview, false),
            time("2024-01-02T00:01:00Z"),
        )
        .expect("delete dataset a");
    assert_eq!(outcome.deleted_files, 1);
    assert!(!raw_a_path.exists());
    assert!(raw_b_path.exists());
    assert!(store.load_dataset(&dataset_a).expect("load").is_none());
    assert!(store.load_dataset(&dataset_b).expect("load").is_some());

    // Queued jobs in scope refuse deletion even with cascade.
    let job = store
        .submit_job(
            &JobSubmission {
                request_id: RequestId::new("delete-active-job").expect("request id"),
                payload: JobPayload::Collect {
                    request: request("delete-active-job-data"),
                },
            },
            time("2024-01-02T00:02:00Z"),
        )
        .expect("submit queued job");
    let preview = store
        .delete_preview(
            &DeleteResource::Job {
                job_id: job.id.clone(),
            },
            time("2024-01-02T00:03:00Z"),
        )
        .expect("preview active job");
    assert!(
        preview
            .blockers
            .iter()
            .any(|blocker| blocker.class == DeleteBlockerClass::ActiveJob)
    );
    assert!(matches!(
        store.execute_hard_delete(
            &hard_delete_request(&preview, true),
            time("2024-01-02T00:03:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    // The store still closes cleanly with the queued job retained.
    store.close().expect("close");
}

#[test]
fn slim_pages_store_headers_only_and_legacy_rows_still_rebuild() {
    let root = TempRoot::new("slim-legacy-pages");
    let mut store = Store::open(&root.0).expect("open store");
    let first = publish(&store, b"slim-page-raw", time("2024-01-01T02:00:00Z"));
    let second = publish(&store, b"legacy-page-raw", time("2024-01-01T02:01:00Z"));
    let request = request("slim-legacy");
    store.begin_collection(&request).expect("begin collection");

    let slim_page = page(
        &request,
        first.object.clone(),
        observation_at(&first.object, "2024-01-01T00:00:00Z", "100"),
    );
    store.commit_page(&slim_page).expect("commit slim page");
    store.commit_page(&slim_page).expect("idempotent recommit");

    // The legacy writer is simulated below with raw SQL, so the raw row the
    // old page references must be catalogued first, exactly as the old binary
    // did inside its own commit transaction.
    store
        .catalog_raw(&second.object)
        .expect("catalog legacy raw");
    let mut legacy = page(
        &request,
        second.object.clone(),
        observation_at(&second.object, "2024-01-01T01:00:00Z", "200"),
    );
    legacy.page_index = 1;
    let legacy_json = serde_json::to_string(&legacy).expect("legacy page json");
    let legacy_digest = ContentHash::of_bytes(legacy_json.as_bytes());
    store
        .connection
        .execute(
            "INSERT INTO collection_pages(request_id,market,page_index,requested_to_ms,next_to_ms,raw_object_id,page_digest,page_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                legacy.request_id.as_str(),
                legacy.market.code(),
                1_i64,
                timestamp_ms(legacy.requested_to),
                legacy.next_to.map(timestamp_ms),
                legacy.raw_object.id.as_str(),
                legacy_digest.as_str(),
                legacy_json
            ],
        )
        .expect("insert legacy page row");
    let transaction = store.connection.transaction().expect("begin legacy txn");
    insert_observation(&transaction, &legacy.observations[0]).expect("insert legacy observation");
    transaction
        .execute(
            "INSERT INTO collection_page_members(request_id,market,page_index,observation_id,position) VALUES (?1,?2,?3,?4,?5)",
            params![
                legacy.request_id.as_str(),
                legacy.market.code(),
                1_i64,
                legacy.observations[0].id.as_str(),
                0_i64
            ],
        )
        .expect("insert legacy page member");
    transaction.commit().expect("commit legacy page");

    let (slim_bytes, slim_digest): (i64, String) = store
        .connection
        .query_row(
            "SELECT length(page_json),page_digest FROM collection_pages WHERE page_index=0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read slim page row");
    let expected_digest = ContentHash::of_bytes(
        serde_json::to_string(
            &SlimCollectionPage::from_collection_page(&slim_page).expect("slim header"),
        )
        .expect("slim json")
        .as_bytes(),
    );
    assert!(slim_bytes < 2048, "slim page stored {slim_bytes} bytes");
    assert_eq!(slim_digest, expected_digest.as_str());

    let loaded = store
        .load_collection_pages(&request.request_id, &request.markets[0])
        .expect("load mixed pages");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].page_index, 0);
    assert_eq!(loaded[0].observations.len(), 1);
    assert_eq!(
        loaded[0].observations[0].raw_object_ids,
        vec![first.object.id.clone()]
    );
    assert_eq!(
        loaded[0].observations[0].content_digest,
        slim_page.observations[0].content_digest
    );
    assert_eq!(loaded[1].page_index, 1);
    assert_eq!(loaded[1].observations.len(), 1);
    assert_eq!(
        loaded[1].observations[0].raw_object_ids,
        vec![second.object.id.clone()]
    );
    assert_eq!(
        loaded[1].observations[0].content_digest,
        legacy.observations[0].content_digest
    );
    store.close().expect("close");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one Store journey compares all three projections across compaction"
)]
fn compacted_multi_model_bundle_summary_and_costs_match_detail() {
    let root = TempRoot::new("compacted-multi-model-readers");
    let mut store = Store::open(&root.0).expect("open store");
    let mut expected = crate::reporting::tests::multi_market_fixture();
    assert_eq!(expected.models.len(), 2);
    assert!(expected.models.iter().all(|model| !model.orders.is_empty()));
    let range_end = expected.plan.spec.range.end();
    for model in &mut expected.models {
        let terminal = model.account_marks.last_mut().expect("final account mark");
        assert_eq!(terminal.context.accounting_event_time, range_end);
        terminal.kind = crate::contracts::MarkKind::Terminal;
    }

    let dataset = &expected.datasets[0];
    store
        .begin_collection(&dataset.manifest.request)
        .expect("register fixture collection");
    let prepared = PreparedDatasetPublication {
        digests: dataset_digests(dataset).expect("fixture dataset digests"),
    };
    let transaction = store.connection.transaction().expect("dataset transaction");
    insert_dataset_rows(&transaction, dataset, &prepared, None).expect("persist fixture dataset");
    transaction.commit().expect("commit fixture dataset");

    store
        .save_plan(
            &PlanRequest {
                request_id: RequestId::new("compacted-reader-plan").expect("plan request id"),
                spec: expected.plan.spec.clone(),
            },
            &expected.plan,
        )
        .expect("persist fixture plan");
    let run_request = RunRequest {
        request_id: RequestId::new("compacted-reader-run").expect("run request id"),
        plan_id: expected.plan.id.clone(),
        input_digest: expected.plan.input_digest.clone(),
    };
    let job = store
        .submit_job(
            &JobSubmission {
                request_id: RequestId::new("compacted-reader-job").expect("job request id"),
                payload: JobPayload::Backtest {
                    request: run_request,
                },
            },
            time("2024-01-02T00:00:00Z"),
        )
        .expect("submit fixture run");
    let attempt = store
        .claim_next(time("2024-01-02T00:00:01Z"))
        .expect("claim fixture run")
        .expect("running fixture attempt");
    expected.manifest.job_id = job.id;
    expected.manifest.attempt_id = attempt.id.clone();
    store
        .begin_run(&RunHeader {
            manifest: expected.manifest.clone(),
            plan_id: expected.plan.id.clone(),
            state: JobStatus::Running,
            last_committed_event_seq: 0,
            semantic_digest: None,
        })
        .expect("begin fixture run");

    for model in &expected.models {
        store
            .append_model_facts(
                &expected.manifest.run_id,
                &model.model_id,
                &ModelFactBatch {
                    signals: model.signals.clone(),
                    order_events: model.order_events.clone(),
                    fills: model.fills.clone(),
                    account_marks: model.account_marks.clone(),
                },
            )
            .expect("append fixture facts");
        store
            .finish_model(&expected.manifest.run_id, model)
            .expect("finish fixture model");
    }
    expected.semantic_digest = crate::reporting::semantic_digest(&expected).expect("run digest");
    let validation = crate::reporting::verify_run(&expected);
    assert_eq!(validation.status, ValidationStatus::Pass);
    store
        .publish_attempt(
            &attempt.id,
            AttemptState::Completed {
                ended_at: time("2024-01-02T00:00:02Z"),
                output: JobOutput::Run {
                    run_id: expected.manifest.run_id.clone(),
                    completed_models: 2,
                    blocked_models: 0,
                },
            },
            AttemptPublication::Run {
                run_id: expected.manifest.run_id.clone(),
                semantic_digest: expected.semantic_digest.clone(),
                expected_models: 2,
                comparisons: crate::reporting::build_comparisons(
                    &expected,
                    &crate::research::causal_input_digest(
                        &expected.plan,
                        &expected.datasets,
                        expected.evidence.as_ref(),
                    )
                    .expect("fixture causal digest"),
                )
                .expect("fixture comparisons"),
                validation,
            },
        )
        .expect("publish fixture run");

    let detail_bundle = store
        .load_run_bundle(&expected.manifest.run_id)
        .expect("load detail bundle")
        .expect("detail bundle");
    let detail_summary = store
        .load_run_summary(&expected.manifest.run_id)
        .expect("load detail summary")
        .expect("detail summary");
    let detail_costs = expected
        .models
        .iter()
        .map(|model| {
            store
                .load_model_cost_summary(&expected.manifest.run_id, &model.model_id)
                .expect("load detail costs")
                .expect("detail costs")
        })
        .collect::<Vec<_>>();

    store
        .seal_and_compact_run_ledger(&expected.manifest.run_id)
        .expect("seal and compact fixture run");
    let compacted_bundle = store
        .load_run_bundle(&expected.manifest.run_id)
        .expect("load compacted bundle")
        .expect("compacted bundle");
    let compacted_summary = store
        .load_run_summary(&expected.manifest.run_id)
        .expect("load compacted summary")
        .expect("compacted summary");
    let compacted_costs = expected
        .models
        .iter()
        .map(|model| {
            store
                .load_model_cost_summary(&expected.manifest.run_id, &model.model_id)
                .expect("load compacted costs")
                .expect("compacted costs")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        serde_json::to_value(compacted_bundle).expect("compacted bundle JSON"),
        serde_json::to_value(detail_bundle).expect("detail bundle JSON")
    );
    assert_eq!(
        serde_json::to_value(compacted_summary).expect("compacted summary JSON"),
        serde_json::to_value(detail_summary).expect("detail summary JSON")
    );
    assert_eq!(
        serde_json::to_value(compacted_costs).expect("compacted costs JSON"),
        serde_json::to_value(detail_costs).expect("detail costs JSON")
    );
    store.close().expect("close store");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers seal, readback, compaction, corrupt rejection and delete integration"
)]
fn run_ledger_sealing_round_trips_compacts_and_joins_hard_delete() {
    let root = TempRoot::new("ledger-seal");
    let mut store = Store::open(&root.0).expect("open store");
    store
        .connection
        .execute_batch(
            r#"
        INSERT INTO plans(id,request_id,config_digest,input_digest,original_request_json,resolved_plan_json)
        VALUES ('plan-seal','plan-request-seal','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','{}','{}');
        INSERT INTO jobs VALUES ('job-seal','job-request-seal','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','BACKTEST','{}',1,'COMPLETED',0);
        INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms)
        VALUES ('attempt-seal','job-seal',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','COMPLETED','{}','done',0,4,0,0);
        INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json,ledger_state)
        VALUES ('run-seal','job-seal','attempt-seal','plan-seal','COMPLETED',0,4,'{}','DETAIL');
        INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,status,last_committed_event_seq)
        VALUES ('run-seal','model-seal','KRW-BTC','S1','{}','COMPLETED',4);
        INSERT INTO run_events VALUES
          ('run-seal',1,'model-seal','SIGNAL',100,'signal-seal'),
          ('run-seal',2,'model-seal','ORDER_EVENT',200,'order-seal'),
          ('run-seal',3,'model-seal','FILL',300,'fill-seal'),
          ('run-seal',4,'model-seal','ACCOUNT_MARK',400,'mark-4');
        INSERT INTO signals VALUES
          ('signal-seal','run-seal','model-seal',1,'KRW-BTC',100,100,1,'{"marker":"signal"}');
        INSERT INTO order_identities VALUES
          ('run-seal','model-seal','order-seal','signal-seal');
        INSERT INTO order_events VALUES
          ('run-seal','model-seal',2,'order-seal','CREATED',200,'{"marker":"order_event"}');
        INSERT INTO candle_observations VALUES ('obs-seal','KRW-BTC','h1',0,3600000,'1','1','1','1','1','1',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{}');
        INSERT INTO episode_identities VALUES
          ('run-seal','model-seal','episode-seal');
        INSERT INTO episodes(run_id,model_id,episode_id,status,opened_at_ms,closed_at_ms,net_realized_decimal,fees_decimal,record_json)
        VALUES ('run-seal','model-seal','episode-seal','OPEN',100,NULL,'0','0','{}');
        INSERT INTO fills(fill_id,run_id,model_id,event_seq,order_id,episode_id,accounting_mark_seq,price_decimal,qty_decimal,notional_decimal,fee_decimal,source_bar_id,record_json,liquidity_source_bar_id,liquidity_source_close_time_ms)
        VALUES ('fill-seal','run-seal','model-seal',3,'order-seal','episode-seal',4,'1','1','1','0','obs-seal','{"marker":"fill"}','obs-seal',3600000);
        INSERT INTO episode_fills(run_id,model_id,episode_id,fill_id,position) VALUES ('run-seal','model-seal','episode-seal','fill-seal',0);
        INSERT INTO account_marks VALUES
          ('run-seal','model-seal',4,'INITIAL','1','0','0','0','0','0','1','obs-seal','{"marker":"mark"}');
        "#,
        )
        .expect("insert sealed-run fixture");
    let run_id = RunId::new("run-seal").expect("run id");
    let fact_rows = |store: &Store, table: &str| -> i64 {
        store
            .connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE run_id='run-seal'"),
                [],
                |row| row.get(0),
            )
            .expect("count fact rows")
    };

    // Detail growth before sealing (the Before state).
    let detail_rows: i64 = [
        "signals",
        "order_events",
        "orders",
        "fills",
        "account_marks",
        "run_events",
    ]
    .iter()
    .map(|table| fact_rows(&store, table))
    .sum();
    assert_eq!(detail_rows, 8);

    let stats = store.seal_run_ledger(&run_id).expect("seal run ledger");
    assert_eq!(stats.chunks, 1);
    // 4 run_events + 3 event-keyed fact rows (signals, order_events, fills,
    // account_marks); this fixture intentionally omits the final orders
    // projection row.
    assert_eq!(stats.events, 4);
    assert_eq!(
        store.run_ledger_state(&run_id).expect("ledger state"),
        super::ledger_seal::RunLedgerState::Sealed
    );
    let chunk_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM run_ledger_chunks", [], |row| {
            row.get(0)
        })
        .expect("chunk rows");
    assert_eq!(chunk_count, 1);
    // Sealing does not remove anything by itself.
    assert_eq!(
        [
            "signals",
            "order_events",
            "orders",
            "fills",
            "account_marks",
            "run_events"
        ]
        .iter()
        .map(|table| fact_rows(&store, table))
        .sum::<i64>(),
        detail_rows
    );
    let chunk_files = read_directories(&root.0.join("ledgers"))
        .expect("shards")
        .len();
    assert!(chunk_files > 0, "chunk file must be published");

    // Compaction removes the detail rows but keeps episodes and identities.
    let removed = store.compact_run_ledger(&run_id).expect("compact");
    assert!(removed > 0);
    assert_eq!(
        super::ledger_seal::RunLedgerState::Compacted,
        store.run_ledger_state(&run_id).expect("ledger state")
    );
    for table in [
        "signals",
        "order_events",
        "orders",
        "fills",
        "account_marks",
        "run_events",
        "order_identities",
        "episode_fills",
        "episode_orders",
    ] {
        assert_eq!(fact_rows(&store, table), 0, "{table} must be compacted");
    }
    let episodes: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM episode_identities", [], |row| {
            row.get(0)
        })
        .expect("episode identities kept");
    assert_eq!(episodes, 1);

    // Sealed lines still round-trip after compaction.
    let lines = store
        .load_run_ledger_lines(&run_id)
        .expect("sealed lines")
        .expect("sealed run must return lines");
    assert_eq!(lines.len(), 4);
    let mut kinds: Vec<&'static str> = lines
        .iter()
        .map(super::ledger_seal::LedgerLine::kind)
        .collect();
    kinds.sort_unstable();
    assert_eq!(kinds, vec!["ACCOUNT_MARK", "FILL", "ORDER_EVENT", "SIGNAL"]);

    // Corrupt chunk bytes are rejected, never served as a normal run.
    let chunk_file = std::fs::read_dir(root.0.join("ledgers"))
        .expect("shards")
        .next()
        .expect("one shard")
        .expect("shard entry")
        .path();
    let chunk_path = std::fs::read_dir(&chunk_file)
        .expect("chunk dir")
        .next()
        .expect("chunk file")
        .expect("chunk entry")
        .path();
    let mut bytes = std::fs::read(&chunk_path).expect("chunk bytes");
    let flip = bytes.len() / 2;
    bytes[flip] ^= 0xFF;
    std::fs::write(&chunk_path, &bytes).expect("corrupt chunk");
    assert!(store.load_run_ledger_lines(&run_id).is_err());
    std::fs::write(&chunk_path, {
        bytes[flip] ^= 0xFF;
        &bytes
    })
    .expect("restore chunk bytes");

    // Persisted chunk counts never degrade to zero/default on corruption.
    store
        .connection
        .execute_batch("PRAGMA ignore_check_constraints=ON")
        .expect("allow corrupt metadata only in this fixture");
    store
        .connection
        .execute(
            "UPDATE run_ledger_chunks SET event_count=-1 WHERE run_id=?1",
            [run_id.as_str()],
        )
        .expect("corrupt stored event count");
    assert!(matches!(
        store.load_run_ledger_lines(&run_id),
        Err(LabError::DataCorrupt(_))
    ));
    store
        .connection
        .execute(
            "UPDATE run_ledger_chunks SET event_count=4 WHERE run_id=?1",
            [run_id.as_str()],
        )
        .expect("restore stored event count");
    store
        .connection
        .execute_batch("PRAGMA ignore_check_constraints=OFF")
        .expect("restore fixture constraint checks");

    // A COMPACTED run without its chunk catalogue is corrupt, not an empty ledger.
    store
        .connection
        .execute_batch(
            "CREATE TEMP TABLE saved_run_ledger_chunk AS SELECT * FROM run_ledger_chunks WHERE run_id='run-seal';
             DELETE FROM run_ledger_chunks WHERE run_id='run-seal';",
        )
        .expect("detach chunk catalogue");
    assert!(matches!(
        super::ledger_seal::run_ledger_source(&store, &run_id),
        Err(LabError::DataCorrupt(_))
    ));
    store
        .connection
        .execute_batch(
            "INSERT INTO run_ledger_chunks SELECT * FROM saved_run_ledger_chunk;
             DROP TABLE saved_run_ledger_chunk;",
        )
        .expect("restore chunk catalogue");

    // Hard delete covers the sealed chunk catalog and its file.
    let preview = store
        .delete_preview(
            &DeleteResource::Run {
                run_id: run_id.clone(),
            },
            time("2024-01-01T02:30:00Z"),
        )
        .expect("delete preview");
    assert!(
        preview
            .cascade
            .iter()
            .any(|group| group.kind == DeleteEntryKind::Artifact && group.count == 1),
        "preview must disclose the sealed chunk file"
    );
    let outcome = store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview: preview.clone(),
                cascade: true,
            },
            time("2024-01-01T02:35:00Z"),
        )
        .expect("hard delete sealed run");
    assert_eq!(outcome.deleted_files, 1);
    assert!(!chunk_path.exists());
    // The run is gone entirely: no ledger lines can ever be served for it.
    assert!(matches!(
        store.load_run_ledger_lines(&run_id),
        Ok(None) | Err(LabError::InvalidConfig(_))
    ));
    store.close().expect("close");
}

#[test]
fn sealed_writes_shrink_per_run_sqlite_growth() {
    let root = TempRoot::new("ledger-growth");
    let mut store = Store::open(&root.0).expect("open store");
    store
        .connection
        .execute_batch(
            r"
        INSERT INTO plans(id,request_id,config_digest,input_digest,original_request_json,resolved_plan_json)
        VALUES ('plan-growth','plan-request-growth','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','{}','{}');
        INSERT INTO jobs VALUES ('job-growth','job-request-growth','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','BACKTEST','{}',1,'COMPLETED',0);
        INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms)
        VALUES ('attempt-growth','job-growth',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','COMPLETED','{}','done',0,600,0,0);
        INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json,ledger_state)
        VALUES ('run-growth','job-growth','attempt-growth','plan-growth','COMPLETED',0,600,'{}','DETAIL');
        INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,status,last_committed_event_seq)
        VALUES ('run-growth','model-growth','KRW-BTC','S1','{}','COMPLETED',600);
        INSERT INTO candle_observations VALUES ('obs-growth','KRW-BTC','h1',0,3600000,'1','1','1','1','1','1',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{}');
        ",
        )
        .expect("insert growth fixture");
    for seq in 1..=300_i64 {
        store
            .connection
            .execute(
                "INSERT INTO run_events VALUES ('run-growth',?1,'model-growth','SIGNAL',?2,'signal-growth')",
                rusqlite::params![seq, seq * 100],
            )
            .expect("insert event");
        store
            .connection
            .execute(
                "INSERT INTO signals VALUES ('signal-growth-'||?1,'run-growth','model-growth',?1,'KRW-BTC',?2,?2,1,'{\"pad\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}')",
                rusqlite::params![seq, seq * 100],
            )
            .expect("insert signal");
    }
    let pages_before: i64 = store
        .connection
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .expect("page count");
    let stats = store
        .seal_run_ledger(&RunId::new("run-growth").expect("run id"))
        .expect("seal");
    store
        .compact_run_ledger(&RunId::new("run-growth").expect("run id"))
        .expect("compact");
    store
        .connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
    let pages_after: i64 = store
        .connection
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .expect("page count");
    let freelist: i64 = store
        .connection
        .pragma_query_value(None, "freelist_count", |row| row.get(0))
        .expect("freelist");
    println!(
        "detail_pages_before_seal={pages_before} pages_after_compact={pages_after} reusable_freelist_pages={freelist} sealed_chunks={} sealed_compressed_bytes={} sealed_uncompressed_bytes={}",
        stats.chunks, stats.compressed_bytes, stats.uncompressed_bytes
    );
    // Freed detail pages return to the freelist for reuse by the next run; the
    // file shrinks only via an explicit VACUUM, which compaction does not run.
    assert!(freelist > 0, "compaction must return pages for reuse");
    assert!(pages_after <= pages_before);
    store.close().expect("close");
}

#[test]
fn parameter_sweep_freezes_deterministic_policies_and_reuses_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{ParameterSweepMode, PolicyRevisionRef, StrategySpec, SweepValue};
    let root = TempRoot::new("policy-sweep");
    let mut store = Store::open(&root.0)?;
    let template = StrategySpec::S2 {
        entry_length: 20,
        exit_length: 10,
    };
    let tuple = |entry: u32, exit: u32| {
        std::collections::BTreeMap::from([
            ("entry_length".to_string(), SweepValue::Integer(entry)),
            ("exit_length".to_string(), SweepValue::Integer(exit)),
        ])
    };
    let mode = ParameterSweepMode::Tuples {
        tuples: vec![tuple(20, 10), tuple(55, 20), tuple(20, 10)],
    };
    let now = time("2024-01-01T00:00:00Z");
    let first = store.sweep_policy(
        &RequestId::new("sweep-one")?,
        StrategyKind::S2,
        &template,
        &mode,
        now,
    )?;
    assert_eq!(first.plan.candidates.len(), 2, "duplicate tuple suppressed");
    assert_eq!(first.plan.duplicates_suppressed, 1);
    assert_eq!(first.revisions.len(), 2);
    for revision in &first.revisions {
        assert_eq!(revision.revision_number, 1);
    }
    // Rerunning the same request is idempotent and reuses the policies.
    let rerun = store.sweep_policy(
        &RequestId::new("sweep-one")?,
        StrategyKind::S2,
        &template,
        &mode,
        now,
    )?;
    let references = |result: &crate::contracts::PolicySweepResult| {
        result
            .revisions
            .iter()
            .map(|revision| revision.reference.clone())
            .collect::<std::collections::BTreeSet<PolicyRevisionRef>>()
    };
    assert_eq!(references(&first), references(&rerun));
    // A different request with the same candidates reuses the same policies.
    let reused = store.sweep_policy(
        &RequestId::new("sweep-two")?,
        StrategyKind::S2,
        &template,
        &mode,
        now,
    )?;
    assert_eq!(references(&first), references(&reused));
    // The frozen definitions differ per candidate and stay loadable.
    let loaded = store
        .load_policy_revision(&first.revisions[1].reference)?
        .expect("reused revision loads");
    let crate::contracts::FrozenPolicyRevision {
        definition, family, ..
    } = loaded.snapshot;
    assert_eq!(family, StrategyKind::S2);
    let crate::contracts::PolicyDefinition {
        program: crate::contracts::PolicyProgram::Builtin { strategy },
        ..
    } = definition
    else {
        panic!("sweep candidates are builtin definitions");
    };
    assert!(matches!(
        strategy,
        StrategySpec::S2 {
            entry_length: 55,
            exit_length: 20
        }
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded ledger journey proves publish, paging, rejection and failure rows"
)]
fn portfolio_run_publishes_atomically_and_pages_facts() -> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        ArbitrationPolicy, AssetQuantity, BasisPoints, ContentHash, PortfolioAssetSpec,
        PortfolioFillRecord, PortfolioMarkRecord, PortfolioRejectionReason,
        PortfolioRejectionRecord, PortfolioRiskPolicy, PortfolioRunRequest, PortfolioSpec,
        PortfolioTotals, PriceKrw, QuoteAmount, RunId, Side, Weight,
    };
    use std::str::FromStr;
    let root = TempRoot::new("portfolio-store");
    let mut store = Store::open(&root.0)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let request = PortfolioRunRequest {
        request_id: RequestId::new("portfolio-store-request")?,
        plan_id: crate::contracts::PlanId::new("portfolio-plan")?,
        input_digest: ContentHash::of_bytes(b"portfolio-store-input"),
        portfolio: PortfolioSpec {
            initial_cash: QuoteAmount::new(Decimal::from(300_000))?,
            assets: vec![PortfolioAssetSpec {
                market: market.clone(),
                max_weight: Weight::new(Decimal::new(45, 2))?,
            }],
            risk: PortfolioRiskPolicy {
                max_gross_exposure: Weight::new(Decimal::new(70, 2))?,
                min_cash_weight: Weight::new(Decimal::new(20, 2))?,
                drawdown_stop: Weight::new(Decimal::new(15, 3))?,
            },
            arbitration: ArbitrationPolicy::Priority,
        },
        regime: None,
    };
    let run_id = RunId::new("run-portfolio-store")?;
    // portfolio_runs.plan_id references the frozen plan row.
    store.connection.execute(
        "INSERT INTO plans(id,request_id,config_digest,input_digest,original_request_json,resolved_plan_json)          VALUES ('portfolio-plan','portfolio-plan-request','cfg','in','{}','{}')",
        [],
    )?;
    let ledger = crate::contracts::PortfolioLedger {
        run_id: run_id.clone(),
        model_ids: Vec::new(),
        spec: request.portfolio.clone(),
        decision_interval: crate::contracts::CandleInterval::H1,
        execution_resolution: crate::contracts::CandleInterval::H1,
        status: crate::contracts::PortfolioStatus::Completed,
        status_reason: None,
        intents: Vec::new(),
        fills: vec![PortfolioFillRecord {
            event_seq: 1,
            market: market.clone(),
            side: Side::Buy,
            price: PriceKrw::new(Decimal::from(100))?,
            qty: AssetQuantity::new(Decimal::ONE)?,
            notional: QuoteAmount::new(Decimal::from(100))?,
            fee: QuoteAmount::new(Decimal::ZERO)?,
            fee_bps: BasisPoints::new(Decimal::ZERO)?,
            price_cost: crate::contracts::SignedAmount::new(Decimal::ZERO)?,
            reserved_cash: QuoteAmount::new(Decimal::ZERO)?,
            decision_time: time("2025-01-01T01:00:00Z"),
            execution_time: time("2025-01-01T01:00:00Z"),
            source_bar_id: "obs-1".into(),
            liquidity_source_bar_id: "obs-0".into(),
            cash_after: QuoteAmount::new(Decimal::from(299_900))?,
        }],
        rejections: vec![PortfolioRejectionRecord {
            event_seq: 2,
            decision_time: time("2025-01-01T02:00:00Z"),
            market: market.clone(),
            side: Side::Buy,
            target_weight: Weight::new(Decimal::new(30, 2))?,
            requested_notional: QuoteAmount::new(Decimal::from(1_000))?,
            reason: PortfolioRejectionReason::GrossExposureCap,
        }],
        marks: vec![PortfolioMarkRecord {
            event_seq: 3,
            time: time("2025-01-01T02:00:00Z"),
            cash: QuoteAmount::new(Decimal::from(299_900))?,
            position_value: QuoteAmount::new(Decimal::from(100))?,
            gross_exposure_weight: Weight::new(Decimal::ZERO)?,
            equity: QuoteAmount::new(Decimal::from(300_000))?,
            peak_equity: QuoteAmount::new(Decimal::from(300_000))?,
            drawdown: Weight::new(Decimal::ZERO)?,
            stopped: false,
            weights: std::collections::BTreeMap::from([("KRW-BTC".into(), Decimal::ZERO)]),
        }],
        attribution: Vec::new(),
        totals: PortfolioTotals {
            terminal_equity: QuoteAmount::new(Decimal::from(300_000))?,
            total_return: Decimal::ZERO,
            max_drawdown: Weight::new(Decimal::ZERO)?,
            turnover: Decimal::ZERO,
            total_fees: QuoteAmount::new(Decimal::ZERO)?,
            price_cost_drag: crate::contracts::SignedAmount::new(Decimal::ZERO)?,
            exposure_seconds: 0,
            rejected_signals: 1,
            rejection_reasons: std::collections::BTreeMap::from([("GROSS_EXPOSURE_CAP".into(), 1)]),
        },
        regime_observations: Vec::new(),
        last_event_seq: 3,
    };
    let benchmarks = serde_json::json!([{"kind": "CASH"}]);
    store.publish_portfolio_run(&request, &ledger, &benchmarks)?;
    // Idempotent duplicate run id is rejected (primary key).
    assert!(
        store
            .publish_portfolio_run(&request, &ledger, &benchmarks)
            .is_err()
    );
    let summary = store
        .portfolio_run_summary(&run_id)?
        .expect("published summary");
    assert_eq!(summary.status, "completed");
    assert_eq!(summary.fact_count, 3);
    assert_eq!(summary.totals["rejected_signals"], 1);
    assert_eq!(summary.benchmarks[0]["kind"], "CASH");
    let marks = store.portfolio_facts(&run_id, "mark", 0, 10)?;
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0]["event_seq"], 3);
    let rejections = store.portfolio_facts(&run_id, "rejection", 0, 10)?;
    assert_eq!(
        rejections[0]["reason"],
        serde_json::json!("GROSS_EXPOSURE_CAP")
    );
    assert!(store.portfolio_facts(&run_id, "bogus", 0, 10).is_err());
    assert!(store.portfolio_facts(&run_id, "mark", 0, 0).is_err());
    let runs = store.list_portfolio_runs(0, 10)?;
    assert_eq!(runs, vec![run_id.as_str().to_string()]);
    // Failure rows are recorded for observability.
    let failed_id = RunId::new("run-portfolio-failed")?;
    let failed_request = PortfolioRunRequest {
        request_id: RequestId::new("portfolio-store-failed")?,
        ..request
    };
    store.record_portfolio_failure(
        &failed_request,
        &failed_id,
        &LabError::InvalidConfig("fixture failure".into()),
        time("2025-01-02T00:00:00Z"),
    )?;
    let failed = store
        .portfolio_run_summary(&failed_id)?
        .expect("failed summary");
    assert_eq!(failed.status, "failed");
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|text| text.contains("fixture"))
    );
    let _ = Decimal::from_str("0");
    Ok(())
}
