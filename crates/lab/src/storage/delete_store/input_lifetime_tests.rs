use super::*;
use crate::contracts::{
    AdmissionStatus, ArbitrationPolicy, ArtifactId, ArtifactRef, AssetQuantity, AttemptState,
    BasisPoints, CandleInterval, CandleObservation, CandleRecord, CapitalMode, CollectRequest,
    ContentHash, CostPolicy, DatasetId, DatasetManifest, DatasetSnapshot, DatasetStatus,
    DeleteBlockerClass, EvidenceUnavailablePolicy, ExecutionPolicy, ExperimentSpec,
    HardDeleteRequest, JobPayload, JobStatus, JobSubmission, MarketDataOrigin, MarketId,
    MarketRuleSnapshot, ModelAdmission, ModelId, ObservationId, PitPolicy, PlanId, PlanRequest,
    PortfolioAssetSpec, PortfolioRiskPolicy, PortfolioRunRequest, PortfolioSpec, PriceKrw,
    QualityIssue, QualityKind, QualitySeverity, QuoteAmount, ReportClock, RequestId, ResolvedPlan,
    RuleProvenance, RuleSnapshotId, RunId, RunManifest, RunRequest, StrategyKind, StrategySpec,
    TerminalPolicy, UtcRange, UtcTimestamp, Weight, dataset_digests, dataset_id,
    experiment_config_digest, observation_digest,
};
use crate::storage::RawObjectInput;
use rusqlite::params;
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static INPUT_LIFETIME_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct InputLifetimeRoot(PathBuf);

impl InputLifetimeRoot {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let sequence = INPUT_LIFETIME_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-input-lifetime-{}-{nonce}-{sequence}",
            std::process::id(),
        ));
        std::fs::create_dir(&root).expect("create isolated input-lifetime root");
        Self(root)
    }
}

impl Drop for InputLifetimeRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct InputPlan {
    dataset_id: DatasetId,
    plan_id: PlanId,
    input_digest: crate::contracts::ContentHash,
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

fn frozen_plan(store: &mut Store, suffix: &str) -> InputPlan {
    frozen_plan_with_mode(store, suffix, false, DatasetStatus::Ready)
}

fn frozen_portfolio_plan(store: &mut Store, suffix: &str) -> InputPlan {
    frozen_plan_with_mode(store, suffix, true, DatasetStatus::Ready)
}

fn frozen_blocked_plan(store: &mut Store, suffix: &str) -> InputPlan {
    frozen_plan_with_mode(store, suffix, false, DatasetStatus::BlockedData)
}

#[expect(
    clippy::too_many_lines,
    reason = "one isolated dataset and frozen plan fixture keeps input identities explicit"
)]
fn frozen_plan_with_mode(
    store: &mut Store,
    suffix: &str,
    portfolio: bool,
    dataset_status: DatasetStatus,
) -> InputPlan {
    #[derive(serde::Serialize)]
    struct Input<'a> {
        config_digest: &'a crate::contracts::ContentHash,
        dataset_digests: &'a [(DatasetId, crate::contracts::ContentHash)],
        evidence_digest: &'a Option<crate::contracts::ContentHash>,
    }

    let start = time("2024-01-01T00:00:00Z");
    let end = time("2024-01-01T01:00:00Z");
    let range = UtcRange::new(start, end).expect("dataset range");
    let request_id =
        RequestId::new(format!("input-lifetime-collect-{suffix}")).expect("collect request ID");
    let market = MarketId::parse_upbit("KRW-BTC").expect("market");
    let request = CollectRequest {
        request_id: request_id.clone(),
        markets: vec![market.clone()],
        range,
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    };
    let published = store
        .publish_raw(RawObjectInput {
            source_url: "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC".into(),
            fetched_at: time("2024-01-01T02:00:00Z"),
            persisted_at: time("2024-01-01T02:00:00Z"),
            http_status: 200,
            remaining_req: Some("group=candle; sec=9".into()),
            origin: MarketDataOrigin::ExchangeObserved,
            body: format!("SYNTHETIC_TEST_ONLY input-lifetime fixture {suffix}").into_bytes(),
        })
        .expect("publish isolated raw evidence");
    let raw = published.object;
    let candle = CandleRecord {
        market: market.code(),
        interval: CandleInterval::H1,
        open_time_utc: start,
        close_time_utc: end,
        open: PriceKrw::new(Decimal::from(100)).expect("open"),
        high: PriceKrw::new(Decimal::from(110)).expect("high"),
        low: PriceKrw::new(Decimal::from(90)).expect("low"),
        close: PriceKrw::new(Decimal::from(105)).expect("close"),
        volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
        quote_turnover: QuoteAmount::new(Decimal::from(250)).expect("turnover"),
        completed: true,
    };
    let mut observation = CandleObservation {
        id: ObservationId::new("placeholder").expect("observation ID"),
        content_digest: crate::contracts::ContentHash::of_bytes(b"placeholder"),
        candle,
        raw_object_ids: vec![raw.id.clone()],
        constituent_ids: Vec::new(),
    };
    let digest = observation_digest(&observation.candle).expect("observation digest");
    observation.id = ObservationId::from_seed(digest.as_str());
    observation.content_digest = digest;
    let placeholder = crate::contracts::ContentHash::of_bytes(b"placeholder");
    let mut snapshot = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: "1".into(),
            id: DatasetId::new("placeholder").expect("dataset ID"),
            request: request.clone(),
            coverage: range,
            status: dataset_status,
            row_count: 1,
            normalizer_version: crate::contracts::NORMALIZER_VERSION.into(),
            gap_policy: "reject".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: MarketDataOrigin::ExchangeObserved,
            raw_objects: vec![raw],
            quality_issues: if dataset_status == DatasetStatus::BlockedData {
                vec![QualityIssue {
                    kind: QualityKind::DataUnavailable,
                    severity: QualitySeverity::Error,
                    market: market.clone(),
                    start,
                    end,
                    count: 1,
                    raw_object_ids: Vec::new(),
                    detail: "SYNTHETIC_TEST_ONLY blocked-data fixture".into(),
                }]
            } else {
                Vec::new()
            },
            reuse: None,
        },
        observations: vec![observation],
    };
    let digests = dataset_digests(&snapshot).expect("dataset digest projection");
    snapshot.manifest.id = dataset_id(&digests);
    snapshot.manifest.semantic_digest = digests.semantic;
    snapshot.manifest.provenance_digest = digests.provenance;
    let dataset_id = store
        .finish_dataset(&snapshot)
        .expect("register fixture dataset");

    let zero = BasisPoints::new(Decimal::ZERO).expect("zero basis points");
    let spec = ExperimentSpec {
        schema_version: "1.0".into(),
        causal_execution: None,
        capital_mode: portfolio.then_some(CapitalMode::SharedPortfolio),
        dataset_ids: vec![dataset_id.clone()],
        markets: vec![market.clone()],
        range,
        strategies: vec![StrategySpec::BuyAndHold],
        policy_selections: Vec::new(),
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: QuoteAmount::new(Decimal::from(1_000)).expect("initial cash"),
        costs: CostPolicy {
            buy_fee_bps: zero,
            sell_fee_bps: zero,
            maker_fee_bps: zero,
            half_spread_bps: zero,
            slippage_bps: zero,
            impact_bps: zero,
            assumption_label: "SYNTHETIC_TEST_ONLY".into(),
            dynamic: None,
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE).expect("participation cap"),
        },
        market_rules: MarketRuleSnapshot {
            id: RuleSnapshotId::new(format!("input-lifetime-rules-{suffix}")).expect("rule ID"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: range,
            observed_at: start,
            source_refs: vec!["SYNTHETIC_TEST_ONLY".into()],
            assumption_label: "SYNTHETIC_TEST_ONLY".into(),
            min_notional: QuoteAmount::new(Decimal::ONE).expect("minimum notional"),
            quantity_step: AssetQuantity::new(Decimal::new(1, 4)).expect("quantity step"),
            ticks: vec![crate::contracts::TickBand {
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
    let config_digest = experiment_config_digest(&spec, &[]).expect("config digest");
    let dataset_digests = vec![(
        dataset_id.clone(),
        snapshot.manifest.semantic_digest.clone(),
    )];
    let evidence_digest = None;
    let input_digest = crate::contracts::ContentHash::of_value(&Input {
        config_digest: &config_digest,
        dataset_digests: &dataset_digests,
        evidence_digest: &evidence_digest,
    })
    .expect("plan input digest");
    let plan_id = PlanId::from_seed(input_digest.as_str());
    let plan = ResolvedPlan {
        id: plan_id.clone(),
        spec: spec.clone(),
        config_digest,
        input_digest: input_digest.clone(),
        dataset_digests,
        evidence_digest,
        admissions: vec![ModelAdmission {
            model_id: ModelId::from_seed(&format!("input-lifetime-model-{suffix}")),
            market,
            strategy: StrategyKind::BuyAndHold,
            policy_ref: None,
            status: if dataset_status == DatasetStatus::BlockedData {
                AdmissionStatus::BlockedData
            } else {
                AdmissionStatus::Eligible
            },
            reasons: if dataset_status == DatasetStatus::BlockedData {
                vec!["DATA_GAP: fixture has an error-severity quality issue".into()]
            } else {
                Vec::new()
            },
        }],
        policy_revisions: Vec::new(),
        warnings: Vec::new(),
        estimated_events: 8,
    };
    store
        .save_plan(
            &PlanRequest {
                request_id: RequestId::new(format!("input-lifetime-plan-{suffix}"))
                    .expect("plan request ID"),
                spec,
            },
            &plan,
        )
        .expect("save frozen plan");
    InputPlan {
        dataset_id,
        plan_id,
        input_digest,
    }
}

fn submit_backtest(
    store: &mut Store,
    input: &InputPlan,
    suffix: &str,
) -> crate::contracts::JobRecord {
    submit_backtest_at(store, input, suffix, time("2024-01-02T00:00:00Z"))
}

fn submit_backtest_at(
    store: &mut Store,
    input: &InputPlan,
    suffix: &str,
    now: UtcTimestamp,
) -> crate::contracts::JobRecord {
    let request_id =
        RequestId::new(format!("input-lifetime-job-{suffix}")).expect("job request ID");
    store
        .submit_job(
            &JobSubmission {
                request_id: request_id.clone(),
                payload: JobPayload::Backtest {
                    request: RunRequest {
                        request_id,
                        plan_id: input.plan_id.clone(),
                        input_digest: input.input_digest.clone(),
                    },
                },
            },
            now,
        )
        .expect("submit valid backtest job")
}

fn submit_portfolio_backtest(
    store: &mut Store,
    input: &InputPlan,
    suffix: &str,
) -> crate::contracts::JobRecord {
    let request_id =
        RequestId::new(format!("input-lifetime-portfolio-job-{suffix}")).expect("job request ID");
    let market = MarketId::parse_upbit("KRW-BTC").expect("market");
    store
        .submit_job(
            &JobSubmission {
                request_id: request_id.clone(),
                payload: JobPayload::PortfolioBacktest {
                    request: Box::new(PortfolioRunRequest {
                        request_id,
                        plan_id: input.plan_id.clone(),
                        input_digest: input.input_digest.clone(),
                        portfolio: PortfolioSpec {
                            initial_cash: QuoteAmount::new(Decimal::from(1_000))
                                .expect("initial cash"),
                            assets: vec![PortfolioAssetSpec {
                                market,
                                max_weight: Weight::new(Decimal::new(8, 1)).expect("asset weight"),
                            }],
                            risk: PortfolioRiskPolicy {
                                max_gross_exposure: Weight::new(Decimal::new(8, 1))
                                    .expect("gross exposure"),
                                min_cash_weight: Weight::new(Decimal::new(2, 1))
                                    .expect("cash reserve"),
                                drawdown_stop: Weight::new(Decimal::ZERO).expect("drawdown stop"),
                            },
                            arbitration: ArbitrationPolicy::Priority,
                        },
                        regime: None,
                    }),
                },
            },
            time("2024-01-02T00:00:00Z"),
        )
        .expect("submit valid portfolio job")
}

fn dataset_resource(input: &InputPlan) -> DeleteResource {
    DeleteResource::Dataset {
        dataset_id: input.dataset_id.clone(),
    }
}

fn insert_verification_package(store: &mut Store, suffix: &str) -> Vec<ArtifactId> {
    store
        .connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("disable foreign keys for orphan package fixture");
    let run_id = RunId::from_seed(&format!("verification-run-{suffix}"));
    let package_dir = format!("exports/verification-package-{suffix}");
    let mut ids = Vec::new();
    for (index, name) in ["manifest.json", "review.json", "ledger.json.gz"]
        .into_iter()
        .enumerate()
    {
        let relative_path = format!("{package_dir}/{name}");
        let path = store.raw.root.join(&relative_path);
        std::fs::create_dir_all(path.parent().expect("artifact parent"))
            .expect("create export package");
        let bytes = format!("SYNTHETIC_TEST_ONLY {name}").into_bytes();
        std::fs::write(&path, &bytes).expect("write package member");
        let artifact_id = ArtifactId::from_seed(&format!("verification-{suffix}-{index}"));
        let artifact = ArtifactRef {
            id: artifact_id.clone(),
            run_id: run_id.clone(),
            relative_path,
            media_type: "application/octet-stream".into(),
            bytes: u64::try_from(bytes.len()).expect("artifact byte count"),
            sha256: ContentHash::of_bytes(&bytes),
            uncompressed_sha256: None,
            uncompressed_bytes: None,
            complete: true,
        };
        store
            .connection
            .execute(
                "INSERT INTO run_artifacts(id,run_id,relative_path,media_type,bytes,sha256,\
                 uncompressed_sha256,uncompressed_bytes,complete,artifact_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    artifact.id.as_str(),
                    artifact.run_id.as_str(),
                    artifact.relative_path,
                    artifact.media_type,
                    i64::try_from(artifact.bytes).expect("sqlite byte count"),
                    artifact.sha256.as_str(),
                    artifact
                        .uncompressed_sha256
                        .as_ref()
                        .map(ContentHash::as_str),
                    artifact.uncompressed_bytes.map(|bytes| {
                        i64::try_from(bytes).expect("sqlite uncompressed byte count")
                    }),
                    artifact.complete,
                    serde_json::to_string(&artifact).expect("serialize artifact")
                ],
            )
            .expect("register package member");
        ids.push(artifact_id);
    }
    store
        .connection
        .execute_batch("PRAGMA foreign_keys=ON;")
        .expect("restore foreign key enforcement");
    ids
}

fn insert_exportable_run(
    store: &Store,
    input: &InputPlan,
    job: &crate::contracts::JobRecord,
    attempt: &crate::contracts::JobAttempt,
    suffix: &str,
) -> RunId {
    let run_id = RunId::new(format!("input-lifetime-run-{suffix}")).expect("run ID");
    let manifest = RunManifest {
        schema_version: "1.0".into(),
        run_id: run_id.clone(),
        job_id: job.id.clone(),
        attempt_id: attempt.id.clone(),
        created_at: time("2024-01-02T00:03:00Z"),
        code_revision: None,
        source_digest: ContentHash::of_bytes(b"SYNTHETIC_TEST_ONLY source"),
        lockfile_digest: ContentHash::of_bytes(b"SYNTHETIC_TEST_ONLY lockfile"),
        toolchain: "SYNTHETIC_TEST_ONLY".into(),
        engine_version: "SYNTHETIC_TEST_ONLY".into(),
        indicator_version: "SYNTHETIC_TEST_ONLY".into(),
        rounding_version: "SYNTHETIC_TEST_ONLY".into(),
        mode: "HISTORICAL_REPLAY".into(),
        historical_availability_model: "BAR_CLOSE_ASSUMED".into(),
        execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false,
        origin: MarketDataOrigin::ExchangeObserved,
        numeric_tolerance: "0.00000001".into(),
        metric_tolerance: 1e-10,
        start_policy: "START_FROM_CASH".into(),
        account_mode: "INDEPENDENT_ASSET_STRATEGY".into(),
        assumptions: vec!["SYNTHETIC_TEST_ONLY".into()],
    };
    store
        .connection
        .execute(
            "INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,\
             last_committed_event_seq,manifest_json) VALUES (?1,?2,?3,?4,'COMPLETED',?5,0,?6)",
            params![
                run_id.as_str(),
                job.id.as_str(),
                attempt.id.as_str(),
                input.plan_id.as_str(),
                manifest.created_at.0.timestamp_millis(),
                serde_json::to_string(&manifest).expect("serialize run manifest")
            ],
        )
        .expect("insert isolated exportable run");
    run_id
}

fn active_job_blocker(preview: &DeletePreview) -> bool {
    preview
        .blockers
        .iter()
        .any(|blocker| blocker.class == DeleteBlockerClass::ActiveJob)
}

#[test]
fn queued_backtest_input_blocks_dataset_cascade() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "queued");
    let job = submit_backtest(&mut store, &input, "queued");
    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:01:00Z"))
        .expect("preview dataset cascade");

    assert!(preview.cascade_required, "the frozen plan is disclosed");
    assert!(
        active_job_blocker(&preview),
        "queued consumer must block input delete"
    );
    assert!(matches!(
        store.execute_hard_delete(
            &HardDeleteRequest {
                preview: preview.clone(),
                cascade: true,
            },
            time("2024-01-02T00:02:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    assert!(matches!(
        store.execute_hard_delete_batch(
            &[HardDeleteRequest {
                preview,
                cascade: true,
            }],
            time("2024-01-02T00:02:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    let dataset = store
        .load_dataset(&input.dataset_id)
        .expect("dataset readback")
        .expect("dataset retained");
    let raw_path = store
        .raw
        .root
        .join(&dataset.manifest.raw_objects[0].relative_path);
    assert!(raw_path.exists(), "active consumer keeps raw input files");
    assert_eq!(
        store
            .get_job(&job.id)
            .expect("job readback")
            .expect("job exists")
            .attempts[0]
            .state
            .status(),
        JobStatus::Queued
    );
}

#[test]
fn missing_raw_queued_attempt_fails_and_later_valid_job_is_claimed() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let unavailable_input = frozen_plan(&mut store, "unavailable");
    let unavailable_job = submit_backtest_at(
        &mut store,
        &unavailable_input,
        "unavailable",
        time("2024-01-02T00:00:00Z"),
    );
    let available_input = frozen_plan(&mut store, "available");
    let available_job = submit_backtest_at(
        &mut store,
        &available_input,
        "available",
        time("2024-01-02T00:00:01Z"),
    );

    let unavailable_dataset = store
        .load_dataset(&unavailable_input.dataset_id)
        .expect("dataset readback")
        .expect("unavailable input dataset remains stored");
    let body_path = store
        .raw
        .root
        .join(&unavailable_dataset.manifest.raw_objects[0].relative_path)
        .join("body.json.gz");
    std::fs::remove_file(body_path).expect("remove only the isolated fixture raw body");

    let claimed = store
        .claim_next(time("2024-01-02T00:00:02Z"))
        .expect("invalid queued input is terminalized without stopping the queue")
        .expect("later valid job is claimable");
    assert_eq!(claimed.job_id, available_job.id);
    assert_eq!(claimed.id, available_job.attempts[0].id);

    let failed = store
        .get_job(&unavailable_job.id)
        .expect("failed job readback")
        .expect("failed job remains readable");
    let AttemptState::Failed { error, .. } = &failed.attempts[0].state else {
        panic!("missing raw input must fail the queued attempt");
    };
    assert_eq!(error.code, "INPUT_UNAVAILABLE");
    assert!(error.message.chars().count() <= 160);
    assert!(
        !store
            .active_job_input_holds
            .contains_key(&failed.attempts[0].id)
    );
    assert!(store.active_job_input_holds.contains_key(&claimed.id));
}

#[test]
fn blocked_data_backtest_input_remains_pinnable_and_protected() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_blocked_plan(&mut store, "blocked-data");
    let dataset = store
        .load_dataset(&input.dataset_id)
        .expect("dataset readback")
        .expect("blocked dataset remains stored");
    assert_eq!(dataset.manifest.status, DatasetStatus::BlockedData);
    let plan = store
        .load_plan(&input.plan_id)
        .expect("plan readback")
        .expect("blocked plan remains stored");
    assert_eq!(
        plan.resolved.admissions[0].status,
        AdmissionStatus::BlockedData
    );

    let job = submit_backtest(&mut store, &input, "blocked-data");
    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:01:00Z"))
        .expect("preview blocked dataset cascade");
    assert!(active_job_blocker(&preview));
    assert!(matches!(
        store.execute_hard_delete(
            &HardDeleteRequest {
                preview,
                cascade: true,
            },
            time("2024-01-02T00:02:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    assert_eq!(
        store
            .get_job(&job.id)
            .expect("job readback")
            .expect("job remains stored")
            .attempts[0]
            .state
            .status(),
        JobStatus::Queued
    );
}

#[test]
fn queued_portfolio_backtest_input_blocks_dataset_cascade() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_portfolio_plan(&mut store, "portfolio");
    let _job = submit_portfolio_backtest(&mut store, &input, "portfolio");
    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:01:00Z"))
        .expect("preview portfolio dataset cascade");

    assert!(
        active_job_blocker(&preview),
        "portfolio consumer must protect its plan inputs"
    );
    assert!(
        store
            .load_dataset(&input.dataset_id)
            .expect("dataset readback")
            .is_some()
    );
}

#[test]
fn verify_job_holds_every_artifact_in_its_package() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let artifacts = insert_verification_package(&mut store, "siblings-é");
    let request_id = RequestId::new("input-lifetime-verify-siblings").expect("request ID");
    store
        .submit_job(
            &JobSubmission {
                request_id,
                payload: JobPayload::Verify {
                    artifact_id: artifacts[0].clone(),
                    replay: true,
                },
            },
            time("2024-01-02T00:00:00Z"),
        )
        .expect("submit valid package verification");

    for artifact_id in artifacts {
        let preview = store
            .delete_preview(
                &DeleteResource::Artifact { artifact_id },
                time("2024-01-02T00:01:00Z"),
            )
            .expect("preview package member");
        assert!(
            active_job_blocker(&preview),
            "verification must hold every sibling file, including its manifest and ledger"
        );
    }
}

#[test]
fn export_job_holds_run_bundle_inputs_for_direct_and_dataset_cascade_deletes() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "export");
    let producer = submit_backtest(&mut store, &input, "export-producer");
    let attempt = store
        .claim_next(time("2024-01-02T00:01:00Z"))
        .expect("claim producer")
        .expect("producer attempt");
    store
        .finish_attempt(
            &attempt.id,
            AttemptState::Failed {
                ended_at: time("2024-01-02T00:02:00Z"),
                error: crate::contracts::FailureRecord {
                    code: "SYNTHETIC_TEST_ONLY".into(),
                    message: "fixture producer is terminal".into(),
                },
            },
        )
        .expect("finish producer");
    store
        .release_terminal_job_input_hold(&attempt.id)
        .expect("release producer inputs");
    let run_id = insert_exportable_run(&store, &input, &producer, &attempt, "export");
    let request_id = RequestId::new("input-lifetime-export-consumer").expect("request ID");
    store
        .submit_job(
            &JobSubmission {
                request_id,
                payload: JobPayload::Export {
                    run_id: run_id.clone(),
                    market: None,
                },
            },
            time("2024-01-02T00:04:00Z"),
        )
        .expect("submit export job");

    for resource in [
        DeleteResource::Run {
            run_id: run_id.clone(),
        },
        dataset_resource(&input),
    ] {
        let preview = store
            .delete_preview(&resource, time("2024-01-02T00:05:00Z"))
            .expect("preview export input deletion");
        assert!(
            active_job_blocker(&preview),
            "export owns the run and its frozen input bundle until terminal readback"
        );
    }
}

#[test]
fn running_cancel_request_keeps_backtest_input_blocked() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "cancel");
    let job = submit_backtest(&mut store, &input, "cancel");
    store
        .claim_next(time("2024-01-02T00:01:00Z"))
        .expect("claim job")
        .expect("claimed attempt");
    store
        .request_cancel(&job.id, time("2024-01-02T00:02:00Z"))
        .expect("persist running cancellation request");

    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:03:00Z"))
        .expect("preview dataset cascade after cancel request");
    assert!(
        active_job_blocker(&preview),
        "cancel request is not job termination"
    );
    let cancelled = store
        .get_job(&job.id)
        .expect("job readback")
        .expect("job exists");
    assert!(matches!(
        cancelled.attempts.last().expect("current attempt").state,
        AttemptState::Running {
            cancel_requested_at: Some(_),
            ..
        }
    ));
    assert!(
        store
            .load_dataset(&input.dataset_id)
            .expect("dataset readback")
            .is_some()
    );
}

#[test]
fn input_hold_survives_terminal_publication_until_runner_release() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "post-terminal");
    let job = submit_backtest(&mut store, &input, "post-terminal");
    let attempt = store
        .claim_next(time("2024-01-02T00:01:00Z"))
        .expect("claim job")
        .expect("claimed attempt");
    store
        .finish_attempt(
            &attempt.id,
            AttemptState::Failed {
                ended_at: time("2024-01-02T00:02:00Z"),
                error: crate::contracts::FailureRecord {
                    code: "SYNTHETIC_TEST_ONLY".into(),
                    message: "finish before runner readback release".into(),
                },
            },
        )
        .expect("persist terminal attempt");

    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:03:00Z"))
        .expect("preview while terminal readback hold remains");
    assert!(
        active_job_blocker(&preview),
        "terminal status alone must not release hold"
    );
    assert!(
        store
            .load_dataset(&input.dataset_id)
            .expect("dataset readback")
            .is_some()
    );
    assert!(store.get_job(&job.id).expect("job readback").is_some());
    let job_preview = store
        .delete_preview(
            &DeleteResource::Job {
                job_id: job.id.clone(),
            },
            time("2024-01-02T00:03:30Z"),
        )
        .expect("preview terminal job during runner readback");
    assert!(active_job_blocker(&job_preview));
    assert!(matches!(
        store.execute_hard_delete(
            &HardDeleteRequest {
                preview: job_preview,
                cascade: true,
            },
            time("2024-01-02T00:03:45Z")
        ),
        Err(LabError::Conflict(_))
    ));

    store
        .release_terminal_job_input_hold(&attempt.id)
        .expect("release only after terminal readback");
    let released = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:04:00Z"))
        .expect("preview after worker release");
    assert!(!active_job_blocker(&released));
    store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview: released,
                cascade: true,
            },
            time("2024-01-02T00:05:00Z"),
        )
        .expect("terminal input may be deliberately deleted");
    assert!(
        store
            .load_dataset(&input.dataset_id)
            .expect("dataset readback after deletion")
            .is_none()
    );
}

#[test]
fn submit_rejects_missing_plan_without_queue_mutation() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let request_id = RequestId::new("missing-plan-submit").expect("job request ID");
    let submission = JobSubmission {
        request_id: request_id.clone(),
        payload: JobPayload::Backtest {
            request: RunRequest {
                request_id,
                plan_id: PlanId::new("missing-plan").expect("plan ID"),
                input_digest: crate::contracts::ContentHash::of_bytes(b"missing plan"),
            },
        },
    };

    assert!(matches!(
        store.submit_job(&submission, time("2024-01-02T00:00:00Z")),
        Err(LabError::InvalidConfig(_))
    ));
    let queue = store.job_queue_status().expect("queue status");
    assert_eq!(queue.queued_attempts, 0);
    assert_eq!(queue.running_attempts, 0);
    assert!(
        store
            .get_job(&crate::contracts::JobId::from_seed("missing-plan-submit"))
            .expect("job readback")
            .is_none()
    );
}

#[test]
fn retry_rejects_deleted_inputs_but_idempotent_submit_reads_back_terminal_job() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "retry");
    let job = submit_backtest(&mut store, &input, "retry");
    let attempt = store
        .claim_next(time("2024-01-02T00:01:00Z"))
        .expect("claim job")
        .expect("claimed attempt");
    store
        .finish_attempt(
            &attempt.id,
            AttemptState::Failed {
                ended_at: time("2024-01-02T00:02:00Z"),
                error: crate::contracts::FailureRecord {
                    code: "SYNTHETIC_TEST_ONLY".into(),
                    message: "terminal fixture".into(),
                },
            },
        )
        .expect("finish attempt");
    store
        .release_terminal_job_input_hold(&attempt.id)
        .expect("release finished attempt inputs");

    store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview: store
                    .delete_preview(&dataset_resource(&input), time("2024-01-02T00:03:00Z"))
                    .expect("preview terminal input deletion"),
                cascade: true,
            },
            time("2024-01-02T00:04:00Z"),
        )
        .expect("finished inputs remain deliberately deletable");

    let repeated = submit_backtest(&mut store, &input, "retry");
    assert_eq!(repeated.id, job.id);
    assert_eq!(repeated.attempts.len(), 1);
    assert!(matches!(
        store.retry_job(&job.id, time("2024-01-02T00:05:00Z")),
        Err(LabError::InvalidConfig(_))
    ));
    assert_eq!(
        store
            .get_job(&job.id)
            .expect("job readback")
            .expect("job exists")
            .attempts
            .len(),
        1,
        "rejected retry must not create an attempt"
    );
}

#[test]
fn dataset_preview_is_rechecked_after_a_consumer_is_submitted() {
    let root = InputLifetimeRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated Store");
    let input = frozen_plan(&mut store, "preview-race");
    let preview = store
        .delete_preview(&dataset_resource(&input), time("2024-01-02T00:01:00Z"))
        .expect("preview before submit");
    let _job = submit_backtest(&mut store, &input, "preview-race");

    assert!(matches!(
        store.execute_hard_delete(
            &HardDeleteRequest {
                preview,
                cascade: true,
            },
            time("2024-01-02T00:02:00Z")
        ),
        Err(LabError::Conflict(_))
    ));
    assert!(
        store
            .load_dataset(&input.dataset_id)
            .expect("dataset readback")
            .is_some()
    );
}
