use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, CandleInterval, CausalExecutionPolicy, CollectRequest, CostPolicy,
    CostSweep, DatasetId, EvidenceUnavailablePolicy, ExecutionPolicy, MarketId, MarketRuleSnapshot,
    PitPolicy, PriceKrw, QuoteAmount, ReportClock, ResearchDesign, ResearchSuiteAction,
    ResearchSuiteRequest, RuleProvenance, RuleSnapshotId, SuiteSummary, TerminalPolicy, TickBand,
    UtcRange, Weight,
};
use crate::database::DatabaseOwner;
use crate::jobs::JobRuntime;
use crate::market_data::UpbitClient;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use rust_decimal::Decimal;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempRoot(std::path::PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-coordinator-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        fs::create_dir(&path).expect("create coordinator test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct RequestGate {
    entered: Notify,
    release: Notify,
    held: AtomicBool,
}

#[derive(Clone)]
struct FixtureState {
    candles: Arc<Vec<u8>>,
    gate: Option<Arc<RequestGate>>,
}

struct FixtureServer {
    base_url: String,
    gate: Option<Arc<RequestGate>>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn fixture_response(
    State(state): State<FixtureState>,
    uri: Uri,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    if uri.path().ends_with("/v1/market/all") {
        return (
            StatusCode::OK,
            [("Remaining-Req", "group=market; min=1800; sec=9")],
            br#"[{"market":"KRW-BTC"}]"#.to_vec(),
        );
    }
    if let Some(gate) = &state.gate
        && !gate.held.swap(true, AtomicOrdering::AcqRel)
    {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    (
        StatusCode::OK,
        [("Remaining-Req", "group=candle; min=1800; sec=9")],
        state.candles.as_ref().clone(),
    )
}

async fn start_fixture(block_candles: bool) -> Result<FixtureServer, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gate = block_candles.then(|| Arc::new(RequestGate::default()));
    let app = axum::Router::new()
        .fallback(fixture_response)
        .with_state(FixtureState {
            candles: Arc::new(wire_candles()),
            gate: gate.clone(),
        });
    let (stop_tx, stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ignored = stop_rx.await;
            })
            .await
    });
    Ok(FixtureServer {
        base_url: format!("http://{address}"),
        gate,
        stop: stop_tx,
        task,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_runtime_completes_walk_forward_and_reuses_content_stable_plans()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("walk-forward");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let server = start_fixture(false).await?;
    let client = UpbitClient::synthetic_local(&server.base_url)?;
    let dataset = crate::collection::collect(
        &client,
        &database,
        collection_request("runtime-dataset"),
        &CancellationToken::new(),
    )
    .await?;
    let policy = builtin_policy(&database, "builtin-buy-and-hold").await?;
    let runtime = JobRuntime::start(
        database.clone(),
        client,
        root.0.clone(),
        Some("test".into()),
    )
    .await?;
    let service = runtime.service();

    let first = create_suite(
        &service,
        suite_request(
            "runtime-suite-a",
            dataset.manifest.id.clone(),
            policy.clone(),
        ),
    )
    .await?;
    let first_done = wait_suite(&service, first.id.clone()).await?;
    assert_eq!(first_done.status, SuiteStatus::Completed);
    let second = create_suite(
        &service,
        suite_request("runtime-suite-b", dataset.manifest.id, policy),
    )
    .await?;
    let second_done = wait_suite(&service, second.id.clone()).await?;
    assert_eq!(second_done.status, SuiteStatus::Completed);

    let first_id = first.id.clone();
    let second_id = second.id.clone();
    let (first_record, second_record) = database
        .call("inspect_completed_suites", move |store| {
            Ok((
                store
                    .get_research_suite(&first_id)?
                    .ok_or_else(|| LabError::DataCorrupt("first suite disappeared".into()))?,
                store
                    .get_research_suite(&second_id)?
                    .ok_or_else(|| LabError::DataCorrupt("second suite disappeared".into()))?,
            ))
        })
        .await?;
    assert!(first_record.cases.iter().all(|case| {
        case.status == SuiteCaseStatus::Completed && case.plan_id.is_some() && case.run_id.is_some()
    }));
    assert_eq!(first_record.cases.len(), 4);
    assert_eq!(first_record.folds.len(), 2);
    assert!(first_record.folds.iter().all(|fold| fold.winner.is_some()));
    assert_eq!(
        first_record
            .cases
            .iter()
            .map(|case| case.plan_id.clone())
            .collect::<Vec<_>>(),
        second_record
            .cases
            .iter()
            .map(|case| case.plan_id.clone())
            .collect::<Vec<_>>()
    );

    runtime.shutdown().await?;
    let _ignored = server.stop.send(());
    server.task.await??;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_unavailable_fold_freezes_no_winner_and_runtime_keeps_processing()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("all-unavailable");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let server = start_fixture(false).await?;
    let client = UpbitClient::synthetic_local(&server.base_url)?;
    let dataset = crate::collection::collect(
        &client,
        &database,
        collection_request("blocked-dataset"),
        &CancellationToken::new(),
    )
    .await?;
    let unavailable = builtin_policy(&database, "builtin-s5").await?;
    let available = builtin_policy(&database, "builtin-buy-and-hold").await?;
    let dataset_id = dataset.manifest.id;
    let runtime = JobRuntime::start(
        database.clone(),
        client,
        root.0.clone(),
        Some("test".into()),
    )
    .await?;
    let service = runtime.service();
    let created = create_suite(
        &service,
        suite_request("blocked-suite", dataset_id.clone(), unavailable),
    )
    .await?;
    let blocked_id = created.id.clone();
    let blocked = wait_suite(&service, created.id.clone()).await?;
    assert_eq!(blocked.status, SuiteStatus::Blocked);
    assert_eq!(blocked.selected_folds.len(), 1);
    assert!(blocked.selected_folds[0].winner.is_none());
    assert!(!blocked.selected_folds[0].unavailable_candidates.is_empty());
    let blocked_record = database
        .call("inspect_blocked_suite", move |store| {
            store
                .get_research_suite(&blocked_id)?
                .ok_or_else(|| LabError::DataCorrupt("blocked suite disappeared".into()))
        })
        .await?;
    assert_eq!(
        blocked_record
            .cases
            .iter()
            .filter(|case| case.attempt_id.is_some())
            .count(),
        1
    );
    assert!(blocked_record.cases.iter().all(|case| {
        case.phase != SuitePhase::Evaluation || (case.attempt_id.is_none() && case.run_id.is_none())
    }));

    let healthy = create_suite(
        &service,
        suite_request("healthy-after-blocked-suite", dataset_id, available),
    )
    .await?;
    let healthy = wait_suite(&service, healthy.id).await?;
    assert_eq!(healthy.status, SuiteStatus::Completed);
    assert!(service.status().await?.runner_available);

    runtime.shutdown().await?;
    let _ignored = server.stop.send(());
    server.task.await??;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_queue_defers_suite_without_mutating_case_or_stopping_runtime()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("queue-saturation");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let fixture = start_fixture(false).await?;
    let fixture_client = UpbitClient::synthetic_local(&fixture.base_url)?;
    let dataset = crate::collection::collect(
        &fixture_client,
        &database,
        collection_request("queue-fixture-dataset"),
        &CancellationToken::new(),
    )
    .await?;
    let _ignored = fixture.stop.send(());
    fixture.task.await??;

    let policy = builtin_policy(&database, "builtin-buy-and-hold").await?;
    let slow_server = start_fixture(true).await?;
    let gate = slow_server.gate.clone().expect("slow gate");
    let slow_client = UpbitClient::synthetic_local(&slow_server.base_url)?;
    let runtime = JobRuntime::start(
        database.clone(),
        slow_client,
        root.0.clone(),
        Some("test".into()),
    )
    .await?;
    let service = runtime.service();
    let active_request = slow_collection_request("slow-active");
    let active = service
        .submit(crate::contracts::JobSubmission {
            request_id: active_request.request_id.clone(),
            payload: crate::contracts::JobPayload::Collect {
                request: active_request,
            },
        })
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .map_err(|_| "slow HTTP boundary was not entered")?;
    assert_eq!(service.status().await?.active_job_id, Some(active.id));

    for index in 0..8 {
        let request = slow_collection_request(&format!("queued-{index}"));
        service
            .submit(crate::contracts::JobSubmission {
                request_id: request.request_id.clone(),
                payload: crate::contracts::JobPayload::Collect { request },
            })
            .await?;
    }
    assert_eq!(service.status().await?.queued_attempts, 8);

    let created = create_suite(
        &service,
        suite_request("queue-pressure-suite", dataset.manifest.id, policy),
    )
    .await?;
    let before_id = created.id.clone();
    let before = database
        .call("queue_suite_before", move |store| {
            store
                .get_research_suite(&before_id)?
                .ok_or_else(|| LabError::DataCorrupt("queue suite disappeared".into()))
        })
        .await?;
    service.coordinate_once(UtcTimestamp::now()).await?;
    let after_id = created.id;
    let after = database
        .call("queue_suite_after", move |store| {
            store
                .get_research_suite(&after_id)?
                .ok_or_else(|| LabError::DataCorrupt("queue suite disappeared".into()))
        })
        .await?;
    assert_eq!(after.next_action_at, before.next_action_at);
    assert_eq!(after.cases[0].status, SuiteCaseStatus::Planned);
    assert!(after.cases[0].plan_id.is_none());
    assert!(after.cases[0].job_id.is_none());
    let status = service.status().await?;
    assert!(status.runner_available);
    assert!(status.active_job_id.is_some());
    assert_eq!(status.queued_attempts, 8);

    gate.release.notify_waiters();
    runtime.shutdown().await?;
    let _ignored = slow_server.stop.send(());
    slow_server.task.await??;
    owner.shutdown()?;
    Ok(())
}

async fn create_suite(
    service: &JobService,
    request: ResearchSuiteRequest,
) -> Result<SuiteSummary, LabError> {
    serde_json::from_value(
        service
            .research_suite(ResearchSuiteAction::Create {
                request: Box::new(request),
            })
            .await?,
    )
    .map_err(Into::into)
}

async fn wait_suite(service: &JobService, id: SuiteId) -> Result<SuiteSummary, LabError> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let summary: SuiteSummary = serde_json::from_value(
                service
                    .research_suite(ResearchSuiteAction::Get {
                        suite_id: id.clone(),
                    })
                    .await?,
            )?;
            if summary.status != SuiteStatus::Running {
                return Ok(summary);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| LabError::ResourceLimit("suite test deadline exceeded".into()))?
}

async fn builtin_policy(
    database: &crate::database::DatabaseHandle,
    id: &str,
) -> Result<crate::contracts::PolicyRevisionRef, LabError> {
    let id = id.to_owned();
    database
        .call("test_builtin_policy", move |store| {
            store
                .list_policies(None, 100)?
                .items
                .into_iter()
                .find(|policy| policy.policy_id.as_str() == id)
                .map(|policy| policy.head)
                .ok_or_else(|| LabError::DataCorrupt("built-in policy missing".into()))
        })
        .await
}

fn suite_request(
    id: &str,
    dataset_id: DatasetId,
    policy: crate::contracts::PolicyRevisionRef,
) -> ResearchSuiteRequest {
    let range = test_range();
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    ResearchSuiteRequest {
        request_id: RequestId::new(id).expect("suite request"),
        template: crate::contracts::ExperimentSpec {
            schema_version: "3.0".into(),
            dataset_ids: vec![dataset_id],
            markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
            range,
            strategies: Vec::new(),
            policy_selections: vec![policy],
            causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            latency_ms: 0,
            initial_cash: QuoteAmount::new(Decimal::from(1_000_000)).expect("cash"),
            costs: zero_costs(),
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: Weight::new(Decimal::ONE).expect("weight"),
            },
            market_rules: MarketRuleSnapshot {
                id: RuleSnapshotId::new("runtime-rules").expect("rule ID"),
                provenance: RuleProvenance::ExplicitScenario,
                valid_range: range,
                observed_at: range.start(),
                source_refs: vec!["synthetic-loopback".into()],
                assumption_label: "synthetic runtime fixture".into(),
                min_notional: QuoteAmount::new(Decimal::ONE).expect("notional"),
                quantity_step: AssetQuantity::new(Decimal::new(1, 4)).expect("quantity"),
                ticks: vec![TickBand {
                    lower_bound: QuoteAmount::new(Decimal::ZERO).expect("lower"),
                    tick: PriceKrw::new(Decimal::ONE).expect("tick"),
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
            seed: 11,
        },
        design: ResearchDesign::WalkForward {
            selection_bars: 2,
            evaluation_bars: 2,
            step_bars: 2,
            embargo_bars: 0,
        },
        cost_sweep: CostSweep {
            fee_bps: vec![zero],
            slippage_bps: vec![zero],
        },
    }
}

fn collection_request(id: &str) -> CollectRequest {
    CollectRequest {
        request_id: RequestId::new(id).expect("collection request"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: test_range(),
        data_resolution: CandleInterval::H1,
        warmup_bars: 3,
        completed_only: true,
    }
}

fn slow_collection_request(id: &str) -> CollectRequest {
    CollectRequest {
        request_id: RequestId::new(id).expect("slow collection request"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(
            crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-02T02:00:00Z")
                .expect("slow start"),
            crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-02T04:00:00Z")
                .expect("slow end"),
        )
        .expect("slow range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    }
}

fn test_range() -> UtcRange {
    UtcRange::new(
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T02:00:00Z").expect("start"),
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T08:00:00Z").expect("end"),
    )
    .expect("range")
}

fn zero_costs() -> CostPolicy {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "zero synthetic costs".into(),
    }
}

fn wire_candles() -> Vec<u8> {
    let rows = (-1_i64..8)
        .rev()
        .map(|hour| {
            let opened = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .expect("base time")
                .checked_add_signed(chrono::Duration::hours(hour))
                .expect("wire time")
                .naive_utc()
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            serde_json::json!({
                "market": "KRW-BTC",
                "candle_date_time_utc": opened,
                "opening_price": 100 + hour,
                "high_price": 102 + hour,
                "low_price": 99 + hour,
                "trade_price": 101 + hour,
                "candle_acc_trade_volume": 10,
                "candle_acc_trade_price": 1000,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&rows).expect("wire candles")
}
