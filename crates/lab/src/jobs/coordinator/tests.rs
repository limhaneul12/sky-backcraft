use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, CandleInterval, CausalExecutionPolicy, CollectRequest, CostPolicy,
    CostSweep, DatasetId, EvidenceUnavailablePolicy, ExecutionPolicy, MarketId, MarketRuleSnapshot,
    PitPolicy, PriceKrw, QuoteAmount, ReportClock, ResearchDesign, ResearchSuiteAction,
    ResearchSuiteRequest, RuleProvenance, RuleSnapshotId, SuiteSummary, TerminalPolicy, TickBand,
    UtcRange, Weight,
};
use crate::database::{DatabaseHandle, DatabaseOwner};
use crate::jobs::JobRuntime;
use crate::market_data::UpbitClient;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
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
    request_aware: bool,
    temporary_failures: Option<Arc<AtomicU64>>,
    candle_responses: Arc<AtomicU64>,
    candle_response_ready: Arc<Notify>,
}

struct FixtureServer {
    base_url: String,
    gate: Option<Arc<RequestGate>>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    candle_responses: Arc<AtomicU64>,
    candle_response_ready: Arc<Notify>,
}

async fn fixture_response(
    State(state): State<FixtureState>,
    uri: Uri,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    if uri.path().ends_with("/v1/market/all") {
        return (
            StatusCode::OK,
            [("Remaining-Req", "group=market; min=1800; sec=9")],
            br#"[{"market":"KRW-BTC"},{"market":"KRW-ETH"}]"#.to_vec(),
        );
    }
    if let Some(gate) = &state.gate
        && !gate.held.swap(true, AtomicOrdering::AcqRel)
    {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    if let Some(remaining) = &state.temporary_failures
        && remaining
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok()
    {
        record_candle_response(&state);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("Remaining-Req", "group=candle; min=1800; sec=9")],
            br#"{"error":"temporary fixture outage"}"#.to_vec(),
        );
    }
    record_candle_response(&state);
    (
        StatusCode::OK,
        [("Remaining-Req", "group=candle; min=1800; sec=9")],
        if state.request_aware {
            request_aware_wire_candles(&uri)
        } else {
            state.candles.as_ref().clone()
        },
    )
}

async fn start_fixture(block_candles: bool) -> Result<FixtureServer, std::io::Error> {
    start_fixture_with_mode(block_candles, false, 0).await
}

async fn start_request_aware_fixture(
    temporary_failures: u64,
) -> Result<FixtureServer, std::io::Error> {
    start_fixture_with_mode(false, true, temporary_failures).await
}

#[tokio::test]
async fn synthetic_job_runtime_does_not_open_the_production_public_feed() -> Result<(), LabError> {
    use crate::contracts::RealtimeConnectionState;

    let root = TempRoot::new("synthetic-no-production-ws");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let runtime = JobRuntime::start(
        owner.handle(),
        UpbitClient::synthetic_local("http://127.0.0.1:1")?,
        root.0.clone(),
        None,
    )
    .await?;
    let service = runtime.service();
    assert!(service.inner.public_market_feed.is_none());
    assert_eq!(
        service.realtime_status().market.state,
        RealtimeConnectionState::Unavailable
    );
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[expect(
    clippy::too_many_lines,
    reason = "one local fixture proves paper and MOCK automation share the joined WebSocket source and causal candle"
)]
async fn shared_public_feed_drives_paper_and_mock_from_one_confirmed_candle()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::contracts::{
        AssetQuantity, AutomationRiskPolicy, AutomationSessionMode, AutomationSessionQuery,
        AutomationSessionWrite, CandleInterval, CostPolicy, ExecutionPolicy, PaperEventKind,
        PaperMarketRuleScenario, PaperMarketStart, PaperSessionQuery, PaperSessionWrite,
        PolicyDefinition, PolicyIndicator, PolicyIndicatorKind, PolicyProgram, PolicyRule,
        PolicySignalExpiry, PolicyTarget, PolicyWrite, QuoteAmount, RequestId, RulesProgram,
        TickBand, UtcTimestamp, Weight,
    };
    use crate::upbit::execution::LiveExecutionPolicy;

    let root = TempRoot::new("shared-public-feed-paper-mock");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();

    let now = UtcTimestamp::now();
    let interval = CandleInterval::M1;
    let close = crate::scheduling::latest_completed_boundary(now, interval)?;
    let open = UtcTimestamp(close.0 - interval.duration());
    let started_at = UtcTimestamp(open.0 + chrono::Duration::seconds(5));
    let candle_json = |open_at: UtcTimestamp, price: i64| {
        serde_json::json!({
            "market": "KRW-BTC",
            "candle_date_time_utc": open_at.0.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
            "opening_price": price,
            "high_price": price,
            "low_price": price,
            "trade_price": price,
            "candle_acc_trade_volume": 1,
            "candle_acc_trade_price": price,
        })
    };
    // The joined coordinator bootstraps the same bounded history requested by
    // paper recovery. Keep this provider fixture honest about that count rather
    // than relying on an earlier two-row cache to bypass bootstrap validation.
    let history = (0..crate::paper::MAX_PAPER_RECOVERY_BARS)
        .map(|offset| {
            let open_at = UtcTimestamp(open.0 - interval.duration() * i32::try_from(offset)?);
            Ok(candle_json(
                open_at,
                if offset == 0 { 10_000 } else { 9_900 },
            ))
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;
    let rest_body = serde_json::to_vec(&history)?;
    let rest_listener = TcpListener::bind("127.0.0.1:0").await?;
    let rest_address = rest_listener.local_addr()?;
    let rest_body = Arc::new(rest_body);
    let app = axum::Router::new().fallback({
        let rest_body = rest_body.clone();
        move || {
            let body = rest_body.as_ref().clone();
            async move {
                (
                    StatusCode::OK,
                    [("Remaining-Req", "group=candle; min=1800; sec=59")],
                    body,
                )
            }
        }
    });
    let (rest_stop, rest_stopped) = oneshot::channel();
    let rest_server = tokio::spawn(async move {
        axum::serve(rest_listener, app)
            .with_graceful_shutdown(async {
                let _ = rest_stopped.await;
            })
            .await
    });

    let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ws_address = ws_listener.local_addr()?;
    let ws_endpoint = format!("ws://{ws_address}/websocket/v1");
    let completed_frame = serde_json::json!({
        "type": "candle.1m",
        "code": "KRW-BTC",
        "timestamp": (close.0 - chrono::Duration::milliseconds(1)).timestamp_millis(),
        "candle_date_time_utc": open.0.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
        "opening_price": 10_000,
        "high_price": 10_000,
        "low_price": 10_000,
        "trade_price": 10_000,
        "candle_acc_trade_volume": 1,
        "candle_acc_trade_price": 10_000,
        "stream_type": "REALTIME",
    })
    .to_string();
    let forming_frame = serde_json::json!({
        "type": "candle.1m",
        "code": "KRW-BTC",
        "timestamp": now.0.timestamp_millis(),
        "candle_date_time_utc": close.0.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
        "opening_price": 50_000,
        "high_price": 50_000,
        "low_price": 50_000,
        "trade_price": 50_000,
        "candle_acc_trade_volume": 1,
        "candle_acc_trade_price": 50_000,
        "stream_type": "REALTIME",
    })
    .to_string();
    let frame_diagnostic = format!("completed={completed_frame}; forming={forming_frame}");
    let (subscription_tx, subscription_rx) = oneshot::channel();
    let (ws_closed_tx, ws_closed_rx) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(15), ws_listener.accept())
                .await??;
        let mut socket = accept_async(stream)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let subscription =
            match tokio::time::timeout(std::time::Duration::from_secs(5), socket.next()).await? {
                Some(Ok(Message::Text(text))) => text.to_string(),
                _ => return Err(std::io::Error::other("public subscription request missing")),
            };
        let _sent = subscription_tx.send(subscription);
        for frame in [completed_frame, forming_frame] {
            socket
                .send(Message::Text(frame.into()))
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        loop {
            match socket.next().await {
                Some(Ok(Message::Ping(payload))) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                }
                Some(Ok(Message::Close(_frame))) => {
                    socket
                        .flush()
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                    let _sent = ws_closed_tx.send(());
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(std::io::Error::other(error.to_string())),
                None => break,
            }
        }
        Ok::<(), std::io::Error>(())
    });

    let runtime = JobRuntime::start_with_test_market_feed_config(
        database.clone(),
        UpbitClient::synthetic_local(&format!("http://{rest_address}"))?,
        crate::upbit::client::UpbitPrivateClient::new_mock(),
        crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default()),
        root.0.clone(),
        Some("shared-feed-test".into()),
        crate::realtime::PublicMarketFeedConfig::loopback_fixture(&ws_endpoint)?,
    )
    .await?;
    let service = runtime.service();

    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "shared-source-paper-mock-test".into(),
        description: "SYNTHETIC_TEST_ONLY shared feed decision fixture".into(),
        program: PolicyProgram::Rules {
            program: RulesProgram {
                condition_roles: Vec::new(),
                indicators: vec![PolicyIndicator {
                    id: "close".into(),
                    indicator: PolicyIndicatorKind::Close,
                    source_interval: None,
                }],
                states: Vec::new(),
                rules: vec![PolicyRule {
                    id: "buy".into(),
                    condition: crate::contracts::BoolExpr::Compare {
                        comparison: crate::contracts::CompareOp::Gt,
                        left: crate::contracts::NumericExpr::Indicator { id: "close".into() },
                        right: crate::contracts::NumericExpr::Constant { value: 0.0 },
                    },
                    target: PolicyTarget::Weight {
                        value: crate::contracts::NumericExpr::Constant { value: 0.001 },
                    },
                }],
                fallback: PolicyTarget::Hold,
                signal_expiry: PolicySignalExpiry::EndOfRange,
            },
        },
    };
    let policy_write = PolicyWrite::Create {
        request_id: RequestId::new("shared-feed-test-policy")?,
        definition,
    };
    let policy = database
        .call("shared_feed_test_policy", move |store| {
            store.write_policy(&policy_write, started_at)
        })
        .await?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let zero = crate::contracts::BasisPoints::new(Decimal::ZERO)?;
    let paper_write = PaperSessionWrite::Start {
        request_id: RequestId::new("shared-feed-test-paper")?,
        policy_ref: policy.snapshot.reference.clone(),
        decision_interval: interval,
        execution_resolution: interval,
        latency_ms: 0,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        markets: vec![PaperMarketStart {
            market: market.clone(),
            initial_cash: QuoteAmount::new(Decimal::from(1_000_000))?,
            costs: CostPolicy {
                buy_fee_bps: zero,
                sell_fee_bps: zero,
                maker_fee_bps: zero,
                half_spread_bps: zero,
                slippage_bps: zero,
                impact_bps: zero,
                assumption_label: "zero synthetic costs".into(),
                dynamic: None,
            },
            market_rules: PaperMarketRuleScenario::SimulatedScenario {
                min_notional: QuoteAmount::new(Decimal::ONE)?,
                quantity_step: AssetQuantity::new(Decimal::new(1, 4))?,
                ticks: vec![TickBand {
                    lower_bound: QuoteAmount::new(Decimal::ZERO)?,
                    tick: crate::contracts::PriceKrw::new(Decimal::ONE)?,
                }],
                assumption_label: "synthetic test rules".into(),
            },
        }],
    };
    let paper_policy_ref = policy.snapshot.reference.clone();
    let paper_response = database
        .call("shared_feed_test_paper_start", move |store| {
            if store.load_policy_revision(&paper_policy_ref)?.is_none() {
                return Err(LabError::DataCorrupt("test policy disappeared".into()));
            }
            store.paper_write(&paper_write, started_at)
        })
        .await?;

    let automation_write = AutomationSessionWrite::Start {
        request_id: RequestId::new("shared-feed-test-automation")?,
        policy_ref: policy.snapshot.reference.clone(),
        market: market.clone(),
        decision_interval: interval,
        mode: AutomationSessionMode::Mock,
        risk: AutomationRiskPolicy {
            max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
            activation_loss_limit_krw: None,
            max_open_orders: 1,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        },
    };
    let policy_snapshot = policy.snapshot;
    let automation_session = database
        .call("shared_feed_test_mock_start", move |store| {
            store.automation_begin_request(
                &automation_write,
                Some("mock:upbit-default"),
                started_at,
            )?;
            store.automation_write_with_policy(
                &automation_write,
                Some(policy_snapshot),
                Some("mock:upbit-default".into()),
                started_at,
                LiveExecutionPolicy::Disabled,
            )
        })
        .await?;
    let requirements = database
        .call("shared_feed_test_requirements", |store| {
            store.active_realtime_market_requirements()
        })
        .await?;
    assert_eq!(requirements.len(), 1, "paper and MOCK share one feed key");
    assert!(requirements.contains(&(market.clone(), interval)));

    let feed = service
        .inner
        .public_market_feed
        .as_ref()
        .ok_or("runtime did not retain the injected public feed")?;
    // This fixture creates sessions through direct Store calls, so explicitly
    // wake the coordinator as the service admission path does after a write.
    feed.wake().notify_one();
    let subscription = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        subscription_rx,
    )
    .await
    .map_err(|error| {
        std::io::Error::other(format!(
            "public WS subscription timed out: {error}; health={:?}; status={:?}; server_finished={}; requirements={requirements:?}",
            feed.health(),
            service.realtime_status(),
            ws_server.is_finished(),
        ))
    })??;
    let subscription: serde_json::Value = serde_json::from_str(&subscription)?;
    assert_eq!(subscription[1]["type"], "candle.1m");
    let mut health_receiver = feed.subscribe_health();
    let mut first_feed_error = None;
    let readiness = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if feed.ensure_ready(&market, interval).is_ok() {
                break;
            }
            tokio::select! {
                changed = health_receiver.changed() => {
                    if changed.is_ok() && first_feed_error.is_none() {
                        first_feed_error = health_receiver
                            .borrow_and_update()
                            .last_error_category
                            .map(|category| format!("{category:?}"));
                    }
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    })
    .await;
    if let Err(error) = readiness {
        let health = feed.health();
        let server_finished = ws_server.is_finished();
        let server_result = if server_finished {
            format!("{:?}", ws_server.await)
        } else {
            "server still running".into()
        };
        return Err(std::io::Error::other(format!(
            "public WS per-key readiness timed out: {error}; first_feed_error={first_feed_error:?}; health={health:?}; status={:?}; server_finished={server_finished}; server_result={server_result}; frame={frame_diagnostic}; requirements={requirements:?}",
            service.realtime_status(),
        ))
        .into());
    }
    let count = crate::contracts::ProbeCount::try_from(2_u32)?;
    let (_, shared_observations) = feed
        .read_or_bootstrap_observations(&market, interval, count)
        .await?;
    let shared_candle = shared_observations
        .iter()
        .find(|observation| observation.candle.close_time_utc == close)
        .ok_or("confirmed feed candle missing")?;
    let provenance = {
        let catalog = rusqlite::Connection::open_with_flags(
            root.0.join("lab.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        shared_candle
            .raw_object_ids
            .iter()
            .map(|id| {
                let json: String = catalog.query_row(
                    "SELECT object_json FROM raw_objects WHERE id=?1",
                    [id.as_str()],
                    |row| row.get(0),
                )?;
                Ok::<crate::contracts::RawObjectRef, Box<dyn std::error::Error + Send + Sync>>(
                    serde_json::from_str(&json)?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut rest_reads = provenance
        .iter()
        .filter(|raw| raw.http_status == 200)
        .collect::<Vec<_>>();
    rest_reads.sort_by_key(|raw| raw.fetched_at);
    assert_eq!(rest_reads.len(), 2);
    assert_ne!(rest_reads[0].id, rest_reads[1].id);
    assert!(
        rest_reads[1].fetched_at.0 - rest_reads[0].fetched_at.0 >= chrono::Duration::seconds(1)
    );
    assert_eq!(
        provenance
            .iter()
            .filter(|raw| raw.http_status == 101)
            .count(),
        1
    );
    assert_eq!(provenance.len(), 3);
    assert!(shared_candle.candle.completed);
    assert_eq!(
        shared_candle.candle.close.get(),
        rust_decimal::Decimal::from(10_000)
    );
    assert!(
        shared_observations
            .iter()
            .all(|observation| observation.candle.open_time_utc != close)
    );

    let paper_session_id = paper_response.session.session_id;
    let automation_session_id = automation_session.session.session_id;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let paper_page: crate::contracts::PaperSessionEventPage = serde_json::from_value(
                service
                    .paper_session_query(PaperSessionQuery::Events {
                        session_id: paper_session_id.clone(),
                        after_sequence: None,
                        limit: 100,
                    })
                    .await?,
            )?;
            let mock_ledger: crate::contracts::AutomationLedgerPage = serde_json::from_value(
                service
                    .automation_session_query(AutomationSessionQuery::Ledger {
                        session_id: automation_session_id.clone(),
                        after_sequence: None,
                        limit: 100,
                    })
                    .await?,
            )?;
            let paper_consumed = paper_page.items.iter().any(|event| {
                event.kind == PaperEventKind::CandleProcessed
                    && event.finalized_at == Some(close)
                    && event.candle.as_ref().is_some_and(|candle| {
                        candle.open_time == open && candle.close_time == close
                    })
            });
            if paper_consumed && !mock_ledger.fills.is_empty() {
                assert_eq!(
                    mock_ledger.fills.len(),
                    1,
                    "replayed wakes do not double-fill"
                );
                break Ok::<(), LabError>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await??;

    runtime.shutdown().await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), ws_closed_rx).await??;
    ws_server.await??;
    let _sent = rest_stop.send(());
    rest_server.await??;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one loopback runtime regression reproduces durable-history versus cold-feed-cache readiness"
)]
async fn coordinator_bootstraps_cold_mtf_feed_keys_when_durable_candles_are_already_present()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::contracts::{
        AssetQuantity, BasisPoints, CandleInterval, CostPolicy, ExecutionPolicy, MarketId,
        PaperMarketRuleScenario, PaperMarketStart, PaperSessionWrite, PolicyDefinition,
        PolicyIndicator, PolicyIndicatorKind, PolicyRule, PolicySignalExpiry, PolicyTarget,
        PolicyWrite, ProbeCount, QuoteAmount, RequestId, RulesProgram, TickBand, UtcTimestamp,
        Weight,
    };
    use futures_util::{SinkExt, StreamExt};
    use rust_decimal::Decimal;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    // Keep the run away from a source-interval boundary so the old durable-due
    // selector cannot legitimately begin bootstrapping H1/H4 during this fixture.
    let now = UtcTimestamp::now();
    let next_h1 = crate::scheduling::latest_completed_boundary(now, CandleInterval::H1)?
        .0
        .checked_add_signed(CandleInterval::H1.duration())
        .ok_or("H1 test boundary overflow")?;
    let until_boundary = next_h1.signed_duration_since(now.0);
    if until_boundary >= chrono::Duration::zero() && until_boundary < chrono::Duration::seconds(30)
    {
        let wait_ms =
            u64::try_from((until_boundary + chrono::Duration::seconds(1)).num_milliseconds())?;
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
    }

    let root = TempRoot::new("cold-mtf-feed-cache");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let rest_fixture = start_request_aware_fixture(0).await?;
    let rest_client = UpbitClient::synthetic_local(&rest_fixture.base_url)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let preexisting_count = ProbeCount::try_from(199_u32)?;
    let (raw, observations) = rest_client
        .poll_paper_observations(&market, CandleInterval::H1, preexisting_count, &database)
        .await?;
    database
        .call("seed_preexisting_h1_candles", move |store| {
            store.reconcile_running_paper_sessions(&[(raw, observations)], UtcTimestamp::now())
        })
        .await?;

    let zero_bps = BasisPoints::new(Decimal::ZERO)?;
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "cold-mtf-feed-cache-test".into(),
        description: "SYNTHETIC_TEST_ONLY M5 policy with persisted H1 source".into(),
        program: crate::contracts::PolicyProgram::Rules {
            program: RulesProgram {
                condition_roles: Vec::new(),
                indicators: vec![PolicyIndicator {
                    id: "h1_close".into(),
                    indicator: PolicyIndicatorKind::Close,
                    source_interval: Some(CandleInterval::H1),
                }],
                states: Vec::new(),
                rules: vec![PolicyRule {
                    id: "hold".into(),
                    condition: crate::contracts::BoolExpr::Compare {
                        comparison: crate::contracts::CompareOp::Gt,
                        left: crate::contracts::NumericExpr::Indicator {
                            id: "h1_close".into(),
                        },
                        right: crate::contracts::NumericExpr::Constant { value: 0.0 },
                    },
                    target: PolicyTarget::Hold,
                }],
                fallback: PolicyTarget::Hold,
                signal_expiry: PolicySignalExpiry::EndOfRange,
            },
        },
    };
    let policy_write = PolicyWrite::Create {
        request_id: RequestId::new("cold-mtf-feed-cache-policy")?,
        definition,
    };
    let started_at = UtcTimestamp::now();
    let policy = database
        .call("create_cold_mtf_feed_cache_policy", move |store| {
            store.write_policy(&policy_write, started_at)
        })
        .await?;
    let paper_write = PaperSessionWrite::Start {
        request_id: RequestId::new("cold-mtf-feed-cache-paper")?,
        policy_ref: policy.snapshot.reference,
        decision_interval: CandleInterval::M5,
        execution_resolution: CandleInterval::M5,
        latency_ms: 0,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        markets: vec![PaperMarketStart {
            market: market.clone(),
            initial_cash: QuoteAmount::new(Decimal::from(100_000))?,
            costs: CostPolicy {
                buy_fee_bps: zero_bps,
                sell_fee_bps: zero_bps,
                maker_fee_bps: zero_bps,
                half_spread_bps: zero_bps,
                slippage_bps: zero_bps,
                impact_bps: zero_bps,
                assumption_label: "synthetic test costs".into(),
                dynamic: None,
            },
            market_rules: PaperMarketRuleScenario::SimulatedScenario {
                min_notional: QuoteAmount::new(Decimal::ONE)?,
                quantity_step: AssetQuantity::new(Decimal::new(1, 8))?,
                ticks: vec![TickBand {
                    lower_bound: QuoteAmount::new(Decimal::ZERO)?,
                    tick: crate::contracts::PriceKrw::new(Decimal::ONE)?,
                }],
                assumption_label: "synthetic test rules".into(),
            },
        }],
    };
    database
        .call("start_cold_mtf_feed_cache_paper", move |store| {
            store.paper_write(&paper_write, started_at)
        })
        .await?;

    let requirements = database
        .call("cold_mtf_feed_cache_requirements", |store| {
            store.active_realtime_market_requirements()
        })
        .await?;
    assert_eq!(requirements.len(), 2);
    for interval in [CandleInterval::M5, CandleInterval::H1] {
        assert!(requirements.contains(&(market.clone(), interval)));
    }
    let due = database
        .call("cold_mtf_feed_cache_durable_due", move |store| {
            store.due_paper_network_polls(UtcTimestamp::now())
        })
        .await?;
    assert!(
        due.iter()
            .any(|requirement| requirement.interval == CandleInterval::M5)
            && due
                .iter()
                .all(|requirement| requirement.interval != CandleInterval::H1),
        "M5 may be due, but persisted H1 history already reaches its source boundary: {due:?}"
    );

    let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ws_address = ws_listener.local_addr()?;
    let ws_endpoint = format!("ws://{ws_address}/websocket/v1");
    let frame_time = UtcTimestamp::now();
    let mut frames = Vec::new();
    for (interval, stream_type) in [
        (CandleInterval::M5, "candle.5m"),
        (CandleInterval::H1, "candle.60m"),
    ] {
        let open = crate::scheduling::latest_completed_boundary(frame_time, interval)?;
        frames.push(
            serde_json::json!({
                "type": stream_type,
                "code": market.code(),
                "timestamp": frame_time.0.timestamp_millis(),
                "candle_date_time_utc": open.0.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
                "opening_price": 100_000,
                "high_price": 100_001,
                "low_price": 99_999,
                "trade_price": 100_000,
                "candle_acc_trade_volume": 1,
                "candle_acc_trade_price": 100_000,
                "stream_type": "SNAPSHOT",
            })
            .to_string(),
        );
    }
    let (subscription_tx, subscription_rx) = oneshot::channel();
    let (frames_sent_tx, frames_sent_rx) = oneshot::channel();
    let (ws_stop_tx, mut ws_stop_rx) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (stream, _) = tokio::select! {
            _ = &mut ws_stop_rx => return Ok::<(), std::io::Error>(()),
            result = ws_listener.accept() => result?,
        };
        let mut socket = accept_async(stream)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let subscription =
            match tokio::time::timeout(std::time::Duration::from_secs(5), socket.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
                _ => return Err(std::io::Error::other("public subscription request missing")),
            };
        let _sent = subscription_tx.send(subscription);
        for frame in frames {
            socket
                .send(Message::Text(frame.into()))
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        let _sent = frames_sent_tx.send(());
        loop {
            tokio::select! {
                _ = &mut ws_stop_rx => break,
                incoming = socket.next() => {
                    match incoming {
                        Some(Ok(Message::Ping(payload))) => {
                            socket
                                .send(Message::Pong(payload))
                                .await
                                .map_err(|error| std::io::Error::other(error.to_string()))?;
                        }
                        Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                        Some(Ok(_)) => {}
                    }
                }
            }
        }
        Ok::<(), std::io::Error>(())
    });

    let runtime = JobRuntime::start_with_test_market_feed_config(
        database.clone(),
        rest_client,
        crate::upbit::client::UpbitPrivateClient::new_mock(),
        crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default()),
        root.0.clone(),
        Some("cold-mtf-feed-cache-test".into()),
        crate::realtime::PublicMarketFeedConfig::loopback_fixture(&ws_endpoint)?,
    )
    .await?;
    let service = runtime.service();
    let feed = service
        .inner
        .public_market_feed
        .as_ref()
        .ok_or("runtime did not retain the injected public feed")?;
    feed.wake().notify_one();

    let subscription = tokio::time::timeout(std::time::Duration::from_secs(10), subscription_rx)
        .await
        .ok()
        .and_then(Result::ok);
    let frames_sent = if subscription.is_some() {
        tokio::time::timeout(std::time::Duration::from_secs(5), frames_sent_rx)
            .await
            .is_ok_and(|result| result.is_ok())
    } else {
        false
    };
    let readiness = if frames_sent {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if service.realtime_status().market.ready_for_decisions {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or(false)
    } else {
        false
    };
    let market_status = service.realtime_status().market;
    runtime.shutdown().await?;
    let _sent = ws_stop_tx.send(());
    ws_server.await??;
    let _sent = rest_fixture.stop.send(());
    rest_fixture.task.await??;
    owner.shutdown()?;

    assert!(
        subscription.is_some(),
        "runtime did not subscribe to active MTF keys"
    );
    assert!(
        frames_sent,
        "loopback WebSocket did not send its MTF snapshots"
    );
    assert!(
        readiness,
        "active M5/H1 keys remained unready after coordinator bootstrap; market_status={market_status:?}"
    );
    Ok(())
}

async fn start_fixture_with_mode(
    block_candles: bool,
    request_aware: bool,
    temporary_failures: u64,
) -> Result<FixtureServer, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gate = block_candles.then(|| Arc::new(RequestGate::default()));
    let candle_responses = Arc::new(AtomicU64::new(0));
    let candle_response_ready = Arc::new(Notify::new());
    let app = axum::Router::new()
        .fallback(fixture_response)
        .with_state(FixtureState {
            candles: Arc::new(wire_candles()),
            gate: gate.clone(),
            request_aware,
            temporary_failures: (temporary_failures > 0)
                .then(|| Arc::new(AtomicU64::new(temporary_failures))),
            candle_responses: Arc::clone(&candle_responses),
            candle_response_ready: Arc::clone(&candle_response_ready),
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
        candle_responses,
        candle_response_ready,
    })
}

fn record_candle_response(state: &FixtureState) {
    state.candle_responses.fetch_add(1, AtomicOrdering::Release);
    state.candle_response_ready.notify_waiters();
}

async fn wait_for_candle_responses(server: &FixtureServer, expected: u64) {
    loop {
        let ready = server.candle_response_ready.notified();
        if server.candle_responses.load(AtomicOrdering::Acquire) >= expected {
            return;
        }
        ready.await;
    }
}

async fn wait_for_schedule_recovery_state(
    database: &DatabaseHandle,
    schedule_id: &crate::contracts::ScheduleId,
    expected: crate::contracts::ScheduleRecoveryState,
) -> Result<crate::contracts::CollectionScheduleRecord, LabError> {
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        poll.tick().await;
        let id = schedule_id.clone();
        let record = database
            .call("wait_schedule_recovery_state", move |store| {
                store.reconcile_collection_schedule(&id, UtcTimestamp::now())
            })
            .await?;
        if record.recovery_state == expected {
            return Ok(record);
        }
    }
}

fn request_aware_wire_candles(uri: &Uri) -> Vec<u8> {
    let parsed = reqwest::Url::parse(&format!("http://fixture{uri}")).expect("fixture request URL");
    let parameters = parsed
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let market = parameters
        .get("market")
        .map_or("KRW-BTC", std::borrow::Cow::as_ref);
    let count = parameters
        .get("count")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(2)
        .clamp(1, 200);
    let unit = uri
        .path()
        .rsplit('/')
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .expect("fixture candle interval");
    let interval = match unit {
        1 => crate::contracts::CandleInterval::M1,
        5 => crate::contracts::CandleInterval::M5,
        15 => crate::contracts::CandleInterval::M15,
        60 => crate::contracts::CandleInterval::H1,
        240 => crate::contracts::CandleInterval::H4,
        _ => panic!("unsupported fixture candle interval {unit}"),
    };
    let step_seconds = interval.duration().num_seconds();
    let requested_anchor = parameters
        .get("to")
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map_or_else(chrono::Utc::now, |value| value.with_timezone(&chrono::Utc));
    let anchor_seconds = requested_anchor.timestamp();
    let anchor = chrono::DateTime::from_timestamp(
        anchor_seconds - anchor_seconds.rem_euclid(step_seconds),
        0,
    )
    .expect("aligned fixture candle boundary");
    let rows = (0..count)
        .map(|offset| {
            let opened = anchor
                .checked_sub_signed(chrono::Duration::seconds(step_seconds * (offset + 1)))
                .expect("fixture candle time");
            serde_json::json!({
                "market": market,
                "candle_date_time_utc": opened.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
                "opening_price": 100 + offset,
                "high_price": 102 + offset,
                "low_price": 99 + offset,
                "trade_price": 101 + offset,
                "candle_acc_trade_volume": 10,
                "candle_acc_trade_price": 1000,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&rows).expect("request-aware wire candles")
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
            capital_mode: None,
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
        dynamic: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_schedule_mutations_report_distinguishable_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("schedule-receipts");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let server = start_fixture(false).await?;
    let client = UpbitClient::synthetic_local(&server.base_url)?;
    let runtime = JobRuntime::start(database, client, root.0.clone(), None).await?;
    let service = runtime.service();
    let request = crate::contracts::CollectionScheduleRequest {
        request_id: crate::contracts::RequestId::new("schedule-receipts")?,
        markets: vec![crate::contracts::MarketId::parse_upbit("KRW-BTC")?],
        interval: crate::contracts::CandleInterval::H1,
        lookback_bars: 2,
        cadence_seconds: 3_600,
        freshness_policy: None,
        retry: crate::contracts::ScheduleRetryPolicy {
            max_retries: 1,
            backoff_seconds: 60,
        },
    };
    let created = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Create {
            request: Box::new(request),
        })
        .await?;
    let schedule_id =
        crate::contracts::ScheduleId::new(created["id"].as_str().expect("created schedule id"))?;
    let outcome = |value: &serde_json::Value| {
        value
            .get("mutation_outcome")
            .and_then(|outcome| outcome.as_str())
            .expect("mutation outcome receipt")
            .to_string()
    };
    let paused = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Pause {
            schedule_id: schedule_id.clone(),
        })
        .await?;
    assert_eq!(outcome(&paused), "APPLIED");
    assert_eq!(paused["status"].as_str(), Some("paused"));
    let paused_again = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Pause {
            schedule_id: schedule_id.clone(),
        })
        .await?;
    assert_eq!(outcome(&paused_again), "NOT_APPLIED");
    let resumed = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Resume {
            schedule_id: schedule_id.clone(),
        })
        .await?;
    assert_eq!(outcome(&resumed), "APPLIED");
    assert_eq!(resumed["status"].as_str(), Some("active"));
    let resumed_again = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Resume {
            schedule_id: schedule_id.clone(),
        })
        .await?;
    assert_eq!(outcome(&resumed_again), "NOT_APPLIED");
    runtime.shutdown().await?;
    // A stopped fixture may already be down; the outcome is irrelevant here.
    let _stopped = server.stop.send(());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one runtime journey collects data, freezes a plan, runs the shared pool and inspects its ledger"
)]
async fn real_runtime_runs_shared_capital_portfolio_and_publishes_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        ArbitrationPolicy, CandleInterval, CollectRequest, DatasetStatus, ExecutionPolicy,
        PortfolioAssetSpec, PortfolioRiskPolicy, PortfolioRunRequest, PortfolioSpec,
        RequestId as RequestIdType, SCHEMA_VERSION, StrategySpec, TerminalPolicy,
        UtcRange as FixtureRange, Weight,
    };
    let root = TempRoot::new("portfolio-runtime");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let server = start_fixture(false).await?;
    let client = UpbitClient::synthetic_local(&server.base_url)?;
    let request = CollectRequest {
        request_id: RequestIdType::new("portfolio-runtime-dataset")?,
        markets: vec![MarketId::parse_upbit("KRW-BTC")?],
        range: test_range(),
        data_resolution: CandleInterval::H1,
        warmup_bars: 3,
        completed_only: true,
    };
    let dataset =
        crate::collection::collect(&client, &database, request, &CancellationToken::new()).await?;
    assert_eq!(dataset.manifest.status, DatasetStatus::Ready);
    let runtime = JobRuntime::start(
        database.clone(),
        client,
        root.0.clone(),
        Some("test".into()),
    )
    .await?;
    let service = runtime.service();

    // Freeze a plan over the collected dataset via the ordinary planning path.
    let range = FixtureRange::new(
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T02:00:00Z")?,
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T06:00:00Z")?,
    )?;
    let rules_range = FixtureRange::new(
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z")?,
        crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-02T00:00:00Z")?,
    )?;
    let zero = crate::contracts::BasisPoints::new(Decimal::ZERO)?;
    let spec = crate::contracts::ExperimentSpec {
        schema_version: SCHEMA_VERSION.into(),
        dataset_ids: vec![dataset.manifest.id.clone()],
        markets: vec![MarketId::parse_upbit("KRW-BTC")?],
        range,
        strategies: vec![StrategySpec::BuyAndHold],
        policy_selections: Vec::new(),
        causal_execution: None,
        capital_mode: Some(crate::contracts::CapitalMode::SharedPortfolio),
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: crate::contracts::QuoteAmount::new(Decimal::from(300_000))?,
        costs: crate::contracts::CostPolicy {
            buy_fee_bps: zero,
            sell_fee_bps: zero,
            maker_fee_bps: zero,
            half_spread_bps: zero,
            slippage_bps: zero,
            impact_bps: zero,
            assumption_label: "portfolio runtime fixture".into(),
            dynamic: None,
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        market_rules: crate::contracts::MarketRuleSnapshot {
            id: crate::contracts::RuleSnapshotId::new("runtime-portfolio-rules")?,
            provenance: crate::contracts::RuleProvenance::ExplicitScenario,
            valid_range: rules_range,
            observed_at: rules_range.start(),
            source_refs: vec!["synthetic-loopback".into()],
            assumption_label: "synthetic runtime fixture".into(),
            min_notional: crate::contracts::QuoteAmount::new(Decimal::ONE)?,
            quantity_step: crate::contracts::AssetQuantity::new(Decimal::new(1, 4))?,
            ticks: vec![crate::contracts::TickBand {
                lower_bound: crate::contracts::QuoteAmount::new(Decimal::ZERO)?,
                tick: crate::contracts::PriceKrw::new(Decimal::ONE)?,
            }],
            fee_schedule: None,
            trading_state: None,
            maintenance_windows: Vec::new(),
        },
        market_rules_history: Vec::new(),
        terminal_policy: TerminalPolicy::LiquidateScenario,
        evidence_snapshot_id: None,
        pit_policy: crate::contracts::PitPolicy::StrictPit,
        evidence_unavailable: crate::contracts::EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: crate::contracts::ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 1,
            risk_free_annual: 0.0,
        },
        seed: 5,
    };
    let plan = crate::planning::prepare(
        &database,
        crate::contracts::PlanRequest {
            request_id: RequestIdType::new("portfolio-runtime-plan")?,
            spec,
        },
    )
    .await?;
    let portfolio = PortfolioSpec {
        initial_cash: crate::contracts::QuoteAmount::new(Decimal::from(300_000))?,
        assets: vec![PortfolioAssetSpec {
            market: MarketId::parse_upbit("KRW-BTC")?,
            max_weight: Weight::new(Decimal::new(45, 2))?,
        }],
        risk: PortfolioRiskPolicy {
            max_gross_exposure: Weight::new(Decimal::new(70, 2))?,
            min_cash_weight: Weight::new(Decimal::new(20, 2))?,
            drawdown_stop: Weight::new(Decimal::new(15, 3))?,
        },
        arbitration: ArbitrationPolicy::Priority,
    };
    let submission = JobSubmission {
        request_id: RequestIdType::new("portfolio-runtime-run")?,
        payload: JobPayload::PortfolioBacktest {
            request: Box::new(PortfolioRunRequest {
                request_id: RequestIdType::new("portfolio-runtime-run")?,
                plan_id: plan.id.clone(),
                input_digest: plan.input_digest.clone(),
                portfolio,
                regime: None,
            }),
        },
    };
    let job = service.submit(submission).await?;
    let finished = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let job = service
                .control(crate::contracts::JobControl::Get {
                    job_id: job.id.clone(),
                })
                .await?;
            let state = &job
                .attempts
                .last()
                .expect("submitted job has one attempt")
                .state;
            if state.is_terminal() {
                return Ok::<_, LabError>(job);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await??;
    let state = finished
        .attempts
        .last()
        .expect("terminal attempt")
        .state
        .clone();
    let crate::contracts::AttemptState::Completed { output, .. } = state else {
        panic!("portfolio job must complete, got {state:?}");
    };
    let crate::contracts::JobOutput::Portfolio { run_id } = output else {
        panic!("portfolio job produces a portfolio output");
    };
    let inspect_run_id = run_id.clone();
    let projection_run_id = run_id.clone();
    let (summary, facts, projections) = database
        .call("inspect_portfolio_runtime", move |store| {
            let summary = store
                .portfolio_run_summary(&inspect_run_id)?
                .ok_or_else(|| LabError::DataCorrupt("portfolio run disappeared".into()))?;
            let marks = store.portfolio_facts(
                &inspect_run_id,
                crate::contracts::PortfolioFactKind::Mark,
                0,
                500,
            )?;
            let fills = store.portfolio_facts(
                &inspect_run_id,
                crate::contracts::PortfolioFactKind::Fill,
                0,
                500,
            )?;
            let projections = serde_json::to_value((
                store.portfolio_summary_projection(&projection_run_id)?,
                store.portfolio_equity(&projection_run_id, 0, 500)?,
                store.portfolio_allocations(&projection_run_id, 0, 500)?,
                store.portfolio_rebalances(&projection_run_id, 0, 500)?,
                store.portfolio_contributions(&projection_run_id)?,
            ))?;
            Ok((summary, (marks.len(), fills.len()), projections))
        })
        .await?;
    assert_eq!(summary.status, "completed");
    assert!(facts.0 >= 2, "terminal marks published: {facts:?}");
    assert!(facts.1 >= 1, "at least one fill published: {facts:?}");
    // The wire prices rise one unit per hour, so a held position liquidates
    // above its entry: the shared pool ends above its initial 300000.
    let terminal: Decimal = serde_json::from_value(summary.totals["terminal_equity"].clone())?;
    assert!(
        terminal > Decimal::from(300_000),
        "rising wire prices end above the initial pool: {terminal}"
    );
    assert_eq!(
        summary.totals["rejected_signals"],
        serde_json::json!(0),
        "single asset with 45 percent cap never rejects a BuyAndHold entry"
    );
    let projected = projections
        .as_array()
        .expect("five typed portfolio projections");
    assert_eq!(projected.len(), 5);
    assert!(projected.iter().all(|value| value["status"] == "AVAILABLE"));

    runtime.shutdown().await?;
    let _stopped = server.stop.send(());
    server.task.await??;
    owner.shutdown()?;
    let reopened = DatabaseOwner::open(root.0.clone())?;
    let reopened_database = reopened.handle();
    let reopened_run_id = run_id;
    let reopened_projections = reopened_database
        .call("inspect_reopened_portfolio_projections", move |store| {
            Ok(serde_json::to_value((
                store.portfolio_summary_projection(&reopened_run_id)?,
                store.portfolio_equity(&reopened_run_id, 0, 500)?,
                store.portfolio_allocations(&reopened_run_id, 0, 500)?,
                store.portfolio_rebalances(&reopened_run_id, 0, 500)?,
                store.portfolio_contributions(&reopened_run_id)?,
            ))?)
        })
        .await?;
    assert_eq!(reopened_projections, projections);
    reopened.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one causal fixture proves the actual 503, durable retry, all-market probe, backfill and freshness state journey"
)]
async fn recoverable_schedule_probes_all_markets_backfills_and_verifies_freshness()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{CandleInterval, RequestId as RequestIdType, ScheduleRetryPolicy};
    let root = TempRoot::new("schedule-self-heal");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    // Three bounded HTTP attempts for the initial fire and three for its one
    // ordinary retry fail with real 503 responses. Recovery probes then see a
    // healthy request-aware source.
    let server = start_request_aware_fixture(6).await?;
    let client = UpbitClient::synthetic_local(&server.base_url)?;
    let request = crate::contracts::CollectionScheduleRequest {
        request_id: RequestIdType::new("schedule-self-heal")?,
        markets: vec![
            crate::contracts::MarketId::parse_upbit("KRW-BTC")?,
            crate::contracts::MarketId::parse_upbit("KRW-ETH")?,
        ],
        interval: CandleInterval::H1,
        lookback_bars: 2,
        cadence_seconds: 3_600,
        freshness_policy: None,
        retry: ScheduleRetryPolicy {
            max_retries: 1,
            backoff_seconds: 60,
        },
    };
    let created_at = UtcTimestamp::now();
    let frozen = crate::scheduling::create_schedule(request, created_at)?;
    let schedule_id = frozen.id.clone();
    database
        .call("create_self_heal_fixture", move |store| {
            store.create_collection_schedule(&frozen)?;
            Ok(())
        })
        .await?;
    let runtime = JobRuntime::start(database.clone(), client, root.0.clone(), None).await?;
    let service = runtime.service();
    service
        .coordinate_once(UtcTimestamp(
            created_at
                .0
                .checked_add_signed(chrono::Duration::seconds(1))
                .expect("initial scheduler clock"),
        ))
        .await?;
    let degraded = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        wait_for_candle_responses(&server, 3).await;
        wait_for_schedule_recovery_state(
            &database,
            &schedule_id,
            crate::contracts::ScheduleRecoveryState::Degraded,
        )
        .await
    })
    .await
    .map_err(|_| "initial 503 to durable degraded phase timed out")??;
    let retry_at = degraded
        .in_flight
        .as_ref()
        .and_then(|fire| fire.retry_at)
        .expect("ordinary retry wait");
    service
        .coordinate_once(UtcTimestamp(
            retry_at
                .0
                .checked_add_signed(chrono::Duration::seconds(1))
                .expect("retry clock"),
        ))
        .await?;
    let waiting = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        wait_for_candle_responses(&server, 6).await;
        wait_for_schedule_recovery_state(
            &database,
            &schedule_id,
            crate::contracts::ScheduleRecoveryState::RecoveryWait,
        )
        .await
    })
    .await
    .map_err(|_| "ordinary retry to durable recovery wait phase timed out")??;
    assert_eq!(
        waiting
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some("NETWORK_UNAVAILABLE")
    );
    let recovery_at = waiting.next_recovery_at.expect("durable recovery wait");
    service
        .coordinate_once(UtcTimestamp(
            recovery_at
                .0
                .checked_add_signed(chrono::Duration::seconds(1))
                .expect("recovery clock"),
        ))
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        wait_for_candle_responses(&server, 10).await;
        wait_for_schedule_recovery_state(
            &database,
            &schedule_id,
            crate::contracts::ScheduleRecoveryState::Active,
        )
        .await
    })
    .await
    .map_err(|_| "probe/backfill to freshness-verified active phase timed out")??;
    let healed = service
        .collection_schedule(crate::contracts::CollectionScheduleAction::Get {
            schedule_id: schedule_id.clone(),
        })
        .await?;
    assert_eq!(healed["status"].as_str(), Some("active"));
    assert_eq!(healed["recovery_state"].as_str(), Some("active"));
    assert!(healed["last_success_boundary"].is_string());
    assert_eq!(
        healed["failure"],
        serde_json::Value::Null,
        "recovery clears the recorded failure"
    );
    runtime.shutdown().await?;
    let _stopped = server.stop.send(());
    server.task.await??;
    owner.shutdown()?;
    Ok(())
}
