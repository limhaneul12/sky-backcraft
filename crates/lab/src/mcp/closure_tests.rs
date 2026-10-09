use super::{LabMcpService, McpExposure, result_text};
use crate::contracts::LabError;
use crate::database::DatabaseOwner;
use crate::jobs::JobRuntime;
use crate::market_data::UpbitClient;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, object};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn install_crypto_provider() -> Result<(), LabError> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    match rustls::crypto::ring::default_provider().install_default() {
        Ok(()) => Ok(()),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
        Err(_) => Err(LabError::Internal(
            "test TLS crypto provider could not be installed".into(),
        )),
    }
}

struct TempRoot(std::path::PathBuf);

impl TempRoot {
    fn new() -> Result<Self, std::io::Error> {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-closure-http-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct UpbitFixture;

struct FixtureServer {
    base_url: String,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn upbit_response(
    State(_state): State<UpbitFixture>,
    uri: Uri,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    let body = if uri.path().ends_with("/v1/market/all") {
        br#"[{"market":"KRW-BTC"}]"#.to_vec()
    } else {
        fixture_candles(&uri)
    };
    (
        StatusCode::OK,
        [("Remaining-Req", "group=candle; min=1800; sec=9")],
        body,
    )
}

fn fixture_candles(uri: &Uri) -> Vec<u8> {
    let parsed = reqwest::Url::parse(&format!("http://fixture{uri}"))
        .expect("fixture receives a valid request URI");
    let query = parsed
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let market = query
        .get("market")
        .map_or("KRW-BTC", std::borrow::Cow::as_ref);
    let count = query
        .get("count")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1)
        .clamp(1, 200);
    let unit_minutes = uri
        .path()
        .rsplit('/')
        .next()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1);
    let anchor = query
        .get("to")
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map_or_else(
            || {
                let now = chrono::Utc::now();
                let seconds = unit_minutes * 60;
                chrono::DateTime::from_timestamp(
                    now.timestamp() - now.timestamp().rem_euclid(seconds),
                    0,
                )
                .expect("current candle boundary")
            },
            |value| value.with_timezone(&chrono::Utc),
        );
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("fixture base time")
        .with_timezone(&chrono::Utc);
    let rows = (0..count)
        .map(|offset| {
            let opened = anchor
                .checked_sub_signed(chrono::Duration::minutes(unit_minutes * (offset + 1)))
                .expect("fixture candle time");
            let minute = opened.signed_duration_since(base).num_minutes();
            let close = 100_000_i64.saturating_add(minute.max(0));
            serde_json::json!({
                "market": market,
                "candle_date_time_utc": opened.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string(),
                "opening_price": close - 1,
                "high_price": close + 2,
                "low_price": close - 2,
                "trade_price": close,
                "candle_acc_trade_volume": 10,
                "candle_acc_trade_price": close * 10,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&rows).expect("serialize fixture candles")
}

async fn start_fixture() -> Result<FixtureServer, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = axum::Router::new()
        .fallback(upbit_response)
        .with_state(UpbitFixture);
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ignored = stopped.await;
            })
            .await
    });
    Ok(FixtureServer {
        base_url: format!("http://{address}"),
        stop,
        task,
    })
}

struct McpServer {
    url: String,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn start_mcp(
    database: crate::database::DatabaseHandle,
    upbit: UpbitClient,
    jobs: crate::jobs::JobService,
) -> Result<McpServer, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let stop = CancellationToken::new();
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_cancellation_token(stop.child_token());
    let service = StreamableHttpService::new(
        move || {
            Ok(LabMcpService::new(
                database.clone(),
                upbit.clone(),
                jobs.clone(),
                McpExposure::Local,
            ))
        },
        LocalSessionManager::default().into(),
        config,
    );
    let app = axum::Router::new().nest_service("/mcp", service);
    let graceful = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(graceful.cancelled_owned())
            .await
    });
    Ok(McpServer {
        url: format!("http://{address}/mcp"),
        stop,
        task,
    })
}

type McpClient = RunningService<RoleClient, ()>;

async fn call(
    client: &McpClient,
    name: &str,
    arguments: serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut request = CallToolRequestParams::default();
    request.name = name.to_owned().into();
    request.arguments = Some(object(arguments));
    let result = client.call_tool(request).await?;
    if result.is_error == Some(true) {
        return Err(format!("{name} returned a tool error: {}", result_text(result)).into());
    }
    Ok(serde_json::from_str(&result_text(result))?)
}

async fn wait_job(
    client: &McpClient,
    submitted: serde_json::Value,
    label: &'static str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let job_id = submitted
        .pointer("/job/id")
        .and_then(serde_json::Value::as_str)
        .ok_or("submitted MCP job omitted job.id")?
        .to_owned();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
    loop {
        let job = call(
            client,
            "job_control",
            serde_json::json!({"action":"get", "job_id":job_id}),
        )
        .await?;
        let attempt = latest_attempt(&job)?;
        let state = attempt
            .get("state")
            .and_then(serde_json::Value::as_object)
            .and_then(|record| record.get("state"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| LabError::DataCorrupt("public job attempt omitted state tag".into()))?;
        if matches!(state, "COMPLETED" | "PARTIAL") {
            return Ok(job);
        }
        if matches!(state, "FAILED" | "BLOCKED" | "CANCELLED" | "INTERRUPTED") {
            return Err(format!("{label} job {job_id} ended as {state}: {job}").into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                format!("{label} job {job_id} exceeded 45 seconds in {state}: {job}").into(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn latest_attempt(job: &serde_json::Value) -> Result<&serde_json::Value, LabError> {
    job.get("job")
        .and_then(|record| record.get("attempts"))
        .and_then(serde_json::Value::as_array)
        .and_then(|attempts| attempts.last())
        .ok_or_else(|| LabError::DataCorrupt("public job omitted its latest attempt".into()))
}

fn output_id<'a>(job: &'a serde_json::Value, kind: &str, field: &str) -> Result<&'a str, LabError> {
    let output = latest_attempt(job)?
        .get("state")
        .and_then(|state| state.get("output"))
        .ok_or_else(|| LabError::DataCorrupt("completed job omitted output".into()))?;
    if output["kind"] != kind {
        return Err(LabError::DataCorrupt(format!(
            "expected {kind} output, got {output}"
        )));
    }
    output[field]
        .as_str()
        .ok_or_else(|| LabError::DataCorrupt(format!("{kind} output omitted {field}")))
}

fn multi_timeframe_policy() -> serde_json::Value {
    let indicator = |id: &str| serde_json::json!({"op":"INDICATOR", "id":id});
    let compare = |left: &str, right: &str| {
        serde_json::json!({
            "op":"COMPARE", "comparison":"GT",
            "left":indicator(left), "right":indicator(right)
        })
    };
    serde_json::json!({
        "schema_version":"1.0",
        "name":"H1 M15 M5 causal closure",
        "description":"H1 regime, M15 setup and M5 trigger evaluated on M1 execution bars.",
        "program": {"kind":"RULES", "program": {
            "indicators":[
                {"id":"h1_ema20", "indicator":{"kind":"EMA", "window":20}, "source_interval":"h1"},
                {"id":"h1_ema50", "indicator":{"kind":"EMA", "window":50}, "source_interval":"h1"},
                {"id":"m15_rsi14", "indicator":{"kind":"RSI", "window":14}, "source_interval":"m15"},
                {"id":"m5_close", "indicator":{"kind":"CLOSE"}, "source_interval":"m5"},
                {"id":"m5_ema20", "indicator":{"kind":"EMA", "window":20}, "source_interval":"m5"}
            ],
            "states":[],
            "rules":[{"id":"causal_entry", "condition":{
                "op":"AND", "conditions":[
                    compare("h1_ema20", "h1_ema50"),
                    {"op":"COMPARE", "comparison":"GT", "left":indicator("m15_rsi14"), "right":{"op":"CONSTANT", "value":50.0}},
                    compare("m5_close", "m5_ema20")
                ]
            }, "target":{"kind":"WEIGHT", "value":{"op":"CONSTANT", "value":1.0}}}],
            "fallback":{"kind":"WEIGHT", "value":{"op":"CONSTANT", "value":0.0}},
            "signal_expiry":{"kind":"END_OF_RANGE"}
        }}
    })
}

fn experiment(dataset_id: &str, policy_ref: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "schema_version":"3.0",
        "dataset_ids":[dataset_id],
        "markets":["KRW-BTC"],
        "range":{"start":"2024-01-03T02:00:00Z", "end":"2024-01-03T06:00:00Z"},
        "strategies":[],
        "policy_selections":[policy_ref],
        "causal_execution":"DECLARED_POLICY_WARMUP",
        "capital_mode":"SHARED_PORTFOLIO",
        "decision_interval":"m1",
        "execution_resolution":"m1",
        "latency_ms":0,
        "initial_cash":"300000",
        "costs":{
            "buy_fee_bps":"5", "sell_fee_bps":"5", "maker_fee_bps":"5",
            "half_spread_bps":"0", "slippage_bps":"0", "impact_bps":"0",
            "assumption_label":"deterministic synthetic closure fixture", "dynamic":null
        },
        "execution":{"kind":"NEXT_BAR_OPEN", "participation_cap":"1"},
        "market_rules":{
            "id":"closure-synthetic-rules", "provenance":"EXPLICIT_SCENARIO",
            "valid_range":{"start":"2024-01-01T00:00:00Z", "end":"2024-01-04T00:00:00Z"},
            "observed_at":"2024-01-01T00:00:00Z", "source_refs":["loopback-fixture"],
            "assumption_label":"synthetic rules only", "min_notional":"1",
            "quantity_step":"0.0001", "ticks":[{"lower_bound":"0", "tick":"1"}],
            "fee_schedule":null, "trading_state":null, "maintenance_windows":[]
        },
        "market_rules_history":[],
        "terminal_policy":"LIQUIDATE_SCENARIO",
        "evidence_snapshot_id":null,
        "pit_policy":"STRICT_PIT",
        "evidence_unavailable":"CASH_WITH_MATCHED_CONTROL",
        "report_clock":{"timezone":"UTC", "min_annualization_days":1, "risk_free_annual":0.0},
        "seed":20_261_008
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[expect(
    clippy::too_many_lines,
    reason = "one real TCP MCP journey proves the cross-feature public closure without duplicating internal unit contracts"
)]
async fn real_http_mcp_closes_research_runtime_contracts() -> Result<(), Box<dyn std::error::Error>>
{
    install_crypto_provider()?;
    let root = TempRoot::new()?;
    let fixture = start_fixture().await?;
    let upbit = UpbitClient::synthetic_local(&fixture.base_url)?;
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let runtime = JobRuntime::start(
        database.clone(),
        upbit.clone(),
        root.0.clone(),
        Some("runtime-fallback-must-not-win".into()),
    )
    .await?;
    let mcp = start_mcp(database.clone(), upbit, runtime.service()).await?;
    let client = ().serve(StreamableHttpClientTransport::from_uri(mcp.url.clone())).await?;

    let outcome = async {
    let tools = client.list_all_tools().await?;
    let names = tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "lab_status",
        "policy_write",
        "collect_data",
        "plan_backtest",
        "portfolio_backtest",
        "result_query",
        "collection_schedule",
        "storage_maintenance",
    ] {
        assert!(
            names.contains(expected),
            "public catalog omitted {expected}"
        );
    }
    let status = call(&client, "lab_status", serde_json::json!({})).await?;
    let revision = status["code_revision"]
        .as_str()
        .ok_or("build identity omitted code_revision")?;
    assert!(
        revision.len() >= 40,
        "revision is not a full build-time Git identity"
    );
        assert_eq!(status["scope"]["execution_origin"], "SIMULATED_ONLY");
        assert_eq!(status["scope"]["private_api"], "never");
        assert_eq!(status["implementation_status"], "IMPLEMENTED_UNVERIFIED");
        assert!(status["verification_receipt"].is_null());
    for digest in ["source_digest", "lockfile_digest"] {
        assert_eq!(status[digest].as_str().map(str::len), Some(64));
    }

    let list = || serde_json::json!({"action":"list", "after_policy_id":null, "limit":100});
    let policy_count_before = call(&client, "policy_query", list()).await?["total_count"]
        .as_u64()
        .ok_or("policy list omitted total_count")?;
    let sweep_mode = serde_json::json!({
        "kind":"grid",
        "axes":{
            "entry_length":[{"integer":20},{"integer":55}],
            "exit_length":[{"integer":10},{"integer":20}]
        }
    });
    let preflight = call(
        &client,
        "policy_write",
        serde_json::json!({
            "action":"preflight", "request_id":"closure-preflight", "family":"S2",
            "template":{"kind":"S2", "entry_length":20, "exit_length":10},
            "mode":sweep_mode.clone()
        }),
    )
    .await?;
    assert_eq!(preflight["exact_tuple_count"], 4);
    assert_eq!(preflight["candidate_count"], 4);
    assert_eq!(preflight["resource_admissible"], true);
    assert_eq!(
        call(&client, "policy_query", list()).await?["total_count"],
        policy_count_before,
        "preflight must not materialize candidates"
    );
    let swept = call(
        &client,
        "policy_write",
        serde_json::json!({
            "action":"sweep", "request_id":"closure-sweep", "family":"S2",
            "template":{"kind":"S2", "entry_length":20, "exit_length":10},
            "mode":sweep_mode
        }),
    )
    .await?;
    assert_eq!(swept["revisions"].as_array().map(Vec::len), Some(4));

    let policy = call(
        &client,
        "policy_write",
        serde_json::json!({
            "action":"create", "request_id":"closure-multi-timeframe-policy",
            "definition":multi_timeframe_policy()
        }),
    )
    .await?;
    let policy_ref = policy["snapshot"]["reference"].clone();
    let expected_policy_ref = policy_ref.clone();

    let collection = call(
        &client,
        "collect_data",
        serde_json::json!({
            "request_id":"closure-m1-collection", "markets":["KRW-BTC"],
            "range":{"start":"2024-01-01T00:00:00Z", "end":"2024-01-03T06:01:00Z"},
            "data_resolution":"m1", "warmup_bars":0, "completed_only":true
        }),
    )
    .await?;
        let collection = wait_job(&client, collection, "M1 collection").await?;
    let dataset_id = output_id(&collection, "DATASET", "dataset_id")?.to_owned();
    let dataset = call(
        &client,
        "dataset_query",
        serde_json::json!({
            "action":"get", "dataset_id":dataset_id.clone(), "offset":0, "limit":1
        }),
    )
    .await?;
    assert_eq!(dataset["manifest"]["request"]["data_resolution"], "m1");
    assert!(
        dataset["total_count"]
            .as_u64()
            .is_some_and(|count| count >= 3_241)
    );

        let plan = call(
            &client,
            "plan_backtest",
            serde_json::json!({
                "request_id":"closure-multi-timeframe-plan",
                    "spec":experiment(&dataset_id, &policy_ref)
            }),
    )
    .await?;
    let plan_id = plan["id"].as_str().ok_or("plan omitted id")?;
    let input_digest = plan["input_digest"].as_str().ok_or("plan omitted digest")?;
    let portfolio = call(&client, "portfolio_backtest", serde_json::json!({
        "action":"create", "request":{
            "request_id":"closure-shared-portfolio", "plan_id":plan_id,
            "input_digest":input_digest,
            "portfolio":{
                "initial_cash":"300000",
                "assets":[{"market":"KRW-BTC", "max_weight":"0.45"}],
                "risk":{"max_gross_exposure":"0.70", "min_cash_weight":"0.20", "drawdown_stop":"0.15"},
                "arbitration":"PRIORITY"
            },
            "regime":{
                "classifier":{
                    "revision":"closure-rule-v1", "sma_long":3100, "sma_mid":3, "sma_short":5,
                    "slope_lookback":2, "vol_lookback":4, "atr_lookback":4,
                    "high_vol_annualized":null, "low_vol_annualized":null
                },
                "rules":{
                    "trend_up":{"kind":"ENABLED"}, "trend_down":{"kind":"DISABLED"},
                    "chop":{"kind":"DISABLED"}, "unknown":{"kind":"DISABLED"}
                }
            }
        }
    })).await?;
        let portfolio_job_id = portfolio["id"]
            .as_str()
            .ok_or("portfolio submission omitted job id")?
            .to_owned();
        let portfolio = match wait_job(
            &client,
            serde_json::json!({"job":portfolio}),
            "shared portfolio run",
        )
        .await
        {
            Ok(job) => job,
            Err(public_error) => {
                let job_id = crate::contracts::JobId::new(&portfolio_job_id)?;
                let internal = database
                    .call("inspect_failed_http_portfolio", move |store| {
                        store.get_job(&job_id)?.ok_or_else(|| {
                            LabError::DataCorrupt("failed portfolio job disappeared".into())
                        })
                    })
                    .await?;
                return Err(format!(
                    "{public_error}; internal={}",
                    serde_json::to_string(&internal)?
                )
                .into());
            }
        };
    let run_id = output_id(&portfolio, "PORTFOLIO", "run_id")?.to_owned();

    let mut projections = Vec::new();
    for query in [
        serde_json::json!({"action":"portfolio_summary", "run_id":run_id.clone()}),
        serde_json::json!({"action":"portfolio_equity", "run_id":run_id.clone(), "offset":0, "limit":500}),
        serde_json::json!({"action":"portfolio_allocations", "run_id":run_id.clone(), "offset":0, "limit":500}),
        serde_json::json!({"action":"portfolio_rebalances", "run_id":run_id.clone(), "offset":0, "limit":500}),
        serde_json::json!({"action":"portfolio_contributions", "run_id":run_id.clone()}),
    ] {
        let value = call(&client, "result_query", query).await?;
        assert_eq!(value["status"], "AVAILABLE");
        projections.push(value);
    }
    let summary = &projections[0]["data"];
    let contributions = &projections[4]["data"];
    let pnl = Decimal::from_str(summary["portfolio_pnl"].as_str().ok_or("summary pnl")?)?;
    let market_sum = contributions["market"]
        .as_array()
        .ok_or("market contributions")?
        .iter()
        .try_fold(Decimal::ZERO, |sum, row| {
            Decimal::from_str(row["net_pnl"].as_str().ok_or("market net pnl")?)
                .map(|value| sum + value)
        })?;
    let residual = Decimal::from_str(contributions["residual"].as_str().ok_or("residual")?)?;
    let tolerance = Decimal::from_str(contributions["tolerance"].as_str().ok_or("tolerance")?)?;
    assert!((market_sum + residual - pnl).abs() <= tolerance);
    for point in projections[2]["data"]["items"]
        .as_array()
        .ok_or("allocation page")?
    {
        let cash = Decimal::from_str(point["cash_weight"].as_str().ok_or("cash weight")?)?;
        let assets = point["assets"]
            .as_array()
            .ok_or("asset allocations")?
            .iter()
            .try_fold(Decimal::ZERO, |sum, asset| {
                Decimal::from_str(asset["weight"].as_str().ok_or("asset weight")?)
                    .map(|value| sum + value)
            })?;
        assert!(cash >= Decimal::ZERO);
        assert!(assets <= Decimal::ONE);
    }
    assert!(
        projections[2]["data"]["items"]
            .as_array()
            .is_some_and(|items| items.iter().any(|point| {
                point["assets"].as_array().is_some_and(|assets| {
                    assets
                        .iter()
                        .any(|asset| asset["source"]["policy_ref"] == expected_policy_ref)
                })
            })),
        "allocation projection must retain the exact frozen policy revision"
    );
    let timeline = call(
        &client,
        "result_query",
        serde_json::json!({
            "action":"regime_timeline", "run_id":run_id.clone(), "offset":0, "limit":500
        }),
    )
    .await?;
    let regimes = call(
        &client,
        "result_query",
        serde_json::json!({
            "action":"regime_summary", "run_id":run_id
        }),
    )
    .await?;
    assert_eq!(timeline["status"], "AVAILABLE");
    assert_eq!(regimes["status"], "AVAILABLE");
    let unknown_times = timeline["data"]["items"]
        .as_array()
        .ok_or("regime timeline page")?
        .iter()
        .filter(|row| row["current_regime"] == "UNKNOWN")
        .filter_map(|row| row["timestamp"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !unknown_times.is_empty(),
        "classifier warmup must remain UNKNOWN"
    );
    let allocations = projections[2]["data"]["items"]
        .as_array()
        .ok_or("allocation page")?;
    assert!(
        allocations.iter().any(|point| {
            unknown_times.contains(point["timestamp"].as_str().unwrap_or_default())
                && point["cash_weight"]
                    .as_str()
                    .and_then(|value| Decimal::from_str(value).ok())
                    == Some(Decimal::ONE)
                && point["assets"].as_array().is_some_and(|assets| {
                    assets.iter().all(|asset| {
                        asset["weight"]
                            .as_str()
                            .and_then(|value| Decimal::from_str(value).ok())
                            == Some(Decimal::ZERO)
                    })
                })
        }),
        "UNKNOWN regime must gate the policy to cash"
    );
        assert!(
            projections[3]["data"]["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty()),
            "TREND_UP must produce an actual shared-pool rebalance"
        );
        assert!(
            Decimal::from_str(summary["fees"].as_str().ok_or("portfolio fees")?)?
                > Decimal::ZERO,
            "executed fills must project nonzero fees"
        );

    let schedule = call(&client, "collection_schedule", serde_json::json!({
        "action":"create", "request":{
            "request_id":"closure-schedule", "markets":["KRW-BTC"], "interval":"m1",
            "lookback_bars":5, "cadence_seconds":60,
            "freshness_policy":{"grace_seconds":30, "source_delay_seconds":30, "consecutive_gap_threshold":2},
            "retry":{"max_retries":1, "backoff_seconds":60}
        }
    })).await?;
    let schedule_id = schedule["id"]
        .as_str()
        .ok_or("schedule omitted id")?
        .to_owned();
    for field in [
        "failure_class",
        "recovery_state",
        "recovery_attempt_count",
        "last_probe_at",
        "last_recovery_at",
        "pending_gap",
        "backfill_job_id",
        "next_recovery_at",
    ] {
        assert!(schedule.get(field).is_some(), "schedule omitted {field}");
    }
    let recovered_schedule = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let record = call(
                &client,
                "collection_schedule",
                serde_json::json!({"action":"get", "schedule_id":schedule_id.clone()}),
            )
            .await?;
            if record["last_success_dataset_id"].is_string() {
                return Ok::<_, Box<dyn std::error::Error>>(record);
            }
            if record["status"] == "blocked" {
                return Err(format!("schedule blocked during HTTP acceptance: {record}").into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| "schedule did not publish a completed collection within 20 seconds")??;
    assert_eq!(recovered_schedule["recovery_state"], "active");
    let freshness = call(
        &client,
        "collection_schedule",
        serde_json::json!({
            "action":"freshness", "schedule_id":schedule_id.clone(), "probe_source":false
        }),
    )
    .await?;
    assert_eq!(freshness["schedule_id"], schedule_id);
    assert!(
        freshness["markets"]
            .as_array()
            .is_some_and(|markets| markets.len() == 1)
    );
    call(
        &client,
        "collection_schedule",
        serde_json::json!({
            "action":"pause", "schedule_id":schedule_id
        }),
    )
    .await?;

        let unused_policy = |index: usize| {
            swept["revisions"][index]["reference"]["policy_id"]
                .as_str()
                .ok_or("sweep revision omitted policy id")
        };
        let first_preview = call(
            &client,
            "resource_delete_preview",
            serde_json::json!({"kind":"policy", "policy_id":unused_policy(0)?}),
        )
        .await?;
        let second_preview = call(
            &client,
            "resource_delete_preview",
            serde_json::json!({"kind":"policy", "policy_id":unused_policy(1)?}),
        )
        .await?;
        let deleted = call(
            &client,
            "storage_maintenance",
            serde_json::json!({
                "action":"hard_delete_batch", "requests":[
                    {"preview":first_preview, "cascade":false},
                    {"preview":second_preview, "cascade":false}
                ]
            }),
        )
        .await?;
        assert_eq!(deleted["outcomes"].as_array().map(Vec::len), Some(2));
    let checkpoint = call(
        &client,
        "storage_maintenance",
        serde_json::json!({
            "action":"checkpoint", "mode":"TRUNCATE"
        }),
        )
        .await?;
        let outcome = &checkpoint["outcome"];
        if outcome["busy"] == true {
            assert_eq!(outcome["busy_reason"], "SQLITE_BUSY");
        } else {
            assert!(outcome["busy_reason"].is_null());
            assert_eq!(outcome["after"]["wal_bytes"], 0);
        }
    let compact = call(
        &client,
        "storage_maintenance",
        serde_json::json!({"action":"compact"}),
    )
    .await?;
    assert_eq!(compact["outcome"]["integrity"], "ok");
    Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let client_result = client.cancel().await;
    mcp.stop.cancel();
    let mcp_result = mcp.task.await;
    let runtime_result = runtime.shutdown().await;
    let _ignored = fixture.stop.send(());
    let fixture_result = fixture.task.await;
    let owner_result = owner.shutdown();
    outcome?;
    client_result?;
    mcp_result??;
    runtime_result?;
    fixture_result??;
    owner_result?;
    Ok(())
}
