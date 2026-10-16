use super::{LabMcpService, McpExposure, result_text};
use crate::contracts::{
    AssetQuantity, AutomationAccountDayBaselineInput, AutomationAssetValuationBasis,
    AutomationBaselineAssetInput, AutomationBaselineSource, AutomationOrderIdentifier,
    AutomationRiskPolicy, AutomationSessionId, AutomationSessionMode, AutomationSessionWrite,
    CandleInterval, ContentHash, LabError, MarketId, PolicyId, PolicyRevisionId, PolicyRevisionRef,
    PolicyWrite, QuoteAmount, RequestId, UtcTimestamp,
};
use crate::database::DatabaseOwner;
use crate::jobs::JobRuntime;
use crate::market_data::UpbitClient;
use crate::oauth::OAuthPermissions;
use crate::upbit::UpbitExchangeService;
use crate::upbit::client::UpbitPrivateClient;
use crate::upbit::execution::LiveExecutionPolicy;
use axum::body::Body;
use axum::http::{Method, Request, Response, StatusCode, Uri, header};
use axum::middleware::{Next, from_fn};
use axum::routing::any;
use axum::{Router, extract::State};
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, CallToolResult, JsonObject, ListToolsResult, object};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rust_decimal::Decimal;
use std::error::Error;
use std::io;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TEST_RESOURCE_METADATA_URL: &str =
    "https://mcp.example.test/.well-known/oauth-protected-resource";

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
    fn new() -> Result<Self, io::Error> {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-mcp-trading-{}-{}",
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

#[derive(Clone, Default)]
struct PrivateApiFixture {
    requests: Arc<Mutex<Vec<(String, String)>>>,
}

impl PrivateApiFixture {
    fn requests(&self) -> Vec<(String, String)> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct LocalHttpServer {
    url: String,
    stop: CancellationToken,
    task: JoinHandle<Result<(), io::Error>>,
}

impl LocalHttpServer {
    async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        self.stop.cancel();
        self.task.await??;
        Ok(())
    }
}

async fn start_private_api_fixture() -> Result<(LocalHttpServer, PrivateApiFixture), io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let fixture = PrivateApiFixture::default();
    let app = Router::new()
        .fallback(any(private_api_fixture))
        .with_state(fixture.clone());
    let stop = CancellationToken::new();
    let graceful = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(graceful.cancelled_owned())
            .await
    });
    Ok((
        LocalHttpServer {
            url: format!("http://{address}"),
            stop,
            task,
        },
        fixture,
    ))
}

async fn private_api_fixture(
    State(fixture): State<PrivateApiFixture>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    if uri.path() == "/v1/accounts"
        || uri.path() == "/v1/order"
        || uri.path().starts_with("/v1/orders")
    {
        fixture
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((method.to_string(), uri.to_string()));
    }

    if method != Method::GET {
        return json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            &serde_json::json!({"error":"method not allowed"}),
        );
    }
    match uri.path() {
        "/v1/accounts" => json_response(
            StatusCode::OK,
            &serde_json::json!([{
                "currency":"KRW", "balance":"125000.25", "locked":"500.00",
                "avg_buy_price":"1", "avg_buy_price_modified":false, "unit_currency":"KRW"
            }]),
        ),
        "/v1/orders/open" => json_response(StatusCode::OK, &serde_json::json!([])),
        "/v1/orders/closed" => json_response(
            StatusCode::OK,
            &serde_json::json!([
                history_order("fixture-filled", "done", "0.010", "0", "0.010", 1),
                history_order(
                    "fixture-partial-cancel",
                    "cancel",
                    "0.020",
                    "0.010",
                    "0.010",
                    1
                ),
                history_order("fixture-empty-cancel", "cancel", "0.020", "0.020", "0", 0),
            ]),
        ),
        "/v1/order" => {
            let uuid = uri
                .query()
                .unwrap_or_default()
                .split('&')
                .find_map(|pair| pair.strip_prefix("uuid="))
                .map(str::to_owned);
            let order = match uuid.as_deref() {
                Some("fixture-filled") => Some(history_order(
                    "fixture-filled",
                    "done",
                    "0.010",
                    "0",
                    "0.010",
                    1,
                )),
                Some("fixture-partial-cancel") => Some(history_order(
                    "fixture-partial-cancel",
                    "cancel",
                    "0.020",
                    "0.010",
                    "0.010",
                    1,
                )),
                Some("fixture-empty-cancel") => Some(history_order(
                    "fixture-empty-cancel",
                    "cancel",
                    "0.020",
                    "0.020",
                    "0",
                    0,
                )),
                _ => None,
            };
            order.map_or_else(
                || {
                    json_response(
                        StatusCode::NOT_FOUND,
                        &serde_json::json!({"error":"order not found"}),
                    )
                },
                |order| json_response(StatusCode::OK, &order),
            )
        }
        _ => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error":"unknown fixture path"}),
        ),
    }
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(value.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        "remaining-req",
        axum::http::HeaderValue::from_static("group=default; min=1800; sec=9"),
    );
    response
}

fn history_order(
    uuid: &str,
    state: &str,
    volume: &str,
    remaining_volume: &str,
    executed_volume: &str,
    trades_count: u64,
) -> serde_json::Value {
    let funds = if trades_count == 0 { "0" } else { "1000" };
    serde_json::json!({
        "uuid": uuid,
        "identifier": format!("identifier-{uuid}"),
        "side": "bid",
        "ord_type": "limit",
        "price": "100000",
        "state": state,
        "market": "KRW-BTC",
        "created_at": "2026-10-14T00:00:00Z",
        "volume": volume,
        "remaining_volume": remaining_volume,
        "reserved_fee": "0.5",
        "remaining_fee": "0",
        "paid_fee": if trades_count == 0 { "0" } else { "0.5" },
        "locked": "0",
        "executed_volume": executed_volume,
        "trades_count": trades_count,
        "trades": if trades_count == 0 {
            serde_json::json!([])
        } else {
            serde_json::json!([{
                "uuid": format!("trade-{uuid}"),
                "market": "KRW-BTC",
                "side": "bid",
                "price": "100000",
                "volume": executed_volume,
                "funds": funds,
                "created_at": "2026-10-14T00:00:01Z"
            }])
        }
    })
}

fn exact_decimal(value: &serde_json::Value) -> Result<Decimal, rust_decimal::Error> {
    let source = value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned);
    source.parse()
}

async fn insert_fixture_permission(mut request: Request<Body>, next: Next) -> Response<Body> {
    // This test substitutes only the OAuth validator. RMCP's HTTP service and
    // RequestContext propagation remain real; an unknown token deliberately
    // reaches RMCP without a per-request OAuthPermissions extension.
    let permissions = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| match token {
            "research-fixture" => Some(OAuthPermissions::Research),
            "read-fixture" => Some(OAuthPermissions::TradingRead),
            "control-fixture" => Some(OAuthPermissions::TradingControl),
            _ => None,
        });
    if let Some(permissions) = permissions {
        request.extensions_mut().insert(permissions);
    }
    next.run(request).await
}

async fn start_mcp_http_fixture(service: LabMcpService) -> Result<LocalHttpServer, io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let stop = CancellationToken::new();
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_cancellation_token(stop.child_token());
    let transport = StreamableHttpService::new(
        move || Ok(service.clone()),
        LocalSessionManager::default().into(),
        config,
    );
    let app = Router::new()
        .nest_service("/mcp", transport)
        .route_layer(from_fn(insert_fixture_permission));
    let graceful = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(graceful.cancelled_owned())
            .await
    });
    Ok(LocalHttpServer {
        url: format!("http://{address}/mcp"),
        stop,
        task,
    })
}

type McpClient = RunningService<RoleClient, ()>;

async fn connect_mcp(url: &str, token: Option<&str>) -> Result<McpClient, Box<dyn Error>> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned());
    if let Some(token) = token {
        config = config.auth_header(token);
    }
    Ok(
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await?,
    )
}

async fn call_tool(
    client: &McpClient,
    name: &str,
    arguments: serde_json::Value,
) -> Result<CallToolResult, Box<dyn Error>> {
    let mut request = CallToolRequestParams::default();
    request.name = name.to_owned().into();
    request.arguments = Some(object(arguments));
    Ok(client.call_tool(request).await?)
}

fn tool_json(result: CallToolResult) -> Result<serde_json::Value, Box<dyn Error>> {
    let is_error = result.is_error;
    let text = result_text(result);
    if is_error == Some(true) {
        return Err(format!("unexpected MCP tool error: {text}").into());
    }
    Ok(serde_json::from_str(&text)?)
}

fn assert_tool_denied(result: CallToolResult, expected: &str) {
    assert_eq!(result.is_error, Some(true), "expected an MCP tool error");
    let text = result_text(result);
    assert!(text.contains(expected), "unexpected denial: {text}");
}

fn assert_scope_challenge(result: CallToolResult, required_scope: &str) {
    assert_eq!(result.is_error, Some(true), "expected a tool-level error");
    let wire = serde_json::to_value(&result).expect("serialize MCP tool result");
    let challenges = wire["_meta"]["mcp/www_authenticate"]
        .as_array()
        .expect("OAuth denial must carry an MCP WWW-Authenticate challenge");
    assert_eq!(challenges.len(), 1);
    let challenge = challenges[0]
        .as_str()
        .expect("WWW-Authenticate challenge must be a string");
    assert!(challenge.starts_with(&format!(
        "Bearer resource_metadata=\"{TEST_RESOURCE_METADATA_URL}\", error=\"insufficient_scope\""
    )));
    assert!(challenge.contains("error_description=\""));
    assert!(
        challenge.contains(&format!("scope=\"{required_scope}\"")),
        "missing required scope in challenge: {challenge}"
    );
    assert!(result_text(result).contains("INSUFFICIENT_SCOPE"));
}

fn assert_tool_security_schemes(listed: &ListToolsResult, name: &str, expected_scopes: &[&str]) {
    let tool = listed
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == name)
        .unwrap_or_else(|| panic!("tool {name} missing from catalog"));
    let wire = serde_json::to_value(tool).expect("serialize listed tool descriptor");
    assert_eq!(
        &wire["_meta"]["securitySchemes"],
        &serde_json::json!([{ "type": "oauth2", "scopes": expected_scopes }]),
        "unexpected OAuth scopes for {name}"
    );
}

fn missing_policy_ref() -> Result<PolicyRevisionRef, Box<dyn Error>> {
    Ok(PolicyRevisionRef {
        policy_id: PolicyId::new("missing-scope-policy")?,
        revision_id: PolicyRevisionId::new("missing-scope-revision")?,
        definition_digest: ContentHash::of_bytes(b"intentionally absent scope-test policy"),
    })
}

fn typed_start(
    request_id: RequestId,
    policy_ref: PolicyRevisionRef,
) -> Result<AutomationSessionWrite, Box<dyn Error>> {
    Ok(AutomationSessionWrite::Start {
        request_id,
        policy_ref,
        market: MarketId::parse_upbit("KRW-BTC")?,
        decision_interval: CandleInterval::M1,
        mode: AutomationSessionMode::Mock,
        risk: AutomationRiskPolicy {
            activation_loss_limit_krw: None,
            max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_open_orders: 1,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        },
    })
}

fn write_object(write: &AutomationSessionWrite) -> Result<JsonObject, Box<dyn Error>> {
    Ok(object(serde_json::to_value(write)?))
}

async fn automation_request_readback(
    client: &McpClient,
    request_id: &RequestId,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let response = call_tool(
        client,
        "automation_session_query",
        serde_json::json!({"action":"request", "request_id":request_id.as_str()}),
    )
    .await?;
    tool_json(response)
}

async fn automation_session_get(
    client: &McpClient,
    session_id: &AutomationSessionId,
) -> Result<serde_json::Value, Box<dyn Error>> {
    tool_json(
        call_tool(
            client,
            "automation_session_query",
            serde_json::json!({"action":"get", "session_id":session_id.as_str()}),
        )
        .await?,
    )
}

async fn assert_automation_lifecycle_readback(
    client: &McpClient,
    request_id: &RequestId,
    action: &str,
    sequence: u64,
    state: &str,
) -> Result<(), Box<dyn Error>> {
    let readback = automation_request_readback(client, request_id).await?;
    assert_eq!(readback["state"], "APPLIED");
    assert_eq!(readback["response"]["receipt"]["action"], action);
    assert_eq!(readback["response"]["receipt"]["outcome"], "APPLIED");
    assert_eq!(readback["response"]["receipt"]["sequence"], sequence);
    assert_eq!(readback["response"]["session"]["state"], state);
    assert_eq!(readback["response"]["session"]["sequence"], sequence);
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one local Streamable HTTP journey proves OAuth scope propagation and its history/effect boundary"
)]
async fn streamable_http_oauth_scopes_gate_private_history_and_durable_controls()
-> Result<(), Box<dyn Error>> {
    install_crypto_provider()?;
    let root = TempRoot::new()?;
    let (private_server, private_fixture) = start_private_api_fixture().await?;
    let database = DatabaseOwner::open(root.0.clone())?;
    let (_, _, mut definition) = crate::contracts::builtin_policy_definitions()?
        .into_iter()
        .next()
        .ok_or("repository omitted its built-in policy fixture")?;
    definition.name = "OAuth MCP lifecycle transport fixture".into();
    let policy_write = PolicyWrite::Create {
        request_id: RequestId::new("oauth-mcp-lifecycle-policy-create")?,
        definition,
    };
    let lifecycle_policy = database
        .handle()
        .call("mcp_oauth_lifecycle_policy_fixture", move |store| {
            store.write_policy(&policy_write, UtcTimestamp::now())
        })
        .await?;
    let upbit = UpbitClient::synthetic_local(&private_server.url)?;
    let runtime = JobRuntime::start(database.handle(), upbit.clone(), root.0.clone(), None).await?;
    let private_client =
        UpbitPrivateClient::loopback_fixture(&private_server.url, LiveExecutionPolicy::Disabled)?;
    let exchange = UpbitExchangeService::from_fixture_client(private_client);
    let service = LabMcpService::new_with_exchange(
        database.handle(),
        upbit,
        runtime.service(),
        McpExposure::OAuth,
        exchange,
    )
    .with_remote_trading_oauth(TEST_RESOURCE_METADATA_URL.to_owned());
    let mcp_server = start_mcp_http_fixture(service).await?;

    let research = connect_mcp(&mcp_server.url, Some("research-fixture")).await?;
    let unscoped = connect_mcp(&mcp_server.url, Some("missing-context-fixture")).await?;
    let read = connect_mcp(&mcp_server.url, Some("read-fixture")).await?;
    let control = connect_mcp(&mcp_server.url, Some("control-fixture")).await?;

    let assertions = async {
        let catalog = research.peer().list_tools(None).await?;
        assert!(
            catalog.tools.iter().all(|tool| tool
                .meta
                .as_ref()
                .is_some_and(|meta| meta.0.contains_key("securitySchemes"))),
            "every OAuth tool must declare its security scheme in the compatibility metadata"
        );
        assert_tool_security_schemes(&catalog, "probe_upbit", &["mcp"]);
        assert_tool_security_schemes(&catalog, "upbit_account_balances", &["mcp", "trading:read"]);
        assert_tool_security_schemes(
            &catalog,
            "automation_session_write",
            &["mcp", "trading:read", "trading:control"],
        );
        assert_tool_security_schemes(
            &catalog,
            "trading_control",
            &["mcp", "trading:read", "trading:control"],
        );
        assert_tool_security_schemes(
            &catalog,
            "live_order_control",
            &["mcp", "trading:read", "trading:control"],
        );

        let status = call_tool(&research, "lab_status", serde_json::json!({})).await?;
        assert_ne!(
            status.is_error,
            Some(true),
            "research scope should retain research tools"
        );
        let research_private =
            call_tool(&research, "upbit_account_balances", serde_json::json!({})).await?;
        assert_scope_challenge(research_private, "mcp trading:read");
        let research_control = call_tool(
            &research,
            "trading_control",
            serde_json::json!({"action":"status"}),
        )
        .await?;
        assert_scope_challenge(research_control, "mcp trading:read");
        let research_mutation =
            call_tool(&research, "automation_session_write", serde_json::json!({})).await?;
        assert_scope_challenge(research_mutation, "mcp trading:read trading:control");
        assert!(
            private_fixture.requests().is_empty(),
            "scope challenges must be returned before private provider access"
        );

        let missing_context =
            call_tool(&unscoped, "upbit_account_balances", serde_json::json!({})).await?;
        assert_scope_challenge(missing_context, "mcp trading:read");

        let accounts =
            tool_json(call_tool(&read, "upbit_account_balances", serde_json::json!({})).await?)?;
        assert_eq!(accounts["mode"], "LIVE_HTTP_READ");
        assert_eq!(accounts["accounts_count"], 1);
        assert_eq!(accounts["balances"][0]["currency"], "KRW");

        let open = tool_json(call_tool(&read, "upbit_orders_open", serde_json::json!({})).await?)?;
        assert_eq!(open["exchange_read_mode"], "LIVE_READ");
        assert_eq!(open["records"], serde_json::json!([]));

        let closed =
            tool_json(call_tool(&read, "upbit_orders_closed", serde_json::json!({})).await?)?;
        assert_eq!(closed["exchange_read_mode"], "LIVE_READ");
        let records = closed["records"]
            .as_array()
            .ok_or("closed history page is not an array")?;
        assert_eq!(records.len(), 3);
        let status_and_fill = records
            .iter()
            .map(|record| {
                let state = record["state"]
                    .as_str()
                    .ok_or("closed order record omitted state")?
                    .to_owned();
                Ok::<_, Box<dyn Error>>((state, exact_decimal(&record["executed_volume"])?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            status_and_fill,
            [
                ("done".to_owned(), Decimal::new(10, 3)),
                ("cancel".to_owned(), Decimal::new(10, 3)),
                ("cancel".to_owned(), Decimal::ZERO),
            ],
            "history fixture retains filled, partially filled/cancelled, and empty-cancel rows"
        );
        assert_eq!(records[1]["trades_count"], 1);
        assert_eq!(
            exact_decimal(&records[1]["paid_fee"])?,
            Decimal::new(5, 1),
            "partial cancellation retains paid-fee history"
        );
        assert_eq!(
            records[1]["trades"][0]["uuid"],
            "trade-fixture-partial-cancel"
        );
        assert_eq!(records[2]["trades_count"], 0);

        let order = tool_json(
            call_tool(
                &read,
                "upbit_order_get",
                serde_json::json!({"uuid":"fixture-filled"}),
            )
            .await?,
        )?;
        assert_eq!(order["uuid"], "fixture-filled");
        assert_eq!(order["state"], "done");
        assert_eq!(order["trades_count"], 1);

        let sessions = call_tool(
            &read,
            "automation_session_query",
            serde_json::json!({"action":"list", "offset":0, "limit":10}),
        )
        .await?;
        assert_ne!(
            sessions.is_error,
            Some(true),
            "trading:read should permit session history"
        );

        let before_control = tool_json(
            call_tool(
                &read,
                "trading_control",
                serde_json::json!({"action":"status"}),
            )
            .await?,
        )?;
        let provider_reads_before_read_mutations = private_fixture.requests();
        let read_start_id = RequestId::new("oauth-read-start-denied")?;
        let read_start = write_object(&typed_start(read_start_id.clone(), missing_policy_ref()?)?)?;
        let denied_start = call_tool(
            &read,
            "automation_session_write",
            serde_json::Value::Object(read_start),
        )
        .await?;
        assert_scope_challenge(denied_start, "mcp trading:read trading:control");
        let request_readback = automation_request_readback(&read, &read_start_id).await?;
        assert_eq!(request_readback["state"], "NOT_RECORDED");
        let sessions_after_denied_start = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({"action":"list", "offset":0, "limit":10}),
            )
            .await?,
        )?;
        assert_eq!(sessions_after_denied_start["total_count"], 0);

        let denied_pause = call_tool(
            &read,
            "trading_control",
            serde_json::json!({
                "action":"pause",
                "request_id":"oauth-read-control-denied",
                "expected_sequence":before_control["sequence"]
            }),
        )
        .await?;
        assert_scope_challenge(denied_pause, "mcp trading:read trading:control");
        let after_denied_pause = tool_json(
            call_tool(
                &read,
                "trading_control",
                serde_json::json!({"action":"status"}),
            )
            .await?,
        )?;
        assert_eq!(after_denied_pause["sequence"], before_control["sequence"]);
        assert_eq!(after_denied_pause["state"], before_control["state"]);
        assert_eq!(
            private_fixture.requests(),
            provider_reads_before_read_mutations,
            "read-scope mutations must be denied before any private provider request"
        );

        let control_start_id = RequestId::new("oauth-control-start-missing-policy")?;
        let control_start = write_object(&typed_start(
            control_start_id.clone(),
            missing_policy_ref()?,
        )?)?;
        let start_result = call_tool(
            &control,
            "automation_session_write",
            serde_json::Value::Object(control_start),
        )
        .await?;
        assert_eq!(start_result.is_error, Some(true));
        assert!(
            start_result.meta.is_none(),
            "a granted control scope must not receive an auth challenge"
        );
        assert!(
            !result_text(start_result).contains("explicitly scoped OAuth trading grant"),
            "trading:control should pass scope admission and reach lifecycle policy validation"
        );
        let rejected_start = automation_request_readback(&control, &control_start_id).await?;
        assert_eq!(rejected_start["state"], "REJECTED");
        let sessions_after_rejected_start = tool_json(
            call_tool(
                &control,
                "automation_session_query",
                serde_json::json!({"action":"list", "offset":0, "limit":10}),
            )
            .await?,
        )?;
        assert_eq!(
            sessions_after_rejected_start["items"]
                .as_array()
                .map(Vec::len),
            Some(0),
            "invalid policy fixture must not create or start an automation session"
        );

        let lifecycle_start_id = RequestId::new("oauth-control-lifecycle-start")?;
        let lifecycle_start = typed_start(
            lifecycle_start_id.clone(),
            lifecycle_policy.snapshot.reference.clone(),
        )?;
        let lifecycle_start_args = write_object(&lifecycle_start)?;
        let mut invalid_start = serde_json::Value::Object(lifecycle_start_args.clone());
        invalid_start["request_id"] = serde_json::json!("invalid-risk-start");
        invalid_start["risk"]["max_orders_per_interval"] = serde_json::json!(0);
        let invalid_result = call_tool(&control, "automation_session_write", invalid_start).await?;
        assert_tool_denied(invalid_result, "risk limits");
        let no_session = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({"action":"list", "offset":0, "limit":10}),
            )
            .await?,
        )?;
        assert_eq!(
            no_session["total_count"], 0,
            "invalid risk must not create a session"
        );
        let started = tool_json(
            call_tool(
                &control,
                "automation_session_write",
                serde_json::Value::Object(lifecycle_start_args.clone()),
            )
            .await?,
        )?;
        assert_eq!(started["receipt"]["action"], "START");
        assert_eq!(started["receipt"]["outcome"], "APPLIED");
        assert_eq!(started["session"]["mode"], "MOCK");
        assert_eq!(started["session"]["state"], "RUNNING");
        let start_sequence = started["session"]["sequence"]
            .as_u64()
            .ok_or("automation start omitted its sequence")?;
        assert_eq!(started["receipt"]["sequence"], start_sequence);
        let session_id = AutomationSessionId::new(
            started["session"]["session_id"]
                .as_str()
                .ok_or("automation start omitted its session ID")?,
        )?;
        assert_automation_lifecycle_readback(
            &control,
            &lifecycle_start_id,
            "START",
            start_sequence,
            "RUNNING",
        )
        .await?;
        let started_durable = automation_session_get(&control, &session_id).await?;
        assert_eq!(started_durable["state"], "RUNNING");
        assert_eq!(started_durable["sequence"], start_sequence);
        for field in ["policy_ref", "market", "decision_interval", "mode", "risk"] {
            assert_eq!(
                started_durable[field], lifecycle_start_args[field],
                "MCP must preserve frozen start field {field}"
            );
        }
        assert!(
            started_durable["risk"]["activation_loss_limit_krw"].is_null(),
            "MOCK preserves absent activation limit"
        );

        let research_review = call_tool(
            &research,
            "automation_session_query",
            serde_json::json!({
                "action":"decisions", "session_id":session_id.as_str(), "offset":0, "limit":10
            }),
        )
        .await?;
        assert_scope_challenge(research_review, "mcp trading:read");
        let decisions = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({
                    "action":"decisions", "session_id":session_id.as_str(), "offset":0, "limit":10
                }),
            )
            .await?,
        )?;
        assert_eq!(decisions["session_id"], session_id.as_str());
        let performance = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({"action":"performance", "session_id":session_id.as_str()}),
            )
            .await?,
        )?;
        assert_eq!(performance["session"]["session_id"], session_id.as_str());
        let manifest = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({"action":"export_manifest", "session_id":session_id.as_str()}),
            )
            .await?,
        )?;
        assert_eq!(manifest["session_id"], session_id.as_str());
        let file = manifest["files"][0]["file"].clone();
        let chunk = tool_json(
            call_tool(
                &read,
                "automation_session_query",
                serde_json::json!({
                    "action":"export_read",
                    "session_id":session_id.as_str(),
                    "file":file,
                    "offset":0,
                    "expected_sha256":manifest["files"][0]["sha256"],
                    "expected_snapshot_digest":manifest["snapshot_digest"],
                }),
            )
            .await?,
        )?;
        assert_eq!(chunk["session_id"], session_id.as_str());

        let replayed_start = tool_json(
            call_tool(
                &control,
                "automation_session_write",
                serde_json::Value::Object(lifecycle_start_args),
            )
            .await?,
        )?;
        assert_eq!(replayed_start["receipt"]["outcome"], "REPLAYED");
        assert_eq!(replayed_start["session"]["session_id"], session_id.as_str());
        assert_eq!(replayed_start["session"]["sequence"], start_sequence);
        let sessions_after_replay = tool_json(
            call_tool(
                &control,
                "automation_session_query",
                serde_json::json!({"action":"list", "offset":0, "limit":10}),
            )
            .await?,
        )?;
        assert_eq!(sessions_after_replay["total_count"], 1);
        assert_eq!(
            sessions_after_replay["items"][0]["session_id"],
            session_id.as_str()
        );

        let read_pause_id = RequestId::new("oauth-read-lifecycle-pause-denied")?;
        let denied_lifecycle_pause = call_tool(
            &read,
            "automation_session_write",
            serde_json::to_value(AutomationSessionWrite::Pause {
                request_id: read_pause_id.clone(),
                session_id: session_id.clone(),
                expected_sequence: start_sequence,
            })?,
        )
        .await?;
        assert_scope_challenge(denied_lifecycle_pause, "mcp trading:read trading:control");
        assert_eq!(
            automation_request_readback(&read, &read_pause_id).await?["state"],
            "NOT_RECORDED"
        );
        let after_read_pause_denial = automation_session_get(&read, &session_id).await?;
        assert_eq!(after_read_pause_denial["state"], "RUNNING");
        assert_eq!(after_read_pause_denial["sequence"], start_sequence);
        assert_eq!(
            private_fixture.requests(),
            provider_reads_before_read_mutations,
            "read-scope lifecycle denial must precede provider access"
        );

        let pause_request_id = RequestId::new("oauth-control-lifecycle-pause")?;
        let paused = tool_json(
            call_tool(
                &control,
                "automation_session_write",
                write_object(&AutomationSessionWrite::Pause {
                    request_id: pause_request_id.clone(),
                    session_id: session_id.clone(),
                    expected_sequence: start_sequence,
                })
                .map(serde_json::Value::Object)?,
            )
            .await?,
        )?;
        assert_eq!(paused["receipt"]["action"], "PAUSE");
        assert_eq!(paused["receipt"]["outcome"], "APPLIED");
        assert_eq!(paused["session"]["state"], "PAUSED");
        let pause_sequence = paused["session"]["sequence"]
            .as_u64()
            .ok_or("automation pause omitted its sequence")?;
        assert_eq!(paused["receipt"]["sequence"], pause_sequence);
        assert_automation_lifecycle_readback(
            &control,
            &pause_request_id,
            "PAUSE",
            pause_sequence,
            "PAUSED",
        )
        .await?;
        let paused_durable = automation_session_get(&control, &session_id).await?;
        assert_eq!(paused_durable["state"], "PAUSED");
        assert_eq!(paused_durable["sequence"], pause_sequence);

        let stop_request_id = RequestId::new("oauth-control-lifecycle-stop")?;
        let stopped = tool_json(
            call_tool(
                &control,
                "automation_session_write",
                write_object(&AutomationSessionWrite::Stop {
                    request_id: stop_request_id.clone(),
                    session_id: session_id.clone(),
                    expected_sequence: pause_sequence,
                })
                .map(serde_json::Value::Object)?,
            )
            .await?,
        )?;
        assert_eq!(stopped["receipt"]["action"], "STOP");
        assert_eq!(stopped["receipt"]["outcome"], "APPLIED");
        assert_eq!(stopped["session"]["state"], "STOPPED");
        let stop_sequence = stopped["session"]["sequence"]
            .as_u64()
            .ok_or("automation stop omitted its sequence")?;
        assert_eq!(stopped["receipt"]["sequence"], stop_sequence);
        assert_automation_lifecycle_readback(
            &control,
            &stop_request_id,
            "STOP",
            stop_sequence,
            "STOPPED",
        )
        .await?;
        let stopped_durable = automation_session_get(&control, &session_id).await?;
        assert_eq!(stopped_durable["state"], "STOPPED");
        assert_eq!(stopped_durable["sequence"], stop_sequence);

        let stop_request = serde_json::json!({
            "action":"emergency_stop",
            "request_id":"oauth-control-emergency-stop",
            "expected_sequence":before_control["sequence"]
        });
        let emergency_stop =
            tool_json(call_tool(&control, "trading_control", stop_request.clone()).await?)?;
        assert_eq!(emergency_stop["live_trading_enabled"], false);
        assert_eq!(emergency_stop["order_admission_blocked"], true);
        assert_eq!(
            emergency_stop["receipt"]["request_id"],
            "oauth-control-emergency-stop"
        );
        assert_eq!(emergency_stop["receipt"]["sequence"], 1);
        let replayed_stop = tool_json(call_tool(&control, "trading_control", stop_request).await?)?;
        assert_eq!(
            replayed_stop["receipt"]["request_id"],
            "oauth-control-emergency-stop"
        );
        assert_eq!(
            replayed_stop["receipt"]["sequence"],
            emergency_stop["receipt"]["sequence"]
        );
        assert_eq!(replayed_stop["sequence"], emergency_stop["sequence"]);

        let before_denied_effects = private_fixture.requests();
        for (name, args) in [
            ("upbit_order_execute", serde_json::json!({})),
            (
                "upbit_order_test",
                serde_json::json!({
                    "market":"KRW-BTC", "side":"bid", "ord_type":"limit",
                    "volume":"0.010", "price":"100000", "identifier":"oauth-denied-test"
                }),
            ),
            (
                "upbit_order_cancel",
                serde_json::json!({"order_id":"fixture-filled", "unfilled_volume":"0.010"}),
            ),
        ] {
            let denied = call_tool(&control, name, args).await?;
            assert_tool_denied(denied, "require the native desktop capability");
        }

        let remote_session = AutomationSessionId::new("scope-test-session")?;
        let baseline_request_id = RequestId::new("oauth-control-baseline-denied")?;
        let baseline = AutomationSessionWrite::RecordBaseline {
            request_id: baseline_request_id.clone(),
            session_id: remote_session.clone(),
            expected_sequence: 0,
            baseline: AutomationAccountDayBaselineInput {
                account_identity: "mock:upbit-default".into(),
                day_utc: "2026-10-15".into(),
                reference_time_utc: UtcTimestamp::parse_rfc3339("2026-10-15T00:00:00Z")?,
                source: AutomationBaselineSource::OperatorAttested,
                assets: vec![AutomationBaselineAssetInput {
                    currency: "KRW".into(),
                    free: AssetQuantity::new(Decimal::ZERO)?,
                    locked: AssetQuantity::new(Decimal::ZERO)?,
                    valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                    mark_price_krw: None,
                }],
                operator_note: None,
            },
        };
        let denied_baseline = call_tool(
            &control,
            "automation_session_write",
            serde_json::Value::Object(write_object(&baseline)?),
        )
        .await?;
        assert_tool_denied(denied_baseline, "require Desktop authority");
        assert_eq!(
            automation_request_readback(&control, &baseline_request_id).await?["state"],
            "NOT_RECORDED"
        );

        let cancel_request_id = RequestId::new("oauth-control-order-cancel-denied")?;
        let cancel = AutomationSessionWrite::CancelOrder {
            request_id: cancel_request_id.clone(),
            session_id: remote_session,
            expected_sequence: 0,
            client_identifier: AutomationOrderIdentifier::new("scope-test-order")?,
        };
        let denied_automation_cancel = call_tool(
            &control,
            "automation_session_write",
            serde_json::Value::Object(write_object(&cancel)?),
        )
        .await?;
        assert_tool_denied(denied_automation_cancel, "require Desktop authority");
        assert_eq!(
            automation_request_readback(&control, &cancel_request_id).await?["state"],
            "NOT_RECORDED"
        );

        assert_eq!(
            private_fixture.requests(),
            before_denied_effects,
            "OAuth control must not reach order execute/test/cancel provider endpoints"
        );
        assert!(
            private_fixture
                .requests()
                .iter()
                .all(|(method, _)| method == "GET"),
            "this HTTP journey must issue only authenticated private reads to the loopback fixture"
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let research_cancel = research.cancel().await;
    let unscoped_cancel = unscoped.cancel().await;
    let read_cancel = read.cancel().await;
    let control_cancel = control.cancel().await;
    let mcp_result = mcp_server.shutdown().await;
    let runtime_result = runtime.shutdown().await;
    let private_result = private_server.shutdown().await;
    let owner_result = database.shutdown();

    assertions?;
    research_cancel?;
    unscoped_cancel?;
    read_cancel?;
    control_cancel?;
    mcp_result?;
    runtime_result?;
    private_result?;
    owner_result?;
    Ok(())
}

#[tokio::test]
async fn managed_live_send_switch_requires_scoped_control_and_reports_current_state()
-> Result<(), Box<dyn Error>> {
    install_crypto_provider()?;
    let root = TempRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let gate = crate::upbit::execution::LiveOrderSendGate::new_disabled();
    let exchange = UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::OperatorEnabled,
        gate.clone(),
    );
    let upbit = UpbitClient::new()?;
    let runtime = JobRuntime::start(owner.handle(), upbit.clone(), root.0.clone(), None).await?;
    let service = LabMcpService::new_with_exchange(
        owner.handle(),
        upbit,
        runtime.service(),
        McpExposure::OAuth,
        exchange,
    )
    .with_remote_trading_oauth(TEST_RESOURCE_METADATA_URL.to_owned());
    let server = start_mcp_http_fixture(service).await?;
    let research = connect_mcp(&server.url, Some("research-fixture")).await?;
    let unscoped = connect_mcp(&server.url, Some("missing-context-fixture")).await?;
    let read = connect_mcp(&server.url, Some("read-fixture")).await?;
    let control = connect_mcp(&server.url, Some("control-fixture")).await?;
    let assertions = async {
        for client in [&research, &unscoped] {
            assert_scope_challenge(
                call_tool(client, "live_order_control", serde_json::json!({"action":"status"})).await?,
                "mcp trading:read",
            );
        }
        let initial = tool_json(call_tool(&read, "live_order_control", serde_json::json!({"action":"status"})).await?)?;
        assert_eq!(initial["live_order_control_available"], true);
        assert_eq!(initial["live_trading_enabled"], false);
        assert_eq!(initial["exchange_read_mode"], "MOCK");
        let enable = serde_json::json!({"action":"enable", "request_id":"scope-enable", "expected_sequence":initial["sequence"], "expected_epoch":initial["boot_epoch"], "confirmation":"ENABLE_LIVE_ORDER_SENDING"});
        assert_scope_challenge(
            call_tool(&read, "live_order_control", enable.clone()).await?,
            "mcp trading:read trading:control",
        );
        assert!(!gate.is_enabled());
        let enabled = tool_json(call_tool(&control, "live_order_control", enable.clone()).await?)?;
        assert_eq!(enabled["receipt"]["outcome"], "ACKNOWLEDGED");
        assert!(gate.is_enabled());
        let current = tool_json(call_tool(&read, "live_order_control", serde_json::json!({"action":"status"})).await?)?;
        assert_eq!(current["live_trading_enabled"], true);
        let admission = tool_json(call_tool(&read, "trading_control", serde_json::json!({"action":"status"})).await?)?;
        assert_eq!(admission["live_trading_enabled"], true, "legacy projection must read the shared runtime gate");
        assert_ne!(admission["state"], "RUNNING", "enabling sends must not start trading admission");
        let disabled = tool_json(call_tool(&control, "live_order_control", serde_json::json!({"action":"disable", "request_id":"scope-disable", "expected_sequence":current["sequence"], "expected_epoch":current["boot_epoch"]})).await?)?;
        assert_eq!(disabled["live_trading_enabled"], false);
        assert!(!gate.is_enabled());
        let historical = tool_json(call_tool(&read, "live_order_control", serde_json::json!({"action":"request", "request_id":"scope-enable"})).await?)?;
        assert_eq!(historical["receipt"]["outcome"], "ACKNOWLEDGED");
        assert_eq!(historical["live_trading_enabled"], false, "historical receipt cannot restore ON");
        let replay = call_tool(&control, "live_order_control", enable).await?;
        assert_ne!(replay.is_error, Some(true));
        assert!(!gate.is_enabled());
        let sessions = tool_json(call_tool(&read, "automation_session_query", serde_json::json!({"action":"list", "offset":0, "limit":10})).await?)?;
        assert_eq!(sessions["total_count"], 0, "gate enable must not create a strategy session");
        Ok::<(), Box<dyn Error>>(())
    }.await;
    research.cancel().await?;
    unscoped.cancel().await?;
    read.cancel().await?;
    control.cancel().await?;
    server.shutdown().await?;
    runtime.shutdown().await?;
    owner.shutdown()?;
    assertions
}

#[tokio::test]
async fn public_and_disabled_oauth_exposures_do_not_emit_scope_challenges()
-> Result<(), Box<dyn Error>> {
    install_crypto_provider()?;
    let root = TempRoot::new()?;
    let (private_server, private_fixture) = start_private_api_fixture().await?;
    let owner = DatabaseOwner::open(root.0.clone())?;
    let upbit = UpbitClient::synthetic_local(&private_server.url)?;
    let runtime = JobRuntime::start(owner.handle(), upbit.clone(), root.0.clone(), None).await?;
    let private_client =
        UpbitPrivateClient::loopback_fixture(&private_server.url, LiveExecutionPolicy::Disabled)?;
    let exchange = UpbitExchangeService::from_fixture_client(private_client);
    let disabled_oauth = LabMcpService::new_with_exchange(
        owner.handle(),
        upbit.clone(),
        runtime.service(),
        McpExposure::OAuth,
        exchange.clone(),
    );
    let public = LabMcpService::new_with_exchange(
        owner.handle(),
        upbit,
        runtime.service(),
        McpExposure::PublicNoAuth,
        exchange,
    );
    let disabled_server = start_mcp_http_fixture(disabled_oauth).await?;
    let public_server = start_mcp_http_fixture(public).await?;
    let research = connect_mcp(&disabled_server.url, Some("research-fixture")).await?;
    let anonymous = connect_mcp(&public_server.url, None).await?;

    let assertions = async {
        for (client, description) in [
            (&research, "OAuth trading disabled"),
            (&anonymous, "public no-auth exposure"),
        ] {
            let denied = call_tool(client, "upbit_account_balances", serde_json::json!({})).await?;
            assert_eq!(
                denied.is_error,
                Some(true),
                "{description} must deny private reads"
            );
            assert!(
                denied.meta.is_none(),
                "{description} must not trigger scope reauthorization"
            );
            assert_tool_denied(denied, "explicitly scoped OAuth trading grant");
        }
        assert!(
            private_fixture.requests().is_empty(),
            "disabled exposures must deny before private provider access"
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let research_cancel = research.cancel().await;
    let anonymous_cancel = anonymous.cancel().await;
    let disabled_result = disabled_server.shutdown().await;
    let public_result = public_server.shutdown().await;
    let runtime_result = runtime.shutdown().await;
    let private_result = private_server.shutdown().await;
    let owner_result = owner.shutdown();

    assertions?;
    research_cancel?;
    anonymous_cancel?;
    disabled_result?;
    public_result?;
    runtime_result?;
    private_result?;
    owner_result?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one native-desktop MCP journey covers policy denial, start admission, CAS, emergency latch and effect isolation"
)]
async fn native_desktop_trading_control_uses_trusted_attestation_without_order_effects()
-> Result<(), Box<dyn Error>> {
    install_crypto_provider()?;
    let root = TempRoot::new()?;
    let (private_server, private_fixture) = start_private_api_fixture().await?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let upbit = UpbitClient::synthetic_local(&private_server.url)?;
    let runtime = JobRuntime::start(database.clone(), upbit.clone(), root.0.clone(), None).await?;
    let jobs = runtime.service();

    let hard_off_gate = crate::upbit::execution::LiveOrderSendGate::new_disabled();
    let hard_off_exchange = UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::Disabled,
        hard_off_gate.clone(),
    );
    let hard_off_service = LabMcpService::new_with_exchange(
        database.clone(),
        upbit.clone(),
        jobs.clone(),
        McpExposure::Desktop,
        hard_off_exchange,
    );
    let hard_off_server = start_mcp_http_fixture(hard_off_service).await?;

    let gate = crate::upbit::execution::LiveOrderSendGate::new_disabled();
    let exchange = UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::OperatorEnabled,
        gate.clone(),
    );
    let service =
        LabMcpService::new_with_exchange(database, upbit, jobs, McpExposure::Desktop, exchange);
    let server = start_mcp_http_fixture(service).await?;
    let hard_off = connect_mcp(&hard_off_server.url, Some("native-desktop-fixture")).await?;
    let desktop = connect_mcp(&server.url, Some("native-desktop-fixture")).await?;

    let assertions = async {
        let denied_start = tool_json(
            call_tool(
                &hard_off,
                "trading_control",
                serde_json::json!({
                    "action":"start",
                    "request_id":"native-desktop-hard-off-start",
                    "expected_sequence":0
                }),
            )
            .await?,
        )?;
        assert_eq!(denied_start["state"], "DISABLED_BY_CONFIG");
        assert_eq!(denied_start["sequence"], 0);
        assert_eq!(denied_start["receipt"]["outcome"], "DENIED");
        assert_eq!(
            denied_start["receipt"]["denied_code"],
            "LIVE_TRADING_DISABLED"
        );
        assert!(!hard_off_gate.is_enabled());

        let order_gate_status = tool_json(
            call_tool(
                &desktop,
                "live_order_control",
                serde_json::json!({"action":"status"}),
            )
            .await?,
        )?;
        assert_eq!(order_gate_status["live_order_control_available"], true);
        assert_eq!(order_gate_status["live_trading_enabled"], false);
        assert_eq!(order_gate_status["exchange_read_mode"], "MOCK");
        let permit_request = serde_json::json!({
            "action":"enable",
            "request_id":"native-desktop-test-enable-order-gate",
            "expected_sequence":order_gate_status["sequence"],
            "expected_epoch":order_gate_status["boot_epoch"],
            "confirmation":"ENABLE_LIVE_ORDER_SENDING"
        });
        let sender_receipt =
            tool_json(call_tool(&desktop, "live_order_control", permit_request).await?)?;
        assert_eq!(sender_receipt["receipt"]["outcome"], "ACKNOWLEDGED");
        assert!(gate.is_enabled());

        let initial = tool_json(
            call_tool(
                &desktop,
                "trading_control",
                serde_json::json!({"action":"status"}),
            )
            .await?,
        )?;
        assert_eq!(initial["state"], "STANDBY");
        assert_eq!(initial["live_trading_enabled"], true);
        assert_eq!(initial["exchange_read_mode"], "MOCK");
        let started = tool_json(
            call_tool(
                &desktop,
                "trading_control",
                serde_json::json!({
                    "action":"start",
                    "request_id":"native-desktop-global-start",
                    "expected_sequence":initial["sequence"]
                }),
            )
            .await?,
        )?;
        assert_eq!(started["receipt"]["outcome"], "ACKNOWLEDGED");
        assert_eq!(started["state"], "RUNNING");
        assert_eq!(started["sequence"], 1);
        assert_eq!(started["order_admission_blocked"], false);
        assert_eq!(started["emergency_latched"], false);
        assert!(
            gate.is_enabled(),
            "trading start must not change the send gate"
        );

        let stale_pause = call_tool(
            &desktop,
            "trading_control",
            serde_json::json!({
                "action":"pause",
                "request_id":"native-desktop-stale-pause",
                "expected_sequence":0
            }),
        )
        .await?;
        assert_tool_denied(stale_pause, "trading control sequence conflict");
        let after_stale = tool_json(
            call_tool(
                &desktop,
                "trading_control",
                serde_json::json!({"action":"status"}),
            )
            .await?,
        )?;
        assert_eq!(after_stale["state"], "RUNNING");
        assert_eq!(after_stale["sequence"], 1);

        let emergency = tool_json(
            call_tool(
                &desktop,
                "trading_control",
                serde_json::json!({
                    "action":"emergency_stop",
                    "request_id":"native-desktop-emergency-stop",
                    "expected_sequence":after_stale["sequence"]
                }),
            )
            .await?,
        )?;
        assert_eq!(emergency["state"], "EMERGENCY_STOP_ACTIVE");
        assert_eq!(emergency["sequence"], 2);
        assert_eq!(emergency["order_admission_blocked"], true);
        assert_eq!(emergency["emergency_latched"], true);
        let denied_resume = tool_json(
            call_tool(
                &desktop,
                "trading_control",
                serde_json::json!({
                    "action":"resume",
                    "request_id":"native-desktop-emergency-resume-denied",
                    "expected_sequence":emergency["sequence"]
                }),
            )
            .await?,
        )?;
        assert_eq!(denied_resume["receipt"]["outcome"], "DENIED");
        assert_eq!(denied_resume["receipt"]["denied_code"], "INVALID_STATE");
        assert_eq!(denied_resume["state"], "EMERGENCY_STOP_ACTIVE");
        assert_eq!(denied_resume["sequence"], emergency["sequence"]);
        assert_eq!(denied_resume["emergency_latched"], true);

        let sessions = tool_json(
            call_tool(
                &desktop,
                "automation_session_query",
                serde_json::json!({"action":"list", "offset":0, "limit":10}),
            )
            .await?,
        )?;
        assert_eq!(sessions["total_count"], 0);
        assert!(
            private_fixture.requests().is_empty(),
            "global admission control must not issue private exchange requests"
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let hard_off_cancel = hard_off.cancel().await;
    let desktop_cancel = desktop.cancel().await;
    let hard_off_result = hard_off_server.shutdown().await;
    let server_result = server.shutdown().await;
    let runtime_result = runtime.shutdown().await;
    let private_result = private_server.shutdown().await;
    let owner_result = owner.shutdown();

    assertions?;
    hard_off_cancel?;
    desktop_cancel?;
    hard_off_result?;
    server_result?;
    runtime_result?;
    private_result?;
    owner_result?;
    Ok(())
}
