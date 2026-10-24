//! Typed Upbit API client with read paths and guarded automation effects.
//!
//! Enforces:
//! - Strict host allowlist on every outbound HTTP request.
//! - Automatic JWT signature generation with canonical query hash.
//! - Production automation order POST requires the explicit live-execution policy.
//! - Production test-order POST and cancellation DELETE remain unavailable.
//! - No withdrawal/transfer endpoint.
//! - Mock mode support for hermetic offline testing and verification.

use crate::contracts::automation::{
    AutomationOrderIdentifier, AutomationOrderKind, AutomationOrderRequest, AutomationSide,
};
use crate::upbit::auth::{
    AuthError, UPBIT_PRODUCTION_HOST, UpbitCredentials, verify_host_allowlist,
};
use crate::upbit::execution::LiveExecutionPolicy;
use crate::upbit::live_order_control::LiveOrderSendGate;
use crate::upbit::realtime::PrivateReadinessFence;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Maximum response body allowed for private API calls (1 MiB).
const MAX_BODY_BYTES: usize = 1_048_576;
const MAX_OPEN_ORDER_BASELINE_PAGES: u32 = 100;
const OPEN_ORDER_PAGE_SIZE: u32 = 100;
pub(crate) const MAX_PRIVATE_BASELINE_BYTES: usize = 4 * 1_048_576;
mod rate_limit;
use rate_limit::RateLimitState;

/// Private API Client Errors.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("LIVE_TRADING_DISABLED: production exchange effects are not authorized")]
    LiveTradingDisabled,
    #[error("LIVE_ORDER_SENDING_DISABLED: runtime gate denied before provider POST")]
    LiveOrderControlDenied,
    #[error("LIVE_PRIVATE_ACCOUNT_NOT_READY: private account baseline/recovery is incomplete")]
    PrivateAccountNotReady,
    #[error("private open-order baseline exceeded its {limit} order bound")]
    PrivateBaselineLimit { limit: u64 },
    #[error("private REST baseline used {observed} bytes; limit is {limit}")]
    PrivateBaselineByteLimit { observed: usize, limit: usize },
    #[error(
        "provider request budget exhausted; next request allowed after {retry_after_seconds} seconds"
    )]
    RateLimited { retry_after_seconds: u64 },
    #[error(
        "provider blocked requests without a verified retry time; automatic requests suspended"
    )]
    ProviderBlocked,
    #[error("Authentication error: {0}")]
    Auth(#[from] AuthError),
    #[error("Network failure: {0}")]
    Network(String),
    #[error("Upbit API error (HTTP {status}): {body}")]
    Api { status: u16, body: String },
    #[error("JSON decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid request parameters: {0}")]
    InvalidParams(String),
    #[error("Response size {observed} exceeded max limit {limit}")]
    ResponseTooLarge { observed: u64, limit: u64 },
}

/// Shared aggregate wire-byte budget for one REST snapshot plus bootstrap events.
pub(crate) struct PrivateBaselineByteBudget {
    used: AtomicUsize,
}

impl PrivateBaselineByteBudget {
    pub(crate) fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
        }
    }

    pub(crate) fn record(&self, bytes: usize) -> Result<usize, ClientError> {
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= MAX_PRIVATE_BASELINE_BYTES)
            });
        match result {
            Ok(previous) => Ok(previous + bytes),
            Err(previous) => Err(ClientError::PrivateBaselineByteLimit {
                observed: previous.saturating_add(bytes),
                limit: MAX_PRIVATE_BASELINE_BYTES,
            }),
        }
    }

    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

/// Upbit account balance row from `GET /v1/accounts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitAccountBalance {
    pub currency: String,
    pub balance: Decimal,
    pub locked: Decimal,
    pub avg_buy_price: Decimal,
    pub avg_buy_price_modified: bool,
    pub unit_currency: String,
}

/// Upbit order chance response from `GET /v1/orders/chance`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderChance {
    pub bid_fee: Decimal,
    pub ask_fee: Decimal,
    #[serde(default)]
    pub maker_bid_fee: Option<Decimal>,
    #[serde(default)]
    pub maker_ask_fee: Option<Decimal>,
    pub market: serde_json::Value,
    pub bid_account: Option<serde_json::Value>,
    pub ask_account: Option<serde_json::Value>,
}

/// Upbit individual order details from `GET /v1/order` or `GET /v1/orders`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderDetail {
    pub uuid: String,
    #[serde(default)]
    pub identifier: Option<String>,
    pub side: String,
    pub ord_type: String,
    pub price: Option<Decimal>,
    pub state: String,
    pub market: String,
    pub created_at: String,
    pub volume: Option<Decimal>,
    pub remaining_volume: Option<Decimal>,
    pub reserved_fee: Option<Decimal>,
    pub remaining_fee: Option<Decimal>,
    pub paid_fee: Option<Decimal>,
    pub locked: Option<Decimal>,
    pub executed_volume: Option<Decimal>,
    pub trades_count: Option<u64>,
    #[serde(default)]
    pub trades: Vec<serde_json::Value>,
}

/// Shared JSON order parameters for the guarded order and non-executing test endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderRequest {
    pub market: String,
    pub side: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub ord_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
}

/// Existing non-executing endpoint uses the same wire parameters.
pub type UpbitTestOrderRequest = UpbitOrderRequest;

/// Response returned by `POST /v1/orders/test`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitTestOrderResponse {
    pub status: String,
    pub market: String,
    pub side: String,
    pub ord_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    pub test_order_acknowledged: bool,
    pub raw_response: serde_json::Value,
}

/// Transport mode: Live HTTP vs In-Memory Hermetic Mock.
#[derive(Clone)]
pub enum TransportMode {
    Live(UpbitCredentials),
    Mock,
}

/// Thread-safe Upbit private API client.
#[derive(Clone)]
pub struct UpbitPrivateClient {
    http: reqwest::Client,
    base_url: String,
    mode: TransportMode,
    live_execution_policy: LiveExecutionPolicy,
    live_order_send_gate: Option<LiveOrderSendGate>,
    rate_limit: Arc<Mutex<RateLimitState>>,
    private_readiness: Arc<OnceLock<PrivateReadinessFence>>,
    #[cfg(test)]
    effect_fixture: bool,
}

impl UpbitPrivateClient {
    /// Create a new private client in Live mode with valid credentials.
    ///
    /// # Errors
    /// Returns `ClientError::InvalidParams` if HTTP client construction fails.
    pub fn new_live(credentials: UpbitCredentials) -> Result<Self, ClientError> {
        Self::new_live_with_policy(credentials, LiveExecutionPolicy::Disabled)
    }

    /// Create a production private client with an explicit process-level effect policy.
    /// Read-only account and order projections remain available in either mode.
    /// # Errors
    /// Returns `ClientError::InvalidParams` if HTTP client construction fails.
    pub fn new_live_with_policy(
        credentials: UpbitCredentials,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("Sky-BackCraft-P12/0.1.0 (Mac; Rust 1.98.0)")
            .build()
            .map_err(|e| ClientError::Network(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            http,
            base_url: UPBIT_PRODUCTION_HOST.to_string(),
            mode: TransportMode::Live(credentials),
            live_execution_policy,
            live_order_send_gate: None,
            rate_limit: Arc::new(Mutex::new(RateLimitState::new())),
            private_readiness: Arc::new(OnceLock::new()),
            #[cfg(test)]
            effect_fixture: false,
        })
    }

    /// Create a live client whose final order POST shares a runtime send gate.
    ///
    /// # Errors
    /// Returns `ClientError` when HTTP client construction fails.
    pub fn new_live_with_policy_and_send_gate(
        credentials: UpbitCredentials,
        live_execution_policy: LiveExecutionPolicy,
        live_order_send_gate: LiveOrderSendGate,
    ) -> Result<Self, ClientError> {
        let mut client = Self::new_live_with_policy(credentials, live_execution_policy)?;
        client.live_order_send_gate = Some(live_order_send_gate);
        Ok(client)
    }

    /// Create a mock private client for testing without network requests.
    #[must_use]
    pub fn new_mock() -> Self {
        let http = reqwest::Client::builder()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            http,
            base_url: UPBIT_PRODUCTION_HOST.to_string(),
            mode: TransportMode::Mock,
            live_execution_policy: LiveExecutionPolicy::Disabled,
            live_order_send_gate: None,
            rate_limit: Arc::new(Mutex::new(RateLimitState::new())),
            private_readiness: Arc::new(OnceLock::new()),
            #[cfg(test)]
            effect_fixture: false,
        }
    }

    /// Construct a credential-free MOCK client for runtime-control tests.
    #[must_use]
    pub fn new_mock_with_policy_and_send_gate(
        live_execution_policy: LiveExecutionPolicy,
        live_order_send_gate: LiveOrderSendGate,
    ) -> Self {
        let mut client = Self::new_mock();
        client.live_execution_policy = live_execution_policy;
        client.live_order_send_gate = Some(live_order_send_gate);
        client
    }

    #[must_use]
    pub fn live_order_send_gate(&self) -> Option<LiveOrderSendGate> {
        self.live_order_send_gate.clone()
    }

    #[must_use]
    pub fn live_trading_enabled(&self) -> bool {
        self.live_order_send_gate.as_ref().map_or_else(
            || self.live_execution_policy.is_enabled(),
            LiveOrderSendGate::is_enabled,
        )
    }

    /// Construct a test-only authenticated client pinned to an exact loopback
    /// HTTP origin. Production clients cannot override Upbit's host.
    #[cfg(test)]
    pub(crate) fn loopback_fixture(
        base_url: &str,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<Self, ClientError> {
        let credentials = UpbitCredentials::generated_for_test().map_err(ClientError::Auth)?;
        let client =
            Self::loopback_fixture_with_credentials(base_url, live_execution_policy, credentials)?;
        client.private_readiness_fence().mark_test_ready();
        Ok(client)
    }

    /// Construct an isolated loopback client with explicitly supplied test credentials.
    #[cfg(test)]
    pub(crate) fn loopback_fixture_with_credentials(
        base_url: &str,
        live_execution_policy: LiveExecutionPolicy,
        credentials: UpbitCredentials,
    ) -> Result<Self, ClientError> {
        let parsed = reqwest::Url::parse(base_url)
            .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
        if parsed.scheme() != "http"
            || parsed.host_str() != Some("127.0.0.1")
            || parsed.port().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ClientError::InvalidParams(
                "test private client requires an exact http://127.0.0.1:<port> origin".into(),
            ));
        }
        let base_url = parsed.as_str().trim_end_matches('/').to_owned();
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ClientError::Network(error.to_string()))?;
        Ok(Self {
            http,
            base_url,
            mode: TransportMode::Live(credentials),
            live_execution_policy,
            live_order_send_gate: None,
            rate_limit: Arc::new(Mutex::new(RateLimitState::new())),
            private_readiness: Arc::new(OnceLock::new()),
            effect_fixture: true,
        })
    }

    /// Check whether the client is in Live or Mock mode.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self.mode, TransportMode::Live(_))
    }

    /// Return the immutable process-level policy this client enforces for effects.
    #[must_use]
    pub const fn live_execution_policy(&self) -> LiveExecutionPolicy {
        self.live_execution_policy
    }

    /// Install or retrieve this client's shared private-stream fence.
    /// Every clone of this client observes the same readiness generation.
    #[must_use]
    pub(crate) fn private_readiness_fence(&self) -> PrivateReadinessFence {
        self.private_readiness
            .get_or_init(PrivateReadinessFence::new)
            .clone()
    }

    /// Sign the fixed private WebSocket handshake inside the Rust credential boundary.
    /// The returned header is marked sensitive and must never be logged or persisted.
    ///
    /// # Errors
    /// Rejects MOCK clients and signing/header construction failures.
    pub(crate) fn private_websocket_authorization_header(
        &self,
    ) -> Result<reqwest::header::HeaderValue, ClientError> {
        let TransportMode::Live(credentials) = &self.mode else {
            return Err(ClientError::InvalidParams(
                "private WebSocket requires a configured LIVE account".into(),
            ));
        };
        let authorization = credentials.sign_request(None)?;
        let mut header =
            reqwest::header::HeaderValue::try_from(authorization.as_str()).map_err(|_| {
                ClientError::InvalidParams("invalid private authorization header".into())
            })?;
        header.set_sensitive(true);
        Ok(header)
    }

    /// Fetch account balances (`GET /v1/accounts`).
    ///
    /// # Errors
    /// Returns `ClientError` on HTTP, network, or decoding error.
    pub async fn get_accounts(&self) -> Result<Vec<UpbitAccountBalance>, ClientError> {
        self.get_accounts_with_size()
            .await
            .map(|(accounts, _)| accounts)
    }

    async fn get_accounts_with_size(
        &self,
    ) -> Result<(Vec<UpbitAccountBalance>, usize), ClientError> {
        match &self.mode {
            TransportMode::Mock => Ok((Vec::new(), 0)),
            TransportMode::Live(creds) => {
                let url = format!("{}/v1/accounts", self.base_url);
                self.verify_request_host(&url)?;
                let auth_header = creds.sign_request(None)?;

                self.throttle().await?;
                let res = self
                    .http
                    .get(&url)
                    .header(reqwest::header::AUTHORIZATION, auth_header)
                    .send()
                    .await
                    .map_err(|e| ClientError::Network(e.to_string()))?;

                let (status, bytes) = self.private_response(res).await?;
                if status != 200 {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    return Err(ClientError::Api { status, body });
                }

                let response_bytes = bytes.len();
                let accounts: Vec<UpbitAccountBalance> = serde_json::from_slice(&bytes)?;
                Ok((accounts, response_bytes))
            }
        }
    }

    /// Fetch market chance information (`GET /v1/orders/chance`).
    ///
    /// # Errors
    /// Returns `ClientError` on HTTP, network, or decoding error.
    pub async fn get_order_chance(&self, market: &str) -> Result<UpbitOrderChance, ClientError> {
        validate_order_token(market)?;
        match &self.mode {
            TransportMode::Mock => Ok(UpbitOrderChance {
                bid_fee: Decimal::new(5, 4), // 0.0005
                ask_fee: Decimal::new(5, 4),
                maker_bid_fee: Some(Decimal::new(5, 4)),
                maker_ask_fee: Some(Decimal::new(5, 4)),
                market: serde_json::json!({
                    "id": market,
                    "name": market,
                    "order_types": ["limit", "price", "market"],
                    "order_sides": ["ask", "bid"],
                    "bid": {"currency": "KRW", "price_unit": null, "min_total": "5000"},
                    "ask": {"currency": market.split('-').nth(1).unwrap_or("BTC"), "price_unit": null, "min_total": "5000"},
                    "max_total": "1000000000",
                    "state": "active"
                }),
                bid_account: Some(serde_json::json!({
                    "currency": "KRW",
                    "balance": "10000000.0",
                    "locked": "0.0",
                    "unit_currency": "KRW"
                })),
                ask_account: Some(serde_json::json!({
                    "currency": market.split('-').nth(1).unwrap_or("BTC"),
                    "balance": "1.0",
                    "locked": "0.0",
                    "unit_currency": "KRW"
                })),
            }),
            TransportMode::Live(creds) => {
                let query = format!("market={market}");
                let url = format!("{}/v1/orders/chance?{query}", self.base_url);
                self.verify_request_host(&url)?;
                let auth_header = creds.sign_request(Some(&query))?;

                self.throttle().await?;
                let res = self
                    .http
                    .get(&url)
                    .header(reqwest::header::AUTHORIZATION, auth_header)
                    .send()
                    .await
                    .map_err(|e| ClientError::Network(e.to_string()))?;

                let (status, bytes) = self.private_response(res).await?;
                if status != 200 {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    return Err(ClientError::Api { status, body });
                }

                let mut chance: UpbitOrderChance = serde_json::from_slice(&bytes)?;
                let tick = self.get_orderbook_tick_size(market).await?;
                for side in ["bid", "ask"] {
                    let rules = chance
                        .market
                        .get_mut(side)
                        .and_then(serde_json::Value::as_object_mut)
                        .ok_or_else(|| {
                            ClientError::InvalidParams("chance response omitted side rules".into())
                        })?;
                    rules.insert(
                        "price_unit".into(),
                        serde_json::Value::String(tick.to_string()),
                    );
                }
                chance
                    .market
                    .as_object_mut()
                    .ok_or_else(|| {
                        ClientError::InvalidParams("chance market is not an object".into())
                    })?
                    .insert(
                        "tick_size_source".into(),
                        serde_json::Value::String("UPBIT_ORDERBOOK_INSTRUMENTS".into()),
                    );
                Ok(chance)
            }
        }
    }

    /// Fetch authoritative current tick policy; never fall back to deprecated chance fields.
    /// # Errors
    /// Rejects HTTP failures, mismatched markets or malformed/nonpositive tick units.
    pub async fn get_orderbook_tick_size(&self, market: &str) -> Result<Decimal, ClientError> {
        validate_order_token(market)?;
        if !self.is_live() {
            return Err(ClientError::InvalidParams(
                "real instrument policy requires the live read adapter".into(),
            ));
        }
        let url = format!(
            "{}/v1/orderbook/instruments?markets={market}",
            self.base_url
        );
        self.verify_request_host(&url)?;
        self.throttle().await?;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| ClientError::Network(error.to_string()))?;
        let (status, bytes) = self.private_response(response).await?;
        if status != 200 {
            return Err(ClientError::Api {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        let entries = value
            .as_array()
            .filter(|entries| entries.len() == 1)
            .ok_or_else(|| {
                ClientError::InvalidParams(
                    "instrument response must contain exactly the requested market".into(),
                )
            })?;
        let entry = &entries[0];
        if entry.get("market").and_then(serde_json::Value::as_str) != Some(market)
            || entry
                .get("quote_currency")
                .and_then(serde_json::Value::as_str)
                != Some("KRW")
        {
            return Err(ClientError::InvalidParams(
                "instrument market/quote identity differs".into(),
            ));
        }
        let text = entry
            .get("tick_size")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ClientError::InvalidParams("instrument tick size must be a decimal string".into())
            })?;
        let tick = Decimal::from_str_exact(text)
            .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
        if tick <= Decimal::ZERO {
            return Err(ClientError::InvalidParams(
                "instrument tick size must be positive".into(),
            ));
        }
        Ok(tick)
    }

    /// Fetch individual order detail by uuid or identifier (`GET /v1/order`).
    ///
    /// # Errors
    /// Returns `ClientError` on HTTP, network, or decoding error.
    pub async fn get_order(
        &self,
        uuid: Option<&str>,
        identifier: Option<&str>,
    ) -> Result<UpbitOrderDetail, ClientError> {
        if let Some(uuid) = uuid {
            validate_order_token(uuid)?;
        }
        if let Some(identifier) = identifier {
            validate_order_token(identifier)?;
        }
        let query = match (uuid, identifier) {
            (Some(u), _) => format!("uuid={u}"),
            (None, Some(i)) => format!("identifier={i}"),
            (None, None) => {
                return Err(ClientError::InvalidParams(
                    "Either 'uuid' or 'identifier' must be specified".into(),
                ));
            }
        };

        match &self.mode {
            TransportMode::Mock => Ok(UpbitOrderDetail {
                uuid: uuid.unwrap_or("mock-uuid-1234").to_string(),
                identifier: identifier.map(str::to_owned),
                side: "bid".into(),
                ord_type: "limit".into(),
                price: Some(Decimal::new(95_000_000, 0)),
                state: "done".into(),
                market: "KRW-BTC".into(),
                created_at: "2026-10-12T00:00:00Z".into(),
                volume: Some(Decimal::new(1, 2)), // 0.01
                remaining_volume: Some(Decimal::ZERO),
                reserved_fee: Some(Decimal::new(475, 0)),
                remaining_fee: Some(Decimal::ZERO),
                paid_fee: Some(Decimal::new(475, 0)),
                locked: Some(Decimal::ZERO),
                executed_volume: Some(Decimal::new(1, 2)),
                trades_count: Some(1),
                trades: vec![],
            }),
            TransportMode::Live(creds) => {
                let url = format!("{}/v1/order?{query}", self.base_url);
                self.verify_request_host(&url)?;
                let auth_header = creds.sign_request(Some(&query))?;

                self.throttle().await?;
                let res = self
                    .http
                    .get(&url)
                    .header(reqwest::header::AUTHORIZATION, auth_header)
                    .send()
                    .await
                    .map_err(|e| ClientError::Network(e.to_string()))?;

                let (status, bytes) = self.private_response(res).await?;
                if status != 200 {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    return Err(ClientError::Api { status, body });
                }

                let detail: UpbitOrderDetail = serde_json::from_slice(&bytes)?;
                Ok(detail)
            }
        }
    }

    /// Submit a previously journaled automation intent. Never retries an effect.
    ///
    /// # Errors
    /// Live order admission requires the operator policy and all durable service
    /// gates. Transport/decoding failures may occur after exchange admission; the
    /// caller must reconcile the stored identifier.
    pub async fn submit_automation_order(
        &self,
        request: &AutomationOrderRequest,
    ) -> Result<UpbitOrderDetail, ClientError> {
        self.submit_automation_order_with_generation(request, None)
            .await
    }

    pub(crate) async fn submit_automation_order_with_generation(
        &self,
        request: &AutomationOrderRequest,
        send_generation: Option<u64>,
    ) -> Result<UpbitOrderDetail, ClientError> {
        let send_generation = send_generation.or_else(|| {
            self.live_order_send_gate
                .as_ref()
                .map(LiveOrderSendGate::generation)
        });
        self.check_effect_gate()?;
        let wire = automation_wire(request)?;
        let query = canonical_order_query(&wire);
        self.order_effect(
            reqwest::Method::POST,
            "/v1/orders",
            &query,
            Some(&wire),
            send_generation,
        )
        .await
    }

    /// Cancel a journaled exchange order by UUID. Does not assume no raced fills.
    ///
    /// # Errors
    /// Live cancellation is outside the accepted production capability; uncertain
    /// historical outcomes remain query-only and require identifier readback.
    pub async fn cancel_automation_order(
        &self,
        uuid: &str,
    ) -> Result<UpbitOrderDetail, ClientError> {
        self.check_cancellation_gate()?;
        validate_order_token(uuid)?;
        let query = format!("uuid={uuid}");
        self.order_effect(reqwest::Method::DELETE, "/v1/order", &query, None, None)
            .await
    }

    /// Lookup an uncertain intent. A missing order is not permission to resend it.
    ///
    /// # Errors
    /// Rejects stateless mock use and preserves non-not-found exchange errors.
    pub async fn automation_order_by_identifier(
        &self,
        identifier: &AutomationOrderIdentifier,
    ) -> Result<Option<UpbitOrderDetail>, ClientError> {
        if matches!(self.mode, TransportMode::Mock) {
            return Err(ClientError::InvalidParams(
                "automation requires its durable mock venue, not stateless mock reads".into(),
            ));
        }
        let query = format!("identifier={}", identifier.as_str());
        let url = format!("{}/v1/order?{query}", self.base_url);
        self.verify_request_host(&url)?;
        let TransportMode::Live(credentials) = &self.mode else {
            return Err(ClientError::LiveTradingDisabled);
        };
        let authorization = credentials.sign_request(Some(&query))?;
        self.throttle().await?;
        let response = self
            .http
            .get(url)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .send()
            .await
            .map_err(|error| ClientError::Network(error.to_string()))?;
        let (status, body) = self.private_response(response).await?;
        if status == 404 {
            let error: serde_json::Value = serde_json::from_slice(&body)?;
            if error
                .pointer("/error/name")
                .and_then(serde_json::Value::as_str)
                == Some("order_not_found")
            {
                return Ok(None);
            }
        }
        if status != 200 {
            return Err(ClientError::Api {
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        let detail: UpbitOrderDetail = serde_json::from_slice(&body)?;
        if detail.identifier.as_deref() != Some(identifier.as_str()) {
            return Err(ClientError::InvalidParams(
                "exchange lookup returned a different order identifier".into(),
            ));
        }
        Ok(Some(detail))
    }

    fn check_effect_gate(&self) -> Result<(), ClientError> {
        if !self.live_execution_policy.is_enabled() || !self.is_live() {
            return Err(ClientError::LiveTradingDisabled);
        }
        Ok(())
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::unused_self,
            reason = "production cancellation is denied; test builds inspect the per-client loopback fixture"
        )
    )]
    fn check_cancellation_gate(&self) -> Result<(), ClientError> {
        #[cfg(test)]
        if self.effect_fixture {
            return Ok(());
        }
        Err(ClientError::LiveTradingDisabled)
    }

    fn check_test_order_gate() -> Result<(), ClientError> {
        Err(ClientError::LiveTradingDisabled)
    }

    #[allow(
        clippy::unused_self,
        reason = "test builds use the per-client loopback fixture; production checks the fixed allowlist"
    )]
    fn verify_request_host(&self, url: &str) -> Result<(), ClientError> {
        #[cfg(test)]
        if self.effect_fixture {
            let parsed = reqwest::Url::parse(url)
                .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
            if parsed.scheme() == "http"
                && parsed.host_str() == Some("127.0.0.1")
                && parsed.port().is_some()
                && parsed.username().is_empty()
                && parsed.password().is_none()
            {
                return Ok(());
            }
            return Err(ClientError::InvalidParams(
                "test venue must remain loopback".into(),
            ));
        }
        verify_host_allowlist(url)?;
        Ok(())
    }

    async fn order_effect(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &str,
        body: Option<&UpbitOrderRequest>,
        send_generation: Option<u64>,
    ) -> Result<UpbitOrderDetail, ClientError> {
        let TransportMode::Live(credentials) = &self.mode else {
            return Err(ClientError::LiveTradingDisabled);
        };
        let private_generation = if method == reqwest::Method::POST {
            let generation = self
                .private_readiness
                .get()
                .and_then(PrivateReadinessFence::ready_generation)
                .ok_or(ClientError::PrivateAccountNotReady)?;
            Some(generation)
        } else {
            None
        };
        let url = if method == reqwest::Method::DELETE {
            format!("{}{path}?{query}", self.base_url)
        } else {
            format!("{}{path}", self.base_url)
        };
        self.verify_request_host(&url)?;
        let authorization = credentials.sign_request(Some(query))?;
        self.throttle().await?;
        let mut request = self
            .http
            .request(method.clone(), url)
            .header(reqwest::header::AUTHORIZATION, authorization);
        if let Some(body) = body {
            request = request.json(body);
        }
        let _send_lease = if method == reqwest::Method::POST {
            self.live_order_send_gate
                .as_ref()
                .map(|gate| {
                    gate.try_lease(send_generation)
                        .ok_or(ClientError::LiveOrderControlDenied)
                })
                .transpose()?
        } else {
            None
        };
        if let Some(generation) = private_generation
            && !self
                .private_readiness
                .get()
                .is_some_and(|fence| fence.is_ready_generation(generation))
        {
            return Err(ClientError::PrivateAccountNotReady);
        }
        let response = request
            .send()
            .await
            .map_err(|error| ClientError::Network(error.to_string()))?;
        let (status, bytes) = self.private_response(response).await?;
        let expected = if method == reqwest::Method::POST {
            201
        } else {
            200
        };
        if status != expected {
            return Err(ClientError::Api {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes).map_err(ClientError::from)
    }

    /// Fetch open orders from the current official endpoint.
    ///
    /// # Errors
    /// Rejects invalid markets or limits and reports authenticated HTTP failures.
    pub async fn get_open_orders(
        &self,
        market: Option<&str>,
        page: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<UpbitOrderDetail>, ClientError> {
        self.get_open_orders_page_with_size(market, page, limit)
            .await
            .map(|(orders, _)| orders)
    }

    async fn get_open_orders_page_with_size(
        &self,
        market: Option<&str>,
        page: Option<u32>,
        limit: Option<u32>,
    ) -> Result<(Vec<UpbitOrderDetail>, usize), ClientError> {
        let page = page.unwrap_or(1);
        if !(1..=10_000).contains(&page) {
            return Err(ClientError::InvalidParams(
                "open order page must be in 1..=10000".into(),
            ));
        }
        let limit = limit.unwrap_or(100);
        if !(1..=100).contains(&limit) {
            return Err(ClientError::InvalidParams(
                "open order limit must be in 1..=100".into(),
            ));
        }
        let mut params = Vec::new();
        if let Some(market) = market {
            let market = crate::contracts::MarketId::parse_upbit(market)
                .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
            params.push(format!("market={}", market.code()));
        }
        params.push("states[]=wait".into());
        params.push("states[]=watch".into());
        params.push(format!("page={page}"));
        params.push(format!("limit={limit}"));
        self.get_order_list_with_size("/v1/orders/open", &params.join("&"))
            .await
    }

    /// Read account balances plus every open-order page under one aggregate
    /// response-byte and record budget.
    pub(crate) async fn get_private_account_baseline_bounded(
        &self,
        byte_budget: Arc<PrivateBaselineByteBudget>,
    ) -> Result<(Vec<UpbitAccountBalance>, Vec<UpbitOrderDetail>, usize), ClientError> {
        let (balances, mut observed_bytes) = self.get_accounts_with_size().await?;
        byte_budget.record(observed_bytes)?;
        let mut orders = Vec::with_capacity(OPEN_ORDER_PAGE_SIZE as usize);
        let mut uuids = std::collections::HashSet::with_capacity(OPEN_ORDER_PAGE_SIZE as usize);
        for page in 1..=MAX_OPEN_ORDER_BASELINE_PAGES + 1 {
            let (page_orders, page_bytes) = self
                .get_open_orders_page_with_size(None, Some(page), Some(OPEN_ORDER_PAGE_SIZE))
                .await?;
            byte_budget.record(page_bytes)?;
            observed_bytes = observed_bytes.checked_add(page_bytes).ok_or(
                ClientError::PrivateBaselineByteLimit {
                    observed: usize::MAX,
                    limit: MAX_PRIVATE_BASELINE_BYTES,
                },
            )?;
            if observed_bytes > MAX_PRIVATE_BASELINE_BYTES {
                return Err(ClientError::PrivateBaselineByteLimit {
                    observed: observed_bytes,
                    limit: MAX_PRIVATE_BASELINE_BYTES,
                });
            }
            let page_len = page_orders.len();
            if page > MAX_OPEN_ORDER_BASELINE_PAGES && page_len > 0 {
                return Err(ClientError::PrivateBaselineLimit { limit: 10_000 });
            }
            if orders.len().saturating_add(page_len) > 10_000 {
                return Err(ClientError::PrivateBaselineLimit { limit: 10_000 });
            }
            for order in page_orders {
                if !uuids.insert(order.uuid.clone()) {
                    return Err(ClientError::InvalidParams(
                        "open-order pagination returned a duplicate UUID".into(),
                    ));
                }
                orders.push(order);
            }
            if page_len < OPEN_ORDER_PAGE_SIZE as usize {
                return Ok((balances, orders, observed_bytes));
            }
        }
        Err(ClientError::PrivateBaselineLimit { limit: 10_000 })
    }

    /// Fetch closed orders from the current official endpoint.
    ///
    /// # Errors
    /// Rejects invalid markets, limits, times, or ranges and reports authenticated HTTP failures.
    pub async fn get_closed_orders(
        &self,
        market: Option<&str>,
        start_time: Option<&str>,
        end_time: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<UpbitOrderDetail>, ClientError> {
        let limit = limit.unwrap_or(100);
        if !(1..=1_000).contains(&limit) {
            return Err(ClientError::InvalidParams(
                "closed order limit must be in 1..=1000".into(),
            ));
        }
        let mut params = Vec::new();
        if let Some(market) = market {
            let market = crate::contracts::MarketId::parse_upbit(market)
                .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
            params.push(format!("market={}", market.code()));
        }
        let start_ms = start_time.map(parse_order_time).transpose()?;
        let end_ms = end_time.map(parse_order_time).transpose()?;
        if let (Some(start), Some(end)) = (start_ms, end_ms) {
            let maximum = chrono::Duration::days(7).num_milliseconds();
            if end < start || end - start > maximum {
                return Err(ClientError::InvalidParams(
                    "closed order time range must be ordered and no longer than 7 days".into(),
                ));
            }
        }
        if let Some(start) = start_ms {
            params.push(format!("start_time={start}"));
        }
        if let Some(end) = end_ms {
            params.push(format!("end_time={end}"));
        }
        params.push(format!("limit={limit}"));
        self.get_order_list("/v1/orders/closed", &params.join("&"))
            .await
    }

    async fn get_order_list(
        &self,
        path: &str,
        query: &str,
    ) -> Result<Vec<UpbitOrderDetail>, ClientError> {
        self.get_order_list_with_size(path, query)
            .await
            .map(|(orders, _)| orders)
    }

    async fn get_order_list_with_size(
        &self,
        path: &str,
        query: &str,
    ) -> Result<(Vec<UpbitOrderDetail>, usize), ClientError> {
        match &self.mode {
            TransportMode::Mock => Ok((vec![], 0)),
            TransportMode::Live(creds) => {
                let url = format!("{}{path}?{query}", self.base_url);
                self.verify_request_host(&url)?;
                let auth_header = creds.sign_request(Some(query))?;

                self.throttle().await?;
                let res = self
                    .http
                    .get(&url)
                    .header(reqwest::header::AUTHORIZATION, auth_header)
                    .send()
                    .await
                    .map_err(|e| ClientError::Network(e.to_string()))?;

                let (status, bytes) = self.private_response(res).await?;
                if status != 200 {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    return Err(ClientError::Api { status, body });
                }

                let response_bytes = bytes.len();
                let orders: Vec<UpbitOrderDetail> = serde_json::from_slice(&bytes)?;
                Ok((orders, response_bytes))
            }
        }
    }

    /// Call official non-executing test order endpoint (`POST /v1/orders/test`).
    ///
    /// This endpoint validates order parameters and market viability on Upbit
    /// WITHOUT placing any real order or mutating any balances.
    ///
    /// # Errors
    /// Returns `ClientError` on invalid parameters, API rejection, or network error.
    pub async fn post_order_test(
        &self,
        req: &UpbitTestOrderRequest,
    ) -> Result<UpbitTestOrderResponse, ClientError> {
        match &self.mode {
            TransportMode::Mock => Ok(UpbitTestOrderResponse {
                status: "MOCK_TEST_ONLY".to_string(),
                market: req.market.clone(),
                side: req.side.clone(),
                ord_type: req.ord_type.clone(),
                volume: req.volume.as_deref().and_then(|v| v.parse().ok()),
                price: req.price.as_deref().and_then(|p| p.parse().ok()),
                identifier: req.identifier.clone(),
                test_order_acknowledged: false,
                raw_response: serde_json::json!({
                    "test": true,
                    "acknowledged": false,
                    "source_mode": "MOCK",
                    "market": req.market,
                    "side": req.side,
                    "ord_type": req.ord_type,
                    "message": "Mock-only parameter projection; no official Upbit acknowledgement"
                }),
            }),
            TransportMode::Live(creds) => {
                Self::check_test_order_gate()?;
                let url = format!("{}/v1/orders/test", self.base_url);
                self.verify_request_host(&url)?;

                // Build canonical query string for query_hash per Upbit specs
                let mut query_params: Vec<(&str, &str)> = Vec::new();
                query_params.push(("market", &req.market));
                query_params.push(("side", &req.side));
                if let Some(v) = &req.volume {
                    query_params.push(("volume", v));
                }
                if let Some(p) = &req.price {
                    query_params.push(("price", p));
                }
                query_params.push(("ord_type", &req.ord_type));
                if let Some(i) = &req.identifier {
                    query_params.push(("identifier", i));
                }

                let query_string: String = query_params
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&");

                let auth_header = creds.sign_request(Some(&query_string))?;

                self.throttle().await?;
                let res = self
                    .http
                    .post(&url)
                    .header(reqwest::header::AUTHORIZATION, auth_header)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .json(req)
                    .send()
                    .await
                    .map_err(|e| ClientError::Network(e.to_string()))?;

                let (status, bytes) = self.private_response(res).await?;
                if status != 200 && status != 201 {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    return Err(ClientError::Api { status, body });
                }

                let raw_val: serde_json::Value = serde_json::from_slice(&bytes)?;

                Ok(UpbitTestOrderResponse {
                    status: "OFFICIAL_TEST_SUCCESS".to_string(),
                    market: req.market.clone(),
                    side: req.side.clone(),
                    ord_type: req.ord_type.clone(),
                    volume: req.volume.as_deref().and_then(|v| v.parse().ok()),
                    price: req.price.as_deref().and_then(|p| p.parse().ok()),
                    identifier: req.identifier.clone(),
                    test_order_acknowledged: true,
                    raw_response: raw_val,
                })
            }
        }
    }

    /// Reserve a bounded request slot without holding a lock over network IO.
    async fn throttle(&self) -> Result<(), ClientError> {
        let next = self.rate_limit.lock().await.reserve(Instant::now())?;
        tokio::time::sleep_until(next).await;
        self.rate_limit.lock().await.check_block(Instant::now())
    }

    async fn private_response(
        &self,
        response: reqwest::Response,
    ) -> Result<(u16, Vec<u8>), ClientError> {
        self.rate_limit.lock().await.observe(
            response.status().as_u16(),
            response.headers(),
            Instant::now(),
        );
        process_response(response).await
    }
}

impl crate::automation::VenueAdapter for UpbitPrivateClient {
    fn account_identity(&self) -> String {
        match &self.mode {
            TransportMode::Live(credentials) => format!(
                "live:upbit:{}",
                crate::contracts::ContentHash::of_bytes(credentials.access_key().as_bytes())
                    .as_str()
            ),
            TransportMode::Mock => "mock:upbit-default".to_owned(),
        }
    }

    fn is_live(&self) -> bool {
        Self::is_live(self)
    }

    fn account_balances(&self) -> crate::automation::VenueFuture<'_, Vec<UpbitAccountBalance>> {
        Box::pin(self.get_accounts())
    }

    fn order_chance<'a>(
        &'a self,
        market: &'a crate::contracts::MarketId,
    ) -> crate::automation::VenueFuture<'a, UpbitOrderChance> {
        Box::pin(async move { self.get_order_chance(&market.code()).await })
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        _decision_mark: crate::contracts::PriceKrw,
        send_generation: Option<u64>,
    ) -> crate::automation::VenueFuture<'a, UpbitOrderDetail> {
        Box::pin(self.submit_automation_order_with_generation(request, send_generation))
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> crate::automation::VenueFuture<'a, Option<UpbitOrderDetail>> {
        Box::pin(self.automation_order_by_identifier(identifier))
    }

    fn cancel_order<'a>(
        &'a self,
        uuid: &'a str,
    ) -> crate::automation::VenueFuture<'a, UpbitOrderDetail> {
        Box::pin(self.cancel_automation_order(uuid))
    }
}

fn automation_wire(request: &AutomationOrderRequest) -> Result<UpbitOrderRequest, ClientError> {
    request
        .validate()
        .map_err(|error| ClientError::InvalidParams(error.to_string()))?;
    let (ord_type, volume, price) = match &request.order {
        AutomationOrderKind::Limit { volume, price } => (
            "limit",
            Some(volume.get().to_string()),
            Some(price.get().to_string()),
        ),
        AutomationOrderKind::MarketBuy { budget } => {
            ("price", None, Some(budget.get().to_string()))
        }
        AutomationOrderKind::MarketSell { volume } => {
            ("market", Some(volume.get().to_string()), None)
        }
    };
    Ok(UpbitOrderRequest {
        market: request.market.code().clone(),
        side: match request.side {
            AutomationSide::Bid => "bid",
            AutomationSide::Ask => "ask",
        }
        .into(),
        volume,
        price,
        ord_type: ord_type.into(),
        identifier: Some(request.client_identifier.as_str().to_owned()),
    })
}

fn canonical_order_query(request: &UpbitOrderRequest) -> String {
    let mut fields = vec![
        format!("market={}", request.market),
        format!("side={}", request.side),
    ];
    if let Some(volume) = &request.volume {
        fields.push(format!("volume={volume}"));
    }
    if let Some(price) = &request.price {
        fields.push(format!("price={price}"));
    }
    fields.push(format!("ord_type={}", request.ord_type));
    if let Some(identifier) = &request.identifier {
        fields.push(format!("identifier={identifier}"));
    }
    fields.join("&")
}

fn validate_order_token(token: &str) -> Result<(), ClientError> {
    if token.is_empty()
        || token.len() > 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(ClientError::InvalidParams(
            "order token must be 1..=64 ASCII letters/digits/-/_".into(),
        ));
    }
    Ok(())
}

fn parse_order_time(value: &str) -> Result<i64, ClientError> {
    if let Ok(milliseconds) = value.parse::<i64>() {
        chrono::DateTime::from_timestamp_millis(milliseconds).ok_or_else(|| {
            ClientError::InvalidParams("order time millisecond timestamp is out of range".into())
        })?;
        return Ok(milliseconds);
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|time| time.timestamp_millis())
        .map_err(|error| {
            ClientError::InvalidParams(format!(
                "order time must be ISO 8601 with offset or milliseconds: {error}"
            ))
        })
}

async fn process_response(mut res: reqwest::Response) -> Result<(u16, Vec<u8>), ClientError> {
    let status = res.status().as_u16();
    let limit = u64::try_from(MAX_BODY_BYTES)
        .map_err(|_| ClientError::Network("response limit exceeds u64".into()))?;
    if let Some(length) = res.content_length()
        && length > limit
    {
        return Err(ClientError::ResponseTooLarge {
            observed: length,
            limit,
        });
    }
    let capacity = res
        .content_length()
        .map_or(Ok(0), |length| usize::try_from(length.min(limit)))
        .map_err(|_| ClientError::Network("response length exceeds platform capacity".into()))?;
    let mut body = Vec::with_capacity(capacity);
    while let Some(chunk) = res
        .chunk()
        .await
        .map_err(|error| ClientError::Network(error.to_string()))?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(ClientError::ResponseTooLarge {
                observed: u64::MAX,
                limit,
            })?;
        if next > MAX_BODY_BYTES {
            return Err(ClientError::ResponseTooLarge {
                observed: u64::try_from(next).unwrap_or(u64::MAX),
                limit,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, body))
}

#[cfg(test)]
mod automation_adapter_tests;

#[cfg(test)]
mod response_tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn fixture_response(
        response: Vec<u8>,
    ) -> Result<(reqwest::Response, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            let mut request = [0_u8; 4_096];
            let _read = stream.read(&mut request).await;
            let _write = stream.write_all(&response).await;
            let _shutdown = stream.shutdown().await;
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/fixture"))
            .send()
            .await?;
        Ok((response, server))
    }

    #[tokio::test]
    async fn content_length_rejects_oversized_response_before_body_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_BODY_BYTES + 1
        )
        .into_bytes();
        let (response, server) = fixture_response(response).await?;
        let result = process_response(response).await;
        let expected = u64::try_from(MAX_BODY_BYTES + 1)?;
        assert!(matches!(
            result,
            Err(ClientError::ResponseTooLarge { observed, .. })
                if observed == expected
        ));
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn chunked_response_is_bounded_during_streaming() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut response =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        response.extend_from_slice(format!("{MAX_BODY_BYTES:x}\r\n").as_bytes());
        response.extend(std::iter::repeat_n(b'x', MAX_BODY_BYTES));
        response.extend_from_slice(b"\r\n1\r\ny\r\n0\r\n\r\n");
        let (response, server) = fixture_response(response).await?;
        assert!(matches!(
            process_response(response).await,
            Err(ClientError::ResponseTooLarge { .. })
        ));
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn bounded_response_round_trips_exact_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let body = br#"{"ok":true}"#;
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let (response, server) = fixture_response(response).await?;
        let (status, observed) = process_response(response).await?;
        assert_eq!(status, 200);
        assert_eq!(observed, body);
        server.await?;
        Ok(())
    }
}
