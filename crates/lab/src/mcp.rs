//! Typed F09 MCP adapter over shared process-owned services.
//!
//! The adapter accepts only domain DTOs and opaque IDs. Long work is submitted to
//! the durable job runner; handlers never accept arbitrary SQL, URLs, paths, or code.

#![allow(clippy::unused_async_trait_impl)]

use crate::contracts::{
    ArtifactId, AttemptState, AutomationSessionQuery, AutomationSessionWrite, CandleInterval,
    CandleObservation, CollectRequest, ContentHash, DatasetId, DatasetManifest, DatasetStatus,
    DecisionAnalysisQuery, DeleteResource, EvidenceImport, EvidenceSnapshotId, HardDeleteRequest,
    HistoryQuery, JobAttempt, JobControl, JobId, JobPayload, JobRecord, JobSubmission, LabError,
    MarketDataOrigin, MarketId, PaperReviewQuery, PaperSessionQuery, PaperSessionWrite,
    PlanRequest, PolicyDeleteQueryRequest, PolicyDeleteWriteRequest, PolicyQuery, PolicyWrite,
    ProbeCount, RequestId, ResultQuery, RunId, RunRequest, UtcRange,
};
use crate::database::DatabaseHandle;
use crate::jobs::JobService;
use crate::market_data::UpbitClient;
use crate::oauth::OAuthPermissions;
use crate::storage::{PolicyDeleteAdmission, ResultPage};
use crate::upbit::UpbitExchangeService;
use crate::upbit::client::{UpbitOrderDetail, UpbitTestOrderRequest};
use crate::upbit::control::{TradingControlRequest, TradingControlService};
use crate::upbit::execution::OrderExecuteRequest;
use crate::upbit::live_order_control::{LiveOrderControlRequest, LiveOrderControlService};
use crate::upbit::preview::{OrderPreview, OrderPreviewRequest};
use crate::upbit::reconciliation::{mock_cancel_order, reconcile_order_detail};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::router::tool::ToolRouter,
    model::{
        CallToolResult, ContentBlock, Implementation, JsonObject, MetaObject, ProtocolVersion,
        ServerCapabilities, ServerConfig,
    },
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const MAX_DATASET_PAGE_RECORDS: u32 = 100;
const MAX_PUBLIC_JOB_ATTEMPTS: usize = 32;
// Reserve framing space after measuring the real JSON-RPC id and encoded result.
const MAX_DECISION_ANALYSIS_ENCODED_BYTES: usize = crate::transport::MAX_RESPONSE_BYTES - 1_024;
const TRADING_READ_TOOL_NAMES: &[&str] = &[
    "upbit_account_balances",
    "upbit_order_chance",
    "upbit_order_get",
    "upbit_orders_open",
    "upbit_orders_closed",
    "automation_session_query",
];
const TRADING_CONTROL_TOOL_NAMES: &[&str] = &[
    "automation_session_write",
    "live_order_control",
    "trading_control",
];
const TRADING_ACCESS_DENIED_MESSAGE: &str = "private Upbit account, order and control tools require the native desktop capability or an explicitly scoped OAuth trading grant";
const DESKTOP_TOOL_NAMES: &[&str] = &[
    "lab_status",
    "realtime_status",
    "automation_session_query",
    "automation_session_write",
    "result_query",
    "history_query",
    "policy_query",
    "paper_review_query",
    "decision_analysis_query",
    "paper_session_query",
    "paper_session_write",
    "resource_delete_preview",
    "policy_delete_write",
    "policy_delete_query",
    "upbit_account_balances",
    "upbit_orders_open",
    "upbit_orders_closed",
    "trading_control",
    "live_order_control",
];

fn default_market() -> String {
    "KRW-BTC".to_owned()
}

fn default_interval() -> String {
    "h1".to_owned()
}

fn default_true() -> bool {
    true
}

/// Bounded tool input for `probe_upbit`; defaults keep one call small.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProbeParams {
    #[serde(default = "default_market")]
    pub market: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub count: ProbeCount,
    #[serde(default = "default_true")]
    pub save_raw: bool,
}

/// Dataset catalog or bounded observation-page query.
#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatasetQueryAction {
    List,
    Get,
}

/// Explicit dataset action with a bounded page and no path input.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetQueryParams {
    pub action: DatasetQueryAction,
    pub dataset_id: Option<DatasetId>,
    pub offset: Option<u64>,
    pub limit: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobControlAction {
    Get,
    Cancel,
    Retry,
}

/// Explicit job action by opaque ID.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobControlParams {
    pub action: JobControlAction,
    pub job_id: JobId,
}

/// Bounded result projection selected without optional-field ambiguity.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResultQueryParams {
    Ledger {
        query: ResultQuery,
    },
    Summary {
        run_id: RunId,
    },
    Costs {
        run_id: RunId,
        model_id: crate::contracts::ModelId,
    },
    EquityChart {
        run_id: RunId,
        model_id: crate::contracts::ModelId,
        max_points: u32,
    },
    PortfolioSummary {
        run_id: RunId,
    },
    PortfolioEquity {
        run_id: RunId,
        offset: u64,
        limit: u32,
    },
    PortfolioAllocations {
        run_id: RunId,
        offset: u64,
        limit: u32,
    },
    PortfolioRebalances {
        run_id: RunId,
        offset: u64,
        limit: u32,
    },
    PortfolioContributions {
        run_id: RunId,
    },
    RegimeTimeline {
        run_id: RunId,
        offset: u64,
        limit: u32,
    },
    RegimeSummary {
        run_id: RunId,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum DatasetQueryResult {
    List {
        dataset_ids: Vec<DatasetId>,
        returned_count: u64,
    },
    Snapshot {
        manifest: Box<DatasetSummary>,
        observations: Vec<CandleObservation>,
        returned_count: u64,
        total_count: u64,
        next_offset: Option<u64>,
        truncated_reason: Option<String>,
    },
}

#[derive(Debug, Serialize)]
struct DatasetSummary {
    schema_version: String,
    id: DatasetId,
    request: CollectRequest,
    coverage: UtcRange,
    status: DatasetStatus,
    row_count: u64,
    normalizer_version: String,
    gap_policy: String,
    semantic_digest: ContentHash,
    provenance_digest: ContentHash,
    origin: MarketDataOrigin,
    raw_object_count: u64,
    /// Present when part of this snapshot reused previously stored observations.
    reuse: Option<crate::contracts::CollectionReuse>,
    quality_issues: Vec<crate::quality::QualityIssueView>,
    total_quality_issues: u64,
    quality_truncated_reason: Option<String>,
}

/// Submit creation of a full or one-market report package by opaque run ID.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExportReportParams {
    pub request_id: RequestId,
    pub run_id: RunId,
    pub market: Option<MarketId>,
}

/// Submit independent verification, optionally including deterministic replay.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VerifyRunParams {
    pub request_id: RequestId,
    pub artifact_id: ArtifactId,
    pub replay: bool,
}

#[derive(Debug, Serialize)]
struct EvidenceRegistered {
    snapshot_id: EvidenceSnapshotId,
    digest: ContentHash,
    version_count: u64,
}

#[derive(Debug, Serialize)]
struct PublicJobRecord {
    job: JobRecord,
    total_attempts: u64,
    truncated_reason: Option<String>,
    artifacts: Vec<crate::contracts::ArtifactDescriptor>,
    artifact_catalog_query: Option<crate::contracts::ArtifactQuery>,
}

/// Tool input for `upbit_order_chance`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderChanceParams {
    pub market: String,
}

/// Tool input for `upbit_order_get`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderGetParams {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
}

/// Tool input for `upbit_orders_open`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOpenOrdersParams {
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub page: Option<u32>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Tool input for `upbit_orders_closed`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitClosedOrdersParams {
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct UpbitOrdersPage {
    venue: &'static str,
    exchange_read_mode: &'static str,
    order_scope: &'static str,
    observed_at_utc: String,
    page: Option<u32>,
    limit: u32,
    returned_count: u64,
    records: Vec<UpbitOrderDetail>,
    next_page: Option<u32>,
    truncated_reason: Option<&'static str>,
    start_time: Option<String>,
    end_time: Option<String>,
}

/// Tool input for `upbit_order_preview`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderPreviewParams {
    pub market: String,
    pub side: String,
    pub ord_type: String,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub volume: Option<String>,
    #[serde(default)]
    pub current_krw_balance: Option<String>,
    #[serde(default)]
    pub current_coin_balance: Option<String>,
}

/// Tool input for `upbit_order_test`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderTestParams {
    pub market: String,
    pub side: String,
    pub ord_type: String,
    #[serde(default)]
    pub volume: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
}

/// Tool input for `upbit_order_execute`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderExecuteParams {
    pub preview: serde_json::Value,
    #[serde(default)]
    pub human_approval_token: Option<String>,
    #[serde(default)]
    pub client_identifier: Option<String>,
    #[serde(default)]
    pub allow_mock_execution: bool,
}

/// Tool input for `upbit_order_reconcile`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderReconcileParams {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
    pub expected_volume: String,
}

/// Tool input for `upbit_order_cancel`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpbitOrderCancelParams {
    pub order_id: String,
    #[serde(default)]
    pub unfilled_volume: Option<String>,
    #[serde(default)]
    pub human_approval_token: Option<String>,
}

fn parse_arguments<T: DeserializeOwned>(arguments: JsonObject) -> Result<T, McpError> {
    serde_json::from_value(serde_json::Value::Object(arguments))
        .map_err(|error| McpError::invalid_params(error.to_string(), None))
}

fn invalid_params(error: &LabError) -> McpError {
    let details = match error {
        LabError::RequestLimit(report) => serde_json::to_value(report).ok(),
        _ => None,
    };
    McpError::invalid_params(error.to_string(), details)
}

fn public_error_text(error: &LabError) -> String {
    match error {
        LabError::Internal(_) => "INTERNAL: operation failed".to_owned(),
        LabError::DataCorrupt(_) => "DATA_CORRUPT: stored data failed integrity checks".to_owned(),
        LabError::NetworkUnavailable(_) => {
            "NETWORK_UNAVAILABLE: Upbit public data request failed".to_owned()
        }
        LabError::OutcomeUnknown(_) => {
            "OUTCOME_UNKNOWN: admitted storage outcome requires durable ID read-back".to_owned()
        }
        _ => error.to_string().chars().take(1_024).collect(),
    }
}

fn tool_error(error: &LabError) -> Result<CallToolResult, McpError> {
    if let LabError::RequestLimit(report) = error {
        let text =
            serde_json::to_string(&serde_json::json!({"code":"RESOURCE_LIMIT","details":report}))
                .map_err(|_| McpError::internal_error("serialize limit report", None))?;
        return Ok(CallToolResult::error(vec![ContentBlock::text(text)]));
    }
    if matches!(error, LabError::Internal(_)) {
        Err(McpError::internal_error(public_error_text(error), None))
    } else {
        Ok(CallToolResult::error(vec![ContentBlock::text(
            public_error_text(error),
        )]))
    }
}

#[derive(Clone, Copy)]
enum TradingScope {
    Read,
    Control,
}

impl TradingScope {
    const fn required(self) -> &'static str {
        match self {
            Self::Read => "mcp trading:read",
            Self::Control => "mcp trading:read trading:control",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Read => "This operation requires trading:read.",
            Self::Control => "This operation requires trading:control.",
        }
    }
}

#[derive(Clone, Copy)]
enum TradingPermissionError {
    Unavailable,
    InsufficientScope(TradingScope),
}

fn declare_oauth_tool_scopes<S>(
    route: &mut rmcp::handler::server::router::tool::ToolRoute<S>,
    scopes: &[&str],
) {
    let meta = route.attr.meta.get_or_insert_with(MetaObject::new);
    meta.0.insert(
        "securitySchemes".to_owned(),
        serde_json::json!([{ "type": "oauth2", "scopes": scopes }]),
    );
}

fn insufficient_scope_tool_error(
    resource_metadata_url: &str,
    scope: TradingScope,
) -> CallToolResult {
    let challenge = format!(
        "Bearer resource_metadata=\"{resource_metadata_url}\", error=\"insufficient_scope\", error_description=\"{}\", scope=\"{}\"",
        scope.description(),
        scope.required(),
    );
    let mut meta = MetaObject::new();
    meta.0.insert(
        "mcp/www_authenticate".to_owned(),
        serde_json::json!([challenge]),
    );
    CallToolResult::error(vec![ContentBlock::text(format!(
        "INSUFFICIENT_SCOPE: {TRADING_ACCESS_DENIED_MESSAGE}"
    ))])
    .with_meta(Some(meta))
}

fn typed_tool_error(value: &impl Serialize) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|_| McpError::internal_error("serialize typed tool error", None))?;
    Ok(CallToolResult::error(vec![ContentBlock::text(text)]))
}

fn upbit_tool_error(err: &impl std::fmt::Display) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(err.to_string())])
}

fn upbit_invalid_params(msg: impl std::fmt::Display) -> McpError {
    McpError::invalid_params(msg.to_string(), None)
}

fn text_result(value: &impl Serialize) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|_| McpError::internal_error("serialize tool result", None))?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

fn result_or_error<T: Serialize>(result: Result<T, LabError>) -> Result<CallToolResult, McpError> {
    match result {
        Ok(value) => text_result(&value),
        Err(error) => tool_error(&error),
    }
}

fn decision_analysis_result(
    result: Result<serde_json::Value, LabError>,
    request_id: &rmcp::model::RequestId,
) -> Result<CallToolResult, McpError> {
    let result = result_or_error(result)?;
    let envelope = rmcp::model::JsonRpcResponse {
        jsonrpc: rmcp::model::JsonRpcVersion2_0,
        id: request_id.clone(),
        result: &result,
    };
    let encoded = serde_json::to_vec(&envelope)
        .map_err(|_| McpError::internal_error("serialize analysis response envelope", None))?;
    if encoded.len() > MAX_DECISION_ANALYSIS_ENCODED_BYTES {
        return tool_error(&LabError::ResourceLimit(
            "decision analysis encoded response exceeds wire byte budget; request a smaller list page or source population; no partial totals were returned".into(),
        ));
    }
    Ok(result)
}

fn sanitize_attempt(attempt: &mut JobAttempt) {
    match &mut attempt.state {
        AttemptState::Failed { error, .. } => {
            // RESOURCE_LIMIT records carry the server-generated structured limit
            // report (limit name, unit, allowed bytes, stage, remedy) and
            // INVALID_CONFIG records echo only the caller's own rejected input
            // with fixed guidance text; masking either would hide exactly the
            // diagnosis a caller needs, so both pass through bounded.
            // ACCOUNTING_INVARIANT_FAILURE carries invariant name, event seq,
            // and diagnostic findings required for auditing.
            error.message = if matches!(
                error.code.as_str(),
                "RESOURCE_LIMIT" | "INVALID_CONFIG" | "ACCOUNTING_INVARIANT_FAILURE"
            ) {
                error.message.chars().take(512).collect()
            } else {
                public_failure_message(&error.code)
            };
        }
        AttemptState::Blocked { reason, .. } => {
            reason.message = if matches!(
                reason.code.as_str(),
                "RESOURCE_LIMIT" | "INVALID_CONFIG" | "ACCOUNTING_INVARIANT_FAILURE"
            ) {
                reason.message.chars().take(512).collect()
            } else {
                public_failure_message(&reason.code)
            };
        }
        AttemptState::Cancelled { reason, .. } => *reason = "job cancelled".into(),
        AttemptState::Interrupted { reason, .. } => *reason = "job interrupted".into(),
        AttemptState::Queued { .. }
        | AttemptState::Running { .. }
        | AttemptState::Completed { .. }
        | AttemptState::Partial { .. } => {}
    }
}

fn public_failure_message(code: &str) -> String {
    match code {
        "INVALID_CONFIG" => "job input was invalid",
        "INSUFFICIENT_WARMUP" => "job input has insufficient warmup",
        "DATA_GAP" => "required market data is incomplete",
        "INPUT_HASH_MISMATCH" => "frozen input identity changed",
        "BLOCKED_EVIDENCE" | "BLOCKED_INPUT" => "required input is unavailable",
        "NETWORK_UNAVAILABLE" => "Upbit public data request failed",
        "RATE_LIMITED" => "Upbit public rate limit was reached",
        "TEMPORARILY_BLOCKED" => "Upbit temporarily blocked public requests",
        "CAPACITY_EXCEEDED" | "RESOURCE_LIMIT" => "configured resource limit was reached",
        "CONFLICT" => "durable state conflicts with this request",
        "DATA_CORRUPT" => "stored data failed integrity checks",
        "CANCELLED" => "job was cancelled",
        "UNVERIFIED_MARKET_RULES" => "market rule coverage is unverified",
        "ACCOUNTING_INVARIANT_FAILURE" => "independent accounting verification failed",
        "OUTCOME_UNKNOWN" => "durable outcome requires ID read-back",
        "CONTRACT_PARSE" => "upstream response violated the data contract",
        _ => "internal job failure",
    }
    .to_owned()
}

fn public_job(mut job: JobRecord) -> Result<PublicJobRecord, LabError> {
    let total_attempts = u64::try_from(job.attempts.len()).map_err(|_| {
        LabError::ResourceLimit("job attempt count exceeds platform capacity".into())
    })?;
    for attempt in &mut job.attempts {
        sanitize_attempt(attempt);
    }
    let truncated_reason = if job.attempts.len() > MAX_PUBLIC_JOB_ATTEMPTS {
        let remove = job.attempts.len() - MAX_PUBLIC_JOB_ATTEMPTS;
        job.attempts.drain(..remove);
        Some(format!(
            "older attempts omitted; latest {MAX_PUBLIC_JOB_ATTEMPTS} returned"
        ))
    } else {
        None
    };
    let artifact_catalog_query = match &job.payload {
        JobPayload::Export { run_id, .. } => Some(crate::contracts::ArtifactQuery::List {
            run_id: run_id.clone(),
        }),
        _ => None,
    };
    Ok(PublicJobRecord {
        job,
        total_attempts,
        truncated_reason,
        artifacts: Vec::new(),
        artifact_catalog_query,
    })
}

async fn public_job_result(
    result: Result<JobRecord, LabError>,
    database: &DatabaseHandle,
) -> Result<CallToolResult, McpError> {
    let mut public = match result.and_then(public_job) {
        Ok(job) => job,
        Err(error) => return tool_error(&error),
    };
    if let Some(crate::contracts::ArtifactQuery::List { run_id }) = &public.artifact_catalog_query {
        let ids = public
            .job
            .attempts
            .last()
            .and_then(|attempt| match &attempt.state {
                AttemptState::Completed {
                    output: crate::contracts::JobOutput::Artifacts { artifact_ids },
                    ..
                }
                | AttemptState::Partial {
                    output: crate::contracts::JobOutput::Artifacts { artifact_ids },
                    ..
                } => Some(artifact_ids.clone()),
                _ => None,
            });
        if let Some(ids) = ids {
            let run = run_id.clone();
            let result = database
                .call("export_artifact_metadata", move |store| {
                    let mut artifacts = store.artifact_catalog(&run)?;
                    artifacts.retain(|artifact| ids.contains(&artifact.artifact_id));
                    if artifacts.len() != ids.len() {
                        return Err(LabError::DataCorrupt(
                            "completed export artifact catalog differs from output".into(),
                        ));
                    }
                    Ok(artifacts)
                })
                .await;
            public.artifacts = match result {
                Ok(value) => value,
                Err(error) => return tool_error(&error),
            };
        }
    }
    text_result(&public)
}

fn result_page_value(
    store: &crate::storage::Store,
    page: ResultPage,
) -> Result<serde_json::Value, LabError> {
    let (section, value) = match page {
        ResultPage::Signals(page) => ("signals", serde_json::to_value(page)?),
        ResultPage::Orders(page) => ("orders", serde_json::to_value(page)?),
        ResultPage::OrderEvents(page) => ("order_events", serde_json::to_value(page)?),
        ResultPage::Fills(page) => {
            let mut value = serde_json::to_value(&page)?;
            let records = value["records"].as_array_mut().ok_or_else(|| {
                LabError::Internal("serialized fill page has no record array".into())
            })?;
            if records.len() != page.records.len() {
                return Err(LabError::Internal(
                    "serialized fill page count changed".into(),
                ));
            }
            for (value, fill) in records.iter_mut().zip(&page.records) {
                value["price_cost_attribution_unit"] = serde_json::json!("KRW_PER_BASE_UNIT");
                value["price_difference_per_unit"] =
                    serde_json::to_value(fill.price_cost_attribution)?;
                value["embedded_price_cost_quote"] =
                    serde_json::to_value(crate::reporting::fill_price_cost_quote(fill)?)?;
                value["embedded_price_cost_quote_unit"] = serde_json::json!("KRW");
            }
            ("fills", value)
        }
        ResultPage::Episodes(page) => {
            let mut value = serde_json::to_value(&page)?;
            let records = value["records"].as_array_mut().ok_or_else(|| {
                LabError::Internal("serialized episode page has no record array".into())
            })?;
            if records.len() != page.records.len() {
                return Err(LabError::Internal(
                    "serialized episode page count changed".into(),
                ));
            }
            for (value, episode) in records.iter_mut().zip(&page.records) {
                value["exit_reason_meaning"] = serde_json::json!("EXECUTION_RESULT_LEGACY_NAME");
                value["exit_details"] = serde_json::to_value(store.episode_exit_details(episode)?)?;
            }
            ("episodes", value)
        }
        ResultPage::Equity(page) => ("equity", serde_json::to_value(page)?),
    };
    let mut result = serde_json::Map::new();
    result.insert("section".into(), serde_json::Value::String(section.into()));
    result.insert("page".into(), value);
    Ok(serde_json::Value::Object(result))
}

fn portfolio_result_value(
    store: &crate::storage::Store,
    query: ResultQueryParams,
) -> Result<serde_json::Value, LabError> {
    let value = match query {
        ResultQueryParams::PortfolioSummary { run_id } => store
            .portfolio_summary_projection(&run_id)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::PortfolioEquity {
            run_id,
            offset,
            limit,
        } => store
            .portfolio_equity(&run_id, offset, limit)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::PortfolioAllocations {
            run_id,
            offset,
            limit,
        } => store
            .portfolio_allocations(&run_id, offset, limit)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::PortfolioRebalances {
            run_id,
            offset,
            limit,
        } => store
            .portfolio_rebalances(&run_id, offset, limit)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::PortfolioContributions { run_id } => store
            .portfolio_contributions(&run_id)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::RegimeTimeline {
            run_id,
            offset,
            limit,
        } => store
            .portfolio_regime_timeline(&run_id, offset, limit)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::RegimeSummary { run_id } => store
            .portfolio_regime_summary(&run_id)?
            .map(serde_json::to_value)
            .transpose(),
        ResultQueryParams::Ledger { .. }
        | ResultQueryParams::Summary { .. }
        | ResultQueryParams::Costs { .. }
        | ResultQueryParams::EquityChart { .. } => {
            return Err(LabError::Internal(
                "ordinary results cannot enter the portfolio projection adapter".into(),
            ));
        }
    }?;
    value.ok_or_else(|| LabError::InvalidConfig("unknown portfolio run ID".into()))
}

fn tagged_schema<T: JsonSchema + 'static>() -> std::sync::Arc<JsonObject> {
    let mut schema = rmcp::handler::server::common::schema_for_type::<T>()
        .as_ref()
        .clone();
    let root_keys_supported = schema.keys().all(|key| {
        matches!(
            key.as_str(),
            "$defs" | "definitions" | "title" | "description" | "type" | "oneOf" | "$schema"
        )
    });
    let root_type_supported = schema
        .get("type")
        .is_none_or(|value| value.as_str() == Some("object"));
    let host_extensible_union = if !root_keys_supported || !root_type_supported {
        None
    } else {
        schema
            .get("oneOf")
            .and_then(serde_json::Value::as_array)
            .and_then(|branches| {
                if branches.is_empty() {
                    return None;
                }
                let mut union_fields = std::collections::BTreeSet::new();
                let mut branch_fields = Vec::with_capacity(branches.len());
                for branch in branches {
                    let branch_object = branch.as_object()?;
                    if branch_object.keys().any(|key| {
                        !matches!(
                            key.as_str(),
                            "type"
                                | "properties"
                                | "required"
                                | "additionalProperties"
                                | "title"
                                | "description"
                                | "$schema"
                        )
                    }) {
                        return None;
                    }
                    if !matches!(
                        branch_object.get("type"),
                        Some(serde_json::Value::String(kind)) if kind == "object"
                    ) || !matches!(
                        branch_object.get("additionalProperties"),
                        Some(serde_json::Value::Bool(false))
                    ) {
                        return None;
                    }
                    let properties = branch_object.get("properties")?.as_object()?;
                    let names = properties
                        .keys()
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>();
                    union_fields.extend(names.iter().cloned());
                    branch_fields.push(names);
                }
                let transformed_branches = branches
                    .iter()
                    .cloned()
                    .zip(branch_fields)
                    .map(|(mut branch, fields)| {
                        let branch_object = branch.as_object_mut()?;
                        branch_object.remove("additionalProperties");
                        let absent_fields = union_fields
                            .difference(&fields)
                            .map(|name| serde_json::json!({"required": [name]}))
                            .collect::<Vec<_>>();
                        if !absent_fields.is_empty() {
                            branch_object.insert(
                                "not".to_owned(),
                                serde_json::json!({"anyOf": absent_fields}),
                            );
                        }
                        Some(branch)
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((union_fields, transformed_branches))
            })
    };
    // Internally tagged enums are object-shaped at the wire even though Schemars
    // expresses their alternatives with oneOf and omits the common root type.
    schema.insert("type".into(), serde_json::Value::String("object".into()));
    if let Some((union_fields, branches)) = host_extensible_union {
        // Root closure allows host-added properties; absent-field guards keep
        // fields exclusive to another action invalid within each oneOf branch.
        let properties = union_fields
            .into_iter()
            .map(|name| (name, serde_json::Value::Object(JsonObject::new())))
            .collect();
        schema.insert("properties".into(), serde_json::Value::Object(properties));
        schema.insert(
            "additionalProperties".into(),
            serde_json::Value::Bool(false),
        );
        schema.insert("oneOf".into(), serde_json::Value::Array(branches));
    }
    std::sync::Arc::new(schema)
}

#[expect(
    dead_code,
    reason = "schema-only MCP input projection; runtime validation stays on PaperReviewQuery"
)]
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum PaperReviewToolInput {
    Decision {
        selection: crate::contracts::PaperReviewSelection,
    },
    AuditRules {
        selection: crate::contracts::PaperReviewSelection,
    },
    AuditIndicators {
        selection: crate::contracts::PaperReviewSelection,
        max_source_bars: u32,
    },
    CompareContract {
        selection: crate::contracts::PaperReviewSelection,
        contract: crate::contracts::MachineContractComparisonInput,
    },
    Counterfactual {
        request: PaperReviewCounterfactualInput,
    },
}

#[expect(
    dead_code,
    reason = "opaque counterfactual request objects exist here only to generate the MCP schema"
)]
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PaperReviewCounterfactualInput {
    #[schemars(length(
        min = 1,
        max = crate::contracts::MAX_COUNTERFACTUAL_CANDIDATES
    ))]
    candidates: Vec<std::collections::BTreeMap<String, serde_json::Value>>,
    #[schemars(length(
        min = 1,
        max = crate::contracts::MAX_COUNTERFACTUAL_VARIANTS
    ))]
    variants: Vec<std::collections::BTreeMap<String, serde_json::Value>>,
}

/// MCP-only schema projection. Runtime validation continues to use `PaperReviewQuery`.
#[must_use]
pub fn paper_review_tool_input_schema() -> std::sync::Arc<JsonObject> {
    tagged_schema::<PaperReviewToolInput>()
}

/// Host-extensible schema over the same typed contract used by saved-fact analysis.
#[must_use]
pub fn decision_analysis_tool_input_schema() -> std::sync::Arc<JsonObject> {
    tagged_schema::<DecisionAnalysisQuery>()
}

#[derive(Clone, Copy)]
enum PaperQueryDiagnosticTool {
    Session,
    Review,
}

impl PaperQueryDiagnosticTool {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "paper_session_query",
            Self::Review => "paper_review_query",
        }
    }
}

#[derive(Clone, Copy)]
enum PaperQueryDiagnosticAction {
    StartOptions,
    List,
    Get,
    Events,
    ReviewEvents,
    Request,
    Decision,
    AuditRules,
    AuditIndicators,
    CompareContract,
    Counterfactual,
    Other,
    Missing,
}

impl PaperQueryDiagnosticAction {
    fn from_arguments(tool: PaperQueryDiagnosticTool, arguments: &JsonObject) -> Self {
        let Some(action) = arguments.get("action") else {
            return Self::Missing;
        };
        let Some(action) = action.as_str() else {
            return Self::Other;
        };
        match (tool, action) {
            (PaperQueryDiagnosticTool::Session, "start_options") => Self::StartOptions,
            (PaperQueryDiagnosticTool::Session, "list") => Self::List,
            (PaperQueryDiagnosticTool::Session, "get") => Self::Get,
            (PaperQueryDiagnosticTool::Session, "events") => Self::Events,
            (PaperQueryDiagnosticTool::Session, "review_events") => Self::ReviewEvents,
            (PaperQueryDiagnosticTool::Session, "request") => Self::Request,
            (PaperQueryDiagnosticTool::Review, "decision") => Self::Decision,
            (PaperQueryDiagnosticTool::Review, "audit_rules") => Self::AuditRules,
            (PaperQueryDiagnosticTool::Review, "audit_indicators") => Self::AuditIndicators,
            (PaperQueryDiagnosticTool::Review, "compare_contract") => Self::CompareContract,
            (PaperQueryDiagnosticTool::Review, "counterfactual") => Self::Counterfactual,
            _ => Self::Other,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::StartOptions => "start_options",
            Self::List => "list",
            Self::Get => "get",
            Self::Events => "events",
            Self::ReviewEvents => "review_events",
            Self::Request => "request",
            Self::Decision => "decision",
            Self::AuditRules => "audit_rules",
            Self::AuditIndicators => "audit_indicators",
            Self::CompareContract => "compare_contract",
            Self::Counterfactual => "counterfactual",
            Self::Other => "other",
            Self::Missing => "missing",
        }
    }
}

fn paper_query_selection_shape(value: Option<&serde_json::Value>) -> &'static str {
    match value {
        Some(serde_json::Value::Object(_)) => "object",
        Some(serde_json::Value::Null) => "null",
        Some(_) => "other",
        None => "missing",
    }
}

fn paper_query_number_repr(value: Option<&serde_json::Value>) -> &'static str {
    match value {
        Some(serde_json::Value::Number(number)) if number.is_u64() => "unsigned_integer",
        Some(serde_json::Value::Number(number)) if number.is_i64() => "signed_integer",
        Some(serde_json::Value::Number(number)) if number.is_f64() => "floating_point",
        Some(serde_json::Value::String(_)) => "string",
        Some(serde_json::Value::Null) => "null",
        Some(_) => "other",
        None => "missing",
    }
}

struct PaperQueryTraceContext<'a> {
    request_id: &'a str,
    origin: &'static str,
    protocol_header: &'static str,
}

impl<'a> PaperQueryTraceContext<'a> {
    fn from_request_context(context: &'a RequestContext<RoleServer>) -> Self {
        let Some(parts) = context.extensions.get::<axum::http::request::Parts>() else {
            return Self {
                request_id: "unavailable",
                origin: "local",
                protocol_header: "unavailable",
            };
        };
        let request_id = parts
            .extensions
            .get::<crate::transport::RequestCorrelationId>()
            .map_or("unavailable", |correlation| correlation.0.as_ref());
        let protocol_header = match parts.headers.get("mcp-protocol-version") {
            None => "missing",
            Some(value) => match value.to_str() {
                Ok("2025-03-26") => "known_2025_03_26",
                Ok(_) => "other",
                Err(_) => "invalid",
            },
        };
        Self {
            request_id,
            origin: "http",
            protocol_header,
        }
    }
}

#[derive(Clone, Copy)]
struct PaperQueryDiagnosticShape {
    tool: PaperQueryDiagnosticTool,
    action: PaperQueryDiagnosticAction,
    selection: &'static str,
    event_sequence: &'static str,
    as_of_sequence: &'static str,
    after_sequence: &'static str,
    limit: &'static str,
}

impl PaperQueryDiagnosticShape {
    fn from_arguments(tool: PaperQueryDiagnosticTool, arguments: &JsonObject) -> Self {
        let action = PaperQueryDiagnosticAction::from_arguments(tool, arguments);
        match tool {
            PaperQueryDiagnosticTool::Review => {
                let selection = arguments.get("selection");
                let selection_object = selection.and_then(serde_json::Value::as_object);
                Self {
                    tool,
                    action,
                    selection: paper_query_selection_shape(selection),
                    event_sequence: paper_query_number_repr(
                        selection_object.and_then(|selection| selection.get("event_sequence")),
                    ),
                    as_of_sequence: paper_query_number_repr(
                        selection_object.and_then(|selection| selection.get("as_of_sequence")),
                    ),
                    after_sequence: "not_applicable",
                    limit: "not_applicable",
                }
            }
            PaperQueryDiagnosticTool::Session => Self {
                tool,
                action,
                selection: "not_applicable",
                event_sequence: "not_applicable",
                as_of_sequence: paper_query_number_repr(arguments.get("as_of_sequence")),
                after_sequence: paper_query_number_repr(arguments.get("after_sequence")),
                limit: paper_query_number_repr(arguments.get("limit")),
            },
        }
    }
}

#[derive(Clone, Copy)]
enum PaperQueryDiagnosticStage {
    ParseError,
    ValidationError,
    Dispatch,
}

impl PaperQueryDiagnosticStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ParseError => "parse_error",
            Self::ValidationError => "validation_error",
            Self::Dispatch => "dispatch",
        }
    }
}

#[derive(Clone, Copy)]
enum PaperQueryDiagnosticOutcome {
    InvalidParams,
    Success,
    ToolError,
    RpcError,
}

impl PaperQueryDiagnosticOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidParams => "invalid_params",
            Self::Success => "success",
            Self::ToolError => "tool_error",
            Self::RpcError => "rpc_error",
        }
    }
}

fn record_paper_query_entry(
    context: &RequestContext<RoleServer>,
    shape: PaperQueryDiagnosticShape,
) {
    let trace_context = PaperQueryTraceContext::from_request_context(context);
    tracing::info!(
        target: "spot_lab::mcp::paper_query_diagnostics",
        event = "paper_query_entry",
        request_id = trace_context.request_id,
        context_origin = trace_context.origin,
        protocol_version_header = trace_context.protocol_header,
        tool = shape.tool.as_str(),
        action = shape.action.as_str(),
        selection_shape = shape.selection,
        event_sequence_repr = shape.event_sequence,
        as_of_sequence_repr = shape.as_of_sequence,
        after_sequence_repr = shape.after_sequence,
        limit_repr = shape.limit,
        elapsed_ms = 0_u64,
    );
}

fn record_paper_query_completion(
    context: &RequestContext<RoleServer>,
    shape: PaperQueryDiagnosticShape,
    stage: PaperQueryDiagnosticStage,
    outcome: PaperQueryDiagnosticOutcome,
    started_at: std::time::Instant,
) {
    let trace_context = PaperQueryTraceContext::from_request_context(context);
    let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        target: "spot_lab::mcp::paper_query_diagnostics",
        event = "paper_query_completion",
        request_id = trace_context.request_id,
        context_origin = trace_context.origin,
        protocol_version_header = trace_context.protocol_header,
        tool = shape.tool.as_str(),
        action = shape.action.as_str(),
        stage = stage.as_str(),
        outcome = outcome.as_str(),
        elapsed_ms,
    );
}

/// Credential-free projection of the transport owner's authentication decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpExposure {
    Local,
    Desktop,
    PublicNoAuth,
    Bearer,
    OAuth,
}

impl McpExposure {
    /// Stable wire name used by `lab_status.transport.auth`.
    #[must_use]
    pub const fn auth_name(self) -> &'static str {
        match self {
            Self::Local | Self::PublicNoAuth => "none",
            Self::Desktop => "desktop_bearer",
            Self::Bearer => "bearer",
            Self::OAuth => "oauth",
        }
    }

    /// Whether the server owns an explicitly isolated public research root.
    #[must_use]
    pub const fn public_no_auth(self) -> bool {
        matches!(self, Self::PublicNoAuth)
    }
}

#[derive(Clone)]
pub struct LabMcpService {
    tool_router: ToolRouter<Self>,
    upbit: UpbitClient,
    upbit_exchange: UpbitExchangeService,
    trading_control: TradingControlService,
    live_order_control: LiveOrderControlService,
    database: DatabaseHandle,
    jobs: JobService,
    exposure: McpExposure,
    remote_trading_enabled: bool,
    managed_send_gate_bound: bool,
    oauth_resource_metadata_url: Option<String>,
}

#[tool_router]
impl LabMcpService {
    /// Build the stateless adapter from process-wide storage, HTTP and job owners.
    #[cfg(test)]
    #[must_use]
    pub fn new(
        database: DatabaseHandle,
        upbit: UpbitClient,
        jobs: JobService,
        exposure: McpExposure,
    ) -> Self {
        let upbit_exchange = UpbitExchangeService::new_mock();
        Self::new_with_exchange(database, upbit, jobs, exposure, upbit_exchange)
    }

    /// Build an adapter around the process-owned exchange service.
    #[must_use]
    pub fn new_with_exchange(
        database: DatabaseHandle,
        upbit: UpbitClient,
        jobs: JobService,
        exposure: McpExposure,
        upbit_exchange: UpbitExchangeService,
    ) -> Self {
        let managed_send_gate_bound = upbit_exchange.live_order_send_gate().is_some();
        let mut tool_router = Self::tool_router();
        if matches!(exposure, McpExposure::Desktop) {
            tool_router
                .map
                .retain(|name, _route| DESKTOP_TOOL_NAMES.contains(&name.as_ref()));
        }
        if matches!(exposure, McpExposure::OAuth) {
            for route in tool_router.map.values_mut() {
                declare_oauth_tool_scopes(route, &["mcp"]);
            }
        }
        let mut trading_control = TradingControlService::production_with_policy(
            database.clone(),
            upbit_exchange.risk_guard().clone(),
            upbit_exchange.exchange_read_mode(),
            upbit_exchange.live_execution_policy(),
        );
        if let Some(gate) = upbit_exchange.live_order_send_gate() {
            trading_control = trading_control.with_live_order_send_gate(gate);
        }
        let live_order_control = LiveOrderControlService::new(
            database.clone(),
            upbit_exchange.live_execution_policy(),
            upbit_exchange.live_order_send_gate(),
            upbit_exchange.exchange_read_mode(),
        );
        Self {
            tool_router,
            upbit,
            upbit_exchange,
            trading_control,
            live_order_control,
            database,
            jobs,
            exposure,
            remote_trading_enabled: false,
            managed_send_gate_bound,
            oauth_resource_metadata_url: None,
        }
    }

    /// Provide a custom or pre-configured Upbit exchange service.
    #[must_use]
    pub fn with_exchange(mut self, exchange: UpbitExchangeService) -> Self {
        self.managed_send_gate_bound = exchange.live_order_send_gate().is_some();
        self.trading_control = TradingControlService::production_with_policy(
            self.database.clone(),
            exchange.risk_guard().clone(),
            exchange.exchange_read_mode(),
            exchange.live_execution_policy(),
        );
        if let Some(gate) = exchange.live_order_send_gate() {
            self.trading_control = self.trading_control.with_live_order_send_gate(gate);
        }
        self.live_order_control = LiveOrderControlService::new(
            self.database.clone(),
            exchange.live_execution_policy(),
            exchange.live_order_send_gate(),
            exchange.exchange_read_mode(),
        );
        self.upbit_exchange = exchange;
        self
    }

    /// Enable remote trading scopes using the OAuth owner's validated metadata URL.
    #[must_use]
    pub fn with_remote_trading_oauth(mut self, resource_metadata_url: String) -> Self {
        self.remote_trading_enabled = true;
        self.oauth_resource_metadata_url = Some(resource_metadata_url);
        for name in TRADING_READ_TOOL_NAMES {
            if let Some(route) = self.tool_router.map.get_mut(*name) {
                declare_oauth_tool_scopes(route, &["mcp", "trading:read"]);
            }
        }
        for name in TRADING_CONTROL_TOOL_NAMES {
            if let Some(route) = self.tool_router.map.get_mut(*name) {
                declare_oauth_tool_scopes(route, &["mcp", "trading:read", "trading:control"]);
            }
        }
        self
    }

    fn require_trading_read(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<(), TradingPermissionError> {
        self.require_trading_permission(context, false)
    }

    fn require_trading_permission(
        &self,
        context: &RequestContext<RoleServer>,
        control: bool,
    ) -> Result<(), TradingPermissionError> {
        if matches!(self.exposure, McpExposure::Desktop) {
            return Ok(());
        }
        let permissions = context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<OAuthPermissions>())
            .copied();
        if self.remote_trading_enabled && matches!(self.exposure, McpExposure::OAuth) {
            if permissions.is_some_and(|grant| {
                if control {
                    grant.can_control_trading()
                } else {
                    grant.can_read_trading()
                }
            }) {
                return Ok(());
            }
            return Err(TradingPermissionError::InsufficientScope(if control {
                TradingScope::Control
            } else {
                TradingScope::Read
            }));
        }
        Err(TradingPermissionError::Unavailable)
    }

    fn trading_permission_error(
        &self,
        error: TradingPermissionError,
    ) -> Result<CallToolResult, McpError> {
        match error {
            TradingPermissionError::Unavailable => {
                tool_error(&LabError::Conflict(TRADING_ACCESS_DENIED_MESSAGE.into()))
            }
            TradingPermissionError::InsufficientScope(scope) => {
                let Some(resource_metadata_url) = self.oauth_resource_metadata_url.as_deref()
                else {
                    return tool_error(&LabError::Conflict(TRADING_ACCESS_DENIED_MESSAGE.into()));
                };
                Ok(insufficient_scope_tool_error(resource_metadata_url, scope))
            }
        }
    }

    fn require_private_exchange_access(&self) -> Result<(), LabError> {
        if matches!(self.exposure, McpExposure::Desktop) {
            Ok(())
        } else {
            Err(LabError::Conflict(
                "private Upbit account, order and control tools require the native desktop capability".into(),
            ))
        }
    }

    #[tool(
        description = "Current market connection health and authorized private feed health; no account facts or credentials",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn realtime_status(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mut status = self.jobs.realtime_status();
        if self.require_trading_read(&context).is_err() {
            status.private = crate::contracts::RealtimeFeedStatus::unavailable();
        }
        text_result(&status)
    }

    #[tool(
        description = "Lab version, implemented capabilities, exposure mode and fixed resource limits",
        annotations(
            title = "Lab Status",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn lab_status(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let jobs = match self.jobs.status().await {
            Ok(status) => status,
            Err(error) => return tool_error(&error),
        };
        let storage = match self
            .database
            .call("storage_accounting", move |store| {
                store.storage_accounting()
            })
            .await
        {
            Ok(value) => value,
            Err(error) => return tool_error(&error),
        };
        let verification_receipt = crate::verification::verified_build_receipt();
        let implementation_status = if verification_receipt.is_some() {
            "VERIFIED"
        } else {
            "IMPLEMENTED_UNVERIFIED"
        };
        let status = serde_json::json!({
            "lab": "upbit-spot-lab",
            "package": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "code_revision": build_git_revision(),
            "source_digest": env!("SPOT_LAB_SOURCE_SHA256"),
            "lockfile_digest": env!("SPOT_LAB_LOCK_SHA256"),
            "toolchain": env!("SPOT_LAB_TOOLCHAIN"),
            "schema_version": crate::contracts::SCHEMA_VERSION,
            "engine_version": crate::contracts::ENGINE_VERSION,
            "rounding_version": crate::contracts::ROUNDING_VERSION,
            "normalizer_version": crate::contracts::NORMALIZER_VERSION,
            "sqlite_version": rusqlite::version(),
            "implementation_status": implementation_status,
            "verification_receipt": verification_receipt,
            "implemented": {
                "contracts": true,
                "market_data_upbit": true,
                "storage_sqlite": true,
                "strategies_engine": true,
                "reporting_export_verify": true,
                "durable_jobs": true,
                "policy_authoring": true,
                "execution_history": true,
                "typed_mcp": true,
                "upbit_private_read_mcp": true,
                "upbit_order_preview": true,
                "upbit_order_test": true,
                "upbit_risk_guard": true,
                "upbit_human_approval_issuance": false,
                "upbit_human_approval_verification": true,
                "upbit_order_reconcile": true,
            },
            "scope": {
                "venue": "upbit",
                "markets": "any listed Upbit KRW-<SYMBOL> market (validated against the live market catalog at collection admission)",
                "quote": "KRW",
                "position_types": "long/cash only",
                "private_api": if matches!(self.exposure, McpExposure::Desktop) {
                    "DESKTOP_READ_ONLY_AND_CONTROL"
                } else if self.require_trading_permission(&context, true).is_ok() {
                    "SCOPED_OAUTH_TRADING_READ_AND_CONTROL"
                } else if self.require_trading_read(&context).is_ok() {
                    "SCOPED_OAUTH_TRADING_READ"
                } else {
                    "UNAVAILABLE_ON_THIS_EXPOSURE"
                },
                "execution_origin": if self.upbit_exchange.live_trading_enabled() { "MOCK_AND_GATED_LIVE_AUTOMATION" } else { "SIMULATED_ONLY" },
                "fill_observed": false,
                "upbit_private_mode": if self.upbit_exchange.client().is_live() { "LIVE_READ" } else { "MOCK" },
                "live_trading_enabled": self.upbit_exchange.live_trading_enabled(),
            },
            "transport": {
                "auth": self.exposure.auth_name(),
                "public_no_auth": self.exposure.public_no_auth(),
                "progress_stream": false,
                "realtime_status_stream": "/realtime/events",
                "realtime_stream_semantics": "latest health snapshots; durable results remain queryable",
                "job_results": "poll job_control by opaque job_id",
            },
            "deletion": {
                "contract": "two_step_resource_delete_preview_then_resource_hard_delete",
                "preview_ttl_seconds": crate::contracts::DELETE_PREVIEW_TTL_SECONDS,
                "shared_by_all_callers": false,
                "mode": if matches!(self.exposure, McpExposure::Desktop) {
                    "PREVIEW_ONLY"
                } else if self.exposure.public_no_auth() {
                    "DISABLED_IN_PUBLIC_NO_AUTH"
                } else {
                    "HARD_DELETE_ROWS_AND_EXCLUSIVE_FILES"
                },
                "public_no_auth_warning": if self.exposure.public_no_auth() {
                    "destructive operations are disabled in public unauthenticated mode"
                } else {
                    ""
                },
            },
            "policy_deletion": {
                "mode": if self.exposure.public_no_auth() {
                    "DISABLED_IN_PUBLIC_NO_AUTH"
                } else {
                    "POLICY_ONLY_CONFIRMED"
                },
                "max_items": crate::contracts::MAX_POLICY_DELETE_ITEMS,
                "archive_available": false,
                "request_readback": true,
                "confirmation_required": true,
            },
            "jobs": jobs,
            "storage": storage,
            "limits": {
                "database_command_capacity": 32,
                "dataset_page_records": MAX_DATASET_PAGE_RECORDS,
                "result_page_records": 100,
                "history_page_records": 100,
                "public_job_attempts": MAX_PUBLIC_JOB_ATTEMPTS,
                "request_limits": crate::contracts::active_limits(),
                "artifact_chunk_bytes": crate::contracts::MAX_ARTIFACT_CHUNK_BYTES,
                "markets": 3,
                "models": crate::contracts::MAX_MODELS,
                "policy_selections": 7,
            }
        });
        text_result(&status)
    }

    #[tool(
        description = "Fetch completed candles from the fixed Upbit public quotation endpoint",
        input_schema = rmcp::handler::server::common::schema_for_type::<ProbeParams>(),
        annotations(
            title = "Probe Upbit",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn probe_upbit(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: ProbeParams = parse_arguments(arguments)?;
        let market =
            MarketId::parse_upbit(&params.market).map_err(|error| invalid_params(&error))?;
        let interval =
            CandleInterval::parse_code(&params.interval).map_err(|error| invalid_params(&error))?;
        let database = params.save_raw.then_some(&self.database);
        match self
            .upbit
            .fetch_completed_candles(&market, interval, params.count, database)
            .await
        {
            Ok(report) => text_result(&report),
            Err(error) => tool_error(&error),
        }
    }

    #[tool(
        description = "Submit a bounded durable Upbit public-data collection job",
        input_schema = rmcp::handler::server::common::schema_for_type::<CollectRequest>(),
        annotations(
            title = "Collect Data",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn collect_data(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let request: CollectRequest = parse_arguments(arguments)?;
        if self.exposure.public_no_auth() {
            return result_or_error::<()>(Err(LabError::Conflict(
                "data collection submission is disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        request
            .validate(crate::contracts::UtcTimestamp::now())
            .map_err(|error| invalid_params(&error))?;
        let submission = JobSubmission {
            request_id: request.request_id.clone(),
            payload: JobPayload::Collect { request },
        };
        public_job_result(self.jobs.submit(submission).await, &self.database).await
    }

    #[tool(
        description = "List dataset IDs or read one bounded immutable snapshot page",
        input_schema = rmcp::handler::server::common::schema_for_type::<DatasetQueryParams>(),
        annotations(
            title = "Dataset Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn dataset_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: DatasetQueryParams = parse_arguments(arguments)?;
        validate_page_limit(params.limit).map_err(|error| invalid_params(&error))?;
        let result = match params.action {
            DatasetQueryAction::List => {
                if params.dataset_id.is_some() || params.offset.is_some() {
                    return Err(invalid_params(&LabError::InvalidConfig(
                        "dataset list does not accept dataset_id or offset".into(),
                    )));
                }
                let limit = params.limit;
                self.database
                    .call("mcp_list_datasets", move |store| {
                        let ids = store.list_datasets(usize::try_from(limit).map_err(|_| {
                            LabError::ResourceLimit(
                                "dataset limit exceeds platform capacity".into(),
                            )
                        })?)?;
                        Ok(DatasetQueryResult::List {
                            returned_count: u64::try_from(ids.len()).map_err(|_| {
                                LabError::ResourceLimit(
                                    "dataset result count exceeds platform capacity".into(),
                                )
                            })?,
                            dataset_ids: ids,
                        })
                    })
                    .await
            }
            DatasetQueryAction::Get => {
                let dataset_id = params.dataset_id.ok_or_else(|| {
                    invalid_params(&LabError::InvalidConfig(
                        "dataset get requires dataset_id".into(),
                    ))
                })?;
                let offset = params.offset.unwrap_or(0);
                let limit = params.limit;
                self.database
                    .call("mcp_get_dataset", move |store| {
                        let snapshot = store
                            .load_dataset(&dataset_id)?
                            .ok_or_else(|| LabError::InvalidConfig("unknown dataset ID".into()))?;
                        dataset_page(snapshot, offset, limit)
                    })
                    .await
            }
        };
        result_or_error(result)
    }

    #[tool(
        description = "Validate and register a public non-sensitive immutable Evidence snapshot",
        input_schema = rmcp::handler::server::common::schema_for_type::<EvidenceImport>(),
        annotations(
            title = "Register Evidence",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn evidence_register(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let import: EvidenceImport = parse_arguments(arguments)?;
        if self.exposure.public_no_auth() {
            return result_or_error::<()>(Err(LabError::Conflict(
                "evidence registration is disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        let snapshot =
            crate::evidence::build_snapshot(import).map_err(|error| invalid_params(&error))?;
        let response = EvidenceRegistered {
            snapshot_id: snapshot.id.clone(),
            digest: snapshot.digest.clone(),
            version_count: u64::try_from(snapshot.versions.len())
                .map_err(|_| McpError::internal_error("Evidence version count overflow", None))?,
        };
        let saved = snapshot.clone();
        match self
            .database
            .call("mcp_register_evidence", move |store| {
                store.register_evidence_snapshot(&saved)
            })
            .await
        {
            Ok(_) => text_result(&response),
            Err(error) => tool_error(&error),
        }
    }

    #[tool(
        description = "Run and query shared-capital portfolio backtests over frozen plans",
        input_schema = tagged_schema::<crate::contracts::PortfolioAction>(),
        annotations(
            title = "Portfolio Backtest",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn portfolio_backtest(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let action: crate::contracts::PortfolioAction = parse_arguments(arguments)?;
        match action {
            crate::contracts::PortfolioAction::Create { request } => {
                let submission = JobSubmission {
                    request_id: request.request_id.clone(),
                    payload: JobPayload::PortfolioBacktest { request },
                };
                result_or_error(
                    self.jobs
                        .submit(submission)
                        .await
                        .and_then(|job| Ok(public_job(job)?.job)),
                )
            }
            crate::contracts::PortfolioAction::Get { run_id } => result_or_error(
                self.database
                    .call("portfolio_summary", move |store| {
                        store.portfolio_run_summary(&run_id)
                    })
                    .await,
            ),
            crate::contracts::PortfolioAction::Facts {
                run_id,
                kind,
                offset,
                limit,
            } => result_or_error(
                self.database
                    .call("portfolio_facts", move |store| {
                        store.portfolio_facts(&run_id, kind, offset, limit)
                    })
                    .await,
            ),
            crate::contracts::PortfolioAction::List { offset, limit } => result_or_error(
                self.database
                    .call("portfolio_list", move |store| {
                        store.list_portfolio_runs(offset, limit)
                    })
                    .await,
            ),
        }
    }

    #[tool(
        description = "Create or revise one validated immutable quant policy revision",
        input_schema = tagged_schema::<PolicyWrite>(),
        annotations(
            title = "Policy Write",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn policy_write(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let write: PolicyWrite = parse_arguments(arguments)?;
        if self.exposure.public_no_auth() && !matches!(&write, PolicyWrite::Preflight { .. }) {
            return result_or_error::<()>(Err(LabError::Conflict(
                "policy mutation is disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        match &write {
            PolicyWrite::Create { definition, .. } | PolicyWrite::Revise { definition, .. } => {
                definition
                    .validate()
                    .map_err(|error| invalid_params(&error))?;
            }
            // Sweep definitions are validated during expansion.
            PolicyWrite::Sweep { .. } | PolicyWrite::Preflight { .. } => {}
        }
        let now = crate::contracts::UtcTimestamp::now();
        let result = match write {
            PolicyWrite::Sweep {
                request_id,
                family,
                template,
                mode,
                research,
            } => {
                self.database
                    .call("mcp_policy_sweep", move |store| {
                        Ok(serde_json::to_value(store.sweep_policy(
                            &request_id,
                            family,
                            &template,
                            &mode,
                            research.as_ref(),
                            now,
                        )?)?)
                    })
                    .await
            }
            PolicyWrite::Preflight {
                family,
                template,
                mode,
                research,
                ..
            } => crate::contracts::preflight_parameter_sweep(
                family,
                &template,
                &mode,
                research.as_ref(),
            )
            .and_then(|report| serde_json::to_value(report).map_err(LabError::from)),
            write => {
                self.database
                    .call("mcp_policy_write", move |store| {
                        Ok(serde_json::to_value(store.write_policy(&write, now)?)?)
                    })
                    .await
            }
        };
        result_or_error(result)
    }

    #[tool(
        description = "List policies, read an exact revision, inspect revision history, or list runs using it",
        input_schema = tagged_schema::<PolicyQuery>(),
        annotations(
            title = "Policy Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn policy_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let query: PolicyQuery = parse_arguments(arguments)?;
        validate_policy_query(&query).map_err(|error| invalid_params(&error))?;
        let result = self
            .database
            .call("mcp_policy_query", move |store| match query {
                PolicyQuery::List {
                    after_policy_id,
                    limit,
                } => serde_json::to_value(store.list_policies(after_policy_id.as_ref(), limit)?)
                    .map_err(Into::into),
                PolicyQuery::Get { reference } => store
                    .load_policy_revision(&reference)?
                    .ok_or_else(|| LabError::InvalidConfig("unknown policy revision".into()))
                    .and_then(|revision| serde_json::to_value(revision).map_err(Into::into)),
                PolicyQuery::History {
                    policy_id,
                    before_revision_number,
                    limit,
                } => serde_json::to_value(store.policy_history(
                    &policy_id,
                    before_revision_number,
                    limit,
                )?)
                .map_err(Into::into),
                PolicyQuery::Runs {
                    reference,
                    cursor,
                    limit,
                } => serde_json::to_value(store.policy_runs(&reference, cursor.as_ref(), limit)?)
                    .map_err(Into::into),
            })
            .await;
        result_or_error(result)
    }

    #[tool(
        description = "Delete only explicitly confirmed USER policies whose current policy and owned-revision scope is unreferenced; every submitted item is revalidated and durably read back",
        input_schema = tagged_schema::<PolicyDeleteWriteRequest>(),
        annotations(
            title = "Policy Delete Write",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn policy_delete_write(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        if self.exposure.public_no_auth() {
            return result_or_error::<()>(Err(LabError::Conflict(
                "destructive operations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        let request: PolicyDeleteWriteRequest = parse_arguments(arguments)?;
        let admission_request = request.clone();
        let admission = self
            .database
            .call("policy_delete_request_admit", move |store| {
                store.begin_policy_delete_request(&admission_request)
            })
            .await;
        let admission = match admission {
            Ok(admission) => admission,
            Err(error) => return tool_error(&error),
        };
        match admission {
            PolicyDeleteAdmission::Rejected(error) => typed_tool_error(&error),
            PolicyDeleteAdmission::Existing(readback) => text_result(&readback),
            PolicyDeleteAdmission::Started => {
                let mut policy_ids = request
                    .items
                    .iter()
                    .map(|item| item.policy_id.clone())
                    .collect::<Vec<_>>();
                policy_ids.sort_unstable();
                for policy_id in policy_ids {
                    let request_id = request.request_id.clone();
                    if let Err(error) = self
                        .database
                        .call("policy_delete_item", move |store| {
                            store.apply_policy_delete_item(&request_id, &policy_id)
                        })
                        .await
                    {
                        // Do not retry an admitted item. The owner queue orders this
                        // typed readback after the uncertain operation if it is live.
                        let request_id = request.request_id.clone();
                        return match self
                            .database
                            .call("policy_delete_readback", move |store| {
                                store.policy_delete_readback(&request_id)
                            })
                            .await
                        {
                            Ok(readback) if readback.found => text_result(&readback),
                            _ => tool_error(&error),
                        };
                    }
                }
                let request_id = request.request_id;
                let readback = self
                    .database
                    .call("policy_delete_readback", move |store| {
                        store.policy_delete_readback(&request_id)
                    })
                    .await;
                result_or_error(readback)
            }
        }
    }

    #[tool(
        description = "Read one policy-deletion request by its durable request ID without resuming work",
        input_schema = tagged_schema::<PolicyDeleteQueryRequest>(),
        annotations(
            title = "Policy Delete Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn policy_delete_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let request: PolicyDeleteQueryRequest = parse_arguments(arguments)?;
        let readback = self
            .database
            .call("policy_delete_readback", move |store| {
                store.policy_delete_readback(&request.request_id)
            })
            .await;
        result_or_error(readback)
    }

    #[tool(
        description = "List bounded durable run or job history using stable opaque cursors",
        input_schema = tagged_schema::<HistoryQuery>(),
        annotations(
            title = "History Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn history_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let query: HistoryQuery = parse_arguments(arguments)?;
        validate_history_query(&query).map_err(|error| invalid_params(&error))?;
        let result = self
            .database
            .call("mcp_history_query", move |store| match query {
                HistoryQuery::Runs { cursor, limit } => {
                    serde_json::to_value(store.run_history(cursor.as_ref(), limit)?)
                        .map_err(Into::into)
                }
                HistoryQuery::Jobs { cursor, limit } => {
                    serde_json::to_value(store.job_history(cursor.as_ref(), limit)?)
                        .map_err(Into::into)
                }
            })
            .await;
        result_or_error(result)
    }

    #[tool(
        description = "Resolve and freeze an explicit backtest plan with immutable input digests",
        input_schema = rmcp::handler::server::common::schema_for_type::<PlanRequest>(),
        annotations(
            title = "Plan Backtest",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn plan_backtest(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let request: PlanRequest = parse_arguments(arguments)?;
        request
            .spec
            .validate()
            .map_err(|error| invalid_params(&error))?;
        let result = crate::planning::prepare(&self.database, request)
            .await
            .and_then(|plan| {
                let size = crate::planning::plan_size(&plan)?;
                let mut value = serde_json::to_value(plan)?;
                value["active_limits"] = serde_json::to_value(crate::contracts::active_limits())?;
                value["request_size"] = serde_json::to_value(size)?;
                Ok(value)
            });
        result_or_error(result)
    }

    #[tool(
        description = "Submit a durable backtest job for a frozen plan and exact input digest",
        input_schema = rmcp::handler::server::common::schema_for_type::<RunRequest>(),
        annotations(
            title = "Run Backtests",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn run_backtests(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let request: RunRequest = parse_arguments(arguments)?;
        let submission = JobSubmission {
            request_id: request.request_id.clone(),
            payload: JobPayload::Backtest { request },
        };
        public_job_result(self.jobs.submit(submission).await, &self.database).await
    }

    #[tool(
        description = "Get, request cancellation of, or retry one durable job by opaque ID",
        input_schema = rmcp::handler::server::common::schema_for_type::<JobControlParams>(),
        annotations(
            title = "Job Control",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn job_control(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: JobControlParams = parse_arguments(arguments)?;
        if self.exposure.public_no_auth()
            && matches!(
                params.action,
                JobControlAction::Cancel | JobControlAction::Retry
            )
        {
            return result_or_error::<()>(Err(LabError::Conflict(
                "job mutations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        let control = match params.action {
            JobControlAction::Get => JobControl::Get {
                job_id: params.job_id,
            },
            JobControlAction::Cancel => JobControl::Cancel {
                job_id: params.job_id,
            },
            JobControlAction::Retry => JobControl::Retry {
                job_id: params.job_id,
            },
        };
        public_job_result(self.jobs.control(control).await, &self.database).await
    }

    #[tool(
        description = "Read bounded model-ledger or shared-portfolio equity, allocation, attribution and regime results",
        input_schema = tagged_schema::<ResultQueryParams>(),
        annotations(
            title = "Result Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn result_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: ResultQueryParams = parse_arguments(arguments)?;
        match params {
            ResultQueryParams::Ledger { query } => {
                if query.limit == 0 || query.limit > 100 {
                    return Err(invalid_params(&LabError::InvalidConfig(
                        "result limit must be in 1..=100".into(),
                    )));
                }
                let result = self
                    .database
                    .call("mcp_result_query", move |store| {
                        result_page_value(store, store.query_result(&query)?)
                    })
                    .await;
                result_or_error(result)
            }
            ResultQueryParams::Summary { run_id } => {
                let result = self
                    .database
                    .call("mcp_run_summary", move |store| {
                        store
                            .load_run_summary(&run_id)?
                            .ok_or_else(|| LabError::InvalidConfig("unknown run ID".into()))
                    })
                    .await;
                result_or_error(result)
            }
            ResultQueryParams::Costs { run_id, model_id } => {
                let result = self
                    .database
                    .call("mcp_model_costs", move |store| {
                        store
                            .load_model_cost_summary(&run_id, &model_id)?
                            .ok_or_else(|| LabError::InvalidConfig("unknown run/model ID".into()))
                    })
                    .await;
                result_or_error(result)
            }
            ResultQueryParams::EquityChart {
                run_id,
                model_id,
                max_points,
            } => {
                let result = self
                    .database
                    .call("mcp_equity_chart", move |store| {
                        store.equity_chart_projection(&run_id, &model_id, max_points)
                    })
                    .await;
                result_or_error(result)
            }
            query @ (ResultQueryParams::PortfolioSummary { .. }
            | ResultQueryParams::PortfolioEquity { .. }
            | ResultQueryParams::PortfolioAllocations { .. }
            | ResultQueryParams::PortfolioRebalances { .. }
            | ResultQueryParams::PortfolioContributions { .. }
            | ResultQueryParams::RegimeTimeline { .. }
            | ResultQueryParams::RegimeSummary { .. }) => {
                let result = self
                    .database
                    .call("mcp_portfolio_result", move |store| {
                        portfolio_result_value(store, query)
                    })
                    .await;
                result_or_error(result)
            }
        }
    }

    #[tool(
        description = "Submit creation of a complete or one-market immutable report package",
        input_schema = rmcp::handler::server::common::schema_for_type::<ExportReportParams>(),
        annotations(
            title = "Export Report",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn export_report(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: ExportReportParams = parse_arguments(arguments)?;
        let submission = JobSubmission {
            request_id: params.request_id,
            payload: JobPayload::Export {
                run_id: params.run_id,
                market: params.market,
            },
        };
        public_job_result(self.jobs.submit(submission).await, &self.database).await
    }

    #[tool(
        description = "Receive full exported files: list metadata by run_id, then read pinned-hash HEX chunks using each retrieval.read_arguments and next_offset; concatenate bytes and verify the full SHA-256",
        input_schema = tagged_schema::<crate::contracts::ArtifactQuery>(),
        annotations(title = "Receive Exported Files", read_only_hint = true, destructive_hint = false, idempotent_hint = true, open_world_hint = false)
    )]
    async fn artifact_query(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        use crate::contracts::{
            ArtifactChunk, ArtifactEncoding, ArtifactQuery, ArtifactQueryResult,
            MAX_ARTIFACT_CHUNK_BYTES,
        };
        let query: ArtifactQuery = parse_arguments(arguments)?;
        match query {
            ArtifactQuery::List { run_id } => {
                let result = self
                    .database
                    .call("artifact_catalog", move |store| {
                        let artifacts = store.artifact_catalog(&run_id)?;
                        Ok(ArtifactQueryResult::Catalog {
                            run_id,
                            returned_count: u64::try_from(artifacts.len()).map_err(|_| {
                                LabError::ResourceLimit("artifact count overflow".into())
                            })?,
                            artifacts,
                        })
                    })
                    .await;
                result_or_error(result)
            }
            ArtifactQuery::Read {
                artifact_id,
                expected_sha256,
                offset,
                limit,
            } => {
                if !(1..=MAX_ARTIFACT_CHUNK_BYTES).contains(&limit) {
                    return Err(invalid_params(&LabError::InvalidConfig(
                        "artifact limit must be in 1..=131072 bytes".into(),
                    )));
                }
                let result = self
                    .database
                    .call("artifact_chunk", move |store| {
                        store.read_artifact_chunk(&artifact_id, &expected_sha256, offset, limit)
                    })
                    .await
                    .and_then(|read| {
                        let raw_bytes = u32::try_from(read.data.len())
                            .map_err(|_| LabError::ResourceLimit("chunk size overflow".into()))?;
                        let data_hex = encode_hex(&read.data);
                        Ok(ArtifactQueryResult::Chunk {
                            chunk: Box::new(ArtifactChunk {
                                artifact: read.artifact,
                                offset: read.offset,
                                raw_bytes,
                                encoding: ArtifactEncoding::Hex,
                                data_hex,
                                chunk_sha256: read.chunk_sha256,
                                next_offset: read.next_offset,
                            }),
                        })
                    });
                result_or_error(result)
            }
        }
    }

    #[tool(
        description = "Submit independent verification or replay for a catalogued export artifact",
        input_schema = rmcp::handler::server::common::schema_for_type::<VerifyRunParams>(),
        annotations(
            title = "Verify Run",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn verify_run(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: VerifyRunParams = parse_arguments(arguments)?;
        let submission = JobSubmission {
            request_id: params.request_id,
            payload: JobPayload::Verify {
                artifact_id: params.artifact_id,
                replay: params.replay,
            },
        };
        public_job_result(self.jobs.submit(submission).await, &self.database).await
    }

    #[tool(
        description = "Preview the exact irreversible hard-delete scope of one registered resource (policy, dataset, job, run, evidence snapshot, artifact): blockers, cascade dependents, retained shared rows, reclaimable files, expiring confirmation token",
        input_schema = tagged_schema::<DeleteResource>(),
        annotations(
            title = "Delete Preview",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn resource_delete_preview(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let resource: DeleteResource = parse_arguments(arguments)?;
        let result = self
            .database
            .call("resource_delete_preview", move |store| {
                store.delete_preview(&resource, crate::contracts::UtcTimestamp::now())
            })
            .await;
        result_or_error(result)
    }

    #[tool(
        description = "Execute the previewed hard delete before its token expires: business rows and exclusive files are really removed, live references refuse without cascade, queued/running jobs always refuse, and every caller of this instance shares the same contract",
        input_schema = tagged_schema::<HardDeleteRequest>(),
        annotations(
            title = "Hard Delete",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn resource_hard_delete(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let request: HardDeleteRequest = parse_arguments(arguments)?;
        if self.exposure.public_no_auth() {
            return result_or_error::<()>(Err(LabError::Conflict(
                "destructive operations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        let result = self
            .database
            .call("resource_hard_delete", move |store| {
                store.execute_hard_delete(&request, crate::contracts::UtcTimestamp::now())
            })
            .await;
        result_or_error(result)
    }

    #[tool(
        description = "Create, pause, resume or page durable batch/walk-forward suites. Candidates are exact policy revisions; selection uses past data, evaluation uses the committed winner, and results retain scenario/fold context.",
        input_schema = tagged_schema::<crate::contracts::ResearchSuiteAction>(),
        annotations(title = "Research Suite", read_only_hint = false, destructive_hint = false, idempotent_hint = false, open_world_hint = false)
    )]
    async fn research_suite(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let action: crate::contracts::ResearchSuiteAction = parse_arguments(arguments)?;
        if self.exposure.public_no_auth()
            && matches!(
                &action,
                crate::contracts::ResearchSuiteAction::Create { .. }
                    | crate::contracts::ResearchSuiteAction::Pause { .. }
                    | crate::contracts::ResearchSuiteAction::Resume { .. }
            )
        {
            return result_or_error::<()>(Err(LabError::Conflict(
                "research suite mutations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        match &action {
            crate::contracts::ResearchSuiteAction::Create { request } => {
                crate::research::expand_geometry(request).map_err(|e| invalid_params(&e))?;
            }
            crate::contracts::ResearchSuiteAction::List { limit, .. }
            | crate::contracts::ResearchSuiteAction::Cases { limit, .. }
            | crate::contracts::ResearchSuiteAction::Comparisons { limit, .. } => {
                crate::contracts::validate_research_page(*limit).map_err(|e| invalid_params(&e))?;
            }
            _ => {}
        }
        result_or_error(self.jobs.research_suite(action).await)
    }

    #[tool(
        description = "Manage persistent completed-candle collection schedules and per-market freshness. Cadence coalesces missed ticks; collection and bounded retries use the owned durable runner.",
        input_schema = tagged_schema::<crate::contracts::CollectionScheduleAction>(),
        annotations(title = "Collection Schedule", read_only_hint = false, destructive_hint = false, idempotent_hint = false, open_world_hint = true)
    )]
    async fn collection_schedule(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let action: crate::contracts::CollectionScheduleAction = parse_arguments(arguments)?;
        if self.exposure.public_no_auth()
            && matches!(
                &action,
                crate::contracts::CollectionScheduleAction::Create { .. }
                    | crate::contracts::CollectionScheduleAction::Pause { .. }
                    | crate::contracts::CollectionScheduleAction::Resume { .. }
            )
        {
            return result_or_error::<()>(Err(LabError::Conflict(
                "schedule mutations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        match &action {
            crate::contracts::CollectionScheduleAction::Create { request } => {
                request.validate().map_err(|e| invalid_params(&e))?;
            }
            crate::contracts::CollectionScheduleAction::List { limit, .. } => {
                crate::contracts::validate_research_page(*limit).map_err(|e| invalid_params(&e))?;
            }
            _ => {}
        }
        result_or_error(self.jobs.collection_schedule(action).await)
    }

    #[tool(
        description = "Inspect DB/raw/export/backup capacity, create a verified bounded backup, list backup receipts or propose retention candidates with the existing delete previews. This tool never deletes resources or accepts filesystem paths.",
        input_schema = tagged_schema::<crate::contracts::StorageMaintenanceAction>(),
        annotations(title = "Storage Maintenance", read_only_hint = false, destructive_hint = false, idempotent_hint = false, open_world_hint = false)
    )]
    async fn storage_maintenance(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let action: crate::contracts::StorageMaintenanceAction = parse_arguments(arguments)?;
        if matches!(
            &action,
            crate::contracts::StorageMaintenanceAction::HardDeleteBatch { .. }
                | crate::contracts::StorageMaintenanceAction::Compact
        ) && self.exposure.public_no_auth()
        {
            return result_or_error::<()>(Err(LabError::Conflict(
                "destructive or physical maintenance operations are disabled when public_no_auth is enabled; authenticated local or bearer access required".into(),
            )));
        }
        if let crate::contracts::StorageMaintenanceAction::RetentionCandidates { limit, .. } =
            &action
        {
            crate::contracts::validate_research_page(*limit).map_err(|e| invalid_params(&e))?;
        }
        result_or_error(self.jobs.storage_maintenance(action).await)
    }

    #[tool(
        description = "Query Upbit account balances and currency holdings",
        annotations(
            title = "Upbit Account Balances",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_account_balances(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        match self.upbit_exchange.client().get_accounts().await {
            Ok(balances) => {
                let out = serde_json::json!({
                    "venue": "upbit",
                    "mode": if self.upbit_exchange.client().is_live() { "LIVE_HTTP_READ" } else { "HERMETIC_MOCK" },
                    "observed_at_utc": chrono::Utc::now().to_rfc3339(),
                    "accounts_count": balances.len(),
                    "balances": balances,
                });
                text_result(&out)
            }
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Query Upbit market order chance, fee structure, and limits",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderChanceParams>(),
        annotations(
            title = "Upbit Order Chance",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_order_chance(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let params: UpbitOrderChanceParams = parse_arguments(arguments)?;
        match self
            .upbit_exchange
            .client()
            .get_order_chance(&params.market)
            .await
        {
            Ok(chance) => text_result(&chance),
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Query detailed information for an Upbit order by uuid or identifier",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderGetParams>(),
        annotations(
            title = "Upbit Order Get",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_order_get(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let params: UpbitOrderGetParams = parse_arguments(arguments)?;
        match self
            .upbit_exchange
            .client()
            .get_order(params.uuid.as_deref(), params.identifier.as_deref())
            .await
        {
            Ok(order) => text_result(&order),
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Query currently open orders on Upbit",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOpenOrdersParams>(),
        annotations(
            title = "Upbit Orders Open",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_orders_open(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let params: UpbitOpenOrdersParams = parse_arguments(arguments)?;
        let page = params.page.unwrap_or(1);
        let limit = params.limit.unwrap_or(100);
        match self
            .upbit_exchange
            .client()
            .get_open_orders(params.market.as_deref(), params.page, params.limit)
            .await
        {
            Ok(records) => {
                let returned_count = u64::try_from(records.len())
                    .map_err(|_| McpError::internal_error("open order count exceeds u64", None))?;
                let full = returned_count == u64::from(limit);
                text_result(&UpbitOrdersPage {
                    venue: "upbit",
                    exchange_read_mode: if self.upbit_exchange.client().is_live() {
                        "LIVE_READ"
                    } else {
                        "MOCK"
                    },
                    order_scope: "OPEN",
                    observed_at_utc: chrono::Utc::now().to_rfc3339(),
                    page: Some(page),
                    limit,
                    returned_count,
                    records,
                    next_page: (full && page < 10_000).then_some(page + 1),
                    truncated_reason: full.then_some("PAGE_LIMIT_REACHED"),
                    start_time: None,
                    end_time: None,
                })
            }
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Query closed/completed orders on Upbit",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitClosedOrdersParams>(),
        annotations(
            title = "Upbit Orders Closed",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_orders_closed(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let params: UpbitClosedOrdersParams = parse_arguments(arguments)?;
        let limit = params.limit.unwrap_or(100);
        let start_time = params.start_time.clone();
        let end_time = params.end_time.clone();
        match self
            .upbit_exchange
            .client()
            .get_closed_orders(
                params.market.as_deref(),
                params.start_time.as_deref(),
                params.end_time.as_deref(),
                params.limit,
            )
            .await
        {
            Ok(records) => {
                let returned_count = u64::try_from(records.len()).map_err(|_| {
                    McpError::internal_error("closed order count exceeds u64", None)
                })?;
                let full = returned_count == u64::from(limit);
                text_result(&UpbitOrdersPage {
                    venue: "upbit",
                    exchange_read_mode: if self.upbit_exchange.client().is_live() {
                        "LIVE_READ"
                    } else {
                        "MOCK"
                    },
                    order_scope: "CLOSED",
                    observed_at_utc: chrono::Utc::now().to_rfc3339(),
                    page: None,
                    limit,
                    returned_count,
                    records,
                    next_page: None,
                    truncated_reason: full.then_some("TIME_RANGE_OR_LIMIT_REACHED"),
                    start_time,
                    end_time,
                })
            }
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Deterministically preview and validate an order with tick rules, minimum notional (5,000 KRW), fees, and cryptographic intent digest",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderPreviewParams>(),
        annotations(
            title = "Upbit Order Preview",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn upbit_order_preview(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let params: UpbitOrderPreviewParams = parse_arguments(arguments)?;
        let price = params
            .price
            .as_deref()
            .map(std::str::FromStr::from_str)
            .transpose()
            .map_err(|e| upbit_invalid_params(format!("invalid price: {e}")))?;
        let volume = params
            .volume
            .as_deref()
            .map(std::str::FromStr::from_str)
            .transpose()
            .map_err(|e| upbit_invalid_params(format!("invalid volume: {e}")))?;
        let current_krw_balance = params
            .current_krw_balance
            .as_deref()
            .map(std::str::FromStr::from_str)
            .transpose()
            .map_err(|e| upbit_invalid_params(format!("invalid current_krw_balance: {e}")))?;
        let current_coin_balance = params
            .current_coin_balance
            .as_deref()
            .map(std::str::FromStr::from_str)
            .transpose()
            .map_err(|e| upbit_invalid_params(format!("invalid current_coin_balance: {e}")))?;

        let req = OrderPreviewRequest {
            market: params.market,
            side: params.side,
            ord_type: params.ord_type,
            price,
            volume,
            current_krw_balance,
            current_coin_balance,
        };

        match OrderPreview::build(req, chrono::Utc::now()) {
            Ok(preview) => text_result(&preview),
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Send official non-executing test order to POST /v1/orders/test (zero execution, zero balance change)",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderTestParams>(),
        annotations(
            title = "Upbit Order Test",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn upbit_order_test(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_private_exchange_access() {
            return tool_error(&error);
        }
        let params: UpbitOrderTestParams = parse_arguments(arguments)?;
        let req = UpbitTestOrderRequest {
            market: params.market,
            side: params.side,
            ord_type: params.ord_type,
            volume: params.volume,
            price: params.price,
            identifier: params.identifier,
        };
        match self.upbit_exchange.client().post_order_test(&req).await {
            Ok(response) => text_result(&response),
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Execute a Desktop simulated order intent behind risk and human approval checks; legacy raw LIVE execution remains disabled, and strategy execution uses separately gated automation",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderExecuteParams>(),
        annotations(
            title = "Upbit Order Execute",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn upbit_order_execute(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_private_exchange_access() {
            return tool_error(&error);
        }
        let params: UpbitOrderExecuteParams = parse_arguments(arguments)?;
        let preview: OrderPreview = serde_json::from_value(params.preview)
            .map_err(|e| upbit_invalid_params(format!("invalid preview payload: {e}")))?;
        let req = OrderExecuteRequest {
            preview,
            human_approval_token: params.human_approval_token,
            client_identifier: params.client_identifier,
            allow_mock_execution: params.allow_mock_execution,
        };
        match self
            .upbit_exchange
            .execution_service()
            .execute(req, chrono::Utc::now())
            .await
        {
            Ok(receipt) => text_result(&receipt),
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Reconcile order state, fills, partials, and residuals against exchange order details",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderReconcileParams>(),
        annotations(
            title = "Upbit Order Reconcile",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn upbit_order_reconcile(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_private_exchange_access() {
            return tool_error(&error);
        }
        let params: UpbitOrderReconcileParams = parse_arguments(arguments)?;
        let expected_volume = params
            .expected_volume
            .parse::<rust_decimal::Decimal>()
            .map_err(|e| upbit_invalid_params(format!("invalid expected_volume: {e}")))?;
        match self
            .upbit_exchange
            .client()
            .get_order(params.uuid.as_deref(), params.identifier.as_deref())
            .await
        {
            Ok(detail) => {
                let report = reconcile_order_detail(expected_volume, &detail);
                text_result(&report)
            }
            Err(error) => Ok(upbit_tool_error(&error)),
        }
    }

    #[tool(
        description = "Cancel order in mock execution ledger (real exchange DELETE is disabled)",
        input_schema = rmcp::handler::server::common::schema_for_type::<UpbitOrderCancelParams>(),
        annotations(
            title = "Upbit Order Cancel",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn upbit_order_cancel(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_private_exchange_access() {
            return tool_error(&error);
        }
        let params: UpbitOrderCancelParams = parse_arguments(arguments)?;
        let unfilled = params
            .unfilled_volume
            .as_deref()
            .map(std::str::FromStr::from_str)
            .transpose()
            .map_err(|e| upbit_invalid_params(format!("invalid unfilled_volume: {e}")))?;
        let receipt = mock_cancel_order(
            &params.order_id,
            unfilled.unwrap_or(rust_decimal::Decimal::ZERO),
        );
        text_result(&receipt)
    }

    #[tool(
        description = "Read paper start options, sessions, receipts or events. For query-only saved facts use review_events: pins as_of_sequence, returns original configuration and bounded events without polling/reconciliation. Echo as_of_sequence with after_sequence on continuation. Legacy list/get/events may advance paper processing. Policy AST is resolved by exact configuration.policy_ref; condition evaluation and independent strategy/indicator audits are not provided by review_events.",
        input_schema = tagged_schema::<PaperSessionQuery>(),
        annotations(
            title = "Paper Session Query",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn paper_session_query(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (query, shape, started_at) = parse_and_validate_paper_query::<PaperSessionQuery>(
            PaperQueryDiagnosticTool::Session,
            arguments,
            &context,
            validate_paper_session_query,
        )?;
        let response = result_or_error(self.jobs.paper_session_query(query).await);
        complete_paper_query_dispatch(&context, shape, started_at, &response);
        response
    }

    #[tool(
        description = "Read-only saved paper decision evidence, independent snapshot-rule/source-indicator audits, explicit machine-contract comparison, or virtual counterfactual scenarios. Pin event_sequence/as_of_sequence; missing historical trace/seed remains unknown. compare_contract requires CallerDeclared expectations and an empty observed array, resolved from the saved session. Counterfactual supplied source events/configuration/bars must exactly match stored facts. No polling, reconciliation, session/policy writes or order admission.",
        input_schema = paper_review_tool_input_schema(),
        annotations(
            title = "Paper Saved Evidence Review",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn paper_review_query(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (query, shape, started_at) = parse_and_validate_paper_query::<PaperReviewQuery>(
            PaperQueryDiagnosticTool::Review,
            arguments,
            &context,
            PaperReviewQuery::validate,
        )?;
        let response = result_or_error(self.jobs.paper_review_query(query).await);
        complete_paper_query_dispatch(&context, shape, started_at, &response);
        response
    }

    #[tool(
        description = "Read complete bounded period decision counts, exact matching saved decisions, original explanation, or previous/selected recorded-decision comparisons. Uses one explicit PAPER or AUTOMATION session and immutable policy identity. Follow-ups require the overview selection fence: paper event prefix or unchanged saved automation snapshot digest. Changed automation evidence requires explicit refresh. Distinguishes strategy/constrained actions, execution, nonexclusive reasons, condition occurrences and missing capture; no AI, replay, polling, reconciliation or order effects. AUTOMATION requires the existing private trading-read permission.",
        input_schema = decision_analysis_tool_input_schema(),
        annotations(
            title = "Saved Decision Analysis",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn decision_analysis_query(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let query: DecisionAnalysisQuery = parse_arguments(arguments)?;
        query.validate().map_err(|error| invalid_params(&error))?;
        if query.requires_trading_read()
            && let Err(error) = self.require_trading_read(&context)
        {
            return self.trading_permission_error(error);
        }
        decision_analysis_result(self.jobs.decision_analysis_query(query).await, &context.id)
    }

    #[tool(
        description = "Create-and-start, pause, resume, or stop a persistent virtual paper investment session with request_id idempotency and expected_sequence CAS",
        input_schema = tagged_schema::<PaperSessionWrite>(),
        annotations(
            title = "Paper Session Write",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn paper_session_write(&self, arguments: JsonObject) -> Result<CallToolResult, McpError> {
        let write: PaperSessionWrite = parse_arguments(arguments)?;
        if self.exposure.public_no_auth() {
            return result_or_error::<()>(Err(LabError::Conflict(
                "paper session mutation is disabled when public_no_auth is enabled; authenticated local, desktop, or bearer access required".into(),
            )));
        }
        result_or_error(self.jobs.paper_session_write(write).await)
    }

    #[tool(
        description = "Read private strategy automation sessions, daily starting baseline, exact decision candles and saved evaluations, linked order/fill events, deterministic performance reports, and hash-pinned CSV/JSON export chunks",
        input_schema = tagged_schema::<AutomationSessionQuery>(),
        annotations(
            title = "Strategy Automation Query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn automation_session_query(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let query: AutomationSessionQuery = parse_arguments(arguments)?;
        validate_automation_query(&query).map_err(|error| invalid_params(&error))?;
        result_or_error(self.jobs.automation_session_query(query).await)
    }

    #[tool(
        description = "Start a frozen-policy explicitly selected MOCK or LIVE strategy automation session, record/verify an account daily starting baseline, or pause, resume, stop, or explicitly cancel one owned mock order",
        input_schema = tagged_schema::<AutomationSessionWrite>(),
        annotations(
            title = "Strategy Automation Write",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn automation_session_write(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_permission(&context, true) {
            return self.trading_permission_error(error);
        }
        let write: AutomationSessionWrite = parse_arguments(arguments)?;
        if !matches!(self.exposure, McpExposure::Desktop)
            && !matches!(
                &write,
                AutomationSessionWrite::Start { .. }
                    | AutomationSessionWrite::Pause { .. }
                    | AutomationSessionWrite::Resume { .. }
                    | AutomationSessionWrite::Stop { .. }
            )
        {
            return tool_error(&LabError::Conflict("OAuth trading control permits only start, pause, resume and stop; order cancellation and baseline changes require Desktop authority".into()));
        }
        result_or_error(self.jobs.automation_session_write(write).await)
    }

    #[tool(
        description = "Read or explicitly enable/disable the shared live order-sending gate. Starts and restarts OFF in managed mode. Enable requires confirmation ENABLE_LIVE_ORDER_SENDING, request_id, expected_sequence and expected_epoch; disable blocks new sends and never cancels exchange orders. This does not start/resume strategies or clear emergency admission. Read a request receipt after uncertain replies.",
        input_schema = rmcp::handler::server::common::schema_for_type::<LiveOrderControlRequest>(),
        annotations(title = "Live Order Sending", read_only_hint = false, destructive_hint = false,
            idempotent_hint = true, open_world_hint = false)
    )]
    async fn live_order_control(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let request: LiveOrderControlRequest = parse_arguments(arguments)?;
        if request.action.is_mutation()
            && let Err(error) = self.require_trading_permission(&context, true)
        {
            return self.trading_permission_error(error);
        }
        result_or_error(self.live_order_control.execute(request).await)
    }

    #[tool(
        description = "Read or mutate the durable server-side trading admission control. Mutations require request_id and expected_sequence; Start/Resume require the independent operator effect gate and an explicitly authorized trading-control capability. Emergency stop only latches new-order admission and never submits or cancels exchange orders.",
        input_schema = rmcp::handler::server::common::schema_for_type::<TradingControlRequest>(),
        annotations(
            title = "Trading Control",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn trading_control(
        &self,
        arguments: JsonObject,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if let Err(error) = self.require_trading_read(&context) {
            return self.trading_permission_error(error);
        }
        let request: TradingControlRequest = parse_arguments(arguments)?;
        if request.action.is_mutation() {
            if let Err(error) = self.require_trading_permission(&context, true) {
                return self.trading_permission_error(error);
            }
            if matches!(self.exposure, McpExposure::OAuth | McpExposure::Desktop) {
                return result_or_error(
                    self.trading_control
                        .execute_with_operator_approval(request)
                        .await,
                );
            }
        }
        result_or_error(self.trading_control.execute(request).await)
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}

fn validate_page_limit(limit: u32) -> Result<(), LabError> {
    if limit == 0 || limit > MAX_DATASET_PAGE_RECORDS {
        Err(LabError::InvalidConfig(format!(
            "dataset limit must be in 1..={MAX_DATASET_PAGE_RECORDS}"
        )))
    } else {
        Ok(())
    }
}

fn validate_history_limit(limit: u32) -> Result<(), LabError> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(LabError::InvalidConfig(
            "history limit must be in 1..=100".into(),
        ))
    }
}

fn validate_policy_query(query: &PolicyQuery) -> Result<(), LabError> {
    match query {
        PolicyQuery::List { limit, .. }
        | PolicyQuery::History { limit, .. }
        | PolicyQuery::Runs { limit, .. } => validate_history_limit(*limit),
        PolicyQuery::Get { .. } => Ok(()),
    }
}

fn validate_paper_session_query(query: &PaperSessionQuery) -> Result<(), LabError> {
    match query {
        PaperSessionQuery::List { limit, .. }
        | PaperSessionQuery::Events { limit, .. }
        | PaperSessionQuery::ReviewEvents { limit, .. } => validate_history_limit(*limit),
        PaperSessionQuery::StartOptions
        | PaperSessionQuery::Get { .. }
        | PaperSessionQuery::Request { .. } => Ok(()),
    }
}

fn parse_and_validate_paper_query<T>(
    tool: PaperQueryDiagnosticTool,
    arguments: JsonObject,
    context: &RequestContext<RoleServer>,
    validate: fn(&T) -> Result<(), LabError>,
) -> Result<(T, PaperQueryDiagnosticShape, std::time::Instant), McpError>
where
    T: serde::de::DeserializeOwned,
{
    let started_at = std::time::Instant::now();
    let shape = PaperQueryDiagnosticShape::from_arguments(tool, &arguments);
    record_paper_query_entry(context, shape);
    let query: T = match parse_arguments(arguments) {
        Ok(query) => query,
        Err(error) => {
            record_paper_query_completion(
                context,
                shape,
                PaperQueryDiagnosticStage::ParseError,
                PaperQueryDiagnosticOutcome::InvalidParams,
                started_at,
            );
            return Err(error);
        }
    };
    if let Err(error) = validate(&query) {
        record_paper_query_completion(
            context,
            shape,
            PaperQueryDiagnosticStage::ValidationError,
            PaperQueryDiagnosticOutcome::InvalidParams,
            started_at,
        );
        return Err(invalid_params(&error));
    }
    Ok((query, shape, started_at))
}

fn complete_paper_query_dispatch(
    context: &RequestContext<RoleServer>,
    shape: PaperQueryDiagnosticShape,
    started_at: std::time::Instant,
    response: &Result<CallToolResult, McpError>,
) {
    let outcome = match response {
        Ok(result) if result.is_error == Some(true) => PaperQueryDiagnosticOutcome::ToolError,
        Ok(_) => PaperQueryDiagnosticOutcome::Success,
        Err(_) => PaperQueryDiagnosticOutcome::RpcError,
    };
    record_paper_query_completion(
        context,
        shape,
        PaperQueryDiagnosticStage::Dispatch,
        outcome,
        started_at,
    );
}

fn validate_automation_query(query: &AutomationSessionQuery) -> Result<(), LabError> {
    match query {
        AutomationSessionQuery::List { limit, .. }
        | AutomationSessionQuery::Events { limit, .. }
        | AutomationSessionQuery::Ledger { limit, .. } => validate_history_limit(*limit),
        AutomationSessionQuery::Decisions { limit, .. } => {
            if (1..=100).contains(limit) {
                Ok(())
            } else {
                Err(LabError::InvalidConfig(
                    "automation decision page limit must be in 1..=100".into(),
                ))
            }
        }
        AutomationSessionQuery::Get { .. }
        | AutomationSessionQuery::Request { .. }
        | AutomationSessionQuery::Baseline { .. }
        | AutomationSessionQuery::Decision { .. }
        | AutomationSessionQuery::Performance { .. }
        | AutomationSessionQuery::ExportManifest { .. }
        | AutomationSessionQuery::ExportRead { .. } => Ok(()),
    }
}

fn validate_history_query(query: &HistoryQuery) -> Result<(), LabError> {
    match query {
        HistoryQuery::Runs { limit, .. } | HistoryQuery::Jobs { limit, .. } => {
            validate_history_limit(*limit)
        }
    }
}

fn dataset_page(
    snapshot: crate::contracts::DatasetSnapshot,
    offset: u64,
    limit: u32,
) -> Result<DatasetQueryResult, LabError> {
    let offset = usize::try_from(offset)
        .map_err(|_| LabError::ResourceLimit("dataset offset exceeds platform capacity".into()))?;
    let limit = usize::try_from(limit)
        .map_err(|_| LabError::ResourceLimit("dataset limit exceeds platform capacity".into()))?;
    let total = snapshot.observations.len();
    let start = offset.min(total);
    let end = start.saturating_add(limit).min(total);
    let observations = snapshot.observations[start..end].to_vec();
    let next_offset = if end < total {
        Some(u64::try_from(end).map_err(|_| {
            LabError::ResourceLimit("dataset cursor exceeds platform capacity".into())
        })?)
    } else {
        None
    };
    Ok(DatasetQueryResult::Snapshot {
        manifest: Box::new(dataset_summary(snapshot.manifest)?),
        returned_count: u64::try_from(observations.len()).map_err(|_| {
            LabError::ResourceLimit("dataset page count exceeds platform capacity".into())
        })?,
        total_count: u64::try_from(total).map_err(|_| {
            LabError::ResourceLimit("dataset row count exceeds platform capacity".into())
        })?,
        truncated_reason: next_offset
            .is_some()
            .then(|| "page limit reached; continue with next_offset".into()),
        observations,
        next_offset,
    })
}

fn dataset_summary(manifest: DatasetManifest) -> Result<DatasetSummary, LabError> {
    let total_quality_issues = u64::try_from(manifest.quality_issues.len()).map_err(|_| {
        LabError::ResourceLimit("quality issue count exceeds platform capacity".into())
    })?;
    let raw_object_count = u64::try_from(manifest.raw_objects.len()).map_err(|_| {
        LabError::ResourceLimit("raw object count exceeds platform capacity".into())
    })?;
    let mut quality_issues = manifest.quality_issues;
    let quality_limit = usize::try_from(MAX_DATASET_PAGE_RECORDS)
        .map_err(|_| LabError::ResourceLimit("quality issue limit is unsupported".into()))?;
    let quality_truncated_reason = if quality_issues.len() > quality_limit {
        quality_issues.truncate(quality_limit);
        Some(format!(
            "quality issue limit reached; first {MAX_DATASET_PAGE_RECORDS} returned"
        ))
    } else {
        None
    };
    Ok(DatasetSummary {
        schema_version: manifest.schema_version,
        id: manifest.id,
        request: manifest.request,
        coverage: manifest.coverage,
        status: manifest.status,
        row_count: manifest.row_count,
        normalizer_version: manifest.normalizer_version,
        gap_policy: manifest.gap_policy,
        semantic_digest: manifest.semantic_digest,
        provenance_digest: manifest.provenance_digest,
        origin: manifest.origin,
        raw_object_count,
        reuse: manifest.reuse,
        quality_issues: quality_issues
            .iter()
            .map(crate::quality::classify_issue)
            .collect::<Result<_, _>>()?,
        total_quality_issues,
        quality_truncated_reason,
    })
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for LabMcpService {
    fn get_info(&self) -> ServerConfig {
        let instructions = if matches!(self.exposure, McpExposure::Desktop) {
            "Native desktop account/order history, frozen MOCK/LIVE strategy sessions, durable trading admission, and shared live_order_control. Managed order sending starts OFF on every restart; enabling requires explicit confirmation and never starts or resumes a strategy. Raw live execute/test/cancel, withdrawals, arbitrary SQL, caller URLs, paths, and code execution are unavailable."
        } else if matches!(self.exposure, McpExposure::OAuth)
            && self.remote_trading_enabled
            && self.managed_send_gate_bound
        {
            "Upbit research with owner-consented private account/order reads, supported strategy lifecycle, trading admission, and shared live_order_control. Managed full-trading OAuth consent turns the shared live-order send gate ON after owner-code approval and PKCE; this does not start or resume a strategy, and an already-running strategy may submit real orders. Every tool call still checks the verified grant, and risk and emergency controls remain separate. Raw live execute/test/cancel, withdrawals, arbitrary SQL, caller URLs, paths, and code execution are unavailable."
        } else if matches!(self.exposure, McpExposure::OAuth) && self.remote_trading_enabled {
            "Upbit research with owner-consented private account/order reads, supported strategy lifecycle and trading admission. The managed OAuth live-order send gate is not configured on this server, so OAuth consent does not turn it ON. Every tool call still checks the verified grant, and risk and emergency controls remain separate. Raw live execute/test/cancel, withdrawals, arbitrary SQL, caller URLs, paths, and code execution are unavailable."
        } else {
            "Upbit public-data research lab. Private account/order/control tools and live order execution are unavailable on this exposure. Quant policies use immutable revisions; durable jobs are polled by ID. Arbitrary SQL, caller URLs, paths, and code execution are disabled."
        };
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2025_06_18)
            .with_instructions(instructions.to_owned())
    }
}

/// Build-time git identity embedded by `build.rs`; `code_revision` never
/// collapses to null when the server runs outside a git checkout.
#[must_use]
pub fn build_git_revision() -> Option<String> {
    let value = env!("SPOT_LAB_GIT_REVISION");
    if value == "unknown" {
        None
    } else {
        Some(value.to_owned())
    }
}

/// Flatten a tool result into its text content blocks.
#[must_use]
pub fn result_text(result: CallToolResult) -> String {
    let mut text = String::new();
    for block in result.content {
        if let ContentBlock::Text(content) = block {
            text.push_str(&content.text);
        }
    }
    text
}

#[cfg(test)]
mod closure_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod trading_tests;
