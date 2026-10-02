//! Typed F09 MCP adapter over shared process-owned services.
//!
//! The adapter accepts only domain DTOs and opaque IDs. Long work is submitted to
//! the durable job runner; handlers never accept arbitrary SQL, URLs, paths, or code.

#![allow(clippy::unused_async_trait_impl)]

use crate::contracts::{
    ArtifactId, AttemptState, CandleInterval, CandleObservation, CollectRequest, ContentHash,
    DatasetId, DatasetManifest, DatasetStatus, DeleteResource, EvidenceImport, EvidenceSnapshotId,
    HardDeleteRequest, HistoryQuery, JobAttempt, JobControl, JobId, JobPayload, JobRecord,
    JobSubmission, LabError, MarketDataOrigin, MarketId, PlanRequest, PolicyQuery, PolicyWrite,
    ProbeCount, RequestId, ResultQuery, RunId, RunRequest, UtcRange,
};
use crate::database::DatabaseHandle;
use crate::jobs::JobService;
use crate::market_data::UpbitClient;
use crate::storage::ResultPage;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::router::tool::ToolRouter,
    model::{
        CallToolResult, ContentBlock, Implementation, JsonObject, ProtocolVersion,
        ServerCapabilities, ServerConfig,
    },
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const MAX_DATASET_PAGE_RECORDS: u32 = 100;
const MAX_PUBLIC_JOB_ATTEMPTS: usize = 32;

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

fn sanitize_attempt(attempt: &mut JobAttempt) {
    match &mut attempt.state {
        AttemptState::Failed { error, .. } => {
            // RESOURCE_LIMIT records carry the server-generated structured limit
            // report (limit name, unit, allowed bytes, stage, remedy) and
            // INVALID_CONFIG records echo only the caller's own rejected input
            // with fixed guidance text; masking either would hide exactly the
            // diagnosis a caller needs, so both pass through bounded.
            error.message = if matches!(error.code.as_str(), "RESOURCE_LIMIT" | "INVALID_CONFIG") {
                error.message.chars().take(512).collect()
            } else {
                public_failure_message(&error.code)
            };
        }
        AttemptState::Blocked { reason, .. } => {
            reason.message = public_failure_message(&reason.code);
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

fn tagged_schema<T: JsonSchema + 'static>() -> std::sync::Arc<JsonObject> {
    let mut schema = rmcp::handler::server::common::schema_for_type::<T>()
        .as_ref()
        .clone();
    // Internally tagged enums are object-shaped at the wire even though Schemars
    // expresses their alternatives with oneOf and omits the common root type.
    schema.insert("type".into(), serde_json::Value::String("object".into()));
    std::sync::Arc::new(schema)
}

#[derive(Clone)]
pub struct LabMcpService {
    tool_router: ToolRouter<Self>,
    upbit: UpbitClient,
    database: DatabaseHandle,
    git_revision: Option<String>,
    jobs: JobService,
    public_no_auth: bool,
}

#[tool_router]
impl LabMcpService {
    /// Build the stateless adapter from process-wide storage, HTTP and job owners.
    #[must_use]
    pub fn new(
        database: DatabaseHandle,
        upbit: UpbitClient,
        git_revision: Option<String>,
        jobs: JobService,
        public_no_auth: bool,
    ) -> Self {
        Self {
            tool_router: Self::tool_router(),
            upbit,
            database,
            git_revision,
            jobs,
            public_no_auth,
        }
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
    async fn lab_status(&self) -> Result<CallToolResult, McpError> {
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
        let status = serde_json::json!({
            "lab": "upbit-spot-lab",
            "package": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "code_revision": self.git_revision,
            "source_digest": env!("SPOT_LAB_SOURCE_SHA256"),
            "lockfile_digest": env!("SPOT_LAB_LOCK_SHA256"),
            "toolchain": env!("SPOT_LAB_TOOLCHAIN"),
            "schema_version": crate::contracts::SCHEMA_VERSION,
            "engine_version": crate::contracts::ENGINE_VERSION,
            "rounding_version": crate::contracts::ROUNDING_VERSION,
            "normalizer_version": crate::contracts::NORMALIZER_VERSION,
            "sqlite_version": rusqlite::version(),
            "implementation_status": "IMPLEMENTED_UNVERIFIED",
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
            },
            "scope": {
                "venue": "upbit",
                "markets": "any listed Upbit KRW-<SYMBOL> market (validated against the live market catalog at collection admission)",
                "quote": "KRW",
                "position_types": "long/cash only",
                "private_api": "never",
                "execution_origin": "SIMULATED_ONLY",
                "fill_observed": false,
            },
            "transport": {
                "auth": "none",
                "public_no_auth": self.public_no_auth,
                "progress_stream": false,
                "job_results": "poll job_control by opaque job_id",
            },
            "deletion": {
                "contract": "two_step_resource_delete_preview_then_resource_hard_delete",
                "preview_ttl_seconds": crate::contracts::DELETE_PREVIEW_TTL_SECONDS,
                "shared_by_all_callers": true,
                "mode": "HARD_DELETE_ROWS_AND_EXCLUSIVE_FILES",
                "public_no_auth_warning": if self.public_no_auth {
                    "any caller reaching this unauthenticated URL can delete with the same contract"
                } else {
                    ""
                },
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
        let definition = match &write {
            PolicyWrite::Create { definition, .. } | PolicyWrite::Revise { definition, .. } => {
                definition
            }
        };
        definition
            .validate()
            .map_err(|error| invalid_params(&error))?;
        let now = crate::contracts::UtcTimestamp::now();
        let result = self
            .database
            .call("mcp_policy_write", move |store| {
                store.write_policy(&write, now)
            })
            .await;
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
        description = "Read one bounded typed ledger page by run, model, section, range and cursor",
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
        let result = self
            .database
            .call("resource_hard_delete", move |store| {
                store.execute_hard_delete(&request, crate::contracts::UtcTimestamp::now())
            })
            .await;
        result_or_error(result)
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
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2025_06_18)
            .with_instructions(
                "Upbit public-data research lab. Quant policies use immutable revisions; runs and jobs expose bounded JSON history. Durable jobs are polled by ID. No trading, private API, auth, arbitrary SQL, URL, path, or code execution. Public-no-auth exposure is explicit in lab_status."
                    .to_owned(),
            )
    }
}

/// Read the short workspace revision once at startup (never per request).
#[must_use]
pub fn detect_git_revision() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|text| text.trim().to_owned())
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
mod tests;
