//! Typed local CLI and owned runtime composition for public-data research.

use spot_lab::contracts::{
    CandleInterval, CandleRecord, CollectRequest, DatasetStatus, LabError, MarketId, ProbeCount,
    ProbeReport, UtcTimestamp,
};
use spot_lab::database::{DatabaseHandle, DatabaseOwner};
use spot_lab::market_data::UpbitClient;
use spot_lab::mcp::{self, LabMcpService};

use spot_lab::observability::{LifecyclePhase, record_lifecycle_phase};
use std::collections::HashSet;
use std::future::IntoFuture;
use std::path::PathBuf;

fn main() {
    if let Err(error) = spot_lab::observability::init_json_stderr() {
        eprintln!("spot-lab: initialize logging: {error}");
        std::process::exit(1);
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Both pinned Reqwest versions use the existing Ring provider. RMCP's
    // no-provider TLS feature requires this before its HTTPS client is built.
    let outcome = rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| LabError::Internal("TLS provider was already installed before startup".into()))
        .and_then(|()| dispatch(&args));
    if let Err(error) = outcome {
        eprintln!("spot-lab: {error}");
        std::process::exit(1);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "explicit CLI command router keeps synchronous owner startup and close visible"
)]
fn dispatch(args: &[String]) -> Result<(), LabError> {
    match args.first().map(String::as_str) {
        Some("probe") => {
            validate_args(
                &args[1..],
                &["--market", "--interval", "--count", "--data-root"],
                &["--no-save"],
                &[],
            )?;
            let market = MarketId::parse_upbit(
                &flag_value(&args[1..], "--market").unwrap_or_else(|| "KRW-BTC".into()),
            )?;
            let interval = CandleInterval::parse_code(
                &flag_value(&args[1..], "--interval").unwrap_or_else(|| "h1".into()),
            )?;
            let count = parse_count(&args[1..])?;
            let client = UpbitClient::new()?;
            let owner = if flag_flag(&args[1..], "--no-save") {
                None
            } else {
                Some(DatabaseOwner::open(data_root(&args[1..]))?)
            };
            let handle = owner.as_ref().map(DatabaseOwner::handle);
            let result = runtime()?.block_on(run_probe(market, interval, count, client, handle));
            finish_database(result, owner)
        }
        Some("collect") => {
            validate_args(&args[1..], &["--request", "--data-root"], &[], &[])?;
            let path = flag_value(&args[1..], "--request").ok_or_else(|| {
                LabError::InvalidConfig("collect requires --request <json-or-toml>".into())
            })?;
            let request: CollectRequest = read_config(&path)?;
            request.validate(UtcTimestamp::now())?;
            let owner = DatabaseOwner::open(data_root(&args[1..]))?;
            let client = UpbitClient::new()?;
            let result = runtime()?.block_on(async {
                let cancellation = tokio_util::sync::CancellationToken::new();
                let snapshot =
                    spot_lab::collection::collect(&client, &owner.handle(), request, &cancellation)
                        .await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "dataset_id": snapshot.manifest.id,
                        "status": snapshot.manifest.status,
                        "row_count": snapshot.manifest.row_count,
                        "semantic_digest": snapshot.manifest.semantic_digest,
                        "raw_objects": snapshot.manifest.raw_objects.len(),
                        "quality_issues": snapshot.manifest.quality_issues.len(),
                        "origin": snapshot.manifest.origin,
                    }))?
                );
                if snapshot.manifest.status == DatasetStatus::BlockedData {
                    return Err(LabError::DataGap(
                        "collection persisted with BLOCKED_DATA; inspect dataset quality".into(),
                    ));
                }
                Ok(())
            });
            finish_database(result, Some(owner))
        }
        Some("plan" | "job" | "job-control" | "evidence-register") => {
            validate_args(&args[1..], &["--request", "--data-root"], &["--wait"], &[])?;
            let path = flag_value(&args[1..], "--request").ok_or_else(|| {
                LabError::InvalidConfig("command requires --request <json-or-toml>".into())
            })?;
            // Parsing is completed before opening storage or starting workers.
            let command = parse_local_request(&args[0], &path)?;
            let root = data_root(&args[1..]);
            let owner = DatabaseOwner::open(root.clone())?;
            let upbit = UpbitClient::new()?;
            let revision = mcp::detect_git_revision();
            let result = runtime()?.block_on(run_local_request(
                command,
                owner.handle(),
                upbit,
                root,
                revision,
                flag_flag(&args[1..], "--wait"),
            ));
            finish_database(result, Some(owner))
        }
        Some("policy-write" | "policy-query" | "history-query") => run_policy_command(args),
        Some("verify-export" | "replay-export") => {
            validate_args(&args[1..], &["--directory"], &[], &[])?;
            let path = flag_value(&args[1..], "--directory").ok_or_else(|| {
                LabError::InvalidConfig("verification requires --directory".into())
            })?;
            let package = spot_lab::reporting::read_export_package(std::path::Path::new(&path))?;
            let report = if args[0] == "replay-export" {
                spot_lab::replay::replay_package(&package)?
            } else {
                package.validation
            };
            print_json(&report)?;
            if report.status != spot_lab::contracts::ValidationStatus::Pass {
                return Err(LabError::AccountingInvariant(report.findings.join("; ")));
            }
            Ok(())
        }
        Some("backup") => {
            validate_args(&args[1..], &["--destination", "--data-root"], &[], &[])?;
            let destination =
                PathBuf::from(flag_value(&args[1..], "--destination").ok_or_else(|| {
                    LabError::InvalidConfig("backup requires --destination".into())
                })?);
            let owner = DatabaseOwner::open(data_root(&args[1..]))?;
            let result = owner
                .handle()
                .call_blocking("backup", move |store| store.backup(destination));
            finish_database(result, Some(owner))
        }
        Some("reseed-policies") => {
            validate_args(&args[1..], &["--data-root"], &[], &[])?;
            let owner = DatabaseOwner::open(data_root(&args[1..]))?;
            let result = owner
                .handle()
                .call_blocking("reseed_builtin_policies", move |store| {
                    let builtins = spot_lab::contracts::builtin_policy_definitions()?;
                    store.seed_builtin_policies(&builtins, UtcTimestamp::now())?;
                    serde_json::to_value(store.list_policies(None, 100)?).map_err(LabError::from)
                })
                .and_then(|value| print_json(&value));
            finish_database(result, Some(owner))
        }
        Some("restore") => {
            validate_args(&args[1..], &["--backup", "--data-root"], &[], &[])?;
            let backup = PathBuf::from(
                flag_value(&args[1..], "--backup")
                    .ok_or_else(|| LabError::InvalidConfig("restore requires --backup".into()))?,
            );
            let root = PathBuf::from(flag_value(&args[1..], "--data-root").ok_or_else(|| {
                LabError::InvalidConfig("restore requires an explicit new --data-root".into())
            })?);
            let owner = DatabaseOwner::restore(backup, root)?;
            owner.shutdown()
        }
        Some("derive-dataset") => {
            validate_args(
                &args[1..],
                &["--source", "--interval", "--data-root"],
                &[],
                &[],
            )?;
            let id = spot_lab::contracts::DatasetId::new(
                flag_value(&args[1..], "--source").ok_or_else(|| {
                    LabError::InvalidConfig("derive-dataset requires --source <dataset-id>".into())
                })?,
            )?;
            let target =
                CandleInterval::parse_code(&flag_value(&args[1..], "--interval").ok_or_else(
                    || LabError::InvalidConfig("derive-dataset requires --interval".into()),
                )?)?;
            let owner = DatabaseOwner::open(data_root(&args[1..]))?;
            let result = runtime()?.block_on(async {
                let snapshot = owner
                    .handle()
                    .call("derive_dataset", move |store| {
                        let source = store.load_dataset(&id)?.ok_or_else(|| {
                            LabError::InvalidConfig("unknown source dataset".into())
                        })?;
                        let snapshot = spot_lab::collection::derive_snapshot(&source, target)?;
                        store.finish_derived_dataset(
                            &id,
                            &snapshot,
                            spot_lab::collection::RESAMPLE_VERSION,
                        )?;
                        Ok(snapshot.manifest)
                    })
                    .await?;
                print_json(&snapshot)
            });
            finish_database(result, Some(owner))
        }
        Some("mcp-serve") => {
            validate_args(
                &args[1..],
                &["--bind", "--port", "--data-root"],
                &["--public-no-auth", "--allow-network-bind"],
                &["--allow-host"],
            )?;
            prepare_server_root(&args[1..])?;
            let upbit = UpbitClient::new()?;
            let git_revision = mcp::detect_git_revision();
            let owner = DatabaseOwner::open(data_root(&args[1..]))?;
            let result = runtime()?.block_on(run_mcp_serve(
                &args[1..],
                upbit,
                git_revision,
                owner.handle(),
            ));
            finish_database(result, Some(owner))
        }
        Some("mcp-selfcheck") => {
            validate_args(
                &args[1..],
                &["--url", "--market", "--interval", "--count"],
                &[],
                &[],
            )?;
            runtime()?.block_on(run_mcp_selfcheck(&args[1..]))
        }
        Some("schemas") => {
            validate_args(&args[1..], &["--out"], &[], &[])?;
            run_schemas(&args[1..])
        }
        Some("setup-gui") => {
            validate_args(&args[1..], &["--config", "--listen"], &[], &[])?;
            let config_path = PathBuf::from(
                flag_value(&args[1..], "--config")
                    .unwrap_or_else(|| spot_lab::gui::DEFAULT_CONFIG_FILE.into()),
            );
            let listen = flag_value(&args[1..], "--listen")
                .unwrap_or_else(|| spot_lab::gui::DEFAULT_LISTEN.into());
            let addr: std::net::SocketAddr = listen
                .parse()
                .map_err(|error| LabError::InvalidConfig(format!("--listen {listen}: {error}")))?;
            println!("설정 GUI: http://{addr} (Ctrl+C로 종료)");
            spot_lab::gui::serve(addr, config_path)
        }
        Some("--help" | "-h") if args.len() == 1 => {
            print_help();
            Ok(())
        }
        None => {
            print_help();
            Ok(())
        }
        Some(other) => Err(LabError::InvalidConfig(format!(
            "unknown command: {other} (try: probe | mcp-serve | mcp-selfcheck | schemas | setup-gui)"
        ))),
    }
}

fn run_policy_command(args: &[String]) -> Result<(), LabError> {
    use spot_lab::contracts::{HistoryQuery, PolicyQuery, PolicyWrite};
    enum Request {
        Write(PolicyWrite),
        Query(PolicyQuery),
        History(HistoryQuery),
    }
    validate_args(&args[1..], &["--request", "--data-root"], &[], &[])?;
    let path = flag_value(&args[1..], "--request").ok_or_else(|| {
        LabError::InvalidConfig("command requires --request <json-or-toml>".into())
    })?;
    let request = match args[0].as_str() {
        "policy-write" => Request::Write(read_config(&path)?),
        "policy-query" => Request::Query(read_config(&path)?),
        "history-query" => Request::History(read_config(&path)?),
        _ => return Err(LabError::Internal("unknown policy command".into())),
    };
    let owner = DatabaseOwner::open(data_root(&args[1..]))?;
    let result = owner
        .handle()
        .call_blocking("policy_cli", move |store| {
            let value = match request {
                Request::Write(write) => {
                    serde_json::to_value(store.write_policy(&write, UtcTimestamp::now())?)?
                }
                Request::Query(PolicyQuery::List {
                    after_policy_id,
                    limit,
                }) => serde_json::to_value(store.list_policies(after_policy_id.as_ref(), limit)?)?,
                Request::Query(PolicyQuery::Get { reference }) => {
                    serde_json::to_value(store.load_policy_revision(&reference)?.ok_or_else(
                        || LabError::InvalidConfig("unknown policy revision".into()),
                    )?)?
                }
                Request::Query(PolicyQuery::History {
                    policy_id,
                    before_revision_number,
                    limit,
                }) => serde_json::to_value(store.policy_history(
                    &policy_id,
                    before_revision_number,
                    limit,
                )?)?,
                Request::Query(PolicyQuery::Runs {
                    reference,
                    cursor,
                    limit,
                }) => {
                    serde_json::to_value(store.policy_runs(&reference, cursor.as_ref(), limit)?)?
                }
                Request::History(HistoryQuery::Runs { cursor, limit }) => {
                    serde_json::to_value(store.run_history(cursor.as_ref(), limit)?)?
                }
                Request::History(HistoryQuery::Jobs { cursor, limit }) => {
                    serde_json::to_value(store.job_history(cursor.as_ref(), limit)?)?
                }
            };
            Ok(value)
        })
        .and_then(|value| print_json(&value));
    finish_database(result, Some(owner))
}

enum LocalRequest {
    Plan(Box<spot_lab::contracts::PlanRequest>),
    Job(spot_lab::contracts::JobSubmission),
    Control(spot_lab::contracts::JobControl),
    Evidence(spot_lab::contracts::EvidenceImport),
}

fn parse_local_request(command: &str, path: &str) -> Result<LocalRequest, LabError> {
    match command {
        "plan" => read_config(path).map(Box::new).map(LocalRequest::Plan),
        "job" => read_config(path).map(LocalRequest::Job),
        "job-control" => read_config(path).map(LocalRequest::Control),
        "evidence-register" => read_config(path).map(LocalRequest::Evidence),
        _ => Err(LabError::InvalidConfig(
            "unknown local request command".into(),
        )),
    }
}

fn print_json(value: &impl serde::Serialize) -> Result<(), LabError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

async fn run_local_request(
    request: LocalRequest,
    database: DatabaseHandle,
    upbit: UpbitClient,
    root: PathBuf,
    revision: Option<String>,
    wait: bool,
) -> Result<(), LabError> {
    use spot_lab::contracts::{AttemptState, JobControl};
    match request {
        LocalRequest::Plan(request) => {
            print_json(&spot_lab::planning::prepare(&database, *request).await?)
        }
        LocalRequest::Evidence(import) => {
            let snapshot = spot_lab::evidence::build_snapshot(import)?;
            let saved = snapshot.clone();
            database
                .call("register_evidence", move |store| {
                    store.register_evidence_snapshot(&saved)
                })
                .await?;
            print_json(&snapshot)
        }
        LocalRequest::Control(JobControl::Get { job_id }) => {
            let job = database
                .call("get_job", move |store| store.get_job(&job_id))
                .await?
                .ok_or_else(|| LabError::InvalidConfig("unknown job ID".into()))?;
            print_json(&job)
        }
        LocalRequest::Control(JobControl::Cancel { job_id }) => {
            let job = database
                .call("cancel_job", move |store| {
                    store.request_cancel(&job_id, UtcTimestamp::now())
                })
                .await?;
            print_json(&job)
        }
        request => {
            let runtime =
                spot_lab::jobs::JobRuntime::start(database, upbit, root, revision).await?;
            let service = runtime.service();
            let should_wait =
                wait || matches!(&request, LocalRequest::Job(_) | LocalRequest::Control(_));
            let outcome = async {
                let mut job = match request {
                    LocalRequest::Job(submission) => service.submit(submission).await?,
                    LocalRequest::Control(control) => service.control(control).await?,
                    LocalRequest::Plan(_) | LocalRequest::Evidence(_) => {
                        return Err(LabError::Internal("invalid local dispatch".into()));
                    }
                };
                if should_wait {
                    let deadline =
                        tokio::time::Instant::now() + std::time::Duration::from_secs(1900);
                    loop {
                        let attempt = job
                            .attempts
                            .last()
                            .ok_or_else(|| LabError::DataCorrupt("job has no attempt".into()))?;
                        if attempt.state.is_terminal() {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            service
                                .control(JobControl::Cancel {
                                    job_id: job.id.clone(),
                                })
                                .await?;
                            return Err(LabError::ResourceLimit(
                                "CLI wait expired; cancellation requested".into(),
                            ));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        job = service
                            .control(JobControl::Get {
                                job_id: job.id.clone(),
                            })
                            .await?;
                    }
                }
                print_json(&job)?;
                if job.attempts.last().is_some_and(|a| {
                    matches!(
                        a.state,
                        AttemptState::Failed { .. }
                            | AttemptState::Cancelled { .. }
                            | AttemptState::Interrupted { .. }
                    )
                }) {
                    return Err(LabError::BlockedEvidence(
                        "job did not complete; inspect typed output".into(),
                    ));
                }
                Ok(())
            }
            .await;
            let shutdown = runtime.shutdown().await;
            outcome.and(shutdown)
        }
    }
}

fn prepare_server_root(args: &[String]) -> Result<(), LabError> {
    let public = flag_flag(args, "--public-no-auth");
    if args.iter().any(|arg| arg == "--allow-host") && !public {
        return Err(LabError::InvalidConfig(
            "additional hosts require explicit --public-no-auth".into(),
        ));
    }
    if !public {
        return Ok(());
    }
    if flag_value(args, "--data-root").is_none() {
        return Err(LabError::InvalidConfig(
            "public mode requires an explicit isolated --data-root".into(),
        ));
    }
    let root = data_root(args);
    let marker = root.join(".public-no-auth");
    if marker.is_file() {
        if std::fs::read_to_string(&marker)
            .map_err(|e| LabError::InvalidConfig(format!("public marker: {e}")))?
            != "PUBLIC_UPBIT_RESEARCH_ONLY\n"
        {
            return Err(LabError::InvalidConfig(
                "invalid public data-root marker".into(),
            ));
        }
        return Ok(());
    }
    if root.exists()
        && std::fs::read_dir(&root)
            .map_err(|e| LabError::InvalidConfig(format!("public root: {e}")))?
            .next()
            .is_some()
    {
        return Err(LabError::InvalidConfig(
            "public mode requires a new empty root or an explicitly marked public root".into(),
        ));
    }
    std::fs::create_dir_all(&root)
        .map_err(|e| LabError::InvalidConfig(format!("create public root: {e}")))?;
    std::fs::write(marker, "PUBLIC_UPBIT_RESEARCH_ONLY\n")
        .map_err(|e| LabError::InvalidConfig(format!("write public marker: {e}")))
}

fn read_config<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, LabError> {
    use std::io::Read as _;
    let file = std::fs::File::open(path)
        .map_err(|error| LabError::InvalidConfig(format!("read configuration: {error}")))?;
    let mut bytes = Vec::new();
    file.take(262_145)
        .read_to_end(&mut bytes)
        .map_err(|error| LabError::InvalidConfig(format!("read configuration: {error}")))?;
    if bytes.len() > 262_144 {
        return Err(LabError::ResourceLimit(
            "configuration exceeds 256 KiB".into(),
        ));
    }
    if std::path::Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("toml"))
    {
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| LabError::InvalidConfig(format!("configuration UTF-8: {error}")))?;
        toml::from_str(text)
            .map_err(|error| LabError::InvalidConfig(format!("configuration: {error}")))
    } else {
        serde_json::from_slice(&bytes).map_err(Into::into)
    }
}

fn data_root(args: &[String]) -> PathBuf {
    PathBuf::from(flag_value(args, "--data-root").unwrap_or_else(|| "data".into()))
}

fn finish_database(
    result: Result<(), LabError>,
    owner: Option<DatabaseOwner>,
) -> Result<(), LabError> {
    let closed = owner.map(DatabaseOwner::shutdown).transpose();
    match (result, closed) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(close_error)) => Err(LabError::Internal(format!(
            "{error}; storage close also failed: {close_error}"
        ))),
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, LabError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| LabError::Internal(format!("build Tokio runtime: {error}")))
}

fn print_help() {
    println!(
        "spot-lab <probe|collect|derive-dataset|plan|job|job-control|evidence-register|verify-export|replay-export|backup|restore|mcp-serve|mcp-selfcheck|schemas>"
    );
    println!(
        "  probe [--market KRW-BTC] [--interval h1] [--count 2] [--data-root data] [--no-save]"
    );
    println!("  mcp-serve [--bind 127.0.0.1] [--port 8130] [--data-root data]");
    println!("            [--allow-network-bind] # explicit container/network namespace binding");
    println!(
        "            [--allow-host <host>]  # repeatable: extra Host headers to accept (e.g. a tunnel domain)"
    );
    println!(
        "  mcp-selfcheck [--url http://127.0.0.1:8130/mcp] [--market KRW-BTC] [--interval h1] [--count 2]"
    );
    println!(
        "  collect|plan|job|job-control|evidence-register --request <json-or-toml> [--data-root data]"
    );
    println!(
        "  job and retry retain the process until a terminal result; MCP submits asynchronously"
    );
    println!("  verify-export|replay-export --directory <local-export-directory>");
    println!(
        "  public MCP: --public-no-auth --data-root <isolated-root> --allow-host <actual-tunnel-host>"
    );
    println!("  derive-dataset --source <dataset-id> --interval <h1|h4|d1> [--data-root data]");
    println!("  backup --destination <new-directory> [--data-root data]");
    println!("  restore --backup <backup-directory> --data-root <new-directory>");
    println!("  reseed-policies --data-root data # explicit builtin reinstallation after deletion");
    println!(
        "  policy-write | policy-query | history-query --request <json-or-toml> [--data-root data]"
    );
    println!("  schemas [--out docs/schema]");
}

fn validate_args(
    args: &[String],
    value_flags: &[&str],
    switches: &[&str],
    repeatable_value_flags: &[&str],
) -> Result<(), LabError> {
    let mut seen = HashSet::new();
    let mut index = 0;
    while let Some(argument) = args.get(index) {
        let is_value_flag = value_flags.contains(&argument.as_str());
        let is_repeatable = repeatable_value_flags.contains(&argument.as_str());
        let is_switch = switches.contains(&argument.as_str());

        if !is_value_flag && !is_repeatable && !is_switch {
            return Err(LabError::InvalidConfig(format!(
                "unknown option or positional argument: {argument}"
            )));
        }
        if !is_repeatable && !seen.insert(argument.as_str()) {
            return Err(LabError::InvalidConfig(format!(
                "duplicate option: {argument}"
            )));
        }
        if is_switch {
            index += 1;
            continue;
        }

        let Some(value) = args.get(index + 1) else {
            return Err(LabError::InvalidConfig(format!(
                "missing value for {argument}"
            )));
        };
        if value.starts_with("--") {
            return Err(LabError::InvalidConfig(format!(
                "missing value for {argument}"
            )));
        }
        index += 2;
    }
    Ok(())
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|argument| argument == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}

fn flag_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|argument| argument == name)
}

/// Upbit candle `count` accepts 1..=200; the probe requests one extra wire
/// candle for the completed-only filter, so callers get 1..=199.
fn parse_count(args: &[String]) -> Result<ProbeCount, LabError> {
    let count = match flag_value(args, "--count") {
        Some(raw) => raw
            .parse::<u32>()
            .map_err(|error| LabError::InvalidConfig(format!("--count: {error}")))?,
        None => return Ok(ProbeCount::default()),
    };
    ProbeCount::try_from(count)
}

/// Real Upbit probe over the public API; preserves the raw response.
async fn run_probe(
    market: MarketId,
    interval: CandleInterval,
    completed_count: ProbeCount,
    client: UpbitClient,
    database: Option<DatabaseHandle>,
) -> Result<(), LabError> {
    let report = client
        .fetch_completed_candles(&market, interval, completed_count, database.as_ref())
        .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "single owner visibly composes startup, stop admission, cancel and joined shutdown"
)]
async fn run_mcp_serve(
    args: &[String],
    upbit: UpbitClient,
    git_revision: Option<String>,
    database: DatabaseHandle,
) -> Result<(), LabError> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    let bind = flag_value(args, "--bind").unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = flag_value(args, "--port")
        .map(|raw| {
            raw.parse::<u16>()
                .map_err(|error| LabError::InvalidConfig(format!("--port: {error}")))
        })
        .transpose()?
        .unwrap_or(8130);
    let allow_hosts: Vec<String> = args
        .windows(2)
        .filter(|window| window[0] == "--allow-host")
        .map(|window| window[1].clone())
        .collect();

    let bind_address: std::net::IpAddr = bind
        .parse()
        .map_err(|_| LabError::InvalidConfig("bind must be a loopback IP address".into()))?;
    if !bind_address.is_loopback() && !flag_flag(args, "--allow-network-bind") {
        return Err(LabError::InvalidConfig(
            "non-loopback bind requires explicit --allow-network-bind; publish container ports on loopback unless public exposure is intended".into(),
        ));
    }
    let public_no_auth = flag_flag(args, "--public-no-auth");
    let cancellation = tokio_util::sync::CancellationToken::new();
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_max_request_body_bytes(spot_lab::transport::MAX_REQUEST_BODY_BYTES)
        .with_cancellation_token(cancellation.child_token());
    // RMCP treats an empty Origin allowlist as disabled validation.
    // Requests without Origin remain valid; present Origins must match exactly.
    config.allowed_origins = vec![
        format!("http://127.0.0.1:{port}"),
        format!("http://localhost:{port}"),
        format!("http://[::1]:{port}"),
    ];
    config
        .allowed_origins
        .extend(allow_hosts.iter().map(|host| format!("https://{host}")));
    config.allowed_hosts.extend(allow_hosts);
    let listener = tokio::net::TcpListener::bind((bind_address, port))
        .await
        .map_err(|error| LabError::InvalidConfig(format!("bind {bind}:{port}: {error}")))?;
    let listener = spot_lab::transport::BoundedListener::new(
        listener,
        spot_lab::transport::TransportLimits::default(),
    );
    let state = listener.state();
    let jobs = spot_lab::jobs::JobRuntime::start(
        database.clone(),
        upbit.clone(),
        data_root(args),
        git_revision.clone(),
    )
    .await?;
    let job_service = jobs.service();
    let service = StreamableHttpService::new(
        move || {
            Ok(LabMcpService::new(
                database.clone(),
                upbit.clone(),
                git_revision.clone(),
                job_service.clone(),
                public_no_auth,
            ))
        },
        LocalSessionManager::default().into(),
        config,
    );
    let app = spot_lab::transport::apply_limits(
        axum::Router::new().nest_service("/mcp", service),
        state.clone(),
    );
    record_lifecycle_phase(LifecyclePhase::Startup, &state);
    tracing::info!(event = "mcp_listening", bind = %bind_address, port, public_no_auth, transport = "STATELESS_JSON");
    let graceful = cancellation.clone();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(graceful.cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let server_outcome = tokio::select! {
        result = &mut server => Some(result),
        signal = tokio::signal::ctrl_c() => {
            if signal.is_err() { tracing::error!(event = "shutdown_signal_failed"); }
            None
        }
    };
    state.stop_admission();
    record_lifecycle_phase(LifecyclePhase::StopAdmission, &state);
    cancellation.cancel();
    record_lifecycle_phase(LifecyclePhase::Cancel, &state);
    let joined_server = async {
        if let Some(outcome) = server_outcome {
            return outcome.map_err(|e| LabError::Internal(format!("MCP server: {e}")));
        }
        if let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut server).await
        {
            result.map_err(|error| LabError::Internal(format!("MCP server: {error}")))
        } else {
            record_lifecycle_phase(LifecyclePhase::Timeout, &state);
            // Retain ownership; a late join is explicitly failed graceful shutdown.
            server
                .await
                .map_err(|error| LabError::Internal(format!("MCP late join: {error}")))?;
            Err(LabError::ResourceLimit(
                "MCP graceful shutdown exceeded 30 seconds".into(),
            ))
        }
    };
    let (server_result, jobs_result) = tokio::join!(joined_server, jobs.shutdown());
    record_lifecycle_phase(LifecyclePhase::Drain, &state);
    server_result?;
    jobs_result?;
    if !matches!(
        state
            .wait_for_drain(std::time::Duration::from_secs(1))
            .await,
        spot_lab::transport::DrainOutcome::Drained
    ) {
        return Err(LabError::Internal(
            "transport resources remain after server join".into(),
        ));
    }
    record_lifecycle_phase(LifecyclePhase::Join, &state);
    Ok(())
}

/// Real MCP client round trip (initialize -> tools/list -> tools/call) using
/// the official SDK client transport against a running server URL.
async fn run_mcp_selfcheck(args: &[String]) -> Result<(), LabError> {
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::StreamableHttpClientTransport;

    let url = flag_value(args, "--url").unwrap_or_else(|| "http://127.0.0.1:8130/mcp".into());
    let market = flag_value(args, "--market").unwrap_or_else(|| "KRW-BTC".into());
    let interval = flag_value(args, "--interval").unwrap_or_else(|| "h1".into());
    CandleInterval::parse_code(&interval)?;
    let count = parse_count(args)?;

    let client = ()
        .serve(StreamableHttpClientTransport::from_uri(url.clone()))
        .await
        .map_err(|error| LabError::NetworkUnavailable(format!("mcp initialize {url}: {error}")))?;
    let check = async {
        if let Some(info) = client.peer_info() {
            let (name, version) =
                info.server_info
                    .as_ref()
                    .map_or(("<unknown>", "<unknown>"), |implementation| {
                        (
                            implementation.name.as_str(),
                            implementation.version.as_str(),
                        )
                    });
            println!(
                "initialize: server={name} version={version} protocol={}",
                info.protocol_version
            );
        } else {
            println!("initialize: peer info unavailable");
        }

        let tools = client
            .list_all_tools()
            .await
            .map_err(|error| LabError::NetworkUnavailable(format!("mcp tools/list: {error}")))?;
        let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        require_selfcheck_tools(&names)?;
        println!("tools/list: {}", names.join(", "));

        for name in ["lab_status", "probe_upbit"] {
            let arguments = (name == "probe_upbit").then(|| {
                let mut object = serde_json::Map::new();
                object.insert(
                    "market".to_owned(),
                    serde_json::Value::String(market.clone()),
                );
                object.insert(
                    "interval".to_owned(),
                    serde_json::Value::String(interval.clone()),
                );
                object.insert("count".to_owned(), serde_json::Value::from(count.get()));
                object
            });
            let mut params = CallToolRequestParams::default();
            params.name = name.into();
            params.arguments = arguments;
            let result = client.call_tool(params).await.map_err(|error| {
                LabError::NetworkUnavailable(format!("mcp call {name}: {error}"))
            })?;
            let is_error = result.is_error.unwrap_or(false);
            let text = require_tool_success(name, result)?;
            println!("tools/call {name} (is_error={is_error}):\n{text}");
        }
        Ok(())
    }
    .await;

    let cancelled = client
        .cancel()
        .await
        .map_err(|error| LabError::Internal(format!("cancel MCP selfcheck client: {error}")));
    match (check, cancelled) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) | (Ok(()), Err(error)) => Err(error),
        (Err(check_error), Err(cancel_error)) => Err(LabError::Internal(format!(
            "selfcheck failed ({check_error}); client cancellation also failed ({cancel_error})"
        ))),
    }
}

fn require_selfcheck_tools(names: &[&str]) -> Result<(), LabError> {
    let missing: Vec<&str> = ["lab_status", "probe_upbit"]
        .into_iter()
        .filter(|expected| !names.contains(expected))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(LabError::BlockedEvidence(format!(
            "mcp tools/list missing expected tools: {}",
            missing.join(", ")
        )))
    }
}

fn require_tool_success(
    name: &str,
    result: rmcp::model::CallToolResult,
) -> Result<String, LabError> {
    let is_error = result.is_error.unwrap_or(false);
    let text = mcp::result_text(result);
    if is_error {
        Err(LabError::BlockedEvidence(format!(
            "mcp call {name} returned a tool error: {text}"
        )))
    } else {
        Ok(text)
    }
}

/// Generate the public typed input, ledger and result JSON Schemas.
#[expect(
    clippy::too_many_lines,
    reason = "declarative public DTO schema catalog; no repeated behavioral logic"
)]
fn run_schemas(args: &[String]) -> Result<(), LabError> {
    let out = PathBuf::from(flag_value(args, "--out").unwrap_or_else(|| "docs/schema".into()));
    std::fs::create_dir_all(&out)
        .map_err(|error| LabError::InvalidConfig(format!("create {}: {error}", out.display())))?;
    let schemas = [
        (
            "artifact-query",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ArtifactQuery))?,
        ),
        (
            "artifact-query-result",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::ArtifactQueryResult
            ))?,
        ),
        (
            "limit-report",
            schema_json(&schemars::schema_for!(spot_lab::contracts::LimitReport))?,
        ),
        (
            "run-result-summary",
            schema_json(&schemars::schema_for!(spot_lab::storage::RunResultSummary))?,
        ),
        (
            "model-cost-summary",
            schema_json(&schemars::schema_for!(spot_lab::storage::ModelCostSummary))?,
        ),
        (
            "quality-issue-view",
            schema_json(&schemars::schema_for!(spot_lab::quality::QualityIssueView))?,
        ),
        (
            "policy-definition",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::PolicyDefinition
            ))?,
        ),
        (
            "policy-write",
            schema_json(&schemars::schema_for!(spot_lab::contracts::PolicyWrite))?,
        ),
        (
            "policy-query",
            schema_json(&schemars::schema_for!(spot_lab::contracts::PolicyQuery))?,
        ),
        (
            "policy-revision",
            schema_json(&schemars::schema_for!(spot_lab::contracts::PolicyRevision))?,
        ),
        (
            "history-query",
            schema_json(&schemars::schema_for!(spot_lab::contracts::HistoryQuery))?,
        ),
        (
            "delete-resource",
            schema_json(&schemars::schema_for!(spot_lab::contracts::DeleteResource))?,
        ),
        (
            "delete-preview",
            schema_json(&schemars::schema_for!(spot_lab::contracts::DeletePreview))?,
        ),
        (
            "hard-delete-request",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::HardDeleteRequest
            ))?,
        ),
        (
            "delete-outcome",
            schema_json(&schemars::schema_for!(spot_lab::contracts::DeleteOutcome))?,
        ),
        (
            "candle-record",
            schema_json(&schemars::schema_for!(CandleRecord))?,
        ),
        (
            "probe-report",
            schema_json(&schemars::schema_for!(ProbeReport))?,
        ),
        (
            "probe-params",
            schema_json(&schemars::schema_for!(mcp::ProbeParams))?,
        ),
        ("market-id", schema_json(&schemars::schema_for!(MarketId))?),
        (
            "collect-request",
            schema_json(&schemars::schema_for!(spot_lab::contracts::CollectRequest))?,
        ),
        (
            "dataset-snapshot",
            schema_json(&schemars::schema_for!(spot_lab::contracts::DatasetSnapshot))?,
        ),
        (
            "experiment-spec",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ExperimentSpec))?,
        ),
        (
            "plan-request",
            schema_json(&schemars::schema_for!(spot_lab::contracts::PlanRequest))?,
        ),
        (
            "resolved-plan",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ResolvedPlan))?,
        ),
        (
            "run-request",
            schema_json(&schemars::schema_for!(spot_lab::contracts::RunRequest))?,
        ),
        (
            "run-bundle",
            schema_json(&schemars::schema_for!(spot_lab::contracts::RunBundle))?,
        ),
        (
            "model-ledger",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ModelLedger))?,
        ),
        (
            "evidence-import",
            schema_json(&schemars::schema_for!(spot_lab::contracts::EvidenceImport))?,
        ),
        (
            "job-submission",
            schema_json(&schemars::schema_for!(spot_lab::contracts::JobSubmission))?,
        ),
        (
            "job-control",
            schema_json(&schemars::schema_for!(spot_lab::contracts::JobControl))?,
        ),
        (
            "job-record",
            schema_json(&schemars::schema_for!(spot_lab::contracts::JobRecord))?,
        ),
        (
            "result-query",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ResultQuery))?,
        ),
        (
            "validation-report",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::ValidationReport
            ))?,
        ),
        (
            "artifact-ref",
            schema_json(&schemars::schema_for!(spot_lab::contracts::ArtifactRef))?,
        ),
        (
            "mcp-dataset-query",
            schema_json(&schemars::schema_for!(mcp::DatasetQueryParams))?,
        ),
        (
            "mcp-job-control",
            schema_json(&schemars::schema_for!(mcp::JobControlParams))?,
        ),
        (
            "mcp-result-query",
            schema_json(&schemars::schema_for!(mcp::ResultQueryParams))?,
        ),
        (
            "mcp-export-report",
            schema_json(&schemars::schema_for!(mcp::ExportReportParams))?,
        ),
        (
            "mcp-verify-run",
            schema_json(&schemars::schema_for!(mcp::VerifyRunParams))?,
        ),
    ];
    for (name, text) in &schemas {
        std::fs::write(out.join(format!("{name}.json")), format!("{text}\n"))
            .map_err(|error| LabError::Internal(format!("write schema {name}: {error}")))?;
    }
    println!("wrote {} schemas to {}", schemas.len(), out.display());
    Ok(())
}

fn schema_json(schema: &schemars::Schema) -> Result<String, LabError> {
    serde_json::to_string_pretty(schema).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{CallToolResult, ContentBlock};

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn cli_rejects_malformed_options_and_only_repeats_allow_host() {
        let malformed = [
            strings(&["--out"]),
            strings(&["--out", "one", "--out", "two"]),
            strings(&["--unknown"]),
            strings(&["stray"]),
        ];
        for args in &malformed {
            assert!(validate_args(args, &["--out"], &[], &[]).is_err());
        }

        let hosts = strings(&["--allow-host", "one", "--allow-host", "two"]);
        assert!(validate_args(&hosts, &[], &[], &["--allow-host"]).is_ok());
    }

    #[test]
    fn selfcheck_requires_expected_tools_and_successful_results() {
        assert!(require_selfcheck_tools(&["lab_status"]).is_err());
        assert!(require_selfcheck_tools(&["probe_upbit", "lab_status"]).is_ok());

        let failed = CallToolResult::error(vec![ContentBlock::text("typed failure")]);
        assert!(require_tool_success("probe_upbit", failed).is_err());
        let succeeded = CallToolResult::success(vec![ContentBlock::text("ok")]);
        assert!(matches!(
            require_tool_success("probe_upbit", succeeded).as_deref(),
            Ok("ok")
        ));
    }
}
