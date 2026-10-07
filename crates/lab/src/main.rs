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
        Some("research-suite" | "collection-schedule" | "storage-maintenance") => {
            run_research_command(args)
        }
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
                &[
                    "--bind",
                    "--port",
                    "--data-root",
                    "--auth-token",
                    "--public-url",
                ],
                &["--public-no-auth", "--allow-network-bind", "--oauth"],
                &["--allow-host"],
            )?;
            let config = McpServeConfig::parse(&args[1..])?;
            prepare_server_root(&config)?;
            let upbit = UpbitClient::new()?;
            let git_revision = mcp::detect_git_revision();
            let owner = DatabaseOwner::open(config.data_root.clone())?;
            let result =
                runtime()?.block_on(run_mcp_serve(config, upbit, git_revision, owner.handle()));
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
        Some("setup-gui") => Err(LabError::InvalidConfig(
            "the browser setup has been retired; open the native Sky Backcraft.app for settings"
                .into(),
        )),
        Some("--help" | "-h") if args.len() == 1 => {
            print_help();
            Ok(())
        }
        None => {
            print_help();
            Ok(())
        }
        Some(other) => Err(LabError::InvalidConfig(format!(
            "unknown command: {other} (try: probe | mcp-serve | mcp-selfcheck | schemas)"
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
                Request::Write(PolicyWrite::Sweep {
                    request_id,
                    family,
                    template,
                    mode,
                }) => serde_json::to_value(store.sweep_policy(
                    &request_id,
                    family,
                    &template,
                    &mode,
                    UtcTimestamp::now(),
                )?)?,
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

enum ResearchCommand {
    Suite(spot_lab::contracts::ResearchSuiteAction),
    Schedule(spot_lab::contracts::CollectionScheduleAction),
    Maintenance(spot_lab::contracts::StorageMaintenanceAction),
}

fn parse_research_command(args: &[String]) -> Result<(ResearchCommand, bool), LabError> {
    use spot_lab::contracts::{
        CollectionScheduleAction, ResearchSuiteAction, StorageMaintenanceAction,
        validate_research_page,
    };

    let command = args
        .first()
        .ok_or_else(|| LabError::InvalidConfig("research command is missing".into()))?;
    let switches: &[&str] = if command == "research-suite" {
        &["--wait"]
    } else {
        &[]
    };
    validate_args(&args[1..], &["--request", "--data-root"], switches, &[])?;
    let path = flag_value(&args[1..], "--request")
        .ok_or_else(|| LabError::InvalidConfig("research command requires --request".into()))?;
    let request = match command.as_str() {
        "research-suite" => {
            let action: ResearchSuiteAction = read_config(&path)?;
            match &action {
                ResearchSuiteAction::Create { request } => {
                    let _geometry = spot_lab::research::expand_geometry(request)?;
                }
                ResearchSuiteAction::Plan { request } => {
                    let _plan = spot_lab::research::plan_suite(request)?;
                }
                ResearchSuiteAction::List { limit, .. }
                | ResearchSuiteAction::Cases { limit, .. }
                | ResearchSuiteAction::Comparisons { limit, .. } => {
                    validate_research_page(*limit)?;
                }
                ResearchSuiteAction::Get { .. }
                | ResearchSuiteAction::Pause { .. }
                | ResearchSuiteAction::Resume { .. } => {}
            }
            ResearchCommand::Suite(action)
        }
        "collection-schedule" => {
            let action: CollectionScheduleAction = read_config(&path)?;
            match &action {
                CollectionScheduleAction::Create { request } => request.validate()?,
                CollectionScheduleAction::List { limit, .. } => {
                    validate_research_page(*limit)?;
                }
                CollectionScheduleAction::Get { .. }
                | CollectionScheduleAction::Pause { .. }
                | CollectionScheduleAction::Resume { .. }
                | CollectionScheduleAction::Freshness { .. } => {}
            }
            ResearchCommand::Schedule(action)
        }
        "storage-maintenance" => {
            let action: StorageMaintenanceAction = read_config(&path)?;
            if let StorageMaintenanceAction::RetentionCandidates {
                keep_recent, limit, ..
            } = &action
            {
                if *keep_recent == 0 {
                    return Err(LabError::InvalidConfig(
                        "retention keep_recent must be at least 1".into(),
                    ));
                }
                validate_research_page(*limit)?;
            }
            ResearchCommand::Maintenance(action)
        }
        _ => return Err(LabError::Internal("unknown research command".into())),
    };
    let wait = flag_flag(&args[1..], "--wait");
    if wait
        && !matches!(
            &request,
            ResearchCommand::Suite(
                ResearchSuiteAction::Create { .. } | ResearchSuiteAction::Resume { .. }
            )
        )
    {
        return Err(LabError::InvalidConfig(
            "--wait is supported only for research-suite create or resume".into(),
        ));
    }
    Ok((request, wait))
}

fn run_research_command(args: &[String]) -> Result<(), LabError> {
    let (request, wait) = parse_research_command(args)?;
    let root = data_root(&args[1..]);
    let owner = DatabaseOwner::open(root.clone())?;
    let outcome = if wait {
        let ResearchCommand::Suite(action) = request else {
            return Err(LabError::Internal(
                "validated wait request was not a research suite".into(),
            ));
        };
        let upbit = UpbitClient::new()?;
        runtime()?.block_on(run_waiting_suite(
            action,
            owner.handle(),
            upbit,
            root,
            mcp::detect_git_revision(),
        ))
    } else {
        runtime()?.block_on(run_offline_research_command(request, owner.handle()))
    };
    finish_database(outcome, Some(owner))
}

async fn run_waiting_suite(
    action: spot_lab::contracts::ResearchSuiteAction,
    database: DatabaseHandle,
    upbit: UpbitClient,
    root: PathBuf,
    revision: Option<String>,
) -> Result<(), LabError> {
    use spot_lab::contracts::ResearchSuiteAction;

    let runtime = spot_lab::jobs::JobRuntime::start(database, upbit, root, revision).await?;
    let service = runtime.service();
    let result = async {
        let mut value = service.research_suite(action).await?;
        let id = value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| LabError::DataCorrupt("suite result omitted id".into()))?;
        let suite_id = spot_lab::contracts::SuiteId::new(id)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1900);
        while value.get("status").and_then(serde_json::Value::as_str) == Some("running") {
            if tokio::time::Instant::now() >= deadline {
                service
                    .research_suite(ResearchSuiteAction::Pause {
                        suite_id: suite_id.clone(),
                    })
                    .await?;
                return Err(LabError::ResourceLimit(
                    "suite CLI wait expired; pause requested".into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            value = service
                .research_suite(ResearchSuiteAction::Get {
                    suite_id: suite_id.clone(),
                })
                .await?;
        }
        print_json(&value)
    }
    .await;
    result.and(runtime.shutdown().await)
}

async fn run_offline_research_command(
    request: ResearchCommand,
    database: DatabaseHandle,
) -> Result<(), LabError> {
    let value = match request {
        ResearchCommand::Suite(action) => run_offline_suite(action, &database).await?,
        ResearchCommand::Schedule(action) => run_offline_schedule(action, &database).await?,
        ResearchCommand::Maintenance(action) => run_offline_maintenance(action, &database).await?,
    };
    print_json(&value)
}

async fn run_offline_suite(
    action: spot_lab::contracts::ResearchSuiteAction,
    database: &DatabaseHandle,
) -> Result<serde_json::Value, LabError> {
    use spot_lab::contracts::ResearchSuiteAction;

    match action {
        ResearchSuiteAction::Plan { request } => {
            // Read-only preview works without a database handle.
            Ok(serde_json::to_value(spot_lab::research::plan_suite(
                &request,
            )?)?)
        }
        ResearchSuiteAction::Create { request } => create_offline_suite(*request, database).await,
        ResearchSuiteAction::Get { suite_id } => {
            database
                .call("get_research_suite_cli", move |store| {
                    let record = store
                        .get_research_suite(&suite_id)?
                        .ok_or_else(|| LabError::InvalidConfig("unknown research suite".into()))?;
                    Ok(serde_json::to_value(spot_lab::research::summarize(
                        &record,
                    ))?)
                })
                .await
        }
        ResearchSuiteAction::List { offset, limit } => {
            database
                .call("list_research_suites_cli", move |store| {
                    Ok(serde_json::to_value(
                        store.list_research_suites(offset, limit)?,
                    )?)
                })
                .await
        }
        ResearchSuiteAction::Cases {
            suite_id,
            offset,
            limit,
        } => {
            database
                .call("list_suite_cases_cli", move |store| {
                    Ok(serde_json::to_value(
                        store.list_suite_cases(&suite_id, offset, limit)?,
                    )?)
                })
                .await
        }
        ResearchSuiteAction::Comparisons {
            suite_id,
            offset,
            limit,
        } => {
            database
                .call("list_suite_comparisons_cli", move |store| {
                    Ok(serde_json::to_value(
                        store.list_suite_comparisons(&suite_id, offset, limit)?,
                    )?)
                })
                .await
        }
        ResearchSuiteAction::Pause { suite_id } => {
            database
                .call("pause_research_suite_cli", move |store| {
                    let (record, _running_jobs) =
                        store.pause_research_suite(&suite_id, UtcTimestamp::now())?;
                    Ok(serde_json::to_value(spot_lab::research::summarize(
                        &record,
                    ))?)
                })
                .await
        }
        ResearchSuiteAction::Resume { suite_id } => {
            database
                .call("resume_research_suite_cli", move |store| {
                    let record = store.resume_research_suite(&suite_id, UtcTimestamp::now())?;
                    Ok(serde_json::to_value(spot_lab::research::summarize(
                        &record,
                    ))?)
                })
                .await
        }
    }
}

async fn create_offline_suite(
    request: spot_lab::contracts::ResearchSuiteRequest,
    database: &DatabaseHandle,
) -> Result<serde_json::Value, LabError> {
    let _verified_inputs = spot_lab::planning::load_inputs(database, &request.template).await?;
    let references = request.template.policy_selections.clone();
    let policies = database
        .call("suite_cli_frozen_policies", move |store| {
            references
                .iter()
                .map(|reference| {
                    store
                        .load_policy_revision(reference)?
                        .map(|revision| revision.snapshot)
                        .ok_or_else(|| {
                            LabError::InvalidConfig("unknown suite policy revision".into())
                        })
                })
                .collect::<Result<Vec<_>, LabError>>()
        })
        .await?;
    let frozen = spot_lab::research::freeze(request, policies)?;
    let cases = spot_lab::research::initial_cases(&frozen)?;
    let record = database
        .call("create_research_suite_cli", move |store| {
            store.create_research_suite(&frozen, &cases, UtcTimestamp::now())
        })
        .await?;
    Ok(serde_json::to_value(spot_lab::research::summarize(
        &record,
    ))?)
}

async fn run_offline_schedule(
    action: spot_lab::contracts::CollectionScheduleAction,
    database: &DatabaseHandle,
) -> Result<serde_json::Value, LabError> {
    use spot_lab::contracts::CollectionScheduleAction;

    match action {
        CollectionScheduleAction::Create { request } => {
            let record = spot_lab::scheduling::create_schedule(*request, UtcTimestamp::now())?;
            database
                .call("create_collection_schedule_cli", move |store| {
                    Ok(serde_json::to_value(
                        store.create_collection_schedule(&record)?,
                    )?)
                })
                .await
        }
        CollectionScheduleAction::Get { schedule_id } => {
            database
                .call("get_collection_schedule_cli", move |store| {
                    let record = store
                        .get_collection_schedule(&schedule_id)?
                        .ok_or_else(|| {
                            LabError::InvalidConfig("unknown collection schedule".into())
                        })?;
                    Ok(serde_json::to_value(record)?)
                })
                .await
        }
        CollectionScheduleAction::List { offset, limit } => {
            database
                .call("list_collection_schedules_cli", move |store| {
                    Ok(serde_json::to_value(
                        store.list_collection_schedules(offset, limit)?,
                    )?)
                })
                .await
        }
        CollectionScheduleAction::Pause { schedule_id } => {
            database
                .call("pause_collection_schedule_cli", move |store| {
                    let (record, _running_jobs) =
                        store.pause_collection_schedule(&schedule_id, UtcTimestamp::now())?;
                    Ok(serde_json::to_value(record)?)
                })
                .await
        }
        CollectionScheduleAction::Resume { schedule_id } => {
            database
                .call("resume_collection_schedule_cli", move |store| {
                    Ok(serde_json::to_value(store.resume_collection_schedule(
                        &schedule_id,
                        UtcTimestamp::now(),
                    )?)?)
                })
                .await
        }
        CollectionScheduleAction::Freshness {
            schedule_id,
            probe_source,
        } => {
            let probes = if probe_source {
                // The offline CLI has no live client; freshness stays time-based.
                std::collections::BTreeMap::new()
            } else {
                std::collections::BTreeMap::new()
            };
            database
                .call("collection_freshness_cli", move |store| {
                    Ok(serde_json::to_value(store.collection_freshness(
                        &schedule_id,
                        UtcTimestamp::now(),
                        &probes,
                    )?)?)
                })
                .await
        }
    }
}

async fn run_offline_maintenance(
    action: spot_lab::contracts::StorageMaintenanceAction,
    database: &DatabaseHandle,
) -> Result<serde_json::Value, LabError> {
    use spot_lab::contracts::{StorageMaintenanceAction, StorageMaintenanceResult};

    let result = match action {
        StorageMaintenanceAction::Usage => {
            database
                .call("maintenance_usage_cli", |store| {
                    Ok(StorageMaintenanceResult::Usage {
                        usage: store.maintenance_usage()?,
                    })
                })
                .await?
        }
        StorageMaintenanceAction::Checkpoint { mode } => {
            database
                .call("checkpoint_wal_cli", move |store| {
                    Ok(StorageMaintenanceResult::Checkpoint {
                        outcome: store.checkpoint_wal(mode)?,
                    })
                })
                .await?
        }
        StorageMaintenanceAction::Compact => {
            database
                .call("compact_database_cli", move |store| {
                    Ok(StorageMaintenanceResult::Compact {
                        outcome: store.compact_database()?,
                    })
                })
                .await?
        }
        StorageMaintenanceAction::HardDeleteBatch { requests } => {
            database
                .call("hard_delete_batch_cli", move |store| {
                    let mut outcomes = Vec::with_capacity(requests.len());
                    for request in requests {
                        outcomes.push(store.execute_hard_delete(&request, UtcTimestamp::now())?);
                    }
                    Ok(StorageMaintenanceResult::HardDeleteBatch { outcomes })
                })
                .await?
        }
        StorageMaintenanceAction::CreateBackup { request_id } => {
            database
                .call("create_managed_backup_cli", move |store| {
                    Ok(StorageMaintenanceResult::Backup {
                        receipt: store.create_managed_backup(&request_id, UtcTimestamp::now())?,
                    })
                })
                .await?
        }
        StorageMaintenanceAction::ListBackups => {
            database
                .call("list_managed_backups_cli", |store| {
                    Ok(StorageMaintenanceResult::Backups {
                        page: store.list_managed_backups()?,
                    })
                })
                .await?
        }
        StorageMaintenanceAction::RetentionCandidates {
            cutoff,
            keep_recent,
            offset,
            limit,
        } => {
            database
                .call("retention_candidates_cli", move |store| {
                    Ok(StorageMaintenanceResult::RetentionCandidates {
                        page: store.retention_candidates(
                            cutoff,
                            keep_recent,
                            offset,
                            limit,
                            UtcTimestamp::now(),
                        )?,
                    })
                })
                .await?
        }
    };
    Ok(serde_json::to_value(result)?)
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

fn prepare_server_root(config: &McpServeConfig) -> Result<(), LabError> {
    if !config.auth.exposure().public_no_auth() {
        return Ok(());
    }
    let root = &config.data_root;
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
        && std::fs::read_dir(root)
            .map_err(|e| LabError::InvalidConfig(format!("public root: {e}")))?
            .next()
            .is_some()
    {
        return Err(LabError::InvalidConfig(
            "public mode requires a new empty root or an explicitly marked public root".into(),
        ));
    }
    std::fs::create_dir_all(root)
        .map_err(|e| LabError::InvalidConfig(format!("create public root: {e}")))?;
    std::fs::write(marker, "PUBLIC_UPBIT_RESEARCH_ONLY\n")
        .map_err(|e| LabError::InvalidConfig(format!("write public marker: {e}")))
}

enum ServerAuth {
    None,
    PublicNoAuth,
    Bearer(String),
    OAuth(spot_lab::oauth::OAuthState),
}

impl ServerAuth {
    const fn exposure(&self) -> mcp::McpExposure {
        match self {
            Self::None => mcp::McpExposure::Local,
            Self::PublicNoAuth => mcp::McpExposure::PublicNoAuth,
            Self::Bearer(_) => mcp::McpExposure::Bearer,
            Self::OAuth(_) => mcp::McpExposure::OAuth,
        }
    }
}

struct McpServeConfig {
    bind_address: std::net::IpAddr,
    port: u16,
    data_root: PathBuf,
    allow_hosts: Vec<String>,
    auth: ServerAuth,
}

impl McpServeConfig {
    fn parse(args: &[String]) -> Result<Self, LabError> {
        let bind_address: std::net::IpAddr = flag_value(args, "--bind")
            .unwrap_or_else(|| "127.0.0.1".into())
            .parse()
            .map_err(|_| LabError::InvalidConfig("--bind must be an IP address".into()))?;
        if !bind_address.is_loopback() && !flag_flag(args, "--allow-network-bind") {
            return Err(LabError::InvalidConfig(
                "non-loopback bind requires explicit --allow-network-bind; publish container ports on loopback unless public exposure is intended".into(),
            ));
        }
        let port = flag_value(args, "--port")
            .map(|raw| {
                raw.parse::<u16>()
                    .map_err(|error| LabError::InvalidConfig(format!("--port: {error}")))
            })
            .transpose()?
            .unwrap_or(8130);
        if port == 0 {
            return Err(LabError::InvalidConfig(
                "--port must be in 1..=65535".into(),
            ));
        }
        let allow_hosts = args
            .windows(2)
            .filter(|window| window[0] == "--allow-host")
            .map(|window| {
                let authority = window[1]
                    .parse::<axum::http::uri::Authority>()
                    .map_err(|_| {
                        LabError::InvalidConfig("--allow-host must be an HTTP authority".into())
                    })?;
                if authority.as_str().contains('@')
                    || authority.host().is_empty()
                    || authority
                        .port()
                        .is_some_and(|_| authority.port_u16().is_none_or(|port| port == 0))
                {
                    return Err(LabError::InvalidConfig(
                        "invalid --allow-host authority".into(),
                    ));
                }
                Ok(window[1].clone())
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        let auth = server_auth_config(args)?;
        if !allow_hosts.is_empty()
            && !matches!(auth, ServerAuth::PublicNoAuth | ServerAuth::OAuth(_))
        {
            return Err(LabError::InvalidConfig(
                "additional hosts require explicit --public-no-auth or --oauth".into(),
            ));
        }
        if auth.exposure().public_no_auth() && flag_value(args, "--data-root").is_none() {
            return Err(LabError::InvalidConfig(
                "public mode requires an explicit isolated --data-root".into(),
            ));
        }
        Ok(Self {
            bind_address,
            port,
            data_root: data_root(args),
            allow_hosts,
            auth,
        })
    }
}

fn server_auth_config(args: &[String]) -> Result<ServerAuth, LabError> {
    let public = flag_flag(args, "--public-no-auth");
    let oauth = flag_flag(args, "--oauth");
    let auth_token = flag_value(args, "--auth-token");
    let public_url = flag_value(args, "--public-url");
    if usize::from(public) + usize::from(oauth) + usize::from(auth_token.is_some()) > 1 {
        return Err(LabError::InvalidConfig(
            "--public-no-auth, --oauth, and --auth-token are mutually exclusive".into(),
        ));
    }
    if !oauth && public_url.is_some() {
        return Err(LabError::InvalidConfig(
            "--public-url requires --oauth".into(),
        ));
    }
    if public {
        return Ok(ServerAuth::PublicNoAuth);
    }
    if let Some(token) = auth_token {
        if token.len() < 16 {
            return Err(LabError::InvalidConfig(
                "--auth-token must contain at least 16 characters".into(),
            ));
        }
        return Ok(ServerAuth::Bearer(token));
    }
    if !oauth {
        return Ok(ServerAuth::None);
    }
    let public_url = public_url.ok_or_else(|| {
        LabError::InvalidConfig("--oauth requires --public-url https://HOST".into())
    })?;
    let owner_code = std::env::var("SPOT_LAB_OAUTH_OWNER_CODE").map_err(|_| {
        LabError::InvalidConfig(
            "--oauth requires SPOT_LAB_OAUTH_OWNER_CODE in the environment".into(),
        )
    })?;
    let oauth = spot_lab::oauth::OAuthState::new(&public_url, owner_code)
        .map_err(LabError::InvalidConfig)?;
    if !args
        .windows(2)
        .any(|window| window[0] == "--allow-host" && window[1] == oauth.public_host())
    {
        return Err(LabError::InvalidConfig(
            "--oauth requires --allow-host matching the --public-url host".into(),
        ));
    }
    Ok(ServerAuth::OAuth(oauth))
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
        "            [--oauth --public-url https://HOST] # owner code comes from SPOT_LAB_OAUTH_OWNER_CODE"
    );
    println!("            [--auth-token <secret>] # legacy Bearer header only");
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
    println!("  research-suite --request <json-or-toml> [--wait] [--data-root data]");
    println!("  collection-schedule --request <json-or-toml> [--data-root data]");
    println!("  storage-maintenance --request <json-or-toml> [--data-root data]");
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
    server: McpServeConfig,
    upbit: UpbitClient,
    git_revision: Option<String>,
    database: DatabaseHandle,
) -> Result<(), LabError> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    let McpServeConfig {
        bind_address,
        port,
        data_root,
        allow_hosts,
        auth: server_auth,
    } = server;
    let exposure = server_auth.exposure();
    let public_no_auth = exposure.public_no_auth();
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
    config.allowed_hosts.extend(allow_hosts.iter().cloned());
    let allowed_origins: std::sync::Arc<[String]> = config.allowed_origins.clone().into();
    let listener = tokio::net::TcpListener::bind((bind_address, port))
        .await
        .map_err(|error| LabError::InvalidConfig(format!("bind {bind_address}:{port}: {error}")))?;
    let listener = spot_lab::transport::BoundedListener::new(
        listener,
        spot_lab::transport::TransportLimits::default(),
    );
    let state = listener.state();
    let jobs = spot_lab::jobs::JobRuntime::start(
        database.clone(),
        upbit.clone(),
        data_root,
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
                exposure,
            ))
        },
        LocalSessionManager::default().into(),
        config,
    );
    let server_auth = std::sync::Arc::new(server_auth);
    let auth_guard = server_auth.clone();
    let protected_mcp = axum::Router::new()
        .nest_service("/mcp", service)
        .route_layer(axum::middleware::from_fn(
            move |request: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| {
                let auth = auth_guard.clone();
                async move {
                    let authorized = match auth.as_ref() {
                        ServerAuth::None | ServerAuth::PublicNoAuth => true,
                        ServerAuth::Bearer(expected) => {
                            spot_lab::oauth::accepts_legacy_bearer(request.headers(), expected)
                        }
                        ServerAuth::OAuth(state) => state.accepts_bearer(request.headers()),
                    };
                    if authorized {
                        next.run(request).await
                    } else {
                        let mut response = axum::http::Response::builder()
                            .status(axum::http::StatusCode::UNAUTHORIZED);
                        if let ServerAuth::OAuth(state) = auth.as_ref() {
                            response = response.header(
                                axum::http::header::WWW_AUTHENTICATE,
                                format!(
                                    "Bearer resource_metadata=\"{}\", scope=\"mcp\"",
                                    state.resource_metadata_url()
                                ),
                            );
                        }
                        response
                            .body(axum::body::Body::empty())
                            .unwrap_or_else(|_| axum::response::Response::default())
                    }
                }
            },
        ));
    let oauth_routes = match server_auth.as_ref() {
        ServerAuth::OAuth(oauth) => oauth.router(),
        _ => axum::Router::new(),
    };
    let allowed_hosts: std::sync::Arc<[String]> = [
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
    ]
    .into_iter()
    .chain(allow_hosts)
    .collect::<Vec<_>>()
    .into();
    let app = oauth_routes
        .merge(protected_mcp)
        .layer(axum::middleware::from_fn(
            move |request: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| {
                let allowed_hosts = allowed_hosts.clone();
                let allowed_origins = allowed_origins.clone();
                async move {
                    let host_ok = request
                        .headers()
                        .get(axum::http::header::HOST)
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|host| allowed_hosts.iter().any(|allowed| allowed == host));
                    let origin_ok = request
                        .headers()
                        .get(axum::http::header::ORIGIN)
                        .is_none_or(|value| {
                            value.to_str().is_ok_and(|origin| {
                                allowed_origins.iter().any(|allowed| allowed == origin)
                            })
                        });
                    if host_ok && origin_ok {
                        next.run(request).await
                    } else {
                        axum::http::Response::builder()
                            .status(axum::http::StatusCode::FORBIDDEN)
                            .body(axum::body::Body::empty())
                            .unwrap_or_else(|_| axum::response::Response::default())
                    }
                }
            },
        ));
    let app = spot_lab::transport::apply_limits(app, state.clone());
    record_lifecycle_phase(LifecyclePhase::Startup, &state);
    tracing::info!(event = "mcp_listening", bind = %bind_address, port, public_no_auth, auth = exposure.auth_name(), transport = "STATELESS_JSON");
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
            "research-suite-action",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::ResearchSuiteAction
            ))?,
        ),
        (
            "research-suite-request",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::ResearchSuiteRequest
            ))?,
        ),
        (
            "research-suite-summary",
            schema_json(&schemars::schema_for!(spot_lab::contracts::SuiteSummary))?,
        ),
        (
            "research-suite-case",
            schema_json(&schemars::schema_for!(spot_lab::contracts::SuiteCase))?,
        ),
        (
            "research-comparison-row",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::SuiteComparisonRow
            ))?,
        ),
        (
            "collection-schedule-action",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::CollectionScheduleAction
            ))?,
        ),
        (
            "collection-schedule-record",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::CollectionScheduleRecord
            ))?,
        ),
        (
            "collection-freshness",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::CollectionFreshness
            ))?,
        ),
        (
            "storage-maintenance-action",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::StorageMaintenanceAction
            ))?,
        ),
        (
            "storage-maintenance-result",
            schema_json(&schemars::schema_for!(
                spot_lab::contracts::StorageMaintenanceResult
            ))?,
        ),
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

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(label: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("spot-lab-{label}-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ignored = std::fs::remove_dir_all(&self.0);
        }
    }

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
    fn invalid_listener_configuration_does_not_create_research_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-invalid-listener-{}-{nonce}",
            std::process::id()
        ));
        let args = vec![
            "mcp-serve".into(),
            "--public-no-auth".into(),
            "--port".into(),
            "not-a-port".into(),
            "--data-root".into(),
            root.to_string_lossy().into_owned(),
        ];
        let result = dispatch(&args);
        let storage_created = root.exists();
        if storage_created {
            std::fs::remove_dir_all(&root)?;
        }
        assert!(result.is_err());
        assert!(!storage_created, "invalid listener settings opened storage");
        Ok(())
    }

    #[test]
    fn server_auth_modes_reject_conflicts_before_runtime_effects() {
        assert!(server_auth_config(&strings(&["--public-no-auth", "--oauth"])).is_err());
        assert!(
            server_auth_config(&strings(&["--public-url", "https://skybackcraft.store"])).is_err()
        );
        assert!(
            server_auth_config(&strings(&[
                "--oauth",
                "--public-url",
                "https://skybackcraft.store",
                "--allow-host",
                "other.example",
            ]))
            .is_err()
        );
        assert!(server_auth_config(&strings(&["--auth-token", "too-short"])).is_err());
        assert!(matches!(
            server_auth_config(&strings(&["--auth-token", "long-enough-secret"])),
            Ok(ServerAuth::Bearer(_))
        ));
    }

    #[test]
    fn suite_wait_rejects_read_only_actions_before_storage_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempRoot::new("invalid-suite-wait")?;
        let data_root = temporary.0.join("data");
        let request = temporary.0.join("request.json");
        std::fs::write(
            &request,
            serde_json::to_vec(&spot_lab::contracts::ResearchSuiteAction::Get {
                suite_id: spot_lab::contracts::SuiteId::new("suite-read-only")?,
            })?,
        )?;
        let result = dispatch(&[
            "research-suite".into(),
            "--request".into(),
            request.to_string_lossy().into_owned(),
            "--wait".into(),
            "--data-root".into(),
            data_root.to_string_lossy().into_owned(),
        ]);
        assert!(result.is_err());
        assert!(!data_root.exists(), "invalid --wait opened storage");
        Ok(())
    }

    #[test]
    fn readonly_research_cli_child() -> Result<(), Box<dyn std::error::Error>> {
        let (Ok(command), Ok(request), Ok(root)) = (
            std::env::var("SPOT_LAB_READONLY_CHILD_COMMAND"),
            std::env::var("SPOT_LAB_READONLY_CHILD_REQUEST"),
            std::env::var("SPOT_LAB_READONLY_CHILD_ROOT"),
        ) else {
            return Ok(());
        };
        dispatch(&[
            command,
            "--request".into(),
            request,
            "--data-root".into(),
            root,
        ])?;
        Ok(())
    }

    #[test]
    fn readonly_research_cli_preserves_stale_and_queued_work_across_processes()
    -> Result<(), Box<dyn std::error::Error>> {
        use spot_lab::contracts::{CollectionScheduleAction, StorageMaintenanceAction};

        let temporary = TempRoot::new("readonly-research-cli")?;
        let data_root = temporary.0.join("data");
        let (first_id, second_id, schedule_id) = seed_readonly_research_cli(&data_root)?;

        let before = research_cli_state(&data_root, &first_id, &second_id, &schedule_id)?;
        let cases = [
            (
                "storage-maintenance",
                serde_json::to_value(StorageMaintenanceAction::Usage)?,
            ),
            (
                "collection-schedule",
                serde_json::to_value(CollectionScheduleAction::Get {
                    schedule_id: schedule_id.clone(),
                })?,
            ),
            (
                "collection-schedule",
                serde_json::to_value(CollectionScheduleAction::List {
                    offset: 0,
                    limit: 10,
                })?,
            ),
            (
                "collection-schedule",
                serde_json::to_value(CollectionScheduleAction::Freshness {
                    schedule_id: schedule_id.clone(),
                    probe_source: false,
                })?,
            ),
        ];
        for (index, (command, action)) in cases.into_iter().enumerate() {
            run_readonly_research_cli_child(&temporary.0, &data_root, index, command, &action)?;
            let after = research_cli_state(&data_root, &first_id, &second_id, &schedule_id)?;
            assert_eq!(
                before, after,
                "read-only {command} changed attempts or producer state"
            );
        }
        Ok(())
    }

    fn seed_readonly_research_cli(
        data_root: &std::path::Path,
    ) -> Result<
        (
            spot_lab::contracts::JobId,
            spot_lab::contracts::JobId,
            spot_lab::contracts::ScheduleId,
        ),
        Box<dyn std::error::Error>,
    > {
        use spot_lab::contracts::{
            CandleInterval, CollectRequest, CollectionScheduleRequest, JobPayload, JobSubmission,
            MarketId, RequestId, ScheduleRetryPolicy, UtcRange,
        };

        let now = UtcTimestamp::parse_rfc3339("2024-01-02T00:00:00Z")?;
        let range = UtcRange::new(
            UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z")?,
            UtcTimestamp::parse_rfc3339("2024-01-01T01:00:00Z")?,
        )?;
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let queued = |seed: &str| -> Result<JobSubmission, LabError> {
            Ok(JobSubmission {
                request_id: RequestId::new(format!("readonly-job-{seed}"))?,
                payload: JobPayload::Collect {
                    request: CollectRequest {
                        request_id: RequestId::new(format!("readonly-collect-{seed}"))?,
                        markets: vec![market.clone()],
                        range,
                        data_resolution: CandleInterval::H1,
                        warmup_bars: 0,
                        completed_only: true,
                    },
                },
            })
        };
        let first = queued("running")?;
        let second = queued("queued")?;
        let schedule = spot_lab::scheduling::create_schedule(
            CollectionScheduleRequest {
                request_id: RequestId::new("readonly-schedule")?,
                markets: vec![market],
                interval: CandleInterval::H1,
                lookback_bars: 1,
                cadence_seconds: 60,
                freshness_policy: None,
                retry: ScheduleRetryPolicy {
                    max_retries: 1,
                    backoff_seconds: 60,
                },
            },
            now,
        )?;
        let schedule_id = schedule.id.clone();
        let owner = DatabaseOwner::open(data_root.to_path_buf())?;
        let (first_id, second_id) =
            owner
                .handle()
                .call_blocking("seed_readonly_research_cli", move |store| {
                    let first = store.submit_job(&first, now)?;
                    let second = store.submit_job(&second, now)?;
                    let _claimed = store.claim_next(now)?.ok_or_else(|| {
                        LabError::Internal("seeded queued attempt was not claimable".into())
                    })?;
                    store.create_collection_schedule(&schedule)?;
                    Ok((first.id, second.id))
                })?;
        owner.shutdown()?;
        Ok((first_id, second_id, schedule_id))
    }

    fn run_readonly_research_cli_child(
        temporary: &std::path::Path,
        data_root: &std::path::Path,
        index: usize,
        command: &str,
        action: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request_path = temporary.join(format!("request-{index}.json"));
        std::fs::write(&request_path, serde_json::to_vec(action)?)?;
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "tests::readonly_research_cli_child",
                "--nocapture",
            ])
            .env("SPOT_LAB_READONLY_CHILD_COMMAND", command)
            .env("SPOT_LAB_READONLY_CHILD_REQUEST", &request_path)
            .env("SPOT_LAB_READONLY_CHILD_ROOT", data_root)
            .output()?;
        assert!(
            output.status.success(),
            "read-only {command} subprocess failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn research_cli_state(
        root: &std::path::Path,
        first: &spot_lab::contracts::JobId,
        second: &spot_lab::contracts::JobId,
        schedule: &spot_lab::contracts::ScheduleId,
    ) -> Result<serde_json::Value, LabError> {
        let owner = DatabaseOwner::open(root.to_path_buf())?;
        let first = first.clone();
        let second = second.clone();
        let schedule = schedule.clone();
        let value =
            owner
                .handle()
                .call_blocking("snapshot_readonly_research_cli", move |store| {
                    Ok(serde_json::json!({
                        "queue": store.job_queue_status()?,
                        "first": store.get_job(&first)?,
                        "second": store.get_job(&second)?,
                        "schedule": store.get_collection_schedule(&schedule)?,
                    }))
                })?;
        owner.shutdown()?;
        Ok(value)
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
