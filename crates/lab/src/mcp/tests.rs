use super::*;
use rmcp::{
    ServiceError, ServiceExt,
    model::{CallToolRequestParams, ErrorCode, object},
};

#[test]
fn decision_analysis_wire_budget_includes_escaping_and_rpc_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    let request_id = rmcp::model::RequestId::Number(17);
    let empty = text_result(&serde_json::json!({"value": ""}))?;
    let overhead = serde_json::to_vec(&rmcp::model::JsonRpcResponse {
        jsonrpc: rmcp::model::JsonRpcVersion2_0,
        id: request_id.clone(),
        result: empty,
    })?
    .len();
    let maximum_ascii = MAX_DECISION_ANALYSIS_ENCODED_BYTES - overhead;
    let at_limit = decision_analysis_result(
        Ok(serde_json::json!({"value": "x".repeat(maximum_ascii)})),
        &request_id,
    )?;
    assert_ne!(at_limit.is_error, Some(true));
    assert_eq!(
        serde_json::to_vec(&rmcp::model::JsonRpcResponse {
            jsonrpc: rmcp::model::JsonRpcVersion2_0,
            id: request_id.clone(),
            result: at_limit,
        })?
        .len(),
        MAX_DECISION_ANALYSIS_ENCODED_BYTES
    );
    let over_limit = decision_analysis_result(
        Ok(serde_json::json!({"value": "x".repeat(maximum_ascii + 1)})),
        &request_id,
    )?;
    assert_eq!(over_limit.is_error, Some(true));

    let escaped = serde_json::json!({"value": "\n".repeat(180_000)});
    assert!(
        serde_json::to_vec(&escaped)?.len() < crate::contracts::MAX_DECISION_ANALYSIS_OUTPUT_BYTES
    );
    let rejected = decision_analysis_result(Ok(escaped), &request_id)?;
    assert_eq!(rejected.is_error, Some(true));
    assert!(result_text(rejected).contains("no partial totals were returned"));
    Ok(())
}

async fn analysis_rpc(
    peer: &rmcp::service::Peer<rmcp::RoleClient>,
    query: crate::contracts::DecisionAnalysisQuery,
) -> Result<serde_json::Value, LabError> {
    let mut request = CallToolRequestParams::default();
    request.name = "decision_analysis_query".into();
    request.arguments = Some(object(serde_json::to_value(query)?));
    let result = peer
        .call_tool(request)
        .await
        .map_err(|error| LabError::Internal(format!("decision analysis MCP call: {error}")))?;
    if result.is_error == Some(true) {
        return Err(LabError::Internal(format!(
            "decision analysis MCP tool error: {}",
            result_text(result)
        )));
    }
    Ok(serde_json::from_str(&result_text(result))?)
}

fn save_analysis_wire(name: &str, value: &serde_json::Value) -> Result<(), LabError> {
    if let Some(directory) = std::env::var_os("SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)
            .map_err(|error| LabError::Internal(format!("create test wire directory: {error}")))?;
        std::fs::write(directory.join(name), serde_json::to_vec_pretty(value)?)
            .map_err(|error| LabError::Internal(format!("save test wire: {error}")))?;
    }
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real MCP journey proves both saved source families, period/page units and original comparison wire"
)]
fn saved_decision_analysis_mcp_workflow_preserves_both_source_families()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        DecisionAnalysisComparisonTarget, DecisionAnalysisDecisionFilter,
        DecisionAnalysisPageRequest, DecisionAnalysisQuery, DecisionAnalysisSelection,
        RecordedDecisionAction,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-decision-analysis-mcp-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = crate::database::DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let fixture = owner
            .handle()
            .call("seed_decision_analysis_mcp_fixture", |store| {
                store.seed_decision_analysis_fixture()
            })
            .await?;
        let upbit = UpbitClient::synthetic_local("http://127.0.0.1:1")?;
        let jobs =
            crate::jobs::JobRuntime::start(owner.handle(), upbit.clone(), root.clone(), None)
                .await?;
        let service =
            LabMcpService::new(owner.handle(), upbit, jobs.service(), McpExposure::Desktop);
        let (server_transport, client_transport) = tokio::io::duplex(32_768);
        let server = tokio::spawn(async move {
            service
                .serve(server_transport)
                .await
                .map_err(|error| error.to_string())?
                .waiting()
                .await
                .map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        });
        let client = ().serve(client_transport).await?;
        let journey = async {
            for (family, population, previous, selected) in [
                (
                    "paper",
                    fixture.paper_population,
                    fixture.paper_previous,
                    fixture.paper_selected,
                ),
                (
                    "automation",
                    fixture.automation_population,
                    fixture.automation_previous,
                    fixture.automation_selected,
                ),
            ] {
                let overview = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Overview {
                        population: population.clone(),
                    },
                )
                .await?;
                assert_eq!(overview["schema_version"], "1.0");
                assert_eq!(overview["population_complete"], true);
                assert_eq!(overview["totals"]["decisions"], 2);
                assert_eq!(overview["totals"]["unplaced_decisions"], 0);
                let selection: DecisionAnalysisSelection =
                    serde_json::from_value(overview["source_snapshot"]["selection"].clone())?;
                let page = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Decisions {
                        selection: selection.clone(),
                        page: DecisionAnalysisPageRequest {
                            offset: 0,
                            limit: 1,
                        },
                        filter: None,
                    },
                )
                .await?;
                assert_eq!(page["page"]["returned_count"], 1);
                assert_eq!(page["page"]["total_count"], 2);
                assert_eq!(page["page"]["in_period_count"], 2);
                assert_eq!(page["page"]["unplaced_count"], 0);
                assert_eq!(
                    page["page"]["items"][0]["decision_id"],
                    serde_json::to_value(&previous)?
                );
                let next_page = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Decisions {
                        selection: selection.clone(),
                        page: DecisionAnalysisPageRequest {
                            offset: 1,
                            limit: 1,
                        },
                        filter: None,
                    },
                )
                .await?;
                assert_eq!(
                    next_page["page"]["items"][0]["decision_id"],
                    serde_json::to_value(&selected)?
                );
                let detail = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Decision {
                        selection: selection.clone(),
                        decision_id: selected.clone(),
                    },
                )
                .await?;
                assert_eq!(
                    detail["decision"]["row"]["decision_id"],
                    serde_json::to_value(&selected)?
                );
                let comparison = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Compare {
                        selection: selection.clone(),
                        decision_id: selected.clone(),
                        against: DecisionAnalysisComparisonTarget::Previous,
                    },
                )
                .await?;
                assert_eq!(comparison["comparison"]["kind"], "previous");
                assert_eq!(
                    comparison["comparison"]["unavailable_reason"],
                    serde_json::Value::Null
                );
                assert_eq!(
                    comparison["comparison"]["compared"]["row"]["decision_id"],
                    serde_json::to_value(&previous)?
                );
                let mut complete_history = population.clone();
                complete_history.period.start = crate::contracts::UtcTimestamp::parse_rfc3339(
                    "2026-10-01T00:00:00Z",
                )?;
                let history_overview = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Overview { population: complete_history },
                )
                .await?;
                let history_selection: DecisionAnalysisSelection = serde_json::from_value(
                    history_overview["source_snapshot"]["selection"].clone(),
                )?;
                let earliest_page = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Decisions {
                        selection: history_selection.clone(),
                        page: DecisionAnalysisPageRequest { offset: 0, limit: 1 },
                        filter: None,
                    },
                )
                .await?;
                assert_eq!(earliest_page["page"]["returned_count"], 1);
                let earliest_id = serde_json::from_value(
                    earliest_page["page"]["items"][0]["decision_id"].clone(),
                )?;
                let unavailable = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Compare {
                        selection: history_selection,
                        decision_id: earliest_id,
                        against: DecisionAnalysisComparisonTarget::Previous,
                    },
                )
                .await?;
                assert_eq!(unavailable["comparison"]["unavailable_reason"], "no_previous_decision");
                assert!(unavailable["comparison"]["compared"].is_null());
                assert!(unavailable["compared_source_snapshot"].is_null());
                save_analysis_wire(&format!("{family}-no-previous-compare.json"), &unavailable)?;

                let mut all_markets = population.clone();
                all_markets.market = None;
                let all_overview = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Overview { population: all_markets.clone() },
                )
                .await?;
                let all_selection = serde_json::from_value(
                    all_overview["source_snapshot"]["selection"].clone(),
                )?;
                let all_comparison = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Compare {
                        selection: all_selection,
                        decision_id: selected.clone(),
                        against: DecisionAnalysisComparisonTarget::Previous,
                    },
                )
                .await?;
                assert!(all_comparison["selected_source_snapshot"]["selection"]["population"]["market"].is_null());
                assert!(all_comparison["compared_source_snapshot"]["selection"]["population"]["market"].is_null());
                assert_eq!(all_comparison["comparison"]["compared"]["row"]["decision_id"], serde_json::to_value(&previous)?);
                save_analysis_wire(&format!("{family}-all-markets-compare.json"), &all_comparison)?;
                if family == "automation" {
                    all_markets.period.start = crate::contracts::UtcTimestamp::parse_rfc3339(
                        detail["decision"]["row"]["decision_time"].as_str()
                            .ok_or_else(|| LabError::Internal("fixture decision time absent".into()))?,
                    )?;
                    let context_overview = analysis_rpc(
                        client.peer(),
                        DecisionAnalysisQuery::Overview { population: all_markets },
                    )
                    .await?;
                    assert_eq!(context_overview["totals"]["decisions"], 1);
                    let context_selection = serde_json::from_value(context_overview["source_snapshot"]["selection"].clone())?;
                    let context_comparison = analysis_rpc(
                        client.peer(),
                        DecisionAnalysisQuery::Compare {
                            selection: context_selection,
                            decision_id: selected.clone(),
                            against: DecisionAnalysisComparisonTarget::Previous,
                        },
                    )
                    .await?;
                    assert!(context_comparison["compared_source_snapshot"]["selection"]["population"]["market"].is_null());
                    assert_eq!(context_comparison["comparison"]["compared"]["row"]["decision_id"], serde_json::to_value(&previous)?);
                    save_analysis_wire("automation-preperiod-all-markets-compare.json", &context_comparison)?;
                }
                let explicit_comparison = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Compare {
                        selection: selection.clone(),
                        decision_id: selected.clone(),
                        against: DecisionAnalysisComparisonTarget::Selected {
                            selection: Box::new(selection.clone()),
                            decision_id: previous.clone(),
                        },
                    },
                )
                .await?;
                assert_eq!(explicit_comparison["comparison"]["kind"], "selected");
                let buys = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Decisions {
                        selection: selection.clone(),
                        page: DecisionAnalysisPageRequest {
                            offset: 0,
                            limit: 10,
                        },
                        filter: Some(DecisionAnalysisDecisionFilter {
                            strategy_action: Some(RecordedDecisionAction::Buy),
                            ..DecisionAnalysisDecisionFilter::default()
                        }),
                    },
                )
                .await?;
                assert_eq!(buys["page"]["total_count"], 1);
                assert_eq!(
                    buys["page"]["items"][0]["decision_id"],
                    serde_json::to_value(&previous)?
                );
                let reread = analysis_rpc(
                    client.peer(),
                    DecisionAnalysisQuery::Overview { population },
                )
                .await?;
                assert_eq!(reread["source_snapshot"], overview["source_snapshot"]);
                assert_eq!(reread["totals"], overview["totals"]);
                for (kind, value) in [
                    ("overview", &overview),
                    ("decisions", &page),
                    ("decision", &detail),
                    ("compare", &comparison),
                    ("selected-compare", &explicit_comparison),
                    ("buy-decisions", &buys),
                ] {
                    save_analysis_wire(&format!("{family}-{kind}.json"), value)?;
                }
            }
            Ok::<(), LabError>(())
        }
        .await;
        let client_closed = client.cancel().await;
        let server_closed = server.await;
        let jobs_closed = jobs.shutdown().await;
        if journey.is_err() {
            eprintln!("decision analysis fixture runtime shutdown: {jobs_closed:?}");
        }
        journey?;
        client_closed?;
        server_closed?.map_err(std::io::Error::other)?;
        jobs_closed?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    let removed = std::fs::remove_dir_all(root);
    result?;
    closed?;
    removed?;
    Ok(())
}

#[test]
fn tagged_schema_preserves_action_constraints_and_serde_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let original = serde_json::to_value(
        rmcp::handler::server::common::schema_for_type::<PaperReviewToolInput>().as_ref(),
    )?;
    let projected = serde_json::to_value(tagged_schema::<PaperReviewToolInput>().as_ref())?;
    let original_branches = original["oneOf"]
        .as_array()
        .expect("paper review input is an internally tagged enum");
    let projected_branches = projected["oneOf"]
        .as_array()
        .expect("projected paper review input remains a tagged union");
    assert_eq!(projected_branches.len(), original_branches.len());

    let mut union_fields = std::collections::BTreeSet::new();
    for branch in original_branches {
        let fields = branch["properties"]
            .as_object()
            .expect("supported action branches declare object properties");
        union_fields.extend(fields.keys().cloned());
    }
    let root_properties = union_fields
        .iter()
        .map(|name| (name.clone(), serde_json::json!({})))
        .collect::<serde_json::Map<_, _>>();
    assert_eq!(
        projected["properties"],
        serde_json::Value::Object(root_properties),
        "the root declares every action field without weakening branch constraints"
    );
    assert_eq!(projected["additionalProperties"], false);
    assert!(projected["properties"].get("link_id").is_none());

    for (original_branch, projected_branch) in original_branches.iter().zip(projected_branches) {
        let branch_fields = original_branch["properties"]
            .as_object()
            .expect("action branch properties remain available");
        let branch_field_names = branch_fields
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let absent_guards = union_fields
            .difference(&branch_field_names)
            .map(|name| serde_json::json!({"required": [name]}))
            .collect::<Vec<_>>();

        let mut expected_branch = original_branch.clone();
        let expected_object = expected_branch
            .as_object_mut()
            .expect("action branch is an object schema");
        assert_eq!(
            expected_object.remove("additionalProperties"),
            Some(serde_json::Value::Bool(false))
        );
        if !absent_guards.is_empty() {
            expected_object.insert(
                "not".to_owned(),
                serde_json::json!({"anyOf": absent_guards}),
            );
        }
        if let Some(selection) = original_branch["properties"].get("selection") {
            assert_eq!(selection["additionalProperties"], false);
        }
        assert_eq!(
            projected_branch, &expected_branch,
            "original properties, required fields, and nested schemas remain intact"
        );
    }
    assert_eq!(projected["$defs"], original["$defs"]);

    let original_struct = serde_json::to_value(
        rmcp::handler::server::common::schema_for_type::<crate::contracts::PaperReviewSelection>()
            .as_ref(),
    )?;
    let projected_struct =
        serde_json::to_value(tagged_schema::<crate::contracts::PaperReviewSelection>().as_ref())?;
    let mut expected_struct = original_struct;
    expected_struct["type"] = serde_json::json!("object");
    assert_eq!(
        projected_struct, expected_struct,
        "struct schemas stay unchanged"
    );

    let valid = serde_json::json!({
        "action": "decision",
        "selection": {
            "session_id": "6c9b0244-359d-4d2f-96d0-bb3ce810e2e4",
            "event_sequence": 1,
            "as_of_sequence": 1
        }
    });
    assert!(serde_json::from_value::<PaperReviewQuery>(valid.clone()).is_ok());

    let mut cross_action_field = valid.clone();
    cross_action_field["max_source_bars"] = serde_json::json!(32);
    assert!(serde_json::from_value::<PaperReviewQuery>(cross_action_field).is_err());

    let mut host_only_field = valid.clone();
    host_only_field["link_id"] = serde_json::json!("host-routing-id");
    assert!(serde_json::from_value::<PaperReviewQuery>(host_only_field).is_err());

    let mut missing_required = valid;
    missing_required
        .as_object_mut()
        .unwrap()
        .remove("selection");
    assert!(serde_json::from_value::<PaperReviewQuery>(missing_required).is_err());
    Ok(())
}

#[test]
fn tagged_schema_static_tool_catalog_keeps_root_compositions_host_extensible()
-> Result<(), Box<dyn std::error::Error>> {
    let tools = LabMcpService::tool_router().list_all();
    let mut composed_tools = 0;
    for tool in tools {
        let schema = serde_json::to_value(&tool.input_schema)?;
        let composition_names = ["oneOf", "anyOf", "allOf"]
            .into_iter()
            .filter(|name| schema.get(*name).is_some())
            .collect::<Vec<_>>();
        if composition_names.is_empty() {
            continue;
        }
        composed_tools += 1;

        assert_eq!(schema["type"], "object", "{} input schema", tool.name);
        assert_eq!(
            schema["additionalProperties"], false,
            "{} input root must close unknown fields",
            tool.name
        );
        let root_properties = schema["properties"]
            .as_object()
            .expect("composed MCP input schema declares root properties");
        let mut union_fields = std::collections::BTreeSet::new();
        for composition in &composition_names {
            let branches = schema[*composition]
                .as_array()
                .expect("root JSON Schema composition is an array");
            for branch in branches {
                assert_ne!(
                    branch.get("additionalProperties"),
                    Some(&serde_json::Value::Bool(false)),
                    "{} {composition} branch cannot close host-added root properties",
                    tool.name
                );
                if let Some(properties) =
                    branch.get("properties").and_then(|value| value.as_object())
                {
                    union_fields.extend(properties.keys().cloned());
                }
            }
        }
        assert_eq!(
            root_properties
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            union_fields,
            "{} root properties declare exactly the composed action fields",
            tool.name
        );
        assert!(
            root_properties
                .values()
                .all(|value| value == &serde_json::json!({})),
            "{} root declarations leave action-specific constraints in branches",
            tool.name
        );
    }
    assert!(
        composed_tools > 0,
        "the tool catalog contains composed schemas"
    );
    Ok(())
}

#[test]
fn ledger_projection_preserves_page_metadata_cost_units_and_open_exit()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{EpisodeStatus, Page, QueryCursor};
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-projection-{}-{nonce}",
        std::process::id()
    ));
    let store = crate::storage::Store::open(&root)?;
    let outcome = (|| {
        let bundle = crate::reporting::tests::policy_fixture();
        let model = &bundle.models[0];
        let fill = model.fills[0].clone();
        let fill_page = Page {
            records: vec![fill.clone()],
            returned_count: 1,
            total_count: 3,
            next_cursor: Some(QueryCursor {
                model_id: model.model_id.clone(),
                offset: 1,
            }),
            truncated_reason: Some("LIMIT".into()),
        };
        let mut expected = serde_json::to_value(&fill_page)?;
        let record = &mut expected["records"][0];
        record["price_cost_attribution_unit"] = serde_json::json!("KRW_PER_BASE_UNIT");
        record["price_difference_per_unit"] = serde_json::to_value(fill.price_cost_attribution)?;
        record["embedded_price_cost_quote"] =
            serde_json::to_value(crate::reporting::fill_price_cost_quote(&fill)?)?;
        record["embedded_price_cost_quote_unit"] = serde_json::json!("KRW");
        assert_eq!(
            result_page_value(&store, ResultPage::Fills(fill_page))?,
            serde_json::json!({"section":"fills", "page":expected})
        );

        let mut episode = model.episodes[0].clone();
        episode.status = EpisodeStatus::Open;
        episode.closed_at = None;
        episode.exit_reason = None;
        let episode_page = Page {
            records: vec![episode],
            returned_count: 1,
            total_count: 1,
            next_cursor: None,
            truncated_reason: None,
        };
        let mut expected = serde_json::to_value(&episode_page)?;
        expected["records"][0]["exit_reason_meaning"] =
            serde_json::json!("EXECUTION_RESULT_LEGACY_NAME");
        expected["records"][0]["exit_details"] = serde_json::json!({
            "execution_result":null, "closing_signal_id":null, "strategy_exit_reasons":[],
        });
        assert_eq!(
            result_page_value(&store, ResultPage::Episodes(episode_page))?,
            serde_json::json!({"section":"episodes", "page":expected})
        );
        Ok::<(), LabError>(())
    })();
    store.close()?;
    std::fs::remove_dir_all(root)?;
    outcome?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one SDK journey verifies the complete tool catalog and shared protocol boundary"
)]
fn full_tool_catalog_and_malformed_inputs_use_protocol_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!("spot-lab-mcp-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&root)?;
    let database = crate::database::DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let upbit = UpbitClient::new()?;
        let jobs =
            crate::jobs::JobRuntime::start(database.handle(), upbit.clone(), root.clone(), None)
                .await?;
        let service = LabMcpService::new(
            database.handle(),
            upbit,
            jobs.service(),
            McpExposure::Local,
        );
        let (server_transport, client_transport) = tokio::io::duplex(16_384);
        let (peer_tx, peer_rx) = tokio::sync::oneshot::channel();
        let transport_service = service.clone();
        let server_handle = tokio::spawn(async move {
            let server = transport_service
                .serve(server_transport)
                .await
                .map_err(|error| error.to_string())?;
            let _ = peer_tx.send(server.peer().clone());
            server.waiting().await.map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        });
        let client = ()
            .serve(client_transport)
            .await
            .map_err(|error| LabError::Internal(format!("start MCP regression client: {error}")))?;

        let peer = peer_rx.await.map_err(|error| LabError::Internal(error.to_string()))?;
        let context = || RequestContext::new(rmcp::model::RequestId::Number(1), peer.clone());
        for (exposure, auth, public) in [
            (McpExposure::Local, "none", false),
            (McpExposure::Desktop, "desktop_bearer", false),
            (McpExposure::PublicNoAuth, "none", true),
            (McpExposure::Bearer, "bearer", false),
            (McpExposure::OAuth, "oauth", false),
        ] {
            let mut projection = service.clone();
            projection.exposure = exposure;
            let status_result = projection
                .lab_status(context())
                .await
                .map_err(|error| LabError::Internal(format!("status projection: {error:?}")))?;
            let status: serde_json::Value = serde_json::from_str(&result_text(status_result))?;
            assert_eq!(
                status["transport"],
                serde_json::json!({
                    "auth": auth, "public_no_auth": public, "progress_stream": false,
                    "job_results": "poll job_control by opaque job_id",
                    "realtime_status_stream": "/realtime/events",
                    "realtime_stream_semantics": "latest health snapshots; durable results remain queryable",
                })
            );
            assert_eq!(
                status["deletion"]["public_no_auth_warning"]
                    .as_str()
                    .is_some_and(|warning| !warning.is_empty()),
                public
            );
            let expected_deletion = if matches!(exposure, McpExposure::Desktop) {
                "PREVIEW_ONLY"
            } else if public {
                "DISABLED_IN_PUBLIC_NO_AUTH"
            } else {
                "HARD_DELETE_ROWS_AND_EXCLUSIVE_FILES"
            };
            assert_eq!(status["deletion"]["mode"], expected_deletion);
            assert_eq!(status["deletion"]["shared_by_all_callers"], false);
            assert_eq!(
                status["policy_deletion"],
                serde_json::json!({
                    "mode": if matches!(exposure, McpExposure::Desktop) {
                        "POLICY_ONLY_CONFIRMED"
                    } else {
                        "UNAVAILABLE"
                    },
                    "max_items": 100,
                    "archive_available": false,
                    "request_readback": true,
                    "confirmation_required": true,
                })
            );
        }
        let desktop = LabMcpService::new_with_exchange(
            service.database.clone(),
            service.upbit.clone(),
            service.jobs.clone(),
            McpExposure::Desktop,
            UpbitExchangeService::credential_free_mock(),
        );
        let mut desktop_names = desktop
            .tool_router
            .map
            .keys()
            .map(std::convert::AsRef::as_ref)
            .collect::<Vec<_>>();
        desktop_names.sort_unstable();
        let mut expected_desktop = DESKTOP_TOOL_NAMES.to_vec();
        expected_desktop.sort_unstable();
        assert_eq!(desktop_names, expected_desktop);
        let local_private = service
            .upbit_account_balances(context())
            .await
            .map_err(|error| LabError::Internal(format!("local private guard: {error:?}")))?;
        assert_eq!(local_private.is_error, Some(true));
        assert!(result_text(local_private).contains("require the native desktop capability"));
        let desktop_private = desktop
            .upbit_account_balances(context())
            .await
            .map_err(|error| LabError::Internal(format!("desktop private read: {error:?}")))?;
        assert_ne!(desktop_private.is_error, Some(true));
        let desktop_automation = desktop
            .automation_session_query(object(serde_json::json!({
                "action": "list", "offset": 0, "limit": 10
            })), context())
            .await
            .map_err(|error| LabError::Internal(format!("desktop automation query: {error:?}")))?;
        assert_ne!(desktop_automation.is_error, Some(true));
        let rejected_request_id = crate::contracts::RequestId::new(
            "automation-mcp-rejected-request",
        )?;
        let rejected_write = crate::contracts::AutomationSessionWrite::Start {
            request_id: rejected_request_id.clone(),
            policy_ref: crate::contracts::PolicyRevisionRef {
                policy_id: crate::contracts::PolicyId::new("missing-automation-policy")?,
                revision_id: crate::contracts::PolicyRevisionId::new(
                    "missing-automation-revision",
                )?,
                definition_digest: crate::contracts::ContentHash::of_bytes(
                    b"missing automation fixture",
                ),
            },
            market: crate::contracts::MarketId::parse_upbit("KRW-BTC")?,
            decision_interval: crate::contracts::CandleInterval::M1,
            mode: crate::contracts::AutomationSessionMode::Mock,
            risk: crate::contracts::AutomationRiskPolicy {
                activation_loss_limit_krw: None,
                max_order_notional_krw: crate::contracts::QuoteAmount::new(
                    rust_decimal::Decimal::from(100_000),
                )?,
                max_daily_notional_krw: crate::contracts::QuoteAmount::new(
                    rust_decimal::Decimal::from(1_000_000),
                )?,
                max_asset_exposure_krw: crate::contracts::QuoteAmount::new(
                    rust_decimal::Decimal::from(100_000),
                )?,
                max_daily_loss_krw: crate::contracts::QuoteAmount::new(
                    rust_decimal::Decimal::from(100_000),
                )?,
                max_open_orders: 1,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 3_600,
                max_feed_age_seconds: 3_600,
            },
        };
        let rejected_write_args = serde_json::to_value(rejected_write)?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                LabError::Internal("automation write did not serialize as an object".into())
            })?;
        let rejected_write_result = desktop
            .automation_session_write(rejected_write_args.clone(), context())
            .await
            .map_err(|error| LabError::Internal(format!("desktop automation write: {error:?}")))?;
        assert_eq!(rejected_write_result.is_error, Some(true));
        let readback_args = object(serde_json::json!({
            "action": "request",
            "request_id": rejected_request_id.as_str(),
        }));
        let request_readback = desktop
            .automation_session_query(readback_args, context())
            .await
            .map_err(|error| LabError::Internal(format!("desktop request readback: {error:?}")))?;
        let request_readback: serde_json::Value =
            serde_json::from_str(&result_text(request_readback))?;
        assert_eq!(request_readback["state"], "REJECTED");
        for exposure in [
            McpExposure::PublicNoAuth,
            McpExposure::OAuth,
            McpExposure::Bearer,
        ] {
            let mut protected = service.clone();
            protected.exposure = exposure;
            let read = protected
                .automation_session_query(object(serde_json::json!({
                    "action": "list", "offset": 0, "limit": 10
                })), context())
                .await
                .map_err(|error| LabError::Internal(format!("protected automation read: {error:?}")))?;
            assert_eq!(read.is_error, Some(true));
            assert!(result_text(read).contains("require the native desktop capability"));
            let write = protected
                .automation_session_write(rejected_write_args.clone(), context())
                .await
                .map_err(|error| LabError::Internal(format!("protected automation write: {error:?}")))?;
            assert_eq!(write.is_error, Some(true));
            assert!(result_text(write).contains("require the native desktop capability"));
        }
        let local_automation = service
            .automation_session_query(object(serde_json::json!({
                "action": "list", "offset": 0, "limit": 10
            })), context())
            .await
            .map_err(|error| LabError::Internal(format!("local automation query: {error:?}")))?;
        assert_eq!(local_automation.is_error, Some(true));
        assert!(result_text(local_automation).contains("require the native desktop capability"));
        for exposure in [
            McpExposure::Local,
            McpExposure::PublicNoAuth,
            McpExposure::Bearer,
            McpExposure::OAuth,
        ] {
            let mut protected = service.clone();
            protected.exposure = exposure;
            let write = protected
                .policy_delete_write(object(serde_json::json!({})))
                .await
                .map_err(|error| LabError::Internal(format!("protected policy delete write: {error:?}")))?;
            assert_eq!(write.is_error, Some(true));
            assert!(result_text(write).contains("require the native desktop capability"));
            let query = protected
                .policy_delete_query(object(serde_json::json!({})))
                .await
                .map_err(|error| LabError::Internal(format!("protected policy delete query: {error:?}")))?;
            assert_eq!(query.is_error, Some(true));
            assert!(result_text(query).contains("require the native desktop capability"));
        }
        let missing_policy_delete = desktop
            .policy_delete_query(object(serde_json::json!({
                "request_id": "policy-delete-missing-query"
            })))
            .await
            .map_err(|error| LabError::Internal(format!("desktop policy delete readback: {error:?}")))?;
        let missing_policy_delete: serde_json::Value =
            serde_json::from_str(&result_text(missing_policy_delete))?;
        assert_eq!(missing_policy_delete["found"], false);
        let mut definition = crate::contracts::builtin_policy_definitions()?[0].2.clone();
        definition.name = "Synthetic MCP policy deletion".into();
        let policy_write = PolicyWrite::Create {
            request_id: RequestId::new("mcp-policy-delete-fixture-create")?,
            definition,
        };
        let policy = desktop
            .database
            .call("mcp_test_policy_delete_fixture", move |store| {
                store.write_policy(&policy_write, crate::contracts::UtcTimestamp::now())
            })
            .await?;
        let preview = desktop
            .resource_delete_preview(object(serde_json::json!({
                "kind": "policy",
                "policy_id": policy.snapshot.reference.policy_id.as_str(),
            })))
            .await
            .map_err(|error| LabError::Internal(format!("synthetic policy preview: {error:?}")))?;
        let preview: crate::contracts::DeletePreview =
            serde_json::from_str(&result_text(preview))?;
        let invalid_policy_delete = PolicyDeleteWriteRequest {
            request_id: RequestId::new("mcp-policy-delete-invalid-confirm")?,
            items: vec![crate::contracts::PolicyDeleteItemRequest {
                policy_id: policy.snapshot.reference.policy_id.clone(),
                preview: Some(crate::contracts::PolicyDeletePreviewEcho {
                    scope_digest: Some(preview.scope_digest.clone()),
                    expires_at: Some(preview.expires_at),
                    confirmation_token: Some(preview.confirmation_token.clone()),
                }),
            }],
            confirmation_text: "not the policy name or ID".into(),
        };
        let invalid_args = serde_json::to_value(&invalid_policy_delete)?
            .as_object()
            .cloned()
            .ok_or_else(|| LabError::Internal("invalid policy delete was not an object".into()))?;
        let invalid_result = desktop
            .policy_delete_write(invalid_args)
            .await
            .map_err(|error| LabError::Internal(format!("invalid policy delete: {error:?}")))?;
        assert_eq!(invalid_result.is_error, Some(true));
        let invalid_error: crate::contracts::PolicyDeleteWriteError =
            serde_json::from_str(&result_text(invalid_result))?;
        assert_eq!(
            invalid_error.error.code,
            crate::contracts::PolicyDeleteWriteErrorCode::ConfirmationMismatch
        );
        let invalid_readback = desktop
            .policy_delete_query(object(serde_json::json!({
                "request_id": invalid_policy_delete.request_id.as_str(),
            })))
            .await
            .map_err(|error| LabError::Internal(format!("invalid policy delete readback: {error:?}")))?;
        let invalid_readback: serde_json::Value =
            serde_json::from_str(&result_text(invalid_readback))?;
        assert_eq!(invalid_readback["found"], false);
        let policy_delete = PolicyDeleteWriteRequest {
            request_id: RequestId::new("mcp-policy-delete-fixture-confirm")?,
            items: vec![crate::contracts::PolicyDeleteItemRequest {
                policy_id: policy.snapshot.reference.policy_id.clone(),
                preview: Some(crate::contracts::PolicyDeletePreviewEcho {
                    scope_digest: Some(preview.scope_digest),
                    expires_at: Some(preview.expires_at),
                    confirmation_token: Some(preview.confirmation_token),
                }),
            }],
            confirmation_text: policy.snapshot.definition.name,
        };
        let policy_delete_args = serde_json::to_value(&policy_delete)?
            .as_object()
            .cloned()
            .ok_or_else(|| LabError::Internal("policy delete request was not an object".into()))?;
        let policy_delete = desktop
            .policy_delete_write(policy_delete_args)
            .await
            .map_err(|error| LabError::Internal(format!("desktop policy delete write: {error:?}")))?;
        assert_ne!(policy_delete.is_error, Some(true));
        let policy_delete: crate::contracts::PolicyDeleteReadback =
            serde_json::from_str(&result_text(policy_delete))?;
        assert_eq!(
            policy_delete.state,
            Some(crate::contracts::PolicyDeleteRequestState::Complete)
        );
        assert_eq!(
            policy_delete.items.as_ref().unwrap()[0].status,
            crate::contracts::PolicyDeleteItemStatus::Deleted
        );
        let tools = client
            .list_all_tools()
            .await
            .map_err(|error| LabError::Internal(format!("list MCP tools: {error}")))?;
        let mut names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "artifact_query",
                "automation_session_query",
                "automation_session_write",
                "collect_data",
                "collection_schedule",
                "dataset_query",
                "decision_analysis_query",
                "evidence_register",
                "export_report",
                "history_query",
                "job_control",
                "lab_status",
                "live_order_control",
                "paper_review_query",
                "paper_session_query",
                "paper_session_write",
                "plan_backtest",
                "policy_query",
                "policy_write",
                "portfolio_backtest",
                "probe_upbit",
                "realtime_status",
                "research_suite",
                "resource_delete_preview",
                "resource_hard_delete",
                "result_query",
                "run_backtests",
                "storage_maintenance",
                "trading_control",
                "upbit_account_balances",
                "upbit_order_cancel",
                "upbit_order_chance",
                "upbit_order_execute",
                "upbit_order_get",
                "upbit_order_preview",
                "upbit_order_reconcile",
                "upbit_order_test",
                "upbit_orders_closed",
                "upbit_orders_open",
                "verify_run",
            ]
        );
        let analysis_tool = tools
            .iter()
            .find(|tool| tool.name.as_ref() == "decision_analysis_query")
            .expect("saved decision analysis is in the catalog");
        assert_eq!(
            serde_json::to_value(&analysis_tool.input_schema)?,
            serde_json::to_value(decision_analysis_tool_input_schema().as_ref())?
        );
        for exposure in [
            McpExposure::Local,
            McpExposure::PublicNoAuth,
            McpExposure::Bearer,
            McpExposure::OAuth,
        ] {
            let mut protected = service.clone();
            protected.exposure = exposure;
            let denied = protected
                .decision_analysis_query(
                    object(serde_json::json!({
                        "action": "overview",
                        "population": {
                            "source": {"family": "automation", "session_id": "analysis-private-fixture"},
                            "period": {"start": "2026-10-01T00:00:00Z", "end": "2026-11-01T00:00:00Z"}
                        }
                    })),
                    context(),
                )
                .await
                .map_err(|error| LabError::Internal(format!("protected decision analysis: {error:?}")))?;
            assert_eq!(denied.is_error, Some(true));
            assert!(result_text(denied).contains("require the native desktop capability"));
        }
        let review_tool = tools
            .iter()
            .find(|tool| tool.name.as_ref() == "paper_review_query")
            .expect("paper review tool is in the catalog");
        let review_schema = serde_json::to_value(&review_tool.input_schema)?;
        assert_eq!(
            review_schema,
            serde_json::to_value(paper_review_tool_input_schema().as_ref())?
        );
        let review_variants = review_schema["oneOf"]
            .as_array()
            .expect("paper review input schema is a tagged union");
        let mut review_actions = review_variants
            .iter()
            .map(|variant| {
                variant["properties"]["action"]["const"]
                    .as_str()
                    .expect("paper review variant has a string action")
            })
            .collect::<Vec<_>>();
        review_actions.sort_unstable();
        assert_eq!(
            review_actions,
            [
                "audit_indicators",
                "audit_rules",
                "compare_contract",
                "counterfactual",
                "decision",
            ]
        );
        let expected_selection = serde_json::json!({
            "type": "object",
            "description": "An immutable event inside an explicitly selected saved prefix.",
            "properties": {
                "session_id": {"type": "string"},
                "event_sequence": {"type": "integer", "format": "uint64", "minimum": 0},
                "as_of_sequence": {"type": "integer", "format": "uint64", "minimum": 0},
                "signal_id": {"type": ["string", "null"]},
            },
            "required": ["session_id", "event_sequence", "as_of_sequence"],
            "additionalProperties": false,
        });
        let selection_schemas = review_variants
            .iter()
            .filter_map(|variant| variant["properties"].get("selection"))
            .collect::<Vec<_>>();
        assert_eq!(selection_schemas.len(), 4);
        for selection_schema in selection_schemas {
            assert_eq!(selection_schema, &expected_selection);
        }
        let domain_review_schema =
            serde_json::to_value(tagged_schema::<PaperReviewQuery>().as_ref())?;
        let domain_review_variants = domain_review_schema["oneOf"]
            .as_array()
            .expect("domain paper review schema is a tagged union");
        for action in [
            "decision",
            "audit_rules",
            "audit_indicators",
            "compare_contract",
        ] {
            let tool_variant = review_variants
                .iter()
                .find(|variant| variant["properties"]["action"]["const"] == action)
                .expect("MCP schema includes each original review action");
            let domain_variant = domain_review_variants
                .iter()
                .find(|variant| variant["properties"]["action"]["const"] == action)
                .expect("domain schema includes each original review action");
            assert_eq!(tool_variant, domain_variant, "original branch {action}");
        }
        let counterfactual_variant = review_variants
            .iter()
            .find(|variant| variant["properties"]["action"]["const"] == "counterfactual")
            .expect("MCP schema includes the counterfactual action");
        let request_schema = &counterfactual_variant["properties"]["request"];
        let counterfactual_request = request_schema
            .get("$ref")
            .and_then(serde_json::Value::as_str)
            .and_then(|reference| reference.strip_prefix('#'))
            .and_then(|pointer| review_schema.pointer(pointer))
            .unwrap_or(request_schema);
        assert_eq!(counterfactual_request["type"], "object");
        assert_eq!(
            counterfactual_request["required"],
            serde_json::json!(["candidates", "variants"])
        );
        for (name, max_items) in [
            ("candidates", crate::contracts::MAX_COUNTERFACTUAL_CANDIDATES),
            ("variants", crate::contracts::MAX_COUNTERFACTUAL_VARIANTS),
        ] {
            let array = &counterfactual_request["properties"][name];
            assert_eq!(array["type"], "array");
            assert_eq!(array["minItems"], 1);
            assert_eq!(array["maxItems"], serde_json::json!(max_items));
            assert_eq!(array["items"]["type"], "object");
            assert!(array["items"].get("properties").is_none());
            assert!(!matches!(
                array["items"].get("additionalProperties"),
                Some(serde_json::Value::Bool(false))
            ));
        }
        for tool in &tools {
            let annotations = tool
                .annotations
                .as_ref()
                .ok_or_else(|| LabError::Internal(format!("{} lacks annotations", tool.name)))?;
            let expected_read_only = matches!(
                tool.name.as_ref(),
                "lab_status"
                    | "realtime_status"
                    | "automation_session_query"
                    | "dataset_query"
                    | "decision_analysis_query"
                    | "history_query"
                    | "policy_query"
                    | "paper_review_query"
                    | "result_query"
                    | "artifact_query"
                    | "resource_delete_preview"
                    | "upbit_account_balances"
                    | "upbit_order_chance"
                    | "upbit_order_get"
                    | "upbit_orders_open"
                    | "upbit_orders_closed"
                    | "upbit_order_preview"
                    | "upbit_order_reconcile"
            );
            let expected_destructive = tool.name.as_ref() == "resource_hard_delete";
            assert_eq!(annotations.read_only_hint, Some(expected_read_only));
            assert_eq!(annotations.destructive_hint, Some(expected_destructive));
            assert_eq!(
                annotations.open_world_hint,
                Some(matches!(
                    tool.name.as_ref(),
                    "probe_upbit"
                        | "collect_data"
                        | "collection_schedule"
                        | "upbit_account_balances"
                        | "upbit_order_chance"
                        | "upbit_order_get"
                        | "upbit_orders_open"
                        | "upbit_orders_closed"
                        | "upbit_order_test"
                        | "upbit_order_reconcile"
                        | "automation_session_write"
                ))
            );
            assert_eq!(
                tool.input_schema.get("type"),
                Some(&serde_json::json!("object"))
            );
        }

        for (tool_name, arguments, expected) in [
            (
                "decision_analysis_query",
                serde_json::json!({
                    "action": "overview",
                    "population": {
                        "source": {"family": "paper", "session_id": "analysis-period-fixture"},
                        "period": {"start": "2026-11-01T00:00:00Z", "end": "2026-10-01T00:00:00Z"}
                    }
                }),
                "decision analysis period requires start < end",
            ),
            (
                "paper_review_query",
                serde_json::json!({"action":"decision","selection":{"session_id":"fixture","event_sequence":0,"as_of_sequence":1}}),
                "paper review requires 1 <= event_sequence <= as_of_sequence",
            ),
            (
                "paper_review_query",
                serde_json::json!({"action":"audit_indicators","selection":{"session_id":"fixture","event_sequence":1,"as_of_sequence":1},"max_source_bars":0}),
                "source review limit must be in 1..=4096",
            ),
            (
                "paper_review_query",
                serde_json::json!({
                    "action": "counterfactual",
                    "request": {"candidates": [], "variants": []}
                }),
                "counterfactual candidates require 1..=",
            ),
            (
                "probe_upbit",
                serde_json::json!({"count": 0}),
                "count must be in 1..=199",
            ),
            (
                "probe_upbit",
                serde_json::json!({"count": 500}),
                "count must be in 1..=199",
            ),
            (
                "probe_upbit",
                serde_json::json!({"cout": 2}),
                "unknown field `cout`",
            ),
            (
                "collect_data",
                serde_json::json!({"request_id": "request-test", "unknown": true}),
                "unknown field `unknown`",
            ),
            (
                "policy_query",
                serde_json::json!({"action": "list", "after_policy_id": null, "limit": 0}),
                "history limit must be in 1..=100",
            ),
            (
                "research_suite",
                serde_json::json!({"action": "list", "offset": 0, "limit": 0}),
                "research page limit must be in 1..=100",
            ),
            (
                "collection_schedule",
                serde_json::json!({"action": "list", "offset": 0, "limit": 101}),
                "research page limit must be in 1..=100",
            ),
            (
                "storage_maintenance",
                serde_json::json!({"action": "retention_candidates", "cutoff": "2024-01-01T00:00:00Z", "keep_recent": 1, "offset": 0, "limit": 0}),
                "research page limit must be in 1..=100",
            ),
        ] {
            let mut request = CallToolRequestParams::default();
            request.name = tool_name.into();
            request.arguments = Some(object(arguments));
            let result = client.call_tool(request).await;
            let Err(ServiceError::McpError(error)) = result else {
                return Err(LabError::Internal(format!(
                    "malformed {tool_name} arguments did not return an MCP error"
                )));
            };
            if error.code != ErrorCode::INVALID_PARAMS || !error.message.contains(expected) {
                return Err(LabError::Internal(format!(
                    "malformed {tool_name} returned unexpected MCP error: {error:?}"
                )));
            }
        }

        let mut create = CallToolRequestParams::default();
        create.name = "policy_write".into();
        create.arguments = Some(object(serde_json::json!({
            "action": "create",
            "request_id": "request-mcp-policy-create",
            "definition": {
                "schema_version": "1.0",
                "name": "MCP editable policy",
                "description": "MCP contract regression policy",
                "program": {
                    "kind": "BUILTIN",
                    "strategy": {"kind": "BUY_AND_HOLD"}
                }
            }
        })));
        let created = client
            .call_tool(create)
            .await
            .map_err(|error| LabError::Internal(format!("create policy through MCP: {error}")))?;
        if created.is_error == Some(true) {
            return Err(LabError::Internal(format!(
                "policy_write returned tool error: {}",
                result_text(created)
            )));
        }
        let created: serde_json::Value = serde_json::from_str(&result_text(created))?;
        let reference = created
            .pointer("/snapshot/reference")
            .cloned()
            .ok_or_else(|| LabError::Internal("policy_write omitted revision reference".into()))?;

        for (action, arguments) in [
            (
                "get",
                serde_json::json!({"action": "get", "reference": reference}),
            ),
            (
                "history",
                serde_json::json!({
                    "action": "history",
                    "policy_id": created.pointer("/snapshot/reference/policy_id"),
                    "before_revision_number": null,
                    "limit": 10
                }),
            ),
        ] {
            let mut request = CallToolRequestParams::default();
            request.name = "policy_query".into();
            request.arguments = Some(object(arguments));
            let response = client.call_tool(request).await.map_err(|error| {
                LabError::Internal(format!("policy {action} through MCP: {error}"))
            })?;
            if response.is_error == Some(true) {
                return Err(LabError::Internal(format!(
                    "policy {action} returned tool error: {}",
                    result_text(response)
                )));
            }
        }

        let mut history = CallToolRequestParams::default();
        history.name = "history_query".into();
        history.arguments = Some(object(serde_json::json!({
            "action": "jobs",
            "cursor": null,
            "limit": 10
        })));
        let history = client
            .call_tool(history)
            .await
            .map_err(|error| LabError::Internal(format!("query history through MCP: {error}")))?;
        if history.is_error == Some(true) {
            return Err(LabError::Internal(format!(
                "history_query returned tool error: {}",
                result_text(history)
            )));
        }

        let mut paper_options = CallToolRequestParams::default();
        paper_options.name = "paper_session_query".into();
        paper_options.arguments = Some(object(serde_json::json!({
            "action": "start_options"
        })));
        let paper_options = client
            .call_tool(paper_options)
            .await
            .map_err(|error| LabError::Internal(format!("query paper start_options: {error}")))?;
        if paper_options.is_error == Some(true) {
            return Err(LabError::Internal(format!(
                "paper_session_query start_options returned tool error: {}",
                result_text(paper_options)
            )));
        }

        client.cancel().await.map_err(|error| {
            LabError::Internal(format!("cancel MCP regression client: {error}"))
        })?;
        server_handle
            .await
            .map_err(|error| LabError::Internal(format!("join MCP regression server: {error}")))?
            .map_err(|error| LabError::Internal(format!("MCP regression server: {error}")))?;
        jobs.shutdown().await?;
        Ok::<(), LabError>(())
    });
    drop(runtime);
    let closed = database.shutdown();
    let removed = std::fs::remove_dir_all(root);
    result?;
    closed?;
    removed?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one transport journey verifies the full public-no-auth security boundary"
)]
fn public_no_auth_blocks_mutations_and_permits_reads() -> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-mcp-noauth-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let database = crate::database::DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let upbit = UpbitClient::new()?;
        let jobs =
            crate::jobs::JobRuntime::start(database.handle(), upbit.clone(), root.clone(), None)
                .await?;
        let service = LabMcpService::new(
            database.handle(),
            upbit,
            jobs.service(),
            McpExposure::PublicNoAuth,
        );

        let (server_transport, client_transport) = tokio::io::duplex(16_384);
        let server_handle = tokio::spawn(async move {
            let server = service
                .serve(server_transport)
                .await
                .map_err(|error| error.to_string())?;
            server.waiting().await.map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        });
        let client = ()
            .serve(client_transport)
            .await
            .map_err(|error| LabError::Internal(format!("start MCP noauth client: {error}")))?;

        // Mutations must be blocked
        let mut policy_params = CallToolRequestParams::default();
        policy_params.name = "policy_write".into();
        policy_params.arguments = Some(object(serde_json::json!({
            "action": "create",
            "request_id": "req-blocked",
            "definition": {
                "schema_version": "1.0",
                "name": "blocked",
                "description": "test",
                "program": { "kind": "BUILTIN", "strategy": { "kind": "BUY_AND_HOLD" } }
            }
        })));
        let policy_output = client
            .call_tool(policy_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_eq!(policy_output.is_error, Some(true));
        assert!(result_text(policy_output).contains("policy mutation is disabled"));

        let mut paper_write_params = CallToolRequestParams::default();
        paper_write_params.name = "paper_session_write".into();
        paper_write_params.arguments = Some(object(serde_json::json!({
            "action": "stop",
            "request_id": "req-paper-blocked",
            "session_id": "00000000-0000-0000-0000-000000000001",
            "expected_sequence": 1
        })));
        let paper_write_output = client
            .call_tool(paper_write_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_eq!(paper_write_output.is_error, Some(true));
        assert!(result_text(paper_write_output).contains("paper session mutation is disabled"));

        let mut compact_params = CallToolRequestParams::default();
        compact_params.name = "storage_maintenance".into();
        compact_params.arguments = Some(object(serde_json::json!({ "action": "compact" })));
        let compact_output = client
            .call_tool(compact_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_eq!(compact_output.is_error, Some(true));
        assert!(result_text(compact_output).contains("disabled when public_no_auth is enabled"));

        // Reads must be permitted
        let mut status_params = CallToolRequestParams::default();
        status_params.name = "lab_status".into();
        status_params.arguments = Some(object(serde_json::json!({})));
        let status_output = client
            .call_tool(status_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_ne!(status_output.is_error, Some(true));

        let mut list_params = CallToolRequestParams::default();
        list_params.name = "policy_query".into();
        list_params.arguments = Some(object(serde_json::json!({ "action": "list", "limit": 10 })));
        let list_output = client
            .call_tool(list_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_ne!(list_output.is_error, Some(true));

        let mut paper_list_params = CallToolRequestParams::default();
        paper_list_params.name = "paper_session_query".into();
        paper_list_params.arguments =
            Some(object(serde_json::json!({ "action": "list", "limit": 10 })));
        let paper_list_output = client
            .call_tool(paper_list_params)
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        assert_ne!(paper_list_output.is_error, Some(true));

        for (tool_name, arguments) in [
            (
                "automation_session_query",
                serde_json::json!({ "action": "list", "offset": 0, "limit": 10 }),
            ),
            (
                "automation_session_write",
                serde_json::json!({
                    "action": "stop",
                    "request_id": "req-public-automation",
                    "session_id": "automation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "expected_sequence": 1
                }),
            ),
            ("upbit_account_balances", serde_json::json!({})),
            ("upbit_orders_open", serde_json::json!({"limit": 10})),
            (
                "upbit_order_cancel",
                serde_json::json!({"order_id": "mock-order"}),
            ),
            ("trading_control", serde_json::json!({"action": "status"})),
            ("live_order_control", serde_json::json!({"action": "status"})),
            ("live_order_control", serde_json::json!({"action": "enable", "request_id":"public-denied", "expected_sequence":0, "expected_epoch":"uninitialized", "confirmation":"ENABLE_LIVE_ORDER_SENDING"})),
        ] {
            let mut request = CallToolRequestParams::default();
            request.name = tool_name.into();
            request.arguments = Some(object(arguments));
            let output = client
                .call_tool(request)
                .await
                .map_err(|error| LabError::Internal(error.to_string()))?;
            assert_eq!(output.is_error, Some(true), "{tool_name} must be denied");
            assert!(
                result_text(output).contains("require the native desktop capability"),
                "{tool_name} must fail before private-client access"
            );
        }

        client
            .cancel()
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?;
        server_handle
            .await
            .map_err(|e| LabError::Internal(e.to_string()))?
            .map_err(LabError::Internal)?;
        jobs.shutdown().await?;
        Ok::<(), LabError>(())
    });
    drop(runtime);
    let closed = database.shutdown();
    let removed = std::fs::remove_dir_all(root);
    result?;
    closed?;
    removed?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real TCP MCP journey checks correlation, privacy and parse/validation/dispatch outcomes"
)]
fn paper_query_diagnostics_correlate_and_redact_over_http() -> Result<(), Box<dyn std::error::Error>>
{
    const CHILD_PROCESS_ENV: &str = "SPOT_LAB_PAPER_QUERY_DIAGNOSTICS_CHILD";
    const CHILD_PROCESS_VALUE: &str = "isolated-v1";
    const CHILD_TEST: &str = "mcp::tests::paper_query_diagnostics_correlate_and_redact_over_http";

    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl<'writer> MakeWriter<'writer> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CaptureWriter(Arc::clone(&self.0))
        }
    }

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| io::Error::other("trace capture poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // Keep this HTTP correlation check separate from other parallel scoped-subscriber tests.
    let is_isolated_child = std::env::var_os(CHILD_PROCESS_ENV).as_deref()
        == Some(std::ffi::OsStr::new(CHILD_PROCESS_VALUE));
    if !is_isolated_child {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_PROCESS_ENV, CHILD_PROCESS_VALUE)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "isolated test process failed ({:?})\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            )
            .into());
        }
        let child_stdout = String::from_utf8_lossy(&output.stdout);
        let child_stderr = String::from_utf8_lossy(&output.stderr);
        let child_evidence = format!("{child_stdout}{child_stderr}");
        let expected_test_line = format!("test {CHILD_TEST} ... ok");
        let executed_exactly_one_test = child_evidence.contains("running 1 test")
            && child_evidence.contains(&expected_test_line)
            && child_evidence.contains("test result: ok. 1 passed; 0 failed;");
        if !executed_exactly_one_test {
            return Err(format!(
                "isolated child did not report exactly one passing test:\n{child_evidence}"
            )
            .into());
        }
        println!("{child_evidence}");
        return Ok(());
    }

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-paper-query-diagnostics-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let database = crate::database::DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let capture = Capture::default();
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(capture.clone())
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target() == "spot_lab::mcp::paper_query_diagnostics"
        }));
    let subscriber = tracing_subscriber::registry().with(layer);

    let result = tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            use axum::serve::Listener as _;
            use rmcp::transport::streamable_http_server::{
                StreamableHttpServerConfig, StreamableHttpService,
                session::local::LocalSessionManager,
            };
            use tokio_util::sync::CancellationToken;

            let upbit = UpbitClient::new()?;
            let jobs = crate::jobs::JobRuntime::start(
                database.handle(),
                upbit.clone(),
                root.clone(),
                None,
            )
            .await?;
            let mcp_service =
                LabMcpService::new(database.handle(), upbit, jobs.service(), McpExposure::Local);

            let shutdown = CancellationToken::new();
            let config = StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_cancellation_token(shutdown.child_token());
            let session_service = StreamableHttpService::new(
                move || Ok(mcp_service.clone()),
                LocalSessionManager::default().into(),
                config,
            );
            let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let listener = crate::transport::BoundedListener::new(
                tcp,
                crate::transport::TransportLimits::default(),
            );
            let address = listener.local_addr()?;
            let router = axum::Router::new().nest_service("/mcp", session_service);
            let router = crate::transport::apply_limits(router, listener.state());
            let server_shutdown = shutdown.clone();
            let server = tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(server_shutdown.cancelled_owned())
                    .await
            });

            let endpoint = format!("http://{address}/mcp");
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()?;
            let initialized = client
                .post(&endpoint)
                .header(
                    reqwest::header::ACCEPT,
                    "application/json, text/event-stream",
                )
                .json(&serde_json::json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "method":"initialize",
                    "params":{
                        "protocolVersion":"2025-03-26",
                        "capabilities":{},
                        "clientInfo":{"name":"diagnostic-test","version":"1"}
                    }
                }))
                .send()
                .await?;
            let session_id = initialized
                .headers()
                .get("mcp-session-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let initialized_body = initialized.bytes().await?;
            let initialized_json: serde_json::Value = serde_json::from_slice(&initialized_body)?;
            assert!(initialized_json["result"].is_object());

            let calls = [
                (
                    "paper_review_query",
                    "jsonrpc-id-secret-sentinel-unknown",
                    serde_json::json!({
                        "action":"raw-action-secret-sentinel",
                        "selection":{
                            "session_id":"6c9b0244-359d-4d2f-96d0-bb3ce810e2e4",
                            "event_sequence":3329.0,
                            "as_of_sequence":3329.0
                        }
                    }),
                    "parse_error",
                    "other",
                    "object",
                    "floating_point",
                    "floating_point",
                    "invalid_params",
                    Some("?query-secret-sentinel=hidden"),
                ),
                (
                    "paper_session_query",
                    "jsonrpc-id-secret-sentinel-session",
                    serde_json::json!({
                        "action":"review_events",
                        "session_id":"6c9b0244-359d-4d2f-96d0-bb3ce810e2e4",
                        "after_sequence":0,
                        "as_of_sequence":0,
                        "limit":0
                    }),
                    "validation_error",
                    "review_events",
                    "not_applicable",
                    "not_applicable",
                    "unsigned_integer",
                    "invalid_params",
                    None,
                ),
                (
                    "paper_review_query",
                    "jsonrpc-id-secret-sentinel-dispatch",
                    serde_json::json!({
                        "action":"decision",
                        "selection":{
                            "session_id":"6c9b0244-359d-4d2f-96d0-bb3ce810e2e4",
                            "event_sequence":3329,
                            "as_of_sequence":3329
                        }
                    }),
                    "dispatch",
                    "decision",
                    "object",
                    "unsigned_integer",
                    "unsigned_integer",
                    "tool_error",
                    None,
                ),
            ];
            let mut request_ids = Vec::with_capacity(calls.len());
            for (
                tool,
                rpc_id,
                arguments,
                expected_stage,
                expected_action,
                expected_selection_shape,
                expected_event_sequence_repr,
                expected_as_of_sequence_repr,
                expected_outcome,
                query,
            ) in calls
            {
                let request_url =
                    query.map_or_else(|| endpoint.clone(), |query| format!("{endpoint}{query}"));
                let mut request = client
                    .post(request_url)
                    .header(
                        reqwest::header::ACCEPT,
                        "application/json, text/event-stream",
                    )
                    .header("MCP-Protocol-Version", "2025-03-26")
                    .header("x-diagnostic-secret", "raw-header-secret-sentinel")
                    .json(&serde_json::json!({
                        "jsonrpc":"2.0",
                        "id":rpc_id,
                        "method":"tools/call",
                        "params":{"name":tool,"arguments":arguments}
                    }));
                if let Some(session_id) = &session_id {
                    request = request.header("Mcp-Session-Id", session_id);
                }
                let response = request.send().await?;
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                let request_id = response
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok())
                    .ok_or("HTTP response omitted x-request-id")?
                    .to_owned();
                let body = response.bytes().await?;
                let rpc_response: serde_json::Value = serde_json::from_slice(&body)?;
                if expected_stage == "dispatch" {
                    assert!(rpc_response["result"].is_object());
                } else {
                    assert_eq!(rpc_response["error"]["code"], -32602);
                }
                request_ids.push((
                    request_id,
                    tool,
                    expected_stage,
                    expected_action,
                    expected_selection_shape,
                    expected_event_sequence_repr,
                    expected_as_of_sequence_repr,
                    expected_outcome,
                ));
            }

            shutdown.cancel();
            server.await??;
            jobs.shutdown().await?;

            let trace = capture
                .0
                .lock()
                .map_err(|_| io::Error::other("trace capture poisoned"))?
                .clone();
            let text = String::from_utf8(trace)?;
            for secret in [
                "raw-action-secret-sentinel",
                "6c9b0244-359d-4d2f-96d0-bb3ce810e2e4",
                "raw-header-secret-sentinel",
                "jsonrpc-id-secret-sentinel",
                "query-secret-sentinel",
                "3329.0",
            ] {
                assert!(!text.contains(secret), "diagnostic leaked {secret}");
            }
            let events = text
                .lines()
                .map(serde_json::from_str::<serde_json::Value>)
                .collect::<Result<Vec<_>, _>>()?;
            for (
                request_id,
                tool,
                stage,
                action,
                selection_shape,
                event_sequence_repr,
                as_of_sequence_repr,
                outcome,
            ) in request_ids
            {
                let entry = events
                    .iter()
                    .find(|event| {
                        event["event"] == "paper_query_entry" && event["request_id"] == request_id
                    })
                    .ok_or("paper query entry was not correlated to x-request-id")?;
                let completion = events
                    .iter()
                    .find(|event| {
                        event["event"] == "paper_query_completion"
                            && event["request_id"] == request_id
                    })
                    .ok_or("paper query completion was not correlated to x-request-id")?;
                assert_eq!(entry["tool"], tool);
                assert_eq!(entry["context_origin"], "http");
                assert_eq!(entry["action"], action);
                assert_eq!(entry["selection_shape"], selection_shape);
                assert_eq!(entry["event_sequence_repr"], event_sequence_repr);
                assert_eq!(entry["as_of_sequence_repr"], as_of_sequence_repr);
                assert_eq!(entry["protocol_version_header"], "known_2025_03_26");
                assert_eq!(completion["stage"], stage);
                assert_eq!(completion["outcome"], outcome);
                assert!(completion["elapsed_ms"].as_u64().is_some());
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    });
    drop(runtime);
    let closed = database.shutdown();
    let removed = std::fs::remove_dir_all(root);
    result?;
    closed?;
    removed?;
    Ok(())
}
