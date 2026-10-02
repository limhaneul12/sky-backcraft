use super::*;
use rmcp::{
    ServiceError, ServiceExt,
    model::{CallToolRequestParams, ErrorCode, object},
};

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
        let service = LabMcpService::new(database.handle(), upbit, None, jobs.service(), true);
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
            .map_err(|error| LabError::Internal(format!("start MCP regression client: {error}")))?;

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
                "collect_data",
                "dataset_query",
                "evidence_register",
                "export_report",
                "history_query",
                "job_control",
                "lab_status",
                "plan_backtest",
                "policy_query",
                "policy_write",
                "probe_upbit",
                "resource_delete_preview",
                "resource_hard_delete",
                "result_query",
                "run_backtests",
                "verify_run",
            ]
        );
        for tool in &tools {
            let annotations = tool
                .annotations
                .as_ref()
                .ok_or_else(|| LabError::Internal(format!("{} lacks annotations", tool.name)))?;
            let expected_read_only = matches!(
                tool.name.as_ref(),
                "lab_status"
                    | "dataset_query"
                    | "history_query"
                    | "policy_query"
                    | "result_query"
                    | "artifact_query"
                    | "resource_delete_preview"
            );
            let expected_destructive = tool.name.as_ref() == "resource_hard_delete";
            assert_eq!(annotations.read_only_hint, Some(expected_read_only));
            assert_eq!(annotations.destructive_hint, Some(expected_destructive));
            assert_eq!(
                annotations.open_world_hint,
                Some(matches!(tool.name.as_ref(), "probe_upbit" | "collect_data"))
            );
            assert_eq!(
                tool.input_schema.get("type"),
                Some(&serde_json::json!("object"))
            );
        }

        for (tool_name, arguments, expected) in [
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
