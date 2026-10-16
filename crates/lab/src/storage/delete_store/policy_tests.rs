use super::*;
use crate::contracts::{
    AutomationSessionMode, AutomationSessionWrite, CandleInterval, ContentHash, PolicyDeleteEffect,
    PolicyDeleteItemRequest, PolicyDeleteItemStatus, PolicyDeletePreviewEcho, PolicyDeleteReadback,
    PolicyDeleteRequestState, PolicyDeleteWriteErrorCode, PolicyDeleteWriteRequest, PolicyId,
    PolicyRevision, PolicyWrite, RequestId, UtcTimestamp, builtin_policy_definitions,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

struct PolicyDeleteRoot(PathBuf);

impl PolicyDeleteRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-policy-delete-{label}-{}-{}",
            std::process::id(),
            super::super::TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create policy-delete test root");
        Self(path)
    }
}

impl Drop for PolicyDeleteRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn now() -> UtcTimestamp {
    UtcTimestamp::now()
}

fn create_user_policy(store: &mut Store, request_id: &str, name: &str) -> PolicyRevision {
    let mut definitions = builtin_policy_definitions().expect("builtin definitions");
    let (_, _, mut definition) = definitions.remove(0);
    definition.name = name.to_owned();
    store
        .write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new(request_id).expect("request ID"),
                definition,
            },
            now(),
        )
        .expect("create synthetic USER policy")
}

fn add_mock_automation_reference(store: &mut Store, policy: &PolicyRevision, request_id: &str) {
    let automation = AutomationSessionWrite::Start {
        request_id: RequestId::new(request_id).expect("request ID"),
        policy_ref: policy.snapshot.reference.clone(),
        market: crate::contracts::MarketId::parse_upbit("KRW-BTC").expect("market"),
        decision_interval: CandleInterval::M1,
        mode: AutomationSessionMode::Mock,
        risk: serde_json::from_value(serde_json::json!({
            "max_order_notional_krw":"100000", "max_daily_notional_krw":"1000000",
            "max_asset_exposure_krw":"500000", "max_daily_loss_krw":"50000",
            "max_open_orders":2, "max_orders_per_interval":1,
            "max_balance_age_seconds":30, "max_feed_age_seconds":90
        }))
        .expect("mock risk"),
    };
    store
        .automation_write(
            &automation,
            Some(policy.snapshot.clone()),
            Some("mock:upbit-default".into()),
            now(),
        )
        .expect("start synthetic automation reference");
}

fn preview_item(store: &Store, policy_id: &PolicyId) -> PolicyDeleteItemRequest {
    let preview = store
        .delete_preview(
            &DeleteResource::Policy {
                policy_id: policy_id.clone(),
            },
            now(),
        )
        .expect("policy preview");
    PolicyDeleteItemRequest {
        policy_id: policy_id.clone(),
        preview: Some(PolicyDeletePreviewEcho {
            scope_digest: Some(preview.scope_digest),
            expires_at: Some(preview.expires_at),
            confirmation_token: Some(preview.confirmation_token),
        }),
    }
}

fn request(
    id: &str,
    items: Vec<PolicyDeleteItemRequest>,
    confirmation: &str,
) -> PolicyDeleteWriteRequest {
    PolicyDeleteWriteRequest {
        request_id: RequestId::new(id).expect("request ID"),
        items,
        confirmation_text: confirmation.to_owned(),
    }
}

fn assert_started(store: &mut Store, request: &PolicyDeleteWriteRequest) {
    assert!(matches!(
        store
            .begin_policy_delete_request(request)
            .expect("begin request"),
        super::policy::PolicyDeleteAdmission::Started
    ));
}

fn assert_item_status(
    readback: &crate::contracts::PolicyDeleteReadback,
    policy_id: &PolicyId,
    expected: PolicyDeleteItemStatus,
) {
    let item = readback
        .items
        .as_ref()
        .expect("found readback items")
        .iter()
        .find(|item| &item.policy_id == policy_id)
        .expect("readback item");
    assert_eq!(item.status, expected);
}

#[derive(Debug)]
struct HistoricalPolicyGraph {
    evidence_snapshot_id: String,
    plan_id: String,
    job_id: String,
    attempt_id: String,
    run_id: String,
    ledger_path: String,
    suite_id: String,
    suite_case_id: String,
    schedule_id: String,
    schedule_fire_id: String,
}

#[derive(Clone, Copy)]
struct HistoricalGraphStates {
    job: &'static str,
    attempt: &'static str,
    run: &'static str,
    suite: &'static str,
    case: &'static str,
    schedule: &'static str,
    fire: &'static str,
}

fn insert_historical_policy_graph(
    store: &mut Store,
    policy: &PolicyRevision,
    suffix: &str,
    active: bool,
) -> HistoricalPolicyGraph {
    let graph = HistoricalPolicyGraph {
        evidence_snapshot_id: format!("snapshot-{suffix}"),
        plan_id: format!("plan-{suffix}"),
        job_id: format!("job-{suffix}"),
        attempt_id: format!("attempt-{suffix}"),
        run_id: format!("run-{suffix}"),
        ledger_path: format!("ledgers/policy-delete-{suffix}.ledger"),
        suite_id: format!("suite-{suffix}"),
        suite_case_id: format!("suite-case-{suffix}"),
        schedule_id: format!("schedule-{suffix}"),
        schedule_fire_id: format!("schedule-fire-{suffix}"),
    };
    let digest = ContentHash::of_bytes(suffix.as_bytes());
    let states = if active {
        HistoricalGraphStates {
            job: "RUNNING",
            attempt: "RUNNING",
            run: "RUNNING",
            suite: "running",
            case: "running",
            schedule: "active",
            fire: "running",
        }
    } else {
        HistoricalGraphStates {
            job: "COMPLETED",
            attempt: "COMPLETED",
            run: "COMPLETED",
            suite: "blocked",
            case: "completed",
            schedule: "blocked",
            fire: "blocked",
        }
    };
    insert_policy_evidence_and_plan(store, policy, suffix, &graph, &digest);
    insert_policy_job_run_ledger(store, policy, suffix, &graph, &digest, states);
    insert_policy_suite_schedule(store, suffix, &graph, &digest, states);
    graph
}

fn insert_policy_evidence_and_plan(
    store: &Store,
    policy: &PolicyRevision,
    suffix: &str,
    graph: &HistoricalPolicyGraph,
    digest: &ContentHash,
) {
    let evidence_revision_id = format!("evidence-revision-{suffix}");
    store
        .connection
        .execute(
            "INSERT INTO evidence_versions(revision_id,evidence_id,event_id,purpose,category,body,declared_available_at_ms,registered_at_ms,valid_until_ms,body_hash,mapping_version,supersedes_revision_id,version_json) VALUES (?1,?2,?3,'fixture','macro','{}',0,0,1000,?4,'fixture',NULL,'{}')",
            rusqlite::params![
                evidence_revision_id,
                format!("evidence-{suffix}"),
                format!("event-{suffix}"),
                digest.as_str(),
            ],
        )
        .expect("insert evidence version");
    store
        .connection
        .execute(
            "INSERT INTO evidence_snapshots(id,digest,snapshot_json) VALUES (?1,?2,'{}')",
            rusqlite::params![graph.evidence_snapshot_id, digest.as_str()],
        )
        .expect("insert evidence snapshot");
    store
        .connection
        .execute(
            "INSERT INTO evidence_snapshot_members(snapshot_id,revision_id,position) VALUES (?1,?2,0)",
            rusqlite::params![graph.evidence_snapshot_id, evidence_revision_id],
        )
        .expect("link evidence revision");
    store
        .connection
        .execute(
            "INSERT INTO plans(id,request_id,config_digest,input_digest,evidence_snapshot_id,original_request_json,resolved_plan_json) VALUES (?1,?2,?3,?4,?5,'{}','{}')",
            rusqlite::params![
                graph.plan_id,
                format!("plan-request-{suffix}"),
                digest.as_str(),
                ContentHash::of_bytes(format!("input-{suffix}").as_bytes()).as_str(),
                graph.evidence_snapshot_id,
            ],
        )
        .expect("insert stored plan");
    store
        .connection
        .execute(
            "INSERT INTO plan_policy_revisions(plan_id,position,policy_id,revision_id,definition_digest,frozen_json) VALUES (?1,0,?2,?3,?4,?5)",
            rusqlite::params![
                graph.plan_id,
                policy.snapshot.reference.policy_id.as_str(),
                policy.snapshot.reference.revision_id.as_str(),
                policy.snapshot.reference.definition_digest.as_str(),
                serde_json::to_string(&policy.snapshot).expect("frozen policy JSON"),
            ],
        )
        .expect("link plan to selected policy revision");
}

fn insert_policy_job_run_ledger(
    store: &mut Store,
    policy: &PolicyRevision,
    suffix: &str,
    graph: &HistoricalPolicyGraph,
    digest: &ContentHash,
    states: HistoricalGraphStates,
) {
    store
        .connection
        .execute(
            "INSERT INTO jobs(id,request_id,normalized_input_digest,payload_kind,payload_json,current_attempt_number,current_status,created_at_ms) VALUES (?1,?2,?3,'BACKTEST','{}',1,?4,0)",
            rusqlite::params![graph.job_id, format!("job-request-{suffix}"), digest.as_str(), states.job],
        )
        .expect("insert stored job");
    store
        .connection
        .execute(
            "INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms,ended_at_ms) VALUES (?1,?2,1,?3,?4,'{}',?5,1,1,0,0,0)",
            rusqlite::params![graph.attempt_id, graph.job_id, digest.as_str(), states.attempt, states.case],
        )
        .expect("insert stored job attempt");
    store
        .connection
        .execute(
            "INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,last_committed_event_seq,manifest_json) VALUES (?1,?2,?3,?4,?5,0,1,'{}')",
            rusqlite::params![graph.run_id, graph.job_id, graph.attempt_id, graph.plan_id, states.run],
        )
        .expect("insert stored run");
    store
        .connection
        .execute(
            "INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,status,last_committed_event_seq,policy_id,policy_revision_id,policy_definition_digest) VALUES (?1,?2,'KRW-BTC','S1','{}',?3,1,?4,?5,?6)",
            rusqlite::params![
                graph.run_id,
                format!("model-{suffix}"),
                states.run,
                policy.snapshot.reference.policy_id.as_str(),
                policy.snapshot.reference.revision_id.as_str(),
                policy.snapshot.reference.definition_digest.as_str(),
            ],
        )
        .expect("link stored run model to policy revision");
    store
        .connection
        .execute(
            "INSERT INTO job_attempt_run_outputs(attempt_id,run_id,completed_models,blocked_models) VALUES (?1,?2,1,0)",
            rusqlite::params![graph.attempt_id, graph.run_id],
        )
        .expect("link job attempt to run output");
    std::fs::create_dir_all(store.raw.root.join("ledgers")).expect("create ledger fixture dir");
    std::fs::write(
        store.raw.root.join(&graph.ledger_path),
        b"synthetic ledger bytes",
    )
    .expect("write synthetic ledger file");
    store
        .connection
        .execute(
            "INSERT INTO run_ledger_chunks(run_id,chunk_index,event_seq_start,event_seq_end,event_count,sha256,uncompressed_bytes,compressed_bytes,format_version,relative_path) VALUES (?1,0,1,1,1,?2,22,22,'fixture',?3)",
            rusqlite::params![graph.run_id, digest.as_str(), graph.ledger_path],
        )
        .expect("insert stored run ledger chunk");
}

fn insert_policy_suite_schedule(
    store: &Store,
    suffix: &str,
    graph: &HistoricalPolicyGraph,
    digest: &ContentHash,
    states: HistoricalGraphStates,
) {
    store
        .connection
        .execute(
            "INSERT INTO research_suites(id,request_id,input_digest,status,created_at_ms,next_action_at_ms,frozen_json) VALUES (?1,?2,?3,?4,0,0,'{}')",
            rusqlite::params![graph.suite_id, format!("suite-request-{suffix}"), digest.as_str(), states.suite],
        )
        .expect("insert stored research suite");
    store
        .connection
        .execute(
            "INSERT INTO research_suite_cases(id,suite_id,case_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest) VALUES (?1,?2,0,'evaluation',0,0,1,?3,?4,?5,?6,?7,?8)",
            rusqlite::params![
                graph.suite_case_id,
                graph.suite_id,
                states.case,
                graph.plan_id,
                graph.job_id,
                graph.attempt_id,
                graph.run_id,
                digest.as_str(),
            ],
        )
        .expect("link suite case to historical plan and run");
    store
        .connection
        .execute(
            "INSERT INTO collection_schedules(id,request_id,input_digest,status,created_at_ms,next_action_at_ms,request_json) VALUES (?1,?2,?3,?4,0,0,'{}')",
            rusqlite::params![graph.schedule_id, format!("schedule-request-{suffix}"), digest.as_str(), states.schedule],
        )
        .expect("insert stored collection schedule");
    store
        .connection
        .execute(
            "INSERT INTO schedule_fires(id,schedule_id,boundary_ms,request_id,status,job_id,attempt_id,created_at_ms) VALUES (?1,?2,0,?3,?4,?5,?6,0)",
            rusqlite::params![
                graph.schedule_fire_id,
                graph.schedule_id,
                format!("schedule-fire-request-{suffix}"),
                states.fire,
                graph.job_id,
                graph.attempt_id,
            ],
        )
        .expect("link schedule fire to historical job");
}

fn delete_policy_and_readback(
    store: &mut Store,
    policy: &PolicyRevision,
    request_id: &str,
) -> PolicyDeleteReadback {
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let request = request(
        request_id,
        vec![preview_item(store, &policy_id)],
        &policy.snapshot.definition.name,
    );
    assert_started(store, &request);
    store
        .apply_policy_delete_item(&request.request_id, &policy_id)
        .expect("apply policy-only deletion");
    store
        .policy_delete_readback(&request.request_id)
        .expect("read policy-only deletion receipt")
}

fn assert_row_exists(store: &Store, table: &str, key_column: &str, id: &str) {
    let sql = format!("SELECT 1 FROM {table} WHERE {key_column}=?1");
    assert!(
        store.connection.query_row(&sql, [id], |_| Ok(())).is_ok(),
        "expected {table}.{key_column}={id} to remain"
    );
}

#[test]
fn policy_delete_success_is_scoped_idempotent_and_keeps_opaque_receipt() {
    let root = PolicyDeleteRoot::new("success");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-user-1", "Unique mock policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let item = preview_item(&store, &policy_id);
    let write_request = request(
        "policy-delete-request-1",
        vec![item.clone()],
        "Unique mock policy",
    );

    assert_started(&mut store, &write_request);
    store
        .apply_policy_delete_item(&write_request.request_id, &policy_id)
        .expect("apply scoped policy delete");

    let receipt = store
        .policy_delete_readback(&write_request.request_id)
        .expect("read request receipt");
    assert!(receipt.found);
    assert_eq!(receipt.state, Some(PolicyDeleteRequestState::Complete));
    assert_eq!(receipt.requested_count, Some(1));
    let read_item = &receipt.items.as_ref().expect("items")[0];
    assert_eq!(read_item.status, PolicyDeleteItemStatus::Deleted);
    assert_eq!(read_item.effect, PolicyDeleteEffect::Applied);
    assert_eq!(
        read_item.preview_scope_digest,
        item.preview.as_ref().unwrap().scope_digest
    );
    assert_eq!(read_item.deleted_revision_count, 1);
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM policies WHERE policy_id=?1",
                [policy_id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .expect("policy row count"),
        0
    );
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM policy_revisions WHERE policy_id=?1",
                [policy_id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .expect("revision row count"),
        0
    );
    let receipt_fk_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('policy_delete_request_items') WHERE \"table\" IN ('policies','policy_revisions')",
            [],
            |row| row.get(0),
        )
        .expect("receipt foreign keys");
    assert_eq!(receipt_fk_count, 0);

    assert!(matches!(
        store
            .begin_policy_delete_request(&write_request)
            .expect("replay same request"),
        super::policy::PolicyDeleteAdmission::Existing(_)
    ));
    assert_eq!(
        store
            .policy_delete_readback(&write_request.request_id)
            .expect("read replayed receipt"),
        receipt,
        "same request ID and digest must return the original durable result"
    );

    let conflicting = request(
        write_request.request_id.as_str(),
        vec![item],
        "different confirmation",
    );
    match store
        .begin_policy_delete_request(&conflicting)
        .expect("request ID conflict is structured")
    {
        super::policy::PolicyDeleteAdmission::Rejected(error) => {
            assert_eq!(
                error.error.code,
                PolicyDeleteWriteErrorCode::RequestIdConflict
            );
            assert_eq!(error.error.effect, PolicyDeleteEffect::NoNewEffect);
        }
        other => panic!("expected request ID conflict, got {other:?}"),
    }
}

#[test]
fn policy_delete_partial_batch_blocks_builtin_and_live_automation_reference() {
    let root = PolicyDeleteRoot::new("partial-protected");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let user = create_user_policy(&mut store, "policy-delete-user-2", "Referenced mock policy");
    let user_id = user.snapshot.reference.policy_id.clone();
    let user_preview = preview_item(&store, &user_id);
    let now = now();
    add_mock_automation_reference(&mut store, &user, "policy-delete-automation-start");

    let builtins = builtin_policy_definitions().expect("builtin definitions");
    store
        .seed_builtin_policies_once(&builtins, now)
        .expect("seed builtins");
    let builtin_id = builtins[0].0.clone();
    let builtin_preview = preview_item(&store, &builtin_id);
    let mut items = vec![user_preview, builtin_preview];
    items.sort_by(|lhs, rhs| lhs.policy_id.cmp(&rhs.policy_id));
    let request = request("policy-delete-partial-1", items, "DELETE 2 POLICIES");

    assert_started(&mut store, &request);
    let admitted = store
        .policy_delete_readback(&request.request_id)
        .expect("read admitted request");
    assert_eq!(admitted.state, Some(PolicyDeleteRequestState::Pending));
    assert_eq!(admitted.requested_count, Some(2));
    assert_eq!(admitted.counts.as_ref().unwrap().pending, 2);
    for (index, item) in request.items.iter().enumerate() {
        store
            .apply_policy_delete_item(&request.request_id, &item.policy_id)
            .expect("process protected item");
        if index == 0 {
            let mid_request = store
                .policy_delete_readback(&request.request_id)
                .expect("read partially processed request");
            assert_eq!(mid_request.state, Some(PolicyDeleteRequestState::Pending));
            assert_eq!(mid_request.counts.as_ref().unwrap().pending, 1);
            let counts = mid_request.counts.as_ref().unwrap();
            assert_eq!(
                counts.pending
                    + counts.deleted
                    + counts.blocked
                    + counts.stale
                    + counts.failed
                    + counts.interrupted_not_applied,
                2
            );
        }
    }
    let receipt = store
        .policy_delete_readback(&request.request_id)
        .expect("read protected outcomes");
    assert_eq!(receipt.state, Some(PolicyDeleteRequestState::Partial));
    assert_item_status(&receipt, &user_id, PolicyDeleteItemStatus::Blocked);
    assert_item_status(&receipt, &builtin_id, PolicyDeleteItemStatus::Blocked);
    let user_result = receipt
        .items
        .as_ref()
        .unwrap()
        .iter()
        .find(|item| item.policy_id == user_id)
        .unwrap();
    assert_eq!(
        user_result.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::ReferencedAutomation)
    );
    assert_eq!(
        user_result.remediation_code,
        Some(crate::contracts::PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable)
    );
    assert!(user_result.retained);
    let builtin_result = receipt
        .items
        .as_ref()
        .unwrap()
        .iter()
        .find(|item| item.policy_id == builtin_id)
        .unwrap();
    assert_eq!(
        builtin_result.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::BuiltinPolicy)
    );
    assert!(builtin_result.retained);
    assert_eq!(receipt.counts.as_ref().unwrap().blocked, 2);
}

#[test]
fn policy_delete_changed_reference_missing_echo_and_expiry_are_no_effect() {
    let root = PolicyDeleteRoot::new("stale");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let referenced = create_user_policy(&mut store, "policy-delete-user-3", "Changed scope policy");
    let referenced_id = referenced.snapshot.reference.policy_id.clone();
    let stale_item = preview_item(&store, &referenced_id);

    add_mock_automation_reference(&mut store, &referenced, "policy-delete-late-automation");

    let missing = create_user_policy(&mut store, "policy-delete-user-4", "Missing preview policy");
    let missing_id = missing.snapshot.reference.policy_id.clone();
    let expired = create_user_policy(&mut store, "policy-delete-user-5", "Expired preview policy");
    let expired_id = expired.snapshot.reference.policy_id.clone();
    let expired_preview = store
        .delete_preview(
            &DeleteResource::Policy {
                policy_id: expired_id.clone(),
            },
            UtcTimestamp::parse_rfc3339("2020-01-01T00:00:00Z").expect("old time"),
        )
        .expect("expired fixture preview");
    let mut items = vec![
        stale_item,
        PolicyDeleteItemRequest {
            policy_id: missing_id.clone(),
            preview: None,
        },
        PolicyDeleteItemRequest {
            policy_id: expired_id.clone(),
            preview: Some(PolicyDeletePreviewEcho {
                scope_digest: Some(expired_preview.scope_digest),
                expires_at: Some(expired_preview.expires_at),
                confirmation_token: Some(expired_preview.confirmation_token),
            }),
        },
    ];
    items.sort_by(|lhs, rhs| lhs.policy_id.cmp(&rhs.policy_id));
    let request = request("policy-delete-stale-1", items, "DELETE 3 POLICIES");
    assert_started(&mut store, &request);
    for item in &request.items {
        store
            .apply_policy_delete_item(&request.request_id, &item.policy_id)
            .expect("process stale fixture item");
    }
    let receipt = store
        .policy_delete_readback(&request.request_id)
        .expect("read stale outcomes");
    assert_item_status(&receipt, &referenced_id, PolicyDeleteItemStatus::Blocked);
    assert_item_status(&receipt, &missing_id, PolicyDeleteItemStatus::Stale);
    assert_item_status(&receipt, &expired_id, PolicyDeleteItemStatus::Stale);
    for policy_id in [&referenced_id, &missing_id, &expired_id] {
        assert!(
            store
                .connection
                .query_row(
                    "SELECT 1 FROM policies WHERE policy_id=?1",
                    [policy_id.as_str()],
                    |_| Ok(())
                )
                .is_ok(),
            "stale/refreshed scope must not delete policy {policy_id}"
        );
    }
}

#[test]
fn policy_delete_keeps_real_plan_run_ledger_job_evidence_suite_and_schedule_graph() {
    let root = PolicyDeleteRoot::new("historical-graph");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(
        &mut store,
        "policy-delete-graph-user",
        "Historical graph policy",
    );
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let graph = insert_historical_policy_graph(&mut store, &policy, "completed-graph", false);
    let ledger_file = store.raw.root.join(&graph.ledger_path);
    let ledger_bytes = std::fs::read(&ledger_file).expect("read synthetic ledger file");

    let receipt = delete_policy_and_readback(
        &mut store,
        &policy,
        "policy-delete-historical-graph-request",
    );
    assert_eq!(receipt.state, Some(PolicyDeleteRequestState::Partial));
    assert_item_status(&receipt, &policy_id, PolicyDeleteItemStatus::Blocked);
    let item = &receipt.items.as_ref().unwrap()[0];
    assert_eq!(
        item.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::ReferencedHistorical)
    );
    assert_eq!(
        item.remediation_code,
        Some(crate::contracts::PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable)
    );
    assert_eq!(item.effect, PolicyDeleteEffect::NotApplied);
    assert!(item.retained);
    assert_row_exists(&store, "policies", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "policy_revisions", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "plans", "id", &graph.plan_id);
    assert_row_exists(&store, "jobs", "id", &graph.job_id);
    assert_row_exists(&store, "job_attempts", "id", &graph.attempt_id);
    assert_row_exists(&store, "runs", "id", &graph.run_id);
    assert_row_exists(&store, "run_models", "run_id", &graph.run_id);
    assert_row_exists(
        &store,
        "evidence_snapshots",
        "id",
        &graph.evidence_snapshot_id,
    );
    assert_row_exists(&store, "research_suites", "id", &graph.suite_id);
    assert_row_exists(&store, "research_suite_cases", "id", &graph.suite_case_id);
    assert_row_exists(&store, "collection_schedules", "id", &graph.schedule_id);
    assert_row_exists(&store, "schedule_fires", "id", &graph.schedule_fire_id);
    assert!(
        std::fs::read(&ledger_file).expect("ledger file retained") == ledger_bytes,
        "policy deletion must leave the run-owned ledger file untouched"
    );
}

#[test]
fn policy_delete_blocks_active_job_reference_with_no_effect() {
    let root = PolicyDeleteRoot::new("active-job-graph");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-active-user", "Active job policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let graph = insert_historical_policy_graph(&mut store, &policy, "running-graph", true);

    let receipt =
        delete_policy_and_readback(&mut store, &policy, "policy-delete-active-job-request");
    assert_eq!(receipt.state, Some(PolicyDeleteRequestState::Partial));
    assert_item_status(&receipt, &policy_id, PolicyDeleteItemStatus::Blocked);
    let item = &receipt.items.as_ref().unwrap()[0];
    assert_eq!(
        item.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::ActiveJob)
    );
    assert_eq!(item.effect, PolicyDeleteEffect::NotApplied);
    assert!(item.retained);
    assert_row_exists(&store, "policies", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "policy_revisions", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "jobs", "id", &graph.job_id);
    assert_row_exists(&store, "job_attempts", "id", &graph.attempt_id);
    assert_row_exists(&store, "runs", "id", &graph.run_id);
}

#[test]
fn policy_delete_blocks_paper_references_in_every_persisted_state() {
    let root = PolicyDeleteRoot::new("paper-states");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-paper-user", "Paper state policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let reference_json =
        serde_json::to_string(&policy.snapshot.reference).expect("policy ref JSON");
    for state in ["RUNNING", "PAUSED", "STOPPED", "RECOVERY_REQUIRED"] {
        let session_id = format!("paper-ref-{}", state.to_ascii_lowercase());
        let paused_at = (state == "PAUSED").then_some("2026-10-15T00:01:00Z");
        let stopped_at =
            matches!(state, "STOPPED" | "RECOVERY_REQUIRED").then_some("2026-10-15T00:02:00Z");
        store
            .connection
            .execute(
                "INSERT INTO paper_sessions(session_id,origin_request_id,state,sequence,schema_version,engine_version,policy_id,policy_revision_id,policy_ref_json,decision_interval,execution_resolution,latency_ms,execution_json,markets_json,created_at_utc,started_at_utc,paused_at_utc,stopped_at_utc,last_evaluated_at_utc,event_count,recovery_reason,view_json) VALUES (?1,?2,?3,1,'fixture','fixture',?4,?5,?6,'m1','m1',0,'{}','[]','2026-10-15T00:00:00Z','2026-10-15T00:00:00Z',?7,?8,NULL,0,NULL,'{}')",
                rusqlite::params![
                    session_id,
                    format!("paper-ref-request-{}", state.to_ascii_lowercase()),
                    state,
                    policy.snapshot.reference.policy_id.as_str(),
                    policy.snapshot.reference.revision_id.as_str(),
                    reference_json,
                    paused_at,
                    stopped_at,
                ],
            )
            .expect("insert persisted paper policy reference");
    }

    let receipt =
        delete_policy_and_readback(&mut store, &policy, "policy-delete-paper-states-request");
    assert_item_status(&receipt, &policy_id, PolicyDeleteItemStatus::Blocked);
    let item = &receipt.items.as_ref().unwrap()[0];
    assert_eq!(
        item.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::ReferencedPaper)
    );
    assert_eq!(
        item.remediation_code,
        Some(crate::contracts::PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable)
    );
    assert!(item.retained);
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_sessions WHERE policy_id=?1",
                [policy_id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .expect("count paper references"),
        4
    );
    assert_row_exists(&store, "policies", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "policy_revisions", "policy_id", policy_id.as_str());
}

#[test]
fn policy_delete_receipt_abort_rolls_back_rows_and_startup_recovers_pending() {
    let root = PolicyDeleteRoot::new("receipt-abort");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(
        &mut store,
        "policy-delete-fault-user",
        "Receipt fault policy",
    );
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let request = request(
        "policy-delete-receipt-abort-request",
        vec![preview_item(&store, &policy_id)],
        policy.snapshot.definition.name.as_str(),
    );
    assert_started(&mut store, &request);
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER abort_policy_delete_receipt BEFORE UPDATE OF status ON policy_delete_request_items WHEN OLD.status='PENDING' AND NEW.status='DELETED' BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END;",
        )
        .expect("install synthetic receipt failure");
    assert!(
        store
            .apply_policy_delete_item(&request.request_id, &policy_id)
            .is_err(),
        "receipt fault must abort the full policy-delete transaction"
    );
    let pending = store
        .policy_delete_readback(&request.request_id)
        .expect("read pending receipt after rolled back transaction");
    assert_eq!(pending.state, Some(PolicyDeleteRequestState::Pending));
    assert_item_status(&pending, &policy_id, PolicyDeleteItemStatus::Pending);
    assert_row_exists(&store, "policies", "policy_id", policy_id.as_str());
    assert_row_exists(&store, "policy_revisions", "policy_id", policy_id.as_str());
    store
        .connection
        .execute_batch("DROP TRIGGER abort_policy_delete_receipt")
        .expect("remove synthetic receipt failure");
    store.close().expect("close synthetic owner");

    let reopened = Store::open(&root.0).expect("reopen after fault");
    let interrupted = reopened
        .policy_delete_readback(&request.request_id)
        .expect("read recovered receipt");
    assert_eq!(
        interrupted.state,
        Some(PolicyDeleteRequestState::Interrupted)
    );
    assert_item_status(
        &interrupted,
        &policy_id,
        PolicyDeleteItemStatus::InterruptedNotApplied,
    );
    assert_row_exists(&reopened, "policies", "policy_id", policy_id.as_str());
    assert_row_exists(
        &reopened,
        "policy_revisions",
        "policy_id",
        policy_id.as_str(),
    );
}

#[test]
fn policy_delete_two_request_ids_race_without_replaying_and_startup_interrupts_pending() {
    let root = PolicyDeleteRoot::new("readback");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-user-6", "Race policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let item = preview_item(&store, &policy_id);
    let request_a = request(
        "policy-delete-race-a",
        vec![item.clone()],
        policy_id.as_str(),
    );
    let request_b = request("policy-delete-race-b", vec![item], policy_id.as_str());
    assert_started(&mut store, &request_a);
    assert_started(&mut store, &request_b);
    store
        .apply_policy_delete_item(&request_a.request_id, &policy_id)
        .expect("first item applies");
    store
        .apply_policy_delete_item(&request_b.request_id, &policy_id)
        .expect("second item revalidates without replay");
    let deleted = store
        .policy_delete_readback(&request_a.request_id)
        .expect("first receipt");
    let blocked = store
        .policy_delete_readback(&request_b.request_id)
        .expect("second receipt");
    assert_item_status(&deleted, &policy_id, PolicyDeleteItemStatus::Deleted);
    assert_item_status(&blocked, &policy_id, PolicyDeleteItemStatus::Stale);
    assert!(matches!(
        store
            .begin_policy_delete_request(&request_a)
            .expect("replay prior request"),
        super::policy::PolicyDeleteAdmission::Existing(_)
    ));
    assert_item_status(
        &store
            .policy_delete_readback(&request_a.request_id)
            .expect("replay readback"),
        &policy_id,
        PolicyDeleteItemStatus::Deleted,
    );

    let pending = create_user_policy(&mut store, "policy-delete-user-7", "Interrupted policy");
    let pending_id = pending.snapshot.reference.policy_id.clone();
    let pending_item = preview_item(&store, &pending_id);
    let pending_request = request(
        "policy-delete-pending-1",
        vec![pending_item],
        pending_id.as_str(),
    );
    assert_started(&mut store, &pending_request);
    store.close().expect("close temporary owner");

    let mut reopened = Store::open(&root.0).expect("reopen temporary owner");
    assert!(matches!(
        reopened
            .begin_policy_delete_request(&pending_request)
            .expect("same request never resumes after restart"),
        super::policy::PolicyDeleteAdmission::Existing(_)
    ));
    let interrupted = reopened
        .policy_delete_readback(&pending_request.request_id)
        .expect("read interrupted request");
    assert_eq!(
        interrupted.state,
        Some(PolicyDeleteRequestState::Interrupted)
    );
    assert_item_status(
        &interrupted,
        &pending_id,
        PolicyDeleteItemStatus::InterruptedNotApplied,
    );
    assert!(
        reopened
            .connection
            .query_row(
                "SELECT 1 FROM policies WHERE policy_id=?1",
                [pending_id.as_str()],
                |_| Ok(())
            )
            .is_ok(),
        "startup recovery must not delete pending policy"
    );
}

#[test]
fn unknown_persisted_policy_origin_is_blocked_without_migration_backfill() {
    let root = PolicyDeleteRoot::new("unknown-origin");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-user-9", "Legacy-origin policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    store
        .connection
        .execute(
            "UPDATE policies SET origin='LEGACY_UNKNOWN' WHERE policy_id=?1",
            [policy_id.as_str()],
        )
        .expect("make synthetic unknown-origin row");
    let item = preview_item(&store, &policy_id);
    let request = request(
        "policy-delete-unknown-origin",
        vec![item],
        policy_id.as_str(),
    );
    assert_started(&mut store, &request);
    store
        .apply_policy_delete_item(&request.request_id, &policy_id)
        .expect("process unknown origin");
    let receipt = store
        .policy_delete_readback(&request.request_id)
        .expect("read blocked unknown-origin outcome");
    assert_item_status(&receipt, &policy_id, PolicyDeleteItemStatus::Blocked);
    let result = &receipt.items.as_ref().unwrap()[0];
    assert_eq!(
        result.reason_code,
        Some(crate::contracts::PolicyDeleteReasonCode::UnknownOrigin)
    );
    assert_eq!(
        result.remediation_code,
        Some(crate::contracts::PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable)
    );
    assert!(result.retained);
}

#[test]
fn policy_delete_101_item_reuse_is_rejected_before_existing_request_lookup() {
    let root = PolicyDeleteRoot::new("batch-limit");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(
        &mut store,
        "policy-delete-limit-user",
        "Limit fixture policy",
    );
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let item = preview_item(&store, &policy_id);
    let existing = request("policy-delete-over-limit", vec![item], policy_id.as_str());
    assert_started(&mut store, &existing);
    let before = store
        .policy_delete_readback(&existing.request_id)
        .expect("read existing request before invalid reuse");

    let items = (0..=crate::contracts::MAX_POLICY_DELETE_ITEMS)
        .map(|index| PolicyDeleteItemRequest {
            policy_id: PolicyId::new(format!("policy-limit-{index}")).expect("policy ID"),
            preview: None,
        })
        .collect();
    let oversized = request(existing.request_id.as_str(), items, "DELETE 101 POLICIES");
    match store
        .begin_policy_delete_request(&oversized)
        .expect("over-limit request is structured")
    {
        super::policy::PolicyDeleteAdmission::Rejected(error) => {
            assert_eq!(error.error.code, PolicyDeleteWriteErrorCode::SelectionLimit);
            assert!(error.error.submitted_request_digest.is_none());
            assert!(error.error.existing_request_digest.is_none());
            assert_eq!(error.error.effect, PolicyDeleteEffect::NotApplied);
        }
        other => panic!("expected selection limit error, got {other:?}"),
    }
    assert_eq!(
        store
            .policy_delete_readback(&existing.request_id)
            .expect("existing receipt is unchanged"),
        before
    );
    assert!(
        store
            .connection
            .query_row(
                "SELECT 1 FROM policies WHERE policy_id=?1",
                [policy_id.as_str()],
                |_| Ok(())
            )
            .is_ok()
    );
}

#[test]
fn policy_delete_whole_request_rejection_has_no_receipt_or_effect() {
    let root = PolicyDeleteRoot::new("whole-request-rejection");
    let mut store = Store::open(&root.0).expect("open synthetic Store");
    let policy = create_user_policy(&mut store, "policy-delete-user-8", "Confirmation policy");
    let policy_id = policy.snapshot.reference.policy_id.clone();
    let item = preview_item(&store, &policy_id);
    let request = request("policy-delete-invalid-confirm", vec![item], "wrong phrase");
    match store
        .begin_policy_delete_request(&request)
        .expect("request-level rejection")
    {
        super::policy::PolicyDeleteAdmission::Rejected(error) => {
            assert_eq!(
                error.error.code,
                PolicyDeleteWriteErrorCode::ConfirmationMismatch
            );
            assert_eq!(error.error.effect, PolicyDeleteEffect::NotApplied);
        }
        other => panic!("expected confirmation rejection, got {other:?}"),
    }
    assert!(
        !store
            .policy_delete_readback(&request.request_id)
            .expect("not-found readback")
            .found
    );
    assert!(
        store
            .connection
            .query_row(
                "SELECT 1 FROM policies WHERE policy_id=?1",
                [policy_id.as_str()],
                |_| Ok(())
            )
            .is_ok()
    );
}
