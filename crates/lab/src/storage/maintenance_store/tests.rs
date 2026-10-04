use super::*;
use crate::contracts::{
    DatasetId, DeleteBlockerClass, DeleteEntryKind, DeleteResource, HardDeleteRequest, PlanId,
    RequestId, ScheduleId, SuiteId,
};
use std::sync::atomic::Ordering;

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-maintenance-{label}-{}-{}",
            std::process::id(),
            super::super::TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
        if let Ok(backup_root) = managed_backup_root(&self.0) {
            let _ignored = fs::remove_dir_all(backup_root);
        }
    }
}

fn time(text: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(text).expect("valid fixture time")
}

fn insert_job(store: &Store, id: &str, created_at: UtcTimestamp, status: &str) {
    let request_id = format!("request-{id}");
    store.connection.execute(
        "INSERT INTO jobs(id,request_id,normalized_input_digest,payload_kind,payload_json,current_attempt_number,current_status,created_at_ms) VALUES (?1,?2,?3,'run','{}',1,?4,?5)",
        params![
            id,
            request_id,
            ContentHash::of_bytes(id.as_bytes()).as_str(),
            status,
            timestamp_ms(created_at)
        ],
    ).expect("insert fixture job");
}

fn insert_plan(store: &Store, id: &PlanId) {
    store.connection.execute(
        "INSERT INTO plans(id,request_id,config_digest,input_digest,evidence_snapshot_id,original_request_json,resolved_plan_json) VALUES (?1,?2,?3,?4,NULL,'{}','{}')",
        params![
            id.as_str(),
            format!("request-{}", id.as_str()),
            ContentHash::of_bytes(b"config").as_str(),
            ContentHash::of_bytes(b"input").as_str()
        ],
    ).expect("insert fixture plan");
}

fn insert_suite(store: &Store, id: &SuiteId, created_at: UtcTimestamp, plan: Option<&PlanId>) {
    store.connection.execute(
        "INSERT INTO research_suites(id,request_id,input_digest,status,created_at_ms,next_action_at_ms,frozen_json) VALUES (?1,?2,?3,'completed',?4,?4,'{}')",
        params![
            id.as_str(),
            format!("request-{}", id.as_str()),
            ContentHash::of_bytes(id.as_str().as_bytes()).as_str(),
            timestamp_ms(created_at)
        ],
    ).expect("insert fixture suite");
    if let Some(plan) = plan {
        store.connection.execute(
            "INSERT INTO research_suite_cases(id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json) VALUES (?1,?2,0,NULL,'batch',0,0,1,'completed',?3,NULL,NULL,NULL,NULL,NULL)",
            params![format!("suite-case-{}", id.as_str()), id.as_str(), plan.as_str()],
        ).expect("insert fixture suite case");
    }
}

fn insert_dataset(store: &Store, id: &DatasetId) {
    let request = format!("request-{}", id.as_str());
    store
        .connection
        .execute(
            "INSERT INTO collections(request_id,normalized_request_digest,request_json) VALUES (?1,?2,'{}')",
            params![request, ContentHash::of_bytes(b"collection").as_str()],
        )
        .expect("insert fixture collection");
    store.connection.execute(
        "INSERT INTO datasets(id,request_id,normalized_request_digest,schema_version,status,coverage_start_ms,coverage_end_ms,row_count,normalizer_version,gap_policy,semantic_digest,provenance_digest,origin,manifest_json) VALUES (?1,?2,?3,'v1','COMPLETED',0,1,0,'v1','STRICT',?4,?5,'EXCHANGE_OBSERVED','{}')",
        params![
            id.as_str(),
            format!("request-{}", id.as_str()),
            ContentHash::of_bytes(b"normalized").as_str(),
            ContentHash::of_bytes(b"semantic").as_str(),
            ContentHash::of_bytes(b"provenance").as_str()
        ],
    ).expect("insert fixture dataset");
}

#[test]
fn managed_backup_is_verified_idempotent_and_reconciles_a_lost_receipt() {
    let root = TempRoot::new("backup-reconcile");
    let mut store = Store::open(&root.0).expect("open store");
    let request = RequestId::new("backup-request").expect("request ID");
    let first = store
        .create_managed_backup(&request, time("2024-01-01T00:00:00Z"))
        .expect("create verified backup");
    let backup_path = managed_backup_root(&root.0)
        .expect("backup root")
        .join(first.id.as_str());
    let restored_root = TempRoot::new("backup-restored");
    let restored = Store::restore(&backup_path, &restored_root.0).expect("restore managed backup");
    restored
        .storage_accounting()
        .expect("restored store remains readable");
    let repeated = store
        .create_managed_backup(&request, time("2024-01-01T00:01:00Z"))
        .expect("read same verified receipt");
    assert_eq!(first, repeated);

    store
        .connection
        .execute(
            "DELETE FROM managed_backups WHERE id=?1",
            [first.id.as_str()],
        )
        .expect("simulate lost receipt after rename");
    let reconciled = store
        .create_managed_backup(&request, time("2024-01-01T00:02:00Z"))
        .expect("reconcile verified renamed backup");
    assert_eq!(reconciled.id, first.id);
    assert_eq!(reconciled.bytes, first.bytes);
    assert_eq!(reconciled.manifest_digest, first.manifest_digest);
    assert_eq!(
        reconciled.source_identity_digest,
        first.source_identity_digest
    );
    assert_eq!(store.list_managed_backups().expect("list").total_count, 1);
}

#[test]
fn incomplete_deterministic_backup_is_rejected_without_overwrite() {
    let root = TempRoot::new("backup-partial");
    let mut store = Store::open(&root.0).expect("open store");
    let request = RequestId::new("partial-request").expect("request ID");
    let id = BackupId::from_seed(request.as_str());
    let backup_root = managed_backup_root(&root.0).expect("backup root");
    fs::create_dir(&backup_root).expect("create backup root");
    let destination = backup_root.join(id.as_str());
    fs::create_dir(&destination).expect("create partial destination");
    fs::write(destination.join("sentinel"), b"do-not-overwrite").expect("write sentinel");

    assert!(
        store
            .create_managed_backup(&request, time("2024-01-01T00:00:00Z"))
            .is_err()
    );
    assert_eq!(
        fs::read(destination.join("sentinel")).expect("sentinel remains"),
        b"do-not-overwrite"
    );
    assert_eq!(
        store
            .list_managed_backups()
            .expect("empty list")
            .total_count,
        0
    );
}

#[test]
fn lost_receipt_reconciliation_cannot_bypass_the_aggregate_byte_cap() {
    let root = TempRoot::new("backup-cap-reconcile");
    let mut store = Store::open(&root.0).expect("open store");
    let request = RequestId::new("capped-request").expect("request ID");
    let receipt = store
        .create_managed_backup(&request, time("2024-01-01T00:00:00Z"))
        .expect("create verified backup");
    let backup_root = managed_backup_root(&root.0).expect("backup root");
    let backup_path = backup_root.join(receipt.id.as_str());
    let manifest_before = fs::read(backup_path.join("backup-manifest.json")).expect("manifest");
    store
        .connection
        .execute(
            "DELETE FROM managed_backups WHERE id=?1",
            [receipt.id.as_str()],
        )
        .expect("simulate receipt loss");

    let filler_id = BackupId::from_seed("cap-filler");
    let filler_root = backup_root.join(filler_id.as_str());
    fs::create_dir(&filler_root).expect("create filler backup directory");
    let filler = fs::File::create(filler_root.join("sparse-cap-filler")).expect("create filler");
    filler
        .set_len(MAX_MANAGED_BACKUP_BYTES - receipt.bytes)
        .expect("set exact-cap sparse length");
    let reconciled = store
        .create_managed_backup(&request, time("2024-01-01T00:01:00Z"))
        .expect("exact aggregate cap remains receiptable");
    assert_eq!(reconciled.id, receipt.id);

    store
        .connection
        .execute(
            "DELETE FROM managed_backups WHERE id=?1",
            [receipt.id.as_str()],
        )
        .expect("simulate second receipt loss");
    filler
        .set_len(MAX_MANAGED_BACKUP_BYTES - receipt.bytes + 1)
        .expect("exceed cap by one logical byte");
    assert!(matches!(
        store.create_managed_backup(&request, time("2024-01-01T00:02:00Z")),
        Err(LabError::OutcomeUnknown(_))
    ));
    let receipt_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM managed_backups", [], |row| row.get(0))
        .expect("receipt count");
    assert_eq!(receipt_count, 0);
    assert_eq!(
        fs::read(backup_path.join("backup-manifest.json")).expect("manifest remains"),
        manifest_before
    );
    assert_eq!(
        fs::metadata(filler_root.join("sparse-cap-filler"))
            .expect("filler remains")
            .len(),
        MAX_MANAGED_BACKUP_BYTES - receipt.bytes + 1
    );
}

#[test]
fn lost_receipt_reconciliation_cannot_bypass_the_backup_count_cap() {
    let root = TempRoot::new("backup-count-reconcile");
    let mut store = Store::open(&root.0).expect("open store");
    let request = RequestId::new("counted-request").expect("request ID");
    let receipt = store
        .create_managed_backup(&request, time("2024-01-01T00:00:00Z"))
        .expect("create verified backup");
    store
        .connection
        .execute(
            "DELETE FROM managed_backups WHERE id=?1",
            [receipt.id.as_str()],
        )
        .expect("simulate receipt loss");
    let backup_root = managed_backup_root(&root.0).expect("backup root");
    for index in 0..MAX_MANAGED_BACKUPS {
        let id = BackupId::from_seed(&format!("count-filler-{index}"));
        fs::create_dir(backup_root.join(id.as_str())).expect("create filler directory");
    }

    assert!(matches!(
        store.create_managed_backup(&request, time("2024-01-01T00:01:00Z")),
        Err(LabError::OutcomeUnknown(_))
    ));
    let receipt_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM managed_backups", [], |row| row.get(0))
        .expect("receipt count");
    assert_eq!(receipt_count, 0);
    assert!(
        backup_root
            .join(receipt.id.as_str())
            .join("backup-manifest.json")
            .is_file()
    );
}

#[test]
fn retention_listing_is_pure_deterministic_and_excludes_suite_lineage() {
    let root = TempRoot::new("retention-pure");
    let mut store = Store::open(&root.0).expect("open store");
    insert_job(
        &store,
        "job-protected",
        time("2024-01-01T00:00:00Z"),
        "COMPLETED",
    );
    insert_job(&store, "job-old", time("2024-01-02T00:00:00Z"), "COMPLETED");
    insert_job(&store, "job-new", time("2024-01-03T00:00:00Z"), "COMPLETED");
    let suite = SuiteId::new("suite-protector").expect("suite ID");
    insert_suite(&store, &suite, time("2024-01-04T00:00:00Z"), None);
    store.connection.execute(
        "INSERT INTO research_suite_cases(id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json) VALUES ('suite-case-protected',?1,0,NULL,'batch',0,0,1,'completed',NULL,NULL,NULL,NULL,NULL,NULL)",
        [suite.as_str()],
    ).expect("insert case");
    // Link the protected job after creating the minimal attempt required by the schema.
    store.connection.execute(
        "INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms,ended_at_ms,cancel_requested_at_ms) VALUES ('attempt-protected','job-protected',1,?1,'COMPLETED','{}','completed',0,0,0,0,1,NULL)",
        [ContentHash::of_bytes(b"attempt").as_str()],
    ).expect("insert attempt");
    store.connection.execute(
        "UPDATE research_suite_cases SET job_id='job-protected',attempt_id='attempt-protected' WHERE id='suite-case-protected'",
        [],
    ).expect("link protected job");

    let before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .expect("count before");
    let first = store
        .retention_candidates(
            time("2024-02-01T00:00:00Z"),
            1,
            0,
            100,
            time("2024-02-02T00:00:00Z"),
        )
        .expect("retention candidates");
    let second = store
        .retention_candidates(
            time("2024-02-01T00:00:00Z"),
            1,
            0,
            100,
            time("2024-02-02T00:00:00Z"),
        )
        .expect("repeat candidates");
    assert_eq!(
        serde_json::to_value(&first).expect("serialize"),
        serde_json::to_value(&second).expect("serialize")
    );
    assert!(first.records.iter().any(|candidate| {
        candidate.resource
            == DeleteResource::Job {
                job_id: crate::contracts::JobId::new("job-old").expect("job ID"),
            }
    }));
    assert!(!first.records.iter().any(|candidate| {
        candidate.resource
            == DeleteResource::Job {
                job_id: crate::contracts::JobId::new("job-protected").expect("job ID"),
            }
    }));
    let protected = store
        .delete_preview(
            &DeleteResource::Job {
                job_id: crate::contracts::JobId::new("job-protected").expect("job ID"),
            },
            time("2024-02-02T00:00:00Z"),
        )
        .expect("protected job preview");
    assert!(protected.blockers.iter().any(|blocker| {
        blocker.class == DeleteBlockerClass::ProtectedReference
            && blocker.reference == "suite:suite-protector"
    }));
    assert!(
        store
            .execute_hard_delete(
                &HardDeleteRequest {
                    preview: protected,
                    cascade: true,
                },
                time("2024-02-02T00:01:00Z"),
            )
            .is_err()
    );
    let after: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .expect("count after");
    assert_eq!(before, after);
}

#[test]
fn suite_preview_delete_releases_links_and_retains_ordinary_plan() {
    let root = TempRoot::new("suite-delete");
    let mut store = Store::open(&root.0).expect("open store");
    let plan = PlanId::new("plan-retained").expect("plan ID");
    insert_plan(&store, &plan);
    let suite = SuiteId::new("suite-delete").expect("suite ID");
    insert_suite(&store, &suite, time("2024-01-01T00:00:00Z"), Some(&plan));
    let preview = store
        .delete_preview(
            &DeleteResource::Suite {
                suite_id: suite.clone(),
            },
            time("2024-02-01T00:00:00Z"),
        )
        .expect("suite preview");
    assert!(!preview.cascade_required);
    assert!(
        preview
            .retained_shared
            .iter()
            .any(|group| group.kind == DeleteEntryKind::Plan && group.count == 1)
    );
    store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview,
                cascade: false,
            },
            time("2024-02-01T00:01:00Z"),
        )
        .expect("delete suite links");
    let suite_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM research_suites", [], |row| row.get(0))
        .expect("suite count");
    let plan_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM plans WHERE id=?1",
            [plan.as_str()],
            |row| row.get(0),
        )
        .expect("plan count");
    assert_eq!(suite_count, 0);
    assert_eq!(plan_count, 1);
}

#[test]
fn schedule_preview_delete_releases_links_and_retains_last_success_dataset() {
    let root = TempRoot::new("schedule-delete");
    let mut store = Store::open(&root.0).expect("open store");
    let dataset = DatasetId::new("dataset-retained").expect("dataset ID");
    insert_dataset(&store, &dataset);
    let schedule = ScheduleId::new("schedule-delete").expect("schedule ID");
    store.connection.execute(
        "INSERT INTO collection_schedules(id,request_id,input_digest,status,created_at_ms,next_action_at_ms,request_json,last_success_dataset_id,last_success_boundary_ms,failure_json) VALUES (?1,'request-schedule-delete',?2,'paused',0,0,'{}',?3,1,NULL)",
        params![
            schedule.as_str(),
            ContentHash::of_bytes(b"schedule").as_str(),
            dataset.as_str()
        ],
    ).expect("insert fixture schedule");
    let preview = store
        .delete_preview(
            &DeleteResource::Schedule {
                schedule_id: schedule,
            },
            time("2024-02-01T00:00:00Z"),
        )
        .expect("schedule preview");
    assert!(
        preview
            .retained_shared
            .iter()
            .any(|group| group.kind == DeleteEntryKind::Dataset && group.count == 1)
    );
    store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview,
                cascade: false,
            },
            time("2024-02-01T00:01:00Z"),
        )
        .expect("delete schedule links");
    let schedule_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM collection_schedules", [], |row| {
            row.get(0)
        })
        .expect("schedule count");
    let dataset_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM datasets WHERE id=?1",
            [dataset.as_str()],
            |row| row.get(0),
        )
        .expect("dataset count");
    assert_eq!(schedule_count, 0);
    assert_eq!(dataset_count, 1);
}

#[test]
fn managed_backup_uses_the_common_preview_and_journal_delete_engine() {
    let root = TempRoot::new("backup-delete");
    let mut store = Store::open(&root.0).expect("open store");
    let receipt = store
        .create_managed_backup(
            &RequestId::new("delete-backup").expect("request ID"),
            time("2024-01-01T00:00:00Z"),
        )
        .expect("create backup");
    let backup_path = managed_backup_root(&root.0)
        .expect("backup root")
        .join(receipt.id.as_str());
    let preview = store
        .delete_preview(
            &DeleteResource::Backup {
                backup_id: receipt.id.clone(),
            },
            time("2024-01-02T00:00:00Z"),
        )
        .expect("backup preview");
    assert_eq!(preview.exclusive_file_count, 1);
    assert_eq!(preview.reclaimable_file_bytes, Some(receipt.bytes));
    store
        .execute_hard_delete(
            &HardDeleteRequest {
                preview,
                cascade: false,
            },
            time("2024-01-02T00:01:00Z"),
        )
        .expect("delete backup");
    assert!(!backup_path.exists());
    assert_eq!(
        store
            .list_managed_backups()
            .expect("empty list")
            .total_count,
        0
    );
}

#[test]
fn active_managed_child_blocks_parent_deletion() {
    let root = TempRoot::new("active-child");
    let store = Store::open(&root.0).expect("open store");
    insert_job(&store, "job-active", time("2024-01-01T00:00:00Z"), "QUEUED");
    store.connection.execute(
        "INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms,ended_at_ms,cancel_requested_at_ms) VALUES ('attempt-active','job-active',1,?1,'QUEUED','{}','queued',0,0,0,NULL,NULL,NULL)",
        [ContentHash::of_bytes(b"active").as_str()],
    ).expect("insert active attempt");
    let suite = SuiteId::new("suite-active").expect("suite ID");
    insert_suite(&store, &suite, time("2024-01-01T00:00:00Z"), None);
    store.connection.execute(
        "INSERT INTO research_suite_cases(id,suite_id,case_index,fold_index,phase,scenario_index,range_start_ms,range_end_ms,status,plan_id,job_id,attempt_id,run_id,causal_input_digest,failure_json) VALUES ('suite-case-active',?1,0,NULL,'batch',0,0,1,'queued',NULL,'job-active','attempt-active',NULL,NULL,NULL)",
        [suite.as_str()],
    ).expect("insert active case");
    let preview = store
        .delete_preview(
            &DeleteResource::Suite { suite_id: suite },
            time("2024-01-02T00:00:00Z"),
        )
        .expect("preview");
    assert!(preview.blockers.iter().any(|blocker| {
        blocker.class == DeleteBlockerClass::ActiveJob && blocker.reference == "job:job-active"
    }));
}
