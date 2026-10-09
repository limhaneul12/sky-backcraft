use super::*;
use crate::contracts::{
    ArtifactId, CandleInterval, CollectionScheduleRequest, DeleteBlockerClass, DeleteResource,
    FreshnessState, JobSubmission, MarketId, ScheduleFailureClass, ScheduleRecoveryState,
    ScheduleRetryPolicy, SourceProbeResult,
};
use crate::scheduling::{ScheduleTick, create_schedule, schedule_tick};
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-schedule-{label}-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create schedule test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("valid fixture time")
}

fn request(id: &str) -> CollectionScheduleRequest {
    CollectionScheduleRequest {
        request_id: RequestId::new(id).expect("request id"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        interval: CandleInterval::H1,
        lookback_bars: 24,
        cadence_seconds: 300,
        freshness_policy: None,
        retry: ScheduleRetryPolicy {
            max_retries: 2,
            backoff_seconds: 60,
        },
    }
}

fn admit(store: &mut Store, schedule_id: &ScheduleId, now: UtcTimestamp) -> ScheduleFire {
    let schedule = store
        .get_collection_schedule(schedule_id)
        .expect("load schedule")
        .expect("schedule exists");
    let ScheduleTick::Admit {
        boundary,
        submission,
    } = schedule_tick(&schedule, now).expect("project tick")
    else {
        panic!("schedule must be due");
    };
    store
        .admit_collection_schedule_fire(schedule_id, boundary, submission, now)
        .expect("admit fire")
}

#[test]
fn sqlite_create_is_idempotent_and_freshness_keeps_absence_typed() {
    let root = TempRoot::new("create");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let input = request("schedule-create");
    let schedule = create_schedule(input.clone(), now).expect("freeze schedule");
    let created = store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let repeated = store
        .create_collection_schedule(&schedule)
        .expect("idempotent create");
    assert_eq!(created.id, repeated.id);

    let freshness = store
        .collection_freshness(&schedule.id, now, &std::collections::BTreeMap::new())
        .expect("freshness");
    assert_eq!(freshness.markets.len(), 1);
    assert_eq!(freshness.markets[0].latest_completed_end, None);
    assert_eq!(freshness.markets[0].age_seconds, None);
    assert_eq!(
        freshness.markets[0].missing,
        Some(FreshnessMissingReason::NeverCollected)
    );
    assert_eq!(freshness.markets[0].gap_count, 24);

    let mut conflict_request = request("schedule-create");
    conflict_request.lookback_bars = 12;
    let conflict = create_schedule(conflict_request, now).expect("freeze conflict");
    assert!(matches!(
        store.create_collection_schedule(&conflict),
        Err(LabError::Conflict(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real SQLite journey proves latest boundary, gaps, age and derived coverage"
)]
fn freshness_reports_latest_completed_boundary_and_real_grid_gaps() {
    let root = TempRoot::new("freshness");
    let mut store = Store::open(&root.0).expect("open store");
    let created_at = time("2024-01-01T03:00:00Z");
    let mut input = request("schedule-freshness");
    input.lookback_bars = 3;
    let schedule = create_schedule(input, created_at).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    store
        .connection
        .execute(
            "INSERT INTO collections(request_id,normalized_request_digest,request_json) \
             VALUES ('freshness-dataset-request',?1,'{}')",
            [digest],
        )
        .expect("insert collection fixture");
    store
        .connection
        .execute(
            "INSERT INTO datasets(id,request_id,normalized_request_digest,schema_version,status,\
         coverage_start_ms,coverage_end_ms,row_count,normalizer_version,gap_policy,\
         semantic_digest,provenance_digest,origin,manifest_json) \
         VALUES ('dataset-freshness','freshness-dataset-request',?1,'1','READY',\
         ?2,?3,2,'fixture','fixture',?1,?1,'SYNTHETIC_TEST_ONLY','{}')",
            params![
                digest,
                timestamp_ms(time("2024-01-01T00:00:00Z")),
                timestamp_ms(time("2024-01-01T03:00:00Z"))
            ],
        )
        .expect("insert dataset fixture");
    for (id, open, close, position) in [
        (
            "obs-freshness-0",
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
            0,
        ),
        (
            "obs-freshness-2",
            "2024-01-01T02:00:00Z",
            "2024-01-01T03:00:00Z",
            1,
        ),
    ] {
        store
            .connection
            .execute(
                "INSERT INTO candle_observations(id,market,interval,open_time_ms,close_time_ms,\
             open_decimal,high_decimal,low_decimal,close_decimal,volume_decimal,\
             quote_turnover_decimal,completed,content_digest,observation_json) \
             VALUES (?1,'KRW-BTC','h1',?2,?3,'1','1','1','1','1','1',1,?4,'{}')",
                params![
                    id,
                    timestamp_ms(time(open)),
                    timestamp_ms(time(close)),
                    digest
                ],
            )
            .expect("insert observation fixture");
        store
            .connection
            .execute(
                "INSERT INTO dataset_members(dataset_id,observation_id,position) \
             VALUES ('dataset-freshness',?1,?2)",
                params![id, position],
            )
            .expect("insert dataset member fixture");
    }
    store
        .connection
        .execute(
            "UPDATE collection_schedules SET last_success_dataset_id='dataset-freshness',\
         last_success_boundary_ms=?1 WHERE id=?2",
            params![
                timestamp_ms(time("2024-01-01T03:00:00Z")),
                schedule.id.as_str()
            ],
        )
        .expect("link schedule fixture dataset");

    let current = store
        .collection_freshness(
            &schedule.id,
            time("2024-01-01T03:00:00Z"),
            &std::collections::BTreeMap::new(),
        )
        .expect("current freshness");
    assert_eq!(
        current.markets[0].latest_completed_end,
        Some(time("2024-01-01T03:00:00Z"))
    );
    assert_eq!(current.markets[0].age_seconds, Some(0));
    assert_eq!(current.markets[0].gap_count, 1);
    assert_eq!(current.markets[0].missing, None);

    let stale = store
        .collection_freshness(
            &schedule.id,
            time("2024-01-01T04:00:00Z"),
            &std::collections::BTreeMap::new(),
        )
        .expect("stale freshness");
    assert_eq!(
        stale.markets[0].latest_completed_end,
        Some(time("2024-01-01T03:00:00Z"))
    );
    assert_eq!(stale.markets[0].age_seconds, Some(3_600));
    assert_eq!(stale.markets[0].gap_count, 2);
    assert_eq!(
        stale.markets[0].missing,
        Some(FreshnessMissingReason::ExpectedBoundaryMissing)
    );
    let loaded = store
        .get_collection_schedule(&schedule.id)
        .expect("load schedule")
        .expect("schedule exists");
    assert_eq!(
        loaded.last_success_coverage,
        Some(
            crate::contracts::UtcRange::new(
                time("2024-01-01T00:00:00Z"),
                time("2024-01-01T03:00:00Z")
            )
            .expect("coverage")
        )
    );
}

#[test]
fn permanent_producer_failure_blocks_schedule_until_explicit_resume() {
    let root = TempRoot::new("producer-block");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let schedule =
        create_schedule(request("schedule-producer-block"), now).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let failure = FailureRecord {
        code: "RESOURCE_LIMIT".into(),
        message: "job attempt history limit is 32".into(),
    };
    let blocked = store
        .block_collection_schedule(&schedule.id, failure.clone(), now)
        .expect("block schedule");
    assert_eq!(blocked.status, CollectionScheduleStatus::Blocked);
    assert_eq!(
        blocked.failure_class,
        Some(ScheduleFailureClass::OperatorRequired)
    );
    assert_eq!(
        blocked.recovery_state,
        ScheduleRecoveryState::OperatorRequired
    );
    assert_eq!(
        blocked.failure.as_ref().map(|value| value.code.as_str()),
        Some("RESOURCE_LIMIT")
    );
    assert!(
        store
            .due_collection_schedules(time("2024-01-02T00:00:00Z"), 8)
            .expect("due schedules")
            .is_empty(),
        "blocked schedule must not auto retry"
    );
    let readback = store
        .block_collection_schedule(&schedule.id, failure, now)
        .expect("idempotent blocked readback");
    assert_eq!(readback.status, CollectionScheduleStatus::Blocked);
    let resumed = store
        .resume_collection_schedule(&schedule.id, now)
        .expect("explicit resume");
    assert_eq!(resumed.status, CollectionScheduleStatus::Active);
}

#[test]
fn restart_interruption_repins_same_fire_without_consuming_network_retry_budget() {
    let root = TempRoot::new("restart");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let schedule = create_schedule(request("schedule-restart"), now).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let fire = admit(&mut store, &schedule.id, now);
    let claimed = store
        .claim_next(now)
        .expect("claim")
        .expect("queued attempt");
    assert_eq!(claimed.id, fire.attempt_id.clone().expect("attempt pin"));
    store
        .finish_attempt(
            &claimed.id,
            AttemptState::Interrupted {
                ended_at: now,
                reason: "runtime restart".into(),
            },
        )
        .expect("interrupt attempt");
    drop(store);
    let mut store = Store::open(&root.0).expect("reopen after runtime restart");

    let reconciled = store
        .reconcile_collection_schedule(&schedule.id, now)
        .expect("reconcile interruption");
    let waiting = reconciled.in_flight.expect("retry wait fire");
    assert_eq!(waiting.status, ScheduleFireStatus::RetryWait);
    assert_eq!(waiting.retry_count, 0);
    assert_eq!(waiting.request_id, fire.request_id);

    let retried = store
        .retry_collection_schedule_fire(&schedule.id, now)
        .expect("retry interrupted fire");
    assert_eq!(retried.job_id, fire.job_id);
    assert_ne!(retried.attempt_id, fire.attempt_id);
    assert_eq!(retried.request_id, fire.request_id);
    assert_eq!(retried.retry_count, 0);
    let job = store
        .get_job(retried.job_id.as_ref().expect("job pin"))
        .expect("get job")
        .expect("job exists");
    assert_eq!(job.attempts.len(), 2);
}

#[test]
fn pause_cancellation_is_durable_and_resume_atomically_repins() {
    let root = TempRoot::new("pause");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let schedule = create_schedule(request("schedule-pause"), now).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let fire = admit(&mut store, &schedule.id, now);
    let (paused, cancel_job) = store
        .pause_collection_schedule(&schedule.id, now)
        .expect("pause schedule");
    assert_eq!(paused.status, CollectionScheduleStatus::Paused);
    assert!(cancel_job.is_empty(), "queued work needs no runtime signal");
    assert!(paused.in_flight.is_none());
    let job_id = fire.job_id.clone().expect("owned job");

    let resumed = store
        .resume_collection_schedule(&schedule.id, now)
        .expect("resume schedule");
    assert_eq!(resumed.status, CollectionScheduleStatus::Active);
    let current = resumed.in_flight.expect("repinned fire");
    assert_eq!(current.request_id, fire.request_id);
    assert_eq!(current.job_id.as_ref(), Some(&job_id));
    assert_ne!(current.attempt_id, fire.attempt_id);
    let job = store
        .get_job(&job_id)
        .expect("get job")
        .expect("job exists");
    assert_eq!(job.attempts.len(), 2);
}

#[test]
fn pausing_running_fire_persists_cancel_before_returning_runtime_signal() {
    let root = TempRoot::new("pause-running");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let schedule =
        create_schedule(request("schedule-pause-running"), now).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    let fire = admit(&mut store, &schedule.id, now);
    let claimed = store
        .claim_next(now)
        .expect("claim")
        .expect("running attempt");

    let (paused, signals) = store
        .pause_collection_schedule(&schedule.id, now)
        .expect("pause running schedule");
    let job_id = fire.job_id.clone().expect("job pin");
    assert_eq!(signals, vec![job_id.clone()]);
    assert_eq!(paused.status, CollectionScheduleStatus::Paused);
    assert!(matches!(
        store
            .get_job(&job_id)
            .expect("get job")
            .expect("job exists")
            .attempts
            .last()
            .map(|attempt| &attempt.state),
        Some(AttemptState::Running {
            cancel_requested_at: Some(_),
            ..
        })
    ));

    let (_, repeated_signals) = store
        .pause_collection_schedule(&schedule.id, now)
        .expect("idempotent pause readback");
    assert_eq!(repeated_signals, vec![job_id]);
    store
        .finish_attempt(
            &claimed.id,
            AttemptState::Cancelled {
                ended_at: now,
                reason: "owner pause".into(),
            },
        )
        .expect("acknowledge runtime cancellation");
    let reconciled = store
        .reconcile_collection_schedule(&schedule.id, now)
        .expect("reconcile cancellation");
    assert_eq!(reconciled.status, CollectionScheduleStatus::Paused);
    assert!(reconciled.in_flight.is_none());
}

#[test]
fn full_job_queue_leaves_schedule_and_fire_boundary_unchanged() {
    let root = TempRoot::new("queue");
    let mut store = Store::open(&root.0).expect("open store");
    let now = time("2024-01-01T08:47:19Z");
    let schedule = create_schedule(request("schedule-full"), now).expect("freeze schedule");
    store
        .create_collection_schedule(&schedule)
        .expect("create schedule");
    for index in 0..8 {
        store
            .submit_job(
                &JobSubmission {
                    request_id: RequestId::new(format!("filler-{index}")).expect("request id"),
                    payload: JobPayload::Verify {
                        artifact_id: ArtifactId::new(format!("artifact-{index}"))
                            .expect("artifact id"),
                        replay: false,
                    },
                },
                now,
            )
            .expect("fill durable queue");
    }
    let before = store
        .get_collection_schedule(&schedule.id)
        .expect("load before")
        .expect("schedule exists");
    let ScheduleTick::Admit {
        boundary,
        submission,
    } = schedule_tick(&before, now).expect("tick")
    else {
        panic!("schedule must be due");
    };
    assert!(matches!(
        store.admit_collection_schedule_fire(&schedule.id, boundary, submission, now),
        Err(LabError::CapacityExceeded(_))
    ));
    let after = store
        .get_collection_schedule(&schedule.id)
        .expect("load after")
        .expect("schedule exists");
    assert_eq!(after.next_action_at, before.next_action_at);
    assert_eq!(after.last_success_boundary, None);
    assert!(after.in_flight.is_none());
}

#[test]
fn freshness_classifies_waiting_source_delay_collector_delay_and_true_gap()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("freshness-states");
    let mut store = Store::open(&root.0)?;
    let created_at = time("2024-01-01T01:30:00Z");
    let mut input = request("schedule-states");
    input.lookback_bars = 4;
    let schedule = create_schedule(input, created_at)?;
    store.create_collection_schedule(&schedule)?;
    let digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    store.connection.execute(
        "INSERT INTO collections(request_id,normalized_request_digest,request_json) \
         VALUES ('states-dataset-request',?1,'{}')",
        [digest],
    )?;
    store.connection.execute(
        "INSERT INTO datasets(id,request_id,normalized_request_digest,schema_version,status,\
         coverage_start_ms,coverage_end_ms,row_count,normalizer_version,gap_policy,\
         semantic_digest,provenance_digest,origin,manifest_json) \
         VALUES ('dataset-states','states-dataset-request',?1,'1','READY',\
         ?2,?3,1,'fixture','fixture',?1,?1,'SYNTHETIC_TEST_ONLY','{}')",
        params![
            digest,
            timestamp_ms(time("2024-01-01T00:00:00Z")),
            timestamp_ms(time("2024-01-01T01:00:00Z"))
        ],
    )?;
    store.connection.execute(
        "INSERT INTO candle_observations(id,market,interval,open_time_ms,close_time_ms,\
         open_decimal,high_decimal,low_decimal,close_decimal,volume_decimal,\
         quote_turnover_decimal,completed,content_digest,observation_json) \
         VALUES ('obs-states-0','KRW-BTC','h1',?1,?2,'1','1','1','1','1','1',1,?3,'{}')",
        params![
            timestamp_ms(time("2024-01-01T00:00:00Z")),
            timestamp_ms(time("2024-01-01T01:00:00Z")),
            digest
        ],
    )?;
    store.connection.execute(
        "INSERT INTO dataset_members(dataset_id,observation_id,position) \
         VALUES ('dataset-states','obs-states-0',0)",
        [],
    )?;
    store.connection.execute(
        "UPDATE collection_schedules SET last_success_dataset_id='dataset-states',\
         last_success_boundary_ms=?1 WHERE id=?2",
        params![
            timestamp_ms(time("2024-01-01T01:00:00Z")),
            schedule.id.as_str()
        ],
    )?;

    let empty = std::collections::BTreeMap::new();
    // expected_end = 02:00 at both observation points; the boundary candle may
    // still be finalizing inside the default 600s H1 grace.
    let waiting = store.collection_freshness(&schedule.id, time("2024-01-01T02:00:00Z"), &empty)?;
    assert_eq!(
        waiting.markets[0].state,
        FreshnessState::WaitingForFinalization
    );
    assert_eq!(waiting.markets[0].consecutive_missing, 1);
    // Past grace but within the two-bar source publication window.
    let delayed = store.collection_freshness(&schedule.id, time("2024-01-01T02:20:00Z"), &empty)?;
    assert_eq!(delayed.markets[0].state, FreshnessState::SourceDelay);
    // A live source probe that exposes the boundary turns delay into collector delay.
    let mut probes = std::collections::BTreeMap::new();
    probes.insert(
        "KRW-BTC".to_string(),
        SourceProbeResult {
            attempted: true,
            source_has_boundary: Some(true),
            note: "fixture probe".into(),
        },
    );
    let collector =
        store.collection_freshness(&schedule.id, time("2024-01-01T02:20:00Z"), &probes)?;
    assert_eq!(collector.markets[0].state, FreshnessState::CollectorDelay);
    assert_eq!(
        collector.markets[0]
            .source_probe
            .as_ref()
            .and_then(|probe| probe.source_has_boundary),
        Some(true)
    );
    // Past the source window with 3 consecutive missing boundaries: a true gap.
    let gap = store.collection_freshness(&schedule.id, time("2024-01-01T05:30:00Z"), &empty)?;
    assert_eq!(gap.markets[0].state, FreshnessState::TrueGap);
    assert_eq!(gap.markets[0].consecutive_missing, 4);
    assert_eq!(
        gap.markets[0].missing,
        Some(FreshnessMissingReason::ExpectedBoundaryMissing)
    );
    // A blocked schedule reports FAILED before any timing classification.
    store.connection.execute(
        "UPDATE collection_schedules SET failure_json='{\"code\":\"COLLECTION_BLOCKED\",\"message\":\"fixture\"}' \
         WHERE id=?1",
        [schedule.id.as_str()],
    )?;
    let failed = store.collection_freshness(&schedule.id, time("2024-01-01T05:30:00Z"), &empty)?;
    assert_eq!(failed.markets[0].state, FreshnessState::Failed);
    Ok(())
}

#[test]
fn recoverable_failures_persist_backoff_while_permanent_failures_require_operator()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::FailureRecord;
    let root = TempRoot::new("recovery-backoff");
    let mut store = Store::open(&root.0)?;
    let now = time("2024-01-01T08:00:00Z");
    let schedule = create_schedule(request("schedule-recovery"), now)?;
    store.create_collection_schedule(&schedule)?;
    // Recoverable failures stay under automatic ownership and never become a
    // permanent blocked schedule after the ordinary retry budget is exhausted.
    store.block_collection_schedule(
        &schedule.id,
        FailureRecord {
            code: "NETWORK_UNAVAILABLE".into(),
            message: "fixture outage".into(),
        },
        now,
    )?;
    let waiting = store
        .get_collection_schedule(&schedule.id)?
        .expect("recovering schedule");
    assert_eq!(waiting.status, CollectionScheduleStatus::Active);
    assert_eq!(waiting.recovery_state, ScheduleRecoveryState::RecoveryWait);
    assert_eq!(
        waiting.failure_class,
        Some(ScheduleFailureClass::Recoverable)
    );
    assert!(
        store
            .schedule_recovery_candidates(time("2024-01-01T08:00:30Z"), 8)?
            .is_empty()
    );
    let due = time("2024-01-01T08:02:00Z");
    assert_eq!(
        store.schedule_recovery_candidates(due, 8)?.len(),
        1,
        "due recovering schedule becomes a probe candidate"
    );
    let probing = store.claim_schedule_recovery_probe(&schedule.id, due)?;
    assert_eq!(probing.recovery_state, ScheduleRecoveryState::Probing);
    assert_eq!(probing.recovery_attempt_count, 1);
    store.defer_schedule_recovery(
        &schedule.id,
        &FailureRecord {
            code: "RATE_LIMITED".into(),
            message: "still unavailable".into(),
        },
        due,
    )?;
    assert!(
        store
            .schedule_recovery_candidates(time("2024-01-01T08:02:30Z"), 8)?
            .is_empty(),
        "a deferred candidate waits out the durable backoff"
    );
    assert_eq!(
        store
            .schedule_recovery_candidates(time("2024-01-01T08:03:00Z"), 8)?
            .len(),
        1
    );

    let permanent = create_schedule(request("schedule-permanent"), now)?;
    store.create_collection_schedule(&permanent)?;
    store.block_collection_schedule(
        &permanent.id,
        FailureRecord {
            code: "INVALID_CONFIG".into(),
            message: "bad market".into(),
        },
        now,
    )?;
    let operator = store
        .get_collection_schedule(&permanent.id)?
        .expect("operator schedule");
    assert_eq!(operator.status, CollectionScheduleStatus::Blocked);
    assert_eq!(
        operator.recovery_state,
        ScheduleRecoveryState::OperatorRequired
    );
    assert!(
        store
            .schedule_recovery_candidates(time("2024-01-02T00:00:00Z"), 8)?
            .iter()
            .all(|candidate| candidate.id != permanent.id),
        "permanent failures never enter automatic recovery"
    );
    Ok(())
}

#[test]
fn restart_preserves_one_pinned_recovery_chunk_without_duplicate_jobs()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("recovery-restart");
    let now = time("2024-01-02T08:00:00Z");
    let schedule = create_schedule(request("schedule-recovery-restart"), now)?;
    let schedule_id = schedule.id.clone();
    {
        let mut store = Store::open(&root.0)?;
        store.create_collection_schedule(&schedule)?;
        store.block_collection_schedule(
            &schedule_id,
            FailureRecord {
                code: "NETWORK_UNAVAILABLE".into(),
                message: "fixture outage".into(),
            },
            now,
        )?;
        let due = time("2024-01-02T08:02:00Z");
        store.claim_schedule_recovery_probe(&schedule_id, due)?;
        let started = store.begin_schedule_recovery_backfill(
            &schedule_id,
            time("2024-01-02T08:00:00Z"),
            due,
        )?;
        assert_eq!(started.recovery_state, ScheduleRecoveryState::Backfilling);
        let job_id = started.backfill_job_id.clone().expect("backfill job");
        let queued_schedule_preview = store.delete_preview(
            &DeleteResource::Schedule {
                schedule_id: schedule_id.clone(),
            },
            now,
        )?;
        assert!(queued_schedule_preview.blockers.iter().any(|blocker| {
            blocker.class == DeleteBlockerClass::ActiveJob
                && blocker.reference == format!("job:{}", job_id.as_str())
        }));
        let child_preview = store.delete_preview(
            &DeleteResource::Job {
                job_id: job_id.clone(),
            },
            now,
        )?;
        assert!(child_preview.blockers.iter().any(|blocker| {
            blocker.class == DeleteBlockerClass::ProtectedReference
                && blocker.reference == format!("schedule:{}", schedule_id.as_str())
        }));
        let claimed = store.claim_next(now)?.expect("recovery chunk claimed");
        let running_schedule_preview = store.delete_preview(
            &DeleteResource::Schedule {
                schedule_id: schedule_id.clone(),
            },
            now,
        )?;
        assert!(
            running_schedule_preview
                .blockers
                .iter()
                .any(|blocker| blocker.class == DeleteBlockerClass::ActiveJob)
        );
        store.finish_attempt(
            &claimed.id,
            AttemptState::Interrupted {
                ended_at: now,
                reason: "fixture restart".into(),
            },
        )?;
    }
    let mut reopened = Store::open(&root.0)?;
    reopened.reconcile_schedule_recovery(&schedule_id, now)?;
    let durable = reopened
        .get_collection_schedule(&schedule_id)?
        .expect("recovery survives restart");
    let target = durable.pending_gap.expect("frozen gap").end();
    let repeated = reopened.begin_schedule_recovery_backfill(&schedule_id, target, now)?;
    assert_eq!(repeated.backfill_job_id, durable.backfill_job_id);
    let (chunks, jobs, attempts): (i64, i64, i64) = reopened.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM schedule_recovery_chunks WHERE schedule_id=?1),\
                (SELECT COUNT(*) FROM jobs WHERE id=?2),\
                (SELECT COUNT(*) FROM job_attempts WHERE job_id=?2)",
        params![
            schedule_id.as_str(),
            durable.backfill_job_id.as_ref().expect("job").as_str()
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!((chunks, jobs, attempts), (1, 1, 2));
    Ok(())
}

#[test]
fn pausing_recovery_backfill_cancels_owner_and_resume_repins_same_job()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("recovery-pause");
    let mut store = Store::open(&root.0)?;
    let now = time("2024-01-02T08:00:00Z");
    let schedule = create_schedule(request("schedule-recovery-pause"), now)?;
    store.create_collection_schedule(&schedule)?;
    store.block_collection_schedule(
        &schedule.id,
        FailureRecord {
            code: "NETWORK_UNAVAILABLE".into(),
            message: "fixture outage".into(),
        },
        now,
    )?;
    let due = time("2024-01-02T08:02:00Z");
    store.claim_schedule_recovery_probe(&schedule.id, due)?;
    let started =
        store.begin_schedule_recovery_backfill(&schedule.id, time("2024-01-02T08:00:00Z"), due)?;
    let job_id = started.backfill_job_id.expect("backfill job");
    let claimed = store.claim_next(due)?.expect("running backfill");
    let (paused, signals) = store.pause_collection_schedule(&schedule.id, due)?;
    assert_eq!(paused.status, CollectionScheduleStatus::Paused);
    assert_eq!(signals, vec![job_id.clone()]);
    store.finish_attempt(
        &claimed.id,
        AttemptState::Cancelled {
            ended_at: due,
            reason: "owner pause".into(),
        },
    )?;
    let paused = store.reconcile_schedule_recovery(&schedule.id, due)?;
    assert_eq!(paused.recovery_state, ScheduleRecoveryState::RecoveryWait);
    assert_eq!(paused.backfill_job_id.as_ref(), Some(&job_id));
    let resumed = store.resume_collection_schedule(&schedule.id, due)?;
    assert_eq!(resumed.status, CollectionScheduleStatus::Active);
    assert_eq!(resumed.recovery_state, ScheduleRecoveryState::Backfilling);
    assert_eq!(resumed.backfill_job_id.as_ref(), Some(&job_id));
    let job = store.get_job(&job_id)?.expect("backfill job retained");
    assert_eq!(job.attempts.len(), 2);
    Ok(())
}

#[test]
fn exhausted_recoverable_job_rotates_generation_without_permanent_block()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("recovery-attempt-cap");
    let mut store = Store::open(&root.0)?;
    let now = time("2024-01-02T08:00:00Z");
    let schedule = create_schedule(request("schedule-recovery-attempt-cap"), now)?;
    store.create_collection_schedule(&schedule)?;
    store.block_collection_schedule(
        &schedule.id,
        FailureRecord {
            code: "NETWORK_UNAVAILABLE".into(),
            message: "fixture outage".into(),
        },
        now,
    )?;
    let due = time("2024-01-02T08:02:00Z");
    store.claim_schedule_recovery_probe(&schedule.id, due)?;
    let started =
        store.begin_schedule_recovery_backfill(&schedule.id, time("2024-01-02T08:00:00Z"), due)?;
    let exhausted_job = started.backfill_job_id.expect("first generation job");
    let mut clock = due;
    for attempt_number in 1..=32_u32 {
        let claimed = store.claim_next(clock)?.expect("queued recovery attempt");
        assert_eq!(claimed.job_id, exhausted_job);
        assert_eq!(claimed.number, attempt_number);
        store.finish_attempt(
            &claimed.id,
            AttemptState::Failed {
                ended_at: clock,
                error: FailureRecord {
                    code: "NETWORK_UNAVAILABLE".into(),
                    message: format!("fixture outage attempt {attempt_number}"),
                },
            },
        )?;
        let waiting = store.reconcile_schedule_recovery(&schedule.id, clock)?;
        assert_eq!(waiting.status, CollectionScheduleStatus::Active);
        assert_eq!(waiting.recovery_state, ScheduleRecoveryState::RecoveryWait);
        assert_eq!(
            waiting.failure_class,
            Some(ScheduleFailureClass::Recoverable)
        );
        let retry_at = waiting.next_recovery_at.expect("bounded retry wait");
        clock = UtcTimestamp(
            retry_at
                .0
                .checked_add_signed(chrono::Duration::seconds(1))
                .expect("fixture retry time"),
        );
        if attempt_number < 32 {
            let retried = store.retry_schedule_recovery_backfill(&schedule.id, clock)?;
            assert_eq!(retried.backfill_job_id.as_ref(), Some(&exhausted_job));
        }
    }

    let rotated = store.retry_schedule_recovery_backfill(&schedule.id, clock)?;
    let new_job = rotated.backfill_job_id.expect("rotated generation job");
    assert_ne!(new_job, exhausted_job);
    assert_eq!(rotated.status, CollectionScheduleStatus::Active);
    assert_eq!(rotated.recovery_state, ScheduleRecoveryState::Backfilling);
    assert_eq!(
        rotated.failure_class,
        Some(ScheduleFailureClass::Recoverable)
    );
    let (chunks, old_attempts, new_attempts): (i64, i64, i64) = store.connection.query_row(
        "SELECT (SELECT COUNT(*) FROM schedule_recovery_chunks WHERE schedule_id=?1),\
                (SELECT COUNT(*) FROM job_attempts WHERE job_id=?2),\
                (SELECT COUNT(*) FROM job_attempts WHERE job_id=?3)",
        params![
            schedule.id.as_str(),
            exhausted_job.as_str(),
            new_job.as_str()
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!((chunks, old_attempts, new_attempts), (2, 32, 1));
    Ok(())
}
