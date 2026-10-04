use super::*;
use crate::contracts::{
    ArtifactId, CandleInterval, CollectionScheduleRequest, JobSubmission, MarketId,
    ScheduleRetryPolicy,
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
        .collection_freshness(&schedule.id, now)
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
        .collection_freshness(&schedule.id, time("2024-01-01T03:00:00Z"))
        .expect("current freshness");
    assert_eq!(
        current.markets[0].latest_completed_end,
        Some(time("2024-01-01T03:00:00Z"))
    );
    assert_eq!(current.markets[0].age_seconds, Some(0));
    assert_eq!(current.markets[0].gap_count, 1);
    assert_eq!(current.markets[0].missing, None);

    let stale = store
        .collection_freshness(&schedule.id, time("2024-01-01T04:00:00Z"))
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
