use super::*;
use crate::contracts::{
    CandleInterval, FailureRecord, FreshnessPolicy, FreshnessState, MarketId, ScheduleRetryPolicy,
    SourceProbeResult,
};

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("valid fixture time")
}

fn request() -> CollectionScheduleRequest {
    CollectionScheduleRequest {
        request_id: RequestId::new("hourly-btc").expect("request id"),
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

#[test]
fn missed_ticks_coalesce_to_latest_completed_boundary() {
    let created = time("2024-01-01T00:02:00Z");
    let mut schedule = create_schedule(request(), created).expect("create schedule");
    schedule.last_success_boundary = Some(time("2024-01-01T01:00:00Z"));

    let ScheduleTick::Admit {
        boundary,
        submission,
    } = schedule_tick(&schedule, time("2024-01-01T08:47:19Z")).expect("tick")
    else {
        panic!("latest boundary must be admitted");
    };
    assert_eq!(boundary, time("2024-01-01T08:00:00Z"));
    assert_eq!(submission.range.end(), boundary);
    assert_eq!(submission.range.start(), time("2023-12-31T08:00:00Z"));
    assert_eq!(
        submission.request_id,
        fire_request_id(&schedule.id, boundary)
    );
}

#[test]
fn duplicate_boundary_is_suppressed_and_full_queue_can_leave_state_unchanged() {
    let now = time("2024-01-01T08:47:19Z");
    let mut schedule = create_schedule(request(), now).expect("create schedule");
    schedule.last_success_boundary = Some(time("2024-01-01T08:00:00Z"));
    let before = serde_json::to_vec(&schedule).expect("serialize before");

    assert!(matches!(
        schedule_tick(&schedule, now).expect("tick"),
        ScheduleTick::NoNewBoundary { .. }
    ));
    assert_eq!(
        serde_json::to_vec(&schedule).expect("serialize after"),
        before,
        "pure scheduling cannot advance durable state before admission succeeds"
    );
}

#[test]
fn only_classified_transient_failures_retry_with_a_cap() {
    let policy = request().retry;
    let now = time("2024-01-01T00:00:00Z");
    for code in ["NETWORK_UNAVAILABLE", "RATE_LIMITED", "TEMPORARILY_BLOCKED"] {
        let failure = FailureRecord {
            code: code.into(),
            message: "transient fixture".into(),
        };
        assert_eq!(
            classify_failure_record(&failure, 0, &policy, now).expect("classification"),
            ScheduleFailureAction::RetryAt {
                retry_count: 1,
                retry_at: time("2024-01-01T00:01:00Z")
            }
        );
        assert_eq!(
            classify_failure_record(&failure, 2, &policy, now).expect("classification"),
            ScheduleFailureAction::Block
        );
    }
    for code in ["INVALID_CONFIG", "DATA_CORRUPT"] {
        let failure = FailureRecord {
            code: code.into(),
            message: "permanent fixture".into(),
        };
        assert_eq!(
            classify_failure_record(&failure, 0, &policy, now).expect("classification"),
            ScheduleFailureAction::Block
        );
    }
}

#[test]
fn schedule_request_enforces_joint_collection_and_control_bounds() {
    let mut value = request();
    value.markets = vec![
        MarketId::parse_upbit("KRW-BTC").expect("market"),
        MarketId::parse_upbit("KRW-ETH").expect("market"),
        MarketId::parse_upbit("KRW-XRP").expect("market"),
    ];
    value.interval = CandleInterval::M1;
    value.lookback_bars = 10_000;
    value.validate().expect("30k rows accepted");
    value.lookback_bars = 10_001;
    assert!(matches!(value.validate(), Err(LabError::ResourceLimit(_))));
    value.lookback_bars = 1;
    value.cadence_seconds = 59;
    assert!(matches!(value.validate(), Err(LabError::InvalidConfig(_))));
    value.cadence_seconds = 60;
    value.retry.max_retries = 6;
    assert!(matches!(value.validate(), Err(LabError::InvalidConfig(_))));

    let mut duration = request();
    duration.interval = CandleInterval::D1;
    duration.lookback_bars = 367;
    assert!(matches!(
        duration.validate(),
        Err(LabError::ResourceLimit(_))
    ));
}

#[test]
fn an_in_flight_fire_prevents_parallel_admission() {
    let now = time("2024-01-01T08:47:19Z");
    let mut schedule = create_schedule(request(), now).expect("create schedule");
    let boundary = time("2024-01-01T08:00:00Z");
    schedule.in_flight = Some(crate::contracts::ScheduleFire {
        schedule_id: schedule.id.clone(),
        boundary,
        request_id: fire_request_id(&schedule.id, boundary),
        status: crate::contracts::ScheduleFireStatus::Queued,
        job_id: None,
        attempt_id: None,
        dataset_id: None,
        created_at: now,
        retry_count: 0,
        retry_at: None,
        failure: None,
    });
    assert!(matches!(
        schedule_tick(&schedule, time("2024-01-01T10:00:00Z")).expect("tick"),
        ScheduleTick::NotDue
    ));
}

#[test]
fn recovery_chunks_cover_the_entire_frozen_gap_beyond_normal_lookback() {
    let created = time("2023-01-01T00:00:00Z");
    let mut schedule = create_schedule(request(), created).expect("create schedule");
    schedule.last_success_boundary = Some(created);
    let target = time("2025-01-01T00:00:00Z");
    let gap = recovery_gap(&schedule, target)
        .expect("recovery gap")
        .expect("nonempty outage");
    assert_eq!(gap.start(), created);
    assert_eq!(gap.end(), target);

    let mut cursor = gap.start();
    let mut index = 0_u32;
    while cursor < target {
        let chunk = recovery_chunk_request(&schedule, target, cursor, index, 0)
            .expect("bounded recovery chunk");
        assert_eq!(chunk.range.start(), cursor);
        assert!(chunk.range.end() <= target);
        chunk
            .validate(target)
            .expect("chunk stays within public bounds");
        cursor = chunk.range.end();
        index = index.checked_add(1).expect("bounded fixture chunks");
    }
    assert_eq!(cursor, target);
    assert!(index > 1, "two-year outage must require bounded chunking");
}

#[test]
fn freshness_classifier_separates_finalization_publication_and_true_gaps() {
    let expected = time("2024-01-01T02:00:00Z");
    let latest = Some(time("2024-01-01T01:00:00Z"));
    let policy = FreshnessPolicy {
        grace_seconds: 600,
        source_delay_seconds: 3_600,
        consecutive_gap_threshold: 2,
    };
    let probe = |has: bool| SourceProbeResult {
        attempted: true,
        source_has_boundary: Some(has),
        note: "classifier fixture".into(),
    };
    let classify = |now: UtcTimestamp, consecutive: u64, probe: Option<&SourceProbeResult>| {
        classify_market_freshness(
            expected,
            latest,
            now,
            consecutive,
            3_600,
            &policy,
            probe,
            None,
            None,
        )
        .state
    };
    // A covered boundary is fresh regardless of age.
    assert_eq!(
        classify_market_freshness(
            expected,
            Some(time("2024-01-01T02:00:00Z")),
            time("2024-01-01T09:00:00Z"),
            0,
            3_600,
            &policy,
            None,
            None,
            None,
        )
        .state,
        FreshnessState::Fresh
    );
    // Boundary directly behind us: still finalizing inside the grace window.
    assert_eq!(
        classify(expected, 1, None),
        FreshnessState::WaitingForFinalization
    );
    // A source probe that exposes the boundary is a collector problem when no cycle is pending.
    assert_eq!(
        classify(time("2024-01-01T02:20:00Z"), 1, Some(&probe(true))),
        FreshnessState::CollectorDelay
    );
    // Past grace but inside the publication window: source delay, not a gap.
    assert_eq!(
        classify(time("2024-01-01T02:30:00Z"), 1, None),
        FreshnessState::SourceDelay
    );
    // Past the publication window with enough consecutive absence: true gap.
    assert_eq!(
        classify(time("2024-01-01T03:30:00Z"), 3, None),
        FreshnessState::TrueGap
    );
    // Only one historical boundary below the threshold stays recoverable.
    assert_eq!(
        classify(time("2024-01-01T03:30:00Z"), 2, None),
        FreshnessState::SourceDelay
    );
    // One missing boundary below the threshold stays a recoverable delay.
    assert_eq!(
        classify(time("2024-01-01T03:30:00Z"), 1, None),
        FreshnessState::SourceDelay
    );
    // A blocked schedule fails freshness classification outright.
    let failure = FailureRecord {
        code: "COLLECTION_BLOCKED".into(),
        message: "fixture".into(),
    };
    assert_eq!(
        classify_market_freshness(
            expected,
            latest,
            expected,
            1,
            3_600,
            &policy,
            None,
            Some(&failure),
            None,
        )
        .state,
        FreshnessState::Failed
    );
}

#[test]
fn freshness_classifier_cycle_awareness_prevents_false_collector_delay_and_gap() {
    let expected = time("2024-01-01T02:00:00Z");
    let latest = Some(time("2024-01-01T01:00:00Z"));
    let policy = FreshnessPolicy {
        grace_seconds: 600,
        source_delay_seconds: 3_600,
        consecutive_gap_threshold: 2,
    };
    let probe = SourceProbeResult {
        attempted: true,
        source_has_boundary: Some(true),
        note: "classifier fixture".into(),
    };
    // When next_action_at is pending in the future, source boundary presence is waiting for finalization, not collector delay.
    assert_eq!(
        classify_market_freshness(
            expected,
            latest,
            time("2024-01-01T02:20:00Z"),
            1,
            3_600,
            &policy,
            Some(&probe),
            None,
            Some(time("2024-01-01T04:00:00Z")),
        )
        .state,
        FreshnessState::WaitingForFinalization
    );
    // When within the scheduled collection cycle, multiple uncollected bars are waiting for scheduled collection, not a true gap.
    assert_eq!(
        classify_market_freshness(
            expected,
            latest,
            time("2024-01-01T03:30:00Z"),
            3,
            3_600,
            &policy,
            None,
            None,
            Some(time("2024-01-01T04:00:00Z")),
        )
        .state,
        FreshnessState::WaitingForFinalization
    );
}
