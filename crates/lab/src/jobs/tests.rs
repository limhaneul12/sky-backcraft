use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, CandleInterval, CandleObservation, CandleRecord, CostPolicy,
    ExecutionPolicy, MarketDataOrigin, MarketId, PaperEventKind, PaperMarketRuleScenario,
    PaperMarketStart, PaperSessionState, PaperSessionWrite, QuoteAmount, RequestId, StrategyKind,
    TickBand, Weight,
};
use crate::database::{DatabaseHandle, DatabaseOwner};
use crate::storage::RawObjectInput;
use rust_decimal::Decimal;

#[test]
fn failed_execution_owner_closes_admission_and_is_joined() -> Result<(), Box<dyn std::error::Error>>
{
    let root = std::env::temp_dir().join(format!("spot-lab-runner-failure-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let database = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let jobs = runtime.block_on(JobRuntime::start(
        database.handle(),
        UpbitClient::new()?,
        root.clone(),
        None,
    ))?;
    let service = jobs.service();
    // The owner is deliberately removed while the owned runner remains alive.
    // Wake then forces a real failed claim, rather than a mocked boolean state.
    database.shutdown()?;
    service.inner.wake.notify_one();
    let result = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.inner.runner_available.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!service.inner.admitting.load(Ordering::Acquire));
        assert!(
            service
                .control(JobControl::Retry {
                    job_id: JobId::new("not-admitted")?
                })
                .await
                .is_err()
        );
        assert!(jobs.shutdown().await.is_err());
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    result
}

#[test]
fn durable_queue_is_polled_when_submission_notification_is_lost()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("spot-lab-lost-wake-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();
        // Let the initial empty claim and interval tick settle. The committed
        // mutation below deliberately bypasses the caller's Notify continuation.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let submission = JobSubmission {
            request_id: crate::contracts::RequestId::new("lost-notification")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run")?,
                market: None,
            },
        };
        let admitted = database
            .call("submit_without_wake", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;
        let observed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let job = service.get(admitted.id.clone()).await?;
                if job
                    .attempts
                    .last()
                    .is_some_and(|attempt| attempt.state.is_terminal())
                {
                    return Ok::<_, LabError>(job);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        let shutdown = jobs.shutdown().await;
        let job = observed??;
        assert!(matches!(job.attempts[0].state, AttemptState::Failed { .. }));
        shutdown?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    result?;
    closed?;
    Ok(())
}

fn paper_test_time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("valid paper test timestamp")
}

fn paper_test_bar(
    _label: &str,
    open_at: UtcTimestamp,
    interval: CandleInterval,
    open: &str,
    close: &str,
) -> CandleObservation {
    let open_price =
        crate::contracts::PriceKrw::new(open.parse().expect("valid open")).expect("positive open");
    let close_price = crate::contracts::PriceKrw::new(close.parse().expect("valid close"))
        .expect("positive close");
    let candle = CandleRecord {
        market: "KRW-BTC".into(),
        interval,
        open_time_utc: open_at,
        close_time_utc: UtcTimestamp(open_at.0 + interval.duration()),
        open: open_price,
        high: crate::contracts::PriceKrw::new(open_price.get().max(close_price.get()))
            .expect("valid high"),
        low: crate::contracts::PriceKrw::new(open_price.get().min(close_price.get()))
            .expect("valid low"),
        close: close_price,
        volume: AssetQuantity::new(Decimal::from(100)).expect("valid volume"),
        quote_turnover: QuoteAmount::new(Decimal::from(10_000_000)).expect("valid turnover"),
        completed: true,
    };
    let content_digest = crate::contracts::observation_digest(&candle).expect("candle digest");
    CandleObservation {
        id: crate::contracts::ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn paper_test_bars(
    prefix: &str,
    first_open: UtcTimestamp,
    count: usize,
    interval: CandleInterval,
) -> Vec<CandleObservation> {
    (0..count)
        .map(|index| {
            paper_test_bar(
                &format!("{prefix}-{index}"),
                UtcTimestamp(
                    first_open.0
                        + interval.duration()
                            * i32::try_from(index).expect("fixture count fits i32"),
                ),
                interval,
                "100",
                "100",
            )
        })
        .collect()
}

fn paper_test_batch(
    store: &crate::storage::Store,
    mut bars: Vec<CandleObservation>,
    recorded_at: UtcTimestamp,
) -> Result<(crate::contracts::RawObjectRef, Vec<CandleObservation>), LabError> {
    let published = store.publish_raw(RawObjectInput {
        source_url: "http://127.0.0.1/synthetic-paper-test".into(),
        fetched_at: recorded_at,
        persisted_at: recorded_at,
        http_status: 200,
        remaining_req: Some("synthetic fixture".into()),
        origin: MarketDataOrigin::SyntheticTestOnly,
        body: b"PAPER_SESSION_TEST_ONLY synthetic candle payload".to_vec(),
    })?;
    for bar in &mut bars {
        bar.raw_object_ids = vec![published.object.id.clone()];
    }
    Ok((published.object, bars))
}

fn paper_test_costs() -> Result<CostPolicy, LabError> {
    let zero = BasisPoints::new(Decimal::ZERO)?;
    Ok(CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "SYNTHETIC_TEST_ONLY zero-cost scenario".into(),
        dynamic: None,
    })
}

fn paper_test_start(
    request_id: &str,
    policy_ref: crate::contracts::PolicyRevisionRef,
) -> Result<PaperSessionWrite, LabError> {
    Ok(PaperSessionWrite::Start {
        request_id: RequestId::new(request_id)?,
        policy_ref,
        decision_interval: CandleInterval::M1,
        execution_resolution: CandleInterval::M1,
        latency_ms: 0,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        markets: vec![PaperMarketStart {
            market: MarketId::parse_upbit("KRW-BTC")?,
            initial_cash: QuoteAmount::new(Decimal::from(1_000_000))?,
            costs: paper_test_costs()?,
            market_rules: PaperMarketRuleScenario::SimulatedScenario {
                min_notional: QuoteAmount::new(Decimal::from(10))?,
                quantity_step: AssetQuantity::new(Decimal::new(1, 1))?,
                ticks: vec![TickBand {
                    lower_bound: QuoteAmount::new(Decimal::ZERO)?,
                    tick: crate::contracts::PriceKrw::new(Decimal::ONE)?,
                }],
                assumption_label: "SYNTHETIC_TEST_ONLY market rules".into(),
            },
        }],
    })
}

async fn publish_paper_test_batch(
    database: &DatabaseHandle,
    bars: Vec<CandleObservation>,
    recorded_at: UtcTimestamp,
) -> Result<u32, LabError> {
    database
        .call("paper_test_publish_batch", move |store| {
            let batch = paper_test_batch(store, bars, recorded_at)?;
            store.reconcile_running_paper_sessions(&[batch], recorded_at)
        })
        .await
}

async fn start_paper_test_session(
    database: &DatabaseHandle,
    request_id: &'static str,
    family: StrategyKind,
    started_at: UtcTimestamp,
) -> Result<crate::contracts::PaperSessionId, LabError> {
    database
        .call("paper_test_start_session", move |store| {
            let options = store.paper_start_options()?;
            let option = options
                .policy_options
                .iter()
                .find(|option| option.family == family && option.supported)
                .ok_or_else(|| LabError::InvalidConfig("test policy unavailable".into()))?;
            if family == StrategyKind::S1 && option.decision_warmup_bars != 100 {
                return Err(LabError::Internal(format!(
                    "fixture expects S1 100-bar warmup, got {}",
                    option.decision_warmup_bars
                )));
            }
            let write = paper_test_start(request_id, option.reference.clone())?;
            Ok(store.paper_write(&write, started_at)?.session.session_id)
        })
        .await
}

async fn assert_job_service_admitting(service: &JobService) -> Result<(), LabError> {
    let status = service.status().await?;
    assert!(status.admission_open);
    assert!(status.runner_available);
    Ok(())
}

fn assert_paper_session_advanced(
    view: &crate::contracts::PaperSessionView,
    last_close: UtcTimestamp,
) {
    assert_eq!(view.state, PaperSessionState::Running);
    assert_eq!(view.markets[0].last_finalized_candle_at, Some(last_close));
    assert!(view.sequence > 0);
}

#[test]
fn paper_data_gap_is_session_local_and_runtime_keeps_typed_stop_available()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, _guard) = temp_root("paper-session-local-gap");
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let started_at = paper_test_time("2025-01-02T12:35:30Z");
    let boundary = crate::scheduling::latest_completed_boundary(started_at, CandleInterval::M1)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(async {
        let pre_start = paper_test_bars(
            "warmup",
            UtcTimestamp(boundary.0 - chrono::Duration::minutes(99)),
            99,
            CandleInterval::M1,
        );
        publish_paper_test_batch(&database, pre_start, boundary).await?;
        let local_only = UpbitClient::synthetic_local("http://127.0.0.1:9")?;
        let jobs = JobRuntime::start(database.clone(), local_only, root.clone(), None).await?;
        let service = jobs.service();
        let s1 =
            start_paper_test_session(&database, "s1-gap-session", StrategyKind::S1, started_at)
                .await?;
        let buy_hold = start_paper_test_session(
            &database,
            "buy-hold-gap-session",
            StrategyKind::BuyAndHold,
            started_at,
        )
        .await?;

        let gap_start = UtcTimestamp(boundary.0 + chrono::Duration::minutes(1));
        let post_start = vec![
            paper_test_bar("gap-first", gap_start, CandleInterval::M1, "100", "101"),
            paper_test_bar(
                "gap-second",
                UtcTimestamp(gap_start.0 + chrono::Duration::minutes(1)),
                CandleInterval::M1,
                "101",
                "102",
            ),
            paper_test_bar(
                "gap-third",
                UtcTimestamp(gap_start.0 + chrono::Duration::minutes(2)),
                CandleInterval::M1,
                "102",
                "103",
            ),
        ];
        let reconcile_at = post_start
            .last()
            .expect("post-start fixture")
            .candle
            .close_time_utc;
        let advanced = publish_paper_test_batch(&database, post_start, reconcile_at).await?;
        assert!(advanced >= 1);

        let s1_for_read = s1.clone();
        let buy_hold_for_read = buy_hold.clone();
        let (s1_view, buy_hold_view) = database
            .call("paper_test_read_gap_sessions", move |store| {
                Ok((
                    store.paper_get_session(&s1_for_read, reconcile_at)?.session,
                    store
                        .paper_get_session(&buy_hold_for_read, reconcile_at)?
                        .session,
                ))
            })
            .await?;
        assert_eq!(s1_view.state, PaperSessionState::RecoveryRequired);
        assert_paper_session_advanced(&buy_hold_view, reconcile_at);
        let s1_id_for_events = s1_view.session_id.clone();
        let s1_events = database
            .call("paper_test_read_gap_events", move |store| {
                store.paper_list_events(&s1_id_for_events, None, 100)
            })
            .await?;
        assert!(
            s1_events
                .items
                .iter()
                .any(|event| event.kind == PaperEventKind::RecoveryRequired)
        );

        assert_job_service_admitting(&service).await?;
        let stopped = service
            .paper_session_write(PaperSessionWrite::Stop {
                request_id: RequestId::new("stop-session-after-gap")?,
                session_id: s1_view.session_id.clone(),
                expected_sequence: s1_view.sequence,
            })
            .await?;
        assert_eq!(stopped.session.state, PaperSessionState::Stopped);
        assert_job_service_admitting(&service).await?;
        jobs.shutdown().await?;
        Ok::<(), LabError>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    result?;
    closed?;
    Ok(())
}

static TEST_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn temp_root(name: &str) -> (PathBuf, TempDirGuard) {
    let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("spot-lab-{name}-{}-{id}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create temp root");
    (path.clone(), TempDirGuard(path))
}

struct TempDirGuard(PathBuf);
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn wait_terminal_attempt(
    service: &JobService,
    id: JobId,
    min_attempts: usize,
) -> Result<JobRecord, LabError> {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let record = service.get(id.clone()).await?;
            if record
                .attempts
                .last()
                .is_some_and(|a| a.state.is_terminal())
                && record.attempts.len() >= min_attempts
            {
                return Ok::<_, LabError>(record);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| LabError::Internal("timeout waiting for terminal attempt".into()))?
}

#[test]
fn queued_job_cancellation_and_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let (root, _guard) = temp_root("queued-cancel");
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();

        // 1. Occupy the single running slot with a blocker claimed attempt
        let blocker_sub = JobSubmission {
            request_id: crate::contracts::RequestId::new("blocker-job-1")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run-blocker-1")?,
                market: None,
            },
        };
        let blocker = database
            .call("submit_blocker", move |store| {
                let job = store.submit_job(&blocker_sub, UtcTimestamp::now())?;
                let claimed = store.claim_next(UtcTimestamp::now())?;
                assert!(claimed.is_some());
                Ok::<_, LabError>(job)
            })
            .await?;

        // 2. Submit the target job - it must stay Queued because running == 1
        let submission = JobSubmission {
            request_id: crate::contracts::RequestId::new("queued-job-1")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run-1")?,
                market: None,
            },
        };
        let admitted = database
            .call("submit_queued", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;

        let mid_status = service.status().await?;
        assert_eq!(mid_status.queued_attempts, 1);
        assert_eq!(mid_status.running_attempts, 1);

        // 3. Cancel the queued job via service control
        let cancelled = service
            .control(JobControl::Cancel {
                job_id: admitted.id.clone(),
            })
            .await?;
        assert!(
            cancelled
                .attempts
                .last()
                .is_some_and(|a| matches!(a.state, AttemptState::Cancelled { .. }))
        );

        // 4. Release the blocker
        let blocker_attempt_id = blocker.attempts[0].id.clone();
        database
            .call("release_blocker", move |store| {
                store.finish_attempt(
                    &blocker_attempt_id,
                    AttemptState::Cancelled {
                        ended_at: UtcTimestamp::now(),
                        reason: "blocker done".into(),
                    },
                )
            })
            .await?;

        // 5. Verify runner status is completely recovered and admission is open
        let status = service.status().await?;
        assert!(status.admission_open);
        assert!(status.runner_available);
        assert_eq!(status.active_job_id, None);
        assert_eq!(status.queued_attempts, 0);
        assert_eq!(status.running_attempts, 0);

        // 6. Submit a new job through service and verify normal processing
        let next_submission = JobSubmission {
            request_id: crate::contracts::RequestId::new("next-job-after-cancel")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run-2")?,
                market: None,
            },
        };
        let next_job = service.submit(next_submission).await?;
        let observed = wait_terminal_attempt(&service, next_job.id, 1).await?;
        assert!(matches!(
            observed.attempts[0].state,
            AttemptState::Failed { .. }
        ));

        let final_status = service.status().await?;
        assert!(final_status.admission_open);
        assert!(final_status.runner_available);
        assert_eq!(final_status.active_job_id, None);

        jobs.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let _ = owner.shutdown();
    result
}

#[test]
fn status_self_heals_stale_active_when_running_attempts_is_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, _guard) = temp_root("status-heal");
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();

        // Deliberately pollute in-memory active with a phantom job ID
        let phantom_id = JobId::new("job-phantom-test")?;
        {
            let mut active = service
                .inner
                .active
                .lock()
                .map_err(|_| LabError::Internal("lock".into()))?;
            *active = Some((phantom_id.clone(), CancellationToken::new()));
        }

        // status() should detect that SQLite running_attempts is 0 and self-heal
        let status = service.status().await?;
        assert_eq!(status.active_job_id, None);
        assert_eq!(status.running_attempts, 0);

        // Verify in-memory lock is also cleaned
        {
            let active = service
                .inner
                .active
                .lock()
                .map_err(|_| LabError::Internal("lock".into()))?;
            assert!(active.is_none());
        }

        jobs.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let _ = owner.shutdown();
    result
}

#[test]
fn restart_recovers_interrupted_attempts() -> Result<(), Box<dyn std::error::Error>> {
    let (root, _guard) = temp_root("restart-recovery");
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // 1. Submit a job and claim it directly to put it into Running state in SQLite
    let submission = JobSubmission {
        request_id: crate::contracts::RequestId::new("unrecovered-job")?,
        payload: JobPayload::Export {
            run_id: RunId::new("missing-run-recovery")?,
            market: None,
        },
    };
    let unrecovered_id = runtime.block_on(async {
        let job = database
            .call("submit_unrecovered", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;
        let claimed = database
            .call("claim_unrecovered", |store| {
                store.claim_next(UtcTimestamp::now())
            })
            .await?;
        assert!(claimed.is_some());
        Ok::<_, LabError>(job.id)
    })?;

    // 2. Start a fresh JobRuntime - this must recover interrupted attempts
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();

        let status = service.status().await?;
        assert!(status.admission_open);
        assert!(status.runner_available);
        assert_eq!(status.active_job_id, None);
        assert_eq!(status.running_attempts, 0);

        let recovered_job = service.get(unrecovered_id).await?;
        assert!(recovered_job.attempts[0].state.is_terminal());

        jobs.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let _ = owner.shutdown();
    result
}

#[test]
fn cancelled_job_can_be_retried_and_processed() -> Result<(), Box<dyn std::error::Error>> {
    let (root, _guard) = temp_root("cancel-retry");
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();

        // 1. Blocker in running state
        let blocker_sub = JobSubmission {
            request_id: crate::contracts::RequestId::new("blocker-retry-job")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run-blocker-retry")?,
                market: None,
            },
        };
        let blocker = database
            .call("submit_blocker_retry", move |store| {
                let job = store.submit_job(&blocker_sub, UtcTimestamp::now())?;
                let claimed = store.claim_next(UtcTimestamp::now())?;
                assert!(claimed.is_some());
                Ok::<_, LabError>(job)
            })
            .await?;

        // 2. Target job submitted (queued)
        let submission = JobSubmission {
            request_id: crate::contracts::RequestId::new("retry-target-job")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run-retry")?,
                market: None,
            },
        };
        let job = database
            .call("submit_for_cancel", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;

        // 3. Cancel target job while queued
        let cancelled = service
            .control(JobControl::Cancel {
                job_id: job.id.clone(),
            })
            .await?;
        assert!(
            cancelled
                .attempts
                .last()
                .is_some_and(|a| matches!(a.state, AttemptState::Cancelled { .. }))
        );

        // 4. Release blocker
        let blocker_attempt_id = blocker.attempts[0].id.clone();
        database
            .call("release_blocker_retry", move |store| {
                store.finish_attempt(
                    &blocker_attempt_id,
                    AttemptState::Cancelled {
                        ended_at: UtcTimestamp::now(),
                        reason: "blocker done".into(),
                    },
                )
            })
            .await?;

        // 5. Retry target job via service control
        let retried = service
            .control(JobControl::Retry {
                job_id: job.id.clone(),
            })
            .await?;
        assert_eq!(retried.attempts.len(), 2);

        // 6. Wait for attempt 2 to finish
        let observed = wait_terminal_attempt(&service, job.id.clone(), 2).await?;
        assert!(matches!(
            observed.attempts[1].state,
            AttemptState::Failed { .. }
        ));

        let status = service.status().await?;
        assert!(status.admission_open);
        assert!(status.runner_available);
        assert_eq!(status.active_job_id, None);

        jobs.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let _ = owner.shutdown();
    result
}
