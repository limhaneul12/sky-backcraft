use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, CandleInterval, CausalExecutionPolicy, CostPolicy, CostSweep,
    EvidenceUnavailablePolicy, ExecutionPolicy, ExperimentSpec, FrozenPolicyRevision, MarketId,
    MarketRuleSnapshot, PitPolicy, PolicyDefinition, PolicyId, PolicyOrigin, PolicyProgram,
    PolicyRevisionId, PolicyRevisionRef, QuoteAmount, ReportClock, RequestId, ResearchDesign,
    ResearchSuiteRequest, RuleProvenance, RuleSnapshotId, StateParameters, StrategyKind,
    StrategySpec, TerminalPolicy, TickBand, Weight,
};
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-suite-store-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create test root");
        Self(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn v9_upgrade_and_backup_restore_preserve_suite_state() {
    let root = TestRoot::new("upgrade-source");
    let store = Store::open(&root.0).expect("open store");
    store
        .connection
        .execute_batch(
            "DROP TABLE managed_backups;
             DROP TABLE schedule_fires;
             DROP TABLE collection_schedules;
             DROP TABLE run_model_comparisons;
             DROP TABLE research_suite_cases;
             DROP TABLE research_suite_folds;
             DROP TABLE research_suites;
             DROP INDEX job_attempts_identity_owner;
             DELETE FROM schema_migrations WHERE version=10;",
        )
        .expect("simulate v9 database");
    store.close().expect("close v9 store");

    let mut upgraded = Store::open(&root.0).expect("upgrade v9 store");
    let (frozen, cases) = suite_fixture("upgrade");
    upgraded
        .create_research_suite(&frozen, &cases, time("2024-01-02T00:00:00Z"))
        .expect("create suite after upgrade");
    let failure = FailureRecord {
        code: "OWNER_BLOCKED".into(),
        message: "durable suite owner failure".into(),
    };
    let blocked = upgraded
        .block_research_suite(&frozen.id, &failure, time("2024-01-02T00:00:01Z"))
        .expect("block suite");
    assert_eq!(blocked.status, SuiteStatus::Blocked);
    assert_eq!(
        blocked.failure.as_ref().map(|value| value.code.as_str()),
        Some("OWNER_BLOCKED")
    );
    let backup = root.0.with_extension("backup");
    upgraded.backup(&backup).expect("backup suite store");
    upgraded.close().expect("close upgraded store");

    let restored_root = root.0.with_extension("restored");
    let restored = Store::restore(&backup, &restored_root).expect("restore suite backup");
    let record = restored
        .get_research_suite(&frozen.id)
        .expect("read restored suite")
        .expect("restored suite exists");
    assert_eq!(record.frozen.input_digest, frozen.input_digest);
    assert_eq!(record.status, SuiteStatus::Blocked);
    assert_eq!(
        record.failure.as_ref().map(|value| value.code.as_str()),
        Some("OWNER_BLOCKED")
    );
    assert_eq!(record.cases.len(), 1);
    restored.close().expect("close restored store");
    std::fs::remove_dir_all(&backup).expect("remove backup");
    std::fs::remove_dir_all(&restored_root).expect("remove restored root");
}

#[test]
fn terminal_failure_blocks_until_explicit_resume_marks_retry_pending() {
    let root = TestRoot::new("explicit-retry");
    let mut store = Store::open(&root.0).expect("open store");
    let (frozen, cases) = suite_fixture("explicit-retry");
    store
        .create_research_suite(&frozen, &cases, time("2024-01-02T00:00:00Z"))
        .expect("create suite");
    let (plan_id, plan_digest) = save_placeholder_plan(&store, "explicit-retry");
    let causal_digest = ContentHash::of_bytes(b"explicit-retry-causal");
    let submission = backtest_submission("explicit-retry", &plan_id, &plan_digest);
    store
        .admit_prepared_suite_case(
            &cases[0].id,
            &plan_id,
            &causal_digest,
            &submission,
            time("2024-01-02T00:00:01Z"),
        )
        .expect("admit child");
    let attempt = store
        .claim_next(time("2024-01-02T00:00:02Z"))
        .expect("claim child")
        .expect("running child");
    store
        .finish_attempt(
            &attempt.id,
            AttemptState::Failed {
                ended_at: time("2024-01-02T00:00:03Z"),
                error: FailureRecord {
                    code: "FAILED_EXECUTION".into(),
                    message: "synthetic terminal failure".into(),
                },
            },
        )
        .expect("finish failed child");
    let blocked = store
        .reconcile_research_suite(&frozen.id, time("2024-01-02T00:00:04Z"))
        .expect("reconcile terminal failure");
    assert_eq!(blocked.status, SuiteStatus::Blocked);
    assert_eq!(blocked.cases[0].status, SuiteCaseStatus::Failed);
    assert!(
        store
            .next_suite_case(&frozen.id)
            .expect("read blocked next case")
            .is_none()
    );

    let resumed = store
        .resume_research_suite(&frozen.id, time("2024-01-02T00:00:05Z"))
        .expect("explicit resume");
    assert_eq!(resumed.status, SuiteStatus::Running);
    assert_eq!(resumed.cases[0].status, SuiteCaseStatus::RetryPending);
    assert_eq!(
        store
            .next_suite_case(&frozen.id)
            .expect("read retry intent")
            .expect("retry-pending case")
            .status,
        SuiteCaseStatus::RetryPending
    );
}

#[test]
fn queue_full_rolls_back_child_job_and_case_link() {
    let root = TestRoot::new("queue-full");
    let mut store = Store::open(&root.0).expect("open store");
    let (frozen, cases) = suite_fixture("queue-full");
    store
        .create_research_suite(&frozen, &cases, time("2024-01-02T00:00:00Z"))
        .expect("create suite");
    let (plan_id, plan_digest) = save_placeholder_plan(&store, "queue-full");
    let causal_digest = ContentHash::of_bytes(b"queue-full-causal");
    assert_ne!(plan_digest, causal_digest);
    let mut occupying_jobs = Vec::new();
    for index in 0..8 {
        occupying_jobs.push(
            store
                .submit_job(
                    &collect_submission(&format!("occupy-{index}")),
                    time("2024-01-02T00:00:00Z"),
                )
                .expect("fill queue"),
        );
    }
    let submission = backtest_submission("suite-full", &plan_id, &plan_digest);
    assert!(matches!(
        store.admit_prepared_suite_case(
            &cases[0].id,
            &plan_id,
            &causal_digest,
            &submission,
            time("2024-01-02T00:00:01Z")
        ),
        Err(LabError::CapacityExceeded(_))
    ));
    let case = load_case(&store.connection, &cases[0].id).expect("load case");
    assert_eq!(case.status, SuiteCaseStatus::Planned);
    assert!(case.plan_id.is_none());
    assert!(case.causal_input_digest.is_none());
    assert!(case.job_id.is_none());
    let job_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE request_id=?1",
            [submission.request_id.as_str()],
            |row| row.get(0),
        )
        .expect("count rolled back job");
    assert_eq!(job_count, 0);
    store
        .request_cancel(&occupying_jobs[0].id, time("2024-01-02T00:00:02Z"))
        .expect("free one queue slot");
    store
        .admit_prepared_suite_case(
            &cases[0].id,
            &plan_id,
            &causal_digest,
            &submission,
            time("2024-01-02T00:00:03Z"),
        )
        .expect("retry stable prepared admission");
    let linked = load_case(&store.connection, &cases[0].id).expect("load linked case");
    assert_eq!(linked.status, SuiteCaseStatus::Queued);
    assert_eq!(linked.plan_id.as_ref(), Some(&plan_id));
    assert_eq!(linked.causal_input_digest.as_ref(), Some(&causal_digest));
}

#[test]
fn pause_resume_retry_repins_once_and_admission_readback_is_idempotent() {
    let root = TestRoot::new("pause-retry");
    let mut store = Store::open(&root.0).expect("open store");
    let (frozen, cases) = suite_fixture("pause-retry");
    store
        .create_research_suite(&frozen, &cases, time("2024-01-02T00:00:00Z"))
        .expect("create suite");
    let (plan_id, plan_digest) = save_placeholder_plan(&store, "pause-retry");
    let causal_digest = ContentHash::of_bytes(b"pause-retry-causal");
    assert_ne!(plan_digest, causal_digest);
    let submission = backtest_submission("suite-pause", &plan_id, &plan_digest);
    let first = store
        .admit_prepared_suite_case(
            &cases[0].id,
            &plan_id,
            &causal_digest,
            &submission,
            time("2024-01-02T00:00:01Z"),
        )
        .expect("admit child");
    let repeated = store
        .admit_prepared_suite_case(
            &cases[0].id,
            &plan_id,
            &causal_digest,
            &submission,
            time("2024-01-02T00:00:02Z"),
        )
        .expect("idempotent admission readback");
    assert_eq!(first.id, repeated.id);
    assert_eq!(repeated.attempts.len(), 1);
    assert!(matches!(
        store.request_cancel(&first.id, time("2024-01-02T00:00:02Z")),
        Err(LabError::Conflict(message)) if message.contains("managed child")
    ));
    assert!(matches!(
        store.retry_job(&first.id, time("2024-01-02T00:00:02Z")),
        Err(LabError::Conflict(message)) if message.contains("managed child")
    ));
    store
        .claim_next(time("2024-01-02T00:00:02Z"))
        .expect("claim child")
        .expect("running child");
    store
        .reconcile_research_suite(&frozen.id, time("2024-01-02T00:00:02Z"))
        .expect("project running child");

    let (paused, cancelled_jobs) = store
        .pause_research_suite(&frozen.id, time("2024-01-02T00:00:03Z"))
        .expect("pause suite");
    assert_eq!(paused.status, SuiteStatus::Paused);
    assert_eq!(cancelled_jobs, vec![first.id.clone()]);
    assert_eq!(paused.cases[0].status, SuiteCaseStatus::Running);
    let (paused_again, still_running) = store
        .pause_research_suite(&frozen.id, time("2024-01-02T00:00:03Z"))
        .expect("idempotent repeated pause");
    assert_eq!(paused_again.status, SuiteStatus::Paused);
    assert_eq!(still_running, vec![first.id.clone()]);
    assert_eq!(
        store
            .recover_interrupted(time("2024-01-02T00:00:03Z"))
            .expect("finish cancelled running attempt"),
        1
    );
    let paused_terminal = store
        .reconcile_research_suite(&frozen.id, time("2024-01-02T00:00:03Z"))
        .expect("project paused terminal child");
    assert_eq!(paused_terminal.status, SuiteStatus::Paused);
    assert_eq!(paused_terminal.cases[0].status, SuiteCaseStatus::Cancelled);
    let first_attempt = paused.cases[0]
        .attempt_id
        .clone()
        .expect("first attempt pin");

    let resumed = store
        .resume_research_suite(&frozen.id, time("2024-01-02T00:00:04Z"))
        .expect("resume suite");
    assert_eq!(resumed.cases[0].status, SuiteCaseStatus::RetryPending);
    let resumed_again = store
        .resume_research_suite(&frozen.id, time("2024-01-02T00:00:04Z"))
        .expect("idempotent repeated resume");
    assert_eq!(resumed_again.status, SuiteStatus::Running);
    let retried = store
        .retry_suite_case(&cases[0].id, time("2024-01-02T00:00:05Z"))
        .expect("retry suite case");
    assert_eq!(retried.attempts.len(), 2);
    assert_eq!(
        retried.attempts[0].state.status(),
        crate::contracts::JobStatus::Cancelled
    );
    let case = load_case(&store.connection, &cases[0].id).expect("load repinned case");
    assert_eq!(case.status, SuiteCaseStatus::Queued);
    assert_ne!(case.attempt_id.as_ref(), Some(&first_attempt));
    assert_eq!(case.attempt_id.as_ref(), Some(&retried.attempts[1].id));
}

fn suite_fixture(seed: &str) -> (FrozenResearchSuite, Vec<SuiteCase>) {
    let range =
        UtcRange::new(time("2024-01-01T00:00:00Z"), time("2024-01-01T02:00:00Z")).expect("range");
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero bps");
    let costs = CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "suite storage fixture".into(),
    };
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: format!("candidate {seed}"),
        description: "suite storage fixture".into(),
        program: PolicyProgram::Builtin {
            strategy: StrategySpec::S5 {
                state: StateParameters {
                    ema_length: 2,
                    vol_length: 2,
                    k: 0.0,
                },
            },
        },
    };
    let reference = PolicyRevisionRef {
        policy_id: PolicyId::new(format!("policy-{seed}")).expect("policy"),
        revision_id: PolicyRevisionId::new(format!("policy-{seed}-r1")).expect("revision"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    };
    let frozen_policy = FrozenPolicyRevision {
        reference: reference.clone(),
        revision_number: 1,
        parent_revision_id: None,
        family: StrategyKind::S5,
        origin: PolicyOrigin::User,
        definition,
    };
    let template = ExperimentSpec {
        schema_version: "3.0".into(),
        dataset_ids: vec![
            crate::contracts::DatasetId::new(format!("dataset-{seed}")).expect("dataset id"),
        ],
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range,
        strategies: Vec::new(),
        policy_selections: vec![reference],
        causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: QuoteAmount::new(Decimal::from(1_000)).expect("cash"),
        costs: costs.clone(),
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE).expect("weight"),
        },
        market_rules: MarketRuleSnapshot {
            id: RuleSnapshotId::new(format!("rules-{seed}")).expect("rules"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: range,
            observed_at: range.start(),
            source_refs: vec!["suite-storage-test".into()],
            assumption_label: "suite storage fixture".into(),
            min_notional: QuoteAmount::new(Decimal::ONE).expect("notional"),
            quantity_step: AssetQuantity::new(Decimal::ONE).expect("quantity"),
            ticks: vec![TickBand {
                lower_bound: QuoteAmount::new(Decimal::ZERO).expect("lower"),
                tick: crate::contracts::PriceKrw::new(Decimal::ONE).expect("tick"),
            }],
        },
        terminal_policy: TerminalPolicy::MarkToMarket,
        evidence_snapshot_id: None,
        pit_policy: PitPolicy::StrictPit,
        evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 1,
            risk_free_annual: 0.0,
        },
        seed: 1,
    };
    let request = ResearchSuiteRequest {
        request_id: RequestId::new(format!("suite-request-{seed}")).expect("request"),
        template,
        design: ResearchDesign::Batch,
        cost_sweep: CostSweep {
            fee_bps: vec![zero],
            slippage_bps: vec![zero],
        },
    };
    let frozen = crate::research::freeze(request, vec![frozen_policy]).expect("freeze suite");
    let cases = crate::research::initial_cases(&frozen).expect("initial cases");
    (frozen, cases)
}

fn save_placeholder_plan(store: &Store, seed: &str) -> (PlanId, ContentHash) {
    let id = PlanId::new(format!("plan-{seed}")).expect("plan id");
    let input_digest = ContentHash::of_bytes(format!("input-{seed}").as_bytes());
    store.connection.execute(
        "INSERT INTO plans(id,request_id,config_digest,input_digest,evidence_snapshot_id,original_request_json,resolved_plan_json) VALUES (?1,?2,?3,?4,NULL,'{}','{}')",
        params![id.as_str(), format!("plan-request-{seed}"), ContentHash::of_bytes(format!("config-{seed}").as_bytes()).as_str(), input_digest.as_str()],
    ).expect("insert placeholder plan");
    (id, input_digest)
}

fn backtest_submission(seed: &str, plan_id: &PlanId, digest: &ContentHash) -> JobSubmission {
    JobSubmission {
        request_id: RequestId::new(format!("job-{seed}")).expect("job request"),
        payload: JobPayload::Backtest {
            request: crate::contracts::RunRequest {
                request_id: RequestId::new(format!("run-{seed}")).expect("run request"),
                plan_id: plan_id.clone(),
                input_digest: digest.clone(),
            },
        },
    }
}

fn collect_submission(seed: &str) -> JobSubmission {
    JobSubmission {
        request_id: RequestId::new(format!("job-{seed}")).expect("job request"),
        payload: JobPayload::Collect {
            request: crate::contracts::CollectRequest {
                request_id: RequestId::new(format!("collect-{seed}")).expect("collect request"),
                markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
                range: UtcRange::new(time("2024-01-01T00:00:00Z"), time("2024-01-01T01:00:00Z"))
                    .expect("range"),
                data_resolution: CandleInterval::H1,
                warmup_bars: 0,
                completed_only: true,
            },
        },
    }
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("timestamp")
}
