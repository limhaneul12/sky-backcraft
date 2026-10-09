use super::*;
use crate::contracts::{
    AdmissionStatus, AssetQuantity, BasisPoints, CandleInterval, CandleObservation, CandleRecord,
    CausalExecutionPolicy, CollectRequest, CostSweep, DatasetManifest, DatasetSnapshot,
    DatasetStatus, EvidenceId, EvidenceImport, EvidenceProvenance, EvidencePurpose,
    EvidenceRevisionId, EvidenceUnavailablePolicy, EvidenceVersion, ExecutionPolicy,
    MarketDataOrigin, MarketId, MarketRuleSnapshot, MetricValue, ModelAdmission, ModelId,
    PitPolicy, PlanId, PolicyDefinition, PolicyId, PolicyOrigin, PolicyProgram, PolicyRevisionId,
    QuoteAmount, ReportClock, RequestId, ResolvedPlan, RuleProvenance, RuleSnapshotId,
    SignedAmount, StateParameters, StrategyKind, StrategySpec, SuiteStatus, TerminalPolicy,
    TickBand, UtcTimestamp, Weight, experiment_config_digest,
};
use chrono::Duration;
use rust_decimal::Decimal;

#[test]
fn geometry_expands_cartesian_scenarios_and_disjoint_rolling_folds() {
    let (request, policies) = fixture_request();
    let frozen = freeze(request, policies).expect("freeze suite");

    assert_eq!(frozen.scenarios.len(), 2);
    assert_eq!(frozen.folds.len(), 2);
    assert_eq!(frozen.planned_runs, 8);
    assert_eq!(frozen.planned_comparison_cells, 12);
    assert_eq!(frozen.estimated_events, 520);
    assert_eq!(
        frozen.folds[0].evaluation_range.end(),
        frozen.folds[1].evaluation_range.start()
    );
    assert!(frozen.folds[0].selection_range.end() < frozen.folds[0].evaluation_range.start());
    assert_eq!(
        frozen
            .unused_tail
            .expect("one-bar tail")
            .bars(CandleInterval::H1)
            .expect("tail bars"),
        1
    );
    assert_eq!(frozen.scenarios[0].buy_fee_bps.get(), Decimal::ZERO);
    assert_eq!(frozen.scenarios[1].buy_fee_bps.get(), Decimal::ONE);
    assert_eq!(frozen.scenarios[0].slippage_bps.get(), Decimal::new(2, 0));
}

#[test]
fn geometry_rejects_duplicate_axes_overlapping_evaluations_and_joint_cell_excess() {
    let (mut request, _) = fixture_request();
    request.cost_sweep.fee_bps.push(bps(0));
    assert!(matches!(
        expand_geometry(&request),
        Err(LabError::InvalidConfig(message)) if message.contains("duplicates")
    ));

    let (mut request, _) = fixture_request();
    request.design = ResearchDesign::WalkForward {
        selection_bars: 4,
        evaluation_bars: 3,
        step_bars: 2,
        embargo_bars: 0,
    };
    assert!(matches!(
        expand_geometry(&request),
        Err(LabError::InvalidConfig(message)) if message.contains("step")
    ));

    let (mut request, _) = fixture_request();
    request.template.markets = vec![market("KRW-BTC"), market("KRW-ETH"), market("KRW-XRP")];
    request.template.policy_selections = (0..7).map(policy_reference).collect();
    request.template.range = range("2024-01-01T00:00:00Z", "2024-01-03T12:00:00Z");
    request.template.market_rules.valid_range = request.template.range;
    request.cost_sweep.slippage_bps = vec![bps(0), bps(1)];
    request.design = ResearchDesign::WalkForward {
        selection_bars: 12,
        evaluation_bars: 4,
        step_bars: 4,
        embargo_bars: 0,
    };
    assert!(matches!(
        expand_geometry(&request),
        Err(LabError::ResourceLimit(message)) if message.contains("comparison cell")
            || message.contains("run count")
            || message.contains("fold")
    ));
}

#[test]
fn cases_are_stable_and_evaluation_uses_only_committed_winner() {
    let (request, policies) = fixture_request();
    let frozen = freeze(request, policies).expect("freeze suite");
    let cases = initial_cases(&frozen).expect("initial cases");
    assert_eq!(
        cases.len(),
        usize::try_from(frozen.planned_runs).expect("run count")
    );
    let repeated = initial_cases(&frozen).expect("stable cases");
    assert_eq!(
        cases.iter().map(|case| &case.id).collect::<Vec<_>>(),
        repeated.iter().map(|case| &case.id).collect::<Vec<_>>()
    );

    let now = time("2024-01-04T00:00:00Z");
    let mut record = SuiteRecord {
        frozen: frozen.clone(),
        status: SuiteStatus::Running,
        created_at: now,
        next_action_at: now,
        failure: None,
        folds: frozen.folds.clone(),
        cases,
    };
    let evaluation = record
        .cases
        .iter()
        .find(|case| case.fold_index == Some(0) && case.phase == SuitePhase::Evaluation)
        .expect("evaluation case")
        .clone();
    assert!(matches!(
        case_spec(&record, &evaluation),
        Err(LabError::Conflict(message)) if message.contains("winner")
    ));
    record.folds[0].winner = Some(frozen.request.template.policy_selections[1].clone());
    record.folds[0].selection_digest = Some(ContentHash::of_bytes(b"selection"));
    let spec = case_spec(&record, &evaluation).expect("evaluation spec");
    assert_eq!(
        spec.policy_selections,
        vec![record.folds[0].winner.clone().expect("winner")]
    );
    assert_eq!(spec.range, record.folds[0].evaluation_range);
    let expected_costs =
        &frozen.scenarios[usize::try_from(evaluation.scenario_index).expect("scenario")];
    assert_eq!(spec.costs.buy_fee_bps, expected_costs.buy_fee_bps);
    assert_eq!(spec.costs.slippage_bps, expected_costs.slippage_bps);
}

#[test]
fn selection_uses_exact_pnl_then_drawdown_then_lexical_identity() {
    let mut record = completed_record();
    let candidates = record.frozen.request.template.policy_selections.clone();
    let selection_cases = fold_selection_cases(&record, 0);
    let mut rows = Vec::new();
    for case in &selection_cases {
        rows.push(comparison(
            case.run_id.as_ref().expect("run"),
            &candidates[0],
            "100",
            0.10,
        ));
        rows.push(comparison(
            case.run_id.as_ref().expect("run"),
            &candidates[1],
            "100",
            0.10,
        ));
    }
    let first = select_winner(&record, 0, &rows).expect("select winner");
    assert_eq!(first.winner, Some(candidates[0].clone()));
    assert!(first.unavailable.is_empty());
    let again = select_winner(&record, 0, &rows).expect("deterministic selection");
    assert_eq!(first.selection_digest, again.selection_digest);

    for row in rows
        .iter_mut()
        .filter(|row| row.policy_ref.as_ref() == Some(&candidates[1]))
    {
        row.max_drawdown = MetricValue::value(0.05);
    }
    let lower_drawdown = select_winner(&record, 0, &rows).expect("drawdown tie break");
    assert_eq!(lower_drawdown.winner, Some(candidates[1].clone()));

    record.folds[0].winner = lower_drawdown.winner;
    record.folds[0].selection_digest = Some(lower_drawdown.selection_digest);
    let evaluation = record
        .cases
        .iter()
        .find(|case| case.fold_index == Some(0) && case.phase == SuitePhase::Evaluation)
        .expect("evaluation");
    assert_eq!(
        case_spec(&record, evaluation)
            .expect("winner-only evaluation")
            .policy_selections,
        vec![candidates[1].clone()]
    );
}

#[test]
fn selection_uses_exact_pnl_when_displayed_returns_are_equal() {
    let record = completed_record();
    let candidates = record.frozen.request.template.policy_selections.clone();
    let selection_cases = fold_selection_cases(&record, 0);
    let mut rows = Vec::new();
    for case in &selection_cases {
        let mut lower = comparison(case.run_id.as_ref().expect("run"), &candidates[0], "0", 0.1);
        let mut higher = comparison(
            case.run_id.as_ref().expect("run"),
            &candidates[1],
            "0.000000000000000001",
            0.1,
        );
        lower.net_return = MetricValue::value(0.0);
        higher.net_return = MetricValue::value(0.0);
        rows.extend([lower, higher]);
    }
    let outcome = select_winner(&record, 0, &rows).expect("exact PnL selection");
    assert_eq!(outcome.winner, Some(candidates[1].clone()));
}

#[test]
fn unavailable_candidate_is_explicit_and_never_scored_as_zero() {
    let record = completed_record();
    let candidates = record.frozen.request.template.policy_selections.clone();
    let selection_cases = fold_selection_cases(&record, 0);
    let rows = selection_cases
        .iter()
        .map(|case| {
            comparison(
                case.run_id.as_ref().expect("run"),
                &candidates[1],
                "-20",
                0.2,
            )
        })
        .collect::<Vec<_>>();
    let outcome = select_winner(&record, 0, &rows).expect("select available candidate");
    assert_eq!(outcome.winner, Some(candidates[1].clone()));
    assert_eq!(outcome.unavailable.len(), 1);
    assert_eq!(outcome.unavailable[0].candidate, candidates[0]);
    assert!(outcome.unavailable[0].reason.contains("missing"));
}

#[test]
fn summary_counts_terminal_incomplete_cases_and_only_committed_folds() {
    let mut record = completed_record();
    record.cases[0].status = SuiteCaseStatus::Failed;
    record.cases[1].status = SuiteCaseStatus::Blocked;
    record.folds[0].winner = Some(record.frozen.request.template.policy_selections[0].clone());
    record.folds[0].selection_digest = Some(ContentHash::of_bytes(b"winner"));
    let summary = summarize(&record);
    assert_eq!(summary.completed_runs, record.frozen.planned_runs - 2);
    assert_eq!(summary.blocked_or_failed_runs, 2);
    assert_eq!(summary.selected_folds.len(), 1);
}

#[test]
fn causal_digest_ignores_future_observation_perturbation_but_commits_used_window() {
    let (plan, mut dataset) = causal_fixture();
    let baseline = causal_input_digest(&plan, &[dataset.clone()], None).expect("causal digest");

    let future = dataset
        .observations
        .iter_mut()
        .find(|observation| observation.candle.open_time_utc > plan.spec.range.end())
        .expect("future observation");
    future.content_digest = ContentHash::of_bytes(b"changed future observation");
    let unchanged = causal_input_digest(&plan, &[dataset.clone()], None).expect("future digest");
    assert_eq!(baseline, unchanged);

    dataset
        .observations
        .retain(|observation| observation.candle.open_time_utc <= plan.spec.range.end());
    let future_gap = causal_input_digest(&plan, &[dataset.clone()], None).expect("future gap");
    assert_eq!(baseline, future_gap);

    let used = dataset
        .observations
        .iter_mut()
        .find(|observation| plan.spec.range.contains(observation.candle.open_time_utc))
        .expect("used observation");
    used.content_digest = ContentHash::of_bytes(b"changed used observation");
    let changed = causal_input_digest(&plan, &[dataset], None).expect("used digest");
    assert_ne!(baseline, changed);
}

#[test]
fn causal_digest_excludes_terminal_decision_but_keeps_terminal_execution_bar() {
    let (mut plan, mut dataset) = causal_fixture();
    plan.spec.range = range("2024-01-01T00:00:00Z", "2024-01-01T08:00:00Z");
    plan.spec.decision_interval = CandleInterval::H4;
    let h4_observations = vec![
        interval_observation(
            "decision-h4-warmup-1",
            CandleInterval::H4,
            "2023-12-31T12:00:00Z",
            "2023-12-31T16:00:00Z",
        ),
        interval_observation(
            "decision-h4-warmup-2",
            CandleInterval::H4,
            "2023-12-31T16:00:00Z",
            "2023-12-31T20:00:00Z",
        ),
        interval_observation(
            "decision-h4-warmup-3",
            CandleInterval::H4,
            "2023-12-31T20:00:00Z",
            "2024-01-01T00:00:00Z",
        ),
        interval_observation(
            "decision-h4-first",
            CandleInterval::H4,
            "2024-01-01T00:00:00Z",
            "2024-01-01T04:00:00Z",
        ),
        interval_observation(
            "decision-h4-terminal-unused",
            CandleInterval::H4,
            "2024-01-01T04:00:00Z",
            "2024-01-01T08:00:00Z",
        ),
    ];
    let h4_id = crate::contracts::DatasetId::new("causal-h4-dataset").expect("H4 dataset id");
    let mut h4_dataset = dataset.clone();
    h4_dataset.manifest.id = h4_id.clone();
    h4_dataset.manifest.request.request_id =
        RequestId::new("causal-h4-collection").expect("H4 collection");
    h4_dataset.manifest.request.data_resolution = CandleInterval::H4;
    let h4_coverage = range("2023-12-31T12:00:00Z", "2024-01-01T08:00:00Z");
    h4_dataset.manifest.request.range = h4_coverage;
    h4_dataset.manifest.coverage = h4_coverage;
    h4_dataset.manifest.row_count = 5;
    h4_dataset.manifest.semantic_digest = ContentHash::of_bytes(b"causal-h4-semantic");
    h4_dataset.manifest.provenance_digest = ContentHash::of_bytes(b"causal-h4-provenance");
    h4_dataset.observations = h4_observations;
    plan.spec.dataset_ids.push(h4_id.clone());
    plan.dataset_digests
        .push((h4_id, h4_dataset.manifest.semantic_digest.clone()));
    let baseline = causal_input_digest(&plan, &[dataset.clone(), h4_dataset.clone()], None)
        .expect("baseline digest");

    h4_dataset
        .observations
        .iter_mut()
        .find(|observation| observation.id.as_str().contains("terminal-unused"))
        .expect("terminal decision")
        .content_digest = ContentHash::of_bytes(b"unused terminal decision changed");
    let unused_changed = causal_input_digest(&plan, &[dataset.clone(), h4_dataset.clone()], None)
        .expect("unused decision digest");
    assert_eq!(baseline, unused_changed);

    dataset
        .observations
        .iter_mut()
        .find(|observation| {
            observation.candle.interval == CandleInterval::H1
                && observation.candle.close_time_utc == plan.spec.range.end()
        })
        .expect("terminal execution bar")
        .content_digest = ContentHash::of_bytes(b"terminal execution changed");
    let execution_changed = causal_input_digest(&plan, &[dataset, h4_dataset], None)
        .expect("terminal execution digest");
    assert_ne!(baseline, execution_changed);
}

#[test]
fn legacy_causal_digest_remains_the_original_plan_input_digest() {
    let (mut plan, _) = causal_fixture();
    plan.spec.schema_version = "2.0".into();
    plan.spec.causal_execution = None;
    let expected = ContentHash::of_bytes(b"legacy-plan-input");
    plan.input_digest = expected.clone();
    assert_eq!(
        causal_input_digest(&plan, &[], None).expect("legacy digest"),
        expected
    );
}

#[test]
fn causal_digest_binds_blocked_admission_reason_without_unused_feed_inputs() {
    let (mut plan, _) = causal_fixture();
    for admission in &mut plan.admissions {
        admission.status = AdmissionStatus::BlockedData;
        admission.reasons = vec!["DATA_GAP: frozen fixture".into()];
    }
    let baseline = causal_input_digest(&plan, &[], None)
        .expect("blocked models consume no observation stream");
    plan.admissions[0].reasons = vec!["DATA_GAP: different frozen reason".into()];
    let changed = causal_input_digest(&plan, &[], None)
        .expect("blocked reason remains part of causal identity");
    assert_ne!(baseline, changed);
}

#[test]
fn causal_digest_ignores_evidence_available_at_or_after_the_range_end() {
    let (mut plan, dataset) = causal_fixture();
    let base_version = evidence_version(
        "evidence-base",
        "evidence-event",
        "2023-12-31T23:00:00Z",
        "1",
        None,
    );
    let base = crate::evidence::build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions: vec![base_version.clone()],
    })
    .expect("base evidence");
    let with_future = crate::evidence::build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions: vec![
            base_version,
            evidence_version(
                "evidence-future",
                "evidence-event",
                "2024-01-02T04:00:00Z",
                "0.1",
                Some("evidence-base"),
            ),
        ],
    })
    .expect("future evidence");
    let at_boundary = crate::evidence::build_snapshot(EvidenceImport {
        public_non_sensitive_ack: true,
        versions: vec![
            evidence_version(
                "evidence-base",
                "evidence-event",
                "2023-12-31T23:00:00Z",
                "1",
                None,
            ),
            evidence_version(
                "evidence-boundary",
                "evidence-event",
                "2024-01-01T10:00:00Z",
                "0.2",
                Some("evidence-base"),
            ),
        ],
    })
    .expect("boundary evidence");

    plan.evidence_digest = Some(base.digest.clone());
    plan.spec.evidence_snapshot_id = Some(base.id.clone());
    let baseline = causal_input_digest(&plan, std::slice::from_ref(&dataset), Some(&base))
        .expect("base causal digest");
    plan.evidence_digest = Some(with_future.digest.clone());
    plan.spec.evidence_snapshot_id = Some(with_future.id.clone());
    let unchanged = causal_input_digest(&plan, std::slice::from_ref(&dataset), Some(&with_future))
        .expect("future causal digest");
    assert_eq!(baseline, unchanged);
    plan.evidence_digest = Some(at_boundary.digest.clone());
    plan.spec.evidence_snapshot_id = Some(at_boundary.id.clone());
    let boundary =
        causal_input_digest(&plan, &[dataset], Some(&at_boundary)).expect("boundary causal digest");
    assert_eq!(baseline, boundary);
}

#[test]
fn blocked_published_training_cases_make_all_unavailable_without_hiding_fold() {
    let mut record = completed_record();
    let candidates = record.frozen.request.template.policy_selections.clone();
    let mut rows = Vec::new();
    for case in record
        .cases
        .iter_mut()
        .filter(|case| case.fold_index == Some(0) && case.phase == SuitePhase::Selection)
    {
        case.status = SuiteCaseStatus::Blocked;
        for candidate in &candidates {
            let mut row = comparison(
                case.run_id.as_ref().expect("published blocked run"),
                candidate,
                "0",
                0.0,
            );
            row.status = ModelStatus::BlockedEvidence;
            row.initial_equity = None;
            row.final_equity = None;
            row.net_pnl = None;
            row.net_return = MetricValue::null(crate::contracts::NullReason::DataNotCaptured);
            row.max_drawdown = MetricValue::null(crate::contracts::NullReason::DataNotCaptured);
            rows.push(row);
        }
    }
    let outcome = select_winner(&record, 0, &rows).expect("blocked selection decision");
    assert!(outcome.winner.is_none());
    assert_eq!(outcome.unavailable.len(), candidates.len());
    assert!(
        outcome
            .unavailable
            .iter()
            .all(|candidate| candidate.reason.contains("BlockedEvidence"))
    );

    record.folds[0].selection_digest = Some(outcome.selection_digest);
    record.folds[0].unavailable_candidates = outcome.unavailable;
    let summary = summarize(&record);
    assert_eq!(summary.selected_folds.len(), 1);
    assert!(summary.selected_folds[0].winner.is_none());
    assert_eq!(summary.selected_folds[0].unavailable_candidates.len(), 2);
    assert!(
        summary.selected_folds[0].unavailable_candidates[0]
            .reason
            .contains("BlockedEvidence")
    );
}

fn completed_record() -> SuiteRecord {
    let (request, policies) = fixture_request();
    let frozen = freeze(request, policies).expect("freeze suite");
    let mut cases = initial_cases(&frozen).expect("cases");
    for case in &mut cases {
        case.status = SuiteCaseStatus::Completed;
        case.run_id = Some(RunId::from_seed(case.id.as_str()));
    }
    let now = time("2024-01-04T00:00:00Z");
    SuiteRecord {
        frozen: frozen.clone(),
        status: SuiteStatus::Running,
        created_at: now,
        next_action_at: now,
        failure: None,
        folds: frozen.folds.clone(),
        cases,
    }
}

fn fold_selection_cases(record: &SuiteRecord, index: u32) -> Vec<&SuiteCase> {
    record
        .cases
        .iter()
        .filter(|case| case.fold_index == Some(index) && case.phase == SuitePhase::Selection)
        .collect()
}

fn comparison(
    run_id: &RunId,
    policy: &PolicyRevisionRef,
    pnl: &str,
    drawdown: f64,
) -> RunModelComparison {
    let initial = quote("1000");
    let net_pnl = SignedAmount::new(pnl.parse().expect("decimal PnL")).expect("signed amount");
    let final_equity = QuoteAmount::new(initial.get() + net_pnl.get()).expect("final equity");
    RunModelComparison {
        run_id: run_id.clone(),
        model_id: crate::contracts::ModelId::from_seed(&format!("{run_id}:{policy:?}")),
        market: market("KRW-BTC"),
        policy_ref: Some(policy.clone()),
        status: ModelStatus::Completed,
        initial_equity: Some(initial),
        final_equity: Some(final_equity),
        net_pnl: Some(net_pnl),
        net_return: MetricValue::value(pnl.parse::<f64>().expect("f64 PnL") / 1000.0),
        max_drawdown: MetricValue::value(drawdown),
        turnover: MetricValue::value(1.0),
        cumulative_fees: Some(quote("1")),
        closed_episodes: Some(1),
        semantic_digest: ContentHash::of_bytes(format!("semantic:{run_id}").as_bytes()),
        causal_input_digest: ContentHash::of_bytes(format!("causal:{run_id}").as_bytes()),
    }
}

fn fixture_request() -> (ResearchSuiteRequest, Vec<FrozenPolicyRevision>) {
    let refs = vec![policy_reference(0), policy_reference(1)];
    let policies = refs
        .iter()
        .enumerate()
        .map(|(index, reference)| frozen_policy(index, reference.clone()))
        .collect();
    let full_range = range("2024-01-01T00:00:00Z", "2024-01-01T10:00:00Z");
    let zero = bps(0);
    let template = ExperimentSpec {
        schema_version: "3.0".into(),
        dataset_ids: vec![crate::contracts::DatasetId::new("dataset-suite").expect("dataset")],
        markets: vec![market("KRW-BTC")],
        range: full_range,
        strategies: Vec::new(),
        policy_selections: refs,
        causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
        capital_mode: None,
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: quote("1000"),
        costs: CostPolicy {
            buy_fee_bps: zero,
            sell_fee_bps: zero,
            maker_fee_bps: zero,
            half_spread_bps: zero,
            slippage_bps: zero,
            impact_bps: zero,
            assumption_label: "base cost assumptions".into(),
            dynamic: None,
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE).expect("weight"),
        },
        market_rules: MarketRuleSnapshot {
            id: RuleSnapshotId::new("suite-rules").expect("rules"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: full_range,
            observed_at: full_range.start(),
            source_refs: vec!["synthetic-suite-fixture".into()],
            assumption_label: "synthetic suite rules".into(),
            min_notional: quote("1"),
            quantity_step: AssetQuantity::new(Decimal::new(1, 2)).expect("quantity"),
            ticks: vec![TickBand {
                lower_bound: quote("0"),
                tick: crate::contracts::PriceKrw::new(Decimal::ONE).expect("tick"),
            }],
            fee_schedule: None,
            trading_state: None,
            maintenance_windows: Vec::new(),
        },
        market_rules_history: Vec::new(),
        terminal_policy: TerminalPolicy::MarkToMarket,
        evidence_snapshot_id: None,
        pit_policy: PitPolicy::StrictPit,
        evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 1,
            risk_free_annual: 0.0,
        },
        seed: 9,
    };
    (
        ResearchSuiteRequest {
            request_id: RequestId::new("research-suite-request").expect("request"),
            template,
            design: ResearchDesign::WalkForward {
                selection_bars: 4,
                evaluation_bars: 2,
                step_bars: 2,
                embargo_bars: 1,
            },
            cost_sweep: CostSweep {
                fee_bps: vec![bps(0), bps(1)],
                slippage_bps: vec![bps(2)],
            },
        },
        policies,
    )
}

fn causal_fixture() -> (ResolvedPlan, DatasetSnapshot) {
    let (request, policies) = fixture_request();
    let config_digest = experiment_config_digest(&request.template, &policies).expect("config");
    let market = request.template.markets[0].clone();
    let admissions = request
        .template
        .policy_selections
        .iter()
        .enumerate()
        .map(|(index, reference)| ModelAdmission {
            model_id: ModelId::from_seed(&format!("causal-model-{index}")),
            market: market.clone(),
            strategy: policies[index].family,
            policy_ref: Some(reference.clone()),
            status: AdmissionStatus::Eligible,
            reasons: Vec::new(),
        })
        .collect();
    let plan = ResolvedPlan {
        id: PlanId::new("causal-plan").expect("plan"),
        spec: request.template.clone(),
        config_digest,
        input_digest: ContentHash::of_bytes(b"full-plan-input"),
        dataset_digests: vec![(
            request.template.dataset_ids[0].clone(),
            ContentHash::of_bytes(b"full-dataset"),
        )],
        evidence_digest: None,
        admissions,
        policy_revisions: policies,
        warnings: Vec::new(),
        estimated_events: 1,
    };
    let coverage = range("2023-12-31T21:00:00Z", "2024-01-01T13:00:00Z");
    let observations = (0..16)
        .map(|index| {
            let open = UtcTimestamp(
                coverage
                    .start()
                    .0
                    .checked_add_signed(Duration::hours(index))
                    .expect("observation time"),
            );
            let close = UtcTimestamp(
                open.0
                    .checked_add_signed(Duration::hours(1))
                    .expect("close time"),
            );
            CandleObservation {
                id: crate::contracts::ObservationId::from_seed(&format!("causal-{index}")),
                candle: CandleRecord {
                    market: market.code(),
                    interval: CandleInterval::H1,
                    open_time_utc: open,
                    close_time_utc: close,
                    open: crate::contracts::PriceKrw::new(Decimal::from(100 + index))
                        .expect("open"),
                    high: crate::contracts::PriceKrw::new(Decimal::from(101 + index))
                        .expect("high"),
                    low: crate::contracts::PriceKrw::new(Decimal::from(99 + index)).expect("low"),
                    close: crate::contracts::PriceKrw::new(Decimal::from(100 + index))
                        .expect("close"),
                    volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
                    quote_turnover: quote("100"),
                    completed: true,
                },
                content_digest: ContentHash::of_bytes(format!("observation-{index}").as_bytes()),
                raw_object_ids: Vec::new(),
                constituent_ids: Vec::new(),
            }
        })
        .collect();
    let dataset = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: "1.0".into(),
            id: request.template.dataset_ids[0].clone(),
            request: CollectRequest {
                request_id: RequestId::new("causal-collection").expect("collection"),
                markets: vec![market],
                range: request.template.range,
                data_resolution: CandleInterval::H1,
                warmup_bars: 1,
                completed_only: true,
            },
            coverage,
            status: DatasetStatus::Ready,
            row_count: 16,
            normalizer_version: "test".into(),
            gap_policy: "test".into(),
            semantic_digest: ContentHash::of_bytes(b"semantic"),
            provenance_digest: ContentHash::of_bytes(b"provenance"),
            origin: MarketDataOrigin::SyntheticTestOnly,
            raw_objects: Vec::new(),
            quality_issues: Vec::new(),
            reuse: None,
        },
        observations,
    };
    (plan, dataset)
}

fn frozen_policy(index: usize, reference: PolicyRevisionRef) -> FrozenPolicyRevision {
    let definition = policy_definition(index);
    assert_eq!(
        reference.definition_digest,
        ContentHash::of_value(&definition).expect("definition digest")
    );
    FrozenPolicyRevision {
        reference,
        revision_number: 1,
        parent_revision_id: None,
        family: if index == 0 {
            StrategyKind::S5
        } else {
            StrategyKind::BuyAndHold
        },
        origin: PolicyOrigin::User,
        definition,
    }
}

fn policy_reference(index: usize) -> PolicyRevisionRef {
    let definition = policy_definition(index);
    PolicyRevisionRef {
        policy_id: PolicyId::new(format!("policy-{index}")).expect("policy ID"),
        revision_id: PolicyRevisionId::new(format!("policy-{index}-r1")).expect("revision ID"),
        definition_digest: ContentHash::of_value(&definition).expect("definition digest"),
    }
}

fn policy_definition(index: usize) -> PolicyDefinition {
    PolicyDefinition {
        schema_version: "1.0".into(),
        name: format!("candidate {index}"),
        description: "frozen candidate".into(),
        program: PolicyProgram::Builtin {
            strategy: if index == 0 {
                StrategySpec::S5 {
                    state: StateParameters {
                        ema_length: 2,
                        vol_length: 2,
                        k: 0.0,
                    },
                }
            } else {
                StrategySpec::BuyAndHold
            },
        },
    }
}

fn evidence_version(
    revision: &str,
    event: &str,
    available_at: &str,
    multiplier: &str,
    parent: Option<&str>,
) -> EvidenceVersion {
    let body = format!("synthetic body {revision}");
    let body_hash = ContentHash::of_bytes(body.as_bytes());
    let available_at = time(available_at);
    EvidenceVersion {
        evidence_id: EvidenceId::new(format!("evidence-{event}")).expect("evidence id"),
        revision_id: EvidenceRevisionId::new(revision).expect("revision id"),
        event_id: event.into(),
        purpose: EvidencePurpose::StrategyInput,
        category: "synthetic-boundary".into(),
        regime_label: None,
        markets: vec![market("KRW-BTC")],
        source_refs: vec!["https://example.invalid/synthetic".into()],
        body,
        body_hash: body_hash.clone(),
        event_time: Some(available_at),
        published_at: Some(available_at),
        first_seen_at: Some(available_at),
        content_updated_at: None,
        declared_available_at: available_at,
        registered_at: available_at,
        valid_until: time("2024-01-03T00:00:00Z"),
        supersedes_revision_id: parent
            .map(|id| EvidenceRevisionId::new(id).expect("parent revision id")),
        provenance: EvidenceProvenance::ForwardCaptured {
            captured_at: available_at,
            captured_body_hash: body_hash,
        },
        mapping_version: "synthetic-v1".into(),
        weight_multiplier: Weight::new(multiplier.parse().expect("multiplier"))
            .expect("evidence multiplier"),
    }
}

fn interval_observation(
    id: &str,
    interval: CandleInterval,
    open: &str,
    close: &str,
) -> CandleObservation {
    CandleObservation {
        id: crate::contracts::ObservationId::new(id).expect("observation id"),
        candle: CandleRecord {
            market: "KRW-BTC".into(),
            interval,
            open_time_utc: time(open),
            close_time_utc: time(close),
            open: crate::contracts::PriceKrw::new(Decimal::from(100)).expect("open"),
            high: crate::contracts::PriceKrw::new(Decimal::from(101)).expect("high"),
            low: crate::contracts::PriceKrw::new(Decimal::from(99)).expect("low"),
            close: crate::contracts::PriceKrw::new(Decimal::from(100)).expect("close"),
            volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
            quote_turnover: quote("100"),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(id.as_bytes()),
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    }
}

fn range(start: &str, end: &str) -> UtcRange {
    UtcRange::new(time(start), time(end)).expect("valid range")
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("valid timestamp")
}

fn market(value: &str) -> MarketId {
    MarketId::parse_upbit(value).expect("valid market")
}

fn bps(value: i64) -> BasisPoints {
    BasisPoints::new(Decimal::from(value)).expect("basis points")
}

fn quote(value: &str) -> QuoteAmount {
    QuoteAmount::new(value.parse().expect("decimal quote")).expect("quote amount")
}

/// Planner fixture with controllable geometry axes.
fn plan_request(
    markets: usize,
    candidates: usize,
    fee_count: usize,
    slip_count: usize,
    folds: Option<(u32, u32, u32, u32)>,
) -> ResearchSuiteRequest {
    let refs = (0..candidates).map(policy_reference).collect::<Vec<_>>();
    let full_range = range("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z");
    let zero = bps(0);
    let mut template_markets = Vec::new();
    for index in 0..markets {
        template_markets.push(market(&format!("KRW-M{index}")));
    }
    let template = ExperimentSpec {
        schema_version: "3.0".into(),
        dataset_ids: vec![crate::contracts::DatasetId::new("dataset-plan").expect("dataset")],
        markets: template_markets,
        range: full_range,
        strategies: Vec::new(),
        policy_selections: refs,
        causal_execution: Some(CausalExecutionPolicy::DeclaredPolicyWarmup),
        capital_mode: None,
        decision_interval: CandleInterval::H1,
        execution_resolution: CandleInterval::H1,
        latency_ms: 0,
        initial_cash: quote("1000"),
        costs: CostPolicy {
            buy_fee_bps: zero,
            sell_fee_bps: zero,
            maker_fee_bps: zero,
            half_spread_bps: zero,
            slippage_bps: zero,
            impact_bps: zero,
            assumption_label: "planner fixture".into(),
            dynamic: None,
        },
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE).expect("weight"),
        },
        market_rules: crate::contracts::MarketRuleSnapshot {
            id: RuleSnapshotId::new("planner-rules").expect("rules"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: full_range,
            observed_at: full_range.start(),
            source_refs: vec!["planner-fixture".into()],
            assumption_label: "planner fixture rules".into(),
            min_notional: quote("1"),
            quantity_step: AssetQuantity::new(Decimal::new(1, 2)).expect("quantity"),
            ticks: vec![crate::contracts::TickBand {
                lower_bound: quote("0"),
                tick: crate::contracts::PriceKrw::new(Decimal::ONE).expect("tick"),
            }],
            fee_schedule: None,
            trading_state: None,
            maintenance_windows: Vec::new(),
        },
        market_rules_history: Vec::new(),
        terminal_policy: TerminalPolicy::MarkToMarket,
        evidence_snapshot_id: None,
        pit_policy: PitPolicy::StrictPit,
        evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
        report_clock: ReportClock {
            timezone: "UTC".into(),
            min_annualization_days: 1,
            risk_free_annual: 0.0,
        },
        seed: 3,
    };
    ResearchSuiteRequest {
        request_id: RequestId::new("research-plan-request").expect("request"),
        template,
        design: match folds {
            None => ResearchDesign::Batch,
            Some((selection, evaluation, step, embargo)) => ResearchDesign::WalkForward {
                selection_bars: selection,
                evaluation_bars: evaluation,
                step_bars: step,
                embargo_bars: embargo,
            },
        },
        cost_sweep: CostSweep {
            fee_bps: (1_i64..=i64::try_from(fee_count).unwrap_or(i64::MAX))
                .map(bps)
                .collect(),
            slippage_bps: (10_i64..10_i64 + i64::try_from(slip_count).unwrap_or(i64::MAX))
                .map(bps)
                .collect(),
        },
    }
}

#[test]
fn planner_admits_the_exact_cell_limit_and_rejects_one_more_fold() {
    // cells = folds x scenarios x markets x (candidates + 1) = 8 x 2 x 2 x 8 = 256.
    let exact = plan_request(2, 7, 1, 2, Some((2, 2, 2, 0)));
    let report = plan_suite(&exact).expect("planner runs");
    assert_eq!(report.folds, 11, "24-hour range yields 11 folds of span 4");
    assert_eq!(report.cost_scenarios, 2);
    // 11 folds make 352 cells; the suite stays rejected.
    assert!(matches!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Rejected { .. }
    ));
    // One fold fewer keeps the suite inside the cell limit and matches create.
    let within = plan_request(2, 7, 1, 2, Some((2, 2, 2, 0)));
    let mut within = within;
    within.template.range = range("2024-01-01T00:00:00Z", "2024-01-01T20:00:00Z");
    let report = plan_suite(&within).expect("planner runs");
    assert_eq!(report.folds, 9);
    // 9 x 2 x 2 x 8 = 288 cells -> still rejected.
    assert!(matches!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Rejected { violations: _ }
    ));
    let limited = plan_request(2, 7, 1, 2, Some((2, 2, 2, 0)));
    let mut limited = limited;
    limited.template.range = range("2024-01-01T00:00:00Z", "2024-01-01T18:00:00Z");
    let report = plan_suite(&limited).expect("planner runs");
    assert_eq!(report.folds, 8);
    assert_eq!(report.comparison_cells, 256);
    assert_eq!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Admitted
    );
    // Planner/create consistency: the same request passes create geometry.
    expand_geometry(&limited).expect("create geometry admits the planned suite");
}

#[test]
fn planner_rejection_matches_create_rejection_with_numeric_violations() {
    let request = plan_request(3, 7, 1, 2, Some((2, 2, 2, 0)));
    let report = plan_suite(&request).expect("planner runs");
    let crate::contracts::SuitePlanAdmission::Rejected { violations } = &report.admission else {
        panic!("oversized suite must be rejected");
    };
    assert!(
        violations.iter().any(|violation| violation.item
            == crate::contracts::SuitePlanLimitKind::SuiteCells
            && violation.allowed == 256
            && violation.requested > 256),
        "cell violation carries the numeric excess: {violations:?}"
    );
    assert!(
        report
            .suggestions
            .iter()
            .any(|suggestion| suggestion.contains("split markets")),
        "market split is suggested: {:?}",
        report.suggestions
    );
    assert!(
        report
            .suggestions
            .iter()
            .any(|suggestion| suggestion.contains("warning")),
        "semantic warnings accompany meaning-changing splits"
    );
    assert!(expand_geometry(&request).is_err(), "create rejects too");
}

#[test]
fn planner_reports_invalid_templates_like_create_would() {
    let mut request = plan_request(1, 2, 1, 1, None);
    request.template.schema_version = "1.0".into();
    let report = plan_suite(&request).expect("planner runs");
    assert!(matches!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Invalid { .. }
    ));
    // Duplicate sweep axes are invalid, not a limit violation.
    let mut duplicate = plan_request(1, 2, 2, 1, None);
    duplicate.cost_sweep.fee_bps = vec![bps(1), bps(1)];
    let report = plan_suite(&duplicate).expect("planner runs");
    assert!(matches!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Invalid { .. }
    ));
}

#[test]
fn dynamic_cost_templates_reject_cost_sweeps_at_create_and_planner() {
    let request = plan_request(1, 2, 1, 1, None);
    let mut dynamic = request;
    dynamic.template.costs.dynamic = Some(crate::contracts::DynamicCostModel::VolatilityAware {
        base_slippage_bps: bps(5),
        range_weight: Decimal::from(1_000),
        max_slippage_bps: bps(100),
    });
    dynamic.template.costs.slippage_bps = bps(0);
    assert!(
        matches!(
            expand_geometry(&dynamic),
            Err(LabError::InvalidConfig(message))
            if message.contains("dynamic cost model")
        ),
        "create rejects a swept dynamic template"
    );
    let report = plan_suite(&dynamic).expect("planner runs");
    assert!(matches!(
        report.admission,
        crate::contracts::SuitePlanAdmission::Invalid { .. }
    ));
    // Single-scenario sweeps are still sweeps and stay rejected.
    let mut single = dynamic;
    single.cost_sweep = CostSweep {
        fee_bps: vec![bps(1)],
        slippage_bps: vec![bps(2)],
    };
    assert!(expand_geometry(&single).is_err());
}
