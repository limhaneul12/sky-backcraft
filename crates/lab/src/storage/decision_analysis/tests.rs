use super::*;
use crate::contracts::{
    DecisionAnalysisPageRequest, DecisionAnalysisQuery, DecisionAnalysisResponse,
    DecisionAnalysisSelection, DecisionAnalysisSource, DecisionAnalysisSourceSnapshot,
    DecisionAnalysisTotals,
};
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Result<Self, LabError> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| LabError::Internal(format!("system clock: {error}")))?
            .as_nanos();
        let sequence = TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "spot-lab-decision-analysis-{}-{nonce}-{sequence}",
            std::process::id(),
        ));
        std::fs::create_dir(&root)
            .map_err(|error| LabError::Internal(format!("create test root: {error}")))?;
        Ok(Self(root))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn overview(
    store: &mut Store,
    population: &DecisionAnalysisPopulation,
) -> Result<(DecisionAnalysisSourceSnapshot, DecisionAnalysisTotals), LabError> {
    let query = DecisionAnalysisQuery::Overview {
        population: population.clone(),
    };
    let snapshot = store.decision_analysis_snapshot(&query)?;
    match crate::reporting::decision_analysis::analyze(snapshot, query)? {
        DecisionAnalysisResponse::Overview {
            source_snapshot,
            totals,
            population_complete: true,
            ..
        } => Ok((source_snapshot, totals)),
        _ => Err(LabError::Internal(
            "decision-analysis overview returned another action".into(),
        )),
    }
}

fn selection(source_snapshot: &DecisionAnalysisSourceSnapshot) -> DecisionAnalysisSelection {
    source_snapshot.selection.clone()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one hand-counted query-only Store journey verifies the shared surface for both saved source families"
)]
fn saved_source_journeys_share_fences_complete_counts_and_read_only_store_state()
-> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    store
        .connection
        .pragma_update(None, "query_only", true)
        .map_err(super::super::sql_error)?;
    let before_changes = store.connection.total_changes();

    let (paper_snapshot, paper_totals) = overview(&mut store, &fixture.paper_population)?;
    assert_eq!(paper_snapshot.mode, DecisionAnalysisMode::Simulated);
    assert_eq!(paper_totals.decisions, 2);
    assert_eq!(paper_totals.decisions_with_time, paper_totals.decisions);
    assert_eq!(paper_totals.unplaced_decisions, 0);
    assert_eq!(paper_totals.strategy.buy, 1);
    assert_eq!(paper_totals.strategy.hold, 1);
    assert_eq!(paper_totals.constrained.hold, 2);
    assert_eq!(paper_totals.execution.no_order, 2);
    assert_eq!(paper_totals.orders, 0);
    assert_eq!(paper_totals.fills, 0);
    assert_eq!(paper_totals.condition_trace_capture.not_captured, 2);
    assert_eq!(paper_totals.non_decision_observations.len(), 2);
    assert!(paper_totals.non_decision_observations.iter().any(|row| {
        row.kind == "PROVIDER_RECOVERY"
            && row.scope
                == DecisionAnalysisObservationScope::Market {
                    market: crate::contracts::MarketId::parse_upbit("KRW-BTC")
                        .expect("fixture market"),
                }
    }));
    assert!(paper_totals.non_decision_observations.iter().any(|row| {
        row.kind == "STOP" && row.scope == DecisionAnalysisObservationScope::SessionWide
    }));
    assert!(
        !paper_totals
            .non_decision_observations
            .iter()
            .any(|row| row.kind == "DECISION_DEFERRED")
    );
    let mut all_markets_population = fixture.paper_population.clone();
    all_markets_population.market = None;
    let (_, all_markets_totals) = overview(&mut store, &all_markets_population)?;
    assert!(
        all_markets_totals
            .non_decision_observations
            .iter()
            .any(|row| {
                row.kind == "DECISION_DEFERRED"
                    && row.scope
                        == DecisionAnalysisObservationScope::Market {
                            market: crate::contracts::MarketId::parse_upbit("KRW-ETH")
                                .expect("fixture market"),
                        }
            })
    );
    assert_eq!(
        all_markets_totals
            .non_decision_observations
            .iter()
            .filter(|row| {
                row.kind == "PROVIDER_RECOVERY"
                    && matches!(row.scope, DecisionAnalysisObservationScope::Market { .. })
            })
            .count(),
        2
    );

    let paper_selection = selection(&paper_snapshot);
    let paper_page_query = DecisionAnalysisQuery::Decisions {
        selection: paper_selection.clone(),
        page: DecisionAnalysisPageRequest {
            offset: 0,
            limit: 1,
        },
        filter: Some(crate::contracts::DecisionAnalysisDecisionFilter {
            strategy_action: Some(crate::contracts::RecordedDecisionAction::Buy),
            constrained_action: Some(crate::contracts::RecordedDecisionAction::Hold),
            ..crate::contracts::DecisionAnalysisDecisionFilter::default()
        }),
    };
    let DecisionAnalysisResponse::Decisions {
        page: paper_page, ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&paper_page_query)?,
        paper_page_query,
    )?
    else {
        return Err(LabError::Internal(
            "paper list returned another action".into(),
        ));
    };
    assert_eq!(paper_page.total_count, 1);
    assert_eq!(paper_page.in_period_count, 1);
    assert_eq!(paper_page.unplaced_count, 0);
    assert_eq!(paper_page.returned_count, 1);
    assert_eq!(paper_page.items[0].decision_id, fixture.paper_previous);

    let paper_detail_query = DecisionAnalysisQuery::Decision {
        selection: paper_selection.clone(),
        decision_id: fixture.paper_selected.clone(),
    };
    let DecisionAnalysisResponse::Decision {
        decision: paper_detail,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&paper_detail_query)?,
        paper_detail_query,
    )?
    else {
        return Err(LabError::Internal(
            "paper detail returned another action".into(),
        ));
    };
    assert_eq!(paper_detail.row.decision_id, fixture.paper_selected);
    assert_eq!(
        paper_detail.explanation.condition_trace_capture,
        crate::contracts::SavedFactCaptureStatus::NotCaptured
    );

    let paper_compare_query = DecisionAnalysisQuery::Compare {
        selection: paper_selection.clone(),
        decision_id: fixture.paper_selected.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let DecisionAnalysisResponse::Compare {
        comparison: paper_comparison,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&paper_compare_query)?,
        paper_compare_query,
    )?
    else {
        return Err(LabError::Internal(
            "paper comparison returned another action".into(),
        ));
    };
    assert_eq!(
        paper_comparison.kind,
        crate::contracts::DecisionAnalysisComparisonKind::Previous
    );
    assert_eq!(
        paper_comparison
            .compared
            .as_ref()
            .map(|row| &row.row.decision_id),
        Some(&fixture.paper_previous)
    );
    assert!(paper_comparison.unavailable_reason.is_none());

    let paper_boundary_query = DecisionAnalysisQuery::Compare {
        selection: paper_selection,
        decision_id: fixture.paper_previous.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let DecisionAnalysisResponse::Compare {
        compared_source_snapshot: Some(paper_boundary_context),
        comparison: paper_boundary_comparison,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&paper_boundary_query)?,
        paper_boundary_query,
    )?
    else {
        return Err(LabError::Internal(
            "paper boundary comparison omitted predecessor context".into(),
        ));
    };
    assert_eq!(
        paper_boundary_comparison
            .compared
            .as_ref()
            .map(|record| &record.row.decision_id),
        Some(&fixture.paper_before_period)
    );
    assert_eq!(
        paper_boundary_context.selection.population.period.start,
        analysis_time("2026-10-14T00:30:00Z")?
    );
    assert_eq!(
        paper_boundary_context.selection.population.period.end,
        fixture.paper_population.period.start
    );
    assert_eq!(paper_boundary_context.mode, paper_snapshot.mode);
    assert_eq!(paper_boundary_context.policy_ref, paper_snapshot.policy_ref);
    assert_eq!(
        paper_boundary_context.selection.fence,
        paper_snapshot.selection.fence
    );

    let (automation_snapshot, automation_totals) =
        overview(&mut store, &fixture.automation_population)?;
    assert_eq!(automation_snapshot.mode, DecisionAnalysisMode::Mock);
    assert_eq!(automation_totals.decisions, 2);
    assert_eq!(
        automation_totals.decisions_with_time,
        automation_totals.decisions
    );
    assert_eq!(automation_totals.unplaced_decisions, 0);
    assert_eq!(automation_totals.strategy.buy, 1);
    assert_eq!(automation_totals.strategy.hold, 1);
    assert_eq!(automation_totals.execution.partially_filled, 1);
    assert_eq!(automation_totals.execution.no_order, 1);
    assert_eq!(automation_totals.orders, 1);
    assert_eq!(automation_totals.decisions_with_orders, 1);
    assert_eq!(automation_totals.fills, 1);
    assert_eq!(automation_totals.decisions_with_fills, 1);
    assert!(
        automation_totals
            .non_decision_observations
            .iter()
            .any(|row| {
                row.kind == "POLICY_WARMUP"
                    && row.observations == 1
                    && row.condition_trace_capture
                        == crate::contracts::SavedFactCaptureStatus::NotDecisionEvent
                    && row.scope
                        == DecisionAnalysisObservationScope::Market {
                            market: crate::contracts::MarketId::parse_upbit("KRW-BTC")
                                .expect("fixture market"),
                        }
            })
    );
    let automation_list_query = DecisionAnalysisQuery::Decisions {
        selection: selection(&automation_snapshot),
        page: DecisionAnalysisPageRequest {
            offset: 0,
            limit: 10,
        },
        filter: None,
    };
    let DecisionAnalysisResponse::Decisions {
        page: automation_page,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&automation_list_query)?,
        automation_list_query,
    )?
    else {
        return Err(LabError::Internal(
            "automation list returned another action".into(),
        ));
    };
    assert_eq!(automation_page.total_count, 2);
    assert_eq!(automation_page.returned_count, 2);
    assert_eq!(
        automation_page.items[0].decision_id,
        fixture.automation_previous
    );
    assert_eq!(
        automation_page.items[1].decision_id,
        fixture.automation_selected
    );

    let automation_selection = selection(&automation_snapshot);
    let automation_detail_query = DecisionAnalysisQuery::Decision {
        selection: automation_selection.clone(),
        decision_id: fixture.automation_previous.clone(),
    };
    let DecisionAnalysisResponse::Decision {
        decision: automation_detail,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&automation_detail_query)?,
        automation_detail_query,
    )?
    else {
        return Err(LabError::Internal(
            "automation detail returned another action".into(),
        ));
    };
    assert_eq!(automation_detail.explanation.orders.len(), 1);
    assert_eq!(automation_detail.explanation.fills.len(), 1);

    let automation_compare_query = DecisionAnalysisQuery::Compare {
        selection: automation_selection.clone(),
        decision_id: fixture.automation_selected.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let DecisionAnalysisResponse::Compare {
        comparison: automation_comparison,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&automation_compare_query)?,
        automation_compare_query,
    )?
    else {
        return Err(LabError::Internal(
            "automation comparison returned another action".into(),
        ));
    };
    assert_eq!(
        automation_comparison
            .compared
            .as_ref()
            .map(|row| &row.row.decision_id),
        Some(&fixture.automation_previous)
    );
    assert!(automation_comparison.unavailable_reason.is_none());

    let automation_boundary_query = DecisionAnalysisQuery::Compare {
        selection: automation_selection,
        decision_id: fixture.automation_previous.clone(),
        against: DecisionAnalysisComparisonTarget::Previous,
    };
    let DecisionAnalysisResponse::Compare {
        compared_source_snapshot: Some(automation_boundary_context),
        comparison: automation_boundary_comparison,
        ..
    } = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&automation_boundary_query)?,
        automation_boundary_query,
    )?
    else {
        return Err(LabError::Internal(
            "automation boundary comparison omitted predecessor context".into(),
        ));
    };
    assert_eq!(
        automation_boundary_comparison
            .compared
            .as_ref()
            .map(|record| &record.row.decision_id),
        Some(&fixture.automation_before_period)
    );
    assert_eq!(
        automation_boundary_context
            .selection
            .population
            .period
            .start,
        analysis_time("2026-10-14T00:00:00Z")?
    );
    assert_eq!(
        automation_boundary_context.selection.population.period.end,
        fixture.automation_population.period.start
    );
    assert_eq!(automation_boundary_context.mode, automation_snapshot.mode);
    assert_eq!(
        automation_boundary_context.policy_ref,
        automation_snapshot.policy_ref
    );
    assert_eq!(
        automation_boundary_context.selection.fence,
        automation_snapshot.selection.fence
    );

    assert_eq!(store.connection.total_changes(), before_changes);
    assert!(store.connection.is_autocommit());
    let query_only: i64 = store
        .connection
        .query_row("PRAGMA query_only", [], |row| row.get(0))
        .map_err(super::super::sql_error)?;
    assert_eq!(query_only, 1);
    store
        .connection
        .pragma_update(None, "query_only", false)
        .map_err(super::super::sql_error)?;
    store.close()?;
    Ok(())
}

#[test]
fn persisted_policy_warmup_with_linked_order_is_rejected() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let session_id = match &fixture.automation_population.source {
        DecisionAnalysisSource::Automation { session_id } => session_id.clone(),
        DecisionAnalysisSource::Paper { .. } => {
            return Err(LabError::Internal(
                "fixture source is not automation".into(),
            ));
        }
    };
    let warmup_id = AutomationIntentId::new("analysis-intent-warmup-default")?;
    seed_analysis_partial_order(&mut store, &session_id, &warmup_id)?;

    let query = DecisionAnalysisQuery::Overview {
        population: fixture.automation_population,
    };
    assert!(matches!(
        store.decision_analysis_snapshot(&query),
        Err(LabError::DataCorrupt(_))
    ));
    store.close()?;
    Ok(())
}

#[test]
fn warmup_uses_its_saved_event_clock_when_logical_time_precedes_period() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let session_id = match &fixture.automation_population.source {
        DecisionAnalysisSource::Automation { session_id } => session_id.clone(),
        DecisionAnalysisSource::Paper { .. } => {
            return Err(LabError::Internal(
                "fixture source is not automation".into(),
            ));
        }
    };
    let warmup_id = AutomationIntentId::new("analysis-intent-warmup-default")?;
    let period_start = analysis_time("2026-10-14T02:00:00Z")?;
    let event_time = analysis_time("2026-10-14T02:00:01Z")?;
    let changed = store
        .connection
        .execute(
            "UPDATE automation_session_events SET at_utc=?1 \
             WHERE session_id=?2 AND kind='SIGNAL_OBSERVED' \
               AND json_extract(detail_json,'$.intent_id')=?3",
            params![
                event_time.to_rfc3339(),
                session_id.as_str(),
                warmup_id.as_str()
            ],
        )
        .map_err(super::super::sql_error)?;
    assert_eq!(changed, 1);

    let persisted = store.automation_review_snapshot(&session_id)?;
    let warmup = persisted
        .decisions
        .iter()
        .find(|decision| decision.intent_id == warmup_id)
        .ok_or_else(|| LabError::DataCorrupt("saved warmup intent missing".into()))?;
    assert!(warmup.decision_time_utc < period_start);
    let mut population = fixture.automation_population;
    population.period.start = period_start;
    let (_, totals) = overview(&mut store, &population)?;
    assert_eq!(totals.decisions, 2);
    let warmup_observation = totals
        .non_decision_observations
        .iter()
        .find(|row| row.kind == "POLICY_WARMUP")
        .ok_or_else(|| LabError::Internal("in-period warmup event observation missing".into()))?;
    assert_eq!(warmup_observation.observations, 1);
    assert_eq!(
        warmup_observation.basis_time,
        DecisionAnalysisObservationClock::AutomationEventTime
    );
    store.close()?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixed-prefix append regression proves later event facts do not retarget a saved paper selection"
)]
fn paper_prefix_fence_ignores_later_event_appends() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let (before_snapshot, before_totals) = overview(&mut store, &fixture.paper_population)?;
    let old_selection = selection(&before_snapshot);
    let session_id = match &fixture.paper_population.source {
        DecisionAnalysisSource::Paper { session_id } => session_id.clone(),
        DecisionAnalysisSource::Automation { .. } => {
            return Err(LabError::Internal(
                "paper fixture has an automation source".into(),
            ));
        }
    };
    let as_of = match old_selection.fence {
        DecisionAnalysisFence::Paper { as_of_sequence } => as_of_sequence,
        DecisionAnalysisFence::Automation { .. } => {
            return Err(LabError::Internal(
                "paper fixture has an automation fence".into(),
            ));
        }
    };
    let event_time = analysis_time("2026-10-14T01:30:00Z")?;
    let market = crate::contracts::MarketId::parse_upbit("KRW-BTC")?;
    let future_sequence = as_of
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("paper test sequence overflow".into()))?;
    let future_event = crate::contracts::PaperSessionEvent {
        sequence: future_sequence,
        kind: PaperEventKind::CandleProcessed,
        market: Some(market.clone()),
        candle: None,
        source_event_at: Some(event_time),
        finalized_at: Some(event_time),
        control: None,
        provider_recovery: None,
        preparation: None,
        signals: vec![analysis_paper_signal(PaperSignalFixture {
            signal_id: crate::contracts::SignalId::new("analysis-signal-future")?,
            run_id: "analysis-run-future".into(),
            model_id: "analysis-model-future".into(),
            event_sequence: future_sequence,
            market,
            strategy: crate::contracts::StrategyKind::S1,
            policy_ref: fixture
                .paper_population
                .policy_ref
                .clone()
                .ok_or_else(|| LabError::Internal("paper test policy filter missing".into()))?,
            decision_time: event_time,
            raw_weight: Decimal::new(5, 1),
            constrained_weight: Decimal::new(1, 1),
            reason: crate::contracts::ReasonCode::BandEntry,
        })?],
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    };
    store
        .connection
        .execute(
            "INSERT INTO paper_session_events(session_id,sequence,market,kind,occurred_at_utc,event_json) \
             VALUES (?1,?2,'KRW-BTC','CANDLE_PROCESSED',?3,?4)",
            params![
                session_id.as_str(),
                i64::try_from(future_sequence).map_err(|_| {
                    LabError::ResourceLimit("paper test sequence exceeds SQLite range".into())
                })?,
                event_time.to_rfc3339(),
                serde_json::to_string(&future_event)?,
            ],
        )
        .map_err(super::super::sql_error)?;
    let mut session_view = store.paper_get_session(&session_id, event_time)?.session;
    session_view.sequence = future_sequence;
    session_view.event_count = future_sequence;
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET sequence=?1,event_count=?2,view_json=?3 WHERE session_id=?4",
            params![
                i64::try_from(future_sequence).map_err(|_| {
                    LabError::ResourceLimit("paper test sequence exceeds SQLite range".into())
                })?,
                i64::try_from(future_sequence).map_err(|_| {
                    LabError::ResourceLimit("paper test event count exceeds SQLite range".into())
                })?,
                serde_json::to_string(&session_view)?,
                session_id.as_str(),
            ],
        )
        .map_err(super::super::sql_error)?;

    let page_query = DecisionAnalysisQuery::Decisions {
        selection: old_selection,
        page: DecisionAnalysisPageRequest {
            offset: 0,
            limit: 10,
        },
        filter: None,
    };
    let DecisionAnalysisResponse::Decisions { page, .. } =
        crate::reporting::decision_analysis::analyze(
            store.decision_analysis_snapshot(&page_query)?,
            page_query,
        )?
    else {
        return Err(LabError::Internal(
            "paper follow-up returned another action".into(),
        ));
    };
    assert_eq!(page.in_period_count, before_totals.decisions);
    assert_eq!(page.total_count, before_totals.decisions);
    assert_eq!(page.total_count, 2);
    store.close()?;
    Ok(())
}

#[test]
fn paper_raw_event_bytes_admit_the_exact_ceiling_and_reject_the_next_byte() -> Result<(), LabError>
{
    let limit = MAX_DECISION_ANALYSIS_INPUT_BYTES;
    assert_eq!(admit_paper_event_json_bytes(limit - 1, 1)?, limit);
    assert!(matches!(
        admit_paper_event_json_bytes(limit, 1),
        Err(LabError::ResourceLimit(message)) if message.contains("paper stored event JSON bytes")
    ));
    assert!(matches!(
        admit_paper_event_json_bytes(usize::MAX, 1),
        Err(LabError::ResourceLimit(message)) if message.contains("byte count overflow")
    ));
    Ok(())
}

#[test]
fn automation_order_change_invalidates_the_saved_analysis_fence() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let (source_snapshot, _) = overview(&mut store, &fixture.automation_population)?;
    let updated_orders = store
        .connection
        .execute(
            "UPDATE automation_orders SET updated_at_utc=?1 WHERE client_identifier=?2",
            params![
                "2026-10-14T02:31:00Z",
                fixture.automation_client_identifier.as_str(),
            ],
        )
        .map_err(super::super::sql_error)?;
    assert_eq!(updated_orders, 1);
    let query = DecisionAnalysisQuery::Decision {
        selection: selection(&source_snapshot),
        decision_id: fixture.automation_previous.clone(),
    };
    let stale_order = store.decision_analysis_snapshot(&query);
    assert!(
        matches!(
            &stale_order,
            Err(LabError::InputHashMismatch(message)) if message.contains("automation selected")
        ),
        "order update should invalidate its saved snapshot fence, got {stale_order:?}"
    );

    let (fresh_snapshot, _) = overview(&mut store, &fixture.automation_population)?;
    let session_id = match &fixture.automation_population.source {
        DecisionAnalysisSource::Automation { session_id } => session_id.clone(),
        DecisionAnalysisSource::Paper { .. } => {
            return Err(LabError::Internal(
                "automation fixture has a paper source".into(),
            ));
        }
    };
    store
        .connection
        .execute(
            "INSERT INTO automation_fills(\
                 trade_uuid,client_identifier,session_id,market,side,price_krw,volume,funds_krw,\
                 trade_time_utc,source_digest,created_at_utc\
             ) VALUES ('analysis-late-fill',?1,?2,'KRW-BTC','BID','102','1','102',\
                       '2026-10-14T02:45:00Z',?3,'2026-10-14T02:45:01Z')",
            params![
                fixture.automation_client_identifier.as_str(),
                session_id.as_str(),
                ContentHash::of_bytes(b"decision-analysis-late-fill").as_str(),
            ],
        )
        .map_err(super::super::sql_error)?;
    let late_fill_query = DecisionAnalysisQuery::Decision {
        selection: selection(&fresh_snapshot),
        decision_id: fixture.automation_previous,
    };
    assert!(matches!(
        store.decision_analysis_snapshot(&late_fill_query),
        Err(LabError::InputHashMismatch(message)) if message.contains("automation selected")
    ));
    store.close()?;
    Ok(())
}

#[test]
fn source_family_fence_and_saved_policy_filter_fail_closed() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let (paper_snapshot, _) = overview(&mut store, &fixture.paper_population)?;
    let mut wrong_fence = selection(&paper_snapshot);
    wrong_fence.fence = DecisionAnalysisFence::Automation {
        snapshot_digest: ContentHash::of_bytes(b"wrong-family"),
    };
    let wrong_fence_query = DecisionAnalysisQuery::Decisions {
        selection: wrong_fence,
        page: DecisionAnalysisPageRequest {
            offset: 0,
            limit: 10,
        },
        filter: None,
    };
    assert!(matches!(
        wrong_fence_query.validate(),
        Err(LabError::InvalidConfig(_))
    ));

    let mut wrong_policy = fixture.paper_population.clone();
    let saved = wrong_policy
        .policy_ref
        .as_mut()
        .ok_or_else(|| LabError::Internal("fixture policy filter missing".into()))?;
    saved.revision_id = crate::contracts::PolicyRevisionId::new("other-saved-revision")?;
    let wrong_policy_query = DecisionAnalysisQuery::Overview {
        population: wrong_policy,
    };
    assert!(matches!(
        store.decision_analysis_snapshot(&wrong_policy_query),
        Err(LabError::InvalidConfig(_))
    ));
    store.close()?;
    Ok(())
}

#[test]
fn contradictory_saved_observation_market_evidence_fails_closed() -> Result<(), LabError> {
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let fixture = store.seed_decision_analysis_fixture()?;
    let btc = crate::contracts::MarketId::parse_upbit("KRW-BTC")?;
    let eth = crate::contracts::MarketId::parse_upbit("KRW-ETH")?;
    let at = analysis_time("2026-10-14T01:30:00Z")?;
    let deferred = crate::contracts::PaperSessionEvent {
        sequence: 9,
        kind: PaperEventKind::DecisionDeferred,
        market: Some(eth),
        candle: None,
        source_event_at: Some(at),
        finalized_at: Some(at),
        control: None,
        provider_recovery: None,
        preparation: Some(crate::contracts::DecisionPreparation {
            status: crate::contracts::DecisionPreparationStatus::AwaitingPolicySources,
            market: btc,
            interval: crate::contracts::CandleInterval::H1,
            observation_id: crate::contracts::ObservationId::new("mismatched-preparation-market")?,
            decision_time: at,
            completed_bars_seen: 0,
            required_bars: 1,
        }),
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    };
    assert!(matches!(
        paper_observations(&deferred),
        Err(LabError::DataCorrupt(_))
    ));

    let session_id = match &fixture.automation_population.source {
        DecisionAnalysisSource::Automation { session_id } => session_id.clone(),
        DecisionAnalysisSource::Paper { .. } => {
            return Err(LabError::Internal(
                "fixture source is not automation".into(),
            ));
        }
    };
    let snapshot = store.automation_review_snapshot(&session_id)?;
    let observation = snapshot
        .candles
        .first()
        .ok_or_else(|| LabError::DataCorrupt("automation fixture candle missing".into()))?;
    let event = crate::contracts::AutomationEvent {
        session_id,
        sequence: u64::MAX,
        kind: AutomationEventKind::CandleObserved,
        at_utc: at,
        detail: serde_json::json!({
            "market": "KRW-ETH",
            "observation_id": observation.id.as_str(),
        }),
    };
    assert!(matches!(
        automation_observation_scope(&event, &snapshot),
        Err(LabError::DataCorrupt(_))
    ));
    store.close()?;
    Ok(())
}

#[test]
fn selected_comparison_keeps_independent_live_source_fence_and_stale_side() -> Result<(), LabError>
{
    let root = TestRoot::new()?;
    let mut store = Store::open(&root.0)?;
    let selected_fixture = store.seed_decision_analysis_fixture()?;
    let compared_fixture = store.seed_decision_analysis_fixture_with_namespace("compared")?;
    let compared_session_id = match &compared_fixture.automation_population.source {
        DecisionAnalysisSource::Automation { session_id } => session_id.clone(),
        DecisionAnalysisSource::Paper { .. } => {
            return Err(LabError::Internal(
                "comparison fixture has a paper source".into(),
            ));
        }
    };
    let compared_account = format!("live:upbit:{}", "a".repeat(64));
    store
        .connection
        .execute(
            "UPDATE automation_sessions SET mode='LIVE',account_identity=?1 WHERE session_id=?2",
            params![compared_account, compared_session_id.as_str()],
        )
        .map_err(super::super::sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_orders SET account_identity=?1 WHERE session_id=?2",
            params![compared_account, compared_session_id.as_str()],
        )
        .map_err(super::super::sql_error)?;

    let (selected_snapshot, _) = overview(&mut store, &selected_fixture.automation_population)?;
    let (compared_snapshot, _) = overview(&mut store, &compared_fixture.automation_population)?;
    assert_eq!(compared_snapshot.mode, DecisionAnalysisMode::Live);
    let query = DecisionAnalysisQuery::Compare {
        selection: selection(&selected_snapshot),
        decision_id: selected_fixture.automation_selected.clone(),
        against: DecisionAnalysisComparisonTarget::Selected {
            selection: Box::new(selection(&compared_snapshot)),
            decision_id: compared_fixture.automation_previous.clone(),
        },
    };
    assert!(query.requires_trading_read());
    let response = crate::reporting::decision_analysis::analyze(
        store.decision_analysis_snapshot(&query)?,
        query.clone(),
    )?;
    let DecisionAnalysisResponse::Compare {
        compared_source_snapshot: Some(compared_source),
        comparison,
        ..
    } = response
    else {
        return Err(LabError::Internal(
            "selected comparison omitted its source".into(),
        ));
    };
    assert_eq!(compared_source.mode, DecisionAnalysisMode::Live);
    assert_eq!(
        source_session(&compared_source.selection.population.source),
        compared_session_id.as_str()
    );
    assert_eq!(
        comparison
            .compared
            .as_ref()
            .map(|record| &record.row.decision_id),
        Some(&compared_fixture.automation_previous)
    );
    assert!(
        comparison
            .changed_fields
            .contains(&crate::contracts::DecisionAnalysisChangedField::Mode)
    );
    assert!(
        comparison
            .changed_fields
            .contains(&crate::contracts::DecisionAnalysisChangedField::SessionId)
    );

    store
        .connection
        .execute(
            "UPDATE automation_orders SET updated_at_utc=?1 WHERE session_id=?2",
            params!["2026-10-14T02:31:00Z", compared_session_id.as_str()],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        store.decision_analysis_snapshot(&query),
        Err(LabError::InputHashMismatch(message)) if message.contains("automation compared")
    ));
    store.close()?;
    Ok(())
}

fn source_session(source: &DecisionAnalysisSource) -> &str {
    match source {
        DecisionAnalysisSource::Paper { session_id } => session_id.as_str(),
        DecisionAnalysisSource::Automation { session_id } => session_id.as_str(),
    }
}
