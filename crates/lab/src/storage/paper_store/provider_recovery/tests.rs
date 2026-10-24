//! Regression fixtures for same-session provider-gap recovery.
//!
//! All bars and raw pages below are deterministic synthetic test inputs. The
//! tests exercise the real Store, SQLite transactions, immutable raw objects,
//! and the ordinary paper write/reconciliation APIs; they never contact a
//! provider or use the user's live research root.

use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, CostPolicy, ExecutionPolicy, MarketDataOrigin, MarketId,
    ObservationId, PaperMarketRuleScenario, PaperMarketStart, PaperProviderRecoveryPhase,
    PaperSessionEvent, PaperSessionId, PaperSessionWrite, PaperVerificationStatus,
    PolicyRevisionRef, PriceKrw, QuoteAmount, RawObjectRef, RequestId, TickBand, Weight,
    observation_digest,
};
use crate::storage::{RawObjectInput, Store as TestStore};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

struct RecoveryTestRoot(PathBuf);

impl RecoveryTestRoot {
    fn new() -> std::io::Result<Self> {
        let serial = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-provider-recovery-{}-{serial}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&root)?;
        Ok(Self(root))
    }
}

impl Drop for RecoveryTestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn test_time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("valid deterministic fixture timestamp")
}

fn start_session(
    store: &mut TestStore,
    request_id: &str,
    started_at: UtcTimestamp,
    market_codes: &[&str],
) -> Result<(PaperSessionId, PolicyRevisionRef), LabError> {
    start_session_with_execution(
        store,
        request_id,
        started_at,
        market_codes,
        ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        0,
    )
}

fn start_session_with_execution(
    store: &mut TestStore,
    request_id: &str,
    started_at: UtcTimestamp,
    market_codes: &[&str],
    execution: ExecutionPolicy,
    latency_ms: u64,
) -> Result<(PaperSessionId, PolicyRevisionRef), LabError> {
    store
        .seed_builtin_policies_once(&crate::contracts::builtin_policy_definitions()?, started_at)?;
    let policy_ref = store
        .paper_start_options()?
        .policy_options
        .into_iter()
        .find(|option| option.reference.policy_id.as_str() == "builtin-buy-and-hold")
        .expect("builtin buy-and-hold fixture exists")
        .reference;
    let markets = market_codes
        .iter()
        .map(|code| {
            Ok(PaperMarketStart {
                market: MarketId::parse_upbit(code)?,
                initial_cash: QuoteAmount::new(Decimal::from(1_000_000))?,
                costs: zero_cost_policy(),
                market_rules: simple_market_rules(),
            })
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let write = PaperSessionWrite::Start {
        request_id: RequestId::new(request_id)?,
        policy_ref: policy_ref.clone(),
        decision_interval: CandleInterval::M5,
        execution_resolution: CandleInterval::M5,
        latency_ms,
        execution,
        markets,
    };
    let session_id = store.paper_write(&write, started_at)?.session.session_id;
    Ok((session_id, policy_ref))
}

fn zero_cost_policy() -> CostPolicy {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero fee fixture");
    CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "synthetic provider recovery test costs".into(),
        dynamic: None,
    }
}

fn simple_market_rules() -> PaperMarketRuleScenario {
    PaperMarketRuleScenario::SimulatedScenario {
        min_notional: QuoteAmount::new(Decimal::from(10)).expect("min notional fixture"),
        quantity_step: AssetQuantity::new(Decimal::new(1, 1)).expect("quantity fixture"),
        ticks: vec![TickBand {
            lower_bound: QuoteAmount::new(Decimal::ZERO).expect("zero lower bound"),
            tick: PriceKrw::new(Decimal::ONE).expect("unit price tick"),
        }],
        assumption_label: "SIMULATED_SCENARIO provider recovery fixture".into(),
    }
}

fn publish_page(
    store: &mut TestStore,
    market: &MarketId,
    interval: CandleInterval,
    opens: &[UtcTimestamp],
    fetched_at: UtcTimestamp,
) -> Result<(RawObjectRef, Vec<CandleObservation>), LabError> {
    let records = opens
        .iter()
        .rev()
        .map(|open| {
            serde_json::json!({
                "market": market.code(),
                "candle_date_time_utc": open.0.format("%Y-%m-%dT%H:%M:%S").to_string(),
                "opening_price": 100,
                "high_price": 101,
                "low_price": 99,
                "trade_price": 100,
                "candle_acc_trade_volume": 1,
                "candle_acc_trade_price": 100,
            })
        })
        .collect::<Vec<_>>();
    let endpoint = match interval {
        CandleInterval::M5 => "/v1/candles/minutes/5",
        CandleInterval::H1 => "/v1/candles/minutes/60",
        _ => return Err(LabError::InvalidConfig("unsupported test interval".into())),
    };
    let published = store.publish_raw(RawObjectInput {
        source_url: format!(
            "https://api.upbit.com{endpoint}?market={}&count=200",
            market.code()
        ),
        fetched_at,
        persisted_at: fetched_at,
        http_status: 200,
        remaining_req: None,
        origin: MarketDataOrigin::ExchangeObserved,
        body: serde_json::to_vec(&records).map_err(|error| {
            LabError::InvalidConfig(format!("fixture JSON encoding failed: {error}"))
        })?,
    })?;
    store.catalog_raw(&published.object)?;
    let mut observations =
        crate::market_data::decode_candles(&published.body, market, interval, fetched_at)?
            .into_iter()
            .rev()
            .map(|candle| {
                let content_digest = observation_digest(&candle)?;
                Ok(CandleObservation {
                    id: ObservationId::from_seed(content_digest.as_str()),
                    candle,
                    content_digest,
                    raw_object_ids: vec![published.object.id.clone()],
                    constituent_ids: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
    observations.sort_by_key(|observation| observation.candle.open_time_utc);
    Ok((published.object, observations))
}

fn fetched_after_close(open: UtcTimestamp, interval: CandleInterval) -> UtcTimestamp {
    UtcTimestamp(open.0 + interval.duration() + chrono::Duration::seconds(1))
}

fn event_json_prefix(
    store: &TestStore,
    session_id: &PaperSessionId,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut statement = store.connection.prepare(
        "SELECT event_json FROM paper_session_events WHERE session_id=?1 ORDER BY sequence",
    )?;
    let events = statement
        .query_map([session_id.as_str()], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(events)
}

fn parsed_events(values: &[String]) -> Result<Vec<PaperSessionEvent>, serde_json::Error> {
    values
        .iter()
        .map(|value| serde_json::from_str(value))
        .collect()
}

fn assert_no_recovery_trades(events: &[PaperSessionEvent]) {
    assert!(events.iter().all(|event| {
        event.signals.is_empty()
            && event.orders.is_empty()
            && event.order_events.is_empty()
            && event.fills.is_empty()
    }));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle regression proves bounded continuation, restart durability, ordinary same-session processing, and consumed-proof invalidation"
)]
fn large_provider_gap_continues_same_session_after_restart_and_rechecks_consumed_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let started_at = test_time("2025-01-02T10:00:00Z");
    let (session_id, policy_ref) = start_session(
        &mut store,
        "large-provider-gap-start",
        started_at,
        &["KRW-BTC"],
    )?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let interval = CandleInterval::M5;
    let anchor_opens = [
        test_time("2025-01-02T10:00:00Z"),
        test_time("2025-01-02T11:00:00Z"),
    ];
    let first_read_at = test_time("2025-01-02T11:05:01Z");
    let second_read_at = test_time("2025-01-02T11:05:02Z");
    let (first_raw, first_rows) =
        publish_page(&mut store, &market, interval, &anchor_opens, first_read_at)?;
    store.step_paper_session_bars(
        &session_id,
        &market,
        &first_rows[..1],
        &BTreeMap::new(),
        first_read_at,
    )?;
    let original_events = event_json_prefix(&store, &session_id)?;
    let original_session = store.paper_get_session(&session_id, first_read_at)?.session;
    let original_checkpoints = store.paper_checkpoint_digests_for_test(&session_id)?;
    let first_confirmation = store.observe_provider_absence_snapshot(
        &first_raw,
        &market,
        interval,
        &first_rows,
        first_read_at,
    )?;
    assert!(first_confirmation.confirmed_spans.is_empty());
    assert!(first_confirmation.pending_confirmation);

    let (second_raw, second_rows) =
        publish_page(&mut store, &market, interval, &anchor_opens, second_read_at)?;
    let confirmed = store.observe_provider_absence_snapshot(
        &second_raw,
        &market,
        interval,
        &second_rows,
        second_read_at,
    )?;
    assert_eq!(confirmed.confirmed_spans.len(), 1);
    let proof_id = confirmed.confirmed_spans[0].proof_id.clone();

    // The proof page's 11:00 anchor plus 200 subsequent real fixture candles
    // exceeds the 199-bar per-tick recovery bound by two decision bars.
    let post_gap_count = usize::try_from(crate::paper::MAX_PAPER_RECOVERY_BARS)? + 1;
    let post_gap_opens = (0..post_gap_count)
        .map(|index| {
            UtcTimestamp(
                anchor_opens[1].0
                    + interval.duration()
                    + chrono::Duration::minutes(5 * i64::try_from(index).unwrap()),
            )
        })
        .collect::<Vec<_>>();
    let mut first_recovery_page_opens = vec![anchor_opens[1]];
    first_recovery_page_opens.extend_from_slice(&post_gap_opens[..post_gap_count - 1]);
    let (post_gap_first_raw, post_gap_first_rows) = publish_page(
        &mut store,
        &market,
        interval,
        &first_recovery_page_opens,
        fetched_after_close(
            *first_recovery_page_opens.last().expect("page is nonempty"),
            interval,
        ),
    )?;
    let final_open = *post_gap_opens.last().expect("post-gap page is nonempty");
    let (post_gap_last_raw, post_gap_last_rows) = publish_page(
        &mut store,
        &market,
        interval,
        &[final_open],
        fetched_after_close(final_open, interval),
    )?;
    let recovery_target = UtcTimestamp(final_open.0 + interval.duration());
    let recovery_now = fetched_after_close(final_open, interval);
    let batches = vec![
        (second_raw.clone(), second_rows.clone()),
        (post_gap_first_raw, post_gap_first_rows),
        (post_gap_last_raw, post_gap_last_rows),
    ];

    assert_eq!(
        store.reconcile_running_paper_sessions(&batches, recovery_now)?,
        1,
        "the first bounded recovery chunk must durably advance the original session"
    );
    let progressed = store.paper_get_session(&session_id, recovery_now)?.session;
    assert_eq!(progressed.session_id, session_id);
    assert_eq!(progressed.policy_ref, policy_ref);
    assert_eq!(progressed.state, PaperSessionState::RecoveryRequired);
    assert_eq!(
        progressed.recovery_cause,
        Some(PaperRecoveryCause::ProviderGap)
    );
    assert_eq!(
        event_json_prefix(&store, &session_id)?[..original_events.len()],
        original_events
    );
    assert_ne!(
        store.paper_checkpoint_digests_for_test(&session_id)?,
        original_checkpoints,
        "the first chunk must preserve its progress in the existing checkpoint"
    );
    let progress_json = event_json_prefix(&store, &session_id)?;
    let progress_events = parsed_events(&progress_json[original_events.len()..])?;
    assert_eq!(
        progress_events
            .last()
            .and_then(|event| event.provider_recovery.as_ref())
            .map(|recovery| recovery.phase),
        Some(PaperProviderRecoveryPhase::Progress)
    );
    assert_no_recovery_trades(&progress_events);

    drop(store);
    let mut reopened = TestStore::open(&root.0)?;
    assert_eq!(
        reopened.reconcile_running_paper_sessions(&[], recovery_now)?,
        1,
        "a new Store owner must continue the committed cursor and finish the remaining rows"
    );
    let completed = reopened
        .paper_get_session(&session_id, recovery_now)?
        .session;
    assert_eq!(completed.session_id, session_id);
    assert_eq!(completed.policy_ref, policy_ref);
    assert_eq!(completed.state, PaperSessionState::Running);
    assert_eq!(completed.recovery_cause, None);
    assert_eq!(
        completed.markets[0].last_finalized_candle_at,
        Some(recovery_target)
    );
    assert_eq!(
        completed.markets[0].verification.status,
        PaperVerificationStatus::Pass
    );
    assert!(
        completed
            .markets
            .iter()
            .zip(&original_session.markets)
            .all(|(after, before)| {
                after.cash_total == before.cash_total
                    && after.quantity == before.quantity
                    && after.fill_count == before.fill_count
                    && after.signal_count == before.signal_count
            })
    );
    let completed_json = event_json_prefix(&reopened, &session_id)?;
    assert_eq!(
        &completed_json[..original_events.len()],
        original_events.as_slice()
    );
    let recovered_events = parsed_events(&completed_json[original_events.len()..])?;
    assert_no_recovery_trades(&recovered_events);
    assert!(recovered_events.iter().all(|event| {
        event
            .provider_recovery
            .as_ref()
            .is_some_and(|recovery| recovery.proof_ids.contains(&proof_id))
    }));
    assert_eq!(
        recovered_events
            .last()
            .and_then(|event| event.provider_recovery.as_ref())
            .map(|recovery| recovery.phase),
        Some(PaperProviderRecoveryPhase::Completed)
    );

    // A fresh, contiguous candle takes the ordinary RUNNING path in the same session.
    let next_open = recovery_target;
    let next_fetched = fetched_after_close(next_open, interval);
    let (next_raw, next_rows) =
        publish_page(&mut reopened, &market, interval, &[next_open], next_fetched)?;
    let _ = next_raw;
    let ordinary = reopened.step_paper_session_bars(
        &session_id,
        &market,
        &next_rows,
        &BTreeMap::new(),
        next_fetched,
    )?;
    assert_eq!(ordinary.session_id, session_id);
    assert_eq!(ordinary.policy_ref, policy_ref);
    assert_eq!(ordinary.state, PaperSessionState::Running);
    assert_eq!(
        ordinary.markets[0].last_finalized_candle_at,
        Some(UtcTimestamp(next_open.0 + interval.duration()))
    );
    let ordinary_event_json = event_json_prefix(&reopened, &session_id)?;
    assert!(ordinary_event_json.len() > completed_json.len());

    // A normal collected observation (without the live REST proof observer)
    // is also durable negative evidence for a consumed absence proof.
    let late_open = test_time("2025-01-02T10:05:00Z");
    let late_fetched = UtcTimestamp(next_fetched.0 + chrono::Duration::seconds(1));
    let (late_raw, late_rows) =
        publish_page(&mut reopened, &market, interval, &[late_open], late_fetched)?;
    reopened
        .reconcile_running_paper_sessions(&[(late_raw.clone(), late_rows.clone())], late_fetched)?;
    let conflicts: u32 = reopened.connection.query_row(
        "SELECT count(*) FROM provider_absence_conflicts WHERE proof_id=?1",
        [proof_id.as_str()],
        |row| row.get(0),
    )?;
    assert!(
        conflicts > 0,
        "ordinary ingestion must retain the contradiction durably"
    );
    let held = reopened
        .paper_get_session(&session_id, late_fetched)?
        .session;
    assert_eq!(held.session_id, session_id);
    assert_eq!(held.policy_ref, policy_ref);
    assert_eq!(held.state, PaperSessionState::RecoveryRequired);
    assert_eq!(held.recovery_cause, Some(PaperRecoveryCause::ProviderGap));
    assert_eq!(
        held.markets[0].fill_count, ordinary.markets[0].fill_count,
        "proof contradiction must not add a fill"
    );
    let held_json = event_json_prefix(&reopened, &session_id)?;
    assert_eq!(
        &held_json[..ordinary_event_json.len()],
        ordinary_event_json.as_slice()
    );
    let held_events = parsed_events(&held_json[ordinary_event_json.len()..])?;
    assert!(held_events.iter().all(|event| event.fills.is_empty()));
    let before_rejected_tick = event_json_prefix(&reopened, &session_id)?;
    assert!(matches!(
        reopened.step_paper_session_bars(
            &session_id,
            &market,
            &late_rows,
            &BTreeMap::new(),
            late_fetched,
        ),
        Err(LabError::Conflict(_))
    ));
    assert_eq!(
        event_json_prefix(&reopened, &session_id)?,
        before_rejected_tick
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one multi-market regression proves all-or-nothing evidence gating and SQLite rollback after checkpoint staging"
)]
fn multi_market_recovery_waits_for_all_proofs_and_rolls_back_event_failure_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let started_at = test_time("2025-02-03T10:00:00Z");
    let (session_id, policy_ref) = start_session(
        &mut store,
        "multi-market-provider-gap-start",
        started_at,
        &["KRW-BTC", "KRW-XRP"],
    )?;
    let interval = CandleInterval::M5;
    let btc = MarketId::parse_upbit("KRW-BTC")?;
    let xrp = MarketId::parse_upbit("KRW-XRP")?;
    let anchor_opens = [
        test_time("2025-02-03T10:00:00Z"),
        test_time("2025-02-03T11:00:00Z"),
    ];
    let first_read_at = test_time("2025-02-03T11:05:01Z");
    let second_read_at = test_time("2025-02-03T11:05:02Z");
    let (btc_first_raw, btc_first_rows) =
        publish_page(&mut store, &btc, interval, &anchor_opens, first_read_at)?;
    let (xrp_first_raw, xrp_first_rows) =
        publish_page(&mut store, &xrp, interval, &anchor_opens, first_read_at)?;
    for (market, rows) in [(&btc, &btc_first_rows), (&xrp, &xrp_first_rows)] {
        store.step_paper_session_bars(
            &session_id,
            market,
            &rows[..1],
            &BTreeMap::new(),
            first_read_at,
        )?;
    }
    let original_events = event_json_prefix(&store, &session_id)?;
    let original_session = store.paper_get_session(&session_id, first_read_at)?.session;
    let original_checkpoints = store.paper_checkpoint_digests_for_test(&session_id)?;
    assert!(
        store
            .observe_provider_absence_snapshot(
                &btc_first_raw,
                &btc,
                interval,
                &btc_first_rows,
                first_read_at,
            )?
            .confirmed_spans
            .is_empty()
    );
    let xrp_pending = store.observe_provider_absence_snapshot(
        &xrp_first_raw,
        &xrp,
        interval,
        &xrp_first_rows,
        first_read_at,
    )?;
    assert!(xrp_pending.pending_confirmation && xrp_pending.confirmed_spans.is_empty());
    let (btc_second_raw, btc_second_rows) =
        publish_page(&mut store, &btc, interval, &anchor_opens, second_read_at)?;
    let btc_proof = store.observe_provider_absence_snapshot(
        &btc_second_raw,
        &btc,
        interval,
        &btc_second_rows,
        second_read_at,
    )?;
    assert_eq!(btc_proof.confirmed_spans.len(), 1);
    let btc_proof_id = btc_proof.confirmed_spans[0].proof_id.clone();

    // BTC has verified absence, while XRP has only real anchors and an unexplained
    // gap. No market checkpoint or original event can commit on this partial proof.
    let btc_only_batches = vec![
        (btc_second_raw.clone(), btc_second_rows.clone()),
        (xrp_first_raw.clone(), xrp_first_rows.clone()),
    ];
    assert_eq!(
        store.reconcile_running_paper_sessions(&btc_only_batches, second_read_at)?,
        1,
        "the unresolved market must durably move the session into a provider-gap hold"
    );
    let waiting = store
        .paper_get_session(&session_id, second_read_at)?
        .session;
    assert_eq!(waiting.session_id, session_id);
    assert_eq!(waiting.policy_ref, policy_ref);
    assert_eq!(waiting.state, PaperSessionState::RecoveryRequired);
    assert_eq!(
        waiting.recovery_cause,
        Some(PaperRecoveryCause::ProviderGap)
    );
    assert_eq!(waiting.sequence, original_session.sequence + 1);
    assert_eq!(
        store.paper_checkpoint_digests_for_test(&session_id)?,
        original_checkpoints
    );
    let waiting_events_json = event_json_prefix(&store, &session_id)?;
    assert_eq!(
        &waiting_events_json[..original_events.len()],
        original_events.as_slice()
    );
    let wait_markers = parsed_events(&waiting_events_json[original_events.len()..])?;
    assert_eq!(wait_markers.len(), 1);
    assert_eq!(wait_markers[0].kind, PaperEventKind::RecoveryRequired);
    assert_no_recovery_trades(&wait_markers);

    let (xrp_second_raw, xrp_second_rows) =
        publish_page(&mut store, &xrp, interval, &anchor_opens, second_read_at)?;
    let xrp_proof = store.observe_provider_absence_snapshot(
        &xrp_second_raw,
        &xrp,
        interval,
        &xrp_second_rows,
        second_read_at,
    )?;
    assert_eq!(xrp_proof.confirmed_spans.len(), 1);
    let xrp_proof_id = xrp_proof.confirmed_spans[0].proof_id.clone();
    let before_fault_events = event_json_prefix(&store, &session_id)?;
    assert_eq!(before_fault_events, waiting_events_json);
    let before_fault_checkpoints = store.paper_checkpoint_digests_for_test(&session_id)?;
    store.paper_resume_event_fault_for_test(true)?;
    assert!(
        store
            .reconcile_running_paper_sessions(&[], second_read_at)
            .is_err()
    );
    store.paper_resume_event_fault_for_test(false)?;

    // The event insert happens after checkpoint/session writes in the recovery
    // transaction. Its failure must roll the entire session transition back.
    let after_fault = store
        .paper_get_session(&session_id, second_read_at)?
        .session;
    assert_eq!(after_fault.session_id, session_id);
    assert_eq!(after_fault.policy_ref, policy_ref);
    assert_eq!(after_fault.state, PaperSessionState::RecoveryRequired);
    assert_eq!(
        after_fault.recovery_cause,
        Some(PaperRecoveryCause::ProviderGap)
    );
    assert_eq!(after_fault.sequence, waiting.sequence);
    assert_eq!(
        store.paper_checkpoint_digests_for_test(&session_id)?,
        before_fault_checkpoints
    );
    assert_eq!(event_json_prefix(&store, &session_id)?, before_fault_events);

    assert_eq!(
        store.reconcile_running_paper_sessions(&[], second_read_at)?,
        1,
        "after removing the injected fault, both proven markets must commit together"
    );
    let completed = store
        .paper_get_session(&session_id, second_read_at)?
        .session;
    assert_eq!(completed.session_id, session_id);
    assert_eq!(completed.policy_ref, policy_ref);
    assert_eq!(completed.state, PaperSessionState::Running);
    assert!(completed.markets.iter().all(|market| {
        market.last_finalized_candle_at == Some(test_time("2025-02-03T11:05:00Z"))
            && market.verification.status == PaperVerificationStatus::Pass
    }));
    assert!(
        completed
            .markets
            .iter()
            .zip(&original_session.markets)
            .all(|(after, before)| {
                after.cash_total == before.cash_total
                    && after.quantity == before.quantity
                    && after.fill_count == before.fill_count
                    && after.signal_count == before.signal_count
            })
    );
    let completed_json = event_json_prefix(&store, &session_id)?;
    assert_eq!(
        &completed_json[..original_events.len()],
        original_events.as_slice()
    );
    let all_new_events = parsed_events(&completed_json[original_events.len()..])?;
    assert_eq!(all_new_events[0].kind, PaperEventKind::RecoveryRequired);
    assert_no_recovery_trades(&all_new_events);
    let recovery_events = &all_new_events[1..];
    assert!(recovery_events.iter().all(|event| {
        event.provider_recovery.as_ref().is_some_and(|recovery| {
            recovery.proof_ids.contains(&btc_proof_id) && recovery.proof_ids.contains(&xrp_proof_id)
        })
    }));
    assert_eq!(
        recovery_events
            .last()
            .and_then(|event| event.provider_recovery.as_ref())
            .map(|recovery| recovery.phase),
        Some(PaperProviderRecoveryPhase::Completed)
    );
    Ok(())
}

// Append this test body to `storage/paper_store/provider_recovery/tests.rs`
// after `provider-recovery-tests.rs`; it reuses that module's fixture helpers.

fn mtf_recovery_checkpoint(
    store: &TestStore,
    session_id: &PaperSessionId,
    market: &MarketId,
) -> Result<crate::paper::PaperMarketCheckpoint, Box<dyn std::error::Error>> {
    let encoded: String = store.connection.query_row(
        "SELECT checkpoint_json FROM paper_market_checkpoints WHERE session_id=?1 AND market=?2",
        rusqlite::params![session_id.as_str(), market.code()],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&encoded)?)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real Store lifecycle regression proves finer-source horizon clipping and causal H1 completion after restart"
)]
fn h1_provider_recovery_clips_to_loaded_m5_sources_then_completes_at_h1_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let started_at = test_time("2025-01-01T10:00:00Z");
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let decision_interval = CandleInterval::H1;
    let source_interval = CandleInterval::M5;

    let definition = crate::contracts::PolicyDefinition {
        schema_version: "1.0".into(),
        name: "synthetic_h1_m5_provider_recovery".into(),
        description: "SYNTHETIC_TEST_ONLY H1 decisions with actual M5 source candles".into(),
        program: crate::contracts::PolicyProgram::Rules {
            program: crate::contracts::RulesProgram {
                condition_roles: Vec::new(),
                indicators: vec![crate::contracts::PolicyIndicator {
                    id: "m5_sma".into(),
                    indicator: crate::contracts::PolicyIndicatorKind::Sma { window: 2 },
                    source_interval: Some(source_interval),
                }],
                states: Vec::new(),
                rules: Vec::new(),
                fallback: crate::contracts::PolicyTarget::Hold,
                signal_expiry: crate::contracts::PolicySignalExpiry::EndOfRange,
            },
        },
    };
    let policy = store.write_policy(
        &crate::contracts::PolicyWrite::Create {
            request_id: RequestId::new("synthetic-h1-m5-provider-policy")?,
            definition,
        },
        started_at,
    )?;
    let policy_ref = policy.snapshot.reference.clone();
    let start = PaperSessionWrite::Start {
        request_id: RequestId::new("synthetic-h1-m5-provider-session")?,
        policy_ref: policy_ref.clone(),
        decision_interval,
        execution_resolution: decision_interval,
        latency_ms: 0,
        execution: ExecutionPolicy::NextBarOpen {
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        markets: vec![PaperMarketStart {
            market: market.clone(),
            initial_cash: QuoteAmount::new(Decimal::from(1_000_000))?,
            costs: zero_cost_policy(),
            market_rules: simple_market_rules(),
        }],
    };
    let session_id = store.paper_write(&start, started_at)?.session.session_id;

    // Give the original checkpoint one real completed H1 candle and its 12 real
    // M5 source bars, then omit only the following H1 candle from the proof pages.
    let h1_pre_open = started_at;
    let h1_pre_close = UtcTimestamp(h1_pre_open.0 + decision_interval.duration());
    let warm_m5_opens = (0..12)
        .map(|index| UtcTimestamp(h1_pre_open.0 + chrono::Duration::minutes(5 * i64::from(index))))
        .collect::<Vec<_>>();
    let (_decision_input, initial_decisions) = publish_page(
        &mut store,
        &market,
        decision_interval,
        &[h1_pre_open],
        fetched_after_close(h1_pre_open, decision_interval),
    )?;
    let (_source_input, initial_sources) = publish_page(
        &mut store,
        &market,
        source_interval,
        &warm_m5_opens,
        fetched_after_close(
            *warm_m5_opens.last().expect("warm-up page is nonempty"),
            source_interval,
        ),
    )?;
    let warm_sources = BTreeMap::from([(source_interval, initial_sources.clone())]);
    store.step_paper_session_bars(
        &session_id,
        &market,
        &initial_decisions,
        &warm_sources,
        UtcTimestamp(h1_pre_close.0 + chrono::Duration::seconds(1)),
    )?;
    let original_session = store
        .paper_get_session(
            &session_id,
            UtcTimestamp(h1_pre_close.0 + chrono::Duration::seconds(1)),
        )?
        .session;
    let original_events = event_json_prefix(&store, &session_id)?;
    let original_checkpoints = store.paper_checkpoint_digests_for_test(&session_id)?;
    let warm_checkpoint = mtf_recovery_checkpoint(&store, &session_id, &market)?;
    assert_eq!(warm_checkpoint.last_finalized_candle_at, Some(h1_pre_close));
    assert_eq!(
        warm_checkpoint.source_cursors[&source_interval].last_bar_close_utc,
        Some(h1_pre_close)
    );

    let post_gap_open = UtcTimestamp(h1_pre_open.0 + chrono::Duration::hours(2));
    let proof_page_opens = [h1_pre_open, post_gap_open];
    let proof_first_at =
        UtcTimestamp(post_gap_open.0 + decision_interval.duration() + chrono::Duration::seconds(1));
    let proof_second_at = UtcTimestamp(proof_first_at.0 + chrono::Duration::seconds(1));
    let (proof_first_raw, proof_first_rows) = publish_page(
        &mut store,
        &market,
        decision_interval,
        &proof_page_opens,
        proof_first_at,
    )?;
    let first_observation = store.observe_provider_absence_snapshot(
        &proof_first_raw,
        &market,
        decision_interval,
        &proof_first_rows,
        proof_first_at,
    )?;
    assert!(first_observation.confirmed_spans.is_empty());
    assert!(first_observation.pending_confirmation);
    let (proof_second_raw, proof_second_rows) = publish_page(
        &mut store,
        &market,
        decision_interval,
        &proof_page_opens,
        proof_second_at,
    )?;
    let confirmed = store.observe_provider_absence_snapshot(
        &proof_second_raw,
        &market,
        decision_interval,
        &proof_second_rows,
        proof_second_at,
    )?;
    assert_eq!(confirmed.confirmed_spans.len(), 1);
    let proof_id = confirmed.confirmed_spans[0].proof_id.clone();
    assert_eq!(confirmed.confirmed_spans[0].start_utc, h1_pre_close);
    assert_eq!(confirmed.confirmed_spans[0].end_utc, post_gap_open);
    assert_eq!(confirmed.confirmed_spans[0].interval, decision_interval);

    // 67 actual H1 decision periods require 804 M5 source rows, plus the 12
    // actual M5 bars spanning the proven H1 absence. Keep every raw page at 200.
    let actual_h1_periods = 67_usize;
    let h1_opens = (0..actual_h1_periods)
        .map(|index| {
            UtcTimestamp(
                post_gap_open.0
                    + chrono::Duration::hours(
                        i64::try_from(index).expect("67-row decision fixture fits i64"),
                    ),
            )
        })
        .collect::<Vec<_>>();
    let final_h1_open = *h1_opens.last().expect("actual H1 page is nonempty");
    let h1_target = UtcTimestamp(final_h1_open.0 + decision_interval.duration());
    let wall_now =
        UtcTimestamp(h1_target.0 + chrono::Duration::minutes(5) + chrono::Duration::seconds(30));
    let (actual_h1_raw, actual_h1_rows) = publish_page(
        &mut store,
        &market,
        decision_interval,
        &h1_opens,
        fetched_after_close(final_h1_open, decision_interval),
    )?;

    let m5_source_start = h1_pre_close;
    let source_row_count = usize::try_from((h1_target.0 - m5_source_start.0).num_minutes() / 5)?;
    assert_eq!(source_row_count, 12 * (actual_h1_periods + 1));
    let m5_source_opens = (0..source_row_count)
        .map(|index| {
            UtcTimestamp(
                m5_source_start.0
                    + chrono::Duration::minutes(
                        5 * i64::try_from(index).expect("bounded source fixture fits i64"),
                    ),
            )
        })
        .collect::<Vec<_>>();
    let mut batches = vec![
        (proof_second_raw, proof_second_rows),
        (actual_h1_raw, actual_h1_rows),
    ];
    for page_opens in m5_source_opens.chunks(200) {
        batches.push(publish_page(
            &mut store,
            &market,
            source_interval,
            page_opens,
            fetched_after_close(
                *page_opens.last().expect("source page is nonempty"),
                source_interval,
            ),
        )?);
    }
    assert_eq!(
        crate::scheduling::latest_completed_boundary(wall_now, decision_interval)?,
        h1_target
    );
    let wall_m5_boundary = crate::scheduling::latest_completed_boundary(wall_now, source_interval)?;
    assert_eq!(
        wall_m5_boundary,
        UtcTimestamp(h1_target.0 + source_interval.duration())
    );

    assert_eq!(
        store.reconcile_running_paper_sessions(&batches, wall_now)?,
        1,
        "bounded H1 recovery must commit a progress chunk when the M5 source horizon ends first"
    );
    assert_eq!(
        store.paper_candle_observation_count_for_test(&market, source_interval)?,
        u64::try_from(warm_m5_opens.len() + source_row_count)?
    );
    let progressed = store.paper_get_session(&session_id, wall_now)?.session;
    assert_eq!(progressed.session_id, session_id);
    assert_eq!(progressed.policy_ref, policy_ref);
    assert_eq!(progressed.decision_interval, decision_interval);
    assert_eq!(progressed.state, PaperSessionState::RecoveryRequired);
    assert_eq!(
        progressed.recovery_cause,
        Some(PaperRecoveryCause::ProviderGap)
    );
    let progressed_events_json = event_json_prefix(&store, &session_id)?;
    assert_eq!(
        &progressed_events_json[..original_events.len()],
        original_events.as_slice()
    );
    let progress_events = parsed_events(&progressed_events_json[original_events.len()..])?;
    assert_no_recovery_trades(&progress_events);
    assert!(progress_events.iter().all(|event| {
        event
            .provider_recovery
            .as_ref()
            .is_some_and(|recovery| recovery.proof_ids.contains(&proof_id))
    }));
    assert_eq!(
        progress_events
            .last()
            .and_then(|event| event.provider_recovery.as_ref())
            .map(|recovery| recovery.phase),
        Some(PaperProviderRecoveryPhase::Progress)
    );
    let progress_checkpoint = mtf_recovery_checkpoint(&store, &session_id, &market)?;
    let last_h1_close = progress_checkpoint
        .last_finalized_candle_at
        .expect("progress chunk contains actual H1 decisions");
    let progress_m5_close = progress_checkpoint.source_cursors[&source_interval]
        .last_bar_close_utc
        .expect("progress chunk contains actual M5 source bars");
    assert_eq!(
        last_h1_close,
        UtcTimestamp(post_gap_open.0 + chrono::Duration::hours(65)),
        "796 loaded M5 rows cover 65 complete H1 decisions after the gap"
    );
    assert!(last_h1_close < h1_target);
    assert_eq!(progress_m5_close, last_h1_close);
    assert_ne!(progress_m5_close, wall_m5_boundary);
    assert_ne!(
        store.paper_checkpoint_digests_for_test(&session_id)?,
        original_checkpoints
    );

    drop(store);
    let mut reopened = TestStore::open(&root.0)?;
    assert_eq!(
        reopened.reconcile_running_paper_sessions(&[], wall_now)?,
        1,
        "the remaining actual H1/M5 suffix must complete after Store restart"
    );
    let completed = reopened.paper_get_session(&session_id, wall_now)?.session;
    assert_eq!(completed.session_id, session_id);
    assert_eq!(completed.policy_ref, policy_ref);
    assert_eq!(completed.state, PaperSessionState::Running);
    assert_eq!(completed.recovery_cause, None);
    assert_eq!(
        completed.markets[0].last_finalized_candle_at,
        Some(h1_target)
    );
    assert_eq!(
        completed.markets[0].signal_count,
        original_session.markets[0].signal_count
    );
    assert_eq!(
        completed.markets[0].fill_count,
        original_session.markets[0].fill_count
    );
    assert_eq!(
        completed.markets[0].cash_total,
        original_session.markets[0].cash_total
    );
    assert_eq!(
        completed.markets[0].quantity,
        original_session.markets[0].quantity
    );
    let completed_checkpoint = mtf_recovery_checkpoint(&reopened, &session_id, &market)?;
    let completed_m5_close = completed_checkpoint.source_cursors[&source_interval]
        .last_bar_close_utc
        .expect("completed source cursor has an actual M5 candle");
    assert_eq!(
        completed_checkpoint.last_finalized_candle_at,
        Some(h1_target)
    );
    assert_eq!(completed_m5_close, h1_target);
    assert_ne!(completed_m5_close, wall_m5_boundary);
    let completed_events_json = event_json_prefix(&reopened, &session_id)?;
    assert_eq!(
        &completed_events_json[..original_events.len()],
        original_events.as_slice()
    );
    let recovery_events = parsed_events(&completed_events_json[original_events.len()..])?;
    assert_no_recovery_trades(&recovery_events);
    assert!(recovery_events.iter().all(|event| {
        event
            .provider_recovery
            .as_ref()
            .is_some_and(|recovery| recovery.proof_ids.contains(&proof_id))
    }));
    assert_eq!(
        recovery_events
            .last()
            .and_then(|event| event.provider_recovery.as_ref())
            .map(|recovery| recovery.phase),
        Some(PaperProviderRecoveryPhase::Completed)
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one virtual pending-order lifecycle proves cancellation precedes recovery marks and reservation is released exactly once"
)]
fn provider_recovery_releases_virtual_reservation_before_catchup_marks()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let started = test_time("2025-01-02T09:50:00Z");
    let (sid, policy) = start_session_with_execution(
        &mut store,
        "pending-provider-recovery",
        started,
        &["KRW-BTC"],
        ExecutionPolicy::PassiveBuy {
            offset_bps: BasisPoints::new(Decimal::from(100))?,
            penetration_ticks: 1,
            fill_fraction: crate::contracts::PassiveFraction::Full,
            ttl_execution_bars: 1,
            participation_cap: Weight::new(Decimal::ONE)?,
        },
        1,
    )?;
    let pre_opens = [
        started,
        test_time("2025-01-02T09:55:00Z"),
        test_time("2025-01-02T10:00:00Z"),
    ];
    let (_, rows) = publish_page(
        &mut store,
        &market,
        CandleInterval::M5,
        &pre_opens,
        test_time("2025-01-02T10:05:01Z"),
    )?;
    store.step_paper_session_bars(
        &sid,
        &market,
        &rows,
        &BTreeMap::new(),
        test_time("2025-01-02T10:05:01Z"),
    )?;
    let before = store
        .paper_get_session(&sid, test_time("2025-01-02T10:05:01Z"))?
        .session;
    assert!(
        before.markets[0].cash_reserved.get() > Decimal::ZERO,
        "fixture must carry a real virtual reservation"
    );
    let cp = mtf_recovery_checkpoint(&store, &sid, &market)?;
    let pending = cp
        .pending
        .as_ref()
        .expect("fixture has an actual passive order")
        .order
        .clone();
    let old_events = event_json_prefix(&store, &sid)?;
    let opens = [
        test_time("2025-01-02T10:00:00Z"),
        test_time("2025-01-02T11:00:00Z"),
    ];
    let first_at = test_time("2025-01-02T11:05:01Z");
    let second_at = test_time("2025-01-02T11:05:02Z");
    let (raw, rows) = publish_page(&mut store, &market, CandleInterval::M5, &opens, first_at)?;
    assert!(
        store
            .observe_provider_absence_snapshot(&raw, &market, CandleInterval::M5, &rows, first_at)?
            .pending_confirmation
    );
    let (raw, rows) = publish_page(&mut store, &market, CandleInterval::M5, &opens, second_at)?;
    assert_eq!(
        store
            .observe_provider_absence_snapshot(&raw, &market, CandleInterval::M5, &rows, second_at)?
            .confirmed_spans
            .len(),
        1
    );
    store.reconcile_running_paper_sessions(&[(raw, rows)], second_at)?;
    let after = store.paper_get_session(&sid, second_at)?.session;
    assert_eq!(after.state, PaperSessionState::Running);
    assert_eq!(after.policy_ref, policy);
    assert_eq!(after.markets[0].cash_total, before.markets[0].cash_total);
    assert_eq!(after.markets[0].quantity, before.markets[0].quantity);
    assert_eq!(after.markets[0].fill_count, before.markets[0].fill_count);
    assert_eq!(
        after.markets[0].signal_count,
        before.markets[0].signal_count
    );
    assert_eq!(after.markets[0].cash_reserved.get(), Decimal::ZERO);
    assert_eq!(
        after.markets[0].verification.status,
        PaperVerificationStatus::Pass
    );
    let all = event_json_prefix(&store, &sid)?;
    assert_eq!(&all[..old_events.len()], old_events.as_slice());
    let appended = parsed_events(&all[old_events.len()..])?;
    assert_eq!(
        appended[0]
            .provider_recovery
            .as_ref()
            .map(|meta| meta.phase),
        Some(PaperProviderRecoveryPhase::Started)
    );
    assert_eq!(appended[0].order_events.len(), 1);
    let cancel = &appended[0].order_events[0];
    assert_eq!(cancel.order_id, pending.order_id);
    assert_eq!(cancel.status, crate::contracts::OrderStatus::Cancelled);
    assert_eq!(cancel.effective_at, pending.effective_at);
    assert_eq!(
        cancel.expires_at, pending.expires_at,
        "recovery must not refresh the virtual order TTL"
    );
    assert!(
        appended
            .iter()
            .skip(1)
            .all(|event| event.order_events.is_empty())
    );
    assert!(appended.iter().all(|event| event.signals.is_empty()
        && event.orders.is_empty()
        && event.fills.is_empty()));
    drop(store);
    let mut reopened = TestStore::open(&root.0)?;
    reopened.reconcile_running_paper_sessions(&[], second_at)?;
    assert_eq!(event_json_prefix(&reopened, &sid)?, all);
    Ok(())
}

fn persist_provider_proof_reference_for_readback_test(
    store: &TestStore,
    session_id: &PaperSessionId,
    proof_id: &crate::contracts::ContentHash,
    processed_at: UtcTimestamp,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sequence, event_json): (i64, String) = store.connection.query_row(
        "SELECT sequence, event_json FROM paper_session_events
         WHERE session_id=?1 ORDER BY sequence LIMIT 1",
        [session_id.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut event: serde_json::Value = serde_json::from_str(&event_json)?;
    event["provider_recovery"] = serde_json::json!({
        "phase": "CATCH_UP",
        "processed_at": processed_at.to_string(),
        "target_completed_at": processed_at.to_string(),
        "proof_ids": [proof_id.as_str()],
        "checkpoint_transitions": [],
    });
    let changed = store.connection.execute(
        "UPDATE paper_session_events SET event_json=?1 WHERE session_id=?2 AND sequence=?3",
        rusqlite::params![
            serde_json::to_string(&event)?,
            session_id.as_str(),
            sequence,
        ],
    )?;
    assert_eq!(changed, 1);
    Ok(())
}

fn start_provider_readback_test_session(
    store: &mut TestStore,
    request_id: &str,
) -> Result<(PaperSessionId, MarketId, UtcTimestamp), Box<dyn std::error::Error>> {
    let started_at = test_time("2025-01-02T00:00:00Z");
    let interval = CandleInterval::M5;
    let now = fetched_after_close(started_at, interval);
    let (session_id, _) = start_session(store, request_id, started_at, &["KRW-BTC"])?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let (_raw, rows) = publish_page(store, &market, interval, &[started_at], now)?;
    store.step_paper_session_bars(&session_id, &market, &rows, &BTreeMap::new(), now)?;
    Ok((session_id, market, now))
}

fn provider_readback_rejects_invalid_proof(
    store: &TestStore,
    session_id: &PaperSessionId,
    now: UtcTimestamp,
) -> Result<(), Box<dyn std::error::Error>> {
    let events_before = event_json_prefix(store, session_id)?;
    let state_before: String = store.connection.query_row(
        "SELECT view_json FROM paper_sessions WHERE session_id=?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let readback = store.paper_get_session(session_id, now)?.session;
    assert_eq!(&readback.session_id, session_id);
    assert_eq!(readback.markets.len(), 1);
    assert_eq!(
        readback.markets[0].market,
        MarketId::parse_upbit("KRW-BTC")?
    );
    assert!(readback.markets.iter().all(|market| {
        market.verification.status == PaperVerificationStatus::Fail
            && market
                .verification
                .reason
                .contains("PROVIDER_RECOVERY_PROOF_INVALID")
    }));
    assert_eq!(event_json_prefix(store, session_id)?, events_before);
    let state_after: String = store.connection.query_row(
        "SELECT view_json FROM paper_sessions WHERE session_id=?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    assert_eq!(state_after, state_before);
    Ok(())
}

fn provider_readback_test_proof(
    store: &mut TestStore,
    market: &MarketId,
    first_open: UtcTimestamp,
) -> Result<(crate::contracts::ContentHash, RawObjectRef), Box<dyn std::error::Error>> {
    let interval = CandleInterval::M5;
    let opens = [
        first_open,
        UtcTimestamp(first_open.0 + chrono::Duration::minutes(10)),
    ];
    let first_read = fetched_after_close(opens[1], interval);
    let (first_raw, first_rows) = publish_page(store, market, interval, &opens, first_read)?;
    let pending = store.observe_provider_absence_snapshot(
        &first_raw,
        market,
        interval,
        &first_rows,
        first_read,
    )?;
    assert!(pending.pending_confirmation && pending.confirmed_spans.is_empty());

    let second_read = UtcTimestamp(first_read.0 + chrono::Duration::seconds(1));
    let (second_raw, second_rows) = publish_page(store, market, interval, &opens, second_read)?;
    let confirmed = store.observe_provider_absence_snapshot(
        &second_raw,
        market,
        interval,
        &second_rows,
        second_read,
    )?;
    assert_eq!(confirmed.confirmed_spans.len(), 1);
    Ok((confirmed.confirmed_spans[0].proof_id.clone(), first_raw))
}

#[test]
fn provider_readback_rejects_dangling_proof() -> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let (session_id, _market, now) =
        start_provider_readback_test_session(&mut store, "readback-dangling-provider-proof")?;
    let missing = crate::contracts::ContentHash::of_bytes(b"missing-provider-proof");
    let persisted_count: i64 = store.connection.query_row(
        "SELECT count(*) FROM provider_absence_spans WHERE proof_id=?1",
        [missing.as_str()],
        |row| row.get(0),
    )?;
    assert_eq!(persisted_count, 0);
    persist_provider_proof_reference_for_readback_test(&store, &session_id, &missing, now)?;

    provider_readback_rejects_invalid_proof(&store, &session_id, now)
}

#[test]
fn provider_readback_rejects_tampered_raw_witness() -> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let (session_id, market, now) =
        start_provider_readback_test_session(&mut store, "readback-tampered-provider-witness")?;
    let (proof_id, raw) =
        provider_readback_test_proof(&mut store, &market, test_time("2025-01-01T00:00:00Z"))?;
    let ids = std::iter::once(proof_id.clone()).collect();
    let valid = super::named_coverage(&store.connection, &store.raw, &ids)?;
    assert_eq!(valid.proof_ids(), vec![proof_id.clone()]);
    persist_provider_proof_reference_for_readback_test(&store, &session_id, &proof_id, now)?;

    let body_path = root.0.join(&raw.relative_path).join("body.json.gz");
    let mut compressed = std::fs::read(&body_path)?;
    let first_byte = compressed
        .first_mut()
        .expect("stored compressed raw witness has a header");
    *first_byte ^= 1;
    std::fs::write(&body_path, compressed)?;
    assert!(body_path.is_file());

    provider_readback_rejects_invalid_proof(&store, &session_id, now)
}

fn provider_readback_span_for_test(
    store: &TestStore,
    proof_id: &crate::contracts::ContentHash,
) -> Result<crate::contracts::ProviderAbsenceSpan, Box<dyn std::error::Error>> {
    let json: String = store.connection.query_row(
        "SELECT proof_json FROM provider_absence_spans WHERE proof_id=?1",
        [proof_id.as_str()],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn corrupt_decode_memo_test_raw(
    root: &RecoveryTestRoot,
    raw: &RawObjectRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = root.0.join(&raw.relative_path).join("body.json.gz");
    let mut bytes = std::fs::read(&path)?;
    *bytes
        .first_mut()
        .expect("compressed test body has a header") ^= 1;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn assert_fresh_decode_memo_raw_error(error: crate::contracts::LabError) {
    match error {
        crate::contracts::LabError::DataCorrupt(reason) => assert!(
            reason.contains("compressed raw object hash mismatch"),
            "unexpected corruption reason: {reason}"
        ),
        other => panic!("expected fresh raw read corruption, received {other:?}"),
    }
}

#[test]
fn provider_absence_decode_memo_rechecks_cached_raw_file() -> Result<(), Box<dyn std::error::Error>>
{
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let (proof_id, raw) =
        provider_readback_test_proof(&mut store, &market, test_time("2025-06-01T00:00:00Z"))?;
    let span = provider_readback_span_for_test(&store, &proof_id)?;
    let mut memo = super::super::super::public_feed_recovery::ProviderAbsenceDecodeMemo::default();
    for _ in 0..2 {
        let verified =
            super::super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
                &store.connection,
                store.raw_objects(),
                &market,
                CandleInterval::M5,
                span.start_utc,
                span.end_utc,
                &mut memo,
            )?;
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].proof_id, proof_id);
        assert_eq!(memo.cached_page_count(), 2);
    }

    corrupt_decode_memo_test_raw(&root, &raw)?;
    let error =
        super::super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
            &store.connection,
            store.raw_objects(),
            &market,
            CandleInterval::M5,
            span.start_utc,
            span.end_utc,
            &mut memo,
        )
        .expect_err("a cache hit must still fresh-read the raw file");
    assert_fresh_decode_memo_raw_error(error);
    Ok(())
}

#[test]
fn provider_absence_decode_memo_validates_third_and_fourth_raw_witnesses()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let (first_proof_id, _first_raw) =
        provider_readback_test_proof(&mut store, &market, test_time("2025-06-01T00:00:00Z"))?;
    let first_span = provider_readback_span_for_test(&store, &first_proof_id)?;
    let mut memo = super::super::super::public_feed_recovery::ProviderAbsenceDecodeMemo::default();
    let verified =
        super::super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
            &store.connection,
            store.raw_objects(),
            &market,
            CandleInterval::M5,
            first_span.start_utc,
            first_span.end_utc,
            &mut memo,
        )?;
    assert_eq!(verified[0].proof_id, first_proof_id);
    assert_eq!(memo.cached_page_count(), 2);

    let (second_proof_id, second_first_raw) =
        provider_readback_test_proof(&mut store, &market, test_time("2025-06-01T04:00:00Z"))?;
    let second_span = provider_readback_span_for_test(&store, &second_proof_id)?;
    assert!(
        second_span
            .snapshots
            .iter()
            .all(
                |snapshot| snapshot.raw_object_id != first_span.snapshots[0].raw_object_id
                    && snapshot.raw_object_id != first_span.snapshots[1].raw_object_id
            )
    );
    assert_eq!(second_span.snapshots[0].raw_object_id, second_first_raw.id);
    assert_ne!(
        second_span.snapshots[0].raw_object_id,
        second_span.snapshots[1].raw_object_id
    );
    let verified =
        super::super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
            &store.connection,
            store.raw_objects(),
            &market,
            CandleInterval::M5,
            second_span.start_utc,
            second_span.end_utc,
            &mut memo,
        )?;
    assert_eq!(verified.len(), 1);
    assert_eq!(verified[0].proof_id, second_proof_id);
    assert_eq!(memo.cached_page_count(), 2);

    corrupt_decode_memo_test_raw(&root, &second_first_raw)?;
    let error =
        super::super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
            &store.connection,
            store.raw_objects(),
            &market,
            CandleInterval::M5,
            second_span.start_utc,
            second_span.end_utc,
            &mut memo,
        )
        .expect_err("a full memo must validate rather than skip a third raw witness");
    assert_fresh_decode_memo_raw_error(error);
    assert_eq!(memo.cached_page_count(), 2);
    Ok(())
}

const LEGACY_SESSION_PROVIDER_PROOFS_SQL: &str = "SELECT DISTINCT j.value FROM paper_session_events e, \
     json_each(e.event_json, '$.provider_recovery.proof_ids') j \
     WHERE e.session_id=?1 LIMIT ?2";
const COVERAGE_QUERY_NO_PROOF_EVENTS: i64 = 2_048;

fn session_provider_proof_values(
    connection: &rusqlite::Connection,
    sql: &str,
    session_id: &PaperSessionId,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let values = statement
        .query_map(
            rusqlite::params![
                session_id.as_str(),
                i64::try_from(super::MAX_SESSION_PROVIDER_PROOFS + 1)
                    .expect("provider proof cap fits i64")
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let mut values = values;
    values.sort();
    Ok(values)
}

fn session_proof_query_signature(
    result: rusqlite::Result<Vec<String>>,
) -> Result<Vec<String>, String> {
    result.map_err(|error| error.to_string())
}

fn legacy_session_coverage(
    connection: &rusqlite::Connection,
    raw: &super::super::super::RawObjectStore,
    session: &super::PaperSessionView,
) -> Result<super::VerifiedProviderCoverage, LabError> {
    let strings = session_provider_proof_values(
        connection,
        LEGACY_SESSION_PROVIDER_PROOFS_SQL,
        &session.session_id,
    )
    .map_err(super::sql_error)?;
    let ids = strings
        .into_iter()
        .map(super::ContentHash::try_from)
        .collect::<Result<BTreeSet<_>, _>>()?;
    super::named_coverage(connection, raw, &ids)
}

fn coverage_signature(
    result: &Result<super::VerifiedProviderCoverage, LabError>,
) -> Result<Vec<String>, String> {
    result
        .as_ref()
        .map(|coverage| {
            coverage
                .proof_ids()
                .into_iter()
                .map(|proof_id| proof_id.to_string())
                .collect()
        })
        .map_err(|error| format!("{error:?}"))
}

fn insert_provider_coverage_test_event(
    store: &TestStore,
    session_id: &PaperSessionId,
    sequence: i64,
    kind: &str,
    event_json: &str,
) -> rusqlite::Result<()> {
    store.connection.execute(
        "INSERT INTO paper_session_events(session_id,sequence,kind,occurred_at_utc,event_json) \
         VALUES (?1,?2,?3,'2025-01-02T00:00:00Z',?4)",
        rusqlite::params![session_id.as_str(), sequence, kind, event_json],
    )?;
    Ok(())
}

fn update_provider_coverage_test_event(
    store: &TestStore,
    session_id: &PaperSessionId,
    sequence: i64,
    kind: &str,
    event_json: &str,
) -> rusqlite::Result<()> {
    let changed = store.connection.execute(
        "UPDATE paper_session_events SET kind=?1,event_json=?2 \
         WHERE session_id=?3 AND sequence=?4",
        rusqlite::params![kind, event_json, session_id.as_str(), sequence],
    )?;
    assert_eq!(changed, 1);
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one real Store regression preserves legacy JSON outcomes and proves the bundled SQLite plan"
)]
#[test]
fn provider_proof_index_preserves_legacy_coverage_shapes_and_uses_bundled_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let root = RecoveryTestRoot::new()?;
    let mut store = TestStore::open(&root.0)?;
    let (session_id, _market, now) =
        start_provider_readback_test_session(&mut store, "provider-proof-index-query")?;
    let session = store.paper_get_session(&session_id, now)?.session;
    let first_sequence: i64 = store.connection.query_row(
        "SELECT COALESCE(MAX(sequence),0)+1 FROM paper_session_events WHERE session_id=?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    for offset in 0..COVERAGE_QUERY_NO_PROOF_EVENTS {
        insert_provider_coverage_test_event(
            &store,
            &session_id,
            first_sequence + offset,
            "CANDLE_PROCESSED",
            "{}",
        )?;
    }
    let probe_sequence = first_sequence + COVERAGE_QUERY_NO_PROOF_EVENTS;
    insert_provider_coverage_test_event(
        &store,
        &session_id,
        probe_sequence,
        "PROVIDER_RECOVERY",
        "{}",
    )?;

    let proof_id = super::ContentHash::of_bytes(b"provider-proof-index-shape");
    let cases = [
        ("missing path", "PROVIDER_RECOVERY", "{}".to_owned()),
        (
            "null",
            "CANDLE_PROCESSED",
            r#"{"provider_recovery":{"proof_ids":null}}"#.to_owned(),
        ),
        (
            "text scalar",
            "CANDLE_PROCESSED",
            r#"{"provider_recovery":{"proof_ids":"not-a-hash"}}"#.to_owned(),
        ),
        (
            "numeric scalar",
            "CANDLE_PROCESSED",
            r#"{"provider_recovery":{"proof_ids":7}}"#.to_owned(),
        ),
        (
            "object",
            "PROVIDER_RECOVERY",
            format!(
                r#"{{"provider_recovery":{{"proof_ids":{{"ref":"{}"}}}}}}"#,
                proof_id.as_str()
            ),
        ),
        (
            "empty array",
            "CANDLE_PROCESSED",
            r#"{"provider_recovery":{"proof_ids":[]}}"#.to_owned(),
        ),
        (
            "nonempty array",
            "CANDLE_PROCESSED",
            format!(
                r#"{{"provider_recovery":{{"proof_ids":["{}"]}}}}"#,
                proof_id.as_str()
            ),
        ),
        (
            "escaped keys",
            "CANDLE_PROCESSED",
            format!(
                r#"{{"provider\u005frecovery":{{"proof\u005fids":["{}"]}}}}"#,
                proof_id.as_str()
            ),
        ),
        (
            "catch-up candle",
            "CANDLE_PROCESSED",
            format!(
                r#"{{"provider_recovery":{{"proof_ids":["{}"]}}}}"#,
                proof_id.as_str()
            ),
        ),
    ];

    for (label, kind, event_json) in cases {
        update_provider_coverage_test_event(
            &store,
            &session_id,
            probe_sequence,
            kind,
            &event_json,
        )?;
        let legacy_values = session_provider_proof_values(
            &store.connection,
            LEGACY_SESSION_PROVIDER_PROOFS_SQL,
            &session_id,
        );
        let indexed_values = session_provider_proof_values(
            &store.connection,
            super::SESSION_PROVIDER_PROOF_IDS_SQL,
            &session_id,
        );
        assert_eq!(
            session_proof_query_signature(legacy_values),
            session_proof_query_signature(indexed_values),
            "query values for {label}"
        );

        let legacy = legacy_session_coverage(&store.connection, store.raw_objects(), &session);
        let actual =
            super::session_coverage(&store.connection, store.raw_objects(), &session, &[], now);
        assert_eq!(
            coverage_signature(&legacy),
            coverage_signature(&actual),
            "coverage result for {label}"
        );
        match label {
            "missing path" | "empty array" => assert!(
                actual
                    .as_ref()
                    .is_ok_and(|coverage| coverage.proof_ids().is_empty()),
                "{label} should contribute no proof references"
            ),
            "null" | "numeric scalar" => {
                assert!(matches!(&actual, Err(LabError::Internal(_))));
            }
            "text scalar" => assert!(matches!(
                &actual,
                Err(LabError::InvalidConfig(reason)) if reason == "expected lowercase SHA-256"
            )),
            _ => {}
        }
    }

    for count in [
        super::MAX_SESSION_PROVIDER_PROOFS,
        super::MAX_SESSION_PROVIDER_PROOFS + 1,
    ] {
        let ids = (0_u64..u64::try_from(count)?)
            .map(|value| super::ContentHash::of_bytes(&value.to_be_bytes()))
            .collect::<Vec<_>>();
        let event_json = serde_json::json!({
            "provider_recovery": {
                "proof_ids": ids.iter().map(ToString::to_string).collect::<Vec<_>>()
            }
        })
        .to_string();
        update_provider_coverage_test_event(
            &store,
            &session_id,
            probe_sequence,
            "CANDLE_PROCESSED",
            &event_json,
        )?;
        let legacy_values = session_provider_proof_values(
            &store.connection,
            LEGACY_SESSION_PROVIDER_PROOFS_SQL,
            &session_id,
        )?;
        let indexed_values = session_provider_proof_values(
            &store.connection,
            super::SESSION_PROVIDER_PROOF_IDS_SQL,
            &session_id,
        )?;
        assert_eq!(legacy_values, indexed_values);
        assert_eq!(indexed_values.len(), count);

        let legacy = legacy_session_coverage(&store.connection, store.raw_objects(), &session);
        let actual =
            super::session_coverage(&store.connection, store.raw_objects(), &session, &[], now);
        assert_eq!(coverage_signature(&legacy), coverage_signature(&actual));
        if count == super::MAX_SESSION_PROVIDER_PROOFS {
            assert!(matches!(&actual, Err(LabError::DataCorrupt(_))));
        } else {
            assert!(matches!(&actual, Err(LabError::ResourceLimit(_))));
        }
    }

    store
        .connection
        .execute_batch("ANALYZE paper_session_events")?;
    let explain = format!(
        "EXPLAIN QUERY PLAN {}",
        super::SESSION_PROVIDER_PROOF_IDS_SQL
    );
    let mut statement = store.connection.prepare(&explain)?;
    let plan = statement
        .query_map(
            rusqlite::params![
                session_id.as_str(),
                i64::try_from(super::MAX_SESSION_PROVIDER_PROOFS + 1)?
            ],
            |row| row.get::<_, String>(3),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        plan.iter()
            .any(|detail| detail.contains("paper_session_provider_proofs")),
        "bundled SQLite did not use the sparse proof index: {plan:?}"
    );
    Ok(())
}
