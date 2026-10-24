use super::*;
use crate::contracts::{PaperEventKind, RecordedExecutionState};
use serde_json::json;
use std::time::Duration;

fn signal() -> Result<SignalRecord, LabError> {
    Ok(serde_json::from_value(json!({
        "context": {"run_id":"index-run","model_id":"index-model","market":"KRW-BTC",
                    "event_seq":1,"accounting_event_time":"2026-10-14T01:00:00Z"},
        "signal_id":"index-signal","strategy":"BUY_AND_HOLD","strategy_version":"fixture",
        "source_bar_ids":[],"signal_time":"2026-10-14T01:00:00Z",
        "decision_available_at":"2026-10-14T01:00:00Z","valid_until":"2026-10-14T02:00:00Z",
        "state_before":"CASH","state_after":"CASH","raw_target_weight":"0.5",
        "constrained_target_weight":"0.5","actual_weight_at_decision":"0",
        "intended_side":"BUY","reasons":["BAND_ENTRY"],
        "indicators":{"version":"fixture","ema":null,"bar_sigma":null,"annual_vol":null,
                      "rsi":null,"entry_high":null,"exit_low":null,"completed_bars_seen":1},
        "evidence_effect":null,"outcome":"EXECUTED"
    }))?)
}

fn policy() -> Result<PolicyRevisionRef, LabError> {
    Ok(serde_json::from_value(json!({
        "policy_id":"index-policy","revision_id":"index-revision",
        "definition_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }))?)
}

fn order(signal: &SignalRecord) -> Result<OrderRecord, LabError> {
    let mut context = serde_json::to_value(&signal.context)?;
    context["event_seq"] = json!(2);
    Ok(serde_json::from_value(json!({
        "context":context,"order_id":"index-order","parent_signal_id":signal.signal_id,
        "episode_id":"index-episode","side":"BUY","order_type":"MARKET",
        "requested_price":"100","requested_qty":"1","created_at":"2026-10-14T01:00:01Z",
        "effective_at":"2026-10-14T01:00:01Z","expires_at":"2026-10-14T02:00:00Z",
        "rule_snapshot_id":"index-rules","policy_version":"fixture","reserved_cash":"100",
        "cumulative_filled_qty":"0","status":"CREATED","reason":"BAND_ENTRY",
        "order_origin":"SIMULATED"
    }))?)
}

fn fill(signal: &SignalRecord) -> Result<FillRecord, LabError> {
    let mut context = serde_json::to_value(&signal.context)?;
    context["event_seq"] = json!(3);
    Ok(serde_json::from_value(json!({
        "context":context,"fill_id":"index-fill","order_id":"index-order",
        "episode_id":"index-episode","side":"BUY","price":"100","qty":"0.25",
        "notional":"25","fee":"0","fee_bps":"0","fee_currency":"KRW",
        "timing":{"kind":"EXACT","at":"2026-10-14T01:00:01Z"},
        "source_bar_id":"index-bar","liquidity_source_bar_id":"index-bar",
        "liquidity_source_close_time":"2026-10-14T02:00:00Z","decision_reference":"100",
        "bar_open_proxy":"100","arrival_mid":null,"arrival_mid_null_reason":"NOT_CAPTURED",
        "adverse_slippage_bps":"0","price_cost_attribution":"0",
        "liquidity_role_assumption":"SIMULATED","execution_origin":"SIMULATED",
        "fill_observed":false,"model_version":"fixture","artificial_terminal_exit":false,
        "accounting_mark_seq":4
    }))?)
}

fn event(sequence: u64) -> PaperSessionEvent {
    PaperSessionEvent {
        sequence,
        kind: PaperEventKind::CandleProcessed,
        market: None,
        candle: None,
        source_event_at: None,
        finalized_at: None,
        control: None,
        provider_recovery: None,
        preparation: None,
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    }
}

fn prefix() -> Result<Vec<PaperSessionEvent>, LabError> {
    let signal = signal()?;
    let created = order(&signal)?;
    let mut partial = created.clone();
    partial.context.event_seq = 4;
    partial.cumulative_filled_qty =
        crate::contracts::AssetQuantity::new(rust_decimal::Decimal::new(25, 2))?;
    partial.status = crate::contracts::OrderStatus::PartiallyFilled;
    partial.reason = crate::contracts::ReasonCode::PassivePartial;
    let fill = fill(&signal)?;
    let mut first = event(1);
    first.signals.push(signal);
    let mut second = event(2);
    // The final projection precedes creation in this array; internal sequence owns causality.
    second.orders.push(partial.clone());
    second.order_events.extend([created, partial]);
    second.fills.push(fill);
    Ok(vec![first, second])
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(15)
}

#[test]
fn indexed_prefix_keeps_original_lifecycle_and_excludes_future_fills() -> Result<(), LabError> {
    let mut events = prefix()?;
    let original = PaperLifecycleIndex::from_events(&events, deadline())?
        .explanation(&events[0].signals[0], &policy()?)?;
    assert_eq!(
        original.execution_state,
        RecordedExecutionState::PartiallyFilled
    );
    assert_eq!(original.orders.len(), 1);
    assert_eq!(original.fills.len(), 1);
    let mut future_fill = events[1].fills[0].clone();
    future_fill.fill_id = crate::contracts::FillId::new("index-later-fill")?;
    future_fill.context.event_seq = 5;
    let mut future = event(3);
    future.fills.push(future_fill);
    events.push(future);
    let same_prefix = PaperLifecycleIndex::from_events(&events[..2], deadline())?
        .explanation(&events[0].signals[0], &policy()?)?;
    assert_eq!(
        serde_json::to_value(&original)?,
        serde_json::to_value(same_prefix)?
    );
    let later_prefix = PaperLifecycleIndex::from_events(&events, deadline())?
        .explanation(&events[0].signals[0], &policy()?)?;
    assert_eq!(later_prefix.fills.len(), 2);
    Ok(())
}

#[test]
fn indexed_prefix_reuses_causal_fill_and_order_identity_rejections() -> Result<(), LabError> {
    let mut events = prefix()?;
    events[1].fills[0].context.market = crate::contracts::MarketId::parse_upbit("KRW-ETH")?;
    let index = PaperLifecycleIndex::from_events(&events, deadline())?;
    assert!(matches!(
        index.explanation(&events[0].signals[0], &policy()?),
        Err(LabError::DataCorrupt(_))
    ));
    let mut events = prefix()?;
    events[1].order_events.remove(0);
    let index = PaperLifecycleIndex::from_events(&events, deadline())?;
    assert!(matches!(
        index.explanation(&events[0].signals[0], &policy()?),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

#[test]
fn expired_shared_budget_stops_build_and_projection_without_sleeping() -> Result<(), LabError> {
    let events = prefix()?;
    assert!(matches!(
        PaperLifecycleIndex::from_events(&events, Instant::now()),
        Err(LabError::ResourceLimit(_))
    ));
    let mut index = PaperLifecycleIndex::from_events(&events, deadline())?;
    index.deadline = Instant::now();
    assert!(matches!(
        index.explanation(&events[0].signals[0], &policy()?),
        Err(LabError::ResourceLimit(_))
    ));
    Ok(())
}
