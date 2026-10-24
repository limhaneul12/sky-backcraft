use super::*;
use crate::contracts::{
    AssetQuantity, BasisPoints, EpisodeId, EventContext, FillId, FillRecord, FillTiming, MarketId,
    ModelId, ObservationId, OrderId, OrderRecord, OrderStatus, PaperEventKind, PaperSessionEvent,
    PaperSessionId, PositionState, PriceKrw, QuoteAmount, ReasonCode, RuleSnapshotId, RunId, Side,
    SignalId, SignalOutcome, SignalRecord, SignedAmount, SimulatedOrderType, StrategyKind,
    UtcTimestamp, Weight,
};
use chrono::Duration;
use rust_decimal::Decimal;

fn dec(value: &str) -> Decimal {
    value.parse().expect("fixture decimal")
}

fn time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

fn signal() -> SignalRecord {
    let at = time("2026-01-01T00:00:00Z");
    SignalRecord {
        context: EventContext {
            run_id: RunId::new("review-run").expect("run id"),
            model_id: ModelId::new("review-model").expect("model id"),
            market: MarketId::parse_upbit("KRW-BTC").expect("market"),
            event_seq: 1,
            accounting_event_time: at,
        },
        signal_id: SignalId::new("review-signal").expect("signal id"),
        strategy: StrategyKind::BuyAndHold,
        strategy_version: "fixture-v1".into(),
        policy_ref: None,
        policy_trace: None,
        decision_trace: None,
        actual_weight_at_decision: None,
        source_bar_ids: Vec::new(),
        signal_time: at,
        decision_available_at: at,
        valid_until: UtcTimestamp(at.0 + Duration::hours(1)),
        state_before: PositionState::Cash,
        state_after: PositionState::Long,
        raw_target_weight: Weight::new(Decimal::ONE).expect("target"),
        constrained_target_weight: Weight::new(Decimal::ONE).expect("target"),
        intended_side: Some(Side::Buy),
        reasons: vec![ReasonCode::StartFromCash],
        indicators: crate::contracts::IndicatorSnapshot {
            version: "fixture-v1".into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: 1,
            policy_values: Vec::new(),
        },
        evidence_effect: None,
        outcome: SignalOutcome::Executed,
    }
}

fn order(
    selected: &SignalRecord,
    id: &str,
    event_seq: u64,
    status: OrderStatus,
    filled: &str,
) -> OrderRecord {
    let at = selected.decision_available_at;
    OrderRecord {
        context: EventContext {
            run_id: selected.context.run_id.clone(),
            model_id: selected.context.model_id.clone(),
            market: selected.context.market.clone(),
            event_seq,
            accounting_event_time: UtcTimestamp(
                at.0 + Duration::minutes(
                    i64::try_from(event_seq).expect("fixture event sequence fits in minutes"),
                ),
            ),
        },
        order_id: OrderId::new(id).expect("order id"),
        parent_signal_id: selected.signal_id.clone(),
        episode_id: None,
        side: Side::Buy,
        order_type: SimulatedOrderType::Market,
        requested_price: None,
        requested_qty: AssetQuantity::new(Decimal::ONE).expect("quantity"),
        created_at: at,
        effective_at: at,
        expires_at: UtcTimestamp(at.0 + Duration::hours(1)),
        rule_snapshot_id: RuleSnapshotId::new("review-rules").expect("rule id"),
        policy_version: "fixture-v1".into(),
        reserved_cash: QuoteAmount::new(Decimal::from(100)).expect("reserve"),
        cumulative_filled_qty: AssetQuantity::new(dec(filled)).expect("filled quantity"),
        status,
        reason: ReasonCode::MarketFilled,
        order_origin: "SIMULATED".into(),
    }
}

fn fill(order: &OrderRecord, id: &str, event_seq: u64, qty: &str) -> FillRecord {
    let at = UtcTimestamp(order.context.accounting_event_time.0 + Duration::minutes(1));
    FillRecord {
        context: EventContext {
            event_seq,
            accounting_event_time: at,
            ..order.context.clone()
        },
        fill_id: FillId::new(id).expect("fill id"),
        order_id: order.order_id.clone(),
        episode_id: EpisodeId::new("review-episode").expect("episode id"),
        side: Side::Buy,
        price: PriceKrw::new(Decimal::from(100)).expect("price"),
        qty: AssetQuantity::new(dec(qty)).expect("quantity"),
        notional: QuoteAmount::new(dec(qty) * Decimal::from(100)).expect("notional"),
        fee: QuoteAmount::new(Decimal::ZERO).expect("fee"),
        fee_bps: BasisPoints::new(Decimal::ZERO).expect("fee bps"),
        fee_currency: "KRW".into(),
        timing: FillTiming::Exact { at },
        source_bar_id: ObservationId::new("review-source").expect("source observation"),
        liquidity_source_bar_id: ObservationId::new("review-liquidity")
            .expect("liquidity observation"),
        liquidity_source_close_time: at,
        decision_reference: PriceKrw::new(Decimal::from(100)).expect("reference"),
        bar_open_proxy: PriceKrw::new(Decimal::from(100)).expect("open proxy"),
        arrival_mid: None,
        arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
        adverse_slippage_bps: SignedAmount::new(Decimal::ZERO).expect("slippage"),
        price_cost_attribution: SignedAmount::new(Decimal::ZERO).expect("price cost"),
        liquidity_role_assumption: "fixture".into(),
        execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false,
        model_version: "fixture-v1".into(),
        artificial_terminal_exit: false,
        cost_provenance: None,
        accounting_mark_seq: event_seq + 1,
    }
}

fn event(
    sequence: u64,
    signals: Vec<SignalRecord>,
    orders: Vec<OrderRecord>,
    order_events: Vec<OrderRecord>,
    fills: Vec<FillRecord>,
) -> PaperSessionEvent {
    PaperSessionEvent {
        sequence,
        kind: PaperEventKind::CandleProcessed,
        market: Some(MarketId::parse_upbit("KRW-BTC").expect("market")),
        candle: None,
        source_event_at: Some(time("2026-01-01T00:00:00Z")),
        finalized_at: Some(time("2026-01-01T00:00:00Z")),
        control: None,
        provider_recovery: None,
        preparation: None,
        signals,
        orders,
        order_events,
        fills,
        account_marks: Vec::new(),
        episodes: Vec::new(),
    }
}

fn insert_event(
    connection: &rusqlite::Connection,
    session_id: &PaperSessionId,
    event: &PaperSessionEvent,
) -> Result<(), LabError> {
    connection
        .execute(
            "INSERT INTO paper_session_events(session_id,sequence,market,kind,occurred_at_utc,event_json) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                session_id.as_str(),
                i64::try_from(event.sequence).map_err(|_| LabError::ResourceLimit("fixture sequence".into()))?,
                event.market.as_ref().map(MarketId::code),
                "CANDLE_PROCESSED",
                time("2026-01-01T00:00:00Z").to_rfc3339(),
                serde_json::to_string(event).map_err(super::super::super::json_error)?,
            ],
        )
        .map_err(super::super::super::sql_error)?;
    Ok(())
}

#[test]
fn lifecycle_lookup_links_later_partial_fill_at_the_fixed_prefix_and_excludes_future_fill()
-> Result<(), LabError> {
    let connection =
        rusqlite::Connection::open_in_memory().map_err(super::super::super::sql_error)?;
    connection
        .execute_batch(
            "CREATE TABLE paper_session_events(\
                session_id TEXT NOT NULL,sequence INTEGER NOT NULL,market TEXT,kind TEXT NOT NULL,\
                occurred_at_utc TEXT NOT NULL,event_json TEXT NOT NULL,PRIMARY KEY(session_id,sequence));",
        )
        .map_err(super::super::super::sql_error)?;
    let session_id = PaperSessionId::new("review-session").expect("session id");
    let signal = signal();
    let created = order(&signal, "review-order", 2, OrderStatus::Created, "0");
    let partial = order(
        &signal,
        "review-order",
        3,
        OrderStatus::PartiallyFilled,
        "0.5",
    );
    let first_fill = fill(&partial, "review-fill-1", 4, "0.5");
    let completed = order(&signal, "review-order", 5, OrderStatus::Filled, "1");
    let future_fill = fill(&completed, "review-fill-2", 6, "0.5");
    insert_event(
        &connection,
        &session_id,
        &event(
            1,
            vec![signal.clone()],
            vec![created],
            Vec::new(),
            Vec::new(),
        ),
    )?;
    insert_event(
        &connection,
        &session_id,
        &event(2, Vec::new(), Vec::new(), vec![partial], vec![first_fill]),
    )?;
    insert_event(
        &connection,
        &session_id,
        &event(
            3,
            Vec::new(),
            Vec::new(),
            vec![completed],
            vec![future_fill],
        ),
    )?;
    let selection = PaperReviewSelection {
        session_id: session_id.clone(),
        event_sequence: 1,
        as_of_sequence: 2,
        signal_id: Some(signal.signal_id.clone()),
    };
    let changes_before = connection.total_changes();
    connection
        .execute_batch("PRAGMA query_only=ON")
        .map_err(super::super::super::sql_error)?;
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;
    let (orders, fills) = linked_paper_lifecycle_facts(&transaction, &selection, &signal)?;
    transaction
        .commit()
        .map_err(super::super::super::sql_error)?;

    assert_eq!(changes_before, connection.total_changes());
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::PartiallyFilled);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_str(), "review-fill-1");
    Ok(())
}

#[test]
fn linked_lifecycle_fact_limit_fails_closed_instead_of_truncating() -> Result<(), LabError> {
    let connection =
        rusqlite::Connection::open_in_memory().map_err(super::super::super::sql_error)?;
    connection
        .execute_batch(
            "CREATE TABLE paper_session_events(\
                session_id TEXT NOT NULL,sequence INTEGER NOT NULL,market TEXT,kind TEXT NOT NULL,\
                occurred_at_utc TEXT NOT NULL,event_json TEXT NOT NULL,PRIMARY KEY(session_id,sequence));",
        )
        .map_err(super::super::super::sql_error)?;
    let session_id = PaperSessionId::new("review-session").expect("session id");
    let signal = signal();
    let orders = (0..=MAX_LINKED_PAPER_ORDER_FACTS)
        .map(|index| {
            order(
                &signal,
                &format!("review-order-{index}"),
                u64::try_from(index + 2).expect("sequence"),
                OrderStatus::Created,
                "0",
            )
        })
        .collect();
    insert_event(
        &connection,
        &session_id,
        &event(1, vec![signal.clone()], orders, Vec::new(), Vec::new()),
    )?;
    let selection = PaperReviewSelection {
        session_id: session_id.clone(),
        event_sequence: 1,
        as_of_sequence: 1,
        signal_id: Some(signal.signal_id.clone()),
    };
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;
    assert!(matches!(
        linked_paper_lifecycle_facts(&transaction, &selection, &signal),
        Err(LabError::ResourceLimit(_))
    ));
    Ok(())
}

#[test]
fn lifecycle_fold_uses_internal_order_sequence_for_final_state_and_fill_causality()
-> Result<(), LabError> {
    let connection =
        rusqlite::Connection::open_in_memory().map_err(super::super::super::sql_error)?;
    connection
        .execute_batch(
            "CREATE TABLE paper_session_events(\
                session_id TEXT NOT NULL,sequence INTEGER NOT NULL,market TEXT,kind TEXT NOT NULL,\
                occurred_at_utc TEXT NOT NULL,event_json TEXT NOT NULL,PRIMARY KEY(session_id,sequence));",
        )
        .map_err(super::super::super::sql_error)?;
    let session_id = PaperSessionId::new("review-session").expect("session id");
    let mut signal = signal();
    signal.outcome = SignalOutcome::OrderUnfilled;
    signal.reasons.clear();
    let created = order(&signal, "review-order", 2, OrderStatus::Created, "0");
    let partial = order(
        &signal,
        "review-order",
        3,
        OrderStatus::PartiallyFilled,
        "0.5",
    );
    let cancelled = order(&signal, "review-order", 5, OrderStatus::Cancelled, "0.5");
    let partial_fill = fill(&partial, "review-fill", 4, "0.5");
    insert_event(
        &connection,
        &session_id,
        &event(
            1,
            vec![signal.clone()],
            vec![cancelled],
            vec![created, partial],
            vec![partial_fill],
        ),
    )?;
    let selection = PaperReviewSelection {
        session_id,
        event_sequence: 1,
        as_of_sequence: 1,
        signal_id: Some(signal.signal_id.clone()),
    };
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;
    let (orders, fills) = linked_paper_lifecycle_facts(&transaction, &selection, &signal)?;
    transaction
        .commit()
        .map_err(super::super::super::sql_error)?;

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::Cancelled);
    assert_eq!(fills.len(), 1);
    let explanation = crate::reporting::decision_explanation::paper_recorded_decision(
        Some(&signal),
        None,
        &orders,
        &fills,
    )?;
    assert_eq!(
        explanation.execution_state,
        crate::contracts::RecordedExecutionState::PartiallyFilled
    );
    assert!(
        !explanation
            .reasons
            .iter()
            .any(|reason| reason.code == "ORDER_UNFILLED")
    );
    Ok(())
}

#[test]
fn lifecycle_scan_event_budget_fails_before_decoding_unrelated_events() -> Result<(), LabError> {
    let connection =
        rusqlite::Connection::open_in_memory().map_err(super::super::super::sql_error)?;
    connection
        .execute_batch(
            "CREATE TABLE paper_session_events(\
                session_id TEXT NOT NULL,sequence INTEGER NOT NULL,market TEXT,kind TEXT NOT NULL,\
                occurred_at_utc TEXT NOT NULL,event_json TEXT NOT NULL,PRIMARY KEY(session_id,sequence));",
        )
        .map_err(super::super::super::sql_error)?;
    let session_id = PaperSessionId::new("review-session").expect("session id");
    let signal = signal();
    let event_count = MAX_LINKED_PAPER_SCAN_EVENTS + 1;
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;
    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO paper_session_events(session_id,sequence,kind,occurred_at_utc,event_json) \
                 VALUES (?1,?2,'CANDLE_PROCESSED',?3,'not-json')",
            )
            .map_err(super::super::super::sql_error)?;
        for sequence in 1..=event_count {
            statement
                .execute(rusqlite::params![
                    session_id.as_str(),
                    i64::try_from(sequence).map_err(|_| {
                        LabError::ResourceLimit("fixture sequence overflow".into())
                    })?,
                    time("2026-01-01T00:00:00Z").to_rfc3339(),
                ])
                .map_err(super::super::super::sql_error)?;
        }
    }
    transaction
        .commit()
        .map_err(super::super::super::sql_error)?;
    let selection = PaperReviewSelection {
        session_id,
        event_sequence: 1,
        as_of_sequence: u64::try_from(event_count)
            .map_err(|_| LabError::ResourceLimit("fixture cutoff overflow".into()))?,
        signal_id: Some(signal.signal_id.clone()),
    };
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;

    assert!(matches!(
        linked_paper_lifecycle_facts(&transaction, &selection, &signal),
        Err(LabError::ResourceLimit(_))
    ));
    Ok(())
}

#[test]
fn lifecycle_scan_total_byte_budget_fails_before_json_each() -> Result<(), LabError> {
    let connection =
        rusqlite::Connection::open_in_memory().map_err(super::super::super::sql_error)?;
    connection
        .execute_batch(
            "CREATE TABLE paper_session_events(\
                session_id TEXT NOT NULL,sequence INTEGER NOT NULL,market TEXT,kind TEXT NOT NULL,\
                occurred_at_utc TEXT NOT NULL,event_json TEXT NOT NULL,PRIMARY KEY(session_id,sequence));",
        )
        .map_err(super::super::super::sql_error)?;
    let session_id = PaperSessionId::new("review-session").expect("session id");
    let signal = signal();
    let empty = serde_json::json!({
        "orders": [],
        "order_events": [],
        "fills": [],
        "padding": "",
    })
    .to_string();
    let padding_len = crate::contracts::MAX_PAPER_REVIEW_EVENT_BYTES
        .checked_sub(empty.len() + 1)
        .ok_or_else(|| LabError::ResourceLimit("fixture event size underflow".into()))?;
    let raw = serde_json::json!({
        "orders": [],
        "order_events": [],
        "fills": [],
        "padding": "a".repeat(padding_len),
    })
    .to_string();
    if raw.len() > crate::contracts::MAX_PAPER_REVIEW_EVENT_BYTES {
        return Err(LabError::Internal(
            "fixture event exceeds per-event byte cap".into(),
        ));
    }
    let count = MAX_LINKED_PAPER_SCAN_BYTES
        .checked_div(raw.len())
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| LabError::ResourceLimit("fixture event count overflow".into()))?;
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;
    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO paper_session_events(session_id,sequence,kind,occurred_at_utc,event_json) \
                 VALUES (?1,?2,'CANDLE_PROCESSED',?3,?4)",
            )
            .map_err(super::super::super::sql_error)?;
        for sequence in 1..=count {
            statement
                .execute(rusqlite::params![
                    session_id.as_str(),
                    i64::try_from(sequence).map_err(|_| {
                        LabError::ResourceLimit("fixture sequence overflow".into())
                    })?,
                    time("2026-01-01T00:00:00Z").to_rfc3339(),
                    raw,
                ])
                .map_err(super::super::super::sql_error)?;
        }
    }
    transaction
        .commit()
        .map_err(super::super::super::sql_error)?;
    let selection = PaperReviewSelection {
        session_id,
        event_sequence: 1,
        as_of_sequence: u64::try_from(count)
            .map_err(|_| LabError::ResourceLimit("fixture cutoff overflow".into()))?,
        signal_id: Some(signal.signal_id.clone()),
    };
    let transaction = connection
        .unchecked_transaction()
        .map_err(super::super::super::sql_error)?;

    assert!(matches!(
        linked_paper_lifecycle_facts(&transaction, &selection, &signal),
        Err(LabError::ResourceLimit(_))
    ));
    Ok(())
}
