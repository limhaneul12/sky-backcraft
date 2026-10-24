use super::{Replay, created_order_parent_is_causal, passive_fill_matches_limit};
use crate::contracts::{
    AssetQuantity, BasisPoints, EpisodeId, EventContext, FillId, FillRecord, FillTiming, MarketId,
    ModelId, ObservationId, OrderId, OrderRecord, OrderStatus, PriceKrw, QuoteAmount, ReasonCode,
    RuleSnapshotId, RunId, Side, SignalId, SignedAmount, SimulatedOrderType, UtcTimestamp,
};
use chrono::Duration;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

fn dec(value: &str) -> Decimal {
    value.parse().expect("literal decimal")
}

#[test]
fn hand_calculated_buy_sell_fee_basis_mark_and_reservation_release() {
    let mut replay = Replay::new(dec("1000"));
    let buy_order = hand_order("hand-buy", Side::Buy, "2", "200.2");
    replay
        .reservations
        .insert(buy_order.order_id.clone(), dec("200.2"));
    replay.refresh_reserved().expect("initial reserve sum");

    replay
        .apply_exact_economic_fill(
            &buy_order,
            &hand_fill(
                &buy_order,
                "hand-buy-fill",
                Side::Buy,
                ["2", "100", "200", "0.2", "10"],
            ),
        )
        .expect("hand-calculated buy");
    assert_eq!(replay.cash, dec("799.8"));
    assert_eq!(replay.reserved, Decimal::ZERO);
    assert_eq!(replay.quantity, dec("2"));
    assert_eq!(replay.basis, dec("200"));
    assert_eq!(replay.realized, Decimal::ZERO);
    assert_eq!(replay.fees, dec("0.2"));

    let sell_order = hand_order("hand-sell", Side::Sell, "0.5", "0");
    replay
        .apply_exact_economic_fill(
            &sell_order,
            &hand_fill(
                &sell_order,
                "hand-sell-fill",
                Side::Sell,
                ["0.5", "120", "60", "0.06", "10"],
            ),
        )
        .expect("hand-calculated partial position sale");
    assert_eq!(replay.cash, dec("859.74"));
    assert_eq!(replay.reserved, Decimal::ZERO);
    assert_eq!(replay.quantity, dec("1.5"));
    assert_eq!(replay.basis, dec("150"));
    assert_eq!(replay.realized, dec("10"));
    assert_eq!(replay.fees, dec("0.26"));

    let position_value = replay
        .quantity
        .checked_mul(dec("110"))
        .expect("mark position value");
    let gross_unrealized = position_value
        .checked_sub(replay.basis)
        .expect("mark unrealized PnL");
    let equity = replay
        .cash
        .checked_add(position_value)
        .expect("mark equity");
    assert_eq!(position_value, dec("165"));
    assert_eq!(gross_unrealized, dec("15"));
    assert_eq!(equity, dec("1024.74"));
    assert_eq!(
        dec("1000") + replay.realized + gross_unrealized - replay.fees,
        equity
    );
}

#[test]
fn hand_calculated_fold_rejects_a_fee_tamper_exactly() {
    let mut replay = Replay::new(dec("1000"));
    let buy_order = hand_order("hand-fee-tamper", Side::Buy, "2", "200.21");
    let result = replay.apply_exact_economic_fill(
        &buy_order,
        &hand_fill(
            &buy_order,
            "hand-fee-tamper-fill",
            Side::Buy,
            ["2", "100", "200", "0.21", "10"],
        ),
    );
    assert_eq!(result, Err("FILL_FEE_MISMATCH"));
}

#[test]
fn partial_buy_cannot_spend_more_than_remaining_reservation() {
    let mut replay = Replay::new(dec("1000"));
    let order = hand_order("hand-partial-buy", Side::Buy, "2", "201");
    replay
        .reservations
        .insert(order.order_id.clone(), dec("201"));
    replay.refresh_reserved().expect("initial reserve sum");

    replay
        .apply_exact_economic_fill(
            &order,
            &hand_fill(
                &order,
                "hand-partial-buy-1",
                Side::Buy,
                ["1", "100", "100", "0.5", "50"],
            ),
        )
        .expect("first partial buy");
    assert_eq!(replay.reserved, dec("100.5"));

    let final_fill = replay.apply_exact_economic_fill(
        &order,
        &hand_fill(
            &order,
            "hand-partial-buy-2",
            Side::Buy,
            ["1", "150", "150", "0.75", "50"],
        ),
    );
    assert_eq!(final_fill, Err("BUY_FILL_EXCEEDS_RESERVATION"));
}

#[test]
fn passive_limit_requires_buy_side_and_exact_requested_price() {
    let limit = PriceKrw::new(dec("100")).expect("limit price");
    let above_limit = PriceKrw::new(dec("100.01")).expect("fill price");
    assert!(passive_fill_matches_limit(
        SimulatedOrderType::PassiveBuyLimit,
        Side::Buy,
        Some(limit),
        Side::Buy,
        limit,
    ));
    assert!(!passive_fill_matches_limit(
        SimulatedOrderType::PassiveBuyLimit,
        Side::Buy,
        Some(limit),
        Side::Buy,
        above_limit,
    ));
    assert!(!passive_fill_matches_limit(
        SimulatedOrderType::PassiveBuyLimit,
        Side::Sell,
        Some(limit),
        Side::Sell,
        limit,
    ));
}

#[test]
fn unfilled_requested_order_may_be_rejected_and_sell_cannot_reserve_cash() {
    let mut replay = Replay::new(dec("1000"));
    let created = hand_order("hand-rejected-request", Side::Buy, "2", "201");
    let mut rejected = created.clone();
    rejected.context.event_seq = 2;
    rejected.status = OrderStatus::Rejected;
    rejected.reason = ReasonCode::RuleBlocked;
    replay
        .apply_order_event(&created)
        .expect("created requested order");
    replay
        .apply_order_event(&rejected)
        .expect("engine can reject a nonzero requested order before fill");
    assert_eq!(replay.reserved, Decimal::ZERO);
    assert_eq!(replay.cash, dec("1000"));
    assert!(!created_order_parent_is_causal(&created, &BTreeMap::new()));

    let mut sell_replay = Replay::new(dec("1000"));
    let sell = hand_order("hand-reserved-sell", Side::Sell, "1", "1");
    assert_eq!(
        sell_replay.apply_order_event(&sell),
        Err("SELL_ORDER_RESERVES_CASH")
    );
}

fn hand_order(id: &str, side: Side, requested_qty: &str, reserved_cash: &str) -> OrderRecord {
    let created_at = UtcTimestamp::parse_rfc3339("2025-01-01T00:00:00Z").expect("timestamp");
    OrderRecord {
        context: EventContext {
            run_id: RunId::new("run-hand-order").expect("run ID"),
            model_id: ModelId::new("model-hand-order").expect("model ID"),
            market: MarketId::parse_upbit("KRW-BTC").expect("market ID"),
            event_seq: 1,
            accounting_event_time: created_at,
        },
        order_id: OrderId::new(id).expect("order ID"),
        parent_signal_id: SignalId::new("signal-hand-order").expect("signal ID"),
        episode_id: None,
        side,
        order_type: SimulatedOrderType::Market,
        requested_price: None,
        requested_qty: AssetQuantity::new(dec(requested_qty)).expect("requested quantity"),
        created_at,
        effective_at: created_at,
        expires_at: UtcTimestamp(created_at.0 + Duration::minutes(5)),
        rule_snapshot_id: RuleSnapshotId::new("rule-hand-order").expect("rule ID"),
        policy_version: "HAND_FIXTURE".into(),
        reserved_cash: QuoteAmount::new(dec(reserved_cash)).expect("reserved cash"),
        cumulative_filled_qty: AssetQuantity::new(Decimal::ZERO).expect("zero quantity"),
        status: OrderStatus::Created,
        reason: ReasonCode::MarketFilled,
        order_origin: "SIMULATED".into(),
    }
}

fn hand_fill(order: &OrderRecord, id: &str, side: Side, economics: [&str; 5]) -> FillRecord {
    let [qty, price, notional, fee, fee_bps] = economics;
    let at = UtcTimestamp(order.context.accounting_event_time.0 + Duration::minutes(1));
    let mut context = order.context.clone();
    context.event_seq = context.event_seq.saturating_add(1);
    context.accounting_event_time = at;
    FillRecord {
        context,
        fill_id: FillId::new(id).expect("fill ID"),
        order_id: order.order_id.clone(),
        episode_id: EpisodeId::new("episode-hand-order").expect("episode ID"),
        side,
        price: PriceKrw::new(dec(price)).expect("fill price"),
        qty: AssetQuantity::new(dec(qty)).expect("fill quantity"),
        notional: QuoteAmount::new(dec(notional)).expect("notional"),
        fee: QuoteAmount::new(dec(fee)).expect("fee"),
        fee_bps: BasisPoints::new(dec(fee_bps)).expect("fee rate"),
        fee_currency: "KRW".into(),
        timing: FillTiming::Exact { at },
        source_bar_id: ObservationId::new("obs-hand-source").expect("source ID"),
        liquidity_source_bar_id: ObservationId::new("obs-hand-liquidity").expect("liquidity ID"),
        liquidity_source_close_time: at,
        decision_reference: PriceKrw::new(dec(price)).expect("decision reference"),
        bar_open_proxy: PriceKrw::new(dec(price)).expect("bar open"),
        arrival_mid: None,
        arrival_mid_null_reason: "DATA_NOT_CAPTURED".into(),
        adverse_slippage_bps: SignedAmount::new(Decimal::ZERO).expect("zero slippage"),
        price_cost_attribution: SignedAmount::new(Decimal::ZERO).expect("zero price cost"),
        liquidity_role_assumption: "HAND_FIXTURE".into(),
        execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false,
        model_version: "HAND_FIXTURE".into(),
        artificial_terminal_exit: false,
        cost_provenance: None,
        accounting_mark_seq: order.context.event_seq.saturating_add(2),
    }
}
