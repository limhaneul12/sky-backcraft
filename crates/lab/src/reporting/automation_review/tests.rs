use super::*;
use crate::contracts::*;
use rust_decimal::Decimal;
use serde_json::json;
use std::str::FromStr;

fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).expect("fixture decimal")
}
fn ts(hour: u32, minute: u32) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(&format!("2026-01-01T{hour:02}:{minute:02}:00Z"))
        .expect("fixture timestamp")
}

#[expect(
    clippy::too_many_lines,
    reason = "One explicit hand-computed fixture spells out linked typed candles, partial fills, fees and opening evidence"
)]
fn fixture() -> AutomationReviewSnapshot {
    let (policy_id, family, definition) = builtin_policy_definitions().expect("seeds").remove(0);
    let policy_ref = PolicyRevisionRef {
        policy_id,
        revision_id: PolicyRevisionId::from_seed("review"),
        definition_digest: ContentHash::of_value(&definition).expect("digest"),
    };
    let session = AutomationReviewSession {
        session_id: AutomationSessionId::from_seed("review"), mode: AutomationSessionMode::Mock,
        state: AutomationSessionState::Stopped, policy_ref: policy_ref.clone(),
        market: MarketId::parse_upbit("KRW-BTC").expect("market"), decision_interval: CandleInterval::H1,
        risk: serde_json::from_value(json!({"max_order_notional_krw":"1000","max_daily_notional_krw":"10000","max_asset_exposure_krw":"10000","max_daily_loss_krw":"1000","max_open_orders":3,"max_orders_per_interval":3,"max_balance_age_seconds":30,"max_feed_age_seconds":30})).expect("risk"),
        created_at_utc: ts(0,0), updated_at_utc: ts(3,0),
    };
    let frozen_policy = FrozenPolicyRevision {
        reference: policy_ref,
        revision_number: 1,
        parent_revision_id: None,
        family,
        origin: PolicyOrigin::Builtin,
        definition,
    };
    let mut snapshot = AutomationReviewSnapshot {
        session,
        frozen_policy,
        candles: vec![],
        decisions: vec![],
        orders: vec![],
        fills: vec![],
        fee_accruals: vec![],
        events: vec![],
        opening_inventory: Some(AutomationReviewOpeningInventory {
            snapshot_sequence: 1,
            source_mode: AutomationSessionMode::Mock,
            observed_at_utc: UtcTimestamp(ts(1, 0).0 - chrono::Duration::seconds(5)),
            base_quantity: AssetQuantity::new(Decimal::ZERO).expect("opening quantity"),
            balances_digest: ContentHash::of_value(&opening_balances()).expect("opening digest"),
        }),
    };
    for (hour, price, action) in [
        (1, "100", AutomationReviewAction::Buy),
        (2, "150", AutomationReviewAction::Sell),
        (3, "140", AutomationReviewAction::Hold),
    ] {
        let candle = CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: ts(hour - 1, 0),
            close_time_utc: ts(hour, 0),
            open: PriceKrw::new(dec(price)).expect("price"),
            high: PriceKrw::new(dec(price)).expect("price"),
            low: PriceKrw::new(dec(price)).expect("price"),
            close: PriceKrw::new(dec(price)).expect("price"),
            volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
            quote_turnover: QuoteAmount::new(dec(price)).expect("turnover"),
            completed: true,
        };
        let digest = observation_digest(&candle).expect("digest");
        let observation = CandleObservation {
            id: ObservationId::from_seed(digest.as_str()),
            candle,
            content_digest: digest,
            raw_object_ids: vec![],
            constituent_ids: vec![],
        };
        let evaluation = json!({"decision_time_utc":ts(hour,0),"policy_ref":snapshot.session.policy_ref,"reasons":["POLICY_RULE_MATCHED"],"indicators":{"close":price},"policy_trace":{"matched_rule_id":"entry","state_before":[],"state_after":[]}});
        snapshot.decisions.push(AutomationDecisionRecord {
            session_id: snapshot.session.session_id.clone(),
            intent_id: AutomationIntentId::from_seed(&format!("intent-{hour}")),
            observation_id: observation.id.clone(),
            decision_time_utc: ts(hour, 0),
            recorded_at_utc: ts(hour, 0),
            action,
            state: if hour == 3 {
                "NO_ACTION"
            } else {
                "ORDER_RECONCILED"
            }
            .into(),
            candle: observation,
            evaluation_digest: ContentHash::of_value(&evaluation).expect("eval digest"),
            evaluation,
        });
    }
    for (hour, side, funds, qty, fee, count) in [
        (1, AutomationSide::Bid, "200", "2", "2", 2),
        (2, AutomationSide::Ask, "150", "1", "1.5", 1),
    ] {
        let intent = snapshot.decisions[(hour - 1) as usize].intent_id.clone();
        let id = AutomationOrderIdentifier::from_intent(&intent);
        let order = AutomationOrderView {
            client_identifier: id.clone(),
            intent_id: intent,
            market: snapshot.session.market.clone(),
            side,
            state: AutomationOrderState::Filled,
            venue_order_uuid: Some(format!("mock-order-{hour}")),
            requested_notional_krw: QuoteAmount::new(dec(funds)).expect("funds"),
            reserved_fee_krw: QuoteAmount::new(dec(fee)).expect("fee"),
            executed_volume: AssetQuantity::new(dec(qty)).expect("qty"),
            paid_fee_krw: QuoteAmount::new(dec(fee)).expect("fee"),
            locked_quote_krw: None,
            locked_base: None,
            trade_count: count,
            created_at_utc: ts(hour, 0),
            updated_at_utc: ts(hour, 1),
        };
        snapshot.orders.push(AutomationReviewOrder {
            order,
            decision_mark_krw: PriceKrw::new(dec(if hour == 1 { "100" } else { "150" }))
                .expect("mark"),
            venue_created_at_utc: Some(ts(hour, 0)),
        });
        snapshot.fee_accruals.push(AutomationReviewFee {
            client_identifier: id.clone(),
            sequence: 1,
            delta_paid_fee_krw: QuoteAmount::new(dec(fee)).expect("fee"),
            cumulative_paid_fee_krw: QuoteAmount::new(dec(fee)).expect("fee"),
            observed_at_utc: ts(hour, 1),
        });
        for index in 0..count {
            let trade = AutomationFillView {
                trade_uuid: format!("mock-trade-{hour}-{index}"),
                client_identifier: id.clone(),
                market: snapshot.session.market.clone(),
                side,
                price_krw: PriceKrw::new(dec(if hour == 1 { "100" } else { "150" }))
                    .expect("price"),
                volume: AssetQuantity::new(Decimal::ONE).expect("qty"),
                funds_krw: QuoteAmount::new(dec(if hour == 1 { "100" } else { "150" }))
                    .expect("funds"),
                observed_at_utc: ts(hour, 1),
            };
            snapshot.fills.push(AutomationReviewFill {
                fill: trade,
                stored_trade_time_utc: ts(hour, 1),
                recorded_at_utc: ts(hour, 1),
                source_digest: ContentHash::of_bytes(b"synthetic mock fill"),
            });
        }
    }
    let mut warmup = snapshot.decisions[0].candle.clone();
    warmup.candle.open_time_utc =
        UtcTimestamp(warmup.candle.open_time_utc.0 - chrono::Duration::hours(1));
    warmup.candle.close_time_utc =
        UtcTimestamp(warmup.candle.close_time_utc.0 - chrono::Duration::hours(1));
    warmup.content_digest = observation_digest(&warmup.candle).expect("warmup digest");
    warmup.id = ObservationId::from_seed(warmup.content_digest.as_str());
    snapshot.candles.push(warmup);
    snapshot
        .candles
        .extend(snapshot.decisions.iter().map(|d| d.candle.clone()));
    refresh_events(&mut snapshot);
    snapshot
}

fn refresh_events(s: &mut AutomationReviewSnapshot) {
    s.events.clear();
    for candle in &s.candles {
        s.events.push(AutomationEvent {session_id:s.session.session_id.clone(),sequence:u64::try_from(s.events.len()+1).expect("sequence"),kind:AutomationEventKind::CandleObserved,at_utc:candle.candle.close_time_utc,detail:json!({"observation_id":candle.id,"close_time_utc":candle.candle.close_time_utc,"content_digest":candle.content_digest})});
        for d in s.decisions.iter().filter(|d| d.observation_id == candle.id) {
            s.events.push(AutomationEvent {session_id:s.session.session_id.clone(),sequence:u64::try_from(s.events.len()+1).expect("sequence"),kind:AutomationEventKind::SignalObserved,at_utc:d.recorded_at_utc,detail:json!({"intent_id":d.intent_id,"action":d.action,"decision_time_utc":d.decision_time_utc,"evaluation_digest":d.evaluation_digest})});
        }
    }
}

#[test]
fn hand_computed_partial_fill_cost_basis_is_not_cashflow() {
    // Buy 2*100 plus fee2; sell 1*150 less fee1.5. Cost/unit101.
    let report = build_performance(&fixture()).expect("report");
    assert_eq!(
        report.realized_pnl_krw.value.expect("realized").get(),
        dec("47.5")
    );
    assert_eq!(
        report.unrealized_pnl_krw.value.expect("marked").get(),
        dec("39")
    );
    assert_eq!(
        report.total_pnl_krw.value.expect("total").get(),
        dec("86.5")
    );
    assert_eq!(report.net_quote_cashflow_krw.get(), dec("-53.5"));
    assert_eq!(
        report.remaining_volume.expect("inventory").get(),
        Decimal::ONE
    );
    assert_eq!(report.remaining_cost_krw.expect("cost").get(), dec("101"));
    assert_eq!(
        report.mark_observation_id,
        Some(fixture().decisions[2].candle.id.clone())
    );
    assert_eq!(
        report.mark_content_digest,
        Some(fixture().decisions[2].candle.content_digest.clone())
    );
    assert!(report.return_ratio.value.is_none());
    assert!(report.max_drawdown_ratio.unavailable_reason.is_some());
}

#[test]
fn uneven_fill_fee_residual_and_weighted_average_conserve_exact_total_cost() {
    let mut s = fixture();
    // Buy 1@100 + 1@200, total fee1: fee shares are 1/3 and final residual.
    s.fills[1].fill.price_krw = PriceKrw::new(dec("200")).expect("price");
    s.fills[1].fill.funds_krw = QuoteAmount::new(dec("200")).expect("funds");
    s.orders[0].order.requested_notional_krw = QuoteAmount::new(dec("300")).expect("funds");
    s.orders[0].order.paid_fee_krw = QuoteAmount::new(Decimal::ONE).expect("fee");
    s.fee_accruals[0].delta_paid_fee_krw = QuoteAmount::new(Decimal::ONE).expect("fee");
    s.fee_accruals[0].cumulative_paid_fee_krw = QuoteAmount::new(Decimal::ONE).expect("fee");
    s.fills[2].fill.price_krw = PriceKrw::new(dec("180")).expect("price");
    s.fills[2].fill.funds_krw = QuoteAmount::new(dec("180")).expect("funds");
    s.orders[1].order.paid_fee_krw = QuoteAmount::new(dec("0.18")).expect("fee");
    s.fee_accruals[1].delta_paid_fee_krw = QuoteAmount::new(dec("0.18")).expect("fee");
    s.fee_accruals[1].cumulative_paid_fee_krw = QuoteAmount::new(dec("0.18")).expect("fee");
    let partial = build_performance(&s).expect("weighted report");
    assert_eq!(
        partial.realized_pnl_krw.value.expect("realized").get(),
        dec("29.32")
    );
    assert_eq!(
        partial.remaining_cost_krw.expect("cost").get(),
        dec("150.5")
    );
    // Fully close both units. 360 proceeds - 300 funds - 1 buy fee - .36 sell fee.
    s.fills[2].fill.volume = AssetQuantity::new(dec("2")).expect("volume");
    s.fills[2].fill.funds_krw = QuoteAmount::new(dec("360")).expect("funds");
    s.orders[1].order.executed_volume = AssetQuantity::new(dec("2")).expect("volume");
    s.orders[1].order.paid_fee_krw = QuoteAmount::new(dec("0.36")).expect("fee");
    s.fee_accruals[1].delta_paid_fee_krw = QuoteAmount::new(dec("0.36")).expect("fee");
    s.fee_accruals[1].cumulative_paid_fee_krw = QuoteAmount::new(dec("0.36")).expect("fee");
    let flat = build_performance(&s).expect("flat report");
    assert_eq!(
        flat.realized_pnl_krw.value.expect("realized").get(),
        dec("58.64")
    );
    assert_eq!(flat.net_quote_cashflow_krw.get(), dec("58.64"));
    assert_eq!(flat.remaining_cost_krw.expect("cost").get(), Decimal::ZERO);
    assert_eq!(flat.paid_fees_krw.get(), dec("1.36"));
}

#[test]
fn unknown_opening_inventory_and_unreconciled_orders_do_not_invent_profit() {
    let mut missing = fixture();
    missing.opening_inventory = None;
    assert_eq!(
        build_performance(&missing)
            .expect("unknown opening")
            .realized_pnl_krw
            .unavailable_reason
            .as_deref(),
        Some("OPENING_INVENTORY_UNVERIFIED")
    );
    let mut owned_and_inherited = fixture();
    owned_and_inherited
        .opening_inventory
        .as_mut()
        .expect("reference")
        .base_quantity = AssetQuantity::new(Decimal::ONE).expect("inherited");
    assert!(
        build_performance(&owned_and_inherited)
            .expect("inherited reference")
            .total_pnl_krw
            .value
            .is_none()
    );
    let mut inherited = fixture();
    inherited
        .fills
        .retain(|f| f.fill.side == AutomationSide::Ask);
    inherited
        .orders
        .retain(|o| o.order.side == AutomationSide::Ask);
    inherited
        .fee_accruals
        .retain(|f| f.client_identifier == inherited.orders[0].order.client_identifier);
    inherited
        .opening_inventory
        .as_mut()
        .expect("reference")
        .observed_at_utc = UtcTimestamp(ts(2, 0).0 - chrono::Duration::seconds(5));
    let report = build_performance(&inherited).expect("report with missing basis");
    assert!(report.realized_pnl_krw.value.is_none());
    assert_eq!(
        report.realized_pnl_krw.unavailable_reason.as_deref(),
        Some("OPENING_INVENTORY_COST_UNKNOWN")
    );
    let mut pending = fixture();
    pending.orders[0].order.state = AutomationOrderState::PartiallyFilled;
    assert!(
        build_performance(&pending)
            .expect("provisional report")
            .total_pnl_krw
            .value
            .is_none()
    );
    let mut no_mark = fixture();
    no_mark.decisions.pop();
    no_mark.candles.pop();
    no_mark
        .fills
        .last_mut()
        .expect("sell fill")
        .stored_trade_time_utc = ts(2, 0);
    refresh_events(&mut no_mark);
    assert!(
        build_performance(&no_mark)
            .expect("unmarked report")
            .unrealized_pnl_krw
            .value
            .is_none()
    );
}

#[test]
fn export_chunks_pin_snapshot_and_preserve_csv_text_safely() {
    let mut snapshot = fixture();
    snapshot.decisions[0].evaluation["reasons"] = json!(["=HYPERLINK(\"bad\")\nnext,line"]);
    snapshot.decisions[0].evaluation_digest =
        ContentHash::of_value(&snapshot.decisions[0].evaluation).expect("digest");
    refresh_events(&mut snapshot);
    let manifest = export_manifest(&snapshot).expect("manifest");
    assert_eq!(manifest.files.len(), 6);
    let csv = manifest
        .files
        .iter()
        .find(|f| f.file == AutomationReviewFile::DecisionsCsv)
        .expect("csv");
    let chunk = export_read(
        &snapshot,
        csv.file,
        0,
        &csv.sha256,
        &manifest.snapshot_digest,
    )
    .expect("read");
    let bytes = decode_hex(&chunk.data_hex);
    assert_eq!(ContentHash::of_bytes(&bytes), csv.sha256);
    let text = String::from_utf8(bytes).expect("utf8");
    assert!(text.contains("'=HYPERLINK"));
    assert!(text.contains("\r\n"));
    snapshot.session.updated_at_utc = ts(4, 0);
    assert!(matches!(
        export_read(
            &snapshot,
            csv.file,
            0,
            &csv.sha256,
            &manifest.snapshot_digest
        ),
        Err(LabError::InputHashMismatch(_))
    ));
}

fn decode_hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).expect("ascii"), 16).expect("hex"))
        .collect()
}

#[test]
fn persisted_evaluation_and_candle_corruption_are_rejected() {
    let mut noncanonical = fixture();
    noncanonical.decisions[0].observation_id = ObservationId::from_seed("noncanonical");
    noncanonical.decisions[0].candle.id = noncanonical.decisions[0].observation_id.clone();
    assert!(matches!(
        build_performance(&noncanonical),
        Err(LabError::DataCorrupt(_))
    ));
    let mut s = fixture();
    s.decisions[0].evaluation["indicators"] = json!({"changed":42});
    assert!(matches!(
        build_performance(&s),
        Err(LabError::DataCorrupt(_))
    ));
    let mut s = fixture();
    s.decisions[0].candle.candle.completed = false;
    assert!(matches!(export_manifest(&s), Err(LabError::DataCorrupt(_))));
}

#[test]
#[ignore = "explicit synthetic review wire-fixture export; never operating data"]
fn export_review_wire_fixture() {
    let output = std::env::var("SKY_BACKCRAFT_REVIEW_FIXTURE_OUTPUT")
        .expect("explicit fixture output directory");
    let output = std::path::Path::new(&output);
    assert!(!output.exists(), "never replace fixture output");
    std::fs::create_dir_all(output).expect("fixture output directory");
    let snapshot = fixture();
    let manifest = export_manifest(&snapshot).expect("manifest");
    let detail = AutomationDecisionDetail {
        session: snapshot.session.clone(),
        frozen_policy: snapshot.frozen_policy.clone(),
        decision: snapshot.decisions[0].clone(),
        context_candles: snapshot
            .candles
            .iter()
            .filter(|c| c.candle.close_time_utc <= snapshot.decisions[0].decision_time_utc)
            .cloned()
            .collect(),
        orders: vec![snapshot.orders[0].order.clone()],
        events: snapshot
            .events
            .iter()
            .filter(|event| {
                event
                    .detail
                    .get("intent_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(snapshot.decisions[0].intent_id.as_str())
            })
            .cloned()
            .collect(),
    };
    let page = AutomationDecisionPage {
        session_id: snapshot.session.session_id.clone(),
        mode: snapshot.session.mode,
        policy_ref: snapshot.session.policy_ref.clone(),
        items: snapshot
            .decisions
            .iter()
            .map(|d| AutomationDecisionSummary {
                intent_id: d.intent_id.clone(),
                observation_id: d.observation_id.clone(),
                decision_time_utc: d.decision_time_utc,
                action: d.action,
                state: d.state.clone(),
            })
            .collect(),
        returned_count: 3,
        total_count: 3,
        next_offset: None,
    };
    let wire = json!({"fixture_scope":{"provider_requests":false,"exchange_effects":false,"production_sqlite_writes":false},
        "snapshot":snapshot,"opening_balances":opening_balances(),"decisions":page,"decision":detail,"performance":build_performance(&snapshot).expect("performance"),"export_manifest":manifest});
    std::fs::write(
        output.join("review-wire.json"),
        serde_json::to_vec_pretty(&wire).expect("wire json"),
    )
    .expect("wire write");
    for file in &manifest.files {
        let mut offset = 0;
        let mut bytes = Vec::new();
        loop {
            let chunk = export_read(
                &snapshot,
                file.file,
                offset,
                &file.sha256,
                &manifest.snapshot_digest,
            )
            .expect("chunk");
            bytes.extend(decode_hex(&chunk.data_hex));
            if let Some(next) = chunk.next_offset {
                offset = next;
            } else {
                break;
            }
        }
        assert_eq!(ContentHash::of_bytes(&bytes), file.sha256);
        std::fs::write(output.join(&file.file_name), bytes).expect("export file write");
    }
}

fn opening_balances() -> Vec<crate::upbit::client::UpbitAccountBalance> {
    serde_json::from_value(json!([{"currency":"KRW","balance":"1000","locked":"0","avg_buy_price":"0","avg_buy_price_modified":false,"unit_currency":"KRW"},
        {"currency":"BTC","balance":"0","locked":"0","avg_buy_price":"0","avg_buy_price_modified":false,"unit_currency":"KRW"}])).expect("typed stored balance witness")
}

#[test]
fn replay_warmup_remains_reviewable_without_inflating_policy_signals() {
    let mut snapshot = fixture();
    let mut warmup = snapshot.decisions[2].clone();
    warmup.intent_id = AutomationIntentId::from_seed("pre-start-warmup");
    warmup.candle = snapshot.candles[0].clone();
    warmup.observation_id = warmup.candle.id.clone();
    warmup.decision_time_utc = warmup.candle.candle.close_time_utc;
    warmup.recorded_at_utc = snapshot.session.updated_at_utc;
    warmup.evaluation = json!({"phase":"POLICY_WARMUP","decision_time":warmup.decision_time_utc,"completed_bars_seen":1});
    warmup.evaluation_digest = ContentHash::of_value(&warmup.evaluation).expect("digest");
    snapshot.decisions.push(warmup);
    let report = build_performance(&snapshot).expect("warmup review");
    assert_eq!(report.decision_count, 3);
    assert_eq!(report.hold_signal_count, 1);
    assert_eq!(report.warmup_record_count, 1);
    assert_eq!(report.total_pnl_krw.value.expect("pnl").get(), dec("86.5"));
    let warmup = snapshot.decisions.last_mut().expect("warmup");
    warmup.evaluation = json!({"kind":"POLICY_WARMUP","decision_time":warmup.decision_time_utc,"completed_bars_seen":1});
    warmup.evaluation_digest = ContentHash::of_value(&warmup.evaluation).expect("digest");
    let report = build_performance(&snapshot).expect("in-session warmup shape");
    assert_eq!(report.decision_count, 3);
    assert_eq!(report.hold_signal_count, 1);
    assert_eq!(report.warmup_record_count, 1);
}

#[test]
fn same_time_opening_snapshot_is_ambiguous_even_when_quantity_is_zero() {
    let mut snapshot = fixture();
    snapshot
        .opening_inventory
        .as_mut()
        .expect("opening")
        .observed_at_utc = snapshot.orders[0].order.created_at_utc;
    assert!(matches!(
        build_performance(&snapshot),
        Err(LabError::DataCorrupt(_))
    ));
}
