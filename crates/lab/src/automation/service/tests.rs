use super::{AutomationAccountPosition, plan_order};
use crate::automation::{
    AutomationOrderAdmission, AutomationOrderDispatch, PersistentMockVenue, VenueAdapter,
    VenueFuture, VerifiedMarketRules,
};
use crate::contracts::{
    AssetQuantity, AutomationOrderIdentifier, AutomationOrderRequest, AutomationRiskPolicy,
    AutomationSessionMode, AutomationSessionQuery, AutomationSessionWrite, BoolExpr,
    CandleInterval, CompareOp, NumericExpr, PolicyDefinition, PolicyId, PolicyIndicator,
    PolicyIndicatorKind, PolicyProgram, PolicyRevisionId, PolicyRevisionRef, PolicyRule,
    PolicySignalExpiry, PolicyState, PolicyTarget, PolicyWrite, PriceKrw, QuoteAmount, RequestId,
    RulesProgram, Weight,
};
use crate::database::DatabaseOwner;
use crate::jobs::{Inner, JobService};
use crate::market_data::UpbitClient;
use crate::upbit::client::UpbitPrivateClient;
use crate::upbit::client::{ClientError, UpbitAccountBalance, UpbitOrderChance, UpbitOrderDetail};
use crate::upbit::risk::{RiskConfig, RiskGuard};
use rust_decimal::Decimal;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "tests/live_mcp_tests.rs"]
mod live_mcp_tests;

fn position(quantity: &str, weight: &str, equity: &str) -> AutomationAccountPosition {
    AutomationAccountPosition {
        actual_qty: AssetQuantity::new(Decimal::from_str_exact(quantity).expect("quantity"))
            .expect("valid quantity"),
        actual_weight: Weight::new(Decimal::from_str_exact(weight).expect("weight"))
            .expect("valid weight"),
        equity_krw: QuoteAmount::new(Decimal::from_str_exact(equity).expect("equity"))
            .expect("valid equity"),
        episode_opened_at: None,
        account_observed_at: crate::contracts::UtcTimestamp::now(),
    }
}

#[test]
fn target_weight_sizes_a_hand_computed_buy_from_exchange_equity() {
    let position = position("0", "0", "10000000");
    let plan = plan_order(
        Weight::new(Decimal::new(1, 3)).expect("target weight"),
        &position,
        PriceKrw::new(Decimal::from(10_000)).expect("mark"),
    )
    .expect("plan")
    .expect("buy plan");

    assert_eq!(plan.side, crate::contracts::AutomationSide::Bid);
    assert_eq!(
        plan.requested_notional,
        QuoteAmount::new(Decimal::from(10_000)).unwrap()
    );
}

#[test]
fn target_weight_sizes_a_hand_computed_sell_from_actual_quantity() {
    let position = position("2", "0.002", "10000000");
    let plan = plan_order(
        Weight::new(Decimal::new(1, 3)).expect("target weight"),
        &position,
        PriceKrw::new(Decimal::from(10_000)).expect("mark"),
    )
    .expect("plan")
    .expect("sell plan");

    assert_eq!(plan.side, crate::contracts::AutomationSide::Ask);
    assert_eq!(plan.asset_reserve.get(), Decimal::ONE);
    assert_eq!(plan.requested_notional.get(), Decimal::from(10_000));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one local-only journey verifies frozen policy, durable fill and restart recovery"
)]
fn frozen_policy_fills_through_mock_venue_and_restart_does_not_duplicate_it()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationOrderState, AutomationSessionState, AutomationSide, MarketId, UtcTimestamp,
    };
    use crate::upbit::control::{
        ExchangeReadMode, TradingControlAction, TradingControlRequest, TradingControlService,
        TradingControlState,
    };
    use axum::http::StatusCode;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-service-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let cleanup_root = root.clone();
    let outcome = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let close_epoch = chrono::Utc::now().timestamp().div_euclid(60) * 60;
        let open = chrono::DateTime::from_timestamp(close_epoch - 60, 0)
            .ok_or("fixture candle start is out of range")?;
        let close = chrono::DateTime::from_timestamp(close_epoch, 0)
            .ok_or("fixture candle close is out of range")?;
        let body = serde_json::json!([{
            "market": "KRW-BTC",
            "candle_date_time_utc": open.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "opening_price": 10000,
            "high_price": 10000,
            "low_price": 10000,
            "trade_price": 10000,
            "candle_acc_trade_volume": 1,
            "candle_acc_trade_price": 10000
        }])
        .to_string();
        let app = axum::Router::new().fallback(move || {
            let body = body.clone();
            async move {
                (
                    StatusCode::OK,
                    [("Remaining-Req", "group=candle; min=1800; sec=59")],
                    body,
                )
            }
        });
        let (stop_http, stopped_http) = tokio::sync::oneshot::channel();
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped_http.await;
                })
                .await
        });

        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        let baseline_at = UtcTimestamp(close - chrono::Duration::seconds(60));
        mock_venue.initialize(baseline_at).await?;
        let upbit = UpbitClient::synthetic_local(&format!("http://{address}"))?;
        let fault_venue = Arc::new(AcceptThenLoseReplyVenue::new(mock_venue.clone(), 2));
        let submit_count = fault_venue.submit_count.clone();
        let lookup_count = fault_venue.lookup_count.clone();
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let control = TradingControlService::production(
            database.clone(),
            risk_guard.clone(),
            ExchangeReadMode::Mock,
        );
        let service = make_service_with_venue_overrides(
            database.clone(),
            upbit.clone(),
            mock_venue.clone(),
            root.clone(),
            Some(fault_venue.clone()),
            None,
            risk_guard.clone(),
        );

        let definition = PolicyDefinition {
            schema_version: "1.0".into(),
            name: "automation-target-weight".into(),
            description: "fixed 0.001 target for deterministic mock execution".into(),
            program: PolicyProgram::Rules {
                program: RulesProgram {
                    indicators: vec![PolicyIndicator {
                        id: "close".into(),
                        indicator: PolicyIndicatorKind::Close,
                        source_interval: None,
                    }],
                    states: vec![PolicyState {
                        id: "memory".into(),
                        initial: 0.0,
                        next: NumericExpr::State {
                            id: "memory".into(),
                        },
                    }],
                    rules: vec![PolicyRule {
                        id: "enter".into(),
                        condition: BoolExpr::Compare {
                            comparison: CompareOp::Gt,
                            left: NumericExpr::Indicator { id: "close".into() },
                            right: NumericExpr::Constant { value: 0.0 },
                        },
                        target: PolicyTarget::Weight {
                            value: NumericExpr::Constant { value: 0.001 },
                        },
                    }],
                    fallback: PolicyTarget::Hold,
                    signal_expiry: PolicySignalExpiry::EndOfRange,
                },
            },
        };
        let policy_write = PolicyWrite::Create {
            request_id: RequestId::new("automation-service-policy")?,
            definition,
        };
        let policy = database
            .call("test_create_automation_policy", move |store| {
                store.write_policy(&policy_write, UtcTimestamp::now())
            })
            .await?;
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let missing_request_id = RequestId::new("automation-service-missing-policy")?;
        let missing_write = AutomationSessionWrite::Start {
            request_id: missing_request_id.clone(),
            policy_ref: PolicyRevisionRef {
                policy_id: PolicyId::new("missing-policy")?,
                revision_id: PolicyRevisionId::new("missing-revision")?,
                definition_digest: crate::contracts::ContentHash::of_bytes(b"missing-policy"),
            },
            market: market.clone(),
            decision_interval: CandleInterval::M1,
            mode: AutomationSessionMode::Mock,
            risk: AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 1,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 3_600,
                max_feed_age_seconds: 3_600,
            },
        };
        assert!(
            service
                .write_automation_session(missing_write)
                .await
                .is_err()
        );
        let readback_id = missing_request_id;
        let readback: serde_json::Value = database
            .call("test_read_rejected_missing_policy_request", move |store| {
                serde_json::to_value(store.automation_readback_request(&readback_id)?)
                    .map_err(Into::into)
            })
            .await?;
        assert_eq!(readback["state"], "REJECTED");
        let risk = AutomationRiskPolicy {
            max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
            activation_loss_limit_krw: None,
            max_open_orders: 1,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        };
        let started_at = UtcTimestamp(close - chrono::Duration::seconds(1));
        let start_write = AutomationSessionWrite::Start {
            request_id: RequestId::new("automation-service-start")?,
            policy_ref: policy.snapshot.reference.clone(),
            market,
            decision_interval: CandleInterval::M1,
            mode: AutomationSessionMode::Mock,
            risk,
        };
        let start_copy = start_write.clone();
        let frozen = policy.snapshot.clone();
        let started = database
            .call("test_start_automation_session", move |store| {
                store.automation_begin_request(
                    &start_copy,
                    Some("mock:upbit-default"),
                    started_at,
                )?;
                store.automation_write(
                    &start_copy,
                    Some(frozen),
                    Some("mock:upbit-default".into()),
                    started_at,
                )
            })
            .await?;
        let session_id = started.session.session_id;

        let control_status = control
            .execute(TradingControlRequest {
                action: TradingControlAction::Status,
                request_id: None,
                expected_sequence: None,
                approval_token: None,
            })
            .await?;
        assert!(!control_status.live_trading_enabled);
        assert_eq!(control_status.state, TradingControlState::DisabledByConfig);
        assert!(control_status.order_admission_blocked);
        assert!(risk_guard.with_order_admission(|| Ok(())).is_ok());

        service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        assert_eq!(submit_count.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_count.load(Ordering::SeqCst), 0);

        let ledger_session = session_id.clone();
        let ledger_before_restart: serde_json::Value = database
            .call("test_read_unknown_automation_ledger", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Ledger {
                        session_id: ledger_session,
                        after_sequence: None,
                        limit: 20,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(
            ledger_before_restart["orders"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            ledger_before_restart["fills"].as_array().map(Vec::len),
            Some(0)
        );
        assert_eq!(
            ledger_before_restart["orders"][0]["state"],
            "SUBMITTED_UNKNOWN"
        );
        assert_eq!(ledger_before_restart["orders"][0]["side"], "BID");
        let claimed_session = session_id.clone();
        let claimed_events: serde_json::Value = database
            .call("test_read_durable_send_claim", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Events {
                        session_id: claimed_session,
                        after_sequence: None,
                        limit: 100,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(
            claimed_events["items"]
                .as_array()
                .ok_or("send claim event page omitted items")?
                .iter()
                .filter(|event| event["kind"] == "ORDER_SEND_CLAIMED")
                .count(),
            1
        );

        drop(service);
        drop(fault_venue);
        drop(mock_venue);
        drop(database);
        owner.shutdown()?;

        let reopened_owner = DatabaseOwner::open(root.clone())?;
        let reopened_database = reopened_owner.handle();
        let reopened_mock = PersistentMockVenue::new(reopened_database.clone());
        let mock_seed_at = UtcTimestamp::now();
        reopened_database
            .call(
                "test_seed_mock_after_restart_without_snapshot",
                move |store| store.automation_seed_mock_venue(mock_seed_at),
            )
            .await?;
        reopened_database
            .call("test_mark_process_restart", |store| {
                store.automation_require_restart_reconciliation(UtcTimestamp::now())
            })
            .await?;
        let reopened_fault_venue = Arc::new(AcceptThenLoseReplyVenue::with_counters(
            reopened_mock.clone(),
            submit_count.clone(),
            lookup_count.clone(),
            2,
        ));
        let reopened_service = make_service_with_mock_override(
            reopened_database.clone(),
            upbit,
            reopened_mock,
            root.clone(),
            Some(reopened_fault_venue.clone()),
        );
        reopened_service
            .recover_pending_automation_requests()
            .await?;
        for expected_lookup in 1..=2 {
            reopened_service
                .process_due_automation_sessions(UtcTimestamp::now())
                .await?;
            let ledger_session = session_id.clone();
            let ledger_after_404: serde_json::Value = reopened_database
                .call("test_read_unknown_after_404_ledger", move |store| {
                    store.automation_query(
                        AutomationSessionQuery::Ledger {
                            session_id: ledger_session,
                            after_sequence: None,
                            limit: 20,
                        },
                        UtcTimestamp::now(),
                    )
                })
                .await?;
            assert_eq!(
                ledger_after_404["orders"][0]["state"],
                "NEEDS_RECONCILIATION"
            );
            assert_eq!(ledger_after_404["fills"].as_array().map(Vec::len), Some(0));
            assert_eq!(submit_count.load(Ordering::SeqCst), 1);
            assert_eq!(lookup_count.load(Ordering::SeqCst), expected_lookup);
            let view_session = session_id.clone();
            let view_after_404: serde_json::Value = reopened_database
                .call("test_read_order_reservation_after_404", move |store| {
                    store.automation_query(
                        AutomationSessionQuery::Get {
                            session_id: view_session,
                        },
                        UtcTimestamp::now(),
                    )
                })
                .await?;
            assert_eq!(view_after_404["unresolved_order_count"], 1);
            assert_eq!(
                view_after_404["account_risk"]["risk"]["open_order_count"],
                1
            );
        }
        reopened_service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        let view_session = session_id.clone();
        let view: serde_json::Value = reopened_database
            .call("test_read_restarted_automation_session", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Get {
                        session_id: view_session,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(view["state"], "RUNNING", "restarted session view: {view}");
        let ledger_session = session_id.clone();
        let ledger_after_restart: serde_json::Value = reopened_database
            .call("test_read_restarted_automation_ledger", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Ledger {
                        session_id: ledger_session,
                        after_sequence: None,
                        limit: 20,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(
            ledger_after_restart["orders"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            ledger_after_restart["fills"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(ledger_after_restart["orders"][0]["state"], "FILLED");
        let paid_fee: QuoteAmount =
            serde_json::from_value(ledger_after_restart["orders"][0]["paid_fee_krw"].clone())?;
        assert!(paid_fee.get() > Decimal::ZERO);
        let identifier: crate::contracts::AutomationOrderIdentifier =
            serde_json::from_value(ledger_after_restart["orders"][0]["client_identifier"].clone())?;
        let venue_detail = reopened_fault_venue
            .inner
            .order_by_identifier(&identifier)
            .await?
            .ok_or("mock accepted order disappeared before duplicate readback")?;
        let account_identity = reopened_fault_venue.inner.account_identity();
        reopened_service
            .reconcile_venue_detail(
                &identifier,
                venue_detail,
                AutomationSessionMode::Mock,
                &account_identity,
            )
            .await?;
        let event_session = session_id.clone();
        let events_after_duplicate: serde_json::Value = reopened_database
            .call(
                "test_read_events_after_duplicate_fill_readback",
                move |store| {
                    store.automation_query(
                        AutomationSessionQuery::Events {
                            session_id: event_session,
                            after_sequence: None,
                            limit: 100,
                        },
                        UtcTimestamp::now(),
                    )
                },
            )
            .await?;
        let events = events_after_duplicate["items"]
            .as_array()
            .ok_or("automation event page omitted items")?;
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "FILL_RECORDED")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "ORDER_RECONCILED")
                .count(),
            1
        );
        let restarted_state: AutomationSessionState =
            serde_json::from_value(view["state"].clone())?;
        assert_eq!(restarted_state, AutomationSessionState::Running);
        let side: AutomationSide =
            serde_json::from_value(ledger_after_restart["orders"][0]["side"].clone())?;
        assert_eq!(side, AutomationSide::Bid);
        let order_state: AutomationOrderState =
            serde_json::from_value(ledger_after_restart["orders"][0]["state"].clone())?;
        assert_eq!(order_state, AutomationOrderState::Filled);
        assert_eq!(submit_count.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_count.load(Ordering::SeqCst), 3);
        reopened_service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        assert_eq!(submit_count.load(Ordering::SeqCst), 1);
        let _ = stop_http.send(());
        http_server.await??;
        drop(reopened_service);
        drop(reopened_fault_venue);
        drop(reopened_database);
        reopened_owner.shutdown()?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    std::fs::remove_dir_all(cleanup_root)?;
    outcome?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one hard-gate journey proves denied Live start/cancel and unchanged mock authority"
)]
fn live_disabled_blocks_private_reads_and_effects_before_adapter_access()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationCommandOutcome, AutomationSessionState, MarketId, StrategySpec, UtcTimestamp,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-live-disabled-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome =
        runtime.block_on(async {
            let database = owner.handle();
            let mock_venue = PersistentMockVenue::new(database.clone());
            mock_venue.initialize(UtcTimestamp::now()).await?;
            let mock_before = database
                .call("test_mock_balances_before_live_denial", |store| {
                    store.automation_mock_venue_accounts()
                })
                .await?;
            let mock_before = serde_json::to_value(mock_before)?;
            let policy_write = PolicyWrite::Create {
                request_id: RequestId::new("automation-live-disabled-policy")?,
                definition: PolicyDefinition {
                    schema_version: "1.0".into(),
                    name: "live-disabled-policy".into(),
                    description: "fixture for the hard-disabled LIVE service path".into(),
                    program: PolicyProgram::Builtin {
                        strategy: StrategySpec::BuyAndHold,
                    },
                },
            };
            let policy = database
                .call("test_create_live_disabled_policy", move |store| {
                    store.write_policy(&policy_write, UtcTimestamp::now())
                })
                .await?;
            let live_venue = Arc::new(CountingLiveVenue::new());
            let private_calls = live_venue.private_calls.clone();
            let identity_calls = live_venue.identity_calls.clone();
            let upbit = UpbitClient::synthetic_local("http://127.0.0.1:1")?;
            let service = make_service_with_live_override(
                database.clone(),
                upbit,
                mock_venue,
                root.clone(),
                live_venue,
            );
            let started = service
                .automation_session_write(AutomationSessionWrite::Start {
                    request_id: RequestId::new("automation-live-disabled-start")?,
                    policy_ref: policy.snapshot.reference.clone(),
                    market: MarketId::parse_upbit("KRW-BTC")?,
                    decision_interval: CandleInterval::M1,
                    mode: AutomationSessionMode::Live,
                    risk: AutomationRiskPolicy {
                        max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                        max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                        max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
                        max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
                        activation_loss_limit_krw: None,
                        max_open_orders: 1,
                        max_orders_per_interval: 1,
                        max_balance_age_seconds: 3_600,
                        max_feed_age_seconds: 3_600,
                    },
                })
                .await?;
            assert_eq!(
                started.session.state,
                AutomationSessionState::BlockedApproval
            );
            assert_eq!(started.receipt.outcome, AutomationCommandOutcome::Denied);
            assert_eq!(
                started.receipt.reason.as_deref(),
                Some("LIVE_TRADING_DISABLED")
            );
            assert_eq!(identity_calls.load(Ordering::SeqCst), 1);
            assert_eq!(private_calls.load(Ordering::SeqCst), 0);

            let cancel_request_id = RequestId::new("automation-live-disabled-cancel")?;
            let cancel = service
                .automation_session_write(AutomationSessionWrite::CancelOrder {
                    request_id: cancel_request_id.clone(),
                    session_id: started.session.session_id.clone(),
                    expected_sequence: started.session.sequence,
                    client_identifier: AutomationOrderIdentifier::new("auto-live-disabled-cancel")?,
                })
                .await;
            assert!(matches!(
                cancel,
                Err(crate::contracts::LabError::BlockedEvidence(_)
                    | crate::contracts::LabError::Conflict(_))
            ));
            assert_eq!(private_calls.load(Ordering::SeqCst), 0);
            assert_eq!(identity_calls.load(Ordering::SeqCst), 1);
            let cancel_readback_id = cancel_request_id;
            let cancel_readback = database
                .call("test_read_rejected_live_cancel_request", move |store| {
                    store.automation_readback_request(&cancel_readback_id)
                })
                .await?;
            assert_eq!(
                cancel_readback.state,
                crate::contracts::AutomationRequestReadbackState::Rejected
            );

            let mut unexpected_live_record = crate::automation::AutomationRuntimeRecord {
                view: started.session,
                frozen_policy: policy.snapshot,
                account_identity:
                    "live:upbit:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .into(),
                checkpoint_json: "{}".into(),
            };
            unexpected_live_record.view.state = AutomationSessionState::Running;
            service
                .process_automation_record(&mut unexpected_live_record)
                .await?;
            service
                .process_due_automation_sessions(UtcTimestamp::now())
                .await?;
            assert_eq!(private_calls.load(Ordering::SeqCst), 0);
            assert_eq!(identity_calls.load(Ordering::SeqCst), 1);
            let mock_after = database
                .call("test_mock_balances_after_live_denial", |store| {
                    store.automation_mock_venue_accounts()
                })
                .await?;
            assert_eq!(serde_json::to_value(mock_after)?, mock_before);
            drop(service);
            drop(database);
            Ok::<(), Box<dyn std::error::Error>>(())
        });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
fn queued_owner_claim_observes_fast_emergency_latch_before_store_claim()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{LabError, UtcTimestamp};

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-fast-emergency-claim-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let guard = RiskGuard::new(RiskConfig::default());
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let (owner_entered_tx, owner_entered_rx) = tokio::sync::oneshot::channel();
        let (release_owner_tx, release_owner_rx) = std::sync::mpsc::channel();
        let blocking_database = database.clone();
        let blocker = tokio::spawn(async move {
            blocking_database
                .call("test_hold_database_owner", move |_store| {
                    owner_entered_tx
                        .send(())
                        .map_err(|()| LabError::Cancelled("test owner barrier closed".into()))?;
                    release_owner_rx
                        .recv()
                        .map_err(|_| LabError::Cancelled("test owner release closed".into()))?;
                    Ok(())
                })
                .await
        });
        owner_entered_rx.await?;

        let identifier = crate::contracts::AutomationOrderIdentifier::new("auto-fast-latch")?;
        let observed = UtcTimestamp::now();
        let rules = VerifiedMarketRules {
            market: "KRW-BTC".into(),
            source_mode: AutomationSessionMode::Mock,
            observed_at_utc: observed,
            state: "active".into(),
            bid_types: vec!["price".into()],
            ask_types: vec!["market".into()],
            bid_fee: Decimal::new(5, 4),
            ask_fee: Decimal::new(5, 4),
            bid_min_total: Decimal::from(5_000),
            ask_min_total: Decimal::from(5_000),
            max_total: Decimal::from(1_000_000),
            bid_price_unit: None,
            ask_price_unit: None,
        };
        let claim_database = database.clone();
        let claim_guard = guard.clone();
        let (claim_queued_tx, claim_queued_rx) = tokio::sync::oneshot::channel();
        let claim = tokio::spawn(async move {
            let _ = claim_queued_tx.send(());
            claim_database
                .call("test_claim_send_after_queue", move |store| {
                    claim_guard.with_order_admission(|| {
                        store.automation_claim_order_for_send(&identifier, &rules, observed)
                    })
                })
                .await
        });
        claim_queued_rx.await?;
        let latch = guard.latch_order_admission(true);
        assert!(latch.revision() > 0);
        release_owner_tx
            .send(())
            .map_err(|_| "database owner barrier failed before release")?;
        blocker.await??;
        let claim_result = claim.await?;
        assert!(matches!(
            claim_result,
            Err(LabError::BlockedEvidence(reason))
                if reason == "EMERGENCY_STOP_BLOCKS_ORDER_CLAIM"
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

fn make_service_with_mock_override(
    database: crate::database::DatabaseHandle,
    upbit: UpbitClient,
    automation_mock_venue: PersistentMockVenue,
    root: std::path::PathBuf,
    automation_mock_venue_override: Option<Arc<dyn crate::automation::VenueAdapter>>,
) -> JobService {
    make_service_with_venue_overrides(
        database,
        upbit,
        automation_mock_venue,
        root,
        automation_mock_venue_override,
        None,
        RiskGuard::new(RiskConfig::default()),
    )
}

fn make_service_with_live_override(
    database: crate::database::DatabaseHandle,
    upbit: UpbitClient,
    automation_mock_venue: PersistentMockVenue,
    root: std::path::PathBuf,
    automation_live_venue_override: Arc<dyn crate::automation::VenueAdapter>,
) -> JobService {
    make_service_with_venue_overrides(
        database,
        upbit,
        automation_mock_venue,
        root,
        None,
        Some(automation_live_venue_override),
        RiskGuard::new(RiskConfig::default()),
    )
}

fn make_service_with_venue_overrides(
    database: crate::database::DatabaseHandle,
    upbit: UpbitClient,
    automation_mock_venue: PersistentMockVenue,
    root: std::path::PathBuf,
    automation_mock_venue_override: Option<Arc<dyn crate::automation::VenueAdapter>>,
    automation_live_venue_override: Option<Arc<dyn crate::automation::VenueAdapter>>,
    risk_guard: RiskGuard,
) -> JobService {
    JobService {
        inner: Arc::new(Inner {
            database,
            upbit,
            automation_live_venue: UpbitPrivateClient::new_mock(),
            automation_mock_venue,
            risk_guard,
            automation_due_cursor: Mutex::new(None),
            automation_mock_venue_override,
            automation_live_venue_override,
            root,
            revision: None,
            wake: Notify::new(),
            stop: CancellationToken::new(),
            admitting: AtomicBool::new(true),
            admission_gate: Arc::new(tokio::sync::Mutex::new(())),
            automation_review_budget: Arc::new(tokio::sync::Semaphore::new(2)),
            runner_available: AtomicBool::new(true),
            active: Mutex::new(None),
        }),
    }
}

#[derive(Clone)]
struct AcceptThenLoseReplyVenue {
    inner: PersistentMockVenue,
    submit_count: Arc<AtomicUsize>,
    lookup_count: Arc<AtomicUsize>,
    missing_lookup_count: usize,
}

impl AcceptThenLoseReplyVenue {
    fn new(inner: PersistentMockVenue, missing_lookup_count: usize) -> Self {
        Self {
            inner,
            submit_count: Arc::new(AtomicUsize::new(0)),
            lookup_count: Arc::new(AtomicUsize::new(0)),
            missing_lookup_count,
        }
    }

    fn with_counters(
        inner: PersistentMockVenue,
        submit_count: Arc<AtomicUsize>,
        lookup_count: Arc<AtomicUsize>,
        missing_lookup_count: usize,
    ) -> Self {
        Self {
            inner,
            submit_count,
            lookup_count,
            missing_lookup_count,
        }
    }
}

impl VenueAdapter for AcceptThenLoseReplyVenue {
    fn is_live(&self) -> bool {
        self.inner.is_live()
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.inner.account_balances()
    }

    fn order_chance<'a>(
        &'a self,
        market: &'a crate::contracts::MarketId,
    ) -> VenueFuture<'a, UpbitOrderChance> {
        self.inner.order_chance(market)
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a crate::contracts::AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        let inner = self.inner.clone();
        let request = request.clone();
        let submit_count = self.submit_count.clone();
        Box::pin(async move {
            submit_count.fetch_add(1, Ordering::SeqCst);
            let _accepted_order = inner
                .submit_order(&request, decision_mark, send_generation)
                .await?;
            Err(ClientError::Network(
                "test fault after persistent mock accepted the order".into(),
            ))
        })
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a crate::contracts::AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        let inner = self.inner.clone();
        let identifier = identifier.clone();
        let lookup_count = self.lookup_count.clone();
        let missing_lookup_count = self.missing_lookup_count;
        Box::pin(async move {
            let index = lookup_count.fetch_add(1, Ordering::SeqCst);
            if index < missing_lookup_count {
                Ok(None)
            } else {
                inner.order_by_identifier(&identifier).await
            }
        })
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner.cancel_order(uuid)
    }
}

struct IncompleteSubmitAckVenue {
    inner: PersistentMockVenue,
    submit_calls: Arc<AtomicUsize>,
    lookup_calls: Arc<AtomicUsize>,
}

impl VenueAdapter for IncompleteSubmitAckVenue {
    fn is_live(&self) -> bool {
        false
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.inner.account_balances()
    }

    fn order_chance<'a>(
        &'a self,
        market: &'a crate::contracts::MarketId,
    ) -> VenueFuture<'a, UpbitOrderChance> {
        self.inner.order_chance(market)
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        let inner = self.inner.clone();
        let request = request.clone();
        let submit_calls = self.submit_calls.clone();
        Box::pin(async move {
            submit_calls.fetch_add(1, Ordering::SeqCst);
            let mut acknowledgement = inner
                .submit_order(&request, decision_mark, send_generation)
                .await?;
            // Model a POST receipt without canonical GET trade fields and with
            // a mismatched returned identifier; neither may book fills.
            acknowledgement.identifier = Some("foreign-order-identifier".into());
            acknowledgement.remaining_volume = None;
            acknowledgement.trades.clear();
            Ok(acknowledgement)
        })
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        self.lookup_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.order_by_identifier(identifier)
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner.cancel_order(uuid)
    }
}

struct CycleCancelVenue {
    inner: PersistentMockVenue,
    waiting: UpbitOrderDetail,
    cancelled: UpbitOrderDetail,
    cancel_calls: Arc<AtomicUsize>,
    lookup_calls: Arc<AtomicUsize>,
}

impl VenueAdapter for CycleCancelVenue {
    fn is_live(&self) -> bool {
        false
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.inner.account_balances()
    }

    fn order_chance<'a>(
        &'a self,
        market: &'a crate::contracts::MarketId,
    ) -> VenueFuture<'a, UpbitOrderChance> {
        self.inner.order_chance(market)
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner
            .submit_order(request, decision_mark, send_generation)
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        let identifier = identifier.clone();
        let waiting = self.waiting.clone();
        let cancelled = self.cancelled.clone();
        let lookup_calls = self.lookup_calls.clone();
        Box::pin(async move {
            let index = lookup_calls.fetch_add(1, Ordering::SeqCst);
            let detail = if index == 0 { waiting } else { cancelled };
            Ok((detail.identifier.as_deref() == Some(identifier.as_str())).then_some(detail))
        })
    }

    fn cancel_order<'a>(&'a self, _uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        let waiting = self.waiting.clone();
        Box::pin(async move { Ok(waiting) })
    }
}

fn cycle_cancel_order_detail(
    order: &crate::automation::AutomationPendingOrder,
    now: crate::contracts::UtcTimestamp,
    state: &str,
) -> UpbitOrderDetail {
    let price = match &order.request.order {
        crate::contracts::AutomationOrderKind::MarketBuy { budget } => Some(budget.get()),
        crate::contracts::AutomationOrderKind::Limit { price, .. } => Some(price.get()),
        crate::contracts::AutomationOrderKind::MarketSell { .. } => None,
    };
    UpbitOrderDetail {
        uuid: "mock-open-order-uuid".into(),
        identifier: Some(order.request.client_identifier.as_str().into()),
        side: "bid".into(),
        ord_type: "price".into(),
        price,
        state: state.into(),
        market: order.request.market.code().clone(),
        created_at: now.to_rfc3339(),
        volume: None,
        remaining_volume: Some(Decimal::ZERO),
        reserved_fee: Some(Decimal::from(5)),
        remaining_fee: Some(Decimal::from(5)),
        paid_fee: Some(Decimal::ZERO),
        locked: Some(if state == "wait" {
            Decimal::from(10_005)
        } else {
            Decimal::ZERO
        }),
        executed_volume: Some(Decimal::ZERO),
        trades_count: Some(0),
        trades: Vec::new(),
    }
}

struct CountingLiveVenue {
    private_calls: Arc<AtomicUsize>,
    identity_calls: Arc<AtomicUsize>,
}

impl CountingLiveVenue {
    fn new() -> Self {
        Self {
            private_calls: Arc::new(AtomicUsize::new(0)),
            identity_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl VenueAdapter for CountingLiveVenue {
    fn is_live(&self) -> bool {
        true
    }

    fn account_identity(&self) -> String {
        self.identity_calls.fetch_add(1, Ordering::SeqCst);
        "live:upbit:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.private_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }

    fn order_chance<'a>(
        &'a self,
        _market: &'a crate::contracts::MarketId,
    ) -> VenueFuture<'a, UpbitOrderChance> {
        self.private_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }

    fn submit_order<'a>(
        &'a self,
        _request: &'a AutomationOrderRequest,
        _decision_mark: PriceKrw,
        _send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        self.private_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }

    fn order_by_identifier<'a>(
        &'a self,
        _identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        self.private_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }

    fn cancel_order<'a>(&'a self, _uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.private_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }
}

struct LatchBeforeClaimVenue {
    inner: PersistentMockVenue,
    risk_guard: RiskGuard,
    chance_calls: Arc<AtomicUsize>,
    submit_calls: Arc<AtomicUsize>,
}

impl LatchBeforeClaimVenue {
    fn new(inner: PersistentMockVenue, risk_guard: RiskGuard) -> Self {
        Self {
            inner,
            risk_guard,
            chance_calls: Arc::new(AtomicUsize::new(0)),
            submit_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl VenueAdapter for LatchBeforeClaimVenue {
    fn is_live(&self) -> bool {
        false
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.inner.account_balances()
    }

    fn order_chance<'a>(
        &'a self,
        market: &'a crate::contracts::MarketId,
    ) -> VenueFuture<'a, UpbitOrderChance> {
        let inner = self.inner.clone();
        let guard = self.risk_guard.clone();
        let market = market.clone();
        let chance_calls = self.chance_calls.clone();
        Box::pin(async move {
            let chance = inner.order_chance(&market).await?;
            chance_calls.fetch_add(1, Ordering::SeqCst);
            let latch = guard.latch_order_admission(true);
            assert!(latch.revision() > 0);
            Ok(chance)
        })
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        self.submit_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .submit_order(request, decision_mark, send_generation)
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        self.inner.order_by_identifier(identifier)
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner.cancel_order(uuid)
    }
}

async fn seed_claimed_mock_order(
    database: &crate::database::DatabaseHandle,
    risk_guard: &RiskGuard,
) -> Result<
    (
        crate::automation::AutomationPendingOrder,
        AutomationOrderDispatch,
        crate::contracts::AutomationSessionId,
    ),
    crate::contracts::LabError,
> {
    let (order, dispatch, session_id) = seed_mock_order_fixture(database, risk_guard, true).await?;
    let dispatch = dispatch.ok_or_else(|| {
        crate::contracts::LabError::DataCorrupt("claimed fixture omitted its dispatch".into())
    })?;
    Ok((order, dispatch, session_id))
}

async fn seed_reserved_mock_order(
    database: &crate::database::DatabaseHandle,
    risk_guard: &RiskGuard,
) -> Result<
    (
        crate::automation::AutomationPendingOrder,
        crate::contracts::AutomationSessionId,
    ),
    crate::contracts::LabError,
> {
    let (order, dispatch, session_id) =
        seed_mock_order_fixture(database, risk_guard, false).await?;
    if dispatch.is_some() {
        return Err(crate::contracts::LabError::DataCorrupt(
            "reserved fixture unexpectedly claimed its order".into(),
        ));
    }
    Ok((order, session_id))
}

#[tokio::test]
async fn live_order_disable_preserves_reserved_mock_orders()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::upbit::control::ExchangeReadMode;
    use crate::upbit::execution::LiveExecutionPolicy;
    use crate::upbit::live_order_control::{
        LiveOrderControlAction, LiveOrderControlRequest, LiveOrderControlService, LiveOrderSendGate,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-live-order-disable-mock-preserved-{}-{nonce}",
        std::process::id()
    ));
    let owner = DatabaseOwner::open_with_live_order_control(root.clone(), true)?;
    let database = owner.handle();
    let risk_guard = RiskGuard::new(RiskConfig::default());
    PersistentMockVenue::new(database.clone())
        .initialize(crate::contracts::UtcTimestamp::now())
        .await?;
    let (order, _session_id) = seed_reserved_mock_order(&database, &risk_guard).await?;
    let service = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(LiveOrderSendGate::new_disabled()),
        ExchangeReadMode::Mock,
    );
    let status = service
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?;
    let disabled = service
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Disable,
            request_id: Some("disable-live-only".into()),
            expected_sequence: Some(status.sequence),
            expected_epoch: Some(status.boot_epoch),
            confirmation: None,
        })
        .await?;
    assert!(!disabled.live_trading_enabled);

    let identifier = order.request.client_identifier.clone();
    let readback = database
        .call(
            "test_live_gate_preserves_mock_reserved_order",
            move |store| store.automation_order_readback(&identifier),
        )
        .await?
        .ok_or("mock reservation disappeared during live gate disable")?;
    assert_eq!(
        readback.state,
        crate::contracts::AutomationOrderState::Reserved
    );
    drop(service);
    drop(database);
    owner.shutdown()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the interleaving fixture must seed a real durable intent and reservation"
)]
async fn seed_mock_order_fixture(
    database: &crate::database::DatabaseHandle,
    risk_guard: &RiskGuard,
    claim_for_send: bool,
) -> Result<
    (
        crate::automation::AutomationPendingOrder,
        Option<AutomationOrderDispatch>,
        crate::contracts::AutomationSessionId,
    ),
    crate::contracts::LabError,
> {
    use crate::contracts::{
        AutomationIntentId, AutomationOrderKind, AutomationSessionId, CandleObservation,
        CandleRecord, MarketId, ObservationId, PolicyProgram, PriceKrw, StrategySpec, UtcTimestamp,
    };

    let now = UtcTimestamp::now();
    let close_epoch = now.0.timestamp().div_euclid(60) * 60;
    let close = chrono::DateTime::from_timestamp(close_epoch, 0).ok_or_else(|| {
        crate::contracts::LabError::InvalidConfig("fixture candle boundary is out of range".into())
    })?;
    let close_time = UtcTimestamp(close);
    let open_time = UtcTimestamp(close - chrono::Duration::minutes(1));
    let started_at = UtcTimestamp(close - chrono::Duration::seconds(1));
    let account_identity = "mock:upbit-default";
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let risk = AutomationRiskPolicy {
        max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
        max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
        max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
        max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
        activation_loss_limit_krw: None,
        max_open_orders: 2,
        max_orders_per_interval: 2,
        max_balance_age_seconds: 3_600,
        max_feed_age_seconds: 3_600,
    };
    let policy_write = PolicyWrite::Create {
        request_id: RequestId::new("automation-after-claim-policy")?,
        definition: PolicyDefinition {
            schema_version: "1.0".into(),
            name: "after-claim-emergency".into(),
            description: "durable order fixture for emergency interleaving".into(),
            program: PolicyProgram::Builtin {
                strategy: StrategySpec::BuyAndHold,
            },
        },
    };
    let database = database.clone();
    let policy = database
        .call("test_create_after_claim_policy", move |store| {
            store.write_policy(&policy_write, started_at)
        })
        .await?;
    let mut checkpoint = crate::automation::AutomationCheckpoint::compile(
        &policy.snapshot,
        market.clone(),
        CandleInterval::M1,
    )?;
    let start_write = AutomationSessionWrite::Start {
        request_id: RequestId::new("automation-after-claim-start")?,
        policy_ref: policy.snapshot.reference.clone(),
        market: market.clone(),
        decision_interval: CandleInterval::M1,
        mode: AutomationSessionMode::Mock,
        risk,
    };
    let start_for_store = start_write.clone();
    let frozen_policy = policy.snapshot;
    let start = database
        .call("test_start_after_claim_session", move |store| {
            store.automation_begin_request(&start_for_store, Some(account_identity), started_at)?;
            store.automation_write(
                &start_for_store,
                Some(frozen_policy),
                Some(account_identity.into()),
                started_at,
            )
        })
        .await?;
    let session_id: AutomationSessionId = start.session.session_id;

    let close_price = PriceKrw::new(Decimal::from(10_000))?;
    let candle = CandleRecord {
        market: market.code().clone(),
        interval: CandleInterval::M1,
        open_time_utc: open_time,
        close_time_utc: close_time,
        open: close_price,
        high: PriceKrw::new(Decimal::from(10_001))?,
        low: PriceKrw::new(Decimal::from(9_999))?,
        close: close_price,
        volume: AssetQuantity::new(Decimal::ONE)?,
        quote_turnover: QuoteAmount::new(Decimal::from(10_000))?,
        completed: true,
    };
    let content_digest = crate::contracts::observation_digest(&candle)?;
    let observation = CandleObservation {
        id: ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    };
    checkpoint.last_observation = Some(observation.clone());
    checkpoint.held_decision_bars = 1;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    let intent_id = AutomationIntentId::from_seed("automation-after-claim-intent");
    let evaluation_json = serde_json::json!({
        "decision_time_utc": close_time,
        "constrained_target_weight": 0.001
    });
    let session_for_decision = session_id.clone();
    let observation_for_decision = observation.clone();
    let intent_for_decision = intent_id.clone();
    let evaluation_for_decision = evaluation_json.clone();
    database
        .call("test_record_after_claim_intent", move |store| {
            store.automation_record_decision(
                &session_for_decision,
                &observation_for_decision,
                &checkpoint_json,
                &intent_for_decision,
                "BUY",
                &evaluation_for_decision,
                now,
            )
        })
        .await?;
    let balances = database
        .call("test_read_after_claim_mock_balances", |store| {
            store.automation_mock_venue_accounts()
        })
        .await?;
    database
        .call("test_record_after_claim_account", move |store| {
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                now,
                now,
                &balances,
            )
        })
        .await?;

    let notional = QuoteAmount::new(Decimal::from(10_000))?;
    let fee = QuoteAmount::new(Decimal::from(5))?;
    let request = AutomationOrderRequest {
        client_identifier: AutomationOrderIdentifier::from_intent(&intent_id),
        market,
        side: crate::contracts::AutomationSide::Bid,
        order: AutomationOrderKind::MarketBuy { budget: notional },
    };
    let rules = VerifiedMarketRules {
        market: "KRW-BTC".into(),
        source_mode: AutomationSessionMode::Mock,
        observed_at_utc: now,
        state: "active".into(),
        bid_types: vec!["price".into()],
        ask_types: vec!["market".into()],
        bid_fee: Decimal::new(5, 4),
        ask_fee: Decimal::new(5, 4),
        bid_min_total: Decimal::from(5_000),
        ask_min_total: Decimal::from(5_000),
        max_total: Decimal::from(1_000_000),
        bid_price_unit: None,
        ask_price_unit: None,
    };
    let admission = AutomationOrderAdmission {
        session_id: session_id.clone(),
        intent_id,
        request: request.clone(),
        decision_mark: close_price,
        decision_time: close_time,
        requested_notional: notional,
        quote_reserve: QuoteAmount::new(Decimal::from(10_005))?,
        asset_currency: "BTC".into(),
        asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
        fee_reserve: fee,
        rules: rules.clone(),
        evaluation_json,
    };
    let session_for_admission = session_id.clone();
    let _admitted = database
        .call("test_admit_after_claim_order", move |store| {
            store.automation_admit_order(admission, now)
        })
        .await?;
    let pending = database
        .call("test_read_after_claim_pending_order", move |store| {
            store
                .automation_pending_orders(&session_for_admission)?
                .into_iter()
                .find(|order| order.request.client_identifier == request.client_identifier)
                .ok_or_else(|| {
                    crate::contracts::LabError::DataCorrupt(
                        "admitted fixture order was not pending".into(),
                    )
                })
        })
        .await?;
    if !claim_for_send {
        return Ok((pending, None, session_id));
    }
    let identifier = pending.request.client_identifier.clone();
    let rules_for_claim = rules;
    let guard = risk_guard.clone();
    let dispatch = database
        .call("test_claim_after_claim_order", move |store| {
            guard.with_order_admission(|| {
                store.automation_claim_order_for_send(&identifier, &rules_for_claim, now)
            })
        })
        .await?;
    Ok((pending, Some(dispatch), session_id))
}

#[test]
fn post_ack_is_validated_then_only_canonical_get_reconciles_fills()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{AutomationSessionQuery, UtcTimestamp};

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-post-ack-readback-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, dispatch, session_id) = seed_claimed_mock_order(&database, &risk_guard).await?;
        let adapter = Arc::new(IncompleteSubmitAckVenue {
            inner: mock_venue.clone(),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            lookup_calls: Arc::new(AtomicUsize::new(0)),
        });
        let submit_calls = adapter.submit_calls.clone();
        let lookup_calls = adapter.lookup_calls.clone();
        let service = make_service_with_venue_overrides(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            Some(adapter),
            None,
            risk_guard,
        );

        service
            .submit_claimed_order(&order, AutomationSessionMode::Mock, dispatch, None)
            .await?;
        assert_eq!(submit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 1);

        let ledger_session = session_id.clone();
        let ledger: serde_json::Value = database
            .call("test_read_post_ack_canonical_ledger", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Ledger {
                        session_id: ledger_session,
                        after_sequence: None,
                        limit: 20,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(ledger["orders"][0]["state"], "FILLED");
        assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(1));

        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the pending-cancel test proves one DELETE, cycle readback, and durable request finalization together"
)]
fn coordinator_reconciles_pending_cancel_each_cycle_without_repeating_delete()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationCommandOutcome, AutomationOrderState, AutomationSessionQuery,
        AutomationSessionWrite, RequestId, UtcTimestamp,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-cancel-cycle-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, _dispatch, session_id) =
            seed_claimed_mock_order(&database, &risk_guard).await?;
        let now = UtcTimestamp::now();
        let waiting = cycle_cancel_order_detail(&order, now, "wait");
        let cancelled = cycle_cancel_order_detail(&order, now, "cancel");
        let order_identifier = order.request.client_identifier.clone();
        database
            .call("test_seed_cancelable_open_order", move |store| {
                store.automation_reconcile_order(&waiting, now)
            })
            .await?;
        let record_session = session_id.clone();
        let record = database
            .call("test_read_cancelable_session", move |store| {
                store.automation_runtime_record(&record_session, now)
            })
            .await?;

        let adapter = Arc::new(CycleCancelVenue {
            inner: mock_venue.clone(),
            waiting: cycle_cancel_order_detail(&order, now, "wait"),
            cancelled,
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            lookup_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cancel_calls = adapter.cancel_calls.clone();
        let lookup_calls = adapter.lookup_calls.clone();
        let service = make_service_with_venue_overrides(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            Some(adapter),
            None,
            risk_guard,
        );
        let request_id = RequestId::new("automation-cycle-cancel-request")?;
        let response = service
            .write_automation_session(AutomationSessionWrite::CancelOrder {
                request_id: request_id.clone(),
                session_id: session_id.clone(),
                expected_sequence: record.view.sequence,
                client_identifier: order_identifier.clone(),
            })
            .await?;
        assert_eq!(response.receipt.outcome, AutomationCommandOutcome::Pending);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 1);

        service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 2);

        let readback_request_id = request_id.clone();
        let request_readback = database
            .call("test_read_cycle_cancel_request", move |store| {
                store.automation_readback_request(&readback_request_id)
            })
            .await?;
        assert_eq!(
            request_readback.state,
            crate::contracts::AutomationRequestReadbackState::Applied
        );
        let readback_order_id = order_identifier.clone();
        let order_readback = database
            .call("test_read_cycle_cancel_order", move |store| {
                store.automation_order_readback(&readback_order_id)
            })
            .await?
            .ok_or("cycle cancel lost its durable order")?;
        assert_eq!(order_readback.state, AutomationOrderState::Cancelled);

        let ledger_session = session_id.clone();
        let ledger: serde_json::Value = database
            .call("test_read_cycle_cancel_ledger", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Ledger {
                        session_id: ledger_session,
                        after_sequence: None,
                        limit: 20,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(ledger["orders"][0]["state"], "CANCELLED");

        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
fn paused_restart_reconciliation_clears_reason_without_auto_resuming()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationOrderState, AutomationSessionState, AutomationSessionWrite, RequestId,
        UtcTimestamp,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-paused-restart-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, _dispatch, session_id) =
            seed_claimed_mock_order(&database, &risk_guard).await?;
        let restart_at = UtcTimestamp::now();
        database
            .call("test_mark_paused_restart_required", move |store| {
                store.automation_require_restart_reconciliation(restart_at)
            })
            .await?;
        let pre_pause_session_id = session_id.clone();
        let pre_pause = database
            .call("test_read_restart_sequence_before_pause", move |store| {
                store.automation_runtime_record(&pre_pause_session_id, UtcTimestamp::now())
            })
            .await?;
        let service = make_service_with_mock_override(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            None,
        );
        let paused = service
            .write_automation_session(AutomationSessionWrite::Pause {
                request_id: RequestId::new("automation-paused-restart-pause")?,
                session_id: session_id.clone(),
                expected_sequence: pre_pause.view.sequence,
            })
            .await?;
        assert_eq!(paused.session.state, AutomationSessionState::Paused);
        assert_eq!(
            paused.session.recovery_reason.as_deref(),
            Some("PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK")
        );

        let terminal = cycle_cancel_order_detail(&order, UtcTimestamp::now(), "cancel");
        database
            .call("test_reconcile_paused_restart_order", move |store| {
                store.automation_reconcile_order(&terminal, UtcTimestamp::now())
            })
            .await?;
        service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        let session_readback_id = session_id.clone();
        let recovered = database
            .call("test_read_paused_restart_after_cycle", move |store| {
                store.automation_runtime_record(&session_readback_id, UtcTimestamp::now())
            })
            .await?;
        assert_eq!(recovered.view.state, AutomationSessionState::Paused);
        assert_eq!(recovered.view.recovery_reason, None);
        let order_identifier = order.request.client_identifier.clone();
        let order_readback = database
            .call("test_read_paused_restart_terminal_order", move |store| {
                store.automation_order_readback(&order_identifier)
            })
            .await?
            .ok_or("restart readback lost its durable order")?;
        assert_eq!(order_readback.state, AutomationOrderState::Cancelled);

        let resumed = service
            .write_automation_session(AutomationSessionWrite::Resume {
                request_id: RequestId::new("automation-paused-restart-resume")?,
                session_id,
                expected_sequence: recovered.view.sequence,
            })
            .await?;
        assert_eq!(resumed.session.state, AutomationSessionState::Running);
        assert_eq!(resumed.session.recovery_reason, None);

        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the startup recovery fixture proves a late STOP is applied before a reserved adapter send"
)]
fn startup_drains_more_than_one_lifecycle_page_before_reserved_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationOrderState, AutomationSessionState, AutomationSessionWrite, RequestId,
        UtcTimestamp,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-late-stop-recovery-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, session_id) = seed_reserved_mock_order(&database, &risk_guard).await?;
        let initial_session_id = session_id.clone();
        let initial = database
            .call("test_read_late_stop_initial_session", move |store| {
                store.automation_runtime_record(&initial_session_id, UtcTimestamp::now())
            })
            .await?;
        let paused_sequence = initial.view.sequence.checked_add(1).ok_or_else(|| {
            crate::contracts::LabError::ResourceLimit("fixture sequence overflow".into())
        })?;
        let begin_at = UtcTimestamp::now();
        let batch_session = session_id.clone();
        database
            .call("test_begin_more_than_one_lifecycle_page", move |store| {
                for index in 0..100_u32 {
                    let write = AutomationSessionWrite::Pause {
                        request_id: RequestId::new(format!(
                            "automation-recovery-pause-{index:03}"
                        ))?,
                        session_id: batch_session.clone(),
                        expected_sequence: if index == 0 {
                            initial.view.sequence
                        } else {
                            paused_sequence
                        },
                    };
                    let at =
                        UtcTimestamp(begin_at.0 + chrono::Duration::milliseconds(i64::from(index)));
                    store.automation_begin_request(&write, None, at)?;
                }
                let late_stop = AutomationSessionWrite::Stop {
                    request_id: RequestId::new("automation-recovery-stop-late")?,
                    session_id: batch_session,
                    expected_sequence: paused_sequence,
                };
                store.automation_begin_request(
                    &late_stop,
                    None,
                    UtcTimestamp(begin_at.0 + chrono::Duration::milliseconds(100)),
                )?;
                Ok(())
            })
            .await?;
        let pending_batch = database
            .call("test_confirm_late_stop_is_beyond_first_page", |store| {
                store.automation_pending_lifecycle_requests(100)
            })
            .await?;
        assert_eq!(pending_batch.len(), 100);

        let adapter = Arc::new(IncompleteSubmitAckVenue {
            inner: mock_venue.clone(),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            lookup_calls: Arc::new(AtomicUsize::new(0)),
        });
        let submit_calls = adapter.submit_calls.clone();
        let service = make_service_with_venue_overrides(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            Some(adapter),
            None,
            risk_guard,
        );
        service.recover_pending_automation_requests().await?;

        let pending_after = database
            .call("test_confirm_lifecycle_recovery_drained", |store| {
                store.automation_pending_lifecycle_requests(100)
            })
            .await?;
        assert!(pending_after.is_empty());
        let final_session_id = session_id.clone();
        let final_record = database
            .call("test_read_late_stop_final_session", move |store| {
                store.automation_runtime_record(&final_session_id, UtcTimestamp::now())
            })
            .await?;
        assert_eq!(final_record.view.state, AutomationSessionState::Stopped);
        let order_identifier = order.request.client_identifier.clone();
        let final_order = database
            .call("test_read_late_stop_reserved_order", move |store| {
                store.automation_order_readback(&order_identifier)
            })
            .await?
            .ok_or("late STOP lost its durable reserved order")?;
        assert_eq!(final_order.state, AutomationOrderState::Rejected);

        service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        assert_eq!(submit_calls.load(Ordering::SeqCst), 0);

        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "two joined JobRuntime starts prove a durable pre-crash STOP fences reserved MOCK work"
)]
fn joined_runtime_applies_pending_stop_before_restart_fence_on_two_starts()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationOrderState, AutomationRequestReadbackState, AutomationSessionState,
        AutomationSessionWrite, RequestId, UtcTimestamp,
    };
    use crate::jobs::JobRuntime;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-joined-stop-startup-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root)?;
    let first_owner = DatabaseOwner::open(root.clone())?;
    let first_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (session_id, stop_request_id, order_identifier) = first_runtime.block_on(async {
        let database = first_owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, session_id) = seed_reserved_mock_order(&database, &risk_guard).await?;
        assert_eq!(order.state, AutomationOrderState::Reserved);

        let session_for_sequence = session_id.clone();
        let session_record = database
            .call("test_read_joined_stop_session_sequence", move |store| {
                store.automation_runtime_record(&session_for_sequence, UtcTimestamp::now())
            })
            .await?;
        let stop_request_id = RequestId::new("automation-joined-startup-stop")?;
        let stop_write = AutomationSessionWrite::Stop {
            request_id: stop_request_id.clone(),
            session_id: session_id.clone(),
            expected_sequence: session_record.view.sequence,
        };
        let pending_stop = stop_write.clone();
        database
            .call(
                "test_begin_joined_startup_stop_before_crash",
                move |store| {
                    store
                        .automation_begin_request(&pending_stop, None, UtcTimestamp::now())
                        .map(|_| ())
                },
            )
            .await?;

        let started = JobRuntime::start_with_venue(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            crate::upbit::client::UpbitPrivateClient::new_mock(),
            root.clone(),
            None,
        )
        .await?;

        let request_for_readback = stop_request_id.clone();
        let readback = database
            .call("test_read_joined_startup_stop_receipt", move |store| {
                store.automation_readback_request(&request_for_readback)
            })
            .await?;
        assert_eq!(readback.state, AutomationRequestReadbackState::Applied);

        let session_for_state = session_id.clone();
        let stopped = database
            .call("test_read_joined_startup_stopped_session", move |store| {
                store.automation_runtime_record(&session_for_state, UtcTimestamp::now())
            })
            .await?;
        assert_eq!(stopped.view.state, AutomationSessionState::Stopped);

        let identifier = order.request.client_identifier.clone();
        let order_for_readback = identifier.clone();
        let rejected = database
            .call(
                "test_read_joined_startup_rejected_reservation",
                move |store| store.automation_order_readback(&order_for_readback),
            )
            .await?
            .ok_or("pending STOP lost its durable reserved order")?;
        assert_eq!(rejected.state, AutomationOrderState::Rejected);
        let mock_order_for_readback = identifier.clone();
        let mock_order = database
            .call("test_confirm_joined_startup_did_not_submit", move |store| {
                store.automation_mock_order_by_identifier(&mock_order_for_readback)
            })
            .await?;
        assert!(
            mock_order.is_none(),
            "the MOCK venue recorded an order effect"
        );

        started.shutdown().await?;
        drop(mock_venue);
        drop(database);
        Ok::<_, Box<dyn std::error::Error>>((session_id, stop_request_id, identifier))
    })?;
    drop(first_runtime);
    first_owner.shutdown()?;

    let second_owner = DatabaseOwner::open(root.clone())?;
    let second_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let second_result = second_runtime.block_on(async {
        let database = second_owner.handle();
        let started = JobRuntime::start_with_venue(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            crate::upbit::client::UpbitPrivateClient::new_mock(),
            root.clone(),
            None,
        )
        .await?;

        let request_for_readback = stop_request_id.clone();
        let readback = database
            .call("test_read_second_start_stop_receipt", move |store| {
                store.automation_readback_request(&request_for_readback)
            })
            .await?;
        assert_eq!(readback.state, AutomationRequestReadbackState::Applied);
        let session_for_state = session_id.clone();
        let stopped = database
            .call("test_read_second_start_stopped_session", move |store| {
                store.automation_runtime_record(&session_for_state, UtcTimestamp::now())
            })
            .await?;
        assert_eq!(stopped.view.state, AutomationSessionState::Stopped);
        let order_for_readback = order_identifier.clone();
        let rejected = database
            .call(
                "test_read_second_start_rejected_reservation",
                move |store| store.automation_order_readback(&order_for_readback),
            )
            .await?
            .ok_or("second start lost the durable reserved order")?;
        assert_eq!(rejected.state, AutomationOrderState::Rejected);
        let mock_order_for_readback = order_identifier.clone();
        let mock_order = database
            .call("test_confirm_second_start_did_not_submit", move |store| {
                store.automation_mock_order_by_identifier(&mock_order_for_readback)
            })
            .await?;
        assert!(
            mock_order.is_none(),
            "the second start submitted the MOCK order"
        );

        started.shutdown().await?;
        drop(database);
        Ok::<_, Box<dyn std::error::Error>>(())
    });
    drop(second_runtime);
    second_result?;
    second_owner.shutdown()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn emergency_after_send_claim_blocks_effect_and_preserves_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{AutomationOrderState, AutomationSessionQuery, LabError, UtcTimestamp};

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-after-claim-emergency-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let (order, dispatch, session_id) = seed_claimed_mock_order(&database, &risk_guard).await?;
        assert_eq!(order.state, AutomationOrderState::Reserved);

        let order_identifier = order.request.client_identifier.clone();
        let claimed = database
            .call("test_read_after_claim_state", move |store| {
                store.automation_order_readback(&order_identifier)
            })
            .await?
            .ok_or("send claim did not persist its order")?;
        assert_eq!(claimed.state, AutomationOrderState::SubmittedUnknown);

        let emergency_latch = risk_guard.latch_order_admission(true);
        assert!(emergency_latch.revision() > 0);
        assert!(matches!(
            risk_guard.authorize_order_effect(),
            Err(LabError::BlockedEvidence(reason))
                if reason == "EMERGENCY_STOP_BLOCKS_ORDER_CLAIM"
        ));

        let guarded_venue = Arc::new(LatchBeforeClaimVenue::new(
            mock_venue.clone(),
            risk_guard.clone(),
        ));
        let submit_calls = guarded_venue.submit_calls.clone();
        let service = make_service_with_venue_overrides(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            Some(guarded_venue),
            None,
            risk_guard.clone(),
        );
        service
            .submit_claimed_order(&order, AutomationSessionMode::Mock, dispatch, None)
            .await?;
        assert_eq!(submit_calls.load(Ordering::SeqCst), 0);

        let projection_session = session_id.clone();
        let projection: serde_json::Value = database
            .call("test_read_after_claim_reservation", move |store| {
                store.automation_query(
                    AutomationSessionQuery::Get {
                        session_id: projection_session,
                    },
                    UtcTimestamp::now(),
                )
            })
            .await?;
        assert_eq!(projection["unresolved_order_count"], 1);
        assert_eq!(projection["account_risk"]["risk"]["open_order_count"], 1);
        let quote_reserved: QuoteAmount = serde_json::from_value(
            projection["account_risk"]["balance"]["quote_local_reserved"].clone(),
        )?;
        assert_eq!(quote_reserved.get(), Decimal::from(10_005));
        let order_identifier = order.request.client_identifier.clone();
        let final_order = database
            .call("test_read_blocked_effect_order", move |store| {
                store.automation_order_readback(&order_identifier)
            })
            .await?
            .ok_or("blocked effect lost its durable order")?;
        assert_eq!(final_order.state, AutomationOrderState::SubmittedUnknown);

        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}

#[test]
fn fast_emergency_latch_before_claim_prevents_service_submit()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::contracts::{
        AutomationOrderKind, AutomationOrderState, AutomationSessionId, AutomationSide, LabError,
        MarketId, UtcTimestamp,
    };

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-automation-emergency-claim-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
        let database = owner.handle();
        let mock_venue = PersistentMockVenue::new(database.clone());
        mock_venue.initialize(UtcTimestamp::now()).await?;
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let guarded_venue = Arc::new(LatchBeforeClaimVenue::new(
            mock_venue.clone(),
            risk_guard.clone(),
        ));
        let chance_calls = guarded_venue.chance_calls.clone();
        let submit_calls = guarded_venue.submit_calls.clone();
        let service = make_service_with_venue_overrides(
            database.clone(),
            UpbitClient::synthetic_local("http://127.0.0.1:1")?,
            mock_venue,
            root.clone(),
            Some(guarded_venue),
            None,
            risk_guard.clone(),
        );
        let pending = crate::automation::AutomationPendingOrder {
            session_id: AutomationSessionId::new("automation-emergency-test")?,
            account_identity: "mock:upbit-default".into(),
            state: AutomationOrderState::Reserved,
            request: AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::new("auto-emergency-test")?,
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: QuoteAmount::new(Decimal::from(10_000))?,
                },
            },
            decision_mark: PriceKrw::new(Decimal::from(10_000))?,
        };
        let result = service
            .dispatch_reserved_order(&pending, AutomationSessionMode::Mock)
            .await;
        assert!(matches!(result, Err(LabError::BlockedEvidence(_))));
        assert_eq!(chance_calls.load(Ordering::SeqCst), 1);
        assert_eq!(submit_calls.load(Ordering::SeqCst), 0);
        assert!(risk_guard.is_emergency_stopped());
        drop(service);
        drop(database);
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    outcome?;
    closed?;
    Ok(())
}
