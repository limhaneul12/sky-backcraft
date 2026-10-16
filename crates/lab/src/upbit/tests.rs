//! Comprehensive tests for Upbit integration modules.

use super::approval::HumanApprovalGate;
use super::auth::{UpbitCredentials, verify_host_allowlist};
use super::client::{UpbitOrderDetail, UpbitPrivateClient, UpbitTestOrderRequest};
use super::control::{
    ExchangeReadMode, TradingControlAction, TradingControlDeniedCode, TradingControlOutcome,
    TradingControlRequest, TradingControlService, TradingControlState,
};
use super::execution::{OrderExecuteRequest, OrderExecutionService};
use super::preview::{OrderPreview, OrderPreviewRequest};
use super::reconciliation::{NormalizedOrderState, mock_cancel_order, reconcile_order_detail};
use super::risk::{RiskConfig, RiskGuard};
use chrono::Utc;
use rust_decimal::Decimal;

fn control_request(
    action: TradingControlAction,
    request_id: &str,
    expected_sequence: u64,
    approval_token: Option<&str>,
) -> TradingControlRequest {
    TradingControlRequest {
        action,
        request_id: Some(request_id.to_owned()),
        expected_sequence: Some(expected_sequence),
        approval_token: approval_token.map(str::to_owned),
    }
}

fn control_status_request() -> TradingControlRequest {
    TradingControlRequest {
        action: TradingControlAction::Status,
        request_id: None,
        expected_sequence: None,
        approval_token: None,
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one durable control journey covers config denial, stale CAS, idempotency and reopen"
)]
fn trading_control_is_fail_closed_durable_and_idempotent() -> Result<(), Box<dyn std::error::Error>>
{
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("spot-lab-control-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&root)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let owner = crate::database::DatabaseOwner::open(root.clone())?;
    let risk = RiskGuard::new(RiskConfig::default());
    let service =
        TradingControlService::production(owner.handle(), risk.clone(), ExchangeReadMode::Mock);
    runtime.block_on(async {
        let initial = service.execute(control_status_request()).await?;
        assert_eq!(initial.state, TradingControlState::DisabledByConfig);
        assert!(!initial.live_trading_enabled);
        assert!(initial.order_admission_blocked);
        assert_eq!(initial.sequence, 0);

        let stale_emergency = service
            .execute(control_request(
                TradingControlAction::EmergencyStop,
                "control-stale-emergency",
                99,
                None,
            ))
            .await;
        assert!(matches!(
            stale_emergency,
            Err(crate::contracts::LabError::Conflict(_))
        ));
        let after_stale = service.execute(control_status_request()).await?;
        assert_eq!(after_stale.state, TradingControlState::DisabledByConfig);
        assert!(after_stale.order_admission_blocked);
        assert!(!after_stale.emergency_latched);
        // A rejected stale emergency restores the durable user latch. LIVE-off
        // stays in the effect gate, rather than pausing credential-free MOCK.
        assert!(risk.with_order_admission(|| Ok(())).is_ok());

        let denied = service
            .execute(control_request(
                TradingControlAction::Start,
                "control-start-denied",
                0,
                Some("forged-local-approval"),
            ))
            .await?;
        assert_eq!(denied.state, TradingControlState::DisabledByConfig);
        assert_eq!(
            denied.denied_code,
            Some(TradingControlDeniedCode::LiveTradingDisabled)
        );
        assert_eq!(denied.sequence, 0);
        let replay = service
            .execute(control_request(
                TradingControlAction::Start,
                "control-start-denied",
                0,
                Some("different-forged-token"),
            ))
            .await?;
        assert_eq!(replay.receipt, denied.receipt);

        let emergency = service
            .execute(control_request(
                TradingControlAction::EmergencyStop,
                "control-emergency",
                0,
                None,
            ))
            .await?;
        assert_eq!(emergency.state, TradingControlState::EmergencyStopActive);
        assert_eq!(emergency.sequence, 1);
        assert!(emergency.emergency_latched);
        assert!(risk.is_emergency_stopped());

        let stale = service
            .execute(control_request(
                TradingControlAction::Pause,
                "control-stale",
                0,
                None,
            ))
            .await;
        assert!(matches!(
            stale,
            Err(crate::contracts::LabError::Conflict(_))
        ));
        Ok::<(), crate::contracts::LabError>(())
    })?;
    owner.shutdown()?;

    let reopened = crate::database::DatabaseOwner::open(root.clone())?;
    let reopened_risk = RiskGuard::new(RiskConfig::default());
    let reopened_service = TradingControlService::production(
        reopened.handle(),
        reopened_risk.clone(),
        ExchangeReadMode::Mock,
    );
    runtime.block_on(async {
        let status = reopened_service.execute(control_status_request()).await?;
        assert_eq!(status.state, TradingControlState::EmergencyStopActive);
        assert_eq!(status.sequence, 1);
        assert!(status.emergency_latched);
        assert!(reopened_risk.is_emergency_stopped());
        Ok::<(), crate::contracts::LabError>(())
    })?;
    reopened.shutdown()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn cancelled_emergency_reply_still_commits_and_blocks_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-control-cancel-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root)?;
    let owner = crate::database::DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let risk = RiskGuard::new(RiskConfig::default());
    let service =
        TradingControlService::production(database.clone(), risk.clone(), ExchangeReadMode::Mock);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        service.synchronize_from_storage().await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let blocked_database = database.clone();
        let blocker = tokio::spawn(async move {
            blocked_database
                .call("control_cancel_test_blocker", move |_store| {
                    started_tx.send(()).map_err(|()| {
                        crate::contracts::LabError::Internal(
                            "control cancel test start receiver closed".into(),
                        )
                    })?;
                    release_rx
                        .recv()
                        .map_err(|error| crate::contracts::LabError::Internal(error.to_string()))?;
                    Ok(())
                })
                .await
        });
        started_rx.await?;

        let emergency_service = service.clone();
        let emergency = tokio::spawn(async move {
            emergency_service
                .execute(control_request(
                    TradingControlAction::EmergencyStop,
                    "cancelled-emergency",
                    0,
                    None,
                ))
                .await
        });
        tokio::task::yield_now().await;
        assert!(risk.is_emergency_stopped());
        emergency.abort();
        release_tx.send(())?;
        blocker.await??;

        let status = service.execute(control_status_request()).await?;
        assert_eq!(status.state, TradingControlState::EmergencyStopActive);
        assert_eq!(status.sequence, 1);
        assert!(status.emergency_latched);
        assert!(risk.is_emergency_stopped());

        let preview = OrderPreview::build(
            OrderPreviewRequest {
                market: "KRW-BTC".into(),
                side: "bid".into(),
                ord_type: "limit".into(),
                price: Some(Decimal::new(90_000_000, 0)),
                volume: Some(Decimal::new(1, 4)),
                current_krw_balance: None,
                current_coin_balance: None,
            },
            Utc::now(),
        )
        .map_err(|error| crate::contracts::LabError::InvalidConfig(error.to_string()))?;
        let approval = HumanApprovalGate::default();
        let token = approval
            .issue_token(&preview.preview_digest, preview.valid_until_utc)
            .await?;
        let execution = OrderExecutionService::new(risk.clone(), approval)
            .execute(
                OrderExecuteRequest {
                    preview,
                    human_approval_token: Some(token),
                    client_identifier: Some("blocked-after-cancel".into()),
                    allow_mock_execution: true,
                },
                Utc::now(),
            )
            .await;
        assert!(matches!(
            execution,
            Err(super::execution::ExecutionError::Risk(
                super::risk::RiskError::EmergencyStopActive
            ))
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    owner.shutdown()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one control journey verifies approval, transitions, stale replay and execution gating"
)]
fn isolated_mock_control_exercises_approval_and_transitions()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-control-mock-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root)?;
    let owner = crate::database::DatabaseOwner::open(root.clone())?;
    let risk = RiskGuard::new(RiskConfig::default());
    let service = TradingControlService::isolated_mock_enabled(
        owner.handle(),
        risk.clone(),
        "trusted-test-approval".into(),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let denied = service
            .execute(control_request(
                TradingControlAction::Start,
                "mock-start-denied",
                0,
                Some("forged"),
            ))
            .await?;
        assert_eq!(
            denied.denied_code,
            Some(TradingControlDeniedCode::ApprovalRequired)
        );

        let running = service
            .execute(control_request(
                TradingControlAction::Start,
                "mock-start",
                0,
                Some("trusted-test-approval"),
            ))
            .await?;
        assert_eq!(running.state, TradingControlState::Running);
        assert_eq!(
            running.receipt.as_ref().map(|receipt| receipt.outcome),
            Some(TradingControlOutcome::Acknowledged)
        );

        let paused = service
            .execute(control_request(
                TradingControlAction::Pause,
                "mock-pause",
                1,
                None,
            ))
            .await?;
        assert_eq!(paused.state, TradingControlState::Paused);
        assert!(paused.order_admission_blocked);

        let resumed = service
            .execute(control_request(
                TradingControlAction::Resume,
                "mock-resume",
                2,
                Some("trusted-test-approval"),
            ))
            .await?;
        assert_eq!(resumed.state, TradingControlState::Running);
        assert_eq!(resumed.sequence, 3);

        let emergency = service
            .execute(control_request(
                TradingControlAction::EmergencyStop,
                "mock-emergency",
                3,
                None,
            ))
            .await?;
        assert_eq!(emergency.state, TradingControlState::EmergencyStopActive);
        assert_eq!(emergency.sequence, 4);

        let old_start_replay = service
            .execute(control_request(
                TradingControlAction::Start,
                "mock-start",
                0,
                Some("trusted-test-approval"),
            ))
            .await?;
        assert_eq!(
            old_start_replay.state,
            TradingControlState::EmergencyStopActive
        );
        assert_eq!(old_start_replay.sequence, 4);
        assert_eq!(
            old_start_replay
                .receipt
                .as_ref()
                .map(|receipt| receipt.sequence),
            Some(1)
        );
        assert!(risk.is_emergency_stopped());

        let preview = OrderPreview::build(
            OrderPreviewRequest {
                market: "KRW-BTC".into(),
                side: "bid".into(),
                ord_type: "limit".into(),
                price: Some(Decimal::new(90_000_000, 0)),
                volume: Some(Decimal::new(1, 4)),
                current_krw_balance: None,
                current_coin_balance: None,
            },
            Utc::now(),
        )
        .map_err(|error| crate::contracts::LabError::InvalidConfig(error.to_string()))?;
        let approval = HumanApprovalGate::default();
        let token = approval
            .issue_token(&preview.preview_digest, preview.valid_until_utc)
            .await
            .map_err(|error| crate::contracts::LabError::InvalidConfig(error.to_string()))?;
        let execution = OrderExecutionService::new(risk.clone(), approval)
            .execute(
                OrderExecuteRequest {
                    preview,
                    human_approval_token: Some(token),
                    client_identifier: Some("blocked-after-replay".into()),
                    allow_mock_execution: true,
                },
                Utc::now(),
            )
            .await;
        assert!(matches!(
            execution,
            Err(super::execution::ExecutionError::Risk(
                super::risk::RiskError::EmergencyStopActive
            ))
        ));
        Ok::<(), crate::contracts::LabError>(())
    })?;
    owner.shutdown()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn test_preview_all_order_types() {
    let now = Utc::now();

    // 1. Limit Buy
    let limit_buy = OrderPreview::build(
        OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(90_000_000, 0)),
            volume: Some(Decimal::new(1, 2)), // 0.01 BTC = 900,000 KRW
            current_krw_balance: Some(Decimal::new(1_000_000, 0)),
            current_coin_balance: None,
        },
        now,
    )
    .unwrap();
    assert_eq!(limit_buy.calculated_notional_krw, Decimal::new(900_000, 0));
    assert!(limit_buy.verify_digest().is_ok());

    // 2. Market Buy ('price' type, requires price as KRW total, volume must be None)
    let market_buy = OrderPreview::build(
        OrderPreviewRequest {
            market: "KRW-ETH".into(),
            side: "bid".into(),
            ord_type: "price".into(),
            price: Some(Decimal::new(100_000, 0)), // 100,000 KRW total
            volume: None,
            current_krw_balance: Some(Decimal::new(200_000, 0)),
            current_coin_balance: None,
        },
        now,
    )
    .unwrap();
    assert_eq!(market_buy.calculated_notional_krw, Decimal::new(100_000, 0));
    assert_eq!(market_buy.volume, None);

    // 3. Market Sell ('market' type, requires volume as coin amount, price must be None)
    let market_sell = OrderPreview::build(
        OrderPreviewRequest {
            market: "KRW-XRP".into(),
            side: "ask".into(),
            ord_type: "market".into(),
            price: None,
            volume: Some(Decimal::new(50, 0)), // 50 XRP
            current_krw_balance: None,
            current_coin_balance: Some(Decimal::new(100, 0)),
        },
        now,
    )
    .unwrap();
    assert_eq!(market_sell.price, None);
    assert_eq!(market_sell.volume, Some(Decimal::new(50, 0)));
}

#[tokio::test]
async fn test_risk_guard_violations() {
    let config = RiskConfig {
        max_single_order_krw: Decimal::new(100_000, 0), // 100k ceiling
        ..Default::default()
    };
    let guard = RiskGuard::new(config);
    let now = Utc::now();

    // Normal passes
    assert!(
        guard
            .validate_order("KRW-BTC", Decimal::new(50_000, 0), now)
            .await
            .is_ok()
    );

    // Exceeds single order limit
    let err = guard
        .validate_order("KRW-BTC", Decimal::new(150_000, 0), now)
        .await;
    assert!(err.is_err());

    // Emergency stop
    let stale_status_revision = guard.control_revision();
    guard.trigger_emergency_stop();
    assert!(
        guard
            .validate_order("KRW-BTC", Decimal::new(10_000, 0), now)
            .await
            .is_err()
    );
    assert!(!guard.synchronize_order_admission(stale_status_revision, false, false));
    assert!(guard.is_emergency_stopped());
    guard.clear_emergency_stop();
    let later = now + chrono::Duration::seconds(5);
    assert!(
        guard
            .validate_order("KRW-BTC", Decimal::new(10_000, 0), later)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn test_human_approval_lifecycle() {
    let gate = HumanApprovalGate::default();
    let now = Utc::now();
    let digest = "test-digest-abcdef123456";
    let valid_until = now + chrono::Duration::seconds(60);

    let token = gate.issue_token(digest, valid_until).await.unwrap();

    // Missing token fails
    assert!(gate.verify_and_consume(None, digest, now).await.is_err());

    // Mismatched digest fails
    assert!(
        gate.verify_and_consume(Some(&token), "other-digest", now)
            .await
            .is_err()
    );

    // Valid verification passes
    assert!(
        gate.verify_and_consume(Some(&token), digest, now)
            .await
            .is_ok()
    );

    // Replay fails
    assert!(
        gate.verify_and_consume(Some(&token), digest, now)
            .await
            .is_err()
    );

    let expired = gate
        .issue_token(digest, now - chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(matches!(
        gate.verify_and_consume(Some(&expired), digest, now).await,
        Err(super::approval::ApprovalError::Expired { .. })
    ));
}

#[tokio::test]
async fn test_execution_gate_live_disabled_and_mock() {
    let risk_guard = RiskGuard::new(RiskConfig::default());
    let approval_gate = HumanApprovalGate::default();
    let exec = OrderExecutionService::new(risk_guard, approval_gate.clone());
    let now = Utc::now();

    let preview = OrderPreview::build(
        OrderPreviewRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(95_000_000, 0)),
            volume: Some(Decimal::new(1, 2)),
            current_krw_balance: None,
            current_coin_balance: None,
        },
        now,
    )
    .unwrap();

    let token = approval_gate
        .issue_token(&preview.preview_digest, preview.valid_until_utc)
        .await
        .unwrap();

    // 1. Attempt live execution -> BLOCKED by hardcoded contract
    let live_req = OrderExecuteRequest {
        preview: preview.clone(),
        human_approval_token: Some(token.clone()),
        client_identifier: Some("test-client-1".into()),
        allow_mock_execution: false,
    };
    let err = exec.execute(live_req, now).await;
    assert!(err.is_err());

    // 2. The same token and same timestamp still pass in mock mode, proving
    // config-off rejection consumed neither approval nor risk budget.
    let mock_req = OrderExecuteRequest {
        preview,
        human_approval_token: Some(token),
        client_identifier: Some("test-client-2".into()),
        allow_mock_execution: true,
    };
    let receipt = exec.execute(mock_req, now).await.unwrap();
    assert_eq!(receipt.status, "MOCK_EXECUTED_ACCEPTED");
    assert!(receipt.is_mock);
}

#[tokio::test]
async fn test_client_mock_methods() {
    let client = UpbitPrivateClient::new_mock();
    assert!(!client.is_live());

    let accounts = client.get_accounts().await.unwrap();
    assert!(accounts.is_empty());

    let chance = client.get_order_chance("KRW-BTC").await.unwrap();
    assert_eq!(chance.bid_fee, Decimal::new(5, 4));

    let order = client.get_order(Some("mock-uuid"), None).await.unwrap();
    assert_eq!(order.state, "done");

    assert!(
        client
            .get_open_orders(None, Some(1), Some(100))
            .await
            .is_ok()
    );
    assert!(
        client
            .get_open_orders(None, Some(0), Some(100))
            .await
            .is_err()
    );
    assert!(
        client
            .get_open_orders(None, Some(1), Some(101))
            .await
            .is_err()
    );
    assert!(
        client
            .get_closed_orders(None, None, None, Some(1_000))
            .await
            .is_ok()
    );
    assert!(
        client
            .get_closed_orders(None, None, None, Some(1_001))
            .await
            .is_err()
    );
    assert!(
        client
            .get_closed_orders(
                None,
                Some("2026-10-01T00:00:00+09:00"),
                Some("2026-10-09T00:00:00+09:00"),
                Some(100),
            )
            .await
            .is_err()
    );

    let test_res = client
        .post_order_test(&UpbitTestOrderRequest {
            market: "KRW-BTC".into(),
            side: "bid".into(),
            ord_type: "limit".into(),
            volume: Some("0.01".into()),
            price: Some("95000000".into()),
            identifier: Some("id-test".into()),
        })
        .await
        .unwrap();
    assert_eq!(test_res.status, "MOCK_TEST_ONLY");
    assert!(!test_res.test_order_acknowledged);
}

#[tokio::test]
async fn test_auth_and_reconciliation() {
    let creds = UpbitCredentials::new("acc-test".into(), "sec-test".into());
    assert_eq!(creds.access_key(), "acc-test");
    assert!(verify_host_allowlist("https://api.upbit.com/v1/accounts").is_ok());
    assert!(verify_host_allowlist("https://evil.com").is_err());

    let detail = UpbitOrderDetail {
        uuid: "test-uuid-reconcile".into(),
        identifier: None,
        side: "bid".into(),
        ord_type: "limit".into(),
        price: Some(Decimal::new(95_000_000, 0)),
        state: "done".into(),
        market: "KRW-BTC".into(),
        created_at: "2026-10-12T00:00:00Z".into(),
        volume: Some(Decimal::new(1, 2)),
        remaining_volume: Some(Decimal::ZERO),
        reserved_fee: None,
        remaining_fee: None,
        paid_fee: Some(Decimal::new(475, 0)),
        locked: None,
        executed_volume: Some(Decimal::new(1, 2)),
        trades_count: Some(1),
        trades: vec![],
    };

    let report = reconcile_order_detail(Decimal::new(1, 2), &detail);
    assert_eq!(report.normalized_state, NormalizedOrderState::Filled);
    assert_eq!(report.executed_volume, Decimal::new(1, 2));

    let cancel = mock_cancel_order("test-uuid-reconcile", Decimal::ZERO);
    assert_eq!(cancel.status, "MOCK_CANCELLED");
    assert!(!cancel.real_exchange_delete_called);
}
