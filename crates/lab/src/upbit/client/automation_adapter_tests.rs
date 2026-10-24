use super::*;
use crate::contracts::{AssetQuantity, MarketId, PriceKrw, QuoteAmount};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn intent(side: AutomationSide, order: AutomationOrderKind) -> AutomationOrderRequest {
    AutomationOrderRequest {
        client_identifier: AutomationOrderIdentifier::try_from("automation-wire-1".to_owned())
            .unwrap(),
        market: MarketId::parse_upbit("KRW-BTC").unwrap(),
        side,
        order,
    }
}

#[test]
fn canonical_requests_preserve_market_buy_budget_and_sell_quantity() {
    let buy = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    let wire = automation_wire(&buy).unwrap();
    assert_eq!(
        serde_json::to_value(&wire).unwrap(),
        serde_json::json!({
            "market":"KRW-BTC","side":"bid","ord_type":"price",
            "price":"100","identifier":"automation-wire-1"
        })
    );
    assert_eq!(
        canonical_order_query(&wire),
        "market=KRW-BTC&side=bid&price=100&ord_type=price&identifier=automation-wire-1"
    );
    let sell = intent(
        AutomationSide::Ask,
        AutomationOrderKind::MarketSell {
            volume: AssetQuantity::new(Decimal::new(1, 2)).unwrap(),
        },
    );
    let wire = automation_wire(&sell).unwrap();
    assert_eq!(wire.volume.as_deref(), Some("0.01"));
    assert!(wire.price.is_none());
    assert!(automation_wire(&intent(AutomationSide::Ask, buy.order)).is_err());
    assert!(validate_order_token("id&identifier=other").is_err());
}

#[tokio::test]
async fn production_gate_rejects_before_any_network_or_signing() {
    let client = UpbitPrivateClient::new_live(UpbitCredentials::new(
        "fake-adapter-access".into(),
        "fake-adapter-secret".into(),
    ))
    .unwrap();
    let request = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    assert!(matches!(
        client.submit_automation_order(&request).await,
        Err(ClientError::LiveTradingDisabled)
    ));
    assert!(matches!(
        client.cancel_automation_order("order-wire-1").await,
        Err(ClientError::LiveTradingDisabled)
    ));
    assert!(matches!(
        client
            .post_order_test(&automation_wire(&request).unwrap())
            .await,
        Err(ClientError::LiveTradingDisabled)
    ));
}

#[tokio::test]
async fn operator_policy_enables_only_automation_order_gate() {
    let client = UpbitPrivateClient::new_live_with_policy(
        UpbitCredentials::new("fake-policy-access".into(), "fake-policy-secret".into()),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    assert!(client.live_execution_policy().is_enabled());
    assert!(client.check_effect_gate().is_ok());
    assert!(matches!(
        client.cancel_automation_order("order-wire-1").await,
        Err(ClientError::LiveTradingDisabled)
    ));
    let request = UpbitOrderRequest {
        market: "KRW-BTC".into(),
        side: "bid".into(),
        volume: None,
        price: Some("100".into()),
        ord_type: "price".into(),
        identifier: Some("policy-no-test-order".into()),
    };
    assert!(matches!(
        client.post_order_test(&request).await,
        Err(ClientError::LiveTradingDisabled)
    ));
}

#[tokio::test]
async fn managed_live_post_fails_before_loopback_request_when_switch_is_off() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{}", listener.local_addr().unwrap()),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    client.live_order_send_gate =
        Some(crate::upbit::live_order_control::LiveOrderSendGate::new_disabled());
    let request = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    assert!(matches!(
        client.submit_automation_order(&request).await,
        Err(ClientError::LiveOrderControlDenied)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "OFF reached the loopback provider socket"
    );
}

#[tokio::test]
async fn managed_post_waiting_for_throttle_cannot_survive_off_then_on() {
    use std::future::Future as _;
    use std::task::Poll;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{}", listener.local_addr().unwrap()),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    let gate = crate::upbit::live_order_control::LiveOrderSendGate::new_disabled();
    assert!(gate.open_if_current(gate.generation()).unwrap());
    client.live_order_send_gate = Some(gate.clone());
    let request = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    let throttle = client.rate_limit.lock().await;
    let sending = client.submit_automation_order(&request);
    tokio::pin!(sending);
    assert!(std::future::poll_fn(|cx| Poll::Ready(sending.as_mut().poll(cx).is_pending())).await);
    gate.close().unwrap();
    assert!(gate.open_if_current(gate.generation()).unwrap());
    drop(throttle);
    assert!(matches!(
        sending.await,
        Err(ClientError::LiveOrderControlDenied)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "pre-OFF request escaped after re-enable"
    );
}

#[tokio::test]
async fn live_post_waiting_for_throttle_cannot_survive_private_generation_recovery() {
    use std::future::Future as _;
    use std::task::Poll;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{}", listener.local_addr().unwrap()),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    let send_gate = crate::upbit::live_order_control::LiveOrderSendGate::new_disabled();
    assert!(send_gate.open_if_current(send_gate.generation()).unwrap());
    client.live_order_send_gate = Some(send_gate.clone());
    let fence = client.private_readiness_fence();
    let captured_generation = fence.ready_generation().unwrap();
    let request = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    let sending_client = client.clone();
    let throttle = client.rate_limit.lock().await;
    let sending = sending_client.submit_automation_order(&request);
    tokio::pin!(sending);
    assert!(std::future::poll_fn(|cx| Poll::Ready(sending.as_mut().poll(cx).is_pending())).await);

    let recovered_generation = fence.invalidate_for_recovery(captured_generation).unwrap();
    fence.mark_recovering(recovered_generation).unwrap();
    assert!(fence.mark_ready(recovered_generation));
    assert!(fence.is_ready_generation(recovered_generation));
    assert!(!fence.is_ready_generation(captured_generation));

    drop(throttle);
    assert!(matches!(
        sending.await,
        Err(ClientError::PrivateAccountNotReady)
    ));
    assert!(send_gate.is_enabled());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "a POST admitted under the previous private generation reached the loopback socket"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one barrier-controlled loopback POST proves real provider lease drain and denial without actual Upbit access"
)]
async fn managed_disable_waits_for_actual_loopback_post_and_blocks_a_new_one() {
    use crate::database::DatabaseOwner;
    use crate::upbit::control::ExchangeReadMode;
    use crate::upbit::live_order_control::{
        LiveOrderControlAction, LiveOrderControlRequest, LiveOrderControlService, LiveOrderSendGate,
    };
    use std::future::Future as _;
    use std::task::Poll;
    let root = std::env::temp_dir().join(format!(
        "backcraft-http-drain-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let owner = DatabaseOwner::open_with_live_order_control(root.clone(), true).unwrap();
    let gate = LiveOrderSendGate::new_disabled();
    let service = LiveOrderControlService::new(
        owner.handle(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(gate.clone()),
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
        .await
        .unwrap();
    let enabled = service
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Enable,
            request_id: Some("held-post-enable".into()),
            expected_sequence: Some(status.sequence),
            expected_epoch: Some(status.boot_epoch),
            confirmation: Some("ENABLE_LIVE_ORDER_SENDING".into()),
        })
        .await
        .unwrap();
    assert!(enabled.live_trading_enabled);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{}", listener.local_addr().unwrap()),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    client.live_order_send_gate = Some(gate.clone());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_bounded_request(&mut socket).await;
        assert!(request.starts_with(b"POST /v1/orders "));
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = response("automation-wire-1", "wait");
        let headers = format!(
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "OFF allowed a second provider connection"
        );
    });
    let sending_client = client.clone();
    let sending = tokio::spawn(async move {
        sending_client
            .submit_automation_order(&intent(
                AutomationSide::Bid,
                AutomationOrderKind::MarketBuy {
                    budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
                },
            ))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let disabling = service.execute(LiveOrderControlRequest {
        action: LiveOrderControlAction::Disable,
        request_id: Some("held-post-disable".into()),
        expected_sequence: Some(enabled.sequence),
        expected_epoch: Some(enabled.boot_epoch),
        confirmation: None,
    });
    tokio::pin!(disabling);
    assert!(std::future::poll_fn(|cx| Poll::Ready(disabling.as_mut().poll(cx).is_pending())).await);
    assert!(!gate.is_enabled());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), disabling.as_mut())
            .await
            .is_err(),
        "OFF acknowledged before the admitted HTTP send finished"
    );
    let denied = client
        .submit_automation_order(&intent(
            AutomationSide::Bid,
            AutomationOrderKind::MarketBuy {
                budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
            },
        ))
        .await;
    assert!(matches!(denied, Err(ClientError::LiveOrderControlDenied)));
    release_tx.send(()).unwrap();
    assert!(sending.await.unwrap().is_ok());
    let off = tokio::time::timeout(Duration::from_secs(2), disabling)
        .await
        .unwrap()
        .unwrap();
    assert!(!off.live_trading_enabled);
    assert_eq!(
        off.receipt.unwrap().outcome,
        crate::upbit::live_order_control::LiveOrderControlOutcome::Acknowledged
    );
    server.await.unwrap();
    owner.shutdown().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn response(identifier: &str, state: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "uuid":"order-wire-1", "identifier":identifier, "side":"bid", "ord_type":"price",
        "price":"100", "state":state, "market":"KRW-BTC", "created_at":"2026-10-14T00:00:00Z",
        "volume":"0.01", "remaining_volume":"0", "reserved_fee":"0.05", "remaining_fee":"0",
        "paid_fee":"0.05", "locked":"0", "executed_volume":"0.01", "trades_count":1,
        "trades":[{"uuid":"trade-wire-1","market":"KRW-BTC","side":"bid","price":"10000",
            "volume":"0.01","funds":"100","created_at":"2026-10-14T00:00:00Z"}]
    }))
    .unwrap()
}

async fn read_bounded_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    loop {
        let mut bytes = [0_u8; 2048];
        let count = stream.read(&mut bytes).await.unwrap();
        assert!(count > 0 && request.len() + count <= 16_384);
        request.extend_from_slice(&bytes[..count]);
        if let Some(end) = request.windows(4).position(|chunk| chunk == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
            let length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map_or(0, |value| value.parse::<usize>().unwrap());
            if request.len() >= end + 4 + length {
                return request;
            }
        }
    }
}

async fn fixture(
    responses: Vec<(u16, Vec<u8>)>,
) -> (UpbitPrivateClient, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let request = read_bounded_request(&mut stream).await;
            captured.push(request);
            let headers = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        }
        captured
    });
    let client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{address}"),
        LiveExecutionPolicy::OperatorEnabled,
    )
    .unwrap();
    (client, server)
}

#[tokio::test]
async fn loopback_adapter_uses_signed_json_and_identifier_readback_then_delete() {
    let (client, server) = fixture(vec![
        (201, response("automation-wire-1", "wait")),
        (200, response("automation-wire-1", "done")),
        (200, response("automation-wire-1", "cancel")),
    ])
    .await;
    let request = intent(
        AutomationSide::Bid,
        AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(100, 0)).unwrap(),
        },
    );
    let venue: &dyn crate::automation::VenueAdapter = &client;
    assert_eq!(
        venue
            .submit_order(
                &request,
                PriceKrw::new(Decimal::new(10000, 0)).unwrap(),
                None
            )
            .await
            .unwrap()
            .uuid,
        "order-wire-1"
    );
    assert!(
        client
            .automation_order_by_identifier(&request.client_identifier)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        client
            .cancel_automation_order("order-wire-1")
            .await
            .unwrap()
            .state,
        "cancel"
    );
    let captured = server.await.unwrap();
    assert_eq!(captured.len(), 3);
    let post = String::from_utf8(captured[0].clone()).unwrap();
    assert!(post.starts_with("POST /v1/orders HTTP/1.1\r\n"));
    assert!(post.to_lowercase().contains("authorization: bearer "));
    let body = post.split_once("\r\n\r\n").unwrap().1;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(body).unwrap(),
        serde_json::to_value(automation_wire(&request).unwrap()).unwrap()
    );
    assert!(
        String::from_utf8_lossy(&captured[1])
            .starts_with("GET /v1/order?identifier=automation-wire-1 HTTP/1.1")
    );
    assert!(
        String::from_utf8_lossy(&captured[2])
            .starts_with("DELETE /v1/order?uuid=order-wire-1 HTTP/1.1")
    );
    assert!(
        client
            .verify_request_host("http://example.com/v1/orders")
            .is_err()
    );
}

#[tokio::test]
async fn identifier_lookup_rejects_wrong_identity_and_distinguishes_true_not_found() {
    let (client, server) = fixture(vec![
        (200, response("wrong-id", "done")),
        (404, br#"{"error":{"name":"order_not_found"}}"#.to_vec()),
        (404, br#"{"error":{"name":"route_not_found"}}"#.to_vec()),
    ])
    .await;
    let id = AutomationOrderIdentifier::try_from("automation-wire-1".to_owned()).unwrap();
    assert!(matches!(
        client.automation_order_by_identifier(&id).await,
        Err(ClientError::InvalidParams(_))
    ));
    assert!(
        client
            .automation_order_by_identifier(&id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        client.automation_order_by_identifier(&id).await,
        Err(ClientError::Api { status: 404, .. })
    ));
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn fresh_instrument_policy_overrides_deprecated_tick_without_private_auth_on_public_read() {
    let chance = serde_json::json!({
        "bid_fee":"0.0005", "ask_fee":"0.0005", "market":{
            "id":"KRW-BTC","name":"BTC/KRW","state":"active",
            "order_sides":["bid","ask"],"bid_types":["limit","price"],"ask_types":["limit","market"],
            "bid":{"currency":"KRW","price_unit":"999","min_total":"5000"},
            "ask":{"currency":"BTC","price_unit":"999","min_total":"5000"},"max_total":"1000000000"
        }, "bid_account":null,"ask_account":null
    });
    let instrument = serde_json::json!([{"market":"KRW-BTC","quote_currency":"KRW","tick_size":"1000","supported_levels":["0"]}]);
    let (client, receiver) = fixture(vec![
        (200, serde_json::to_vec(&chance).unwrap()),
        (200, serde_json::to_vec(&instrument).unwrap()),
    ])
    .await;
    let normalized = client.get_order_chance("KRW-BTC").await.unwrap();
    assert_eq!(
        normalized
            .market
            .pointer("/bid/price_unit")
            .and_then(serde_json::Value::as_str),
        Some("1000")
    );
    assert_eq!(
        normalized
            .market
            .get("tick_size_source")
            .and_then(serde_json::Value::as_str),
        Some("UPBIT_ORDERBOOK_INSTRUMENTS")
    );
    let requests = receiver.await.unwrap();
    let public_read = String::from_utf8(requests[1].clone())
        .unwrap()
        .to_ascii_lowercase();
    assert!(public_read.starts_with("get /v1/orderbook/instruments?markets=krw-btc "));
    assert!(!public_read.contains("authorization:"));
}

#[tokio::test]
async fn mismatched_instrument_identity_has_no_static_tick_fallback() {
    let body = serde_json::to_vec(
        &serde_json::json!([{"market":"KRW-ETH","quote_currency":"KRW","tick_size":"1000"}]),
    )
    .unwrap();
    let (client, receiver) = fixture(vec![(200, body)]).await;
    assert!(matches!(
        client.get_orderbook_tick_size("KRW-BTC").await,
        Err(ClientError::InvalidParams(_))
    ));
    assert_eq!(receiver.await.unwrap().len(), 1);
}
