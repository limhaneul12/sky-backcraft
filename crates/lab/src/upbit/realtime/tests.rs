use super::*;
use crate::contracts::automation::{
    AutomationOrderIdentifier, AutomationOrderKind, AutomationOrderRequest, AutomationSide,
};
use crate::contracts::{MarketId, QuoteAmount};
use crate::upbit::auth::{UpbitCredentials, base64url_encode, hmac_sha256};
use crate::upbit::client::{ClientError, UpbitPrivateClient};
use crate::upbit::execution::LiveExecutionPolicy;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as ServerRequest, Response as ServerResponse,
};

fn my_order_trade_frame() -> &'static [u8] {
    br#"{"type":"myOrder","code":"KRW-BTC","uuid":"provider-order","ask_bid":"BID","order_type":"limit","state":"trade","trade_uuid":"provider-trade","price":100.000000000000000001,"avg_price":100.000000000000000001,"volume":0.123456789123456789,"remaining_volume":0,"executed_volume":0.123456789123456789,"trades_count":1,"reserved_fee":0.000061728394561728,"remaining_fee":0,"paid_fee":0.000061728394561728,"locked":0,"executed_funds":12.345678912345678901,"time_in_force":null,"trade_fee":0.000061728394561728,"is_maker":true,"identifier":"automation-order-1","smp_type":null,"prevented_volume":0,"prevented_locked":0,"order_timestamp":1781668800000,"trade_timestamp":1781668800001,"timestamp":1781668800002,"stream_type":"REALTIME"}"#
}

fn my_order_trade_frame_for(index: usize) -> String {
    String::from_utf8(my_order_trade_frame().to_vec())
        .unwrap()
        .replace("provider-order", &format!("provider-order-{index}"))
        .replace("provider-trade", &format!("provider-trade-{index}"))
}

fn my_asset_frame() -> &'static [u8] {
    br#"{"type":"myAsset","asset_uuid":"provider-asset","assets":[{"currency":"KRW","balance":1000.123456789123456789,"locked":0.000000000000000001}],"asset_timestamp":1781668800003,"timestamp":1781668800004,"stream_type":"REALTIME"}"#
}

#[test]
fn private_event_parser_keeps_exact_values_and_separate_provider_and_receipt_clocks() {
    let received_at = UtcTimestamp::parse_rfc3339("2026-10-17T01:02:03.123Z").unwrap();
    let PrivateAccountEvent::OrderObserved(order) =
        decode_event(my_order_trade_frame(), 7, received_at).unwrap()
    else {
        panic!("expected a myOrder observation");
    };
    assert_eq!(order.generation, 7);
    assert_eq!(order.received_at_utc, received_at);
    assert_ne!(order.provider_at_utc, order.received_at_utc);
    assert_eq!(
        order.executed_volume,
        Some(Decimal::from_str_exact("0.123456789123456789").unwrap())
    );
    assert_eq!(
        order.identity,
        PrivateOrderEventIdentity::Trade {
            trade_uuid: "provider-trade".to_owned()
        }
    );

    let scientific_frame = String::from_utf8(my_order_trade_frame().to_vec())
        .unwrap()
        .replace("0.123456789123456789", "1e-8");
    let PrivateAccountEvent::OrderObserved(scientific_order) =
        decode_event(scientific_frame.as_bytes(), 7, received_at).unwrap()
    else {
        panic!("expected scientific notation to decode as a myOrder observation");
    };
    assert_eq!(scientific_order.volume, Some(Decimal::new(1, 8)));
    assert_eq!(scientific_order.executed_volume, Some(Decimal::new(1, 8)));

    let string_frame = String::from_utf8(my_order_trade_frame().to_vec())
        .unwrap()
        .replace(
            "\"price\":100.000000000000000001",
            "\"price\":\"100.000000000000000001\"",
        );
    let PrivateAccountEvent::OrderObserved(string_order) =
        decode_event(string_frame.as_bytes(), 7, received_at).unwrap()
    else {
        panic!("expected a decimal string to decode as a myOrder observation");
    };
    assert_eq!(
        string_order.price,
        Some(Decimal::from_str_exact("100.000000000000000001").unwrap())
    );

    let PrivateAccountEvent::AssetObserved(asset) =
        decode_event(my_asset_frame(), 7, received_at).unwrap()
    else {
        panic!("expected a myAsset observation");
    };
    assert_eq!(asset.generation, 7);
    assert_eq!(asset.received_at_utc, received_at);
    assert_ne!(asset.provider_asset_at_utc, asset.received_at_utc);
    assert_eq!(
        asset.assets[0].balance,
        Decimal::from_str_exact("1000.123456789123456789").unwrap()
    );
}

#[test]
fn duplicate_trade_identities_are_suppressed_without_applying_out_of_order_values() {
    let first_received = UtcTimestamp::parse_rfc3339("2026-10-17T01:02:03Z").unwrap();
    let later_received = UtcTimestamp::parse_rfc3339("2026-10-17T01:02:04Z").unwrap();
    let PrivateAccountEvent::OrderObserved(first) =
        decode_event(my_order_trade_frame(), 3, first_received).unwrap()
    else {
        panic!("expected a trade event");
    };
    let PrivateAccountEvent::OrderObserved(duplicate) =
        decode_event(my_order_trade_frame(), 3, later_received).unwrap()
    else {
        panic!("expected a repeated trade event");
    };
    let mut recent = RecentOrderIdentities::new(8);
    assert!(!is_duplicate_order(
        &PrivateAccountEvent::OrderObserved(first.clone()),
        &mut recent
    ));
    assert!(is_duplicate_order(
        &PrivateAccountEvent::OrderObserved(duplicate),
        &mut recent
    ));

    let older_provider_frame = String::from_utf8(my_order_trade_frame().to_vec())
        .unwrap()
        .replace("1781668800001", "1781668700001")
        .replace("1781668800002", "1781668700002")
        .replace("provider-trade", "provider-trade-older");
    let PrivateAccountEvent::OrderObserved(older_provider_event) = decode_event(
        older_provider_frame.as_bytes(),
        3,
        UtcTimestamp::parse_rfc3339("2026-10-17T01:02:05Z").unwrap(),
    )
    .unwrap() else {
        panic!("expected an older provider trade event");
    };
    assert!(older_provider_event.received_at_utc > first.received_at_utc);
    assert!(older_provider_event.provider_at_utc < first.provider_at_utc);
    assert_ne!(older_provider_event.identity, first.identity);
}

#[test]
fn private_readiness_requires_the_current_generation_bootstrap_ack() {
    let readiness = PrivateReadinessFence::new();
    let first = readiness.begin_initial().unwrap();
    assert!(!readiness.is_ready());
    assert!(!readiness.mark_ready(first));
    readiness.mark_recovering(first).unwrap();
    assert!(readiness.mark_ready(first));

    let second = readiness
        .begin_recovery(first, PrivateRealtimeFailure::Connect, false)
        .unwrap();
    assert!(second > first);
    assert!(!readiness.is_ready());
    assert!(!readiness.mark_ready(second));
    readiness.mark_recovering(second).unwrap();
    assert!(!readiness.mark_ready(first));
    assert!(readiness.mark_ready(second));
}

#[test]
fn supervised_failure_closes_the_exact_generation_and_marks_health_error() {
    let readiness = PrivateReadinessFence::new();
    let generation = readiness.begin_initial().unwrap();
    readiness.mark_recovering(generation).unwrap();
    assert!(readiness.mark_ready(generation));
    assert!(readiness.is_ready());

    assert!(readiness.mark_owner_failure(generation, PrivateRealtimeFailure::ConsumerClosed));
    let failed = readiness.snapshot();
    assert_eq!(failed.generation, generation);
    assert_eq!(failed.state, PrivateRealtimeState::Error);
    assert!(!failed.ready_for_decisions);
    assert!(!readiness.is_ready());

    let next_generation = readiness.begin_initial().unwrap();
    assert!(next_generation > generation);
    assert!(!readiness.mark_owner_failure(generation, PrivateRealtimeFailure::InternalState));
    assert_eq!(readiness.snapshot().generation, next_generation);
    assert!(!readiness.is_ready());
}

#[test]
fn client_clones_share_one_readiness_fence() {
    let client = UpbitPrivateClient::new_mock();
    let clone = client.clone();
    let readiness = client.private_readiness_fence();
    let clone_readiness = clone.private_readiness_fence();
    assert!(!clone_readiness.is_ready());
    readiness.mark_test_ready();
    assert!(clone_readiness.is_ready());
}

#[tokio::test]
async fn final_live_post_is_closed_until_the_shared_private_fence_is_ready() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let credentials = UpbitCredentials::generated_for_test().unwrap();
    let client = UpbitPrivateClient::loopback_fixture_with_credentials(
        &format!("http://{address}"),
        LiveExecutionPolicy::OperatorEnabled,
        credentials,
    )
    .unwrap();
    let clone = client.clone();
    let request = AutomationOrderRequest {
        client_identifier: AutomationOrderIdentifier::try_from("private-fence-test".to_owned())
            .unwrap(),
        market: MarketId::parse_upbit("KRW-BTC").unwrap(),
        side: AutomationSide::Bid,
        order: AutomationOrderKind::MarketBuy {
            budget: QuoteAmount::new(Decimal::new(10_000, 0)).unwrap(),
        },
    };
    assert!(matches!(
        client.submit_automation_order(&request).await,
        Err(ClientError::PrivateAccountNotReady)
    ));
    assert!(!clone.private_readiness_fence().is_ready());
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[test]
fn private_baseline_byte_budget_caps_rest_and_buffered_frames_together() {
    let budget = PrivateBaselineByteBudget::new();
    assert_eq!(
        budget.record(MAX_PRIVATE_BASELINE_BYTES - 1).unwrap(),
        MAX_PRIVATE_BASELINE_BYTES - 1
    );
    assert!(matches!(
        budget.record(2),
        Err(ClientError::PrivateBaselineByteLimit {
            observed,
            limit: MAX_PRIVATE_BASELINE_BYTES
        }) if observed == MAX_PRIVATE_BASELINE_BYTES + 1
    ));
}

#[test]
fn private_stream_rejects_unsupported_endpoint_and_malformed_frames() {
    assert!(validate_endpoint("wss://example.com/websocket/v1/private").is_err());
    assert!(decode_event(b"not-json", 1, UtcTimestamp::now()).is_err());
    assert!(decode_event(br#"{"type":"ticker"}"#, 1, UtcTimestamp::now()).is_err());
    assert!(
        decode_event(
            &vec![b' '; MAX_PRIVATE_MESSAGE_BYTES + 1],
            1,
            UtcTimestamp::now()
        )
        .is_err()
    );
}

#[tokio::test]
async fn saturated_private_event_queue_invalidates_and_advances_the_generation() {
    let client = UpbitPrivateClient::new_mock();
    let (producer, mut consumer) = private_account_realtime_channels(&client);
    for _ in 0..PRIVATE_EVENT_CAPACITY {
        producer
            .events
            .try_send(PrivateAccountEvent::BootstrapComplete {
                generation: 0,
                gap_reconciliation_required: false,
                known_order_readbacks_required: true,
                closed_order_history_complete: false,
            })
            .unwrap();
    }
    let generation = producer.readiness.begin_initial().unwrap();
    producer.readiness.mark_recovering(generation).unwrap();
    let event = PrivateAccountEvent::BootstrapComplete {
        generation,
        gap_reconciliation_required: false,
        known_order_readbacks_required: true,
        closed_order_history_complete: false,
    };
    let failure = emit_event(&producer, generation, event).unwrap_err();
    assert_eq!(failure, SessionFailure::OutputBackpressure);

    let mut backoff_attempt = 0;
    let next_generation = recover_from_session(
        &producer.readiness,
        generation,
        failure,
        &mut backoff_attempt,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(next_generation > generation);
    let health = consumer.readiness.snapshot();
    assert!(!health.ready_for_decisions);
    assert_eq!(health.reconnect_count, 1);
    assert_eq!(health.dropped_messages, 1);
    consumer.events.close();
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the local fixture exercises the private bootstrap, generation invalidation, reconnect, and cancellation contract end to end"
)]
async fn local_private_stream_buffers_orders_reconnects_and_does_not_wait_for_my_asset() {
    let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_address = rest_listener.local_addr().unwrap();
    let websocket_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_address = websocket_listener.local_addr().unwrap();
    let credentials = UpbitCredentials::generated_for_test().unwrap();
    let access_key = credentials.access_key().to_owned();
    let secret_key = credentials.secret_key_bytes().to_vec();
    let client = UpbitPrivateClient::loopback_fixture_with_credentials(
        &format!("http://{rest_address}"),
        LiveExecutionPolicy::Disabled,
        credentials,
    )
    .unwrap();
    let client_clone = client.clone();
    let endpoint = format!("ws://{websocket_address}/websocket/v1/private");
    let (producer, mut consumer) = private_account_realtime_channels(&client);
    let cancellation = CancellationToken::new();
    let stream = tokio::spawn(run_private_account_stream_for_test(
        client,
        cancellation.clone(),
        producer,
        endpoint,
        SessionTiming::test(),
    ));

    let (events_sent, events_sent_rx) = oneshot::channel();
    let rest = tokio::spawn(serve_rest_baselines(rest_listener, events_sent_rx));
    let (allow_asset, allow_asset_rx) = oneshot::channel();
    let authenticated_connections = Arc::new(AtomicUsize::new(0));
    let connection_times = Arc::new(std::sync::Mutex::new(Vec::with_capacity(2)));
    let websocket = tokio::spawn(serve_private_socket(
        websocket_listener,
        access_key,
        secret_key,
        authenticated_connections.clone(),
        connection_times.clone(),
        Some(events_sent),
        allow_asset_rx,
    ));

    let PrivateAccountEvent::Baseline {
        baseline,
        history_scope: PrivateOrderHistoryScope::OpenOrdersOnly,
    } = receive_event(&mut consumer.events).await
    else {
        panic!("expected REST baseline first");
    };
    assert_eq!(baseline.generation, 1);
    assert!(baseline.open_orders_complete);
    assert!(!baseline.closed_order_history_complete);
    assert!(!baseline.gap_reconciliation_required);
    assert_eq!(baseline.open_orders.len(), 101);
    assert!(
        baseline
            .open_orders
            .iter()
            .all(|order| order.identifier.is_none())
    );
    assert!(baseline.buffered_event_bytes > 0);
    assert_eq!(
        baseline.aggregate_bytes_used,
        baseline.rest_response_bytes + baseline.buffered_event_bytes
    );
    assert!(baseline.aggregate_bytes_used <= baseline.aggregate_byte_limit);

    for index in 0..8 {
        let PrivateAccountEvent::OrderObserved(order) = receive_event(&mut consumer.events).await
        else {
            panic!("expected buffered myOrder observation");
        };
        assert_eq!(order.generation, baseline.generation);
        assert_eq!(order.stream_sequence, u64::try_from(index + 1).unwrap());
        assert_eq!(
            order.identity,
            PrivateOrderEventIdentity::Trade {
                trade_uuid: format!("provider-trade-{index}")
            }
        );
    }
    let PrivateAccountEvent::BootstrapComplete {
        generation,
        gap_reconciliation_required,
        known_order_readbacks_required,
        closed_order_history_complete,
    } = receive_event(&mut consumer.events).await
    else {
        panic!("expected bootstrap completion marker");
    };
    assert_eq!(generation, baseline.generation);
    assert!(!gap_reconciliation_required);
    assert!(known_order_readbacks_required);
    assert!(!closed_order_history_complete);

    assert!(!consumer.readiness.is_ready());
    assert!(!consumer.health.borrow().asset_stream_seen);
    assert_eq!(consumer.health.borrow().connection_count, 1);
    assert_eq!(consumer.health.borrow().subscription_count, 2);
    assert!(consumer.readiness.mark_ready(generation));
    assert!(client_clone.private_readiness_fence().is_ready());
    allow_asset.send(()).unwrap();

    let PrivateAccountEvent::AssetObserved(asset) = receive_event(&mut consumer.events).await
    else {
        panic!("expected delayed myAsset observation");
    };
    assert_eq!(asset.generation, generation);
    assert_eq!(asset.stream_sequence, 10);
    assert_eq!(
        asset.assets[0].balance,
        Decimal::from_str_exact("1000.123456789123456789").unwrap()
    );
    let previous_reconciled_at = consumer
        .health
        .borrow()
        .last_reconciled_at_utc
        .expect("bootstrap was durably acknowledged before live reconciliation");
    assert!(consumer.readiness.record_reconciled(generation));
    let reconciled_health = consumer.health.borrow().clone();
    assert_eq!(reconciled_health.generation, generation);
    assert_eq!(reconciled_health.state, PrivateRealtimeState::Ready);
    assert!(reconciled_health.ready_for_decisions);
    assert!(
        reconciled_health
            .last_reconciled_at_utc
            .is_some_and(|updated| updated >= previous_reconciled_at)
    );

    let invalidated_generation = consumer
        .readiness
        .invalidate_for_recovery(generation)
        .unwrap();
    assert_eq!(invalidated_generation, generation + 1);
    assert!(!consumer.readiness.is_ready());
    assert!(!consumer.readiness.record_reconciled(generation));
    assert!(
        !client_clone
            .private_readiness_fence()
            .mark_ready(generation)
    );
    assert_eq!(consumer.readiness.generation(), invalidated_generation);

    wait_for_recovery(&mut consumer.health, generation).await;
    assert!(!consumer.readiness.is_ready());
    assert_eq!(consumer.health.borrow().connection_count, 0);
    assert_eq!(consumer.health.borrow().subscription_count, 0);

    let PrivateAccountEvent::Baseline {
        baseline: recovered,
        history_scope: PrivateOrderHistoryScope::OpenOrdersOnly,
    } = receive_event(&mut consumer.events).await
    else {
        panic!("expected reconnect REST baseline");
    };
    assert_eq!(recovered.generation, invalidated_generation);
    assert!(recovered.gap_reconciliation_required);
    let recovering_health = consumer.health.borrow().clone();
    assert!(!recovering_health.order_stream_seen);
    assert!(!recovering_health.asset_stream_seen);
    assert!(recovering_health.last_order_received_at_utc.is_none());
    assert!(recovering_health.last_asset_received_at_utc.is_none());
    assert!(recovering_health.last_received_at_utc.is_none());
    let PrivateAccountEvent::BootstrapComplete {
        generation: recovered_generation,
        gap_reconciliation_required,
        known_order_readbacks_required,
        closed_order_history_complete,
    } = receive_event(&mut consumer.events).await
    else {
        panic!("expected recovered bootstrap marker");
    };
    assert_eq!(recovered_generation, recovered.generation);
    assert!(gap_reconciliation_required);
    assert!(known_order_readbacks_required);
    assert!(!closed_order_history_complete);
    assert!(!consumer.readiness.is_ready());
    assert_eq!(consumer.health.borrow().connection_count, 1);
    assert_eq!(consumer.health.borrow().subscription_count, 2);
    assert!(!consumer.readiness.record_reconciled(recovered_generation));
    assert!(consumer.health.borrow().last_reconciled_at_utc.is_none());
    assert!(!consumer.readiness.is_ready());
    assert!(consumer.readiness.mark_ready(recovered_generation));

    cancellation.cancel();
    timeout(Duration::from_secs(2), stream)
        .await
        .expect("private stream should join after cancellation")
        .unwrap()
        .unwrap();
    assert!(!consumer.readiness.is_ready());
    assert_eq!(
        consumer.health.borrow().state,
        PrivateRealtimeState::Stopped
    );
    assert_eq!(consumer.health.borrow().connection_count, 0);

    let requests = timeout(Duration::from_secs(2), rest)
        .await
        .expect("REST fixture should finish")
        .unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests.iter().all(|(_, authorized)| *authorized));
    assert!(
        requests
            .iter()
            .filter(|(line, _)| line.contains("/v1/orders/open"))
            .count()
            >= 4
    );

    timeout(Duration::from_secs(2), websocket)
        .await
        .expect("WebSocket fixture should observe close")
        .unwrap();
    assert_eq!(authenticated_connections.load(AtomicOrdering::Acquire), 2);
    let connection_times = connection_times.lock().unwrap();
    assert_eq!(connection_times.len(), 2);
    assert!(
        connection_times[1].duration_since(connection_times[0]) >= PRIVATE_MIN_CONNECTION_SPACING
    );
}

#[tokio::test]
#[ignore = "optional read-only Upbit smoke; requires existing configured credentials and network access"]
async fn actual_provider_private_stream_smoke_is_read_only() {
    let credentials = UpbitCredentials::load_from_env_or_file(None)
        .unwrap_or_else(|_| panic!("existing production Upbit credentials are required"));
    let client =
        UpbitPrivateClient::new_live_with_policy(credentials, LiveExecutionPolicy::Disabled)
            .unwrap_or_else(|_| panic!("read-only Upbit client could not be constructed"));
    let (producer, mut consumer) = private_account_realtime_channels(&client);
    let cancellation = CancellationToken::new();
    let stream = tokio::spawn(run_private_account_stream(
        client.clone(),
        cancellation.clone(),
        producer,
    ));

    let observed = timeout(Duration::from_secs(40), async {
        let mut baseline_generation = None;
        let mut baseline_seen = false;
        let mut bootstrap_complete_generation = None;
        loop {
            let health = consumer.health.borrow().clone();
            let same_generation = baseline_generation == Some(health.generation);
            let socket_shape_seen =
                same_generation && health.connection_count == 1 && health.subscription_count == 2;
            let pong_seen = same_generation && health.last_pong_at_utc.is_some();
            if baseline_seen
                && bootstrap_complete_generation == baseline_generation
                && socket_shape_seen
                && pong_seen
            {
                return true;
            }

            tokio::select! {
                event = consumer.events.recv() => match event {
                    Some(PrivateAccountEvent::Baseline { baseline, history_scope }) => {
                        baseline_generation = Some(baseline.generation);
                        baseline_seen = baseline.open_orders_complete
                            && history_scope == PrivateOrderHistoryScope::OpenOrdersOnly;
                    }
                    Some(PrivateAccountEvent::BootstrapComplete { generation, .. }) => {
                        bootstrap_complete_generation = Some(generation);
                    }
                    Some(PrivateAccountEvent::OrderObserved(_)
                        | PrivateAccountEvent::AssetObserved(_)) => {}
                    None => return false,
                },
                changed = consumer.health.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
            }
        }
    })
    .await
    .unwrap_or(false);

    let closed_window_readable = observed && read_recent_closed_window(&client).await;
    cancellation.cancel();
    let joined_cleanly = matches!(stream.await, Ok(Ok(())));
    assert!(
        joined_cleanly,
        "private read-only stream cancellation joins"
    );
    assert!(
        observed,
        "expected REST baseline, BootstrapComplete, two subscriptions, and Pong within 40 seconds"
    );
    assert!(
        closed_window_readable,
        "bounded closed-order window GET must succeed without effects"
    );
    assert!(
        !consumer.readiness.is_ready(),
        "read-only smoke must never open the effect readiness fence"
    );
}

async fn read_recent_closed_window(client: &UpbitPrivateClient) -> bool {
    let end = UtcTimestamp::now();
    let start = UtcTimestamp(end.0 - chrono::Duration::hours(1));
    client
        .get_closed_orders(
            None,
            Some(&start.0.timestamp_millis().to_string()),
            Some(&end.0.timestamp_millis().to_string()),
            Some(1),
        )
        .await
        .is_ok()
}

async fn receive_event(receiver: &mut mpsc::Receiver<PrivateAccountEvent>) -> PrivateAccountEvent {
    timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("private event should arrive")
        .expect("private event channel should remain open")
}

async fn wait_for_recovery(
    health: &mut watch::Receiver<PrivateRealtimeHealth>,
    prior_generation: u64,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = health.borrow().clone();
            if snapshot.generation > prior_generation
                && snapshot.state == PrivateRealtimeState::Recovering
                && !snapshot.ready_for_decisions
            {
                return;
            }
            health.changed().await.unwrap();
        }
    })
    .await
    .expect("reconnect should invalidate the old generation");
}

async fn serve_rest_baselines(
    listener: TcpListener,
    events_sent: oneshot::Receiver<()>,
) -> Vec<(String, bool)> {
    let mut events_sent = Some(events_sent);
    let mut requests = Vec::with_capacity(6);
    for _ in 0..6 {
        let (mut stream, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let request = read_request_headers(&mut stream).await;
        if let Some(events_sent) = events_sent.take() {
            events_sent.await.unwrap();
        }
        let first_line = request.lines().next().unwrap_or_default().to_owned();
        let authorized = request.lines().any(|line| {
            line.to_ascii_lowercase()
                .starts_with("authorization: bearer ")
        });
        let body = if first_line.contains("/v1/accounts") {
            br#"[{"currency":"KRW","balance":"1000.123456789123456789","locked":"0","avg_buy_price":"0","avg_buy_price_modified":false,"unit_currency":"KRW"}]"#.to_vec()
        } else if first_line.contains("/v1/orders/open") {
            let page = first_line
                .split("page=")
                .nth(1)
                .and_then(|tail| tail.split(['&', ' ']).next())
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or_default();
            open_order_page(page)
        } else {
            panic!("unexpected loopback REST path");
        };
        write_json_response(&mut stream, &body).await;
        requests.push((first_line, authorized));
    }
    requests
}

fn open_order_page(page: u32) -> Vec<u8> {
    let count = match page {
        1 => 100,
        2 => 1,
        _ => 0,
    };
    let offset = if page == 2 { 100 } else { 0 };
    let orders = (0..count)
        .map(|index| {
            let order_id = offset + index;
            serde_json::json!({
                "uuid": format!("manual-open-{order_id}"),
                "identifier": null,
                "side": "bid",
                "ord_type": "limit",
                "price": "100.000000000000000001",
                "state": "wait",
                "market": "KRW-BTC",
                "created_at": "2026-10-17T00:00:00Z",
                "volume": "1.0",
                "remaining_volume": "1.0",
                "reserved_fee": "0.05",
                "remaining_fee": "0.05",
                "paid_fee": "0",
                "locked": "100.05",
                "executed_volume": "0",
                "trades_count": 0,
                "trades": []
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&orders).unwrap()
}

async fn read_request_headers(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::with_capacity(2_048);
    loop {
        let mut chunk = [0_u8; 512];
        let count = timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        assert!(bytes.len() <= 16 * 1024);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).unwrap()
}

async fn write_json_response(stream: &mut TcpStream, body: &[u8]) {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.shutdown().await.unwrap();
}

#[expect(
    clippy::result_large_err,
    reason = "tokio-tungstenite requires its concrete HTTP ErrorResponse in this test-only handshake callback"
)]
async fn serve_private_socket(
    listener: TcpListener,
    access_key: String,
    secret_key: Vec<u8>,
    authenticated_connections: Arc<AtomicUsize>,
    connection_times: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
    events_sent: Option<oneshot::Sender<()>>,
    allow_asset: oneshot::Receiver<()>,
) {
    let mut allow_asset = Some(allow_asset);
    let mut events_sent = events_sent;
    for connection_index in 0..2 {
        let (stream, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        connection_times
            .lock()
            .unwrap()
            .push(std::time::Instant::now());
        let expected_access_key = access_key.clone();
        let expected_secret_key = secret_key.clone();
        let authenticated_connections = Arc::clone(&authenticated_connections);
        let handshake = move |request: &ServerRequest,
                              response: ServerResponse|
              -> Result<ServerResponse, ErrorResponse> {
            let auth_ok = request.uri().path() == "/websocket/v1/private"
                && request.uri().query().is_none()
                && request
                    .headers()
                    .get(reqwest::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        valid_jwt(value, &expected_access_key, &expected_secret_key)
                    });
            if auth_ok {
                authenticated_connections.fetch_add(1, AtomicOrdering::AcqRel);
            }
            Ok(response)
        };
        let mut socket = accept_hdr_async(stream, handshake).await.unwrap();

        let subscription = timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(subscription) = subscription else {
            panic!("expected a private subscription request");
        };
        let subscription_text = subscription.as_str();
        assert!(!subscription_text.contains(&access_key));
        assert!(!subscription_text.contains(std::str::from_utf8(&secret_key).unwrap_or("")));
        let request: serde_json::Value = serde_json::from_str(subscription_text).unwrap();
        assert_eq!(request[0]["ticket"].as_str().map(str::len), Some(36));
        assert_eq!(request[1]["type"], "myOrder");
        assert_eq!(request[1]["codes"], serde_json::json!([]));
        assert_eq!(request[2]["type"], "myAsset");
        assert_eq!(request[3]["format"], "DEFAULT");

        if connection_index == 0 {
            for index in 0..8 {
                socket
                    .send(Message::Text(my_order_trade_frame_for(index).into()))
                    .await
                    .unwrap();
            }
            socket.send(Message::Ping(Vec::new().into())).await.unwrap();
            loop {
                let message = timeout(Duration::from_secs(3), socket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                match message {
                    Message::Pong(_) => break,
                    Message::Ping(payload) => {
                        socket.send(Message::Pong(payload)).await.unwrap();
                    }
                    _ => {}
                }
            }
            events_sent.take().unwrap().send(()).unwrap();
            let mut allow_asset = allow_asset.take().unwrap();
            assert!(wait_for_signal(&mut socket, &mut allow_asset).await);
            socket
                .send(Message::Text(my_order_trade_frame_for(7).into()))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    String::from_utf8_lossy(my_asset_frame())
                        .into_owned()
                        .into(),
                ))
                .await
                .unwrap();
            wait_for_peer_close(&mut socket).await;
        } else {
            wait_for_peer_close(&mut socket).await;
        }
    }
}

async fn wait_for_signal(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    signal: &mut oneshot::Receiver<()>,
) -> bool {
    loop {
        tokio::select! {
            result = &mut *signal => return result.is_ok(),
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await.unwrap(),
                Some(Ok(Message::Close(_)) | Err(_)) | None => return false,
                Some(Ok(_)) => {}
            }
        }
    }
}

async fn wait_for_peer_close(socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>) {
    loop {
        match timeout(Duration::from_secs(3), socket.next()).await {
            Ok(Some(Ok(Message::Close(_)) | Err(_)) | None) | Err(_) => return,
            Ok(Some(Ok(Message::Ping(payload)))) => {
                socket.send(Message::Pong(payload)).await.unwrap();
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

fn valid_jwt(authorization: &str, access_key: &str, secret_key: &[u8]) -> bool {
    let Some(token) = authorization.strip_prefix("Bearer ") else {
        return false;
    };
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let signing_input = format!("{header}.{payload}");
    if base64url_encode(&hmac_sha256(secret_key, signing_input.as_bytes())) != signature {
        return false;
    }
    let Some(payload) = decode_base64url(payload) else {
        return false;
    };
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&payload) else {
        return false;
    };
    payload
        .get("access_key")
        .and_then(serde_json::Value::as_str)
        == Some(access_key)
        && payload.get("query_hash").is_none()
}

fn decode_base64url(input: &str) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(u8::try_from(buffer >> bits).ok()?);
            buffer &= (1_u32 << bits) - 1;
        }
    }
    Some(output)
}
