use super::*;
use crate::contracts::{RealtimeConnectionState, UtcTimestamp};
use std::sync::atomic::{AtomicU64, Ordering};

fn snapshot(subscriptions: u32) -> RealtimeStatus {
    let mut market = RealtimeFeedStatus::unavailable();
    market.state = RealtimeConnectionState::Ready;
    market.connection_count = 1;
    market.subscription_count = subscriptions;
    market.ready_for_decisions = true;
    RealtimeStatus {
        observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-17T00:00:00Z")
            .expect("fixed projection instant"),
        market: market.clone(),
        private: market,
    }
}

fn ephemeral_secret() -> Arc<String> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    Arc::new(format!(
        "isolated-sse-fixture-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn nine_recovered_subscriptions_fit_the_public_event_budget() {
    let mut status = snapshot(9);
    let timestamp = serde_json::json!("2026-10-19T08:45:26.124976Z");
    for market in ["KRW-BTC", "KRW-XRP", "KRW-ETH"] {
        for interval in ["m5", "h1", "h4"] {
            let spans = if interval == "h4" {
                serde_json::json!([])
            } else {
                serde_json::json!([{
                    "proof_id": "a".repeat(64),
                    "start_utc": "2026-10-18T17:05:00Z",
                    "end_utc": "2026-10-18T22:00:00Z"
                }])
            };
            status.market.subscription_readiness.push(
                serde_json::from_value(serde_json::json!({
                    "market": market, "interval": interval, "ready": true,
                    "pending_rest_confirmation": true, "revision_snapshot_count": 2,
                    "last_revision_open_time_utc": timestamp,
                    "last_revision_detected_at_utc": timestamp,
                    "last_received_at_utc": timestamp, "provider_at_utc": timestamp,
                    "latest_websocket_open_time_utc": timestamp,
                    "latest_rest_close_time_utc": timestamp,
                    "last_rest_fetched_at_utc": timestamp,
                    "confirmed_provider_absence_spans": spans,
                    "provider_recovery_progress": {
                        "next_page_to_utc": timestamp, "pages_scanned": 64,
                        "rows_scanned": 12800, "page_limit": 64, "row_limit": 12800,
                        "pending_absence_confirmation": true
                    }
                }))
                .expect("typed compact readiness fixture"),
            );
        }
    }
    let (_sender, receiver) = watch::channel(status);
    let state = Subscriber {
        latest: receiver,
        authorization: RealtimeEventAuthorization::PublicMarket,
        headers: HeaderMap::new(),
        cancellation: CancellationToken::new(),
        auth_tick: tokio::time::interval(AUTH_CHECK_INTERVAL),
        last_payload: None,
        closed: false,
    };
    let (_, state) = next_event(state).await.expect("initial recovered snapshot");
    assert!(!state.closed, "recovery evidence must not close the stream");
    let payload = state.last_payload.expect("snapshot fits the event budget");
    assert!(payload.len() <= MAX_EVENT_BYTES);
    let decoded: RealtimeStatus =
        serde_json::from_str(&payload).expect("same typed status contract");
    assert_eq!(decoded.private.state, RealtimeConnectionState::Unavailable);
    assert_eq!(decoded.market.subscription_readiness.len(), 9);
    assert_eq!(
        decoded
            .market
            .subscription_readiness
            .iter()
            .map(|key| key.confirmed_provider_absence_spans.len())
            .sum::<usize>(),
        6
    );
    assert!(
        decoded
            .market
            .subscription_readiness
            .iter()
            .all(|key| key.provider_recovery_progress.is_some())
    );
    assert!(!payload.contains("accepted_anchor"));
    assert!(!payload.contains("snapshots"));
}

#[tokio::test]
async fn latest_state_coalesces_without_exposing_private_health() {
    let (sender, receiver) = watch::channel(snapshot(1));
    let budget = Arc::new(Semaphore::new(1));
    let state = Subscriber {
        latest: receiver,
        authorization: RealtimeEventAuthorization::PublicMarket,
        headers: HeaderMap::new(),
        cancellation: CancellationToken::new(),
        auth_tick: tokio::time::interval(AUTH_CHECK_INTERVAL),
        last_payload: None,
        closed: false,
    };
    let (_, state) = next_event(state).await.expect("initial snapshot");
    let first: serde_json::Value = serde_json::from_str(
        state
            .last_payload
            .as_ref()
            .expect("emitted snapshot payload"),
    )
    .expect("typed JSON projection");
    assert_eq!(first["private"]["state"], "UNAVAILABLE");
    for count in 2..=100 {
        sender.send_replace(snapshot(count));
    }
    let (_, state) = next_event(state).await.expect("latest coalesced status");
    let latest: serde_json::Value =
        serde_json::from_str(state.last_payload.as_ref().expect("emitted latest payload"))
            .expect("typed JSON projection");
    assert_eq!(latest["market"]["subscription_count"], 100);
    assert_eq!(latest["private"]["connection_count"], 0);
    state.cancellation.cancel();
    assert!(next_event(state).await.is_none());
    assert_eq!(budget.available_permits(), 1);
}

#[tokio::test]
async fn actual_sse_checks_auth_capacity_framing_and_shutdown() {
    let (sender, receiver) = watch::channel(snapshot(1));
    let stop = CancellationToken::new();
    let secret = ephemeral_secret();
    let budget = Arc::new(Semaphore::new(1));
    let tasks = RealtimeEventTasks::default();
    let app = RealtimeEvents::new(
        receiver,
        RealtimeEventAuthorization::Bearer {
            expected: secret.clone(),
            visibility: RealtimeVisibility::TradingObservations,
        },
        stop.child_token(),
        budget.clone(),
        tasks.clone(),
    )
    .router("/events");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated SSE fixture");
    let endpoint = format!(
        "http://{}/events",
        listener.local_addr().expect("bound address")
    );
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let client = reqwest::Client::new();
    let unauthorized = client
        .get(&endpoint)
        .send()
        .await
        .expect("read denied admission");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let replay = client
        .get(&endpoint)
        .bearer_auth(secret.as_str())
        .header("last-event-id", "expired-cursor")
        .send()
        .await
        .expect("read explicit snapshot-only reset requirement");
    assert_eq!(replay.status(), StatusCode::CONFLICT);
    let mut response = client
        .get(&endpoint)
        .bearer_auth(secret.as_str())
        .send()
        .await
        .expect("open authenticated SSE");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    let first = tokio::time::timeout(Duration::from_secs(2), response.chunk())
        .await
        .expect("snapshot deadline")
        .expect("read snapshot bytes")
        .expect("initial event bytes");
    let first = std::str::from_utf8(&first).expect("UTF-8 event framing");
    assert!(first.contains("event: snapshot"));
    assert!(first.contains("\"private\":{\"state\":\"READY\""));
    assert!(!first.contains(secret.as_str()));
    let saturated = client
        .get(&endpoint)
        .bearer_auth(secret.as_str())
        .send()
        .await
        .expect("read bounded client admission");
    assert_eq!(saturated.status(), StatusCode::TOO_MANY_REQUESTS);
    sender.send_replace(snapshot(2));
    let update = tokio::time::timeout(Duration::from_secs(2), response.chunk())
        .await
        .expect("status deadline")
        .expect("read changed status")
        .expect("changed event bytes");
    assert!(
        std::str::from_utf8(&update)
            .expect("UTF-8 update")
            .contains("event: status")
    );
    stop.cancel();
    drop(response);
    drop(saturated);
    drop(replay);
    drop(unauthorized);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("joined SSE shutdown deadline")
        .expect("server task joined")
        .expect("server shutdown succeeded");
    tasks.shutdown().await.expect("subscriber tasks joined");
    assert_eq!(budget.available_permits(), 1);
}

#[tokio::test]
async fn stalled_unpolled_body_releases_admission_without_a_client_pull() {
    let (sender, receiver) = watch::channel(snapshot(1));
    let stop = CancellationToken::new();
    let budget = Arc::new(Semaphore::new(1));
    let tasks = RealtimeEventTasks::default();
    let state = RealtimeEvents::new(
        receiver,
        RealtimeEventAuthorization::PublicMarket,
        stop.clone(),
        budget.clone(),
        tasks.clone(),
    );
    let response = open_stream(State(state), HeaderMap::new()).await;
    assert_eq!(response.status(), StatusCode::OK);
    // Deliberately never poll the HTTP body. One queued snapshot fills its queue.
    tokio::task::yield_now().await;
    sender.send_replace(snapshot(2));
    tokio::time::timeout(SLOW_CONSUMER_TIMEOUT + Duration::from_secs(2), async {
        while budget.available_permits() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("independent producer releases stalled admission");
    stop.cancel();
    tasks.shutdown().await.expect("subscriber tasks joined");
    drop(response);
}

#[tokio::test]
async fn realtime_subscriber_panic_is_reported_on_join() {
    let tasks = RealtimeEventTasks::default();
    assert!(tasks.spawn(async {
        panic!("isolated subscriber failure fixture");
    }));
    assert!(matches!(
        tasks.shutdown().await,
        Err(crate::contracts::LabError::Internal(_))
    ));
}
