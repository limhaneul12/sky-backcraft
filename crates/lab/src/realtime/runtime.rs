use std::future::pending;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use crate::contracts::{LabError, RawWebSocketTransport, RealtimeConnectionState, UtcTimestamp};

use super::parser::decode_candle_frame;
use super::state::FrameDisposition;
use super::subscription::{
    PublicMarketSubscription, websocket_subscription_message, websocket_type_for_interval,
};
use super::{CLOSE_TIMEOUT, FeedShared, PublicFeedErrorCategory, PublicMarketFeedConfig};

type PublicSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

enum ConnectionExit {
    Cancelled,
    SubscriptionChanged,
    Disconnected(ConnectionFailure),
}

#[derive(Debug, Clone, Copy)]
struct ConnectionFailure {
    category: PublicFeedErrorCategory,
    detail: &'static str,
    lab_error_category: Option<&'static str>,
    peer_close_code: Option<u16>,
    http_status: Option<u16>,
}

impl ConnectionFailure {
    const fn new(category: PublicFeedErrorCategory, detail: &'static str) -> Self {
        Self {
            category,
            detail,
            lab_error_category: None,
            peer_close_code: None,
            http_status: None,
        }
    }

    const fn with_lab_error_category(mut self, category: &'static str) -> Self {
        self.lab_error_category = Some(category);
        self
    }

    const fn with_peer_close_code(mut self, code: Option<u16>) -> Self {
        self.peer_close_code = code;
        self
    }

    const fn with_http_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one owner loop preserves retry, union, epoch and shutdown state"
)]
pub(super) async fn run_feed(
    shared: std::sync::Arc<FeedShared>,
    mut subscription_changes: tokio::sync::watch::Receiver<Vec<PublicMarketSubscription>>,
    cancellation: CancellationToken,
) -> Result<(), LabError> {
    let mut subscriptions = subscription_changes.borrow_and_update().clone();
    let mut reconnect_delay = shared.config.reconnect_min;
    let mut connection_epoch = 0_u64;
    let mut prior_connection = false;
    // Retain across subscription changes and all reconnect outcomes.
    let mut last_handshake_attempt = None;

    loop {
        if cancellation.is_cancelled() {
            set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
            return Ok(());
        }
        let supported = subscriptions
            .iter()
            .filter(|subscription| websocket_type_for_interval(subscription.interval).is_some())
            .cloned()
            .collect::<Vec<_>>();
        if supported.is_empty() {
            set_connection_state(&shared, RealtimeConnectionState::Idle, 0, false);
            tokio::select! {
                () = cancellation.cancelled() => {
                    set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                    return Ok(());
                }
                changed = subscription_changes.changed() => {
                    if changed.is_err() {
                        set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                        return Ok(());
                    }
                    subscriptions.clone_from(&subscription_changes.borrow_and_update());
                    continue;
                }
            }
        }

        set_connection_state(&shared, RealtimeConnectionState::Connecting, 0, false);
        if wait_for_handshake_spacing(
            last_handshake_attempt,
            shared.config.handshake_min_spacing,
            &mut subscription_changes,
            &mut subscriptions,
            &cancellation,
        )
        .await?
        {
            set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
            return Ok(());
        }
        let supported = subscriptions
            .iter()
            .filter(|subscription| websocket_type_for_interval(subscription.interval).is_some())
            .cloned()
            .collect::<Vec<_>>();
        if supported.is_empty() {
            continue;
        }
        last_handshake_attempt = Some(Instant::now());
        let connection = tokio::time::timeout(
            shared.config.connect_timeout,
            connect_async_with_config(
                shared.config.endpoint.as_str(),
                Some(websocket_config(&shared.config)),
                false,
            ),
        );
        tokio::pin!(connection);
        let connected = tokio::select! {
            () = cancellation.cancelled() => {
                set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                return Ok(());
            }
            changed = subscription_changes.changed() => {
                if changed.is_err() {
                    set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                    return Ok(());
                }
                subscriptions.clone_from(&subscription_changes.borrow_and_update());
                continue;
            }
            result = &mut connection => result,
        };
        let connection = match connected {
            Ok(Ok(connection)) if connection.1.status().as_u16() == 101 => Ok(connection),
            Ok(Ok((_, response))) => Err(ConnectionFailure::new(
                PublicFeedErrorCategory::Connect,
                "websocket_upgrade_rejected",
            )
            .with_http_status(response.status().as_u16())),
            Ok(Err(_)) => Err(ConnectionFailure::new(
                PublicFeedErrorCategory::Connect,
                "websocket_handshake_failed",
            )),
            Err(_) => Err(ConnectionFailure::new(
                PublicFeedErrorCategory::Connect,
                "websocket_handshake_timeout",
            )),
        };
        let (mut socket, _response) = match connection {
            Ok(connection) => connection,
            Err(failure) => {
                record_connection_failure(&shared, failure, prior_connection, None);
                prior_connection = true;
                if wait_reconnect(
                    &mut subscription_changes,
                    &mut subscriptions,
                    reconnect_delay,
                    &cancellation,
                )
                .await?
                {
                    return Ok(());
                }
                reconnect_delay = next_backoff(reconnect_delay, shared.config.reconnect_max);
                continue;
            }
        };
        connection_epoch = connection_epoch.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("public feed connection epoch exhausted".into())
        })?;
        let connection_started_at = Instant::now();
        let reconnect_count = prior_connection;
        prior_connection = true;
        if reconnect_count {
            increment_reconnect_count(&shared);
        }
        {
            let mut cache = shared
                .cache
                .lock()
                .map_err(|_| LabError::Internal("public feed cache lock poisoned".into()))?;
            cache.clear_reconciled_for(&subscriptions);
        }
        set_connection_state(&shared, RealtimeConnectionState::Recovering, 1, false);
        shared.notify.notify_one();

        let ticket = format!("spot-lab-public-{connection_epoch}");
        let subscription_message = websocket_subscription_message(&supported, &ticket)?;
        if socket
            .send(Message::Text(subscription_message.into()))
            .await
            .is_err()
        {
            let connection_duration = connection_started_at.elapsed();
            record_connection_failure(
                &shared,
                ConnectionFailure::new(
                    PublicFeedErrorCategory::Subscription,
                    "subscription_send_failed",
                ),
                true,
                Some(connection_duration),
            );
            if wait_reconnect(
                &mut subscription_changes,
                &mut subscriptions,
                reconnect_delay,
                &cancellation,
            )
            .await?
            {
                set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                return Ok(());
            }
            reconnect_delay = next_backoff(reconnect_delay, shared.config.reconnect_max);
            continue;
        }

        let exit = run_feed_connection(
            &shared,
            &supported,
            connection_epoch,
            &cancellation,
            &mut subscription_changes,
            &mut subscriptions,
            &mut socket,
        )
        .await?;
        match exit {
            ConnectionExit::Cancelled => {
                set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                return Ok(());
            }
            ConnectionExit::SubscriptionChanged => {
                reconnect_delay = reconnect_delay_after_connection(
                    reconnect_delay,
                    connection_started_at.elapsed(),
                    shared.config.heartbeat_interval,
                    shared.config.reconnect_min,
                );
            }
            ConnectionExit::Disconnected(failure) => {
                let connection_duration = connection_started_at.elapsed();
                reconnect_delay = reconnect_delay_after_connection(
                    reconnect_delay,
                    connection_duration,
                    shared.config.heartbeat_interval,
                    shared.config.reconnect_min,
                );
                set_connection_state(&shared, RealtimeConnectionState::Recovering, 0, false);
                record_connection_failure(&shared, failure, true, Some(connection_duration));
                if wait_reconnect(
                    &mut subscription_changes,
                    &mut subscriptions,
                    reconnect_delay,
                    &cancellation,
                )
                .await?
                {
                    set_connection_state(&shared, RealtimeConnectionState::Stopped, 0, false);
                    return Ok(());
                }
                reconnect_delay = next_backoff(reconnect_delay, shared.config.reconnect_max);
            }
        }
    }
}

pub(super) async fn wait_for_handshake_spacing(
    last_attempt: Option<Instant>,
    minimum_spacing: Duration,
    subscription_changes: &mut tokio::sync::watch::Receiver<Vec<PublicMarketSubscription>>,
    subscriptions: &mut Vec<PublicMarketSubscription>,
    cancellation: &CancellationToken,
) -> Result<bool, LabError> {
    let Some(last_attempt) = last_attempt else {
        subscriptions.clone_from(&subscription_changes.borrow_and_update());
        return Ok(false);
    };
    let deadline = last_attempt + minimum_spacing;
    loop {
        if cancellation.is_cancelled() {
            return Ok(true);
        }
        match subscription_changes.has_changed() {
            Ok(true) => subscriptions.clone_from(&subscription_changes.borrow_and_update()),
            Ok(false) => {}
            Err(_) => return Ok(true),
        }
        tokio::select! {
            () = cancellation.cancelled() => return Ok(true),
            changed = subscription_changes.changed() => {
                if changed.is_err() {
                    return Ok(true);
                }
                subscriptions.clone_from(&subscription_changes.borrow_and_update());
            }
            () = tokio::time::sleep_until(deadline) => {
                subscriptions.clone_from(&subscription_changes.borrow_and_update());
                return Ok(false);
            }
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "single select loop owns socket reads, heartbeat, recovery wake and cancellation"
)]
async fn run_feed_connection(
    shared: &FeedShared,
    subscriptions: &[PublicMarketSubscription],
    connection_epoch: u64,
    cancellation: &CancellationToken,
    subscription_changes: &mut tokio::sync::watch::Receiver<Vec<PublicMarketSubscription>>,
    current_subscriptions: &mut Vec<PublicMarketSubscription>,
    socket: &mut PublicSocket,
) -> Result<ConnectionExit, LabError> {
    let (mut sink, mut stream) = socket.split();
    let mut heartbeat = interval(shared.config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut confirmation_tick = interval(Duration::from_millis(250));
    confirmation_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pending_ping: Option<Instant> = None;
    let mut sequence = 0_u64;

    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                let _ = tokio::time::timeout(CLOSE_TIMEOUT, sink.send(Message::Close(None))).await;
                return Ok(ConnectionExit::Cancelled);
            }
            changed = subscription_changes.changed() => {
                if changed.is_err() {
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, sink.send(Message::Close(None))).await;
                    return Ok(ConnectionExit::Cancelled);
                }
                current_subscriptions.clone_from(&subscription_changes.borrow_and_update());
                let _ = tokio::time::timeout(CLOSE_TIMEOUT, sink.send(Message::Close(None))).await;
                return Ok(ConnectionExit::SubscriptionChanged);
            }
            _ = heartbeat.tick() => {
                if pending_ping.is_some() {
                    continue;
                }
                if sink.send(Message::Ping(Vec::from(&b"spot-lab-heartbeat"[..]).into())).await.is_err() {
                    return Ok(ConnectionExit::Disconnected(ConnectionFailure::new(
                        PublicFeedErrorCategory::Heartbeat,
                        "heartbeat_send_failed",
                    )));
                }
                pending_ping = Some(Instant::now());
            }
            () = wait_for_pong_timeout(pending_ping, shared.config.heartbeat_timeout), if pending_ping.is_some() => {
                return Ok(ConnectionExit::Disconnected(ConnectionFailure::new(
                    PublicFeedErrorCategory::Heartbeat,
                    "heartbeat_timeout",
                )));
            }
            _ = confirmation_tick.tick() => {
                let now = UtcTimestamp::now();
                let due = {
                    let cache = shared
                        .cache
                        .lock()
                        .map_err(|_| LabError::Internal("public feed cache lock poisoned".into()))?;
                    subscriptions.iter().any(|subscription| {
                        cache.has_due_candidate(subscription, now)
                            || cache.silent_rest_probe_due(subscription, now)
                    })
                };
                if due {
                    // Timer wakes REST confirmation/recovery work; it never finalizes a bar.
                    shared.notify.notify_one();
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            return Ok(ConnectionExit::Disconnected(ConnectionFailure::new(
                                PublicFeedErrorCategory::Heartbeat,
                                "peer_ping_response_failed",
                            )));
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        pending_ping = None;
                        let subscriptions = shared.subscriptions.borrow().clone();
                        let all_ready = shared
                            .cache
                            .lock()
                            .map_err(|_| LabError::Internal("public feed cache lock poisoned".into()))?
                            .all_required_ready(&subscriptions, UtcTimestamp::now());
                        shared.health.send_modify(|health| {
                        health.last_pong_at_utc = Some(UtcTimestamp::now());
                        health.status.ready_for_decisions = all_ready
                            && health.status.connection_count == 1;
                        health.status.state = if health.status.ready_for_decisions {
                            RealtimeConnectionState::Ready
                        } else {
                            RealtimeConnectionState::Recovering
                        };
                        });
                    }
                    Some(Ok(Message::Text(text))) => {
                        if let Err(failure) = accept_market_message(
                            shared,
                            text.as_bytes(),
                            subscriptions,
                            connection_epoch,
                            &mut sequence,
                        ) {
                            return Ok(ConnectionExit::Disconnected(failure));
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Err(failure) = accept_market_message(
                            shared,
                            bytes.as_ref(),
                            subscriptions,
                            connection_epoch,
                            &mut sequence,
                        ) {
                            return Ok(ConnectionExit::Disconnected(failure));
                        }
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        let close_code = close_frame.map(|frame| u16::from(frame.code));
                        return Ok(ConnectionExit::Disconnected(
                            ConnectionFailure::new(
                                PublicFeedErrorCategory::Connect,
                                "peer_close_frame",
                            )
                            .with_peer_close_code(close_code),
                        ));
                    }
                    None => {
                        return Ok(ConnectionExit::Disconnected(ConnectionFailure::new(
                            PublicFeedErrorCategory::Connect,
                            "peer_stream_ended",
                        )));
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => {
                        let failure = match error {
                            tokio_tungstenite::tungstenite::Error::Capacity(_) => {
                                ConnectionFailure::new(
                                    PublicFeedErrorCategory::OversizedMessage,
                                    "websocket_read_capacity_exceeded",
                                )
                            }
                            _ => ConnectionFailure::new(
                                PublicFeedErrorCategory::Protocol,
                                "websocket_read_protocol_error",
                            ),
                        };
                        return Ok(ConnectionExit::Disconnected(failure));
                    }
                }
            }
        }
    }
}

fn accept_market_message(
    shared: &FeedShared,
    raw: &[u8],
    subscriptions: &[PublicMarketSubscription],
    connection_epoch: u64,
    sequence: &mut u64,
) -> Result<(), ConnectionFailure> {
    let received_at = UtcTimestamp::now();
    let parsed = decode_candle_frame(raw, received_at, shared.config.max_message_bytes)
        .map_err(|error| parser_failure(&error))?;
    if !subscriptions.contains(&parsed.subscription) {
        return Err(ConnectionFailure::new(
            PublicFeedErrorCategory::Subscription,
            "unrequested_subscription",
        ));
    }
    *sequence = sequence.checked_add(1).ok_or_else(|| {
        ConnectionFailure::new(
            PublicFeedErrorCategory::Protocol,
            "message_sequence_exhausted",
        )
    })?;
    let transport = RawWebSocketTransport {
        connection_epoch,
        message_sequence: *sequence,
        provider_at: Some(parsed.provider_at),
        received_at,
    };
    let frame = parsed.clone();
    let subscriptions_now = shared.subscriptions.borrow().clone();
    let (disposition, all_ready) = {
        let mut cache = shared.cache.lock().map_err(|_| {
            ConnectionFailure::new(
                PublicFeedErrorCategory::Recovery,
                "feed_cache_lock_poisoned",
            )
            .with_lab_error_category("INTERNAL")
        })?;
        let disposition = cache.apply_frame(frame, raw.to_vec(), transport);
        let all_ready = cache.all_required_ready(&subscriptions_now, received_at);
        (disposition, all_ready)
    };
    let lag_ms = received_at
        .0
        .signed_duration_since(parsed.provider_at.0)
        .num_milliseconds();
    shared.health.send_modify(|health| {
        health.status.last_received_at_utc = Some(received_at);
        health.last_provider_at_utc = Some(parsed.provider_at);
        health.last_data_lag_millis = u64::try_from(lag_ms).ok();
        health.status.ready_for_decisions = all_ready
            && health.last_pong_at_utc.is_some()
            && (health.status.connection_count == 1
                || subscriptions_now.iter().all(|subscription| {
                    subscription.interval == crate::contracts::CandleInterval::D1
                }));
        health.status.state = if health.status.ready_for_decisions {
            RealtimeConnectionState::Ready
        } else {
            RealtimeConnectionState::Recovering
        };
        health.last_error_category = None;
        match disposition {
            FrameDisposition::New | FrameDisposition::Updated | FrameDisposition::Duplicate => {}
            FrameDisposition::Gap | FrameDisposition::OutOfOrder => {
                health.status.state = RealtimeConnectionState::Recovering;
                health.status.dropped_messages = health.status.dropped_messages.saturating_add(1);
                health.last_error_category = Some(PublicFeedErrorCategory::Recovery);
            }
        }
    });
    let due = {
        let cache = shared.cache.lock().map_err(|_| {
            ConnectionFailure::new(
                PublicFeedErrorCategory::Recovery,
                "feed_cache_lock_poisoned",
            )
            .with_lab_error_category("INTERNAL")
        })?;
        cache.has_due_candidate(&parsed.subscription, received_at)
    };
    if due
        || matches!(
            disposition,
            FrameDisposition::Gap | FrameDisposition::OutOfOrder
        )
    {
        shared.notify.notify_one();
    }
    Ok(())
}

fn websocket_config(config: &PublicMarketFeedConfig) -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(config.max_message_bytes.min(16 * 1024))
        .write_buffer_size(1024)
        .max_write_buffer_size(64 * 1024)
        .max_message_size(Some(config.max_message_bytes))
        .max_frame_size(Some(config.max_message_bytes))
}

fn parser_failure(error: &LabError) -> ConnectionFailure {
    let (category, detail) = match error {
        LabError::CapacityExceeded(_) => (
            PublicFeedErrorCategory::OversizedMessage,
            "message_capacity_exceeded",
        ),
        LabError::ResourceLimit(_) | LabError::RequestLimit(_) => (
            PublicFeedErrorCategory::OversizedMessage,
            "message_resource_limit",
        ),
        _ => (PublicFeedErrorCategory::Protocol, "invalid_candle_frame"),
    };
    ConnectionFailure::new(category, detail).with_lab_error_category(lab_error_category(error))
}

fn lab_error_category(error: &LabError) -> &'static str {
    match error {
        LabError::InvalidConfig(_) => "INVALID_CONFIG",
        LabError::InsufficientWarmup(_) => "INSUFFICIENT_WARMUP",
        LabError::DataGap(_) => "DATA_GAP",
        LabError::InputHashMismatch(_) => "INPUT_HASH_MISMATCH",
        LabError::BlockedEvidence(_) => "BLOCKED_EVIDENCE",
        LabError::NetworkUnavailable(_) => "NETWORK_UNAVAILABLE",
        LabError::RateLimited(_) => "RATE_LIMITED",
        LabError::TemporarilyBlocked(_) => "TEMPORARILY_BLOCKED",
        LabError::CapacityExceeded(_) => "CAPACITY_EXCEEDED",
        LabError::Conflict(_) => "CONFLICT",
        LabError::DataCorrupt(_) => "DATA_CORRUPT",
        LabError::ResourceLimit(_) => "RESOURCE_LIMIT",
        LabError::StoragePressure(_) => "STORAGE_PRESSURE",
        LabError::RequestLimit(_) => "REQUEST_LIMIT",
        LabError::Cancelled(_) => "CANCELLED",
        LabError::UnverifiedMarketRules(_) => "UNVERIFIED_MARKET_RULES",
        LabError::AccountingInvariant(_) => "ACCOUNTING_INVARIANT_FAILURE",
        LabError::OutcomeUnknown(_) => "OUTCOME_UNKNOWN",
        LabError::ContractParse(_) => "CONTRACT_PARSE",
        LabError::Internal(_) => "INTERNAL",
    }
}

fn reconnect_delay_after_connection(
    current: Duration,
    connected_for: Duration,
    stable_connection: Duration,
    minimum: Duration,
) -> Duration {
    if connected_for >= stable_connection {
        minimum
    } else {
        current
    }
}

async fn wait_for_pong_timeout(pending_ping: Option<Instant>, timeout: Duration) {
    if let Some(sent_at) = pending_ping {
        tokio::time::sleep_until(sent_at + timeout).await;
    } else {
        pending::<()>().await;
    }
}

async fn wait_reconnect(
    subscription_changes: &mut tokio::sync::watch::Receiver<Vec<PublicMarketSubscription>>,
    subscriptions: &mut Vec<PublicMarketSubscription>,
    delay: Duration,
    cancellation: &CancellationToken,
) -> Result<bool, LabError> {
    tokio::select! {
        () = cancellation.cancelled() => Ok(true),
        changed = subscription_changes.changed() => {
            if changed.is_err() {
                return Ok(true);
            }
            subscriptions.clone_from(&subscription_changes.borrow_and_update());
            Ok(false)
        }
        () = tokio::time::sleep(delay) => Ok(false),
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

fn set_connection_state(
    shared: &FeedShared,
    state: RealtimeConnectionState,
    connections: u32,
    ready: bool,
) {
    shared.health.send_modify(|health| {
        health.status.state = state;
        health.status.connection_count = connections;
        health.status.ready_for_decisions = ready;
        if state == RealtimeConnectionState::Connecting
            || (state == RealtimeConnectionState::Recovering && connections == 1)
        {
            health.last_pong_at_utc = None;
        }
        if !ready {
            health.status.ready_for_decisions = false;
        }
    });
}

fn record_connection_failure(
    shared: &FeedShared,
    failure: ConnectionFailure,
    recovering: bool,
    connection_duration: Option<Duration>,
) {
    shared.health.send_modify(|health| {
        health.status.state = if recovering {
            RealtimeConnectionState::Recovering
        } else {
            RealtimeConnectionState::Error
        };
        health.status.connection_count = 0;
        health.status.ready_for_decisions = false;
        if matches!(
            failure.category,
            PublicFeedErrorCategory::Heartbeat
                | PublicFeedErrorCategory::Protocol
                | PublicFeedErrorCategory::OversizedMessage
                | PublicFeedErrorCategory::Subscription
                | PublicFeedErrorCategory::Recovery
        ) {
            health.status.dropped_messages = health.status.dropped_messages.saturating_add(1);
        }
        health.last_error_category = Some(failure.category);
    });
    shared.notify.notify_one();
    let connection_duration_ms =
        connection_duration.and_then(|duration| u64::try_from(duration.as_millis()).ok());
    tracing::warn!(
        error_category = ?failure.category,
        failure_detail = failure.detail,
        lab_error_category = ?failure.lab_error_category,
        peer_close_code = ?failure.peer_close_code,
        http_status = ?failure.http_status,
        connection_duration_ms = ?connection_duration_ms,
        "public Upbit WebSocket connection ended"
    );
}

fn increment_reconnect_count(shared: &FeedShared) {
    shared.health.send_modify(|health| {
        health.status.reconnect_count = health.status.reconnect_count.saturating_add(1);
    });
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;
