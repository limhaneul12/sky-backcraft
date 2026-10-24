//! Authorized, bounded latest-state SSE. Durable facts use their existing APIs.

use crate::contracts::{RealtimeFeedStatus, RealtimeStatus};
use crate::oauth::{OAuthState, accepts_legacy_bearer};
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Eight UI/MCP observers share a budget; strategy count does not multiply it.
pub const MAX_REALTIME_EVENT_CLIENTS: usize = 8;
const MAX_EVENT_BYTES: usize = 8 * 1024;
const SLOW_CONSUMER_TIMEOUT: Duration = Duration::from_secs(5);
const AUTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
// Keep intermediaries active well before ordinary minute-scale idle deadlines.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
pub enum RealtimeVisibility {
    MarketOnly,
    TradingObservations,
}

/// Admission credentials remain process-local and deliberately have no Debug impl.
#[derive(Clone)]
pub enum RealtimeEventAuthorization {
    PublicMarket,
    Bearer {
        expected: Arc<String>,
        visibility: RealtimeVisibility,
    },
    OAuth(OAuthState),
}

impl RealtimeEventAuthorization {
    fn visibility(&self, headers: &HeaderMap) -> Option<RealtimeVisibility> {
        match self {
            Self::PublicMarket => Some(RealtimeVisibility::MarketOnly),
            Self::Bearer {
                expected,
                visibility,
            } => accepts_legacy_bearer(headers, expected).then_some(*visibility),
            Self::OAuth(state) => state.permissions_for_headers(headers).map(|permissions| {
                if permissions.can_read_trading() {
                    RealtimeVisibility::TradingObservations
                } else {
                    RealtimeVisibility::MarketOnly
                }
            }),
        }
    }
}

/// Owns every bounded subscriber producer through cancellation and joining.
#[derive(Clone, Default)]
pub struct RealtimeEventTasks {
    tasks: Arc<Mutex<JoinSet<()>>>,
    closing: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
}

impl RealtimeEventTasks {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let Ok(mut tasks) = self.tasks.lock() else {
            return false;
        };
        if self.closing.load(Ordering::Acquire) {
            return false;
        }
        while let Some(result) = tasks.try_join_next() {
            if result.is_err() {
                self.failed.store(true, Ordering::Release);
                self.closing.store(true, Ordering::Release);
                return false;
            }
        }
        tasks.spawn(future);
        true
    }

    /// Call after cancelling the shared parent; every producer is joined.
    /// # Errors
    /// Reports a poisoned registry or any subscriber panic/cancellation.
    pub async fn shutdown(&self) -> Result<(), crate::contracts::LabError> {
        self.closing.store(true, Ordering::Release);
        let mut tasks = match self.tasks.lock() {
            Ok(mut tasks) => std::mem::take(&mut *tasks),
            Err(_) => {
                return Err(crate::contracts::LabError::Internal(
                    "realtime task registry poisoned".into(),
                ));
            }
        };
        while let Some(result) = tasks.join_next().await {
            if result.is_err() {
                self.failed.store(true, Ordering::Release);
            }
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(crate::contracts::LabError::Internal(
                "realtime subscriber task failed".into(),
            ));
        }
        Ok(())
    }
}

/// A shared watch coalesces state; each admitted observer has one bounded queue.
#[derive(Clone)]
pub struct RealtimeEvents {
    latest: watch::Receiver<RealtimeStatus>,
    authorization: RealtimeEventAuthorization,
    cancellation: CancellationToken,
    clients: Arc<Semaphore>,
    tasks: RealtimeEventTasks,
}

impl RealtimeEvents {
    #[must_use]
    pub fn new(
        latest: watch::Receiver<RealtimeStatus>,
        authorization: RealtimeEventAuthorization,
        cancellation: CancellationToken,
        clients: Arc<Semaphore>,
        tasks: RealtimeEventTasks,
    ) -> Self {
        Self {
            latest,
            authorization,
            cancellation,
            clients,
            tasks,
        }
    }

    /// The composition boundary applies the same Host/Origin checks as MCP.
    pub fn router(self, path: &str) -> Router {
        Router::new().route(path, get(open_stream)).with_state(self)
    }
}

struct Subscriber {
    latest: watch::Receiver<RealtimeStatus>,
    authorization: RealtimeEventAuthorization,
    headers: HeaderMap,
    cancellation: CancellationToken,
    auth_tick: tokio::time::Interval,
    last_payload: Option<String>,
    closed: bool,
}

async fn open_stream(State(state): State<RealtimeEvents>, mut headers: HeaderMap) -> Response {
    if state.cancellation.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if state.authorization.visibility(&headers).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // This is a snapshot stream, not an event-log replay promise. Reject a replay
    // cursor explicitly rather than silently dropping potentially missed history.
    if headers.contains_key("last-event-id") {
        return (
            StatusCode::CONFLICT,
            "RESYNC_REQUIRED: fresh snapshot stream",
        )
            .into_response();
    }
    if let Some(value) = headers.get_mut(axum::http::header::AUTHORIZATION) {
        value.set_sensitive(true);
    }
    let Ok(permit) = state.clients.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let mut auth_tick = tokio::time::interval(AUTH_CHECK_INTERVAL);
    auth_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let subscriber = Subscriber {
        latest: state.latest,
        authorization: state.authorization,
        headers,
        cancellation: state.cancellation.child_token(),
        auth_tick,
        last_payload: None,
        closed: false,
    };
    let body_stop = subscriber.cancellation.clone();
    let body_auth = subscriber.authorization.clone();
    let body_headers = subscriber.headers.clone();
    let (sender, receiver) = mpsc::channel(1);
    if !state
        .tasks
        .spawn(run_subscriber(subscriber, permit, sender))
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let body = EventBody {
        receiver,
        cancellation: body_stop,
        authorization: body_auth,
        headers: body_headers,
        closed: false,
    };
    let events = stream::unfold(body, |mut body| async move {
        if body.closed {
            return None;
        }
        let item = tokio::select! {
            biased;
            () = body.cancellation.cancelled() => return None,
            item = body.receiver.recv() => item?,
        };
        if body.authorization.visibility(&body.headers).is_none() {
            body.closed = true;
            return Some((
                Ok(Event::default()
                    .event("auth_expired")
                    .data("{\"code\":\"AUTH_EXPIRED\"}")),
                body,
            ));
        }
        Some((item, body))
    });
    let mut response = Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(KEEPALIVE_INTERVAL)
                .text("keepalive"),
        )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-transform"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    crate::transport::mark_bounded_status_stream(&mut response);
    response
}

async fn run_subscriber(
    mut subscriber: Subscriber,
    permit: OwnedSemaphorePermit,
    sender: mpsc::Sender<Result<Event, Infallible>>,
) {
    let _permit = permit;
    while let Some((event, next)) = next_event(subscriber).await {
        subscriber = next;
        let delivery = sender.send(event);
        tokio::pin!(delivery);
        let deadline = tokio::time::sleep(SLOW_CONSUMER_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                () = subscriber.cancellation.cancelled() => return,
                () = &mut deadline => return,
                _ = subscriber.auth_tick.tick() => {
                    if subscriber.authorization.visibility(&subscriber.headers).is_none() {
                        return;
                    }
                },
                sent = &mut delivery => {
                    if sent.is_err() { return; }
                    break;
                }
            }
        }
    }
}

struct EventBody {
    receiver: mpsc::Receiver<Result<Event, Infallible>>,
    cancellation: CancellationToken,
    authorization: RealtimeEventAuthorization,
    headers: HeaderMap,
    closed: bool,
}

impl Drop for EventBody {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn next_event(mut state: Subscriber) -> Option<(Result<Event, Infallible>, Subscriber)> {
    if state.closed {
        return None;
    }
    loop {
        if state.last_payload.is_some() {
            tokio::select! {
                () = state.cancellation.cancelled() => return None,
                changed = state.latest.changed() => {
                    if changed.is_err() { return None; }
                },
                _ = state.auth_tick.tick() => {},
            }
        }
        if state.cancellation.is_cancelled() {
            return None;
        }
        let Some(visibility) = state.authorization.visibility(&state.headers) else {
            state.closed = true;
            return Some((
                Ok(Event::default()
                    .event("auth_expired")
                    .data("{\"code\":\"AUTH_EXPIRED\"}")),
                state,
            ));
        };
        let mut snapshot = state.latest.borrow_and_update().clone();
        if matches!(visibility, RealtimeVisibility::MarketOnly) {
            snapshot.private = RealtimeFeedStatus::unavailable();
        }
        let payload = match serde_json::to_string(&snapshot) {
            Ok(value) if value.len() <= MAX_EVENT_BYTES => value,
            Ok(_) | Err(_) => {
                state.closed = true;
                return Some((
                    Ok(Event::default()
                        .event("error")
                        .data("{\"code\":\"STATUS_UNAVAILABLE\"}")),
                    state,
                ));
            }
        };
        if state.last_payload.as_ref() == Some(&payload) {
            continue;
        }
        let event_kind = if state.last_payload.is_none() {
            "snapshot"
        } else {
            "status"
        };
        let event = Event::default()
            .event(event_kind)
            .retry(Duration::from_secs(3))
            .data(&payload);
        state.last_payload = Some(payload);
        return Some((Ok(event), state));
    }
}

#[cfg(test)]
mod tests;
