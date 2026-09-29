//! Bounded public HTTP transport admission and lifecycle controls.
//!
//! This module owns connection/request resource limits only. Durable job
//! admission, idempotency and shutdown ordering remain with their application
//! owners.

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use std::{
    collections::VecDeque,
    future::Future as _,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

pub const MAX_OPEN_CONNECTIONS: usize = 16;
pub const MAX_INFLIGHT_REQUESTS: usize = 8;
pub const MAX_REQUESTS_PER_SECOND: u64 = 20;
pub const MAX_REQUEST_BODY_BYTES: usize = 262_144;
pub const MAX_RESPONSE_BYTES: usize = 524_288;
pub const CONNECTION_IDLE: Duration = Duration::from_secs(30);
pub const ABSOLUTE_CONNECTION_LIFETIME: Duration = Duration::from_secs(120);
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(20);
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

const REQUEST_ID_HEADER: header::HeaderName = header::HeaderName::from_static("x-request-id");
const ROUTE_TEMPLATE: &str = "/mcp";
const LOG_EVENTS_PER_SECOND: u64 = 20;

/// Fixed public transport ceilings. Public requests cannot raise these limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_open_connections: usize,
    pub max_inflight_requests: usize,
    pub max_requests_per_second: u64,
    pub max_request_body_bytes: usize,
    pub max_response_bytes: usize,
    pub connection_idle: Duration,
    pub absolute_connection_lifetime: Duration,
    pub request_deadline: Duration,
    pub shutdown_deadline: Duration,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_open_connections: MAX_OPEN_CONNECTIONS,
            max_inflight_requests: MAX_INFLIGHT_REQUESTS,
            max_requests_per_second: MAX_REQUESTS_PER_SECOND,
            max_request_body_bytes: MAX_REQUEST_BODY_BYTES,
            max_response_bytes: MAX_RESPONSE_BYTES,
            connection_idle: CONNECTION_IDLE,
            absolute_connection_lifetime: ABSOLUTE_CONNECTION_LIFETIME,
            request_deadline: REQUEST_DEADLINE,
            shutdown_deadline: SHUTDOWN_DEADLINE,
        }
    }
}

/// Low-cardinality native counters for capacity and shutdown evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSnapshot {
    pub active_connections: u64,
    pub active_requests: u64,
    pub accepted_requests: u64,
    pub rejected_connections: u64,
    pub rejected_requests: u64,
    pub timed_out_connections: u64,
    pub timed_out_requests: u64,
    pub response_limit_rejections: u64,
    pub admission_stopped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    Drained,
    TimedOut(TransportSnapshot),
}

#[derive(Debug)]
struct WindowCounter {
    events: VecDeque<Instant>,
}

impl WindowCounter {
    fn new() -> Self {
        Self {
            events: VecDeque::new(),
        }
    }

    fn try_take(&mut self, now: Instant, limit: u64) -> bool {
        while self
            .events
            .front()
            .is_some_and(|started_at| now.duration_since(*started_at) >= Duration::from_secs(1))
        {
            self.events.pop_front();
        }
        if u64::try_from(self.events.len()).unwrap_or(u64::MAX) >= limit {
            return false;
        }
        self.events.push_back(now);
        true
    }
}

/// Shared process-wide transport admission state.
pub struct TransportState {
    limits: TransportLimits,
    connection_slots: Arc<Semaphore>,
    request_slots: Arc<Semaphore>,
    request_rate: Mutex<WindowCounter>,
    log_budget: Mutex<WindowCounter>,
    connection_shutdown: CancellationToken,
    admission_stopped: AtomicBool,
    next_request_id: AtomicU64,
    active_connections: AtomicU64,
    active_requests: AtomicU64,
    accepted_requests: AtomicU64,
    rejected_connections: AtomicU64,
    rejected_requests: AtomicU64,
    timed_out_connections: AtomicU64,
    timed_out_requests: AtomicU64,
    response_limit_rejections: AtomicU64,
    drain_notify: Notify,
}

impl TransportState {
    #[must_use]
    fn new(limits: TransportLimits) -> Self {
        Self {
            limits,
            connection_slots: Arc::new(Semaphore::new(limits.max_open_connections)),
            request_slots: Arc::new(Semaphore::new(limits.max_inflight_requests)),
            request_rate: Mutex::new(WindowCounter::new()),
            log_budget: Mutex::new(WindowCounter::new()),
            connection_shutdown: CancellationToken::new(),
            admission_stopped: AtomicBool::new(false),
            next_request_id: AtomicU64::new(1),
            active_connections: AtomicU64::new(0),
            active_requests: AtomicU64::new(0),
            accepted_requests: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
            timed_out_connections: AtomicU64::new(0),
            timed_out_requests: AtomicU64::new(0),
            response_limit_rejections: AtomicU64::new(0),
            drain_notify: Notify::new(),
        }
    }

    #[must_use]
    pub fn limits(&self) -> TransportLimits {
        self.limits
    }

    #[must_use]
    pub fn snapshot(&self) -> TransportSnapshot {
        TransportSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            active_requests: self.active_requests.load(Ordering::Relaxed),
            accepted_requests: self.accepted_requests.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            rejected_requests: self.rejected_requests.load(Ordering::Relaxed),
            timed_out_connections: self.timed_out_connections.load(Ordering::Relaxed),
            timed_out_requests: self.timed_out_requests.load(Ordering::Relaxed),
            response_limit_rejections: self.response_limit_rejections.load(Ordering::Relaxed),
            admission_stopped: self.admission_stopped.load(Ordering::Acquire),
        }
    }

    pub fn stop_admission(&self) {
        self.admission_stopped.store(true, Ordering::Release);
        self.connection_shutdown.cancel();
        let snapshot = self.snapshot();
        tracing::info!(
            target: "spot_lab::transport",
            event = "stop_admission",
            active_connections = snapshot.active_connections,
            active_requests = snapshot.active_requests,
        );
    }

    #[must_use]
    pub fn admission_stopped(&self) -> bool {
        self.admission_stopped.load(Ordering::Acquire)
    }

    pub async fn wait_for_drain(&self, deadline: Duration) -> DrainOutcome {
        let expires_at = Instant::now() + deadline;
        loop {
            let snapshot = self.snapshot();
            if snapshot.active_connections == 0 && snapshot.active_requests == 0 {
                return DrainOutcome::Drained;
            }

            let notified = self.drain_notify.notified();
            let snapshot = self.snapshot();
            if snapshot.active_connections == 0 && snapshot.active_requests == 0 {
                return DrainOutcome::Drained;
            }
            if tokio::time::timeout_at(expires_at, notified).await.is_err() {
                return DrainOutcome::TimedOut(self.snapshot());
            }
        }
    }

    fn next_correlation_id(&self) -> RequestCorrelationId {
        let sequence = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        RequestCorrelationId(Arc::from(format!("req-{sequence:016x}")))
    }

    fn try_admit_request(self: &Arc<Self>) -> Result<RequestPermit, AdmissionRejection> {
        if self.admission_stopped() {
            self.reject_request("admission_stopped");
            return Err(AdmissionRejection::Stopped);
        }
        if !lock_unpoisoned(&self.request_rate)
            .try_take(Instant::now(), self.limits.max_requests_per_second)
        {
            self.reject_request("rate_limit");
            return Err(AdmissionRejection::Rate);
        }
        let permit = self
            .request_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                self.reject_request("inflight_limit");
                AdmissionRejection::Inflight
            })?;
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        self.accepted_requests.fetch_add(1, Ordering::Relaxed);
        Ok(RequestPermit {
            state: Arc::clone(self),
            _permit: permit,
        })
    }

    fn reject_connection(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
        self.emit_bounded_rejection("connection_limit");
    }

    fn connection_opened(&self) {
        if lock_unpoisoned(&self.log_budget).try_take(Instant::now(), LOG_EVENTS_PER_SECOND) {
            tracing::info!(
                target: "spot_lab::transport",
                event = "connection_open",
                active_connections = self.active_connections.load(Ordering::Relaxed),
            );
        }
    }

    fn connection_closed(&self, close_category: &'static str) {
        if lock_unpoisoned(&self.log_budget).try_take(Instant::now(), LOG_EVENTS_PER_SECOND) {
            tracing::info!(
                target: "spot_lab::transport",
                event = "connection_close",
                close_category,
                active_connections = self.active_connections.load(Ordering::Relaxed),
            );
        }
    }

    fn reject_request(&self, reason: &'static str) {
        self.rejected_requests.fetch_add(1, Ordering::Relaxed);
        self.emit_bounded_rejection(reason);
    }

    fn emit_bounded_rejection(&self, reason: &'static str) {
        if lock_unpoisoned(&self.log_budget).try_take(Instant::now(), LOG_EVENTS_PER_SECOND) {
            let snapshot = self.snapshot();
            tracing::warn!(
                target: "spot_lab::transport",
                event = "transport_rejected",
                rejection_reason = reason,
                active_connections = snapshot.active_connections,
                active_requests = snapshot.active_requests,
            );
        }
    }

    fn connection_timed_out(&self, reason: &'static str) {
        self.timed_out_connections.fetch_add(1, Ordering::Relaxed);
        if lock_unpoisoned(&self.log_budget).try_take(Instant::now(), LOG_EVENTS_PER_SECOND) {
            tracing::warn!(
                target: "spot_lab::transport",
                event = "connection_timeout",
                error_category = reason,
            );
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct RequestPermit {
    state: Arc<TransportState>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.state.active_requests.fetch_sub(1, Ordering::Relaxed);
        self.state.drain_notify.notify_waiters();
    }
}

/// Process-local diagnostic correlation only; this is never authentication.
#[derive(Debug, Clone)]
pub struct RequestCorrelationId(pub Arc<str>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionRejection {
    Stopped,
    Rate,
    Inflight,
}

impl AdmissionRejection {
    fn status(self) -> StatusCode {
        match self {
            Self::Stopped => StatusCode::SERVICE_UNAVAILABLE,
            Self::Rate | Self::Inflight => StatusCode::TOO_MANY_REQUESTS,
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::Stopped => "admission_stopped",
            Self::Rate => "rate_limit",
            Self::Inflight => "inflight_limit",
        }
    }
}

/// Apply bounded admission, response-size and deadline policy to any Axum router.
pub fn apply_limits<S>(router: Router<S>, state: Arc<TransportState>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(middleware::from_fn_with_state(state, request_limits))
}

async fn request_limits(
    State(state): State<Arc<TransportState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let correlation_id = state.next_correlation_id();
    request.extensions_mut().insert(correlation_id.clone());
    let route = ROUTE_TEMPLATE;
    let method = method_category(request.method());
    let started_at = Instant::now();

    let permit = match state.try_admit_request() {
        Ok(permit) => permit,
        Err(rejection) => {
            let mut response = sanitized_response(
                rejection.status(),
                "RESOURCE_LIMIT",
                "request admission rejected",
                &correlation_id,
            );
            add_request_id_header(&mut response, &correlation_id);
            record_request(
                &state,
                RequestObservation {
                    event: "request_rejected",
                    correlation_id: &correlation_id,
                    route,
                    method,
                    status: response.status(),
                    elapsed: started_at.elapsed(),
                    error_category: rejection.reason(),
                },
            );
            return response;
        }
    };

    let executed = execute_bounded_response(request, next, &state);
    let (event, error_category, mut response) =
        match tokio::time::timeout(state.limits.request_deadline, executed).await {
            Ok(Ok(response)) => {
                let error_category = response_error_category(response.status());
                ("request_complete", error_category, response)
            }
            Ok(Err(ResponseFailure::UnavailableOrTooLarge)) => {
                state
                    .response_limit_rejections
                    .fetch_add(1, Ordering::Relaxed);
                (
                    "request_rejected",
                    "response_limit",
                    sanitized_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "RESOURCE_LIMIT",
                        "response exceeded configured limit",
                        &correlation_id,
                    ),
                )
            }
            Err(_) => {
                state.timed_out_requests.fetch_add(1, Ordering::Relaxed);
                (
                    "request_timeout",
                    "deadline",
                    sanitized_response(
                        StatusCode::GATEWAY_TIMEOUT,
                        "TIMEOUT",
                        "request deadline exceeded; reconcile mutations by request_id",
                        &correlation_id,
                    ),
                )
            }
        };
    drop(permit);
    add_request_id_header(&mut response, &correlation_id);
    record_request(
        &state,
        RequestObservation {
            event,
            correlation_id: &correlation_id,
            route,
            method,
            status: response.status(),
            elapsed: started_at.elapsed(),
            error_category,
        },
    );
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseFailure {
    UnavailableOrTooLarge,
}

async fn execute_bounded_response(
    request: Request,
    next: Next,
    state: &TransportState,
) -> Result<Response, ResponseFailure> {
    collect_bounded_response(next.run(request).await, state.limits.max_response_bytes).await
}

async fn collect_bounded_response(
    response: Response,
    max_response_bytes: usize,
) -> Result<Response, ResponseFailure> {
    let (parts, body) = response.into_parts();
    match to_bytes(body, max_response_bytes).await {
        Ok(bytes) => Ok(Response::from_parts(parts, Body::from(bytes))),
        Err(_) => Err(ResponseFailure::UnavailableOrTooLarge),
    }
}

fn sanitized_response(
    status: StatusCode,
    category: &'static str,
    message: &'static str,
    correlation_id: &RequestCorrelationId,
) -> Response {
    let value = serde_json::json!({
        "error": {
            "category": category,
            "message": message,
            "request_id": correlation_id.0.as_ref(),
        }
    });
    let body = match serde_json::to_vec(&value) {
        Ok(body) => body,
        Err(_) => {
            b"{\"error\":{\"category\":\"INTERNAL\",\"message\":\"response serialization failed\"}}"
                .to_vec()
        }
    };
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn add_request_id_header(response: &mut Response, correlation_id: &RequestCorrelationId) {
    if let Ok(value) = HeaderValue::from_str(&correlation_id.0) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
}

fn method_category(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

fn response_error_category(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        4 => "client_error",
        5 => "server_error",
        _ => "none",
    }
}

#[derive(Clone, Copy)]
struct RequestObservation<'observation> {
    event: &'static str,
    correlation_id: &'observation RequestCorrelationId,
    route: &'static str,
    method: &'static str,
    status: StatusCode,
    elapsed: Duration,
    error_category: &'static str,
}

fn record_request(state: &TransportState, observation: RequestObservation<'_>) {
    let snapshot = state.snapshot();
    let elapsed_ms = u64::try_from(observation.elapsed.as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        target: "spot_lab::transport",
        event = observation.event,
        request_id = %observation.correlation_id.0,
        route = observation.route,
        method = observation.method,
        status_class = status_class(observation.status),
        elapsed_ms,
        error_category = observation.error_category,
        active_connections = snapshot.active_connections,
        active_requests = snapshot.active_requests,
    );
}

/// Axum listener that owns process-wide connection admission.
pub struct BoundedListener {
    listener: TcpListener,
    state: Arc<TransportState>,
}

impl BoundedListener {
    #[must_use]
    pub fn new(listener: TcpListener, limits: TransportLimits) -> Self {
        Self {
            listener,
            state: Arc::new(TransportState::new(limits)),
        }
    }

    #[must_use]
    pub fn state(&self) -> Arc<TransportState> {
        Arc::clone(&self.state)
    }
}

impl axum::serve::Listener for BoundedListener {
    type Io = BoundedIo<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let accepted = self.listener.accept().await;
            let (stream, address) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::error!(
                        target: "spot_lab::transport",
                        event = "listener_accept_error",
                        error_category = ?error.kind(),
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            if self.state.admission_stopped() {
                self.state.reject_connection();
                drop(stream);
                continue;
            }
            let Ok(permit) = self.state.connection_slots.clone().try_acquire_owned() else {
                self.state.reject_connection();
                drop(stream);
                continue;
            };
            self.state
                .active_connections
                .fetch_add(1, Ordering::Relaxed);
            self.state.connection_opened();
            return (BoundedIo::new(stream, &self.state, Some(permit)), address);
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

pub struct BoundedIo<I> {
    inner: I,
    state: Arc<TransportState>,
    permit: Option<OwnedSemaphorePermit>,
    idle: Pin<Box<Sleep>>,
    absolute: Pin<Box<Sleep>>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
    close_category: &'static str,
}

impl<I> BoundedIo<I> {
    fn new(inner: I, state: &Arc<TransportState>, permit: Option<OwnedSemaphorePermit>) -> Self {
        let now = Instant::now();
        Self {
            inner,
            state: Arc::clone(state),
            permit,
            idle: Box::pin(tokio::time::sleep_until(now + state.limits.connection_idle)),
            absolute: Box::pin(tokio::time::sleep_until(
                now + state.limits.absolute_connection_lifetime,
            )),
            shutdown: Box::pin(state.connection_shutdown.clone().cancelled_owned()),
            close_category: "closed",
        }
    }

    fn poll_termination(&mut self, context: &mut Context<'_>) -> Option<&'static str> {
        let category = if self.shutdown.as_mut().poll(context).is_ready() {
            Some("shutdown")
        } else if self.absolute.as_mut().poll(context).is_ready() {
            Some("absolute_lifetime")
        } else if self.idle.as_mut().poll(context).is_ready() {
            Some("idle")
        } else {
            None
        };
        if let Some(category) = category
            && self.close_category == "closed"
        {
            self.close_category = category;
            if category != "shutdown" {
                self.state.connection_timed_out(category);
            }
        }
        category
    }

    fn reset_idle(&mut self) {
        self.idle
            .as_mut()
            .reset(Instant::now() + self.state.limits.connection_idle);
    }
}

impl<I> Drop for BoundedIo<I> {
    fn drop(&mut self) {
        if self.permit.is_some() {
            self.state
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            self.state.connection_closed(self.close_category);
            self.state.drain_notify.notify_waiters();
        }
    }
}

impl<I> AsyncRead for BoundedIo<I>
where
    I: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(category) = this.poll_termination(context) {
            return Poll::Ready(Err(termination_error(category)));
        }
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > filled_before {
            this.reset_idle();
        }
        result
    }
}

impl<I> AsyncWrite for BoundedIo<I>
where
    I: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        if let Some(category) = this.poll_termination(context) {
            return Poll::Ready(Err(termination_error(category)));
        }
        let result = Pin::new(&mut this.inner).poll_write(context, buffer);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            this.reset_idle();
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        if let Some(category) = this.poll_termination(context) {
            return Poll::Ready(Err(termination_error(category)));
        }
        Pin::new(&mut this.inner).poll_flush(context)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(context)
    }
}

fn termination_error(category: &'static str) -> io::Error {
    let kind = if category == "shutdown" {
        io::ErrorKind::Interrupted
    } else {
        io::ErrorKind::TimedOut
    };
    io::Error::new(kind, category)
}

#[cfg(test)]
mod tests;
