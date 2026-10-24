//! Authenticated, bounded Upbit private account/order stream.
//!
//! REST snapshots and exact order readbacks remain authoritative. WebSocket
//! observations only trigger Rust-side reconciliation and update private
//! projections; the stream never applies fills or balances to financial state.

use crate::contracts::{MarketId, UtcTimestamp};
use crate::upbit::auth::{
    UPBIT_PRIVATE_WEBSOCKET_ENDPOINT, generate_nonce, verify_private_websocket_endpoint,
};
use crate::upbit::client::{
    ClientError, MAX_PRIVATE_BASELINE_BYTES, PrivateBaselineByteBudget, UpbitAccountBalance,
    UpbitOrderDetail, UpbitPrivateClient,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashSet, VecDeque};
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Interval, interval_at, sleep, sleep_until, timeout, timeout_at};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

// A queue holds at most four bounded snapshots (4 MiB each) plus their parsed
// representation; saturation closes readiness and forces a fresh REST baseline.
const PRIVATE_EVENT_CAPACITY: usize = 4;
const MAX_BOOTSTRAP_EVENTS: usize = 256;
const MAX_PRIVATE_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_SUBSCRIPTION_BYTES: usize = 1_024;
const MAX_ASSETS_PER_EVENT: usize = 512;
const MAX_RECENT_ORDER_IDENTITIES: usize = 1_024;
const PRIVATE_BASELINE_TIMEOUT: Duration = Duration::from_secs(120);
const PRIVATE_BOOTSTRAP_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const PRIVATE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PRIVATE_MIN_CONNECTION_SPACING: Duration = Duration::from_millis(500);
const PRIVATE_PING_INTERVAL: Duration = Duration::from_secs(30);
const PRIVATE_PONG_TIMEOUT: Duration = Duration::from_secs(15);
const PRIVATE_MIN_BACKOFF: Duration = Duration::from_millis(500);
const PRIVATE_MAX_BACKOFF: Duration = Duration::from_secs(30);
const GATE_PHASE_MASK: u64 = 0b11;
const GATE_GENERATION_SHIFT: u32 = 2;
const GATE_IDLE: u64 = 0;
const GATE_CONNECTING: u64 = 1;
const GATE_RECOVERING: u64 = 2;
const GATE_READY: u64 = 3;

type PrivateWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// State suitable for a bounded status projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrivateRealtimeState {
    Idle,
    Connecting,
    Recovering,
    Ready,
    Stopped,
    Error,
}

/// Safe failure category; provider text and credentials are never retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrivateRealtimeFailure {
    Connect,
    Authentication,
    Subscription,
    Protocol,
    Baseline,
    BaselineByteLimit,
    BaselineRecordLimit,
    BootstrapByteLimit,
    Reconciliation,
    OutputBackpressure,
    PongTimeout,
    ConsumerClosed,
    InternalState,
}

/// Immutable transport health view. Event receive clocks are local; provider
/// event clocks live on each typed observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateRealtimeHealth {
    pub(crate) state: PrivateRealtimeState,
    pub(crate) generation: u64,
    pub(crate) connection_count: u32,
    pub(crate) subscription_count: u32,
    pub(crate) reconnect_count: u64,
    pub(crate) dropped_messages: u64,
    pub(crate) ready_for_decisions: bool,
    pub(crate) last_received_at_utc: Option<UtcTimestamp>,
    pub(crate) last_order_received_at_utc: Option<UtcTimestamp>,
    pub(crate) last_asset_received_at_utc: Option<UtcTimestamp>,
    pub(crate) last_reconciled_at_utc: Option<UtcTimestamp>,
    pub(crate) last_pong_at_utc: Option<UtcTimestamp>,
    pub(crate) order_stream_seen: bool,
    pub(crate) asset_stream_seen: bool,
    pub(crate) last_error: Option<PrivateRealtimeFailure>,
}

impl Default for PrivateRealtimeHealth {
    fn default() -> Self {
        Self {
            state: PrivateRealtimeState::Idle,
            generation: 0,
            connection_count: 0,
            subscription_count: 0,
            reconnect_count: 0,
            dropped_messages: 0,
            ready_for_decisions: false,
            last_received_at_utc: None,
            last_order_received_at_utc: None,
            last_asset_received_at_utc: None,
            last_reconciled_at_utc: None,
            last_pong_at_utc: None,
            order_stream_seen: false,
            asset_stream_seen: false,
            last_error: None,
        }
    }
}

struct PrivateReadinessShared {
    gate: AtomicU64,
    stream_owner: AtomicBool,
    generation_sender: watch::Sender<u64>,
    health: Mutex<PrivateRealtimeHealth>,
    health_sender: watch::Sender<PrivateRealtimeHealth>,
}

/// Synchronous generation fence shared by every clone of one private client.
/// The stream invalidates it before reconnect/resync; Main opens it only after
/// durable baseline publication and exact readbacks for buffered orders.
#[derive(Clone)]
pub(crate) struct PrivateReadinessFence {
    shared: Arc<PrivateReadinessShared>,
}

impl PrivateReadinessFence {
    pub(crate) fn new() -> Self {
        let initial = PrivateRealtimeHealth::default();
        let (generation_sender, _) = watch::channel(0);
        let (health_sender, _) = watch::channel(initial.clone());
        Self {
            shared: Arc::new(PrivateReadinessShared {
                gate: AtomicU64::new(GATE_IDLE),
                stream_owner: AtomicBool::new(false),
                generation_sender,
                health: Mutex::new(initial),
                health_sender,
            }),
        }
    }

    /// Fast fail-closed check used immediately before the final REST POST.
    #[must_use]
    pub(crate) fn is_ready(&self) -> bool {
        !self.shared.health.is_poisoned()
            && self.shared.gate.load(Ordering::Acquire) & GATE_PHASE_MASK == GATE_READY
    }

    /// Snapshot the exact generation that is currently admitted for effects.
    #[must_use]
    pub(crate) fn ready_generation(&self) -> Option<u64> {
        if self.shared.health.is_poisoned() {
            return None;
        }
        let gate = self.shared.gate.load(Ordering::Acquire);
        (gate & GATE_PHASE_MASK == GATE_READY).then_some(gate >> GATE_GENERATION_SHIFT)
    }

    /// Require that a previously captured generation is still the admitted one.
    #[must_use]
    pub(crate) fn is_ready_generation(&self, generation: u64) -> bool {
        !self.shared.health.is_poisoned()
            && self.shared.gate.load(Ordering::Acquire) == encode_gate(generation, GATE_READY)
    }

    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.shared.gate.load(Ordering::Acquire) >> GATE_GENERATION_SHIFT
    }

    /// Read the latest health snapshot without exposing mutable shared state.
    #[must_use]
    pub(crate) fn snapshot(&self) -> PrivateRealtimeHealth {
        match self.shared.health.lock() {
            Ok(health) => health.clone(),
            Err(_) => PrivateRealtimeHealth {
                state: PrivateRealtimeState::Error,
                last_error: Some(PrivateRealtimeFailure::InternalState),
                ..PrivateRealtimeHealth::default()
            },
        }
    }

    #[must_use]
    pub(crate) fn subscribe_health(&self) -> watch::Receiver<PrivateRealtimeHealth> {
        self.shared.health_sender.subscribe()
    }

    #[must_use]
    pub(crate) fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.shared.generation_sender.subscribe()
    }

    fn begin_initial(&self) -> Result<u64, PrivateRealtimeError> {
        self.advance_generation(None, PrivateRealtimeState::Connecting, None, false, false)
    }

    fn try_claim_stream(&self) -> bool {
        self.shared
            .stream_owner
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn mark_connecting(&self, generation: u64) -> Result<(), PrivateRealtimeError> {
        self.set_phase(
            generation,
            GATE_CONNECTING,
            PrivateRealtimeState::Connecting,
            None,
        )
    }

    fn mark_connected(&self, generation: u64) -> Result<(), PrivateRealtimeError> {
        self.update_health(|health| {
            if health.generation == generation {
                health.connection_count = 1;
                health.subscription_count = 0;
            }
        })
    }

    fn mark_subscribed(&self, generation: u64) -> Result<(), PrivateRealtimeError> {
        self.update_health(|health| {
            if health.generation == generation {
                health.connection_count = 1;
                health.subscription_count = 2;
                health.state = PrivateRealtimeState::Recovering;
                health.ready_for_decisions = false;
            }
        })
    }

    /// Keep effects closed while Main handles this generation's bootstrap.
    pub(crate) fn mark_recovering(&self, generation: u64) -> Result<(), PrivateRealtimeError> {
        self.set_phase(
            generation,
            GATE_RECOVERING,
            PrivateRealtimeState::Recovering,
            None,
        )
    }

    fn begin_recovery(
        &self,
        generation: u64,
        failure: PrivateRealtimeFailure,
        dropped_message: bool,
    ) -> Result<u64, PrivateRealtimeError> {
        self.advance_generation(
            Some(generation),
            PrivateRealtimeState::Recovering,
            Some(failure),
            true,
            dropped_message,
        )
    }

    /// Close the current fence after Main cannot complete REST/durable reconciliation.
    /// The producer observes the new generation and reconnects with a fresh baseline.
    pub(crate) fn invalidate_for_recovery(
        &self,
        generation: u64,
    ) -> Result<u64, PrivateRealtimeError> {
        self.begin_recovery(generation, PrivateRealtimeFailure::Reconciliation, false)
    }

    /// Admit decisions only for the exact generation whose recovery Main handled.
    pub(crate) fn mark_ready(&self, generation: u64) -> bool {
        if self.shared.health.is_poisoned() {
            return false;
        }
        let Ok(mut health) = self.shared.health.lock() else {
            return false;
        };
        let expected = encode_gate(generation, GATE_RECOVERING);
        if health.generation != generation {
            return false;
        }
        health.state = PrivateRealtimeState::Ready;
        health.ready_for_decisions = true;
        health.last_reconciled_at_utc = Some(UtcTimestamp::now());
        health.last_error = None;
        health.connection_count = 1;
        health.subscription_count = 2;
        if self
            .shared
            .gate
            .compare_exchange(
                expected,
                encode_gate(generation, GATE_READY),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            health.ready_for_decisions = false;
            let gate = self.shared.gate.load(Ordering::Acquire);
            health.generation = gate >> GATE_GENERATION_SHIFT;
            health.state = if gate & GATE_PHASE_MASK == GATE_CONNECTING {
                PrivateRealtimeState::Connecting
            } else {
                PrivateRealtimeState::Recovering
            };
            let snapshot = health.clone();
            drop(health);
            self.shared.health_sender.send_replace(snapshot);
            return false;
        }
        let snapshot = health.clone();
        drop(health);
        self.shared.health_sender.send_replace(snapshot);
        true
    }

    /// Record that Main completed durable/readback reconciliation for one
    /// observation while the same generation remains admitted.
    /// This only refreshes the reconciliation clock; it cannot change the
    /// readiness phase or reopen a recovering/stale generation.
    pub(crate) fn record_reconciled(&self, generation: u64) -> bool {
        if self.shared.health.is_poisoned() {
            return false;
        }
        let Ok(mut health) = self.shared.health.lock() else {
            return false;
        };
        let expected = encode_gate(generation, GATE_READY);
        if health.generation != generation || self.shared.gate.load(Ordering::Acquire) != expected {
            return false;
        }

        let previous = health.last_reconciled_at_utc;
        health.last_reconciled_at_utc = Some(UtcTimestamp::now());
        if self.shared.gate.load(Ordering::Acquire) != expected {
            health.last_reconciled_at_utc = previous;
            return false;
        }
        let snapshot = health.clone();
        drop(health);
        self.shared.health_sender.send_replace(snapshot);
        true
    }

    fn mark_stopped(&self, generation: u64) {
        let _ = self.set_phase(generation, GATE_IDLE, PrivateRealtimeState::Stopped, None);
    }

    /// Close the exact failed generation after its supervised owner exits.
    /// Stale owners cannot overwrite a newer generation or reopen admission.
    pub(crate) fn mark_owner_failure(
        &self,
        generation: u64,
        failure: PrivateRealtimeFailure,
    ) -> bool {
        self.mark_error(generation, failure)
    }

    fn mark_error(&self, generation: u64, failure: PrivateRealtimeFailure) -> bool {
        self.set_phase(
            generation,
            GATE_IDLE,
            PrivateRealtimeState::Error,
            Some(failure),
        )
        .is_ok()
    }

    fn record_message(&self, event: &PrivateAccountEvent) -> Result<(), PrivateRealtimeError> {
        let (generation, received_at) = match event {
            PrivateAccountEvent::OrderObserved(value) => (value.generation, value.received_at_utc),
            PrivateAccountEvent::AssetObserved(value) => (value.generation, value.received_at_utc),
            _ => return Ok(()),
        };
        if self.generation() != generation {
            return Err(PrivateRealtimeError::StaleGeneration);
        }
        let mut health = self
            .shared
            .health
            .lock()
            .map_err(|_| PrivateRealtimeError::InternalState)?;
        if health.generation != generation || self.generation() != generation {
            return Err(PrivateRealtimeError::StaleGeneration);
        }
        health.last_received_at_utc = Some(received_at);
        match event {
            PrivateAccountEvent::OrderObserved(value) => {
                health.order_stream_seen = true;
                health.last_order_received_at_utc = Some(value.received_at_utc);
            }
            PrivateAccountEvent::AssetObserved(value) => {
                health.asset_stream_seen = true;
                health.last_asset_received_at_utc = Some(value.received_at_utc);
            }
            _ => {}
        }
        let snapshot = health.clone();
        drop(health);
        self.shared.health_sender.send_replace(snapshot);
        Ok(())
    }

    fn record_pong(&self) -> Result<(), PrivateRealtimeError> {
        self.update_health(|health| health.last_pong_at_utc = Some(UtcTimestamp::now()))
    }

    #[cfg(test)]
    pub(crate) fn mark_test_ready(&self) {
        let Ok(generation) = self.begin_initial() else {
            return;
        };
        let _ = self.mark_recovering(generation);
        let _ = self.mark_ready(generation);
    }

    fn set_phase(
        &self,
        generation: u64,
        phase: u64,
        state: PrivateRealtimeState,
        error: Option<PrivateRealtimeFailure>,
    ) -> Result<(), PrivateRealtimeError> {
        if self.shared.health.is_poisoned() {
            return Err(PrivateRealtimeError::InternalState);
        }
        loop {
            let current = self.shared.gate.load(Ordering::Acquire);
            if current >> GATE_GENERATION_SHIFT != generation {
                return Err(PrivateRealtimeError::StaleGeneration);
            }
            if self
                .shared
                .gate
                .compare_exchange(
                    current,
                    encode_gate(generation, phase),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
        self.update_health(|health| {
            if health.generation == generation {
                health.state = state;
                health.ready_for_decisions = phase == GATE_READY;
                if matches!(
                    state,
                    PrivateRealtimeState::Connecting
                        | PrivateRealtimeState::Stopped
                        | PrivateRealtimeState::Error
                ) {
                    health.connection_count = 0;
                    health.subscription_count = 0;
                }
                if let Some(error) = error {
                    health.last_error = Some(error);
                }
            }
        })
    }

    fn advance_generation(
        &self,
        expected_generation: Option<u64>,
        state: PrivateRealtimeState,
        error: Option<PrivateRealtimeFailure>,
        reconnect: bool,
        dropped_message: bool,
    ) -> Result<u64, PrivateRealtimeError> {
        if self.shared.health.is_poisoned() {
            return Err(PrivateRealtimeError::InternalState);
        }
        let next_generation = loop {
            let current = self.shared.gate.load(Ordering::Acquire);
            let current_generation = current >> GATE_GENERATION_SHIFT;
            if expected_generation.is_some_and(|expected| expected != current_generation) {
                return Err(PrivateRealtimeError::StaleGeneration);
            }
            let next = current_generation
                .checked_add(1)
                .filter(|value| *value <= (u64::MAX >> GATE_GENERATION_SHIFT))
                .ok_or(PrivateRealtimeError::GenerationExhausted)?;
            let next_phase = match state {
                PrivateRealtimeState::Connecting | PrivateRealtimeState::Recovering => {
                    GATE_CONNECTING
                }
                PrivateRealtimeState::Idle
                | PrivateRealtimeState::Ready
                | PrivateRealtimeState::Stopped
                | PrivateRealtimeState::Error => GATE_IDLE,
            };
            if self
                .shared
                .gate
                .compare_exchange(
                    current,
                    encode_gate(next, next_phase),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                break next;
            }
        };
        self.update_health(|health| {
            health.generation = next_generation;
            health.state = state;
            health.connection_count = 0;
            health.subscription_count = 0;
            health.ready_for_decisions = false;
            health.last_received_at_utc = None;
            health.last_order_received_at_utc = None;
            health.last_asset_received_at_utc = None;
            health.last_reconciled_at_utc = None;
            health.last_pong_at_utc = None;
            health.order_stream_seen = false;
            health.asset_stream_seen = false;
            health.last_error = error;
            if reconnect {
                health.reconnect_count = health.reconnect_count.saturating_add(1);
            }
            if dropped_message {
                health.dropped_messages = health.dropped_messages.saturating_add(1);
            }
        })?;
        self.shared.generation_sender.send_replace(next_generation);
        Ok(next_generation)
    }

    fn update_health(
        &self,
        update: impl FnOnce(&mut PrivateRealtimeHealth),
    ) -> Result<(), PrivateRealtimeError> {
        let mut health = self
            .shared
            .health
            .lock()
            .map_err(|_| PrivateRealtimeError::InternalState)?;
        update(&mut health);
        let snapshot = health.clone();
        drop(health);
        self.shared.health_sender.send_replace(snapshot);
        Ok(())
    }
}

fn encode_gate(generation: u64, phase: u64) -> u64 {
    (generation << GATE_GENERATION_SHIFT) | phase
}

/// Provider identity for one order/trade observation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PrivateOrderEventIdentity {
    Trade {
        trade_uuid: String,
    },
    OrderState {
        order_uuid: String,
        state: PrivateOrderState,
        provider_timestamp_ms: u64,
        event_payload_hash: String,
    },
}

/// Normalized side from a private provider order event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrivateOrderSide {
    Bid,
    Ask,
}

/// Provider order states accepted by the current Upbit private stream contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrivateOrderState {
    Wait,
    Watch,
    Trade,
    Done,
    Cancel,
    Prevented,
}

/// Typed provider order observation. It is never a fill/accounting fact: Main
/// exact-reads known identifiers before durable reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateOrderObservation {
    pub(crate) generation: u64,
    pub(crate) stream_sequence: u64,
    pub(crate) identity: PrivateOrderEventIdentity,
    pub(crate) order_uuid: String,
    pub(crate) client_identifier: Option<String>,
    pub(crate) market: MarketId,
    pub(crate) side: PrivateOrderSide,
    pub(crate) order_type: String,
    pub(crate) state: PrivateOrderState,
    pub(crate) price: Option<Decimal>,
    pub(crate) average_price: Option<Decimal>,
    pub(crate) volume: Option<Decimal>,
    pub(crate) remaining_volume: Option<Decimal>,
    pub(crate) executed_volume: Option<Decimal>,
    pub(crate) trades_count: Option<u64>,
    pub(crate) reserved_fee: Option<Decimal>,
    pub(crate) remaining_fee: Option<Decimal>,
    pub(crate) paid_fee: Option<Decimal>,
    pub(crate) locked: Option<Decimal>,
    pub(crate) executed_funds: Option<Decimal>,
    pub(crate) trade_fee: Option<Decimal>,
    pub(crate) is_maker: Option<bool>,
    pub(crate) smp_type: Option<String>,
    pub(crate) prevented_volume: Option<Decimal>,
    pub(crate) prevented_locked: Option<Decimal>,
    pub(crate) provider_order_at_utc: UtcTimestamp,
    pub(crate) provider_trade_at_utc: Option<UtcTimestamp>,
    pub(crate) provider_at_utc: UtcTimestamp,
    pub(crate) received_at_utc: UtcTimestamp,
}

/// Exact-decimal balance observation carried by a myAsset update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateAssetBalance {
    pub(crate) currency: String,
    pub(crate) balance: Decimal,
    pub(crate) locked: Decimal,
}

/// A myAsset event is a change signal; REST refresh supplies the authoritative balance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateAssetObservation {
    pub(crate) generation: u64,
    pub(crate) stream_sequence: u64,
    pub(crate) asset_uuid: String,
    pub(crate) event_payload_hash: String,
    pub(crate) assets: Vec<PrivateAssetBalance>,
    pub(crate) provider_asset_at_utc: UtcTimestamp,
    pub(crate) provider_at_utc: UtcTimestamp,
    pub(crate) received_at_utc: UtcTimestamp,
}

/// Full bounded REST baseline sent before buffered private stream observations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateAccountBaseline {
    pub(crate) generation: u64,
    pub(crate) observed_at_utc: UtcTimestamp,
    pub(crate) rest_response_bytes: usize,
    pub(crate) buffered_event_bytes: usize,
    pub(crate) aggregate_bytes_used: usize,
    pub(crate) aggregate_byte_limit: usize,
    pub(crate) open_orders_complete: bool,
    pub(crate) closed_order_history_complete: bool,
    pub(crate) gap_reconciliation_required: bool,
    pub(crate) balances: Vec<UpbitAccountBalance>,
    pub(crate) open_orders: Vec<UpbitOrderDetail>,
}

/// The current REST snapshot proves the active open-order set only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrivateOrderHistoryScope {
    OpenOrdersOnly,
}

/// Immutable event ordering contract for the single configured live account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PrivateAccountEvent {
    Baseline {
        baseline: PrivateAccountBaseline,
        history_scope: PrivateOrderHistoryScope,
    },
    OrderObserved(Box<PrivateOrderObservation>),
    AssetObserved(PrivateAssetObservation),
    BootstrapComplete {
        generation: u64,
        gap_reconciliation_required: bool,
        known_order_readbacks_required: bool,
        closed_order_history_complete: bool,
    },
}

/// Caller-owned producer half. Main retains the async task and joins it through
/// `JobRuntime`; this type owns only bounded channels and the shared fence.
pub(crate) struct PrivateRealtimeProducer {
    events: mpsc::Sender<PrivateAccountEvent>,
    readiness: PrivateReadinessFence,
}

/// Read-only projections consumed by Main and the durable owner.
pub(crate) struct PrivateRealtimeConsumer {
    pub(crate) events: mpsc::Receiver<PrivateAccountEvent>,
    pub(crate) health: watch::Receiver<PrivateRealtimeHealth>,
    pub(crate) readiness: PrivateReadinessFence,
}

/// Create bounded event and latest-health channels for one Upbit client/account.
pub(crate) fn private_account_realtime_channels(
    client: &UpbitPrivateClient,
) -> (PrivateRealtimeProducer, PrivateRealtimeConsumer) {
    let (event_sender, events) = mpsc::channel(PRIVATE_EVENT_CAPACITY);
    let readiness = client.private_readiness_fence();
    let health = readiness.subscribe_health();
    (
        PrivateRealtimeProducer {
            events: event_sender,
            readiness: readiness.clone(),
        },
        PrivateRealtimeConsumer {
            events,
            health,
            readiness,
        },
    )
}

/// Safe private stream failures. Provider response text, request headers and
/// credentials are intentionally excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PrivateRealtimeError {
    #[error("private stream requires a configured LIVE account")]
    RequiresLiveAccount,
    #[error("private stream endpoint is outside the fixed allowlist")]
    EndpointRejected,
    #[error("private WebSocket authentication was rejected")]
    Authentication,
    #[error("private stream consumer has closed")]
    ConsumerClosed,
    #[error("a private stream already owns this account client")]
    AlreadyRunning,
    #[error("private readiness generation is stale")]
    StaleGeneration,
    #[error("private readiness generation is exhausted")]
    GenerationExhausted,
    #[error("private realtime shared state is unavailable")]
    InternalState,
}

struct PrivateStreamLease {
    readiness: PrivateReadinessFence,
}

impl Drop for PrivateStreamLease {
    fn drop(&mut self) {
        let health = self.readiness.snapshot();
        if health.state != PrivateRealtimeState::Error {
            self.readiness.mark_stopped(health.generation);
        }
        self.readiness
            .shared
            .stream_owner
            .store(false, Ordering::Release);
    }
}

/// Run one account-scoped stream until cancellation. Connection, bootstrap,
/// heartbeat and reconnect work remain inside this caller-owned task.
///
/// # Errors
/// Returns only for a fatal authentication/state error or a closed consumer;
/// transient transport and REST failures are retried with bounded backoff.
pub(crate) async fn run_private_account_stream(
    client: UpbitPrivateClient,
    cancellation: CancellationToken,
    producer: PrivateRealtimeProducer,
) -> Result<(), PrivateRealtimeError> {
    run_private_account_stream_at(
        client,
        cancellation,
        producer,
        UPBIT_PRIVATE_WEBSOCKET_ENDPOINT,
        SessionTiming::production(),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn run_private_account_stream_for_test(
    client: UpbitPrivateClient,
    cancellation: CancellationToken,
    producer: PrivateRealtimeProducer,
    endpoint: String,
    timing: SessionTiming,
) -> Result<(), PrivateRealtimeError> {
    run_private_account_stream_at(client, cancellation, producer, &endpoint, timing).await
}

#[expect(
    clippy::too_many_lines,
    reason = "one reconnect loop owns the private generation fence, handshake, bootstrap, and task cancellation"
)]
async fn run_private_account_stream_at(
    client: UpbitPrivateClient,
    cancellation: CancellationToken,
    producer: PrivateRealtimeProducer,
    endpoint: &str,
    timing: SessionTiming,
) -> Result<(), PrivateRealtimeError> {
    let readiness = producer.readiness.clone();
    if !client.is_live() {
        readiness.mark_error(0, PrivateRealtimeFailure::Authentication);
        return Err(PrivateRealtimeError::RequiresLiveAccount);
    }
    if !readiness.try_claim_stream() {
        return Err(PrivateRealtimeError::AlreadyRunning);
    }
    let _stream_lease = PrivateStreamLease {
        readiness: readiness.clone(),
    };
    if let Err(error) = validate_endpoint(endpoint) {
        readiness.mark_error(
            readiness.snapshot().generation,
            PrivateRealtimeFailure::Subscription,
        );
        return Err(error);
    }
    let mut generation = readiness.begin_initial()?;
    let mut backoff_attempt = 0u32;
    let mut next_connection_attempt_at = Instant::now();

    loop {
        if cancellation.is_cancelled() {
            readiness.mark_stopped(generation);
            return Ok(());
        }
        readiness.mark_connecting(generation)?;
        let request = match private_request(&client, endpoint) {
            Ok(request) => request,
            Err(PrivateRealtimeError::Authentication) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::Authentication);
                return Err(PrivateRealtimeError::Authentication);
            }
            Err(error) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::Subscription);
                return Err(error);
            }
        };
        if wait_until_or_cancel(&cancellation, next_connection_attempt_at).await {
            readiness.mark_stopped(generation);
            return Ok(());
        }
        next_connection_attempt_at = Instant::now() + PRIVATE_MIN_CONNECTION_SPACING;
        let connect_result = tokio::select! {
            () = cancellation.cancelled() => {
                readiness.mark_stopped(generation);
                return Ok(());
            }
            result = timeout(
                timing.connect_timeout,
                connect_async_with_config(request, Some(websocket_config()), false),
            ) => result,
        };
        let mut socket = match connect_result {
            Ok(Ok((socket, _response))) => socket,
            Ok(Err(error)) if is_authentication_failure(&error) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::Authentication);
                return Err(PrivateRealtimeError::Authentication);
            }
            Ok(Err(_)) | Err(_) => {
                generation =
                    readiness.begin_recovery(generation, PrivateRealtimeFailure::Connect, false)?;
                backoff_attempt = backoff_attempt.saturating_add(1);
                if wait_backoff(&cancellation, backoff_attempt).await {
                    readiness.mark_stopped(generation);
                    return Ok(());
                }
                continue;
            }
        };
        let connected_at = Instant::now();
        readiness.mark_connected(generation)?;
        let mut stream_sequence = 0u64;
        let mut recent_order_ids = RecentOrderIdentities::new(MAX_RECENT_ORDER_IDENTITIES);
        let subscription = tokio::select! {
            () = cancellation.cancelled() => {
                readiness.mark_stopped(generation);
                close_socket(&mut socket).await;
                return Ok(());
            }
            result = timeout(timing.connect_timeout, send_subscription(&mut socket, &cancellation)) => {
                result.unwrap_or(Err(SessionFailure::Subscription))
            }
        };
        if let Err(error) = subscription {
            if error == SessionFailure::Cancelled {
                readiness.mark_stopped(generation);
                close_socket(&mut socket).await;
                return Ok(());
            }
            if error == SessionFailure::Authentication {
                readiness.mark_error(generation, PrivateRealtimeFailure::Authentication);
                return Err(PrivateRealtimeError::Authentication);
            }
            generation = recover_from_session(
                &readiness,
                generation,
                error,
                &mut backoff_attempt,
                &cancellation,
            )
            .await?;
            continue;
        }
        readiness.mark_subscribed(generation)?;

        let heartbeat = match bootstrap(
            &client,
            &mut socket,
            &cancellation,
            &producer,
            generation,
            timing,
            &mut stream_sequence,
            &mut recent_order_ids,
        )
        .await
        {
            Ok(heartbeat) => heartbeat,
            Err(SessionFailure::Cancelled) => {
                readiness.mark_stopped(generation);
                close_socket(&mut socket).await;
                return Ok(());
            }
            Err(SessionFailure::Authentication) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::Authentication);
                return Err(PrivateRealtimeError::Authentication);
            }
            Err(SessionFailure::ConsumerClosed) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::ConsumerClosed);
                return Err(PrivateRealtimeError::ConsumerClosed);
            }
            Err(SessionFailure::ExternallyInvalidated) => {
                let next_generation = readiness.snapshot().generation;
                if next_generation <= generation {
                    readiness.mark_error(generation, PrivateRealtimeFailure::InternalState);
                    return Err(PrivateRealtimeError::StaleGeneration);
                }
                generation = next_generation;
                close_socket(&mut socket).await;
                backoff_attempt = backoff_attempt.saturating_add(1);
                if wait_backoff(&cancellation, backoff_attempt).await {
                    readiness.mark_stopped(generation);
                    return Ok(());
                }
                continue;
            }
            Err(error) => {
                generation = recover_from_session(
                    &readiness,
                    generation,
                    error,
                    &mut backoff_attempt,
                    &cancellation,
                )
                .await?;
                continue;
            }
        };

        match stream_live(
            &mut socket,
            &cancellation,
            &producer,
            generation,
            heartbeat,
            &mut stream_sequence,
            &mut recent_order_ids,
        )
        .await
        {
            Ok(()) => {
                readiness.mark_stopped(generation);
                return Ok(());
            }
            Err(SessionFailure::Cancelled) => {
                readiness.mark_stopped(generation);
                close_socket(&mut socket).await;
                return Ok(());
            }
            Err(SessionFailure::Authentication) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::Authentication);
                return Err(PrivateRealtimeError::Authentication);
            }
            Err(SessionFailure::ConsumerClosed) => {
                readiness.mark_error(generation, PrivateRealtimeFailure::ConsumerClosed);
                return Err(PrivateRealtimeError::ConsumerClosed);
            }
            Err(SessionFailure::ExternallyInvalidated) => {
                let next_generation = readiness.snapshot().generation;
                if next_generation <= generation {
                    readiness.mark_error(generation, PrivateRealtimeFailure::InternalState);
                    return Err(PrivateRealtimeError::StaleGeneration);
                }
                generation = next_generation;
                close_socket(&mut socket).await;
                backoff_attempt = backoff_attempt.saturating_add(1);
                if wait_backoff(&cancellation, backoff_attempt).await {
                    readiness.mark_stopped(generation);
                    return Ok(());
                }
            }
            Err(error) => {
                if connected_at.elapsed() >= timing.stable_connection_period {
                    backoff_attempt = 0;
                }
                generation = recover_from_session(
                    &readiness,
                    generation,
                    error,
                    &mut backoff_attempt,
                    &cancellation,
                )
                .await?;
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SessionTiming {
    connect_timeout: Duration,
    baseline_timeout: Duration,
    ping_interval: Duration,
    pong_timeout: Duration,
    stable_connection_period: Duration,
}

impl SessionTiming {
    const fn production() -> Self {
        Self {
            connect_timeout: PRIVATE_CONNECT_TIMEOUT,
            baseline_timeout: PRIVATE_BASELINE_TIMEOUT,
            ping_interval: PRIVATE_PING_INTERVAL,
            pong_timeout: PRIVATE_PONG_TIMEOUT,
            stable_connection_period: Duration::from_secs(60),
        }
    }

    #[cfg(test)]
    pub(crate) const fn test() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            baseline_timeout: Duration::from_secs(2),
            ping_interval: Duration::from_millis(500),
            pong_timeout: Duration::from_millis(250),
            stable_connection_period: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionFailure {
    Disconnected,
    Subscription,
    Protocol,
    Baseline,
    BaselineByteLimit,
    BaselineRecordLimit,
    BootstrapByteLimit,
    OutputBackpressure,
    PongTimeout,
    Authentication,
    ConsumerClosed,
    ExternallyInvalidated,
    Cancelled,
}

fn map_baseline_error(error: &ClientError) -> SessionFailure {
    match error {
        ClientError::PrivateBaselineByteLimit { .. } => SessionFailure::BaselineByteLimit,
        ClientError::PrivateBaselineLimit { .. } => SessionFailure::BaselineRecordLimit,
        _ => SessionFailure::Baseline,
    }
}

impl SessionFailure {
    const fn health_category(self) -> PrivateRealtimeFailure {
        match self {
            Self::Disconnected => PrivateRealtimeFailure::Connect,
            Self::Subscription => PrivateRealtimeFailure::Subscription,
            Self::Protocol => PrivateRealtimeFailure::Protocol,
            Self::Baseline => PrivateRealtimeFailure::Baseline,
            Self::BaselineByteLimit => PrivateRealtimeFailure::BaselineByteLimit,
            Self::BaselineRecordLimit => PrivateRealtimeFailure::BaselineRecordLimit,
            Self::BootstrapByteLimit => PrivateRealtimeFailure::BootstrapByteLimit,
            Self::OutputBackpressure => PrivateRealtimeFailure::OutputBackpressure,
            Self::PongTimeout => PrivateRealtimeFailure::PongTimeout,
            Self::Authentication => PrivateRealtimeFailure::Authentication,
            Self::ConsumerClosed => PrivateRealtimeFailure::ConsumerClosed,
            Self::ExternallyInvalidated => PrivateRealtimeFailure::Reconciliation,
            Self::Cancelled => PrivateRealtimeFailure::InternalState,
        }
    }
}

async fn recover_from_session(
    readiness: &PrivateReadinessFence,
    generation: u64,
    failure: SessionFailure,
    backoff_attempt: &mut u32,
    cancellation: &CancellationToken,
) -> Result<u64, PrivateRealtimeError> {
    let dropped = matches!(
        failure,
        SessionFailure::Protocol
            | SessionFailure::OutputBackpressure
            | SessionFailure::BootstrapByteLimit
    );
    let next_generation =
        readiness.begin_recovery(generation, failure.health_category(), dropped)?;
    *backoff_attempt = backoff_attempt.saturating_add(1);
    if wait_backoff(cancellation, *backoff_attempt).await {
        readiness.mark_stopped(next_generation);
        return Ok(next_generation);
    }
    Ok(next_generation)
}

async fn wait_backoff(cancellation: &CancellationToken, attempt: u32) -> bool {
    let shift = attempt.saturating_sub(1).min(6);
    let factor = 1u32 << shift;
    let base = PRIVATE_MIN_BACKOFF
        .saturating_mul(factor)
        .min(PRIVATE_MAX_BACKOFF);
    let delay = jittered_backoff(base);
    tokio::select! {
        () = cancellation.cancelled() => true,
        () = sleep(delay) => false,
    }
}

async fn wait_until_or_cancel(cancellation: &CancellationToken, deadline: Instant) -> bool {
    tokio::select! {
        () = cancellation.cancelled() => true,
        () = sleep_until(deadline) => false,
    }
}

fn jittered_backoff(base: Duration) -> Duration {
    let base_ms = base.as_millis();
    let spread = base_ms / 5;
    if spread == 0 {
        return base;
    }
    let sample = generate_nonce()
        .ok()
        .and_then(|nonce| u16::from_str_radix(&nonce[..4], 16).ok())
        .map_or(u128::from(32_768_u16), u128::from);
    let width = spread.saturating_mul(2).saturating_add(1);
    let offset = sample.saturating_mul(width) / u128::from(u16::MAX);
    let milliseconds = base_ms.saturating_sub(spread).saturating_add(offset);
    Duration::from_millis(u64::try_from(milliseconds).unwrap_or(u64::MAX)).min(PRIVATE_MAX_BACKOFF)
}

fn validate_endpoint(endpoint: &str) -> Result<(), PrivateRealtimeError> {
    if endpoint == UPBIT_PRIVATE_WEBSOCKET_ENDPOINT {
        return verify_private_websocket_endpoint(endpoint)
            .map_err(|_| PrivateRealtimeError::EndpointRejected);
    }
    #[cfg(test)]
    {
        let parsed =
            reqwest::Url::parse(endpoint).map_err(|_| PrivateRealtimeError::EndpointRejected)?;
        if parsed.scheme() == "ws"
            && parsed.host_str() == Some("127.0.0.1")
            && parsed.port().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.path() == "/websocket/v1/private"
            && parsed.query().is_none()
            && parsed.fragment().is_none()
        {
            return Ok(());
        }
    }
    Err(PrivateRealtimeError::EndpointRejected)
}

fn private_request(
    client: &UpbitPrivateClient,
    endpoint: &str,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, PrivateRealtimeError> {
    validate_endpoint(endpoint)?;
    let mut request = endpoint
        .into_client_request()
        .map_err(|_| PrivateRealtimeError::EndpointRejected)?;
    let authorization = client
        .private_websocket_authorization_header()
        .map_err(|_| PrivateRealtimeError::Authentication)?;
    request
        .headers_mut()
        .insert(reqwest::header::AUTHORIZATION, authorization);
    Ok(request)
}

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_PRIVATE_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_PRIVATE_MESSAGE_BYTES))
        .read_buffer_size(16 * 1024)
        .write_buffer_size(4 * 1024)
        .max_write_buffer_size(8 * 1024)
}

fn is_authentication_failure(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(
        error,
        tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status().as_u16() == 401 || response.status().as_u16() == 403
    )
}

async fn send_subscription(
    socket: &mut PrivateWebSocket,
    cancellation: &CancellationToken,
) -> Result<(), SessionFailure> {
    let ticket = generate_nonce().map_err(|_| SessionFailure::Subscription)?;
    let request = serde_json::json!([
        {"ticket": ticket},
        {"type": "myOrder", "codes": []},
        {"type": "myAsset"},
        {"format": "DEFAULT"}
    ]);
    let payload = serde_json::to_string(&request).map_err(|_| SessionFailure::Subscription)?;
    if payload.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(SessionFailure::Subscription);
    }
    send_or_cancel(socket, Message::Text(payload.into()), cancellation).await
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one generation-scoped bootstrap transaction couples its bounded REST baseline, socket buffer, and delivery fence"
)]
async fn bootstrap(
    client: &UpbitPrivateClient,
    socket: &mut PrivateWebSocket,
    cancellation: &CancellationToken,
    producer: &PrivateRealtimeProducer,
    generation: u64,
    timing: SessionTiming,
    stream_sequence: &mut u64,
    recent_order_ids: &mut RecentOrderIdentities,
) -> Result<Heartbeat, SessionFailure> {
    let byte_budget = Arc::new(PrivateBaselineByteBudget::new());
    let baseline_budget = Arc::clone(&byte_budget);
    let baseline_client = client.clone();
    let baseline = async move {
        timeout(timing.baseline_timeout, async move {
            let (balances, open_orders, response_bytes) = baseline_client
                .get_private_account_baseline_bounded(baseline_budget)
                .await
                .map_err(|error| map_baseline_error(&error))?;
            Ok::<_, SessionFailure>((balances, open_orders, response_bytes))
        })
        .await
        .map_err(|_| SessionFailure::Baseline)?
    };
    tokio::pin!(baseline);
    let mut buffered = VecDeque::with_capacity(MAX_BOOTSTRAP_EVENTS);
    let mut buffered_event_bytes = 0usize;
    let mut heartbeat = Heartbeat::new(timing.ping_interval, timing.pong_timeout);

    let (balances, open_orders, rest_response_bytes) = loop {
        tokio::select! {
            () = cancellation.cancelled() => return Err(SessionFailure::Cancelled),
            () = producer.events.closed() => return Err(SessionFailure::ConsumerClosed),
            _ = heartbeat.interval.tick() => send_ping(socket, &mut heartbeat, cancellation).await?,
            () = wait_for_pong(heartbeat.pending_pong) => return Err(SessionFailure::PongTimeout),
            result = &mut baseline => break result?,
            incoming = socket.next() => {
                match read_private_message(incoming)? {
                    Incoming::Control(ControlMessage::Ping(payload)) => {
                        send_or_cancel(socket, Message::Pong(payload), cancellation).await?;
                    }
                    Incoming::Control(ControlMessage::Pong) => {
                        heartbeat.pending_pong = None;
                        producer.readiness.record_pong().map_err(|_| SessionFailure::Protocol)?;
                    }
                    Incoming::Data(bytes) => {
                        byte_budget
                            .record(bytes.len())
                            .map_err(|_| SessionFailure::BootstrapByteLimit)?;
                        buffered_event_bytes = buffered_event_bytes
                            .checked_add(bytes.len())
                            .ok_or(SessionFailure::BootstrapByteLimit)?;
                        *stream_sequence = stream_sequence
                            .checked_add(1)
                            .ok_or(SessionFailure::Protocol)?;
                        let mut event = decode_event(&bytes, generation, UtcTimestamp::now())
                            .map_err(DecodeFailure::session_failure)?;
                        set_stream_sequence(&mut event, *stream_sequence);
                        if buffered.len() >= MAX_BOOTSTRAP_EVENTS {
                            return Err(SessionFailure::OutputBackpressure);
                        }
                        buffered.push_back(event);
                    }
                    Incoming::Control(ControlMessage::Closed) => return Err(SessionFailure::Disconnected),
                }
            }
        }
    };

    let baseline = PrivateAccountEvent::Baseline {
        baseline: PrivateAccountBaseline {
            generation,
            observed_at_utc: UtcTimestamp::now(),
            rest_response_bytes,
            buffered_event_bytes,
            aggregate_bytes_used: byte_budget.used(),
            aggregate_byte_limit: MAX_PRIVATE_BASELINE_BYTES,
            open_orders_complete: true,
            closed_order_history_complete: false,
            gap_reconciliation_required: generation > 1,
            balances,
            open_orders,
        },
        history_scope: PrivateOrderHistoryScope::OpenOrdersOnly,
    };
    let publication_deadline = Instant::now() + PRIVATE_BOOTSTRAP_DELIVERY_TIMEOUT;
    send_bootstrap_event(
        producer,
        generation,
        baseline,
        cancellation,
        publication_deadline,
    )
    .await?;
    for event in buffered {
        if is_duplicate_order(&event, recent_order_ids) {
            continue;
        }
        producer
            .readiness
            .record_message(&event)
            .map_err(map_readiness_failure)?;
        send_bootstrap_event(
            producer,
            generation,
            event,
            cancellation,
            publication_deadline,
        )
        .await?;
    }
    producer
        .readiness
        .mark_recovering(generation)
        .map_err(map_readiness_failure)?;
    send_bootstrap_event(
        producer,
        generation,
        PrivateAccountEvent::BootstrapComplete {
            generation,
            gap_reconciliation_required: generation > 1,
            known_order_readbacks_required: true,
            closed_order_history_complete: false,
        },
        cancellation,
        publication_deadline,
    )
    .await?;
    Ok(heartbeat)
}

async fn stream_live(
    socket: &mut PrivateWebSocket,
    cancellation: &CancellationToken,
    producer: &PrivateRealtimeProducer,
    generation: u64,
    mut heartbeat: Heartbeat,
    stream_sequence: &mut u64,
    recent_order_ids: &mut RecentOrderIdentities,
) -> Result<(), SessionFailure> {
    let mut generations = producer.readiness.subscribe_generation();
    if *generations.borrow() != generation {
        return Err(SessionFailure::ExternallyInvalidated);
    }
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Err(SessionFailure::Cancelled),
            () = producer.events.closed() => return Err(SessionFailure::ConsumerClosed),
            changed = generations.changed() => {
                if changed.is_err() || *generations.borrow_and_update() != generation {
                    return Err(SessionFailure::ExternallyInvalidated);
                }
            }
            _ = heartbeat.interval.tick() => send_ping(socket, &mut heartbeat, cancellation).await?,
            () = wait_for_pong(heartbeat.pending_pong) => return Err(SessionFailure::PongTimeout),
            incoming = socket.next() => {
                match read_private_message(incoming)? {
                    Incoming::Control(ControlMessage::Ping(payload)) => {
                        send_or_cancel(socket, Message::Pong(payload), cancellation).await?;
                    }
                    Incoming::Control(ControlMessage::Pong) => {
                        heartbeat.pending_pong = None;
                        producer.readiness.record_pong().map_err(|_| SessionFailure::Protocol)?;
                    }
                    Incoming::Data(bytes) => {
                        *stream_sequence = stream_sequence
                            .checked_add(1)
                            .ok_or(SessionFailure::Protocol)?;
                        let mut event = decode_event(&bytes, generation, UtcTimestamp::now())
                            .map_err(DecodeFailure::session_failure)?;
                        set_stream_sequence(&mut event, *stream_sequence);
                        if is_duplicate_order(&event, recent_order_ids) {
                            continue;
                        }
                        producer
                            .readiness
                            .record_message(&event)
                            .map_err(map_readiness_failure)?;
                        emit_event(producer, generation, event)?;
                    }
                    Incoming::Control(ControlMessage::Closed) => return Err(SessionFailure::Disconnected),
                }
            }
        }
    }
}

fn emit_event(
    producer: &PrivateRealtimeProducer,
    generation: u64,
    event: PrivateAccountEvent,
) -> Result<(), SessionFailure> {
    if producer.readiness.generation() != generation {
        return Err(SessionFailure::ExternallyInvalidated);
    }
    match producer.events.try_send(event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Err(SessionFailure::OutputBackpressure),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(SessionFailure::ConsumerClosed),
    }
}

fn map_readiness_failure(error: PrivateRealtimeError) -> SessionFailure {
    match error {
        PrivateRealtimeError::StaleGeneration => SessionFailure::ExternallyInvalidated,
        _ => SessionFailure::Protocol,
    }
}

async fn send_bootstrap_event(
    producer: &PrivateRealtimeProducer,
    generation: u64,
    event: PrivateAccountEvent,
    cancellation: &CancellationToken,
    publication_deadline: Instant,
) -> Result<(), SessionFailure> {
    let mut generations = producer.readiness.subscribe_generation();
    if *generations.borrow() != generation {
        return Err(SessionFailure::ExternallyInvalidated);
    }
    tokio::select! {
        () = cancellation.cancelled() => Err(SessionFailure::Cancelled),
        changed = generations.changed() => {
            if changed.is_err() || *generations.borrow_and_update() != generation {
                Err(SessionFailure::ExternallyInvalidated)
            } else {
                Err(SessionFailure::Protocol)
            }
        }
        result = timeout_at(publication_deadline, producer.events.send(event)) => {
            match result {
                Err(_) => Err(SessionFailure::OutputBackpressure),
                Ok(Err(_)) => Err(SessionFailure::ConsumerClosed),
                Ok(Ok(())) => Ok(()),
            }
        }
    }
}

struct Heartbeat {
    interval: Interval,
    timeout: Duration,
    pending_pong: Option<Instant>,
}

impl Heartbeat {
    fn new(interval_duration: Duration, timeout_duration: Duration) -> Self {
        let start = Instant::now() + interval_duration;
        let mut interval = interval_at(start, interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            interval,
            timeout: timeout_duration,
            pending_pong: None,
        }
    }
}

async fn send_ping(
    socket: &mut PrivateWebSocket,
    heartbeat: &mut Heartbeat,
    cancellation: &CancellationToken,
) -> Result<(), SessionFailure> {
    if heartbeat.pending_pong.is_some() {
        return Err(SessionFailure::PongTimeout);
    }
    send_or_cancel(socket, Message::Ping(Vec::new().into()), cancellation).await?;
    heartbeat.pending_pong = Some(Instant::now() + heartbeat.timeout);
    Ok(())
}

async fn send_or_cancel(
    socket: &mut PrivateWebSocket,
    message: Message,
    cancellation: &CancellationToken,
) -> Result<(), SessionFailure> {
    tokio::select! {
        () = cancellation.cancelled() => Err(SessionFailure::Cancelled),
        result = socket.send(message) => result.map_err(|_| SessionFailure::Disconnected),
    }
}

async fn close_socket(socket: &mut PrivateWebSocket) {
    let _ = timeout(Duration::from_millis(250), socket.close(None)).await;
}

async fn wait_for_pong(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

enum ControlMessage {
    Ping(tokio_tungstenite::tungstenite::Bytes),
    Pong,
    Closed,
}

enum Incoming {
    Control(ControlMessage),
    Data(Vec<u8>),
}

fn read_private_message(
    incoming: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Result<Incoming, SessionFailure> {
    match incoming {
        None | Some(Err(_)) => Err(SessionFailure::Disconnected),
        Some(Ok(Message::Text(text))) => {
            if text.len() > MAX_PRIVATE_MESSAGE_BYTES {
                return Err(SessionFailure::Protocol);
            }
            Ok(Incoming::Data(text.as_bytes().to_vec()))
        }
        Some(Ok(Message::Binary(bytes))) => {
            if bytes.len() > MAX_PRIVATE_MESSAGE_BYTES {
                return Err(SessionFailure::Protocol);
            }
            Ok(Incoming::Data(bytes.to_vec()))
        }
        Some(Ok(Message::Ping(payload))) => Ok(Incoming::Control(ControlMessage::Ping(payload))),
        Some(Ok(Message::Pong(_))) => Ok(Incoming::Control(ControlMessage::Pong)),
        Some(Ok(Message::Close(_))) => Ok(Incoming::Control(ControlMessage::Closed)),
        Some(Ok(Message::Frame(_))) => Err(SessionFailure::Protocol),
    }
}

#[derive(Debug)]
enum DecodeFailure {
    Protocol,
    Authentication,
}

impl DecodeFailure {
    const fn session_failure(self) -> SessionFailure {
        match self {
            Self::Protocol => SessionFailure::Protocol,
            Self::Authentication => SessionFailure::Authentication,
        }
    }
}

fn decode_event(
    bytes: &[u8],
    generation: u64,
    received_at_utc: UtcTimestamp,
) -> Result<PrivateAccountEvent, DecodeFailure> {
    if bytes.is_empty() || bytes.len() > MAX_PRIVATE_MESSAGE_BYTES {
        return Err(DecodeFailure::Protocol);
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| DecodeFailure::Protocol)?;
    if let Some(error_name) = value
        .pointer("/error/name")
        .and_then(serde_json::Value::as_str)
    {
        return if error_name == "INVALID_AUTH" {
            Err(DecodeFailure::Authentication)
        } else {
            Err(DecodeFailure::Protocol)
        };
    }
    let kind = value
        .get("type")
        .or_else(|| value.get("ty"))
        .and_then(serde_json::Value::as_str)
        .ok_or(DecodeFailure::Protocol)?;
    match kind {
        "myOrder" => decode_order(bytes, generation, received_at_utc, payload_hash(bytes))
            .map(Box::new)
            .map(PrivateAccountEvent::OrderObserved),
        "myAsset" => decode_asset(bytes, generation, received_at_utc, payload_hash(bytes))
            .map(PrivateAccountEvent::AssetObserved),
        _ => Err(DecodeFailure::Protocol),
    }
}

#[derive(Deserialize)]
struct MyOrderWire {
    #[serde(rename = "type", alias = "ty")]
    kind: String,
    code: String,
    uuid: String,
    ask_bid: String,
    order_type: String,
    state: String,
    #[serde(default)]
    trade_uuid: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    avg_price: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    volume: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    remaining_volume: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    executed_volume: Option<Decimal>,
    #[serde(default)]
    trades_count: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    reserved_fee: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    remaining_fee: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    paid_fee: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    locked: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    executed_funds: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    trade_fee: Option<Decimal>,
    #[serde(default)]
    is_maker: Option<bool>,
    #[serde(default)]
    identifier: Option<String>,
    #[serde(default)]
    smp_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    prevented_volume: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    prevented_locked: Option<Decimal>,
    order_timestamp: u64,
    #[serde(default)]
    trade_timestamp: Option<u64>,
    timestamp: u64,
    stream_type: String,
}

#[expect(
    clippy::too_many_lines,
    reason = "the provider myOrder decoder validates and projects one fixed wire record without intermediate representations"
)]
fn decode_order(
    bytes: &[u8],
    generation: u64,
    received_at_utc: UtcTimestamp,
    event_payload_hash: String,
) -> Result<PrivateOrderObservation, DecodeFailure> {
    let wire: MyOrderWire = serde_json::from_slice(bytes).map_err(|_| DecodeFailure::Protocol)?;
    if wire.kind != "myOrder"
        || !valid_provider_token(&wire.uuid, 128)
        || wire
            .identifier
            .as_deref()
            .is_some_and(|identifier| !valid_provider_token(identifier, 64))
        || !matches!(
            wire.order_type.as_str(),
            "limit" | "price" | "market" | "best"
        )
        || !matches!(wire.stream_type.as_str(), "REALTIME" | "SNAPSHOT")
    {
        return Err(DecodeFailure::Protocol);
    }
    let side = match wire.ask_bid.as_str() {
        "BID" => PrivateOrderSide::Bid,
        "ASK" => PrivateOrderSide::Ask,
        _ => return Err(DecodeFailure::Protocol),
    };
    let state = match wire.state.as_str() {
        "wait" => PrivateOrderState::Wait,
        "watch" => PrivateOrderState::Watch,
        "trade" => PrivateOrderState::Trade,
        "done" => PrivateOrderState::Done,
        "cancel" => PrivateOrderState::Cancel,
        "prevented" => PrivateOrderState::Prevented,
        _ => return Err(DecodeFailure::Protocol),
    };
    let market = MarketId::parse_upbit(&wire.code).map_err(|_| DecodeFailure::Protocol)?;
    let provider_at_utc = timestamp_from_millis(wire.timestamp)?;
    let provider_order_at_utc = timestamp_from_millis(wire.order_timestamp)?;
    let provider_trade_at_utc = wire
        .trade_timestamp
        .map(timestamp_from_millis)
        .transpose()?;
    if state == PrivateOrderState::Trade
        && (wire
            .trade_uuid
            .as_deref()
            .is_none_or(|trade_uuid| !valid_provider_token(trade_uuid, 128))
            || provider_trade_at_utc.is_none())
    {
        return Err(DecodeFailure::Protocol);
    }
    let identity = if state == PrivateOrderState::Trade {
        PrivateOrderEventIdentity::Trade {
            trade_uuid: wire.trade_uuid.clone().ok_or(DecodeFailure::Protocol)?,
        }
    } else {
        PrivateOrderEventIdentity::OrderState {
            order_uuid: wire.uuid.clone(),
            state,
            provider_timestamp_ms: wire.timestamp,
            event_payload_hash,
        }
    };
    for amount in [
        wire.price,
        wire.avg_price,
        wire.volume,
        wire.remaining_volume,
        wire.executed_volume,
        wire.reserved_fee,
        wire.remaining_fee,
        wire.paid_fee,
        wire.locked,
        wire.executed_funds,
        wire.trade_fee,
        wire.prevented_volume,
        wire.prevented_locked,
    ]
    .into_iter()
    .flatten()
    {
        if amount < Decimal::ZERO {
            return Err(DecodeFailure::Protocol);
        }
    }
    Ok(PrivateOrderObservation {
        generation,
        stream_sequence: 0,
        identity,
        order_uuid: wire.uuid,
        client_identifier: wire.identifier,
        market,
        side,
        order_type: wire.order_type,
        state,
        price: wire.price,
        average_price: wire.avg_price,
        volume: wire.volume,
        remaining_volume: wire.remaining_volume,
        executed_volume: wire.executed_volume,
        trades_count: wire.trades_count,
        reserved_fee: wire.reserved_fee,
        remaining_fee: wire.remaining_fee,
        paid_fee: wire.paid_fee,
        locked: wire.locked,
        executed_funds: wire.executed_funds,
        trade_fee: wire.trade_fee,
        is_maker: wire.is_maker,
        smp_type: wire.smp_type,
        prevented_volume: wire.prevented_volume,
        prevented_locked: wire.prevented_locked,
        provider_order_at_utc,
        provider_trade_at_utc,
        provider_at_utc,
        received_at_utc,
    })
}

#[derive(Deserialize)]
struct MyAssetWire {
    #[serde(rename = "type", alias = "ty")]
    kind: String,
    asset_uuid: String,
    assets: Vec<MyAssetBalanceWire>,
    asset_timestamp: u64,
    timestamp: u64,
    stream_type: String,
}

#[derive(Deserialize)]
struct MyAssetBalanceWire {
    currency: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    balance: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    locked: Decimal,
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    decimal_from_json(value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(decimal_from_json)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn decimal_from_json(value: serde_json::Value) -> Result<Decimal, &'static str> {
    let value = match value {
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(value) => value,
        _ => return Err("private decimal must be a JSON number or decimal string"),
    };
    decimal_from_lexeme(&value)
}

#[expect(
    clippy::too_many_lines,
    reason = "exact exponent normalization must preserve the source decimal lexeme without floating-point conversion"
)]
fn decimal_from_lexeme(value: &str) -> Result<Decimal, &'static str> {
    let Some(exponent_at) = value.bytes().position(|byte| matches!(byte, b'e' | b'E')) else {
        return Decimal::from_str_exact(value).map_err(|_| "private decimal is invalid");
    };
    let mantissa = &value[..exponent_at];
    let exponent_text = &value[exponent_at + 1..];
    let exponent_digits = exponent_text
        .strip_prefix('-')
        .or_else(|| exponent_text.strip_prefix('+'))
        .unwrap_or(exponent_text);
    if exponent_digits.is_empty() || !exponent_digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("private decimal exponent is invalid");
    }

    let (negative, mantissa) = if let Some(value) = mantissa.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = mantissa.strip_prefix('+') {
        (false, value)
    } else {
        (false, mantissa)
    };
    let mut digits = Vec::with_capacity(mantissa.len());
    let mut decimal_position = None;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            if decimal_position.is_some() || digits.is_empty() {
                return Err("private decimal mantissa is invalid");
            }
            decimal_position = Some(digits.len());
        } else if byte.is_ascii_digit() {
            digits.push(byte);
        } else {
            return Err("private decimal mantissa is invalid");
        }
    }
    if digits.is_empty() || decimal_position == Some(digits.len()) {
        return Err("private decimal mantissa is invalid");
    }
    let Some(first_nonzero) = digits.iter().position(|digit| *digit != b'0') else {
        return Ok(Decimal::ZERO);
    };
    let last_nonzero = digits
        .iter()
        .rposition(|digit| *digit != b'0')
        .ok_or("private decimal mantissa is invalid")?
        + 1;
    let significant_digits = &digits[first_nonzero..last_nonzero];

    let exponent = exponent_text
        .parse::<i64>()
        .map_err(|_| "private decimal exponent exceeds the supported range")?;
    let decimal_position = decimal_position.unwrap_or(digits.len());
    let position = i64::try_from(decimal_position)
        .ok()
        .and_then(|position| position.checked_add(exponent))
        .and_then(|position| {
            i64::try_from(first_nonzero)
                .ok()
                .and_then(|leading| position.checked_sub(leading))
        })
        .ok_or("private decimal exponent exceeds the supported range")?;
    let significant_len =
        i64::try_from(significant_digits.len()).map_err(|_| "private decimal is too long")?;
    let scale = if position < significant_len {
        significant_len
            .checked_sub(position)
            .ok_or("private decimal scale exceeds the supported range")?
    } else {
        0
    };
    let coefficient_digits = if position > 0 {
        position.max(significant_len)
    } else {
        significant_len
    };
    if scale > i64::from(Decimal::MAX_SCALE) || coefficient_digits > 29 {
        return Err("private decimal cannot be represented exactly");
    }

    let mut normalized = String::with_capacity(
        usize::try_from(coefficient_digits + i64::from(scale > 0))
            .map_err(|_| "private decimal is too long")?,
    );
    if negative {
        normalized.push('-');
    }
    if position <= 0 {
        normalized.push_str("0.");
        let leading_fractional_zeroes = usize::try_from(-position)
            .map_err(|_| "private decimal scale exceeds the supported range")?;
        normalized.extend(std::iter::repeat_n('0', leading_fractional_zeroes));
        normalized.push_str(
            std::str::from_utf8(significant_digits)
                .map_err(|_| "private decimal mantissa is invalid")?,
        );
    } else {
        let integer_digits =
            usize::try_from(position).map_err(|_| "private decimal is too long")?;
        if integer_digits >= significant_digits.len() {
            normalized.push_str(
                std::str::from_utf8(significant_digits)
                    .map_err(|_| "private decimal mantissa is invalid")?,
            );
            normalized.extend(std::iter::repeat_n(
                '0',
                integer_digits - significant_digits.len(),
            ));
        } else {
            let digits = std::str::from_utf8(significant_digits)
                .map_err(|_| "private decimal mantissa is invalid")?;
            normalized.push_str(&digits[..integer_digits]);
            normalized.push('.');
            normalized.push_str(&digits[integer_digits..]);
        }
    }
    Decimal::from_str_exact(&normalized).map_err(|_| "private decimal is out of range")
}

fn decode_asset(
    bytes: &[u8],
    generation: u64,
    received_at_utc: UtcTimestamp,
    event_payload_hash: String,
) -> Result<PrivateAssetObservation, DecodeFailure> {
    let wire: MyAssetWire = serde_json::from_slice(bytes).map_err(|_| DecodeFailure::Protocol)?;
    if wire.kind != "myAsset"
        || !valid_provider_token(&wire.asset_uuid, 128)
        || wire.assets.len() > MAX_ASSETS_PER_EVENT
        || wire.stream_type != "REALTIME"
    {
        return Err(DecodeFailure::Protocol);
    }
    let mut assets = Vec::with_capacity(wire.assets.len());
    for asset in wire.assets {
        if !valid_currency(&asset.currency)
            || asset.balance < Decimal::ZERO
            || asset.locked < Decimal::ZERO
        {
            return Err(DecodeFailure::Protocol);
        }
        assets.push(PrivateAssetBalance {
            currency: asset.currency,
            balance: asset.balance,
            locked: asset.locked,
        });
    }
    Ok(PrivateAssetObservation {
        generation,
        stream_sequence: 0,
        asset_uuid: wire.asset_uuid,
        event_payload_hash,
        assets,
        provider_asset_at_utc: timestamp_from_millis(wire.asset_timestamp)?,
        provider_at_utc: timestamp_from_millis(wire.timestamp)?,
        received_at_utc,
    })
}

fn payload_hash(bytes: &[u8]) -> String {
    crate::upbit::auth::hex_encode(&Sha256::digest(bytes))
}

fn timestamp_from_millis(value: u64) -> Result<UtcTimestamp, DecodeFailure> {
    let value = i64::try_from(value).map_err(|_| DecodeFailure::Protocol)?;
    DateTime::<Utc>::from_timestamp_millis(value)
        .map(UtcTimestamp)
        .ok_or(DecodeFailure::Protocol)
}

fn valid_provider_token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_currency(value: &str) -> bool {
    (2..=16).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn is_duplicate_order(event: &PrivateAccountEvent, recent: &mut RecentOrderIdentities) -> bool {
    match event {
        PrivateAccountEvent::OrderObserved(observation) => {
            !recent.insert(observation.identity.clone())
        }
        _ => false,
    }
}

fn set_stream_sequence(event: &mut PrivateAccountEvent, sequence: u64) {
    match event {
        PrivateAccountEvent::OrderObserved(observation) => {
            observation.stream_sequence = sequence;
        }
        PrivateAccountEvent::AssetObserved(observation) => {
            observation.stream_sequence = sequence;
        }
        PrivateAccountEvent::Baseline { .. } | PrivateAccountEvent::BootstrapComplete { .. } => {}
    }
}

struct RecentOrderIdentities {
    capacity: usize,
    identities: HashSet<PrivateOrderEventIdentity>,
    order: VecDeque<PrivateOrderEventIdentity>,
}

impl RecentOrderIdentities {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            identities: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    fn insert(&mut self, identity: PrivateOrderEventIdentity) -> bool {
        if self.identities.contains(&identity) {
            return false;
        }
        if self.order.len() == self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.identities.remove(&oldest);
        }
        self.identities.insert(identity.clone());
        self.order.push_back(identity);
        true
    }
}

#[cfg(test)]
mod tests;
