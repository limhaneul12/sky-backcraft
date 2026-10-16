//! Runtime order-send control and its shared provider-boundary lease.

use crate::contracts::LabError;
use crate::database::DatabaseHandle;
use crate::upbit::control::ExchangeReadMode;
use crate::upbit::execution::LiveExecutionPolicy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, Notify};

const MAX_REQUEST_ID_BYTES: usize = 128;
const ENABLE_CONFIRMATION: &str = "ENABLE_LIVE_ORDER_SENDING";
const UNSENT_REJECTION_BATCH: usize = 64;

#[derive(Clone)]
pub struct LiveOrderSendGate {
    inner: Arc<LiveOrderSendGateInner>,
}

struct LiveOrderSendGateInner {
    state: Mutex<LiveOrderSendGateState>,
    drained: Notify,
    transition: AsyncMutex<()>,
}

#[derive(Debug)]
struct LiveOrderSendGateState {
    enabled: bool,
    generation: u64,
    active_sends: usize,
}

pub(crate) struct LiveOrderSendLease {
    inner: Arc<LiveOrderSendGateInner>,
}

impl LiveOrderSendGate {
    #[must_use]
    pub fn new_disabled() -> Self {
        Self {
            inner: Arc::new(LiveOrderSendGateInner {
                state: Mutex::new(LiveOrderSendGateState {
                    enabled: false,
                    generation: 0,
                    active_sends: 0,
                }),
                drained: Notify::new(),
                transition: AsyncMutex::new(()),
            }),
        }
    }

    #[must_use]
    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.state.lock().is_ok_and(|state| state.enabled)
    }

    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        match self.inner.state.lock() {
            Ok(state) => state.generation,
            Err(poisoned) => poisoned.into_inner().generation,
        }
    }

    /// Close admission synchronously and invalidate every pre-close token.
    pub(crate) fn close(&self) -> Result<u64, LabError> {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.enabled = false;
                return Err(LabError::Internal(
                    "live order send gate was poisoned and closed".into(),
                ));
            }
        };
        state.enabled = false;
        state.generation = state.generation.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("live order send generation exhausted".into())
        })?;
        Ok(state.generation)
    }

    /// Open only if no disable or other transition superseded the caller.
    pub(crate) fn open_if_current(&self, expected_generation: u64) -> Result<bool, LabError> {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.enabled = false;
                return Err(LabError::Internal(
                    "live order send gate was poisoned and closed".into(),
                ));
            }
        };
        if state.generation != expected_generation {
            return Ok(false);
        }
        if state.enabled {
            return Ok(true);
        }
        if state.active_sends != 0 {
            return Ok(false);
        }
        state.generation = state.generation.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("live order send generation exhausted".into())
        })?;
        state.enabled = true;
        Ok(true)
    }

    pub(crate) fn try_lease(&self, expected_generation: Option<u64>) -> Option<LiveOrderSendLease> {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.enabled = false;
                return None;
            }
        };
        if !state.enabled
            || expected_generation.is_some_and(|generation| generation != state.generation)
        {
            return None;
        }
        state.active_sends = state.active_sends.checked_add(1)?;
        Some(LiveOrderSendLease {
            inner: Arc::clone(&self.inner),
        })
    }

    pub(crate) async fn drain(&self) {
        loop {
            let notified = self.inner.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let active_sends = match self.inner.state.lock() {
                Ok(state) => state.active_sends,
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    state.enabled = false;
                    state.active_sends
                }
            };
            if active_sends == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn lock_transition(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.transition.lock().await
    }
}

impl Drop for LiveOrderSendLease {
    fn drop(&mut self) {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.active_sends > 0 {
            state.active_sends -= 1;
        }
        if state.active_sends == 0 {
            self.inner.drained.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LiveOrderControlAction {
    Status,
    Enable,
    Disable,
    Request,
}

impl LiveOrderControlAction {
    #[must_use]
    pub const fn is_mutation(self) -> bool {
        matches!(self, Self::Enable | Self::Disable)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiveOrderControlOutcome {
    Acknowledged,
    Denied,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderControlRequest {
    pub action: LiveOrderControlAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_epoch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmation: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
pub struct LiveOrderControlReceipt {
    pub request_id: String,
    pub action: LiveOrderControlAction,
    pub outcome: LiveOrderControlOutcome,
    pub sequence: u64,
    pub boot_epoch: String,
    pub observed_at_utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "frozen status wire keeps runtime capability, send gate, global admission, emergency and confirmation facts independent"
)]
pub struct LiveOrderControlResponse {
    pub boot_epoch: String,
    pub live_order_control_available: bool,
    pub live_trading_enabled: bool,
    pub sequence: u64,
    pub observed_at_utc: String,
    pub exchange_read_mode: ExchangeReadMode,
    pub order_admission_blocked: bool,
    pub emergency_latched: bool,
    pub confirmation_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<LiveOrderControlReceipt>,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedLiveOrderControlSnapshot {
    pub boot_epoch: String,
    pub sequence: u64,
    pub live_trading_enabled: bool,
    pub available: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedLiveOrderControlOutcome {
    pub snapshot: PersistedLiveOrderControlSnapshot,
    pub receipt: LiveOrderControlReceipt,
}

#[derive(Debug, Clone)]
pub(crate) enum LiveOrderControlBegin {
    Pending(PersistedLiveOrderControlSnapshot),
    ExistingPending(PersistedLiveOrderControlSnapshot),
    Complete(PersistedLiveOrderControlOutcome),
}

struct CloseGateOnDrop {
    gate: LiveOrderSendGate,
    armed: bool,
}

impl CloseGateOnDrop {
    fn new(gate: LiveOrderSendGate) -> Self {
        Self { gate, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CloseGateOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _closed_generation = self.gate.close();
        }
    }
}

#[derive(Clone)]
pub struct LiveOrderControlService {
    database: DatabaseHandle,
    policy: LiveExecutionPolicy,
    gate: Option<LiveOrderSendGate>,
    exchange_read_mode: ExchangeReadMode,
}

impl LiveOrderControlService {
    #[must_use]
    pub fn new(
        database: DatabaseHandle,
        policy: LiveExecutionPolicy,
        gate: Option<LiveOrderSendGate>,
        exchange_read_mode: ExchangeReadMode,
    ) -> Self {
        Self {
            database,
            policy,
            gate,
            exchange_read_mode,
        }
    }

    /// Read or apply one typed runtime order-send control request.
    ///
    /// # Errors
    /// Rejects malformed identity, stale CAS, unknown request IDs, or storage failures.
    pub async fn execute(
        &self,
        request: LiveOrderControlRequest,
    ) -> Result<LiveOrderControlResponse, LabError> {
        validate_request(&request)?;
        match request.action {
            LiveOrderControlAction::Status => self.status(None).await,
            LiveOrderControlAction::Request => {
                let request_id = request
                    .request_id
                    .as_deref()
                    .ok_or_else(|| LabError::InvalidConfig("missing request_id".into()))?;
                self.read_request(request_id).await
            }
            LiveOrderControlAction::Enable => self.mutate(request, true).await,
            LiveOrderControlAction::Disable => {
                if let Some(gate) = &self.gate {
                    // Disable linearizes before any async lock, Store command, or drain.
                    let _ = gate.close();
                }
                self.mutate(request, false).await
            }
        }
    }

    async fn mutate(
        &self,
        request: LiveOrderControlRequest,
        enable: bool,
    ) -> Result<LiveOrderControlResponse, LabError> {
        let Some(gate) = &self.gate else {
            return self.begin_unavailable(request).await;
        };
        let expected_generation = gate.generation();
        let effective_gate_enabled = gate.is_enabled();
        let _transition = gate.lock_transition().await;
        let request_id = request
            .request_id
            .as_deref()
            .ok_or_else(|| LabError::InvalidConfig("missing request_id".into()))?;
        let expected_sequence = request
            .expected_sequence
            .ok_or_else(|| LabError::InvalidConfig("missing expected_sequence".into()))?;
        let expected_epoch = request
            .expected_epoch
            .as_deref()
            .ok_or_else(|| LabError::InvalidConfig("missing expected_epoch".into()))?;
        let precondition_denied_code = if !self.policy.is_enabled() {
            Some("CONTROL_UNAVAILABLE")
        } else if enable && request.confirmation.as_deref() != Some(ENABLE_CONFIRMATION) {
            Some("CONFIRMATION_REQUIRED")
        } else {
            None
        };
        let request_id_owned = request_id.to_owned();
        let expected_epoch_owned = expected_epoch.to_owned();
        let action = request.action;
        let observed_at = chrono::Utc::now().to_rfc3339();
        let begin = self
            .database
            .call("begin_live_order_control", move |store| {
                store.live_order_control_begin(
                    action,
                    &request_id_owned,
                    expected_sequence,
                    &expected_epoch_owned,
                    precondition_denied_code,
                    !enable,
                    effective_gate_enabled,
                    &observed_at,
                )
            })
            .await;
        let begin = match begin {
            Ok(begin) => begin,
            Err(error) => {
                if !enable {
                    self.reject_unsent_live_work().await?;
                    gate.drain().await;
                }
                return Err(error);
            }
        };
        match begin {
            LiveOrderControlBegin::Complete(outcome) => {
                if !enable {
                    self.reject_unsent_live_work().await?;
                    gate.drain().await;
                }
                self.project(outcome.snapshot, Some(outcome.receipt)).await
            }
            LiveOrderControlBegin::ExistingPending(_snapshot) => Err(LabError::OutcomeUnknown(
                "live-order control request is still pending; read its request_id before retrying"
                    .into(),
            )),
            LiveOrderControlBegin::Pending(_snapshot) => {
                if enable {
                    self.reject_unsent_live_work().await?;
                    if !gate.open_if_current(expected_generation)? {
                        let outcome = self
                            .finish(request_id, false, Some("PREEMPTED_OR_IN_FLIGHT"))
                            .await?;
                        return self.project(outcome.snapshot, Some(outcome.receipt)).await;
                    }
                    let mut close_if_cancelled = CloseGateOnDrop::new(gate.clone());
                    match self.finish(request_id, true, None).await {
                        Ok(outcome) => {
                            close_if_cancelled.disarm();
                            self.project(outcome.snapshot, Some(outcome.receipt)).await
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    self.reject_unsent_live_work().await?;
                    gate.drain().await;
                    match self.finish(request_id, true, None).await {
                        Ok(outcome) => self.project(outcome.snapshot, Some(outcome.receipt)).await,
                        Err(error) => Err(error),
                    }
                }
            }
        }
    }

    async fn begin_unavailable(
        &self,
        request: LiveOrderControlRequest,
    ) -> Result<LiveOrderControlResponse, LabError> {
        let request_id = request
            .request_id
            .as_deref()
            .ok_or_else(|| LabError::InvalidConfig("missing request_id".into()))?;
        let expected_sequence = request
            .expected_sequence
            .ok_or_else(|| LabError::InvalidConfig("missing expected_sequence".into()))?;
        let expected_epoch = request
            .expected_epoch
            .as_deref()
            .ok_or_else(|| LabError::InvalidConfig("missing expected_epoch".into()))?;
        let action = request.action;
        let request_id = request_id.to_owned();
        let expected_epoch = expected_epoch.to_owned();
        let observed_at = chrono::Utc::now().to_rfc3339();
        let denied_code = "CONTROL_UNAVAILABLE";
        let outcome = self
            .database
            .call("record_unavailable_live_order_control", move |store| {
                store.live_order_control_begin(
                    action,
                    &request_id,
                    expected_sequence,
                    &expected_epoch,
                    Some(denied_code),
                    false,
                    false,
                    &observed_at,
                )
            })
            .await?;
        match outcome {
            LiveOrderControlBegin::Complete(outcome) => {
                self.project(outcome.snapshot, Some(outcome.receipt)).await
            }
            LiveOrderControlBegin::Pending(_) | LiveOrderControlBegin::ExistingPending(_) => {
                Err(LabError::OutcomeUnknown(
                    "unavailable control unexpectedly created a pending mutation".into(),
                ))
            }
        }
    }

    async fn reject_unsent_live_work(&self) -> Result<(), LabError> {
        loop {
            let now = chrono::Utc::now().to_rfc3339();
            let rejected = self
                .database
                .call(
                    "reject_unsent_orders_for_live_order_control",
                    move |store| {
                        store.reject_unsent_live_work_for_live_order_control(
                            "LIVE_ORDER_SENDING_DISABLED",
                            &now,
                            UNSENT_REJECTION_BATCH,
                        )
                    },
                )
                .await?;
            if rejected < UNSENT_REJECTION_BATCH {
                return Ok(());
            }
        }
    }

    async fn finish(
        &self,
        request_id: &str,
        acknowledged: bool,
        denied_code: Option<&'static str>,
    ) -> Result<PersistedLiveOrderControlOutcome, LabError> {
        let request_id = request_id.to_owned();
        let observed_at = chrono::Utc::now().to_rfc3339();
        self.database
            .call("finish_live_order_control", move |store| {
                store.live_order_control_finish(
                    &request_id,
                    acknowledged,
                    denied_code,
                    &observed_at,
                )
            })
            .await
    }

    async fn status(
        &self,
        receipt: Option<LiveOrderControlReceipt>,
    ) -> Result<LiveOrderControlResponse, LabError> {
        if let Some(gate) = &self.gate {
            let _transition = gate.lock_transition().await;
            self.read_status_locked(receipt).await
        } else {
            self.read_status_locked(receipt).await
        }
    }

    async fn read_status_locked(
        &self,
        receipt: Option<LiveOrderControlReceipt>,
    ) -> Result<LiveOrderControlResponse, LabError> {
        let (control, trading) = self
            .database
            .call("read_live_order_control_status", |store| {
                Ok((
                    store.live_order_control_snapshot()?,
                    store.trading_control_snapshot()?,
                ))
            })
            .await?;
        let enabled = self.gate.as_ref().map_or_else(
            || self.policy.is_enabled(),
            |gate| gate.is_enabled() && control.live_trading_enabled,
        );
        Ok(LiveOrderControlResponse {
            boot_epoch: control.boot_epoch,
            live_order_control_available: self.gate.is_some() && control.available,
            live_trading_enabled: enabled,
            sequence: control.sequence,
            observed_at_utc: chrono::Utc::now().to_rfc3339(),
            exchange_read_mode: self.exchange_read_mode,
            order_admission_blocked: trading.order_admission_blocked,
            emergency_latched: trading.emergency_latched,
            confirmation_required: self.gate.is_some(),
            receipt,
        })
    }

    async fn read_request(&self, request_id: &str) -> Result<LiveOrderControlResponse, LabError> {
        if let Some(gate) = &self.gate {
            let _transition = gate.lock_transition().await;
            self.read_request_locked(request_id).await
        } else {
            self.read_request_locked(request_id).await
        }
    }

    async fn read_request_locked(
        &self,
        request_id: &str,
    ) -> Result<LiveOrderControlResponse, LabError> {
        let request_id_owned = request_id.to_owned();
        let result = self
            .database
            .call("read_live_order_control_request", move |store| {
                store.live_order_control_request(&request_id_owned)
            })
            .await?;
        let Some(outcome) = result else {
            return Err(LabError::InvalidConfig(format!(
                "unknown live-order control request {request_id}"
            )));
        };
        self.project(outcome.snapshot, Some(outcome.receipt)).await
    }

    async fn project(
        &self,
        snapshot: PersistedLiveOrderControlSnapshot,
        receipt: Option<LiveOrderControlReceipt>,
    ) -> Result<LiveOrderControlResponse, LabError> {
        let trading = self
            .database
            .call("read_live_order_control_latches", |store| {
                store.trading_control_snapshot()
            })
            .await?;
        let enabled = self.gate.as_ref().map_or_else(
            || self.policy.is_enabled(),
            |gate| gate.is_enabled() && snapshot.live_trading_enabled,
        );
        Ok(LiveOrderControlResponse {
            boot_epoch: snapshot.boot_epoch,
            live_order_control_available: self.gate.is_some() && snapshot.available,
            live_trading_enabled: enabled,
            sequence: snapshot.sequence,
            observed_at_utc: chrono::Utc::now().to_rfc3339(),
            exchange_read_mode: self.exchange_read_mode,
            order_admission_blocked: trading.order_admission_blocked,
            emergency_latched: trading.emergency_latched,
            confirmation_required: self.gate.is_some(),
            receipt,
        })
    }
}

fn validate_request(request: &LiveOrderControlRequest) -> Result<(), LabError> {
    match request.action {
        LiveOrderControlAction::Status => {
            if request.request_id.is_some()
                || request.expected_sequence.is_some()
                || request.expected_epoch.is_some()
                || request.confirmation.is_some()
            {
                return Err(LabError::InvalidConfig(
                    "status accepts no mutation identity or confirmation fields".into(),
                ));
            }
        }
        LiveOrderControlAction::Request => {
            validate_request_id(request.request_id.as_deref())?;
            if request.expected_sequence.is_some()
                || request.expected_epoch.is_some()
                || request.confirmation.is_some()
            {
                return Err(LabError::InvalidConfig(
                    "request accepts only request_id".into(),
                ));
            }
        }
        LiveOrderControlAction::Enable | LiveOrderControlAction::Disable => {
            validate_request_id(request.request_id.as_deref())?;
            if request.expected_sequence.is_none()
                || request.expected_epoch.as_deref().is_none_or(str::is_empty)
                || request
                    .expected_epoch
                    .as_ref()
                    .is_some_and(|epoch| epoch.len() > 128)
            {
                return Err(LabError::InvalidConfig(
                    "mutations require request_id, expected_sequence, and expected_epoch".into(),
                ));
            }
            if request
                .expected_sequence
                .is_some_and(|sequence| i64::try_from(sequence).is_err())
            {
                return Err(LabError::InvalidConfig(
                    "expected_sequence exceeds SQLite integer capacity".into(),
                ));
            }
            if request.action == LiveOrderControlAction::Disable && request.confirmation.is_some() {
                return Err(LabError::InvalidConfig(
                    "disable does not accept confirmation".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_request_id(request_id: Option<&str>) -> Result<(), LabError> {
    let Some(request_id) = request_id else {
        return Err(LabError::InvalidConfig("missing request_id".into()));
    };
    if request_id.is_empty()
        || request_id.len() > MAX_REQUEST_ID_BYTES
        || request_id.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(LabError::InvalidConfig(
            "request_id must contain 1..128 non-control bytes".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseOwner;
    use crate::upbit::control::ExchangeReadMode;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn temporary_root() -> PathBuf {
        static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);
        std::env::temp_dir().join(format!(
            "sky-backcraft-live-order-control-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn request(
        action: LiveOrderControlAction,
        request_id: Option<&str>,
        expected_sequence: Option<u64>,
        expected_epoch: Option<&str>,
        confirmation: Option<&str>,
    ) -> LiveOrderControlRequest {
        LiveOrderControlRequest {
            action,
            request_id: request_id.map(str::to_owned),
            expected_sequence,
            expected_epoch: expected_epoch.map(str::to_owned),
            confirmation: confirmation.map(str::to_owned),
        }
    }

    #[test]
    fn shared_gate_rejects_pre_disable_generations_after_reenable() -> TestResult {
        let gate = LiveOrderSendGate::new_disabled();
        let clone = gate.clone();
        assert!(gate.open_if_current(0)?);
        let admitted_generation = clone.generation();
        assert!(clone.try_lease(Some(admitted_generation)).is_some());
        gate.close()?;
        assert!(gate.open_if_current(gate.generation())?);
        assert!(clone.try_lease(Some(admitted_generation)).is_none());
        assert!(clone.try_lease(Some(gate.generation())).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn disable_drain_waits_for_admitted_send_lease() -> TestResult {
        let gate = LiveOrderSendGate::new_disabled();
        assert!(gate.open_if_current(0)?);
        let lease = gate
            .try_lease(Some(gate.generation()))
            .ok_or("enabled gate did not admit a send lease")?;
        gate.close()?;
        let drain_gate = gate.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let drain = tokio::spawn(async move {
            let _sent = started_tx.send(());
            drain_gate.drain().await;
        });
        started_rx.await?;
        assert!(!drain.is_finished());
        drop(lease);
        drain.await?;
        Ok(())
    }

    #[test]
    fn uncommitted_enable_guard_closes_on_drop() -> TestResult {
        let gate = LiveOrderSendGate::new_disabled();
        assert!(gate.open_if_current(0)?);
        drop(CloseGateOnDrop::new(gate.clone()));
        assert!(!gate.is_enabled());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one real DatabaseOwner lifecycle proves CAS, request replay and restart fencing together"
    )]
    async fn control_is_confirmed_cas_idempotent_and_restart_off() -> TestResult {
        let root = temporary_root();
        let owner = DatabaseOwner::open_with_live_order_control(root.clone(), true)?;
        let gate = LiveOrderSendGate::new_disabled();
        let service = LiveOrderControlService::new(
            owner.handle(),
            LiveExecutionPolicy::OperatorEnabled,
            Some(gate.clone()),
            ExchangeReadMode::Mock,
        );
        let initial = service
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        assert!(initial.live_order_control_available);
        assert!(!initial.live_trading_enabled);
        assert_eq!(initial.exchange_read_mode, ExchangeReadMode::Mock);

        let missing_confirmation = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("missing-confirmation"),
                Some(initial.sequence),
                Some(&initial.boot_epoch),
                None,
            ))
            .await?;
        let denied = missing_confirmation
            .receipt
            .as_ref()
            .ok_or("missing confirmation returned no durable receipt")?;
        assert_eq!(denied.outcome, LiveOrderControlOutcome::Denied);
        assert_eq!(denied.denied_code.as_deref(), Some("CONFIRMATION_REQUIRED"));
        assert!(!missing_confirmation.live_trading_enabled);

        let enabled = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("enable-once"),
                Some(initial.sequence),
                Some(&initial.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        assert!(enabled.live_trading_enabled);
        assert_eq!(enabled.sequence, initial.sequence + 1);
        assert_eq!(
            enabled.receipt.as_ref().map(|receipt| receipt.outcome),
            Some(LiveOrderControlOutcome::Acknowledged)
        );

        let disabled = service
            .execute(request(
                LiveOrderControlAction::Disable,
                Some("disable-once"),
                Some(enabled.sequence),
                Some(&enabled.boot_epoch),
                None,
            ))
            .await?;
        assert!(!disabled.live_trading_enabled);
        assert_eq!(disabled.sequence, enabled.sequence + 1);

        let enabled_again = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("enable-twice"),
                Some(disabled.sequence),
                Some(&disabled.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        assert!(enabled_again.live_trading_enabled);

        let replayed_disable = service
            .execute(request(
                LiveOrderControlAction::Disable,
                Some("disable-once"),
                Some(enabled.sequence),
                Some(&enabled.boot_epoch),
                None,
            ))
            .await?;
        assert!(!replayed_disable.live_trading_enabled);
        assert_eq!(replayed_disable.sequence, enabled_again.sequence + 1);
        assert_eq!(
            replayed_disable
                .receipt
                .as_ref()
                .map(|receipt| receipt.sequence),
            Some(disabled.sequence)
        );

        let replayed_enable = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("enable-once"),
                Some(initial.sequence),
                Some(&initial.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        assert!(!replayed_enable.live_trading_enabled);
        assert_eq!(replayed_enable.sequence, replayed_disable.sequence);
        assert_eq!(
            replayed_enable
                .receipt
                .as_ref()
                .map(|receipt| receipt.sequence),
            Some(enabled.sequence)
        );

        let request_readback = service
            .execute(request(
                LiveOrderControlAction::Request,
                Some("enable-once"),
                None,
                None,
                None,
            ))
            .await?;
        assert!(!request_readback.live_trading_enabled);
        assert_eq!(
            request_readback
                .receipt
                .as_ref()
                .map(|receipt| receipt.outcome),
            Some(LiveOrderControlOutcome::Acknowledged)
        );

        let enabled_third = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("enable-third"),
                Some(replayed_disable.sequence),
                Some(&replayed_disable.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        let conflict_disable = service
            .execute(request(
                LiveOrderControlAction::Disable,
                Some("disable-once"),
                Some(0),
                Some(&initial.boot_epoch),
                None,
            ))
            .await;
        assert!(matches!(conflict_disable, Err(LabError::Conflict(_))));
        let status_after_conflict = service
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        assert!(!status_after_conflict.live_trading_enabled);
        assert_eq!(status_after_conflict.sequence, enabled_third.sequence + 1);
        let conflict_request_id = "disable-once".to_owned();
        let closure = owner
            .handle()
            .call("read_live_order_control_conflict_closure", move |store| {
                store.live_order_control_fail_safe_closure(
                    &conflict_request_id,
                    "IDENTITY_CONFLICT_OFF",
                )
            })
            .await?
            .ok_or("conflicting OFF replay did not persist its fail-safe closure")?;
        assert_eq!(closure.0, status_after_conflict.boot_epoch);
        assert_eq!(closure.1, status_after_conflict.sequence);
        let original_disable_receipt = service
            .execute(request(
                LiveOrderControlAction::Request,
                Some("disable-once"),
                None,
                None,
                None,
            ))
            .await?;
        assert_eq!(
            original_disable_receipt
                .receipt
                .as_ref()
                .map(|receipt| receipt.sequence),
            Some(disabled.sequence)
        );
        let before_restart = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("on-before-restart"),
                Some(status_after_conflict.sequence),
                Some(&status_after_conflict.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        assert!(before_restart.live_trading_enabled);
        drop(service);
        owner.shutdown()?;

        let reopened_owner = DatabaseOwner::open_with_live_order_control(root.clone(), true)?;
        let reopened_gate = LiveOrderSendGate::new_disabled();
        let reopened = LiveOrderControlService::new(
            reopened_owner.handle(),
            LiveExecutionPolicy::OperatorEnabled,
            Some(reopened_gate),
            ExchangeReadMode::Mock,
        );
        let restarted = reopened
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        assert_ne!(restarted.boot_epoch, initial.boot_epoch);
        assert!(!restarted.live_trading_enabled);
        assert_eq!(restarted.sequence, 0);
        let old_receipt = reopened
            .execute(request(
                LiveOrderControlAction::Request,
                Some("enable-once"),
                None,
                None,
                None,
            ))
            .await?;
        assert_eq!(old_receipt.boot_epoch, restarted.boot_epoch);
        assert_eq!(
            old_receipt
                .receipt
                .as_ref()
                .map(|receipt| receipt.boot_epoch.as_str()),
            Some(initial.boot_epoch.as_str())
        );
        assert!(!old_receipt.live_trading_enabled);
        drop(reopened);
        reopened_owner.shutdown()?;
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
    #[tokio::test]
    async fn replayed_pending_disable_remains_unknown_and_audits_a_new_safe_off() -> TestResult {
        let root = temporary_root();
        let owner = DatabaseOwner::open_with_live_order_control(root.clone(), true)?;
        let gate = LiveOrderSendGate::new_disabled();
        let service = LiveOrderControlService::new(
            owner.handle(),
            LiveExecutionPolicy::OperatorEnabled,
            Some(gate.clone()),
            ExchangeReadMode::Mock,
        );
        let initial = service
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        let on = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("pending-setup-on"),
                Some(initial.sequence),
                Some(&initial.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        gate.close()?;
        let epoch = on.boot_epoch.clone();
        let old_sequence = on.sequence;
        owner
            .handle()
            .call("seed_interrupted_disable_fixture", move |store| {
                store.live_order_control_begin(
                    LiveOrderControlAction::Disable,
                    "pending-old-off",
                    old_sequence,
                    &epoch,
                    None,
                    true,
                    false,
                    &chrono::Utc::now().to_rfc3339(),
                )
            })
            .await?;
        let off = service
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        let enabled_again = service
            .execute(request(
                LiveOrderControlAction::Enable,
                Some("pending-setup-new-on"),
                Some(off.sequence),
                Some(&off.boot_epoch),
                Some(ENABLE_CONFIRMATION),
            ))
            .await?;
        assert!(enabled_again.live_trading_enabled);
        let replay = service
            .execute(request(
                LiveOrderControlAction::Disable,
                Some("pending-old-off"),
                Some(old_sequence),
                Some(&on.boot_epoch),
                None,
            ))
            .await;
        assert!(
            matches!(replay, Err(LabError::OutcomeUnknown(_))),
            "pending receipt must not become a fabricated ACK"
        );
        let current = service
            .execute(request(
                LiveOrderControlAction::Status,
                None,
                None,
                None,
                None,
            ))
            .await?;
        assert!(!current.live_trading_enabled);
        assert!(current.sequence > enabled_again.sequence);
        let closure = owner
            .handle()
            .call("read_pending_off_audit", |store| {
                store.live_order_control_fail_safe_closure("pending-old-off", "REPLAYED_OFF")
            })
            .await?;
        assert_eq!(closure, Some((current.boot_epoch, current.sequence)));
        drop(service);
        owner.shutdown()?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
