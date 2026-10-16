//! Durable server-side live trading control state.
//!
//! The production policy is fail-closed: live trading remains disabled at compile
//! time. Control commands only change local order-admission state and never submit,
//! cancel, or liquidate exchange orders.

use crate::contracts::LabError;
use crate::database::DatabaseHandle;
use crate::upbit::execution::LiveExecutionPolicy;
use crate::upbit::live_order_control::LiveOrderSendGate;
use crate::upbit::risk::{RiskGuard, SafetyLatchToken};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_APPROVAL_TOKEN_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TradingControlAction {
    Status,
    Start,
    Resume,
    Pause,
    EmergencyStop,
}

impl TradingControlAction {
    #[must_use]
    pub const fn is_mutation(self) -> bool {
        !matches!(self, Self::Status)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TradingControlRequest {
    pub action: TradingControlAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_token: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeReadMode {
    LiveRead,
    Mock,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradingControlState {
    DisabledByConfig,
    Standby,
    Running,
    Paused,
    EmergencyStopActive,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradingControlOutcome {
    Acknowledged,
    Denied,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradingControlDeniedCode {
    LiveTradingDisabled,
    ApprovalRequired,
    InvalidState,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
pub struct TradingControlReceipt {
    pub request_id: String,
    pub action: TradingControlAction,
    pub outcome: TradingControlOutcome,
    pub sequence: u64,
    pub observed_at_utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_code: Option<TradingControlDeniedCode>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Eq, PartialEq)]
pub struct TradingControlStatus {
    pub exchange_read_mode: ExchangeReadMode,
    pub live_trading_enabled: bool,
    pub state: TradingControlState,
    pub sequence: u64,
    pub observed_at_utc: String,
    pub order_admission_blocked: bool,
    pub emergency_latched: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denied_code: Option<TradingControlDeniedCode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<TradingControlReceipt>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub(crate) struct PersistedControlSnapshot {
    pub state: TradingControlState,
    pub sequence: u64,
    pub observed_at_utc: String,
    pub order_admission_blocked: bool,
    pub emergency_latched: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub(crate) struct PersistedControlOutcome {
    pub snapshot: PersistedControlSnapshot,
    pub receipt: TradingControlReceipt,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ControlDecisionContext {
    pub live_trading_enabled: bool,
    pub approval_valid: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ControlDecision {
    pub state: TradingControlState,
    pub order_admission_blocked: bool,
    pub emergency_latched: bool,
    pub outcome: TradingControlOutcome,
    pub denied_code: Option<TradingControlDeniedCode>,
}

#[must_use]
pub(crate) fn decide_transition(
    action: TradingControlAction,
    current: &PersistedControlSnapshot,
    context: ControlDecisionContext,
) -> ControlDecision {
    if matches!(action, TradingControlAction::EmergencyStop) {
        return ControlDecision {
            state: TradingControlState::EmergencyStopActive,
            order_admission_blocked: true,
            emergency_latched: true,
            outcome: TradingControlOutcome::Acknowledged,
            denied_code: None,
        };
    }
    // Pause only closes local order admission, so it remains available while
    // the separate LIVE execution gate is disabled.
    if matches!(action, TradingControlAction::Pause) {
        return if current.state == TradingControlState::Running {
            ControlDecision {
                state: TradingControlState::Paused,
                order_admission_blocked: true,
                emergency_latched: false,
                outcome: TradingControlOutcome::Acknowledged,
                denied_code: None,
            }
        } else {
            denied(current, TradingControlDeniedCode::InvalidState)
        };
    }
    if matches!(
        action,
        TradingControlAction::Start | TradingControlAction::Resume
    ) && !context.live_trading_enabled
    {
        return denied(current, TradingControlDeniedCode::LiveTradingDisabled);
    }
    if matches!(
        action,
        TradingControlAction::Start | TradingControlAction::Resume
    ) && !context.approval_valid
    {
        return denied(current, TradingControlDeniedCode::ApprovalRequired);
    }
    match (action, current.state) {
        (TradingControlAction::Start, TradingControlState::Standby)
        | (TradingControlAction::Resume, TradingControlState::Paused) => ControlDecision {
            state: TradingControlState::Running,
            order_admission_blocked: false,
            emergency_latched: false,
            outcome: TradingControlOutcome::Acknowledged,
            denied_code: None,
        },
        _ => denied(current, TradingControlDeniedCode::InvalidState),
    }
}

fn denied(current: &PersistedControlSnapshot, code: TradingControlDeniedCode) -> ControlDecision {
    ControlDecision {
        state: current.state,
        order_admission_blocked: current.order_admission_blocked,
        emergency_latched: current.emergency_latched,
        outcome: TradingControlOutcome::Denied,
        denied_code: Some(code),
    }
}

#[derive(Clone)]
pub struct TradingControlService {
    database: DatabaseHandle,
    risk_guard: RiskGuard,
    exchange_read_mode: ExchangeReadMode,
    live_execution_policy: LiveExecutionPolicy,
    live_order_send_gate: Option<LiveOrderSendGate>,
    trusted_approval_token: Option<String>,
}

#[derive(Clone, Copy)]
enum ControlApprovalSource {
    RequestToken,
    TrustedOperatorAttestation,
}

impl TradingControlService {
    #[must_use]
    pub fn production(
        database: DatabaseHandle,
        risk_guard: RiskGuard,
        exchange_read_mode: ExchangeReadMode,
    ) -> Self {
        Self::production_with_policy(
            database,
            risk_guard,
            exchange_read_mode,
            LiveExecutionPolicy::Disabled,
        )
    }

    #[must_use]
    pub fn production_with_policy(
        database: DatabaseHandle,
        risk_guard: RiskGuard,
        exchange_read_mode: ExchangeReadMode,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Self {
        Self {
            database,
            risk_guard,
            exchange_read_mode,
            live_execution_policy,
            live_order_send_gate: None,
            trusted_approval_token: None,
        }
    }

    /// Project current runtime send permission without changing lifecycle admission.
    #[must_use]
    pub fn with_live_order_send_gate(mut self, gate: LiveOrderSendGate) -> Self {
        self.live_order_send_gate = Some(gate);
        self
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn isolated_mock_enabled(
        database: DatabaseHandle,
        risk_guard: RiskGuard,
        trusted_approval_token: String,
    ) -> Self {
        Self {
            database,
            risk_guard,
            exchange_read_mode: ExchangeReadMode::Mock,
            live_execution_policy: LiveExecutionPolicy::OperatorEnabled,
            live_order_send_gate: None,
            trusted_approval_token: Some(trusted_approval_token),
        }
    }

    /// Restore the process-local fast admission latch from durable SQLite state.
    ///
    /// # Errors
    /// Returns storage admission, read, or integrity failures.
    pub async fn synchronize_from_storage(&self) -> Result<(), LabError> {
        let revision = self.risk_guard.control_revision();
        let snapshot = self
            .database
            .call("restore_trading_control_latch", |store| {
                store.trading_control_snapshot()
            })
            .await?;
        if snapshot.state == TradingControlState::Running {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_nanos());
            let pause = self
                .execute(TradingControlRequest {
                    action: TradingControlAction::Pause,
                    request_id: Some(format!(
                        "process-restart-pause-{}-{nonce}",
                        std::process::id()
                    )),
                    expected_sequence: Some(snapshot.sequence),
                    approval_token: None,
                })
                .await?;
            if pause
                .receipt
                .as_ref()
                .is_none_or(|receipt| receipt.outcome != TradingControlOutcome::Acknowledged)
            {
                return Err(LabError::BlockedEvidence(
                    "persisted live grant could not be fenced at process startup".into(),
                ));
            }
            let paused = self
                .database
                .call("verify_process_restart_control_pause", |store| {
                    store.trading_control_snapshot()
                })
                .await?;
            if paused.state == TradingControlState::Running || !paused.order_admission_blocked {
                return Err(LabError::OutcomeUnknown(
                    "process restart did not durably fence prior live admission".into(),
                ));
            }
            return Ok(());
        }
        if !self.synchronize_risk(&snapshot, revision) {
            return Err(LabError::OutcomeUnknown(
                "trading control changed while restoring the process latch".into(),
            ));
        }
        Ok(())
    }

    /// Read or mutate the durable control state.
    ///
    /// # Errors
    /// Rejects malformed mutation identities, stale sequences, storage failures,
    /// or idempotency-key reuse with a different command identity.
    pub async fn execute(
        &self,
        request: TradingControlRequest,
    ) -> Result<TradingControlStatus, LabError> {
        self.execute_with_operator_approval_source(request, ControlApprovalSource::RequestToken)
            .await
    }

    /// Apply a control command after the trusted MCP adapter has verified an
    /// authenticated native desktop request or OAuth owner consent and remote
    /// control enablement. This attestation replaces only the legacy request
    /// approval token; the immutable live policy, CAS, durable emergency latch,
    /// and risk checks still run normally.
    pub(crate) async fn execute_with_operator_approval(
        &self,
        request: TradingControlRequest,
    ) -> Result<TradingControlStatus, LabError> {
        self.execute_with_operator_approval_source(
            request,
            ControlApprovalSource::TrustedOperatorAttestation,
        )
        .await
    }

    async fn execute_with_operator_approval_source(
        &self,
        request: TradingControlRequest,
        approval_source: ControlApprovalSource,
    ) -> Result<TradingControlStatus, LabError> {
        validate_request(&request)?;
        if !request.action.is_mutation() {
            let revision = self.risk_guard.control_revision();
            let snapshot = self
                .database
                .call("trading_control_status", |store| {
                    store.trading_control_snapshot()
                })
                .await?;
            let snapshot = self
                .authoritative_snapshot(snapshot, revision, None)
                .await?;
            return Ok(self.project(&snapshot, None, None, chrono::Utc::now().to_rfc3339()));
        }

        let action = request.action;
        let request_id = request
            .request_id
            .ok_or_else(|| LabError::InvalidConfig("missing validated request_id".into()))?;
        let expected_sequence = request
            .expected_sequence
            .ok_or_else(|| LabError::InvalidConfig("missing validated expected_sequence".into()))?;
        let safety_token = match action {
            TradingControlAction::EmergencyStop => {
                Some(self.risk_guard.latch_order_admission(true))
            }
            TradingControlAction::Pause => Some(self.risk_guard.latch_order_admission(false)),
            TradingControlAction::Status
            | TradingControlAction::Start
            | TradingControlAction::Resume => None,
        };
        let risk_revision = safety_token.map_or_else(
            || self.risk_guard.control_revision(),
            SafetyLatchToken::revision,
        );
        let approval_valid = match approval_source {
            ControlApprovalSource::TrustedOperatorAttestation => true,
            ControlApprovalSource::RequestToken => self
                .trusted_approval_token
                .as_deref()
                .is_some_and(|expected| request.approval_token.as_deref() == Some(expected)),
        };
        let context = ControlDecisionContext {
            live_trading_enabled: self.live_execution_policy.is_enabled(),
            approval_valid,
        };
        let observed_at_utc = chrono::Utc::now().to_rfc3339();
        let durable_request_id = request_id.clone();
        let outcome = match self
            .database
            .call("apply_trading_control", move |store| {
                store.apply_trading_control(
                    action,
                    &request_id,
                    expected_sequence,
                    context,
                    &observed_at_utc,
                )
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let reconciled = self
                    .database
                    .call("reconcile_trading_control", move |store| {
                        store.reconcile_trading_control(
                            &durable_request_id,
                            action,
                            expected_sequence,
                        )
                    })
                    .await;
                match reconciled {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) | Err(_) => return Err(error),
                }
            }
            Err(error) => {
                self.resolve_definite_safety_error(safety_token).await;
                return Err(error);
            }
        };
        let snapshot = self
            .authoritative_snapshot(outcome.snapshot, risk_revision, safety_token)
            .await?;
        let denied_code = outcome.receipt.denied_code;
        let observed_at_utc = outcome.receipt.observed_at_utc.clone();
        Ok(self.project(
            &snapshot,
            denied_code,
            Some(outcome.receipt),
            observed_at_utc,
        ))
    }

    async fn authoritative_snapshot(
        &self,
        snapshot: PersistedControlSnapshot,
        expected_revision: u64,
        safety_token: Option<SafetyLatchToken>,
    ) -> Result<PersistedControlSnapshot, LabError> {
        // The shared process guard represents only durable user Pause/EmergencyStop.
        // LIVE-off remains enforced by the production effect gates and is exposed
        // separately in the status projection; it must not pause MOCK automation.
        let blocked = snapshot.order_admission_blocked;
        let applied = safety_token.map_or_else(
            || self.synchronize_risk(&snapshot, expected_revision),
            |token| {
                self.risk_guard
                    .resolve_safety_latch(token, blocked, snapshot.emergency_latched)
            },
        );
        if applied {
            return Ok(snapshot);
        }
        let revision = self.risk_guard.control_revision();
        let current = self
            .database
            .call("refresh_trading_control_after_race", |store| {
                store.trading_control_snapshot()
            })
            .await?;
        if self.synchronize_risk(&current, revision) {
            Ok(current)
        } else {
            Err(LabError::OutcomeUnknown(
                "trading control changed during durable readback; retry status".into(),
            ))
        }
    }

    async fn resolve_definite_safety_error(&self, safety_token: Option<SafetyLatchToken>) {
        let Some(token) = safety_token else {
            return;
        };
        let Ok(snapshot) = self
            .database
            .call("reconcile_rejected_trading_control", |store| {
                store.trading_control_snapshot()
            })
            .await
        else {
            return;
        };
        let blocked = snapshot.order_admission_blocked;
        let _resolved =
            self.risk_guard
                .resolve_safety_latch(token, blocked, snapshot.emergency_latched);
    }

    fn synchronize_risk(
        &self,
        snapshot: &PersistedControlSnapshot,
        expected_revision: u64,
    ) -> bool {
        self.risk_guard.synchronize_order_admission(
            expected_revision,
            snapshot.order_admission_blocked,
            snapshot.emergency_latched,
        )
    }

    fn project(
        &self,
        snapshot: &PersistedControlSnapshot,
        denied_code: Option<TradingControlDeniedCode>,
        receipt: Option<TradingControlReceipt>,
        observed_at_utc: String,
    ) -> TradingControlStatus {
        let state = if snapshot.emergency_latched {
            TradingControlState::EmergencyStopActive
        } else if !self.live_execution_policy.is_enabled() {
            TradingControlState::DisabledByConfig
        } else {
            snapshot.state
        };
        TradingControlStatus {
            exchange_read_mode: self.exchange_read_mode,
            live_trading_enabled: self.live_order_send_gate.as_ref().map_or_else(
                || self.live_execution_policy.is_enabled(),
                LiveOrderSendGate::is_enabled,
            ),
            state,
            sequence: snapshot.sequence,
            observed_at_utc,
            order_admission_blocked: snapshot.order_admission_blocked
                || !self.live_execution_policy.is_enabled(),
            emergency_latched: snapshot.emergency_latched,
            denied_code,
            receipt,
        }
    }
}

fn validate_request(request: &TradingControlRequest) -> Result<(), LabError> {
    if !request.action.is_mutation() {
        if request.request_id.is_some()
            || request.expected_sequence.is_some()
            || request.approval_token.is_some()
        {
            return Err(LabError::InvalidConfig(
                "STATUS does not accept mutation identity or approval fields".into(),
            ));
        }
        return Ok(());
    }
    let request_id = request.request_id.as_deref().ok_or_else(|| {
        LabError::InvalidConfig("trading control mutation requires request_id".into())
    })?;
    if request_id.is_empty()
        || request_id.len() > MAX_REQUEST_ID_BYTES
        || !request_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(LabError::InvalidConfig(
            "trading control request_id must be 1..=128 ASCII identity characters".into(),
        ));
    }
    if request.expected_sequence.is_none() {
        return Err(LabError::InvalidConfig(
            "trading control mutation requires expected_sequence".into(),
        ));
    }
    if request
        .approval_token
        .as_ref()
        .is_some_and(|token| token.is_empty() || token.len() > MAX_APPROVAL_TOKEN_BYTES)
    {
        return Err(LabError::InvalidConfig(
            "approval_token must contain 1..=512 characters when provided".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upbit::risk::RiskConfig;

    #[test]
    fn stale_status_snapshot_refreshes_after_newer_emergency_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-control-stale-status-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let risk = RiskGuard::new(RiskConfig::default());
        let service = TradingControlService::isolated_mock_enabled(
            owner.handle(),
            risk.clone(),
            "test-approval".into(),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            service.synchronize_from_storage().await?;
            let stale_revision = risk.control_revision();
            let stale_snapshot = PersistedControlSnapshot {
                state: TradingControlState::Standby,
                sequence: 0,
                observed_at_utc: "1970-01-01T00:00:00Z".into(),
                order_admission_blocked: false,
                emergency_latched: false,
            };
            let emergency = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::EmergencyStop,
                    request_id: Some("newer-emergency".into()),
                    expected_sequence: Some(0),
                    approval_token: None,
                })
                .await?;
            assert_eq!(emergency.sequence, 1);

            let refreshed = service
                .authoritative_snapshot(stale_snapshot, stale_revision, None)
                .await?;
            let response = service.project(&refreshed, None, None, chrono::Utc::now().to_rfc3339());
            assert_eq!(response.sequence, 1);
            assert_eq!(response.state, TradingControlState::EmergencyStopActive);
            assert!(response.order_admission_blocked);
            assert!(risk.is_emergency_stopped());
            Ok::<(), LabError>(())
        })?;
        owner.shutdown()?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn status_cannot_clear_emergency_preempted_before_database_enqueue()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-control-pending-safety-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let risk = RiskGuard::new(RiskConfig::default());
        let service = TradingControlService::isolated_mock_enabled(
            owner.handle(),
            risk.clone(),
            "test-approval".into(),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            service.synchronize_from_storage().await?;
            let stale_revision = risk.control_revision();
            let stale_snapshot = PersistedControlSnapshot {
                state: TradingControlState::Standby,
                sequence: 0,
                observed_at_utc: "1970-01-01T00:00:00Z".into(),
                order_admission_blocked: false,
                emergency_latched: false,
            };
            let pending = risk.latch_order_admission(true);
            let response = service
                .authoritative_snapshot(stale_snapshot, stale_revision, None)
                .await;
            assert!(matches!(response, Err(LabError::OutcomeUnknown(_))));
            assert!(risk.is_emergency_stopped());
            assert!(risk.resolve_safety_latch(pending, false, false));
            Ok::<(), LabError>(())
        })?;
        owner.shutdown()?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    async fn seed_previously_running_control(
        database: &crate::database::DatabaseHandle,
    ) -> Result<(), LabError> {
        let observed_at = chrono::Utc::now().to_rfc3339();
        database
            .call("test_restore_prior_live_running_control", move |store| {
                store
                    .apply_trading_control(
                        TradingControlAction::Start,
                        "prior-live-run-before-live-off-restart",
                        0,
                        ControlDecisionContext {
                            live_trading_enabled: true,
                            approval_valid: true,
                        },
                        &observed_at,
                    )
                    .map(|_| ())
            })
            .await?;

        Ok(())
    }

    #[test]
    fn live_disabled_status_keeps_mock_admission_open_but_pause_and_emergency_still_block()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-control-live-off-mock-admission-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let database = owner.handle();
        let risk = RiskGuard::new(RiskConfig::default());
        let service = TradingControlService::production(
            database.clone(),
            risk.clone(),
            ExchangeReadMode::Mock,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let status = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::Status,
                    request_id: None,
                    expected_sequence: None,
                    approval_token: None,
                })
                .await?;
            assert!(!status.live_trading_enabled);
            assert_eq!(status.state, TradingControlState::DisabledByConfig);
            assert!(status.order_admission_blocked);
            assert!(risk.with_order_admission(|| Ok(())).is_ok());

            let start = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::Start,
                    request_id: Some("live-off-start-denied".into()),
                    expected_sequence: Some(0),
                    approval_token: None,
                })
                .await?;
            assert_eq!(
                start.denied_code,
                Some(TradingControlDeniedCode::LiveTradingDisabled)
            );
            assert!(risk.with_order_admission(|| Ok(())).is_ok());

            seed_previously_running_control(&database).await?;

            let paused = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::Pause,
                    request_id: Some("live-off-pause-remains-available".into()),
                    expected_sequence: Some(1),
                    approval_token: None,
                })
                .await?;
            assert_eq!(paused.state, TradingControlState::DisabledByConfig);
            assert_eq!(
                paused.receipt.as_ref().map(|receipt| receipt.outcome),
                Some(TradingControlOutcome::Acknowledged)
            );
            assert!(paused.order_admission_blocked);
            assert!(risk.with_order_admission(|| Ok(())).is_err());

            let resumed = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::Resume,
                    request_id: Some("live-off-resume-denied".into()),
                    expected_sequence: Some(2),
                    approval_token: Some("unused-while-live-off".into()),
                })
                .await?;
            assert_eq!(
                resumed.denied_code,
                Some(TradingControlDeniedCode::LiveTradingDisabled)
            );
            assert!(risk.with_order_admission(|| Ok(())).is_err());

            let emergency = service
                .execute(TradingControlRequest {
                    action: TradingControlAction::EmergencyStop,
                    request_id: Some("live-off-emergency-remains-available".into()),
                    expected_sequence: Some(2),
                    approval_token: None,
                })
                .await?;
            assert!(emergency.emergency_latched);
            assert!(risk.is_emergency_stopped());
            assert!(risk.with_order_admission(|| Ok(())).is_err());
            Ok::<(), LabError>(())
        })?;
        drop(database);
        owner.shutdown()?;
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn restart_fences_persisted_live_grant_until_trusted_control_resume()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "spot-lab-control-restart-fence-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root)?;
        let owner = crate::database::DatabaseOwner::open(root.clone())?;
        let database = owner.handle();
        let before_restart = TradingControlService::production_with_policy(
            database.clone(),
            RiskGuard::new(RiskConfig::default()),
            ExchangeReadMode::LiveRead,
            LiveExecutionPolicy::OperatorEnabled,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let started = before_restart
                .execute_with_operator_approval(TradingControlRequest {
                    action: TradingControlAction::Start,
                    request_id: Some("restart-fence-before".into()),
                    expected_sequence: Some(0),
                    approval_token: None,
                })
                .await?;
            assert_eq!(started.state, TradingControlState::Running);
            assert!(!started.order_admission_blocked);

            let after_restart = TradingControlService::production_with_policy(
                database.clone(),
                RiskGuard::new(RiskConfig::default()),
                ExchangeReadMode::LiveRead,
                LiveExecutionPolicy::OperatorEnabled,
            );
            after_restart.synchronize_from_storage().await?;
            let paused = after_restart
                .execute(TradingControlRequest {
                    action: TradingControlAction::Status,
                    request_id: None,
                    expected_sequence: None,
                    approval_token: None,
                })
                .await?;
            assert_eq!(paused.state, TradingControlState::Paused);
            assert!(paused.order_admission_blocked);
            assert_eq!(paused.sequence, 2);

            let untrusted = after_restart
                .execute(TradingControlRequest {
                    action: TradingControlAction::Resume,
                    request_id: Some("restart-fence-untrusted-resume".into()),
                    expected_sequence: Some(paused.sequence),
                    approval_token: None,
                })
                .await?;
            assert_eq!(
                untrusted.denied_code,
                Some(TradingControlDeniedCode::ApprovalRequired)
            );
            assert_eq!(untrusted.sequence, paused.sequence);

            let resumed = after_restart
                .execute_with_operator_approval(TradingControlRequest {
                    action: TradingControlAction::Resume,
                    request_id: Some("restart-fence-trusted-resume".into()),
                    expected_sequence: Some(paused.sequence),
                    approval_token: None,
                })
                .await?;
            assert_eq!(resumed.state, TradingControlState::Running);
            assert!(!resumed.order_admission_blocked);
            Ok::<(), LabError>(())
        })?;
        owner.shutdown()?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
