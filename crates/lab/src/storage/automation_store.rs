//! Durable strategy-automation lifecycle, risk reservations, and venue ledger.

use super::{Store, json_error, sql_error};
use crate::automation::{
    AutomationCancelPreparation, AutomationDueSessionCursor, AutomationDueSessionPage,
    AutomationOrderAdmission, AutomationOrderDispatch, AutomationPendingCancelRequest,
    AutomationPendingIntent, AutomationPendingLifecycleRequest, AutomationPendingOrder,
    AutomationRuntimeRecord, VerifiedMarketRules, validate_order_update,
};
use crate::contracts::{
    AssetQuantity, AutomationAccountDayBaselineInput, AutomationAccountDayBaselineStatus,
    AutomationAccountDayBaselineView, AutomationAction, AutomationActivationReferenceSource,
    AutomationActivationValuationState, AutomationAssetValuationBasis, AutomationBalanceProjection,
    AutomationBaselineAssetEntry, AutomationBaselineExchangeEvidence, AutomationBaselineId,
    AutomationBaselineSource, AutomationBaselineVerificationState, AutomationCapabilities,
    AutomationCommandOutcome, AutomationDataSource, AutomationEffectiveLossProjection,
    AutomationEvent, AutomationEventKind, AutomationEventPage, AutomationFeedProjection,
    AutomationFillView, AutomationFreshness, AutomationIntentId, AutomationLedgerPage,
    AutomationLegacyMockBaselineView, AutomationLiveActivationReferenceView,
    AutomationLossReferenceBasis, AutomationMockAssumptions, AutomationOrderIdentifier,
    AutomationOrderKind, AutomationOrderRequest, AutomationOrderState, AutomationOrderView,
    AutomationRiskPolicy, AutomationRiskUsageProjection, AutomationSessionCommandResponse,
    AutomationSessionMode, AutomationSessionPage, AutomationSessionQuery, AutomationSessionReceipt,
    AutomationSessionRequestReadback, AutomationSessionState, AutomationSessionView,
    AutomationSessionWrite, CandleObservation, ContentHash, FrozenPolicyRevision, LabError,
    MarketId, ObservationId, PolicyRevisionRef, QuoteAmount, RequestId, UtcTimestamp,
    observation_digest, parse_day_start_utc, validate_frozen_policy,
};
use crate::upbit::client::{UpbitAccountBalance, UpbitOrderChance, UpbitOrderDetail};
use crate::upbit::execution::LiveExecutionPolicy;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

const MAX_AUTOMATION_SESSIONS: u32 = 8;
const MAX_AUTOMATION_PAGE: u32 = 100;

struct AutomationAdmissionSessionRow {
    account_identity: String,
    mode: String,
    state: String,
    market: String,
    risk_json: String,
    last_finalized_candle_at: Option<String>,
}

struct AutomationAdmissionContext {
    account_identity: String,
    mode: AutomationSessionMode,
    state: AutomationSessionState,
    market: MarketId,
    risk: AutomationRiskPolicy,
    last_finalized_candle_at: Option<UtcTimestamp>,
}

struct AutomationAdmissionTerms {
    request_digest: ContentHash,
    side: crate::contracts::AutomationSide,
    expected_asset_currency: String,
}

struct AutomationAccountReservationFacts {
    reserved_bid_exposure: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
enum StoredLiveActivationValuation {
    PendingMarks {
        reason: String,
    },
    Verified {
        assets: Vec<LiveActivationAssetValuation>,
        total_equity_krw: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveActivationAssetValuation {
    currency: String,
    quantity: String,
    mark_basis: LiveActivationMarkBasis,
    mark_price_krw: Option<String>,
    mark_close_at_utc: Option<UtcTimestamp>,
    mark_observation_id: Option<ObservationId>,
    mark_content_digest: Option<ContentHash>,
    value_krw: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum LiveActivationMarkBasis {
    QuoteCurrencyParity,
    FinalizedCandleClose,
}

#[derive(Debug, Clone)]
struct StoredLiveActivationReference {
    view: AutomationLiveActivationReferenceView,
    balances: Vec<UpbitAccountBalance>,
    valuation: StoredLiveActivationValuation,
}

impl Store {
    /// Persist a valid command identity before policy resolution or any external effect.
    /// A missing readback later is explicitly `NOT_RECORDED`, never permission to retry.
    pub(crate) fn automation_begin_request(
        &mut self,
        write: &AutomationSessionWrite,
        account_identity: Option<&str>,
        now: UtcTimestamp,
    ) -> Result<AutomationSessionRequestReadback, LabError> {
        let (request_id, action) = request_action(write);
        let identity = if matches!(write, AutomationSessionWrite::Start { .. }) {
            let mode = match write {
                AutomationSessionWrite::Start { mode, .. } => *mode,
                _ => return Err(LabError::Internal("start mode disappeared".into())),
            };
            let identity = account_identity.ok_or_else(|| {
                LabError::InvalidConfig(
                    "automation start requires a backend-owned account scope".into(),
                )
            })?;
            validate_account_identity(mode, identity)?;
            Some(identity)
        } else {
            None
        };
        let digest = request_digest(write, identity)?;
        if let Some((existing_digest, existing_identity)) = self
            .connection
            .query_row(
                "SELECT payload_digest,account_identity FROM automation_session_requests WHERE request_id=?1",
                [request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
        {
            if existing_digest != digest.as_str()
                || identity.is_some_and(|expected| existing_identity.as_deref() != Some(expected))
            {
                return Err(LabError::Conflict(format!(
                    "automation request_id {} was reused with a different payload or account",
                    request_id.as_str()
                )));
            }
            return self.automation_readback_request(request_id);
        }

        let (session_id, known_account) = lifecycle_session_and_account(&self.connection, write)?;
        let account_identity = identity.or(known_account.as_deref());
        let request_json = serde_json::to_string(write).map_err(json_error)?;
        self.connection
            .execute(
                "INSERT INTO automation_session_requests(\
                     request_id,session_id,account_identity,action,payload_digest,request_json,state,created_at_utc\
                 ) VALUES (?1,?2,?3,?4,?5,?6,'PENDING',?7)",
                params![
                    request_id.as_str(),
                    session_id
                        .as_ref()
                        .map(crate::contracts::AutomationSessionId::as_str),
                    account_identity,
                    action_name(action),
                    digest.as_str(),
                    request_json,
                    now.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        Ok(AutomationSessionRequestReadback {
            request_id: request_id.clone(),
            state: crate::contracts::AutomationRequestReadbackState::Pending,
            response: None,
            reason: Some("command identity is durably admitted".into()),
        })
    }

    pub(crate) fn automation_reject_request(
        &mut self,
        request_id: &RequestId,
        digest: &ContentHash,
        reason: &str,
    ) -> Result<(), LabError> {
        if reason.is_empty() || reason.len() > 2_048 {
            return Err(LabError::InvalidConfig(
                "automation rejection reason exceeds bounds".into(),
            ));
        }
        let changed = self
            .connection
            .execute(
                "UPDATE automation_session_requests SET state='REJECTED',reason=?1 \
                 WHERE request_id=?2 AND payload_digest=?3 AND state='PENDING'",
                params![reason, request_id.as_str(), digest.as_str()],
            )
            .map_err(sql_error)?;
        if changed == 0 {
            let readback = self.automation_readback_request(request_id)?;
            if readback.state == crate::contracts::AutomationRequestReadbackState::Rejected {
                return Ok(());
            }
            return Err(LabError::OutcomeUnknown(
                "automation request rejection was not durably confirmed".into(),
            ));
        }
        Ok(())
    }

    /// Apply a durable automation lifecycle request with payload idempotency and sequence CAS.
    ///
    /// For `Start`, the immutable revision is resolved before this call and must match the
    /// request reference exactly. This compatibility entry point uses the safe default
    /// execution policy; the process owner supplies its immutable policy explicitly.
    ///
    /// # Errors
    /// Rejects invalid limits, policy identity mismatch, stale sequence, or conflicting
    /// reuse of a request id.
    #[allow(clippy::needless_pass_by_value)]
    // The owner command receives the selected account scope by value, then borrows it only while applying this request.
    #[cfg(test)]
    pub(crate) fn automation_write(
        &mut self,
        write: &AutomationSessionWrite,
        frozen_policy: Option<FrozenPolicyRevision>,
        account_identity: Option<String>,
        now: UtcTimestamp,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        self.automation_write_with_policy(
            write,
            frozen_policy,
            account_identity,
            now,
            LiveExecutionPolicy::Disabled,
        )
    }

    #[allow(clippy::needless_pass_by_value)] // The owner command transfers the selected account scope and immutable policy into this synchronous transaction.
    pub(crate) fn automation_write_with_policy(
        &mut self,
        write: &AutomationSessionWrite,
        frozen_policy: Option<FrozenPolicyRevision>,
        account_identity: Option<String>,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let (request_id, _) = request_action(write);
        let account_scope = if matches!(write, AutomationSessionWrite::Start { .. }) {
            account_identity.as_deref()
        } else {
            None
        };
        let initial = self.automation_begin_request(write, account_scope, now)?;
        match initial.state {
            crate::contracts::AutomationRequestReadbackState::Applied => {
                let mut response = initial.response.ok_or_else(|| {
                    LabError::DataCorrupt("applied automation request omitted its response".into())
                })?;
                response.receipt.outcome = AutomationCommandOutcome::Replayed;
                return Ok(response);
            }
            crate::contracts::AutomationRequestReadbackState::Rejected => {
                return Err(LabError::Conflict(initial.reason.unwrap_or_else(|| {
                    "automation command was previously rejected".into()
                })));
            }
            crate::contracts::AutomationRequestReadbackState::Pending
            | crate::contracts::AutomationRequestReadbackState::NotRecorded => {}
        }
        let request_digest = request_digest(write, account_scope)?;
        let result = self.automation_apply_write(
            write,
            frozen_policy,
            account_identity.as_deref(),
            &request_digest,
            now,
            live_execution_policy,
        );
        if let Err(error) = &result
            && is_terminal_automation_rejection(error)
        {
            self.automation_reject_request(request_id, &request_digest, &error.to_string())?;
        }
        result
    }

    fn automation_apply_write(
        &mut self,
        write: &AutomationSessionWrite,
        frozen_policy: Option<FrozenPolicyRevision>,
        account_identity: Option<&str>,
        request_digest: &ContentHash,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let (_, action) = request_action(write);
        match write {
            AutomationSessionWrite::Start {
                request_id,
                policy_ref,
                market,
                decision_interval,
                mode,
                risk,
            } => self.automation_start(
                request_id,
                action,
                request_digest,
                policy_ref,
                market,
                *decision_interval,
                *mode,
                risk,
                frozen_policy,
                account_identity,
                now,
                live_execution_policy,
            ),
            AutomationSessionWrite::Pause {
                request_id,
                session_id,
                expected_sequence,
            } => self.automation_transition(
                request_id,
                action,
                request_digest,
                session_id,
                *expected_sequence,
                AutomationSessionState::Paused,
                AutomationEventKind::Paused,
                now,
                live_execution_policy,
            ),
            AutomationSessionWrite::Resume {
                request_id,
                session_id,
                expected_sequence,
            } => self.automation_transition(
                request_id,
                action,
                request_digest,
                session_id,
                *expected_sequence,
                AutomationSessionState::Running,
                AutomationEventKind::Resumed,
                now,
                live_execution_policy,
            ),
            AutomationSessionWrite::Stop {
                request_id,
                session_id,
                expected_sequence,
            } => self.automation_transition(
                request_id,
                action,
                request_digest,
                session_id,
                *expected_sequence,
                AutomationSessionState::Stopped,
                AutomationEventKind::Stopped,
                now,
                live_execution_policy,
            ),
            AutomationSessionWrite::RecordBaseline {
                request_id,
                session_id,
                expected_sequence,
                baseline,
            } => self.automation_record_baseline(
                request_id,
                action,
                request_digest,
                session_id,
                *expected_sequence,
                baseline,
                now,
                live_execution_policy,
            ),
            AutomationSessionWrite::CancelOrder { .. } => Err(LabError::Internal(
                "automation cancel_order must use the durable effect workflow".into(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One lifecycle transaction validates frozen identity, claims capacity, and writes the start receipt/event atomically.
    fn automation_start(
        &mut self,
        request_id: &RequestId,
        action: AutomationAction,
        request_digest: &ContentHash,
        policy_ref: &PolicyRevisionRef,
        market: &MarketId,
        decision_interval: crate::contracts::CandleInterval,
        mode: AutomationSessionMode,
        risk: &crate::contracts::AutomationRiskPolicy,
        frozen_policy: Option<FrozenPolicyRevision>,
        account_identity: Option<&str>,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        risk.validate_for_mode(mode)?;
        if !matches!(market.code().as_str(), "KRW-BTC" | "KRW-ETH" | "KRW-XRP") {
            return Err(LabError::InvalidConfig(
                "automation currently supports KRW-BTC, KRW-ETH, and KRW-XRP".into(),
            ));
        }
        let frozen_policy = frozen_policy.ok_or_else(|| {
            LabError::InvalidConfig("automation start requires the exact frozen policy".into())
        })?;
        validate_frozen_policy(&frozen_policy)?;
        if frozen_policy.reference != *policy_ref {
            return Err(LabError::InputHashMismatch(
                "automation frozen policy differs from requested revision".into(),
            ));
        }
        let account_identity = account_identity.ok_or_else(|| {
            LabError::InvalidConfig(
                "automation start requires a backend-owned account scope".into(),
            )
        })?;
        validate_account_identity(mode, account_identity)?;
        let risk_json = serde_json::to_string(risk).map_err(json_error)?;
        let policy_ref_json = serde_json::to_string(policy_ref).map_err(json_error)?;
        let frozen_policy_json = serde_json::to_string(&frozen_policy).map_err(json_error)?;
        let checkpoint_json = "{}";
        let session_id = crate::contracts::AutomationSessionId::from_seed(&format!(
            "automation-session:{}:{}",
            request_id.as_str(),
            request_digest.as_str()
        ));
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let conflicting_profile = transaction
            .query_row(
                "SELECT 1 FROM automation_sessions WHERE account_identity=?1 \
                 AND state IN ('RUNNING','PAUSED','NEEDS_RECONCILIATION') AND risk_json!=?2 LIMIT 1",
                params![account_identity, risk_json],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?
            .is_some();
        if conflicting_profile {
            return Err(LabError::Conflict(
                "all active sessions on one automation account must share one frozen risk policy"
                    .into(),
            ));
        }

        let conflicting_market = transaction
            .query_row(
                "SELECT 1 FROM automation_sessions WHERE account_identity=?1 AND market=?2 \
                 AND state IN ('RUNNING','PAUSED','NEEDS_RECONCILIATION') LIMIT 1",
                params![account_identity, market.code()],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?
            .is_some();
        if conflicting_market {
            return Err(LabError::Conflict(format!(
                "automation account already has an active session for {}",
                market.code()
            )));
        }

        let active_count_raw: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM automation_sessions WHERE state IN ('RUNNING','PAUSED','NEEDS_RECONCILIATION')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let active_count = u32::try_from(active_count_raw).map_err(|_| {
            LabError::DataCorrupt(
                "automation active session count is negative or out of u32 range".into(),
            )
        })?;
        if active_count >= MAX_AUTOMATION_SESSIONS {
            return Err(LabError::CapacityExceeded(format!(
                "automation session limit {MAX_AUTOMATION_SESSIONS} reached"
            )));
        }

        let live_approval_active =
            if mode == AutomationSessionMode::Live && live_execution_policy.is_enabled() {
                transaction
                .query_row(
                    "SELECT state='RUNNING' AND order_admission_blocked=0 AND emergency_latched=0 \
                         FROM trading_control_state WHERE singleton=1",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sql_error)?
            } else {
                false
            };
        let live_denial_reason = if mode != AutomationSessionMode::Live {
            None
        } else if !live_execution_policy.is_enabled() {
            Some("LIVE_TRADING_DISABLED")
        } else if !live_approval_active {
            Some("LIVE_GRANT_INACTIVE")
        } else {
            None
        };
        let (state, outcome, reason) = if let Some(reason) = live_denial_reason {
            (
                AutomationSessionState::BlockedApproval,
                AutomationCommandOutcome::Denied,
                Some(reason.to_owned()),
            )
        } else {
            (
                AutomationSessionState::Running,
                AutomationCommandOutcome::Applied,
                None,
            )
        };
        let sequence = 1_u64;
        if mode == AutomationSessionMode::Live {
            ensure_live_account_identity_history(&transaction, account_identity)?;
        }
        transaction
            .execute(
                "INSERT INTO automation_sessions(
                    session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,
                    frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,
                    created_at_utc,updated_at_utc,recovery_reason
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13,?14)",
                params![
                    session_id.as_str(),
                    request_id.as_str(),
                    account_identity,
                    mode_name(mode),
                    state_name(state),
                    to_sql_u64(sequence)?,
                    policy_ref_json,
                    frozen_policy_json,
                    market.code(),
                    interval_name(decision_interval),
                    risk_json,
                    checkpoint_json,
                    now.to_rfc3339(),
                    reason,
                ],
            )
            .map_err(sql_error)?;
        let event_kind = if state == AutomationSessionState::BlockedApproval {
            AutomationEventKind::RecoveryBlocked
        } else {
            AutomationEventKind::Started
        };
        insert_event(
            &transaction,
            &session_id,
            sequence,
            event_kind,
            now,
            serde_json::json!({"mode": mode, "reason": reason}),
        )?;
        let view = load_view_with_policy(&transaction, &session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::DataCorrupt("new automation session cannot be read back".into())
            })?;
        let receipt = AutomationSessionReceipt {
            request_id: request_id.clone(),
            action,
            outcome,
            sequence,
            observed_at_utc: now,
            reason,
        };
        let response = AutomationSessionCommandResponse {
            session: view,
            receipt,
            order: None,
            baseline: None,
        };
        insert_request(
            &transaction,
            request_id,
            &session_id,
            action,
            request_digest,
            &response,
            now,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(response)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // The action, sequence, target and event are one compare-and-swap command committed with its receipt.
    fn automation_transition(
        &mut self,
        request_id: &RequestId,
        action: AutomationAction,
        request_digest: &ContentHash,
        session_id: &crate::contracts::AutomationSessionId,
        expected_sequence: u64,
        target: AutomationSessionState,
        event_kind: AutomationEventKind,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let view = load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("unknown automation session: {session_id}"))
            })?;
        if view.sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "automation session sequence conflict for {session_id}: expected {expected_sequence}, current {}",
                view.sequence
            )));
        }
        let allowed = match (action, view.state) {
            (
                AutomationAction::Pause,
                AutomationSessionState::Running | AutomationSessionState::NeedsReconciliation,
            )
            | (
                AutomationAction::Stop,
                AutomationSessionState::Running
                | AutomationSessionState::Paused
                | AutomationSessionState::NeedsReconciliation
                | AutomationSessionState::BlockedApproval
                | AutomationSessionState::Faulted,
            ) => true,
            (AutomationAction::Resume, AutomationSessionState::Paused) => {
                if view.unresolved_order_count != 0 || view.recovery_reason.is_some() {
                    return Err(LabError::Conflict(
                        "automation resume requires resolved orders and account reconciliation"
                            .into(),
                    ));
                }
                if view.mode == AutomationSessionMode::Live {
                    let risk = view.account_risk.as_ref().ok_or_else(|| {
                        LabError::BlockedEvidence(
                            "live resume requires an account risk projection".into(),
                        )
                    })?;
                    if !live_execution_policy.is_enabled()
                        || !risk.risk.live_grant_active
                        || risk.risk.emergency_latched
                        || risk.balance.freshness != AutomationFreshness::Fresh
                        || risk.feed.freshness != AutomationFreshness::Fresh
                        || risk.effective_loss.admission_blocked
                    {
                        return Err(LabError::BlockedEvidence(
                            "live resume requires the operator policy, active owner grant, fresh account/feed, and verified loss reference".into(),
                        ));
                    }
                }
                true
            }
            (AutomationAction::Pause, AutomationSessionState::Paused)
            | (AutomationAction::Resume, AutomationSessionState::Running)
            | (AutomationAction::Stop, AutomationSessionState::Stopped) => false,
            _ => {
                return Err(LabError::Conflict(format!(
                    "cannot apply {action:?} to automation session {session_id} in state {:?}",
                    view.state
                )));
            }
        };
        if !allowed {
            let receipt = AutomationSessionReceipt {
                request_id: request_id.clone(),
                action,
                outcome: AutomationCommandOutcome::NoOp,
                sequence: view.sequence,
                observed_at_utc: now,
                reason: None,
            };
            let response = AutomationSessionCommandResponse {
                session: view,
                receipt,
                order: None,
                baseline: None,
            };
            insert_request(
                &transaction,
                request_id,
                session_id,
                action,
                request_digest,
                &response,
                now,
            )?;
            transaction.commit().map_err(sql_error)?;
            return Ok(response);
        }

        let next_sequence = view.sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation session sequence exhausted".into())
        })?;
        let updated = transaction
            .execute(
                "UPDATE automation_sessions SET state=?1,sequence=?2,updated_at_utc=?3 \
                 WHERE session_id=?4 AND sequence=?5",
                params![
                    state_name(target),
                    to_sql_u64(next_sequence)?,
                    now.to_rfc3339(),
                    session_id.as_str(),
                    to_sql_u64(expected_sequence)?,
                ],
            )
            .map_err(sql_error)?;
        if updated != 1 {
            return Err(LabError::Conflict(
                "automation session changed during compare-and-swap".into(),
            ));
        }
        if matches!(
            target,
            AutomationSessionState::Paused | AutomationSessionState::Stopped
        ) {
            let reason = if target == AutomationSessionState::Paused {
                "SESSION_PAUSED_BEFORE_SEND"
            } else {
                "SESSION_STOPPED_BEFORE_SEND"
            };
            reject_reserved_orders_for_session(&transaction, session_id, reason, now)?;
        }
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            event_kind,
            now,
            serde_json::json!({"action": action}),
        )?;
        let view = load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::DataCorrupt("transitioned automation session cannot be read back".into())
            })?;
        let receipt = AutomationSessionReceipt {
            request_id: request_id.clone(),
            action,
            outcome: AutomationCommandOutcome::Applied,
            sequence: next_sequence,
            observed_at_utc: now,
            reason: None,
        };
        let response = AutomationSessionCommandResponse {
            session: view,
            receipt,
            order: None,
            baseline: None,
        };
        insert_request(
            &transaction,
            request_id,
            session_id,
            action,
            request_digest,
            &response,
            now,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(response)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Baseline validation, evidence classification, sequence CAS, and receipt commit atomically.
    fn automation_record_baseline(
        &mut self,
        request_id: &RequestId,
        action: AutomationAction,
        request_digest: &ContentHash,
        session_id: &crate::contracts::AutomationSessionId,
        expected_sequence: u64,
        baseline: &AutomationAccountDayBaselineInput,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        baseline.validate()?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let view = load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("unknown automation session: {session_id}"))
            })?;
        if view.sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "automation session sequence conflict for {session_id}: expected {expected_sequence}, current {}",
                view.sequence
            )));
        }
        if view.state == AutomationSessionState::Faulted {
            return Err(LabError::Conflict(format!(
                "cannot record baseline for faulted automation session {session_id}"
            )));
        }
        if baseline.account_identity != view.account_identity {
            return Err(LabError::Conflict(
                "baseline account_identity does not match the session's bound account".into(),
            ));
        }
        validate_account_identity(view.mode, &baseline.account_identity)?;
        if baseline.source == AutomationBaselineSource::DurableMockVenueSeed
            && view.mode != AutomationSessionMode::Mock
        {
            return Err(LabError::InvalidConfig(
                "DURABLE_MOCK_VENUE_SEED baseline source is only valid for MOCK sessions".into(),
            ));
        }
        if baseline.reference_time_utc > now {
            return Err(LabError::InvalidConfig(
                "baseline reference_time_utc is in the future".into(),
            ));
        }
        let current_day = now.0.format("%Y-%m-%d").to_string();
        if baseline.day_utc != current_day {
            return Err(LabError::Conflict(format!(
                "baseline day_utc {} does not match current UTC day {current_day}",
                baseline.day_utc
            )));
        }
        let day_start = parse_day_start_utc(&baseline.day_utc)?;
        if account_day_baseline_exists(&transaction, &view.account_identity, &baseline.day_utc)? {
            return Err(LabError::Conflict(format!(
                "account {} already has a recorded baseline for UTC day {}",
                view.account_identity, baseline.day_utc
            )));
        }
        if account_has_nonterminal_orders(&transaction, &view.account_identity)? {
            return Err(LabError::Conflict(
                "cannot record a daily starting baseline while the account has unresolved or open orders".into(),
            ));
        }
        if account_day_has_recorded_activity(
            &transaction,
            &view.account_identity,
            view.mode,
            day_start,
            now,
        )? {
            return Err(LabError::Conflict(
                "cannot record a daily starting baseline after same-day trade activity or with unresolved orders".into(),
            ));
        }

        let evaluated = evaluate_and_persist_baseline(
            &transaction,
            request_id,
            &view.account_identity,
            view.mode,
            baseline,
            day_start,
            now,
        )?;

        let next_sequence = view.sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation session sequence exhausted".into())
        })?;
        let updated = transaction
            .execute(
                "UPDATE automation_sessions SET sequence=?1,updated_at_utc=?2 \
                 WHERE session_id=?3 AND sequence=?4",
                params![
                    to_sql_u64(next_sequence)?,
                    now.to_rfc3339(),
                    session_id.as_str(),
                    to_sql_u64(expected_sequence)?,
                ],
            )
            .map_err(sql_error)?;
        if updated != 1 {
            return Err(LabError::Conflict(
                "automation session changed during baseline compare-and-swap".into(),
            ));
        }
        let event_kind = if evaluated.sufficient_for_risk_admission {
            AutomationEventKind::BaselineRecorded
        } else {
            AutomationEventKind::BaselineBlocked
        };
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            event_kind,
            now,
            serde_json::json!({
                "baseline_id": evaluated.baseline_id,
                "day_utc": evaluated.day_utc,
                "source": evaluated.source,
                "verification_state": evaluated.verification_state,
                "sufficient_for_risk_admission": evaluated.sufficient_for_risk_admission,
                "baseline_digest": evaluated.baseline_digest,
                "block_reasons": evaluated.block_reasons,
            }),
        )?;
        let refreshed =
            load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
                .ok_or_else(|| {
                    LabError::DataCorrupt(
                        "automation session disappeared after baseline write".into(),
                    )
                })?;
        let reason = (!evaluated.block_reasons.is_empty()).then(|| {
            format!(
                "baseline recorded in blocked state: {}",
                evaluated.block_reasons.join("; ")
            )
        });
        let receipt = AutomationSessionReceipt {
            request_id: request_id.clone(),
            action,
            outcome: AutomationCommandOutcome::Applied,
            sequence: next_sequence,
            observed_at_utc: now,
            reason,
        };
        let response = AutomationSessionCommandResponse {
            session: refreshed,
            receipt,
            order: None,
            baseline: Some(evaluated),
        };
        insert_request(
            &transaction,
            request_id,
            session_id,
            action,
            request_digest,
            &response,
            now,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(response)
    }

    #[cfg(test)]
    pub(crate) fn automation_baseline_status(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        day_utc: Option<&str>,
        now: UtcTimestamp,
    ) -> Result<AutomationAccountDayBaselineStatus, LabError> {
        self.automation_baseline_status_with_policy(
            session_id,
            day_utc,
            now,
            LiveExecutionPolicy::Disabled,
        )
    }

    pub(crate) fn automation_baseline_status_with_policy(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        day_utc: Option<&str>,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationAccountDayBaselineStatus, LabError> {
        let view =
            self.automation_get_session_with_policy(session_id, now, live_execution_policy)?;
        let current_day = now.0.format("%Y-%m-%d").to_string();
        let target_day = match day_utc {
            Some(day) => {
                parse_day_start_utc(day)?;
                day.to_owned()
            }
            None => current_day.clone(),
        };
        let baseline =
            load_account_day_baseline_view(&self.connection, &view.account_identity, &target_day)?;
        let legacy_mock_baseline = if baseline.is_none() {
            if target_day == current_day {
                view.account_risk
                    .as_ref()
                    .and_then(|risk| risk.legacy_mock_baseline.clone())
            } else {
                load_legacy_mock_baseline_view(
                    &self.connection,
                    &view.account_identity,
                    view.mode,
                    &target_day,
                    now,
                )?
            }
        } else {
            None
        };
        let mut block_reasons = Vec::new();
        if target_day == current_day
            && let Some(risk) = &view.account_risk
        {
            block_reasons.extend(risk.admission_block_reasons.clone());
        }
        match &baseline {
            None => {
                if legacy_mock_baseline.is_none()
                    && !block_reasons
                        .iter()
                        .any(|r| r == "DAILY_START_BASELINE_MISSING")
                {
                    block_reasons.push("DAILY_START_BASELINE_MISSING".into());
                }
            }
            Some(record)
                if view.mode == AutomationSessionMode::Live
                    && record.sufficient_for_risk_admission =>
            {
                if !block_reasons
                    .iter()
                    .any(|r| r == "DAILY_START_BASELINE_BLOCKED")
                {
                    block_reasons.push("DAILY_START_BASELINE_BLOCKED".into());
                }
                let reason = "BASELINE:LIVE_PROVIDER_AS_OF_UNAVAILABLE";
                if !block_reasons.iter().any(|r| r == reason) {
                    block_reasons.push(reason.into());
                }
            }
            Some(record) if !record.sufficient_for_risk_admission => {
                if !block_reasons
                    .iter()
                    .any(|r| r == "DAILY_START_BASELINE_BLOCKED")
                {
                    block_reasons.push("DAILY_START_BASELINE_BLOCKED".into());
                }
                for reason in &record.block_reasons {
                    let prefixed = format!("BASELINE:{reason}");
                    if !block_reasons.contains(&prefixed) {
                        block_reasons.push(prefixed);
                    }
                }
            }
            Some(_) => {}
        }
        let has_risk_baseline = baseline
            .as_ref()
            .is_some_and(|record| record.sufficient_for_risk_admission)
            || legacy_mock_baseline.is_some();
        let admission_blocked = !block_reasons.is_empty() || !has_risk_baseline;
        Ok(AutomationAccountDayBaselineStatus {
            session_id: view.session_id,
            account_identity: view.account_identity,
            mode: view.mode,
            day_utc: target_day,
            admission_blocked,
            block_reasons,
            baseline,
            legacy_mock_baseline,
        })
    }

    /// Read the exact persisted response for a lifecycle request after an unknown reply.
    pub(crate) fn automation_readback_request(
        &self,
        request_id: &RequestId,
    ) -> Result<AutomationSessionRequestReadback, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT state,response_json,reason FROM automation_session_requests WHERE request_id=?1",
                [request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?,row.get::<_,Option<String>>(1)?,row.get::<_,Option<String>>(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((state, response_json, reason)) = row else {
            return Ok(AutomationSessionRequestReadback {
                request_id: request_id.clone(),
                state: crate::contracts::AutomationRequestReadbackState::NotRecorded,
                response: None,
                reason: Some("request is not durably recorded; outcome is unconfirmed".into()),
            });
        };
        let state = parse_request_state(&state)?;
        let response = response_json
            .map(|json| serde_json::from_str(&json).map_err(json_error))
            .transpose()?;
        Ok(AutomationSessionRequestReadback {
            request_id: request_id.clone(),
            state,
            response,
            reason,
        })
    }

    /// Recover bounded, locally applied lifecycle commands by their exact stored
    /// payload. Cancellation requests are deliberately excluded from this replay path.
    pub(crate) fn automation_pending_lifecycle_requests(
        &self,
        limit: u32,
    ) -> Result<Vec<AutomationPendingLifecycleRequest>, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(
                "invalid pending automation lifecycle request limit".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT request_json,account_identity FROM automation_session_requests \
                 WHERE state='PENDING' AND action!='CANCEL_ORDER' \
                 ORDER BY created_at_utc,request_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([i64::from(limit)], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (write_json, account_identity) = row.map_err(sql_error)?;
            let write: AutomationSessionWrite =
                serde_json::from_str(&write_json).map_err(json_error)?;
            if matches!(write, AutomationSessionWrite::CancelOrder { .. }) {
                return Err(LabError::DataCorrupt(
                    "cancel appeared in local lifecycle request recovery".into(),
                ));
            }
            Ok(AutomationPendingLifecycleRequest {
                write,
                account_identity,
            })
        })
        .collect()
    }

    /// Recover durable cancellation requests without implying a resend. A claimed
    /// request must only be resolved by identifier lookup; an unclaimed request may
    /// enter `automation_prepare_cancel`, whose CAS returns the sole send permit.
    pub(crate) fn automation_pending_cancel_requests(
        &self,
        limit: u32,
    ) -> Result<Vec<AutomationPendingCancelRequest>, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(
                "invalid pending automation cancellation request limit".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT request_json,response_json FROM automation_session_requests \
                 WHERE state='PENDING' AND action='CANCEL_ORDER' \
                 ORDER BY CASE WHEN response_json IS NOT NULL THEN 0 ELSE 1 END, \
                          created_at_utc,request_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([i64::from(limit)], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (write_json, response_json) = row.map_err(sql_error)?;
            let write: AutomationSessionWrite =
                serde_json::from_str(&write_json).map_err(json_error)?;
            if !matches!(write, AutomationSessionWrite::CancelOrder { .. }) {
                return Err(LabError::DataCorrupt(
                    "non-cancel appeared in cancellation recovery".into(),
                ));
            }
            if let Some(response_json) = response_json.as_deref() {
                let response: AutomationSessionCommandResponse =
                    serde_json::from_str(response_json).map_err(json_error)?;
                if response.receipt.outcome != AutomationCommandOutcome::Pending {
                    return Err(LabError::DataCorrupt(
                        "pending cancellation request has a non-pending claim response".into(),
                    ));
                }
            }
            Ok(AutomationPendingCancelRequest {
                write,
                claimed: response_json.is_some(),
            })
        })
        .collect()
    }

    #[cfg(test)]
    pub(crate) fn automation_get_session(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
    ) -> Result<AutomationSessionView, LabError> {
        self.automation_get_session_with_policy(session_id, now, LiveExecutionPolicy::Disabled)
    }

    /// Fence a session's new intents when its frozen cross-source coverage is incomplete.
    /// Existing orders and reservations remain available for ordinary reconciliation.
    /// # Errors
    /// Rejects a changed session sequence or persistence failure.
    pub(crate) fn automation_hold_source_gap(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        expected_sequence: u64,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        let tx = self.connection.transaction().map_err(sql_error)?;
        let (state, sequence, prior_reason): (String, i64, Option<String>) = tx.query_row(
            "SELECT state,sequence,recovery_reason FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).map_err(sql_error)?;
        if from_sql_u64(sequence)? != expected_sequence {
            return Err(LabError::Conflict(
                "automation source-gap sequence changed".into(),
            ));
        }
        if state == "RUNNING"
            || (state == "NEEDS_RECONCILIATION"
                && prior_reason.as_deref()
                    == Some("PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK"))
        {
            set_automation_recovery(&tx, session_id, reason, now)?;
        }
        tx.commit().map_err(sql_error)
    }

    pub(crate) fn automation_get_session_with_policy(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionView, LabError> {
        load_view_with_policy(&self.connection, session_id, now, live_execution_policy)?.ok_or_else(
            || LabError::InvalidConfig(format!("unknown automation session: {session_id}")),
        )
    }

    pub(crate) fn automation_list_sessions_with_policy(
        &self,
        offset: u32,
        limit: u32,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionPage, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "automation session page limit must be 1..={MAX_AUTOMATION_PAGE}"
            )));
        }
        let total_i64: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM automation_sessions", [], |row| {
                row.get(0)
            })
            .map_err(sql_error)?;
        let total_count = u64::try_from(total_i64)
            .map_err(|_| LabError::DataCorrupt("negative automation session count".into()))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id FROM automation_sessions \
                 ORDER BY created_at_utc DESC, session_id ASC LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_error)?;
        let ids = statement
            .query_map(params![i64::from(limit) + 1, i64::from(offset)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        let has_more = ids.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let mut ids = ids;
        if has_more {
            ids.truncate(usize::try_from(limit).map_err(|_| {
                LabError::ResourceLimit("automation page limit exceeds usize".into())
            })?);
        }
        let items = ids
            .into_iter()
            .map(|id| {
                let id = crate::contracts::AutomationSessionId::new(id)?;
                self.automation_get_session_with_policy(&id, now, live_execution_policy)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let returned_count = u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("automation page count overflow".into()))?;
        let next_offset =
            if has_more {
                Some(offset.checked_add(limit).ok_or_else(|| {
                    LabError::ResourceLimit("automation page offset overflow".into())
                })?)
            } else {
                None
            };
        Ok(AutomationSessionPage {
            items,
            returned_count,
            total_count,
            next_offset,
        })
    }

    pub(crate) fn automation_events(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<AutomationEventPage, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "automation event page limit must be 1..={MAX_AUTOMATION_PAGE}"
            )));
        }
        if !session_exists(&self.connection, session_id)? {
            return Err(LabError::InvalidConfig(format!(
                "unknown automation session: {session_id}"
            )));
        }
        let after = to_sql_u64(after_sequence.unwrap_or(0))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence,kind,at_utc,detail_json FROM automation_session_events \
                 WHERE session_id=?1 AND sequence>?2 ORDER BY sequence ASC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![session_id.as_str(), after, i64::from(limit) + 1],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
        drop(statement);
        let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let mut events = rows
            .into_iter()
            .take(usize::try_from(limit).map_err(|_| {
                LabError::ResourceLimit("automation event limit exceeds usize".into())
            })?)
            .map(|(sequence, kind, at, detail)| {
                Ok(AutomationEvent {
                    session_id: session_id.clone(),
                    sequence: from_sql_u64(sequence)?,
                    kind: parse_event_kind(&kind)?,
                    at_utc: UtcTimestamp::parse_rfc3339(&at)?,
                    detail: serde_json::from_str(&detail).map_err(json_error)?,
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
        let next_sequence = if has_more {
            events.last().map(|event| event.sequence)
        } else {
            None
        };
        Ok(AutomationEventPage {
            items: std::mem::take(&mut events),
            next_sequence,
        })
    }

    #[cfg(test)]
    pub(crate) fn automation_runtime_record(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
    ) -> Result<AutomationRuntimeRecord, LabError> {
        self.automation_runtime_record_with_policy(session_id, now, LiveExecutionPolicy::Disabled)
    }

    pub(crate) fn automation_runtime_record_with_policy(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationRuntimeRecord, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT account_identity,frozen_policy_json,checkpoint_json \
                 FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("unknown automation session: {session_id}"))
            })?;
        let frozen_policy: FrozenPolicyRevision =
            serde_json::from_str(&row.1).map_err(json_error)?;
        validate_frozen_policy(&frozen_policy)?;
        Ok(AutomationRuntimeRecord {
            view: self.automation_get_session_with_policy(
                session_id,
                now,
                live_execution_policy,
            )?,
            frozen_policy,
            account_identity: row.0,
            checkpoint_json: row.2,
        })
    }

    /// Return sessions requiring processing or venue reconciliation in stable order.
    #[cfg(test)]
    pub(crate) fn automation_due_sessions(
        &self,
        now: UtcTimestamp,
        limit: u32,
        after: Option<&AutomationDueSessionCursor>,
    ) -> Result<AutomationDueSessionPage, LabError> {
        self.automation_due_sessions_with_policy(now, limit, after, LiveExecutionPolicy::Disabled)
    }

    pub(crate) fn automation_due_sessions_with_policy(
        &self,
        now: UtcTimestamp,
        limit: u32,
        after: Option<&AutomationDueSessionCursor>,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationDueSessionPage, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(
                "invalid automation due-session limit".into(),
            ));
        }
        let due_predicate = "s.state IN ('RUNNING','NEEDS_RECONCILIATION') \
            OR (s.state IN ('PAUSED','STOPPED') AND s.recovery_reason IN ( \
                'ORDER_READBACK_REQUIRED','PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK')) \
            OR EXISTS (SELECT 1 FROM automation_orders o WHERE o.session_id=s.session_id \
                       AND o.state NOT IN ('FILLED','CANCELLED','REJECTED'))";
        let count_sql = format!("SELECT COUNT(*) FROM automation_sessions s WHERE {due_predicate}");
        let total_count = self
            .connection
            .query_row(&count_sql, [], |row| row.get::<_, i64>(0))
            .map_err(sql_error)?;
        let total_count = from_sql_u64(total_count)?;
        let (query, params) = if let Some(cursor) = after {
            (
                format!(
                    "SELECT s.session_id FROM automation_sessions s WHERE ({due_predicate}) \
                     AND (s.updated_at_utc>?1 OR (s.updated_at_utc=?1 AND s.session_id>?2)) \
                     ORDER BY s.updated_at_utc ASC,s.session_id ASC LIMIT ?3"
                ),
                Some((
                    cursor.updated_at_utc.to_rfc3339(),
                    cursor.session_id.as_str().to_owned(),
                )),
            )
        } else {
            (
                format!(
                    "SELECT s.session_id FROM automation_sessions s WHERE ({due_predicate}) \
                     ORDER BY s.updated_at_utc ASC,s.session_id ASC LIMIT ?1"
                ),
                None,
            )
        };
        let mut statement = self.connection.prepare(&query).map_err(sql_error)?;
        let ids = match params {
            Some((timestamp, session_id)) => statement
                .query_map(params![timestamp, session_id, i64::from(limit)], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?,
            None => statement
                .query_map([i64::from(limit)], |row| row.get::<_, String>(0))
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?,
        };
        drop(statement);
        let sessions = ids
            .into_iter()
            .map(|raw| {
                let id = crate::contracts::AutomationSessionId::new(raw)?;
                self.automation_runtime_record_with_policy(&id, now, live_execution_policy)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = sessions.last().map(|record| AutomationDueSessionCursor {
            updated_at_utc: record.view.updated_at_utc,
            session_id: record.view.session_id.clone(),
        });
        Ok(AutomationDueSessionPage {
            sessions,
            total_count,
            next_cursor,
        })
    }

    /// Mark process-owned running sessions for restart reconciliation before the coordinator
    /// can evaluate another candle or submit another order.
    pub(crate) fn automation_require_restart_reconciliation(
        &mut self,
        now: UtcTimestamp,
    ) -> Result<u32, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let ids = {
            let mut statement = transaction
                .prepare("SELECT session_id FROM automation_sessions WHERE state='RUNNING' ORDER BY session_id ASC")
                .map_err(sql_error)?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?
        };
        for raw in &ids {
            let id = crate::contracts::AutomationSessionId::new(raw.clone())?;
            let view = load_view(&transaction, &id, now)?.ok_or_else(|| {
                LabError::DataCorrupt("automation session vanished during recovery".into())
            })?;
            let sequence = view.sequence.checked_add(1).ok_or_else(|| {
                LabError::ResourceLimit("automation sequence exhausted during recovery".into())
            })?;
            transaction
                .execute(
                    "UPDATE automation_sessions SET state='NEEDS_RECONCILIATION',sequence=?1, \
                     recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK',updated_at_utc=?2 \
                     WHERE session_id=?3 AND sequence=?4",
                    params![to_sql_u64(sequence)?, now.to_rfc3339(), id.as_str(), to_sql_u64(view.sequence)?],
                )
                .map_err(sql_error)?;
            insert_event(
                &transaction,
                &id,
                next_event_sequence(&transaction, &id)?,
                AutomationEventKind::NeedsReconciliation,
                now,
                serde_json::json!({"reason":"PROCESS_RESTART"}),
            )?;
        }
        transaction.commit().map_err(sql_error)?;
        u32::try_from(ids.len())
            .map_err(|_| LabError::ResourceLimit("automation restart count overflow".into()))
    }

    /// Complete only the process-restart recovery reason after exact account and
    /// order reconciliation. Candle decisions may be replayed under that reason,
    /// but no adapter admission or send is allowed until this CAS succeeds.
    #[allow(clippy::too_many_lines)] // Recovery evidence, sequence CAS and audit receipt form one durable state transition.
    pub(crate) fn automation_complete_restart_reconciliation_with_policy(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionView, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT account_identity,mode,state,sequence,market,risk_json,recovery_reason \
                 FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation session".into()))?;
        if row.2 != "NEEDS_RECONCILIATION"
            || row.6.as_deref() != Some("PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK")
        {
            return Err(LabError::Conflict(
                "only the process-restart reconciliation reason can be automatically completed"
                    .into(),
            ));
        }
        let mode = parse_mode(&row.1)?;
        if mode == AutomationSessionMode::Live && !live_execution_policy.is_enabled() {
            return Err(LabError::BlockedEvidence(
                "live automation remains fail-closed during restart recovery".into(),
            ));
        }
        let risk: AutomationRiskPolicy = serde_json::from_str(&row.5).map_err(json_error)?;
        risk.validate_for_mode(mode)?;
        let market = MarketId::parse_upbit(&row.4)?;
        let mark =
            self.latest_account_market_mark(&row.0, &market, now, risk.max_feed_age_seconds)?;
        let position = self.automation_account_position(
            session_id,
            crate::contracts::PriceKrw::new(mark)?,
            now,
        )?;
        if position.equity_krw.get() <= Decimal::ZERO {
            return Err(LabError::BlockedEvidence(
                "restart reconciliation requires positive valued account equity".into(),
            ));
        }
        let view =
            self.automation_get_session_with_policy(session_id, now, live_execution_policy)?;
        let account_risk = view.account_risk.as_ref().ok_or_else(|| {
            LabError::BlockedEvidence("restart reconciliation account projection is missing".into())
        })?;
        if account_risk.balance.freshness != AutomationFreshness::Fresh
            || account_risk.feed.freshness != AutomationFreshness::Fresh
        {
            return Err(LabError::BlockedEvidence(
                "restart reconciliation requires fresh account and completed-feed observations"
                    .into(),
            ));
        }
        if mode == AutomationSessionMode::Live && !account_risk.risk.live_grant_active {
            return Err(LabError::BlockedEvidence(
                "live restart recovery requires an active durable live grant".into(),
            ));
        }
        if mode == AutomationSessionMode::Live && account_risk.effective_loss.admission_blocked {
            return Err(LabError::BlockedEvidence(
                "live restart recovery requires a verified effective loss reference".into(),
            ));
        }
        let blocking_orders: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM automation_orders WHERE session_id=?1 \
                 AND state IN ('SUBMITTED_UNKNOWN','OPEN','PARTIALLY_FILLED','NEEDS_RECONCILIATION')",
                [session_id.as_str()],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if blocking_orders != 0 {
            return Err(LabError::Conflict(
                "restart reconciliation still has orders requiring identifier readback or terminal resolution".into(),
            ));
        }
        let unknown_account_orders: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 \
                 AND state IN ('SUBMITTED_UNKNOWN','NEEDS_RECONCILIATION')",
                [&row.0],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if unknown_account_orders != 0 {
            return Err(LabError::Conflict(
                "another order on the shared account still requires identifier readback".into(),
            ));
        }
        let emergency: bool = self
            .connection
            .query_row(
                if mode == AutomationSessionMode::Live {
                    "SELECT emergency_latched OR order_admission_blocked FROM trading_control_state WHERE singleton=1"
                } else {
                    "SELECT emergency_latched FROM trading_control_state WHERE singleton=1"
                },
                [],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if emergency {
            return Err(LabError::BlockedEvidence(
                "durable emergency/order-admission latch blocks restart recovery".into(),
            ));
        }

        let expected_sequence = from_sql_u64(row.3)?;
        let next_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation session sequence exhausted".into())
        })?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let changed = transaction
            .execute(
                "UPDATE automation_sessions SET state='RUNNING',sequence=?1,recovery_reason=NULL,updated_at_utc=?2 \
                 WHERE session_id=?3 AND state='NEEDS_RECONCILIATION' \
                   AND recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK' AND sequence=?4",
                params![to_sql_u64(next_sequence)?,now.to_rfc3339(),session_id.as_str(),to_sql_u64(expected_sequence)?],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "restart recovery session compare-and-swap failed".into(),
            ));
        }
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            AutomationEventKind::Resumed,
            now,
            serde_json::json!({"reason":"PROCESS_RESTART_RECONCILIATION_COMPLETE","sequence":next_sequence}),
        )?;
        let view = load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::DataCorrupt("recovered automation session cannot be read back".into())
            })?;
        transaction.commit().map_err(sql_error)?;
        Ok(view)
    }

    /// Clear the recoverable order-readback cause, or a restart cause preserved
    /// by user pause/stop, after every account order is terminal and fresh
    /// account/feed/position evidence agrees. The sequence
    /// CAS restores RUNNING only from the automation overlay state; PAUSED,
    /// STOPPED, and approval/fault control states remain unchanged.
    #[allow(clippy::too_many_lines)] // Clearing a recovery cause and its CAS/event must share one evidence-gated transition.
    pub(crate) fn automation_complete_order_reconciliation_with_policy(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        expected_sequence: u64,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationSessionView, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT account_identity,mode,state,sequence,market,risk_json,recovery_reason \
                 FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation session".into()))?;
        let sequence = from_sql_u64(row.3)?;
        if sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "automation recovery sequence conflict for {session_id}: expected {expected_sequence}, current {sequence}"
            )));
        }
        let recovery_reason = row.6.as_deref().ok_or_else(|| {
            LabError::Conflict("automation session has no recoverable order-readback cause".into())
        })?;
        let order_readback_reason = recovery_reason == "ORDER_READBACK_REQUIRED";
        let process_restart_reason =
            recovery_reason == "PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK";
        let user_controlled_state = matches!(row.2.as_str(), "PAUSED" | "STOPPED");
        if !(order_readback_reason || process_restart_reason && user_controlled_state) {
            return Err(LabError::Conflict(
                "only an order-readback cause or a user-controlled restart cause can be cleared"
                    .into(),
            ));
        }
        if !matches!(
            row.2.as_str(),
            "NEEDS_RECONCILIATION" | "PAUSED" | "STOPPED" | "BLOCKED_APPROVAL" | "FAULTED"
        ) {
            return Err(LabError::Conflict(
                "automation session control state does not permit recovery completion".into(),
            ));
        }
        let mode = parse_mode(&row.1)?;
        if mode == AutomationSessionMode::Live && !live_execution_policy.is_enabled() {
            return Err(LabError::BlockedEvidence(
                "live automation remains fail-closed during order recovery".into(),
            ));
        }
        let risk: AutomationRiskPolicy = serde_json::from_str(&row.5).map_err(json_error)?;
        risk.validate_for_mode(mode)?;
        let market = MarketId::parse_upbit(&row.4)?;
        let mark =
            self.latest_account_market_mark(&row.0, &market, now, risk.max_feed_age_seconds)?;
        let position = self.automation_account_position(
            session_id,
            crate::contracts::PriceKrw::new(mark)?,
            now,
        )?;
        if position.equity_krw.get() <= Decimal::ZERO {
            return Err(LabError::BlockedEvidence(
                "order recovery requires positive valued account equity".into(),
            ));
        }
        let view =
            self.automation_get_session_with_policy(session_id, now, live_execution_policy)?;
        let projection = view.account_risk.as_ref().ok_or_else(|| {
            LabError::BlockedEvidence("order recovery account projection is missing".into())
        })?;
        if projection.balance.freshness != AutomationFreshness::Fresh
            || projection.feed.freshness != AutomationFreshness::Fresh
        {
            return Err(LabError::BlockedEvidence(
                "order recovery requires fresh account and completed-feed observations".into(),
            ));
        }
        if mode == AutomationSessionMode::Live && !projection.risk.live_grant_active {
            return Err(LabError::BlockedEvidence(
                "live order recovery requires an active durable live grant".into(),
            ));
        }
        if mode == AutomationSessionMode::Live && projection.effective_loss.admission_blocked {
            return Err(LabError::BlockedEvidence(
                "live order recovery requires a verified effective loss reference".into(),
            ));
        }
        let unresolved_account_orders: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 \
                 AND state NOT IN ('FILLED','CANCELLED','REJECTED')",
                [&row.0],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        let pending_cancel_requests: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM automation_session_requests WHERE session_id=?1 \
                 AND action='CANCEL_ORDER' AND state='PENDING'",
                [session_id.as_str()],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if unresolved_account_orders != 0 || pending_cancel_requests != 0 {
            return Err(LabError::Conflict(
                "account order/cancel readback remains unresolved".into(),
            ));
        }
        let admission_blocked: bool = self
            .connection
            .query_row(
                if mode == AutomationSessionMode::Live {
                    "SELECT emergency_latched OR order_admission_blocked FROM trading_control_state WHERE singleton=1"
                } else {
                    "SELECT emergency_latched FROM trading_control_state WHERE singleton=1"
                },
                [],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if admission_blocked {
            return Err(LabError::BlockedEvidence(
                "durable emergency/order-admission latch blocks order recovery".into(),
            ));
        }

        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation session sequence exhausted".into())
        })?;
        let restored_state = if row.2 == "NEEDS_RECONCILIATION" && order_readback_reason {
            "RUNNING"
        } else {
            row.2.as_str()
        };
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let changed = transaction
            .execute(
                "UPDATE automation_sessions SET state=?1,sequence=?2,recovery_reason=NULL,updated_at_utc=?3 \
                 WHERE session_id=?4 AND state=?5 AND sequence=?6 AND recovery_reason=?7",
                params![restored_state,to_sql_u64(next_sequence)?,now.to_rfc3339(),session_id.as_str(),row.2,to_sql_u64(expected_sequence)?,recovery_reason],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "order recovery compare-and-swap failed".into(),
            ));
        }
        if restored_state == "RUNNING" {
            insert_event(
                &transaction,
                session_id,
                next_event_sequence(&transaction, session_id)?,
                AutomationEventKind::Resumed,
                now,
                serde_json::json!({"reason":"ORDER_READBACK_AND_ACCOUNT_RECONCILIATION_COMPLETE","sequence":next_sequence}),
            )?;
        } else {
            insert_event(
                &transaction,
                session_id,
                next_event_sequence(&transaction, session_id)?,
                AutomationEventKind::OrderReconciled,
                now,
                serde_json::json!({
                    "recovery_reason_cleared": recovery_reason,
                    "state_preserved": restored_state,
                    "sequence": next_sequence,
                }),
            )?;
        }
        let view = load_view_with_policy(&transaction, session_id, now, live_execution_policy)?
            .ok_or_else(|| {
                LabError::DataCorrupt("recovered automation session cannot be read back".into())
            })?;
        transaction.commit().map_err(sql_error)?;
        Ok(view)
    }

    pub(crate) fn automation_ledger(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<AutomationLedgerPage, LabError> {
        if !(1..=MAX_AUTOMATION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "automation ledger page limit must be 1..={MAX_AUTOMATION_PAGE}"
            )));
        }
        if !session_exists(&self.connection, session_id)? {
            return Err(LabError::InvalidConfig(format!(
                "unknown automation session: {session_id}"
            )));
        }
        let offset = i64::try_from(after_sequence.unwrap_or(0))
            .map_err(|_| LabError::InvalidConfig("automation ledger offset is too large".into()))?;
        let orders = load_orders(&self.connection, session_id, limit, offset)?;
        let fills = load_fills(&self.connection, session_id, limit, offset)?;
        let next_sequence = if orders.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            || fills.len() == usize::try_from(limit).unwrap_or(usize::MAX)
        {
            Some(after_sequence.unwrap_or(0).saturating_add(u64::from(limit)))
        } else {
            None
        };
        Ok(AutomationLedgerPage {
            orders,
            fills,
            next_sequence,
        })
    }

    #[cfg(test)]
    pub(crate) fn automation_query(
        &mut self,
        query: AutomationSessionQuery,
        now: UtcTimestamp,
    ) -> Result<serde_json::Value, LabError> {
        self.automation_query_with_policy(query, now, LiveExecutionPolicy::Disabled)
    }

    pub(crate) fn automation_query_with_policy(
        &mut self,
        query: AutomationSessionQuery,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<serde_json::Value, LabError> {
        let value = match query {
            AutomationSessionQuery::List { offset, limit } => {
                serde_json::to_value(self.automation_list_sessions_with_policy(
                    offset,
                    limit,
                    now,
                    live_execution_policy,
                )?)?
            }
            AutomationSessionQuery::Get { session_id } => serde_json::to_value(
                self.automation_get_session_with_policy(&session_id, now, live_execution_policy)?,
            )?,
            AutomationSessionQuery::Request { request_id } => {
                serde_json::to_value(self.automation_readback_request(&request_id)?)?
            }
            AutomationSessionQuery::Events {
                session_id,
                after_sequence,
                limit,
            } => {
                serde_json::to_value(self.automation_events(&session_id, after_sequence, limit)?)?
            }
            AutomationSessionQuery::Ledger {
                session_id,
                after_sequence,
                limit,
            } => {
                serde_json::to_value(self.automation_ledger(&session_id, after_sequence, limit)?)?
            }
            AutomationSessionQuery::Baseline {
                session_id,
                day_utc,
            } => serde_json::to_value(self.automation_baseline_status_with_policy(
                &session_id,
                day_utc.as_deref(),
                now,
                live_execution_policy,
            )?)?,
            AutomationSessionQuery::Decisions {
                session_id,
                offset,
                limit,
            } => serde_json::to_value(self.automation_review_decisions(
                &session_id,
                offset,
                limit,
            )?)?,
            AutomationSessionQuery::Decision {
                session_id,
                intent_id,
            } => serde_json::to_value(self.automation_review_detail(&session_id, &intent_id)?)?,
            AutomationSessionQuery::Performance { .. }
            | AutomationSessionQuery::ExportManifest { .. }
            | AutomationSessionQuery::ExportRead { .. } => {
                return Err(LabError::Internal(
                    "automation review reporting must run through JobService".into(),
                ));
            }
        };
        Ok(value)
    }

    /// Persist a complete account read using the default-off LIVE effect policy.
    /// # Errors
    /// Propagates account integrity and storage failures.
    #[cfg(test)]
    pub(crate) fn automation_record_account_snapshot(
        &mut self,
        account_identity: &str,
        mode: AutomationSessionMode,
        observed_at: UtcTimestamp,
        received_at: UtcTimestamp,
        balances: &[UpbitAccountBalance],
    ) -> Result<(), LabError> {
        self.automation_record_account_snapshot_with_policy(
            account_identity,
            mode,
            observed_at,
            received_at,
            balances,
            LiveExecutionPolicy::Disabled,
        )
    }

    /// Persist the account read and, only after explicit LIVE policy and control
    /// grant, freeze the first account-scoped activation observation.
    /// Decimal values remain text in durable storage.
    #[expect(
        clippy::too_many_lines,
        reason = "snapshot, trusted day-baseline creation, and ledger validation commit atomically"
    )]
    pub(crate) fn automation_record_account_snapshot_with_policy(
        &mut self,
        account_identity: &str,
        mode: AutomationSessionMode,
        observed_at: UtcTimestamp,
        received_at: UtcTimestamp,
        balances: &[UpbitAccountBalance],
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<(), LabError> {
        validate_account_identity(mode, account_identity)?;
        if observed_at > received_at {
            return Err(LabError::DataCorrupt(
                "account observation timestamp is in the future".into(),
            ));
        }
        let mut currencies = std::collections::BTreeSet::new();
        for balance in balances {
            if balance.currency.is_empty()
                || balance.currency.len() > 15
                || !balance
                    .currency
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
                || balance.balance < Decimal::ZERO
                || balance.locked < Decimal::ZERO
                || balance.avg_buy_price < Decimal::ZERO
                || !currencies.insert(balance.currency.clone())
            {
                return Err(LabError::DataCorrupt(
                    "account snapshot has duplicate or invalid currency amounts".into(),
                ));
            }
        }
        let balances_json = serde_json::to_string(balances).map_err(json_error)?;
        let balances_digest = ContentHash::of_value(&balances)?.to_string();
        let day = observed_at.0.format("%Y-%m-%d").to_string();
        let day_start = UtcTimestamp::parse_rfc3339(&format!("{day}T00:00:00Z"))?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let sequence: Option<i64> = transaction
            .query_row(
                "SELECT MAX(snapshot_sequence) FROM automation_account_snapshots",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let sequence = sequence.unwrap_or(0).checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation account snapshot sequence overflow".into())
        })?;
        transaction
            .execute(
                "INSERT INTO automation_account_snapshots(\
                     snapshot_sequence,account_identity,observed_at_utc,source_mode,balances_json,balances_digest,created_at_utc\
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    sequence,
                    account_identity,
                    observed_at.to_rfc3339(),
                    mode_name(mode),
                    balances_json,
                    balances_digest,
                    received_at.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        // Once a post-reconciliation account read includes an accepted venue
        // order's lock, stop subtracting the same amount as a local reservation.
        // Keep the reservation active for order/daily limits until terminal proof.
        transaction
            .execute(
                "UPDATE automation_risk_reservations SET quote_reserved_krw='0',asset_reserved_volume='0' \
                 WHERE state='ACTIVE' AND account_identity=?1 AND client_identifier IN ( \
                   SELECT client_identifier FROM automation_orders WHERE account_identity=?1 \
                     AND state IN ('OPEN','PARTIALLY_FILLED') AND detail_json IS NOT NULL \
                     AND updated_at_utc<=?2 \
                 )",
                params![account_identity, received_at.to_rfc3339()],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE automation_sessions SET last_balance_observed_at_utc=?1 \
                 WHERE account_identity=?2 AND state IN ('RUNNING','PAUSED','NEEDS_RECONCILIATION')",
                params![observed_at.to_rfc3339(), account_identity],
            )
            .map_err(sql_error)?;
        let had_prior_day_baseline = account_has_any_day_baseline(&transaction, account_identity)?;
        if !account_day_exists(&transaction, account_identity, &day)?
            && !account_day_baseline_exists(&transaction, account_identity, &day)?
            && let Some((equity, baseline_at)) = account_day_baseline_if_trustworthy(
                &transaction,
                account_identity,
                mode,
                day_start,
                received_at,
                balances,
            )?
        {
            transaction
                .execute(
                    "INSERT INTO automation_account_days(\
                         account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,baseline_balances_json,created_at_utc\
                     ) VALUES (?1,?2,?3,?4,?5,?6)",
                    params![
                        account_identity,
                        day,
                        equity.to_string(),
                        balances_digest,
                        balances_json,
                        baseline_at.to_rfc3339()
                    ],
                )
                .map_err(sql_error)?;
            insert_automatic_mock_day_baseline(
                &transaction,
                AutomaticMockDayBaseline {
                    account_identity,
                    day: &day,
                    day_start,
                    baseline_at,
                    observed_at,
                    received_at,
                    snapshot_sequence: from_sql_u64(sequence)?,
                    balances,
                    balances_digest: &balances_digest,
                    equity,
                    had_prior_day_baseline,
                },
            )?;
        }
        let has_daily_baseline = account_has_any_day_baseline(&transaction, account_identity)?;
        let mut has_activation_reference =
            activation_reference_exists(&transaction, account_identity)?;
        if mode == AutomationSessionMode::Live
            && live_execution_policy.is_enabled()
            && !has_activation_reference
        {
            capture_live_activation_reference(
                &transaction,
                account_identity,
                from_sql_u64(sequence)?,
                observed_at,
                received_at,
                balances,
                &balances_json,
                &balances_digest,
            )?;
            has_activation_reference = activation_reference_exists(&transaction, account_identity)?;
        }
        if has_daily_baseline || has_activation_reference {
            match validate_account_balance_ledger(
                &transaction,
                account_identity,
                observed_at,
                balances,
            ) {
                Ok(()) => {
                    if mode == AutomationSessionMode::Live {
                        clear_account_flow_reconciliation(
                            &transaction,
                            account_identity,
                            received_at,
                        )?;
                    }
                }
                Err(LabError::Conflict(reason))
                    if mode == AutomationSessionMode::Live
                        && live_execution_policy.is_enabled() =>
                {
                    persist_account_flow_reconciliation(
                        &transaction,
                        account_identity,
                        received_at,
                        &reason,
                    )?;
                    // Keep the observed snapshot and the account-scoped session
                    // fence durable even though the read cannot be admitted as
                    // reconciled evidence. A later fresh matching snapshot may
                    // move fenced sessions to PAUSED, never RUNNING.
                    transaction.commit().map_err(sql_error)?;
                    return Err(LabError::BlockedEvidence(format!(
                        "LIVE_ACCOUNT_FLOW_UNRECONCILED: {reason}"
                    )));
                }
                Err(error) => return Err(error),
            }
        }
        if has_activation_reference {
            complete_pending_live_activation_valuation(
                &transaction,
                account_identity,
                received_at,
            )?;
        }
        transaction.commit().map_err(sql_error)
    }

    /// Resolve actual position context from one account snapshot plus unique exchange fills.
    /// Any unowned balance divergence or unpriced account asset blocks strategy evaluation.
    #[allow(clippy::too_many_lines)] // Snapshot, all-currency valuation and fill-episode consistency are one position-read authority.
    pub(crate) fn automation_account_position(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        mark: crate::contracts::PriceKrw,
        now: UtcTimestamp,
    ) -> Result<crate::automation::AutomationAccountPosition, LabError> {
        let (account_identity, mode, market, risk_json) = self
            .connection
            .query_row(
                "SELECT account_identity,mode,market,risk_json FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation session".into()))?;
        let mode = parse_mode(&mode)?;
        let market = MarketId::parse_upbit(&market)?;
        let risk: AutomationRiskPolicy = serde_json::from_str(&risk_json).map_err(json_error)?;
        let snapshot = self
            .connection
            .query_row(
                "SELECT observed_at_utc,source_mode,balances_json FROM automation_account_snapshots \
                 WHERE account_identity=?1 ORDER BY snapshot_sequence DESC LIMIT 1",
                [&account_identity],
                |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::BlockedEvidence("no complete account snapshot is available".into()))?;
        let observed_at = UtcTimestamp::parse_rfc3339(&snapshot.0)?;
        if parse_mode(&snapshot.1)? != mode
            || observed_at > now
            || (now.0 - observed_at.0).num_seconds() > i64::from(risk.max_balance_age_seconds)
        {
            return Err(LabError::BlockedEvidence(
                "account snapshot source or freshness does not satisfy the frozen risk policy"
                    .into(),
            ));
        }
        let balances: Vec<UpbitAccountBalance> =
            serde_json::from_str(&snapshot.2).map_err(json_error)?;
        validate_account_balance_ledger(
            &self.connection,
            &account_identity,
            observed_at,
            &balances,
        )?;
        for balance in &balances {
            if balance.currency == "KRW" {
                continue;
            }
            let quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
                LabError::DataCorrupt("account currency total exceeds decimal capacity".into())
            })?;
            let other_market = MarketId::parse_upbit(&format!("KRW-{}", balance.currency))?;
            self.account_market_episode(&account_identity, &other_market, quantity)?;
        }
        let base = balances
            .iter()
            .find(|row| row.currency == market.base.code());
        let actual_qty = if let Some(base) = base {
            base.balance.checked_add(base.locked).ok_or_else(|| {
                LabError::DataCorrupt("base account total exceeds decimal capacity".into())
            })?
        } else {
            Decimal::ZERO
        };
        let episode_opened_at =
            self.account_market_episode(&account_identity, &market, actual_qty)?;
        let mut equity = Decimal::ZERO;
        for balance in &balances {
            let quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
                LabError::DataCorrupt("account currency total exceeds decimal capacity".into())
            })?;
            if quantity == Decimal::ZERO {
                continue;
            }
            let value = if balance.currency == "KRW" {
                quantity
            } else if balance.currency == market.base.code() {
                quantity.checked_mul(mark.get()).ok_or_else(|| {
                    LabError::DataCorrupt("market position value exceeds decimal capacity".into())
                })?
            } else {
                let other_market = MarketId::parse_upbit(&format!("KRW-{}", balance.currency))?;
                let other_mark = self.latest_account_market_mark(
                    &account_identity,
                    &other_market,
                    now,
                    risk.max_feed_age_seconds,
                )?;
                quantity.checked_mul(other_mark).ok_or_else(|| {
                    LabError::BlockedEvidence(
                        "other account asset value exceeds decimal capacity".into(),
                    )
                })?
            };
            equity = equity.checked_add(value).ok_or_else(|| {
                LabError::DataCorrupt("account equity exceeds decimal capacity".into())
            })?;
        }
        if actual_qty > Decimal::ZERO && episode_opened_at.is_none() {
            return Err(LabError::Conflict(
                "account holds unowned base inventory with no durable automation fill episode"
                    .into(),
            ));
        }
        if actual_qty == Decimal::ZERO && episode_opened_at.is_some() {
            return Err(LabError::DataCorrupt(
                "fill ledger episode remains open while exchange account is flat".into(),
            ));
        }
        let actual_weight = if equity == Decimal::ZERO {
            crate::contracts::Weight::new(Decimal::ZERO)?
        } else {
            crate::contracts::Weight::new(
                actual_qty.checked_mul(mark.get()).ok_or_else(|| {
                    LabError::DataCorrupt("position value exceeds decimal capacity".into())
                })? / equity,
            )?
        };
        Ok(crate::automation::AutomationAccountPosition {
            actual_qty: AssetQuantity::new(actual_qty)?,
            actual_weight,
            equity_krw: QuoteAmount::new(equity)?,
            episode_opened_at,
            account_observed_at: observed_at,
        })
    }

    fn account_market_episode(
        &self,
        account_identity: &str,
        market: &MarketId,
        observed_qty: Decimal,
    ) -> Result<Option<UtcTimestamp>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT f.side,f.volume,f.trade_time_utc FROM automation_fills f \
                 JOIN automation_orders o USING(client_identifier) \
                 WHERE o.account_identity=?1 AND f.market=?2 \
                 ORDER BY f.trade_time_utc,f.trade_uuid",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![account_identity, market.code()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
        let mut quantity = Decimal::ZERO;
        let mut opened = None;
        for (side, volume, trade_time) in rows {
            let volume = parse_decimal(&volume)?;
            let at = UtcTimestamp::parse_rfc3339(&trade_time)?;
            match side.as_str() {
                "BID" => {
                    if quantity == Decimal::ZERO {
                        opened = Some(at);
                    }
                    quantity = quantity.checked_add(volume).ok_or_else(|| {
                        LabError::DataCorrupt("fill position quantity overflow".into())
                    })?;
                }
                "ASK" => {
                    if volume > quantity {
                        return Err(LabError::DataCorrupt(
                            "account fill ledger sells more than its owned position".into(),
                        ));
                    }
                    quantity -= volume;
                    if quantity == Decimal::ZERO {
                        opened = None;
                    }
                }
                other => return Err(LabError::DataCorrupt(format!("unknown fill side {other}"))),
            }
        }
        if quantity != observed_qty {
            return Err(LabError::Conflict(format!(
                "manual account divergence for {}: venue base balance differs from durable fill ledger",
                market.code()
            )));
        }
        Ok(opened)
    }

    fn latest_account_market_mark(
        &self,
        account_identity: &str,
        market: &MarketId,
        now: UtcTimestamp,
        max_age_seconds: u32,
    ) -> Result<Decimal, LabError> {
        latest_account_market_mark_with_time(
            &self.connection,
            account_identity,
            market,
            now,
            max_age_seconds,
        )
        .map(|observation| observation.candle.close.get())
    }

    /// Persist a preparation fact without creating a candle, intent or cursor.
    pub(crate) fn automation_record_preparation(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        preparation: &crate::contracts::DecisionPreparation,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        decision_preparation::record(self, session_id, preparation, now)
    }

    /// Persist an original gate deferral without changing intent, order, or session state.
    pub(crate) fn automation_record_admission_deferral(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        target: &crate::contracts::AdmissionDeferredTarget,
        reason: crate::contracts::AdmissionDeferredReason,
        reason_detail: Option<&str>,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        admission_deferred::record(self, session_id, target, reason, reason_detail, now)
    }

    /// Persist one causal completed candle and its evaluator checkpoint atomically.
    /// Identical replay is a no-op; conflicting identity or a gap blocks the session.
    #[allow(clippy::too_many_lines)] // Candle identity, checkpoint, cursor and event commit in one SQLite transaction.
    pub(crate) fn automation_record_candle(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        checkpoint_json: &str,
        received_at: UtcTimestamp,
    ) -> Result<bool, LabError> {
        self.automation_record_candle_with_preparation(
            session_id,
            observation,
            checkpoint_json,
            received_at,
            None,
        )
    }

    #[allow(clippy::too_many_lines)] // The preparation is frozen with the same original candle.
    pub(crate) fn automation_record_candle_with_preparation(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        checkpoint_json: &str,
        received_at: UtcTimestamp,
        preparation: Option<&crate::contracts::DecisionPreparation>,
    ) -> Result<bool, LabError> {
        if preparation.is_some_and(|fact| {
            fact.observation_id != observation.id
                || fact.market.code() != observation.candle.market
                || fact.interval != observation.candle.interval
                || fact.decision_time != observation.candle.close_time_utc
        }) {
            return Err(LabError::DataCorrupt(
                "warmup preparation differs from its candle".into(),
            ));
        }
        if checkpoint_json.len() > 2_000_000 {
            return Err(LabError::ResourceLimit(
                "automation evaluator checkpoint exceeds 2 MiB".into(),
            ));
        }
        let _: serde_json::Value = serde_json::from_str(checkpoint_json).map_err(json_error)?;
        if !observation.candle.completed
            || observation.candle.close_time_utc > received_at
            || observation.content_digest != observation_digest(&observation.candle)?
            || observation.id != ObservationId::from_seed(observation.content_digest.as_str())
        {
            return Err(LabError::DataCorrupt(
                "automation candle is incomplete, future-dated, or has mismatched identity".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let session = transaction
            .query_row(
                "SELECT state,market,decision_interval,last_finalized_candle_at_utc,created_at_utc,recovery_reason \
                 FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("unknown automation session {session_id}"))
            })?;
        let restart_replay = session.0 == "NEEDS_RECONCILIATION"
            && session.5.as_deref() == Some("PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK");
        if session.0 != "RUNNING" && !restart_replay {
            return Err(LabError::Conflict(
                "automation session is not running; no candle was processed".into(),
            ));
        }
        let market = MarketId::parse_upbit(&session.1)?;
        let interval = parse_interval(&session.2)?;
        if observation.candle.market != market.code()
            || observation.candle.interval != interval
            || observation.candle.open_time_utc >= observation.candle.close_time_utc
            || observation.candle.close_time_utc.0 - observation.candle.open_time_utc.0
                != interval.duration()
        {
            return Err(LabError::DataCorrupt(
                "automation candle market or interval differs from frozen session".into(),
            ));
        }
        if let Some(existing_digest) = transaction
            .query_row(
                "SELECT content_digest,checkpoint_json FROM automation_candles \
                 WHERE session_id=?1 AND close_time_utc=?2",
                params![
                    session_id.as_str(),
                    observation.candle.close_time_utc.to_rfc3339()
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
        {
            if existing_digest.0 == observation.content_digest.as_str()
                && existing_digest.1 == checkpoint_json
            {
                return Ok(false);
            }
            set_automation_recovery(
                &transaction,
                session_id,
                "CONFLICTING_CANDLE_AT_SAME_CLOSE_TIME",
                received_at,
            )?;
            transaction.commit().map_err(sql_error)?;
            return Ok(false);
        }
        if let Some(last_close) = session.3 {
            let last_close = UtcTimestamp::parse_rfc3339(&last_close)?;
            if observation.candle.close_time_utc <= last_close {
                return Err(LabError::Conflict(
                    "automation candle is older than the durable close cursor".into(),
                ));
            }
            if observation.candle.open_time_utc != last_close {
                set_automation_recovery(
                    &transaction,
                    session_id,
                    "MISSING_FINALIZED_CANDLE_GAP",
                    received_at,
                )?;
                transaction.commit().map_err(sql_error)?;
                return Ok(false);
            }
        } else {
            let started_at = UtcTimestamp::parse_rfc3339(&session.4)?;
            if observation.candle.close_time_utc > started_at {
                let interval_seconds = interval.duration().num_seconds();
                let first_expected_close = started_at
                    .0
                    .timestamp()
                    .div_euclid(interval_seconds)
                    .checked_add(1)
                    .and_then(|bucket| bucket.checked_mul(interval_seconds))
                    .ok_or_else(|| {
                        LabError::ResourceLimit("automation start-boundary overflow".into())
                    })?;
                if observation.candle.close_time_utc.0.timestamp() != first_expected_close {
                    set_automation_recovery(
                        &transaction,
                        session_id,
                        "FIRST_POST_START_CANDLE_MISSING",
                        received_at,
                    )?;
                    transaction.commit().map_err(sql_error)?;
                    return Ok(false);
                }
            }
        }
        transaction
            .execute(
                "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    session_id.as_str(),
                    observation.id.as_str(),
                    observation.candle.close_time_utc.to_rfc3339(),
                    observation.content_digest.as_str(),
                    serde_json::to_string(&observation).map_err(json_error)?,
                    checkpoint_json,
                ],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE automation_sessions SET checkpoint_json=?1,last_finalized_candle_at_utc=?2, \
                 last_feed_received_at_utc=?3,updated_at_utc=?3 WHERE session_id=?4 \
                 AND (state='RUNNING' OR (state='NEEDS_RECONCILIATION' AND recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK'))",
                params![
                    checkpoint_json,
                    observation.candle.close_time_utc.to_rfc3339(),
                    received_at.to_rfc3339(),
                    session_id.as_str(),
                ],
            )
            .map_err(sql_error)?;
        let mut candle_detail = serde_json::json!({
            "observation_id": observation.id,
            "close_time_utc": observation.candle.close_time_utc,
            "content_digest": observation.content_digest
        });
        if let Some(preparation) = preparation {
            candle_detail["preparation"] = serde_json::to_value(preparation)?;
        }
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            AutomationEventKind::CandleObserved,
            received_at,
            candle_detail,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(true)
    }

    /// Commit the completed candle, post-evaluation checkpoint, and resulting
    /// signal/intent in one transaction. A restart can therefore never advance
    /// the causal cursor while losing the decision that belongs to that candle.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // The full causal decision tuple must be validated and committed atomically.
    #[cfg(test)]
    pub(crate) fn automation_record_decision(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        checkpoint_json: &str,
        intent_id: &AutomationIntentId,
        action: &str,
        evaluation_json: &serde_json::Value,
        received_at: UtcTimestamp,
    ) -> Result<bool, LabError> {
        self.automation_record_decision_with_rejection(
            session_id,
            observation,
            checkpoint_json,
            intent_id,
            action,
            evaluation_json,
            received_at,
            None,
        )
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // The causal snapshot and terminal disposition share one transaction.
    pub(crate) fn automation_record_decision_with_rejection(
        &mut self,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        checkpoint_json: &str,
        intent_id: &AutomationIntentId,
        action: &str,
        evaluation_json: &serde_json::Value,
        received_at: UtcTimestamp,
        rejection_reason: Option<&str>,
    ) -> Result<bool, LabError> {
        if rejection_reason.is_some_and(|reason| reason.is_empty() || reason.len() > 2_048) {
            return Err(LabError::InvalidConfig(
                "automation decision rejection reason exceeds bounds".into(),
            ));
        }
        if checkpoint_json.len() > 2_000_000 {
            return Err(LabError::ResourceLimit(
                "automation evaluator checkpoint exceeds 2 MiB".into(),
            ));
        }
        let _: serde_json::Value = serde_json::from_str(checkpoint_json).map_err(json_error)?;
        if !matches!(action, "BUY" | "SELL" | "HOLD") {
            return Err(LabError::InvalidConfig(
                "invalid automation intent action".into(),
            ));
        }
        if !observation.candle.completed
            || observation.candle.close_time_utc > received_at
            || observation.content_digest != observation_digest(&observation.candle)?
            || observation.id != ObservationId::from_seed(observation.content_digest.as_str())
        {
            return Err(LabError::DataCorrupt(
                "automation candle is incomplete, future-dated, or has mismatched identity".into(),
            ));
        }
        let evaluation_digest = ContentHash::of_value(evaluation_json)?;
        let evaluation_text = serde_json::to_string(evaluation_json).map_err(json_error)?;
        let candle_text = serde_json::to_string(observation).map_err(json_error)?;
        let close_text = observation.candle.close_time_utc.to_rfc3339();
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let session = transaction
            .query_row(
                "SELECT state,market,decision_interval,last_finalized_candle_at_utc,created_at_utc,recovery_reason,mode \
                 FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("unknown automation session {session_id}"))
            })?;
        let restart_replay = session.0 == "NEEDS_RECONCILIATION"
            && session.5.as_deref() == Some("PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK");
        if session.0 != "RUNNING" && !restart_replay {
            return Err(LabError::Conflict(
                "automation session is not running; no decision was processed".into(),
            ));
        }
        let market = MarketId::parse_upbit(&session.1)?;
        let interval = parse_interval(&session.2)?;
        if observation.candle.market != market.code()
            || observation.candle.interval != interval
            || observation.candle.open_time_utc >= observation.candle.close_time_utc
            || observation.candle.close_time_utc.0 - observation.candle.open_time_utc.0
                != interval.duration()
        {
            return Err(LabError::DataCorrupt(
                "automation candle market or interval differs from frozen session".into(),
            ));
        }

        let existing_candle = transaction
            .query_row(
                "SELECT observation_id,close_time_utc,content_digest,checkpoint_json FROM automation_candles \
                 WHERE session_id=?1 AND (observation_id=?2 OR close_time_utc=?3)",
                params![session_id.as_str(), observation.id.as_str(), close_text],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let mut inserted_candle = false;
        if let Some((existing_id, existing_close, existing_digest, existing_checkpoint)) =
            existing_candle
        {
            if existing_id != observation.id.as_str()
                || existing_close != close_text
                || existing_digest != observation.content_digest.as_str()
                || existing_checkpoint != checkpoint_json
            {
                set_automation_recovery(
                    &transaction,
                    session_id,
                    "CONFLICTING_CANDLE_AT_SAME_CLOSE_TIME",
                    received_at,
                )?;
                transaction.commit().map_err(sql_error)?;
                return Ok(false);
            }
        } else {
            if let Some(last_close) = session.3 {
                let last_close = UtcTimestamp::parse_rfc3339(&last_close)?;
                if observation.candle.close_time_utc <= last_close {
                    return Err(LabError::Conflict(
                        "automation candle is older than the durable close cursor".into(),
                    ));
                }
                if observation.candle.open_time_utc != last_close {
                    set_automation_recovery(
                        &transaction,
                        session_id,
                        "MISSING_FINALIZED_CANDLE_GAP",
                        received_at,
                    )?;
                    transaction.commit().map_err(sql_error)?;
                    return Ok(false);
                }
            } else {
                let started_at = UtcTimestamp::parse_rfc3339(&session.4)?;
                if observation.candle.close_time_utc > started_at {
                    let interval_seconds = interval.duration().num_seconds();
                    let first_expected_close = started_at
                        .0
                        .timestamp()
                        .div_euclid(interval_seconds)
                        .checked_add(1)
                        .and_then(|bucket| bucket.checked_mul(interval_seconds))
                        .ok_or_else(|| {
                            LabError::ResourceLimit("automation start-boundary overflow".into())
                        })?;
                    if observation.candle.close_time_utc.0.timestamp() != first_expected_close {
                        set_automation_recovery(
                            &transaction,
                            session_id,
                            "FIRST_POST_START_CANDLE_MISSING",
                            received_at,
                        )?;
                        transaction.commit().map_err(sql_error)?;
                        return Ok(false);
                    }
                }
            }
            transaction
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,?6)",
                    params![
                        session_id.as_str(),
                        observation.id.as_str(),
                        close_text,
                        observation.content_digest.as_str(),
                        candle_text,
                        checkpoint_json,
                    ],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE automation_sessions SET checkpoint_json=?1,last_finalized_candle_at_utc=?2, \
                     last_feed_received_at_utc=?3,updated_at_utc=?3 WHERE session_id=?4 \
                     AND (state='RUNNING' OR (state='NEEDS_RECONCILIATION' AND recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK'))",
                    params![
                        checkpoint_json,
                        observation.candle.close_time_utc.to_rfc3339(),
                        received_at.to_rfc3339(),
                        session_id.as_str(),
                    ],
                )
                .map_err(sql_error)?;
            insert_event(
                &transaction,
                session_id,
                next_event_sequence(&transaction, session_id)?,
                AutomationEventKind::CandleObserved,
                received_at,
                serde_json::json!({"observation_id":observation.id,"close_time_utc":observation.candle.close_time_utc,
                    "content_digest":observation.content_digest}),
            )?;
            inserted_candle = true;
        }

        let prior_for_observation = transaction
            .query_row(
                "SELECT intent_id,action,evaluation_json FROM automation_intents \
                 WHERE session_id=?1 AND observation_id=?2",
                params![session_id.as_str(), observation.id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        if let Some((prior_id, prior_action, prior_evaluation)) = prior_for_observation {
            let prior_value: serde_json::Value =
                serde_json::from_str(&prior_evaluation).map_err(json_error)?;
            if prior_id != intent_id.as_str()
                || prior_action != action
                || ContentHash::of_value(&prior_value)? != evaluation_digest
            {
                return Err(LabError::Conflict(
                    "automation observation already has a different durable decision".into(),
                ));
            }
            if inserted_candle {
                return Err(LabError::DataCorrupt(
                    "new candle unexpectedly already had a durable intent".into(),
                ));
            }
            transaction.commit().map_err(sql_error)?;
            return Ok(false);
        }
        let outstanding_intents: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM automation_intents WHERE session_id=?1 AND state='PENDING'",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if outstanding_intents != 0 {
            return Err(LabError::Conflict(
                "resolve the exact pending strategy intent before advancing this session's candle cursor".into(),
            ));
        }
        let intent_id_exists = transaction
            .query_row(
                "SELECT 1 FROM automation_intents WHERE intent_id=?1",
                [intent_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?
            .is_some();
        if intent_id_exists {
            return Err(LabError::Conflict(
                "automation intent identifier is already bound to another decision".into(),
            ));
        }
        let rejection_reason = rejection_reason.filter(|_| session.6 == "LIVE");
        let state = if action == "HOLD" {
            "NO_ACTION"
        } else if rejection_reason.is_some() {
            "REJECTED"
        } else {
            "PENDING"
        };
        transaction
            .execute(
                "INSERT INTO automation_intents(intent_id,session_id,observation_id,decision_time_utc,action,state,evaluation_json,created_at_utc) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    intent_id.as_str(),
                    session_id.as_str(),
                    observation.id.as_str(),
                    close_text,
                    action,
                    state,
                    evaluation_text,
                    received_at.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            AutomationEventKind::SignalObserved,
            received_at,
            serde_json::json!({"intent_id":intent_id,"action":action,"decision_time_utc":observation.candle.close_time_utc,
                "evaluation_digest":evaluation_digest}),
        )?;
        if state == "REJECTED" {
            insert_event(
                &transaction,
                session_id,
                next_event_sequence(&transaction, session_id)?,
                AutomationEventKind::IntentRejected,
                received_at,
                serde_json::json!({"intent_id":intent_id,"reason":rejection_reason,"pre_send":true}),
            )?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(true)
    }

    /// Return bounded, exact durable intents that still need risk admission.
    /// The caller must fetch fresh account/rules evidence before reserving them.
    pub(crate) fn automation_pending_intents(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
    ) -> Result<Vec<AutomationPendingIntent>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT i.session_id,i.observation_id,i.intent_id,i.action,i.decision_time_utc, \
                        i.evaluation_json,c.candle_json \
                 FROM automation_intents i JOIN automation_candles c \
                   ON c.session_id=i.session_id AND c.observation_id=i.observation_id \
                 WHERE i.session_id=?1 AND i.state='PENDING' \
                 ORDER BY i.decision_time_utc,i.intent_id LIMIT 100",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([session_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (
                session,
                observation_id,
                intent_id,
                action,
                decision_time,
                evaluation_json,
                candle_json,
            ) = row.map_err(sql_error)?;
            if !matches!(action.as_str(), "BUY" | "SELL") {
                return Err(LabError::DataCorrupt(
                    "pending automation intent has a non-order action".into(),
                ));
            }
            let observation: CandleObservation =
                serde_json::from_str(&candle_json).map_err(json_error)?;
            let observation_id = ObservationId::new(observation_id)?;
            if observation.id != observation_id {
                return Err(LabError::DataCorrupt(
                    "pending intent source candle identity does not match".into(),
                ));
            }
            let decision_time = UtcTimestamp::parse_rfc3339(&decision_time)?;
            if decision_time != observation.candle.close_time_utc {
                return Err(LabError::DataCorrupt(
                    "pending intent decision time differs from source candle close".into(),
                ));
            }
            Ok(AutomationPendingIntent {
                session_id: crate::contracts::AutomationSessionId::new(session)?,
                observation_id,
                intent_id: AutomationIntentId::new(intent_id)?,
                action,
                decision_time,
                evaluation_json: serde_json::from_str(&evaluation_json).map_err(json_error)?,
                decision_mark: crate::contracts::PriceKrw::new(observation.candle.close.get())?,
                observation,
            })
        })
        .collect()
    }

    pub(crate) fn automation_reject_intent(
        &mut self,
        intent_id: &crate::contracts::AutomationIntentId,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        if reason.is_empty() || reason.len() > 2_048 {
            return Err(LabError::InvalidConfig(
                "automation rejection reason exceeds bounds".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        reject_pending_intent_in_transaction(&transaction, intent_id, reason, now)?;
        transaction.commit().map_err(sql_error)
    }

    /// Atomically enforce the shared account risk policy and reserve its cash/asset
    /// before an order can be claimed for any venue send.
    #[allow(clippy::needless_pass_by_value)]
    // The database-owner command transfers a complete admission snapshot into this synchronous transaction.
    #[cfg(test)]
    pub(crate) fn automation_admit_order(
        &mut self,
        admission: AutomationOrderAdmission,
        now: UtcTimestamp,
    ) -> Result<AutomationOrderView, LabError> {
        self.automation_admit_order_with_policy(admission, now, LiveExecutionPolicy::Disabled)
    }

    #[allow(clippy::needless_pass_by_value)] // The owner command transfers a complete admission snapshot and immutable process policy into this synchronous transaction.
    pub(crate) fn automation_admit_order_with_policy(
        &mut self,
        admission: AutomationOrderAdmission,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationOrderView, LabError> {
        admission.request.validate()?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let session_row = load_automation_admission_session(&transaction, &admission.session_id)?;
        let request_digest = ContentHash::of_value(&(
            &admission.request,
            admission.decision_mark,
            admission.requested_notional,
            admission.quote_reserve,
            admission.asset_currency.as_str(),
            admission.asset_reserve,
            admission.fee_reserve,
            &admission.rules,
            &admission.evaluation_json,
        ))?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT request_digest FROM automation_orders WHERE client_identifier=?1",
                [admission.request.client_identifier.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
        {
            if existing != request_digest.as_str() {
                return Err(LabError::Conflict(
                    "automation order identifier was reused with different economics".into(),
                ));
            }
            return load_order_view(&transaction, &admission.request.client_identifier)?
                .ok_or_else(|| LabError::DataCorrupt("automation order readback missing".into()));
        }

        let context = automation_admission_context(session_row)?;
        if context.mode == AutomationSessionMode::Live {
            ensure_live_order_control_enabled(&transaction)?;
        }
        let side = validate_admission_market_rules(&admission, &context, now)?;
        let expected_asset_currency = validate_admission_order_amounts(&admission, &context, side)?;
        let terms = AutomationAdmissionTerms {
            request_digest,
            side,
            expected_asset_currency,
        };
        validate_pending_admission_intent(&transaction, &admission, &context, &terms)?;
        let reservations =
            load_account_reservation_facts(&transaction, &admission, &context, &terms, now)?;
        let projection = load_admission_risk_projection(
            &transaction,
            &admission,
            &context,
            now,
            live_execution_policy,
        )?;
        validate_shared_order_risk(&admission, &context, &terms, &reservations, &projection)?;
        persist_order_reservation(&transaction, &admission, &context, &terms, now)?;

        let view = load_order_view(&transaction, &admission.request.client_identifier)?
            .ok_or_else(|| LabError::DataCorrupt("new order is missing after admission".into()))?;
        transaction.commit().map_err(sql_error)?;
        Ok(view)
    }

    /// Load every nonterminal owned order for one session in durable creation order.
    pub(crate) fn automation_pending_orders(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
    ) -> Result<Vec<AutomationPendingOrder>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id,account_identity,state,order_request_json,decision_mark_krw \
                 FROM automation_orders WHERE session_id=?1 \
                 AND state NOT IN ('FILLED','CANCELLED','REJECTED') \
                 ORDER BY created_at_utc,client_identifier",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([session_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (session, account, state, request, mark) = row.map_err(sql_error)?;
            Ok(AutomationPendingOrder {
                session_id: crate::contracts::AutomationSessionId::new(session)?,
                account_identity: account,
                state: parse_order_state(&state)?,
                request: serde_json::from_str(&request).map_err(json_error)?,
                decision_mark: crate::contracts::PriceKrw::new(parse_decimal(&mark)?)?,
            })
        })
        .collect()
    }

    /// Exact idempotent readback by the durable Upbit client identifier.
    /// This includes terminal orders and is the recovery path when a mutation
    /// reply was lost; a missing row does not authorize a new venue identifier.
    pub(crate) fn automation_order_readback(
        &self,
        identifier: &AutomationOrderIdentifier,
    ) -> Result<Option<AutomationOrderView>, LabError> {
        load_order_view(&self.connection, identifier)
    }

    /// Resolve a streamed identifier against durable local account/session scope
    /// before the runtime is permitted to issue an exact private REST readback.
    /// Terminal orders remain resolvable for duplicate and late provider events.
    pub(crate) fn automation_order_readback_target(
        &self,
        identifier: &AutomationOrderIdentifier,
    ) -> Result<Option<AutomationPendingOrder>, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT session_id,account_identity,state,order_request_json,decision_mark_krw \
             FROM automation_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|(session, account, state, request, mark)| {
            let request: AutomationOrderRequest =
                serde_json::from_str(&request).map_err(json_error)?;
            if &request.client_identifier != identifier {
                return Err(LabError::DataCorrupt(
                    "durable order identifier mismatch".into(),
                ));
            }
            Ok(AutomationPendingOrder {
                session_id: crate::contracts::AutomationSessionId::new(session)?,
                account_identity: account,
                state: parse_order_state(&state)?,
                request,
                decision_mark: crate::contracts::PriceKrw::new(parse_decimal(&mark)?)?,
            })
        })
        .transpose()
    }

    /// Bounded account recovery targets include uncertain orders in every session.
    /// RESERVED intents have no provider effect and remain on the admission path.
    pub(crate) fn automation_account_order_readback_targets(
        &self,
        account_identity: &str,
    ) -> Result<Vec<AutomationPendingOrder>, LabError> {
        const LIMIT: usize = 1_000;
        if account_identity.is_empty() || account_identity.len() > 256 {
            return Err(LabError::InvalidConfig(
                "invalid private order recovery account".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT client_identifier FROM automation_orders WHERE account_identity=?1 \
             AND state IN ('SUBMITTED_UNKNOWN','OPEN','PARTIALLY_FILLED','NEEDS_RECONCILIATION') \
             ORDER BY client_identifier LIMIT 1001",
            )
            .map_err(sql_error)?;
        let identifiers = statement
            .query_map([account_identity], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        if identifiers.len() > LIMIT {
            return Err(LabError::ResourceLimit(
                "private order recovery target limit exceeded".into(),
            ));
        }
        identifiers
            .into_iter()
            .map(|raw| {
                let identifier = AutomationOrderIdentifier::new(raw)?;
                let target = self
                    .automation_order_readback_target(&identifier)?
                    .ok_or_else(|| {
                        LabError::DataCorrupt("private order recovery target disappeared".into())
                    })?;
                if target.account_identity != account_identity {
                    return Err(LabError::DataCorrupt(
                        "private order recovery account mismatch".into(),
                    ));
                }
                Ok(target)
            })
            .collect()
    }

    /// Release a reservation only after a definitive pre-send denial. A durable
    /// send claim changes the state to `SUBMITTED_UNKNOWN`, which this CAS refuses.
    pub(crate) fn automation_reject_reserved_order(
        &mut self,
        identifier: &AutomationOrderIdentifier,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<AutomationOrderView, LabError> {
        if reason.is_empty() || reason.len() > 2_048 {
            return Err(LabError::InvalidConfig(
                "pre-send rejection reason exceeds bounds".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let (session_raw, state) = transaction
            .query_row(
                "SELECT session_id,state FROM automation_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation order".into()))?;
        let session_id = crate::contracts::AutomationSessionId::new(session_raw)?;
        if state == "REJECTED" {
            let view = load_order_view(&transaction, identifier)?
                .ok_or_else(|| LabError::DataCorrupt("rejected order readback missing".into()))?;
            transaction.commit().map_err(sql_error)?;
            return Ok(view);
        }
        if state != "RESERVED" {
            return Err(LabError::Conflict(
                "only a provably pre-send RESERVED order may release its reservation".into(),
            ));
        }
        if !reject_reserved_order_in_transaction(
            &transaction,
            &session_id,
            identifier,
            reason,
            now,
        )? {
            return Err(LabError::Conflict(
                "reserved order changed before its pre-send rejection CAS".into(),
            ));
        }
        let view = load_order_view(&transaction, identifier)?
            .ok_or_else(|| LabError::DataCorrupt("rejected order readback missing".into()))?;
        transaction.commit().map_err(sql_error)?;
        Ok(view)
    }

    /// Release a claim only for one of the definitive local pre-send gates.
    pub(crate) fn automation_reject_claimed_order_before_send_with_reason(
        &mut self,
        identifier: &AutomationOrderIdentifier,
        now: UtcTimestamp,
        reason: &str,
    ) -> Result<(), LabError> {
        if !matches!(
            reason,
            "LIVE_ORDER_SENDING_DISABLED" | "PRIVATE_ACCOUNT_RECOVERY_REQUIRED"
        ) {
            return Err(LabError::InvalidConfig(
                "invalid pre-send rejection reason".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let row = transaction
            .query_row(
                "SELECT session_id,intent_id,state,venue_order_uuid FROM automation_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation order identifier".into()))?;
        if row.2 != "SUBMITTED_UNKNOWN" || row.3.is_some() {
            return Err(LabError::Conflict(
                "only a claimed order with a definitive pre-send gate denial may release its reservation".into(),
            ));
        }
        let session_id = crate::contracts::AutomationSessionId::new(row.0)?;
        let detail_json = serde_json::to_string(&serde_json::json!({
            "reason": reason,
            "pre_send": true,
        }))
        .map_err(json_error)?;
        let changed = transaction
            .execute(
                "UPDATE automation_orders SET state='REJECTED',detail_json=?1,updated_at_utc=?2 \
                 WHERE client_identifier=?3 AND state='SUBMITTED_UNKNOWN' AND venue_order_uuid IS NULL",
                params![detail_json, now.to_rfc3339(), identifier.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "claimed order changed before its definitive pre-send rejection".into(),
            ));
        }
        let released = transaction
            .execute(
                "UPDATE automation_risk_reservations SET state='RELEASED',released_at_utc=?1 \
                 WHERE client_identifier=?2 AND state='ACTIVE'",
                params![now.to_rfc3339(), identifier.as_str()],
            )
            .map_err(sql_error)?;
        if released != 1 {
            return Err(LabError::DataCorrupt(
                "pre-send gate denial has no unique active risk reservation".into(),
            ));
        }
        let intent_changed = transaction
            .execute(
                "UPDATE automation_intents SET state='REJECTED' WHERE intent_id=?1 AND state='ORDER_SUBMITTED'",
                [&row.1],
            )
            .map_err(sql_error)?;
        if intent_changed != 1 {
            return Err(LabError::DataCorrupt(
                "pre-send gate denial has no submitted strategy intent".into(),
            ));
        }
        insert_event(
            &transaction,
            &session_id,
            next_event_sequence(&transaction, &session_id)?,
            AutomationEventKind::IntentRejected,
            now,
            serde_json::json!({
                "intent_id": row.1,
                "client_identifier": identifier,
                "reason": reason,
                "pre_send": true,
            }),
        )?;
        transaction.commit().map_err(sql_error)
    }

    /// Persist the send claim before invoking POST. Only a RESERVED order can claim;
    /// all later states require readback and cannot be resent.
    #[cfg(test)]
    pub(crate) fn automation_claim_order_for_send(
        &mut self,
        identifier: &AutomationOrderIdentifier,
        fresh_rules: &VerifiedMarketRules,
        now: UtcTimestamp,
    ) -> Result<AutomationOrderDispatch, LabError> {
        self.automation_claim_order_for_send_with_policy(
            identifier,
            fresh_rules,
            now,
            LiveExecutionPolicy::Disabled,
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one claim transaction rechecks rules, scope, reservations and recovery before marking the send unknown"
    )]
    pub(crate) fn automation_claim_order_for_send_with_policy(
        &mut self,
        identifier: &AutomationOrderIdentifier,
        fresh_rules: &VerifiedMarketRules,
        now: UtcTimestamp,
        live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationOrderDispatch, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let row = transaction
            .query_row(
                "SELECT o.session_id,o.account_identity,o.state,o.order_request_json,o.decision_mark_krw, \
                        s.mode,s.state,s.risk_json,s.last_finalized_candle_at_utc,o.rules_snapshot_json, \
                        o.requested_notional_krw,o.reserved_fee_krw,o.side,o.market \
                 FROM automation_orders o JOIN automation_sessions s USING(session_id) \
                 WHERE o.client_identifier=?1",
                [identifier.as_str()],
                |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?,
                    row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,String>(5)?,
                    row.get::<_,String>(6)?,row.get::<_,String>(7)?,row.get::<_,Option<String>>(8)?,
                    row.get::<_,String>(9)?,row.get::<_,String>(10)?,row.get::<_,String>(11)?,
                    row.get::<_,String>(12)?,row.get::<_,String>(13)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation order identifier".into()))?;
        if row.2 != "RESERVED" {
            return Err(LabError::Conflict(
                "order is already claimed or requires identifier readback; resend is forbidden"
                    .into(),
            ));
        }
        let mode = parse_mode(&row.5)?;
        if mode == AutomationSessionMode::Live {
            ensure_live_order_control_enabled(&transaction)?;
        }
        if row.6 != "RUNNING" {
            return Err(LabError::Conflict(
                "automation session is not running".into(),
            ));
        }
        if mode == AutomationSessionMode::Live && !live_execution_policy.is_enabled() {
            return Err(LabError::BlockedEvidence("LIVE_TRADING_DISABLED".into()));
        }
        let risk: AutomationRiskPolicy = serde_json::from_str(&row.7).map_err(json_error)?;
        let request: AutomationOrderRequest = serde_json::from_str(&row.3).map_err(json_error)?;
        request.validate()?;
        let _: VerifiedMarketRules = serde_json::from_str(&row.9).map_err(json_error)?;
        let side = parse_side(&row.12)?;
        let requested_notional = parse_decimal(&row.10)?;
        let reserved_fee = parse_decimal(&row.11)?;
        let current_rule_fee = match side {
            crate::contracts::AutomationSide::Bid => fresh_rules.bid_fee,
            crate::contracts::AutomationSide::Ask => fresh_rules.ask_fee,
        };
        let required_fee = requested_notional
            .checked_mul(current_rule_fee)
            .ok_or_else(|| {
                LabError::AccountingInvariant("fresh order fee calculation overflow".into())
            })?;
        let supported = match (&request.side, &request.order) {
            (crate::contracts::AutomationSide::Bid, AutomationOrderKind::MarketBuy { .. }) => {
                fresh_rules.bid_types.iter().any(|kind| kind == "price")
            }
            (crate::contracts::AutomationSide::Ask, AutomationOrderKind::MarketSell { .. }) => {
                fresh_rules.ask_types.iter().any(|kind| kind == "market")
            }
            (crate::contracts::AutomationSide::Bid, AutomationOrderKind::Limit { .. }) => {
                fresh_rules.bid_types.iter().any(|kind| kind == "limit")
            }
            (crate::contracts::AutomationSide::Ask, AutomationOrderKind::Limit { .. }) => {
                fresh_rules.ask_types.iter().any(|kind| kind == "limit")
            }
            _ => false,
        };
        let minimum = min_total_for(fresh_rules, side);
        if fresh_rules.market != row.13
            || fresh_rules.source_mode != mode
            || fresh_rules.state != "active"
            || fresh_rules.observed_at_utc > now
            || (now.0 - fresh_rules.observed_at_utc.0).num_seconds()
                > i64::from(risk.max_balance_age_seconds)
            || fresh_rules.max_total <= Decimal::ZERO
            || fresh_rules.bid_fee < Decimal::ZERO
            || fresh_rules.ask_fee < Decimal::ZERO
            || fresh_rules.bid_fee > Decimal::new(1, 1)
            || fresh_rules.ask_fee > Decimal::new(1, 1)
            || !supported
            || request.market.code() != fresh_rules.market
            || request.side != side
            || requested_notional < minimum
            || requested_notional > fresh_rules.max_total
            || requested_notional > risk.max_order_notional_krw.get()
            || required_fee > reserved_fee
        {
            return Err(LabError::UnverifiedMarketRules(
                "fresh exchange rules no longer admit the reserved order or its upfront fee".into(),
            ));
        }
        if let AutomationOrderKind::Limit { price, .. } = &request.order {
            let price_unit = match side {
                crate::contracts::AutomationSide::Bid => fresh_rules.bid_price_unit,
                crate::contracts::AutomationSide::Ask => fresh_rules.ask_price_unit,
            }
            .filter(|unit| *unit > Decimal::ZERO)
            .ok_or_else(|| {
                LabError::UnverifiedMarketRules(
                    "fresh venue response has no verified limit price unit".into(),
                )
            })?;
            if price.get() % price_unit != Decimal::ZERO {
                return Err(LabError::UnverifiedMarketRules(
                    "reserved limit price no longer aligns to the current venue tick".into(),
                ));
            }
        }
        let account = transaction
            .query_row(
                "SELECT observed_at_utc,source_mode,balances_json FROM automation_account_snapshots \
                 WHERE account_identity=?1 ORDER BY snapshot_sequence DESC LIMIT 1",
                [&row.1],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::BlockedEvidence("fresh account snapshot is unavailable".into())
            })?;
        let account_at = UtcTimestamp::parse_rfc3339(&account.0)?;
        if parse_mode(&account.1)? != mode
            || account_at > now
            || (now.0 - account_at.0).num_seconds() > i64::from(risk.max_balance_age_seconds)
        {
            return Err(LabError::BlockedEvidence(
                "account snapshot became stale before send".into(),
            ));
        }
        let balances: Vec<UpbitAccountBalance> =
            serde_json::from_str(&account.2).map_err(json_error)?;
        validate_account_balance_ledger(&transaction, &row.1, account_at, &balances)?;
        let feed_at = row
            .8
            .map(|value| UtcTimestamp::parse_rfc3339(&value))
            .transpose()?
            .ok_or_else(|| {
                LabError::BlockedEvidence("finalized feed timestamp is unavailable".into())
            })?;
        if feed_at > now || (now.0 - feed_at.0).num_seconds() > i64::from(risk.max_feed_age_seconds)
        {
            return Err(LabError::BlockedEvidence(
                "finalized feed became stale before send".into(),
            ));
        }
        if mode == AutomationSessionMode::Live {
            let session_id = crate::contracts::AutomationSessionId::new(row.0.clone())?;
            let view =
                load_view_with_policy(&transaction, &session_id, now, live_execution_policy)?
                    .ok_or_else(|| {
                        LabError::DataCorrupt("LIVE send claim lost its session projection".into())
                    })?;
            let projection = view.account_risk.as_ref().ok_or_else(|| {
                LabError::BlockedEvidence("LIVE send claim has no account risk projection".into())
            })?;
            if !projection.risk.live_grant_active || projection.effective_loss.admission_blocked {
                return Err(LabError::BlockedEvidence(
                    "LIVE send claim requires an active durable grant and verified activation loss reference".into(),
                ));
            }
        }
        let emergency: bool = transaction
            .query_row(
                if mode == AutomationSessionMode::Live {
                    "SELECT emergency_latched OR order_admission_blocked FROM trading_control_state WHERE singleton=1"
                } else {
                    "SELECT emergency_latched FROM trading_control_state WHERE singleton=1"
                },
                [],
                |r| r.get(0),
            )
            .map_err(sql_error)?;
        if emergency {
            return Err(LabError::BlockedEvidence(
                "durable emergency/order-admission latch is active".into(),
            ));
        }
        let unresolved_account_orders: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 AND client_identifier!=?2 \
                 AND state IN ('SUBMITTED_UNKNOWN','NEEDS_RECONCILIATION')",
                params![row.1, identifier.as_str()],
                |query| query.get(0),
            )
            .map_err(sql_error)?;
        if unresolved_account_orders != 0 {
            return Err(LabError::BlockedEvidence(
                "another order on the shared account still requires identifier readback".into(),
            ));
        }
        let session_id = crate::contracts::AutomationSessionId::new(row.0.clone())?;
        let live_view =
            load_view_with_policy(&transaction, &session_id, now, live_execution_policy)?
                .ok_or_else(|| LabError::DataCorrupt("claimed session disappeared".into()))?;
        let live_risk = live_view.account_risk.ok_or_else(|| {
            LabError::BlockedEvidence("account risk projection is incomplete before send".into())
        })?;
        let activation_loss_selected =
            mode == AutomationSessionMode::Live && risk.activation_loss_limit_krw.is_some();
        if live_risk.balance.freshness != AutomationFreshness::Fresh
            || live_risk.feed.freshness != AutomationFreshness::Fresh
            || live_risk
                .risk
                .daily_notional_used_krw
                .is_none_or(|used| used > risk.max_daily_notional_krw)
            || live_risk.risk.open_order_count > risk.max_open_orders
            || live_risk
                .risk
                .orders_in_interval
                .is_none_or(|count| count > risk.max_orders_per_interval)
            || (!activation_loss_selected
                && live_risk
                    .risk
                    .daily_loss_used_krw
                    .is_none_or(|loss| !daily_loss_is_below_limit(loss, risk.max_daily_loss_krw)))
        {
            return Err(LabError::BlockedEvidence(
                "shared account risk limits or freshness changed before the order send claim"
                    .into(),
            ));
        }
        let (_, _, reserved_bid_exposure) =
            reservation_totals_for_admission(&transaction, &row.1, request.market.base.code())?;
        let current_exposure = live_risk.risk.asset_exposure_krw.ok_or_else(|| {
            LabError::BlockedEvidence("current per-asset exposure is unknown before send".into())
        })?;
        let projected_exposure = match side {
            crate::contracts::AutomationSide::Bid => current_exposure
                .get()
                .checked_add(reserved_bid_exposure)
                .ok_or_else(|| {
                    LabError::AccountingInvariant("reserved buy exposure overflow".into())
                })?,
            crate::contracts::AutomationSide::Ask => {
                let volume = match &request.order {
                    AutomationOrderKind::MarketSell { volume }
                    | AutomationOrderKind::Limit { volume, .. } => volume.get(),
                    AutomationOrderKind::MarketBuy { .. } => Decimal::ZERO,
                };
                let reduction = volume.checked_mul(parse_decimal(&row.4)?).ok_or_else(|| {
                    LabError::AccountingInvariant("reserved sell exposure overflow".into())
                })?;
                current_exposure
                    .get()
                    .checked_sub(reduction.min(current_exposure.get()))
                    .and_then(|value| value.checked_add(reserved_bid_exposure))
                    .ok_or_else(|| {
                        LabError::AccountingInvariant("reserved sell exposure underflow".into())
                    })?
            }
        };
        if projected_exposure > risk.max_asset_exposure_krw.get() {
            return Err(LabError::BlockedEvidence(
                "shared account asset exposure exceeds the frozen limit before send".into(),
            ));
        }
        let fresh_rules_json = serde_json::to_string(fresh_rules).map_err(json_error)?;
        let changed = transaction
            .execute(
                "UPDATE automation_orders SET state='SUBMITTED_UNKNOWN',rules_snapshot_json=?1,updated_at_utc=?2 \
                 WHERE client_identifier=?3 AND state='RESERVED'",
                params![fresh_rules_json,now.to_rfc3339(), identifier.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "order send claim lost its compare-and-swap".into(),
            ));
        }
        transaction
            .execute(
                "UPDATE automation_intents SET state='ORDER_SUBMITTED' WHERE intent_id=( \
                    SELECT intent_id FROM automation_orders WHERE client_identifier=?1) \
                 AND state='ORDER_RESERVED'",
                [identifier.as_str()],
            )
            .map_err(sql_error)?;
        insert_event(
            &transaction,
            &session_id,
            next_event_sequence(&transaction, &session_id)?,
            AutomationEventKind::OrderSendClaimed,
            now,
            serde_json::json!({"client_identifier":identifier,"outcome":"UNKNOWN_UNTIL_READBACK",
                "current_rules_digest":ContentHash::of_value(fresh_rules)?}),
        )?;
        let decision_mark = crate::contracts::PriceKrw::new(parse_decimal(&row.4)?)?;
        transaction.commit().map_err(sql_error)?;
        Ok(AutomationOrderDispatch {
            request,
            decision_mark,
        })
    }

    /// Durably claim one explicit mock cancellation. The order enters readback-only
    /// reconciliation before DELETE; later calls return `should_send=false`.
    #[cfg(test)]
    pub(crate) fn automation_prepare_cancel(
        &mut self,
        write: &AutomationSessionWrite,
        now: UtcTimestamp,
    ) -> Result<AutomationCancelPreparation, LabError> {
        self.automation_prepare_cancel_with_policy(write, now, LiveExecutionPolicy::Disabled)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "cancel CAS, request claim and reservation state form one durable pre-effect transaction"
    )]
    pub(crate) fn automation_prepare_cancel_with_policy(
        &mut self,
        write: &AutomationSessionWrite,
        now: UtcTimestamp,
        _live_execution_policy: LiveExecutionPolicy,
    ) -> Result<AutomationCancelPreparation, LabError> {
        let (request_id, session_id, expected_sequence, identifier) = match write {
            AutomationSessionWrite::CancelOrder {
                request_id,
                session_id,
                expected_sequence,
                client_identifier,
            } => (
                request_id,
                session_id,
                *expected_sequence,
                client_identifier,
            ),
            _ => {
                return Err(LabError::InvalidConfig(
                    "cancel preparation requires cancel_order".into(),
                ));
            }
        };
        let digest = request_digest(write, None)?;
        let prior = self.automation_readback_request(request_id)?;
        match prior.state {
            crate::contracts::AutomationRequestReadbackState::Applied => {
                let response = prior.response.ok_or_else(|| {
                    LabError::DataCorrupt("applied cancel request omitted its response".into())
                })?;
                let venue_uuid = response
                    .order
                    .as_ref()
                    .and_then(|order| order.venue_order_uuid.clone());
                return Ok(AutomationCancelPreparation {
                    response,
                    venue_uuid,
                    should_send: false,
                });
            }
            crate::contracts::AutomationRequestReadbackState::Rejected => {
                return Err(LabError::Conflict(prior.reason.unwrap_or_else(|| {
                    "cancel request was previously rejected".into()
                })));
            }
            crate::contracts::AutomationRequestReadbackState::NotRecorded => {
                return Err(LabError::OutcomeUnknown(
                    "cancel request is not durably recorded; do not infer that no cancel was sent"
                        .into(),
                ));
            }
            crate::contracts::AutomationRequestReadbackState::Pending => {
                if let Some(response) = prior.response {
                    let venue_uuid = response
                        .order
                        .as_ref()
                        .and_then(|order| order.venue_order_uuid.clone());
                    return Ok(AutomationCancelPreparation {
                        response,
                        venue_uuid,
                        should_send: false,
                    });
                }
            }
        }

        let transaction = self.connection.transaction().map_err(sql_error)?;
        let request_row = transaction
            .query_row(
                "SELECT payload_digest,action,state FROM automation_session_requests WHERE request_id=?1",
                [request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::OutcomeUnknown("cancel request disappeared before claim".into()))?;
        if request_row.0 != digest.as_str()
            || request_row.1 != "CANCEL_ORDER"
            || request_row.2 != "PENDING"
        {
            return Err(LabError::Conflict(
                "cancel request identity or state changed before claim".into(),
            ));
        }
        let session_row = transaction
            .query_row(
                "SELECT mode,state,sequence FROM automation_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig("unknown automation session for cancellation".into())
            })?;
        let mode = parse_mode(&session_row.0)?;
        if mode == AutomationSessionMode::Live {
            return Err(LabError::BlockedEvidence(
                "LIVE_CANCELLATION_UNSUPPORTED".into(),
            ));
        }
        let sequence = from_sql_u64(session_row.2)?;
        if sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "automation session sequence conflict for {session_id}: expected {expected_sequence}, current {sequence}"
            )));
        }
        if !matches!(session_row.1.as_str(), "RUNNING" | "PAUSED" | "STOPPED") {
            return Err(LabError::Conflict(
                "cancel requires a running, paused, or stopped approved session with reconciled account state".into(),
            ));
        }
        let order_row = transaction
            .query_row(
                "SELECT session_id,state,venue_order_uuid FROM automation_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown owned automation order".into()))?;
        if order_row.0 != session_id.as_str() {
            return Err(LabError::Conflict(
                "automation order is not owned by this session".into(),
            ));
        }
        if !matches!(order_row.1.as_str(), "OPEN" | "PARTIALLY_FILLED") {
            return Err(LabError::Conflict(
                "only an owned OPEN or PARTIALLY_FILLED mock order can be cancelled".into(),
            ));
        }
        let venue_uuid = order_row.2.ok_or_else(|| {
            LabError::BlockedEvidence(
                "owned order has no durable venue UUID for cancellation".into(),
            )
        })?;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("automation session sequence exhausted".into())
        })?;
        let next_session_state = if session_row.1 == "RUNNING" {
            "PAUSED"
        } else {
            &session_row.1
        };
        let changed = transaction
            .execute(
                "UPDATE automation_orders SET state='NEEDS_RECONCILIATION',updated_at_utc=?1 \
                 WHERE client_identifier=?2 AND state IN ('OPEN','PARTIALLY_FILLED')",
                params![now.to_rfc3339(), identifier.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "cancel order claim lost its compare-and-swap".into(),
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE automation_sessions SET state=?1,sequence=?2,updated_at_utc=?3 \
                 WHERE session_id=?4 AND sequence=?5",
                params![
                    next_session_state,
                    to_sql_u64(next_sequence)?,
                    now.to_rfc3339(),
                    session_id.as_str(),
                    to_sql_u64(expected_sequence)?
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "cancel session sequence compare-and-swap failed".into(),
            ));
        }
        if session_row.1 == "RUNNING" {
            insert_event(
                &transaction,
                session_id,
                next_event_sequence(&transaction, session_id)?,
                AutomationEventKind::Paused,
                now,
                serde_json::json!({"reason":"EXPLICIT_CANCEL_AWAITING_READBACK","client_identifier":identifier}),
            )?;
        }
        insert_event(
            &transaction,
            session_id,
            next_event_sequence(&transaction, session_id)?,
            AutomationEventKind::OrderCancelRequested,
            now,
            serde_json::json!({"client_identifier":identifier,"outcome":"PENDING_IDENTIFIER_READBACK"}),
        )?;
        let session = load_view(&transaction, session_id, now)?.ok_or_else(|| {
            LabError::DataCorrupt("cancelled session disappeared before readback".into())
        })?;
        let order = load_order_view(&transaction, identifier)?.ok_or_else(|| {
            LabError::DataCorrupt("cancelled order disappeared before readback".into())
        })?;
        let response = AutomationSessionCommandResponse {
            session,
            receipt: AutomationSessionReceipt {
                request_id: request_id.clone(),
                action: AutomationAction::CancelOrder,
                outcome: AutomationCommandOutcome::Pending,
                sequence: next_sequence,
                observed_at_utc: now,
                reason: Some("cancellation outcome requires order-identifier readback".into()),
            },
            order: Some(order),
            baseline: None,
        };
        let updated = transaction
            .execute(
                "UPDATE automation_session_requests SET session_id=?1,response_json=?2,reason=?3 \
                 WHERE request_id=?4 AND payload_digest=?5 AND state='PENDING' AND response_json IS NULL",
                params![session_id.as_str(), serde_json::to_string(&response).map_err(json_error)?,
                    "cancel send outcome is pending; read back by client identifier", request_id.as_str(), digest.as_str()],
            )
            .map_err(sql_error)?;
        if updated != 1 {
            return Err(LabError::Conflict(
                "cancel request claim already has a durable outcome".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(AutomationCancelPreparation {
            response,
            venue_uuid: Some(venue_uuid),
            should_send: true,
        })
    }

    /// Reconcile the identifier after DELETE and finalize the request only when
    /// terminal venue state proves that cancellation or a competing fill won.
    #[allow(clippy::too_many_lines)] // Terminal order reconciliation, cancellation receipt and reservation release are one transaction.
    pub(crate) fn automation_finalize_cancel(
        &mut self,
        request_id: &RequestId,
        detail: &UpbitOrderDetail,
        now: UtcTimestamp,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let readback = self.automation_readback_request(request_id)?;
        if readback.state == crate::contracts::AutomationRequestReadbackState::Applied {
            return readback.response.ok_or_else(|| {
                LabError::DataCorrupt("applied cancel request omitted its response".into())
            });
        }
        if readback.state != crate::contracts::AutomationRequestReadbackState::Pending
            || readback.response.is_none()
        {
            return Err(LabError::Conflict(
                "cancel request has no durable cancel claim to finalize".into(),
            ));
        }
        let request_json: String = self
            .connection
            .query_row(
                "SELECT request_json FROM automation_session_requests WHERE request_id=?1",
                [request_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let write: AutomationSessionWrite =
            serde_json::from_str(&request_json).map_err(json_error)?;
        let AutomationSessionWrite::CancelOrder {
            session_id,
            client_identifier: identifier,
            ..
        } = &write
        else {
            return Err(LabError::DataCorrupt(
                "cancel request payload has a different action".into(),
            ));
        };
        if detail.identifier.as_deref() != Some(identifier.as_str()) {
            return Err(LabError::DataCorrupt(
                "cancel readback identifier does not match the durable request".into(),
            ));
        }
        let order = self.automation_reconcile_order(detail, now)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let session = load_view(&transaction, session_id, now)?.ok_or_else(|| {
            LabError::DataCorrupt("cancel session missing during finalization".into())
        })?;
        let terminal = matches!(
            order.state,
            AutomationOrderState::Cancelled
                | AutomationOrderState::Filled
                | AutomationOrderState::Rejected
        );
        let outcome = match order.state {
            AutomationOrderState::Cancelled => AutomationCommandOutcome::Applied,
            AutomationOrderState::Filled | AutomationOrderState::Rejected => {
                AutomationCommandOutcome::NoOp
            }
            _ => AutomationCommandOutcome::Pending,
        };
        let reason = if terminal {
            match order.state {
                AutomationOrderState::Filled => {
                    Some("order filled before cancellation was confirmed".into())
                }
                AutomationOrderState::Rejected => Some("venue reports the order rejected".into()),
                _ => None,
            }
        } else {
            Some("venue order remains nonterminal; continue readback without another DELETE".into())
        };
        let response = AutomationSessionCommandResponse {
            receipt: AutomationSessionReceipt {
                request_id: request_id.clone(),
                action: AutomationAction::CancelOrder,
                outcome,
                sequence: session.sequence,
                observed_at_utc: now,
                reason: reason.clone(),
            },
            session: session.clone(),
            order: Some(order),
            baseline: None,
        };
        let changed = if terminal {
            transaction.execute(
                "UPDATE automation_session_requests SET state='APPLIED',response_json=?1,reason=NULL \
                 WHERE request_id=?2 AND state='PENDING'",
                params![serde_json::to_string(&response).map_err(json_error)?, request_id.as_str()],
            )
        } else {
            transaction.execute(
                "UPDATE automation_session_requests SET response_json=?1,reason=?2 \
                 WHERE request_id=?3 AND state='PENDING'",
                params![serde_json::to_string(&response).map_err(json_error)?, reason, request_id.as_str()],
            )
        }
        .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::OutcomeUnknown(
                "cancel finalization lost durable request state".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(response)
    }

    pub(crate) fn automation_mark_order_needs_reconciliation(
        &mut self,
        identifier: &AutomationOrderIdentifier,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        if reason.is_empty() || reason.len() > 2_048 {
            return Err(LabError::InvalidConfig(
                "reconciliation reason exceeds bounds".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let order = transaction
            .query_row(
                "SELECT o.session_id,o.state,s.state,s.recovery_reason FROM automation_orders o \
                 JOIN automation_sessions s USING(session_id) WHERE o.client_identifier=?1",
                [identifier.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown automation order identifier".into()))?;
        transaction
            .execute(
                "UPDATE automation_orders SET state='NEEDS_RECONCILIATION',updated_at_utc=?1 \
                 WHERE client_identifier=?2 AND state NOT IN ('FILLED','CANCELLED','REJECTED','NEEDS_RECONCILIATION')",
                params![now.to_rfc3339(), identifier.as_str()],
            )
            .map_err(sql_error)?;
        let changed: u32 = transaction
            .query_row("SELECT changes()", [], |row| row.get(0))
            .map_err(sql_error)?;
        if changed == 1 {
            let session_id = crate::contracts::AutomationSessionId::new(order.0)?;
            // Order read failures get one typed recoverable cause. Preserve any
            // existing restart/gap/manual cause, and never turn user-paused,
            // stopped, or blocked control state back into RUNNING implicitly.
            if order.2 == "RUNNING" {
                transaction
                    .execute(
                        "UPDATE automation_sessions SET state='NEEDS_RECONCILIATION',recovery_reason=?1,updated_at_utc=?2 \
                         WHERE session_id=?3 AND state='RUNNING'",
                        params!["ORDER_READBACK_REQUIRED", now.to_rfc3339(), session_id.as_str()],
                    )
                    .map_err(sql_error)?;
            } else if order.2 == "NEEDS_RECONCILIATION" && order.3.is_none() {
                transaction
                    .execute(
                        "UPDATE automation_sessions SET recovery_reason=?1,updated_at_utc=?2 \
                         WHERE session_id=?3 AND state='NEEDS_RECONCILIATION' AND recovery_reason IS NULL",
                        params!["ORDER_READBACK_REQUIRED", now.to_rfc3339(), session_id.as_str()],
                    )
                    .map_err(sql_error)?;
            } else if matches!(
                order.2.as_str(),
                "PAUSED" | "STOPPED" | "BLOCKED_APPROVAL" | "FAULTED"
            ) {
                transaction
                    .execute(
                        "UPDATE automation_sessions SET recovery_reason=COALESCE(recovery_reason,?1),updated_at_utc=?2 \
                         WHERE session_id=?3 AND state=?4",
                        params!["ORDER_READBACK_REQUIRED", now.to_rfc3339(), session_id.as_str(), order.2],
                    )
                    .map_err(sql_error)?;
            }
            insert_event(
                &transaction,
                &session_id,
                next_event_sequence(&transaction, &session_id)?,
                AutomationEventKind::NeedsReconciliation,
                now,
                serde_json::json!({
                    "client_identifier":identifier,
                    "reason":reason,
                    "recovery_cause":"ORDER_READBACK_REQUIRED",
                    "previous_recovery_reason":order.3
                }),
            )?;
        }
        transaction.commit().map_err(sql_error)
    }

    /// Apply one strict identifier readback, append unique trades and book only the
    /// positive cumulative paid-fee delta. A reservation is released only on terminal proof.
    #[allow(clippy::too_many_lines)] // Unique fills, cumulative fee deltas, state and reservation settlement commit together.
    pub(crate) fn automation_reconcile_order(
        &mut self,
        detail: &UpbitOrderDetail,
        now: UtcTimestamp,
    ) -> Result<AutomationOrderView, LabError> {
        let identifier = detail.identifier.as_deref().ok_or_else(|| {
            LabError::DataCorrupt("order readback omitted client identifier".into())
        })?;
        let identifier = AutomationOrderIdentifier::new(identifier.to_owned())?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let row = transaction
            .query_row(
                "SELECT o.session_id,o.order_request_json,o.state,o.venue_order_uuid, \
                        o.executed_volume,o.paid_fee_krw,o.trades_count,o.detail_json,o.intent_id, \
                        o.remaining_volume,o.reserved_fee_krw,o.remaining_fee_krw,o.locked_amount,o.locked_currency \
                 FROM automation_orders o WHERE o.client_identifier=?1",
                [identifier.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, String>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, String>(13)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig("order readback has no durable local intent".into())
            })?;
        let request: AutomationOrderRequest = serde_json::from_str(&row.1).map_err(json_error)?;
        if request.client_identifier != identifier {
            return Err(LabError::DataCorrupt(
                "stored order request identifier mismatch".into(),
            ));
        }
        let update = validate_order_update(&request, detail, now)?;
        if row.3.as_ref().is_some_and(|uuid| uuid != &detail.uuid) {
            return Err(LabError::DataCorrupt(
                "venue UUID changed for one client identifier".into(),
            ));
        }
        let previous_volume = parse_decimal(&row.4)?;
        let previous_fee = parse_decimal(&row.5)?;
        let previous_count = from_sql_u64(row.6)?;
        let executed = update.executed_volume.get();
        let paid_fee = update.paid_fee.get();
        if executed < previous_volume
            || paid_fee < previous_fee
            || update.trade_count < previous_count
        {
            return Err(LabError::DataCorrupt(
                "venue order readback regressed cumulative volume, fee, or trade count".into(),
            ));
        }
        let previous_state = parse_order_state(&row.2)?;
        if previous_state == AutomationOrderState::Rejected {
            return Err(LabError::DataCorrupt(
                "venue returned an order for an intent already durably rejected".into(),
            ));
        }
        let session_id = crate::contracts::AutomationSessionId::new(row.0)?;
        let mut inserted_fills = 0_u64;
        for fill in &update.fills {
            let source_digest = ContentHash::of_value(fill)?.to_string();
            let existing = transaction
                .query_row(
                    "SELECT client_identifier,market,side,price_krw,volume,funds_krw,trade_time_utc,source_digest \
                     FROM automation_fills WHERE trade_uuid=?1",
                    [&fill.trade_uuid],
                    |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?,
                        row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,String>(5)?,
                        row.get::<_,String>(6)?,row.get::<_,String>(7)?)),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(existing) = existing {
                if existing.0 != identifier.as_str()
                    || existing.1 != fill.market.code()
                    || existing.2 != side_name(fill.side)
                    || existing.3 != fill.price_krw.get().to_string()
                    || existing.4 != fill.volume.get().to_string()
                    || existing.5 != fill.funds_krw.get().to_string()
                    || existing.6 != fill.observed_at_utc.to_rfc3339()
                    || existing.7 != source_digest
                {
                    return Err(LabError::DataCorrupt(
                        "venue trade UUID conflicts with an existing durable fill".into(),
                    ));
                }
                continue;
            }
            transaction
                .execute(
                    "INSERT INTO automation_fills(trade_uuid,client_identifier,session_id,market,side, \
                         price_krw,volume,funds_krw,trade_time_utc,source_digest,created_at_utc) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    params![
                        fill.trade_uuid,
                        identifier.as_str(),
                        session_id.as_str(),
                        fill.market.code(),
                        side_name(fill.side),
                        fill.price_krw.get().to_string(),
                        fill.volume.get().to_string(),
                        fill.funds_krw.get().to_string(),
                        fill.observed_at_utc.to_rfc3339(),
                        source_digest,
                        now.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
            inserted_fills = inserted_fills
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("automation fill count overflow".into()))?;
        }
        let stored_fill_facts = read_order_fill_facts(&transaction, &identifier)?;
        if stored_fill_facts.0 != update.trade_count || stored_fill_facts.1 != executed {
            return Err(LabError::DataCorrupt(
                "durable unique fill ledger differs from venue cumulative count/volume".into(),
            ));
        }
        let fee_delta = paid_fee - previous_fee;
        if fee_delta > Decimal::ZERO {
            let previous_sequence: Option<i64> = transaction
                .query_row(
                    "SELECT MAX(sequence) FROM automation_fee_accruals WHERE client_identifier=?1",
                    [identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let sequence = previous_sequence
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    LabError::ResourceLimit("automation fee sequence overflow".into())
                })?;
            transaction
                .execute(
                    "INSERT INTO automation_fee_accruals(client_identifier,sequence,delta_paid_fee_krw, \
                         cumulative_paid_fee_krw,observed_at_utc) VALUES (?1,?2,?3,?4,?5)",
                    params![identifier.as_str(),sequence,fee_delta.to_string(),paid_fee.to_string(),now.to_rfc3339()],
                )
                .map_err(sql_error)?;
        }
        let merged_state =
            merge_order_state(previous_state, update.state, previous_volume, executed)?;
        let state_text = order_state_name(merged_state);
        let detail_json = serde_json::to_string(detail).map_err(json_error)?;
        let observed_remaining = update
            .remaining_volume
            .map(crate::contracts::AssetQuantity::get);
        let previous_remaining = row.9.as_deref().map(parse_decimal).transpose()?;
        let readback_changed = merged_state != previous_state
            || row.3.as_deref() != Some(update.venue_uuid.as_str())
            || previous_volume != executed
            || previous_fee != paid_fee
            || previous_count != update.trade_count
            || previous_remaining != observed_remaining
            || parse_decimal(&row.10)? != update.reserved_fee.get()
            || parse_decimal(&row.11)? != update.remaining_fee.get()
            || parse_decimal(&row.12)? != update.locked_amount
            || row.13 != update.locked_currency;
        if !readback_changed && inserted_fills == 0 && fee_delta == Decimal::ZERO {
            let view = load_order_view(&transaction, &identifier)?
                .ok_or_else(|| LabError::DataCorrupt("reconciled order is missing".into()))?;
            transaction.commit().map_err(sql_error)?;
            return Ok(view);
        }
        transaction
            .execute(
                "UPDATE automation_orders SET state=?1,venue_order_uuid=COALESCE(venue_order_uuid,?2), \
                     executed_volume=?3,remaining_volume=?4,reserved_fee_krw=?5,remaining_fee_krw=?6, \
                     paid_fee_krw=?7,locked_amount=?8,locked_currency=?9,trades_count=?10, \
                     venue_created_at_utc=COALESCE(venue_created_at_utc,?11),updated_at_utc=?12,detail_json=?13 \
                 WHERE client_identifier=?14",
                params![
                    state_text,
                    detail.uuid,
                    executed.to_string(),
                    observed_remaining.map(|volume| volume.to_string()),
                    update.reserved_fee.get().to_string(),
                    update.remaining_fee.get().to_string(),
                    paid_fee.to_string(),
                    update.locked_amount.to_string(),
                    update.locked_currency,
                    to_sql_u64(update.trade_count)?,
                    detail.created_at,
                    now.to_rfc3339(),
                    detail_json,
                    identifier.as_str(),
                ],
            )
            .map_err(sql_error)?;
        if matches!(
            merged_state,
            AutomationOrderState::Filled
                | AutomationOrderState::Cancelled
                | AutomationOrderState::Rejected
        ) {
            transaction
                .execute(
                    "UPDATE automation_risk_reservations SET state='RELEASED',released_at_utc=?1 \
                     WHERE client_identifier=?2 AND state='ACTIVE'",
                    params![now.to_rfc3339(), identifier.as_str()],
                )
                .map_err(sql_error)?;
            transaction
                .execute(
                    "UPDATE automation_intents SET state='ORDER_RECONCILED' WHERE intent_id=?1",
                    [row.8.as_str()],
                )
                .map_err(sql_error)?;
        }
        insert_event(
            &transaction,
            &session_id,
            next_event_sequence(&transaction, &session_id)?,
            AutomationEventKind::OrderReconciled,
            now,
            serde_json::json!({"client_identifier":identifier,"state":state_text,
                "executed_volume":update.executed_volume,"paid_fee_krw":update.paid_fee,"fee_delta_krw":fee_delta}),
        )?;
        if inserted_fills > 0 {
            insert_event(
                &transaction,
                &session_id,
                next_event_sequence(&transaction, &session_id)?,
                AutomationEventKind::FillRecorded,
                now,
                serde_json::json!({"client_identifier":identifier,"new_trade_count":inserted_fills}),
            )?;
        }
        let view = load_order_view(&transaction, &identifier)?
            .ok_or_else(|| LabError::DataCorrupt("reconciled order is missing".into()))?;
        transaction.commit().map_err(sql_error)?;
        Ok(view)
    }

    /// Initialize the durable mock venue exactly once for this database root.
    pub(crate) fn automation_seed_mock_venue(&mut self, now: UtcTimestamp) -> Result<(), LabError> {
        let exists = self
            .connection
            .query_row(
                "SELECT 1 FROM automation_mock_venue_seed WHERE singleton=1",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?
            .is_some();
        if exists {
            return Ok(());
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO automation_mock_venue_seed(singleton,seed_version,created_at_utc) VALUES (1,1,?1)",
                [now.to_rfc3339()],
            )
            .map_err(sql_error)?;
        for (currency, balance, unit) in [
            ("KRW", "10000000", "KRW"),
            ("BTC", "0", "KRW"),
            ("ETH", "0", "KRW"),
            ("XRP", "0", "KRW"),
        ] {
            transaction
                .execute(
                    "INSERT INTO automation_mock_venue_accounts(\
                         currency,balance,locked,avg_buy_price,avg_buy_price_modified,unit_currency,updated_at_utc\
                     ) VALUES (?1,?2,'0','0',0,?3,?4)",
                    params![currency, balance, unit, now.to_rfc3339()],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)
    }

    pub(crate) fn automation_mock_venue_accounts(
        &self,
    ) -> Result<Vec<UpbitAccountBalance>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT currency,balance,locked,avg_buy_price,avg_buy_price_modified,unit_currency \
                 FROM automation_mock_venue_accounts ORDER BY currency ASC",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (currency, balance, locked, avg_buy_price, avg_buy_price_modified, unit_currency) =
                row.map_err(sql_error)?;
            Ok(UpbitAccountBalance {
                currency,
                balance: parse_decimal(&balance)?,
                locked: parse_decimal(&locked)?,
                avg_buy_price: parse_decimal(&avg_buy_price)?,
                avg_buy_price_modified,
                unit_currency,
            })
        })
        .collect()
    }

    pub(crate) fn automation_mock_order_chance(
        &self,
        market: &MarketId,
    ) -> Result<UpbitOrderChance, LabError> {
        let code = market.code();
        let base_currency = market.base.code();
        let accounts = self.automation_mock_venue_accounts()?;
        let quote = accounts.iter().find(|row| row.currency == "KRW");
        let base = accounts.iter().find(|row| row.currency == base_currency);
        let account_json = |balance: Option<&UpbitAccountBalance>, currency: &str| match balance {
            Some(row) => {
                serde_json::json!({
                    "currency": row.currency,
                    "balance": row.balance.to_string(),
                    "locked": row.locked.to_string(),
                    "avg_buy_price": row.avg_buy_price.to_string(),
                    "avg_buy_price_modified": row.avg_buy_price_modified,
                    "unit_currency": row.unit_currency
                })
            }
            None => {
                serde_json::json!({
                    "currency":currency,"balance":"0","locked":"0","avg_buy_price":"0",
                    "avg_buy_price_modified":false,"unit_currency":"KRW"
                })
            }
        };
        Ok(UpbitOrderChance {
            bid_fee: Decimal::new(5, 4),
            ask_fee: Decimal::new(5, 4),
            maker_bid_fee: Some(Decimal::new(5, 4)),
            maker_ask_fee: Some(Decimal::new(5, 4)),
            market: serde_json::json!({
                "id": code,
                "name": format!("{base_currency}/KRW"),
                "order_sides": ["bid", "ask"],
                "bid_types": ["limit", "price"],
                "ask_types": ["limit", "market"],
                "bid": {"currency":"KRW","min_total":"5000","price_unit":null},
                "ask": {"currency":base_currency,"min_total":"5000","price_unit":null},
                "max_total":"1000000000",
                "state":"active"
            }),
            bid_account: Some(account_json(quote, "KRW")),
            ask_account: Some(account_json(base, base_currency)),
        })
    }

    /// Persist one mock order effect atomically with the mock account and trade facts.
    /// Reusing an identifier with the same request is a readback; changed payload is a conflict.
    pub(crate) fn automation_mock_submit_order(
        &mut self,
        request: &AutomationOrderRequest,
        decision_mark: crate::contracts::PriceKrw,
        now: UtcTimestamp,
    ) -> Result<UpbitOrderDetail, LabError> {
        request.validate()?;
        let digest = ContentHash::of_value(request)?.to_string();
        let transaction = self.connection.transaction().map_err(sql_error)?;
        if let Some((existing_digest, detail_json)) = transaction
            .query_row(
                "SELECT i.request_digest,o.detail_json FROM automation_mock_venue_identifiers i \
                 JOIN automation_mock_venue_orders o USING(client_identifier) WHERE i.client_identifier=?1",
                [request.client_identifier.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
        {
            if existing_digest != digest {
                return Err(LabError::Conflict(
                    "mock venue identifier was reused with different order terms".into(),
                ));
            }
            return serde_json::from_str(&detail_json).map_err(json_error);
        }
        let seeded = transaction
            .query_row(
                "SELECT 1 FROM automation_mock_venue_seed WHERE singleton=1",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?
            .is_some();
        if !seeded {
            return Err(LabError::DataCorrupt(
                "durable mock venue is not initialized".into(),
            ));
        }
        let result = mock_prepare_and_apply_order(&transaction, request, decision_mark, now)?;
        transaction
            .execute(
                "INSERT INTO automation_mock_venue_identifiers(client_identifier,request_digest,venue_uuid,created_at_utc) \
                 VALUES (?1,?2,?3,?4)",
                params![
                    request.client_identifier.as_str(),
                    digest,
                    result.uuid,
                    now.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        let detail_json = serde_json::to_string(&result).map_err(json_error)?;
        transaction
            .execute(
                "INSERT INTO automation_mock_venue_orders(client_identifier,venue_uuid,detail_json,updated_at_utc) \
                 VALUES (?1,?2,?3,?4)",
                params![
                    request.client_identifier.as_str(),
                    result.uuid,
                    detail_json,
                    now.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        for trade in &result.trades {
            let trade_uuid = trade
                .get("uuid")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| LabError::DataCorrupt("mock trade uuid missing".into()))?;
            transaction
                .execute(
                    "INSERT INTO automation_mock_venue_trades(trade_uuid,client_identifier,trade_json,created_at_utc) \
                     VALUES (?1,?2,?3,?4)",
                    params![
                        trade_uuid,
                        request.client_identifier.as_str(),
                        serde_json::to_string(trade).map_err(json_error)?,
                        now.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(result)
    }

    pub(crate) fn automation_mock_order_by_identifier(
        &self,
        identifier: &AutomationOrderIdentifier,
    ) -> Result<Option<UpbitOrderDetail>, LabError> {
        let json = self
            .connection
            .query_row(
                "SELECT detail_json FROM automation_mock_venue_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        json.map(|value| serde_json::from_str(&value).map_err(json_error))
            .transpose()
    }

    pub(crate) fn automation_mock_cancel_order(
        &mut self,
        uuid: &str,
        now: UtcTimestamp,
    ) -> Result<UpbitOrderDetail, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let row = transaction
            .query_row(
                "SELECT client_identifier,detail_json FROM automation_mock_venue_orders WHERE venue_uuid=?1",
                [uuid],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown durable mock order UUID".into()))?;
        let mut detail: UpbitOrderDetail = serde_json::from_str(&row.1).map_err(json_error)?;
        if !matches!(detail.state.as_str(), "done" | "cancel") {
            let locked = detail
                .locked
                .ok_or_else(|| LabError::DataCorrupt("mock order omitted locked amount".into()))?;
            if detail.side == "bid" {
                adjust_mock_account(&transaction, "KRW", locked, -locked, now)?;
            } else {
                let base = detail
                    .market
                    .split_once('-')
                    .map(|(_, base)| base)
                    .ok_or_else(|| {
                        LabError::DataCorrupt("mock order market has no base currency".into())
                    })?;
                adjust_mock_account(&transaction, base, locked, -locked, now)?;
            }
            detail.state = "cancel".into();
            detail.remaining_fee = Some(Decimal::ZERO);
            detail.locked = Some(Decimal::ZERO);
            let json = serde_json::to_string(&detail).map_err(json_error)?;
            transaction
                .execute(
                    "UPDATE automation_mock_venue_orders SET detail_json=?1,updated_at_utc=?2 \
                     WHERE client_identifier=?3",
                    params![json, now.to_rfc3339(), row.0],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(detail)
    }
}

fn request_action(write: &AutomationSessionWrite) -> (&RequestId, AutomationAction) {
    match write {
        AutomationSessionWrite::Start { request_id, .. } => (request_id, AutomationAction::Start),
        AutomationSessionWrite::Pause { request_id, .. } => (request_id, AutomationAction::Pause),
        AutomationSessionWrite::Resume { request_id, .. } => (request_id, AutomationAction::Resume),
        AutomationSessionWrite::Stop { request_id, .. } => (request_id, AutomationAction::Stop),
        AutomationSessionWrite::CancelOrder { request_id, .. } => {
            (request_id, AutomationAction::CancelOrder)
        }
        AutomationSessionWrite::RecordBaseline { request_id, .. } => {
            (request_id, AutomationAction::RecordBaseline)
        }
    }
}

fn request_digest(
    write: &AutomationSessionWrite,
    account_identity: Option<&str>,
) -> Result<ContentHash, LabError> {
    if matches!(write, AutomationSessionWrite::Start { .. }) {
        ContentHash::of_value(&(write, account_identity))
    } else {
        ContentHash::of_value(write)
    }
}

fn lifecycle_session_and_account(
    connection: &Connection,
    write: &AutomationSessionWrite,
) -> Result<
    (
        Option<crate::contracts::AutomationSessionId>,
        Option<String>,
    ),
    LabError,
> {
    let session_id = match write {
        AutomationSessionWrite::Start { .. } => return Ok((None, None)),
        AutomationSessionWrite::Pause { session_id, .. }
        | AutomationSessionWrite::Resume { session_id, .. }
        | AutomationSessionWrite::Stop { session_id, .. }
        | AutomationSessionWrite::CancelOrder { session_id, .. }
        | AutomationSessionWrite::RecordBaseline { session_id, .. } => session_id,
    };
    let account_identity = connection
        .query_row(
            "SELECT account_identity FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    Ok((
        account_identity.as_ref().map(|_| session_id.clone()),
        account_identity,
    ))
}

fn is_terminal_automation_rejection(error: &LabError) -> bool {
    matches!(
        error,
        LabError::InvalidConfig(_)
            | LabError::InsufficientWarmup(_)
            | LabError::DataGap(_)
            | LabError::InputHashMismatch(_)
            | LabError::BlockedEvidence(_)
            | LabError::CapacityExceeded(_)
            | LabError::Conflict(_)
            | LabError::UnverifiedMarketRules(_)
            | LabError::AccountingInvariant(_)
            | LabError::ContractParse(_)
    )
}

fn mode_name(mode: AutomationSessionMode) -> &'static str {
    match mode {
        AutomationSessionMode::Mock => "MOCK",
        AutomationSessionMode::Live => "LIVE",
    }
}

fn validate_account_identity(mode: AutomationSessionMode, identity: &str) -> Result<(), LabError> {
    let valid = match mode {
        AutomationSessionMode::Mock => identity == "mock:upbit-default",
        AutomationSessionMode::Live => {
            identity
                .strip_prefix("live:upbit:")
                .is_some_and(|fingerprint| {
                    fingerprint.len() == 64
                        && fingerprint
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        }
    };
    if valid {
        Ok(())
    } else {
        Err(LabError::InvalidConfig(
            "automation account identity does not match the selected venue mode".into(),
        ))
    }
}

fn load_automation_admission_session(
    transaction: &Transaction<'_>,
    session_id: &crate::contracts::AutomationSessionId,
) -> Result<AutomationAdmissionSessionRow, LabError> {
    transaction
        .query_row(
            "SELECT account_identity,mode,state,market,risk_json,last_finalized_candle_at_utc \
             FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| {
                Ok(AutomationAdmissionSessionRow {
                    account_identity: row.get(0)?,
                    mode: row.get(1)?,
                    state: row.get(2)?,
                    market: row.get(3)?,
                    risk_json: row.get(4)?,
                    last_finalized_candle_at: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown automation session".into()))
}

fn automation_admission_context(
    row: AutomationAdmissionSessionRow,
) -> Result<AutomationAdmissionContext, LabError> {
    let risk: AutomationRiskPolicy = serde_json::from_str(&row.risk_json).map_err(json_error)?;
    let mode = parse_mode(&row.mode)?;
    risk.validate_for_mode(mode)?;
    Ok(AutomationAdmissionContext {
        account_identity: row.account_identity,
        mode,
        state: parse_state(&row.state)?,
        market: MarketId::parse_upbit(&row.market)?,
        risk,
        last_finalized_candle_at: row
            .last_finalized_candle_at
            .map(|value| UtcTimestamp::parse_rfc3339(&value))
            .transpose()?,
    })
}

fn validate_admission_market_rules(
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    now: UtcTimestamp,
) -> Result<crate::contracts::AutomationSide, LabError> {
    if admission.request.client_identifier
        != AutomationOrderIdentifier::from_intent(&admission.intent_id)
    {
        return Err(LabError::InputHashMismatch(
            "venue identifier is not derived from the durable strategy intent".into(),
        ));
    }
    if context.state != AutomationSessionState::Running {
        return Err(LabError::Conflict(
            "automation session is not running; no order was admitted".into(),
        ));
    }
    if admission.request.market != context.market || admission.rules.market != context.market.code()
    {
        return Err(LabError::InputHashMismatch(
            "order, rules, and frozen session market differ".into(),
        ));
    }
    if admission.rules.source_mode != context.mode
        || admission.rules.state != "active"
        || admission.rules.observed_at_utc > now
        || (now.0 - admission.rules.observed_at_utc.0).num_seconds()
            > i64::from(context.risk.max_balance_age_seconds)
    {
        return Err(LabError::UnverifiedMarketRules(
            "market rules are not current for this automation account".into(),
        ));
    }
    if admission.decision_time > now {
        return Err(LabError::DataCorrupt(
            "automation decision is future-dated".into(),
        ));
    }
    let side = admission.request.side;
    let fee_rate = match side {
        crate::contracts::AutomationSide::Bid => admission.rules.bid_fee,
        crate::contracts::AutomationSide::Ask => admission.rules.ask_fee,
    };
    if fee_rate < Decimal::ZERO || fee_rate > Decimal::new(1, 1) {
        return Err(LabError::UnverifiedMarketRules(
            "invalid current fee rate".into(),
        ));
    }
    let expected_fee = admission
        .requested_notional
        .get()
        .checked_mul(fee_rate)
        .ok_or_else(|| {
            LabError::AccountingInvariant("automation fee reserve exceeds decimal capacity".into())
        })?;
    if admission.fee_reserve.get() < expected_fee
        || admission.requested_notional.get() < min_total_for(&admission.rules, side)
        || admission.requested_notional.get() > admission.rules.max_total
        || admission.requested_notional > context.risk.max_order_notional_krw
    {
        return Err(LabError::Conflict(
            "order amount, fee reserve, or current venue minimum/maximum exceeds risk bounds"
                .into(),
        ));
    }
    let order_type_supported = match (&admission.request.side, &admission.request.order) {
        (crate::contracts::AutomationSide::Bid, AutomationOrderKind::MarketBuy { .. }) => admission
            .rules
            .bid_types
            .iter()
            .any(|value| value == "price"),
        (crate::contracts::AutomationSide::Ask, AutomationOrderKind::MarketSell { .. }) => {
            admission
                .rules
                .ask_types
                .iter()
                .any(|value| value == "market")
        }
        (crate::contracts::AutomationSide::Bid, AutomationOrderKind::Limit { .. }) => admission
            .rules
            .bid_types
            .iter()
            .any(|value| value == "limit"),
        (crate::contracts::AutomationSide::Ask, AutomationOrderKind::Limit { .. }) => admission
            .rules
            .ask_types
            .iter()
            .any(|value| value == "limit"),
        _ => false,
    };
    if !order_type_supported {
        return Err(LabError::UnverifiedMarketRules(
            "current market rule response does not support the requested order shape".into(),
        ));
    }
    Ok(side)
}

fn validate_admission_order_amounts(
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    side: crate::contracts::AutomationSide,
) -> Result<String, LabError> {
    let request_notional = match &admission.request.order {
        AutomationOrderKind::MarketBuy { budget } => budget.get(),
        AutomationOrderKind::MarketSell { volume } => volume
            .get()
            .checked_mul(admission.decision_mark.get())
            .ok_or_else(|| LabError::AccountingInvariant("market-sell notional overflow".into()))?,
        AutomationOrderKind::Limit { volume, price } => volume
            .get()
            .checked_mul(price.get())
            .ok_or_else(|| LabError::AccountingInvariant("limit-order notional overflow".into()))?,
    };
    if request_notional != admission.requested_notional.get()
        || (side == crate::contracts::AutomationSide::Bid
            && admission.quote_reserve.get()
                < admission
                    .requested_notional
                    .get()
                    .checked_add(admission.fee_reserve.get())
                    .ok_or_else(|| {
                        LabError::AccountingInvariant("quote reserve overflow".into())
                    })?)
        || (side == crate::contracts::AutomationSide::Ask
            && admission.quote_reserve.get() != Decimal::ZERO)
    {
        return Err(LabError::AccountingInvariant(
            "order notional or quote reservation differs from the typed order".into(),
        ));
    }
    if let AutomationOrderKind::Limit { price, .. } = &admission.request.order {
        let price_unit = match side {
            crate::contracts::AutomationSide::Bid => admission.rules.bid_price_unit,
            crate::contracts::AutomationSide::Ask => admission.rules.ask_price_unit,
        }
        .filter(|unit| *unit > Decimal::ZERO)
        .ok_or_else(|| {
            LabError::UnverifiedMarketRules(
                "current market rules have no verified limit price unit".into(),
            )
        })?;
        if price.get() % price_unit != Decimal::ZERO {
            return Err(LabError::UnverifiedMarketRules(
                "limit price does not align to the current verified price unit".into(),
            ));
        }
    }
    let expected_asset_currency = context.market.base.code().to_owned();
    if admission.asset_currency != expected_asset_currency
        || (side == crate::contracts::AutomationSide::Ask
            && match &admission.request.order {
                AutomationOrderKind::MarketSell { volume }
                | AutomationOrderKind::Limit { volume, .. } => *volume != admission.asset_reserve,
                AutomationOrderKind::MarketBuy { .. } => true,
            })
        || (side == crate::contracts::AutomationSide::Bid
            && admission.asset_reserve.get() != Decimal::ZERO)
    {
        return Err(LabError::AccountingInvariant(
            "reserved asset amount differs from the typed venue request".into(),
        ));
    }
    Ok(expected_asset_currency)
}

fn validate_pending_admission_intent(
    transaction: &Transaction<'_>,
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    terms: &AutomationAdmissionTerms,
) -> Result<(), LabError> {
    let intent_state = transaction
        .query_row(
            "SELECT state,session_id,action,decision_time_utc,evaluation_json,observation_id \
             FROM automation_intents WHERE intent_id=?1",
            [admission.intent_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("automation intent is not durable".into()))?;
    let expected_action = match terms.side {
        crate::contracts::AutomationSide::Bid => "BUY",
        crate::contracts::AutomationSide::Ask => "SELL",
    };
    let stored_evaluation: serde_json::Value =
        serde_json::from_str(&intent_state.4).map_err(json_error)?;
    if intent_state.0 != "PENDING"
        || intent_state.1 != admission.session_id.as_str()
        || intent_state.2 != expected_action
        || UtcTimestamp::parse_rfc3339(&intent_state.3)? != admission.decision_time
        || ContentHash::of_value(&stored_evaluation)?
            != ContentHash::of_value(&admission.evaluation_json)?
    {
        return Err(LabError::Conflict(
            "order economics do not match the exact pending strategy intent".into(),
        ));
    }
    let source = transaction
        .query_row(
            "SELECT close_time_utc,content_digest,candle_json FROM automation_candles \
             WHERE session_id=?1 AND observation_id=?2",
            params![admission.session_id.as_str(), intent_state.5],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("pending intent source candle is missing".into()))?;
    let source_close = UtcTimestamp::parse_rfc3339(&source.0)?;
    let source_observation: CandleObservation =
        serde_json::from_str(&source.2).map_err(json_error)?;
    if source_close != admission.decision_time
        || source_observation.id.as_str() != intent_state.5
        || source_observation.content_digest.as_str() != source.1
        || observation_digest(&source_observation.candle)? != source_observation.content_digest
        || source_observation.candle.close != admission.decision_mark
        || context.last_finalized_candle_at != Some(source_close)
    {
        return Err(LabError::DataCorrupt(
            "order decision mark or time differs from the latest durable source candle".into(),
        ));
    }
    Ok(())
}

fn load_account_reservation_facts(
    transaction: &Transaction<'_>,
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    terms: &AutomationAdmissionTerms,
    now: UtcTimestamp,
) -> Result<AutomationAccountReservationFacts, LabError> {
    let snapshot = transaction
        .query_row(
            "SELECT observed_at_utc,source_mode,balances_json FROM automation_account_snapshots \
             WHERE account_identity=?1 ORDER BY snapshot_sequence DESC LIMIT 1",
            [&context.account_identity],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::BlockedEvidence("fresh account snapshot is unavailable".into()))?;
    let observed_at = UtcTimestamp::parse_rfc3339(&snapshot.0)?;
    if parse_mode(&snapshot.1)? != context.mode
        || observed_at > now
        || (now.0 - observed_at.0).num_seconds() > i64::from(context.risk.max_balance_age_seconds)
    {
        return Err(LabError::BlockedEvidence(
            "account snapshot is stale or belongs to another venue mode".into(),
        ));
    }
    let balances: Vec<UpbitAccountBalance> =
        serde_json::from_str(&snapshot.2).map_err(json_error)?;
    validate_account_balance_ledger(
        transaction,
        &context.account_identity,
        observed_at,
        &balances,
    )?;
    let quote_free = balances
        .iter()
        .find(|row| row.currency == "KRW")
        .map_or(Decimal::ZERO, |row| row.balance);
    let base_free = balances
        .iter()
        .find(|row| row.currency == terms.expected_asset_currency)
        .map_or(Decimal::ZERO, |row| row.balance);
    let (reserved_quote, reserved_base, reserved_bid_exposure) = reservation_totals_for_admission(
        transaction,
        &context.account_identity,
        &terms.expected_asset_currency,
    )?;
    if terms.side == crate::contracts::AutomationSide::Bid
        && quote_free - reserved_quote < admission.quote_reserve.get()
    {
        return Err(LabError::Conflict(
            "insufficient unreserved KRW for order and upfront fee".into(),
        ));
    }
    if terms.side == crate::contracts::AutomationSide::Ask
        && base_free - reserved_base < admission.asset_reserve.get()
    {
        return Err(LabError::Conflict(
            "insufficient unreserved base asset for order".into(),
        ));
    }
    Ok(AutomationAccountReservationFacts {
        reserved_bid_exposure,
    })
}

fn load_admission_risk_projection(
    transaction: &Transaction<'_>,
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    now: UtcTimestamp,
    live_execution_policy: LiveExecutionPolicy,
) -> Result<crate::contracts::AutomationAccountRiskProjection, LabError> {
    let view = load_view_with_policy(
        transaction,
        &admission.session_id,
        now,
        live_execution_policy,
    )?
    .ok_or_else(|| {
        LabError::DataCorrupt("automation session vanished during risk admission".into())
    })?;
    let projection = view
        .account_risk
        .ok_or_else(|| LabError::BlockedEvidence("account risk projection is incomplete".into()))?;
    if projection.risk.emergency_latched
        || (context.mode == AutomationSessionMode::Live
            && (!live_execution_policy.is_enabled() || !projection.risk.live_grant_active))
        || (projection.effective_loss.admission_blocked
            && !is_legacy_mock_daily_loss_block(context.mode, &projection))
    {
        return Err(LabError::BlockedEvidence(
            "durable emergency latch, live grant, or production effect gate blocks order admission"
                .into(),
        ));
    }
    let admission_blocked: bool = transaction
        .query_row(
            "SELECT order_admission_blocked FROM trading_control_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if context.mode == AutomationSessionMode::Live && admission_blocked {
        return Err(LabError::BlockedEvidence(
            "durable order-admission block is active".into(),
        ));
    }
    if projection.balance.freshness != AutomationFreshness::Fresh
        || projection.feed.freshness != AutomationFreshness::Fresh
    {
        return Err(LabError::BlockedEvidence(
            "fresh account balance and finalized feed are required for order admission".into(),
        ));
    }
    let unresolved: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 \
             AND state IN ('SUBMITTED_UNKNOWN','NEEDS_RECONCILIATION')",
            [&context.account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if unresolved != 0 {
        return Err(LabError::BlockedEvidence(
            "shared account has an order requiring identifier readback".into(),
        ));
    }
    Ok(projection)
}

fn validate_shared_order_risk(
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    terms: &AutomationAdmissionTerms,
    reservations: &AutomationAccountReservationFacts,
    projection: &crate::contracts::AutomationAccountRiskProjection,
) -> Result<(), LabError> {
    let risk = &context.risk;
    let used_daily = projection
        .risk
        .daily_notional_used_krw
        .ok_or_else(|| LabError::BlockedEvidence("daily risk usage is unknown".into()))?;
    if used_daily
        .get()
        .checked_add(admission.requested_notional.get())
        .is_none_or(|value| value > risk.max_daily_notional_krw.get())
    {
        return Err(LabError::Conflict(
            "shared account daily notional limit exceeded".into(),
        ));
    }
    if projection.risk.open_order_count >= risk.max_open_orders
        || projection
            .risk
            .orders_in_interval
            .is_none_or(|count| count >= risk.max_orders_per_interval)
    {
        return Err(LabError::Conflict(
            "shared account open-order or interval limit exceeded".into(),
        ));
    }
    let effective_loss = &projection.effective_loss;
    if effective_loss.admission_blocked {
        if is_legacy_mock_daily_loss_block(context.mode, projection) {
            return Err(LabError::Conflict(
                "shared account daily-loss limit reached".into(),
            ));
        }
        return Err(LabError::BlockedEvidence(format!(
            "effective loss reference blocks order admission: {}",
            effective_loss.block_reasons.join("; ")
        )));
    }
    let loss_used = effective_loss.used_krw.ok_or_else(|| {
        LabError::BlockedEvidence("effective account loss usage is unknown".into())
    })?;
    let loss_limit = effective_loss.limit_krw.ok_or_else(|| {
        LabError::BlockedEvidence("effective account loss limit is unknown".into())
    })?;
    if !daily_loss_is_below_limit(loss_used, loss_limit) {
        return Err(LabError::Conflict(
            "shared account effective loss limit reached".into(),
        ));
    }
    let current_exposure = projection
        .risk
        .asset_exposure_krw
        .ok_or_else(|| LabError::BlockedEvidence("current per-asset exposure is unknown".into()))?;
    let projected_exposure = match terms.side {
        crate::contracts::AutomationSide::Bid => current_exposure
            .get()
            .checked_add(reservations.reserved_bid_exposure)
            .and_then(|value| value.checked_add(admission.requested_notional.get()))
            .ok_or_else(|| LabError::AccountingInvariant("asset exposure overflow".into()))?,
        crate::contracts::AutomationSide::Ask => {
            let sell_volume = match &admission.request.order {
                AutomationOrderKind::MarketSell { volume }
                | AutomationOrderKind::Limit { volume, .. } => volume.get(),
                AutomationOrderKind::MarketBuy { .. } => {
                    return Err(LabError::AccountingInvariant(
                        "ask exposure reduction has a buy request shape".into(),
                    ));
                }
            };
            let reducible = sell_volume
                .checked_mul(admission.decision_mark.get())
                .ok_or_else(|| LabError::AccountingInvariant("sell exposure overflow".into()))?;
            current_exposure
                .get()
                .checked_sub(reducible.min(current_exposure.get()))
                .and_then(|value| value.checked_add(reservations.reserved_bid_exposure))
                .ok_or_else(|| {
                    LabError::AccountingInvariant("sell exposure reduction underflow".into())
                })?
        }
    };
    if projected_exposure > risk.max_asset_exposure_krw.get() {
        return Err(LabError::Conflict(
            "shared account asset-exposure limit exceeded".into(),
        ));
    }
    Ok(())
}

fn daily_loss_is_below_limit(loss: QuoteAmount, limit: QuoteAmount) -> bool {
    loss < limit
}

fn persist_order_reservation(
    transaction: &Transaction<'_>,
    admission: &AutomationOrderAdmission,
    context: &AutomationAdmissionContext,
    terms: &AutomationAdmissionTerms,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let order_json = serde_json::to_string(&admission.request).map_err(json_error)?;
    let rules_json = serde_json::to_string(&admission.rules).map_err(json_error)?;
    let zero = Decimal::ZERO.to_string();
    let reservation_asset_volume = admission.asset_reserve.get().to_string();
    let locked_currency = if terms.side == crate::contracts::AutomationSide::Bid {
        "KRW"
    } else {
        terms.expected_asset_currency.as_str()
    };
    transaction
        .execute(
            "INSERT INTO automation_orders(\
                 client_identifier,intent_id,session_id,account_identity,order_request_json,rules_snapshot_json,request_digest, \
                 market,side,state,requested_notional_krw,decision_mark_krw,reserve_quote_krw,reserve_asset, \
                 reserved_asset_volume,reserved_fee_krw,remaining_fee_krw,executed_volume,remaining_volume, \
                 paid_fee_krw,locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc \
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'RESERVED',?10,?11,?12,?13,?14,?15,?15,?16,?16,?16,?16,?17,0,?18,?18)",
            params![
                admission.request.client_identifier.as_str(),
                admission.intent_id.as_str(),
                admission.session_id.as_str(),
                context.account_identity,
                order_json,
                rules_json,
                terms.request_digest.as_str(),
                context.market.code(),
                match terms.side {
                    crate::contracts::AutomationSide::Bid => "BID",
                    crate::contracts::AutomationSide::Ask => "ASK",
                },
                admission.requested_notional.get().to_string(),
                admission.decision_mark.get().to_string(),
                admission.quote_reserve.get().to_string(),
                terms.expected_asset_currency,
                reservation_asset_volume,
                admission.fee_reserve.get().to_string(),
                zero,
                locked_currency,
                now.to_rfc3339(),
            ],
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "INSERT INTO automation_risk_reservations(\
                 client_identifier,account_identity,market,quote_reserved_krw,asset_reserved,asset_reserved_volume, \
                 order_notional_krw,order_day_utc,state,created_at_utc \
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'ACTIVE',?9)",
            params![
                admission.request.client_identifier.as_str(),
                context.account_identity,
                context.market.code(),
                admission.quote_reserve.get().to_string(),
                terms.expected_asset_currency,
                reservation_asset_volume,
                admission.requested_notional.get().to_string(),
                now.0.format("%Y-%m-%d").to_string(),
                now.to_rfc3339(),
            ],
        )
        .map_err(sql_error)?;
    let intent_updated = transaction
        .execute(
            "UPDATE automation_intents SET state='ORDER_RESERVED' WHERE intent_id=?1 AND state='PENDING'",
            [admission.intent_id.as_str()],
        )
        .map_err(sql_error)?;
    if intent_updated != 1 {
        return Err(LabError::Conflict(
            "strategy intent changed during order reservation".into(),
        ));
    }
    insert_event(
        transaction,
        &admission.session_id,
        next_event_sequence(transaction, &admission.session_id)?,
        AutomationEventKind::OrderAdmitted,
        now,
        serde_json::json!({
            "intent_id": admission.intent_id,
            "client_identifier": admission.request.client_identifier,
            "notional_krw": admission.requested_notional,
            "fee_reserve_krw": admission.fee_reserve,
            "rules_observed_at_utc": admission.rules.observed_at_utc,
            "rules_digest": ContentHash::of_value(&admission.rules)?,
        }),
    )?;
    Ok(())
}

fn parse_mode(value: &str) -> Result<AutomationSessionMode, LabError> {
    match value {
        "MOCK" => Ok(AutomationSessionMode::Mock),
        "LIVE" => Ok(AutomationSessionMode::Live),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation mode {value}"
        ))),
    }
}

fn state_name(state: AutomationSessionState) -> &'static str {
    match state {
        AutomationSessionState::Running => "RUNNING",
        AutomationSessionState::Paused => "PAUSED",
        AutomationSessionState::Stopped => "STOPPED",
        AutomationSessionState::NeedsReconciliation => "NEEDS_RECONCILIATION",
        AutomationSessionState::BlockedApproval => "BLOCKED_APPROVAL",
        AutomationSessionState::Faulted => "FAULTED",
    }
}

fn parse_state(value: &str) -> Result<AutomationSessionState, LabError> {
    match value {
        "RUNNING" => Ok(AutomationSessionState::Running),
        "PAUSED" => Ok(AutomationSessionState::Paused),
        "STOPPED" => Ok(AutomationSessionState::Stopped),
        "NEEDS_RECONCILIATION" => Ok(AutomationSessionState::NeedsReconciliation),
        "BLOCKED_APPROVAL" => Ok(AutomationSessionState::BlockedApproval),
        "FAULTED" => Ok(AutomationSessionState::Faulted),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation session state {value}"
        ))),
    }
}

fn parse_request_state(
    value: &str,
) -> Result<crate::contracts::AutomationRequestReadbackState, LabError> {
    match value {
        "PENDING" => Ok(crate::contracts::AutomationRequestReadbackState::Pending),
        "APPLIED" => Ok(crate::contracts::AutomationRequestReadbackState::Applied),
        "REJECTED" => Ok(crate::contracts::AutomationRequestReadbackState::Rejected),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation request state {value}"
        ))),
    }
}

fn action_name(action: AutomationAction) -> &'static str {
    match action {
        AutomationAction::Start => "START",
        AutomationAction::Pause => "PAUSE",
        AutomationAction::Resume => "RESUME",
        AutomationAction::Stop => "STOP",
        AutomationAction::CancelOrder => "CANCEL_ORDER",
        AutomationAction::RecordBaseline => "RECORD_BASELINE",
    }
}

fn interval_name(interval: crate::contracts::CandleInterval) -> &'static str {
    match interval {
        crate::contracts::CandleInterval::M1 => "m1",
        crate::contracts::CandleInterval::M5 => "m5",
        crate::contracts::CandleInterval::M15 => "m15",
        crate::contracts::CandleInterval::H1 => "h1",
        crate::contracts::CandleInterval::H4 => "h4",
        crate::contracts::CandleInterval::D1 => "d1",
    }
}

fn parse_interval(value: &str) -> Result<crate::contracts::CandleInterval, LabError> {
    crate::contracts::CandleInterval::parse_code(value)
}

fn event_name(kind: AutomationEventKind) -> &'static str {
    match kind {
        AutomationEventKind::Started => "STARTED",
        AutomationEventKind::Paused => "PAUSED",
        AutomationEventKind::Resumed => "RESUMED",
        AutomationEventKind::Stopped => "STOPPED",
        AutomationEventKind::CandleObserved => "CANDLE_OBSERVED",
        AutomationEventKind::DecisionDeferred => "DECISION_DEFERRED",
        AutomationEventKind::SignalObserved => "SIGNAL_OBSERVED",
        AutomationEventKind::IntentRejected => "INTENT_REJECTED",
        AutomationEventKind::OrderAdmitted => "ORDER_ADMITTED",
        AutomationEventKind::OrderSendClaimed => "ORDER_SEND_CLAIMED",
        AutomationEventKind::OrderAcknowledged => "ORDER_ACKNOWLEDGED",
        AutomationEventKind::OrderCancelRequested => "ORDER_CANCEL_REQUESTED",
        AutomationEventKind::OrderReconciled => "ORDER_RECONCILED",
        AutomationEventKind::FillRecorded => "FILL_RECORDED",
        AutomationEventKind::NeedsReconciliation => "NEEDS_RECONCILIATION",
        AutomationEventKind::RecoveryBlocked => "RECOVERY_BLOCKED",
        AutomationEventKind::BaselineRecorded => "BASELINE_RECORDED",
        AutomationEventKind::BaselineBlocked => "BASELINE_BLOCKED",
    }
}

fn parse_event_kind(value: &str) -> Result<AutomationEventKind, LabError> {
    match value {
        "STARTED" => Ok(AutomationEventKind::Started),
        "PAUSED" => Ok(AutomationEventKind::Paused),
        "RESUMED" => Ok(AutomationEventKind::Resumed),
        "STOPPED" => Ok(AutomationEventKind::Stopped),
        "CANDLE_OBSERVED" => Ok(AutomationEventKind::CandleObserved),
        "DECISION_DEFERRED" => Ok(AutomationEventKind::DecisionDeferred),
        "SIGNAL_OBSERVED" => Ok(AutomationEventKind::SignalObserved),
        "INTENT_REJECTED" => Ok(AutomationEventKind::IntentRejected),
        "ORDER_ADMITTED" => Ok(AutomationEventKind::OrderAdmitted),
        "ORDER_SEND_CLAIMED" => Ok(AutomationEventKind::OrderSendClaimed),
        "ORDER_ACKNOWLEDGED" => Ok(AutomationEventKind::OrderAcknowledged),
        "ORDER_CANCEL_REQUESTED" => Ok(AutomationEventKind::OrderCancelRequested),
        "ORDER_RECONCILED" => Ok(AutomationEventKind::OrderReconciled),
        "FILL_RECORDED" => Ok(AutomationEventKind::FillRecorded),
        "NEEDS_RECONCILIATION" => Ok(AutomationEventKind::NeedsReconciliation),
        "RECOVERY_BLOCKED" => Ok(AutomationEventKind::RecoveryBlocked),
        "BASELINE_RECORDED" => Ok(AutomationEventKind::BaselineRecorded),
        "BASELINE_BLOCKED" => Ok(AutomationEventKind::BaselineBlocked),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation event kind {value}"
        ))),
    }
}

fn to_sql_u64(value: u64) -> Result<i64, LabError> {
    i64::try_from(value)
        .map_err(|_| LabError::ResourceLimit("automation sequence exceeds SQLite capacity".into()))
}

fn from_sql_u64(value: i64) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt("negative automation sequence".into()))
}

fn session_exists(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT 1 FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .map_err(sql_error)
}

fn next_event_sequence(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
) -> Result<u64, LabError> {
    let maximum: Option<i64> = connection
        .query_row(
            "SELECT MAX(sequence) FROM automation_session_events WHERE session_id=?1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    from_sql_u64(maximum.unwrap_or(0))?
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("automation event sequence exhausted".into()))
}

#[allow(clippy::needless_pass_by_value)] // Call sites build an owned JSON event which this transaction serializes synchronously.
fn insert_event(
    transaction: &Transaction<'_>,
    session_id: &crate::contracts::AutomationSessionId,
    sequence: u64,
    kind: AutomationEventKind,
    at: UtcTimestamp,
    detail: serde_json::Value,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                session_id.as_str(),
                to_sql_u64(sequence)?,
                event_name(kind),
                at.to_rfc3339(),
                serde_json::to_string(&detail).map_err(json_error)?,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn set_automation_recovery(
    transaction: &Transaction<'_>,
    session_id: &crate::contracts::AutomationSessionId,
    reason: &str,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let changed = transaction
        .execute(
            "UPDATE automation_sessions SET state='NEEDS_RECONCILIATION',recovery_reason=?1,updated_at_utc=?2 \
             WHERE session_id=?3 AND state IN ('RUNNING','NEEDS_RECONCILIATION')",
            params![reason, now.to_rfc3339(), session_id.as_str()],
        )
        .map_err(sql_error)?;
    if changed > 0 {
        insert_event(
            transaction,
            session_id,
            next_event_sequence(transaction, session_id)?,
            AutomationEventKind::NeedsReconciliation,
            now,
            serde_json::json!({"reason":reason}),
        )?;
    }
    Ok(())
}

fn insert_request(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    session_id: &crate::contracts::AutomationSessionId,
    action: AutomationAction,
    digest: &ContentHash,
    response: &AutomationSessionCommandResponse,
    _now: UtcTimestamp,
) -> Result<(), LabError> {
    let changed = transaction
        .execute(
            "UPDATE automation_session_requests SET session_id=?1,state='APPLIED', \
                 response_json=?2,reason=NULL WHERE request_id=?3 AND action=?4 \
                 AND payload_digest=?5 AND state='PENDING'",
            params![
                session_id.as_str(),
                serde_json::to_string(response).map_err(json_error)?,
                request_id.as_str(),
                action_name(action),
                digest.as_str(),
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::OutcomeUnknown(
            "automation command response did not finalize its durable request".into(),
        ));
    }
    Ok(())
}

fn activation_reference_exists(
    connection: &Connection,
    account_identity: &str,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT 1 FROM automation_live_activation_references WHERE account_identity=?1",
            [account_identity],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .map_err(sql_error)
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "capture binds one account snapshot and its exact provenance in one transaction"
)]
fn capture_live_activation_reference(
    transaction: &Transaction<'_>,
    account_identity: &str,
    snapshot_sequence: u64,
    observed_at: UtcTimestamp,
    received_at: UtcTimestamp,
    balances: &[UpbitAccountBalance],
    balances_json: &str,
    balances_digest: &str,
) -> Result<(), LabError> {
    if activation_reference_exists(transaction, account_identity)? {
        return Ok(());
    }
    let mut statement = transaction
        .prepare(
            "SELECT risk_json FROM automation_sessions WHERE account_identity=?1 \
             AND mode='LIVE' AND (state='RUNNING' OR (state='NEEDS_RECONCILIATION' \
                 AND recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK')) \
             ORDER BY session_id",
        )
        .map_err(sql_error)?;
    let risk_json = statement
        .query_map([account_identity], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(statement);
    if risk_json.is_empty() {
        return Ok(());
    }
    let active_risks = risk_json
        .iter()
        .map(|json| serde_json::from_str::<AutomationRiskPolicy>(json).map_err(json_error))
        .collect::<Result<Vec<_>, _>>()?;
    for risk in &active_risks {
        risk.validate_for_mode(AutomationSessionMode::Live)?;
    }
    if active_risks
        .iter()
        .any(|risk| risk.activation_loss_limit_krw.is_none())
    {
        return Ok(());
    }
    let max_feed_age_seconds = active_risks
        .iter()
        .map(|risk| risk.max_feed_age_seconds)
        .min()
        .ok_or_else(|| LabError::DataCorrupt("LIVE activation risk profile disappeared".into()))?;
    let control = transaction
        .query_row(
            "SELECT state,sequence,order_admission_blocked,emergency_latched \
             FROM trading_control_state WHERE singleton=1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            },
        )
        .map_err(sql_error)?;
    if control.0 != "RUNNING" || control.2 || control.3 {
        return Ok(());
    }
    let control_sequence = from_sql_u64(control.1)?;
    let valuation = match value_live_activation_balances(
        transaction,
        account_identity,
        balances,
        observed_at,
        max_feed_age_seconds,
    ) {
        Ok((assets, equity)) if equity > Decimal::ZERO => StoredLiveActivationValuation::Verified {
            assets,
            total_equity_krw: equity.to_string(),
        },
        Ok(_) => StoredLiveActivationValuation::PendingMarks {
            reason: "NON_POSITIVE_REFERENCE_EQUITY".into(),
        },
        Err(LabError::BlockedEvidence(_)) => StoredLiveActivationValuation::PendingMarks {
            reason: "FINALIZED_MARK_UNAVAILABLE_OR_STALE".into(),
        },
        Err(LabError::InvalidConfig(_)) => StoredLiveActivationValuation::PendingMarks {
            reason: "UNSUPPORTED_ASSET_MARKET".into(),
        },
        Err(error) => return Err(error),
    };
    let valuation_state = match valuation {
        StoredLiveActivationValuation::PendingMarks { .. } => "PENDING_MARKS",
        StoredLiveActivationValuation::Verified { .. } => "VERIFIED",
    };
    let equity = match &valuation {
        StoredLiveActivationValuation::PendingMarks { .. } => None,
        StoredLiveActivationValuation::Verified {
            total_equity_krw, ..
        } => Some(total_equity_krw.as_str()),
    };
    let valuation_json = serde_json::to_string(&valuation).map_err(json_error)?;
    let valuation_digest = ContentHash::of_value(&valuation)?.to_string();
    transaction
        .execute(
            "INSERT INTO automation_live_activation_references(\
                 account_identity,mode,source,snapshot_sequence,control_sequence,\
                 reference_max_feed_age_seconds,reference_observed_at_utc,reference_received_at_utc,\
                 valuation_recorded_at_utc,balances_json,balances_digest,valuation_state,\
                 valuation_json,valuation_digest,reference_equity_krw,created_at_utc\
             ) VALUES (?1,'LIVE','CURRENT_LOCAL_OBSERVED',?2,?3,?4,?5,?6,?6,?7,?8,?9,?10,?11,?12,?6) \
             ON CONFLICT(account_identity) DO NOTHING",
            params![
                account_identity,
                to_sql_u64(snapshot_sequence)?,
                to_sql_u64(control_sequence)?,
                i64::from(max_feed_age_seconds),
                observed_at.to_rfc3339(),
                received_at.to_rfc3339(),
                balances_json,
                balances_digest,
                valuation_state,
                valuation_json,
                valuation_digest,
                equity,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn complete_pending_live_activation_valuation(
    connection: &Connection,
    account_identity: &str,
    valuation_recorded_at: UtcTimestamp,
) -> Result<(), LabError> {
    let Some(reference) = load_live_activation_reference(connection, account_identity)? else {
        return Ok(());
    };
    if matches!(
        reference.valuation,
        StoredLiveActivationValuation::Verified { .. }
    ) {
        return Ok(());
    }
    let StoredLiveActivationValuation::PendingMarks { .. } = reference.valuation else {
        return Ok(());
    };
    let (assets, equity) = match value_live_activation_balances(
        connection,
        account_identity,
        &reference.balances,
        reference.view.reference_observed_at_utc,
        reference.view.reference_max_feed_age_seconds,
    ) {
        Ok((assets, equity)) if equity > Decimal::ZERO => (assets, equity),
        Ok(_) | Err(LabError::BlockedEvidence(_) | LabError::InvalidConfig(_)) => {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let valuation = StoredLiveActivationValuation::Verified {
        assets,
        total_equity_krw: equity.to_string(),
    };
    let valuation_json = serde_json::to_string(&valuation).map_err(json_error)?;
    let valuation_digest = ContentHash::of_value(&valuation)?.to_string();
    let changed = connection
        .execute(
            "UPDATE automation_live_activation_references SET valuation_state='VERIFIED',\
                 valuation_json=?1,valuation_digest=?2,reference_equity_krw=?3,\
                 valuation_recorded_at_utc=?4 \
             WHERE account_identity=?5 AND valuation_state='PENDING_MARKS' \
               AND balances_digest=?6 AND snapshot_sequence=?7",
            params![
                valuation_json,
                valuation_digest,
                equity.to_string(),
                valuation_recorded_at.to_rfc3339(),
                account_identity,
                reference.view.balances_digest.as_str(),
                to_sql_u64(reference.view.snapshot_sequence)?,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        let current = load_live_activation_reference(connection, account_identity)?;
        if current.is_none_or(|current| {
            current.view.valuation_state != AutomationActivationValuationState::Verified
                || current.view.valuation_digest.as_str() != valuation_digest
        }) {
            return Err(LabError::OutcomeUnknown(
                "activation valuation completion was not durably confirmed".into(),
            ));
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "reference decoding verifies its snapshot, balances, valuation and provenance as one stored contract"
)]
fn load_live_activation_reference(
    connection: &Connection,
    account_identity: &str,
) -> Result<Option<StoredLiveActivationReference>, LabError> {
    let row = connection
        .query_row(
            "SELECT mode,source,snapshot_sequence,control_sequence,reference_max_feed_age_seconds,\
                    reference_observed_at_utc,reference_received_at_utc,valuation_recorded_at_utc,\
                    balances_json,balances_digest,valuation_state,valuation_json,valuation_digest,\
                    reference_equity_krw \
             FROM automation_live_activation_references WHERE account_identity=?1",
            [account_identity],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<String>>(13)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.0 != "LIVE" || row.1 != "CURRENT_LOCAL_OBSERVED" {
        return Err(LabError::DataCorrupt(
            "activation reference has an unsupported mode or source".into(),
        ));
    }
    let snapshot_sequence = from_sql_u64(row.2)?;
    let control_sequence = from_sql_u64(row.3)?;
    let reference_max_feed_age_seconds = u32::try_from(row.4)
        .map_err(|_| LabError::DataCorrupt("activation feed age is out of range".into()))?;
    if !(1..=86_400).contains(&reference_max_feed_age_seconds) {
        return Err(LabError::DataCorrupt(
            "activation feed age is outside supported bounds".into(),
        ));
    }
    let reference_observed_at_utc = UtcTimestamp::parse_rfc3339(&row.5)?;
    let reference_received_at_utc = UtcTimestamp::parse_rfc3339(&row.6)?;
    let valuation_recorded_at_utc = UtcTimestamp::parse_rfc3339(&row.7)?;
    if reference_observed_at_utc > reference_received_at_utc
        || valuation_recorded_at_utc < reference_received_at_utc
    {
        return Err(LabError::DataCorrupt(
            "activation reference observation timestamps are inconsistent".into(),
        ));
    }
    let balances: Vec<UpbitAccountBalance> = serde_json::from_str(&row.8).map_err(json_error)?;
    let balances_digest = ContentHash::of_value(&balances)?;
    if balances_digest.as_str() != row.9 {
        return Err(LabError::DataCorrupt(
            "activation reference balances digest mismatch".into(),
        ));
    }
    let snapshot = connection
        .query_row(
            "SELECT account_identity,source_mode,observed_at_utc,balances_json,balances_digest \
             FROM automation_account_snapshots WHERE snapshot_sequence=?1",
            [to_sql_u64(snapshot_sequence)?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("activation snapshot was removed".into()))?;
    if snapshot.0 != account_identity
        || snapshot.1 != "LIVE"
        || snapshot.2 != reference_observed_at_utc.to_rfc3339()
        || snapshot.3 != row.8
        || snapshot.4 != row.9
    {
        return Err(LabError::DataCorrupt(
            "activation reference differs from its bound account snapshot".into(),
        ));
    }
    let valuation: StoredLiveActivationValuation =
        serde_json::from_str(&row.11).map_err(json_error)?;
    let valuation_digest = ContentHash::of_value(&valuation)?;
    if valuation_digest.as_str() != row.12 {
        return Err(LabError::DataCorrupt(
            "activation valuation digest mismatch".into(),
        ));
    }
    let (valuation_state, reference_equity_krw) = match (&valuation, row.10.as_str()) {
        (StoredLiveActivationValuation::PendingMarks { .. }, "PENDING_MARKS")
            if row.13.is_none() =>
        {
            (AutomationActivationValuationState::PendingMarks, None)
        }
        (
            StoredLiveActivationValuation::Verified {
                total_equity_krw, ..
            },
            "VERIFIED",
        ) => {
            let stored_equity = row.13.as_deref().ok_or_else(|| {
                LabError::DataCorrupt("verified activation reference has no equity".into())
            })?;
            let valuation_equity = Decimal::from_str_exact(total_equity_krw).map_err(|error| {
                LabError::DataCorrupt(format!("invalid activation reference equity: {error}"))
            })?;
            let stored = Decimal::from_str_exact(stored_equity).map_err(|error| {
                LabError::DataCorrupt(format!("invalid stored activation equity: {error}"))
            })?;
            if valuation_equity != stored || stored <= Decimal::ZERO {
                return Err(LabError::DataCorrupt(
                    "activation equity differs from its valuation record".into(),
                ));
            }
            let StoredLiveActivationValuation::Verified { assets, .. } = &valuation else {
                return Err(LabError::DataCorrupt(
                    "verified activation reference has a pending valuation payload".into(),
                ));
            };
            validate_live_activation_valuation(
                &balances,
                assets,
                valuation_equity,
                reference_observed_at_utc,
                reference_max_feed_age_seconds,
            )?;
            (
                AutomationActivationValuationState::Verified,
                Some(QuoteAmount::new(stored)?),
            )
        }
        _ => {
            return Err(LabError::DataCorrupt(
                "activation valuation state differs from its persisted payload".into(),
            ));
        }
    };
    Ok(Some(StoredLiveActivationReference {
        view: AutomationLiveActivationReferenceView {
            account_identity: account_identity.to_owned(),
            mode: AutomationSessionMode::Live,
            source: AutomationActivationReferenceSource::CurrentLocalObserved,
            reference_observed_at_utc,
            reference_received_at_utc,
            valuation_recorded_at_utc,
            snapshot_sequence,
            control_sequence,
            reference_max_feed_age_seconds,
            balances_digest,
            valuation_state,
            valuation_digest,
            reference_equity_krw,
        },
        balances,
        valuation,
    }))
}

fn value_live_activation_balances(
    connection: &Connection,
    account_identity: &str,
    balances: &[UpbitAccountBalance],
    observed_at: UtcTimestamp,
    max_feed_age_seconds: u32,
) -> Result<(Vec<LiveActivationAssetValuation>, Decimal), LabError> {
    let mut ordered = balances.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.currency.cmp(&right.currency));
    let mut assets = Vec::with_capacity(ordered.len());
    let mut equity = Decimal::ZERO;
    for balance in ordered {
        let quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
            LabError::AccountingInvariant("activation balance total exceeds Decimal".into())
        })?;
        let (mark_basis, mark_price, mark_close, mark_id, mark_digest) =
            if balance.currency == "KRW" {
                (
                    LiveActivationMarkBasis::QuoteCurrencyParity,
                    Some(Decimal::ONE),
                    None,
                    None,
                    None,
                )
            } else if quantity == Decimal::ZERO {
                (
                    LiveActivationMarkBasis::FinalizedCandleClose,
                    None,
                    None,
                    None,
                    None,
                )
            } else {
                let market = MarketId::parse_upbit(&format!("KRW-{}", balance.currency))?;
                let observation = latest_account_market_mark_with_time(
                    connection,
                    account_identity,
                    &market,
                    observed_at,
                    max_feed_age_seconds,
                )?;
                (
                    LiveActivationMarkBasis::FinalizedCandleClose,
                    Some(observation.candle.close.get()),
                    Some(observation.candle.close_time_utc),
                    Some(observation.id),
                    Some(observation.content_digest),
                )
            };
        let value = match mark_price {
            Some(mark) => quantity.checked_mul(mark).ok_or_else(|| {
                LabError::AccountingInvariant("activation valuation exceeds Decimal".into())
            })?,
            None => Decimal::ZERO,
        };
        equity = equity.checked_add(value).ok_or_else(|| {
            LabError::AccountingInvariant("activation equity exceeds Decimal".into())
        })?;
        assets.push(LiveActivationAssetValuation {
            currency: balance.currency.clone(),
            quantity: quantity.to_string(),
            mark_basis,
            mark_price_krw: mark_price.map(|mark| mark.to_string()),
            mark_close_at_utc: mark_close,
            mark_observation_id: mark_id,
            mark_content_digest: mark_digest,
            value_krw: value.to_string(),
        });
    }
    Ok((assets, equity))
}

#[expect(
    clippy::too_many_lines,
    reason = "exact valuation verification covers each currency and its bound finalized mark before accepting the total"
)]
fn validate_live_activation_valuation(
    balances: &[UpbitAccountBalance],
    assets: &[LiveActivationAssetValuation],
    expected_equity: Decimal,
    reference_at: UtcTimestamp,
    max_feed_age_seconds: u32,
) -> Result<(), LabError> {
    let mut expected = std::collections::BTreeMap::new();
    for balance in balances {
        let quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
            LabError::DataCorrupt("activation balance amount exceeds Decimal".into())
        })?;
        if expected
            .insert(balance.currency.as_str(), quantity)
            .is_some()
        {
            return Err(LabError::DataCorrupt(
                "activation reference contains duplicate balance currencies".into(),
            ));
        }
    }
    if assets.len() != expected.len() {
        return Err(LabError::DataCorrupt(
            "activation valuation does not cover every account currency".into(),
        ));
    }
    let mut total = Decimal::ZERO;
    let mut prior_currency: Option<&str> = None;
    for asset in assets {
        if prior_currency.is_some_and(|prior| prior >= asset.currency.as_str()) {
            return Err(LabError::DataCorrupt(
                "activation valuation currencies are not uniquely ordered".into(),
            ));
        }
        prior_currency = Some(&asset.currency);
        let quantity = Decimal::from_str_exact(&asset.quantity).map_err(|error| {
            LabError::DataCorrupt(format!("invalid activation quantity: {error}"))
        })?;
        if expected.get(asset.currency.as_str()) != Some(&quantity) {
            return Err(LabError::DataCorrupt(
                "activation valuation quantity differs from observed balances".into(),
            ));
        }
        let value = Decimal::from_str_exact(&asset.value_krw).map_err(|error| {
            LabError::DataCorrupt(format!("invalid activation asset value: {error}"))
        })?;
        let expected_value = match asset.mark_basis {
            LiveActivationMarkBasis::QuoteCurrencyParity if asset.currency == "KRW" => {
                if asset.mark_price_krw.as_deref() != Some("1")
                    || asset.mark_close_at_utc.is_some()
                    || asset.mark_observation_id.is_some()
                    || asset.mark_content_digest.is_some()
                {
                    return Err(LabError::DataCorrupt(
                        "KRW activation valuation has invalid quote-parity evidence".into(),
                    ));
                }
                quantity
            }
            LiveActivationMarkBasis::FinalizedCandleClose if asset.currency != "KRW" => {
                if quantity == Decimal::ZERO {
                    if asset.mark_price_krw.is_some()
                        || asset.mark_close_at_utc.is_some()
                        || asset.mark_observation_id.is_some()
                        || asset.mark_content_digest.is_some()
                    {
                        return Err(LabError::DataCorrupt(
                            "zero activation balance carries a fabricated mark".into(),
                        ));
                    }
                    Decimal::ZERO
                } else {
                    let mark = asset
                        .mark_price_krw
                        .as_deref()
                        .ok_or_else(|| {
                            LabError::DataCorrupt("held activation asset has no mark".into())
                        })
                        .and_then(|raw| {
                            Decimal::from_str_exact(raw).map_err(|error| {
                                LabError::DataCorrupt(format!("invalid activation mark: {error}"))
                            })
                        })?;
                    let close = asset.mark_close_at_utc.ok_or_else(|| {
                        LabError::DataCorrupt("held activation asset has no mark time".into())
                    })?;
                    if mark <= Decimal::ZERO
                        || close > reference_at
                        || (reference_at.0 - close.0).num_seconds()
                            > i64::from(max_feed_age_seconds)
                        || asset.mark_observation_id.is_none()
                        || asset.mark_content_digest.is_none()
                    {
                        return Err(LabError::DataCorrupt(
                            "activation mark is unsupported, stale, or missing identity".into(),
                        ));
                    }
                    quantity.checked_mul(mark).ok_or_else(|| {
                        LabError::DataCorrupt("activation mark value exceeds Decimal".into())
                    })?
                }
            }
            _ => {
                return Err(LabError::DataCorrupt(
                    "activation mark basis does not match its currency".into(),
                ));
            }
        };
        if value != expected_value {
            return Err(LabError::DataCorrupt(
                "activation asset value differs from its exact mark and quantity".into(),
            ));
        }
        total = total
            .checked_add(value)
            .ok_or_else(|| LabError::DataCorrupt("activation equity exceeds Decimal".into()))?;
    }
    if total != expected_equity {
        return Err(LabError::DataCorrupt(
            "activation equity differs from valued assets".into(),
        ));
    }
    Ok(())
}

fn latest_account_market_mark_with_time(
    connection: &Connection,
    account_identity: &str,
    market: &MarketId,
    now: UtcTimestamp,
    max_age_seconds: u32,
) -> Result<CandleObservation, LabError> {
    let json = connection
        .query_row(
            "SELECT c.candle_json FROM automation_candles c \
             JOIN automation_sessions s USING(session_id) \
             WHERE s.account_identity=?1 AND s.market=?2 AND c.close_time_utc<=?3 \
             ORDER BY c.close_time_utc DESC,s.session_id ASC LIMIT 1",
            params![account_identity, market.code(), now.to_rfc3339()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| {
            LabError::BlockedEvidence(format!("no current mark for {}", market.code()))
        })?;
    let observation: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
    let age = (now.0 - observation.candle.close_time_utc.0).num_seconds();
    if observation.candle.close_time_utc > now || age < 0 || age > i64::from(max_age_seconds) {
        return Err(LabError::BlockedEvidence(format!(
            "stale mark for {}",
            market.code()
        )));
    }
    Ok(observation)
}

fn load_view(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
    now: UtcTimestamp,
) -> Result<Option<AutomationSessionView>, LabError> {
    load_view_with_policy(connection, session_id, now, LiveExecutionPolicy::Disabled)
}

#[allow(clippy::too_many_lines)] // This read projection assembles one coherent session snapshot from durable sources.
fn load_view_with_policy(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
    now: UtcTimestamp,
    live_execution_policy: LiveExecutionPolicy,
) -> Result<Option<AutomationSessionView>, LabError> {
    let row = connection
        .query_row(
            "SELECT mode,account_identity,state,sequence,policy_ref_json,market,decision_interval,risk_json, \
                    created_at_utc,updated_at_utc,last_finalized_candle_at_utc, \
                    last_feed_received_at_utc,last_balance_observed_at_utc,recovery_reason \
             FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mode = parse_mode(&row.0)?;
    let projection_live_execution_policy = if mode == AutomationSessionMode::Live
        && live_execution_policy.is_enabled()
    {
        let runtime_control: Option<(bool, bool)> = connection
            .query_row(
                "SELECT available,live_trading_enabled FROM live_order_control_state WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if runtime_control.is_some_and(|(available, enabled)| available && !enabled) {
            LiveExecutionPolicy::Disabled
        } else {
            live_execution_policy
        }
    } else {
        live_execution_policy
    };
    let account_identity = row.1.clone();
    let state = parse_state(&row.2)?;
    let policy_ref: PolicyRevisionRef = serde_json::from_str(&row.4).map_err(json_error)?;
    let market = MarketId::parse_upbit(&row.5)?;
    let decision_interval = parse_interval(&row.6)?;
    let risk: AutomationRiskPolicy = serde_json::from_str(&row.7).map_err(json_error)?;
    risk.validate_for_mode(mode)?;
    let unresolved = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_orders WHERE session_id=?1 \
             AND state NOT IN ('FILLED','CANCELLED','REJECTED')",
            [session_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_error)?;
    let fills = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_fills WHERE session_id=?1",
            [session_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_error)?;
    let last_finalized = row
        .10
        .map(|value| UtcTimestamp::parse_rfc3339(&value))
        .transpose()?;
    let feed_received = row
        .11
        .map(|value| UtcTimestamp::parse_rfc3339(&value))
        .transpose()?;
    let last_balance = latest_balance_times(connection, &account_identity)?
        .map(|(observed, _)| observed)
        .or(row
            .12
            .map(|value| UtcTimestamp::parse_rfc3339(&value))
            .transpose()?);
    let unresolved_order_count = u32::try_from(unresolved)
        .map_err(|_| LabError::DataCorrupt("negative automation unresolved order count".into()))?;
    let fill_count = u64::try_from(fills)
        .map_err(|_| LabError::DataCorrupt("negative automation fill count".into()))?;
    let created_at_utc = UtcTimestamp::parse_rfc3339(&row.8)?;
    let updated_at_utc = UtcTimestamp::parse_rfc3339(&row.9)?;
    let account_risk = load_account_risk_projection(
        connection,
        AccountRiskProjectionInput {
            session_id,
            account_identity: &account_identity,
            mode,
            session_state: state,
            recovery_reason: row.13.as_deref(),
            market: &market,
            interval: decision_interval,
            risk: &risk,
            feed_close: last_finalized,
            feed_received,
            now,
            live_execution_policy: projection_live_execution_policy,
        },
    )?;
    Ok(Some(AutomationSessionView {
        session_id: session_id.clone(),
        mode,
        state,
        account_identity,
        sequence: from_sql_u64(row.3)?,
        policy_ref,
        market,
        decision_interval,
        risk,
        created_at_utc,
        updated_at_utc,
        last_finalized_candle_at: last_finalized,
        last_balance_observed_at: last_balance,
        unresolved_order_count,
        fill_count,
        recovery_reason: row.13,
        account_risk: Some(account_risk),
    }))
}

#[derive(Clone, Copy)]
struct AccountRiskProjectionInput<'a> {
    session_id: &'a crate::contracts::AutomationSessionId,
    account_identity: &'a str,
    mode: AutomationSessionMode,
    session_state: AutomationSessionState,
    recovery_reason: Option<&'a str>,
    market: &'a MarketId,
    interval: crate::contracts::CandleInterval,
    risk: &'a AutomationRiskPolicy,
    feed_close: Option<UtcTimestamp>,
    feed_received: Option<UtcTimestamp>,
    now: UtcTimestamp,
    live_execution_policy: LiveExecutionPolicy,
}

fn strictest_live_activation_limits(
    connection: &Connection,
    account_identity: &str,
) -> Result<Option<(QuoteAmount, u32)>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT risk_json FROM automation_sessions WHERE account_identity=?1 AND mode='LIVE' \
             ORDER BY created_at_utc,session_id",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([account_identity], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(statement);
    let mut strictest_limit: Option<QuoteAmount> = None;
    let mut strictest_feed_age: Option<u32> = None;
    for row in rows {
        let risk: AutomationRiskPolicy = serde_json::from_str(&row).map_err(json_error)?;
        risk.validate_for_mode(AutomationSessionMode::Live)?;
        let Some(limit) = risk.activation_loss_limit_krw else {
            continue;
        };
        if strictest_limit.is_none_or(|current| limit.get() < current.get()) {
            strictest_limit = Some(limit);
        }
        strictest_feed_age = Some(
            strictest_feed_age.map_or(risk.max_feed_age_seconds, |current| {
                current.min(risk.max_feed_age_seconds)
            }),
        );
    }
    Ok(strictest_limit.zip(strictest_feed_age))
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::too_many_lines,
    reason = "one projection selects the explicit loss basis and preserves all associated risk block reasons"
)]
fn effective_loss_projection(
    connection: &Connection,
    account_identity: &str,
    mode: AutomationSessionMode,
    risk: &AutomationRiskPolicy,
    daily_loss: Option<QuoteAmount>,
    day_baseline: Option<&AutomationAccountDayBaselineView>,
    legacy_mock_baseline: Option<&AutomationLegacyMockBaselineView>,
    activation_reference: Option<&StoredLiveActivationReference>,
    has_snapshot: bool,
    observed_at: Option<UtcTimestamp>,
    balance_freshness: AutomationFreshness,
    feed_freshness: AutomationFreshness,
    balances: &[UpbitAccountBalance],
    now: UtcTimestamp,
) -> Result<AutomationEffectiveLossProjection, LabError> {
    if mode == AutomationSessionMode::Live && risk.activation_loss_limit_krw.is_some() {
        let (limit, feed_age) = strictest_live_activation_limits(connection, account_identity)?
            .ok_or_else(|| {
                LabError::DataCorrupt(
                    "activation loss policy has no account-scoped persisted session".into(),
                )
            })?;
        let mut block_reasons = Vec::new();
        let mut used = None;
        match activation_reference {
            None => block_reasons.push("LIVE_ACTIVATION_REFERENCE_MISSING".into()),
            Some(reference)
                if reference.view.valuation_state
                    != AutomationActivationValuationState::Verified =>
            {
                block_reasons.push("LIVE_ACTIVATION_REFERENCE_MARKS_PENDING".into());
            }
            Some(_) if !has_snapshot => {
                block_reasons.push("LIVE_ACTIVATION_ACCOUNT_SNAPSHOT_MISSING".into());
            }
            Some(_) if balance_freshness != AutomationFreshness::Fresh => {
                block_reasons.push("LIVE_ACTIVATION_ACCOUNT_SNAPSHOT_NOT_FRESH".into());
            }
            Some(_) if feed_freshness != AutomationFreshness::Fresh => {
                block_reasons.push("LIVE_ACTIVATION_FEED_NOT_FRESH".into());
            }
            Some(reference) => {
                let observed_at = observed_at.ok_or_else(|| {
                    LabError::DataCorrupt(
                        "fresh LIVE activation snapshot has no observed timestamp".into(),
                    )
                })?;
                match validate_account_balance_ledger(
                    connection,
                    account_identity,
                    observed_at,
                    balances,
                ) {
                    Ok(()) => {}
                    Err(LabError::BlockedEvidence(_) | LabError::Conflict(_)) => {
                        block_reasons.push("LIVE_ACCOUNT_FLOW_UNRECONCILED".into());
                    }
                    Err(error) => return Err(error),
                }
                if block_reasons.is_empty() {
                    match value_live_activation_balances(
                        connection,
                        account_identity,
                        balances,
                        now,
                        feed_age,
                    ) {
                        Ok((_, current_equity)) => {
                            let reference_equity = reference
                                .view
                                .reference_equity_krw
                                .ok_or_else(|| {
                                    LabError::DataCorrupt(
                                        "verified activation reference omitted its equity".into(),
                                    )
                                })?
                                .get();
                            let loss = reference_equity
                                .checked_sub(current_equity)
                                .ok_or_else(|| {
                                    LabError::AccountingInvariant(
                                        "activation equity delta exceeds Decimal".into(),
                                    )
                                })?
                                .max(Decimal::ZERO);
                            used = Some(QuoteAmount::new(loss)?);
                        }
                        Err(LabError::BlockedEvidence(_)) => {
                            block_reasons.push("LIVE_ACTIVATION_CURRENT_MARK_NOT_FRESH".into());
                        }
                        Err(LabError::InvalidConfig(_)) => {
                            block_reasons.push("LIVE_ACTIVATION_CURRENT_MARK_UNSUPPORTED".into());
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        if used.is_some_and(|loss| !daily_loss_is_below_limit(loss, limit)) {
            block_reasons.push("LIVE_ACTIVATION_LOSS_LIMIT_REACHED".into());
        }
        return Ok(AutomationEffectiveLossProjection {
            basis: AutomationLossReferenceBasis::LiveActivationObserved,
            limit_krw: Some(limit),
            used_krw: used,
            admission_blocked: !block_reasons.is_empty(),
            block_reasons,
        });
    }

    let mut block_reasons = Vec::new();
    if mode == AutomationSessionMode::Live {
        match day_baseline {
            None if legacy_mock_baseline.is_none() => {
                block_reasons.push("DAILY_START_BASELINE_MISSING".into());
            }
            Some(baseline)
                if baseline.sufficient_for_risk_admission
                    && mode == AutomationSessionMode::Live =>
            {
                block_reasons.push("DAILY_START_BASELINE_BLOCKED".into());
                block_reasons.push("BASELINE:LIVE_PROVIDER_AS_OF_UNAVAILABLE".into());
            }
            Some(baseline) if !baseline.sufficient_for_risk_admission => {
                block_reasons.push("DAILY_START_BASELINE_BLOCKED".into());
                block_reasons.extend(
                    baseline
                        .block_reasons
                        .iter()
                        .map(|reason| format!("BASELINE:{reason}")),
                );
            }
            Some(_) | None => {}
        }
    }
    if let Some(loss) = daily_loss {
        if !daily_loss_is_below_limit(loss, risk.max_daily_loss_krw) {
            block_reasons.push("DAILY_LOSS_LIMIT_REACHED".into());
        }
    } else {
        block_reasons.push("DAILY_LOSS_UNKNOWN".into());
    }
    Ok(AutomationEffectiveLossProjection {
        basis: AutomationLossReferenceBasis::UtcDayBaseline,
        limit_krw: Some(risk.max_daily_loss_krw),
        used_krw: daily_loss,
        admission_blocked: !block_reasons.is_empty(),
        block_reasons,
    })
}

#[allow(clippy::too_many_lines)] // One projection ties the account, risk, feed and capability readouts to the same session scope.
fn load_account_risk_projection(
    connection: &Connection,
    input: AccountRiskProjectionInput<'_>,
) -> Result<crate::contracts::AutomationAccountRiskProjection, LabError> {
    let AccountRiskProjectionInput {
        session_id,
        account_identity,
        mode,
        session_state,
        recovery_reason,
        market,
        interval,
        risk,
        feed_close,
        feed_received,
        now,
        live_execution_policy,
    } = input;
    let latest: Option<(String, String, String, String)> = connection
        .query_row(
            "SELECT source_mode,observed_at_utc,created_at_utc,balances_json \
             FROM automation_account_snapshots WHERE account_identity=?1 \
             ORDER BY snapshot_sequence DESC LIMIT 1",
            [account_identity],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let has_snapshot = latest.is_some();
    let (source_mode, observed_at, received_at, balances) = if let Some(row) = latest {
        (
            parse_mode(&row.0)?,
            Some(UtcTimestamp::parse_rfc3339(&row.1)?),
            Some(UtcTimestamp::parse_rfc3339(&row.2)?),
            serde_json::from_str::<Vec<UpbitAccountBalance>>(&row.3).map_err(json_error)?,
        )
    } else {
        (mode, None, None, Vec::new())
    };
    if source_mode != mode {
        return Err(LabError::DataCorrupt(
            "account snapshot source mode differs from the frozen automation session".into(),
        ));
    }
    let data_source = match source_mode {
        AutomationSessionMode::Mock => AutomationDataSource::DurableMockVenue,
        AutomationSessionMode::Live => AutomationDataSource::UpbitLiveRead,
    };
    let quote = balances.iter().find(|balance| balance.currency == "KRW");
    let base_code = market.base.code();
    let base = balances
        .iter()
        .find(|balance| balance.currency == base_code);
    let quote_amounts = if has_snapshot {
        Some(quote.map_or((Decimal::ZERO, Decimal::ZERO), |item| {
            (item.balance, item.locked)
        }))
    } else {
        None
    };
    let base_amounts = if has_snapshot {
        Some(base.map_or((Decimal::ZERO, Decimal::ZERO), |item| {
            (item.balance, item.locked)
        }))
    } else {
        None
    };
    let quote_amounts = account_amounts(quote_amounts)?;
    let base_amounts = asset_amounts(base_amounts)?;
    let (quote_local, local_base) = local_reservations(connection, account_identity, base_code)?;
    let balance_freshness = freshness(observed_at, risk.max_balance_age_seconds, now);
    let feed_freshness = freshness(feed_received, risk.max_feed_age_seconds, now);
    let daily_notional = daily_notional_used(connection, account_identity, now)?;
    let order_counts = order_usage(connection, account_identity, interval, now)?;
    let open_count = count_open_orders(connection, account_identity)?;
    let owned_cancelable_order_count = count_session_cancelable_orders(connection, session_id)?;
    let emergency_latched: bool = connection
        .query_row(
            "SELECT emergency_latched FROM trading_control_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let order_admission_blocked: bool = connection
        .query_row(
            "SELECT order_admission_blocked FROM trading_control_state WHERE singleton=1",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_error)?;
    let live_grant_active = connection
        .query_row(
            "SELECT state='RUNNING' AND order_admission_blocked=0 AND emergency_latched=0 \
             FROM trading_control_state WHERE singleton=1",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_error)?;
    let mark_price = latest_mark_price(connection, session_id)?;
    let asset_exposure = base_amounts
        .total
        .zip(mark_price)
        .and_then(|(quantity, price)| quantity.get().checked_mul(price))
        .map(QuoteAmount::new)
        .transpose()?;
    let day = now.0.format("%Y-%m-%d").to_string();
    let day_baseline = load_account_day_baseline_view(connection, account_identity, &day)?;
    let legacy_mock_baseline = if day_baseline.is_none() {
        load_legacy_mock_baseline_view(connection, account_identity, mode, &day, now)?
    } else {
        None
    };
    let daily_loss = daily_loss_used(
        account_identity,
        mode,
        day_baseline.as_ref(),
        legacy_mock_baseline.as_ref(),
        CurrentAccountRiskBalance {
            now,
            quote_total: quote_amounts.total,
            base_total: base_amounts.total,
            mark: mark_price,
            balances: &balances,
            base_currency: base_code,
        },
    )?;
    let activation_reference = if mode == AutomationSessionMode::Live {
        load_live_activation_reference(connection, account_identity)?
    } else {
        None
    };
    let effective_loss = effective_loss_projection(
        connection,
        account_identity,
        mode,
        risk,
        daily_loss,
        day_baseline.as_ref(),
        legacy_mock_baseline.as_ref(),
        activation_reference.as_ref(),
        has_snapshot,
        observed_at,
        balance_freshness,
        feed_freshness,
        &balances,
        now,
    )?;
    let has_open_order = open_count > 0;
    let account_projection = AutomationBalanceProjection {
        source: data_source,
        observed_at_utc: observed_at,
        received_at_utc: received_at,
        freshness: balance_freshness,
        quote_currency: "KRW".into(),
        quote_total: quote_amounts.total,
        quote_free: quote_amounts.free,
        quote_exchange_locked: quote_amounts.locked,
        quote_local_reserved: quote_local,
        base_currency: base_code.to_owned(),
        base_total: base_amounts.total,
        base_free: base_amounts.free,
        base_exchange_locked: base_amounts.locked,
        base_local_reserved: Some(AssetQuantity::new(local_base)?),
    };
    let exposure_limit = risk.max_asset_exposure_krw;
    let usage = AutomationRiskUsageProjection {
        daily_notional_used_krw: daily_notional,
        daily_notional_limit_krw: risk.max_daily_notional_krw,
        daily_loss_used_krw: daily_loss,
        daily_loss_limit_krw: risk.max_daily_loss_krw,
        asset_exposure_krw: asset_exposure,
        asset_exposure_limit_krw: exposure_limit,
        open_order_count: open_count,
        open_order_limit: risk.max_open_orders,
        orders_in_interval: order_counts,
        orders_per_interval_limit: risk.max_orders_per_interval,
        emergency_latched,
        live_grant_active,
    };
    let admission_block_reasons = compute_admission_block_reasons(
        connection,
        AdmissionBlockCheckInput {
            account_identity,
            mode,
            session_state,
            recovery_reason,
            emergency_latched,
            order_admission_blocked,
            live_grant_active,
            live_execution_policy,
            balance_freshness,
            feed_freshness,
            effective_loss: &effective_loss,
            asset_exposure,
        },
    )?;
    let capabilities = AutomationCapabilities {
        can_pause: session_state == AutomationSessionState::Running,
        can_resume: session_state == AutomationSessionState::Paused
            && !has_open_order
            && recovery_reason.is_none()
            && (mode == AutomationSessionMode::Mock
                || live_execution_policy.is_enabled()
                    && live_grant_active
                    && !effective_loss.admission_blocked),
        can_stop: session_state != AutomationSessionState::Stopped,
        can_cancel_order: mode == AutomationSessionMode::Mock && owned_cancelable_order_count > 0,
        can_submit_orders: session_state == AutomationSessionState::Running
            && !emergency_latched
            && (mode == AutomationSessionMode::Mock || !order_admission_blocked)
            && balance_freshness == AutomationFreshness::Fresh
            && feed_freshness == AutomationFreshness::Fresh
            && !effective_loss.admission_blocked
            && asset_exposure.as_ref().is_some()
            && (mode == AutomationSessionMode::Mock
                || (live_execution_policy.is_enabled() && live_grant_active)),
        live_effects_enabled: live_execution_policy.is_enabled(),
        explicit_approval_required: mode == AutomationSessionMode::Live,
    };
    let feed = AutomationFeedProjection {
        source: AutomationDataSource::UpbitPublicFeed,
        observation_close_at_utc: feed_close,
        received_at_utc: feed_received,
        freshness: feed_freshness,
    };
    let mock_assumptions = if mode == AutomationSessionMode::Mock {
        Some(AutomationMockAssumptions {
            initial_quote_balance_krw: QuoteAmount::new(Decimal::new(10_000_000, 0))?,
            initial_base_balance: AssetQuantity::new(Decimal::ZERO)?,
            fee_rate: "0.0005".into(),
            fill_model: "immediate completed-candle close".into(),
        })
    } else {
        None
    };
    Ok(crate::contracts::AutomationAccountRiskProjection {
        balance: account_projection,
        risk: usage,
        effective_loss,
        activation_reference: activation_reference.map(|reference| reference.view),
        feed,
        capabilities,
        mock_assumptions,
        legacy_mock_baseline,
        day_baseline,
        admission_block_reasons,
    })
}

struct AmountProjection<T> {
    total: Option<T>,
    free: Option<T>,
    locked: Option<T>,
}

fn account_amounts(
    amounts: Option<(Decimal, Decimal)>,
) -> Result<AmountProjection<QuoteAmount>, LabError> {
    let Some((free, locked)) = amounts else {
        return Ok(AmountProjection {
            total: None,
            free: None,
            locked: None,
        });
    };
    let total = free.checked_add(locked).ok_or_else(|| {
        LabError::DataCorrupt("account quote total exceeds decimal capacity".into())
    })?;
    Ok(AmountProjection {
        total: Some(QuoteAmount::new(total)?),
        free: Some(QuoteAmount::new(free)?),
        locked: Some(QuoteAmount::new(locked)?),
    })
}

fn asset_amounts(
    amounts: Option<(Decimal, Decimal)>,
) -> Result<AmountProjection<AssetQuantity>, LabError> {
    let Some((free, locked)) = amounts else {
        return Ok(AmountProjection {
            total: None,
            free: None,
            locked: None,
        });
    };
    let total = free.checked_add(locked).ok_or_else(|| {
        LabError::DataCorrupt("account base total exceeds decimal capacity".into())
    })?;
    Ok(AmountProjection {
        total: Some(AssetQuantity::new(total)?),
        free: Some(AssetQuantity::new(free)?),
        locked: Some(AssetQuantity::new(locked)?),
    })
}

fn local_reservations(
    connection: &Connection,
    account_identity: &str,
    base_code: &str,
) -> Result<(Option<QuoteAmount>, Decimal), LabError> {
    let quote_rows = connection
        .prepare(
            "SELECT r.quote_reserved_krw FROM automation_risk_reservations r \
             JOIN automation_orders o USING(client_identifier) WHERE r.state='ACTIVE' AND r.account_identity=?1",
        )
        .map_err(sql_error)?
        .query_map([account_identity], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let quote = sum_decimal_strings(&quote_rows)?;
    let base_rows = connection
        .prepare(
            "SELECT r.asset_reserved_volume FROM automation_risk_reservations r \
             JOIN automation_orders o USING(client_identifier) WHERE r.state='ACTIVE' \
             AND r.account_identity=?1 AND r.asset_reserved=?2",
        )
        .map_err(sql_error)?
        .query_map(params![account_identity, base_code], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    Ok((
        Some(QuoteAmount::new(quote)?),
        sum_decimal_strings(&base_rows)?,
    ))
}

/// Reconcile every current currency total against the durable account reference
/// plus unique fills and cumulative fees. This catches manual deposits,
/// withdrawals, or trades before either policy evaluation or order admission.
const ACCOUNT_FLOW_RECONCILIATION_REASON: &str = "LIVE_ACCOUNT_FLOW_REQUIRES_RECONCILIATION";

fn persist_account_flow_reconciliation(
    transaction: &Transaction<'_>,
    account_identity: &str,
    now: UtcTimestamp,
    detail: &str,
) -> Result<(), LabError> {
    let sessions = {
        let mut statement = transaction
            .prepare(
                "SELECT session_id,state,sequence,recovery_reason FROM automation_sessions \
                 WHERE account_identity=?1 AND mode='LIVE' \
                   AND state IN ('RUNNING','PAUSED','NEEDS_RECONCILIATION') \
                 ORDER BY session_id",
            )
            .map_err(sql_error)?;
        statement
            .query_map([account_identity], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?
    };
    for (raw_id, state, raw_sequence, recovery_reason) in sessions {
        if state == "NEEDS_RECONCILIATION"
            && recovery_reason.as_deref() == Some(ACCOUNT_FLOW_RECONCILIATION_REASON)
        {
            continue;
        }
        let session_id = crate::contracts::AutomationSessionId::new(raw_id)?;
        let sequence = from_sql_u64(raw_sequence)?;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit(
                "automation sequence exhausted during account flow fence".into(),
            )
        })?;
        let changed = transaction
            .execute(
                "UPDATE automation_sessions SET state='NEEDS_RECONCILIATION',recovery_reason=?1, \
                     sequence=?2,updated_at_utc=?3 \
                 WHERE session_id=?4 AND sequence=?5 AND state=?6",
                params![
                    ACCOUNT_FLOW_RECONCILIATION_REASON,
                    to_sql_u64(next_sequence)?,
                    now.to_rfc3339(),
                    session_id.as_str(),
                    to_sql_u64(sequence)?,
                    state,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "automation session changed while persisting account flow fence".into(),
            ));
        }
        reject_reserved_orders_for_session(
            transaction,
            &session_id,
            "LIVE_ACCOUNT_FLOW_UNRECONCILED_BEFORE_SEND",
            now,
        )?;
        insert_event(
            transaction,
            &session_id,
            next_event_sequence(transaction, &session_id)?,
            AutomationEventKind::NeedsReconciliation,
            now,
            serde_json::json!({
                "reason": ACCOUNT_FLOW_RECONCILIATION_REASON,
                "detail": detail,
            }),
        )?;
    }
    Ok(())
}

fn clear_account_flow_reconciliation(
    transaction: &Transaction<'_>,
    account_identity: &str,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let sessions = {
        let mut statement = transaction
            .prepare(
                "SELECT session_id,sequence FROM automation_sessions \
                 WHERE account_identity=?1 AND mode='LIVE' AND state='NEEDS_RECONCILIATION' \
                   AND recovery_reason=?2 ORDER BY session_id",
            )
            .map_err(sql_error)?;
        statement
            .query_map(
                params![account_identity, ACCOUNT_FLOW_RECONCILIATION_REASON],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?
    };
    for (raw_id, raw_sequence) in sessions {
        let session_id = crate::contracts::AutomationSessionId::new(raw_id)?;
        let sequence = from_sql_u64(raw_sequence)?;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit(
                "automation sequence exhausted after account reconciliation".into(),
            )
        })?;
        let changed = transaction
            .execute(
                "UPDATE automation_sessions SET state='PAUSED',recovery_reason=NULL,sequence=?1, \
                     updated_at_utc=?2 WHERE session_id=?3 AND state='NEEDS_RECONCILIATION' \
                     AND recovery_reason=?4 AND sequence=?5",
                params![
                    to_sql_u64(next_sequence)?,
                    now.to_rfc3339(),
                    session_id.as_str(),
                    ACCOUNT_FLOW_RECONCILIATION_REASON,
                    to_sql_u64(sequence)?,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "automation session changed while clearing account flow fence".into(),
            ));
        }
        insert_event(
            transaction,
            &session_id,
            next_event_sequence(transaction, &session_id)?,
            AutomationEventKind::Paused,
            now,
            serde_json::json!({
                "reason": "ACCOUNT_FLOW_RECONCILED_EXPLICIT_RESUME_REQUIRED",
            }),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // Baseline comparison and all-currency fill/fee folding form one account-integrity check.
fn validate_account_balance_ledger(
    connection: &Connection,
    account_identity: &str,
    observed_at: UtcTimestamp,
    balances: &[UpbitAccountBalance],
) -> Result<(), LabError> {
    let daily_baseline = connection
        .query_row(
            "SELECT baseline_balances_json,baseline_balances_digest,created_at_utc FROM automation_account_days \
             WHERE account_identity=?1 ORDER BY day_utc ASC LIMIT 1",
            [account_identity],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let (baseline, baseline_digest, baseline_at, baseline_kind) = if let Some(reference) =
        load_live_activation_reference(connection, account_identity)?
    {
        (
            reference.balances,
            reference.view.balances_digest.as_str().to_owned(),
            reference.view.reference_observed_at_utc,
            "activation",
        )
    } else if let Some((json, digest, at)) = daily_baseline {
        (
            serde_json::from_str::<Vec<UpbitAccountBalance>>(&json).map_err(json_error)?,
            digest,
            UtcTimestamp::parse_rfc3339(&at)?,
            "daily",
        )
    } else {
        return Err(LabError::BlockedEvidence(
            "shared account has no durable starting balance or LIVE activation reference".into(),
        ));
    };
    if baseline_at > observed_at {
        return Err(LabError::BlockedEvidence(
            "account snapshot predates its durable starting balance baseline".into(),
        ));
    }
    if ContentHash::of_value(&baseline)?.as_str() != baseline_digest {
        return Err(LabError::DataCorrupt(format!(
            "{baseline_kind} account balance reference digest mismatch"
        )));
    }
    let mut expected = std::collections::BTreeMap::<String, Decimal>::new();
    for balance in baseline {
        if balance.currency.is_empty()
            || balance.balance < Decimal::ZERO
            || balance.locked < Decimal::ZERO
            || expected.contains_key(&balance.currency)
        {
            return Err(LabError::DataCorrupt(
                "starting account baseline has invalid or duplicate currency amounts".into(),
            ));
        }
        let amount = balance.balance.checked_add(balance.locked).ok_or_else(|| {
            LabError::DataCorrupt("baseline account amount exceeds decimal capacity".into())
        })?;
        expected.insert(balance.currency, amount);
    }
    expected.entry("KRW".into()).or_insert(Decimal::ZERO);

    let mut statement = connection
        .prepare(
            "SELECT f.market,f.side,f.volume,f.funds_krw,f.trade_time_utc FROM automation_fills f \
             JOIN automation_orders o USING(client_identifier) WHERE o.account_identity=?1 \
             ORDER BY f.trade_time_utc,f.trade_uuid",
        )
        .map_err(sql_error)?;
    let fills = statement
        .query_map([account_identity], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(statement);
    for (raw_market, side, volume, funds, raw_time) in fills {
        let at = UtcTimestamp::parse_rfc3339(&raw_time)?;
        if at > observed_at {
            return Err(LabError::BlockedEvidence(
                "account snapshot predates a durable exchange fill".into(),
            ));
        }
        if at < baseline_at {
            continue;
        }
        let market = MarketId::parse_upbit(&raw_market)?;
        let volume = parse_decimal(&volume)?;
        let funds = parse_decimal(&funds)?;
        if volume <= Decimal::ZERO || funds <= Decimal::ZERO {
            return Err(LabError::DataCorrupt(
                "persisted account fill has a nonpositive amount".into(),
            ));
        }
        let base_currency = market.base.code().to_owned();
        let base_current = expected
            .get(&base_currency)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let quote_current = expected.get("KRW").copied().unwrap_or(Decimal::ZERO);
        match side.as_str() {
            "BID" => {
                expected.insert(
                    base_currency,
                    base_current.checked_add(volume).ok_or_else(|| {
                        LabError::DataCorrupt("expected account base balance overflow".into())
                    })?,
                );
                expected.insert(
                    "KRW".into(),
                    quote_current.checked_sub(funds).ok_or_else(|| {
                        LabError::DataCorrupt("expected account quote balance overflow".into())
                    })?,
                );
            }
            "ASK" => {
                expected.insert(
                    base_currency,
                    base_current.checked_sub(volume).ok_or_else(|| {
                        LabError::DataCorrupt("expected account base balance underflow".into())
                    })?,
                );
                expected.insert(
                    "KRW".into(),
                    quote_current.checked_add(funds).ok_or_else(|| {
                        LabError::DataCorrupt("expected account quote balance overflow".into())
                    })?,
                );
            }
            _ => {
                return Err(LabError::DataCorrupt(
                    "unknown persisted automation fill side".into(),
                ));
            }
        }
    }

    let fee_rows = connection
        .prepare(
            "SELECT f.delta_paid_fee_krw,f.observed_at_utc FROM automation_fee_accruals f \
             JOIN automation_orders o USING(client_identifier) WHERE o.account_identity=?1 \
             ORDER BY f.observed_at_utc,f.client_identifier,f.sequence",
        )
        .map_err(sql_error)?
        .query_map([account_identity], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let mut fees = Decimal::ZERO;
    for (raw_fee, raw_time) in fee_rows {
        let fee_at = UtcTimestamp::parse_rfc3339(&raw_time)?;
        if fee_at > observed_at {
            return Err(LabError::BlockedEvidence(
                "account snapshot predates a durable exchange fee accrual".into(),
            ));
        }
        if fee_at < baseline_at {
            continue;
        }
        let fee = parse_decimal(&raw_fee)?;
        if fee < Decimal::ZERO {
            return Err(LabError::DataCorrupt(
                "persisted cumulative order fee is negative".into(),
            ));
        }
        fees = fees.checked_add(fee).ok_or_else(|| {
            LabError::DataCorrupt("cumulative account fees exceed decimal capacity".into())
        })?;
    }
    let quote = expected.get("KRW").copied().unwrap_or(Decimal::ZERO);
    expected.insert(
        "KRW".into(),
        quote.checked_sub(fees).ok_or_else(|| {
            LabError::DataCorrupt("expected account quote balance underflow after fees".into())
        })?,
    );

    let mut actual = std::collections::BTreeMap::<String, Decimal>::new();
    for balance in balances {
        if balance.balance < Decimal::ZERO
            || balance.locked < Decimal::ZERO
            || actual.contains_key(&balance.currency)
        {
            return Err(LabError::DataCorrupt(
                "current account snapshot has invalid or duplicate amounts".into(),
            ));
        }
        let amount = balance.balance.checked_add(balance.locked).ok_or_else(|| {
            LabError::DataCorrupt("current account amount exceeds decimal capacity".into())
        })?;
        actual.insert(balance.currency.clone(), amount);
    }
    for currency in expected.keys().chain(actual.keys()) {
        let expected_amount = expected.get(currency).copied().unwrap_or(Decimal::ZERO);
        let actual_amount = actual.get(currency).copied().unwrap_or(Decimal::ZERO);
        if expected_amount != actual_amount {
            return Err(LabError::Conflict(format!(
                "manual or unreconciled account divergence for {currency}: venue total differs from baseline plus durable fills"
            )));
        }
    }
    Ok(())
}

fn is_legacy_mock_daily_loss_block(
    mode: AutomationSessionMode,
    projection: &crate::contracts::AutomationAccountRiskProjection,
) -> bool {
    mode == AutomationSessionMode::Mock
        && projection.effective_loss.block_reasons.len() == 1
        && projection.effective_loss.block_reasons[0] == "DAILY_LOSS_LIMIT_REACHED"
}

fn sum_decimal_strings(values: &[String]) -> Result<Decimal, LabError> {
    values.iter().try_fold(Decimal::ZERO, |total, value| {
        let amount = Decimal::from_str_exact(value).map_err(|error| {
            LabError::DataCorrupt(format!("invalid persisted decimal: {error}"))
        })?;
        total.checked_add(amount).ok_or_else(|| {
            LabError::DataCorrupt("persisted decimal sum exceeds decimal capacity".into())
        })
    })
}

fn daily_notional_used(
    connection: &Connection,
    account_identity: &str,
    now: UtcTimestamp,
) -> Result<Option<QuoteAmount>, LabError> {
    let day = now.0.format("%Y-%m-%d").to_string();
    let rows = connection
        .prepare(
            "SELECT requested_notional_krw FROM automation_orders WHERE account_identity=?1 \
             AND created_at_utc>=?2 AND state!='REJECTED' ORDER BY created_at_utc,client_identifier",
        )
        .map_err(sql_error)?
        .query_map(params![account_identity, format!("{day}T00:00:00Z")], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    Ok(Some(QuoteAmount::new(sum_decimal_strings(&rows)?)?))
}

fn order_usage(
    connection: &Connection,
    account_identity: &str,
    interval: crate::contracts::CandleInterval,
    now: UtcTimestamp,
) -> Result<Option<u32>, LabError> {
    let since = now.0 - interval.duration();
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_orders o JOIN automation_intents i USING(intent_id) \
             WHERE o.account_identity=?1 AND i.decision_time_utc>=?2 AND o.state!='REJECTED'",
            params![account_identity, UtcTimestamp(since).to_rfc3339()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    u32::try_from(count)
        .map(Some)
        .map_err(|_| LabError::DataCorrupt("negative automation order usage".into()))
}

fn count_open_orders(connection: &Connection, account_identity: &str) -> Result<u32, LabError> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 \
             AND state NOT IN ('FILLED','CANCELLED','REJECTED')",
            [account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    u32::try_from(count)
        .map_err(|_| LabError::DataCorrupt("negative automation open-order count".into()))
}

fn count_session_cancelable_orders(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
) -> Result<u32, LabError> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_orders WHERE session_id=?1 \
             AND state IN ('OPEN','PARTIALLY_FILLED')",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    u32::try_from(count)
        .map_err(|_| LabError::DataCorrupt("negative automation cancelable-order count".into()))
}

fn latest_mark_price(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
) -> Result<Option<Decimal>, LabError> {
    let json = connection
        .query_row(
            "SELECT candle_json FROM automation_candles WHERE session_id=?1 \
             ORDER BY close_time_utc DESC LIMIT 1",
            [session_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    json.map(|value| {
        let observation: crate::contracts::CandleObservation =
            serde_json::from_str(&value).map_err(json_error)?;
        Ok(observation.candle.close.get())
    })
    .transpose()
}

#[derive(Clone, Copy)]
struct CurrentAccountRiskBalance<'a> {
    now: UtcTimestamp,
    quote_total: Option<QuoteAmount>,
    base_total: Option<AssetQuantity>,
    mark: Option<Decimal>,
    balances: &'a [UpbitAccountBalance],
    base_currency: &'a str,
}

fn daily_loss_used(
    account_identity: &str,
    mode: AutomationSessionMode,
    contract_baseline: Option<&AutomationAccountDayBaselineView>,
    legacy_mock_baseline: Option<&AutomationLegacyMockBaselineView>,
    current: CurrentAccountRiskBalance<'_>,
) -> Result<Option<QuoteAmount>, LabError> {
    let CurrentAccountRiskBalance {
        now,
        quote_total,
        base_total,
        mark,
        balances,
        base_currency,
    } = current;
    let day = now.0.format("%Y-%m-%d").to_string();
    let baseline = match (contract_baseline, legacy_mock_baseline) {
        (Some(view), None) => {
            if view.mode != mode {
                return Err(LabError::DataCorrupt(
                    "account day baseline mode differs from the frozen automation session".into(),
                ));
            }
            if mode == AutomationSessionMode::Live
                || !view.sufficient_for_risk_admission
                || view.verification_state != AutomationBaselineVerificationState::Verified
            {
                return Ok(None);
            }
            view.total_equity_krw.map(|equity| equity.get().to_string())
        }
        (None, Some(legacy)) if mode == AutomationSessionMode::Mock => {
            if legacy.account_identity != account_identity || legacy.day_utc != day {
                return Err(LabError::DataCorrupt(
                    "legacy MOCK baseline projection differs from the frozen account/day scope"
                        .into(),
                ));
            }
            Some(legacy.baseline_equity_krw.get().to_string())
        }
        (None, None) => None,
        _ => {
            return Err(LabError::DataCorrupt(
                "typed and legacy MOCK baselines overlap or have an invalid session mode".into(),
            ));
        }
    };
    if balances.iter().any(|balance| {
        balance.currency != "KRW"
            && balance.currency != base_currency
            && (balance.balance != Decimal::ZERO || balance.locked != Decimal::ZERO)
    }) {
        return Ok(None);
    }
    let (Some(baseline), Some(quote), Some(base), Some(mark)) =
        (baseline, quote_total, base_total, mark)
    else {
        return Ok(None);
    };
    let baseline = Decimal::from_str_exact(&baseline).map_err(|error| {
        LabError::DataCorrupt(format!("invalid automation day baseline: {error}"))
    })?;
    let equity = quote
        .get()
        .checked_add(base.get().checked_mul(mark).ok_or_else(|| {
            LabError::DataCorrupt("automation asset value exceeds decimal capacity".into())
        })?)
        .ok_or_else(|| {
            LabError::DataCorrupt("automation equity exceeds decimal capacity".into())
        })?;
    let loss = (baseline - equity).max(Decimal::ZERO);
    Ok(Some(QuoteAmount::new(loss)?))
}

fn freshness(
    observed: Option<UtcTimestamp>,
    max_age_seconds: u32,
    now: UtcTimestamp,
) -> AutomationFreshness {
    let Some(observed) = observed else {
        return AutomationFreshness::Unavailable;
    };
    let age_seconds = (now.0 - observed.0).num_seconds();
    if age_seconds < 0 {
        return AutomationFreshness::Unavailable;
    }
    if age_seconds > i64::from(max_age_seconds) {
        AutomationFreshness::Stale
    } else if age_seconds.saturating_mul(2) > i64::from(max_age_seconds) {
        AutomationFreshness::Delayed
    } else {
        AutomationFreshness::Fresh
    }
}

fn latest_balance_times(
    connection: &Connection,
    account_identity: &str,
) -> Result<Option<(UtcTimestamp, UtcTimestamp)>, LabError> {
    let row = connection
        .query_row(
            "SELECT observed_at_utc,created_at_utc FROM automation_account_snapshots \
             WHERE account_identity=?1 ORDER BY snapshot_sequence DESC LIMIT 1",
            [account_identity],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(observed, received)| {
        Ok((
            UtcTimestamp::parse_rfc3339(&observed)?,
            UtcTimestamp::parse_rfc3339(&received)?,
        ))
    })
    .transpose()
}

pub(super) fn load_orders(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
    limit: u32,
    offset: i64,
) -> Result<Vec<AutomationOrderView>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT client_identifier,intent_id,market,side,state,venue_order_uuid, \
                    requested_notional_krw,reserved_fee_krw,executed_volume,paid_fee_krw, \
                    locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc \
             FROM automation_orders WHERE session_id=?1 \
             ORDER BY created_at_utc,client_identifier LIMIT ?2 OFFSET ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![session_id.as_str(), i64::from(limit), offset],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
    rows.into_iter()
        .map(|row| {
            let side = parse_side(&row.3)?;
            let locked = parse_decimal(&row.10)?;
            let (locked_quote_krw, locked_base) = match (side, row.11.as_str()) {
                (crate::contracts::AutomationSide::Bid, "KRW") => {
                    (Some(QuoteAmount::new(locked)?), None)
                }
                (crate::contracts::AutomationSide::Ask, currency)
                    if currency == MarketId::parse_upbit(&row.2)?.base.code() =>
                {
                    (None, Some(AssetQuantity::new(locked)?))
                }
                _ => {
                    return Err(LabError::DataCorrupt(
                        "stored order locked currency differs from side and market".into(),
                    ));
                }
            };
            Ok(AutomationOrderView {
                client_identifier: AutomationOrderIdentifier::new(row.0)?,
                intent_id: AutomationIntentId::new(row.1)?,
                market: MarketId::parse_upbit(&row.2)?,
                side,
                state: parse_order_state(&row.4)?,
                venue_order_uuid: row.5,
                requested_notional_krw: QuoteAmount::new(parse_decimal(&row.6)?)?,
                reserved_fee_krw: QuoteAmount::new(parse_decimal(&row.7)?)?,
                executed_volume: AssetQuantity::new(parse_decimal(&row.8)?)?,
                paid_fee_krw: QuoteAmount::new(parse_decimal(&row.9)?)?,
                locked_quote_krw,
                locked_base,
                trade_count: from_sql_u64(row.12)?,
                created_at_utc: UtcTimestamp::parse_rfc3339(&row.13)?,
                updated_at_utc: UtcTimestamp::parse_rfc3339(&row.14)?,
            })
        })
        .collect()
}

pub(super) fn load_fills(
    connection: &Connection,
    session_id: &crate::contracts::AutomationSessionId,
    limit: u32,
    offset: i64,
) -> Result<Vec<AutomationFillView>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT trade_uuid,client_identifier,market,side,price_krw,volume,funds_krw,trade_time_utc \
             FROM automation_fills WHERE session_id=?1 \
             ORDER BY trade_time_utc,trade_uuid LIMIT ?2 OFFSET ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![session_id.as_str(), i64::from(limit), offset],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
    rows.into_iter()
        .map(|row| {
            Ok(AutomationFillView {
                trade_uuid: row.0,
                client_identifier: AutomationOrderIdentifier::new(row.1)?,
                market: MarketId::parse_upbit(&row.2)?,
                side: parse_side(&row.3)?,
                price_krw: crate::contracts::PriceKrw::new(parse_decimal(&row.4)?)?,
                volume: AssetQuantity::new(parse_decimal(&row.5)?)?,
                funds_krw: QuoteAmount::new(parse_decimal(&row.6)?)?,
                observed_at_utc: UtcTimestamp::parse_rfc3339(&row.7)?,
            })
        })
        .collect()
}

fn parse_side(value: &str) -> Result<crate::contracts::AutomationSide, LabError> {
    match value {
        "BID" => Ok(crate::contracts::AutomationSide::Bid),
        "ASK" => Ok(crate::contracts::AutomationSide::Ask),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation order side {value}"
        ))),
    }
}

fn order_state_name(state: AutomationOrderState) -> &'static str {
    match state {
        AutomationOrderState::Reserved => "RESERVED",
        AutomationOrderState::SubmittedUnknown => "SUBMITTED_UNKNOWN",
        AutomationOrderState::Open => "OPEN",
        AutomationOrderState::PartiallyFilled => "PARTIALLY_FILLED",
        AutomationOrderState::Filled => "FILLED",
        AutomationOrderState::Cancelled => "CANCELLED",
        AutomationOrderState::Rejected => "REJECTED",
        AutomationOrderState::NeedsReconciliation => "NEEDS_RECONCILIATION",
    }
}

fn parse_order_state(value: &str) -> Result<AutomationOrderState, LabError> {
    match value {
        "RESERVED" => Ok(AutomationOrderState::Reserved),
        "SUBMITTED_UNKNOWN" => Ok(AutomationOrderState::SubmittedUnknown),
        "OPEN" => Ok(AutomationOrderState::Open),
        "PARTIALLY_FILLED" => Ok(AutomationOrderState::PartiallyFilled),
        "FILLED" => Ok(AutomationOrderState::Filled),
        "CANCELLED" => Ok(AutomationOrderState::Cancelled),
        "REJECTED" => Ok(AutomationOrderState::Rejected),
        "NEEDS_RECONCILIATION" => Ok(AutomationOrderState::NeedsReconciliation),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown automation order state {value}"
        ))),
    }
}

fn side_name(side: crate::contracts::AutomationSide) -> &'static str {
    match side {
        crate::contracts::AutomationSide::Bid => "BID",
        crate::contracts::AutomationSide::Ask => "ASK",
    }
}

fn min_total_for(rules: &VerifiedMarketRules, side: crate::contracts::AutomationSide) -> Decimal {
    match side {
        crate::contracts::AutomationSide::Bid => rules.bid_min_total,
        crate::contracts::AutomationSide::Ask => rules.ask_min_total,
    }
}

fn reservation_totals_for_admission(
    transaction: &Transaction<'_>,
    account_identity: &str,
    base_currency: &str,
) -> Result<(Decimal, Decimal, Decimal), LabError> {
    let mut statement = transaction
        .prepare(
            "SELECT o.side,o.state,o.market,o.requested_notional_krw, \
                    r.quote_reserved_krw,r.asset_reserved,r.asset_reserved_volume \
             FROM automation_risk_reservations r JOIN automation_orders o USING(client_identifier) \
             WHERE r.account_identity=?1 AND r.state='ACTIVE'",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([account_identity], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })
        .map_err(sql_error)?;
    let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
    let mut unsubmitted_quote = Decimal::ZERO;
    let mut unsubmitted_asset = Decimal::ZERO;
    let mut pending_bid_exposure = Decimal::ZERO;
    for (side, state, market, notional, quote, asset, asset_volume) in rows {
        if state == "RESERVED" && side == "BID" {
            unsubmitted_quote = checked_sum(unsubmitted_quote, parse_decimal(&quote)?)?;
        }
        if state == "RESERVED" && side == "ASK" && asset == base_currency {
            unsubmitted_asset = checked_sum(unsubmitted_asset, parse_decimal(&asset_volume)?)?;
        }
        if side == "BID" && market == format!("KRW-{base_currency}") {
            pending_bid_exposure = checked_sum(pending_bid_exposure, parse_decimal(&notional)?)?;
        }
    }
    Ok((unsubmitted_quote, unsubmitted_asset, pending_bid_exposure))
}

fn checked_sum(left: Decimal, right: Decimal) -> Result<Decimal, LabError> {
    left.checked_add(right)
        .ok_or_else(|| LabError::AccountingInvariant("automation risk sum overflow".into()))
}

fn read_order_fill_facts(
    transaction: &Transaction<'_>,
    identifier: &AutomationOrderIdentifier,
) -> Result<(u64, Decimal), LabError> {
    let mut statement = transaction
        .prepare(
            "SELECT volume FROM automation_fills WHERE client_identifier=?1 ORDER BY trade_uuid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([identifier.as_str()], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    let mut count = 0_u64;
    let mut volume = Decimal::ZERO;
    for row in rows {
        let row = row.map_err(sql_error)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("automation fill count overflow".into()))?;
        volume = checked_sum(volume, parse_decimal(&row)?)?;
    }
    Ok((count, volume))
}

fn merge_order_state(
    previous: AutomationOrderState,
    observed: AutomationOrderState,
    previous_volume: Decimal,
    observed_volume: Decimal,
) -> Result<AutomationOrderState, LabError> {
    match previous {
        AutomationOrderState::Filled => {
            if observed_volume != previous_volume {
                return Err(LabError::DataCorrupt(
                    "filled order cumulative execution changed after terminal proof".into(),
                ));
            }
            Ok(AutomationOrderState::Filled)
        }
        AutomationOrderState::Cancelled => match observed {
            AutomationOrderState::Filled => Ok(AutomationOrderState::Filled),
            AutomationOrderState::Cancelled => Ok(AutomationOrderState::Cancelled),
            AutomationOrderState::Open | AutomationOrderState::PartiallyFilled
                if observed_volume == previous_volume =>
            {
                Ok(AutomationOrderState::Cancelled)
            }
            AutomationOrderState::Open | AutomationOrderState::PartiallyFilled => {
                Ok(AutomationOrderState::NeedsReconciliation)
            }
            _ => Err(LabError::DataCorrupt(
                "cancelled order received an incompatible terminal state".into(),
            )),
        },
        AutomationOrderState::Rejected => Err(LabError::DataCorrupt(
            "rejected order cannot receive later venue state".into(),
        )),
        AutomationOrderState::Reserved
        | AutomationOrderState::SubmittedUnknown
        | AutomationOrderState::Open
        | AutomationOrderState::PartiallyFilled
        | AutomationOrderState::NeedsReconciliation => Ok(observed),
    }
}

fn load_order_view(
    connection: &Connection,
    identifier: &AutomationOrderIdentifier,
) -> Result<Option<AutomationOrderView>, LabError> {
    let row = connection
        .query_row(
            "SELECT client_identifier,intent_id,market,side,state,venue_order_uuid, \
                    requested_notional_krw,reserved_fee_krw,executed_volume,paid_fee_krw, \
                    locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc \
             FROM automation_orders WHERE client_identifier=?1",
            [identifier.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|row| {
        let side = parse_side(&row.3)?;
        let market = MarketId::parse_upbit(&row.2)?;
        let locked_amount = parse_decimal(&row.10)?;
        let (locked_quote_krw, locked_base) = match (side, row.11.as_str()) {
            (crate::contracts::AutomationSide::Bid, "KRW") => {
                (Some(QuoteAmount::new(locked_amount)?), None)
            }
            (crate::contracts::AutomationSide::Ask, currency) if currency == market.base.code() => {
                (None, Some(AssetQuantity::new(locked_amount)?))
            }
            _ => {
                return Err(LabError::DataCorrupt(
                    "order lock unit does not match side".into(),
                ));
            }
        };
        Ok(AutomationOrderView {
            client_identifier: AutomationOrderIdentifier::new(row.0)?,
            intent_id: AutomationIntentId::new(row.1)?,
            market,
            side,
            state: parse_order_state(&row.4)?,
            venue_order_uuid: row.5,
            requested_notional_krw: QuoteAmount::new(parse_decimal(&row.6)?)?,
            reserved_fee_krw: QuoteAmount::new(parse_decimal(&row.7)?)?,
            executed_volume: AssetQuantity::new(parse_decimal(&row.8)?)?,
            paid_fee_krw: QuoteAmount::new(parse_decimal(&row.9)?)?,
            locked_quote_krw,
            locked_base,
            trade_count: from_sql_u64(row.12)?,
            created_at_utc: UtcTimestamp::parse_rfc3339(&row.13)?,
            updated_at_utc: UtcTimestamp::parse_rfc3339(&row.14)?,
        })
    })
    .transpose()
}

fn parse_decimal(value: &str) -> Result<Decimal, LabError> {
    Decimal::from_str_exact(value).map_err(|error| {
        LabError::DataCorrupt(format!("invalid persisted automation decimal: {error}"))
    })
}

mod admission_deferred;
#[cfg(test)]
mod admission_deferred_tests;
mod decision_preparation;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn preparation_is_deduplicated_without_advancing_candle_or_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-preparation-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root)?;
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2025-01-02T02:01:00Z")?;
            let session = seed_running_automation_session(
                &mut store,
                "preparation-session",
                "KRW-BTC",
                "mock:upbit-default",
                &baseline_test_risk_policy()?,
                now,
            )?;
            let observation =
                completed_test_observation("2025-01-02T02:00:00Z", "2025-01-02T03:00:00Z")?;
            let preparation = crate::contracts::DecisionPreparation {
                status: crate::contracts::DecisionPreparationStatus::PrivateReadinessBlocked,
                market: MarketId::parse_upbit("KRW-BTC")?,
                interval: crate::contracts::CandleInterval::H1,
                observation_id: observation.id.clone(),
                decision_time: observation.candle.close_time_utc,
                completed_bars_seen: 7,
                required_bars: 10,
            };
            let snapshot = |store: &Store| -> Result<(String, Option<String>, i64), LabError> {
                store.connection.query_row(
                    "SELECT checkpoint_json,last_finalized_candle_at_utc,sequence FROM automation_sessions WHERE session_id=?1",
                    [session.as_str()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                ).map_err(sql_error)
            };
            let before = snapshot(&store)?;
            let received = UtcTimestamp::parse_rfc3339("2025-01-02T03:00:01Z")?;
            assert!(store.automation_record_preparation(&session, &preparation, received)?);
            assert!(!store.automation_record_preparation(&session, &preparation, received)?);
            assert_eq!(snapshot(&store)?, before);
            let counts: (i64, i64, i64) = store
                .connection
                .query_row(
                    "SELECT (SELECT count(*) FROM automation_candles), \
                 (SELECT count(*) FROM automation_intents), \
                 (SELECT count(*) FROM automation_session_events WHERE kind='DECISION_DEFERRED')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(sql_error)?;
            assert_eq!(counts, (0, 0, 1));
            let mut changed = preparation.clone();
            changed.status = crate::contracts::DecisionPreparationStatus::AwaitingPolicySources;
            assert!(store.automation_record_preparation(&session, &changed, received)?);
            assert!(store.automation_record_preparation(&session, &preparation, received)?);
            assert!(!store.automation_record_preparation(&session, &preparation, received)?);
            assert_eq!(snapshot(&store)?, before);
            store.automation_record_decision(
                &session,
                &observation,
                "{}",
                &AutomationIntentId::from_seed("ready-after-preparation"),
                "HOLD",
                &serde_json::json!({"original_test_decision":true}),
                received,
            )?;
            assert!(!store.automation_record_preparation(&session, &preparation, received)?);
            assert_eq!(
                snapshot(&store)?.1,
                Some(observation.candle.close_time_utc.to_rfc3339())
            );
            store.close()?;
            Ok(())
        })();
        fs::remove_dir_all(&root)?;
        result?;
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one held-session fixture proves CAS, frozen-policy retention, existing reservation reconciliation, admission fence and restart"
    )]
    fn source_coverage_hold_is_durable_idempotent_and_fences_new_reservations()
    -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-source-hold-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).expect("isolated source-hold store");
        let result = (|| {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2025-01-02T02:01:00Z")?;
            let market = MarketId::parse_upbit("KRW-BTC")?;
            let id = seed_running_automation_session(
                &mut store,
                "source-hold-session",
                "KRW-BTC",
                "mock:upbit-default",
                &baseline_test_risk_policy()?,
                now,
            )?;
            assert!(matches!(
                store.automation_hold_source_gap(&id, 2, "SOURCE_COVERAGE_GAP", now),
                Err(LabError::Conflict(_))
            ));
            let policy_before: String = store
                .connection
                .query_row(
                    "SELECT frozen_policy_json FROM automation_sessions WHERE session_id=?1",
                    [id.as_str()],
                    |r| r.get(0),
                )
                .map_err(sql_error)?;
            let existing_order =
                seed_reserved_test_order(&mut store, &id, "before-source-gap", now)?;
            let reservation_snapshot =
                |store: &Store| -> Result<(String, String, String, String), LabError> {
                    store.connection.query_row(
                    "SELECT o.order_request_json,o.state,r.quote_reserved_krw,r.state \
                     FROM automation_orders o JOIN automation_risk_reservations r USING(client_identifier) \
                     WHERE o.client_identifier=?1", [existing_order.as_str()],
                    |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
                ).map_err(sql_error)
                };
            let reserved_before = reservation_snapshot(&store)?;
            store.automation_hold_source_gap(&id, 1, "SOURCE_COVERAGE_GAP", now)?;
            store.automation_hold_source_gap(&id, 1, "SOURCE_COVERAGE_GAP", now)?;
            let (state, reason, policy_after): (String, String, String) = store.connection.query_row(
                "SELECT state,recovery_reason,frozen_policy_json FROM automation_sessions WHERE session_id=?1", [id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ).map_err(sql_error)?;
            assert_eq!(state, "NEEDS_RECONCILIATION");
            assert_eq!(reason, "SOURCE_COVERAGE_GAP");
            assert_eq!(policy_before, policy_after);
            assert_eq!(reservation_snapshot(&store)?, reserved_before);
            let events: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_session_events WHERE session_id=?1",
                    [id.as_str()],
                    |r| r.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(events, 1);
            let intent = AutomationIntentId::from_seed("source-hold-intent");
            let notional = QuoteAmount::new(Decimal::from(9000))?;
            let rules = VerifiedMarketRules {
                market: market.code(),
                source_mode: AutomationSessionMode::Mock,
                observed_at_utc: now,
                state: "active".into(),
                bid_types: vec!["price".into()],
                ask_types: vec!["market".into()],
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                bid_min_total: Decimal::from(5000),
                ask_min_total: Decimal::from(5000),
                max_total: Decimal::from(1_000_000_000),
                bid_price_unit: None,
                ask_price_unit: None,
            };
            let denied = store.automation_admit_order(
                AutomationOrderAdmission {
                    session_id: id.clone(),
                    intent_id: intent.clone(),
                    request: AutomationOrderRequest {
                        client_identifier: AutomationOrderIdentifier::from_intent(&intent),
                        market,
                        side: crate::contracts::AutomationSide::Bid,
                        order: AutomationOrderKind::MarketBuy { budget: notional },
                    },
                    decision_mark: crate::contracts::PriceKrw::new(Decimal::from(10000))?,
                    decision_time: now,
                    requested_notional: notional,
                    quote_reserve: QuoteAmount::new(Decimal::new(90045, 1))?,
                    asset_currency: "BTC".into(),
                    asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                    fee_reserve: QuoteAmount::new(Decimal::new(45, 1))?,
                    rules,
                    evaluation_json: serde_json::json!({}),
                },
                now,
            );
            assert!(
                matches!(denied, Err(LabError::Conflict(reason)) if reason.contains("session is not running"))
            );
            let reservations: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_risk_reservations",
                    [],
                    |r| r.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(reservations, 1);
            // The hold preserves pre-existing reservations for ordinary reconciliation.
            // This is an isolated mock venue; no provider request or real order occurs.
            store.automation_seed_mock_venue(now)?;
            let request: AutomationOrderRequest =
                serde_json::from_str(&reserved_before.0).map_err(json_error)?;
            let detail = store.automation_mock_submit_order(
                &request,
                crate::contracts::PriceKrw::new(Decimal::from(100))?,
                now,
            )?;
            let reconciled = store.automation_reconcile_order(&detail, now)?;
            assert_eq!(reconciled.client_identifier, existing_order);
            assert_eq!(
                store.automation_get_session(&id, now)?.state,
                AutomationSessionState::NeedsReconciliation
            );
            drop(store);
            let reopened = Store::open(&root)?;
            assert_eq!(
                reopened.automation_get_session(&id, now)?.state,
                AutomationSessionState::NeedsReconciliation
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn daily_loss_boundary_blocks_at_and_above_the_cap() -> Result<(), LabError> {
        let limit = QuoteAmount::new(Decimal::from(100))?;
        assert!(daily_loss_is_below_limit(
            QuoteAmount::new(Decimal::from(99))?,
            limit
        ));
        assert!(!daily_loss_is_below_limit(limit, limit));
        assert!(!daily_loss_is_below_limit(
            QuoteAmount::new(Decimal::from(101))?,
            limit
        ));
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one account journey proves immutable reference, persisted flow fence and blocked resume/send against cached data"
    )]
    fn live_activation_reference_is_immutable_and_blocks_unreconciled_balance_changes()
    -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-live-activation-reference-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated activation-reference test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let account_identity = format!("live:upbit:{}", "d".repeat(64));
            let now = UtcTimestamp::parse_rfc3339("2026-10-15T12:00:00Z")?;
            let control = store.apply_trading_control(
                crate::upbit::control::TradingControlAction::Start,
                "live-activation-test-control-start",
                0,
                crate::upbit::control::ControlDecisionContext {
                    live_trading_enabled: true,
                    approval_valid: true,
                },
                &now.to_rfc3339(),
            )?;
            assert_eq!(
                control.snapshot.state,
                crate::upbit::control::TradingControlState::Running
            );
            let mut risk = baseline_test_risk_policy()?;
            risk.activation_loss_limit_krw = Some(QuoteAmount::new(Decimal::from(1_000))?);
            let session_id = seed_live_automation_session(
                &mut store,
                "live-activation-reference-session",
                &account_identity,
                &risk,
                now,
            )?;
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1,\
                     last_feed_received_at_utc=?1 WHERE session_id=?2",
                    params![now.to_rfc3339(), session_id.as_str()],
                )
                .map_err(sql_error)?;
            let balances = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(100_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            store.automation_record_account_snapshot_with_policy(
                &account_identity,
                AutomationSessionMode::Live,
                now,
                now,
                &balances,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let view = store.automation_get_session_with_policy(
                &session_id,
                now,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let risk_projection = view.account_risk.as_ref().ok_or_else(|| {
                LabError::DataCorrupt("LIVE activation risk projection missing".into())
            })?;
            assert_eq!(
                risk_projection.effective_loss.basis,
                AutomationLossReferenceBasis::LiveActivationObserved
            );
            assert_eq!(
                risk_projection.effective_loss.limit_krw,
                Some(QuoteAmount::new(Decimal::from(1_000))?)
            );
            assert_eq!(
                risk_projection.effective_loss.used_krw,
                Some(QuoteAmount::new(Decimal::ZERO)?)
            );
            assert!(!risk_projection.effective_loss.admission_blocked);
            let reference = risk_projection
                .activation_reference
                .as_ref()
                .ok_or_else(|| LabError::DataCorrupt("LIVE activation reference missing".into()))?;
            assert_eq!(
                reference.source,
                AutomationActivationReferenceSource::CurrentLocalObserved
            );
            assert_eq!(
                reference.valuation_state,
                AutomationActivationValuationState::Verified
            );
            assert_eq!(reference.reference_observed_at_utc, now);
            assert_eq!(
                reference.reference_equity_krw,
                Some(QuoteAmount::new(Decimal::from(100_000))?)
            );
            let initial_digest = reference.balances_digest.clone();

            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1 \
                     WHERE session_id=?2",
                    params!["2026-10-15T11:00:00Z", session_id.as_str()],
                )
                .map_err(sql_error)?;
            let observation = completed_market_observation(
                "KRW-BTC",
                "2026-10-15T11:00:00Z",
                "2026-10-15T12:00:00Z",
                Decimal::from(10),
            )?;
            let intent_id = AutomationIntentId::from_seed("live-activation-flow-order");
            let evaluation = serde_json::json!({
                "decision_time_utc":now.to_rfc3339(),
                "constrained_target_weight":0.001
            });
            store.automation_record_decision(
                &session_id,
                &observation,
                r#"{"cursor":1}"#,
                &intent_id,
                "BUY",
                &evaluation,
                now,
            )?;
            let order_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&intent_id),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: QuoteAmount::new(Decimal::from(5_000))?,
                },
            };
            let rules = VerifiedMarketRules {
                market: "KRW-BTC".into(),
                source_mode: AutomationSessionMode::Live,
                observed_at_utc: now,
                state: "active".into(),
                bid_types: vec!["price".into()],
                ask_types: vec!["market".into()],
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                bid_min_total: Decimal::from(5_000),
                ask_min_total: Decimal::from(5_000),
                max_total: Decimal::from(1_000_000),
                bid_price_unit: None,
                ask_price_unit: None,
            };
            let order_identifier = order_request.client_identifier.clone();
            let reserved = store.automation_admit_order_with_policy(
                AutomationOrderAdmission {
                    session_id: session_id.clone(),
                    intent_id,
                    request: order_request,
                    decision_mark: crate::contracts::PriceKrw::new(Decimal::from(10))?,
                    decision_time: observation.candle.close_time_utc,
                    requested_notional: QuoteAmount::new(Decimal::from(5_000))?,
                    quote_reserve: QuoteAmount::new(Decimal::new(50_025, 1))?,
                    asset_currency: "BTC".into(),
                    asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                    fee_reserve: QuoteAmount::new(Decimal::new(25, 1))?,
                    rules: rules.clone(),
                    evaluation_json: evaluation,
                },
                now,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            assert_eq!(reserved.state, AutomationOrderState::Reserved);

            let later = UtcTimestamp(now.0 + chrono::Duration::seconds(1));
            let unexplained_change = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(95_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            assert!(matches!(
                store.automation_record_account_snapshot_with_policy(
                    &account_identity,
                    AutomationSessionMode::Live,
                    later,
                    later,
                    &unexplained_change,
                    LiveExecutionPolicy::OperatorEnabled,
                ),
                Err(LabError::BlockedEvidence(_))
            ));
            let persisted_snapshot: (String, String) = store
                .connection
                .query_row(
                    "SELECT observed_at_utc,balances_json FROM automation_account_snapshots \
                     WHERE account_identity=?1 ORDER BY snapshot_sequence DESC LIMIT 1",
                    [&account_identity],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(sql_error)?;
            assert_eq!(UtcTimestamp::parse_rfc3339(&persisted_snapshot.0)?, later);
            assert_eq!(
                serde_json::from_str::<Vec<UpbitAccountBalance>>(&persisted_snapshot.1)
                    .map_err(json_error)?[0]
                    .balance,
                Decimal::from(95_000)
            );
            let fenced = store.automation_get_session_with_policy(
                &session_id,
                later,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            assert_eq!(fenced.state, AutomationSessionState::NeedsReconciliation);
            assert_eq!(
                fenced.recovery_reason.as_deref(),
                Some(ACCOUNT_FLOW_RECONCILIATION_REASON)
            );
            let effective_loss = &fenced
                .account_risk
                .as_ref()
                .ok_or_else(|| LabError::DataCorrupt("LIVE risk projection missing".into()))?
                .effective_loss;
            assert!(effective_loss.admission_blocked);
            assert!(
                effective_loss
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "LIVE_ACCOUNT_FLOW_UNRECONCILED")
            );
            assert!(matches!(
                store.automation_write_with_policy(
                    &AutomationSessionWrite::Resume {
                        request_id: RequestId::new("resume-after-unreconciled-live-flow")?,
                        session_id: session_id.clone(),
                        expected_sequence: fenced.sequence,
                    },
                    None,
                    None,
                    later,
                    LiveExecutionPolicy::OperatorEnabled,
                ),
                Err(LabError::Conflict(_))
            ));
            assert!(matches!(
                store.automation_claim_order_for_send_with_policy(
                    &order_identifier,
                    &rules,
                    later,
                    LiveExecutionPolicy::OperatorEnabled,
                ),
                Err(LabError::Conflict(_))
            ));
            assert_eq!(
                store
                    .automation_order_readback(&order_identifier)?
                    .ok_or_else(|| LabError::DataCorrupt("reserved order disappeared".into()))?
                    .state,
                AutomationOrderState::Rejected
            );
            let unchanged =
                load_live_activation_reference(&store.connection, &account_identity)?
                    .ok_or_else(|| LabError::DataCorrupt("activation anchor disappeared".into()))?;
            assert_eq!(unchanged.view.reference_observed_at_utc, now);
            assert_eq!(unchanged.view.balances_digest, initial_digest);
            assert_eq!(
                unchanged.view.reference_equity_krw,
                Some(QuoteAmount::new(Decimal::from(100_000))?)
            );
            let matched_at = UtcTimestamp(later.0 + chrono::Duration::seconds(1));
            store.automation_record_account_snapshot_with_policy(
                &account_identity,
                AutomationSessionMode::Live,
                matched_at,
                matched_at,
                &balances,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let matched = store.automation_get_session_with_policy(
                &session_id,
                matched_at,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            assert_eq!(matched.state, AutomationSessionState::Paused);
            assert_eq!(matched.recovery_reason, None);
            let resume = AutomationSessionWrite::Resume {
                request_id: RequestId::new("resume-only-after-fresh-live-account")?,
                session_id: session_id.clone(),
                expected_sequence: matched.sequence,
            };
            store.automation_begin_request(&resume, Some(&account_identity), matched_at)?;
            let resumed = store.automation_write_with_policy(
                &resume,
                None,
                None,
                matched_at,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            assert_eq!(resumed.session.state, AutomationSessionState::Running);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one held-coin fixture proves immutable reference and causal valuation despite later mark ingestion"
    )]
    fn activation_reference_values_existing_coin_at_its_own_time_after_late_mark_ingest()
    -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-activation-mark-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).expect("create isolated activation mark test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let identity = format!("live:upbit:{}", "e".repeat(64));
            let start = UtcTimestamp::parse_rfc3339("2026-10-15T10:00:00Z")?;
            let reference_at = UtcTimestamp::parse_rfc3339("2026-10-15T11:30:00Z")?;
            let received_at = UtcTimestamp::parse_rfc3339("2026-10-15T12:00:00Z")?;
            store.apply_trading_control(
                crate::upbit::control::TradingControlAction::Start,
                "activation-held-coin-control",
                0,
                crate::upbit::control::ControlDecisionContext {
                    live_trading_enabled: true,
                    approval_valid: true,
                },
                &start.to_rfc3339(),
            )?;
            let mut risk = baseline_test_risk_policy()?;
            risk.activation_loss_limit_krw = Some(QuoteAmount::new(Decimal::from(1_000))?);
            risk.max_feed_age_seconds = 3_600;
            let session = seed_live_automation_session(
                &mut store,
                "activation-held-coin",
                &identity,
                &risk,
                start,
            )?;
            let balances = vec![
                UpbitAccountBalance {
                    currency: "BTC".into(),
                    balance: Decimal::ONE,
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::from(1_000),
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
                UpbitAccountBalance {
                    currency: "KRW".into(),
                    balance: Decimal::from(99_000),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::ZERO,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
            ];
            store.automation_record_account_snapshot_with_policy(
                &identity,
                AutomationSessionMode::Live,
                reference_at,
                reference_at,
                &balances,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let pending = load_live_activation_reference(&store.connection, &identity)?
                .expect("first snapshot retains pending valuation");
            assert_eq!(
                pending.view.valuation_state,
                AutomationActivationValuationState::PendingMarks
            );
            for (open, close, price) in [
                ("2026-10-15T10:00:00Z", "2026-10-15T11:00:00Z", 1_000),
                ("2026-10-15T11:00:00Z", "2026-10-15T12:00:00Z", 2_000),
            ] {
                let observation =
                    completed_market_observation("KRW-BTC", open, close, Decimal::from(price))?;
                store.automation_record_candle(&session, &observation, "{}", received_at)?;
            }
            store.automation_record_account_snapshot_with_policy(
                &identity,
                AutomationSessionMode::Live,
                received_at,
                received_at,
                &balances,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let verified = load_live_activation_reference(&store.connection, &identity)?
                .expect("pending reference is preserved");
            assert_eq!(
                verified.view.valuation_state,
                AutomationActivationValuationState::Verified
            );
            assert_eq!(
                verified.view.snapshot_sequence,
                pending.view.snapshot_sequence
            );
            assert_eq!(verified.view.reference_observed_at_utc, reference_at);
            assert_eq!(verified.view.valuation_recorded_at_utc, received_at);
            assert_eq!(
                verified.view.reference_equity_krw,
                Some(QuoteAmount::new(Decimal::from(100_000))?)
            );
            let StoredLiveActivationValuation::Verified { assets, .. } = verified.valuation else {
                unreachable!("checked verified valuation")
            };
            let btc = assets
                .iter()
                .find(|asset| asset.currency == "BTC")
                .expect("BTC valuation");
            assert_eq!(btc.mark_price_krw.as_deref(), Some("1000"));
            assert_eq!(
                btc.mark_close_at_utc,
                Some(UtcTimestamp::parse_rfc3339("2026-10-15T11:00:00Z")?)
            );
            let current = store.automation_get_session_with_policy(
                &session,
                received_at,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            assert_eq!(
                current
                    .account_risk
                    .expect("account risk")
                    .effective_loss
                    .used_krw,
                Some(QuoteAmount::new(Decimal::ZERO)?)
            );
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET state='STOPPED' WHERE session_id=?1",
                    [session.as_str()],
                )
                .map_err(sql_error)?;
            let next_day = UtcTimestamp(received_at.0 + chrono::Duration::days(1));
            seed_live_automation_session(
                &mut store,
                "activation-held-coin-next-session",
                &identity,
                &risk,
                next_day,
            )?;
            store.automation_record_account_snapshot_with_policy(
                &identity,
                AutomationSessionMode::Live,
                next_day,
                next_day,
                &balances,
                LiveExecutionPolicy::OperatorEnabled,
            )?;
            let retained = load_live_activation_reference(&store.connection, &identity)?
                .expect("new day and session preserve the same account reference");
            assert_eq!(retained.view, verified.view);
            store.close()?;
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn legacy_live_day_does_not_admit_or_occupy_the_baseline_contract_slot() -> Result<(), LabError>
    {
        with_legacy_live_baseline_fixture(
            "legacy-live-slot",
            |store, session_id, account_identity, day_start, now| {
                let before = store.automation_get_session(session_id, now)?;
                assert_eq!(
                    before
                        .account_risk
                        .as_ref()
                        .and_then(|risk| risk.risk.daily_loss_used_krw),
                    None
                );
                let status = store.automation_baseline_status(session_id, None, now)?;
                assert!(status.baseline.is_none());
                assert!(status.admission_blocked);
                assert!(
                    status
                        .block_reasons
                        .iter()
                        .any(|reason| reason == "DAILY_START_BASELINE_MISSING")
                );

                let recorded = store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("legacy-live-baseline-import")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: account_identity.to_owned(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day_start,
                            source: AutomationBaselineSource::ExchangeObservedDayStart,
                            assets: vec![krw_baseline_asset(Decimal::from(10_000))?],
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    now,
                )?;
                let baseline = recorded.baseline.as_ref().expect("baseline readback");
                assert_eq!(
                    baseline.verification_state,
                    AutomationBaselineVerificationState::BlockedInsufficientEvidence
                );
                assert!(
                    baseline
                        .block_reasons
                        .iter()
                        .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
                );
                assert_eq!(baseline.exchange_evidence, None);
                assert_eq!(
                    recorded
                        .session
                        .account_risk
                        .as_ref()
                        .and_then(|risk| risk.risk.daily_loss_used_krw),
                    None
                );
                let legacy_equity: String = store
                    .connection
                    .query_row(
                        "SELECT baseline_equity_krw FROM automation_account_days \
                     WHERE account_identity=?1 AND day_utc='2026-10-14'",
                        [account_identity],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                assert_eq!(legacy_equity, "10000");
                Ok(())
            },
        )
    }

    #[test]
    fn live_local_midnight_snapshot_is_not_provider_asof_evidence() -> Result<(), LabError> {
        use crate::contracts::AutomationBaselineAssetInput;

        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-live-local-midnight-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let account_identity = format!("live:upbit:{}", "e".repeat(64));
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let risk = baseline_test_risk_policy()?;
            let session_id = seed_live_automation_session(
                &mut store,
                "live-local-midnight-baseline-session",
                &account_identity,
                &risk,
                now,
            )?;
            let balances = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(10_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            store.automation_record_account_snapshot(
                &account_identity,
                AutomationSessionMode::Live,
                now,
                now,
                &balances,
            )?;
            let recorded = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("live-local-midnight-baseline")?,
                    session_id: session_id.clone(),
                    expected_sequence: 1,
                    baseline: AutomationAccountDayBaselineInput {
                        account_identity: account_identity.clone(),
                        day_utc: "2026-10-14".into(),
                        reference_time_utc: now,
                        source: AutomationBaselineSource::ExchangeObservedDayStart,
                        assets: vec![AutomationBaselineAssetInput {
                            currency: "KRW".into(),
                            free: AssetQuantity::new(Decimal::from(10_000))?,
                            locked: AssetQuantity::new(Decimal::ZERO)?,
                            valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                            mark_price_krw: None,
                        }],
                        operator_note: None,
                    },
                },
                None,
                None,
                now,
            )?;
            let baseline = recorded.baseline.as_ref().expect("baseline readback");
            assert_eq!(
                baseline.verification_state,
                AutomationBaselineVerificationState::BlockedInsufficientEvidence
            );
            assert!(!baseline.sufficient_for_risk_admission);
            assert!(
                baseline
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
            );
            assert_eq!(baseline.exchange_evidence, None);
            assert_eq!(
                recorded
                    .session
                    .account_risk
                    .as_ref()
                    .and_then(|projection| projection.risk.daily_loss_used_krw),
                None
            );
            let tampered_status = store
                .connection
                .execute(
                    "UPDATE automation_account_day_baselines \
                     SET verification_state='VERIFIED',sufficient_for_risk_admission=1,\
                         total_equity_krw='10000' \
                     WHERE account_identity=?1 AND day_utc='2026-10-14'",
                    [&account_identity],
                )
                .map_err(sql_error)?;
            assert_eq!(tampered_status, 1);
            assert!(matches!(
                store.automation_get_session(&session_id, now),
                Err(LabError::DataCorrupt(message))
                    if message.contains("status, identity, or digest verification")
            ));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_post_midnight_terminal_fill_and_fee_block_a_new_day_baseline() -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-mock-post-midnight-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let day_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:10:00Z")?;
            store.automation_seed_mock_venue(day_start)?;
            let risk = baseline_test_risk_policy()?;
            let session_id = seed_running_automation_session(
                &mut store,
                "mock-post-midnight-baseline-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                day_start,
            )?;
            let midnight_mark = completed_market_observation(
                "KRW-BTC",
                "2026-10-13T23:00:00Z",
                "2026-10-14T00:00:00Z",
                Decimal::from(100),
            )?;
            insert_test_candle(&mut store, &session_id, &midnight_mark)?;
            let reconciled = fill_mock_test_order(&mut store, &session_id, "post-midnight", now)?;
            assert_eq!(reconciled.state, AutomationOrderState::Filled);
            assert!(reconciled.paid_fee_krw.get() > Decimal::ZERO);

            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                now,
                now,
                &balances,
            )?;
            let legacy_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_account_days \
                     WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                    [],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let contract_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_account_day_baselines \
                     WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                    [],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(legacy_count, 0);
            assert_eq!(contract_count, 0);

            let assets = mock_baseline_assets(&balances)?;
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("mock-post-midnight-baseline")?,
                        session_id,
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: "mock:upbit-default".into(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day_start,
                            source: AutomationBaselineSource::DurableMockVenueSeed,
                            assets,
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    now,
                ),
                Err(LabError::Conflict(message)) if message.contains("same-day trade activity")
            ));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_fee_accrued_after_midnight_blocks_even_when_fill_precedes_day() -> Result<(), LabError>
    {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-mock-late-fee-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let seed_at = UtcTimestamp::parse_rfc3339("2026-10-13T00:00:00Z")?;
            let prior_day_fill_at = UtcTimestamp::parse_rfc3339("2026-10-13T23:30:00Z")?;
            let day_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let fee_observed_at = UtcTimestamp::parse_rfc3339("2026-10-14T00:10:00Z")?;
            store.automation_seed_mock_venue(seed_at)?;
            let risk = baseline_test_risk_policy()?;
            let session_id = seed_running_automation_session(
                &mut store,
                "mock-late-fee-baseline-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                prior_day_fill_at,
            )?;
            seed_prior_day_fill_with_late_fee(
                &mut store,
                &session_id,
                prior_day_fill_at,
                fee_observed_at,
            )?;

            let balances = store.automation_mock_venue_accounts()?;
            let assets = mock_baseline_assets(&balances)?;
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("mock-late-fee-baseline")?,
                        session_id,
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: "mock:upbit-default".into(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day_start,
                            source: AutomationBaselineSource::DurableMockVenueSeed,
                            assets,
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    fee_observed_at,
                ),
                Err(LabError::Conflict(message)) if message.contains("same-day trade activity")
            ));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_first_day_seed_after_midnight_keeps_its_actual_reference_time() -> Result<(), LabError>
    {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-mock-post-midnight-genesis-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let day_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let seed_at = UtcTimestamp::parse_rfc3339("2026-10-14T00:10:00Z")?;
            store.automation_seed_mock_venue(seed_at)?;
            let session_id = seed_running_automation_session(
                &mut store,
                "mock-post-midnight-genesis-session",
                "KRW-BTC",
                "mock:upbit-default",
                &baseline_test_risk_policy()?,
                seed_at,
            )?;
            let balances = store.automation_mock_venue_accounts()?;
            let recorded = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("mock-post-midnight-genesis-baseline")?,
                    session_id,
                    expected_sequence: 1,
                    baseline: AutomationAccountDayBaselineInput {
                        account_identity: "mock:upbit-default".into(),
                        day_utc: "2026-10-14".into(),
                        reference_time_utc: day_start,
                        source: AutomationBaselineSource::DurableMockVenueSeed,
                        assets: mock_baseline_assets(&balances)?,
                        operator_note: None,
                    },
                },
                None,
                None,
                seed_at,
            )?;
            let baseline = recorded
                .baseline
                .as_ref()
                .expect("MOCK seed baseline readback");
            assert!(baseline.sufficient_for_risk_admission);
            assert_eq!(
                baseline.source,
                AutomationBaselineSource::DurableMockVenueSeed
            );
            assert_eq!(baseline.reference_time_utc, seed_at);
            assert_eq!(baseline.recorded_at_utc, seed_at);
            assert_eq!(baseline.exchange_evidence, None);
            assert!(baseline.block_reasons.is_empty());
            let stored: (String, String) = store
                .connection
                .query_row(
                    "SELECT reference_time_utc,created_at_utc \
                     FROM automation_account_day_baselines \
                     WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(sql_error)?;
            assert_eq!(stored.0, seed_at.to_rfc3339());
            assert_eq!(stored.1, seed_at.to_rfc3339());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn local_mock_snapshot_cannot_verify_exchange_day_start_source() -> Result<(), LabError> {
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:10:00Z")?;
        with_legacy_mock_baseline_fixture(
            "mock-exchange-source",
            now,
            |store, session_id, balances, _| {
                let snapshot_mode: String = store.connection.query_row(
                "SELECT source_mode FROM automation_account_snapshots \
                 WHERE account_identity='mock:upbit-default' ORDER BY snapshot_sequence DESC LIMIT 1",
                [],
                |row| row.get(0),
            ).map_err(sql_error)?;
                assert_eq!(snapshot_mode, "MOCK");
                let recorded = store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("mock-exchange-day-start-source")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: "mock:upbit-default".into(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: UtcTimestamp::parse_rfc3339(
                                "2026-10-14T00:00:00Z",
                            )?,
                            source: AutomationBaselineSource::ExchangeObservedDayStart,
                            assets: mock_baseline_assets(balances)?,
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    now,
                )?;
                let baseline = recorded
                    .baseline
                    .as_ref()
                    .expect("blocked baseline readback");
                assert!(!baseline.sufficient_for_risk_admission);
                assert_eq!(baseline.exchange_evidence, None);
                assert!(
                    baseline
                        .block_reasons
                        .iter()
                        .any(|reason| reason == "EXCHANGE_DAY_START_REQUIRES_LIVE_PROVIDER_AS_OF")
                );
                Ok(())
            },
        )
    }

    #[test]
    fn mock_seed_baseline_rejects_a_wallet_that_no_longer_matches_genesis() -> Result<(), LabError>
    {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-mock-mutated-seed-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:10:00Z")?;
            let day_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            store.automation_seed_mock_venue(day_start)?;
            let risk = baseline_test_risk_policy()?;
            let session_id = seed_running_automation_session(
                &mut store,
                "mock-mutated-seed-baseline-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                day_start,
            )?;
            store
                .connection
                .execute(
                    "UPDATE automation_mock_venue_accounts SET balance='9000000' \
                     WHERE currency='KRW'",
                    [],
                )
                .map_err(sql_error)?;
            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                now,
                now,
                &balances,
            )?;
            let recorded = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("mock-mutated-seed-baseline")?,
                    session_id,
                    expected_sequence: 1,
                    baseline: AutomationAccountDayBaselineInput {
                        account_identity: "mock:upbit-default".into(),
                        day_utc: "2026-10-14".into(),
                        reference_time_utc: day_start,
                        source: AutomationBaselineSource::DurableMockVenueSeed,
                        assets: mock_baseline_assets(&balances)?,
                        operator_note: None,
                    },
                },
                None,
                None,
                now,
            )?;
            let baseline = recorded.baseline.as_ref().expect("baseline readback");
            assert!(!baseline.sufficient_for_risk_admission);
            assert!(
                baseline
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "DURABLE_MOCK_SEED_GENESIS_MISMATCH")
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn valid_mock_legacy_baseline_is_explicit_for_risk() -> Result<(), LabError> {
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        with_legacy_mock_baseline_fixture(
            "legacy-projection",
            now,
            |store, session_id, _, digest| {
                let status = store.automation_baseline_status(session_id, None, now)?;
                assert!(status.baseline.is_none());
                assert!(!status.admission_blocked);
                let legacy = status
                    .legacy_mock_baseline
                    .as_ref()
                    .expect("explicit legacy MOCK aggregate projection");
                assert_eq!(legacy.account_identity, "mock:upbit-default");
                assert_eq!(legacy.day_utc, "2026-10-14");
                assert_eq!(
                    legacy.baseline_equity_krw,
                    QuoteAmount::new(Decimal::from(10_000_000))?
                );
                assert_eq!(&legacy.balances_digest, digest);
                assert_eq!(legacy.recorded_at_utc, now);

                let risk = store
                    .automation_get_session(session_id, now)?
                    .account_risk
                    .expect("risk projection");
                assert_eq!(risk.legacy_mock_baseline, status.legacy_mock_baseline);
                assert_eq!(
                    risk.risk.daily_loss_used_krw,
                    Some(QuoteAmount::new(Decimal::ZERO)?)
                );
                assert!(risk.capabilities.can_submit_orders);
                Ok(())
            },
        )
    }

    #[test]
    fn legacy_mock_baseline_rejects_corrupt_digest_and_future_local_time() -> Result<(), LabError> {
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        with_legacy_mock_baseline_fixture(
            "legacy-corruption",
            now,
            |store, session_id, _, digest| {
                store
                    .connection
                    .execute(
                        "UPDATE automation_account_days SET baseline_balances_digest='invalid' \
                 WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                        [],
                    )
                    .map_err(sql_error)?;
                assert!(matches!(
                    store.automation_baseline_status(session_id, None, now),
                    Err(LabError::DataCorrupt(message)) if message.contains("balance digest")
                ));
                store
                    .connection
                    .execute(
                        "UPDATE automation_account_days SET baseline_balances_digest=?1,\
                     created_at_utc='2026-10-14T00:00:01Z' \
                 WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                        [digest.as_str()],
                    )
                    .map_err(sql_error)?;
                assert!(matches!(
                    store.automation_baseline_status(session_id, None, now),
                    Err(LabError::DataCorrupt(message)) if message.contains("local record time")
                ));
                Ok(())
            },
        )
    }

    #[test]
    fn typed_mock_baseline_does_not_overwrite_legacy_aggregate() -> Result<(), LabError> {
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        with_legacy_mock_baseline_fixture(
            "legacy-typed-precedence",
            now,
            |store, session_id, balances, digest| {
                let typed = store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("mock-legacy-baseline-typed-upgrade")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: "mock:upbit-default".into(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: now,
                            source: AutomationBaselineSource::DurableMockVenueSeed,
                            assets: mock_baseline_assets(balances)?,
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    now,
                )?;
                assert!(
                    typed
                        .baseline
                        .as_ref()
                        .is_some_and(|row| row.sufficient_for_risk_admission)
                );
                let old: (String, String) = store.connection.query_row(
                "SELECT baseline_equity_krw,baseline_balances_digest FROM automation_account_days \
                 WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).map_err(sql_error)?;
                assert_eq!(old.0, "10000000");
                assert_eq!(old.1, digest.as_str());
                assert!(
                    typed
                        .session
                        .account_risk
                        .as_ref()
                        .is_some_and(|risk| risk.legacy_mock_baseline.is_none())
                );
                let status = store.automation_baseline_status(session_id, None, now)?;
                assert!(status.baseline.is_some());
                assert!(status.legacy_mock_baseline.is_none());
                assert!(!status.admission_blocked);
                Ok(())
            },
        )
    }

    #[test]
    fn legacy_mock_baseline_survives_a_later_filled_order_and_fee() -> Result<(), LabError> {
        let baseline_at = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        with_legacy_mock_baseline_fixture(
            "legacy-later-fill",
            baseline_at,
            |store, session_id, _, _| {
                let fill_at = UtcTimestamp(baseline_at.0 + chrono::Duration::seconds(1));
                let filled = fill_mock_test_order(store, session_id, "later-fill", fill_at)?;
                assert_eq!(filled.state, AutomationOrderState::Filled);
                assert!(filled.paid_fee_krw.get() > Decimal::ZERO);
                let balances = store.automation_mock_venue_accounts()?;
                store
                    .connection
                    .execute(
                        "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1,\
                     last_feed_received_at_utc=?1 WHERE session_id=?2",
                        params![fill_at.to_rfc3339(), session_id.as_str()],
                    )
                    .map_err(sql_error)?;
                store.automation_record_account_snapshot(
                    "mock:upbit-default",
                    AutomationSessionMode::Mock,
                    fill_at,
                    fill_at,
                    &balances,
                )?;
                let status = store.automation_baseline_status(session_id, None, fill_at)?;
                assert!(status.legacy_mock_baseline.is_some());
                assert!(status.baseline.is_none());
                let risk = store
                    .automation_get_session(session_id, fill_at)?
                    .account_risk
                    .expect("risk projection");
                assert!(risk.legacy_mock_baseline.is_some());
                assert!(risk.balance.quote_total.is_some());
                assert!(risk.balance.base_total.is_some());
                let mark = latest_mark_price(&store.connection, session_id)?;
                assert!(mark.is_some());
                let direct_daily_loss = daily_loss_used(
                    "mock:upbit-default",
                    AutomationSessionMode::Mock,
                    None,
                    risk.legacy_mock_baseline.as_ref(),
                    CurrentAccountRiskBalance {
                        now: fill_at,
                        quote_total: risk.balance.quote_total,
                        base_total: risk.balance.base_total,
                        mark,
                        balances: &balances,
                        base_currency: "BTC",
                    },
                )?;
                assert!(
                    direct_daily_loss.is_some(),
                    "balances={balances:?}, risk={risk:?}"
                );
                assert_eq!(risk.risk.daily_loss_used_krw, direct_daily_loss);
                assert!(risk.risk.daily_loss_used_krw.is_some());
                Ok(())
            },
        )
    }

    #[test]
    fn later_open_order_does_not_erase_legacy_mock_baseline_readback() -> Result<(), LabError> {
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        with_legacy_mock_baseline_fixture(
            "legacy-later-open-order",
            now,
            |store, session_id, _, _| {
                let later = UtcTimestamp(now.0 + chrono::Duration::seconds(1));
                let identifier =
                    seed_reserved_test_order(store, session_id, "after-baseline", later)?;
                store
                    .connection
                    .execute(
                        "UPDATE automation_orders SET state='OPEN' WHERE client_identifier=?1",
                        [identifier.as_str()],
                    )
                    .map_err(sql_error)?;
                let status = store.automation_baseline_status(session_id, None, later)?;
                assert!(status.legacy_mock_baseline.is_some());
                assert!(status.baseline.is_none());
                Ok(())
            },
        )
    }

    #[test]
    fn live_snapshot_without_provider_asof_does_not_invent_a_daily_loss_baseline()
    -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-live-late-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let balances = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(10_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            let account_identity = format!("live:upbit:{}", "a".repeat(64));
            for raw_now in ["2026-10-14T12:00:00Z", "2026-10-15T00:00:00Z"] {
                let now = UtcTimestamp::parse_rfc3339(raw_now)?;
                store.automation_record_account_snapshot(
                    &account_identity,
                    AutomationSessionMode::Live,
                    now,
                    now,
                    &balances,
                )?;
                let baseline_count: i64 = store
                    .connection
                    .query_row(
                        "SELECT COUNT(*) FROM automation_account_days WHERE account_identity=?1",
                        [&account_identity],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                assert_eq!(baseline_count, 0);
                assert_eq!(
                    daily_loss_used(
                        &account_identity,
                        AutomationSessionMode::Live,
                        None,
                        None,
                        CurrentAccountRiskBalance {
                            now,
                            quote_total: Some(QuoteAmount::new(Decimal::from(10_000))?),
                            base_total: Some(AssetQuantity::new(Decimal::ZERO)?),
                            mark: Some(Decimal::from(1_000)),
                            balances: &balances,
                            base_currency: "BTC",
                        },
                    )?,
                    None
                );
            }
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_day_baseline_waits_until_every_owned_order_is_terminal() -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-open-order-baseline-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T01:00:00Z")?;
            store.automation_seed_mock_venue(now)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(10_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(2_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 3_600,
            };
            let session_id = seed_running_automation_session(
                &mut store,
                "baseline-open-order-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                now,
            )?;
            let identifier = seed_reserved_test_order(&mut store, &session_id, "baseline", now)?;
            for (index, state) in ["RESERVED", "OPEN", "PARTIALLY_FILLED"]
                .into_iter()
                .enumerate()
            {
                store
                    .connection
                    .execute(
                        "UPDATE automation_orders SET state=?1 WHERE client_identifier=?2",
                        params![state, identifier.as_str()],
                    )
                    .map_err(sql_error)?;
                let seconds = i64::try_from(index + 1).map_err(|error| {
                    LabError::ResourceLimit(format!("test timestamp overflow: {error}"))
                })?;
                let observed = UtcTimestamp(now.0 + chrono::Duration::seconds(seconds));
                let balances = store.automation_mock_venue_accounts()?;
                store.automation_record_account_snapshot(
                    "mock:upbit-default",
                    AutomationSessionMode::Mock,
                    observed,
                    observed,
                    &balances,
                )?;
                let baseline_count: i64 = store
                    .connection
                    .query_row(
                        "SELECT COUNT(*) FROM automation_account_days \
                         WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                assert_eq!(
                    baseline_count, 0,
                    "nonterminal order state {state} cannot seed a baseline"
                );
            }
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_fill_and_fee_at_baseline_second_are_applied_after_snapshot_commit()
    -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-same-second-baseline-fill-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T01:00:00Z")?;
            store.automation_seed_mock_venue(now)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(10_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(2_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 3_600,
            };
            let session_id = seed_running_automation_session(
                &mut store,
                "same-second-baseline-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                now,
            )?;
            let starting_balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                now,
                now,
                &starting_balances,
            )?;
            let identifier = seed_reserved_test_order(&mut store, &session_id, "same-second", now)?;
            let pending = store
                .automation_pending_orders(&session_id)?
                .into_iter()
                .find(|order| order.request.client_identifier == identifier)
                .ok_or_else(|| LabError::DataCorrupt("same-second test order is missing".into()))?;
            let detail =
                store.automation_mock_submit_order(&pending.request, pending.decision_mark, now)?;
            let reconciled = store.automation_reconcile_order(&detail, now)?;
            assert_eq!(reconciled.state, AutomationOrderState::Filled);
            assert_eq!(reconciled.paid_fee_krw.get(), Decimal::new(45, 1));

            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                now,
                now,
                &balances,
            )?;
            let (quote, base): (String, String) = store
                .connection
                .query_row(
                    "SELECT \
                         (SELECT balance FROM automation_mock_venue_accounts WHERE currency='KRW'),\
                         (SELECT balance FROM automation_mock_venue_accounts WHERE currency='BTC')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(sql_error)?;
            assert_eq!(parse_decimal(&quote)?, Decimal::new(99_909_955, 1));
            assert_eq!(parse_decimal(&base)?, Decimal::from(90));
            validate_account_balance_ledger(
                &store.connection,
                "mock:upbit-default",
                now,
                &balances,
            )?;
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn mock_day_rollover_values_held_assets_at_the_finalized_midnight_mark() -> Result<(), LabError>
    {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-mock-day-rollover-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let previous_day = UtcTimestamp::parse_rfc3339("2026-10-13T00:00:00Z")?;
            let next_day = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            store.automation_seed_mock_venue(previous_day)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(10_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(2_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 3_600,
            };
            let session_id = seed_running_automation_session(
                &mut store,
                "day-rollover-btc-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                previous_day,
            )?;
            let mark = completed_market_observation(
                "KRW-BTC",
                "2026-10-13T23:00:00Z",
                "2026-10-14T00:00:00Z",
                Decimal::from(1_100),
            )?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,'{}')",
                    params![
                        session_id.as_str(),
                        mark.id.as_str(),
                        mark.candle.close_time_utc.to_rfc3339(),
                        mark.content_digest.as_str(),
                        serde_json::to_string(&mark).map_err(json_error)?,
                    ],
                )
                .map_err(sql_error)?;
            store
                .connection
                .execute(
                    "UPDATE automation_mock_venue_accounts SET balance='9000' WHERE currency='KRW'",
                    [],
                )
                .map_err(sql_error)?;
            store
                .connection
                .execute(
                    "UPDATE automation_mock_venue_accounts SET balance='1',avg_buy_price='1000' WHERE currency='BTC'",
                    [],
                )
                .map_err(sql_error)?;
            let initial = store.automation_mock_venue_accounts()?;
            let initial_json = serde_json::to_string(&initial).map_err(json_error)?;
            let initial_digest = ContentHash::of_value(&initial)?.to_string();
            store
                .connection
                .execute(
                    "INSERT INTO automation_account_days(account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,baseline_balances_json,created_at_utc) \
                     VALUES ('mock:upbit-default','2026-10-13','10100',?1,?2,?3)",
                    params![initial_digest, initial_json, previous_day.to_rfc3339()],
                )
                .map_err(sql_error)?;

            let balances = store.automation_mock_venue_accounts()?;
            let observed = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:05Z")?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                observed,
                observed,
                &balances,
            )?;
            let (equity, baseline_at): (String, String) = store
                .connection
                .query_row(
                    "SELECT baseline_equity_krw,created_at_utc FROM automation_account_days \
                     WHERE account_identity='mock:upbit-default' AND day_utc='2026-10-14'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(sql_error)?;
            assert_eq!(equity, "10100");
            assert_eq!(UtcTimestamp::parse_rfc3339(&baseline_at)?, next_day);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn due_session_keyset_pages_reach_older_unchanged_rows_and_wrap() -> Result<(), LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-due-pages-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T02:00:00Z")?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(10_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(20_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(2_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 3_600,
            };
            let created_at = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let mut ids = Vec::new();
            for index in 0..7 {
                let raw_id = format!("automation-due-{index:02}");
                ids.push(raw_id.clone());
                seed_running_automation_session(
                    &mut store,
                    &raw_id,
                    "KRW-BTC",
                    "mock:upbit-default",
                    &risk,
                    created_at,
                )?;
            }
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET state='PAUSED', \
                         recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK' \
                     WHERE session_id=?1",
                    [&ids[5]],
                )
                .map_err(sql_error)?;

            let first = store.automation_due_sessions(now, 4, None)?;
            assert_eq!(first.total_count, 7);
            assert_eq!(first.sessions.len(), 4);
            let second = store.automation_due_sessions(now, 4, first.next_cursor.as_ref())?;
            assert_eq!(second.total_count, 7);
            assert_eq!(second.sessions.len(), 3);
            let third = store.automation_due_sessions(now, 4, second.next_cursor.as_ref())?;
            assert_eq!(third.total_count, 7);
            assert!(third.sessions.is_empty());
            assert!(third.next_cursor.is_none());

            let mut reached = first
                .sessions
                .iter()
                .chain(second.sessions.iter())
                .map(|record| record.view.session_id.as_str().to_owned())
                .collect::<Vec<_>>();
            reached.sort();
            assert_eq!(reached, ids);
            assert!(
                second
                    .sessions
                    .iter()
                    .any(|record| record.view.state == AutomationSessionState::Paused)
            );
            let wrapped = store.automation_due_sessions(now, 4, None)?;
            assert_eq!(
                wrapped
                    .sessions
                    .iter()
                    .map(|record| record.view.session_id.as_str())
                    .collect::<Vec<_>>(),
                first
                    .sessions
                    .iter()
                    .map(|record| record.view.session_id.as_str())
                    .collect::<Vec<_>>()
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    fn shared_account_balance_reconciliation_blocks_manual_cash_divergence() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-account-ledger-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let store = Store::open(&root)?;
            let account_identity = "mock:upbit-default";
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let baseline = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::new(10_000_000, 0),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            let baseline_json = serde_json::to_string(&baseline).map_err(json_error)?;
            let baseline_digest = ContentHash::of_value(&baseline)?.to_string();
            store
                .connection
                .execute(
                    "INSERT INTO automation_account_days(\
                         account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,\
                         baseline_balances_json,created_at_utc\
                     ) VALUES (?1,'2026-10-14','10000000',?2,?3,?4)",
                    params![
                        account_identity,
                        baseline_digest,
                        baseline_json,
                        now.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
            validate_account_balance_ledger(&store.connection, account_identity, now, &baseline)?;

            let manually_changed = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::new(10_000_001, 0),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            assert!(matches!(
                validate_account_balance_ledger(
                    &store.connection,
                    account_identity,
                    now,
                    &manually_changed
                ),
                Err(LabError::Conflict(_))
            ));
            Ok(())
        })();
        let cleanup = fs::remove_dir_all(&root);
        assert!(
            cleanup.is_ok(),
            "isolated automation test root should be removed"
        );
        assert!(
            result.is_ok(),
            "account reconciliation test failed: {result:?}"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)] // One crash/reopen decision journey proves cursor, checkpoint, intent and lost-write readback atomically.
    fn completed_candle_checkpoint_and_intent_commit_atomically_and_replay_idempotently() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-decision-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let session_id = crate::contracts::AutomationSessionId::new("decision-session")?;
            let account_identity = "mock:upbit-default";
            let start = UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::new(100_000, 0))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::new(1_000_000, 0))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::new(500_000, 0))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::new(100_000, 0))?,
                activation_loss_limit_krw: None,
                max_open_orders: 3,
                max_orders_per_interval: 2,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            store
                .connection
                .execute(
                    "INSERT INTO automation_sessions(\
                         session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                         frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                     ) VALUES (?1,'decision-start',?2,'MOCK','RUNNING',1,'{}','{}','KRW-BTC','h1',?3,'{}',?4,?4)",
                    params![session_id.as_str(), account_identity, serde_json::to_string(&risk).map_err(json_error)?,start.to_rfc3339()],
                )
                .map_err(sql_error)?;

            let observation =
                completed_test_observation("2026-10-14T00:00:00Z", "2026-10-14T01:00:00Z")?;
            let intent_id = AutomationIntentId::from_seed("decision-session:first-bar");
            let evaluation = serde_json::json!({
                "decision_time_utc":"2026-10-14T01:00:00Z",
                "constrained_target_weight":0.001
            });
            let checkpoint = r#"{"cursor":1}"#;
            assert!(store.automation_record_decision(
                &session_id,
                &observation,
                checkpoint,
                &intent_id,
                "BUY",
                &evaluation,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:01Z")?,
            )?);
            drop(store);
            let mut store = Store::open(&root)?;
            let persisted_checkpoint: String = store
                .connection
                .query_row(
                    "SELECT checkpoint_json FROM automation_sessions WHERE session_id=?1",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(persisted_checkpoint, checkpoint);
            assert!(!store.automation_record_decision(
                &session_id,
                &observation,
                checkpoint,
                &intent_id,
                "BUY",
                &evaluation,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
            )?);
            let pending = store.automation_pending_intents(&session_id)?;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].intent_id, intent_id);
            assert_eq!(pending[0].decision_mark.get(), Decimal::from(105));

            let next_observation =
                completed_test_observation("2026-10-14T01:00:00Z", "2026-10-14T02:00:00Z")?;
            assert!(matches!(
                store.automation_record_decision(
                    &session_id,
                    &next_observation,
                    r#"{"cursor":2}"#,
                    &AutomationIntentId::from_seed("decision-session:second-bar"),
                    "HOLD",
                    &serde_json::json!({"decision_time_utc":"2026-10-14T02:00:00Z"}),
                    UtcTimestamp::parse_rfc3339("2026-10-14T02:00:01Z")?,
                ),
                Err(LabError::Conflict(_))
            ));
            let candle_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_candles WHERE session_id=?1",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let intent_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_intents WHERE session_id=?1",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(candle_count, 1);
            assert_eq!(intent_count, 1);

            let pause_write = AutomationSessionWrite::Pause {
                request_id: RequestId::new("pause-readback-recovery")?,
                session_id: session_id.clone(),
                expected_sequence: 1,
            };
            let missing =
                store.automation_readback_request(&RequestId::new("never-recorded-command")?)?;
            assert_eq!(
                missing.state,
                crate::contracts::AutomationRequestReadbackState::NotRecorded
            );
            assert!(missing.response.is_none());
            assert!(missing.reason.unwrap_or_default().contains("unconfirmed"));
            let begun = store.automation_begin_request(
                &pause_write,
                None,
                UtcTimestamp::parse_rfc3339("2026-10-14T02:00:03Z")?,
            )?;
            assert_eq!(
                begun.state,
                crate::contracts::AutomationRequestReadbackState::Pending
            );
            assert!(begun.response.is_none());
            assert_eq!(
                store
                    .automation_pending_lifecycle_requests(10)?
                    .into_iter()
                    .filter(|pending| {
                        request_action(&pending.write).0 == request_action(&pause_write).0
                    })
                    .count(),
                1
            );
            let digest = request_digest(&pause_write, None)?;
            store.automation_reject_request(
                request_action(&pause_write).0,
                &digest,
                "TEST_CONFIRMED_NO_EFFECT_REJECTION",
            )?;
            let rejected = store.automation_readback_request(request_action(&pause_write).0)?;
            assert_eq!(
                rejected.state,
                crate::contracts::AutomationRequestReadbackState::Rejected
            );
            assert!(rejected.response.is_none());
            assert_eq!(
                store
                    .automation_begin_request(
                        &pause_write,
                        None,
                        UtcTimestamp::parse_rfc3339("2026-10-14T02:00:04Z")?,
                    )?
                    .state,
                crate::contracts::AutomationRequestReadbackState::Rejected
            );
            Ok(())
        })();
        let cleanup = fs::remove_dir_all(&root);
        assert!(
            cleanup.is_ok(),
            "isolated automation test root should be removed"
        );
        assert!(
            result.is_ok(),
            "atomic decision persistence test failed: {result:?}"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)] // One cross-market fixture proves shared cash and interval admission against one account.
    fn account_quote_reservations_and_interval_budget_are_shared_across_markets() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-shared-risk-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let account_identity = "mock:upbit-default";
            let snapshot_at = UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(10_000_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(20_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(20_000_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 5,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            store.automation_seed_mock_venue(snapshot_at)?;
            let first_session = seed_running_automation_session(
                &mut store,
                "shared-btc-session",
                "KRW-BTC",
                account_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?,
            )?;
            let second_session = seed_running_automation_session(
                &mut store,
                "shared-eth-session",
                "KRW-ETH",
                account_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?,
            )?;
            let third_session = seed_running_automation_session(
                &mut store,
                "shared-xrp-session",
                "KRW-XRP",
                account_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?,
            )?;
            let btc_observation = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T00:00:00Z",
                "2026-10-14T01:00:00Z",
                Decimal::from(10_000),
            )?;
            let eth_observation = completed_market_observation(
                "KRW-ETH",
                "2026-10-14T00:00:00Z",
                "2026-10-14T01:00:00Z",
                Decimal::from(2_000),
            )?;
            let xrp_observation = completed_market_observation(
                "KRW-XRP",
                "2026-10-14T00:00:00Z",
                "2026-10-14T01:00:00Z",
                Decimal::from(100),
            )?;
            let btc_intent = AutomationIntentId::from_seed("shared-risk:btc");
            let eth_intent = AutomationIntentId::from_seed("shared-risk:eth");
            let xrp_intent = AutomationIntentId::from_seed("shared-risk:xrp");
            for (session_id, observation, intent_id) in [
                (&first_session, &btc_observation, &btc_intent),
                (&second_session, &eth_observation, &eth_intent),
                (&third_session, &xrp_observation, &xrp_intent),
            ] {
                store.automation_record_decision(
                    session_id,
                    observation,
                    r#"{"cursor":1}"#,
                    intent_id,
                    "BUY",
                    &serde_json::json!({
                        "decision_time_utc":"2026-10-14T01:00:00Z",
                        "constrained_target_weight":0.001
                    }),
                    snapshot_at,
                )?;
            }
            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                snapshot_at,
                snapshot_at,
                &balances,
            )?;

            let rules = |market: &str| VerifiedMarketRules {
                market: market.into(),
                source_mode: AutomationSessionMode::Mock,
                observed_at_utc: snapshot_at,
                state: "active".into(),
                bid_types: vec!["price".into()],
                ask_types: vec!["market".into()],
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                bid_min_total: Decimal::from(5_000),
                ask_min_total: Decimal::from(5_000),
                max_total: Decimal::from(1_000_000_000),
                bid_price_unit: None,
                ask_price_unit: None,
            };
            let first_notional = QuoteAmount::new(Decimal::from(6_000_000))?;
            let first_fee = QuoteAmount::new(Decimal::from(3_000))?;
            let first_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&btc_intent),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: first_notional,
                },
            };
            store.automation_admit_order(
                AutomationOrderAdmission {
                    session_id: first_session.clone(),
                    intent_id: btc_intent.clone(),
                    request: first_request,
                    decision_mark: crate::contracts::PriceKrw::new(Decimal::from(10_000))?,
                    decision_time: btc_observation.candle.close_time_utc,
                    requested_notional: first_notional,
                    quote_reserve: QuoteAmount::new(Decimal::from(6_003_000))?,
                    asset_currency: "BTC".into(),
                    asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                    fee_reserve: first_fee,
                    rules: rules("KRW-BTC"),
                    evaluation_json: serde_json::json!({
                        "decision_time_utc":"2026-10-14T01:00:00Z",
                        "constrained_target_weight":0.001
                    }),
                },
                snapshot_at,
            )?;

            // The first reservation leaves <5,002,500 KRW free, even though each
            // session individually sees the same 10,000,000 KRW account snapshot.
            let second_notional = QuoteAmount::new(Decimal::from(5_000_000))?;
            let second_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&eth_intent),
                market: MarketId::parse_upbit("KRW-ETH")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: second_notional,
                },
            };
            assert!(matches!(
                store.automation_admit_order(
                    AutomationOrderAdmission {
                        session_id: second_session.clone(),
                        intent_id: eth_intent.clone(),
                        request: second_request,
                        decision_mark: crate::contracts::PriceKrw::new(Decimal::from(2_000))?,
                        decision_time: eth_observation.candle.close_time_utc,
                        requested_notional: second_notional,
                        quote_reserve: QuoteAmount::new(Decimal::from(5_002_500))?,
                        asset_currency: "ETH".into(),
                        asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                        fee_reserve: QuoteAmount::new(Decimal::from(2_500))?,
                        rules: rules("KRW-ETH"),
                        evaluation_json: serde_json::json!({
                            "decision_time_utc":"2026-10-14T01:00:00Z",
                            "constrained_target_weight":0.001
                        }),
                    },
                    snapshot_at,
                ),
                Err(LabError::Conflict(message)) if message.contains("insufficient unreserved KRW")
            ));
            store.automation_reject_intent(
                &eth_intent,
                "TEST_SHARED_CASH_RESERVATION",
                snapshot_at,
            )?;

            let xrp_notional = QuoteAmount::new(Decimal::from(5_000))?;
            let xrp_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&xrp_intent),
                market: MarketId::parse_upbit("KRW-XRP")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: xrp_notional,
                },
            };
            assert!(matches!(
                store.automation_admit_order(
                    AutomationOrderAdmission {
                        session_id: third_session,
                        intent_id: xrp_intent,
                        request: xrp_request,
                        decision_mark: crate::contracts::PriceKrw::new(Decimal::from(100))?,
                        decision_time: xrp_observation.candle.close_time_utc,
                        requested_notional: xrp_notional,
                        quote_reserve: QuoteAmount::new(Decimal::new(50_025, 1))?,
                        asset_currency: "XRP".into(),
                        asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                        fee_reserve: QuoteAmount::new(Decimal::new(25, 1))?,
                        rules: rules("KRW-XRP"),
                        evaluation_json: serde_json::json!({
                            "decision_time_utc":"2026-10-14T01:00:00Z",
                            "constrained_target_weight":0.001
                        }),
                    },
                    snapshot_at,
                ),
                Err(LabError::Conflict(message)) if message.contains("interval limit")
            ));
            Ok(())
        })();
        let cleanup = fs::remove_dir_all(&root);
        assert!(
            cleanup.is_ok(),
            "isolated automation test root should be removed"
        );
        assert!(
            result.is_ok(),
            "shared account risk admission test failed: {result:?}"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)] // One complete buy/partial-fill/sell/restart-risk journey proves unique ledger settlement.
    fn partial_fills_replay_once_and_a_fill_wins_an_uncertain_cancel_race() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-order-reconcile-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let account_identity = "mock:upbit-default";
            let session_id = crate::contracts::AutomationSessionId::new("order-session")?;
            let intent_id = AutomationIntentId::from_seed("order-session:limit-buy");
            let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
            let mark = crate::contracts::PriceKrw::new(Decimal::from(105))?;
            let rules = VerifiedMarketRules {
                market: "KRW-BTC".into(),
                source_mode: AutomationSessionMode::Mock,
                observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
                state: "active".into(),
                bid_types: vec!["price".into(), "limit".into()],
                ask_types: vec!["market".into(), "limit".into()],
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                bid_min_total: Decimal::from(5_000),
                ask_min_total: Decimal::from(5_000),
                max_total: Decimal::from(1_000_000_000),
                bid_price_unit: Some(Decimal::ONE),
                ask_price_unit: Some(Decimal::ONE),
            };
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(12_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(1_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 3,
                max_orders_per_interval: 3,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let policy_ref_json = serde_json::to_string(&PolicyRevisionRef {
                policy_id: crate::contracts::PolicyId::new("test-policy")?,
                revision_id: crate::contracts::PolicyRevisionId::new("test-revision")?,
                definition_digest: ContentHash::of_bytes(b"test-policy-definition"),
            })
            .map_err(json_error)?;
            let start = UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_sessions(\
                         session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                         frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                     ) VALUES (?1,'order-start',?2,'MOCK','RUNNING',1,?3,'{}','KRW-BTC','h1',?4,'{}',?5,?5)",
                    params![session_id.as_str(),account_identity,policy_ref_json,serde_json::to_string(&risk).map_err(json_error)?,start.to_rfc3339()],
                )
                .map_err(sql_error)?;
            let observation =
                completed_test_observation("2026-10-14T00:00:00Z", "2026-10-14T01:00:00Z")?;
            let evaluation = serde_json::json!({
                "decision_time_utc":"2026-10-14T01:00:00Z",
                "constrained_target_weight":0.001
            });
            store.automation_record_decision(
                &session_id,
                &observation,
                r#"{"cursor":1}"#,
                &intent_id,
                "BUY",
                &evaluation,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:01Z")?,
            )?;
            store.automation_seed_mock_venue(start)?;
            let initial_balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:01Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:01Z")?,
                &initial_balances,
            )?;

            let request = AutomationOrderRequest {
                client_identifier: identifier.clone(),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::Limit {
                    volume: AssetQuantity::new(Decimal::from(100))?,
                    price: crate::contracts::PriceKrw::new(Decimal::from(90))?,
                },
            };
            let admission = AutomationOrderAdmission {
                session_id: session_id.clone(),
                intent_id: intent_id.clone(),
                request: request.clone(),
                decision_mark: mark,
                decision_time: observation.candle.close_time_utc,
                requested_notional: QuoteAmount::new(Decimal::from(9_000))?,
                quote_reserve: QuoteAmount::new(Decimal::new(90_045, 1))?,
                asset_currency: "BTC".into(),
                asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                fee_reserve: QuoteAmount::new(Decimal::new(45, 1))?,
                rules: rules.clone(),
                evaluation_json: evaluation,
            };
            let admitted = store.automation_admit_order(
                admission,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
            )?;
            assert_eq!(admitted.state, AutomationOrderState::Reserved);
            let dispatch = store.automation_claim_order_for_send(
                &identifier,
                &rules,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
            )?;
            assert_eq!(dispatch.request.client_identifier, identifier);
            assert!(matches!(
                store.automation_reject_reserved_order(
                    &identifier,
                    "MUST_NOT_RELEASE_AFTER_SEND_CLAIM",
                    UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
                ),
                Err(LabError::Conflict(_))
            ));
            let open_detail = store.automation_mock_submit_order(
                &request,
                mark,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:02Z")?,
            )?;
            assert_eq!(open_detail.state, "wait");

            let mut partial_detail = open_detail;
            partial_detail.executed_volume = Some(Decimal::from(40));
            partial_detail.remaining_volume = Some(Decimal::from(60));
            partial_detail.paid_fee = Some(Decimal::new(18, 1));
            partial_detail.remaining_fee = Some(Decimal::new(27, 1));
            partial_detail.locked = Some(Decimal::new(54_027, 1));
            partial_detail.trades_count = Some(1);
            partial_detail.trades = vec![serde_json::json!({
                "market":"KRW-BTC","uuid":"trade-partial-1","price":"90",
                "volume":"40","funds":"3600","trend":"even",
                "created_at":"2026-10-14T01:00:03Z","side":"bid"
            })];
            replace_mock_order_and_balances(
                &mut store,
                &identifier,
                &partial_detail,
                MockVenueBalances {
                    krw_free: Decimal::new(99_909_955, 1),
                    krw_locked: Decimal::new(54_027, 1),
                    btc_free: Decimal::from(40),
                    btc_locked: Decimal::ZERO,
                    btc_avg_buy_price: Decimal::from(90),
                    observed_at: UtcTimestamp::parse_rfc3339("2026-10-14T01:00:03Z")?,
                },
            )?;
            let partial_detail = store
                .automation_mock_order_by_identifier(&identifier)?
                .expect("partial order is durably readable by identifier");
            let partial = store.automation_reconcile_order(
                &partial_detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:03Z")?,
            )?;
            assert_eq!(partial.state, AutomationOrderState::PartiallyFilled);
            assert_eq!(partial.trade_count, 1);
            assert_eq!(partial.paid_fee_krw.get(), Decimal::new(18, 1));
            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:04Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:04Z")?,
                &balances,
            )?;

            let cancel_write = AutomationSessionWrite::CancelOrder {
                request_id: RequestId::new("cancel-race-request")?,
                session_id: session_id.clone(),
                expected_sequence: 1,
                client_identifier: identifier.clone(),
            };
            store.automation_begin_request(
                &cancel_write,
                None,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:05Z")?,
            )?;
            let cancel = store.automation_prepare_cancel(
                &cancel_write,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:05Z")?,
            )?;
            assert!(cancel.should_send);
            assert_eq!(
                cancel.response.receipt.outcome,
                AutomationCommandOutcome::Pending
            );
            let replayed_cancel = store.automation_prepare_cancel(
                &cancel_write,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:05Z")?,
            )?;
            assert!(!replayed_cancel.should_send);

            let mut filled_detail = partial_detail.clone();
            filled_detail.state = "done".into();
            filled_detail.executed_volume = Some(Decimal::from(100));
            filled_detail.remaining_volume = Some(Decimal::ZERO);
            filled_detail.paid_fee = Some(Decimal::new(45, 1));
            filled_detail.remaining_fee = Some(Decimal::ZERO);
            filled_detail.locked = Some(Decimal::ZERO);
            filled_detail.trades_count = Some(2);
            filled_detail.trades.push(serde_json::json!({
                "market":"KRW-BTC","uuid":"trade-partial-2","price":"90",
                "volume":"60","funds":"5400","trend":"even",
                "created_at":"2026-10-14T01:00:06Z","side":"bid"
            }));
            replace_mock_order_and_balances(
                &mut store,
                &identifier,
                &filled_detail,
                MockVenueBalances {
                    krw_free: Decimal::new(99_909_955, 1),
                    krw_locked: Decimal::ZERO,
                    btc_free: Decimal::from(100),
                    btc_locked: Decimal::ZERO,
                    btc_avg_buy_price: Decimal::from(90),
                    observed_at: UtcTimestamp::parse_rfc3339("2026-10-14T01:00:06Z")?,
                },
            )?;
            let filled_detail = store
                .automation_mock_order_by_identifier(&identifier)?
                .expect("filled order is durably readable by identifier");
            let request_id = RequestId::new("cancel-race-request")?;
            let cancelled = store.automation_finalize_cancel(
                &request_id,
                &filled_detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:06Z")?,
            )?;
            assert_eq!(cancelled.receipt.outcome, AutomationCommandOutcome::NoOp);
            assert_eq!(
                cancelled.order.as_ref().expect("order result").state,
                AutomationOrderState::Filled
            );
            assert_eq!(
                store.automation_readback_request(&request_id)?.state,
                crate::contracts::AutomationRequestReadbackState::Applied
            );

            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:07Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:07Z")?,
                &balances,
            )?;
            let paused_after_cancel = store.automation_get_session(
                &session_id,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:08Z")?,
            )?;
            assert_eq!(paused_after_cancel.state, AutomationSessionState::Paused);
            store.automation_write(
                &AutomationSessionWrite::Resume {
                    request_id: RequestId::new("resume-after-filled-cancel-race")?,
                    session_id: session_id.clone(),
                    expected_sequence: paused_after_cancel.sequence,
                },
                None,
                None,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:08Z")?,
            )?;
            let before_replay_events: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_session_events WHERE session_id=?1 AND kind='ORDER_RECONCILED'",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            store.automation_reconcile_order(
                &filled_detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:08Z")?,
            )?;
            let after_replay_events: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_session_events WHERE session_id=?1 AND kind='ORDER_RECONCILED'",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(after_replay_events, before_replay_events);

            let fill_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_fills WHERE client_identifier=?1",
                    [identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let fee_rows: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_fee_accruals WHERE client_identifier=?1",
                    [identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let cumulative_fee: String = store
                .connection
                .query_row(
                    "SELECT cumulative_paid_fee_krw FROM automation_fee_accruals \
                     WHERE client_identifier=?1 ORDER BY sequence DESC LIMIT 1",
                    [identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let reservation_state: String = store
                .connection
                .query_row(
                    "SELECT state FROM automation_risk_reservations WHERE client_identifier=?1",
                    [identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(fill_count, 2);
            assert_eq!(fee_rows, 2);
            assert_eq!(parse_decimal(&cumulative_fee)?, Decimal::new(45, 1));
            assert_eq!(reservation_state, "RELEASED");

            let mut conflicting_detail = filled_detail.clone();
            conflicting_detail.trades[0]["price"] = serde_json::json!("89");
            conflicting_detail.trades[0]["funds"] = serde_json::json!("3560");
            assert!(matches!(
                store.automation_reconcile_order(
                    &conflicting_detail,
                    UtcTimestamp::parse_rfc3339("2026-10-14T01:00:09Z")?
                ),
                Err(LabError::DataCorrupt(_))
            ));

            let stale_sell_observation =
                completed_test_observation("2026-10-14T01:00:00Z", "2026-10-14T02:00:00Z")?;
            let mut sell_rules = rules.clone();
            sell_rules.observed_at_utc = UtcTimestamp::parse_rfc3339("2026-10-14T02:00:01Z")?;
            let stale_sell = reserve_test_market_sell(
                &mut store,
                &session_id,
                &stale_sell_observation,
                &sell_rules,
                "fee-rule-change",
            )?;
            let stale_rules = VerifiedMarketRules {
                observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-14T01:57:59Z")?,
                ..sell_rules.clone()
            };
            assert!(matches!(
                store.automation_claim_order_for_send(
                    &stale_sell,
                    &stale_rules,
                    UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                ),
                Err(LabError::UnverifiedMarketRules(_))
            ));
            store
                .connection
                .execute(
                    "UPDATE automation_account_snapshots SET observed_at_utc='2026-10-14T01:59:00Z' \
                     WHERE snapshot_sequence=(SELECT MAX(snapshot_sequence) FROM automation_account_snapshots \
                         WHERE account_identity='mock:upbit-default')",
                    [],
                )
                .map_err(sql_error)?;
            assert!(matches!(
                store.automation_claim_order_for_send(
                    &stale_sell,
                    &sell_rules,
                    UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                ),
                Err(LabError::BlockedEvidence(_))
            ));
            let increased_fee_rules = VerifiedMarketRules {
                observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                ask_fee: Decimal::new(1, 3),
                ..sell_rules
            };
            assert!(matches!(
                store.automation_claim_order_for_send(
                    &stale_sell,
                    &increased_fee_rules,
                    UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                ),
                Err(LabError::UnverifiedMarketRules(_))
            ));
            assert_eq!(
                store
                    .automation_reject_reserved_order(
                        &stale_sell,
                        "CURRENT_FEE_EXCEEDS_RESERVED_FEE",
                        UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                    )?
                    .state,
                AutomationOrderState::Rejected
            );

            let sell_observation =
                completed_test_observation("2026-10-14T02:00:00Z", "2026-10-14T03:00:00Z")?;
            let mut current_sell_rules = rules.clone();
            current_sell_rules.observed_at_utc =
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:01Z")?;
            let sell_identifier = reserve_test_market_sell(
                &mut store,
                &session_id,
                &sell_observation,
                &current_sell_rules,
                "filled-sell",
            )?;
            let sell_dispatch = store.automation_claim_order_for_send(
                &sell_identifier,
                &current_sell_rules,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:02Z")?,
            )?;
            let sell_detail = store.automation_mock_submit_order(
                &sell_dispatch.request,
                sell_dispatch.decision_mark,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:02Z")?,
            )?;
            assert_eq!(sell_detail.state, "done");
            assert_eq!(sell_detail.reserved_fee, Some(Decimal::ZERO));
            assert_eq!(sell_detail.remaining_fee, Some(Decimal::ZERO));
            assert_eq!(sell_detail.paid_fee, Some(Decimal::new(315, 2)));
            let sell_view = store.automation_reconcile_order(
                &sell_detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:02Z")?,
            )?;
            assert_eq!(sell_view.state, AutomationOrderState::Filled);
            assert_eq!(sell_view.executed_volume.get(), Decimal::from(60));
            assert_eq!(sell_view.paid_fee_krw.get(), Decimal::new(315, 2));
            assert_eq!(sell_view.locked_base.unwrap().get(), Decimal::ZERO);
            let replayed_sell = store.automation_reconcile_order(
                &sell_detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:03Z")?,
            )?;
            assert_eq!(replayed_sell.trade_count, 1);
            let mut statement = store
                .connection
                .prepare(
                    "SELECT delta_paid_fee_krw FROM automation_fee_accruals \
                     WHERE client_identifier=?1 ORDER BY sequence",
                )
                .map_err(sql_error)?;
            let sell_fee_deltas = statement
                .query_map([sell_identifier.as_str()], |row| row.get::<_, String>(0))
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?;
            drop(statement);
            assert_eq!(sum_decimal_strings(&sell_fee_deltas)?, Decimal::new(315, 2));
            let sell_fill_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_fills WHERE client_identifier=?1",
                    [sell_identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(sell_fill_count, 1);
            let sell_ledger = store.automation_ledger(&session_id, None, 100)?;
            let sell_fill = sell_ledger
                .fills
                .iter()
                .find(|fill| fill.client_identifier == sell_identifier)
                .expect("the sold base quantity has one unique durable fill");
            assert_eq!(sell_fill.side, crate::contracts::AutomationSide::Ask);
            assert_eq!(sell_fill.price_krw.get(), Decimal::from(105));
            assert_eq!(sell_fill.volume.get(), Decimal::from(60));
            assert_eq!(sell_fill.funds_krw.get(), Decimal::from(6_300));
            let balances = store.automation_mock_venue_accounts()?;
            let quote_balance = balances
                .iter()
                .find(|balance| balance.currency == "KRW")
                .expect("mock quote balance");
            let base_balance = balances
                .iter()
                .find(|balance| balance.currency == "BTC")
                .expect("mock base balance");
            assert_eq!(quote_balance.balance, Decimal::new(999_729_235, 2));
            assert_eq!(base_balance.balance, Decimal::from(40));
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:03Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:03Z")?,
                &balances,
            )?;

            let session_view = store.automation_get_session(
                &session_id,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:04Z")?,
            )?;
            store.automation_write(
                &AutomationSessionWrite::Stop {
                    request_id: RequestId::new("stop-first-position-session")?,
                    session_id: session_id.clone(),
                    expected_sequence: session_view.sequence,
                },
                None,
                None,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:04Z")?,
            )?;

            let exposure_session = seed_running_automation_session(
                &mut store,
                "exposure-after-fill-session",
                "KRW-BTC",
                account_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:05Z")?,
            )?;
            let exposure_observation =
                completed_test_observation("2026-10-14T03:00:00Z", "2026-10-14T04:00:00Z")?;
            let exposure_intent = AutomationIntentId::from_seed("exposure-after-fill");
            let exposure_evaluation = serde_json::json!({
                "decision_time_utc":"2026-10-14T04:00:00Z",
                "constrained_target_weight":0.002
            });
            store.automation_record_decision(
                &exposure_session,
                &exposure_observation,
                r#"{"cursor":2}"#,
                &exposure_intent,
                "BUY",
                &exposure_evaluation,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:01Z")?,
            )?;
            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:01Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:01Z")?,
                &balances,
            )?;
            let exposure_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&exposure_intent),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::Limit {
                    volume: AssetQuantity::new(Decimal::from(100))?,
                    price: crate::contracts::PriceKrw::new(Decimal::from(90))?,
                },
            };
            let exposure_rules = VerifiedMarketRules {
                observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-14T04:00:02Z")?,
                ..rules.clone()
            };
            assert!(matches!(
                store.automation_admit_order(
                    AutomationOrderAdmission {
                        session_id: exposure_session.clone(),
                        intent_id: exposure_intent.clone(),
                        request: exposure_request,
                        decision_mark: crate::contracts::PriceKrw::new(Decimal::from(105))?,
                        decision_time: exposure_observation.candle.close_time_utc,
                        requested_notional: QuoteAmount::new(Decimal::from(9_000))?,
                        quote_reserve: QuoteAmount::new(Decimal::new(90_045, 1))?,
                        asset_currency: "BTC".into(),
                        asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                        fee_reserve: QuoteAmount::new(Decimal::new(45, 1))?,
                        rules: exposure_rules,
                        evaluation_json: exposure_evaluation,
                    },
                    UtcTimestamp::parse_rfc3339("2026-10-14T04:00:02Z")?,
                ),
                Err(LabError::Conflict(message)) if message.contains("asset-exposure limit")
            ));
            store.automation_reject_intent(
                &exposure_intent,
                "TEST_SHARED_ASSET_EXPOSURE",
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:02Z")?,
            )?;
            let exposure_view = store.automation_get_session(
                &exposure_session,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:03Z")?,
            )?;
            store.automation_write(
                &AutomationSessionWrite::Stop {
                    request_id: RequestId::new("stop-exposure-session")?,
                    session_id: exposure_session,
                    expected_sequence: exposure_view.sequence,
                },
                None,
                None,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:03Z")?,
            )?;

            let loss_session = seed_running_automation_session(
                &mut store,
                "loss-after-fill-session",
                "KRW-BTC",
                account_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T04:00:04Z")?,
            )?;
            let loss_observation = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T04:00:00Z",
                "2026-10-14T05:00:00Z",
                Decimal::from(10),
            )?;
            let loss_intent = AutomationIntentId::from_seed("loss-after-fill");
            let loss_evaluation = serde_json::json!({
                "decision_time_utc":"2026-10-14T05:00:00Z",
                "constrained_target_weight":0.002
            });
            store.automation_record_decision(
                &loss_session,
                &loss_observation,
                r#"{"cursor":3}"#,
                &loss_intent,
                "BUY",
                &loss_evaluation,
                UtcTimestamp::parse_rfc3339("2026-10-14T05:00:01Z")?,
            )?;
            let balances = store.automation_mock_venue_accounts()?;
            store.automation_record_account_snapshot(
                account_identity,
                AutomationSessionMode::Mock,
                UtcTimestamp::parse_rfc3339("2026-10-14T05:00:01Z")?,
                UtcTimestamp::parse_rfc3339("2026-10-14T05:00:01Z")?,
                &balances,
            )?;
            let loss_request = AutomationOrderRequest {
                client_identifier: AutomationOrderIdentifier::from_intent(&loss_intent),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side: crate::contracts::AutomationSide::Bid,
                order: AutomationOrderKind::MarketBuy {
                    budget: QuoteAmount::new(Decimal::from(5_000))?,
                },
            };
            let loss_rules = VerifiedMarketRules {
                observed_at_utc: UtcTimestamp::parse_rfc3339("2026-10-14T05:00:02Z")?,
                ..rules.clone()
            };
            assert!(matches!(
                store.automation_admit_order(
                    AutomationOrderAdmission {
                        session_id: loss_session,
                        intent_id: loss_intent,
                        request: loss_request,
                        decision_mark: crate::contracts::PriceKrw::new(Decimal::from(10))?,
                        decision_time: loss_observation.candle.close_time_utc,
                        requested_notional: QuoteAmount::new(Decimal::from(5_000))?,
                        quote_reserve: QuoteAmount::new(Decimal::new(50_025, 1))?,
                        asset_currency: "BTC".into(),
                        asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
                        fee_reserve: QuoteAmount::new(Decimal::new(25, 1))?,
                        rules: loss_rules,
                        evaluation_json: loss_evaluation,
                    },
                    UtcTimestamp::parse_rfc3339("2026-10-14T05:00:02Z")?,
                ),
                Err(LabError::Conflict(message)) if message.contains("daily-loss limit")
            ));
            Ok(())
        })();
        let cleanup = fs::remove_dir_all(&root);
        assert!(
            cleanup.is_ok(),
            "isolated automation test root should be removed"
        );
        assert!(
            result.is_ok(),
            "order reconciliation test failed: {result:?}"
        );
    }

    #[derive(Clone, Copy)]
    struct MockVenueBalances {
        krw_free: Decimal,
        krw_locked: Decimal,
        btc_free: Decimal,
        btc_locked: Decimal,
        btc_avg_buy_price: Decimal,
        observed_at: UtcTimestamp,
    }

    fn replace_mock_order_and_balances(
        store: &mut Store,
        identifier: &AutomationOrderIdentifier,
        detail: &UpbitOrderDetail,
        balances: MockVenueBalances,
    ) -> Result<(), LabError> {
        let transaction = store.connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE automation_mock_venue_orders SET detail_json=?1,updated_at_utc=?2 \
                 WHERE client_identifier=?3",
                params![
                    serde_json::to_string(detail).map_err(json_error)?,
                    balances.observed_at.to_rfc3339(),
                    identifier.as_str()
                ],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "DELETE FROM automation_mock_venue_trades WHERE client_identifier=?1",
                [identifier.as_str()],
            )
            .map_err(sql_error)?;
        for trade in &detail.trades {
            let trade_uuid = trade
                .get("uuid")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| LabError::DataCorrupt("test mock trade has no UUID".into()))?;
            transaction
                .execute(
                    "INSERT INTO automation_mock_venue_trades(trade_uuid,client_identifier,trade_json,created_at_utc) \
                     VALUES (?1,?2,?3,?4)",
                    params![trade_uuid,identifier.as_str(),serde_json::to_string(trade).map_err(json_error)?,balances.observed_at.to_rfc3339()],
                )
                .map_err(sql_error)?;
        }
        let mut krw = read_mock_account(&transaction, "KRW")?;
        krw.balance = balances.krw_free;
        krw.locked = balances.krw_locked;
        write_mock_account(&transaction, "KRW", &krw, balances.observed_at)?;
        let mut btc = read_mock_account(&transaction, "BTC")?;
        btc.balance = balances.btc_free;
        btc.locked = balances.btc_locked;
        btc.avg_buy_price = balances.btc_avg_buy_price;
        btc.avg_buy_price_modified = false;
        write_mock_account(&transaction, "BTC", &btc, balances.observed_at)?;
        transaction.commit().map_err(sql_error)
    }

    pub(super) fn completed_test_observation(
        open_at: &str,
        close_at: &str,
    ) -> Result<CandleObservation, LabError> {
        completed_market_observation("KRW-BTC", open_at, close_at, Decimal::from(105))
    }

    fn reserve_test_market_sell(
        store: &mut Store,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        rules: &VerifiedMarketRules,
        suffix: &str,
    ) -> Result<AutomationOrderIdentifier, LabError> {
        let now = UtcTimestamp(observation.candle.close_time_utc.0 + chrono::Duration::seconds(1));
        let intent_id = AutomationIntentId::from_seed(&format!("{}:{suffix}", session_id.as_str()));
        let evaluation = serde_json::json!({
            "decision_time_utc": observation.candle.close_time_utc,
            "constrained_target_weight": 0.0,
            "test_case": suffix,
        });
        store.automation_record_decision(
            session_id,
            observation,
            &format!("{{\"cursor\":\"{suffix}\"}}"),
            &intent_id,
            "SELL",
            &evaluation,
            now,
        )?;
        let balances = store.automation_mock_venue_accounts()?;
        store.automation_record_account_snapshot(
            "mock:upbit-default",
            AutomationSessionMode::Mock,
            now,
            now,
            &balances,
        )?;
        let volume = AssetQuantity::new(Decimal::from(60))?;
        let mark = observation.candle.close;
        let notional = mark
            .get()
            .checked_mul(volume.get())
            .ok_or_else(|| LabError::AccountingInvariant("test sell notional overflow".into()))?;
        let fee = notional
            .checked_mul(rules.ask_fee)
            .ok_or_else(|| LabError::AccountingInvariant("test sell fee overflow".into()))?;
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit(&observation.candle.market)?,
            side: crate::contracts::AutomationSide::Ask,
            order: AutomationOrderKind::MarketSell { volume },
        };
        store.automation_admit_order(
            AutomationOrderAdmission {
                session_id: session_id.clone(),
                intent_id,
                request,
                decision_mark: mark,
                decision_time: observation.candle.close_time_utc,
                requested_notional: QuoteAmount::new(notional)?,
                quote_reserve: QuoteAmount::new(Decimal::ZERO)?,
                asset_currency: "BTC".into(),
                asset_reserve: volume,
                fee_reserve: QuoteAmount::new(fee)?,
                rules: rules.clone(),
                evaluation_json: evaluation,
            },
            now,
        )?;
        Ok(identifier)
    }

    fn completed_market_observation(
        market: &str,
        open_at: &str,
        close_at: &str,
        close_price: Decimal,
    ) -> Result<CandleObservation, LabError> {
        let open_time_utc = UtcTimestamp::parse_rfc3339(open_at)?;
        let close_time_utc = UtcTimestamp::parse_rfc3339(close_at)?;
        let close = crate::contracts::PriceKrw::new(close_price)?;
        let candle = crate::contracts::CandleRecord {
            market: market.into(),
            interval: crate::contracts::CandleInterval::H1,
            open_time_utc,
            close_time_utc,
            open: close,
            high: crate::contracts::PriceKrw::new(close_price + Decimal::ONE)?,
            low: crate::contracts::PriceKrw::new(close_price - Decimal::ONE)?,
            close,
            volume: AssetQuantity::new(Decimal::ONE)?,
            quote_turnover: QuoteAmount::new(close_price)?,
            completed: true,
        };
        let content_digest = observation_digest(&candle)?;
        Ok(CandleObservation {
            id: ObservationId::from_seed(content_digest.as_str()),
            candle,
            content_digest,
            raw_object_ids: Vec::new(),
            constituent_ids: Vec::new(),
        })
    }

    #[test]
    fn missing_finalized_candle_gap_is_durable_and_does_not_advance_the_cursor() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-candle-gap-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(500_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(50_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 3,
                max_orders_per_interval: 2,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let session_id = seed_running_automation_session(
                &mut store,
                "candle-gap-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T00:30:00Z")?,
            )?;
            let first = completed_test_observation("2026-10-14T00:00:00Z", "2026-10-14T01:00:00Z")?;
            assert!(store.automation_record_candle(
                &session_id,
                &first,
                r#"{"cursor":1}"#,
                UtcTimestamp::parse_rfc3339("2026-10-14T01:00:01Z")?,
            )?);
            let skipped =
                completed_test_observation("2026-10-14T02:00:00Z", "2026-10-14T03:00:00Z")?;
            assert!(!store.automation_record_candle(
                &session_id,
                &skipped,
                r#"{"cursor":3}"#,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:01Z")?,
            )?);
            let view = store.automation_get_session(
                &session_id,
                UtcTimestamp::parse_rfc3339("2026-10-14T03:00:01Z")?,
            )?;
            assert_eq!(view.state, AutomationSessionState::NeedsReconciliation);
            assert_eq!(
                view.recovery_reason.as_deref(),
                Some("MISSING_FINALIZED_CANDLE_GAP")
            );
            assert_eq!(
                view.last_finalized_candle_at,
                Some(UtcTimestamp::parse_rfc3339("2026-10-14T01:00:00Z")?)
            );
            let candle_count: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM automation_candles WHERE session_id=?1",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(candle_count, 1);
            assert!(matches!(
                store.automation_record_decision(
                    &session_id,
                    &skipped,
                    r#"{"cursor":3}"#,
                    &AutomationIntentId::from_seed("gap-session:order"),
                    "BUY",
                    &serde_json::json!({"decision_time_utc":"2026-10-14T03:00:00Z"}),
                    UtcTimestamp::parse_rfc3339("2026-10-14T03:00:02Z")?,
                ),
                Err(LabError::Conflict(_))
            ));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result.expect("a finalized-candle gap must block the durable decision cursor");
    }

    #[test]
    fn stopped_live_account_identity_rejects_credential_rotation() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-live-identity-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(500_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(50_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 3,
                max_orders_per_interval: 2,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let previous_identity = format!("live:upbit:{}", "a".repeat(64));
            let rotated_identity = format!("live:upbit:{}", "b".repeat(64));
            let session_id = seed_running_automation_session(
                &mut store,
                "stopped-live-identity",
                "KRW-BTC",
                &previous_identity,
                &risk,
                UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?,
            )?;
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET mode='LIVE',state='STOPPED' WHERE session_id=?1",
                    [session_id.as_str()],
                )
                .map_err(sql_error)?;

            let transaction = store.connection.transaction().map_err(sql_error)?;
            ensure_live_account_identity_history(&transaction, &previous_identity)?;
            assert!(matches!(
                ensure_live_account_identity_history(&transaction, &rotated_identity),
                Err(LabError::BlockedEvidence(_))
            ));
            drop(transaction);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result.expect("rotation of a stopped live account must remain fail-closed");
    }

    #[test]
    #[allow(clippy::too_many_lines)] // One pause/resume/stop journey checks send-claim ordering and exact reservation release.
    fn pause_and_stop_release_only_orders_whose_send_claim_never_committed() {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-automation-reserved-lifecycle-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(500_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(50_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 3,
                max_orders_per_interval: 2,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T01:00:00Z")?;
            let session_id = seed_running_automation_session(
                &mut store,
                "reserved-lifecycle-session",
                "KRW-BTC",
                "mock:upbit-default",
                &risk,
                now,
            )?;
            let paused_identifier =
                seed_reserved_test_order(&mut store, &session_id, "pause", now)?;
            let paused = store.automation_write(
                &AutomationSessionWrite::Pause {
                    request_id: RequestId::new("pause-reserved-order")?,
                    session_id: session_id.clone(),
                    expected_sequence: 1,
                },
                None,
                None,
                now,
            )?;
            assert_eq!(paused.session.state, AutomationSessionState::Paused);
            assert_eq!(paused.session.unresolved_order_count, 0);
            let paused_order = store
                .automation_order_readback(&paused_identifier)?
                .expect("paused pre-send order remains readable");
            assert_eq!(paused_order.state, AutomationOrderState::Rejected);
            assert_eq!(
                store
                    .automation_reject_reserved_order(
                        &paused_identifier,
                        "SESSION_PAUSED_BEFORE_SEND",
                        now,
                    )?
                    .state,
                AutomationOrderState::Rejected
            );
            let reservation_state: String = store
                .connection
                .query_row(
                    "SELECT state FROM automation_risk_reservations WHERE client_identifier=?1",
                    [paused_identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(reservation_state, "RELEASED");

            let fresh_rules = VerifiedMarketRules {
                market: "KRW-BTC".into(),
                source_mode: AutomationSessionMode::Mock,
                observed_at_utc: now,
                state: "active".into(),
                bid_types: vec!["price".into(), "limit".into()],
                ask_types: vec!["market".into(), "limit".into()],
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                bid_min_total: Decimal::from(5_000),
                ask_min_total: Decimal::from(5_000),
                max_total: Decimal::from(1_000_000_000),
                bid_price_unit: Some(Decimal::ONE),
                ask_price_unit: Some(Decimal::ONE),
            };
            assert!(
                store
                    .automation_claim_order_for_send(&paused_identifier, &fresh_rules, now)
                    .is_err()
            );

            let resumed = store.automation_write(
                &AutomationSessionWrite::Resume {
                    request_id: RequestId::new("resume-after-pre-send-release")?,
                    session_id: session_id.clone(),
                    expected_sequence: 2,
                },
                None,
                None,
                now,
            )?;
            assert_eq!(resumed.session.state, AutomationSessionState::Running);
            let denied_at = UtcTimestamp::parse_rfc3339("2026-10-14T02:00:00Z")?;
            let denied_identifier =
                seed_reserved_test_order(&mut store, &session_id, "fresh-rule-denial", denied_at)?;
            assert_eq!(
                store
                    .automation_reject_reserved_order(
                        &denied_identifier,
                        "CURRENT_RULES_REJECTED_BEFORE_SEND",
                        denied_at,
                    )?
                    .state,
                AutomationOrderState::Rejected
            );
            let stopped_at = UtcTimestamp::parse_rfc3339("2026-10-14T03:00:00Z")?;
            let stopped_identifier =
                seed_reserved_test_order(&mut store, &session_id, "stop", stopped_at)?;
            let stopped = store.automation_write(
                &AutomationSessionWrite::Stop {
                    request_id: RequestId::new("stop-reserved-order")?,
                    session_id: session_id.clone(),
                    expected_sequence: 3,
                },
                None,
                None,
                stopped_at,
            )?;
            assert_eq!(stopped.session.state, AutomationSessionState::Stopped);
            assert_eq!(stopped.session.unresolved_order_count, 0);
            assert_eq!(
                store
                    .automation_order_readback(&stopped_identifier)?
                    .expect("stopped pre-send order remains readable")
                    .state,
                AutomationOrderState::Rejected
            );
            assert!(
                store
                    .automation_claim_order_for_send(&stopped_identifier, &fresh_rules, stopped_at)
                    .is_err()
            );
            let stopped_reservation: String = store
                .connection
                .query_row(
                    "SELECT state FROM automation_risk_reservations WHERE client_identifier=?1",
                    [stopped_identifier.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            assert_eq!(stopped_reservation, "RELEASED");
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result.expect("Pause/Stop must release only unclaimed reservations atomically");
    }

    pub(super) fn seed_reserved_test_order(
        store: &mut Store,
        session_id: &crate::contracts::AutomationSessionId,
        suffix: &str,
        now: UtcTimestamp,
    ) -> Result<AutomationOrderIdentifier, LabError> {
        let open_at = UtcTimestamp(now.0 - chrono::Duration::hours(1));
        let observation = completed_test_observation(&open_at.to_rfc3339(), &now.to_rfc3339())?;
        let observation_id = observation.id.clone();
        let digest = observation.content_digest.clone();
        store
            .connection
            .execute(
                "INSERT INTO automation_candles(\
                     session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json\
                 ) VALUES (?1,?2,?3,?4,?5,'{}')",
                params![
                    session_id.as_str(),
                    observation_id.as_str(),
                    now.to_rfc3339(),
                    digest.as_str(),
                    serde_json::to_string(&observation).map_err(json_error)?,
                ],
            )
            .map_err(sql_error)?;
        let intent_id = AutomationIntentId::from_seed(&format!("{}:{suffix}", session_id.as_str()));
        store
            .connection
            .execute(
                "INSERT INTO automation_intents(\
                     intent_id,session_id,observation_id,decision_time_utc,action,state,evaluation_json,created_at_utc\
                 ) VALUES (?1,?2,?3,?4,'BUY','ORDER_RESERVED','{}',?4)",
                params![
                    intent_id.as_str(),
                    session_id.as_str(),
                    observation_id.as_str(),
                    now.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            side: crate::contracts::AutomationSide::Bid,
            order: AutomationOrderKind::MarketBuy {
                budget: QuoteAmount::new(Decimal::from(9_000))?,
            },
        };
        store
            .connection
            .execute(
                "INSERT INTO automation_orders(\
                     client_identifier,intent_id,session_id,account_identity,order_request_json,\
                     rules_snapshot_json,request_digest,market,side,state,requested_notional_krw,\
                     decision_mark_krw,reserve_quote_krw,reserve_asset,reserved_asset_volume,\
                     reserved_fee_krw,remaining_fee_krw,executed_volume,remaining_volume,paid_fee_krw,\
                     locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc\
                 ) VALUES (?1,?2,?3,'mock:upbit-default',?4,'{}',?5,'KRW-BTC','BID','RESERVED',\
                           '9000','100','9004.5','BTC','0','4.5','4.5','0','0','0','0','KRW',0,?6,?6)",
                params![
                    identifier.as_str(),
                    intent_id.as_str(),
                    session_id.as_str(),
                    serde_json::to_string(&request).map_err(json_error)?,
                    ContentHash::of_value(&request)?.as_str(),
                    now.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "INSERT INTO automation_risk_reservations(\
                     client_identifier,account_identity,market,quote_reserved_krw,asset_reserved,\
                     asset_reserved_volume,order_notional_krw,order_day_utc,state,created_at_utc\
                 ) VALUES (?1,'mock:upbit-default','KRW-BTC','9004.5','BTC','0','9000',\
                           '2026-10-14','ACTIVE',?2)",
                params![identifier.as_str(), now.to_rfc3339()],
            )
            .map_err(sql_error)?;
        Ok(identifier)
    }

    pub(super) fn seed_running_automation_session(
        store: &mut Store,
        raw_session_id: &str,
        raw_market: &str,
        account_identity: &str,
        risk: &AutomationRiskPolicy,
        created_at: UtcTimestamp,
    ) -> Result<crate::contracts::AutomationSessionId, LabError> {
        let session_id = crate::contracts::AutomationSessionId::new(raw_session_id)?;
        let market = MarketId::parse_upbit(raw_market)?;
        let frozen_policy = test_frozen_policy();
        let policy_ref = frozen_policy.reference.clone();
        store
            .connection
            .execute(
                "INSERT INTO automation_sessions(\
                     session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                     frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                 ) VALUES (?1,?2,?3,'MOCK','RUNNING',1,?4,?5,?6,'h1',?7,'{}',?8,?8)",
                params![
                    session_id.as_str(),
                    format!("start-{raw_session_id}"),
                    account_identity,
                    serde_json::to_string(&policy_ref).map_err(json_error)?,
                    serde_json::to_string(&frozen_policy).map_err(json_error)?,
                    market.code(),
                    serde_json::to_string(risk).map_err(json_error)?,
                    created_at.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        Ok(session_id)
    }

    pub(super) fn baseline_test_risk_policy() -> Result<AutomationRiskPolicy, LabError> {
        Ok(AutomationRiskPolicy {
            max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(500_000))?,
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(50_000))?,
            activation_loss_limit_krw: None,
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 60,
            max_feed_age_seconds: 60,
        })
    }

    fn mock_baseline_assets(
        balances: &[UpbitAccountBalance],
    ) -> Result<Vec<crate::contracts::AutomationBaselineAssetInput>, LabError> {
        balances
            .iter()
            .map(|balance| {
                let total = balance.balance.checked_add(balance.locked).ok_or_else(|| {
                    LabError::DataCorrupt("test baseline balance overflow".into())
                })?;
                let valuation_basis = if balance.currency == "KRW" {
                    AutomationAssetValuationBasis::QuoteCurrencyParity
                } else if total == Decimal::ZERO {
                    AutomationAssetValuationBasis::ZeroBalance
                } else {
                    AutomationAssetValuationBasis::FinalizedCandleClose
                };
                Ok(crate::contracts::AutomationBaselineAssetInput {
                    currency: balance.currency.clone(),
                    free: AssetQuantity::new(balance.balance)?,
                    locked: AssetQuantity::new(balance.locked)?,
                    valuation_basis,
                    mark_price_krw: None,
                })
            })
            .collect()
    }

    fn seed_live_automation_session(
        store: &mut Store,
        raw_session_id: &str,
        account_identity: &str,
        risk: &AutomationRiskPolicy,
        created_at: UtcTimestamp,
    ) -> Result<crate::contracts::AutomationSessionId, LabError> {
        let session_id = crate::contracts::AutomationSessionId::new(raw_session_id)?;
        let frozen_policy = test_frozen_policy();
        store
            .connection
            .execute(
                "INSERT INTO automation_sessions(\
                     session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                     frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                 ) VALUES (?1,?2,?3,'LIVE','RUNNING',1,?4,?5,'KRW-BTC','h1',?6,'{}',?7,?7)",
                params![
                    session_id.as_str(),
                    format!("start-{raw_session_id}"),
                    account_identity,
                    serde_json::to_string(&frozen_policy.reference).map_err(json_error)?,
                    serde_json::to_string(&frozen_policy).map_err(json_error)?,
                    serde_json::to_string(risk).map_err(json_error)?,
                    created_at.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        Ok(session_id)
    }

    fn krw_baseline_asset(
        amount: Decimal,
    ) -> Result<crate::contracts::AutomationBaselineAssetInput, LabError> {
        Ok(crate::contracts::AutomationBaselineAssetInput {
            currency: "KRW".into(),
            free: AssetQuantity::new(amount)?,
            locked: AssetQuantity::new(Decimal::ZERO)?,
            valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
            mark_price_krw: None,
        })
    }

    fn insert_legacy_test_account_day(
        store: &mut Store,
        account_identity: &str,
        day: &str,
        equity_krw: &str,
        balances: &[UpbitAccountBalance],
        recorded_at: UtcTimestamp,
    ) -> Result<ContentHash, LabError> {
        let balances_json = serde_json::to_string(balances).map_err(json_error)?;
        let digest = ContentHash::of_value(&balances)?;
        store
            .connection
            .execute(
                "INSERT INTO automation_account_days(\
                     account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,\
                     baseline_balances_json,created_at_utc\
                 ) VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    account_identity,
                    day,
                    equity_krw,
                    digest.as_str(),
                    balances_json,
                    recorded_at.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
        Ok(digest)
    }

    fn insert_test_candle(
        store: &mut Store,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        store
            .connection
            .execute(
                "INSERT INTO automation_candles(\
                     session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json\
                 ) VALUES (?1,?2,?3,?4,?5,'{}')",
                params![
                    session_id.as_str(),
                    observation.id.as_str(),
                    observation.candle.close_time_utc.to_rfc3339(),
                    observation.content_digest.as_str(),
                    serde_json::to_string(observation).map_err(json_error)?,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    fn seed_prior_day_fill_with_late_fee(
        store: &mut Store,
        session_id: &crate::contracts::AutomationSessionId,
        fill_at: UtcTimestamp,
        fee_at: UtcTimestamp,
    ) -> Result<(), LabError> {
        let identifier = seed_reserved_test_order(store, session_id, "prior-day-fee", fill_at)?;
        let intent_id: String = store
            .connection
            .query_row(
                "SELECT intent_id FROM automation_orders WHERE client_identifier=?1",
                [identifier.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "UPDATE automation_orders SET state='FILLED',executed_volume='1',\
                     remaining_volume='0',paid_fee_krw='1',remaining_fee_krw='0',\
                     trades_count=1,updated_at_utc=?2 WHERE client_identifier=?1",
                params![identifier.as_str(), fill_at.to_rfc3339()],
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "UPDATE automation_risk_reservations SET state='RELEASED',released_at_utc=?2 \
                 WHERE client_identifier=?1",
                params![identifier.as_str(), fill_at.to_rfc3339()],
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "UPDATE automation_intents SET state='ORDER_RECONCILED' WHERE intent_id=?1",
                [&intent_id],
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "INSERT INTO automation_fills(\
                     trade_uuid,client_identifier,session_id,market,side,price_krw,volume,\
                     funds_krw,trade_time_utc,source_digest,created_at_utc\
                 ) VALUES ('prior-day-fee-trade',?1,?2,'KRW-BTC','BID','100','1','100',?3,'fixture',?3)",
                params![identifier.as_str(), session_id.as_str(), fill_at.to_rfc3339()],
            )
            .map_err(sql_error)?;
        store
            .connection
            .execute(
                "INSERT INTO automation_fee_accruals(\
                     client_identifier,sequence,delta_paid_fee_krw,cumulative_paid_fee_krw,observed_at_utc\
                 ) VALUES (?1,1,'1','1',?2)",
                params![identifier.as_str(), fee_at.to_rfc3339()],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    fn fill_mock_test_order(
        store: &mut Store,
        session_id: &crate::contracts::AutomationSessionId,
        suffix: &str,
        at: UtcTimestamp,
    ) -> Result<AutomationOrderView, LabError> {
        let identifier = seed_reserved_test_order(store, session_id, suffix, at)?;
        let pending = store
            .automation_pending_orders(session_id)?
            .into_iter()
            .find(|order| order.request.client_identifier == identifier)
            .ok_or_else(|| LabError::DataCorrupt(format!("test order {suffix} is missing")))?;
        let detail =
            store.automation_mock_submit_order(&pending.request, pending.decision_mark, at)?;
        store.automation_reconcile_order(&detail, at)
    }

    fn with_legacy_live_baseline_fixture<T>(
        name: &str,
        body: impl FnOnce(
            &mut Store,
            &crate::contracts::AutomationSessionId,
            &str,
            UtcTimestamp,
            UtcTimestamp,
        ) -> Result<T, LabError>,
    ) -> Result<T, LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-{name}-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<T, LabError> {
            let mut store = Store::open(&root)?;
            let identity = format!("live:upbit:{}", "d".repeat(64));
            let day_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let now = UtcTimestamp::parse_rfc3339("2026-10-14T12:00:00Z")?;
            let session_id = seed_live_automation_session(
                &mut store,
                "live-legacy-baseline-session",
                &identity,
                &baseline_test_risk_policy()?,
                day_start,
            )?;
            let balances = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(10_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            insert_legacy_test_account_day(
                &mut store,
                &identity,
                "2026-10-14",
                "10000",
                &balances,
                day_start,
            )?;
            let mark = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T11:00:00Z",
                "2026-10-14T12:00:00Z",
                Decimal::from(1_000),
            )?;
            insert_test_candle(&mut store, &session_id, &mark)?;
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1,\
                         last_feed_received_at_utc=?1 WHERE session_id=?2",
                    params![now.to_rfc3339(), session_id.as_str()],
                )
                .map_err(sql_error)?;
            store.automation_record_account_snapshot(
                &identity,
                AutomationSessionMode::Live,
                now,
                now,
                &balances,
            )?;
            body(&mut store, &session_id, &identity, day_start, now)
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    fn with_legacy_mock_baseline_fixture<T>(
        name: &str,
        now: UtcTimestamp,
        body: impl FnOnce(
            &mut Store,
            &crate::contracts::AutomationSessionId,
            &[UpbitAccountBalance],
            &ContentHash,
        ) -> Result<T, LabError>,
    ) -> Result<T, LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-{name}-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<T, LabError> {
            let mut store = Store::open(&root)?;
            store.automation_seed_mock_venue(now)?;
            let session_id = seed_running_automation_session(
                &mut store,
                "mock-legacy-baseline-fixture-session",
                "KRW-BTC",
                "mock:upbit-default",
                &baseline_test_risk_policy()?,
                now,
            )?;
            let balances = store.automation_mock_venue_accounts()?;
            let digest = insert_legacy_test_account_day(
                &mut store,
                "mock:upbit-default",
                &now.0.format("%Y-%m-%d").to_string(),
                "10000000",
                &balances,
                now,
            )?;
            let mark = completed_market_observation(
                "KRW-BTC",
                UtcTimestamp(now.0 - chrono::Duration::hours(1))
                    .to_rfc3339()
                    .as_str(),
                &now.to_rfc3339(),
                Decimal::from(1_000),
            )?;
            insert_test_candle(&mut store, &session_id, &mark)?;
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1,\
                         last_feed_received_at_utc=?1 WHERE session_id=?2",
                    params![now.to_rfc3339(), session_id.as_str()],
                )
                .map_err(sql_error)?;
            store.automation_record_account_snapshot(
                "mock:upbit-default",
                AutomationSessionMode::Mock,
                now,
                now,
                &balances,
            )?;
            body(&mut store, &session_id, &balances, &digest)
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    fn test_frozen_policy() -> crate::contracts::FrozenPolicyRevision {
        let definition = crate::contracts::PolicyDefinition {
            schema_version: "1.0".into(),
            name: "automation_store_test_policy".into(),
            description: "synthetic store fixture".into(),
            program: crate::contracts::PolicyProgram::Builtin {
                strategy: crate::contracts::StrategySpec::S1 {
                    state: crate::contracts::StateParameters {
                        ema_length: 2,
                        vol_length: 2,
                        k: 0.0,
                    },
                },
            },
        };
        let definition_digest = ContentHash::of_value(&definition)
            .expect("synthetic policy definition serializes deterministically");
        crate::contracts::FrozenPolicyRevision {
            reference: PolicyRevisionRef {
                policy_id: crate::contracts::PolicyId::new("automation-store-test-policy")
                    .expect("synthetic policy id is valid"),
                revision_id: crate::contracts::PolicyRevisionId::new(
                    "automation-store-test-revision",
                )
                .expect("synthetic revision id is valid"),
                definition_digest,
            },
            revision_number: 1,
            parent_revision_id: None,
            family: crate::contracts::StrategyKind::S1,
            origin: crate::contracts::PolicyOrigin::Builtin,
            definition,
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)] // Comprehensive baseline contract test covering missing, blocked, verified, duplicate, idempotent replay, and restart persistence.
    fn live_day_baseline_contract_blocks_without_provider_asof_and_rejects_invalid_inputs()
    -> Result<(), LabError> {
        use crate::contracts::{AutomationBaselineAssetInput, PriceKrw};

        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-live-day-baseline-contract-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let live_identity = format!("live:upbit:{}", "a".repeat(64));
            let other_identity = format!("live:upbit:{}", "b".repeat(64));
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(500_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(50_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let day1_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let day1_now = UtcTimestamp::parse_rfc3339("2026-10-14T01:00:00Z")?;
            let session_id = crate::contracts::AutomationSessionId::new("live-baseline-session")?;
            let frozen_policy = test_frozen_policy();
            store
                .connection
                .execute(
                    "INSERT INTO automation_sessions(\
                         session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                         frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                     ) VALUES (?1,'start-live-baseline',?2,'LIVE','RUNNING',1,?3,?4,'KRW-BTC','h1',?5,'{}',?6,?6)",
                    params![
                        session_id.as_str(),
                        live_identity,
                        serde_json::to_string(&frozen_policy.reference).map_err(json_error)?,
                        serde_json::to_string(&frozen_policy).map_err(json_error)?,
                        serde_json::to_string(&risk).map_err(json_error)?,
                        day1_start.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;

            // 1. Before any baseline is recorded, status reports missing baseline and blocks admission.
            let initial_status = store.automation_baseline_status(&session_id, None, day1_now)?;
            assert!(initial_status.baseline.is_none());
            assert!(initial_status.admission_blocked);
            assert!(
                initial_status
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "DAILY_START_BASELINE_MISSING")
            );

            // Seed a snapshot on day 1 (observed at 01:00:00Z) with 10,000,000 KRW and 0.1 BTC.
            let day1_balances = vec![
                UpbitAccountBalance {
                    currency: "KRW".into(),
                    balance: Decimal::from(10_000_000),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::ZERO,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
                UpbitAccountBalance {
                    currency: "BTC".into(),
                    balance: Decimal::new(1, 1),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::from(100_000_000),
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
            ];
            store.automation_record_account_snapshot(
                &live_identity,
                AutomationSessionMode::Live,
                day1_now,
                day1_now,
                &day1_balances,
            )?;

            let valid_assets = vec![
                AutomationBaselineAssetInput {
                    currency: "KRW".into(),
                    free: AssetQuantity::new(Decimal::from(10_000_000))?,
                    locked: AssetQuantity::new(Decimal::ZERO)?,
                    valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                    mark_price_krw: None,
                },
                AutomationBaselineAssetInput {
                    currency: "BTC".into(),
                    free: AssetQuantity::new(Decimal::new(1, 1))?,
                    locked: AssetQuantity::new(Decimal::ZERO)?,
                    valuation_basis: AutomationAssetValuationBasis::OperatorAttestedPrice,
                    mark_price_krw: Some(PriceKrw::new(Decimal::from(100_000_000))?),
                },
            ];

            // 2. Reject mismatched account_identity.
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-wrong-account")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: other_identity,
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day1_start,
                            source: AutomationBaselineSource::OperatorAttested,
                            assets: valid_assets.clone(),
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("does not match the session's bound account")
            ));

            // 3. Reject DurableMockVenueSeed on LIVE session.
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-wrong-source")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day1_start,
                            source: AutomationBaselineSource::DurableMockVenueSeed,
                            assets: valid_assets.clone(),
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::InvalidConfig(msg)) if msg.contains("DURABLE_MOCK_VENUE_SEED")
            ));

            // 4. Reject non-midnight reference_time_utc.
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-non-midnight")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day1_now,
                            source: AutomationBaselineSource::OperatorAttested,
                            assets: valid_assets.clone(),
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::InvalidConfig(msg)) if msg.contains("must equal UTC midnight")
            ));

            // 5. Reject stale or future day_utc relative to current UTC day.
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-date-2026-10-13")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-13".into(),
                            reference_time_utc: UtcTimestamp::parse_rfc3339("2026-10-13T00:00:00Z")?,
                            source: AutomationBaselineSource::OperatorAttested,
                            assets: valid_assets.clone(),
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("does not match current UTC day")
            ));
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-date-2026-10-15")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-15".into(),
                            reference_time_utc: UtcTimestamp::parse_rfc3339("2026-10-15T00:00:00Z")?,
                            source: AutomationBaselineSource::OperatorAttested,
                            assets: valid_assets.clone(),
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::InvalidConfig(msg)) if msg.contains("in the future")
            ));

            // 6. Reject missing non-zero asset present in exchange snapshot (omitting BTC).
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-missing-btc")?,
                        session_id: session_id.clone(),
                        expected_sequence: 1,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-14".into(),
                            reference_time_utc: day1_start,
                            source: AutomationBaselineSource::OperatorAttested,
                            assets: vec![valid_assets[0].clone()],
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("missing non-zero exchange asset BTC")
            ));

            // 7. OperatorAttested baseline is durably recorded in BLOCKED_INSUFFICIENT_EVIDENCE state (never admitted for risk).
            let op_input = AutomationAccountDayBaselineInput {
                account_identity: live_identity.clone(),
                day_utc: "2026-10-14".into(),
                reference_time_utc: day1_start,
                source: AutomationBaselineSource::OperatorAttested,
                assets: valid_assets.clone(),
                operator_note: Some("operator mid-day attestation".into()),
            };
            let op_req_id = RequestId::new("baseline-day1-operator")?;
            let recorded = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: op_req_id.clone(),
                    session_id: session_id.clone(),
                    expected_sequence: 1,
                    baseline: op_input.clone(),
                },
                None,
                None,
                day1_now,
            )?;
            assert_eq!(recorded.receipt.outcome, AutomationCommandOutcome::Applied);
            let day1_view = recorded
                .baseline
                .as_ref()
                .expect("baseline view returned on RecordBaseline");
            assert_eq!(
                day1_view.verification_state,
                AutomationBaselineVerificationState::BlockedInsufficientEvidence
            );
            assert!(!day1_view.sufficient_for_risk_admission);
            assert!(
                day1_view
                    .block_reasons
                    .iter()
                    .any(|r| r == "OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION")
            );
            assert!(
                day1_view
                    .block_reasons
                    .iter()
                    .any(|r| r.starts_with("OPERATOR_ATTESTED_VALUATION_INSUFFICIENT:BTC"))
            );
            assert_eq!(
                recorded
                    .session
                    .account_risk
                    .as_ref()
                    .and_then(|p| p.risk.daily_loss_used_krw),
                None
            );

            // 8. Replaying the exact same request_id + payload returns Replayed; submitting a second baseline for the same day under a new request_id is rejected with Conflict.
            let replayed = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: op_req_id,
                    session_id: session_id.clone(),
                    expected_sequence: 1,
                    baseline: op_input.clone(),
                },
                None,
                None,
                day1_now,
            )?;
            assert_eq!(replayed.receipt.outcome, AutomationCommandOutcome::Replayed);
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("baseline-day1-duplicate")?,
                        session_id: session_id.clone(),
                        expected_sequence: recorded.session.sequence,
                        baseline: op_input,
                    },
                    None,
                    None,
                    day1_now,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("already has a recorded baseline")
            ));

            // 9. Day 2 records a local midnight snapshot. Local timestamps do not prove a provider as-of balance.
            let day2_start = UtcTimestamp::parse_rfc3339("2026-10-15T00:00:00Z")?;
            let day2_now = UtcTimestamp::parse_rfc3339("2026-10-15T01:00:00Z")?;
            store.automation_record_account_snapshot(
                &live_identity,
                AutomationSessionMode::Live,
                day2_start,
                day2_start,
                &day1_balances,
            )?;
            let midnight_candle = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T23:00:00Z",
                "2026-10-15T00:00:00Z",
                Decimal::from(100_000_000),
            )?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,'{}')",
                    params![
                        session_id.as_str(),
                        midnight_candle.id.as_str(),
                        midnight_candle.candle.close_time_utc.to_rfc3339(),
                        midnight_candle.content_digest.as_str(),
                        serde_json::to_string(&midnight_candle).map_err(json_error)?,
                    ],
                )
                .map_err(sql_error)?;

            // Also record current candle at 01:00:00Z with close = 99,600,000 KRW (0.1 BTC loses 40,000 KRW) and fresh snapshot at 01:00:00Z.
            let hour1_candle = completed_market_observation(
                "KRW-BTC",
                "2026-10-15T00:00:00Z",
                "2026-10-15T01:00:00Z",
                Decimal::from(99_600_000),
            )?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,'{}')",
                    params![
                        session_id.as_str(),
                        hour1_candle.id.as_str(),
                        hour1_candle.candle.close_time_utc.to_rfc3339(),
                        hour1_candle.content_digest.as_str(),
                        serde_json::to_string(&hour1_candle).map_err(json_error)?,
                    ],
                )
                .map_err(sql_error)?;
            store
                .connection
                .execute(
                    "UPDATE automation_sessions SET last_finalized_candle_at_utc=?1,last_feed_received_at_utc=?1 WHERE session_id=?2",
                    params![day2_now.to_rfc3339(), session_id.as_str()],
                )
                .map_err(sql_error)?;
            store.automation_record_account_snapshot(
                &live_identity,
                AutomationSessionMode::Live,
                day2_now,
                day2_now,
                &day1_balances,
            )?;

            let day2_candidate_input = AutomationAccountDayBaselineInput {
                account_identity: live_identity,
                day_utc: "2026-10-15".into(),
                reference_time_utc: day2_start,
                source: AutomationBaselineSource::ExchangeObservedDayStart,
                assets: vec![
                    AutomationBaselineAssetInput {
                        currency: "KRW".into(),
                        free: AssetQuantity::new(Decimal::from(10_000_000))?,
                        locked: AssetQuantity::new(Decimal::ZERO)?,
                        valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                        mark_price_krw: None,
                    },
                    AutomationBaselineAssetInput {
                        currency: "BTC".into(),
                        free: AssetQuantity::new(Decimal::new(1, 1))?,
                        locked: AssetQuantity::new(Decimal::ZERO)?,
                        valuation_basis: AutomationAssetValuationBasis::FinalizedCandleClose,
                        mark_price_krw: None,
                    },
                ],
                operator_note: Some("candidate exchange day-start baseline".into()),
            };
            let blocked_res = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("baseline-day2-without-provider-asof")?,
                    session_id: session_id.clone(),
                    expected_sequence: recorded.session.sequence,
                    baseline: day2_candidate_input,
                },
                None,
                None,
                day2_now,
            )?;
            let day2_view = blocked_res
                .baseline
                .as_ref()
                .expect("blocked day 2 baseline view");
            assert_eq!(
                day2_view.verification_state,
                AutomationBaselineVerificationState::BlockedInsufficientEvidence
            );
            assert!(!day2_view.sufficient_for_risk_admission);
            assert_eq!(day2_view.total_equity_krw, None);
            assert_eq!(day2_view.exchange_evidence, None);
            assert!(
                day2_view
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
            );
            assert_eq!(
                blocked_res
                    .session
                    .account_risk
                    .as_ref()
                    .and_then(|p| p.risk.daily_loss_used_krw),
                None
            );

            // 10. Reopen after a simulated process restart and verify the BLOCKED record remains durable.
            drop(store);
            let reopened = Store::open(&root)?;
            let reopened_status =
                reopened.automation_baseline_status(&session_id, Some("2026-10-15"), day2_now)?;
            assert!(
                reopened_status
                    .baseline
                    .as_ref()
                    .is_some_and(|b| !b.sufficient_for_risk_admission)
            );
            assert_eq!(
                reopened_status
                    .baseline
                    .as_ref()
                    .and_then(|b| b.total_equity_krw),
                None
            );
            assert!(reopened_status.admission_blocked);
            let reopened_view = reopened.automation_get_session(&session_id, day2_now)?;
            assert_eq!(
                reopened_view
                    .account_risk
                    .as_ref()
                    .and_then(|p| p.risk.daily_loss_used_krw),
                None
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }

    #[test]
    #[allow(clippy::too_many_lines)] // Deterministic rollover test covers prior-day fill/fee accounting, external deposit rejection, and late snapshot blocking.
    fn live_day_baseline_rollover_preserves_ledger_checks_but_blocks_without_provider_asof()
    -> Result<(), LabError> {
        use crate::contracts::AutomationBaselineAssetInput;

        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-live-day-baseline-rollover-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create isolated automation test root");
        let result = (|| -> Result<(), LabError> {
            let mut store = Store::open(&root)?;
            let live_identity = format!("live:upbit:{}", "c".repeat(64));
            let risk = AutomationRiskPolicy {
                max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
                max_daily_notional_krw: QuoteAmount::new(Decimal::from(10_000_000))?,
                max_asset_exposure_krw: QuoteAmount::new(Decimal::from(10_000_000))?,
                max_daily_loss_krw: QuoteAmount::new(Decimal::from(500_000))?,
                activation_loss_limit_krw: None,
                max_open_orders: 2,
                max_orders_per_interval: 1,
                max_balance_age_seconds: 60,
                max_feed_age_seconds: 60,
            };
            let day1_start = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
            let day1_fill_time = UtcTimestamp::parse_rfc3339("2026-10-14T12:00:00Z")?;
            let session_id = crate::contracts::AutomationSessionId::new("live-rollover-session")?;
            let frozen_policy = test_frozen_policy();
            store
                .connection
                .execute(
                    "INSERT INTO automation_sessions(\
                         session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                         frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                     ) VALUES (?1,'start-live-rollover',?2,'LIVE','RUNNING',1,?3,?4,'KRW-BTC','h1',?5,'{}',?6,?6)",
                    params![
                        session_id.as_str(),
                        live_identity,
                        serde_json::to_string(&frozen_policy.reference).map_err(json_error)?,
                        serde_json::to_string(&frozen_policy).map_err(json_error)?,
                        serde_json::to_string(&risk).map_err(json_error)?,
                        day1_start.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;

            // Day 1: local exact-midnight timestamps are recorded but cannot verify a LIVE baseline.
            let day1_balances = vec![UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(10_000_000),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            store.automation_record_account_snapshot(
                &live_identity,
                AutomationSessionMode::Live,
                day1_start,
                day1_start,
                &day1_balances,
            )?;
            let day1_res = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("rollover-day1-verified")?,
                    session_id: session_id.clone(),
                    expected_sequence: 1,
                    baseline: AutomationAccountDayBaselineInput {
                        account_identity: live_identity.clone(),
                        day_utc: "2026-10-14".into(),
                        reference_time_utc: day1_start,
                        source: AutomationBaselineSource::ExchangeObservedDayStart,
                        assets: vec![AutomationBaselineAssetInput {
                            currency: "KRW".into(),
                            free: AssetQuantity::new(Decimal::from(10_000_000))?,
                            locked: AssetQuantity::new(Decimal::ZERO)?,
                            valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                            mark_price_krw: None,
                        }],
                        operator_note: None,
                    },
                },
                None,
                None,
                day1_start,
            )?;
            let day1_baseline = day1_res.baseline.as_ref().expect("day 1 baseline readback");
            assert!(!day1_baseline.sufficient_for_risk_admission);
            assert!(
                day1_baseline
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
            );
            let legacy_balances_json = serde_json::to_string(&day1_balances).map_err(json_error)?;
            let legacy_balances_digest = ContentHash::of_value(&day1_balances)?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_account_days(\
                         account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,\
                         baseline_balances_json,created_at_utc\
                     ) VALUES (?1,'2026-10-14','10000000',?2,?3,?4)",
                    params![
                        live_identity,
                        legacy_balances_digest.as_str(),
                        legacy_balances_json,
                        day1_start.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;

            // Record a terminal FILLED buy order on day 1: bought 0.05 BTC for 5,000,000 KRW + 2,500 KRW fee.
            // Expected balances after fill: KRW = 4,997,500, BTC = 0.05.
            let obs = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T11:00:00Z",
                "2026-10-14T12:00:00Z",
                Decimal::from(100_000_000),
            )?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,'{}')",
                    params![
                        session_id.as_str(),
                        obs.id.as_str(),
                        obs.candle.close_time_utc.to_rfc3339(),
                        obs.content_digest.as_str(),
                        serde_json::to_string(&obs).map_err(json_error)?,
                    ],
                )
                .map_err(sql_error)?;
            let intent_id = AutomationIntentId::from_seed("rollover-buy-intent");
            store
                .connection
                .execute(
                    "INSERT INTO automation_intents(intent_id,session_id,observation_id,decision_time_utc,action,state,evaluation_json,created_at_utc) \
                     VALUES (?1,?2,?3,?4,'BUY','ORDER_RECONCILED','{}',?4)",
                    params![
                        intent_id.as_str(),
                        session_id.as_str(),
                        obs.id.as_str(),
                        day1_fill_time.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
            let client_id = AutomationOrderIdentifier::from_intent(&intent_id);
            store
                .connection
                .execute(
                    "INSERT INTO automation_orders(\
                         client_identifier,intent_id,session_id,account_identity,order_request_json,\
                         rules_snapshot_json,request_digest,market,side,state,requested_notional_krw,\
                         decision_mark_krw,reserve_quote_krw,reserve_asset,reserved_asset_volume,\
                         reserved_fee_krw,remaining_fee_krw,executed_volume,remaining_volume,paid_fee_krw,\
                         locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc\
                     ) VALUES (?1,?2,?3,?4,'{}','{}','digest','KRW-BTC','BID','FILLED',\
                               '5000000','100000000','5002500','BTC','0','2500','0','0.05','0','2500','0','KRW',1,?5,?5)",
                    params![
                        client_id.as_str(),
                        intent_id.as_str(),
                        session_id.as_str(),
                        live_identity,
                        day1_fill_time.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_fills(\
                         trade_uuid,client_identifier,session_id,market,side,\
                         price_krw,volume,funds_krw,trade_time_utc,source_digest,created_at_utc\
                     ) VALUES ('rollover-trade-1',?1,?2,'KRW-BTC','BID','100000000','0.05','5000000',?3,'digest',?3)",
                    params![
                        client_id.as_str(),
                        session_id.as_str(),
                        day1_fill_time.to_rfc3339(),
                    ],
                )
                .map_err(sql_error)?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_fee_accruals(\
                         client_identifier,sequence,delta_paid_fee_krw,cumulative_paid_fee_krw,observed_at_utc\
                     ) VALUES (?1,1,'2500','2500',?2)",
                    params![client_id.as_str(), day1_fill_time.to_rfc3339()],
                )
                .map_err(sql_error)?;

            // Day 2: finalized midnight candle at 2026-10-15T00:00:00Z with close = 100,000,000 KRW.
            let day2_start = UtcTimestamp::parse_rfc3339("2026-10-15T00:00:00Z")?;
            let day2_late = UtcTimestamp::parse_rfc3339("2026-10-15T00:00:05Z")?;
            let midnight_candle = completed_market_observation(
                "KRW-BTC",
                "2026-10-14T23:00:00Z",
                "2026-10-15T00:00:00Z",
                Decimal::from(100_000_000),
            )?;
            store
                .connection
                .execute(
                    "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json) \
                     VALUES (?1,?2,?3,?4,?5,'{}')",
                    params![
                        session_id.as_str(),
                        midnight_candle.id.as_str(),
                        midnight_candle.candle.close_time_utc.to_rfc3339(),
                        midnight_candle.content_digest.as_str(),
                        serde_json::to_string(&midnight_candle).map_err(json_error)?,
                    ],
                )
                .map_err(sql_error)?;

            // First, simulate an external 100,000 KRW deposit in the snapshot (KRW = 5,097,500 instead of 4,997,500).
            // Both snapshot recording and baseline input reject external deposits without silent auto-correction.
            let diverged_balances = vec![
                UpbitAccountBalance {
                    currency: "KRW".into(),
                    balance: Decimal::from(5_097_500),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::ZERO,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
                UpbitAccountBalance {
                    currency: "BTC".into(),
                    balance: Decimal::new(5, 2),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::from(100_000_000),
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
            ];
            assert!(matches!(
                store.automation_record_account_snapshot(
                    &live_identity,
                    AutomationSessionMode::Live,
                    day2_late,
                    day2_late,
                    &diverged_balances,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("manual or unreconciled account divergence for KRW")
            ));

            // Record the exact ledger-consistent snapshot at 00:00:05Z (KRW = 4,997,500, BTC = 0.05).
            // Submitting a baseline with diverged KRW (5,097,500) is also rejected with Conflict.
            let matching_balances = vec![
                UpbitAccountBalance {
                    currency: "KRW".into(),
                    balance: Decimal::from(4_997_500),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::ZERO,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
                UpbitAccountBalance {
                    currency: "BTC".into(),
                    balance: Decimal::new(5, 2),
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::from(100_000_000),
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
            ];
            store.automation_record_account_snapshot(
                &live_identity,
                AutomationSessionMode::Live,
                day2_late,
                day2_late,
                &matching_balances,
            )?;
            assert!(matches!(
                store.automation_write(
                    &AutomationSessionWrite::RecordBaseline {
                        request_id: RequestId::new("rollover-diverged-deposit")?,
                        session_id: session_id.clone(),
                        expected_sequence: day1_res.session.sequence,
                        baseline: AutomationAccountDayBaselineInput {
                            account_identity: live_identity.clone(),
                            day_utc: "2026-10-15".into(),
                            reference_time_utc: day2_start,
                            source: AutomationBaselineSource::PriorDayVerifiedLedgerRollover,
                            assets: vec![
                                AutomationBaselineAssetInput {
                                    currency: "KRW".into(),
                                    free: AssetQuantity::new(Decimal::from(5_097_500))?,
                                    locked: AssetQuantity::new(Decimal::ZERO)?,
                                    valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                                    mark_price_krw: None,
                                },
                                AutomationBaselineAssetInput {
                                    currency: "BTC".into(),
                                    free: AssetQuantity::new(Decimal::new(5, 2))?,
                                    locked: AssetQuantity::new(Decimal::ZERO)?,
                                    valuation_basis: AutomationAssetValuationBasis::FinalizedCandleClose,
                                    mark_price_krw: None,
                                },
                            ],
                            operator_note: None,
                        },
                    },
                    None,
                    None,
                    day2_late,
                ),
                Err(LabError::Conflict(msg)) if msg.contains("diverge from exchange snapshot")
            ));
            let rollover_res = store.automation_write(
                &AutomationSessionWrite::RecordBaseline {
                    request_id: RequestId::new("rollover-day2-verified")?,
                    session_id: session_id.clone(),
                    expected_sequence: day1_res.session.sequence,
                    baseline: AutomationAccountDayBaselineInput {
                        account_identity: live_identity,
                        day_utc: "2026-10-15".into(),
                        reference_time_utc: day2_start,
                        source: AutomationBaselineSource::PriorDayVerifiedLedgerRollover,
                        assets: vec![
                            AutomationBaselineAssetInput {
                                currency: "KRW".into(),
                                free: AssetQuantity::new(Decimal::from(4_997_500))?,
                                locked: AssetQuantity::new(Decimal::ZERO)?,
                                valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                                mark_price_krw: None,
                            },
                            AutomationBaselineAssetInput {
                                currency: "BTC".into(),
                                free: AssetQuantity::new(Decimal::new(5, 2))?,
                                locked: AssetQuantity::new(Decimal::ZERO)?,
                                valuation_basis:
                                    AutomationAssetValuationBasis::FinalizedCandleClose,
                                mark_price_krw: None,
                            },
                        ],
                        operator_note: Some("verified rollover from day 1".into()),
                    },
                },
                None,
                None,
                day2_late,
            )?;
            let rollover_view = rollover_res
                .baseline
                .as_ref()
                .expect("blocked rollover baseline view");
            assert_eq!(
                rollover_view.verification_state,
                AutomationBaselineVerificationState::BlockedInsufficientEvidence
            );
            assert!(!rollover_view.sufficient_for_risk_admission);
            assert_eq!(rollover_view.total_equity_krw, None);
            assert_eq!(rollover_view.exchange_evidence, None);
            assert!(
                rollover_view
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
            );
            assert!(
                rollover_view
                    .block_reasons
                    .iter()
                    .any(|reason| reason == "PRIOR_DAY_VERIFIED_BASELINE_MISSING")
            );
            assert_eq!(
                rollover_res
                    .session
                    .account_risk
                    .as_ref()
                    .and_then(|projection| projection.risk.daily_loss_used_krw),
                None
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&root);
        result
    }
}

fn reject_reserved_orders_for_session(
    transaction: &Transaction<'_>,
    session_id: &crate::contracts::AutomationSessionId,
    reason: &str,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let mut statement = transaction
        .prepare(
            "SELECT client_identifier FROM automation_orders \
             WHERE session_id=?1 AND state='RESERVED' ORDER BY created_at_utc,client_identifier",
        )
        .map_err(sql_error)?;
    let identifiers = statement
        .query_map([session_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(statement);
    for raw_identifier in identifiers {
        let identifier = AutomationOrderIdentifier::new(raw_identifier)?;
        reject_reserved_order_in_transaction(transaction, session_id, &identifier, reason, now)?;
    }
    Ok(())
}

fn reject_pending_intent_in_transaction(
    transaction: &Transaction<'_>,
    intent_id: &crate::contracts::AutomationIntentId,
    reason: &str,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    let session_id: String = transaction
        .query_row(
            "SELECT session_id FROM automation_intents WHERE intent_id=?1",
            [intent_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown automation intent".into()))?;
    let changed = transaction
        .execute(
            "UPDATE automation_intents SET state='REJECTED' WHERE intent_id=?1 AND state='PENDING'",
            [intent_id.as_str()],
        )
        .map_err(sql_error)?;
    if changed == 1 {
        let session_id = crate::contracts::AutomationSessionId::new(session_id)?;
        let sequence = next_event_sequence(transaction, &session_id)?;
        insert_event(
            transaction,
            &session_id,
            sequence,
            AutomationEventKind::IntentRejected,
            now,
            serde_json::json!({"intent_id":intent_id,"reason":reason}),
        )?;
    }
    Ok(changed == 1)
}

pub(super) fn reject_unsent_live_work_in_control_transaction(
    transaction: &Transaction<'_>,
    reason: &str,
    now: UtcTimestamp,
    limit: usize,
) -> Result<usize, LabError> {
    if reason.is_empty() || reason.len() > 2_048 || !(1..=64).contains(&limit) {
        return Err(LabError::InvalidConfig(
            "live-order reserved rejection exceeds bounds".into(),
        ));
    }
    let limit = i64::try_from(limit).map_err(|_| {
        LabError::InvalidConfig("order rejection batch exceeds SQLite bounds".into())
    })?;
    let mut statement = transaction
        .prepare(
            "SELECT o.session_id,o.client_identifier FROM automation_orders o \
             JOIN automation_sessions s USING(session_id) \
             WHERE o.state='RESERVED' AND s.mode='LIVE' \
             ORDER BY o.created_at_utc,o.client_identifier LIMIT ?1",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    drop(statement);
    let mut rejected = 0;
    for (session, raw_identifier) in rows {
        let session_id = crate::contracts::AutomationSessionId::new(session)?;
        let identifier = AutomationOrderIdentifier::new(raw_identifier)?;
        if reject_reserved_order_in_transaction(transaction, &session_id, &identifier, reason, now)?
        {
            rejected += 1;
        }
    }
    let remaining = limit
        - i64::try_from(rejected)
            .map_err(|_| LabError::Internal("rejection batch count overflow".into()))?;
    if remaining > 0 {
        let mut statement = transaction.prepare(
            "SELECT i.intent_id FROM automation_intents i JOIN automation_sessions s USING(session_id) \
             WHERE i.state='PENDING' AND s.mode='LIVE' ORDER BY i.created_at_utc,i.intent_id LIMIT ?1"
        ).map_err(sql_error)?;
        let pending = statement
            .query_map([remaining], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        for raw_intent in pending {
            let intent_id = crate::contracts::AutomationIntentId::new(raw_intent)?;
            if reject_pending_intent_in_transaction(transaction, &intent_id, reason, now)? {
                rejected += 1;
            }
        }
    }
    Ok(rejected)
}

fn reject_reserved_order_in_transaction(
    transaction: &Transaction<'_>,
    session_id: &crate::contracts::AutomationSessionId,
    identifier: &AutomationOrderIdentifier,
    reason: &str,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    let intent_id = transaction
        .query_row(
            "SELECT intent_id FROM automation_orders \
             WHERE client_identifier=?1 AND session_id=?2 AND state='RESERVED'",
            params![identifier.as_str(), session_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(intent_id) = intent_id else {
        return Ok(false);
    };
    let detail_json = serde_json::to_string(&serde_json::json!({
        "reason": reason,
        "pre_send": true,
    }))
    .map_err(json_error)?;
    let rejected = transaction
        .execute(
            "UPDATE automation_orders SET state='REJECTED',detail_json=?1,updated_at_utc=?2 \
             WHERE client_identifier=?3 AND session_id=?4 AND state='RESERVED'",
            params![
                detail_json,
                now.to_rfc3339(),
                identifier.as_str(),
                session_id.as_str()
            ],
        )
        .map_err(sql_error)?;
    if rejected != 1 {
        return Ok(false);
    }
    let released = transaction
        .execute(
            "UPDATE automation_risk_reservations SET state='RELEASED',released_at_utc=?1 \
             WHERE client_identifier=?2 AND state='ACTIVE'",
            params![now.to_rfc3339(), identifier.as_str()],
        )
        .map_err(sql_error)?;
    if released != 1 {
        return Err(LabError::DataCorrupt(
            "pre-send rejected order has no unique active risk reservation".into(),
        ));
    }
    let intent_rejected = transaction
        .execute(
            "UPDATE automation_intents SET state='REJECTED' \
             WHERE intent_id=?1 AND state='ORDER_RESERVED'",
            [&intent_id],
        )
        .map_err(sql_error)?;
    if intent_rejected != 1 {
        return Err(LabError::DataCorrupt(
            "pre-send rejected order has no reserved strategy intent".into(),
        ));
    }
    insert_event(
        transaction,
        session_id,
        next_event_sequence(transaction, session_id)?,
        AutomationEventKind::IntentRejected,
        now,
        serde_json::json!({
            "intent_id": intent_id,
            "client_identifier": identifier,
            "reason": reason,
            "pre_send": true,
        }),
    )?;
    Ok(true)
}

fn ensure_live_order_control_enabled(connection: &Connection) -> Result<(), LabError> {
    let (available, enabled): (bool, bool) = connection
        .query_row(
            "SELECT available,live_trading_enabled FROM live_order_control_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    if available && !enabled {
        return Err(LabError::BlockedEvidence(
            "LIVE_ORDER_SENDING_DISABLED".into(),
        ));
    }
    Ok(())
}

fn ensure_live_account_identity_history(
    transaction: &Transaction<'_>,
    account_identity: &str,
) -> Result<(), LabError> {
    let rotated: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_sessions \
             WHERE mode='LIVE' AND account_identity!=?1)",
            [account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if rotated {
        return Err(LabError::BlockedEvidence(
            "live credential fingerprint changed; explicit account rebind is required".into(),
        ));
    }
    Ok(())
}

fn account_day_exists(
    connection: &Connection,
    account_identity: &str,
    day: &str,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT 1 FROM automation_account_days WHERE account_identity=?1 AND day_utc=?2",
            params![account_identity, day],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .map_err(sql_error)
}

fn account_has_any_day_baseline(
    connection: &Connection,
    account_identity: &str,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_account_days WHERE account_identity=?1)",
            [account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn account_day_baseline_exists(
    connection: &Connection,
    account_identity: &str,
    day: &str,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_account_day_baselines \
             WHERE account_identity=?1 AND day_utc=?2)",
            params![account_identity, day],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

struct PersistedAccountDayBaseline {
    record_json: String,
    baseline_id: String,
    mode: String,
    reference_time_utc: String,
    source: String,
    verification_state: String,
    sufficient_for_risk_admission: bool,
    total_equity_krw: Option<String>,
    baseline_balances_digest: String,
    baseline_balances_json: String,
    valuation_evidence_json: String,
    block_reasons_json: String,
    baseline_digest: String,
    request_id: Option<String>,
    created_at_utc: String,
}

fn load_account_day_baseline_view(
    connection: &Connection,
    account_identity: &str,
    day: &str,
) -> Result<Option<AutomationAccountDayBaselineView>, LabError> {
    let row = connection
        .query_row(
            "SELECT record_json,baseline_id,mode,reference_time_utc,source,verification_state,\
                    sufficient_for_risk_admission,total_equity_krw,baseline_balances_digest,\
                    baseline_balances_json,valuation_evidence_json,block_reasons_json,\
                    baseline_digest,request_id,created_at_utc \
             FROM automation_account_day_baselines \
             WHERE account_identity=?1 AND day_utc=?2",
            params![account_identity, day],
            |row| {
                Ok(PersistedAccountDayBaseline {
                    record_json: row.get(0)?,
                    baseline_id: row.get(1)?,
                    mode: row.get(2)?,
                    reference_time_utc: row.get(3)?,
                    source: row.get(4)?,
                    verification_state: row.get(5)?,
                    sufficient_for_risk_admission: row.get(6)?,
                    total_equity_krw: row.get(7)?,
                    baseline_balances_digest: row.get(8)?,
                    baseline_balances_json: row.get(9)?,
                    valuation_evidence_json: row.get(10)?,
                    block_reasons_json: row.get(11)?,
                    baseline_digest: row.get(12)?,
                    request_id: row.get(13)?,
                    created_at_utc: row.get(14)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let view: AutomationAccountDayBaselineView =
        serde_json::from_str(&row.record_json).map_err(json_error)?;
    if view.account_identity != account_identity
        || view.day_utc != day
        || view.baseline_id.as_str() != row.baseline_id
        || mode_name(view.mode) != row.mode
        || view.reference_time_utc.to_rfc3339() != row.reference_time_utc
        || baseline_source_name(view.source) != row.source
        || baseline_verification_state_name(view.verification_state) != row.verification_state
        || view.sufficient_for_risk_admission != row.sufficient_for_risk_admission
        || view.total_equity_krw.map(|equity| equity.get().to_string()) != row.total_equity_krw
        || serde_json::to_string(&view.exchange_evidence).map_err(json_error)?
            != row.valuation_evidence_json
        || serde_json::to_string(&view.block_reasons).map_err(json_error)? != row.block_reasons_json
        || view.baseline_digest.as_str() != row.baseline_digest
        || view.request_id.as_ref().map(RequestId::as_str) != row.request_id.as_deref()
        || view.recorded_at_utc.to_rfc3339() != row.created_at_utc
        || compute_baseline_digest(&view)? != view.baseline_digest
    {
        return Err(LabError::DataCorrupt(
            "persisted account day baseline failed record, status, identity, or digest verification"
                .into(),
        ));
    }
    let balances: Vec<UpbitAccountBalance> =
        serde_json::from_str(&row.baseline_balances_json).map_err(json_error)?;
    if ContentHash::of_value(&balances)?.as_str() != row.baseline_balances_digest {
        return Err(LabError::DataCorrupt(
            "persisted account day baseline failed balance digest verification".into(),
        ));
    }
    let input_assets = view
        .assets
        .iter()
        .map(|asset| crate::contracts::AutomationBaselineAssetInput {
            currency: asset.currency.clone(),
            free: asset.free,
            locked: asset.locked,
            valuation_basis: asset.valuation_basis,
            mark_price_krw: asset.mark_price_krw,
        })
        .collect::<Vec<_>>();
    verify_baseline_assets_match_snapshot(&input_assets, &balances)?;
    Ok(Some(view))
}

struct StoredLegacyMockBaselineRow {
    equity_krw: String,
    balances_digest: String,
    balances_json: String,
    recorded_at_utc: String,
}

struct ValidatedLegacyMockBaselineRecord {
    baseline_equity_krw: QuoteAmount,
    balances_digest: ContentHash,
    recorded_at_utc: UtcTimestamp,
}

fn load_legacy_mock_baseline_view(
    connection: &Connection,
    account_identity: &str,
    mode: AutomationSessionMode,
    day: &str,
    now: UtcTimestamp,
) -> Result<Option<AutomationLegacyMockBaselineView>, LabError> {
    if mode != AutomationSessionMode::Mock {
        return Ok(None);
    }
    validate_account_identity(mode, account_identity).map_err(|_| {
        LabError::DataCorrupt("legacy MOCK baseline has an invalid account identity".into())
    })?;
    let Some(row) = load_legacy_mock_baseline_row(connection, account_identity, day)? else {
        return Ok(None);
    };
    let Some(record) = validate_legacy_mock_baseline_record(connection, day, now, row)? else {
        return Ok(None);
    };
    if account_day_has_recorded_activity(
        connection,
        account_identity,
        AutomationSessionMode::Mock,
        parse_day_start_utc(day)?,
        record.recorded_at_utc,
    )? || !validate_legacy_mock_current_ledger(connection, account_identity, now)?
    {
        return Ok(None);
    }
    Ok(Some(AutomationLegacyMockBaselineView {
        account_identity: account_identity.to_owned(),
        day_utc: day.to_owned(),
        baseline_equity_krw: record.baseline_equity_krw,
        balances_digest: record.balances_digest,
        recorded_at_utc: record.recorded_at_utc,
    }))
}

fn load_legacy_mock_baseline_row(
    connection: &Connection,
    account_identity: &str,
    day: &str,
) -> Result<Option<StoredLegacyMockBaselineRow>, LabError> {
    connection
        .query_row(
            "SELECT baseline_equity_krw,baseline_balances_digest,\
                    baseline_balances_json,created_at_utc \
             FROM automation_account_days \
             WHERE account_identity=?1 AND day_utc=?2",
            params![account_identity, day],
            |row| {
                Ok(StoredLegacyMockBaselineRow {
                    equity_krw: row.get(0)?,
                    balances_digest: row.get(1)?,
                    balances_json: row.get(2)?,
                    recorded_at_utc: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

fn validate_legacy_mock_baseline_record(
    connection: &Connection,
    day: &str,
    now: UtcTimestamp,
    row: StoredLegacyMockBaselineRow,
) -> Result<Option<ValidatedLegacyMockBaselineRecord>, LabError> {
    let day_start = parse_day_start_utc(day)?;
    let recorded_at = UtcTimestamp::parse_rfc3339(&row.recorded_at_utc)?;
    if recorded_at.0.format("%Y-%m-%d").to_string() != day
        || recorded_at < day_start
        || recorded_at > now
    {
        return Err(LabError::DataCorrupt(
            "legacy MOCK baseline local record time does not match its UTC day or current read time"
                .into(),
        ));
    }
    let seed_at = connection
        .query_row(
            "SELECT created_at_utc FROM automation_mock_venue_seed \
             WHERE singleton=1 AND seed_version=1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(seed_at) = seed_at else {
        return Ok(None);
    };
    if UtcTimestamp::parse_rfc3339(&seed_at)? > recorded_at {
        return Err(LabError::DataCorrupt(
            "legacy MOCK baseline local record time predates its durable venue seed".into(),
        ));
    }
    let balances: Vec<UpbitAccountBalance> =
        serde_json::from_str(&row.balances_json).map_err(json_error)?;
    let digest = ContentHash::try_from(row.balances_digest).map_err(|error| {
        LabError::DataCorrupt(format!(
            "invalid persisted MOCK legacy balance digest: {error}"
        ))
    })?;
    if ContentHash::of_value(&balances)? != digest {
        return Err(LabError::DataCorrupt(
            "persisted MOCK legacy day baseline failed balance digest verification".into(),
        ));
    }
    validate_mock_legacy_balances(&balances)?;
    let equity = Decimal::from_str_exact(&row.equity_krw).map_err(|error| {
        LabError::DataCorrupt(format!(
            "invalid persisted MOCK legacy day baseline equity: {error}"
        ))
    })?;
    if equity < Decimal::ZERO {
        return Err(LabError::DataCorrupt(
            "persisted MOCK legacy day baseline equity is negative".into(),
        ));
    }
    Ok(Some(ValidatedLegacyMockBaselineRecord {
        baseline_equity_krw: QuoteAmount::new(equity)?,
        balances_digest: digest,
        recorded_at_utc: recorded_at,
    }))
}

fn validate_mock_legacy_balances(balances: &[UpbitAccountBalance]) -> Result<(), LabError> {
    let mut currencies = std::collections::BTreeSet::new();
    for balance in balances {
        if balance.currency.is_empty()
            || balance.balance < Decimal::ZERO
            || balance.locked < Decimal::ZERO
            || !currencies.insert(balance.currency.as_str())
        {
            return Err(LabError::DataCorrupt(
                "persisted MOCK legacy day baseline has invalid or duplicate currency amounts"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_legacy_mock_current_ledger(
    connection: &Connection,
    account_identity: &str,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    let latest: Option<(String, String, String, String, String)> = connection
        .query_row(
            "SELECT source_mode,observed_at_utc,created_at_utc,balances_json,balances_digest \
             FROM automation_account_snapshots WHERE account_identity=?1 \
             ORDER BY snapshot_sequence DESC LIMIT 1",
            [account_identity],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((mode, observed_raw, received_raw, balances_json, balances_digest)) = latest else {
        return Ok(false);
    };
    if parse_mode(&mode)? != AutomationSessionMode::Mock {
        return Err(LabError::DataCorrupt(
            "legacy MOCK baseline has a non-MOCK account snapshot".into(),
        ));
    }
    let observed_at = UtcTimestamp::parse_rfc3339(&observed_raw)?;
    let received_at = UtcTimestamp::parse_rfc3339(&received_raw)?;
    if observed_at > received_at || received_at > now {
        return Err(LabError::DataCorrupt(
            "legacy MOCK baseline account snapshot has an invalid local observation/receipt time"
                .into(),
        ));
    }
    let balances: Vec<UpbitAccountBalance> =
        serde_json::from_str(&balances_json).map_err(json_error)?;
    if ContentHash::of_value(&balances)?.as_str() != balances_digest {
        return Err(LabError::DataCorrupt(
            "persisted MOCK account snapshot failed balance digest verification".into(),
        ));
    }
    match validate_account_balance_ledger(connection, account_identity, observed_at, &balances) {
        Ok(()) => Ok(true),
        Err(LabError::BlockedEvidence(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn compute_baseline_digest(
    view: &AutomationAccountDayBaselineView,
) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&(
        &view.baseline_id,
        &view.account_identity,
        view.mode,
        &view.day_utc,
        view.reference_time_utc,
        view.source,
        view.verification_state,
        view.sufficient_for_risk_admission,
        view.total_equity_krw,
        &view.assets,
        &view.exchange_evidence,
        &view.block_reasons,
        &view.request_id,
    ))
}

#[derive(Clone, Copy)]
struct AdmissionBlockCheckInput<'a> {
    account_identity: &'a str,
    mode: AutomationSessionMode,
    session_state: AutomationSessionState,
    recovery_reason: Option<&'a str>,
    emergency_latched: bool,
    order_admission_blocked: bool,
    live_grant_active: bool,
    live_execution_policy: LiveExecutionPolicy,
    balance_freshness: AutomationFreshness,
    feed_freshness: AutomationFreshness,
    effective_loss: &'a AutomationEffectiveLossProjection,
    asset_exposure: Option<QuoteAmount>,
}

fn compute_admission_block_reasons(
    connection: &Connection,
    input: AdmissionBlockCheckInput<'_>,
) -> Result<Vec<String>, LabError> {
    let mut reasons = Vec::new();
    if input.session_state != AutomationSessionState::Running {
        reasons.push("SESSION_NOT_RUNNING".into());
    }
    if let Some(reason) = input.recovery_reason {
        reasons.push(format!("RECOVERY:{reason}"));
    }
    if input.emergency_latched {
        reasons.push("EMERGENCY_STOP_LATCHED".into());
    }
    if input.mode == AutomationSessionMode::Live && input.order_admission_blocked {
        reasons.push("ORDER_ADMISSION_BLOCKED".into());
    }
    if input.mode == AutomationSessionMode::Live {
        if !input.live_execution_policy.is_enabled() {
            reasons.push("LIVE_TRADING_DISABLED".into());
        }
        if !input.live_grant_active {
            reasons.push("LIVE_GRANT_INACTIVE".into());
        }
    }
    reasons.extend(input.effective_loss.block_reasons.iter().cloned());
    let unresolved: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_orders WHERE account_identity=?1 \
             AND state IN ('SUBMITTED_UNKNOWN','NEEDS_RECONCILIATION')",
            [input.account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if unresolved > 0 {
        reasons.push("UNRESOLVED_VENUE_ORDER_READBACK_REQUIRED".into());
    }
    if input.balance_freshness != AutomationFreshness::Fresh {
        reasons.push(format!(
            "BALANCE_SNAPSHOT_NOT_FRESH:{:?}",
            input.balance_freshness
        ));
    }
    if input.feed_freshness != AutomationFreshness::Fresh {
        reasons.push(format!(
            "FINALIZED_FEED_NOT_FRESH:{:?}",
            input.feed_freshness
        ));
    }
    if input.asset_exposure.is_none() {
        reasons.push("ASSET_EXPOSURE_UNKNOWN".into());
    }
    Ok(reasons)
}

#[allow(clippy::too_many_lines)] // Baseline verification folds exchange snapshot proof, prior-day ledger continuity, and midnight valuation atomically.
fn evaluate_and_persist_baseline(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    account_identity: &str,
    mode: AutomationSessionMode,
    input: &AutomationAccountDayBaselineInput,
    day_start: UtcTimestamp,
    now: UtcTimestamp,
) -> Result<AutomationAccountDayBaselineView, LabError> {
    if input.reference_time_utc != day_start {
        return Err(LabError::InvalidConfig(format!(
            "daily starting baseline reference_time_utc ({}) must equal UTC midnight ({})",
            input.reference_time_utc.to_rfc3339(),
            day_start.to_rfc3339()
        )));
    }
    let mut reference_time_utc = input.reference_time_utc;
    let latest_snapshot: Option<(i64, String, String, String, String, String)> = transaction
        .query_row(
            "SELECT snapshot_sequence,observed_at_utc,created_at_utc,source_mode,balances_json,balances_digest \
             FROM automation_account_snapshots WHERE account_identity=?1 \
             ORDER BY snapshot_sequence DESC LIMIT 1",
            [account_identity],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;

    let mut canonical_balances = Vec::with_capacity(input.assets.len());
    for asset in &input.assets {
        canonical_balances.push(UpbitAccountBalance {
            currency: asset.currency.clone(),
            balance: asset.free.get(),
            locked: asset.locked.get(),
            avg_buy_price: Decimal::ZERO,
            avg_buy_price_modified: false,
            unit_currency: "KRW".into(),
        });
    }
    canonical_balances.sort_by(|lhs, rhs| lhs.currency.cmp(&rhs.currency));
    let canonical_balances_digest = ContentHash::of_value(&canonical_balances)?;
    let canonical_balances_json = serde_json::to_string(&canonical_balances).map_err(json_error)?;

    if let Some((_, ref obs_str, _, ref src_mode_str, ref bal_json, _)) = latest_snapshot {
        if parse_mode(src_mode_str)? != mode {
            return Err(LabError::Conflict(
                "exchange account snapshot mode differs from session mode".into(),
            ));
        }
        let obs_at = UtcTimestamp::parse_rfc3339(obs_str)?;
        let snapshot_balances: Vec<UpbitAccountBalance> =
            serde_json::from_str(bal_json).map_err(json_error)?;
        if account_has_any_day_baseline(transaction, account_identity)? {
            validate_account_balance_ledger(
                transaction,
                account_identity,
                obs_at,
                &snapshot_balances,
            )?;
        }
        verify_baseline_assets_match_snapshot(&input.assets, &snapshot_balances)?;
    }

    let mut block_reasons = Vec::new();
    let mut exchange_evidence = None;
    let mut prior_day_verified_digest = None;
    if mode == AutomationSessionMode::Live {
        // Current Upbit account reads expose local observation and receipt times,
        // but no provider-owned as-of timestamp for a UTC-day opening balance.
        block_reasons.push("LIVE_PROVIDER_AS_OF_UNAVAILABLE".into());
    }

    let prev_day = (day_start.0 - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    if mode == AutomationSessionMode::Mock {
        if let Some(previous) =
            load_account_day_baseline_view(transaction, account_identity, &prev_day)?
        {
            if previous.mode != AutomationSessionMode::Mock {
                return Err(LabError::DataCorrupt(
                    "prior MOCK baseline has a non-MOCK persisted mode".into(),
                ));
            }
            if previous.sufficient_for_risk_admission
                && previous.verification_state == AutomationBaselineVerificationState::Verified
            {
                prior_day_verified_digest = Some(previous.baseline_digest);
            }
        } else if let Some(previous) =
            load_legacy_mock_baseline_view(transaction, account_identity, mode, &prev_day, now)?
        {
            prior_day_verified_digest = Some(previous.balances_digest);
        }
    }

    let has_latest_snapshot = latest_snapshot.is_some();
    match input.source {
        AutomationBaselineSource::OperatorAttested => {
            block_reasons.push("OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION".into());
        }
        AutomationBaselineSource::ExchangeObservedDayStart => {
            if mode != AutomationSessionMode::Live {
                block_reasons.push("EXCHANGE_DAY_START_REQUIRES_LIVE_PROVIDER_AS_OF".into());
            }
            if !has_latest_snapshot {
                block_reasons.push("EXCHANGE_ACCOUNT_SNAPSHOT_MISSING".into());
            }
        }
        AutomationBaselineSource::PriorDayVerifiedLedgerRollover => {
            if prior_day_verified_digest.is_none() {
                block_reasons.push("PRIOR_DAY_VERIFIED_BASELINE_MISSING".into());
            }
            if mode == AutomationSessionMode::Mock
                && let Some((seq, obs_str, rec_str, _, _, bal_digest)) = latest_snapshot
            {
                let obs_at = UtcTimestamp::parse_rfc3339(&obs_str)?;
                let rec_at = UtcTimestamp::parse_rfc3339(&rec_str)?;
                if obs_at < day_start {
                    block_reasons
                        .push("POST_MIDNIGHT_EXCHANGE_SNAPSHOT_REQUIRED_FOR_ROLLOVER".into());
                }
                exchange_evidence = Some(AutomationBaselineExchangeEvidence {
                    snapshot_sequence: Some(from_sql_u64(seq)?),
                    snapshot_observed_at_utc: Some(obs_at),
                    snapshot_received_at_utc: Some(rec_at),
                    snapshot_balances_digest: Some(ContentHash::try_from(bal_digest)?),
                    prior_day_baseline_digest: prior_day_verified_digest,
                });
            } else if !has_latest_snapshot {
                block_reasons.push("EXCHANGE_ACCOUNT_SNAPSHOT_MISSING".into());
            }
        }
        AutomationBaselineSource::DurableMockVenueSeed => {
            if mode != AutomationSessionMode::Mock {
                return Err(LabError::InvalidConfig(
                    "DURABLE_MOCK_VENUE_SEED is only valid for MOCK sessions".into(),
                ));
            }
            let seed_at = transaction
                .query_row(
                    "SELECT created_at_utc FROM automation_mock_venue_seed \
                     WHERE singleton=1 AND seed_version=1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(seed_at) = seed_at {
                let seed_at = UtcTimestamp::parse_rfc3339(&seed_at)?;
                if seed_at > now {
                    return Err(LabError::DataCorrupt(
                        "durable MOCK venue seed timestamp is in the future".into(),
                    ));
                }
                if seed_at > day_start {
                    // A virtual seed becomes authoritative when it is created; preserve that
                    // actual reference time rather than projecting it back to UTC midnight.
                    reference_time_utc = seed_at;
                }
            } else {
                block_reasons.push("DURABLE_MOCK_SEED_MISSING".into());
            }
            if mock_venue_has_any_trade(transaction)? || !mock_wallet_matches_genesis(transaction)?
            {
                block_reasons.push("DURABLE_MOCK_SEED_GENESIS_MISMATCH".into());
            }
            let mock_balances = load_mock_venue_balances(transaction)?;
            verify_baseline_assets_match_snapshot(&input.assets, &mock_balances)?;
            if let Some((seq, obs_str, rec_str, _, _, bal_digest)) = latest_snapshot {
                exchange_evidence = Some(AutomationBaselineExchangeEvidence {
                    snapshot_sequence: Some(from_sql_u64(seq)?),
                    snapshot_observed_at_utc: Some(UtcTimestamp::parse_rfc3339(&obs_str)?),
                    snapshot_received_at_utc: Some(UtcTimestamp::parse_rfc3339(&rec_str)?),
                    snapshot_balances_digest: Some(ContentHash::try_from(bal_digest)?),
                    prior_day_baseline_digest: prior_day_verified_digest,
                });
            }
        }
    }

    let mut evaluated_assets = Vec::with_capacity(input.assets.len());
    let mut total_equity = Decimal::ZERO;
    let mut all_assets_valued = true;

    for asset in &input.assets {
        let total_qty = asset
            .free
            .get()
            .checked_add(asset.locked.get())
            .ok_or_else(|| {
                LabError::DataCorrupt("baseline asset total exceeds decimal capacity".into())
            })?;
        let total = AssetQuantity::new(total_qty)?;
        if asset.currency == "KRW" {
            let mark = crate::contracts::PriceKrw::new(Decimal::ONE)?;
            let valuation = QuoteAmount::new(total_qty)?;
            total_equity = total_equity.checked_add(total_qty).ok_or_else(|| {
                LabError::DataCorrupt("baseline total equity exceeds decimal capacity".into())
            })?;
            evaluated_assets.push(AutomationBaselineAssetEntry {
                currency: asset.currency.clone(),
                free: asset.free,
                locked: asset.locked,
                total,
                valuation_basis: asset.valuation_basis,
                mark_price_krw: Some(mark),
                valuation_krw: Some(valuation),
                candle_observation_id: None,
                candle_content_digest: None,
            });
            continue;
        }
        if total_qty == Decimal::ZERO {
            evaluated_assets.push(AutomationBaselineAssetEntry {
                currency: asset.currency.clone(),
                free: asset.free,
                locked: asset.locked,
                total,
                valuation_basis: asset.valuation_basis,
                mark_price_krw: asset.mark_price_krw,
                valuation_krw: Some(QuoteAmount::new(Decimal::ZERO)?),
                candle_observation_id: None,
                candle_content_digest: None,
            });
            continue;
        }
        match asset.valuation_basis {
            AutomationAssetValuationBasis::OperatorAttestedPrice => {
                all_assets_valued = false;
                block_reasons.push(format!(
                    "OPERATOR_ATTESTED_VALUATION_INSUFFICIENT:{}",
                    asset.currency
                ));
                evaluated_assets.push(AutomationBaselineAssetEntry {
                    currency: asset.currency.clone(),
                    free: asset.free,
                    locked: asset.locked,
                    total,
                    valuation_basis: asset.valuation_basis,
                    mark_price_krw: asset.mark_price_krw,
                    valuation_krw: None,
                    candle_observation_id: None,
                    candle_content_digest: None,
                });
            }
            AutomationAssetValuationBasis::FinalizedCandleClose => {
                let market = MarketId::parse_upbit(&format!("KRW-{}", asset.currency))?;
                if let Some((close_price, obs_id, obs_digest)) =
                    resolve_midnight_candle_observation(
                        transaction,
                        account_identity,
                        &market,
                        day_start,
                    )?
                {
                    if let Some(supplied_mark) = asset.mark_price_krw
                        && supplied_mark != close_price
                    {
                        return Err(LabError::Conflict(format!(
                            "baseline mark_price_krw for {} ({}) does not match finalized midnight candle close ({})",
                            asset.currency,
                            supplied_mark.get(),
                            close_price.get()
                        )));
                    }
                    let val_dec = total_qty.checked_mul(close_price.get()).ok_or_else(|| {
                        LabError::DataCorrupt(
                            "baseline asset valuation exceeds decimal capacity".into(),
                        )
                    })?;
                    let valuation = QuoteAmount::new(val_dec)?;
                    total_equity = total_equity.checked_add(val_dec).ok_or_else(|| {
                        LabError::DataCorrupt(
                            "baseline total equity exceeds decimal capacity".into(),
                        )
                    })?;
                    evaluated_assets.push(AutomationBaselineAssetEntry {
                        currency: asset.currency.clone(),
                        free: asset.free,
                        locked: asset.locked,
                        total,
                        valuation_basis: asset.valuation_basis,
                        mark_price_krw: Some(close_price),
                        valuation_krw: Some(valuation),
                        candle_observation_id: Some(obs_id),
                        candle_content_digest: Some(obs_digest),
                    });
                } else {
                    all_assets_valued = false;
                    block_reasons.push(format!(
                        "MISSING_FINALIZED_MIDNIGHT_CANDLE_MARK:{}",
                        asset.currency
                    ));
                    evaluated_assets.push(AutomationBaselineAssetEntry {
                        currency: asset.currency.clone(),
                        free: asset.free,
                        locked: asset.locked,
                        total,
                        valuation_basis: asset.valuation_basis,
                        mark_price_krw: asset.mark_price_krw,
                        valuation_krw: None,
                        candle_observation_id: None,
                        candle_content_digest: None,
                    });
                }
            }
            AutomationAssetValuationBasis::QuoteCurrencyParity
            | AutomationAssetValuationBasis::ZeroBalance => {
                return Err(LabError::InvalidConfig(format!(
                    "invalid valuation_basis {:?} for non-zero non-KRW asset {}",
                    asset.valuation_basis, asset.currency
                )));
            }
        }
    }
    evaluated_assets.sort_by(|lhs, rhs| lhs.currency.cmp(&rhs.currency));

    let sufficient = block_reasons.is_empty() && all_assets_valued;
    let verification_state = if sufficient {
        AutomationBaselineVerificationState::Verified
    } else {
        AutomationBaselineVerificationState::BlockedInsufficientEvidence
    };
    let total_equity_krw = if sufficient {
        Some(QuoteAmount::new(total_equity)?)
    } else {
        None
    };
    let baseline_id = AutomationBaselineId::from_seed(&format!(
        "{}:{}:{}",
        account_identity,
        input.day_utc,
        request_id.as_str()
    ));
    let mut view = AutomationAccountDayBaselineView {
        baseline_id: baseline_id.clone(),
        account_identity: account_identity.to_owned(),
        mode,
        day_utc: input.day_utc.clone(),
        reference_time_utc,
        recorded_at_utc: now,
        source: input.source,
        verification_state,
        sufficient_for_risk_admission: sufficient,
        total_equity_krw,
        assets: evaluated_assets,
        exchange_evidence,
        block_reasons,
        baseline_digest: ContentHash::of_bytes(b"placeholder"),
        request_id: Some(request_id.clone()),
        operator_note: input.operator_note.clone(),
    };
    let digest = compute_baseline_digest(&view)?;
    view.baseline_digest = digest.clone();
    let record_json = serde_json::to_string(&view).map_err(json_error)?;
    let evidence_json = serde_json::to_string(&view.exchange_evidence).map_err(json_error)?;
    let block_reasons_json = serde_json::to_string(&view.block_reasons).map_err(json_error)?;

    transaction
        .execute(
            "INSERT INTO automation_account_day_baselines(\
                 baseline_id,account_identity,day_utc,mode,reference_time_utc,source,\
                 verification_state,sufficient_for_risk_admission,total_equity_krw,\
                 baseline_balances_digest,baseline_balances_json,valuation_evidence_json,\
                 block_reasons_json,baseline_digest,request_id,record_json,created_at_utc\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
            params![
                baseline_id.as_str(),
                account_identity,
                input.day_utc,
                mode_name(mode),
                reference_time_utc.to_rfc3339(),
                baseline_source_name(input.source),
                baseline_verification_state_name(verification_state),
                sufficient,
                total_equity_krw.map(|q| q.get().to_string()),
                canonical_balances_digest.as_str(),
                canonical_balances_json,
                evidence_json,
                block_reasons_json,
                digest.as_str(),
                request_id.as_str(),
                record_json,
                now.to_rfc3339(),
            ],
        )
        .map_err(sql_error)?;

    if sufficient && !account_day_exists(transaction, account_identity, &input.day_utc)? {
        transaction
            .execute(
                "INSERT INTO automation_account_days(\
                     account_identity,day_utc,baseline_equity_krw,baseline_balances_digest,baseline_balances_json,created_at_utc\
                 ) VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    account_identity,
                    input.day_utc,
                    total_equity.to_string(),
                    canonical_balances_digest.as_str(),
                    canonical_balances_json,
                    reference_time_utc.to_rfc3339(),
                ],
            )
            .map_err(sql_error)?;
    }

    Ok(view)
}

fn verify_baseline_assets_match_snapshot(
    assets: &[crate::contracts::AutomationBaselineAssetInput],
    snapshot: &[UpbitAccountBalance],
) -> Result<(), LabError> {
    let mut input_map = std::collections::BTreeMap::<&str, (Decimal, Decimal)>::new();
    for asset in assets {
        input_map.insert(
            asset.currency.as_str(),
            (asset.free.get(), asset.locked.get()),
        );
    }
    let mut snap_map = std::collections::BTreeMap::<&str, (Decimal, Decimal)>::new();
    for bal in snapshot {
        if snap_map
            .insert(bal.currency.as_str(), (bal.balance, bal.locked))
            .is_some()
        {
            return Err(LabError::DataCorrupt(format!(
                "exchange snapshot contains duplicate currency {}",
                bal.currency
            )));
        }
    }
    for (currency, (snap_free, snap_locked)) in &snap_map {
        let snap_total = snap_free.checked_add(*snap_locked).ok_or_else(|| {
            LabError::DataCorrupt("snapshot balance total exceeds decimal capacity".into())
        })?;
        match input_map.get(currency) {
            Some((in_free, in_locked)) => {
                if in_free != snap_free || in_locked != snap_locked {
                    return Err(LabError::Conflict(format!(
                        "baseline asset {currency} quantities (free={in_free}, locked={in_locked}) diverge from exchange snapshot (free={snap_free}, locked={snap_locked})"
                    )));
                }
            }
            None => {
                if snap_total > Decimal::ZERO {
                    return Err(LabError::Conflict(format!(
                        "baseline input is missing non-zero exchange asset {currency} (total={snap_total})"
                    )));
                }
            }
        }
    }
    for (currency, (in_free, in_locked)) in &input_map {
        let in_total = in_free.checked_add(*in_locked).ok_or_else(|| {
            LabError::DataCorrupt("baseline input balance total exceeds decimal capacity".into())
        })?;
        if !snap_map.contains_key(currency) && in_total > Decimal::ZERO {
            return Err(LabError::Conflict(format!(
                "baseline input declares non-zero asset {currency} (total={in_total}) absent from exchange snapshot"
            )));
        }
    }
    Ok(())
}

fn baseline_source_name(source: AutomationBaselineSource) -> &'static str {
    match source {
        AutomationBaselineSource::OperatorAttested => "OPERATOR_ATTESTED",
        AutomationBaselineSource::ExchangeObservedDayStart => "EXCHANGE_OBSERVED_DAY_START",
        AutomationBaselineSource::PriorDayVerifiedLedgerRollover => {
            "PRIOR_DAY_VERIFIED_LEDGER_ROLLOVER"
        }
        AutomationBaselineSource::DurableMockVenueSeed => "DURABLE_MOCK_VENUE_SEED",
    }
}

fn baseline_verification_state_name(state: AutomationBaselineVerificationState) -> &'static str {
    match state {
        AutomationBaselineVerificationState::Verified => "VERIFIED",
        AutomationBaselineVerificationState::BlockedInsufficientEvidence => {
            "BLOCKED_INSUFFICIENT_EVIDENCE"
        }
    }
}

#[derive(Clone, Copy)]
struct AutomaticMockDayBaseline<'a> {
    account_identity: &'a str,
    day: &'a str,
    day_start: UtcTimestamp,
    baseline_at: UtcTimestamp,
    observed_at: UtcTimestamp,
    received_at: UtcTimestamp,
    snapshot_sequence: u64,
    balances: &'a [UpbitAccountBalance],
    balances_digest: &'a str,
    equity: Decimal,
    had_prior_day_baseline: bool,
}

#[allow(clippy::too_many_lines)] // Automatic mock baseline mirrors verified genesis/rollover state into the typed baseline contract table.
fn insert_automatic_mock_day_baseline(
    transaction: &Transaction<'_>,
    spec: AutomaticMockDayBaseline<'_>,
) -> Result<(), LabError> {
    let source = if spec.had_prior_day_baseline {
        AutomationBaselineSource::PriorDayVerifiedLedgerRollover
    } else {
        AutomationBaselineSource::DurableMockVenueSeed
    };
    let prev_day = (spec.day_start.0 - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let prior_digest: Option<String> = transaction
        .query_row(
            "SELECT baseline_digest FROM automation_account_day_baselines \
             WHERE account_identity=?1 AND day_utc=?2 AND verification_state='VERIFIED'",
            params![spec.account_identity, prev_day],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;

    let mut sorted_balances = spec.balances.to_vec();
    sorted_balances.sort_by(|lhs, rhs| lhs.currency.cmp(&rhs.currency));
    let mut assets = Vec::with_capacity(sorted_balances.len());
    for bal in &sorted_balances {
        let free = AssetQuantity::new(bal.balance)?;
        let locked = AssetQuantity::new(bal.locked)?;
        let total_dec = bal.balance.checked_add(bal.locked).ok_or_else(|| {
            LabError::DataCorrupt("mock baseline asset total exceeds decimal capacity".into())
        })?;
        let total = AssetQuantity::new(total_dec)?;
        if bal.currency == "KRW" {
            assets.push(AutomationBaselineAssetEntry {
                currency: bal.currency.clone(),
                free,
                locked,
                total,
                valuation_basis: AutomationAssetValuationBasis::QuoteCurrencyParity,
                mark_price_krw: Some(crate::contracts::PriceKrw::new(Decimal::ONE)?),
                valuation_krw: Some(QuoteAmount::new(total_dec)?),
                candle_observation_id: None,
                candle_content_digest: None,
            });
        } else if total_dec == Decimal::ZERO {
            assets.push(AutomationBaselineAssetEntry {
                currency: bal.currency.clone(),
                free,
                locked,
                total,
                valuation_basis: AutomationAssetValuationBasis::ZeroBalance,
                mark_price_krw: None,
                valuation_krw: Some(QuoteAmount::new(Decimal::ZERO)?),
                candle_observation_id: None,
                candle_content_digest: None,
            });
        } else {
            let market = MarketId::parse_upbit(&format!("KRW-{}", bal.currency))?;
            let (close_price, obs_id, obs_digest) = resolve_midnight_candle_observation(
                transaction,
                spec.account_identity,
                &market,
                spec.day_start,
            )?
            .ok_or_else(|| {
                LabError::DataCorrupt(
                    "mock day baseline missing finalized midnight candle mark".into(),
                )
            })?;
            let val_dec = total_dec.checked_mul(close_price.get()).ok_or_else(|| {
                LabError::DataCorrupt("mock baseline asset value overflow".into())
            })?;
            assets.push(AutomationBaselineAssetEntry {
                currency: bal.currency.clone(),
                free,
                locked,
                total,
                valuation_basis: AutomationAssetValuationBasis::FinalizedCandleClose,
                mark_price_krw: Some(close_price),
                valuation_krw: Some(QuoteAmount::new(val_dec)?),
                candle_observation_id: Some(obs_id),
                candle_content_digest: Some(obs_digest),
            });
        }
    }

    let baseline_id = AutomationBaselineId::from_seed(&format!(
        "{}:{}:mock-auto:{}",
        spec.account_identity, spec.day, spec.snapshot_sequence
    ));
    let exchange_evidence = Some(AutomationBaselineExchangeEvidence {
        snapshot_sequence: Some(spec.snapshot_sequence),
        snapshot_observed_at_utc: Some(spec.observed_at),
        snapshot_received_at_utc: Some(spec.received_at),
        snapshot_balances_digest: Some(ContentHash::try_from(spec.balances_digest.to_owned())?),
        prior_day_baseline_digest: prior_digest.map(ContentHash::try_from).transpose()?,
    });
    let total_equity_krw = Some(QuoteAmount::new(spec.equity)?);
    let mut view = AutomationAccountDayBaselineView {
        baseline_id: baseline_id.clone(),
        account_identity: spec.account_identity.to_owned(),
        mode: AutomationSessionMode::Mock,
        day_utc: spec.day.to_owned(),
        reference_time_utc: spec.baseline_at,
        recorded_at_utc: spec.received_at,
        source,
        verification_state: AutomationBaselineVerificationState::Verified,
        sufficient_for_risk_admission: true,
        total_equity_krw,
        assets,
        exchange_evidence,
        block_reasons: Vec::new(),
        baseline_digest: ContentHash::of_bytes(b"placeholder"),
        request_id: None,
        operator_note: None,
    };
    let digest = compute_baseline_digest(&view)?;
    view.baseline_digest = digest.clone();
    let record_json = serde_json::to_string(&view).map_err(json_error)?;
    let evidence_json = serde_json::to_string(&view.exchange_evidence).map_err(json_error)?;
    let balances_json = serde_json::to_string(spec.balances).map_err(json_error)?;

    transaction
        .execute(
            "INSERT INTO automation_account_day_baselines(\
                 baseline_id,account_identity,day_utc,mode,reference_time_utc,source,\
                 verification_state,sufficient_for_risk_admission,total_equity_krw,\
                 baseline_balances_digest,baseline_balances_json,valuation_evidence_json,\
                 block_reasons_json,baseline_digest,request_id,record_json,created_at_utc\
             ) VALUES (?1,?2,?3,'MOCK',?4,?5,'VERIFIED',1,?6,?7,?8,?9,'[]',?10,NULL,?11,?12)",
            params![
                baseline_id.as_str(),
                spec.account_identity,
                spec.day,
                spec.baseline_at.to_rfc3339(),
                baseline_source_name(source),
                spec.equity.to_string(),
                spec.balances_digest,
                balances_json,
                evidence_json,
                digest.as_str(),
                record_json,
                spec.received_at.to_rfc3339(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn resolve_midnight_candle_observation(
    transaction: &Transaction<'_>,
    account_identity: &str,
    market: &MarketId,
    day_start: UtcTimestamp,
) -> Result<Option<(crate::contracts::PriceKrw, ObservationId, ContentHash)>, LabError> {
    let session_candle_json = transaction
        .query_row(
            "SELECT c.candle_json FROM automation_candles c \
             JOIN automation_sessions s USING(session_id) \
             WHERE s.account_identity=?1 AND s.market=?2 AND c.close_time_utc=?3 \
             ORDER BY s.created_at_utc DESC LIMIT 1",
            params![account_identity, market.code(), day_start.to_rfc3339()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    let candle_json = match session_candle_json {
        Some(json) => Some(json),
        None => transaction
            .query_row(
                "SELECT observation_json FROM candle_observations \
                 WHERE market=?1 AND completed=1 AND close_time_ms=?2 \
                 ORDER BY id ASC LIMIT 1",
                params![market.code(), day_start.0.timestamp_millis()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?,
    };
    let Some(json) = candle_json else {
        return Ok(None);
    };
    let observation: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
    if !observation.candle.completed
        || observation.candle.market != market.code()
        || observation.candle.close_time_utc != day_start
        || observation.content_digest != observation_digest(&observation.candle)?
        || observation.id != ObservationId::from_seed(observation.content_digest.as_str())
    {
        return Err(LabError::DataCorrupt(
            "midnight mark is not a finalized identity-verified candle".into(),
        ));
    }
    Ok(Some((
        observation.candle.close,
        observation.id,
        observation.content_digest,
    )))
}

fn account_day_baseline_if_trustworthy(
    transaction: &Transaction<'_>,
    account_identity: &str,
    mode: AutomationSessionMode,
    day_start: UtcTimestamp,
    received_at: UtcTimestamp,
    balances: &[UpbitAccountBalance],
) -> Result<Option<(Decimal, UtcTimestamp)>, LabError> {
    if account_has_nonterminal_orders(transaction, account_identity)?
        || account_day_has_recorded_activity(
            transaction,
            account_identity,
            mode,
            day_start,
            received_at,
        )?
    {
        return Ok(None);
    }
    match mode {
        // `GET /v1/accounts` has no provider-owned observation timestamp. A
        // local request made at UTC midnight cannot prove a midnight balance;
        // LIVE loss stays unknown until an authoritative timestamped baseline
        // import/rebaseline contract exists.
        AutomationSessionMode::Live => Ok(None),
        AutomationSessionMode::Mock => {
            let seed_at = transaction
                .query_row(
                    "SELECT created_at_utc FROM automation_mock_venue_seed WHERE singleton=1 AND seed_version=1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_error)?;
            let Some(seed_at) = seed_at else {
                return Ok(None);
            };
            if !mock_account_snapshot_matches(transaction, balances)? {
                return Ok(None);
            }
            let has_prior_day_baseline =
                account_has_any_day_baseline(transaction, account_identity)?;
            let baseline_at = if has_prior_day_baseline {
                day_start
            } else {
                if mock_venue_has_any_trade(transaction)?
                    || !mock_wallet_matches_genesis(transaction)?
                {
                    return Ok(None);
                }
                let seed_at = UtcTimestamp::parse_rfc3339(&seed_at)?;
                seed_at.max(day_start)
            };
            Ok(
                account_equity_at_midnight(transaction, account_identity, balances, day_start)?
                    .map(|equity| (equity, baseline_at)),
            )
        }
    }
}

fn account_day_has_recorded_activity(
    transaction: &Connection,
    account_identity: &str,
    mode: AutomationSessionMode,
    day_start: UtcTimestamp,
    activity_end: UtcTimestamp,
) -> Result<bool, LabError> {
    let start = day_start.to_rfc3339();
    let end = activity_end.to_rfc3339();
    let day_fill: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_fills f \
             JOIN automation_orders o USING(client_identifier) \
             WHERE o.account_identity=?1 AND f.trade_time_utc>=?2 AND f.trade_time_utc<=?3)",
            params![account_identity, start, end],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let day_fee: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_fee_accruals f \
             JOIN automation_orders o USING(client_identifier) \
             WHERE o.account_identity=?1 AND f.observed_at_utc>=?2 AND f.observed_at_utc<=?3)",
            params![account_identity, start, end],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let mock_trade = if mode == AutomationSessionMode::Mock {
        transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM automation_mock_venue_trades \
                 WHERE created_at_utc>=?1 AND created_at_utc<=?2)",
                params![day_start.to_rfc3339(), activity_end.to_rfc3339()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?
    } else {
        false
    };
    Ok(day_fill || day_fee || mock_trade)
}

fn account_has_nonterminal_orders(
    connection: &Connection,
    account_identity: &str,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_orders WHERE account_identity=?1 \
             AND state NOT IN ('FILLED','CANCELLED','REJECTED'))",
            [account_identity],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn account_equity_at_midnight(
    transaction: &Transaction<'_>,
    account_identity: &str,
    balances: &[UpbitAccountBalance],
    day_start: UtcTimestamp,
) -> Result<Option<Decimal>, LabError> {
    let mut equity = Decimal::ZERO;
    for balance in balances {
        let quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
            LabError::DataCorrupt("midnight account total exceeds decimal capacity".into())
        })?;
        if quantity == Decimal::ZERO {
            continue;
        }
        let value = if balance.currency == "KRW" {
            quantity
        } else {
            let market = MarketId::parse_upbit(&format!("KRW-{}", balance.currency))?;
            let Some(mark) =
                account_midnight_market_mark(transaction, account_identity, &market, day_start)?
            else {
                return Ok(None);
            };
            let Some(value) = quantity.checked_mul(mark) else {
                return Ok(None);
            };
            value
        };
        let Some(next_equity) = equity.checked_add(value) else {
            return Ok(None);
        };
        equity = next_equity;
    }
    Ok(Some(equity))
}

fn account_midnight_market_mark(
    transaction: &Transaction<'_>,
    account_identity: &str,
    market: &MarketId,
    day_start: UtcTimestamp,
) -> Result<Option<Decimal>, LabError> {
    let json = transaction
        .query_row(
            "SELECT c.candle_json FROM automation_candles c \
             JOIN automation_sessions s USING(session_id) \
             WHERE s.account_identity=?1 AND s.market=?2 AND c.close_time_utc=?3 \
             ORDER BY s.created_at_utc DESC LIMIT 1",
            params![account_identity, market.code(), day_start.to_rfc3339()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(json) = json else {
        return Ok(None);
    };
    let observation: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
    if !observation.candle.completed
        || observation.candle.market != market.code()
        || observation.candle.close_time_utc != day_start
        || observation.content_digest != observation_digest(&observation.candle)?
    {
        return Err(LabError::DataCorrupt(
            "midnight mark is not a finalized identity-verified candle".into(),
        ));
    }
    Ok(Some(observation.candle.close.get()))
}

fn load_mock_venue_balances(
    transaction: &Transaction<'_>,
) -> Result<Vec<UpbitAccountBalance>, LabError> {
    let mut statement = transaction
        .prepare(
            "SELECT currency,balance,locked,avg_buy_price,avg_buy_price_modified,unit_currency \
             FROM automation_mock_venue_accounts ORDER BY currency",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, bool>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(sql_error)?;
    rows.map(|row| {
        let (currency, balance, locked, avg_buy_price, modified, unit_currency) =
            row.map_err(sql_error)?;
        Ok(UpbitAccountBalance {
            currency,
            balance: parse_decimal(&balance)?,
            locked: parse_decimal(&locked)?,
            avg_buy_price: parse_decimal(&avg_buy_price)?,
            avg_buy_price_modified: modified,
            unit_currency,
        })
    })
    .collect::<Result<Vec<_>, LabError>>()
}

fn mock_account_snapshot_matches(
    transaction: &Transaction<'_>,
    balances: &[UpbitAccountBalance],
) -> Result<bool, LabError> {
    let mut venue_balances = load_mock_venue_balances(transaction)?;
    let mut snapshot_balances = balances.to_vec();
    venue_balances.sort_by(|lhs, rhs| lhs.currency.cmp(&rhs.currency));
    snapshot_balances.sort_by(|lhs, rhs| lhs.currency.cmp(&rhs.currency));
    Ok(ContentHash::of_value(&venue_balances)? == ContentHash::of_value(&snapshot_balances)?)
}

fn mock_venue_has_any_trade(transaction: &Transaction<'_>) -> Result<bool, LabError> {
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM automation_mock_venue_trades)",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn mock_wallet_matches_genesis(transaction: &Transaction<'_>) -> Result<bool, LabError> {
    let mut statement = transaction
        .prepare(
            "SELECT currency,balance,locked,avg_buy_price,avg_buy_price_modified,unit_currency \
             FROM automation_mock_venue_accounts ORDER BY currency",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, bool>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(sql_error)?;
    let actual = rows
        .map(|row| {
            let (currency, balance, locked, average, modified, unit) = row.map_err(sql_error)?;
            Ok(UpbitAccountBalance {
                currency,
                balance: parse_decimal(&balance)?,
                locked: parse_decimal(&locked)?,
                avg_buy_price: parse_decimal(&average)?,
                avg_buy_price_modified: modified,
                unit_currency: unit,
            })
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let expected = [
        ("BTC", Decimal::ZERO),
        ("ETH", Decimal::ZERO),
        ("KRW", Decimal::from(10_000_000)),
        ("XRP", Decimal::ZERO),
    ]
    .into_iter()
    .map(|(currency, amount)| UpbitAccountBalance {
        currency: currency.into(),
        balance: amount,
        locked: Decimal::ZERO,
        avg_buy_price: Decimal::ZERO,
        avg_buy_price_modified: false,
        unit_currency: "KRW".into(),
    })
    .collect::<Vec<_>>();
    Ok(ContentHash::of_value(&actual)? == ContentHash::of_value(&expected)?)
}

#[derive(Debug)]
struct MockAccount {
    balance: Decimal,
    locked: Decimal,
    avg_buy_price: Decimal,
    avg_buy_price_modified: bool,
    unit_currency: String,
}

fn read_mock_account(
    transaction: &Transaction<'_>,
    currency: &str,
) -> Result<MockAccount, LabError> {
    let row = transaction
        .query_row(
            "SELECT balance,locked,avg_buy_price,avg_buy_price_modified,unit_currency \
             FROM automation_mock_venue_accounts WHERE currency=?1",
            [currency],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt(format!("mock account lacks currency {currency}")))?;
    Ok(MockAccount {
        balance: parse_decimal(&row.0)?,
        locked: parse_decimal(&row.1)?,
        avg_buy_price: parse_decimal(&row.2)?,
        avg_buy_price_modified: row.3,
        unit_currency: row.4,
    })
}

fn write_mock_account(
    transaction: &Transaction<'_>,
    currency: &str,
    account: &MockAccount,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    if account.balance < Decimal::ZERO || account.locked < Decimal::ZERO {
        return Err(LabError::AccountingInvariant(
            "mock venue account would become negative".into(),
        ));
    }
    transaction
        .execute(
            "UPDATE automation_mock_venue_accounts SET balance=?1,locked=?2,avg_buy_price=?3, \
             avg_buy_price_modified=?4,unit_currency=?5,updated_at_utc=?6 WHERE currency=?7",
            params![
                account.balance.to_string(),
                account.locked.to_string(),
                account.avg_buy_price.to_string(),
                account.avg_buy_price_modified,
                account.unit_currency,
                now.to_rfc3339(),
                currency,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn adjust_mock_account(
    transaction: &Transaction<'_>,
    currency: &str,
    balance_delta: Decimal,
    locked_delta: Decimal,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let mut account = read_mock_account(transaction, currency)?;
    account.balance = account.balance.checked_add(balance_delta).ok_or_else(|| {
        LabError::AccountingInvariant("mock account balance exceeds decimal capacity".into())
    })?;
    account.locked = account.locked.checked_add(locked_delta).ok_or_else(|| {
        LabError::AccountingInvariant("mock account lock exceeds decimal capacity".into())
    })?;
    write_mock_account(transaction, currency, &account, now)
}

#[allow(clippy::too_many_lines)] // Mock exchange state and its account balances update atomically as one venue fact.
fn mock_prepare_and_apply_order(
    transaction: &Transaction<'_>,
    request: &AutomationOrderRequest,
    decision_mark: crate::contracts::PriceKrw,
    now: UtcTimestamp,
) -> Result<UpbitOrderDetail, LabError> {
    const FEE_RATE: Decimal = Decimal::from_parts(5, 0, 0, false, 4);
    const MIN_NOTIONAL: Decimal = Decimal::from_parts(5_000, 0, 0, false, 0);
    const MAX_NOTIONAL: Decimal = Decimal::from_parts(1_000_000_000, 0, 0, false, 0);
    let mark = decision_mark.get();
    let market = request.market.code();
    let base = request.market.base.code().to_owned();
    let (side, order_type, requested_price, requested_volume, budget) = match &request.order {
        AutomationOrderKind::Limit { volume, price } => (
            request.side,
            "limit",
            Some(price.get()),
            Some(volume.get()),
            None,
        ),
        AutomationOrderKind::MarketBuy { budget } => (
            crate::contracts::AutomationSide::Bid,
            "price",
            Some(budget.get()),
            None,
            Some(budget.get()),
        ),
        AutomationOrderKind::MarketSell { volume } => (
            crate::contracts::AutomationSide::Ask,
            "market",
            None,
            Some(volume.get()),
            None,
        ),
    };
    let side_text = match side {
        crate::contracts::AutomationSide::Bid => "bid",
        crate::contracts::AutomationSide::Ask => "ask",
    };
    let limit_notional = match (requested_price, requested_volume, budget) {
        (Some(price), Some(volume), _) => price.checked_mul(volume),
        (Some(price), None, Some(_)) => Some(price),
        (None, Some(volume), None) => mark.checked_mul(volume),
        _ => None,
    }
    .ok_or_else(|| LabError::AccountingInvariant("mock order notional overflow".into()))?;
    if limit_notional < MIN_NOTIONAL || limit_notional > MAX_NOTIONAL {
        return Err(LabError::InvalidConfig(
            "mock venue order is outside its explicit 5,000..=1,000,000,000 KRW rule".into(),
        ));
    }
    let limit_price = requested_price.filter(|_| order_type == "limit");
    let marketable = match (side, limit_price) {
        (crate::contracts::AutomationSide::Bid, Some(price)) => mark <= price,
        (crate::contracts::AutomationSide::Ask, Some(price)) => mark >= price,
        (_, None) => true,
    };
    let fill_price = mark;
    let mut fill_volume = requested_volume.unwrap_or(Decimal::ZERO);
    if let Some(budget) = budget {
        fill_volume =
            (budget / mark).round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
        if fill_volume <= Decimal::ZERO {
            return Err(LabError::InvalidConfig(
                "mock market-buy budget rounds to zero base volume".into(),
            ));
        }
    }
    if !marketable && requested_volume.is_none() {
        return Err(LabError::Internal(
            "mock market order cannot be non-marketable".into(),
        ));
    }
    let filled = marketable;
    let funds = if filled {
        fill_price
            .checked_mul(fill_volume)
            .ok_or_else(|| LabError::AccountingInvariant("mock fill notional overflow".into()))?
    } else {
        limit_notional
    };
    if filled && (funds < MIN_NOTIONAL || funds > MAX_NOTIONAL) {
        return Err(LabError::InvalidConfig(
            "mock fill is outside its explicit venue notional bounds".into(),
        ));
    }
    let fee_basis = if filled { funds } else { limit_notional };
    let fee = fee_basis
        .checked_mul(FEE_RATE)
        .ok_or_else(|| LabError::AccountingInvariant("mock fee calculation overflow".into()))?;
    let mut locked_amount = Decimal::ZERO;

    if side == crate::contracts::AutomationSide::Bid {
        let mut quote = read_mock_account(transaction, "KRW")?;
        if filled {
            let debit = funds
                .checked_add(fee)
                .ok_or_else(|| LabError::AccountingInvariant("mock buy debit overflow".into()))?;
            if quote.balance < debit {
                return Err(LabError::InvalidConfig(
                    "mock venue has insufficient KRW".into(),
                ));
            }
            quote.balance -= debit;
            let mut asset = read_mock_account(transaction, &base)?;
            let prior_total = asset.balance.checked_add(asset.locked).ok_or_else(|| {
                LabError::AccountingInvariant("mock base balance overflow".into())
            })?;
            let cost_basis = asset
                .avg_buy_price
                .checked_mul(prior_total)
                .and_then(|value| value.checked_add(funds))
                .ok_or_else(|| {
                    LabError::AccountingInvariant("mock average price overflow".into())
                })?;
            let new_total = prior_total
                .checked_add(fill_volume)
                .ok_or_else(|| LabError::AccountingInvariant("mock base total overflow".into()))?;
            asset.balance = asset.balance.checked_add(fill_volume).ok_or_else(|| {
                LabError::AccountingInvariant("mock base free balance overflow".into())
            })?;
            asset.avg_buy_price = cost_basis.checked_div(new_total).ok_or_else(|| {
                LabError::AccountingInvariant("mock average price division failed".into())
            })?;
            asset.avg_buy_price_modified = false;
            write_mock_account(transaction, &base, &asset, now)?;
        } else {
            locked_amount = limit_notional.checked_add(fee).ok_or_else(|| {
                LabError::AccountingInvariant("mock limit reserve overflow".into())
            })?;
            if quote.balance < locked_amount {
                return Err(LabError::InvalidConfig(
                    "mock venue has insufficient KRW".into(),
                ));
            }
            quote.balance -= locked_amount;
            quote.locked = quote.locked.checked_add(locked_amount).ok_or_else(|| {
                LabError::AccountingInvariant("mock KRW locked amount overflow".into())
            })?;
        }
        write_mock_account(transaction, "KRW", &quote, now)?;
    } else {
        let mut asset = read_mock_account(transaction, &base)?;
        if filled {
            if asset.balance < fill_volume {
                return Err(LabError::InvalidConfig(
                    "mock venue has insufficient base asset".into(),
                ));
            }
            asset.balance -= fill_volume;
            if asset.balance + asset.locked == Decimal::ZERO {
                asset.avg_buy_price = Decimal::ZERO;
            }
            write_mock_account(transaction, &base, &asset, now)?;
            let net = funds.checked_sub(fee).ok_or_else(|| {
                LabError::AccountingInvariant("mock sell fee exceeds proceeds".into())
            })?;
            adjust_mock_account(transaction, "KRW", net, Decimal::ZERO, now)?;
        } else {
            if asset.balance < fill_volume {
                return Err(LabError::InvalidConfig(
                    "mock venue has insufficient base asset".into(),
                ));
            }
            asset.balance -= fill_volume;
            asset.locked = asset.locked.checked_add(fill_volume).ok_or_else(|| {
                LabError::AccountingInvariant("mock base locked amount overflow".into())
            })?;
            write_mock_account(transaction, &base, &asset, now)?;
            locked_amount = fill_volume;
        }
    }

    let identifier_digest = ContentHash::of_bytes(request.client_identifier.as_str().as_bytes());
    let uuid = format!("mock-order-{}", &identifier_digest.as_str()[..24]);
    let state = if filled { "done" } else { "wait" };
    let executed = if filled { fill_volume } else { Decimal::ZERO };
    let remaining = if filled {
        Decimal::ZERO
    } else {
        requested_volume.unwrap_or(fill_volume)
    };
    let paid_fee = if filled { fee } else { Decimal::ZERO };
    let (reserved_fee, remaining_fee) = if side_text == "ask" {
        // Upbit locks only base volume for sells; the fee is accrued from
        // proceeds after execution and never appears as a reserved fee.
        (Decimal::ZERO, Decimal::ZERO)
    } else {
        (fee, if filled { Decimal::ZERO } else { fee })
    };
    let trade = if filled {
        let trade_digest = ContentHash::of_bytes(
            format!("mock-trade:{}", request.client_identifier.as_str()).as_bytes(),
        );
        Some(serde_json::json!({
            "market": market,
            "uuid": format!("mock-trade-{}", &trade_digest.as_str()[..24]),
            "price": fill_price.to_string(),
            "volume": fill_volume.to_string(),
            "funds": funds.to_string(),
            "trend": "even",
            "created_at": now.to_rfc3339(),
            "side": side_text
        }))
    } else {
        None
    };
    let detail = UpbitOrderDetail {
        uuid,
        identifier: Some(request.client_identifier.as_str().to_owned()),
        side: side_text.to_owned(),
        ord_type: order_type.to_owned(),
        price: requested_price,
        state: state.to_owned(),
        market,
        created_at: now.to_rfc3339(),
        volume: Some(fill_volume),
        remaining_volume: Some(remaining),
        reserved_fee: Some(reserved_fee),
        remaining_fee: Some(remaining_fee),
        paid_fee: Some(paid_fee),
        locked: Some(locked_amount),
        executed_volume: Some(executed),
        trades_count: Some(u64::from(trade.is_some())),
        trades: trade.into_iter().collect(),
    };
    Ok(detail)
}
