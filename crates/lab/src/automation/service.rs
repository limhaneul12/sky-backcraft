//! JobRuntime-owned strategy automation orchestration.
//!
//! This module consumes the frozen policy and durable SQLite projections. It
//! never uses paper-session execution or the simulated engine account.

use super::JobService;
use crate::automation::{
    AutomationAccountPosition, AutomationCancelPreparation, AutomationCheckpoint,
    AutomationOrderAdmission, AutomationOrderDispatch, AutomationPendingCancelRequest,
    AutomationPendingIntent, AutomationPendingOrder, AutomationRuntimeRecord, VenueAdapter,
    VerifiedMarketRules, validate_submit_ack_identity, verify_market_rules,
};
use crate::contracts::{
    AssetQuantity, AutomationCommandOutcome, AutomationIntentId, AutomationOrderIdentifier,
    AutomationOrderKind, AutomationOrderRequest, AutomationOrderState,
    AutomationRequestReadbackState, AutomationSessionCommandResponse, AutomationSessionMode,
    AutomationSessionRequestReadback, AutomationSessionState, AutomationSessionWrite,
    AutomationSide, CandleInterval, CandleObservation, ContentHash, LabError, PriceKrw,
    QuoteAmount, RequestId, UtcTimestamp, Weight,
};
use crate::strategy::StrategyEvaluation;
use crate::upbit::client::ClientError;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const AUTOMATION_DUE_LIMIT: u32 = 4;
const AUTOMATION_CATCHUP_LIMIT: usize = 32;
const AUTOMATION_MAX_CANDLES: u32 = 199;
const AUTOMATION_RECOVERY_LIMIT: u32 = 100;
const AUTOMATION_RECOVERY_MAX_WORK: usize = 10_000;
const AUTOMATION_RECONCILIATION_REASON: &str =
    "PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK";
const AUTOMATION_LIVE_DISABLED_REASON: &str = "LIVE_TRADING_DISABLED";

fn automation_mode_enabled(
    mode: AutomationSessionMode,
    live_execution_policy: crate::upbit::execution::LiveExecutionPolicy,
) -> bool {
    match mode {
        AutomationSessionMode::Mock => true,
        AutomationSessionMode::Live => live_execution_policy.is_enabled(),
    }
}

fn verify_venue_mode(
    venue: &dyn VenueAdapter,
    mode: AutomationSessionMode,
) -> Result<(), LabError> {
    if venue.is_live() == (mode == AutomationSessionMode::Live) {
        Ok(())
    } else {
        Err(LabError::BlockedEvidence(
            "automation session mode does not match its selected venue adapter".into(),
        ))
    }
}

#[derive(Debug, Clone)]
struct PlannedOrder {
    side: AutomationSide,
    order: AutomationOrderKind,
    requested_notional: QuoteAmount,
    asset_reserve: AssetQuantity,
}

#[derive(Debug)]
struct CurrentDecision {
    intent_id: AutomationIntentId,
    action: &'static str,
}

impl JobService {
    /// Recover only persisted local requests, then let the owned coordinator
    /// reconcile venue identifiers before it can resume any session.
    /// # Errors
    /// Reports storage-owner failures that prevent exact request readback.
    pub(super) async fn recover_pending_automation_requests(&self) -> Result<(), LabError> {
        let mut recovered = 0_usize;
        let mut prior_batch = BTreeSet::new();
        loop {
            let lifecycle = self
                .inner
                .database
                .call("automation_pending_lifecycle_requests", |store| {
                    store.automation_pending_lifecycle_requests(AUTOMATION_RECOVERY_LIMIT)
                })
                .await?;
            if lifecycle.is_empty() {
                break;
            }
            if recovered.saturating_add(lifecycle.len()) > AUTOMATION_RECOVERY_MAX_WORK {
                return Err(LabError::ResourceLimit(
                    "pending automation lifecycle recovery exceeded its bounded startup work"
                        .into(),
                ));
            }
            let batch_ids = lifecycle
                .iter()
                .map(|pending| automation_request_id(&pending.write).clone())
                .collect::<BTreeSet<_>>();
            if batch_ids
                .iter()
                .any(|request_id| prior_batch.contains(request_id))
            {
                return Err(LabError::BlockedEvidence(
                    "automation lifecycle recovery made no progress; runtime startup remains closed"
                        .into(),
                ));
            }
            for pending in lifecycle {
                if self.inner.stop.is_cancelled() {
                    return Err(LabError::Cancelled(
                        "automation lifecycle recovery stopped before admission".into(),
                    ));
                }
                let request_id = automation_request_id(&pending.write).clone();
                let response =
                    Box::pin(self.write_automation_session_with_scope(
                        pending.write,
                        pending.account_identity,
                    ))
                    .await?;
                if matches!(
                    response.receipt.outcome,
                    AutomationCommandOutcome::Denied | AutomationCommandOutcome::Pending
                ) {
                    return Err(LabError::BlockedEvidence(
                        "pending automation lifecycle command did not apply before runtime startup"
                            .into(),
                    ));
                }
                let readback = self.read_automation_request(&request_id).await?;
                if readback.state != AutomationRequestReadbackState::Applied {
                    return Err(LabError::OutcomeUnknown(
                        "automation lifecycle recovery lacks a terminal exact request readback"
                            .into(),
                    ));
                }
                recovered += 1;
            }
            prior_batch = batch_ids;
        }

        self.recover_pending_cancel_requests(true).await
    }

    /// Reconcile claimed cancellation requests through identifier GET only.
    /// Unclaimed requests are recovered during startup, when no live command
    /// can race the command's durable begin/claim sequence.
    async fn recover_pending_cancel_requests(
        &self,
        include_unclaimed: bool,
    ) -> Result<(), LabError> {
        let cancellations = self
            .inner
            .database
            .call("automation_pending_cancel_requests", |store| {
                store.automation_pending_cancel_requests(AUTOMATION_RECOVERY_LIMIT)
            })
            .await?;
        for pending in cancellations {
            if self.inner.stop.is_cancelled() {
                return Ok(());
            }
            if !include_unclaimed && !pending.claimed {
                continue;
            }
            if let Err(error) = self.recover_pending_cancel(pending).await {
                tracing::warn!(
                    event = "automation_cancel_cycle_recovery_pending",
                    error = %error,
                    outcome = "IDENTIFIER_READBACK_REQUIRED"
                );
            }
        }
        Ok(())
    }

    /// Bounded coordinator hook. A failure for one automation session is
    /// isolated to that session; durable unknown effects remain readback-only.
    /// # Errors
    /// Returns only failures to enumerate due sessions from the SQLite owner.
    pub(super) async fn process_due_automation_sessions(
        &self,
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        if let Err(error) = self.recover_pending_cancel_requests(false).await {
            tracing::warn!(
                event = "automation_cancel_cycle_recovery_unavailable",
                error = %error,
                outcome = "CLAIMED_CANCELS_REMAIN_PENDING_FOR_IDENTIFIER_READBACK"
            );
        }
        let cursor = self
            .inner
            .automation_due_cursor
            .lock()
            .map_err(|_| LabError::Internal("automation due-session cursor poisoned".into()))?
            .clone();
        let live_execution_policy = self.live_execution_policy();
        let page = self
            .inner
            .database
            .call("automation_due_sessions", move |store| {
                store.automation_due_sessions_with_policy(
                    now,
                    AUTOMATION_DUE_LIMIT,
                    cursor.as_ref(),
                    live_execution_policy,
                )
            })
            .await?;
        tracing::debug!(
            total_due_sessions = page.total_count,
            returned_due_sessions = page.sessions.len(),
            event = "automation_due_session_page"
        );
        let next_cursor = if page.sessions.len() < AUTOMATION_DUE_LIMIT as usize {
            None
        } else {
            page.next_cursor.clone()
        };
        {
            let mut cursor =
                self.inner.automation_due_cursor.lock().map_err(|_| {
                    LabError::Internal("automation due-session cursor poisoned".into())
                })?;
            cursor.clone_from(&next_cursor);
        }
        for mut record in page.sessions {
            if self.inner.stop.is_cancelled() {
                return Ok(());
            }
            if !automation_mode_enabled(record.view.mode, live_execution_policy) {
                // The disabled LIVE gate is checked before selecting or calling
                // any private adapter read method.
                continue;
            }
            if let Err(error) = self.process_automation_record(&mut record).await {
                if matches!(error, LabError::Cancelled(_)) && self.inner.stop.is_cancelled() {
                    return Ok(());
                }
                tracing::warn!(
                    event = "automation_session_cycle_blocked",
                    session_id = %record.view.session_id,
                    error = %error,
                    new_order_admission = "BLOCKED"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn write_automation_session(
        &self,
        write: AutomationSessionWrite,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        Box::pin(self.write_automation_session_with_scope(write, None)).await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one durable command path keeps request admission, exact readback, local replay and rejection ordered"
    )]
    async fn write_automation_session_with_scope(
        &self,
        write: AutomationSessionWrite,
        persisted_start_scope: Option<String>,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let now = UtcTimestamp::now();
        let request_id = automation_request_id(&write).clone();
        let account_scope = match (&write, persisted_start_scope) {
            (AutomationSessionWrite::Start { .. }, Some(identity)) => Some(identity),
            (AutomationSessionWrite::Start { mode, .. }, None) => {
                Some(self.automation_venue(*mode).account_identity())
            }
            (_, _) => None,
        };
        let begin_write = write.clone();
        let begin_scope = account_scope.clone();
        let begin = self
            .inner
            .database
            .call("automation_begin_request", move |store| {
                store.automation_begin_request(&begin_write, begin_scope.as_deref(), now)
            })
            .await;
        let begin = match begin {
            Ok(readback) => readback,
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let readback = self.read_automation_request(&request_id).await?;
                if let Some(response) = response_from_readback(readback.clone())? {
                    return Ok(replayed_automation_response(response));
                }
                if readback.state == AutomationRequestReadbackState::NotRecorded {
                    return Err(error);
                }
                readback
            }
            Err(error) => return Err(error),
        };
        if let Some(response) = response_from_readback(begin.clone())? {
            return Ok(replayed_automation_response(response));
        }
        if begin.state == AutomationRequestReadbackState::NotRecorded {
            return Err(LabError::OutcomeUnknown(
                "automation request is not durably recorded; no command was applied".into(),
            ));
        }

        if matches!(&write, AutomationSessionWrite::CancelOrder { .. }) {
            return self.cancel_automation_order(write).await;
        }

        let frozen_policy_result = match &write {
            AutomationSessionWrite::Start { policy_ref, .. } => {
                let reference = policy_ref.clone();
                self.inner
                    .database
                    .call("automation_freeze_policy_revision", move |store| {
                        Ok(store
                            .load_policy_revision(&reference)?
                            .map(|revision| revision.snapshot))
                    })
                    .await
            }
            _ => Ok(None),
        };
        let frozen_policy = match frozen_policy_result {
            Ok(policy) => policy,
            Err(error) => {
                self.reject_pending_lifecycle_request(&write, &error)
                    .await?;
                return Err(error);
            }
        };
        let write_for_store = write.clone();
        let replay_scope = account_scope.clone();
        let scope_for_store = account_scope;
        let live_execution_policy = self.live_execution_policy();
        let result = self
            .inner
            .database
            .call("automation_session_write", move |store| {
                store.automation_write_with_policy(
                    &write_for_store,
                    frozen_policy,
                    scope_for_store,
                    now,
                    live_execution_policy,
                )
            })
            .await;
        match result {
            Ok(response) => Ok(response),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let readback = self.read_automation_request(&request_id).await?;
                if let Some(response) = response_from_readback(readback.clone())? {
                    return Ok(replayed_automation_response(response));
                }
                if readback.state == AutomationRequestReadbackState::Pending {
                    // Lifecycle writes have no venue effect. Replay the exact
                    // persisted payload after observing its PENDING readback.
                    let frozen_policy = match &write {
                        AutomationSessionWrite::Start { policy_ref, .. } => {
                            let reference = policy_ref.clone();
                            self.inner
                                .database
                                .call("automation_freeze_policy_revision_replay", move |store| {
                                    Ok(store
                                        .load_policy_revision(&reference)?
                                        .map(|revision| revision.snapshot))
                                })
                                .await
                        }
                        _ => Ok(None),
                    };
                    let frozen_policy = match frozen_policy {
                        Ok(policy) => policy,
                        Err(error) => {
                            self.reject_pending_lifecycle_request(&write, &error)
                                .await?;
                            return Err(error);
                        }
                    };
                    let write_for_store = write.clone();
                    let scope_for_store = replay_scope;
                    let replay = self
                        .inner
                        .database
                        .call("automation_session_write_replay", move |store| {
                            store.automation_write_with_policy(
                                &write_for_store,
                                frozen_policy,
                                scope_for_store,
                                UtcTimestamp::now(),
                                live_execution_policy,
                            )
                        })
                        .await;
                    return match replay {
                        Ok(response) => Ok(response),
                        Err(error @ LabError::OutcomeUnknown(_)) => Err(error),
                        Err(error) => {
                            self.reject_pending_lifecycle_request(&write, &error)
                                .await?;
                            Err(error)
                        }
                    };
                }
                Err(error)
            }
            Err(error) => {
                self.reject_pending_lifecycle_request(&write, &error)
                    .await?;
                Err(error)
            }
        }
    }

    async fn reject_pending_lifecycle_request(
        &self,
        write: &AutomationSessionWrite,
        error: &LabError,
    ) -> Result<(), LabError> {
        let request_id = automation_request_id(write).clone();
        let digest = ContentHash::of_value(write)?;
        let reason = bounded_reason(&error.to_string());
        self.inner
            .database
            .call(
                "automation_reject_unapplied_lifecycle_request",
                move |store| store.automation_reject_request(&request_id, &digest, &reason),
            )
            .await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one cancel workflow keeps durable claim, single DELETE, identifier readback and finalization ordered"
    )]
    async fn cancel_automation_order(
        &self,
        write: AutomationSessionWrite,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        let AutomationSessionWrite::CancelOrder {
            request_id,
            session_id,
            client_identifier,
            ..
        } = &write
        else {
            return Err(LabError::Internal(
                "cancel workflow received a different command".into(),
            ));
        };
        let request_id = request_id.clone();
        let session_id = session_id.clone();
        let client_identifier = client_identifier.clone();
        let request_digest = ContentHash::of_value(&write)?;
        let now = UtcTimestamp::now();
        let write_for_prepare = write.clone();
        let live_execution_policy = self.live_execution_policy();
        let preparation = self
            .inner
            .database
            .call("automation_prepare_cancel", move |store| {
                store.automation_prepare_cancel_with_policy(
                    &write_for_prepare,
                    now,
                    live_execution_policy,
                )
            })
            .await;
        let preparation = match preparation {
            Ok(preparation) => preparation,
            Err(error) => {
                let readback = self.read_automation_request(&request_id).await?;
                if let Some(response) = response_from_readback(readback.clone())? {
                    return Ok(replayed_automation_response(response));
                }
                if readback.state == AutomationRequestReadbackState::Pending
                    && readback.response.is_some()
                {
                    let response = readback.response.ok_or_else(|| {
                        LabError::DataCorrupt("pending cancel readback omitted its response".into())
                    })?;
                    let venue_uuid = response
                        .order
                        .as_ref()
                        .and_then(|order| order.venue_order_uuid.clone());
                    AutomationCancelPreparation {
                        response,
                        venue_uuid,
                        should_send: false,
                    }
                } else if matches!(error, LabError::OutcomeUnknown(_)) {
                    return Err(error);
                } else {
                    let id = request_id.clone();
                    let digest = request_digest.clone();
                    let reason = bounded_reason(&error.to_string());
                    let rejected = self
                        .inner
                        .database
                        .call("automation_reject_unclaimed_cancel", move |store| {
                            store.automation_reject_request(&id, &digest, &reason)
                        })
                        .await;
                    rejected?;
                    return Err(error);
                }
            }
        };
        if preparation.response.receipt.request_id != request_id
            || preparation.response.session.session_id != session_id
            || preparation
                .response
                .order
                .as_ref()
                .is_some_and(|order| order.client_identifier != client_identifier)
        {
            return Err(LabError::DataCorrupt(
                "durable cancel preparation does not match its request and order identity".into(),
            ));
        }
        if preparation.should_send && preparation.response.order.is_none() {
            return Err(LabError::OutcomeUnknown(
                "durable cancel claim omitted its order projection; identifier readback is required".into(),
            ));
        }
        match preparation.response.receipt.outcome {
            AutomationCommandOutcome::Applied
            | AutomationCommandOutcome::Replayed
            | AutomationCommandOutcome::NoOp => return Ok(preparation.response),
            AutomationCommandOutcome::Denied => {
                return Err(LabError::Conflict(
                    preparation
                        .response
                        .receipt
                        .reason
                        .unwrap_or_else(|| "cancel request was denied".into()),
                ));
            }
            AutomationCommandOutcome::Pending => {}
        }
        let mode = preparation.response.session.mode;
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            // A claimed cancellation may already have reached the venue. Keep
            // it pending for identifier readback; never infer a no-op or send
            // again just because the production Live gate is currently off.
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        if venue.account_identity() != preparation.response.session.account_identity {
            return Err(LabError::Conflict(
                "cancel adapter identity differs from the durable session account".into(),
            ));
        }
        if preparation.should_send {
            let Some(uuid) = preparation.venue_uuid.as_deref() else {
                return Err(LabError::OutcomeUnknown(
                    "durable cancel claim has no venue UUID; identifier readback is required"
                        .into(),
                ));
            };
            // The request and order CAS are durable before this first and only
            // DELETE. Any error is resolved by identifier GET below.
            if let Err(error) = venue.cancel_order(uuid).await {
                tracing::warn!(
                    event = "automation_cancel_reply_unknown",
                    client_identifier = %client_identifier,
                    error = %error
                );
            }
        }
        let identifier = client_identifier.clone();
        let detail = venue
            .order_by_identifier(&identifier)
            .await
            .map_err(client_error)?;
        let Some(detail) = detail else {
            return Err(LabError::OutcomeUnknown(format!(
                "cancel request {} has no identifier readback; DELETE will not be repeated",
                request_id.as_str()
            )));
        };
        let request_id = request_id.clone();
        let finalize = self
            .inner
            .database
            .call("automation_finalize_cancel", move |store| {
                store.automation_finalize_cancel(&request_id, &detail, UtcTimestamp::now())
            })
            .await;
        match finalize {
            Ok(response) => Ok(response),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let readback = self.read_automation_request(request_id_ref(&write)).await?;
                if let Some(response) = response_from_readback(readback.clone())? {
                    return Ok(response);
                }
                if let Some(response) = readback.response {
                    return Ok(response);
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    async fn recover_pending_cancel(
        &self,
        pending: AutomationPendingCancelRequest,
    ) -> Result<(), LabError> {
        let AutomationSessionWrite::CancelOrder {
            request_id,
            session_id,
            client_identifier,
            ..
        } = &pending.write
        else {
            return Err(LabError::DataCorrupt(
                "pending cancellation recovery contains another command".into(),
            ));
        };
        let record = self
            .load_runtime_record(session_id, UtcTimestamp::now())
            .await?;
        let mode = record.view.mode;
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            if pending.claimed {
                return Err(LabError::BlockedEvidence(
                    AUTOMATION_LIVE_DISABLED_REASON.into(),
                ));
            }
            // The durable recovery projection proves the DELETE claim never
            // happened, so this request can close locally without venue I/O.
            let id = request_id.clone();
            let request = pending.write.clone();
            let digest = ContentHash::of_value(&request)?;
            let reason = AUTOMATION_LIVE_DISABLED_REASON;
            return self
                .inner
                .database
                .call(
                    "automation_reject_unclaimed_cancel_live_disabled",
                    move |store| store.automation_reject_request(&id, &digest, reason),
                )
                .await;
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        if venue.account_identity() != record.account_identity {
            return Err(LabError::Conflict(
                "cancel recovery adapter identity differs from the durable session account".into(),
            ));
        }
        let detail = venue.order_by_identifier(client_identifier).await;
        match detail {
            Ok(Some(detail)) if pending.claimed => {
                let id = request_id.clone();
                let result = self
                    .inner
                    .database
                    .call("automation_recover_cancel_readback", move |store| {
                        store.automation_finalize_cancel(&id, &detail, UtcTimestamp::now())
                    })
                    .await;
                result.map(|_| ())
            }
            Ok(Some(detail)) => {
                // A pre-claim request is proven never to have crossed DELETE.
                // Read its current order fact, then close the stale request so
                // a new explicit sequence can authorize any later cancel.
                let identifier = client_identifier.clone();
                let _readback = self
                    .inner
                    .database
                    .call("automation_recover_unclaimed_cancel_order", move |store| {
                        store.automation_reconcile_order(&detail, UtcTimestamp::now())?;
                        store.automation_order_readback(&identifier)
                    })
                    .await?;
                let id = request_id.clone();
                let request = pending.write.clone();
                let digest = ContentHash::of_value(&request)?;
                let reason = "cancel request was not claimed before restart; order was read back and no DELETE was repeated";
                self.inner
                    .database
                    .call(
                        "automation_reject_unclaimed_cancel_recovery",
                        move |store| store.automation_reject_request(&id, &digest, reason),
                    )
                    .await
            }
            Ok(None) if pending.claimed => Err(LabError::OutcomeUnknown(format!(
                "claimed cancel {} remains pending after identifier 404; DELETE will not be repeated",
                request_id.as_str()
            ))),
            Ok(None) => {
                let id = request_id.clone();
                let request = pending.write.clone();
                let digest = ContentHash::of_value(&request)?;
                let reason = "unclaimed cancel request had no venue identifier readback at restart; no DELETE was sent";
                self.inner
                    .database
                    .call("automation_reject_unclaimed_cancel_404", move |store| {
                        store.automation_reject_request(&id, &digest, reason)
                    })
                    .await
            }
            Err(error) => Err(client_error(error)),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one bounded session cycle orders account/order recovery before causal decisions and admissions"
    )]
    async fn process_automation_record(
        &self,
        record: &mut AutomationRuntimeRecord,
    ) -> Result<(), LabError> {
        let mode = record.view.mode;
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Ok(());
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        let expected_identity = venue.account_identity();
        if record.account_identity != expected_identity
            || record.view.account_identity != expected_identity
        {
            return Err(LabError::Conflict(
                "automation session is scoped to a different account identity".into(),
            ));
        }
        let pending_orders = self.load_pending_orders(&record.view.session_id).await?;
        let (has_unresolved, dispatched) =
            self.process_pending_orders(record, &pending_orders).await?;
        if dispatched || has_unresolved {
            return Ok(());
        }

        let user_controlled_state = matches!(
            record.view.state,
            AutomationSessionState::Paused | AutomationSessionState::Stopped
        );
        let order_readback_reason =
            record.view.recovery_reason.as_deref() == Some("ORDER_READBACK_REQUIRED");
        let user_controlled_restart_reason = user_controlled_state
            && record.view.recovery_reason.as_deref() == Some(AUTOMATION_RECONCILIATION_REASON);
        let can_complete_order_reconciliation = (order_readback_reason
            && matches!(
                record.view.state,
                AutomationSessionState::NeedsReconciliation
                    | AutomationSessionState::Paused
                    | AutomationSessionState::Stopped
            ))
            || user_controlled_restart_reason;
        if can_complete_order_reconciliation {
            self.refresh_account(mode, &record.account_identity).await?;
            let mark = last_checkpoint_mark(record)?;
            let now = UtcTimestamp::now();
            let position = self
                .read_account_position(&record.view.session_id, mark, now)
                .await?;
            validate_account_position_freshness(
                &position,
                now,
                record.view.risk.max_balance_age_seconds,
            )?;
            match self
                .complete_order_reconciliation(&record.view.session_id, record.view.sequence, now)
                .await
            {
                Ok(view) => record.view = view,
                Err(LabError::BlockedEvidence(_) | LabError::Conflict(_)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }

        if record.view.state == AutomationSessionState::NeedsReconciliation {
            if record.view.recovery_reason.as_deref() != Some(AUTOMATION_RECONCILIATION_REASON) {
                return Ok(());
            }
            self.refresh_account(mode, &record.account_identity).await?;
            if let Some(last) = &record.view.last_finalized_candle_at {
                let mark = last_checkpoint_mark(record)?;
                let now = UtcTimestamp::now();
                let position = self
                    .read_account_position(&record.view.session_id, mark, now)
                    .await?;
                validate_account_position_freshness(
                    &position,
                    now,
                    record.view.risk.max_balance_age_seconds,
                )?;
                let feed_age = age_seconds(now, *last)?;
                if feed_age <= i64::from(record.view.risk.max_feed_age_seconds) {
                    match self
                        .complete_restart_reconciliation(&record.view.session_id, now)
                        .await
                    {
                        Ok(view) => record.view = view,
                        Err(LabError::BlockedEvidence(_) | LabError::Conflict(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            if record.view.state == AutomationSessionState::NeedsReconciliation {
                self.process_next_decision_batch(record, true).await?;
                let refreshed = self
                    .load_runtime_record(&record.view.session_id, UtcTimestamp::now())
                    .await?;
                if refreshed.view.state == AutomationSessionState::NeedsReconciliation
                    && refreshed.view.recovery_reason.as_deref()
                        == Some(AUTOMATION_RECONCILIATION_REASON)
                    && refreshed
                        .view
                        .last_finalized_candle_at
                        .is_some_and(|close| {
                            age_seconds(UtcTimestamp::now(), close).is_ok_and(|age| {
                                age <= i64::from(refreshed.view.risk.max_feed_age_seconds)
                            })
                        })
                {
                    self.refresh_account(mode, &refreshed.account_identity)
                        .await?;
                    let mark = last_checkpoint_mark(&refreshed)?;
                    let now = UtcTimestamp::now();
                    let position = self
                        .read_account_position(&refreshed.view.session_id, mark, now)
                        .await?;
                    validate_account_position_freshness(
                        &position,
                        now,
                        refreshed.view.risk.max_balance_age_seconds,
                    )?;
                    if let Ok(view) = self
                        .complete_restart_reconciliation(&refreshed.view.session_id, now)
                        .await
                    {
                        record.view = view;
                    } else {
                        record.view = refreshed.view;
                        return Ok(());
                    }
                } else {
                    record.view = refreshed.view;
                    return Ok(());
                }
            }
            if record.view.state != AutomationSessionState::Running {
                return Ok(());
            }
            *record = self
                .load_runtime_record(&record.view.session_id, UtcTimestamp::now())
                .await?;
        }

        if !matches!(record.view.state, AutomationSessionState::Running) {
            return Ok(());
        }
        if self.process_pending_intents(record).await? {
            return Ok(());
        }
        self.process_next_decision_batch(record, false).await?;
        // The atomic decision write is the durable source for recovery. Read it
        // back before converting a BUY/SELL intent into a reservation.
        if record.view.state == AutomationSessionState::Running {
            let refreshed = self
                .load_runtime_record(&record.view.session_id, UtcTimestamp::now())
                .await?;
            *record = refreshed;
            let _processed = self.process_pending_intents(record).await?;
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one bounded evaluator pass preserves finalized-candle causality through catch-up and durable intent"
    )]
    async fn process_next_decision_batch(
        &self,
        record: &mut AutomationRuntimeRecord,
        restart_replay: bool,
    ) -> Result<(), LabError> {
        let decision_guard = if record.view.mode == AutomationSessionMode::Live {
            self.inner
                .automation_live_venue
                .live_order_send_gate()
                .map(|gate| {
                    let generation = gate.generation();
                    (gate, generation)
                })
        } else {
            None
        };
        let mut checkpoint = restore_checkpoint(record)?;
        verify_checkpoint_cursor(&checkpoint, &record.view)?;
        let now = UtcTimestamp::now();
        let count = decision_fetch_count(record, &checkpoint, now)?;
        if count == 0 {
            return Ok(());
        }
        let observations = self
            .poll_observations(&record.view.market, record.view.decision_interval, count)
            .await?;
        let candidates = observations
            .into_iter()
            .filter(|observation| {
                record
                    .view
                    .last_finalized_candle_at
                    .is_none_or(|cursor| observation.candle.close_time_utc > cursor)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }

        let source_bars = self
            .load_cross_interval_bars(&checkpoint, &record.view.market, UtcTimestamp::now())
            .await?;
        let batch_end = candidates.len().min(AUTOMATION_CATCHUP_LIMIT);
        let mut latest_decision = None;
        for index in 0..batch_end {
            if self.inner.stop.is_cancelled() {
                return Err(LabError::Cancelled(
                    "automation candle processing stopped".into(),
                ));
            }
            let observation = candidates[index].clone();
            let is_last_available = index + 1 == candidates.len();
            let is_current_eligible = is_last_available
                && age_seconds(UtcTimestamp::now(), observation.candle.close_time_utc)?
                    <= i64::from(record.view.risk.max_feed_age_seconds);
            if let Some(cursor) = record.view.last_finalized_candle_at
                && observation.candle.open_time_utc != cursor
            {
                self.mark_candle_gap(record, &checkpoint, &observation)
                    .await?;
                return Ok(());
            }
            if record.view.last_finalized_candle_at.is_none()
                && observation.candle.close_time_utc > record.view.created_at_utc
            {
                let expected =
                    first_close_after(record.view.created_at_utc, record.view.decision_interval)?;
                if observation.candle.close_time_utc != expected {
                    self.mark_first_candle_gap(record, &checkpoint, &observation)
                        .await?;
                    return Ok(());
                }
            }

            feed_causal_sources(
                &mut checkpoint,
                &source_bars,
                observation.candle.close_time_utc,
            )?;
            let pre_start = observation.candle.close_time_utc <= record.view.created_at_utc;
            if pre_start || !is_current_eligible {
                let cash = cash_position()?;
                let evaluation = checkpoint.observe(&observation, cash)?;
                checkpoint.last_observation = Some(observation.clone());
                checkpoint.episode_opened_at = None;
                checkpoint.held_decision_bars = 0;
                if pre_start {
                    self.persist_warmup_candle(record, &observation, &checkpoint, restart_replay)
                        .await?;
                } else {
                    let reason =
                        if age_seconds(UtcTimestamp::now(), observation.candle.close_time_utc)?
                            > i64::from(record.view.risk.max_feed_age_seconds)
                        {
                            "FEED_STALE"
                        } else {
                            "SUPERSEDED_BY_CURRENT_CAUSAL_CANDLE"
                        };
                    self.persist_catchup_decision(
                        record,
                        &observation,
                        &checkpoint,
                        evaluation.as_ref(),
                        reason,
                    )
                    .await?;
                }
                record.view.last_finalized_candle_at = Some(observation.candle.close_time_utc);
                continue;
            }

            self.refresh_account(record.view.mode, &record.account_identity)
                .await?;
            let received_at = UtcTimestamp::now();
            let position = self
                .read_account_position(
                    &record.view.session_id,
                    observation.candle.close,
                    received_at,
                )
                .await?;
            validate_account_position_freshness(
                &position,
                received_at,
                record.view.risk.max_balance_age_seconds,
            )?;
            let position_view = advance_position_checkpoint(
                &mut checkpoint,
                &position,
                observation.candle.close_time_utc,
                observation.candle.interval,
            )?;
            let evaluation = checkpoint.observe(&observation, position_view)?;
            checkpoint.last_observation = Some(observation.clone());
            if let Some(evaluation) = evaluation {
                if evaluation.decision_time != observation.candle.close_time_utc {
                    return Err(LabError::DataCorrupt(
                        "strategy evaluation time differs from its finalized source candle".into(),
                    ));
                }
                let action =
                    action_for_target(evaluation.constrained_target_weight, position.actual_weight);
                let evaluation_json = evaluation_to_value(&evaluation);
                let intent_id = AutomationIntentId::from_seed(&format!(
                    "{}:{}",
                    record.view.session_id.as_str(),
                    observation.id.as_str()
                ));
                self.record_decision(
                    &record.view.session_id,
                    &observation,
                    &checkpoint,
                    &intent_id,
                    action,
                    &evaluation_json,
                    received_at,
                    restart_replay,
                    decision_guard.clone(),
                )
                .await?;
                record.view.last_finalized_candle_at = Some(observation.candle.close_time_utc);
                latest_decision = Some(CurrentDecision { intent_id, action });
            } else {
                checkpoint.last_observation = Some(observation.clone());
                let warmup = serde_json::json!({
                    "kind": "POLICY_WARMUP",
                    "completed_bars_seen": checkpoint.evaluator.completed_bars_seen(),
                    "decision_time": observation.candle.close_time_utc,
                });
                if restart_replay {
                    let intent_id = AutomationIntentId::from_seed(&format!(
                        "{}:{}",
                        record.view.session_id.as_str(),
                        observation.id.as_str()
                    ));
                    self.record_decision(
                        &record.view.session_id,
                        &observation,
                        &checkpoint,
                        &intent_id,
                        "HOLD",
                        &warmup,
                        received_at,
                        true,
                        decision_guard.clone(),
                    )
                    .await?;
                } else {
                    self.persist_warmup_candle(record, &observation, &checkpoint, false)
                        .await?;
                }
                record.view.last_finalized_candle_at = Some(observation.candle.close_time_utc);
            }
        }
        if restart_replay {
            let refreshed = self
                .load_runtime_record(&record.view.session_id, UtcTimestamp::now())
                .await?;
            *record = refreshed;
            return Ok(());
        }
        if let Some(decision) = latest_decision
            && decision.action != "HOLD"
        {
            let pending = self.load_pending_intents(&record.view.session_id).await?;
            if pending
                .iter()
                .any(|intent| intent.intent_id == decision.intent_id)
            {
                self.admit_pending_intent(record, &pending[0]).await?;
            }
        }
        Ok(())
    }

    async fn process_pending_intents(
        &self,
        record: &AutomationRuntimeRecord,
    ) -> Result<bool, LabError> {
        let pending = self.load_pending_intents(&record.view.session_id).await?;
        if pending.is_empty() {
            return Ok(false);
        }
        let now = UtcTimestamp::now();
        let Some(intent) = pending.into_iter().next() else {
            return Ok(false);
        };
        if intent.session_id != record.view.session_id
            || intent.observation_id != intent.observation.id
        {
            self.reject_intent(
                &intent.intent_id,
                "PENDING_INTENT_IDENTITY_MISMATCH",
                UtcTimestamp::now(),
            )
            .await?;
            return Ok(true);
        }
        let stale = age_seconds(now, intent.decision_time)?
            > i64::from(record.view.risk.max_feed_age_seconds);
        let current = record.view.last_finalized_candle_at == Some(intent.decision_time);
        if stale || !current {
            self.reject_intent(
                &intent.intent_id,
                if stale {
                    "FEED_STALE"
                } else {
                    "SUPERSEDED_BY_CURRENT_CAUSAL_CANDLE"
                },
                UtcTimestamp::now(),
            )
            .await?;
            return Ok(true);
        }
        if record.view.state == AutomationSessionState::NeedsReconciliation {
            // A process-restart claim is restored only after the fresh
            // account/position and order readback CAS succeeds.
            return Ok(true);
        }
        if record.view.state != AutomationSessionState::Running {
            return Ok(true);
        }
        self.admit_pending_intent(record, &intent).await?;
        Ok(true)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one admission path keeps signal freshness, account authority, market rules and reservation ordered"
    )]
    async fn admit_pending_intent(
        &self,
        record: &AutomationRuntimeRecord,
        intent: &AutomationPendingIntent,
    ) -> Result<(), LabError> {
        let mode = record.view.mode;
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return self
                .reject_intent(
                    &intent.intent_id,
                    AUTOMATION_LIVE_DISABLED_REASON,
                    UtcTimestamp::now(),
                )
                .await;
        }
        let admission_gate = (mode == AutomationSessionMode::Live)
            .then(|| self.inner.automation_live_venue.live_order_send_gate())
            .flatten();
        let admission_generation = admission_gate
            .as_ref()
            .map(crate::upbit::live_order_control::LiveOrderSendGate::generation);
        if admission_gate
            .as_ref()
            .is_some_and(|gate| !gate.is_enabled())
        {
            return self
                .reject_intent(
                    &intent.intent_id,
                    "LIVE_ORDER_SENDING_DISABLED",
                    UtcTimestamp::now(),
                )
                .await;
        }
        if intent.action != "BUY" && intent.action != "SELL" {
            return self
                .reject_intent(
                    &intent.intent_id,
                    "INVALID_PENDING_ACTION",
                    UtcTimestamp::now(),
                )
                .await;
        }
        if intent.decision_time != intent.observation.candle.close_time_utc
            || intent.decision_mark != intent.observation.candle.close
        {
            return self
                .reject_intent(
                    &intent.intent_id,
                    "PENDING_INTENT_SOURCE_MISMATCH",
                    UtcTimestamp::now(),
                )
                .await;
        }
        let evaluation_time = serde_json::from_value::<UtcTimestamp>(
            intent
                .evaluation_json
                .get("decision_time_utc")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let target_weight = serde_json::from_value::<Weight>(
            intent
                .evaluation_json
                .get("constrained_target_weight")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let (evaluation_time, target_weight) = match (evaluation_time, target_weight) {
            (Ok(time), Ok(weight)) => (time, weight),
            (Err(error), _) | (_, Err(error)) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &format!("PENDING_EVALUATION_CORRUPT: {error}"),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        if evaluation_time != intent.decision_time {
            return self
                .reject_intent(
                    &intent.intent_id,
                    "PENDING_EVALUATION_TIME_MISMATCH",
                    UtcTimestamp::now(),
                )
                .await;
        }
        let now = UtcTimestamp::now();
        if age_seconds(now, intent.decision_time)?
            > i64::from(record.view.risk.max_feed_age_seconds)
        {
            return self
                .reject_intent(&intent.intent_id, "FEED_STALE", now)
                .await;
        }
        self.refresh_account(mode, &record.account_identity).await?;
        let position = self
            .read_account_position(
                &record.view.session_id,
                intent.decision_mark,
                UtcTimestamp::now(),
            )
            .await;
        let position = match position {
            Ok(position) => position,
            Err(error) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &bounded_reason(&error.to_string()),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        if let Err(error) = validate_account_position_freshness(
            &position,
            UtcTimestamp::now(),
            record.view.risk.max_balance_age_seconds,
        ) {
            return self
                .reject_intent(
                    &intent.intent_id,
                    &bounded_reason(&error.to_string()),
                    UtcTimestamp::now(),
                )
                .await;
        }
        let plan = match plan_order(target_weight, &position, intent.decision_mark) {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        "POSITION_ALREADY_AT_FROZEN_TARGET",
                        UtcTimestamp::now(),
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &bounded_reason(&error.to_string()),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        if (intent.action == "BUY" && plan.side != AutomationSide::Bid)
            || (intent.action == "SELL" && plan.side != AutomationSide::Ask)
        {
            return self
                .reject_intent(
                    &intent.intent_id,
                    "ACCOUNT_POSITION_CHANGED_AFTER_SIGNAL",
                    UtcTimestamp::now(),
                )
                .await;
        }
        let venue = self.automation_venue(mode);
        if let Err(error) = verify_venue_mode(venue, mode) {
            return self
                .reject_intent(
                    &intent.intent_id,
                    &bounded_reason(&error.to_string()),
                    UtcTimestamp::now(),
                )
                .await;
        }
        let chance = match venue.order_chance(&record.view.market).await {
            Ok(chance) => chance,
            Err(error) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &bounded_reason(&error.to_string()),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        let rules_observed_at = UtcTimestamp::now();
        let rules = match verify_market_rules(&record.view.market, &chance, mode, rules_observed_at)
        {
            Ok(rules) => rules,
            Err(error) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &bounded_reason(&error.to_string()),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        let admission = match make_admission(
            record,
            &intent.intent_id,
            intent.action.as_str(),
            plan,
            &rules,
            &intent.evaluation_json,
            intent.decision_time,
            intent.decision_mark,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                return self
                    .reject_intent(
                        &intent.intent_id,
                        &bounded_reason(&error.to_string()),
                        UtcTimestamp::now(),
                    )
                    .await;
            }
        };
        let identifier = admission.request.client_identifier.clone();
        let now = UtcTimestamp::now();
        let live_execution_policy = self.live_execution_policy();
        let result = self
            .inner
            .database
            .call("automation_admit_order", move |store| {
                // This synchronous owner check orders the old decision against
                // OFF's durable sweep. No await separates it from reservation.
                if admission_gate
                    .as_ref()
                    .zip(admission_generation)
                    .is_some_and(|(gate, generation)| {
                        !gate.is_enabled() || gate.generation() != generation
                    })
                {
                    return Err(LabError::BlockedEvidence(
                        "LIVE_ORDER_SEND_GENERATION_CHANGED".into(),
                    ));
                }
                store.automation_admit_order_with_policy(admission, now, live_execution_policy)
            })
            .await;
        match result {
            Ok(_order) => {
                self.dispatch_reserved_by_identifier(&identifier, mode, &record.view.session_id)
                    .await
            }
            Err(error) => {
                // Admission has no venue effect. Exact identifier readback
                // resolves a lost SQLite reply before any send is considered.
                let readback = self.read_order_view(&identifier).await?;
                if let Some(order) = readback {
                    if order.state == AutomationOrderState::Reserved
                        && record.view.state == AutomationSessionState::Running
                    {
                        self.dispatch_reserved_by_identifier(
                            &identifier,
                            mode,
                            &record.view.session_id,
                        )
                        .await?;
                    }
                    return Ok(());
                }
                if matches!(error, LabError::OutcomeUnknown(_)) {
                    return Err(error);
                }
                self.reject_intent(
                    &intent.intent_id,
                    &bounded_reason(&error.to_string()),
                    UtcTimestamp::now(),
                )
                .await
            }
        }
    }

    async fn process_pending_orders(
        &self,
        record: &AutomationRuntimeRecord,
        orders: &[AutomationPendingOrder],
    ) -> Result<(bool, bool), LabError> {
        let mut unresolved = false;
        for order in orders {
            if order.session_id != record.view.session_id
                || order.account_identity != record.account_identity
                || order.decision_mark.get() <= Decimal::ZERO
            {
                self.mark_order_needs_reconciliation(
                    &order.request.client_identifier,
                    "PENDING_ORDER_SCOPE_OR_MARK_MISMATCH",
                )
                .await?;
                unresolved = true;
                continue;
            }
            if matches!(order.state, AutomationOrderState::Reserved) {
                continue;
            }
            if matches!(
                order.state,
                AutomationOrderState::SubmittedUnknown
                    | AutomationOrderState::Open
                    | AutomationOrderState::PartiallyFilled
                    | AutomationOrderState::NeedsReconciliation
            ) {
                let view = self.readback_pending_order(order, record.view.mode).await?;
                if view.as_ref().is_none_or(|view| {
                    !matches!(
                        view.state,
                        AutomationOrderState::Filled
                            | AutomationOrderState::Cancelled
                            | AutomationOrderState::Rejected
                    )
                }) {
                    unresolved = true;
                }
            }
        }
        if unresolved || record.view.state != AutomationSessionState::Running {
            return Ok((unresolved, false));
        }
        if let Some(order) = orders
            .iter()
            .find(|order| order.state == AutomationOrderState::Reserved)
        {
            self.dispatch_reserved_order(order, record.view.mode)
                .await?;
            return Ok((false, true));
        }
        Ok((false, false))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the send-claim path keeps exact readback, RESERVED-only release, and unknown-effect handling ordered"
    )]
    async fn dispatch_reserved_order(
        &self,
        order: &AutomationPendingOrder,
        mode: AutomationSessionMode,
    ) -> Result<(), LabError> {
        if order.state != AutomationOrderState::Reserved {
            return Err(LabError::Conflict(
                "only a durable RESERVED order may enter the send-claim path".into(),
            ));
        }
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let send_generation = if mode == AutomationSessionMode::Live {
            self.inner
                .automation_live_venue
                .live_order_send_gate()
                .map(|gate| gate.generation())
        } else {
            None
        };
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        if venue.account_identity() != order.account_identity {
            return Err(LabError::Conflict(
                "reserved order is scoped to a different venue account".into(),
            ));
        }
        let chance = venue
            .order_chance(&order.request.market)
            .await
            .map_err(client_error)?;
        let rules = verify_market_rules(&order.request.market, &chance, mode, UtcTimestamp::now())?;
        let identifier = order.request.client_identifier.clone();
        let now = UtcTimestamp::now();
        let risk_guard = self.inner.risk_guard.clone();
        let live_execution_policy = self.live_execution_policy();
        let claimed = self
            .inner
            .database
            .call("automation_claim_order_for_send", move |store| {
                if mode == AutomationSessionMode::Mock {
                    risk_guard.with_mock_order_admission(|| {
                        store.automation_claim_order_for_send_with_policy(
                            &identifier,
                            &rules,
                            now,
                            live_execution_policy,
                        )
                    })
                } else {
                    risk_guard.with_order_admission(|| {
                        store.automation_claim_order_for_send_with_policy(
                            &identifier,
                            &rules,
                            now,
                            live_execution_policy,
                        )
                    })
                }
            })
            .await;
        let dispatch = match claimed {
            Ok(dispatch) => dispatch,
            Err(error) => {
                let current = self
                    .read_order_view(&order.request.client_identifier)
                    .await?;
                let Some(current) = current else {
                    return Err(error);
                };
                if current.state == AutomationOrderState::Reserved {
                    // The exact readback proves no send claim committed. A
                    // terminal rejection releases only this still-RESERVED
                    // order; if the claim raced, its CAS refuses and we
                    // continue with identifier-only reconciliation.
                    match self
                        .reject_reserved_order(
                            &order.request.client_identifier,
                            &bounded_reason(&error.to_string()),
                            UtcTimestamp::now(),
                        )
                        .await
                    {
                        Ok(view) if view.state == AutomationOrderState::Rejected => {
                            return Err(error);
                        }
                        Ok(_) => return Err(error),
                        Err(LabError::Conflict(_) | LabError::OutcomeUnknown(_)) => {
                            let latest = self
                                .read_order_view(&order.request.client_identifier)
                                .await?;
                            if latest.is_some_and(|view| {
                                matches!(
                                    view.state,
                                    AutomationOrderState::Filled
                                        | AutomationOrderState::Cancelled
                                        | AutomationOrderState::Rejected
                                )
                            }) {
                                return Ok(());
                            }
                            if let Some(latest) = self
                                .load_pending_orders(&order.session_id)
                                .await?
                                .into_iter()
                                .find(|candidate| {
                                    candidate.request.client_identifier
                                        == order.request.client_identifier
                                })
                            {
                                let _ = self.readback_pending_order(&latest, mode).await?;
                                return Ok(());
                            }
                            return Err(error);
                        }
                        Err(rejection_error) => return Err(rejection_error),
                    }
                }
                if matches!(
                    current.state,
                    AutomationOrderState::Filled
                        | AutomationOrderState::Cancelled
                        | AutomationOrderState::Rejected
                ) {
                    return Ok(());
                }
                if let Some(latest) = self
                    .load_pending_orders(&order.session_id)
                    .await?
                    .into_iter()
                    .find(|candidate| {
                        candidate.request.client_identifier == order.request.client_identifier
                    })
                {
                    let _ = self.readback_pending_order(&latest, mode).await?;
                    return Ok(());
                }
                return Err(error);
            }
        };
        self.submit_claimed_order(order, mode, dispatch, send_generation)
            .await
    }

    async fn submit_claimed_order(
        &self,
        order: &AutomationPendingOrder,
        mode: AutomationSessionMode,
        dispatch: AutomationOrderDispatch,
        send_generation: Option<u64>,
    ) -> Result<(), LabError> {
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        if venue.account_identity() != order.account_identity {
            return Err(LabError::Conflict(
                "claimed order is scoped to a different venue account".into(),
            ));
        }
        let effect_authorization = if mode == AutomationSessionMode::Mock {
            self.inner.risk_guard.with_mock_order_admission(|| Ok(()))
        } else {
            self.inner.risk_guard.authorize_order_effect()
        };
        if let Err(error) = effect_authorization {
            tracing::warn!(
                event = "automation_order_effect_blocked_after_claim",
                client_identifier = %dispatch.request.client_identifier,
                error = %error,
                disposition = "KEEP_SUBMITTED_UNKNOWN_AND_READ_BACK_IDENTIFIER_ONLY"
            );
            return Ok(());
        }
        match venue
            .submit_order(&dispatch.request, dispatch.decision_mark, send_generation)
            .await
        {
            Ok(acknowledgement) => {
                if let Err(error) =
                    validate_submit_ack_identity(&dispatch.request, &acknowledgement)
                {
                    tracing::warn!(
                        event = "automation_submit_ack_identity_mismatch",
                        client_identifier = %order.request.client_identifier,
                        error = %error,
                        disposition = "DISCARD_ACK_AND_READ_BACK_EXACT_DURABLE_IDENTIFIER"
                    );
                }
                // POST responses can omit trade details or nullable market-buy
                // fields. Only the exact identifier GET is complete enough to
                // reconcile; the durable claim remains UNKNOWN until then.
                self.readback_pending_order(order, mode).await.map(|_| ())
            }
            Err(ClientError::LiveOrderControlDenied) => {
                let identifier = dispatch.request.client_identifier.clone();
                let now = UtcTimestamp::now();
                self.inner
                    .database
                    .call(
                        "automation_reject_order_before_runtime_send",
                        move |store| {
                            store.automation_reject_claimed_order_before_send(&identifier, now)
                        },
                    )
                    .await?;
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    event = "automation_submit_reply_unknown",
                    client_identifier = %order.request.client_identifier,
                    error = %error,
                    disposition = "PERSIST_SUBMITTED_UNKNOWN_AND_READ_BACK_NEXT_COORDINATOR_CYCLE"
                );
                // The durable claim already advanced the order to
                // SUBMITTED_UNKNOWN. Leave it held for the next coordinator
                // pass (or process-restart recovery), which queries this exact
                // identifier before any later admission. No submit retry.
                Ok(())
            }
        }
    }

    async fn readback_pending_order(
        &self,
        order: &AutomationPendingOrder,
        mode: AutomationSessionMode,
    ) -> Result<Option<crate::contracts::AutomationOrderView>, LabError> {
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        if venue.account_identity() != order.account_identity {
            return Err(LabError::Conflict(
                "pending order is scoped to a different venue account".into(),
            ));
        }
        match venue
            .order_by_identifier(&order.request.client_identifier)
            .await
        {
            Ok(Some(detail)) => {
                match self
                    .reconcile_venue_detail(
                        &order.request.client_identifier,
                        detail,
                        mode,
                        &order.account_identity,
                    )
                    .await
                {
                    Ok(()) => self.read_order_view(&order.request.client_identifier).await,
                    Err(error) => {
                        if order.state != AutomationOrderState::NeedsReconciliation {
                            self.mark_order_needs_reconciliation(
                                &order.request.client_identifier,
                                &bounded_reason(&error.to_string()),
                            )
                            .await?;
                        }
                        self.read_order_view(&order.request.client_identifier).await
                    }
                }
            }
            Ok(None) => {
                if order.state != AutomationOrderState::NeedsReconciliation {
                    self.mark_order_needs_reconciliation(
                        &order.request.client_identifier,
                        "VENUE_IDENTIFIER_NOT_FOUND_OUTCOME_REMAINS_UNKNOWN",
                    )
                    .await?;
                }
                self.read_order_view(&order.request.client_identifier).await
            }
            Err(error) => {
                if order.state != AutomationOrderState::NeedsReconciliation {
                    self.mark_order_needs_reconciliation(
                        &order.request.client_identifier,
                        &bounded_reason(&error.to_string()),
                    )
                    .await?;
                }
                Err(client_error(error))
            }
        }
    }

    async fn reconcile_venue_detail(
        &self,
        identifier: &AutomationOrderIdentifier,
        detail: crate::upbit::client::UpbitOrderDetail,
        mode: AutomationSessionMode,
        account_identity: &str,
    ) -> Result<(), LabError> {
        let id = identifier.clone();
        let now = UtcTimestamp::now();
        match self
            .inner
            .database
            .call("automation_reconcile_order", move |store| {
                store.automation_reconcile_order(&detail, now)
            })
            .await
        {
            Ok(view) => {
                if matches!(
                    view.state,
                    AutomationOrderState::Filled
                        | AutomationOrderState::Cancelled
                        | AutomationOrderState::Rejected
                ) {
                    self.refresh_account(mode, account_identity).await?;
                }
                Ok(())
            }
            Err(error @ LabError::OutcomeUnknown(_)) => {
                if self.read_order_view(&id).await?.is_some() {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn mark_order_needs_reconciliation(
        &self,
        identifier: &AutomationOrderIdentifier,
        reason: &str,
    ) -> Result<(), LabError> {
        let identifier = identifier.clone();
        let reason = bounded_reason(reason);
        let now = UtcTimestamp::now();
        self.inner
            .database
            .call("automation_mark_order_needs_reconciliation", move |store| {
                store.automation_mark_order_needs_reconciliation(&identifier, &reason, now)
            })
            .await
    }

    async fn dispatch_reserved_by_identifier(
        &self,
        identifier: &AutomationOrderIdentifier,
        mode: AutomationSessionMode,
        session_id: &crate::contracts::AutomationSessionId,
    ) -> Result<(), LabError> {
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let live_execution_policy = self.live_execution_policy();
        let (order, stored_mode) = self
            .inner
            .database
            .call("automation_pending_order_readback", {
                let identifier = identifier.clone();
                let session_id = session_id.clone();
                move |store| {
                    let record = store.automation_runtime_record_with_policy(
                        &session_id,
                        UtcTimestamp::now(),
                        live_execution_policy,
                    )?;
                    let mode = record.view.mode;
                    let order = store
                        .automation_pending_orders(&session_id)?
                        .into_iter()
                        .find(|order| order.request.client_identifier == identifier)
                        .ok_or_else(|| LabError::DataCorrupt("pending order disappeared".into()))?;
                    Ok((order, mode))
                }
            })
            .await?;
        if stored_mode != mode {
            return Err(LabError::Conflict(
                "reserved order session mode changed before dispatch".into(),
            ));
        }
        self.dispatch_reserved_order(&order, mode).await
    }

    async fn persist_catchup_decision(
        &self,
        record: &AutomationRuntimeRecord,
        observation: &CandleObservation,
        checkpoint: &AutomationCheckpoint,
        evaluation: Option<&StrategyEvaluation>,
        reason: &str,
    ) -> Result<(), LabError> {
        let intent_id = AutomationIntentId::from_seed(&format!(
            "{}:{}",
            record.view.session_id.as_str(),
            observation.id.as_str()
        ));
        let policy_evaluation = evaluation.map(evaluation_to_value);
        let evaluation_json = serde_json::json!({
            "recovery_disposition": reason,
            "decision_time_utc": observation.candle.close_time_utc,
            "policy_evaluation": policy_evaluation,
        });
        self.record_decision(
            &record.view.session_id,
            observation,
            checkpoint,
            &intent_id,
            "HOLD",
            &evaluation_json,
            UtcTimestamp::now(),
            record.view.state == AutomationSessionState::NeedsReconciliation,
            None,
        )
        .await?;
        Ok(())
    }

    async fn mark_candle_gap(
        &self,
        record: &AutomationRuntimeRecord,
        checkpoint: &AutomationCheckpoint,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        let session_id = record.view.session_id.clone();
        let candle = observation.clone();
        let checkpoint_json = serde_json::to_string(checkpoint)?;
        let received_at = UtcTimestamp::now();
        let _gap = self
            .inner
            .database
            .call("automation_candle_gap_recovery", move |store| {
                store.automation_record_candle(&session_id, &candle, &checkpoint_json, received_at)
            })
            .await?;
        Ok(())
    }

    async fn mark_first_candle_gap(
        &self,
        record: &AutomationRuntimeRecord,
        checkpoint: &AutomationCheckpoint,
        observation: &CandleObservation,
    ) -> Result<(), LabError> {
        let intent_id = AutomationIntentId::from_seed(&format!(
            "{}:{}",
            record.view.session_id.as_str(),
            observation.id.as_str()
        ));
        let blocked = serde_json::json!({
            "recovery_disposition": "FIRST_POST_START_CANDLE_MISSING",
            "decision_time_utc": observation.candle.close_time_utc,
        });
        self.record_decision(
            &record.view.session_id,
            observation,
            checkpoint,
            &intent_id,
            "HOLD",
            &blocked,
            UtcTimestamp::now(),
            record.view.state == AutomationSessionState::NeedsReconciliation,
            None,
        )
        .await
    }

    async fn persist_warmup_candle(
        &self,
        record: &AutomationRuntimeRecord,
        observation: &CandleObservation,
        checkpoint: &AutomationCheckpoint,
        restart_replay: bool,
    ) -> Result<(), LabError> {
        if restart_replay {
            let evaluation = serde_json::json!({
                "phase": "POLICY_WARMUP",
                "decision_time": observation.candle.close_time_utc,
                "completed_bars_seen": checkpoint.evaluator.completed_bars_seen(),
            });
            let id = AutomationIntentId::from_seed(&format!(
                "{}:{}",
                record.view.session_id.as_str(),
                observation.id.as_str()
            ));
            return self
                .record_decision(
                    &record.view.session_id,
                    observation,
                    checkpoint,
                    &id,
                    "HOLD",
                    &evaluation,
                    UtcTimestamp::now(),
                    true,
                    None,
                )
                .await;
        }
        let session_id = record.view.session_id.clone();
        let observation = observation.clone();
        let close_time = observation.candle.close_time_utc;
        let checkpoint_json = serde_json::to_string(checkpoint)?;
        let received = UtcTimestamp::now();
        let result = self
            .inner
            .database
            .call("automation_record_warmup_candle", move |store| {
                store.automation_record_candle(
                    &session_id,
                    &observation,
                    &checkpoint_json,
                    received,
                )
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let session_id = record.view.session_id.clone();
                let live_execution_policy = self.live_execution_policy();
                let restored = self
                    .inner
                    .database
                    .call("automation_warmup_cursor_readback", move |store| {
                        store.automation_runtime_record_with_policy(
                            &session_id,
                            UtcTimestamp::now(),
                            live_execution_policy,
                        )
                    })
                    .await?;
                if restored.view.last_finalized_candle_at == Some(close_time) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "these exact source, evaluator and recovery facts form one atomic store decision"
    )]
    async fn record_decision(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        observation: &CandleObservation,
        checkpoint: &AutomationCheckpoint,
        intent_id: &AutomationIntentId,
        action: &str,
        evaluation_json: &Value,
        received_at: UtcTimestamp,
        restart_replay: bool,
        decision_guard: Option<(crate::upbit::live_order_control::LiveOrderSendGate, u64)>,
    ) -> Result<(), LabError> {
        let replay_guard = decision_guard.clone();
        let session = session_id.clone();
        let candle = observation.clone();
        let checkpoint_json = serde_json::to_string(checkpoint)?;
        let intent = intent_id.clone();
        let action_value = action.to_owned();
        let retry_action = action_value.clone();
        let evaluation = evaluation_json.clone();
        let result = self
            .inner
            .database
            .call("automation_record_decision", move |store| {
                let rejection = decision_guard
                    .as_ref()
                    .filter(|(gate, generation)| {
                        !gate.is_enabled() || gate.generation() != *generation
                    })
                    .map(|_| "LIVE_ORDER_SEND_GENERATION_CHANGED");
                store.automation_record_decision_with_rejection(
                    &session,
                    &candle,
                    &checkpoint_json,
                    &intent,
                    &action_value,
                    &evaluation,
                    received_at,
                    rejection,
                )
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                // The operation is a single immutable (session, observation)
                // transaction; exact replay is a durable no-op if it landed.
                let session = session_id.clone();
                let candle = observation.clone();
                let checkpoint_json = serde_json::to_string(checkpoint)?;
                let intent = intent_id.clone();
                let action = retry_action;
                let evaluation = evaluation_json.clone();
                let replay = self
                    .inner
                    .database
                    .call("automation_record_decision_readback", move |store| {
                        let rejection = replay_guard
                            .as_ref()
                            .filter(|(gate, generation)| {
                                !gate.is_enabled() || gate.generation() != *generation
                            })
                            .map(|_| "LIVE_ORDER_SEND_GENERATION_CHANGED");
                        store.automation_record_decision_with_rejection(
                            &session,
                            &candle,
                            &checkpoint_json,
                            &intent,
                            &action,
                            &evaluation,
                            UtcTimestamp::now(),
                            rejection,
                        )
                    })
                    .await;
                match replay {
                    Ok(_) => Ok(()),
                    Err(replay_error) => {
                        let session = session_id.clone();
                        let live_execution_policy = self.live_execution_policy();
                        let current = self
                            .inner
                            .database
                            .call("automation_decision_cursor_readback", move |store| {
                                store.automation_runtime_record_with_policy(
                                    &session,
                                    UtcTimestamp::now(),
                                    live_execution_policy,
                                )
                            })
                            .await;
                        if current.is_ok_and(|record| {
                            record.view.last_finalized_candle_at
                                == Some(observation.candle.close_time_utc)
                        }) {
                            Ok(())
                        } else if restart_replay {
                            Err(replay_error)
                        } else {
                            Err(error)
                        }
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn load_pending_orders(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
    ) -> Result<Vec<AutomationPendingOrder>, LabError> {
        let id = session_id.clone();
        self.inner
            .database
            .call("automation_pending_orders", move |store| {
                store.automation_pending_orders(&id)
            })
            .await
    }

    async fn load_pending_intents(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
    ) -> Result<Vec<AutomationPendingIntent>, LabError> {
        let id = session_id.clone();
        self.inner
            .database
            .call("automation_pending_intents", move |store| {
                store.automation_pending_intents(&id)
            })
            .await
    }

    async fn load_runtime_record(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
    ) -> Result<AutomationRuntimeRecord, LabError> {
        let id = session_id.clone();
        let live_execution_policy = self.live_execution_policy();
        self.inner
            .database
            .call("automation_runtime_record", move |store| {
                store.automation_runtime_record_with_policy(&id, now, live_execution_policy)
            })
            .await
    }

    async fn read_automation_request(
        &self,
        request_id: &RequestId,
    ) -> Result<AutomationSessionRequestReadback, LabError> {
        let id = request_id.clone();
        self.inner
            .database
            .call("automation_readback_request", move |store| {
                store.automation_readback_request(&id)
            })
            .await
    }

    async fn read_order_view(
        &self,
        identifier: &AutomationOrderIdentifier,
    ) -> Result<Option<crate::contracts::AutomationOrderView>, LabError> {
        let id = identifier.clone();
        self.inner
            .database
            .call("automation_order_readback", move |store| {
                store.automation_order_readback(&id)
            })
            .await
    }

    async fn reject_reserved_order(
        &self,
        identifier: &AutomationOrderIdentifier,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<crate::contracts::AutomationOrderView, LabError> {
        let identifier = identifier.clone();
        let reason = bounded_reason(reason);
        self.inner
            .database
            .call("automation_reject_reserved_order", move |store| {
                store.automation_reject_reserved_order(&identifier, &reason, now)
            })
            .await
    }

    async fn reject_intent(
        &self,
        intent_id: &AutomationIntentId,
        reason: &str,
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        let id = intent_id.clone();
        let reason = bounded_reason(reason);
        self.inner
            .database
            .call("automation_reject_intent", move |store| {
                store.automation_reject_intent(&id, &reason, now)
            })
            .await
    }

    async fn refresh_account(
        &self,
        mode: AutomationSessionMode,
        expected_account_identity: &str,
    ) -> Result<(), LabError> {
        if !automation_mode_enabled(mode, self.live_execution_policy()) {
            return Err(LabError::BlockedEvidence(
                AUTOMATION_LIVE_DISABLED_REASON.into(),
            ));
        }
        let venue = self.automation_venue(mode);
        verify_venue_mode(venue, mode)?;
        let identity = venue.account_identity();
        if identity != expected_account_identity {
            return Err(LabError::Conflict(
                "account refresh adapter identity differs from the durable session scope".into(),
            ));
        }
        let balances = venue.account_balances().await.map_err(client_error)?;
        // Private account responses do not carry an authoritative historical
        // as-of clock; bind the local observation to when the response arrived.
        let observed_at = UtcTimestamp::now();
        let received_at = observed_at;
        let live_execution_policy = self.live_execution_policy();
        self.inner
            .database
            .call("automation_record_fresh_account_snapshot", move |store| {
                store.automation_record_account_snapshot_with_policy(
                    &identity,
                    mode,
                    observed_at,
                    received_at,
                    &balances,
                    live_execution_policy,
                )
            })
            .await
    }

    async fn read_account_position(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        mark: PriceKrw,
        now: UtcTimestamp,
    ) -> Result<AutomationAccountPosition, LabError> {
        let id = session_id.clone();
        self.inner
            .database
            .call("automation_account_position", move |store| {
                store.automation_account_position(&id, mark, now)
            })
            .await
    }

    async fn complete_restart_reconciliation(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        now: UtcTimestamp,
    ) -> Result<crate::contracts::AutomationSessionView, LabError> {
        let id = session_id.clone();
        let live_execution_policy = self.live_execution_policy();
        self.inner
            .database
            .call("automation_complete_restart_reconciliation", move |store| {
                store.automation_complete_restart_reconciliation_with_policy(
                    &id,
                    now,
                    live_execution_policy,
                )
            })
            .await
    }

    async fn complete_order_reconciliation(
        &self,
        session_id: &crate::contracts::AutomationSessionId,
        expected_sequence: u64,
        now: UtcTimestamp,
    ) -> Result<crate::contracts::AutomationSessionView, LabError> {
        let id = session_id.clone();
        let live_execution_policy = self.live_execution_policy();
        self.inner
            .database
            .call("automation_complete_order_reconciliation", move |store| {
                store.automation_complete_order_reconciliation_with_policy(
                    &id,
                    expected_sequence,
                    now,
                    live_execution_policy,
                )
            })
            .await
    }

    async fn poll_observations(
        &self,
        market: &crate::contracts::MarketId,
        interval: CandleInterval,
        count: u32,
    ) -> Result<Vec<CandleObservation>, LabError> {
        let count = crate::contracts::ProbeCount::try_from(count)?;
        let (_, observations) = self
            .inner
            .upbit
            .poll_paper_observations(market, interval, count, &self.inner.database)
            .await?;
        Ok(observations)
    }

    async fn load_cross_interval_bars(
        &self,
        checkpoint: &AutomationCheckpoint,
        market: &crate::contracts::MarketId,
        now: UtcTimestamp,
    ) -> Result<BTreeMap<CandleInterval, Vec<CandleObservation>>, LabError> {
        let indicators = checkpoint.evaluator.source_indicators();
        let mut intervals = Vec::<(CandleInterval, usize)>::new();
        for (_, interval, warmup) in indicators {
            if let Some((_, current)) = intervals
                .iter_mut()
                .find(|(candidate, _)| *candidate == interval)
            {
                *current = (*current).max(warmup);
            } else {
                intervals.push((interval, warmup));
            }
        }
        intervals.sort_by_key(|(interval, _)| interval.duration().num_seconds());
        let cursors = checkpoint.evaluator.cross_last_close();
        let indicators = checkpoint.evaluator.source_indicators();
        let mut batches = BTreeMap::new();
        for (interval, warmup) in intervals {
            let mut oldest = None;
            let mut missing_cursor = false;
            for (id, candidate, _) in &indicators {
                if *candidate != interval {
                    continue;
                }
                match cursors.get(id) {
                    Some(close) => {
                        oldest =
                            Some(oldest.map_or(*close, |prior: UtcTimestamp| prior.min(*close)));
                    }
                    None => missing_cursor = true,
                }
            }
            let count = if missing_cursor || oldest.is_none() {
                warmup.max(1)
            } else {
                let age = now.0.signed_duration_since(oldest.unwrap_or(now).0);
                if age < chrono::Duration::zero() {
                    return Err(LabError::DataCorrupt(
                        "policy source cursor is later than the runtime clock".into(),
                    ));
                }
                let seconds = interval.duration().num_seconds();
                let elapsed = age.num_seconds().div_euclid(seconds);
                if elapsed <= 0 {
                    continue;
                }
                usize::try_from(elapsed).map_err(|_| {
                    LabError::DataGap("policy source catch-up exceeds platform bounds".into())
                })?
            };
            let count = u32::try_from(count).map_err(|_| {
                LabError::InsufficientWarmup(
                    "policy source warmup exceeds supported history".into(),
                )
            })?;
            if count > AUTOMATION_MAX_CANDLES {
                return Err(LabError::DataGap(format!(
                    "policy source {interval:?} needs {count} bars; bounded feed history is {AUTOMATION_MAX_CANDLES}"
                )));
            }
            let batch = self.poll_observations(market, interval, count).await?;
            batches.insert(interval, batch);
        }
        Ok(batches)
    }
}

fn evaluation_to_value(evaluation: &StrategyEvaluation) -> Value {
    serde_json::json!({
        "decision_time_utc": evaluation.decision_time,
        "state_before": evaluation.state_before,
        "state_after": evaluation.state_after,
        "raw_target_weight": evaluation.raw_target_weight,
        "constrained_target_weight": evaluation.constrained_target_weight,
        "reasons": evaluation.reasons,
        "indicators": evaluation.indicators,
        "evidence_effect": evaluation.evidence_effect,
        "policy_ref": evaluation.policy_ref,
        "policy_trace": evaluation.policy_trace,
        "base_state_before": evaluation.base_state_before,
        "base_state_after": evaluation.base_state_after,
    })
}

fn restore_checkpoint(record: &AutomationRuntimeRecord) -> Result<AutomationCheckpoint, LabError> {
    if record.checkpoint_json.trim().is_empty() || record.checkpoint_json.trim() == "{}" {
        return AutomationCheckpoint::compile(
            &record.frozen_policy,
            record.view.market.clone(),
            record.view.decision_interval,
        );
    }
    serde_json::from_str(&record.checkpoint_json).map_err(|error| {
        LabError::DataCorrupt(format!(
            "automation evaluator checkpoint is invalid: {error}"
        ))
    })
}

fn verify_checkpoint_cursor(
    checkpoint: &AutomationCheckpoint,
    view: &crate::contracts::AutomationSessionView,
) -> Result<(), LabError> {
    match (
        view.last_finalized_candle_at,
        checkpoint.last_observation.as_ref(),
    ) {
        (None, None) => {
            if checkpoint.evaluator.completed_bars_seen() != 0 {
                return Err(LabError::DataCorrupt(
                    "automation evaluator advanced without a durable candle cursor".into(),
                ));
            }
        }
        (Some(close), Some(observation)) if observation.candle.close_time_utc == close => {}
        (Some(_), _) | (None, Some(_)) => {
            return Err(LabError::DataCorrupt(
                "automation evaluator checkpoint differs from the durable candle cursor".into(),
            ));
        }
    }
    Ok(())
}

fn decision_fetch_count(
    record: &AutomationRuntimeRecord,
    checkpoint: &AutomationCheckpoint,
    now: UtcTimestamp,
) -> Result<u32, LabError> {
    let interval = record.view.decision_interval;
    let period = interval.duration().num_seconds();
    if period <= 0 {
        return Err(LabError::InvalidConfig(
            "automation decision interval has no positive duration".into(),
        ));
    }
    if let Some(last) = record.view.last_finalized_candle_at {
        let age = now.0.signed_duration_since(last.0);
        if age < chrono::Duration::zero() {
            return Err(LabError::DataCorrupt(
                "automation candle cursor is in the future".into(),
            ));
        }
        let due = age.num_seconds().div_euclid(period);
        if due <= 0 {
            return Ok(0);
        }
        if due > i64::from(AUTOMATION_MAX_CANDLES) {
            return Err(LabError::DataGap(
                "automation feed catch-up exceeds the bounded finalized-candle window".into(),
            ));
        }
        return u32::try_from(due)
            .map_err(|_| LabError::DataGap("automation catch-up count is out of range".into()));
    }
    let required = checkpoint.evaluator.decision_warmup_bars();
    if required > usize::try_from(AUTOMATION_MAX_CANDLES).unwrap_or(usize::MAX) {
        return Err(LabError::InsufficientWarmup(
            "frozen policy warmup exceeds the bounded public feed window".into(),
        ));
    }
    let requested = required.saturating_add(1).max(1);
    u32::try_from(requested.min(usize::try_from(AUTOMATION_MAX_CANDLES).unwrap_or(usize::MAX)))
        .map_err(|_| LabError::InsufficientWarmup("automation warmup count is unsupported".into()))
}

fn feed_causal_sources(
    checkpoint: &mut AutomationCheckpoint,
    sources: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
    decision_close: UtcTimestamp,
) -> Result<(), LabError> {
    let indicators = checkpoint.evaluator.source_indicators();
    for (indicator_id, interval, _) in indicators {
        let Some(bars) = sources.get(&interval) else {
            continue;
        };
        let mut last_fed = checkpoint
            .evaluator
            .cross_last_close()
            .get(&indicator_id)
            .copied();
        for bar in bars {
            if !bar.candle.completed || bar.candle.close_time_utc > decision_close {
                continue;
            }
            if last_fed.is_some_and(|previous| bar.candle.close_time_utc <= previous) {
                continue;
            }
            if last_fed.is_some_and(|previous| bar.candle.open_time_utc != previous) {
                return Err(LabError::DataGap(format!(
                    "policy source {indicator_id} has a gap before {}",
                    bar.candle.close_time_utc
                )));
            }
            checkpoint.evaluator.observe_source(&indicator_id, bar)?;
            last_fed = Some(bar.candle.close_time_utc);
        }
    }
    Ok(())
}

fn cash_position() -> Result<crate::strategy::PositionView, LabError> {
    Ok(crate::strategy::PositionView {
        actual_qty: AssetQuantity::new(Decimal::ZERO)?,
        actual_weight: Weight::new(Decimal::ZERO)?,
        episode_opened_at: None,
        held_decision_bars: None,
    })
}

fn validate_account_position_freshness(
    position: &AutomationAccountPosition,
    now: UtcTimestamp,
    max_age_seconds: u32,
) -> Result<(), LabError> {
    let age = age_seconds(now, position.account_observed_at)?;
    if age > i64::from(max_age_seconds) {
        return Err(LabError::BlockedEvidence(
            "ACCOUNT_BALANCE_STALE: current-position sizing requires a fresh scoped snapshot"
                .into(),
        ));
    }
    Ok(())
}

fn advance_position_checkpoint(
    checkpoint: &mut AutomationCheckpoint,
    position: &AutomationAccountPosition,
    decision_time: UtcTimestamp,
    interval: CandleInterval,
) -> Result<crate::strategy::PositionView, LabError> {
    if position.actual_qty.get() == Decimal::ZERO {
        checkpoint.episode_opened_at = None;
        checkpoint.held_decision_bars = 0;
        return Ok(crate::strategy::PositionView {
            actual_qty: position.actual_qty,
            actual_weight: position.actual_weight,
            episode_opened_at: None,
            held_decision_bars: None,
        });
    }
    let opened = position.episode_opened_at.ok_or_else(|| {
        LabError::Conflict("filled account position has no durable fill episode".into())
    })?;
    if opened > decision_time {
        return Err(LabError::Conflict(
            "current account position opened after the causal decision candle".into(),
        ));
    }
    let held = if checkpoint.episode_opened_at == Some(opened) {
        checkpoint
            .held_decision_bars
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("held decision bar count overflow".into()))?
    } else {
        let elapsed = decision_time
            .0
            .signed_duration_since(opened.0)
            .num_seconds();
        let seconds = interval.duration().num_seconds();
        let bars = if elapsed == 0 {
            0
        } else {
            elapsed
                .checked_sub(1)
                .and_then(|elapsed| elapsed.checked_div(seconds))
                .and_then(|bars| bars.checked_add(1))
                .ok_or_else(|| LabError::ResourceLimit("held decision bar count overflow".into()))?
        };
        u64::try_from(bars)
            .map_err(|_| LabError::ResourceLimit("held decision bars exceed u64".into()))?
    };
    checkpoint.episode_opened_at = Some(opened);
    checkpoint.held_decision_bars = held;
    Ok(crate::strategy::PositionView {
        actual_qty: position.actual_qty,
        actual_weight: position.actual_weight,
        episode_opened_at: Some(opened),
        held_decision_bars: Some(held),
    })
}

fn action_for_target(target: Weight, actual: Weight) -> &'static str {
    match target.get().cmp(&actual.get()) {
        std::cmp::Ordering::Greater => "BUY",
        std::cmp::Ordering::Less => "SELL",
        std::cmp::Ordering::Equal => "HOLD",
    }
}

fn plan_order(
    target_weight: Weight,
    position: &AutomationAccountPosition,
    mark: PriceKrw,
) -> Result<Option<PlannedOrder>, LabError> {
    if mark.get() <= Decimal::ZERO {
        return Err(LabError::DataCorrupt(
            "automation decision mark must be positive".into(),
        ));
    }
    let target_value = position
        .equity_krw
        .get()
        .checked_mul(target_weight.get())
        .ok_or_else(|| LabError::AccountingInvariant("target position value overflow".into()))?;
    let actual_value = position
        .actual_qty
        .get()
        .checked_mul(mark.get())
        .ok_or_else(|| LabError::AccountingInvariant("actual position value overflow".into()))?;
    let delta = target_value
        .checked_sub(actual_value)
        .ok_or_else(|| LabError::AccountingInvariant("target exposure delta overflow".into()))?;
    if delta == Decimal::ZERO {
        return Ok(None);
    }
    if delta > Decimal::ZERO {
        let budget = QuoteAmount::new(delta)?;
        return Ok(Some(PlannedOrder {
            side: AutomationSide::Bid,
            order: AutomationOrderKind::MarketBuy { budget },
            requested_notional: budget,
            asset_reserve: AssetQuantity::new(Decimal::ZERO)?,
        }));
    }
    let reduction = Decimal::ZERO
        .checked_sub(delta)
        .ok_or_else(|| LabError::AccountingInvariant("sell exposure magnitude overflow".into()))?;
    let volume = reduction
        .checked_div(mark.get())
        .ok_or_else(|| {
            LabError::AccountingInvariant("sell quantity exceeds decimal capacity".into())
        })?
        .min(position.actual_qty.get());
    if volume <= Decimal::ZERO {
        return Ok(None);
    }
    let asset_reserve = AssetQuantity::new(volume)?;
    let requested_notional = QuoteAmount::new(
        volume
            .checked_mul(mark.get())
            .ok_or_else(|| LabError::AccountingInvariant("sell notional overflow".into()))?,
    )?;
    Ok(Some(PlannedOrder {
        side: AutomationSide::Ask,
        order: AutomationOrderKind::MarketSell {
            volume: asset_reserve,
        },
        requested_notional,
        asset_reserve,
    }))
}

#[expect(
    clippy::too_many_arguments,
    reason = "admission pins one source intent to exact rules, economics and evaluation facts"
)]
fn make_admission(
    record: &AutomationRuntimeRecord,
    intent_id: &AutomationIntentId,
    action: &str,
    plan: PlannedOrder,
    rules: &VerifiedMarketRules,
    evaluation_json: &Value,
    decision_time: UtcTimestamp,
    decision_mark: PriceKrw,
) -> Result<AutomationOrderAdmission, LabError> {
    let expected_action = match plan.side {
        AutomationSide::Bid => "BUY",
        AutomationSide::Ask => "SELL",
    };
    if action != expected_action {
        return Err(LabError::Conflict(
            "durable signal direction differs from the exact account-weight order plan".into(),
        ));
    }
    let fee_rate = match plan.side {
        AutomationSide::Bid => rules.bid_fee,
        AutomationSide::Ask => rules.ask_fee,
    };
    let fee_reserve = QuoteAmount::new(
        plan.requested_notional
            .get()
            .checked_mul(fee_rate)
            .ok_or_else(|| LabError::AccountingInvariant("fee reserve overflow".into()))?,
    )?;
    let quote_reserve = if plan.side == AutomationSide::Bid {
        QuoteAmount::new(
            plan.requested_notional
                .get()
                .checked_add(fee_reserve.get())
                .ok_or_else(|| {
                    LabError::AccountingInvariant("quote reservation overflow".into())
                })?,
        )?
    } else {
        QuoteAmount::new(Decimal::ZERO)?
    };
    let request = AutomationOrderRequest {
        client_identifier: AutomationOrderIdentifier::from_intent(intent_id),
        market: record.view.market.clone(),
        side: plan.side,
        order: plan.order,
    };
    request.validate()?;
    Ok(AutomationOrderAdmission {
        session_id: record.view.session_id.clone(),
        intent_id: intent_id.clone(),
        request,
        decision_mark,
        decision_time,
        requested_notional: plan.requested_notional,
        quote_reserve,
        asset_currency: record.view.market.base.code().to_owned(),
        asset_reserve: plan.asset_reserve,
        fee_reserve,
        rules: rules.clone(),
        evaluation_json: evaluation_json.clone(),
    })
}

fn first_close_after(
    started_at: UtcTimestamp,
    interval: CandleInterval,
) -> Result<UtcTimestamp, LabError> {
    let seconds = interval.duration().num_seconds();
    if seconds <= 0 {
        return Err(LabError::InvalidConfig(
            "automation interval duration must be positive".into(),
        ));
    }
    let timestamp = started_at.0.timestamp();
    let boundary = timestamp
        .div_euclid(seconds)
        .checked_add(1)
        .and_then(|bucket| bucket.checked_mul(seconds))
        .ok_or_else(|| LabError::ResourceLimit("automation start boundary overflow".into()))?;
    let datetime = chrono::DateTime::from_timestamp(boundary, 0)
        .ok_or_else(|| LabError::InvalidConfig("automation boundary is out of range".into()))?;
    Ok(UtcTimestamp(datetime.with_timezone(&chrono::Utc)))
}

fn age_seconds(now: UtcTimestamp, at: UtcTimestamp) -> Result<i64, LabError> {
    let age = now.0.signed_duration_since(at.0);
    if age < chrono::Duration::zero() {
        return Err(LabError::DataCorrupt(
            "automation source timestamp is in the future".into(),
        ));
    }
    Ok(age.num_seconds())
}

fn last_checkpoint_mark(record: &AutomationRuntimeRecord) -> Result<PriceKrw, LabError> {
    let checkpoint = restore_checkpoint(record)?;
    checkpoint
        .last_observation
        .map(|observation| observation.candle.close)
        .ok_or_else(|| {
            LabError::BlockedEvidence("automation has no finalized mark for recovery".into())
        })
}

fn automation_request_id(write: &AutomationSessionWrite) -> &RequestId {
    match write {
        AutomationSessionWrite::Start { request_id, .. }
        | AutomationSessionWrite::Pause { request_id, .. }
        | AutomationSessionWrite::Resume { request_id, .. }
        | AutomationSessionWrite::Stop { request_id, .. }
        | AutomationSessionWrite::CancelOrder { request_id, .. }
        | AutomationSessionWrite::RecordBaseline { request_id, .. } => request_id,
    }
}

fn request_id_ref(write: &AutomationSessionWrite) -> &RequestId {
    automation_request_id(write)
}

fn response_from_readback(
    readback: AutomationSessionRequestReadback,
) -> Result<Option<AutomationSessionCommandResponse>, LabError> {
    match readback.state {
        AutomationRequestReadbackState::Applied => readback.response.map(Some).ok_or_else(|| {
            LabError::DataCorrupt("applied automation request omitted response".into())
        }),
        AutomationRequestReadbackState::Rejected => {
            Err(LabError::Conflict(readback.reason.unwrap_or_else(|| {
                "automation request was rejected".into()
            })))
        }
        AutomationRequestReadbackState::Pending => Ok(None),
        AutomationRequestReadbackState::NotRecorded => {
            Err(LabError::OutcomeUnknown(readback.reason.unwrap_or_else(
                || "automation request is not recorded".into(),
            )))
        }
    }
}

fn replayed_automation_response(
    mut response: AutomationSessionCommandResponse,
) -> AutomationSessionCommandResponse {
    response.receipt.outcome = AutomationCommandOutcome::Replayed;
    response
}

fn client_error(error: ClientError) -> LabError {
    match error {
        ClientError::LiveTradingDisabled => {
            LabError::BlockedEvidence("LIVE_TRADING_DISABLED".into())
        }
        ClientError::LiveOrderControlDenied => {
            LabError::BlockedEvidence("LIVE_ORDER_SENDING_DISABLED".into())
        }
        ClientError::RateLimited {
            retry_after_seconds,
        } => LabError::RateLimited(format!(
            "UPBIT_RATE_LIMITED_RETRY_AFTER_SECONDS:{retry_after_seconds}"
        )),
        ClientError::ProviderBlocked => LabError::TemporarilyBlocked(
            "UPBIT_PROVIDER_BLOCKED_WITHOUT_VERIFIED_RETRY_TIME".into(),
        ),
        ClientError::Network(message) => LabError::NetworkUnavailable(message),
        ClientError::Api { status, body } => LabError::NetworkUnavailable(format!(
            "Upbit API HTTP {status}; body length {}",
            body.len()
        )),
        ClientError::Auth(error) => {
            LabError::NetworkUnavailable(format!("private adapter auth: {error}"))
        }
        ClientError::Json(error) => LabError::DataCorrupt(format!("venue JSON: {error}")),
        ClientError::InvalidParams(message) => LabError::InvalidConfig(message),
        ClientError::ResponseTooLarge { observed, limit } => {
            LabError::ResourceLimit(format!("venue response size {observed} exceeds {limit}"))
        }
    }
}

fn bounded_reason(reason: &str) -> String {
    if reason.len() <= 2_048 {
        reason.to_owned()
    } else {
        reason.chars().take(1_900).collect()
    }
}

#[cfg(test)]
#[path = "service/tests.rs"]
mod tests;
