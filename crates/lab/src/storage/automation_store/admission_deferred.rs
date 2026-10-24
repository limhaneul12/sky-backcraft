//! Original admission gate observations preserve the exact immutable decision link.

use super::{
    AutomationEventKind, LabError, OptionalExtension, Store, Transaction, UtcTimestamp,
    insert_event, next_event_sequence, parse_interval, sql_error,
};
use crate::contracts::{
    AdmissionDeferredFact, AdmissionDeferredPhase, AdmissionDeferredReason,
    AdmissionDeferredTarget, AutomationIntentId, AutomationOrderIdentifier, AutomationOrderState,
    AutomationSessionId, AutomationSessionState, CandleObservation, MarketId, ObservationId,
    observation_digest,
};
use rusqlite::params;

const MAX_REASON_DETAIL_BYTES: usize = 2_048;

struct ResolvedTarget {
    phase: AdmissionDeferredPhase,
    intent_id: AutomationIntentId,
    observation_id: ObservationId,
    decision_time: UtcTimestamp,
    client_identifier: Option<AutomationOrderIdentifier>,
}

struct StoredOrderTargetRow {
    owner: String,
    intent_id: String,
    order_state: String,
    intent_owner: Option<String>,
    observation_id: Option<String>,
    decision_time: Option<String>,
    intent_state: Option<String>,
}

/// Persist one exact gate deferral without advancing evaluation or effect state.
/// # Errors
/// Rejects invalid reason/phase pairs, cross-session targets, corrupt frozen links and storage failure.
#[expect(
    clippy::too_many_lines,
    reason = "target resolution, immutable-link validation, deduplication, and event append share one transaction"
)]
pub(super) fn record(
    store: &mut Store,
    session_id: &AutomationSessionId,
    target: &AdmissionDeferredTarget,
    reason: AdmissionDeferredReason,
    reason_detail: Option<&str>,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    if reason_detail.is_some_and(|detail| detail.len() > MAX_REASON_DETAIL_BYTES) {
        return Err(LabError::ResourceLimit(
            "admission deferral detail exceeds 2048 bytes".into(),
        ));
    }
    let phase = phase_for(target, reason)?;
    let transaction = store.connection.transaction().map_err(sql_error)?;
    let (raw_state, market, interval): (String, String, String) = transaction
        .query_row(
            "SELECT state,market,decision_interval FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown automation session {session_id}"))
        })?;
    let session_state: AutomationSessionState =
        serde_json::from_value(serde_json::Value::String(raw_state)).map_err(|_| {
            LabError::DataCorrupt("stored automation session state is invalid".into())
        })?;

    let resolved = match target {
        AdmissionDeferredTarget::PendingIntent(intent_id) => {
            let (owner, raw_observation_id, raw_decision_time, state): (
                String,
                String,
                String,
                String,
            ) = transaction
                .query_row(
                    "SELECT session_id,observation_id,decision_time_utc,state \
                     FROM automation_intents WHERE intent_id=?1",
                    [intent_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| LabError::InvalidConfig("unknown automation intent".into()))?;
            ensure_target_owner(session_id, &owner, "intent")?;
            validate_intent_state(&state)?;
            if state == "PENDING" {
                Some(ResolvedTarget {
                    phase,
                    intent_id: intent_id.clone(),
                    observation_id: parse_observation_id(&raw_observation_id)?,
                    decision_time: parse_decision_time(&raw_decision_time)?,
                    client_identifier: None,
                })
            } else {
                None
            }
        }
        AdmissionDeferredTarget::ReservedOrder(identifier) => resolve_order_target(
            &transaction,
            session_id,
            identifier,
            AutomationOrderState::Reserved,
            phase,
            "ORDER_RESERVED",
        )?,
        AdmissionDeferredTarget::ClaimedOrder(identifier) => resolve_order_target(
            &transaction,
            session_id,
            identifier,
            AutomationOrderState::SubmittedUnknown,
            phase,
            "ORDER_SUBMITTED",
        )?,
    };
    let Some(resolved) = resolved else {
        transaction.commit().map_err(sql_error)?;
        return Ok(false);
    };
    if session_state != AutomationSessionState::Running {
        transaction.commit().map_err(sql_error)?;
        return Ok(false);
    }

    validate_observation_link(
        &transaction,
        session_id,
        &resolved.observation_id,
        resolved.decision_time,
        &market,
        &interval,
        now,
    )?;

    let fact = AdmissionDeferredFact {
        phase: resolved.phase,
        reason,
        intent_id: resolved.intent_id,
        observation_id: resolved.observation_id,
        client_identifier: resolved.client_identifier,
        reason_detail: reason_detail.map(str::to_owned),
    };
    if latest_admission_deferred_fact(&transaction, session_id)?.as_ref() == Some(&fact) {
        transaction.commit().map_err(sql_error)?;
        return Ok(false);
    }

    let mut detail = serde_json::json!({
        "admission_deferral": &fact,
        "intent_id": fact.intent_id.as_str(),
        "observation_id": fact.observation_id.as_str(),
        "reason": reason,
    });
    if let Some(identifier) = &fact.client_identifier {
        detail["client_identifier"] = serde_json::json!(identifier.as_str());
    }
    insert_event(
        &transaction,
        session_id,
        next_event_sequence(&transaction, session_id)?,
        AutomationEventKind::DecisionDeferred,
        now,
        detail,
    )?;
    transaction.commit().map_err(sql_error)?;
    Ok(true)
}

fn phase_for(
    target: &AdmissionDeferredTarget,
    reason: AdmissionDeferredReason,
) -> Result<AdmissionDeferredPhase, LabError> {
    match (target, reason) {
        (
            AdmissionDeferredTarget::PendingIntent(_),
            AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        ) => Ok(AdmissionDeferredPhase::BeforeAdmission),
        (
            AdmissionDeferredTarget::ReservedOrder(_),
            AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        ) => Ok(AdmissionDeferredPhase::BeforeSendClaim),
        (
            AdmissionDeferredTarget::ClaimedOrder(_),
            AdmissionDeferredReason::EffectAuthorizationBlocked,
        ) => Ok(AdmissionDeferredPhase::AfterSendClaim),
        _ => Err(LabError::InvalidConfig(
            "admission deferral reason is not valid for this target phase".into(),
        )),
    }
}

fn resolve_order_target(
    transaction: &Transaction<'_>,
    session_id: &AutomationSessionId,
    identifier: &AutomationOrderIdentifier,
    expected_order_state: AutomationOrderState,
    phase: AdmissionDeferredPhase,
    required_intent_state: &'static str,
) -> Result<Option<ResolvedTarget>, LabError> {
    let row = transaction
        .query_row(
            "SELECT o.session_id,o.intent_id,o.state,i.session_id,i.observation_id, \
                    i.decision_time_utc,i.state \
             FROM automation_orders AS o \
             LEFT JOIN automation_intents AS i ON i.intent_id=o.intent_id \
             WHERE o.client_identifier=?1",
            [identifier.as_str()],
            |row| {
                Ok(StoredOrderTargetRow {
                    owner: row.get(0)?,
                    intent_id: row.get(1)?,
                    order_state: row.get(2)?,
                    intent_owner: row.get(3)?,
                    observation_id: row.get(4)?,
                    decision_time: row.get(5)?,
                    intent_state: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown automation order".into()))?;
    ensure_target_owner(session_id, &row.owner, "order")?;
    let order_state = parse_order_state(&row.order_state)?;
    if order_state != expected_order_state {
        return Ok(None);
    }
    let intent_id = AutomationIntentId::new(row.intent_id)
        .map_err(|_| LabError::DataCorrupt("stored order intent identity is invalid".into()))?;
    let intent_owner = row
        .intent_owner
        .ok_or_else(|| LabError::DataCorrupt("admission order intent is missing".into()))?;
    if intent_owner != row.owner {
        return Err(LabError::DataCorrupt(
            "stored order and linked intent have different owners".into(),
        ));
    }
    let raw_observation_id = row
        .observation_id
        .ok_or_else(|| LabError::DataCorrupt("admission order observation is missing".into()))?;
    let raw_decision_time = row
        .decision_time
        .ok_or_else(|| LabError::DataCorrupt("admission order decision time is missing".into()))?;
    let intent_state = row
        .intent_state
        .ok_or_else(|| LabError::DataCorrupt("admission order intent state is missing".into()))?;
    validate_intent_state(&intent_state)?;
    if intent_state != required_intent_state {
        return Ok(None);
    }
    Ok(Some(ResolvedTarget {
        phase,
        intent_id,
        observation_id: parse_observation_id(&raw_observation_id)?,
        decision_time: parse_decision_time(&raw_decision_time)?,
        client_identifier: Some(identifier.clone()),
    }))
}

fn parse_observation_id(raw: &str) -> Result<ObservationId, LabError> {
    ObservationId::new(raw.to_owned())
        .map_err(|_| LabError::DataCorrupt("stored intent observation identity is invalid".into()))
}

fn parse_decision_time(raw: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(raw)
        .map_err(|_| LabError::DataCorrupt("stored intent decision time is invalid".into()))
}

fn parse_order_state(raw: &str) -> Result<AutomationOrderState, LabError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|_| LabError::DataCorrupt("stored automation order state is invalid".into()))
}

fn validate_intent_state(state: &str) -> Result<(), LabError> {
    if matches!(
        state,
        "NO_ACTION"
            | "PENDING"
            | "REJECTED"
            | "ORDER_RESERVED"
            | "ORDER_SUBMITTED"
            | "ORDER_UNKNOWN"
            | "ORDER_RECONCILED"
    ) {
        Ok(())
    } else {
        Err(LabError::DataCorrupt(
            "stored automation intent state is invalid".into(),
        ))
    }
}

fn validate_observation_link(
    transaction: &Transaction<'_>,
    session_id: &AutomationSessionId,
    observation_id: &ObservationId,
    decision_time: UtcTimestamp,
    session_market: &str,
    session_interval: &str,
    now: UtcTimestamp,
) -> Result<(), LabError> {
    let (candle_json, raw_digest): (String, String) = transaction
        .query_row(
            "SELECT candle_json,content_digest FROM automation_candles \
             WHERE session_id=?1 AND observation_id=?2",
            params![session_id.as_str(), observation_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::DataCorrupt("admission target observation is missing".into()))?;
    let observation: CandleObservation = serde_json::from_str(&candle_json).map_err(|_| {
        LabError::DataCorrupt("stored admission observation JSON is invalid".into())
    })?;
    let stored_digest = crate::contracts::ContentHash::try_from(raw_digest)
        .map_err(|_| LabError::DataCorrupt("stored observation digest is invalid".into()))?;
    let computed_digest = observation_digest(&observation.candle)?;
    let market = MarketId::parse_upbit(session_market)
        .map_err(|_| LabError::DataCorrupt("stored automation market is invalid".into()))?;
    let interval = parse_interval(session_interval)
        .map_err(|_| LabError::DataCorrupt("stored automation interval is invalid".into()))?;
    if observation.id != *observation_id
        || observation.id != ObservationId::from_seed(stored_digest.as_str())
        || observation.content_digest != stored_digest
        || computed_digest != stored_digest
        || observation.candle.market != market.code()
        || observation.candle.interval != interval
        || observation.candle.close_time_utc != decision_time
        || !observation.candle.completed
        || decision_time > now
    {
        return Err(LabError::DataCorrupt(
            "admission target does not link to its frozen completed observation".into(),
        ));
    }
    Ok(())
}

fn latest_admission_deferred_fact(
    transaction: &Transaction<'_>,
    session_id: &AutomationSessionId,
) -> Result<Option<AdmissionDeferredFact>, LabError> {
    let detail: Option<String> = transaction
        .query_row(
            "SELECT detail_json FROM automation_session_events INDEXED BY automation_events_deferred_lookup \
             WHERE session_id=?1 AND kind='DECISION_DEFERRED' \
             ORDER BY sequence DESC LIMIT 1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(detail) = detail else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(&detail)
        .map_err(|_| LabError::DataCorrupt("stored deferred event JSON is invalid".into()))?;
    let Some(fact) = value.get("admission_deferral") else {
        return Ok(None);
    };
    serde_json::from_value(fact.clone())
        .map(Some)
        .map_err(|_| LabError::DataCorrupt("stored admission deferral fact is invalid".into()))
}

fn ensure_target_owner(
    requested_session: &AutomationSessionId,
    stored_session: &str,
    kind: &str,
) -> Result<(), LabError> {
    if requested_session.as_str() != stored_session {
        return Err(LabError::Conflict(format!(
            "admission deferral {kind} belongs to another automation session"
        )));
    }
    Ok(())
}
