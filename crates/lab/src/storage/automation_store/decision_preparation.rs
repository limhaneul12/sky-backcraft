//! Original readiness observations leave evaluator/candle/order cursors intact.

use super::{
    AutomationEventKind, LabError, OptionalExtension, Store, UtcTimestamp, insert_event,
    json_error, next_event_sequence, parse_interval, sql_error,
};

pub(super) fn record(
    store: &mut Store,
    session_id: &crate::contracts::AutomationSessionId,
    preparation: &crate::contracts::DecisionPreparation,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    if preparation.decision_time > now || preparation.required_bars == 0 {
        return Err(LabError::InvalidConfig(
            "invalid decision preparation boundary".into(),
        ));
    }
    let transaction = store.connection.transaction().map_err(sql_error)?;
    let session: (String, String, String, Option<String>) = transaction.query_row(
        "SELECT state,market,decision_interval,last_finalized_candle_at_utc FROM automation_sessions WHERE session_id=?1",
        [session_id.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional().map_err(sql_error)?.ok_or_else(|| LabError::InvalidConfig(
        format!("unknown automation session {session_id}")))?;
    if session.0 != "RUNNING" {
        return Ok(false);
    }
    if session
        .3
        .as_ref()
        .map(|close| UtcTimestamp::parse_rfc3339(close))
        .transpose()?
        .is_some_and(|close| close >= preparation.decision_time)
    {
        // A delayed readiness reply cannot supersede an already committed decision.
        return Ok(false);
    }
    if preparation.market.code() != session.1 || preparation.interval != parse_interval(&session.2)?
    {
        return Err(LabError::DataCorrupt(
            "preparation differs from frozen automation session".into(),
        ));
    }
    let previous: Option<String> = transaction
        .query_row(
            "SELECT detail_json FROM automation_session_events INDEXED BY automation_events_deferred_lookup \
         WHERE session_id=?1 AND kind='DECISION_DEFERRED' \
         ORDER BY sequence DESC LIMIT 1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if previous
        .map(
            |json| -> Result<Option<crate::contracts::DecisionPreparation>, LabError> {
                let detail: serde_json::Value = serde_json::from_str(&json).map_err(json_error)?;
                detail
                    .get("preparation")
                    .map(|fact| serde_json::from_value(fact.clone()).map_err(json_error))
                    .transpose()
            },
        )
        .transpose()?
        .flatten()
        .as_ref()
        == Some(preparation)
    {
        return Ok(false);
    }
    insert_event(
        &transaction,
        session_id,
        next_event_sequence(&transaction, session_id)?,
        AutomationEventKind::DecisionDeferred,
        now,
        serde_json::json!({"preparation": preparation,
            "observation_id": preparation.observation_id,
            "decision_time_utc": preparation.decision_time}),
    )?;
    transaction.commit().map_err(sql_error)?;
    Ok(true)
}
