//! Deduplicated preparation observations without advancing the economic cursor.

use super::{
    CandleObservation, LabError, OptionalExtension, PaperCandleReference, PaperEventKind,
    PaperSessionId, Transaction, UtcTimestamp, json_error, params, sql_error,
};

pub(super) fn deferred_event(
    transaction: &Transaction<'_>,
    session_id: &PaperSessionId,
    bar: &CandleObservation,
    preparation: &crate::contracts::DecisionPreparation,
    now: UtcTimestamp,
) -> Result<Option<crate::paper::PendingPaperEvent>, LabError> {
    if preparation.observation_id != bar.id
        || preparation.market.code() != bar.candle.market
        || preparation.interval != bar.candle.interval
        || preparation.decision_time != bar.candle.close_time_utc
        || preparation.decision_time > now
        || preparation.required_bars == 0
    {
        return Err(LabError::DataCorrupt(
            "decision preparation differs from its original candidate candle".into(),
        ));
    }
    // Only identical consecutive observations are duplicates: A -> B -> A is
    // a real readiness transition and must remain visible in the saved record.
    let previous: Option<String> = transaction
        .query_row(
            "SELECT json_extract(event_json,'$.preparation') FROM paper_session_events \
         WHERE session_id=?1 AND kind='DECISION_DEFERRED' \
         AND json_extract(event_json,'$.preparation.market')=?2 \
         ORDER BY sequence DESC LIMIT 1",
            params![session_id.as_str(), preparation.market.code()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if previous
        .map(|json| {
            serde_json::from_str::<crate::contracts::DecisionPreparation>(&json).map_err(json_error)
        })
        .transpose()?
        .as_ref()
        == Some(preparation)
    {
        return Ok(None);
    }
    Ok(Some(crate::paper::PendingPaperEvent {
        kind: PaperEventKind::DecisionDeferred,
        market: Some(preparation.market.clone()),
        candle: Some(PaperCandleReference {
            observation_id: bar.id.clone(),
            interval: bar.candle.interval,
            open_time: bar.candle.open_time_utc,
            close_time: bar.candle.close_time_utc,
            content_digest: bar.content_digest.clone(),
        }),
        source_event_at: Some(now),
        finalized_at: None,
        preparation: Some(preparation.clone()),
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    }))
}
