//! Query-only, fixed-prefix views of existing paper facts. No evaluator or feed access.

use super::{MAX_EVENT_LIST_LIMIT, load_session_view};
use crate::contracts::{
    LabError, PaperEventCursor, PaperMarketStart, PaperSessionEvent, PaperSessionEventPage,
    PaperSessionId,
};
use crate::contracts::{PaperSessionReviewPage, PaperSessionStartRequest};
use crate::storage::{Store, json_error, sql_error, u64_to_i64};
use rusqlite::params;

impl Store {
    /// Read an immutable event prefix without polling or reconciling the session.
    ///
    /// # Errors
    /// Rejects unknown sessions, invalid cursors, oversized pages and corrupt facts.
    pub fn paper_review_events(
        &self,
        session_id: &PaperSessionId,
        after_sequence: Option<u64>,
        as_of_sequence: Option<u64>,
        limit: u32,
    ) -> Result<PaperSessionReviewPage, LabError> {
        if after_sequence.is_some() && as_of_sequence.is_none() {
            return Err(LabError::InvalidConfig(
                "paper review continuation requires as_of_sequence".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction().map_err(sql_error)?;
        let session = load_session_view(&transaction, session_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown paper session: {session_id}"))
        })?;
        let (sequence, latency_ms, execution_json): (i64, i64, String) = transaction
            .query_row(
                "SELECT sequence, latency_ms, execution_json FROM paper_sessions WHERE session_id=?1",
                [session_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(sql_error)?;
        if &session.session_id != session_id
            || u64::try_from(sequence).ok() != Some(session.sequence)
        {
            return Err(LabError::DataCorrupt(
                "paper review SQL/JSON session identity or sequence mismatch".into(),
            ));
        }
        let cutoff = as_of_sequence.unwrap_or(session.sequence);
        let after = after_sequence.unwrap_or(0);
        if cutoff > session.sequence || after > cutoff {
            return Err(LabError::InvalidConfig(
                "paper review cursor exceeds saved sequence range".into(),
            ));
        }
        let configuration = PaperSessionStartRequest {
            request_id: session.origin_request_id,
            policy_ref: session.policy_ref,
            decision_interval: session.decision_interval,
            execution_resolution: session.execution_resolution,
            latency_ms: u64::try_from(latency_ms)
                .map_err(|_| LabError::DataCorrupt("negative paper latency".into()))?,
            execution: serde_json::from_str(&execution_json).map_err(json_error)?,
            markets: session
                .markets
                .into_iter()
                .map(|market| PaperMarketStart {
                    market: market.market,
                    initial_cash: market.initial_cash,
                    costs: market.costs,
                    market_rules: market.market_rules,
                })
                .collect(),
        };
        let page = load_event_page(
            &transaction,
            session_id,
            after_sequence,
            u64_to_i64(cutoff)?,
            limit,
            crate::contracts::MAX_PAPER_REVIEW_EVENT_BYTES,
        )?;
        if page.total_count != cutoff {
            return Err(LabError::DataCorrupt(
                "paper review event prefix is incomplete".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(PaperSessionReviewPage {
            schema_version: "1.0".into(),
            mode: session.mode,
            configuration,
            as_of_sequence: cutoff,
            after_sequence: after,
            page,
        })
    }
}

pub(super) fn load_event_page(
    connection: &rusqlite::Connection,
    session_id: &PaperSessionId,
    after_sequence: Option<u64>,
    cutoff: i64,
    limit: u32,
    max_bytes: usize,
) -> Result<PaperSessionEventPage, LabError> {
    if !(1..=MAX_EVENT_LIST_LIMIT).contains(&limit) {
        return Err(LabError::InvalidConfig(format!(
            "paper event list limit must be 1..={MAX_EVENT_LIST_LIMIT}"
        )));
    }
    let after = u64_to_i64(after_sequence.unwrap_or(0))?;
    let total: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM paper_session_events WHERE session_id=?1 AND sequence<=?2",
            params![session_id.as_str(), cutoff],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let mut statement = connection.prepare(
        "SELECT sequence, length(CAST(event_json AS BLOB)), event_json FROM paper_session_events \
         WHERE session_id=?1 AND sequence>?2 AND sequence<=?3 ORDER BY sequence LIMIT ?4",
    ).map_err(sql_error)?;
    let mut rows = statement
        .query(params![
            session_id.as_str(),
            after,
            cutoff,
            i64::from(limit) + 1
        ])
        .map_err(sql_error)?;
    let capacity =
        usize::try_from(limit).map_err(|_| LabError::ResourceLimit("page limit".into()))?;
    let mut items = Vec::with_capacity(capacity);
    let mut bytes = 0_usize;
    let mut has_more = false;
    while let Some(row) = rows.next().map_err(sql_error)? {
        // Look ahead for a cursor without deserializing an event we will discard.
        if items.len() == capacity {
            has_more = true;
            break;
        }
        let size: i64 = row.get(1).map_err(sql_error)?;
        bytes = bytes.saturating_add(
            usize::try_from(size)
                .map_err(|_| LabError::DataCorrupt("negative paper event size".into()))?,
        );
        if bytes > max_bytes {
            return Err(LabError::ResourceLimit(
                "paper event page exceeds byte budget; request a smaller limit".into(),
            ));
        }
        let sequence: i64 = row.get(0).map_err(sql_error)?;
        let json: String = row.get(2).map_err(sql_error)?;
        let event: PaperSessionEvent = serde_json::from_str(&json).map_err(json_error)?;
        if u64::try_from(sequence).ok() != Some(event.sequence) {
            return Err(LabError::DataCorrupt(
                "paper event SQL/JSON sequence mismatch".into(),
            ));
        }
        items.push(event);
    }
    let next_cursor = if has_more {
        items.last().map(|event| PaperEventCursor {
            sequence: event.sequence,
        })
    } else {
        None
    };
    Ok(PaperSessionEventPage {
        session_id: session_id.clone(),
        returned_count: u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("page count".into()))?,
        total_count: u64::try_from(total)
            .map_err(|_| LabError::DataCorrupt("negative paper event count".into()))?,
        items,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_page_bounds_bytes_and_checks_identity_without_decoding_lookahead()
    -> Result<(), Box<dyn std::error::Error>> {
        let connection = rusqlite::Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE paper_session_events(session_id TEXT, sequence INTEGER, event_json TEXT,
             PRIMARY KEY(session_id, sequence));",
        )?;
        let session_id = PaperSessionId::new("review-byte-fixture")?;
        let event = PaperSessionEvent {
            preparation: None,
            sequence: 1,
            kind: crate::contracts::PaperEventKind::CandleProcessed,
            market: None,
            candle: None,
            source_event_at: None,
            finalized_at: None,
            control: None,
            provider_recovery: None,
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        };
        let json = serde_json::to_string(&event)?;
        connection.execute(
            "INSERT INTO paper_session_events VALUES(?1,1,?2)",
            params![session_id.as_str(), json],
        )?;
        assert!(matches!(
            load_event_page(&connection, &session_id, None, 1, 1, 1),
            Err(LabError::ResourceLimit(_))
        ));
        connection.execute(
            "INSERT INTO paper_session_events VALUES(?1,2,?2)",
            params![session_id.as_str(), "not a decodable event".repeat(100)],
        )?;
        let first = load_event_page(&connection, &session_id, None, 2, 1, 1024)?;
        assert_eq!(first.next_cursor.expect("lookahead").sequence, 1);
        assert_eq!(first.items.len(), 1);
        assert!(load_event_page(&connection, &session_id, Some(1), 2, 1, 1024).is_err());
        connection.execute(
            "UPDATE paper_session_events SET sequence=3 WHERE sequence=1",
            [],
        )?;
        assert!(matches!(
            load_event_page(&connection, &session_id, Some(2), 3, 1, 1024),
            Err(LabError::DataCorrupt(_))
        ));
        Ok(())
    }
}
