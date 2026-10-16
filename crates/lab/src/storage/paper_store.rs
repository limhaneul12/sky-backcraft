//! SQLite persistence and single-transaction state transitions for persistent paper sessions.

use super::{
    Store, enum_text, insert_observation, insert_raw, json_error, sql_error, timestamp_ms,
    u64_to_i64, usize_to_i64,
};
use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, ENGINE_VERSION, LabError, MarketId,
    PaperCommandOutcome, PaperControlEvent, PaperEventCursor, PaperEventKind, PaperMarketStart,
    PaperRecoveryCause, PaperSessionCommandAction, PaperSessionCommandResponse, PaperSessionCursor,
    PaperSessionEvent, PaperSessionEventPage, PaperSessionGetResponse, PaperSessionId,
    PaperSessionMode, PaperSessionPage, PaperSessionReceipt, PaperSessionRequestReadback,
    PaperSessionStartOptions, PaperSessionState, PaperSessionView, PaperSessionWrite, PolicyId,
    PolicyRevisionId, PolicyRevisionRef, RawObjectRef, RequestId, SCHEMA_VERSION, UtcTimestamp,
};
use crate::paper::{
    MAX_ACTIVE_PAPER_SESSIONS, MAX_PAPER_MARKETS_PER_SESSION, MAX_PAPER_RECOVERY_BARS,
    PAPER_CHECKPOINT_SCHEMA_VERSION, PaperMarketCheckpoint, PaperStepOutcome,
    PreparedPaperSourceFrame, default_start_options, materialize_candle_event,
    policy_option_from_revision, request_payload_digest, semantic_digest,
};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

const MAX_SESSION_LIST_LIMIT: u32 = 50;
const MAX_EVENT_LIST_LIMIT: u32 = 100;

/// Market/interval polling requirement for an active `RUNNING` paper session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperPollRequirement {
    pub market: MarketId,
    pub interval: CandleInterval,
    pub last_bar_close_utc: Option<UtcTimestamp>,
}

impl Store {
    /// Return dynamic start options for the persistent paper-session creation form.
    ///
    /// # Errors
    /// Returns an error if stored policies cannot be read or default options fail validation.
    pub fn paper_start_options(&self) -> Result<PaperSessionStartOptions, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT p.policy_id, pr.revision_id, pr.definition_digest \
                 FROM policies p \
                 JOIN policy_revisions pr ON pr.policy_id = p.policy_id \
                 ORDER BY p.policy_id ASC, pr.revision_number DESC",
            )
            .map_err(sql_error)?;
        let refs = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);

        let mut options = Vec::with_capacity(refs.len());
        for (policy_id, revision_id, digest_str) in refs {
            let reference = PolicyRevisionRef {
                policy_id: PolicyId::new(policy_id)?,
                revision_id: PolicyRevisionId::new(revision_id)?,
                definition_digest: ContentHash::try_from(digest_str)?,
            };
            if let Some(revision) = self.load_policy_revision(&reference)? {
                options.push(policy_option_from_revision(&revision.snapshot));
            }
        }
        default_start_options(options)
    }

    /// List persistent paper sessions ordered newest-first.
    ///
    /// # Errors
    /// Rejects invalid pagination parameters or corrupt stored session rows.
    pub fn paper_list_sessions(
        &self,
        after_session_id: Option<&PaperSessionId>,
        limit: u32,
        now: UtcTimestamp,
    ) -> Result<PaperSessionPage, LabError> {
        if !(1..=MAX_SESSION_LIST_LIMIT).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "paper session list limit must be 1..={MAX_SESSION_LIST_LIMIT}"
            )));
        }
        let total_i64: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM paper_sessions", [], |row| row.get(0))
            .map_err(sql_error)?;
        let total_count = u64::try_from(total_i64)
            .map_err(|_| LabError::DataCorrupt("negative paper session count".into()))?;

        let cursor_tuple: Option<(String, String)> = if let Some(after_id) = after_session_id {
            let created: String = self
                .connection
                .query_row(
                    "SELECT created_at_utc FROM paper_sessions WHERE session_id = ?1",
                    [after_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    LabError::InvalidConfig(format!(
                        "unknown after_session_id cursor: {}",
                        after_id.as_str()
                    ))
                })?;
            Some((created, after_id.as_str().to_owned()))
        } else {
            None
        };

        let mut statement = self
            .connection
            .prepare(
                "SELECT view_json FROM paper_sessions \
                 WHERE (?1 IS NULL \
                        OR created_at_utc < ?1 \
                        OR (created_at_utc = ?1 AND session_id > ?2)) \
                 ORDER BY created_at_utc DESC, session_id ASC \
                 LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    cursor_tuple.as_ref().map(|(created, _)| created.as_str()),
                    cursor_tuple.as_ref().map(|(_, id)| id.as_str()),
                    i64::from(limit) + 1
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;

        let mut items = Vec::new();
        for row in rows {
            let view_json = row.map_err(sql_error)?;
            let mut view: PaperSessionView =
                serde_json::from_str(&view_json).map_err(json_error)?;
            refresh_view_freshness(&mut view, now);
            items.push(view);
        }
        let limit_usize = usize::try_from(limit)
            .map_err(|_| LabError::ResourceLimit("limit exceeds usize".into()))?;
        let has_more = items.len() > limit_usize;
        if has_more {
            items.truncate(limit_usize);
        }
        let next_cursor = if has_more {
            items.last().map(|item| PaperSessionCursor {
                session_id: item.session_id.clone(),
            })
        } else {
            None
        };
        let returned_count = u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("returned_count overflow".into()))?;
        Ok(PaperSessionPage {
            items,
            returned_count,
            total_count,
            next_cursor,
        })
    }

    /// Load one persistent paper session by ID.
    ///
    /// # Errors
    /// Returns an error if the session does not exist or its stored JSON is corrupt.
    pub fn paper_get_session(
        &self,
        session_id: &PaperSessionId,
        now: UtcTimestamp,
    ) -> Result<PaperSessionGetResponse, LabError> {
        let mut session = load_session_view(&self.connection, session_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown paper session: {}", session_id.as_str()))
        })?;
        refresh_view_freshness(&mut session, now);
        Ok(PaperSessionGetResponse { session })
    }

    #[cfg(test)]
    pub(crate) fn paper_checkpoint_digests_for_test(
        &self,
        session_id: &PaperSessionId,
    ) -> Result<Vec<ContentHash>, LabError> {
        load_session_checkpoints(&self.connection, session_id)?
            .iter()
            .map(PaperMarketCheckpoint::checkpoint_digest)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn paper_candle_observation_count_for_test(
        &self,
        market: &MarketId,
        interval: CandleInterval,
    ) -> Result<u64, LabError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM candle_observations WHERE market=?1 AND interval=?2",
                params![market.code(), enum_text(&interval)?],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        u64::try_from(count)
            .map_err(|_| LabError::DataCorrupt("negative paper candle observation count".into()))
    }

    #[cfg(test)]
    pub(crate) fn paper_resume_event_fault_for_test(&self, enabled: bool) -> Result<(), LabError> {
        let sql = if enabled {
            "CREATE TRIGGER synthetic_paper_resume_event_abort \
             BEFORE INSERT ON paper_session_events \
             BEGIN SELECT RAISE(ABORT, 'synthetic paper resume transaction fault'); END;"
        } else {
            "DROP TRIGGER IF EXISTS synthetic_paper_resume_event_abort;"
        };
        self.connection.execute_batch(sql).map_err(sql_error)
    }

    /// List ordered session events for a paper session with optional sequence cursor.
    ///
    /// # Errors
    /// Rejects unknown sessions, invalid limits, or corrupt stored events.
    pub fn paper_list_events(
        &self,
        session_id: &PaperSessionId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<PaperSessionEventPage, LabError> {
        let exists = self
            .connection
            .query_row(
                "SELECT 1 FROM paper_sessions WHERE session_id = ?1",
                [session_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?;
        if exists.is_none() {
            return Err(LabError::InvalidConfig(format!(
                "unknown paper session: {}",
                session_id.as_str()
            )));
        }
        if !(1..=MAX_EVENT_LIST_LIMIT).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "paper event list limit must be 1..={MAX_EVENT_LIST_LIMIT}"
            )));
        }
        let after_seq_i64 = u64_to_i64(after_sequence.unwrap_or(0))?;

        let total_i64: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_session_events WHERE session_id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let total_count = u64::try_from(total_i64)
            .map_err(|_| LabError::DataCorrupt("negative paper event count".into()))?;

        let mut statement = self
            .connection
            .prepare(
                "SELECT event_json FROM paper_session_events \
                 WHERE session_id = ?1 AND sequence > ?2 \
                 ORDER BY sequence ASC \
                 LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![session_id.as_str(), after_seq_i64, i64::from(limit) + 1],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;

        let mut items = Vec::new();
        for row in rows {
            let event_json = row.map_err(sql_error)?;
            let event: PaperSessionEvent = serde_json::from_str(&event_json).map_err(json_error)?;
            items.push(event);
        }
        let limit_usize = usize::try_from(limit)
            .map_err(|_| LabError::ResourceLimit("limit exceeds usize".into()))?;
        let has_more = items.len() > limit_usize;
        if has_more {
            items.truncate(limit_usize);
        }
        let next_cursor = if has_more {
            items.last().map(|event| PaperEventCursor {
                sequence: event.sequence,
            })
        } else {
            None
        };
        let returned_count = u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("returned_count overflow".into()))?;
        Ok(PaperSessionEventPage {
            session_id: session_id.clone(),
            items,
            returned_count,
            total_count,
            next_cursor,
        })
    }

    /// Read back the durable outcome of a prior `paper_session_write` request by `request_id`.
    ///
    /// # Errors
    /// Returns an error if stored receipt or session JSON is corrupt.
    pub fn paper_readback_request(
        &self,
        request_id: &RequestId,
        now: UtcTimestamp,
    ) -> Result<PaperSessionRequestReadback, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT session_id, receipt_json FROM paper_request_receipts WHERE request_id = ?1",
                [request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((session_id_str, receipt_json)) = row else {
            return Ok(PaperSessionRequestReadback {
                request_id: request_id.clone(),
                found: false,
                action: None,
                session_id: None,
                receipt: None,
                session: None,
            });
        };
        let receipt: PaperSessionReceipt =
            serde_json::from_str(&receipt_json).map_err(json_error)?;
        let action = receipt.action;
        let session_id = PaperSessionId::new(session_id_str)?;
        let mut session = load_session_view(&self.connection, &session_id)?;
        if let Some(view) = &mut session {
            refresh_view_freshness(view, now);
        }
        Ok(PaperSessionRequestReadback {
            request_id: request_id.clone(),
            found: true,
            action: Some(action),
            session_id: Some(session_id),
            receipt: Some(receipt),
            session,
        })
    }

    /// Execute a persistent paper-session lifecycle command (`start`, `pause`, `resume`, `stop`)
    /// with request-id idempotency and compare-and-swap sequence validation.
    ///
    /// # Errors
    /// Rejects conflicting request reuse, stale `expected_sequence`, invalid state transitions,
    /// resource limits, or SQLite failures.
    pub fn paper_write(
        &mut self,
        write: &PaperSessionWrite,
        now: UtcTimestamp,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        self.paper_write_with_recovery_context(write, None, &[], now)
    }

    /// Commit an explicit feed-gap Resume with batches fetched from the public paper feed.
    /// The expected cursors are rechecked inside the same transaction as observation insertion.
    pub(crate) fn paper_write_for_feed_gap_resume(
        &mut self,
        write: &PaperSessionWrite,
        expected_requirements: &[PaperPollRequirement],
        fetched_batches: &[(RawObjectRef, Vec<CandleObservation>)],
        now: UtcTimestamp,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        if !matches!(write, PaperSessionWrite::Resume { .. }) {
            return Err(LabError::InvalidConfig(
                "feed-gap recovery data is only valid for a paper Resume".into(),
            ));
        }
        self.paper_write_with_recovery_context(
            write,
            Some(expected_requirements),
            fetched_batches,
            now,
        )
    }

    fn paper_write_with_recovery_context(
        &mut self,
        write: &PaperSessionWrite,
        expected_requirements: Option<&[PaperPollRequirement]>,
        fetched_batches: &[(RawObjectRef, Vec<CandleObservation>)],
        now: UtcTimestamp,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        if expected_requirements.is_none() && !fetched_batches.is_empty() {
            return Err(LabError::InvalidConfig(
                "paper recovery batches require verified feed-gap cursors".into(),
            ));
        }
        self.enforce_storage_pressure()?;
        let action = write.action();
        let action_label = enum_text(&action)?;
        let request_id = write.request_id();
        let payload_digest = request_payload_digest(&action_label, request_id, write)?;

        if let Some(response) =
            self.replay_paper_write_if_present(request_id, &action_label, &payload_digest, now)?
        {
            return Ok(response);
        }

        match write {
            PaperSessionWrite::Start {
                request_id,
                policy_ref,
                decision_interval,
                execution_resolution,
                latency_ms,
                execution,
                markets,
            } => self.paper_start_session(
                request_id,
                &payload_digest,
                policy_ref,
                *decision_interval,
                *execution_resolution,
                *latency_ms,
                execution,
                markets,
                now,
            ),
            PaperSessionWrite::Pause { .. } | PaperSessionWrite::Stop { .. } => {
                self.paper_transition_session(write, &payload_digest, None, &[], now)
            }
            PaperSessionWrite::Resume { .. } => self.paper_transition_session(
                write,
                &payload_digest,
                expected_requirements,
                fetched_batches,
                now,
            ),
        }
    }

    fn replay_paper_write_if_present(
        &self,
        request_id: &RequestId,
        action_label: &str,
        payload_digest: &ContentHash,
        now: UtcTimestamp,
    ) -> Result<Option<PaperSessionCommandResponse>, LabError> {
        let Some((existing_action, existing_digest, session_id_str, receipt_json)) = self
            .connection
            .query_row(
                "SELECT action, payload_digest, session_id, receipt_json \
                 FROM paper_request_receipts WHERE request_id = ?1",
                [request_id.as_str()],
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
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        if existing_action != action_label || existing_digest != payload_digest.as_str() {
            return Err(LabError::Conflict(format!(
                "request_id {} was already used for a different paper_session_write payload",
                request_id.as_str()
            )));
        }
        let mut receipt: PaperSessionReceipt =
            serde_json::from_str(&receipt_json).map_err(json_error)?;
        receipt.outcome = PaperCommandOutcome::Replayed;
        let session_id = PaperSessionId::new(session_id_str)?;
        let mut session = load_session_view(&self.connection, &session_id)?.ok_or_else(|| {
            LabError::DataCorrupt("paper receipt references missing paper session".into())
        })?;
        refresh_view_freshness(&mut session, now);
        Ok(Some(PaperSessionCommandResponse { receipt, session }))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn paper_start_session(
        &mut self,
        request_id: &RequestId,
        payload_digest: &ContentHash,
        policy_ref: &PolicyRevisionRef,
        decision_interval: CandleInterval,
        execution_resolution: CandleInterval,
        latency_ms: u64,
        execution: &crate::contracts::ExecutionPolicy,
        markets: &[PaperMarketStart],
        now: UtcTimestamp,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        if markets.is_empty()
            || markets.len()
                > usize::try_from(MAX_PAPER_MARKETS_PER_SESSION)
                    .map_err(|_| LabError::ResourceLimit("market limit overflow".into()))?
        {
            return Err(LabError::InvalidConfig(format!(
                "paper session requires 1..={MAX_PAPER_MARKETS_PER_SESSION} markets"
            )));
        }
        let unique_markets: BTreeSet<&MarketId> = markets.iter().map(|m| &m.market).collect();
        if unique_markets.len() != markets.len() {
            return Err(LabError::InvalidConfig(
                "paper session markets must not contain duplicates".into(),
            ));
        }
        let active_count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_sessions WHERE state IN ('RUNNING', 'PAUSED')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if active_count >= i64::from(MAX_ACTIVE_PAPER_SESSIONS) {
            return Err(LabError::ResourceLimit(format!(
                "active paper session limit ({MAX_ACTIVE_PAPER_SESSIONS}) reached; stop an existing session first"
            )));
        }
        let policy_revision = self.load_policy_revision(policy_ref)?.ok_or_else(|| {
            LabError::InvalidConfig(format!(
                "unknown policy revision {}:{}",
                policy_ref.policy_id.as_str(),
                policy_ref.revision_id.as_str()
            ))
        })?;

        let session_id = PaperSessionId::from_seed(&format!(
            "paper:{}:{}",
            request_id.as_str(),
            payload_digest.as_str()
        ));

        let mut checkpoints = Vec::with_capacity(markets.len());
        let mut market_snapshots = Vec::with_capacity(markets.len());
        for market_start in markets {
            let mut checkpoint = PaperMarketCheckpoint::new(
                &session_id,
                market_start.market.clone(),
                &policy_revision.snapshot,
                decision_interval,
                execution_resolution,
                latency_ms,
                execution.clone(),
                market_start.initial_cash,
                market_start.costs.clone(),
                market_start.market_rules.clone(),
                now,
            )?;
            let warmup_limit = (checkpoint.warmup_required_bars.max(1) as usize)
                .saturating_mul(4)
                .clamp(64, 1_500);
            let pre_start_decision_bars = load_completed_bars_up_to(
                &self.connection,
                &market_start.market,
                decision_interval,
                None,
                now,
                warmup_limit,
            )?;
            let mut pre_start_source_bars = BTreeMap::new();
            for &source_interval in checkpoint.source_cursors.keys() {
                let bars = load_completed_bars_up_to(
                    &self.connection,
                    &market_start.market,
                    source_interval,
                    None,
                    now,
                    warmup_limit,
                )?;
                pre_start_source_bars.insert(source_interval, bars);
            }
            checkpoint.warm_up_pre_start(
                &pre_start_decision_bars,
                &pre_start_source_bars,
                None,
                now,
            )?;
            market_snapshots.push(checkpoint.to_market_snapshot()?);
            checkpoints.push(checkpoint);
        }

        let sequence = 0_u64;
        let last_evaluated_at = checkpoints
            .iter()
            .filter_map(|cp| cp.last_source_event_at)
            .max();
        let session_view = PaperSessionView {
            session_id: session_id.clone(),
            origin_request_id: request_id.clone(),
            state: PaperSessionState::Running,
            sequence,
            policy_ref: policy_ref.clone(),
            decision_interval,
            execution_resolution,
            mode: PaperSessionMode::Simulated,
            created_at: now,
            started_at: now,
            paused_at: None,
            stopped_at: None,
            last_evaluated_at,
            event_count: 0,
            markets: market_snapshots,
            recovery_cause: None,
        };
        let receipt = PaperSessionReceipt {
            request_id: request_id.clone(),
            session_id: session_id.clone(),
            action: PaperSessionCommandAction::Start,
            outcome: PaperCommandOutcome::Applied,
            expected_sequence: None,
            sequence,
            created_at: now,
            cancelled_pending_orders: Vec::new(),
        };

        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_session_row(&transaction, execution, latency_ms, None, &session_view)?;
        for (order, checkpoint) in checkpoints.iter().enumerate() {
            upsert_checkpoint_row(&transaction, order, sequence, now, checkpoint)?;
        }
        insert_receipt_row(&transaction, "start", payload_digest, &receipt)?;
        transaction.commit().map_err(sql_error)?;

        Ok(PaperSessionCommandResponse {
            receipt,
            session: session_view,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn paper_transition_session(
        &mut self,
        write: &PaperSessionWrite,
        payload_digest: &ContentHash,
        expected_recovery_requirements: Option<&[PaperPollRequirement]>,
        recovery_batches: &[(RawObjectRef, Vec<CandleObservation>)],
        now: UtcTimestamp,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        let request_id = write.request_id();
        let action = write.action();
        let (session_id, expected_sequence) = match write {
            PaperSessionWrite::Pause {
                session_id,
                expected_sequence,
                ..
            }
            | PaperSessionWrite::Resume {
                session_id,
                expected_sequence,
                ..
            }
            | PaperSessionWrite::Stop {
                session_id,
                expected_sequence,
                ..
            } => (session_id, *expected_sequence),
            PaperSessionWrite::Start { .. } => {
                return Err(LabError::InvalidConfig(
                    "paper session Start is not a transition".into(),
                ));
            }
        };
        if action != PaperSessionCommandAction::Resume
            && (expected_recovery_requirements.is_some() || !recovery_batches.is_empty())
        {
            return Err(LabError::InvalidConfig(
                "paper recovery data is only valid for a Resume command".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        if let Some((existing_action, existing_digest, receipt_session_id, receipt_json)) =
            transaction
                .query_row(
                    "SELECT action,payload_digest,session_id,receipt_json \
             FROM paper_request_receipts WHERE request_id=?1",
                    [request_id.as_str()],
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
                .map_err(sql_error)?
        {
            if existing_action != enum_text(&action)? || existing_digest != payload_digest.as_str()
            {
                return Err(LabError::Conflict(format!(
                    "request_id {} was already used for a different paper_session_write payload",
                    request_id.as_str()
                )));
            }
            let mut receipt: PaperSessionReceipt =
                serde_json::from_str(&receipt_json).map_err(json_error)?;
            receipt.outcome = PaperCommandOutcome::Replayed;
            let receipt_session_id = PaperSessionId::new(receipt_session_id)?;
            let mut session =
                load_session_view(&transaction, &receipt_session_id)?.ok_or_else(|| {
                    LabError::DataCorrupt("paper receipt references missing paper session".into())
                })?;
            refresh_view_freshness(&mut session, now);
            return Ok(PaperSessionCommandResponse { receipt, session });
        }
        let mut session_view = load_session_view(&transaction, session_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown paper session: {}", session_id.as_str()))
        })?;
        if session_view.sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "paper session sequence conflict for {}: expected {expected_sequence}, current {}",
                session_id.as_str(),
                session_view.sequence
            )));
        }
        let prior_state = session_view.state;
        let mut checkpoints = load_session_checkpoints(&transaction, session_id)?;

        let mut cancelled_order_events = Vec::new();
        let mut recovery_failure: Option<String> = None;
        let mut recovery_cause: Option<PaperRecoveryCause> = None;
        let mut catch_up_events = Vec::new();

        let (resulting_state, event_kind, action_label) = match (action, prior_state) {
            (PaperSessionCommandAction::Pause, PaperSessionState::Running) => {
                for checkpoint in &mut checkpoints {
                    let mut cancelled = checkpoint.cancel_pending_for_lifecycle(now)?;
                    cancelled_order_events.append(&mut cancelled);
                }
                session_view.paused_at = Some(now);
                (PaperSessionState::Paused, PaperEventKind::Pause, "pause")
            }
            (PaperSessionCommandAction::Resume, PaperSessionState::Paused) => {
                if expected_recovery_requirements.is_some() || !recovery_batches.is_empty() {
                    return Err(LabError::Conflict(
                        "feed-gap recovery preflight no longer matches the paused session".into(),
                    ));
                }
                let mut staged_checkpoints = checkpoints.clone();
                for checkpoint in &mut staged_checkpoints {
                    let outcome = catch_up_checkpoint_from_store(
                        &transaction,
                        &session_view,
                        checkpoint,
                        now,
                    )?;
                    if let Some(reason) = outcome.recovery_required {
                        recovery_failure = Some(reason);
                        recovery_cause = outcome.recovery_cause;
                        break;
                    }
                    if let Some(reason) = &checkpoint.recovery_reason {
                        recovery_failure = Some(reason.clone());
                        break;
                    }
                }
                if recovery_failure.is_some() {
                    session_view.recovery_cause = recovery_cause;
                    (
                        PaperSessionState::RecoveryRequired,
                        PaperEventKind::RecoveryRequired,
                        "resume",
                    )
                } else {
                    checkpoints = staged_checkpoints;
                    session_view.paused_at = None;
                    session_view.last_evaluated_at = Some(now);
                    session_view.recovery_cause = None;
                    (PaperSessionState::Running, PaperEventKind::Resume, "resume")
                }
            }
            (PaperSessionCommandAction::Resume, PaperSessionState::RecoveryRequired) => {
                if session_view.recovery_cause != Some(PaperRecoveryCause::FeedGap) {
                    return Err(LabError::Conflict(
                        "paper recovery cause is not eligible for feed-gap resume".into(),
                    ));
                }
                let Some(expected_requirements) = expected_recovery_requirements else {
                    return Err(LabError::Conflict(
                        "feed-gap resume requires bounded public-feed preflight".into(),
                    ));
                };
                verify_recovery_checkpoint_identity(&transaction, &session_view, &checkpoints)?;
                let current_requirements = recovery_poll_requirements(&checkpoints);
                if expected_requirements != current_requirements.as_slice() {
                    return Err(LabError::Conflict(
                        "paper recovery cursors changed after feed preflight".into(),
                    ));
                }
                if expected_requirements.len() != recovery_batches.len() {
                    return Err(LabError::Conflict(
                        "paper recovery batch count does not match requested cursors".into(),
                    ));
                }
                for (requirement, (raw_object, observations)) in
                    expected_requirements.iter().zip(recovery_batches)
                {
                    self.raw.verify(raw_object)?;
                    insert_raw(&transaction, raw_object)?;
                    for observation in observations {
                        if !observation.candle.completed
                            || observation.candle.close_time_utc > now
                            || observation.candle.market != requirement.market.code()
                            || observation.candle.interval != requirement.interval
                            || !observation.raw_object_ids.contains(&raw_object.id)
                        {
                            return Err(LabError::DataCorrupt(
                                "paper recovery observation does not match its fetched source batch"
                                    .into(),
                            ));
                        }
                        insert_observation(&transaction, observation)?;
                    }
                }
                let mut staged_checkpoints = checkpoints.clone();
                for checkpoint in &mut staged_checkpoints {
                    // The typed FEED_GAP gate above is the only authority that permits clearing
                    // this checkpoint-local explanatory text before bounded catch-up.
                    checkpoint.recovery_reason = None;
                }
                let mut staged_catch_up_events = Vec::new();
                for checkpoint in &mut staged_checkpoints {
                    let outcome = catch_up_checkpoint_from_store(
                        &transaction,
                        &session_view,
                        checkpoint,
                        now,
                    )?;
                    if outcome.events.iter().any(|event| {
                        !event.signals.is_empty()
                            || !event.orders.is_empty()
                            || !event.order_events.is_empty()
                            || !event.fills.is_empty()
                            || !event.episodes.is_empty()
                    }) {
                        return Err(LabError::DataCorrupt(
                            "paper feed-gap catch-up attempted a retroactive economic effect"
                                .into(),
                        ));
                    }
                    let failed_reason = outcome.recovery_required;
                    let failed_cause = outcome.recovery_cause;
                    staged_catch_up_events.extend(outcome.events);
                    if let Some(reason) = failed_reason {
                        recovery_failure = Some(reason);
                        recovery_cause = failed_cause;
                        break;
                    }
                    if let Some(reason) = &checkpoint.recovery_reason {
                        recovery_failure = Some(reason.clone());
                        break;
                    }
                }
                if recovery_failure.is_some() {
                    session_view.recovery_cause = recovery_cause;
                    (
                        PaperSessionState::RecoveryRequired,
                        PaperEventKind::RecoveryRequired,
                        "resume",
                    )
                } else {
                    checkpoints = staged_checkpoints;
                    catch_up_events = staged_catch_up_events;
                    session_view.paused_at = None;
                    session_view.last_evaluated_at = Some(now);
                    session_view.recovery_cause = None;
                    (PaperSessionState::Running, PaperEventKind::Resume, "resume")
                }
            }
            (
                PaperSessionCommandAction::Stop,
                PaperSessionState::Running
                | PaperSessionState::Paused
                | PaperSessionState::RecoveryRequired,
            ) => {
                for checkpoint in &mut checkpoints {
                    let mut cancelled = checkpoint.cancel_pending_for_lifecycle(now)?;
                    cancelled_order_events.append(&mut cancelled);
                }
                session_view.stopped_at = Some(now);
                session_view.recovery_cause = None;
                (PaperSessionState::Stopped, PaperEventKind::Stop, "stop")
            }
            _ => {
                return Err(LabError::Conflict(format!(
                    "cannot apply {action:?} to paper session {} in state {prior_state:?}",
                    session_id.as_str()
                )));
            }
        };

        catch_up_events.sort_by(|left, right| {
            left.finalized_at.cmp(&right.finalized_at).then_with(|| {
                left.market
                    .as_ref()
                    .map(MarketId::code)
                    .cmp(&right.market.as_ref().map(MarketId::code))
            })
        });
        let mut next_sequence = session_view.sequence;
        let mut catch_up_records = Vec::with_capacity(catch_up_events.len());
        for pending in catch_up_events {
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("paper session sequence overflow".into()))?;
            catch_up_records.push(materialize_candle_event(next_sequence, pending));
        }
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("paper session sequence overflow".into()))?;
        session_view.sequence = next_sequence;
        session_view.state = resulting_state;
        session_view.event_count = session_view
            .event_count
            .saturating_add(u64::try_from(catch_up_records.len()).unwrap_or(u64::MAX))
            .saturating_add(1);

        let mut snapshots = Vec::with_capacity(checkpoints.len());
        for (order, checkpoint) in checkpoints.iter_mut().enumerate() {
            checkpoint.refresh_freshness(now);
            snapshots.push(checkpoint.to_market_snapshot()?);
            upsert_checkpoint_row(&transaction, order, next_sequence, now, checkpoint)?;
        }
        session_view.markets = snapshots;

        let event = PaperSessionEvent {
            sequence: next_sequence,
            kind: event_kind,
            market: None,
            candle: None,
            source_event_at: None,
            finalized_at: None,
            control: Some(PaperControlEvent {
                action,
                cancelled_pending_orders: cancelled_order_events.clone(),
            }),
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: cancelled_order_events.clone(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        };
        let receipt = PaperSessionReceipt {
            request_id: request_id.clone(),
            session_id: session_id.clone(),
            action,
            outcome: PaperCommandOutcome::Applied,
            expected_sequence: Some(expected_sequence),
            sequence: next_sequence,
            created_at: now,
            cancelled_pending_orders: cancelled_order_events,
        };

        update_session_row(
            &transaction,
            expected_sequence,
            recovery_failure.as_deref(),
            &session_view,
        )?;
        for catch_up_event in &catch_up_records {
            insert_event_row(&transaction, session_id, catch_up_event, now)?;
        }
        insert_event_row(&transaction, session_id, &event, now)?;
        insert_receipt_row(&transaction, action_label, payload_digest, &receipt)?;
        transaction.commit().map_err(sql_error)?;

        Ok(PaperSessionCommandResponse {
            receipt,
            session: session_view,
        })
    }

    /// Return the distinct `(market, interval)` pairs required by all currently `RUNNING`
    /// paper sessions (`PAUSED`, `STOPPED`, and `RECOVERY_REQUIRED` sessions are never polled).
    ///
    /// # Errors
    /// Returns an error if SQLite queries or checkpoint deserialization fail.
    pub fn active_running_paper_polls(&self) -> Result<Vec<PaperPollRequirement>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id FROM paper_sessions WHERE state = 'RUNNING' ORDER BY session_id ASC",
            )
            .map_err(sql_error)?;
        let session_ids = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;

        let mut requirements: BTreeMap<(MarketId, CandleInterval), Option<UtcTimestamp>> =
            BTreeMap::new();
        for session_id_str in session_ids {
            let session_id = PaperSessionId::new(session_id_str)?;
            let Ok(checkpoints) = load_session_checkpoints(&self.connection, &session_id) else {
                continue;
            };
            for cp in checkpoints {
                let decision_interval = cp.plan.spec.decision_interval;
                merge_poll_cursor(
                    &mut requirements,
                    cp.market.clone(),
                    decision_interval,
                    cp.last_finalized_candle_at,
                );
                for (&source_interval, cursor) in &cp.source_cursors {
                    merge_poll_cursor(
                        &mut requirements,
                        cp.market.clone(),
                        source_interval,
                        cursor.last_bar_close_utc,
                    );
                }
            }
        }
        Ok(requirements
            .into_iter()
            .map(
                |((market, interval), last_bar_close_utc)| PaperPollRequirement {
                    market,
                    interval,
                    last_bar_close_utc,
                },
            )
            .collect())
    }

    /// Return `(market, interval)` pairs required by `RUNNING` paper sessions whose
    /// latest completed candle boundary at `now` is not yet stored in `candle_observations`.
    ///
    /// # Errors
    /// Returns an error if SQLite queries or boundary calculations fail.
    pub fn due_paper_network_polls(
        &self,
        now: UtcTimestamp,
    ) -> Result<Vec<PaperPollRequirement>, LabError> {
        let active = self.active_running_paper_polls()?;
        let mut due = Vec::new();
        for req in active {
            let Ok(boundary) = crate::scheduling::latest_completed_boundary(now, req.interval)
            else {
                continue;
            };
            if req.last_bar_close_utc.is_some_and(|last| last >= boundary) {
                continue;
            }
            let interval_str = enum_text(&req.interval)?;
            let max_stored_close: Option<i64> = self
                .connection
                .query_row(
                    "SELECT MAX(close_time_ms) FROM candle_observations \
                     WHERE market = ?1 AND interval = ?2 AND completed = 1 AND close_time_ms <= ?3",
                    params![req.market.code(), interval_str, timestamp_ms(now)],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if max_stored_close.is_none_or(|stored_ms| stored_ms < timestamp_ms(boundary)) {
                due.push(req);
            }
        }
        Ok(due)
    }

    /// Return exact per-session cursors for an explicit recoverable paper feed-gap resume.
    ///
    /// `None` means the session follows the ordinary lifecycle path. A legacy or unclassified
    /// recovery state is rejected; only a verified `FEED_GAP` checkpoint may request reads.
    ///
    /// # Errors
    /// Rejects stale sequence, non-resumable recovery causes, or invalid frozen checkpoints.
    pub fn paper_recovery_poll_requirements(
        &self,
        session_id: &PaperSessionId,
        expected_sequence: u64,
    ) -> Result<Option<Vec<PaperPollRequirement>>, LabError> {
        let session_view = load_session_view(&self.connection, session_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown paper session: {}", session_id.as_str()))
        })?;
        if session_view.sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "paper session sequence conflict for {}: expected {expected_sequence}, current {}",
                session_id.as_str(),
                session_view.sequence
            )));
        }
        if session_view.state != PaperSessionState::RecoveryRequired {
            return Ok(None);
        }
        if session_view.recovery_cause != Some(PaperRecoveryCause::FeedGap) {
            return Err(LabError::Conflict(
                "paper recovery cause is not eligible for feed-gap resume".into(),
            ));
        }

        let checkpoints = load_session_checkpoints(&self.connection, session_id)?;
        verify_recovery_checkpoint_identity(&self.connection, &session_view, &checkpoints)?;
        let stored_revision = self
            .load_policy_revision(&session_view.policy_ref)?
            .ok_or_else(|| {
                LabError::DataCorrupt("frozen paper policy revision is missing".into())
            })?;
        verify_frozen_policy_revision(&session_view, &checkpoints, &stored_revision.snapshot)?;
        Ok(Some(recovery_poll_requirements(&checkpoints)))
    }

    /// Persist any newly fetched completed candle batches and reconcile all `RUNNING` paper sessions
    /// forward in causal bar order.
    ///
    /// # Errors
    /// Persists session-local strategy data gaps as `RECOVERY_REQUIRED`. Propagates storage
    /// capacity, SQLite, and other non-data-gap errors.
    pub fn reconcile_running_paper_sessions(
        &mut self,
        fetched_batches: &[(RawObjectRef, Vec<CandleObservation>)],
        now: UtcTimestamp,
    ) -> Result<u32, LabError> {
        if !fetched_batches.is_empty() {
            self.enforce_storage_pressure()?;
            let tx = self.connection.transaction().map_err(sql_error)?;
            for (raw_object, observations) in fetched_batches {
                self.raw.verify(raw_object)?;
                insert_raw(&tx, raw_object)?;
                for obs in observations {
                    if obs.candle.completed && obs.candle.close_time_utc <= now {
                        insert_observation(&tx, obs)?;
                    }
                }
            }
            tx.commit().map_err(sql_error)?;
        }

        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id FROM paper_sessions WHERE state = 'RUNNING' ORDER BY created_at_utc ASC, session_id ASC",
            )
            .map_err(sql_error)?;
        let running_ids = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);

        let mut advanced_sessions = 0_u32;
        for session_id_str in running_ids {
            let session_id = PaperSessionId::new(session_id_str)?;
            match self.reconcile_one_running_session(&session_id, now) {
                Ok(true) => advanced_sessions = advanced_sessions.saturating_add(1),
                Ok(false) => {}
                Err(LabError::DataGap(reason)) => {
                    let transaction = self.connection.transaction().map_err(sql_error)?;
                    if let Some(session_view) = load_session_view(&transaction, &session_id)?
                        && session_view.state == PaperSessionState::Running
                    {
                        let expected_sequence = session_view.sequence;
                        let recovery_reason = format!("DATA_GAP: {reason}");
                        mark_session_recovery_required(
                            transaction,
                            session_view,
                            expected_sequence,
                            Some(PaperRecoveryCause::FeedGap),
                            &recovery_reason,
                            now,
                        )?;
                        advanced_sessions = advanced_sessions.saturating_add(1);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(advanced_sessions)
    }

    /// Feed explicit decision and source bars into a specific `RUNNING` paper session market
    /// in one atomic transaction.
    ///
    /// Used both for deterministic causal continuation verification and direct bar ingestion.
    ///
    /// # Errors
    /// Rejects non-running sessions, incomplete or future bars, or SQLite failures.
    #[allow(clippy::too_many_lines)]
    pub fn step_paper_session_bars(
        &mut self,
        session_id: &PaperSessionId,
        market: &MarketId,
        decision_bars: &[CandleObservation],
        source_bars: &BTreeMap<CandleInterval, Vec<CandleObservation>>,
        now: UtcTimestamp,
    ) -> Result<PaperSessionView, LabError> {
        self.enforce_storage_pressure()?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let mut session_view = load_session_view(&transaction, session_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown paper session: {}", session_id.as_str()))
        })?;
        if session_view.state != PaperSessionState::Running {
            return Err(LabError::Conflict(format!(
                "paper session {} is in state {:?}; only RUNNING sessions process new bars",
                session_id.as_str(),
                session_view.state
            )));
        }
        let expected_sequence = session_view.sequence;
        let mut checkpoints = match load_session_checkpoints(&transaction, session_id) {
            Ok(cps) => cps,
            Err(err) => {
                return mark_session_recovery_required(
                    transaction,
                    session_view,
                    expected_sequence,
                    None,
                    &format!("checkpoint integrity failure: {err}"),
                    now,
                );
            }
        };
        let cp_index = checkpoints
            .iter()
            .position(|cp| &cp.market == market)
            .ok_or_else(|| {
                LabError::InvalidConfig(format!(
                    "market {} is not part of paper session {}",
                    market.code(),
                    session_id.as_str()
                ))
            })?;

        for obs in decision_bars {
            if obs.candle.completed
                && obs.candle.close_time_utc <= now
                && !obs.raw_object_ids.is_empty()
            {
                let raw_exists: Option<i64> = transaction
                    .query_row(
                        "SELECT 1 FROM raw_objects WHERE id = ?1",
                        [obs.raw_object_ids[0].as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?;
                if raw_exists.is_some() {
                    insert_observation(&transaction, obs)?;
                }
            }
        }
        for bars in source_bars.values() {
            for obs in bars {
                if obs.candle.completed
                    && obs.candle.close_time_utc <= now
                    && !obs.raw_object_ids.is_empty()
                {
                    let raw_exists: Option<i64> = transaction
                        .query_row(
                            "SELECT 1 FROM raw_objects WHERE id = ?1",
                            [obs.raw_object_ids[0].as_str()],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(sql_error)?;
                    if raw_exists.is_some() {
                        insert_observation(&transaction, obs)?;
                    }
                }
            }
        }

        let mut pending_events = Vec::new();
        let mut recovery_reason: Option<String> = None;
        let mut recovery_cause: Option<PaperRecoveryCause> = None;
        {
            let checkpoint = &mut checkpoints[cp_index];
            if !decision_bars.is_empty() {
                let mut source_frame = PreparedPaperSourceFrame::new(
                    &checkpoint.evaluator,
                    &checkpoint.market,
                    source_bars,
                );
                for bar in decision_bars {
                    let outcome = checkpoint.step_bar_with_prepared_sources(
                        bar,
                        &mut source_frame,
                        None,
                        now,
                    )?;
                    if outcome.did_advance {
                        pending_events.extend(outcome.events);
                    }
                    if let Some(reason) = outcome.recovery_required {
                        recovery_reason = Some(reason);
                        recovery_cause = outcome.recovery_cause;
                        break;
                    }
                }
            }
        }

        if pending_events.is_empty() && recovery_reason.is_none() {
            refresh_view_freshness(&mut session_view, now);
            return Ok(session_view);
        }

        let mut next_sequence = session_view.sequence;
        let mut materialized_events = Vec::with_capacity(pending_events.len());
        for pending in pending_events {
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("paper session sequence overflow".into()))?;
            let event = materialize_candle_event(next_sequence, pending);
            materialized_events.push(event);
        }

        session_view.sequence = next_sequence;
        session_view.event_count = session_view
            .event_count
            .saturating_add(u64::try_from(materialized_events.len()).unwrap_or(0));
        session_view.last_evaluated_at = Some(now);
        if recovery_reason.is_some() {
            session_view.state = PaperSessionState::RecoveryRequired;
            session_view.recovery_cause = recovery_cause;
        }

        let mut snapshots = Vec::with_capacity(checkpoints.len());
        for (order, cp) in checkpoints.iter_mut().enumerate() {
            cp.refresh_freshness(now);
            snapshots.push(cp.to_market_snapshot()?);
            upsert_checkpoint_row(&transaction, order, next_sequence, now, cp)?;
        }
        session_view.markets = snapshots;

        update_session_row(
            &transaction,
            expected_sequence,
            recovery_reason.as_deref(),
            &session_view,
        )?;
        for event in &materialized_events {
            insert_event_row(&transaction, session_id, event, now)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(session_view)
    }

    #[allow(clippy::too_many_lines)]
    fn reconcile_one_running_session(
        &mut self,
        session_id: &PaperSessionId,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let Some(mut session_view) = load_session_view(&transaction, session_id)? else {
            return Ok(false);
        };
        if session_view.state != PaperSessionState::Running {
            return Ok(false);
        }
        let expected_sequence = session_view.sequence;
        let mut checkpoints = match load_session_checkpoints(&transaction, session_id) {
            Ok(cps) => cps,
            Err(err) => {
                mark_session_recovery_required(
                    transaction,
                    session_view,
                    expected_sequence,
                    None,
                    &format!("checkpoint integrity failure: {err}"),
                    now,
                )?;
                return Ok(true);
            }
        };

        let mut pending_events = Vec::new();
        let mut recovery_reason: Option<String> = None;
        let mut recovery_cause: Option<PaperRecoveryCause> = None;
        let mut warmup_changed = false;

        for checkpoint in &mut checkpoints {
            let decision_interval = checkpoint.plan.spec.decision_interval;
            if checkpoint.last_finalized_candle_at.is_none()
                && checkpoint.warmup_completed_bars < checkpoint.warmup_required_bars
            {
                let warmup_limit = (checkpoint.warmup_required_bars.max(1) as usize)
                    .saturating_mul(4)
                    .clamp(64, 1_500);
                let pre_decision = load_completed_bars_up_to(
                    &transaction,
                    &checkpoint.market,
                    decision_interval,
                    None,
                    session_view.started_at,
                    warmup_limit,
                )?;
                let mut pre_sources = BTreeMap::new();
                for &source_interval in checkpoint.source_cursors.keys() {
                    let bars = load_completed_bars_up_to(
                        &transaction,
                        &checkpoint.market,
                        source_interval,
                        None,
                        session_view.started_at,
                        warmup_limit,
                    )?;
                    pre_sources.insert(source_interval, bars);
                }
                let before = checkpoint.warmup_completed_bars;
                checkpoint.warm_up_pre_start(
                    &pre_decision,
                    &pre_sources,
                    None,
                    session_view.started_at,
                )?;
                if checkpoint.warmup_completed_bars != before {
                    warmup_changed = true;
                }
            }

            let required_pre_start_bars = checkpoint.warmup_required_bars.saturating_sub(1);
            if checkpoint.last_finalized_candle_at.is_none()
                && checkpoint.warmup_completed_bars < required_pre_start_bars
            {
                checkpoint.refresh_freshness(now);
                continue;
            }

            let after_close = checkpoint
                .last_finalized_candle_at
                .unwrap_or(session_view.started_at);
            let decision_bars = load_completed_bars_after(
                &transaction,
                &checkpoint.market,
                decision_interval,
                after_close,
                now,
                MAX_PAPER_RECOVERY_BARS as usize,
            )?;
            if decision_bars.is_empty() {
                checkpoint.last_heartbeat_at = Some(now);
                checkpoint.refresh_freshness(now);
                continue;
            }
            let mut source_bars = BTreeMap::new();
            for (&source_interval, cursor) in &checkpoint.source_cursors {
                let after_source = cursor.last_bar_close_utc.unwrap_or(session_view.started_at);
                let bars = load_completed_bars_after(
                    &transaction,
                    &checkpoint.market,
                    source_interval,
                    after_source,
                    now,
                    MAX_PAPER_RECOVERY_BARS as usize * 4,
                )?;
                source_bars.insert(source_interval, bars);
            }

            let mut source_frame = PreparedPaperSourceFrame::new(
                &checkpoint.evaluator,
                &checkpoint.market,
                &source_bars,
            );
            for bar in &decision_bars {
                let outcome =
                    checkpoint.step_bar_with_prepared_sources(bar, &mut source_frame, None, now)?;
                if outcome.did_advance {
                    pending_events.extend(outcome.events);
                }
                if let Some(reason) = outcome.recovery_required {
                    recovery_reason = Some(reason);
                    recovery_cause = outcome.recovery_cause;
                    break;
                }
            }
            if recovery_reason.is_some() {
                break;
            }
        }

        if pending_events.is_empty() && recovery_reason.is_none() && !warmup_changed {
            return Ok(false);
        }

        let mut next_sequence = session_view.sequence;
        let mut materialized_events = Vec::with_capacity(pending_events.len());
        for pending in pending_events {
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("paper session sequence overflow".into()))?;
            let event = materialize_candle_event(next_sequence, pending);
            materialized_events.push(event);
        }

        session_view.sequence = next_sequence;
        session_view.event_count = session_view
            .event_count
            .saturating_add(u64::try_from(materialized_events.len()).unwrap_or(0));
        session_view.last_evaluated_at = Some(now);
        if recovery_reason.is_some() {
            session_view.state = PaperSessionState::RecoveryRequired;
            session_view.recovery_cause = recovery_cause;
        }

        let mut snapshots = Vec::with_capacity(checkpoints.len());
        for (order, cp) in checkpoints.iter_mut().enumerate() {
            cp.refresh_freshness(now);
            snapshots.push(cp.to_market_snapshot()?);
            upsert_checkpoint_row(&transaction, order, next_sequence, now, cp)?;
        }
        session_view.markets = snapshots;

        update_session_row(
            &transaction,
            expected_sequence,
            recovery_reason.as_deref(),
            &session_view,
        )?;
        for event in &materialized_events {
            insert_event_row(&transaction, session_id, event, now)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(true)
    }
}

fn merge_poll_cursor(
    map: &mut BTreeMap<(MarketId, CandleInterval), Option<UtcTimestamp>>,
    market: MarketId,
    interval: CandleInterval,
    cursor: Option<UtcTimestamp>,
) {
    map.entry((market, interval))
        .and_modify(|existing| {
            *existing = match (*existing, cursor) {
                (Some(a), Some(b)) => Some(a.min(b)),
                _ => None,
            };
        })
        .or_insert(cursor);
}

fn recovery_poll_requirements(checkpoints: &[PaperMarketCheckpoint]) -> Vec<PaperPollRequirement> {
    let mut requirements = BTreeMap::new();
    for checkpoint in checkpoints {
        merge_poll_cursor(
            &mut requirements,
            checkpoint.market.clone(),
            checkpoint.plan.spec.decision_interval,
            checkpoint.last_finalized_candle_at,
        );
        for (&interval, cursor) in &checkpoint.source_cursors {
            merge_poll_cursor(
                &mut requirements,
                checkpoint.market.clone(),
                interval,
                cursor.last_bar_close_utc,
            );
        }
    }
    requirements
        .into_iter()
        .map(
            |((market, interval), last_bar_close_utc)| PaperPollRequirement {
                market,
                interval,
                last_bar_close_utc,
            },
        )
        .collect()
}

fn catch_up_checkpoint_from_store(
    transaction: &Transaction<'_>,
    session_view: &PaperSessionView,
    checkpoint: &mut PaperMarketCheckpoint,
    now: UtcTimestamp,
) -> Result<PaperStepOutcome, LabError> {
    let after_close = checkpoint
        .last_finalized_candle_at
        .unwrap_or(session_view.started_at);
    let decision_bars = load_completed_bars_after(
        transaction,
        &checkpoint.market,
        checkpoint.plan.spec.decision_interval,
        after_close,
        now,
        MAX_PAPER_RECOVERY_BARS as usize,
    )?;
    let mut source_bars = BTreeMap::new();
    for (&source_interval, cursor) in &checkpoint.source_cursors {
        let after_source = cursor.last_bar_close_utc.unwrap_or(session_view.started_at);
        let bars = load_completed_bars_after(
            transaction,
            &checkpoint.market,
            source_interval,
            after_source,
            now,
            MAX_PAPER_RECOVERY_BARS as usize * 4,
        )?;
        source_bars.insert(source_interval, bars);
    }
    checkpoint.catch_up_paused_bars(&decision_bars, &source_bars, None, now)
}

fn verify_recovery_checkpoint_identity(
    connection: &rusqlite::Connection,
    session_view: &PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
) -> Result<(), LabError> {
    if checkpoints.len() != session_view.markets.len() {
        return Err(LabError::DataCorrupt(
            "paper recovery checkpoint count does not match session markets".into(),
        ));
    }
    let stored_policy_digest: Option<String> = connection
        .query_row(
            "SELECT definition_digest FROM policy_revisions \
             WHERE policy_id=?1 AND revision_id=?2",
            params![
                session_view.policy_ref.policy_id.as_str(),
                session_view.policy_ref.revision_id.as_str()
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if stored_policy_digest.as_deref() != Some(session_view.policy_ref.definition_digest.as_str()) {
        return Err(LabError::DataCorrupt(
            "stored paper policy revision is missing or has a changed digest".into(),
        ));
    }

    let mut seen_markets = BTreeSet::new();
    for checkpoint in checkpoints {
        let snapshot = session_view
            .markets
            .iter()
            .find(|snapshot| snapshot.market == checkpoint.market)
            .ok_or_else(|| {
                LabError::DataCorrupt(
                    "paper recovery checkpoint market is absent from session view".into(),
                )
            })?;
        if !seen_markets.insert(checkpoint.market.clone()) {
            return Err(LabError::DataCorrupt(
                "duplicate paper recovery checkpoint market".into(),
            ));
        }
        checkpoint.verify_integrity(
            &session_view.session_id,
            &snapshot.market,
            &session_view.policy_ref,
            snapshot.initial_cash,
        )?;
        if checkpoint.plan.spec.decision_interval != session_view.decision_interval
            || !checkpoint
                .plan
                .spec
                .policy_selections
                .contains(&session_view.policy_ref)
        {
            return Err(LabError::DataCorrupt(
                "paper recovery plan does not match session policy/interval".into(),
            ));
        }
        let planned_revision = checkpoint
            .plan
            .policy_revisions
            .iter()
            .find(|revision| revision.reference == session_view.policy_ref)
            .ok_or_else(|| {
                LabError::DataCorrupt("paper plan is missing its frozen policy revision".into())
            })?;
        if ContentHash::of_value(&planned_revision.definition)?
            != session_view.policy_ref.definition_digest
        {
            return Err(LabError::DataCorrupt(
                "paper recovery plan policy revision digest changed".into(),
            ));
        }
    }
    Ok(())
}

fn verify_frozen_policy_revision(
    session_view: &PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
    stored_revision: &crate::contracts::FrozenPolicyRevision,
) -> Result<(), LabError> {
    if stored_revision.reference != session_view.policy_ref
        || ContentHash::of_value(&stored_revision.definition)?
            != session_view.policy_ref.definition_digest
    {
        return Err(LabError::DataCorrupt(
            "stored paper policy revision does not match session reference".into(),
        ));
    }
    for checkpoint in checkpoints {
        let planned_revision = checkpoint
            .plan
            .policy_revisions
            .iter()
            .find(|revision| revision.reference == session_view.policy_ref)
            .ok_or_else(|| {
                LabError::DataCorrupt("paper plan is missing its frozen policy revision".into())
            })?;
        if planned_revision.revision_number != stored_revision.revision_number
            || planned_revision.parent_revision_id != stored_revision.parent_revision_id
            || planned_revision.family != stored_revision.family
            || planned_revision.origin != stored_revision.origin
            || ContentHash::of_value(&planned_revision.definition)?
                != session_view.policy_ref.definition_digest
        {
            return Err(LabError::DataCorrupt(
                "paper recovery frozen policy revision does not match stored revision".into(),
            ));
        }
    }
    Ok(())
}

fn refresh_view_freshness(view: &mut PaperSessionView, now: UtcTimestamp) {
    let interval = view.decision_interval.duration();
    for market in &mut view.markets {
        if view.state == PaperSessionState::RecoveryRequired {
            market.freshness = crate::contracts::PaperFreshness::Offline;
            continue;
        }
        let Some(last_close) = market.last_finalized_candle_at else {
            market.freshness = crate::contracts::PaperFreshness::Offline;
            continue;
        };
        let age = now.0 - last_close.0;
        market.freshness = if age <= interval + interval / 2 {
            crate::contracts::PaperFreshness::Live
        } else if age <= interval * 2 {
            crate::contracts::PaperFreshness::Delayed
        } else if age <= interval * 4 {
            crate::contracts::PaperFreshness::Stale
        } else {
            crate::contracts::PaperFreshness::Offline
        };
    }
}

fn load_session_view(
    connection: &rusqlite::Connection,
    session_id: &PaperSessionId,
) -> Result<Option<PaperSessionView>, LabError> {
    let view_json = connection
        .query_row(
            "SELECT view_json FROM paper_sessions WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    view_json
        .map(|json| serde_json::from_str::<PaperSessionView>(&json).map_err(json_error))
        .transpose()
}

fn load_session_checkpoints(
    connection: &rusqlite::Connection,
    session_id: &PaperSessionId,
) -> Result<Vec<PaperMarketCheckpoint>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT schema_version, engine_version, checkpoint_digest, checkpoint_json \
             FROM paper_market_checkpoints \
             WHERE session_id = ?1 \
             ORDER BY market_order ASC",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(sql_error)?;
    let mut checkpoints = Vec::new();
    for row in rows {
        let (schema_version, engine_version, expected_digest, checkpoint_json) =
            row.map_err(sql_error)?;
        if schema_version != PAPER_CHECKPOINT_SCHEMA_VERSION {
            return Err(LabError::DataCorrupt(format!(
                "unsupported paper checkpoint schema version: {schema_version}"
            )));
        }
        if engine_version != ENGINE_VERSION {
            return Err(LabError::DataCorrupt(format!(
                "paper checkpoint engine version mismatch: expected {ENGINE_VERSION}, found {engine_version}"
            )));
        }
        let actual_digest = ContentHash::of_bytes(checkpoint_json.as_bytes());
        if actual_digest.as_str() != expected_digest {
            return Err(LabError::DataCorrupt(
                "paper market checkpoint digest mismatch".into(),
            ));
        }
        let checkpoint: PaperMarketCheckpoint =
            serde_json::from_str(&checkpoint_json).map_err(json_error)?;
        checkpoints.push(checkpoint);
    }
    if checkpoints.is_empty() {
        return Err(LabError::DataCorrupt(
            "paper session has no market checkpoints".into(),
        ));
    }
    Ok(checkpoints)
}

fn insert_session_row(
    transaction: &Transaction<'_>,
    execution: &crate::contracts::ExecutionPolicy,
    latency_ms: u64,
    recovery_reason: Option<&str>,
    view: &PaperSessionView,
) -> Result<(), LabError> {
    let policy_ref_json = serde_json::to_string(&view.policy_ref).map_err(json_error)?;
    let execution_json = serde_json::to_string(execution).map_err(json_error)?;
    let markets_json = serde_json::to_string(&view.markets).map_err(json_error)?;
    let view_json = serde_json::to_string(view).map_err(json_error)?;
    transaction
        .execute(
            "INSERT INTO paper_sessions( \
                session_id, origin_request_id, state, sequence, schema_version, engine_version, \
                policy_id, policy_revision_id, policy_ref_json, decision_interval, \
                execution_resolution, latency_ms, execution_json, markets_json, \
                created_at_utc, started_at_utc, paused_at_utc, stopped_at_utc, \
                last_evaluated_at_utc, event_count, recovery_reason, view_json \
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
            params![
                view.session_id.as_str(),
                view.origin_request_id.as_str(),
                enum_text(&view.state)?,
                u64_to_i64(view.sequence)?,
                SCHEMA_VERSION,
                ENGINE_VERSION,
                view.policy_ref.policy_id.as_str(),
                view.policy_ref.revision_id.as_str(),
                policy_ref_json,
                enum_text(&view.decision_interval)?,
                enum_text(&view.execution_resolution)?,
                u64_to_i64(latency_ms)?,
                execution_json,
                markets_json,
                view.created_at.to_string(),
                view.started_at.to_string(),
                view.paused_at.map(|ts| ts.to_string()),
                view.stopped_at.map(|ts| ts.to_string()),
                view.last_evaluated_at.map(|ts| ts.to_string()),
                u64_to_i64(view.event_count)?,
                recovery_reason,
                view_json,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn update_session_row(
    transaction: &Transaction<'_>,
    expected_sequence: u64,
    recovery_reason: Option<&str>,
    view: &PaperSessionView,
) -> Result<(), LabError> {
    let markets_json = serde_json::to_string(&view.markets).map_err(json_error)?;
    let view_json = serde_json::to_string(view).map_err(json_error)?;
    let changed = transaction
        .execute(
            "UPDATE paper_sessions SET \
                state = ?1, sequence = ?2, markets_json = ?3, paused_at_utc = ?4, \
                stopped_at_utc = ?5, last_evaluated_at_utc = ?6, event_count = ?7, \
                recovery_reason = ?8, view_json = ?9 \
             WHERE session_id = ?10 AND sequence = ?11",
            params![
                enum_text(&view.state)?,
                u64_to_i64(view.sequence)?,
                markets_json,
                view.paused_at.map(|ts| ts.to_string()),
                view.stopped_at.map(|ts| ts.to_string()),
                view.last_evaluated_at.map(|ts| ts.to_string()),
                u64_to_i64(view.event_count)?,
                recovery_reason,
                view_json,
                view.session_id.as_str(),
                u64_to_i64(expected_sequence)?,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::Conflict(
            "paper session state changed concurrently during compare-and-swap".into(),
        ));
    }
    Ok(())
}

fn upsert_checkpoint_row(
    transaction: &Transaction<'_>,
    market_order: usize,
    sequence: u64,
    updated_at: UtcTimestamp,
    checkpoint: &PaperMarketCheckpoint,
) -> Result<(), LabError> {
    let checkpoint_json = serde_json::to_string(checkpoint).map_err(json_error)?;
    let checkpoint_digest = ContentHash::of_bytes(checkpoint_json.as_bytes());
    let _ = semantic_digest("paper_checkpoint_v1", &checkpoint_digest)?;
    transaction
        .execute(
            "INSERT INTO paper_market_checkpoints( \
                session_id, market_order, market, sequence, schema_version, engine_version, \
                checkpoint_digest, checkpoint_json, updated_at_utc \
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
             ON CONFLICT(session_id, market) DO UPDATE SET \
                market_order = excluded.market_order, \
                sequence = excluded.sequence, \
                schema_version = excluded.schema_version, \
                engine_version = excluded.engine_version, \
                checkpoint_digest = excluded.checkpoint_digest, \
                checkpoint_json = excluded.checkpoint_json, \
                updated_at_utc = excluded.updated_at_utc",
            params![
                checkpoint.session_id.as_str(),
                usize_to_i64(market_order)?,
                checkpoint.market.code(),
                u64_to_i64(sequence)?,
                checkpoint.schema_version,
                checkpoint.engine_version,
                checkpoint_digest.as_str(),
                checkpoint_json,
                updated_at.to_string(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn insert_event_row(
    transaction: &Transaction<'_>,
    session_id: &PaperSessionId,
    event: &PaperSessionEvent,
    default_now: UtcTimestamp,
) -> Result<(), LabError> {
    let event_json = serde_json::to_string(event).map_err(json_error)?;
    let occurred_at = event
        .finalized_at
        .or(event.source_event_at)
        .unwrap_or(default_now);
    transaction
        .execute(
            "INSERT INTO paper_session_events( \
                session_id, sequence, market, kind, occurred_at_utc, event_json \
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                session_id.as_str(),
                u64_to_i64(event.sequence)?,
                event.market.as_ref().map(MarketId::code),
                enum_text(&event.kind)?,
                occurred_at.to_string(),
                event_json,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn insert_receipt_row(
    transaction: &Transaction<'_>,
    action: &str,
    payload_digest: &ContentHash,
    receipt: &PaperSessionReceipt,
) -> Result<(), LabError> {
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    transaction
        .execute(
            "INSERT INTO paper_request_receipts( \
                request_id, action, session_id, payload_digest, recorded_at_utc, receipt_json \
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                receipt.request_id.as_str(),
                action,
                receipt.session_id.as_str(),
                payload_digest.as_str(),
                receipt.created_at.to_string(),
                receipt_json,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn mark_session_recovery_required(
    transaction: Transaction<'_>,
    mut session_view: PaperSessionView,
    expected_sequence: u64,
    recovery_cause: Option<PaperRecoveryCause>,
    reason: &str,
    now: UtcTimestamp,
) -> Result<PaperSessionView, LabError> {
    let next_sequence = session_view
        .sequence
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("paper session sequence overflow".into()))?;
    session_view.sequence = next_sequence;
    session_view.state = PaperSessionState::RecoveryRequired;
    session_view.recovery_cause = recovery_cause;
    session_view.event_count = session_view.event_count.saturating_add(1);
    session_view.last_evaluated_at = Some(now);
    for market in &mut session_view.markets {
        market.freshness = crate::contracts::PaperFreshness::Offline;
    }
    let event = PaperSessionEvent {
        sequence: next_sequence,
        kind: PaperEventKind::RecoveryRequired,
        market: None,
        candle: None,
        source_event_at: Some(now),
        finalized_at: None,
        control: None,
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: Vec::new(),
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    };
    update_session_row(&transaction, expected_sequence, Some(reason), &session_view)?;
    insert_event_row(&transaction, &session_view.session_id, &event, now)?;
    transaction.commit().map_err(sql_error)?;
    Ok(session_view)
}

fn load_completed_bars_up_to(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    after_close_exclusive: Option<UtcTimestamp>,
    up_to_close_inclusive: UtcTimestamp,
    limit: usize,
) -> Result<Vec<CandleObservation>, LabError> {
    let interval_str = enum_text(&interval)?;
    let after_ms = after_close_exclusive.map(timestamp_ms);
    let up_to_ms = timestamp_ms(up_to_close_inclusive);
    let limit_i64 = usize_to_i64(limit)?;
    let mut statement = connection
        .prepare(
            "SELECT observation_json FROM ( \
                SELECT observation_json, close_time_ms \
                FROM candle_observations \
                WHERE market = ?1 AND interval = ?2 AND completed = 1 \
                  AND (?3 IS NULL OR close_time_ms > ?3) \
                  AND close_time_ms <= ?4 \
                ORDER BY close_time_ms DESC \
                LIMIT ?5 \
             ) ORDER BY close_time_ms ASC",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![market.code(), interval_str, after_ms, up_to_ms, limit_i64],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_error)?;
    let mut bars = Vec::new();
    for row in rows {
        let json = row.map_err(sql_error)?;
        let obs: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
        bars.push(obs);
    }
    Ok(bars)
}

fn load_completed_bars_after(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    after_close_exclusive: UtcTimestamp,
    up_to_close_inclusive: UtcTimestamp,
    limit: usize,
) -> Result<Vec<CandleObservation>, LabError> {
    let interval_str = enum_text(&interval)?;
    let after_ms = timestamp_ms(after_close_exclusive);
    let up_to_ms = timestamp_ms(up_to_close_inclusive);
    let limit_i64 = usize_to_i64(limit)?;
    let mut statement = connection
        .prepare(
            "SELECT observation_json FROM candle_observations \
             WHERE market = ?1 AND interval = ?2 AND completed = 1 \
               AND close_time_ms > ?3 AND close_time_ms <= ?4 \
             ORDER BY close_time_ms ASC \
             LIMIT ?5",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![market.code(), interval_str, after_ms, up_to_ms, limit_i64],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_error)?;
    let mut bars = Vec::new();
    for row in rows {
        let json = row.map_err(sql_error)?;
        let obs: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
        bars.push(obs);
    }
    Ok(bars)
}
