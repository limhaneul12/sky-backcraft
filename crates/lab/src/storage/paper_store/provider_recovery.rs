//! Proof-authorized, non-trading continuation of an existing paper checkpoint.

use super::{
    BTreeMap, BTreeSet, CandleInterval, CandleObservation, ContentHash, LabError,
    MAX_PAPER_RECOVERY_BARS, OptionalExtension, PAPER_SESSION_EVENT_INSERT_SQL, PaperEventKind,
    PaperMarketCheckpoint, PaperRecoveryCause, PaperSessionEvent, PaperSessionState,
    PaperSessionView, PaperStepOutcome, Transaction, UtcTimestamp, execute_paper_session_event_row,
    json_error, load_completed_bars_after, mark_session_recovery_required,
    materialize_candle_event, params, sql_error, update_session_row, upsert_checkpoint_row,
    usize_to_i64, verify_recovery_checkpoint_identity,
};
use crate::contracts::{
    PaperCheckpointTransition, PaperProviderRecovery, PaperProviderRecoveryPhase,
    ProviderAbsenceSpan, VerifiedProviderCoverage,
};

const MAX_SESSION_PROVIDER_PROOFS: usize = 256;
const SESSION_PROVIDER_PROOF_IDS_SQL: &str = "SELECT DISTINCT j.value FROM paper_session_events e, \
     json_each(e.event_json, '$.provider_recovery.proof_ids') j \
     WHERE e.session_id=?1 AND json_type(e.event_json, '$.provider_recovery.proof_ids') IS NOT NULL \
     LIMIT ?2";

pub(super) fn named_coverage(
    connection: &rusqlite::Connection,
    raw: &super::super::RawObjectStore,
    ids: &BTreeSet<ContentHash>,
) -> Result<VerifiedProviderCoverage, LabError> {
    if ids.len() > MAX_SESSION_PROVIDER_PROOFS {
        return Err(LabError::ResourceLimit(
            "paper provider proof reference limit".into(),
        ));
    }
    let mut verified = BTreeMap::new();
    let mut decode_memo = super::super::public_feed_recovery::ProviderAbsenceDecodeMemo::default();
    for id in ids {
        if verified.contains_key(id) {
            continue;
        }
        let json: Option<String> = connection
            .query_row(
                "SELECT proof_json FROM provider_absence_spans WHERE proof_id=?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        let span: ProviderAbsenceSpan = serde_json::from_str(&json.ok_or_else(|| {
            LabError::DataCorrupt("paper recovery references a missing provider proof".into())
        })?)
        .map_err(json_error)?;
        let found =
            super::super::public_feed_recovery::load_verified_provider_absence_spans_with_memo(
                connection,
                raw,
                &span.market,
                span.interval,
                span.start_utc,
                span.end_utc,
                &mut decode_memo,
            )?;
        if !found.iter().any(|candidate| candidate.proof_id == *id) {
            return Err(LabError::DataCorrupt(
                "paper recovery proof identity changed".into(),
            ));
        }
        for candidate in found {
            if ids.contains(&candidate.proof_id) {
                verified.insert(candidate.proof_id.clone(), candidate);
            }
        }
    }
    VerifiedProviderCoverage::from_verified_spans(verified.into_values().collect())
}

pub(super) fn session_coverage(
    connection: &rusqlite::Connection,
    raw: &super::super::RawObjectStore,
    session: &PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
    now: UtcTimestamp,
) -> Result<VerifiedProviderCoverage, LabError> {
    // Revalidate consumed proofs too: a later contradictory inner candle must
    // stop new decisions rather than silently invalidate already-used coverage.
    let mut statement = connection
        .prepare(SESSION_PROVIDER_PROOF_IDS_SQL)
        .map_err(sql_error)?;
    let strings = statement
        .query_map(
            params![
                session.session_id.as_str(),
                usize_to_i64(MAX_SESSION_PROVIDER_PROOFS + 1)?
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let ids = strings
        .into_iter()
        .map(ContentHash::try_from)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let old = named_coverage(connection, raw, &ids)?;
    let mut spans: BTreeMap<_, _> = old
        .spans()
        .iter()
        .cloned()
        .map(|span| (span.proof_id.clone(), span))
        .collect();
    for checkpoint in checkpoints {
        let requirements = std::iter::once((
            checkpoint.plan.spec.decision_interval,
            checkpoint.last_finalized_candle_at,
        ))
        .chain(
            checkpoint
                .source_cursors
                .iter()
                .map(|(interval, cursor)| (*interval, cursor.last_bar_close_utc)),
        );
        for (interval, cursor) in requirements {
            let Some(start) = cursor else {
                continue;
            };
            let end = crate::scheduling::latest_completed_boundary(now, interval)?;
            if start >= end {
                continue;
            }
            for span in super::super::public_feed_recovery::load_verified_provider_absence_spans(
                connection,
                raw,
                &checkpoint.market,
                interval,
                start,
                end,
            )? {
                if span
                    .snapshots
                    .iter()
                    .any(|sample| sample.fetched_at_utc > now)
                {
                    return Err(LabError::DataCorrupt("future paper provider proof".into()));
                }
                spans.insert(span.proof_id.clone(), span);
            }
        }
    }
    if spans.len() > MAX_SESSION_PROVIDER_PROOFS {
        return Err(LabError::ResourceLimit(
            "paper provider proof reference limit".into(),
        ));
    }
    VerifiedProviderCoverage::from_verified_spans(spans.into_values().collect())
}

pub(super) fn needs_recovery(
    session: &PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
    coverage: &VerifiedProviderCoverage,
) -> bool {
    if session.state == PaperSessionState::RecoveryRequired
        && session.recovery_cause == Some(PaperRecoveryCause::ProviderGap)
    {
        return true;
    }
    coverage.spans().iter().any(|span| {
        checkpoints.iter().any(|cp| {
            if cp.market != span.market {
                return false;
            }
            let cursor = if span.interval == cp.plan.spec.decision_interval {
                cp.last_finalized_candle_at
            } else {
                cp.source_cursors
                    .get(&span.interval)
                    .and_then(|cursor| cursor.last_bar_close_utc)
            };
            cursor.is_some_and(|last| last < span.end_utc)
        })
    })
}

fn transition(
    before: &PaperMarketCheckpoint,
    after: &PaperMarketCheckpoint,
) -> Result<PaperCheckpointTransition, LabError> {
    Ok(PaperCheckpointTransition {
        market: before.market.clone(),
        before_digest: before.checkpoint_digest()?,
        after_digest: after.checkpoint_digest()?,
        from_close: before.last_finalized_candle_at,
        to_close: after.last_finalized_candle_at,
    })
}

fn marker(
    phase: PaperProviderRecoveryPhase,
    coverage: &VerifiedProviderCoverage,
    target: UtcTimestamp,
    now: UtcTimestamp,
    transitions: Vec<PaperCheckpointTransition>,
    cancellations: Vec<crate::contracts::OrderRecord>,
) -> PaperSessionEvent {
    PaperSessionEvent {
        preparation: None,
        sequence: 0,
        kind: PaperEventKind::ProviderRecovery,
        market: None,
        candle: None,
        source_event_at: Some(now),
        finalized_at: None,
        control: None,
        provider_recovery: Some(PaperProviderRecovery {
            phase,
            processed_at: now,
            target_completed_at: target,
            proof_ids: coverage.proof_ids(),
            checkpoint_transitions: transitions,
        }),
        signals: Vec::new(),
        orders: Vec::new(),
        order_events: cancellations,
        fills: Vec::new(),
        account_marks: Vec::new(),
        episodes: Vec::new(),
    }
}

fn recovery_in_progress(
    transaction: &Transaction<'_>,
    session: &PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
) -> Result<bool, LabError> {
    let previous_json: Option<String> = transaction
        .query_row(
            "SELECT event_json FROM paper_session_events WHERE session_id=?1 \
         AND kind='PROVIDER_RECOVERY' ORDER BY sequence DESC LIMIT 1",
            [session.session_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let previous = previous_json
        .map(|json| serde_json::from_str::<PaperSessionEvent>(&json).map_err(json_error))
        .transpose()?;
    let continuing = session.state == PaperSessionState::RecoveryRequired
        && previous
            .as_ref()
            .and_then(|event| event.provider_recovery.as_ref())
            .is_some_and(|meta| meta.phase == PaperProviderRecoveryPhase::Progress);
    if continuing {
        let meta = previous
            .as_ref()
            .and_then(|event| event.provider_recovery.as_ref())
            .ok_or_else(|| LabError::DataCorrupt("provider recovery progress missing".into()))?;
        if meta.checkpoint_transitions.len() != checkpoints.len() {
            return Err(LabError::DataCorrupt(
                "provider recovery checkpoint count changed".into(),
            ));
        }
        for cp in checkpoints {
            let transition = meta
                .checkpoint_transitions
                .iter()
                .find(|item| item.market == cp.market)
                .ok_or_else(|| {
                    LabError::DataCorrupt("provider recovery checkpoint market changed".into())
                })?;
            if transition.after_digest != cp.checkpoint_digest()?
                || transition.to_close != cp.last_finalized_candle_at
            {
                return Err(LabError::DataCorrupt(
                    "provider recovery checkpoint diverged from committed progress".into(),
                ));
            }
        }
    }
    Ok(continuing)
}

fn begin_recovery(
    checkpoints: &mut [PaperMarketCheckpoint],
    coverage: &VerifiedProviderCoverage,
    target: UtcTimestamp,
    now: UtcTimestamp,
) -> Result<PaperSessionEvent, LabError> {
    let before = checkpoints.to_vec();
    let mut cancellations = Vec::new();
    for cp in checkpoints.iter_mut() {
        cancellations.extend(cp.cancel_pending_for_lifecycle(now)?);
    }
    let transitions = before
        .iter()
        .zip(checkpoints.iter())
        .map(|(old, new)| transition(old, new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(marker(
        PaperProviderRecoveryPhase::Started,
        coverage,
        target,
        now,
        transitions,
        cancellations,
    ))
}

pub(super) fn recover(
    transaction: Transaction<'_>,
    mut session: PaperSessionView,
    mut checkpoints: Vec<PaperMarketCheckpoint>,
    coverage: &VerifiedProviderCoverage,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    verify_recovery_checkpoint_identity(&transaction, &session, &checkpoints)?;
    if coverage.spans().is_empty() {
        return Ok(false);
    }
    let target = crate::scheduling::latest_completed_boundary(now, session.decision_interval)?;
    let expected_sequence = session.sequence;
    let continuing = recovery_in_progress(&transaction, &session, &checkpoints)?;
    let mut events = Vec::new();
    if !continuing {
        events.push(begin_recovery(&mut checkpoints, coverage, target, now)?);
    }
    let before_chunk = checkpoints.clone();
    let mut did_advance = false;
    for cp in &mut checkpoints {
        let outcome = stage_market_chunk(&transaction, &session, cp, coverage, now)?;
        if let Some(reason) = outcome.recovery_required {
            let detail =
                format!("Provider recovery is waiting for complete market coverage: {reason}");
            if session.state == PaperSessionState::Running
                || (!continuing && session.recovery_detail.as_ref() != Some(&detail))
            {
                mark_session_recovery_required(
                    transaction,
                    session,
                    expected_sequence,
                    Some(PaperRecoveryCause::ProviderGap),
                    &detail,
                    now,
                )?;
                return Ok(true);
            }
            return Ok(false);
        }
        did_advance |= outcome.did_advance;
        for pending in outcome.events {
            let mut event = materialize_candle_event(0, pending);
            event.provider_recovery = Some(PaperProviderRecovery {
                phase: PaperProviderRecoveryPhase::CatchUp,
                processed_at: now,
                target_completed_at: target,
                proof_ids: coverage.proof_ids(),
                checkpoint_transitions: Vec::new(),
            });
            events.push(event);
        }
    }
    let complete = checkpoints.iter().try_fold(true, |all, cp| {
        checkpoint_is_current(cp, coverage, now).map(|ready| all && ready)
    })?;
    if continuing && !did_advance && !complete {
        return Ok(false);
    }
    let phase = if complete {
        PaperProviderRecoveryPhase::Completed
    } else {
        PaperProviderRecoveryPhase::Progress
    };
    for cp in &mut checkpoints {
        cp.refresh_freshness(now);
    }
    let transitions = before_chunk
        .iter()
        .zip(&checkpoints)
        .map(|(old, new)| transition(old, new))
        .collect::<Result<Vec<_>, _>>()?;
    events.push(marker(
        phase,
        coverage,
        target,
        now,
        transitions,
        Vec::new(),
    ));
    session.state = if complete {
        PaperSessionState::Running
    } else {
        PaperSessionState::RecoveryRequired
    };
    session.recovery_cause = if complete {
        None
    } else {
        Some(PaperRecoveryCause::ProviderGap)
    };
    session.recovery_detail = if complete {
        None
    } else {
        Some(
            "Provider evidence verified; bounded actual-candle state catch-up is continuing".into(),
        )
    };
    session.paused_at = None;
    session.last_evaluated_at = Some(now);
    commit_recovery(
        transaction,
        session,
        &checkpoints,
        events,
        expected_sequence,
        now,
    )
}

fn commit_recovery(
    transaction: Transaction<'_>,
    mut session: PaperSessionView,
    checkpoints: &[PaperMarketCheckpoint],
    mut events: Vec<PaperSessionEvent>,
    expected_sequence: u64,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    for event in &mut events {
        session.sequence = session
            .sequence
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("paper recovery sequence overflow".into()))?;
        event.sequence = session.sequence;
    }
    session.event_count = session.event_count.saturating_add(
        u64::try_from(events.len())
            .map_err(|_| LabError::ResourceLimit("paper recovery event count overflow".into()))?,
    );
    session.markets = checkpoints
        .iter()
        .map(PaperMarketCheckpoint::to_market_snapshot)
        .collect::<Result<Vec<_>, _>>()?;
    for (order, cp) in checkpoints.iter().enumerate() {
        upsert_checkpoint_row(&transaction, order, session.sequence, now, cp)?;
    }
    update_session_row(
        &transaction,
        expected_sequence,
        session.recovery_detail.as_deref(),
        &session,
    )?;
    if !events.is_empty() {
        let mut statement = transaction
            .prepare(PAPER_SESSION_EVENT_INSERT_SQL)
            .map_err(sql_error)?;
        for event in &events {
            execute_paper_session_event_row(&session.session_id, event, now, |parameters| {
                statement.execute(parameters)
            })?;
        }
        drop(statement);
    }
    transaction.commit().map_err(sql_error)?;
    Ok(true)
}

struct RecoveryRows {
    decisions: Vec<CandleObservation>,
    sources: BTreeMap<CandleInterval, Vec<CandleObservation>>,
}

fn recovery_rows(
    transaction: &Transaction<'_>,
    session: &PaperSessionView,
    cp: &PaperMarketCheckpoint,
    coverage: &VerifiedProviderCoverage,
    now: UtcTimestamp,
) -> Result<RecoveryRows, LabError> {
    let after = cp.last_finalized_candle_at.unwrap_or(session.started_at);
    let mut decision_bars = unique_rows(load_completed_bars_after(
        transaction,
        &cp.market,
        cp.plan.spec.decision_interval,
        after,
        now,
        MAX_PAPER_RECOVERY_BARS as usize,
    )?)?;
    let mut sources = BTreeMap::new();
    for (&interval, cursor) in &cp.source_cursors {
        sources.insert(
            interval,
            unique_rows(load_completed_bars_after(
                transaction,
                &cp.market,
                interval,
                cursor.last_bar_close_utc.unwrap_or(session.started_at),
                now,
                source_row_limit(cp.plan.spec.decision_interval, interval)?,
            )?)?,
        );
    }
    // A finer source can need more than four rows per decision. Admit
    // only the decision prefix whose complete causal source horizon was
    // loaded; the next bounded chunk starts at this committed actual bar.
    let mut admitted = 0;
    for bar in &decision_bars {
        let mut available = true;
        for (&interval, cursor) in &cp.source_cursors {
            let expected =
                crate::scheduling::latest_completed_boundary(bar.candle.close_time_utc, interval)?;
            let through = sources
                .get(&interval)
                .and_then(|rows| rows.last())
                .map(|row| row.candle.close_time_utc)
                .or(cursor.last_bar_close_utc);
            if through.is_none_or(|last| {
                last < expected && !coverage.covers(&cp.market, interval, last, expected)
            }) {
                available = false;
                break;
            }
        }
        if !available {
            break;
        }
        admitted += 1;
    }
    decision_bars.truncate(admitted);
    Ok(RecoveryRows {
        decisions: decision_bars,
        sources,
    })
}

fn stage_market_chunk(
    transaction: &Transaction<'_>,
    session: &PaperSessionView,
    cp: &mut PaperMarketCheckpoint,
    coverage: &VerifiedProviderCoverage,
    now: UtcTimestamp,
) -> Result<PaperStepOutcome, LabError> {
    let rows = recovery_rows(transaction, session, cp, coverage, now)?;
    cp.recovery_reason = None;
    let before = cp.to_market_snapshot()?;
    let outcome =
        cp.catch_up_with_provider_coverage(&rows.decisions, &rows.sources, None, now, coverage)?;
    let after = cp.to_market_snapshot()?;
    if before.cash_total != after.cash_total
        || before.quantity != after.quantity
        || before.total_fees != after.total_fees
        || before.fill_count != after.fill_count
        || before.signal_count != after.signal_count
    {
        return Err(LabError::DataCorrupt(
            "provider catch-up changed trading facts".into(),
        ));
    }
    if outcome.events.iter().any(|event| {
        !event.signals.is_empty()
            || !event.orders.is_empty()
            || !event.order_events.is_empty()
            || !event.fills.is_empty()
            || !event.episodes.is_empty()
    }) {
        return Err(LabError::DataCorrupt(
            "provider catch-up emitted retroactive trading facts".into(),
        ));
    }
    Ok(outcome)
}

fn checkpoint_is_current(
    cp: &PaperMarketCheckpoint,
    coverage: &VerifiedProviderCoverage,
    now: UtcTimestamp,
) -> Result<bool, LabError> {
    let expected =
        crate::scheduling::latest_completed_boundary(now, cp.plan.spec.decision_interval)?;
    if cp.last_finalized_candle_at.is_none_or(|last| {
        !coverage.covers(&cp.market, cp.plan.spec.decision_interval, last, expected)
    }) {
        return Ok(false);
    }
    for (&interval, cursor) in &cp.source_cursors {
        let expected = crate::scheduling::latest_completed_boundary(expected, interval)?;
        if cursor
            .last_bar_close_utc
            .is_none_or(|last| !coverage.covers(&cp.market, interval, last, expected))
        {
            return Ok(false);
        }
    }
    Ok(cp.warmup_completed_bars >= cp.warmup_required_bars && cp.recovery_reason.is_none())
}

fn source_row_limit(decision: CandleInterval, source: CandleInterval) -> Result<usize, LabError> {
    let ratio = (decision.duration().num_seconds() / source.duration().num_seconds()).max(1);
    let minimum_pair = usize::try_from(ratio)
        .ok()
        .and_then(|ratio| ratio.checked_mul(2))
        .ok_or_else(|| LabError::ResourceLimit("paper source ratio overflow".into()))?;
    // The closed interval enum bounds D1/M1 at 2,880 rows: one pending
    // previous decision plus at least one new actual decision in a chunk.
    Ok((MAX_PAPER_RECOVERY_BARS as usize * 4).max(minimum_pair))
}

fn unique_rows(rows: Vec<CandleObservation>) -> Result<Vec<CandleObservation>, LabError> {
    let mut unique: Vec<CandleObservation> = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(previous) = unique.last()
            && previous.candle.close_time_utc == row.candle.close_time_utc
        {
            if previous.id != row.id
                || previous.content_digest != row.content_digest
                || previous.candle != row.candle
            {
                return Err(LabError::DataCorrupt(
                    "ambiguous paper recovery candle slot".into(),
                ));
            }
            continue;
        }
        unique.push(row);
    }
    Ok(unique)
}

#[cfg(test)]
mod tests;
