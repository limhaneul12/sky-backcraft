use super::{
    MAX_PROVIDER_RECOVERY_PAGES, MAX_PROVIDER_RECOVERY_ROWS, ProviderFeedRecoveryCursor, enum_text,
    insert_observation, insert_raw, json_error, sql_error, timestamp_ms,
};
use crate::contracts::{
    CandleInterval, CandleObservation, CandleRecord, ContentHash, LabError, MarketId,
    ProviderAbsenceAnchor, ProviderFeedRecoveryProgress, RawObjectId, RawObjectRef, UtcTimestamp,
    observation_digest,
};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeMap;

use super::Store;

#[derive(Clone, Copy)]
struct RecoveryPageRecord<'a> {
    market: &'a MarketId,
    interval: CandleInterval,
    started_at: UtcTimestamp,
    page_index: u32,
    repeat_of_page_index: Option<u32>,
    raw: &'a RawObjectRef,
    requested_to: Option<UtcTimestamp>,
    observations: &'a [CandleObservation],
    json: &'a str,
    digest: &'a ContentHash,
}

struct StagedPage {
    index: u32,
    repeat_of: Option<u32>,
    requested_to: Option<UtcTimestamp>,
    raw: RawObjectRef,
    observations: Vec<CandleObservation>,
}

#[derive(Clone, Copy)]
struct RecoveryCursorUpdate<'a> {
    market: &'a MarketId,
    interval: CandleInterval,
    accepted_anchor: Option<&'a ProviderAbsenceAnchor>,
    last_tip: Option<&'a ProviderAbsenceAnchor>,
    next_page_to: Option<UtcTimestamp>,
    pages_scanned: u32,
    rows_scanned: u64,
    target_count: u32,
    pending: bool,
    started_at: UtcTimestamp,
    updated_at: UtcTimestamp,
}

/// One complete, already fetched provider page admitted to durable backfill.
#[derive(Debug, Clone)]
pub(crate) struct PersistedProviderRecoveryPage {
    pub(crate) raw: RawObjectRef,
    pub(crate) market: MarketId,
    pub(crate) interval: CandleInterval,
    pub(crate) requested_to: Option<UtcTimestamp>,
    pub(crate) seed_anchor: Option<ProviderAbsenceAnchor>,
    pub(crate) target_count: u32,
    pub(crate) observations: Vec<CandleObservation>,
    pub(crate) pending_absence_confirmation: bool,
    pub(crate) as_of_utc: UtcTimestamp,
}

impl ProviderFeedRecoveryCursor {
    pub(crate) fn is_complete(&self) -> bool {
        self.progress.next_page_to_utc.is_none()
            && !self.progress.pending_absence_confirmation
            && (self.progress.accepted_anchor.is_some()
                || self.progress.rows_scanned >= u64::from(self.target_count))
    }
}

impl Store {
    /// Read the durable cursor for one public market/timeframe recovery.
    pub(crate) fn provider_feed_recovery_cursor(
        &self,
        market: &MarketId,
        interval: CandleInterval,
    ) -> Result<Option<ProviderFeedRecoveryCursor>, LabError> {
        load_cursor(&self.connection, market, interval)
    }

    /// Save one full validated page and advance the durable backwards cursor.
    /// A page with a pending absence read retains its current exclusive `to` cursor.
    #[expect(
        clippy::too_many_lines,
        reason = "one storage transaction verifies the raw page and atomically advances its bounded durable cursor"
    )]
    pub(crate) fn record_provider_feed_recovery_page(
        &mut self,
        page: PersistedProviderRecoveryPage,
    ) -> Result<ProviderFeedRecoveryCursor, LabError> {
        let PersistedProviderRecoveryPage {
            raw,
            market,
            interval,
            requested_to,
            seed_anchor,
            target_count,
            observations,
            pending_absence_confirmation,
            as_of_utc,
        } = page;
        let raw = &raw;
        let market = &market;
        let observations = &observations;
        if !(1..=199).contains(&target_count) {
            return Err(LabError::InvalidConfig(
                "provider recovery target count must be in 1..=199".into(),
            ));
        }
        if observations.is_empty() {
            return Err(LabError::DataGap(
                "provider recovery page has no completed observed anchors".into(),
            ));
        }
        let body = self.raw.read_verified(raw)?;
        super::validate_snapshot_page(raw, market, interval, observations, &body, as_of_utc)?;
        if super::request_to(raw)? != requested_to {
            return Err(LabError::DataCorrupt(
                "provider recovery cursor does not match the raw REST request".into(),
            ));
        }

        let previous = load_cursor(&self.connection, market, interval)?;
        let (started_at, accepted_anchor, last_tip, old_pages, old_rows, old_pending) =
            if let Some(cursor) = previous {
                if cursor.is_complete() {
                    if requested_to.is_some() {
                        return Err(LabError::Conflict(
                            "provider recovery cannot restart at an unrecorded historical cursor"
                                .into(),
                        ));
                    }
                    (
                        as_of_utc,
                        seed_anchor,
                        cursor.last_recovered_tip,
                        0,
                        0,
                        false,
                    )
                } else {
                    if cursor.target_count != target_count
                        || cursor.progress.accepted_anchor != seed_anchor
                    {
                        return Err(LabError::Conflict(
                            "provider recovery identity changed before its cursor completed".into(),
                        ));
                    }
                    if cursor.progress.next_page_to_utc != requested_to {
                        return Err(LabError::Conflict(
                            "provider recovery page does not match its durable exclusive cursor"
                                .into(),
                        ));
                    }
                    (
                        cursor.started_at_utc,
                        cursor.progress.accepted_anchor,
                        cursor.last_recovered_tip,
                        cursor.progress.pages_scanned,
                        cursor.progress.rows_scanned,
                        cursor.progress.pending_absence_confirmation,
                    )
                }
            } else {
                if requested_to.is_some() {
                    return Err(LabError::Conflict(
                        "provider recovery cannot begin from an unrecorded historical cursor"
                            .into(),
                    ));
                }
                (as_of_utc, seed_anchor, None, 0, 0, false)
            };

        let page_rows = u64::try_from(observations.len())
            .map_err(|_| LabError::ResourceLimit("recovery page row count overflow".into()))?;
        let pages_scanned = old_pages
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("recovery page count overflow".into()))?;
        let rows_scanned = old_rows
            .checked_add(page_rows)
            .ok_or_else(|| LabError::ResourceLimit("recovery row count overflow".into()))?;
        if pages_scanned > MAX_PROVIDER_RECOVERY_PAGES || rows_scanned > MAX_PROVIDER_RECOVERY_ROWS
        {
            return Err(LabError::ResourceLimit(
                "bounded public-feed recovery page/row budget exhausted".into(),
            ));
        }
        let reached_anchor = page_reaches_anchor(observations, accepted_anchor.as_ref());
        let enough_rows = accepted_anchor.is_none() && rows_scanned >= u64::from(target_count);
        let complete = !pending_absence_confirmation && (reached_anchor || enough_rows);
        let next_page_to = if pending_absence_confirmation {
            requested_to
        } else if complete {
            None
        } else {
            let next_to = UtcTimestamp(
                observations[0]
                    .candle
                    .open_time_utc
                    .0
                    .checked_add_signed(interval.duration())
                    .ok_or_else(|| {
                        LabError::DataCorrupt("provider recovery cursor overflow".into())
                    })?,
            );
            if requested_to.is_some_and(|cursor| next_to >= cursor) {
                return Err(LabError::Conflict(
                    "provider recovery page did not move its exclusive cursor backwards".into(),
                ));
            }
            Some(next_to)
        };

        let page_json = serde_json::to_string(observations).map_err(json_error)?;
        let page_digest = ContentHash::of_bytes(page_json.as_bytes());
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_raw(&transaction, raw)?;
        insert_recovery_page(
            &transaction,
            RecoveryPageRecord {
                market,
                interval,
                started_at,
                page_index: old_pages,
                repeat_of_page_index: old_pending.then(|| old_pages.saturating_sub(1)),
                raw,
                requested_to,
                observations,
                json: &page_json,
                digest: &page_digest,
            },
        )?;
        upsert_cursor(
            &transaction,
            RecoveryCursorUpdate {
                market,
                interval,
                accepted_anchor: accepted_anchor.as_ref(),
                last_tip: last_tip.as_ref(),
                next_page_to,
                pages_scanned,
                rows_scanned,
                target_count,
                pending: pending_absence_confirmation,
                started_at,
                updated_at: as_of_utc,
            },
        )?;
        transaction.commit().map_err(sql_error)?;
        self.provider_feed_recovery_cursor(market, interval)?
            .ok_or_else(|| {
                LabError::DataCorrupt("provider recovery cursor vanished after publication".into())
            })
    }

    /// Load and revalidate staged decoded rows without publishing them as canonical candles.
    pub(crate) fn provider_feed_recovery_observations(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        started_at_utc: UtcTimestamp,
    ) -> Result<Vec<CandleObservation>, LabError> {
        let pages = load_staged_pages(&self.connection, market, interval, started_at_utc)?;
        let mut merged: BTreeMap<UtcTimestamp, CandleObservation> = BTreeMap::new();
        let mut total_rows = 0_u64;
        for (raw, observations) in pages {
            let body = self.raw.read_verified(&raw)?;
            let as_of = raw.persisted_at.max(raw.fetched_at);
            super::validate_snapshot_page(&raw, market, interval, &observations, &body, as_of)?;
            for observation in observations {
                total_rows = total_rows.saturating_add(1);
                if total_rows > MAX_PROVIDER_RECOVERY_ROWS {
                    return Err(LabError::ResourceLimit(
                        "provider recovery staged observation row limit".into(),
                    ));
                }
                let open = observation.candle.open_time_utc;
                if let Some(existing) = merged.get_mut(&open) {
                    if existing.id != observation.id
                        || existing.content_digest != observation.content_digest
                        || existing.candle != observation.candle
                    {
                        // Every version was decoded from its own verified raw page.
                        // Keep the later version for revision diagnostics; canonical
                        // accepted economics are pinned downstream, after coverage.
                        *existing = observation;
                        continue;
                    }
                    for raw_id in observation.raw_object_ids {
                        if !existing.raw_object_ids.contains(&raw_id) {
                            existing.raw_object_ids.push(raw_id);
                        }
                    }
                } else {
                    merged.insert(open, observation);
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    /// Load canonical database rows at opens already present in a candidate window.
    /// The cache uses these as temporary pins so raw revisions remain observable.
    pub(crate) fn canonical_provider_feed_observations(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        observations: &[CandleObservation],
    ) -> Result<Vec<CandleObservation>, LabError> {
        let Some(first) = observations.first() else {
            return Ok(Vec::new());
        };
        let Some(last) = observations.last() else {
            return Err(LabError::DataCorrupt(
                "provider source window has no final observation".into(),
            ));
        };
        if observations
            .windows(2)
            .any(|pair| pair[0].candle.open_time_utc >= pair[1].candle.open_time_utc)
        {
            return Err(LabError::DataCorrupt(
                "provider source window is not strictly ordered".into(),
            ));
        }
        let known = load_canonical_provider_rows(
            &self.connection,
            market,
            interval,
            first.candle.open_time_utc,
            last.candle.open_time_utc,
        )?;
        let candidate_opens = observations
            .iter()
            .map(|row| row.candle.open_time_utc)
            .collect::<std::collections::BTreeSet<_>>();
        Ok(known
            .into_iter()
            .filter_map(|(open, row)| candidate_opens.contains(&open).then_some(row))
            .collect())
    }

    /// Repair missing canonical rows from a completed immutable recovery page chain.
    /// It returns rows only when it restored source history.
    pub(crate) fn repair_provider_feed_recovery_window(
        &mut self,
        market: &MarketId,
        interval: CandleInterval,
        started_at_utc: UtcTimestamp,
        as_of_utc: UtcTimestamp,
    ) -> Result<Option<Vec<CandleObservation>>, LabError> {
        let cursor = load_cursor(&self.connection, market, interval)?.ok_or_else(|| {
            LabError::Conflict(
                "provider recovery cursor is missing during source-window repair".into(),
            )
        })?;
        if !cursor.is_complete() || cursor.started_at_utc != started_at_utc {
            return Err(LabError::Conflict(
                "source-window repair does not match its completed recovery epoch".into(),
            ));
        }
        let Some(last_tip) = cursor.last_recovered_tip.as_ref() else {
            return Ok(None);
        };
        let mut staged =
            self.provider_feed_recovery_observations(market, interval, started_at_utc)?;
        let Some(first) = staged.first() else {
            return Err(LabError::DataCorrupt(
                "completed provider recovery epoch has no admitted source rows".into(),
            ));
        };
        let last = staged.last().ok_or_else(|| {
            LabError::DataCorrupt(
                "completed provider recovery epoch has no final source row".into(),
            )
        })?;
        if last.candle.open_time_utc != last_tip.open_time_utc {
            // An older archive cannot bridge a rolling watermark; a newer archive
            // may be complete while its new tip awaits independent confirmation.
            // Neither can publish through same-tip repair. The caller keeps normal
            // confirmation and bounded consumer-history recovery in control.
            return Ok(None);
        }
        let known = load_canonical_provider_rows(
            &self.connection,
            market,
            interval,
            first.candle.open_time_utc,
            last.candle.open_time_utc,
        )?;
        if staged
            .iter()
            .all(|row| known.contains_key(&row.candle.open_time_utc))
        {
            return Ok(None);
        }
        canonicalize_provider_rows(&mut staged, &known)?;
        let canonical_tip = staged.last().ok_or_else(|| {
            LabError::DataCorrupt("repaired provider window has no final row".into())
        })?;
        if canonical_tip.id != last_tip.observation_id
            || canonical_tip.content_digest != last_tip.content_digest
        {
            return Err(LabError::Conflict(
                "archived provider window disagrees with its durable source watermark".into(),
            ));
        }
        if let Some(accepted) = cursor.progress.accepted_anchor.as_ref()
            && !staged.iter().any(|row| {
                row.candle.open_time_utc == accepted.open_time_utc
                    && row.id == accepted.observation_id
                    && row.content_digest == accepted.content_digest
            })
        {
            return Err(LabError::Conflict(
                "archived provider window lost its accepted consumer boundary".into(),
            ));
        }
        self.confirm_provider_feed_recovery_window(market, interval, &staged, as_of_utc)?;
        Ok(Some(staged))
    }

    /// Decide whether a cold owner must retrieve missing actual consumer history.
    /// A current source tip cannot stand in for its consumer's older accepted row.
    pub(crate) fn provider_feed_history_needs_backfill(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        accepted: &ProviderAbsenceAnchor,
        watermark: &ProviderAbsenceAnchor,
    ) -> Result<bool, LabError> {
        if accepted.open_time_utc > watermark.open_time_utc {
            return Ok(true);
        }
        let known = load_canonical_provider_rows(
            &self.connection,
            market,
            interval,
            accepted.open_time_utc,
            watermark.open_time_utc,
        )?;
        let matches = |anchor: &ProviderAbsenceAnchor| {
            known.get(&anchor.open_time_utc).is_some_and(|row| {
                row.id == anchor.observation_id && row.content_digest == anchor.content_digest
            })
        };
        if !matches(accepted) || !matches(watermark) {
            return Ok(true);
        }
        let observations = known.into_values().collect::<Vec<_>>();
        match validate_provider_window_gaps(
            &self.connection,
            &self.raw,
            market,
            interval,
            &observations,
        ) {
            Ok(_) => Ok(false),
            Err(LabError::DataGap(_)) => Ok(true),
            Err(error) => Err(error),
        }
    }

    /// Publish a complete verified source window and its latest tip atomically.
    /// An unchanged tip still repairs missing immutable candle rows.
    pub(crate) fn confirm_provider_feed_recovery_window(
        &mut self,
        market: &MarketId,
        interval: CandleInterval,
        observations: &[CandleObservation],
        as_of_utc: UtcTimestamp,
    ) -> Result<(), LabError> {
        validate_provider_window_shape(market, interval, observations, as_of_utc)?;
        let first = observations.first().ok_or_else(|| {
            LabError::DataGap("confirmed public-feed source window is empty".into())
        })?;
        let last = observations.last().ok_or_else(|| {
            LabError::DataGap("confirmed public-feed source window is empty".into())
        })?;
        let known = load_canonical_provider_rows(
            &self.connection,
            market,
            interval,
            first.candle.open_time_utc,
            last.candle.open_time_utc,
        )?;
        validate_provider_window_raw_rows(
            &self.connection,
            &self.raw,
            market,
            interval,
            observations,
            &known,
            as_of_utc,
        )?;
        validate_provider_window_gaps(&self.connection, &self.raw, market, interval, observations)?;
        let mut canonical_rows = observations.to_vec();
        canonicalize_provider_rows(&mut canonical_rows, &known)?;
        let tip = canonical_rows.last().ok_or_else(|| {
            LabError::DataGap("confirmed public-feed source window is empty".into())
        })?;
        let anchor = ProviderAbsenceAnchor {
            observation_id: tip.id.clone(),
            open_time_utc: tip.candle.open_time_utc,
            content_digest: tip.content_digest.clone(),
        };
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let cursor = load_cursor(&transaction, market, interval)?.ok_or_else(|| {
            LabError::Conflict("provider recovery cursor is missing during tip confirmation".into())
        })?;
        if !cursor.is_complete() {
            return Err(LabError::Conflict(
                "provider recovery tip cannot advance before the accepted boundary is reached"
                    .into(),
            ));
        }
        if let Some(previous) = &cursor.last_recovered_tip {
            if anchor.open_time_utc < previous.open_time_utc {
                return Err(LabError::Conflict(
                    "confirmed public-feed tip moved behind its durable watermark".into(),
                ));
            }
            if anchor.open_time_utc == previous.open_time_utc && anchor != *previous {
                return Err(LabError::Conflict(
                    "confirmed public-feed tip changed at an accepted open time".into(),
                ));
            }
        }
        // Publish every observed candle through the generic immutable-row gate,
        // including its durable provider-absence contradiction check.
        for observation in &canonical_rows {
            insert_observation(&transaction, observation)?;
        }
        let tip_advanced = cursor
            .last_recovered_tip
            .as_ref()
            .is_none_or(|previous| anchor.open_time_utc > previous.open_time_utc);
        if !tip_advanced {
            transaction.commit().map_err(sql_error)?;
            return Ok(());
        }
        let expected_tip_json = cursor
            .last_recovered_tip
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(json_error)?;
        let changed = transaction
            .execute(
                "UPDATE provider_feed_recovery_cursors SET last_recovered_tip_json=?1,updated_at_ms=?2 \
                 WHERE market=?3 AND interval=?4 AND last_recovered_tip_json IS ?5",
                params![
                    serde_json::to_string(&anchor).map_err(json_error)?,
                    timestamp_ms(as_of_utc),
                    market.code(),
                    enum_text(&interval)?,
                    expected_tip_json,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "provider recovery cursor is missing during tip confirmation".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(())
    }
}

fn load_canonical_provider_rows(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
) -> Result<BTreeMap<UtcTimestamp, CandleObservation>, LabError> {
    if start > end {
        return Err(LabError::DataCorrupt(
            "provider source row query has a backward range".into(),
        ));
    }
    let limit = i64::try_from(MAX_PROVIDER_RECOVERY_ROWS.saturating_add(1))
        .map_err(|_| LabError::ResourceLimit("source row query limit overflow".into()))?;
    let mut statement = connection
        .prepare(
            "SELECT open_time_ms,observation_json FROM candle_observations \
             WHERE market=?1 AND interval=?2 AND open_time_ms>=?3 AND open_time_ms<=?4 \
             ORDER BY open_time_ms,id LIMIT ?5",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(start),
                timestamp_ms(end),
                limit,
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(sql_error)?;
    let mut canonical = BTreeMap::new();
    for row in rows {
        if canonical.len() >= usize::try_from(MAX_PROVIDER_RECOVERY_ROWS).unwrap_or(usize::MAX) {
            return Err(LabError::ResourceLimit(
                "canonical provider source rows exceed their bound".into(),
            ));
        }
        let (open_ms, json) = row.map_err(sql_error)?;
        let open = super::timestamp_from_ms(open_ms, "canonical provider open")?;
        let observation: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
        validate_canonical_provider_row(&observation, market, interval, open)?;
        if canonical.insert(open, observation).is_some() {
            return Err(LabError::Conflict(
                "multiple canonical observations occupy one provider candle slot".into(),
            ));
        }
    }
    Ok(canonical)
}

fn canonicalize_provider_rows(
    observations: &mut [CandleObservation],
    canonical: &BTreeMap<UtcTimestamp, CandleObservation>,
) -> Result<(), LabError> {
    for observation in observations {
        if let Some(existing) = canonical.get(&observation.candle.open_time_utc) {
            if observation.candle.market != existing.candle.market
                || observation.candle.interval != existing.candle.interval
                || observation.candle.open_time_utc != existing.candle.open_time_utc
                || observation.candle.close_time_utc != existing.candle.close_time_utc
                || !observation.candle.completed
                || !existing.candle.completed
            {
                return Err(LabError::DataCorrupt(
                    "provider source revision changed an accepted candle slot".into(),
                ));
            }
            *observation = existing.clone();
        }
    }
    Ok(())
}

fn validate_provider_window_shape(
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
    as_of_utc: UtcTimestamp,
) -> Result<(), LabError> {
    if observations.is_empty() {
        return Err(LabError::DataGap(
            "confirmed public-feed source window is empty".into(),
        ));
    }
    if observations.len() > usize::try_from(MAX_PROVIDER_RECOVERY_ROWS).unwrap_or(usize::MAX) {
        return Err(LabError::ResourceLimit(
            "confirmed provider source window exceeds its row bound".into(),
        ));
    }
    let duration = interval.duration();
    let duration_seconds = duration.num_seconds();
    for observation in observations {
        let candle = &observation.candle;
        let expected_close = candle.open_time_utc.0.checked_add_signed(duration);
        if candle.market != market.code()
            || candle.interval != interval
            || !candle.completed
            || candle.close_time_utc > as_of_utc
            || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
            || candle.close_time_utc.0.timestamp_subsec_nanos() != 0
            || duration_seconds <= 0
            || candle
                .open_time_utc
                .0
                .timestamp()
                .rem_euclid(duration_seconds)
                != 0
            || expected_close != Some(candle.close_time_utc.0)
            || observation.raw_object_ids.is_empty()
            || observation_digest(candle)? != observation.content_digest
            || crate::contracts::ObservationId::from_seed(observation.content_digest.as_str())
                != observation.id
        {
            return Err(LabError::DataCorrupt(
                "confirmed provider source window has an invalid candle or identity".into(),
            ));
        }
    }
    for pair in observations.windows(2) {
        if pair[0].candle.open_time_utc >= pair[1].candle.open_time_utc
            || pair[0].candle.close_time_utc > pair[1].candle.open_time_utc
        {
            return Err(LabError::DataCorrupt(
                "confirmed provider source window is unordered or overlapping".into(),
            ));
        }
    }
    Ok(())
}

fn validate_provider_window_raw_rows(
    connection: &rusqlite::Connection,
    raw_store: &super::super::RawObjectStore,
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
    canonical: &BTreeMap<UtcTimestamp, CandleObservation>,
    as_of_utc: UtcTimestamp,
) -> Result<(), LabError> {
    let uncanonicalized = observations
        .iter()
        .filter(|observation| {
            !canonical
                .get(&observation.candle.open_time_utc)
                .is_some_and(|existing| {
                    existing.id == observation.id
                        && existing.content_digest == observation.content_digest
                        && existing.candle == observation.candle
                })
        })
        .collect::<Vec<_>>();
    let raw_ids = uncanonicalized
        .iter()
        .filter_map(|observation| observation.raw_object_ids.first().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if raw_ids.len() > usize::try_from(MAX_PROVIDER_RECOVERY_PAGES + 2).unwrap_or(usize::MAX) {
        return Err(LabError::ResourceLimit(
            "provider source window references too many REST pages".into(),
        ));
    }
    let mut candles_by_raw = BTreeMap::<RawObjectId, Vec<CandleRecord>>::new();
    for raw_id in raw_ids {
        let raw = super::load_raw_ref(connection, &raw_id)?;
        let body = raw_store.read_verified(&raw)?;
        if raw.http_status != 200
            || raw.transport.is_some()
            || raw.fetched_at > as_of_utc
            || raw.persisted_at > as_of_utc
            || raw.raw_bytes > u64::try_from(super::super::MAX_RAW_OBJECT_BYTES).unwrap_or(u64::MAX)
            || ContentHash::of_bytes(&body) != raw.raw_sha256
        {
            return Err(LabError::DataCorrupt(
                "provider source row is not bound to a bounded successful REST object".into(),
            ));
        }
        super::validate_snapshot_page_header(&raw, market, interval, &body)?;
        let candles =
            crate::upbit_candles::decode_candles(&body, market, interval, raw.fetched_at)?
                .into_iter()
                .filter(|candle| candle.completed)
                .collect::<Vec<_>>();
        candles_by_raw.insert(raw_id, candles);
    }
    for observation in uncanonicalized {
        let raw_id = observation.raw_object_ids.first().ok_or_else(|| {
            LabError::DataCorrupt("provider source row has no REST raw witness".into())
        })?;
        if !candles_by_raw
            .get(raw_id)
            .is_some_and(|candles| candles.iter().any(|candle| candle == &observation.candle))
        {
            return Err(LabError::DataCorrupt(
                "provider source observation does not match its exact REST raw candle".into(),
            ));
        }
    }
    Ok(())
}

fn validate_provider_window_gaps(
    connection: &rusqlite::Connection,
    raw_store: &super::super::RawObjectStore,
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
) -> Result<Vec<crate::contracts::ProviderAbsenceSpan>, LabError> {
    let first = observations
        .first()
        .ok_or_else(|| LabError::DataGap("confirmed public-feed source window is empty".into()))?;
    let last = observations
        .last()
        .ok_or_else(|| LabError::DataGap("confirmed public-feed source window is empty".into()))?;
    let spans = super::load_verified_provider_absence_spans(
        connection,
        raw_store,
        market,
        interval,
        first.candle.open_time_utc,
        last.candle.close_time_utc,
    )?;
    for span in &spans {
        if observations.iter().any(|observation| {
            observation.candle.open_time_utc >= span.start_utc
                && observation.candle.open_time_utc < span.end_utc
        }) {
            return Err(LabError::Conflict(
                "confirmed provider-absence interval contains an observed source candle".into(),
            ));
        }
    }
    for pair in observations.windows(2) {
        if pair[0].candle.close_time_utc == pair[1].candle.open_time_utc {
            continue;
        }
        if !spans.iter().any(|span| {
            span.start_utc == pair[0].candle.close_time_utc
                && span.end_utc == pair[1].candle.open_time_utc
                && span.pre_anchor.open_time_utc == pair[0].candle.open_time_utc
                && span.post_anchor.open_time_utc == pair[1].candle.open_time_utc
        }) {
            return Err(LabError::DataGap(
                "confirmed provider source window contains an unexplained candle gap".into(),
            ));
        }
    }
    Ok(spans)
}

fn validate_canonical_provider_row(
    observation: &CandleObservation,
    market: &MarketId,
    interval: CandleInterval,
    sql_open: UtcTimestamp,
) -> Result<(), LabError> {
    let candle = &observation.candle;
    if candle.market != market.code()
        || candle.interval != interval
        || candle.open_time_utc != sql_open
        || !candle.completed
        || observation_digest(candle)? != observation.content_digest
        || crate::contracts::ObservationId::from_seed(observation.content_digest.as_str())
            != observation.id
    {
        return Err(LabError::DataCorrupt(
            "canonical provider observation disagrees with its SQL slot or digest".into(),
        ));
    }
    Ok(())
}

fn load_cursor(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
) -> Result<Option<ProviderFeedRecoveryCursor>, LabError> {
    let row = connection
        .query_row(
            "SELECT accepted_anchor_json,last_recovered_tip_json,next_page_to_ms,pages_scanned,\
             rows_scanned,target_count,pending_absence_confirmation,started_at_ms,updated_at_ms \
             FROM provider_feed_recovery_cursors WHERE market=?1 AND interval=?2",
            params![market.code(), enum_text(&interval)?],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, bool>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(
        |(anchor_json, tip_json, next_ms, pages, rows, target, pending, started, _updated)| {
            let pages_scanned = u32::try_from(pages).map_err(|_| {
                LabError::DataCorrupt("negative provider recovery page count".into())
            })?;
            let rows_scanned = u64::try_from(rows).map_err(|_| {
                LabError::DataCorrupt("negative provider recovery row count".into())
            })?;
            let target_count = u32::try_from(target).map_err(|_| {
                LabError::DataCorrupt("invalid provider recovery target count".into())
            })?;
            if pages_scanned > MAX_PROVIDER_RECOVERY_PAGES
                || rows_scanned > MAX_PROVIDER_RECOVERY_ROWS
                || !(1..=199).contains(&target_count)
            {
                return Err(LabError::DataCorrupt(
                    "provider recovery cursor exceeds its persisted bounds".into(),
                ));
            }
            let accepted_anchor = anchor_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?;
            let last_recovered_tip = tip_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?;
            Ok(ProviderFeedRecoveryCursor {
                market: market.clone(),
                interval,
                progress: ProviderFeedRecoveryProgress {
                    accepted_anchor,
                    next_page_to_utc: next_ms
                        .map(|ms| timestamp_from_ms(ms, "next provider page"))
                        .transpose()?,
                    pages_scanned,
                    rows_scanned,
                    page_limit: MAX_PROVIDER_RECOVERY_PAGES,
                    row_limit: MAX_PROVIDER_RECOVERY_ROWS,
                    pending_absence_confirmation: pending,
                },
                target_count,
                started_at_utc: timestamp_from_ms(started, "provider recovery start")?,
                last_recovered_tip,
            })
        },
    )
    .transpose()
}

fn insert_recovery_page(
    transaction: &Transaction<'_>,
    page: RecoveryPageRecord<'_>,
) -> Result<(), LabError> {
    let RecoveryPageRecord {
        market,
        interval,
        started_at,
        page_index,
        repeat_of_page_index,
        raw,
        requested_to,
        observations,
        json,
        digest,
    } = page;
    transaction
        .execute(
            "INSERT INTO provider_feed_recovery_pages(\
             market,interval,recovery_started_at_ms,page_index,repeat_of_page_index,raw_object_id,requested_to_ms,\
             row_count,observations_json,page_digest\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(started_at),
                i64::from(page_index),
                repeat_of_page_index.map(i64::from),
                raw.id.as_str(),
                requested_to.map(timestamp_ms),
                i64::try_from(observations.len())
                    .map_err(|_| LabError::ResourceLimit("recovery page row count overflow".into()))?,
                json,
                digest.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn upsert_cursor(
    transaction: &Transaction<'_>,
    update: RecoveryCursorUpdate<'_>,
) -> Result<(), LabError> {
    let RecoveryCursorUpdate {
        market,
        interval,
        accepted_anchor,
        last_tip,
        next_page_to,
        pages_scanned,
        rows_scanned,
        target_count,
        pending,
        started_at,
        updated_at,
    } = update;
    transaction
        .execute(
            "INSERT INTO provider_feed_recovery_cursors(\
             market,interval,accepted_anchor_json,last_recovered_tip_json,next_page_to_ms,\
             pages_scanned,rows_scanned,target_count,pending_absence_confirmation,started_at_ms,updated_at_ms\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) \
             ON CONFLICT(market,interval) DO UPDATE SET \
             accepted_anchor_json=excluded.accepted_anchor_json,\
             last_recovered_tip_json=excluded.last_recovered_tip_json,\
             next_page_to_ms=excluded.next_page_to_ms,pages_scanned=excluded.pages_scanned,\
             rows_scanned=excluded.rows_scanned,target_count=excluded.target_count,\
             pending_absence_confirmation=excluded.pending_absence_confirmation,\
             started_at_ms=excluded.started_at_ms,updated_at_ms=excluded.updated_at_ms",
            params![
                market.code(),
                enum_text(&interval)?,
                accepted_anchor.map(serde_json::to_string).transpose().map_err(json_error)?,
                last_tip.map(serde_json::to_string).transpose().map_err(json_error)?,
                next_page_to.map(timestamp_ms),
                i64::from(pages_scanned),
                i64::try_from(rows_scanned)
                    .map_err(|_| LabError::ResourceLimit("recovery row count overflow".into()))?,
                i64::from(target_count),
                pending,
                timestamp_ms(started_at),
                timestamp_ms(updated_at).max(timestamp_ms(started_at)),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn load_staged_pages(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    started_at: UtcTimestamp,
) -> Result<Vec<(RawObjectRef, Vec<CandleObservation>)>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT page_index,repeat_of_page_index,raw_object_id,requested_to_ms,row_count,observations_json,page_digest \
             FROM provider_feed_recovery_pages \
             WHERE market=?1 AND interval=?2 AND recovery_started_at_ms=?3 ORDER BY page_index LIMIT ?4",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(started_at),
                i64::from(MAX_PROVIDER_RECOVERY_PAGES) + 1,
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut pages: Vec<StagedPage> = Vec::new();
    for row in rows {
        let (index, repeat_of, raw_id, requested_to, count, json, digest) =
            row.map_err(sql_error)?;
        if pages.len() >= usize::try_from(MAX_PROVIDER_RECOVERY_PAGES).unwrap_or(usize::MAX)
            || ContentHash::of_bytes(json.as_bytes()).as_str() != digest
        {
            return Err(LabError::DataCorrupt(
                "provider recovery staged page count or digest is invalid".into(),
            ));
        }
        let raw_id = crate::contracts::RawObjectId::new(raw_id)?;
        let raw = super::load_raw_ref(connection, &raw_id)?;
        let observations: Vec<CandleObservation> =
            serde_json::from_str(&json).map_err(json_error)?;
        if i64::try_from(observations.len()).ok() != Some(count) || observations.len() > 200 {
            return Err(LabError::DataCorrupt(
                "provider recovery staged page row count is invalid".into(),
            ));
        }
        pages.push(StagedPage {
            index: u32::try_from(index).map_err(|_| {
                LabError::DataCorrupt("negative provider recovery page index".into())
            })?,
            repeat_of: repeat_of
                .map(|value| {
                    u32::try_from(value)
                        .map_err(|_| LabError::DataCorrupt("negative repeated page index".into()))
                })
                .transpose()?,
            requested_to: requested_to
                .map(|value| timestamp_from_ms(value, "provider page cursor"))
                .transpose()?,
            raw,
            observations,
        });
    }
    validate_staged_page_chain(&pages, interval)?;
    Ok(pages
        .into_iter()
        .map(|page| (page.raw, page.observations))
        .collect())
}

fn validate_staged_page_chain(
    pages: &[StagedPage],
    interval: CandleInterval,
) -> Result<(), LabError> {
    for (offset, page) in pages.iter().enumerate() {
        if super::request_to(&page.raw)? != page.requested_to {
            return Err(LabError::DataCorrupt(
                "provider recovery page link does not match its exact raw request".into(),
            ));
        }
        let expected_index = u32::try_from(offset)
            .map_err(|_| LabError::ResourceLimit("recovery page index overflow".into()))?;
        if page.index != expected_index {
            return Err(LabError::DataCorrupt(
                "provider recovery page indices are not contiguous".into(),
            ));
        }
        if offset == 0 {
            if page.repeat_of.is_some() || page.requested_to.is_some() {
                return Err(LabError::DataCorrupt(
                    "provider recovery chain does not start with the latest REST page".into(),
                ));
            }
            continue;
        }
        let previous = &pages[offset - 1];
        if page.repeat_of == Some(previous.index) {
            if page.requested_to != previous.requested_to {
                return Err(LabError::DataCorrupt(
                    "provider confirmation reread changed its exclusive request cursor".into(),
                ));
            }
        } else {
            let expected_to = previous
                .observations
                .first()
                .map(|row| {
                    row.candle
                        .open_time_utc
                        .0
                        .checked_add_signed(interval.duration())
                        .map(UtcTimestamp)
                        .ok_or_else(|| {
                            LabError::DataCorrupt("provider page-chain cursor overflow".into())
                        })
                })
                .transpose()?;
            if page.repeat_of.is_some() || page.requested_to != expected_to {
                return Err(LabError::DataCorrupt(
                    "provider historical pages do not form an exclusive backwards cursor chain"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

fn page_reaches_anchor(
    observations: &[CandleObservation],
    anchor: Option<&ProviderAbsenceAnchor>,
) -> bool {
    let Some(anchor) = anchor else {
        return false;
    };
    for observation in observations {
        if observation.candle.open_time_utc == anchor.open_time_utc {
            // The boundary identity fixes the accepted slot and backfill stop point.
            // Provider economics may have been revised since the consumer accepted
            // its canonical row; FeedCache handles that revision without rewriting it.
            return true;
        }
    }
    false
}

fn timestamp_from_ms(value: i64, label: &str) -> Result<UtcTimestamp, LabError> {
    chrono::DateTime::from_timestamp_millis(value)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::DataCorrupt(format!("invalid stored {label} timestamp")))
}
