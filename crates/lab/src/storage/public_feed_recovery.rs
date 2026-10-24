//! Durable evidence for bounded public-feed recovery and source-absence spans.

use super::{
    RawObjectStore, Store, enum_text, insert_observation, insert_raw, json_error, sql_error,
    timestamp_ms,
};
use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, LabError, MarketDataOrigin, MarketId,
    ObservationId, ProviderAbsenceAnchor, ProviderAbsenceSnapshotRef, ProviderAbsenceSpan,
    RawObjectId, RawObjectRef, UtcTimestamp, observation_digest,
};
use rusqlite::{OptionalExtension, Transaction, params};

pub(super) const MAX_PROVIDER_RECOVERY_PAGES: u32 = 64;
pub(crate) const MAX_PROVIDER_RECOVERY_ROWS: u64 = 12_800;
const MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY: usize = 256;
const MIN_CONFIRMATION_SECONDS: i64 = 1;
const MAX_CONFIRMATION_SECONDS: i64 = 30;
const MAX_ABSENCE_SLOTS: i64 = 12_800;

/// Per-call memo of normalized candle records only. Raw bytes are always freshly read.
#[derive(Default)]
pub(super) struct ProviderAbsenceDecodeMemo {
    pages: Vec<(ContentHash, Vec<crate::contracts::CandleRecord>)>,
}

impl ProviderAbsenceDecodeMemo {
    fn key(
        raw: &RawObjectRef,
        market: &MarketId,
        interval: CandleInterval,
    ) -> Result<ContentHash, LabError> {
        ContentHash::of_value(&(raw, market, interval))
    }

    fn get(&self, key: &ContentHash) -> Option<&[crate::contracts::CandleRecord]> {
        self.pages
            .iter()
            .find(|(cached_key, _)| cached_key == key)
            .map(|(_, records)| records.as_slice())
    }

    fn remember(&mut self, key: ContentHash, records: Vec<crate::contracts::CandleRecord>) {
        if self.pages.len() < 2 && self.pages.iter().all(|(cached_key, _)| cached_key != &key) {
            self.pages.push((key, records));
        }
    }

    #[cfg(test)]
    pub(super) fn cached_page_count(&self) -> usize {
        self.pages.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderFeedRecoveryCursor {
    pub market: MarketId,
    pub interval: CandleInterval,
    pub progress: crate::contracts::ProviderFeedRecoveryProgress,
    pub target_count: u32,
    pub started_at_utc: UtcTimestamp,
    pub last_recovered_tip: Option<ProviderAbsenceAnchor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderAbsenceSnapshotOutcome {
    pub confirmed_spans: Vec<ProviderAbsenceSpan>,
    pub pending_confirmation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AbsenceCandidate {
    start_utc: UtcTimestamp,
    end_utc: UtcTimestamp,
    pre_anchor: ProviderAbsenceAnchor,
    post_anchor: ProviderAbsenceAnchor,
}

// One bounded SQLite read result; this is not a separate persistence contract.
type PendingAbsenceRead = (
    AbsenceCandidate,
    String,
    UtcTimestamp,
    ContentHash,
    ContentHash,
    bool,
);
type PendingAbsenceConflict<'a> = ((i64, i64), &'a CandleObservation);

impl Store {
    /// Persist one verified REST page and record only exact gaps between observed anchors.
    /// A span is returned only after a second independent request at least one
    /// second later reports the same scoped page and exact anchors.
    ///
    /// # Errors
    /// Rejects invalid raw bytes, request scope, candle identities, changed gaps,
    /// contradictory late candles, and persistence failures.
    #[expect(
        clippy::too_many_lines,
        reason = "one owner transaction confirms or invalidates all scoped gaps before publishing any proof"
    )]
    pub(crate) fn observe_provider_absence_snapshot(
        &mut self,
        raw: &RawObjectRef,
        market: &MarketId,
        interval: CandleInterval,
        observations: &[CandleObservation],
        as_of_utc: UtcTimestamp,
    ) -> Result<ProviderAbsenceSnapshotOutcome, LabError> {
        let body = self.raw.read_verified(raw)?;
        let request_scope =
            validate_snapshot_page(raw, market, interval, observations, &body, as_of_utc)?;
        let candidates = absence_candidates(market, interval, observations)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_raw(&transaction, raw)?;

        let proof_conflicts = contradictory_proofs(&transaction, market, interval, observations)?;
        let pending_conflicts =
            contradictory_pending_reads(&transaction, market, interval, observations)?;
        if !proof_conflicts.is_empty() || !pending_conflicts.is_empty() {
            if proof_conflicts.len() + pending_conflicts.len()
                > MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY
            {
                return Err(LabError::ResourceLimit(
                    "provider-absence contradiction page exceeds its durable bound".into(),
                ));
            }
            for (proof_id, observation) in proof_conflicts {
                insert_absence_conflict(&transaction, &proof_id, raw, observation, as_of_utc)?;
            }
            for ((start_ms, end_ms), observation) in pending_conflicts {
                let conflict_digest = ContentHash::of_bytes(
                    &serde_json::to_vec(&(
                        raw.id.as_str(),
                        observation.id.as_str(),
                        observation.content_digest.as_str(),
                    ))
                    .map_err(json_error)?,
                );
                transaction
                    .execute(
                        "UPDATE provider_absence_pending_reads SET conflict_raw_object_id=?1, \
                         conflict_observation_id=?2, conflict_digest=?3 \
                         WHERE market=?4 AND interval=?5 AND start_ms=?6 AND end_ms=?7 \
                           AND conflict_raw_object_id IS NULL",
                        params![
                            raw.id.as_str(),
                            observation.id.as_str(),
                            conflict_digest.as_str(),
                            market.code(),
                            enum_text(&interval)?,
                            start_ms,
                            end_ms,
                        ],
                    )
                    .map_err(sql_error)?;
            }
            transaction.commit().map_err(sql_error)?;
            return Err(LabError::Conflict(
                "a later public candle contradicted provider-absence evidence".into(),
            ));
        }

        let mut confirmed = Vec::new();
        let mut pending_confirmation = false;
        for candidate in candidates {
            let start_ms = timestamp_ms(candidate.start_utc);
            let end_ms = timestamp_ms(candidate.end_utc);
            if let Some(existing) =
                load_span_for_exact_range(&transaction, market, interval, start_ms, end_ms)?
            {
                if !same_gap_geometry(&existing, &candidate) {
                    let conflicting_observation = observations
                        .iter()
                        .find(|row| row.id == candidate.post_anchor.observation_id)
                        .ok_or_else(|| {
                            LabError::DataCorrupt(
                                "changed absence anchor is missing from its raw page".into(),
                            )
                        })?;
                    insert_absence_conflict(
                        &transaction,
                        &existing.proof_id,
                        raw,
                        conflicting_observation,
                        as_of_utc,
                    )?;
                    transaction.commit().map_err(sql_error)?;
                    return Err(LabError::Conflict(
                        "a public-feed gap changed after its absence proof was recorded".into(),
                    ));
                }
                // A proof witnesses omitted slots, not an eternal provider price.
                // Its original IDs/digests and raw bodies stay immutable; current
                // economic revisions are pinned by the feed's canonical history.
                confirmed.push(existing);
                continue;
            }

            let existing_pending =
                load_pending_read(&transaction, market, interval, start_ms, end_ms)?;
            let current_snapshot = snapshot_ref(raw);
            let Some((
                first_candidate,
                first_raw_id,
                first_at,
                first_body,
                first_scope,
                conflicted,
            )) = existing_pending
            else {
                insert_pending_read(
                    &transaction,
                    market,
                    interval,
                    &candidate,
                    raw,
                    &request_scope,
                )?;
                pending_confirmation = true;
                continue;
            };
            if conflicted {
                return Err(LabError::Conflict(
                    "provider-absence candidate was already contradicted".into(),
                ));
            }
            if first_candidate != candidate && first_scope == request_scope {
                if raw.fetched_at < first_at {
                    return Err(LabError::Conflict(
                        "provider-absence read time moved backwards".into(),
                    ));
                }
                // Endpoint economics can change without an inner candle appearing.
                // Start a fresh pair for that version; neither read of the old
                // version is relabelled or counted as its confirmation.
                replace_pending_read(
                    &transaction,
                    market,
                    interval,
                    &candidate,
                    raw,
                    &request_scope,
                )?;
                pending_confirmation = true;
                continue;
            }
            if first_scope != request_scope {
                let conflicting_observation = observations
                    .iter()
                    .find(|row| row.id == candidate.post_anchor.observation_id)
                    .ok_or_else(|| {
                        LabError::DataCorrupt(
                            "changed pending request anchor is missing from its raw page".into(),
                        )
                    })?;
                invalidate_pending_read(
                    &transaction,
                    market,
                    interval,
                    &candidate,
                    raw,
                    conflicting_observation,
                )?;
                transaction.commit().map_err(sql_error)?;
                return Err(LabError::Conflict(
                    "provider-absence confirmation changed its REST request scope".into(),
                ));
            }
            let first_raw_id = RawObjectId::new(first_raw_id)?;
            if current_snapshot.raw_object_id == first_raw_id {
                pending_confirmation = true;
                continue;
            }
            let elapsed = raw_fetched_at_delta(raw, first_at);
            if elapsed < chrono::Duration::zero() {
                return Err(LabError::Conflict(
                    "provider-absence read time moved backwards".into(),
                ));
            }
            if elapsed < chrono::Duration::seconds(MIN_CONFIRMATION_SECONDS) {
                pending_confirmation = true;
                continue;
            }
            if elapsed > chrono::Duration::seconds(MAX_CONFIRMATION_SECONDS) {
                replace_pending_read(
                    &transaction,
                    market,
                    interval,
                    &candidate,
                    raw,
                    &request_scope,
                )?;
                pending_confirmation = true;
                continue;
            }
            let first_snapshot = ProviderAbsenceSnapshotRef {
                raw_object_id: first_raw_id,
                fetched_at_utc: first_at,
                body_digest: first_body,
            };
            let span = make_span(
                market.clone(),
                interval,
                candidate,
                [first_snapshot, current_snapshot],
            )?;
            insert_span(&transaction, &span)?;
            confirmed.push(span);
        }
        transaction.commit().map_err(sql_error)?;
        Ok(ProviderAbsenceSnapshotOutcome {
            confirmed_spans: confirmed,
            pending_confirmation,
        })
    }

    /// Read exact confirmed spans, verifying their raw evidence files on readback.
    ///
    /// # Errors
    /// Rejects invalid ranges, corrupt proofs/raw objects, late contradictions,
    /// excessive result pages, or SQLite failures.
    pub(crate) fn load_provider_absence_spans(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        start: UtcTimestamp,
        end: UtcTimestamp,
    ) -> Result<Vec<ProviderAbsenceSpan>, LabError> {
        load_verified_provider_absence_spans(
            &self.connection,
            &self.raw,
            market,
            interval,
            start,
            end,
        )
    }
}

/// Transaction-safe query used by sibling paper-store classification.
/// It reads only spans whose proof and catalog metadata remain internally valid.
pub(super) fn load_provider_absence_spans(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
) -> Result<Vec<ProviderAbsenceSpan>, LabError> {
    if start >= end {
        return Err(LabError::InvalidConfig(
            "provider-absence query range must be non-empty".into(),
        ));
    }
    let interval_text = enum_text(&interval)?;
    let start_ms = timestamp_ms(start);
    let end_ms = timestamp_ms(end);
    let mut statement = connection
        .prepare(
            "SELECT proof_id,start_ms,end_ms,first_raw_object_id,second_raw_object_id,proof_json,proof_digest \
             FROM provider_absence_spans WHERE market=?1 AND interval=?2 \
               AND start_ms < ?3 AND end_ms > ?4 ORDER BY start_ms,end_ms LIMIT ?5",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                market.code(),
                interval_text,
                end_ms,
                start_ms,
                i64::try_from(MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY + 1)
                    .map_err(|_| LabError::ResourceLimit("span page limit overflow".into()))?,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut spans = Vec::new();
    for row in rows {
        let (
            proof_id,
            stored_start,
            stored_end,
            first_raw_id,
            second_raw_id,
            proof_json,
            proof_digest,
        ) = row.map_err(sql_error)?;
        if spans.len() >= MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY {
            return Err(LabError::ResourceLimit(
                "provider-absence query result limit".into(),
            ));
        }
        if ContentHash::of_bytes(proof_json.as_bytes()).as_str() != proof_digest {
            return Err(LabError::DataCorrupt(
                "provider-absence proof record digest mismatch".into(),
            ));
        }
        let span: ProviderAbsenceSpan = serde_json::from_str(&proof_json).map_err(json_error)?;
        validate_span(&span)?;
        if span.proof_id.as_str() != proof_id
            || span.market != *market
            || span.interval != interval
            || timestamp_ms(span.start_utc) != stored_start
            || timestamp_ms(span.end_utc) != stored_end
            || span.snapshots[0].raw_object_id.as_str() != first_raw_id
            || span.snapshots[1].raw_object_id.as_str() != second_raw_id
        {
            return Err(LabError::DataCorrupt(
                "provider-absence proof columns disagree with its immutable payload".into(),
            ));
        }
        if proof_has_conflict(connection, &span.proof_id)? {
            return Err(LabError::Conflict(
                "provider-absence proof was contradicted by a later candle".into(),
            ));
        }
        for snapshot in &span.snapshots {
            let raw = load_raw_ref(connection, &snapshot.raw_object_id)?;
            if raw.http_status != 200
                || raw.fetched_at != snapshot.fetched_at_utc
                || raw.raw_sha256 != snapshot.body_digest
                || !is_supported_origin(&raw)
                || raw.transport.is_some()
                || raw_scope_digest(&raw, market, interval)?
                    != raw_scope_digest(
                        &load_raw_ref(connection, &span.snapshots[0].raw_object_id)?,
                        market,
                        interval,
                    )?
            {
                return Err(LabError::DataCorrupt(
                    "provider-absence snapshot metadata does not match its proof".into(),
                ));
            }
        }
        spans.push(span);
    }
    Ok(spans)
}

/// Transaction-safe confirmed-span query that also reopens and revalidates the
/// exact admitted raw response bytes. Paper classification must not trust only
/// the typed proof JSON or raw catalog metadata.
pub(super) fn load_verified_provider_absence_spans(
    connection: &rusqlite::Connection,
    raw_store: &RawObjectStore,
    market: &MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
) -> Result<Vec<ProviderAbsenceSpan>, LabError> {
    let mut decode_memo = ProviderAbsenceDecodeMemo::default();
    load_verified_provider_absence_spans_with_memo(
        connection,
        raw_store,
        market,
        interval,
        start,
        end,
        &mut decode_memo,
    )
}

pub(super) fn load_verified_provider_absence_spans_with_memo(
    connection: &rusqlite::Connection,
    raw_store: &RawObjectStore,
    market: &MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
    decode_memo: &mut ProviderAbsenceDecodeMemo,
) -> Result<Vec<ProviderAbsenceSpan>, LabError> {
    let spans = load_provider_absence_spans(connection, market, interval, start, end)?;
    for span in &spans {
        for snapshot in &span.snapshots {
            let raw = load_raw_ref(connection, &snapshot.raw_object_id)?;
            let body = raw_store.read_verified(&raw)?;
            if ContentHash::of_bytes(&body) != snapshot.body_digest {
                return Err(LabError::DataCorrupt(
                    "provider-absence raw body digest changed".into(),
                ));
            }
            validate_snapshot_page_metadata(&raw, &span.market, span.interval, &body)?;
            let key = ProviderAbsenceDecodeMemo::key(&raw, &span.market, span.interval)?;
            if let Some(records) = decode_memo.get(&key) {
                validate_snapshot_page_cursor(&raw, records)?;
                validate_span_is_absent_in_raw_page(records, span)?;
            } else {
                let records = crate::upbit_candles::decode_candles(
                    &body,
                    &span.market,
                    span.interval,
                    raw.fetched_at,
                )?;
                validate_snapshot_page_cursor(&raw, &records)?;
                validate_span_is_absent_in_raw_page(&records, span)?;
                decode_memo.remember(key, records);
            }
        }
    }
    Ok(spans)
}

fn validate_snapshot_page(
    raw: &RawObjectRef,
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
    body: &[u8],
    as_of_utc: UtcTimestamp,
) -> Result<ContentHash, LabError> {
    if raw.http_status != 200
        || raw.transport.is_some()
        || raw.fetched_at > as_of_utc
        || raw.persisted_at > as_of_utc
        || raw.raw_bytes > u64::try_from(super::MAX_RAW_OBJECT_BYTES).unwrap_or(u64::MAX)
        || ContentHash::of_bytes(body) != raw.raw_sha256
    {
        return Err(LabError::DataCorrupt(
            "provider-absence evidence is not a bounded successful raw REST response".into(),
        ));
    }
    let scope = raw_scope_digest(raw, market, interval)?;
    validate_page_rows(raw, market, interval, observations, body)?;
    Ok(scope)
}

fn validate_snapshot_page_header(
    raw: &RawObjectRef,
    market: &MarketId,
    interval: CandleInterval,
    body: &[u8],
) -> Result<Vec<crate::contracts::CandleRecord>, LabError> {
    validate_snapshot_page_metadata(raw, market, interval, body)?;
    let records = crate::upbit_candles::decode_candles(body, market, interval, raw.fetched_at)?;
    validate_snapshot_page_cursor(raw, &records)?;
    Ok(records)
}

fn validate_snapshot_page_metadata(
    raw: &RawObjectRef,
    market: &MarketId,
    interval: CandleInterval,
    body: &[u8],
) -> Result<(), LabError> {
    if raw.http_status != 200
        || raw.transport.is_some()
        || raw.raw_sha256 != ContentHash::of_bytes(body)
    {
        return Err(LabError::DataCorrupt(
            "provider-absence raw response metadata is invalid".into(),
        ));
    }
    let _ = raw_scope_digest(raw, market, interval)?;
    Ok(())
}

fn validate_snapshot_page_cursor(
    raw: &RawObjectRef,
    records: &[crate::contracts::CandleRecord],
) -> Result<(), LabError> {
    if let Some(to) = request_to(raw)?
        && records.iter().any(|record| record.open_time_utc >= to)
    {
        return Err(LabError::DataCorrupt(
            "provider-absence page crossed its exclusive REST cursor".into(),
        ));
    }
    Ok(())
}

fn validate_page_rows(
    raw: &RawObjectRef,
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
    body: &[u8],
) -> Result<(), LabError> {
    let records = validate_snapshot_page_header(raw, market, interval, body)?;
    if observations.len() > super::MAX_PAGE_OBSERVATIONS
        || observations
            .windows(2)
            .any(|pair| pair[0].candle.open_time_utc >= pair[1].candle.open_time_utc)
    {
        return Err(LabError::DataCorrupt(
            "provider-absence observations are not a bounded ascending page".into(),
        ));
    }
    for observation in observations {
        let expected_close = observation
            .candle
            .open_time_utc
            .0
            .checked_add_signed(interval.duration());
        if observation.candle.market != market.code()
            || observation.candle.interval != interval
            || !observation.candle.completed
            || observation.candle.close_time_utc > raw.fetched_at
            || expected_close != Some(observation.candle.close_time_utc.0)
            || !observation.raw_object_ids.contains(&raw.id)
            || observation_digest(&observation.candle)? != observation.content_digest
            || ObservationId::from_seed(observation.content_digest.as_str()) != observation.id
        {
            return Err(LabError::DataCorrupt(
                "provider-absence page contains an unbound or mismatched candle".into(),
            ));
        }
    }
    let completed_count = records.iter().filter(|candle| candle.completed).count();
    if completed_count != observations.len()
        || records
            .iter()
            .filter(|candle| candle.completed)
            .rev()
            .zip(observations)
            .any(|(candle, observation)| {
                candle != &observation.candle
                    || observation_digest(candle).ok().as_ref() != Some(&observation.content_digest)
                    || ObservationId::from_seed(observation.content_digest.as_str())
                        != observation.id
                    || !observation.raw_object_ids.contains(&raw.id)
            })
    {
        return Err(LabError::DataCorrupt(
            "provider-absence observations do not match the exact completed raw candle page".into(),
        ));
    }
    Ok(())
}

fn raw_scope_digest(
    raw: &RawObjectRef,
    market: &MarketId,
    interval: CandleInterval,
) -> Result<ContentHash, LabError> {
    let url = reqwest::Url::parse(&raw.source_url)
        .map_err(|_| LabError::DataCorrupt("provider-absence source URL is invalid".into()))?;
    let exchange_scope = url.scheme() == "https" && url.host_str() == Some("api.upbit.com");
    let fixture_scope = is_test_fixture_origin(raw, &url);
    if !(exchange_scope && raw.origin == MarketDataOrigin::ExchangeObserved || fixture_scope)
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.path() != format!("/{}", interval.upbit_endpoint())
    {
        return Err(LabError::DataCorrupt(
            "provider-absence response is outside the requested public candle endpoint".into(),
        ));
    }
    let mut query_market = None;
    let mut count = None;
    let mut to = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "market" if query_market.is_none() => query_market = Some(value.into_owned()),
            "count" if count.is_none() => {
                count = Some(value.parse::<u32>().map_err(|_| {
                    LabError::DataCorrupt("provider-absence request count is invalid".into())
                })?);
            }
            "to" if to.is_none() => {
                to = Some(UtcTimestamp::parse_rfc3339(&value)?);
            }
            _ => {
                return Err(LabError::DataCorrupt(
                    "provider-absence request has duplicate or unknown query scope".into(),
                ));
            }
        }
    }
    let market_code = market.code();
    if query_market.as_deref() != Some(market_code.as_str())
        || count.is_none_or(|count| !(1..=200).contains(&count))
    {
        return Err(LabError::DataCorrupt(
            "provider-absence request scope does not match its market and page bounds".into(),
        ));
    }
    Ok(ContentHash::of_bytes(
        &serde_json::to_vec(&(market, interval, count, to)).map_err(json_error)?,
    ))
}

fn is_supported_origin(raw: &RawObjectRef) -> bool {
    raw.origin == MarketDataOrigin::ExchangeObserved || {
        #[cfg(test)]
        {
            raw.origin == MarketDataOrigin::SyntheticTestOnly
        }
        #[cfg(not(test))]
        {
            false
        }
    }
}

fn is_test_fixture_origin(raw: &RawObjectRef, url: &reqwest::Url) -> bool {
    #[cfg(test)]
    {
        raw.origin == MarketDataOrigin::SyntheticTestOnly
            && url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
    }
    #[cfg(not(test))]
    {
        let _ = (raw, url);
        false
    }
}

fn request_to(raw: &RawObjectRef) -> Result<Option<UtcTimestamp>, LabError> {
    let url = reqwest::Url::parse(&raw.source_url)
        .map_err(|_| LabError::DataCorrupt("provider-absence source URL is invalid".into()))?;
    let mut to = None;
    for (key, value) in url.query_pairs() {
        if key == "to" {
            if to.is_some() {
                return Err(LabError::DataCorrupt(
                    "provider-absence request contains duplicate cursor parameters".into(),
                ));
            }
            to = Some(UtcTimestamp::parse_rfc3339(&value)?);
        }
    }
    Ok(to)
}

fn absence_candidates(
    market: &MarketId,
    interval: CandleInterval,
    observations: &[CandleObservation],
) -> Result<Vec<AbsenceCandidate>, LabError> {
    let mut candidates = Vec::new();
    for pair in observations.windows(2) {
        let pre = &pair[0];
        let post = &pair[1];
        if pre.candle.close_time_utc == post.candle.open_time_utc {
            continue;
        }
        if pre.candle.close_time_utc > post.candle.open_time_utc {
            return Err(LabError::DataCorrupt(
                "provider-absence page overlaps or reorders candle slots".into(),
            ));
        }
        let duration = post.candle.open_time_utc.0 - pre.candle.close_time_utc.0;
        let step = interval.duration();
        if duration.num_seconds() % step.num_seconds() != 0
            || duration.num_seconds() / step.num_seconds() > MAX_ABSENCE_SLOTS
        {
            return Err(LabError::ResourceLimit(
                "provider-absence span exceeds the bounded slot horizon".into(),
            ));
        }
        candidates.push(AbsenceCandidate {
            start_utc: pre.candle.close_time_utc,
            end_utc: post.candle.open_time_utc,
            pre_anchor: anchor(pre),
            post_anchor: anchor(post),
        });
    }
    if candidates.len() > super::MAX_PAGE_OBSERVATIONS {
        return Err(LabError::ResourceLimit(
            "provider-absence candidate count exceeds one page".into(),
        ));
    }
    let _ = market;
    Ok(candidates)
}

fn anchor(observation: &CandleObservation) -> ProviderAbsenceAnchor {
    ProviderAbsenceAnchor {
        observation_id: observation.id.clone(),
        open_time_utc: observation.candle.open_time_utc,
        content_digest: observation.content_digest.clone(),
    }
}

fn make_span(
    market: MarketId,
    interval: CandleInterval,
    candidate: AbsenceCandidate,
    snapshots: [ProviderAbsenceSnapshotRef; 2],
) -> Result<ProviderAbsenceSpan, LabError> {
    let facts = serde_json::to_vec(&(
        &market,
        interval,
        candidate.start_utc,
        candidate.end_utc,
        &candidate.pre_anchor,
        &candidate.post_anchor,
        &snapshots,
    ))
    .map_err(json_error)?;
    let proof_id = ContentHash::of_bytes(&facts);
    let span = ProviderAbsenceSpan {
        proof_id,
        market,
        interval,
        start_utc: candidate.start_utc,
        end_utc: candidate.end_utc,
        pre_anchor: candidate.pre_anchor,
        post_anchor: candidate.post_anchor,
        snapshots,
    };
    validate_span(&span)?;
    Ok(span)
}

fn validate_span(span: &ProviderAbsenceSpan) -> Result<(), LabError> {
    let step = span.interval.duration();
    let expected_start = span
        .pre_anchor
        .open_time_utc
        .0
        .checked_add_signed(step)
        .ok_or_else(|| LabError::DataCorrupt("provider-absence anchor time overflow".into()))?;
    let duration = span.end_utc.0 - span.start_utc.0;
    let read_elapsed = span.snapshots[1].fetched_at_utc.0 - span.snapshots[0].fetched_at_utc.0;
    if span.start_utc.0 != expected_start
        || span.end_utc != span.post_anchor.open_time_utc
        || span.start_utc >= span.end_utc
        || duration.num_seconds() % step.num_seconds() != 0
        || duration.num_seconds() / step.num_seconds() > MAX_ABSENCE_SLOTS
        || span.snapshots[0].raw_object_id == span.snapshots[1].raw_object_id
        || read_elapsed < chrono::Duration::seconds(MIN_CONFIRMATION_SECONDS)
        || read_elapsed > chrono::Duration::seconds(MAX_CONFIRMATION_SECONDS)
    {
        return Err(LabError::DataCorrupt(
            "provider-absence span is not an exact independently confirmed gap".into(),
        ));
    }
    let candidate = AbsenceCandidate {
        start_utc: span.start_utc,
        end_utc: span.end_utc,
        pre_anchor: span.pre_anchor.clone(),
        post_anchor: span.post_anchor.clone(),
    };
    let expected = make_proof_id(&span.market, span.interval, &candidate, &span.snapshots)?;
    if expected != span.proof_id {
        return Err(LabError::DataCorrupt(
            "provider-absence proof identity does not match its facts".into(),
        ));
    }
    Ok(())
}

fn make_proof_id(
    market: &MarketId,
    interval: CandleInterval,
    candidate: &AbsenceCandidate,
    snapshots: &[ProviderAbsenceSnapshotRef; 2],
) -> Result<ContentHash, LabError> {
    Ok(ContentHash::of_bytes(
        &serde_json::to_vec(&(
            market,
            interval,
            candidate.start_utc,
            candidate.end_utc,
            &candidate.pre_anchor,
            &candidate.post_anchor,
            snapshots,
        ))
        .map_err(json_error)?,
    ))
}

fn insert_pending_read(
    transaction: &Transaction<'_>,
    market: &MarketId,
    interval: CandleInterval,
    candidate: &AbsenceCandidate,
    raw: &RawObjectRef,
    scope: &ContentHash,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO provider_absence_pending_reads(\
             market,interval,start_ms,end_ms,pre_anchor_id,pre_anchor_digest,post_anchor_id,post_anchor_digest,\
             first_raw_object_id,first_fetched_at_ms,first_body_digest,first_request_scope_digest\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) \
             ON CONFLICT(market,interval,start_ms,end_ms) DO NOTHING",
            params![
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(candidate.start_utc),
                timestamp_ms(candidate.end_utc),
                candidate.pre_anchor.observation_id.as_str(),
                candidate.pre_anchor.content_digest.as_str(),
                candidate.post_anchor.observation_id.as_str(),
                candidate.post_anchor.content_digest.as_str(),
                raw.id.as_str(),
                timestamp_ms(raw.fetched_at),
                raw.raw_sha256.as_str(),
                scope.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn replace_pending_read(
    transaction: &Transaction<'_>,
    market: &MarketId,
    interval: CandleInterval,
    candidate: &AbsenceCandidate,
    raw: &RawObjectRef,
    scope: &ContentHash,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE provider_absence_pending_reads SET \
             first_raw_object_id=?1, first_fetched_at_ms=?2, first_body_digest=?3, \
             first_request_scope_digest=?4, pre_anchor_id=?9, pre_anchor_digest=?10, \
             post_anchor_id=?11, post_anchor_digest=?12 \
             WHERE market=?5 AND interval=?6 AND start_ms=?7 AND end_ms=?8 \
               AND conflict_raw_object_id IS NULL",
            params![
                raw.id.as_str(),
                timestamp_ms(raw.fetched_at),
                raw.raw_sha256.as_str(),
                scope.as_str(),
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(candidate.start_utc),
                timestamp_ms(candidate.end_utc),
                candidate.pre_anchor.observation_id.as_str(),
                candidate.pre_anchor.content_digest.as_str(),
                candidate.post_anchor.observation_id.as_str(),
                candidate.post_anchor.content_digest.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn load_pending_read(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    start_ms: i64,
    end_ms: i64,
) -> Result<Option<PendingAbsenceRead>, LabError> {
    let row = connection
        .query_row(
            "SELECT pre_anchor_id,pre_anchor_digest,post_anchor_id,post_anchor_digest,\
             first_raw_object_id,first_fetched_at_ms,first_body_digest,first_request_scope_digest,\
             conflict_raw_object_id \
             FROM provider_absence_pending_reads WHERE market=?1 AND interval=?2 AND start_ms=?3 AND end_ms=?4",
            params![market.code(), enum_text(&interval)?, start_ms, end_ms],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(
        |(
            pre_id,
            pre_digest,
            post_id,
            post_digest,
            raw_id,
            fetched_ms,
            body_digest,
            scope_digest,
            conflict_raw,
        )| {
            let start_utc = timestamp_from_ms(start_ms, "pending absence start")?;
            let end_utc = timestamp_from_ms(end_ms, "pending absence end")?;
            let pre_open = UtcTimestamp(start_utc.0 - interval.duration());
            let candidate = AbsenceCandidate {
                start_utc,
                end_utc,
                pre_anchor: ProviderAbsenceAnchor {
                    observation_id: ObservationId::new(pre_id)?,
                    open_time_utc: pre_open,
                    content_digest: ContentHash::try_from(pre_digest)?,
                },
                post_anchor: ProviderAbsenceAnchor {
                    observation_id: ObservationId::new(post_id)?,
                    open_time_utc: end_utc,
                    content_digest: ContentHash::try_from(post_digest)?,
                },
            };
            let body_digest = ContentHash::try_from(body_digest)?;
            let scope_digest = ContentHash::try_from(scope_digest)?;
            let first_raw = load_raw_ref(connection, &RawObjectId::new(raw_id.clone())?)?;
            if timestamp_ms(first_raw.fetched_at) != fetched_ms
                || first_raw.raw_sha256 != body_digest
                || raw_scope_digest(&first_raw, market, interval)? != scope_digest
            {
                return Err(LabError::DataCorrupt(
                    "pending first read disagrees with its immutable raw metadata".into(),
                ));
            }
            // Milliseconds are an index/projection only. The canonical raw JSON
            // retains the exact clock used for identity and the one-second gate.
            Ok((
                candidate,
                raw_id,
                first_raw.fetched_at,
                body_digest,
                scope_digest,
                conflict_raw.is_some(),
            ))
        },
    )
    .transpose()
}

fn load_span_for_exact_range(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    start_ms: i64,
    end_ms: i64,
) -> Result<Option<ProviderAbsenceSpan>, LabError> {
    let value = connection
        .query_row(
            "SELECT proof_json,proof_digest FROM provider_absence_spans \
             WHERE market=?1 AND interval=?2 AND start_ms=?3 AND end_ms=?4",
            params![market.code(), enum_text(&interval)?, start_ms, end_ms],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    value
        .map(|(proof_json, proof_digest)| {
            if ContentHash::of_bytes(proof_json.as_bytes()).as_str() != proof_digest {
                return Err(LabError::DataCorrupt(
                    "provider-absence proof record digest mismatch".into(),
                ));
            }
            let span: ProviderAbsenceSpan =
                serde_json::from_str(&proof_json).map_err(json_error)?;
            validate_span(&span)?;
            Ok(span)
        })
        .transpose()
}

fn insert_span(transaction: &Transaction<'_>, span: &ProviderAbsenceSpan) -> Result<(), LabError> {
    let proof_json = serde_json::to_string(span).map_err(json_error)?;
    let proof_digest = ContentHash::of_bytes(proof_json.as_bytes());
    transaction
        .execute(
            "INSERT INTO provider_absence_spans(\
             proof_id,market,interval,start_ms,end_ms,first_raw_object_id,second_raw_object_id,proof_json,proof_digest\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                span.proof_id.as_str(),
                span.market.code(),
                enum_text(&span.interval)?,
                timestamp_ms(span.start_utc),
                timestamp_ms(span.end_utc),
                span.snapshots[0].raw_object_id.as_str(),
                span.snapshots[1].raw_object_id.as_str(),
                proof_json,
                proof_digest.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn contradictory_proofs<'a>(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    observations: &'a [CandleObservation],
) -> Result<Vec<(ContentHash, &'a CandleObservation)>, LabError> {
    let interval = enum_text(&interval)?;
    let limit = i64::try_from(MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY + 1)
        .map_err(|_| LabError::ResourceLimit("proof contradiction limit overflow".into()))?;
    let mut conflicts: Vec<(ContentHash, &'a CandleObservation)> = Vec::new();
    for observation in observations {
        let open_ms = timestamp_ms(observation.candle.open_time_utc);
        let mut statement = connection
            .prepare(
                "SELECT proof_id FROM provider_absence_spans WHERE market=?1 AND interval=?2 \
                 AND start_ms<=?3 AND end_ms>?3 ORDER BY proof_id LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![market.code(), interval, open_ms, limit], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        for row in rows {
            let proof = ContentHash::try_from(row.map_err(sql_error)?)?;
            if !conflicts
                .iter()
                .any(|(existing, row)| *existing == proof && row.id == observation.id)
            {
                conflicts.push((proof, observation));
            }
        }
    }
    if conflicts.len() > MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY {
        return Err(LabError::ResourceLimit(
            "proof contradiction page exceeds its bound".into(),
        ));
    }
    Ok(conflicts)
}

fn contradictory_pending_reads<'a>(
    connection: &rusqlite::Connection,
    market: &MarketId,
    interval: CandleInterval,
    observations: &'a [CandleObservation],
) -> Result<Vec<PendingAbsenceConflict<'a>>, LabError> {
    let interval = enum_text(&interval)?;
    let limit = i64::try_from(MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY + 1)
        .map_err(|_| LabError::ResourceLimit("pending contradiction limit overflow".into()))?;
    let mut conflicts: Vec<PendingAbsenceConflict<'a>> = Vec::new();
    for observation in observations {
        let open_ms = timestamp_ms(observation.candle.open_time_utc);
        let mut statement = connection
            .prepare(
                "SELECT start_ms,end_ms FROM provider_absence_pending_reads \
                 WHERE market=?1 AND interval=?2 AND start_ms<=?3 AND end_ms>?3 \
                   AND conflict_raw_object_id IS NULL ORDER BY start_ms,end_ms LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![market.code(), interval, open_ms, limit], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_error)?;
        for row in rows {
            let range = row.map_err(sql_error)?;
            if !conflicts
                .iter()
                .any(|(existing, row)| *existing == range && row.id == observation.id)
            {
                conflicts.push((range, observation));
            }
        }
    }
    if conflicts.len() > MAX_PROVIDER_ABSENCE_SPANS_PER_QUERY {
        return Err(LabError::ResourceLimit(
            "pending contradiction page exceeds its bound".into(),
        ));
    }
    Ok(conflicts)
}

/// Any persisted completed observation is durable negative evidence, including
/// observations published by ordinary collection rather than the live REST poller.
pub(super) fn record_observation_absence_conflicts(
    transaction: &Transaction<'_>,
    observation: &CandleObservation,
) -> Result<(), LabError> {
    if !observation.candle.completed {
        return Ok(());
    }
    let market = MarketId::parse_upbit(&observation.candle.market)?;
    let conflicts = contradictory_proofs(
        transaction,
        &market,
        observation.candle.interval,
        std::slice::from_ref(observation),
    )?;
    if conflicts.is_empty() {
        return Ok(());
    }
    let raw_id = observation.raw_object_ids.first().ok_or_else(|| {
        LabError::DataCorrupt(
            "a candle contradicting provider absence has no captured raw source".into(),
        )
    })?;
    let raw = load_raw_ref(transaction, raw_id)?;
    for (proof_id, row) in conflicts {
        insert_absence_conflict(transaction, &proof_id, &raw, row, raw.fetched_at)?;
    }
    Ok(())
}

fn insert_absence_conflict(
    transaction: &Transaction<'_>,
    proof_id: &ContentHash,
    raw: &RawObjectRef,
    observation: &CandleObservation,
    observed_at: UtcTimestamp,
) -> Result<(), LabError> {
    let digest = ContentHash::of_bytes(
        &serde_json::to_vec(&(
            proof_id.as_str(),
            raw.id.as_str(),
            observation.id.as_str(),
            observation.content_digest.as_str(),
        ))
        .map_err(json_error)?,
    );
    transaction
        .execute(
            "INSERT OR IGNORE INTO provider_absence_conflicts(\
             proof_id,raw_object_id,observation_id,observation_digest,observed_at_ms,conflict_digest\
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                proof_id.as_str(),
                raw.id.as_str(),
                observation.id.as_str(),
                observation.content_digest.as_str(),
                timestamp_ms(observed_at),
                digest.as_str(),
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn invalidate_pending_read(
    transaction: &Transaction<'_>,
    market: &MarketId,
    interval: CandleInterval,
    candidate: &AbsenceCandidate,
    raw: &RawObjectRef,
    observation: &CandleObservation,
) -> Result<(), LabError> {
    let conflict_digest = ContentHash::of_bytes(
        &serde_json::to_vec(&(
            raw.id.as_str(),
            observation.id.as_str(),
            observation.content_digest.as_str(),
            candidate.start_utc,
            candidate.end_utc,
            &candidate.pre_anchor,
            &candidate.post_anchor,
        ))
        .map_err(json_error)?,
    );
    let changed = transaction
        .execute(
            "UPDATE provider_absence_pending_reads SET conflict_raw_object_id=?1, \
             conflict_observation_id=?2, conflict_digest=?3 \
             WHERE market=?4 AND interval=?5 AND start_ms=?6 AND end_ms=?7 \
               AND conflict_raw_object_id IS NULL",
            params![
                raw.id.as_str(),
                observation.id.as_str(),
                conflict_digest.as_str(),
                market.code(),
                enum_text(&interval)?,
                timestamp_ms(candidate.start_utc),
                timestamp_ms(candidate.end_utc),
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::Conflict(
            "provider-absence candidate changed during invalidation".into(),
        ));
    }
    Ok(())
}

fn proof_has_conflict(
    connection: &rusqlite::Connection,
    proof_id: &ContentHash,
) -> Result<bool, LabError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM provider_absence_conflicts WHERE proof_id=?1) \
             OR EXISTS(SELECT 1 FROM candle_observations c JOIN provider_absence_spans p \
               ON c.market=p.market AND c.interval=p.interval \
               WHERE p.proof_id=?1 AND c.completed=1 \
                 AND c.open_time_ms>=p.start_ms AND c.open_time_ms<p.end_ms)",
            [proof_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn load_raw_ref(
    connection: &rusqlite::Connection,
    raw_id: &RawObjectId,
) -> Result<RawObjectRef, LabError> {
    let json = connection
        .query_row(
            "SELECT object_json FROM raw_objects WHERE id=?1",
            [raw_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_error)?;
    let raw: RawObjectRef = serde_json::from_str(&json).map_err(json_error)?;
    if raw.id != *raw_id {
        return Err(LabError::DataCorrupt(
            "raw object identity disagrees with its catalog key".into(),
        ));
    }
    Ok(raw)
}

fn validate_span_is_absent_in_raw_page(
    records: &[crate::contracts::CandleRecord],
    span: &ProviderAbsenceSpan,
) -> Result<(), LabError> {
    let pre_matches = records
        .iter()
        .filter(|candle| candle.completed)
        .find(|candle| candle.open_time_utc == span.pre_anchor.open_time_utc)
        .is_some_and(|candle| {
            observation_digest(candle).ok().as_ref() == Some(&span.pre_anchor.content_digest)
                && ObservationId::from_seed(span.pre_anchor.content_digest.as_str())
                    == span.pre_anchor.observation_id
        });
    let post_matches = records
        .iter()
        .filter(|candle| candle.completed)
        .find(|candle| candle.open_time_utc == span.post_anchor.open_time_utc)
        .is_some_and(|candle| {
            observation_digest(candle).ok().as_ref() == Some(&span.post_anchor.content_digest)
                && ObservationId::from_seed(span.post_anchor.content_digest.as_str())
                    == span.post_anchor.observation_id
        });
    if !pre_matches
        || !post_matches
        || records
            .iter()
            .filter(|candle| candle.completed)
            .any(|candle| {
                candle.open_time_utc >= span.start_utc && candle.open_time_utc < span.end_utc
            })
    {
        return Err(LabError::DataCorrupt(
            "provider-absence raw snapshot no longer proves the exact span".into(),
        ));
    }
    Ok(())
}

fn snapshot_ref(raw: &RawObjectRef) -> ProviderAbsenceSnapshotRef {
    ProviderAbsenceSnapshotRef {
        raw_object_id: raw.id.clone(),
        fetched_at_utc: raw.fetched_at,
        body_digest: raw.raw_sha256.clone(),
    }
}

fn same_gap_geometry(span: &ProviderAbsenceSpan, candidate: &AbsenceCandidate) -> bool {
    span.start_utc == candidate.start_utc
        && span.end_utc == candidate.end_utc
        && span.pre_anchor.open_time_utc == candidate.pre_anchor.open_time_utc
        && span.post_anchor.open_time_utc == candidate.post_anchor.open_time_utc
}

fn timestamp_from_ms(value: i64, label: &str) -> Result<UtcTimestamp, LabError> {
    chrono::DateTime::from_timestamp_millis(value)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::DataCorrupt(format!("invalid stored {label} timestamp")))
}

fn raw_fetched_at_delta(raw: &RawObjectRef, first: UtcTimestamp) -> chrono::Duration {
    raw.fetched_at.0 - first.0
}

#[cfg(test)]
mod tests;

mod progress;
pub(crate) use progress::PersistedProviderRecoveryPage;
