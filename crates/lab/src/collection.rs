//! Bounded historical collection, exact normalization and immutable admission.
//!
//! Collection is incremental: stored observations from earlier collections are
//! reused for any overlapping coverage, and only the remaining missing grid
//! segments are fetched from the exchange. Gaps are recorded, never filled.

use crate::contracts::{
    AssetQuantity, CandleInterval, CandleObservation, CandleRecord, CollectRequest, CollectionPage,
    CollectionReuse, ContentHash, DatasetId, DatasetManifest, DatasetSnapshot, DatasetStatus,
    LabError, MAX_COLLECTION_CALLS, MAX_DATASET_ROWS, MarketDataOrigin, MarketId,
    NORMALIZER_VERSION, ObservationId, QualityIssue, QualityKind, QualitySeverity, QuoteAmount,
    RawObjectId, RawObjectRef, RequestId, SCHEMA_VERSION, UtcRange, UtcTimestamp,
};
use crate::database::DatabaseHandle;
use crate::market_data::{CandleResponse, UpbitClient, decode_candles, parse_market_catalog};
use crate::storage::{RawObjectInput, dataset_digests, dataset_id, observation_digest};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Stored observations available for reuse plus their raw provenance.
#[derive(Debug, Default)]
pub struct ReuseInputs<'a> {
    pub observations: &'a [CandleObservation],
    pub raw_objects: &'a [RawObjectRef],
    pub api_calls: u32,
}

/// Collect exactly the requested grid, recording gaps instead of filling them.
/// Stored observations overlapping the coverage are reused; only missing grid
/// segments are fetched. Resume continues from durably committed pages.
/// # Errors
/// Reports invalid inputs, bounded network failures, cancellation or persistence failure.
/// Admitted DB operations are always awaited; cancellation is checked between pages.
#[expect(
    clippy::too_many_lines,
    reason = "one resumable collection cursor and publication lifecycle"
)]
pub async fn prepare_collection(
    client: &UpbitClient,
    database: &DatabaseHandle,
    request: CollectRequest,
    cancellation: &CancellationToken,
) -> Result<DatasetSnapshot, LabError> {
    request.validate(UtcTimestamp::now())?;
    let deadline = Instant::now() + Duration::from_mins(30);
    let mut calls = 0_u32;
    // Admission gate: every requested market must exist in the live Upbit
    // market catalog, so typo'd or non-listed symbols fail with a clear
    // rejection instead of an empty dataset. Counts against the collection
    // call budget like any other exchange request.
    check_cancel(cancellation)?;
    if calls >= MAX_COLLECTION_CALLS {
        return Err(LabError::ResourceLimit(format!(
            "collection_api_calls exhausted at stage=market_catalog_validation: \
             allowed={MAX_COLLECTION_CALLS} unit=exchange_calls used={calls}"
        )));
    }
    calls += 1;
    let catalog = fetch_market_catalog(client, deadline, cancellation).await?;
    for market in &request.markets {
        if !catalog.contains(&market.code()) {
            return Err(LabError::InvalidConfig(format!(
                "unknown or non-KRW Upbit market {} (validated against the live market catalog); \
                 check the symbol or pick a listed market",
                market.code()
            )));
        }
    }
    let registered = request.clone();
    database
        .call("begin_collection", move |store| {
            store.begin_collection(&registered)
        })
        .await?;
    let coverage = request
        .range
        .with_warmup(request.warmup_bars, request.data_resolution)?;
    let mut all_pages = Vec::new();
    let mut reused_observations: Vec<CandleObservation> = Vec::new();
    let mut reused_raw: Vec<RawObjectRef> = Vec::new();
    for market in &request.markets {
        check_cancel(cancellation)?;
        let request_id = request.request_id.clone();
        let queried_market = market.clone();
        let mut pages = database
            .call("resume_collection", move |store| {
                store.load_collection_pages(&request_id, &queried_market)
            })
            .await?;
        let reuse_markets = vec![market.clone()];
        let reuse_interval = request.data_resolution;
        let (stored, stored_raw) = database
            .call("load_reusable_data", move |store| {
                let observations = store.load_reusable_observations(
                    &reuse_markets,
                    reuse_interval,
                    coverage.start(),
                    coverage.end(),
                )?;
                let raw = store.load_reusable_raw_objects(
                    &reuse_markets,
                    reuse_interval,
                    coverage.start(),
                    coverage.end(),
                )?;
                Ok((observations, raw))
            })
            .await?;
        let covered: BTreeSet<UtcTimestamp> = stored
            .iter()
            .map(|observation| observation.candle.open_time_utc)
            .collect();
        let segments = missing_segments(&coverage, request.data_resolution, &covered)?;
        for (segment_start, segment_end) in segments {
            let mut cursor = Some(segment_end);
            while let Some(to) = cursor.filter(|to| *to > segment_start) {
                check_cancel(cancellation)?;
                let published = fetch_published(
                    client,
                    database,
                    &request.request_id,
                    market,
                    request.data_resolution,
                    to,
                    deadline,
                    cancellation,
                    &mut calls,
                )
                .await?;
                let mut candles = decode_candles(
                    &published.body,
                    market,
                    request.data_resolution,
                    published.object.fetched_at,
                )?;
                candles.sort_by_key(|candle| candle.open_time_utc);
                if candles.iter().any(|candle| candle.open_time_utc >= to) {
                    return Err(LabError::ContractParse(
                        "Upbit exclusive cursor returned a candle at/after to".into(),
                    ));
                }
                let next_to = candles.first().map(|candle| candle.open_time_utc);
                if next_to.is_some_and(|next| next >= to) {
                    return Err(LabError::ContractParse(
                        "Upbit cursor did not advance".into(),
                    ));
                }
                let mut observations = Vec::with_capacity(candles.len());
                for candle in candles {
                    if !coverage.contains(candle.open_time_utc) {
                        continue;
                    }
                    let digest = observation_digest(&candle)?;
                    observations.push(CandleObservation {
                        id: ObservationId::from_seed(digest.as_str()),
                        candle,
                        content_digest: digest,
                        raw_object_ids: vec![published.object.id.clone()],
                        constituent_ids: Vec::new(),
                    });
                }
                let page = CollectionPage {
                    request_id: request.request_id.clone(),
                    market: market.clone(),
                    requested_to: to,
                    next_to,
                    raw_object: published.object,
                    observations,
                    page_index: u32::try_from(pages.len())
                        .map_err(|_| LabError::ResourceLimit("page index overflow".into()))?,
                };
                let committed = page.clone();
                database
                    .call("commit_collection_page", move |store| {
                        store.commit_page(&committed)
                    })
                    .await?;
                cursor = page.next_to;
                pages.push(page);
                tracing::info!(event = "collection_page_committed", request_id = %request.request_id,
                    market = %market, pages = pages.len(), calls);
            }
        }
        reused_observations.extend(stored);
        reused_raw.extend(stored_raw);
        all_pages.extend(pages);
    }
    check_cancel(cancellation)?;
    let request_id = request.request_id.clone();
    let raw_objects = database
        .call("collection_raw_provenance", move |store| {
            store.load_collection_raw_objects(&request_id)
        })
        .await?;
    let reuse = ReuseInputs {
        observations: &reused_observations,
        raw_objects: &reused_raw,
        api_calls: calls,
    };
    assemble_dataset(request, &all_pages, &raw_objects, &reuse)
}

/// Collect and publish a standalone local snapshot without a durable job attempt.
/// Job orchestration uses `prepare_collection` and atomically publishes its output.
/// # Errors
/// Preserves collection, cancellation and storage publication failures.
pub async fn collect(
    client: &UpbitClient,
    database: &DatabaseHandle,
    request: CollectRequest,
    cancellation: &CancellationToken,
) -> Result<DatasetSnapshot, LabError> {
    let snapshot = prepare_collection(client, database, request, cancellation).await?;
    check_cancel(cancellation)?;
    database
        .call("freeze_dataset", move |store| {
            store.finish_dataset(&snapshot)?;
            Ok(snapshot)
        })
        .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "one bounded network/persistence operation context"
)]
async fn fetch_published(
    client: &UpbitClient,
    database: &DatabaseHandle,
    request_id: &RequestId,
    market: &MarketId,
    interval: CandleInterval,
    to: UtcTimestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
    calls: &mut u32,
) -> Result<crate::storage::PublishedRaw, LabError> {
    for attempt in 0..3 {
        check_cancel(cancellation)?;
        if *calls >= MAX_COLLECTION_CALLS {
            return Err(LabError::ResourceLimit(format!(
                "collection_api_calls exhausted at stage=page_fetch: \
                 allowed={MAX_COLLECTION_CALLS} unit=exchange_calls used={calls} \
                 scope=this_collection_request; remedy: split the range, raise the \
                 data_resolution, or submit a fresh request_id (reuse skips stored rows)"
            )));
        }
        *calls += 1;
        let response = match client
            .collect_page(market, interval, to, deadline, cancellation)
            .await
        {
            Ok(response) => response,
            Err(failure) if failure.retryable && attempt < 2 => {
                retry_delay(deadline, cancellation).await?;
                continue;
            }
            Err(failure) => return Err(failure.error),
        };
        let status = response.http_status;
        let published = publish_response(database, request_id, response).await?;
        if (200..300).contains(&status) {
            return Ok(published);
        }
        if status == 418 {
            return Err(LabError::TemporarilyBlocked(format!(
                "Upbit HTTP418; inspect raw {} and wait the full provider ban; collection stopped",
                published.object.id
            )));
        }
        if (status == 429 || (500..600).contains(&status)) && attempt < 2 {
            retry_delay(deadline, cancellation).await?;
            continue;
        }
        return Err(if status == 429 {
            LabError::RateLimited(format!(
                "Upbit HTTP429 after bounded attempts; raw {}",
                published.object.id
            ))
        } else {
            LabError::NetworkUnavailable(format!("Upbit HTTP{status}; raw {}", published.object.id))
        });
    }
    Err(LabError::NetworkUnavailable(
        "collection retry budget exhausted".into(),
    ))
}

async fn publish_response(
    database: &DatabaseHandle,
    request_id: &RequestId,
    response: CandleResponse,
) -> Result<crate::storage::PublishedRaw, LabError> {
    let request_id = request_id.clone();
    database
        .call("publish_collection_raw", move |store| {
            let used = store.collection_raw_bytes(&request_id)?;
            let incoming = u64::try_from(response.body.len())
                .map_err(|_| LabError::ResourceLimit("response length overflow".into()))?;
            if used
                .checked_add(incoming)
                .is_none_or(|total| total > crate::storage::MAX_COLLECTION_BYTES)
            {
                return Err(LabError::ResourceLimit(format!(
                    "collection_raw_bytes exhausted at stage=raw_publication: \
                     allowed={} unit=bytes used={used} incoming={incoming} \
                     scope=this_collection_request; remedy: shrink the range or raise \
                     the data_resolution; deleting datasets restores shared file space \
                     but not this per-request budget",
                    crate::storage::MAX_COLLECTION_BYTES
                )));
            }
            let published = store.publish_raw(RawObjectInput {
                source_url: response.source_url,
                fetched_at: response.fetched_at,
                persisted_at: UtcTimestamp::now(),
                http_status: response.http_status,
                remaining_req: response.remaining_req,
                origin: response.origin,
                body: response.body,
            })?;
            // Rejected-response evidence is also discoverable by raw identity.
            store.catalog_collection_raw(&request_id, &published.object)?;
            Ok(published)
        })
        .await
}

/// Fetch and parse the live Upbit market catalog with the same bounded
/// retry semantics as candle pages.
async fn fetch_market_catalog(
    client: &UpbitClient,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<std::collections::BTreeSet<String>, LabError> {
    for attempt in 0..3 {
        check_cancel(cancellation)?;
        let response = match client.fetch_market_catalog(deadline, cancellation).await {
            Ok(response) => response,
            Err(failure) if failure.retryable && attempt < 2 => {
                retry_delay(deadline, cancellation).await?;
                continue;
            }
            Err(failure) => return Err(failure.error),
        };
        let status = response.http_status;
        if (200..300).contains(&status) {
            let markets = parse_market_catalog(&response.body)?;
            return Ok(markets.into_iter().collect());
        }
        if status == 418 {
            return Err(LabError::TemporarilyBlocked(
                "Upbit HTTP418 while validating the market catalog; wait out the ban before retrying"
                    .into(),
            ));
        }
        if status == 429 || (500..600).contains(&status) {
            retry_delay(deadline, cancellation).await?;
            continue;
        }
        return Err(LabError::NetworkUnavailable(format!(
            "Upbit HTTP{status} while validating the market catalog"
        )));
    }
    Err(LabError::NetworkUnavailable(
        "market catalog retry budget exhausted".into(),
    ))
}

async fn retry_delay(deadline: Instant, cancellation: &CancellationToken) -> Result<(), LabError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(LabError::Cancelled("collection cancelled between retries".into())),
        () = tokio::time::sleep_until(deadline) => Err(LabError::NetworkUnavailable("collection deadline elapsed".into())),
        () = tokio::time::sleep(Duration::from_secs(1)) => Ok(()),
    }
}

fn check_cancel(token: &CancellationToken) -> Result<(), LabError> {
    if token.is_cancelled() {
        Err(LabError::Cancelled(
            "collection cancelled at checkpoint".into(),
        ))
    } else {
        Ok(())
    }
}

/// Split the coverage grid into contiguous missing segments, newest first.
///
/// A segment exists for every maximal run of grid slots without stored data.
fn missing_segments(
    coverage: &UtcRange,
    interval: CandleInterval,
    covered: &BTreeSet<UtcTimestamp>,
) -> Result<Vec<(UtcTimestamp, UtcTimestamp)>, LabError> {
    let mut segments = Vec::new();
    let mut gap_start: Option<UtcTimestamp> = None;
    let mut cursor = coverage.start();
    while cursor < coverage.end() {
        if covered.contains(&cursor) {
            if let Some(start) = gap_start.take() {
                segments.push((start, cursor));
            }
        } else {
            gap_start.get_or_insert(cursor);
        }
        cursor = UtcTimestamp(
            cursor
                .0
                .checked_add_signed(interval.duration())
                .ok_or_else(|| LabError::InvalidConfig("grid timestamp overflow".into()))?,
        );
    }
    if let Some(start) = gap_start {
        segments.push((start, coverage.end()));
    }
    segments.reverse();
    Ok(segments)
}

/// Assemble immutable economic/provenance identities from committed and reused rows.
/// # Errors
/// Rejects mixed origins or capacity/contract failures; missing/conflicting data is `BLOCKED_DATA`.
pub fn assemble_dataset(
    request: CollectRequest,
    pages: &[CollectionPage],
    raw_objects: &[RawObjectRef],
    reuse: &ReuseInputs<'_>,
) -> Result<DatasetSnapshot, LabError> {
    let coverage = request
        .range
        .with_warmup(request.warmup_bars, request.data_resolution)?;
    let origin = pages
        .first()
        .map_or(MarketDataOrigin::ExchangeObserved, |p| p.raw_object.origin);
    let raw = merge_raw_sources(origin, pages, raw_objects, reuse.raw_objects)?;
    let merged = merge_dataset_rows(coverage, pages, reuse.observations)?;
    let mut assembly = DatasetAssembly {
        coverage,
        origin,
        rows: merged.rows,
        raw,
        issues: merged.issues,
        reused_rows: merged.reused_rows,
        fetched_rows: merged.fetched_rows,
    };
    record_row_issues(&assembly.rows, &mut assembly.issues)?;
    record_source_gaps(
        &request,
        coverage,
        &assembly.rows,
        &assembly.raw,
        &mut assembly.issues,
    )?;
    finish_dataset(request, assembly, reuse.api_calls)
}

fn finish_dataset(
    request: CollectRequest,
    assembly: DatasetAssembly,
    api_calls: u32,
) -> Result<DatasetSnapshot, LabError> {
    let status = if assembly
        .issues
        .iter()
        .any(|issue| issue.severity == QualitySeverity::Error)
        || assembly.rows.is_empty()
    {
        DatasetStatus::BlockedData
    } else {
        DatasetStatus::Ready
    };
    let placeholder = ContentHash::of_bytes(b"unpublished");
    let mut snapshot = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: SCHEMA_VERSION.into(),
            id: DatasetId::from_seed("unpublished"),
            request,
            coverage: assembly.coverage,
            status,
            row_count: u64::try_from(assembly.rows.len())
                .map_err(|_| LabError::ResourceLimit("row count overflow".into()))?,
            normalizer_version: NORMALIZER_VERSION.into(),
            gap_policy: "REJECT_UNRESOLVED_GAPS".into(),
            semantic_digest: placeholder.clone(),
            provenance_digest: placeholder,
            origin: assembly.origin,
            raw_objects: assembly.raw.into_values().collect(),
            quality_issues: assembly.issues,
            reuse: (assembly.reused_rows > 0).then_some(CollectionReuse {
                reused_observations: assembly.reused_rows,
                fetched_observations: assembly.fetched_rows,
                api_calls,
            }),
        },
        observations: assembly.rows.into_values().collect(),
    };
    let digests = dataset_digests(&snapshot)?;
    snapshot.manifest.id = dataset_id(&digests);
    snapshot.manifest.semantic_digest = digests.semantic;
    snapshot.manifest.provenance_digest = digests.provenance;
    Ok(snapshot)
}

fn merge_raw_sources(
    origin: MarketDataOrigin,
    pages: &[CollectionPage],
    raw_objects: &[RawObjectRef],
    reused: &[RawObjectRef],
) -> Result<BTreeMap<RawObjectId, RawObjectRef>, LabError> {
    let mut raw: BTreeMap<_, _> = raw_objects
        .iter()
        .map(|raw| (raw.id.clone(), raw.clone()))
        .collect();
    for page in pages {
        if page.raw_object.origin != origin {
            return Err(LabError::DataCorrupt(
                "mixed synthetic and observed collection origins".into(),
            ));
        }
        let object = &page.raw_object;
        raw.insert(object.id.clone(), object.clone());
    }
    for object in reused {
        if object.origin != origin {
            return Err(LabError::DataCorrupt(
                "mixed synthetic and observed reuse origins".into(),
            ));
        }
        raw.insert(object.id.clone(), object.clone());
    }
    Ok(raw)
}

type DatasetRows = BTreeMap<(String, UtcTimestamp), CandleObservation>;

struct DatasetAssembly {
    coverage: UtcRange,
    origin: MarketDataOrigin,
    rows: DatasetRows,
    raw: BTreeMap<RawObjectId, RawObjectRef>,
    issues: Vec<QualityIssue>,
    reused_rows: u64,
    fetched_rows: u64,
}

#[derive(Default)]
struct RowMerge {
    rows: DatasetRows,
    issues: Vec<QualityIssue>,
    reused_rows: u64,
    fetched_rows: u64,
}

impl RowMerge {
    fn merge(
        &mut self,
        coverage: UtcRange,
        row: &CandleObservation,
        reused: bool,
    ) -> Result<(), LabError> {
        if !coverage.contains(row.candle.open_time_utc) {
            return Ok(());
        }
        if reused {
            self.reused_rows += 1;
        } else {
            self.fetched_rows += 1;
        }
        let key = (row.candle.market.clone(), row.candle.open_time_utc);
        if let Some(existing) = self.rows.get_mut(&key) {
            let identical = existing.content_digest == row.content_digest;
            self.issues.push(issue_for(
                row,
                if identical {
                    QualityKind::DuplicateIdentical
                } else {
                    QualityKind::DuplicateConflict
                },
                if identical {
                    QualitySeverity::Info
                } else {
                    QualitySeverity::Error
                },
                if reused {
                    "duplicate open time against reused stored data"
                } else {
                    "duplicate open time across observed pages"
                },
            )?);
            if identical {
                existing
                    .raw_object_ids
                    .extend(row.raw_object_ids.iter().cloned());
                existing.raw_object_ids.sort();
                existing.raw_object_ids.dedup();
            }
        } else {
            self.rows.insert(key, row.clone());
        }
        Ok(())
    }
}

fn merge_dataset_rows(
    coverage: UtcRange,
    pages: &[CollectionPage],
    reused: &[CandleObservation],
) -> Result<RowMerge, LabError> {
    let mut merged = RowMerge::default();
    for (row, is_reused) in pages
        .iter()
        .flat_map(|page| page.observations.iter().map(|row| (row, false)))
        .chain(reused.iter().map(|row| (row, true)))
    {
        merged.merge(coverage, row, is_reused)?;
    }
    if merged.rows.len() > MAX_DATASET_ROWS {
        return Err(LabError::ResourceLimit(
            "dataset row budget exceeded".into(),
        ));
    }
    Ok(merged)
}

fn record_row_issues(rows: &DatasetRows, issues: &mut Vec<QualityIssue>) -> Result<(), LabError> {
    for row in rows.values() {
        let candle = &row.candle;
        if !candle.completed {
            issues.push(issue_for(
                row,
                QualityKind::IncompleteCandle,
                QualitySeverity::Error,
                "forming candle",
            )?);
        }
        if candle.low > candle.open
            || candle.low > candle.close
            || candle.high < candle.open
            || candle.high < candle.close
        {
            issues.push(issue_for(
                row,
                QualityKind::InvalidOhlc,
                QualitySeverity::Error,
                "OHLC price envelope invalid",
            )?);
        }
        if candle.volume.get() == Decimal::ZERO {
            issues.push(issue_for(
                row,
                QualityKind::ZeroVolume,
                QualitySeverity::Warning,
                "zero volume is not assumed liquidity",
            )?);
        }
    }
    Ok(())
}

fn record_source_gaps(
    request: &CollectRequest,
    coverage: UtcRange,
    rows: &DatasetRows,
    raw: &BTreeMap<RawObjectId, RawObjectRef>,
    issues: &mut Vec<QualityIssue>,
) -> Result<(), LabError> {
    for market in &request.markets {
        let code = market.code();
        let market_rows: BTreeSet<_> = rows
            .keys()
            .filter(|(row_market, _)| row_market.as_str() == code)
            .map(|(_, timestamp)| *timestamp)
            .collect();
        let raw_object_ids: Vec<_> = raw
            .values()
            .filter(|object| object.source_url.contains(code.as_str()))
            .map(|object| object.id.clone())
            .collect();
        let mut cursor = coverage.start();
        let mut gap_start = None;
        let mut gap_count = 0;
        while cursor < coverage.end() {
            if market_rows.contains(&cursor) {
                if let Some(start) = gap_start.take() {
                    issues.push(source_gap_issue(
                        market,
                        start,
                        cursor,
                        gap_count,
                        &raw_object_ids,
                        "Upbit omits no-trade intervals; missing source interval is not synthesized",
                    ));
                    gap_count = 0;
                }
            } else {
                gap_start.get_or_insert(cursor);
                gap_count += 1;
            }
            cursor = UtcTimestamp(
                cursor
                    .0
                    .checked_add_signed(request.data_resolution.duration())
                    .ok_or_else(|| LabError::InvalidConfig("grid timestamp overflow".into()))?,
            );
        }
        if let Some(start) = gap_start {
            issues.push(source_gap_issue(
                market,
                start,
                coverage.end(),
                gap_count,
                &raw_object_ids,
                "source ended before the requested completed grid was covered",
            ));
        }
    }
    Ok(())
}

fn source_gap_issue(
    market: &MarketId,
    start: UtcTimestamp,
    end: UtcTimestamp,
    count: u64,
    raw_object_ids: &[RawObjectId],
    detail: &str,
) -> QualityIssue {
    QualityIssue {
        kind: QualityKind::UnknownSourceGap,
        severity: QualitySeverity::Error,
        market: market.clone(),
        start,
        end,
        count,
        raw_object_ids: raw_object_ids.to_vec(),
        detail: detail.into(),
    }
}

fn issue_for(
    row: &CandleObservation,
    kind: QualityKind,
    severity: QualitySeverity,
    detail: &str,
) -> Result<QualityIssue, LabError> {
    Ok(QualityIssue {
        kind,
        severity,
        market: MarketId::parse_upbit(&row.candle.market)?,
        start: row.candle.open_time_utc,
        end: row.candle.close_time_utc,
        count: 1,
        raw_object_ids: row.raw_object_ids.clone(),
        detail: detail.into(),
    })
}

/// Aggregate complete UTC-aligned constituents; never interpolate finer prices.
/// # Errors
/// Rejects coarser inputs, duplicate/gapped/incomplete constituents or arithmetic overflow.
pub fn resample(
    rows: &[CandleObservation],
    target: CandleInterval,
) -> Result<Vec<CandleObservation>, LabError> {
    let mut groups: BTreeMap<(String, i64), Vec<&CandleObservation>> = BTreeMap::new();
    for row in rows {
        let source_step = row.candle.interval.duration().num_seconds();
        let target_step = target.duration().num_seconds();
        if source_step > target_step || target_step % source_step != 0 || !row.candle.completed {
            return Err(LabError::InvalidConfig(
                "resampling needs complete finer/equal divisible source bars".into(),
            ));
        }
        let anchor = row
            .candle
            .open_time_utc
            .0
            .timestamp()
            .div_euclid(target_step)
            * target_step;
        groups
            .entry((row.candle.market.clone(), anchor))
            .or_default()
            .push(row);
    }
    let mut output = Vec::with_capacity(groups.len());
    for ((_market, anchor), mut constituents) in groups {
        constituents.sort_by_key(|row| row.candle.open_time_utc);
        let first = constituents
            .first()
            .ok_or_else(|| LabError::DataGap("empty resampling group".into()))?;
        let last = constituents
            .last()
            .ok_or_else(|| LabError::DataGap("empty resampling group".into()))?;
        let ratio =
            target.duration().num_seconds() / first.candle.interval.duration().num_seconds();
        if i64::try_from(constituents.len()).ok() != Some(ratio)
            || first.candle.open_time_utc.0.timestamp() != anchor
            || constituents.windows(2).any(|p| {
                p[0].candle.close_time_utc != p[1].candle.open_time_utc
                    || p[0].candle.interval != p[1].candle.interval
            })
        {
            return Err(LabError::DataGap(
                "resampling has missing, duplicate or mixed constituents".into(),
            ));
        }
        let mut volume = Decimal::ZERO;
        let mut turnover = Decimal::ZERO;
        let mut high = first.candle.high;
        let mut low = first.candle.low;
        let mut source_refs = BTreeSet::new();
        for row in &constituents {
            volume = volume
                .checked_add(row.candle.volume.get())
                .ok_or_else(|| LabError::ContractParse("resampled volume overflow".into()))?;
            turnover = turnover
                .checked_add(row.candle.quote_turnover.get())
                .ok_or_else(|| LabError::ContractParse("resampled turnover overflow".into()))?;
            high = high.max(row.candle.high);
            low = low.min(row.candle.low);
            source_refs.extend(row.raw_object_ids.iter().cloned());
        }
        let candle = CandleRecord {
            market: first.candle.market.clone(),
            interval: target,
            open_time_utc: first.candle.open_time_utc,
            close_time_utc: last.candle.close_time_utc,
            open: first.candle.open,
            high,
            low,
            close: last.candle.close,
            volume: AssetQuantity::new(volume)?,
            quote_turnover: QuoteAmount::new(turnover)?,
            completed: true,
        };
        let digest = observation_digest(&candle)?;
        output.push(CandleObservation {
            id: ObservationId::from_seed(digest.as_str()),
            candle,
            content_digest: digest,
            raw_object_ids: source_refs.into_iter().collect(),
            constituent_ids: constituents.iter().map(|row| row.id.clone()).collect(),
        });
    }
    Ok(output)
}

pub use crate::contracts::RESAMPLE_VERSION;

/// Derive complete coarser bars while retaining the original raw evidence catalog.
/// Only complete warmup buckets are included; evaluation boundaries are never moved.
/// # Errors
/// Rejects blocked inputs, incompatible resolution, misalignment or incomplete buckets.
pub fn derive_snapshot(
    source: &DatasetSnapshot,
    target: CandleInterval,
) -> Result<DatasetSnapshot, LabError> {
    let source_step = source
        .manifest
        .request
        .data_resolution
        .duration()
        .num_seconds();
    let target_step = target.duration().num_seconds();
    if source.manifest.status != DatasetStatus::Ready
        || target_step <= source_step
        || target_step % source_step != 0
    {
        return Err(LabError::InvalidConfig(
            "derivation requires a READY snapshot and a strictly coarser divisible interval".into(),
        ));
    }
    source.manifest.request.range.aligned(target)?;
    let warmup_seconds = (source.manifest.request.range.start().0
        - source.manifest.coverage.start().0)
        .num_seconds();
    let warmup_bars = u32::try_from(warmup_seconds / target_step)
        .map_err(|_| LabError::ResourceLimit("derived warmup overflow".into()))?;
    let request = CollectRequest {
        request_id: RequestId::from_seed(&serde_json::to_string(&(
            &source.manifest.id,
            target,
            RESAMPLE_VERSION,
        ))?),
        markets: source.manifest.request.markets.clone(),
        range: source.manifest.request.range,
        data_resolution: target,
        warmup_bars,
        completed_only: true,
    };
    let coverage = request.range.with_warmup(warmup_bars, target)?;
    let constituents: Vec<_> = source
        .observations
        .iter()
        .filter(|row| coverage.contains(row.candle.open_time_utc))
        .cloned()
        .collect();
    let observations = resample(&constituents, target)?;
    let expected = coverage
        .bars(target)?
        .checked_mul(request.markets.len())
        .ok_or_else(|| LabError::ResourceLimit("derived row count overflow".into()))?;
    if observations.len() != expected {
        return Err(LabError::DataGap("derived grid is incomplete".into()));
    }
    let mut snapshot = DatasetSnapshot {
        manifest: DatasetManifest {
            schema_version: source.manifest.schema_version.clone(),
            id: DatasetId::from_seed("unpublished-derived"),
            request,
            coverage,
            status: DatasetStatus::Ready,
            row_count: u64::try_from(observations.len())
                .map_err(|_| LabError::ResourceLimit("derived count overflow".into()))?,
            normalizer_version: format!("{NORMALIZER_VERSION}+{RESAMPLE_VERSION}"),
            gap_policy: source.manifest.gap_policy.clone(),
            semantic_digest: ContentHash::of_bytes(b"unpublished"),
            provenance_digest: ContentHash::of_bytes(b"unpublished"),
            origin: source.manifest.origin,
            raw_objects: source.manifest.raw_objects.clone(),
            quality_issues: source
                .manifest
                .quality_issues
                .iter()
                .filter(|issue| issue.start < coverage.end() && issue.end > coverage.start())
                .cloned()
                .collect(),
            reuse: None,
        },
        observations,
    };
    let digests = dataset_digests(&snapshot)?;
    snapshot.manifest.id = dataset_id(&digests);
    snapshot.manifest.semantic_digest = digests.semantic;
    snapshot.manifest.provenance_digest = digests.provenance;
    Ok(snapshot)
}

#[cfg(test)]
mod tests;
