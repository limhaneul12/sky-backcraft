//! Canonical economic and provenance identities for observations and datasets.

use super::{
    CandleRecord, CollectRequest, ContentHash, DatasetId, DatasetSnapshot, LabError, MarketId,
    QualityKind, QualitySeverity,
};
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetDigests {
    pub normalized_request: ContentHash,
    pub semantic: ContentHash,
    pub provenance: ContentHash,
}

/// Calculate normalized request, economic semantic, and retrieval provenance identities.
///
/// `DUPLICATE_IDENTICAL/INFO` describes retrieval overlap, so it affects provenance
/// without changing economic semantics. Error/conflict quality remains semantic.
///
/// # Errors
/// Returns an error when a typed value cannot be represented canonically.
pub fn dataset_digests(snapshot: &DatasetSnapshot) -> Result<DatasetDigests, LabError> {
    let request = normalized_request_value(&snapshot.manifest.request)?;
    let normalized_request = normalized_request_digest(&snapshot.manifest.request)?;
    let mut observations: Vec<_> = snapshot
        .observations
        .iter()
        .map(|observation| {
            Ok((
                observation.candle.market.clone(),
                enum_text(&observation.candle.interval)?,
                timestamp_ms(observation.candle.open_time_utc),
                timestamp_ms(observation.candle.close_time_utc),
                canonical_decimal(observation.candle.open.get()),
                canonical_decimal(observation.candle.high.get()),
                canonical_decimal(observation.candle.low.get()),
                canonical_decimal(observation.candle.close.get()),
                canonical_decimal(observation.candle.volume.get()),
                canonical_decimal(observation.candle.quote_turnover.get()),
                observation.candle.completed,
            ))
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    observations.sort();
    let quality: Vec<_> = snapshot
        .manifest
        .quality_issues
        .iter()
        .filter(|issue| {
            !(issue.kind == QualityKind::DuplicateIdentical
                && issue.severity == QualitySeverity::Info)
        })
        .map(|issue| {
            Ok(serde_json::json!({
                "kind": enum_text(&issue.kind)?,
                "severity": enum_text(&issue.severity)?,
                "market": issue.market.code(),
                "start": timestamp_ms(issue.start),
                "end": timestamp_ms(issue.end),
                "count": issue.count,
                "detail": issue.detail
            }))
        })
        .collect::<Result<_, LabError>>()?;
    let semantic = ContentHash::of_value(&serde_json::json!({
        "schema_version": snapshot.manifest.schema_version,
        "request": request,
        "coverage": [timestamp_ms(snapshot.manifest.coverage.start()), timestamp_ms(snapshot.manifest.coverage.end())],
        "status": enum_text(&snapshot.manifest.status)?,
        "normalizer_version": snapshot.manifest.normalizer_version,
        "gap_policy": snapshot.manifest.gap_policy,
        "origin": enum_text(&snapshot.manifest.origin)?,
        "observations": observations,
        "quality": quality
    }))?;
    let provenance = provenance_digest(snapshot, &semantic)?;
    Ok(DatasetDigests {
        normalized_request,
        semantic,
        provenance,
    })
}

fn provenance_digest(
    snapshot: &DatasetSnapshot,
    semantic: &ContentHash,
) -> Result<ContentHash, LabError> {
    let mut raw: Vec<_> = snapshot
        .manifest
        .raw_objects
        .iter()
        .map(|object| {
            (
                object.id.as_str(),
                object.raw_sha256.as_str(),
                object.compressed_sha256.as_str(),
            )
        })
        .collect();
    raw.sort_unstable();
    let mut observation_sources: Vec<_> = snapshot
        .observations
        .iter()
        .map(|observation| {
            (
                observation.id.as_str().to_owned(),
                observation
                    .raw_object_ids
                    .iter()
                    .map(|id| id.as_str().to_owned())
                    .collect::<Vec<_>>(),
                observation
                    .constituent_ids
                    .iter()
                    .map(|id| id.as_str().to_owned())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    observation_sources.sort_unstable();
    ContentHash::of_value(&serde_json::json!({
        "semantic": semantic.as_str(),
        "raw_objects": raw,
        "observation_sources": observation_sources,
        "quality_issues": snapshot.manifest.quality_issues
    }))
}

/// Derive immutable snapshot identity from separate economic and provenance digests.
#[must_use]
pub fn dataset_id(digests: &DatasetDigests) -> DatasetId {
    DatasetId::from_seed(&format!(
        "{}:{}",
        digests.semantic.as_str(),
        digests.provenance.as_str()
    ))
}

/// Hash normalized collection input while excluding its idempotency key.
///
/// # Errors
/// Returns an error when the request cannot be represented canonically.
pub fn normalized_request_digest(request: &CollectRequest) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&normalized_request_value(request)?)
}

/// Hash only normalized economic candle values, canonicalizing decimal scale.
///
/// # Errors
/// Returns an error when the candle cannot be represented canonically.
pub fn observation_digest(candle: &CandleRecord) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&serde_json::json!({
        "market": candle.market,
        "interval": enum_text(&candle.interval)?,
        "open_time_ms": timestamp_ms(candle.open_time_utc),
        "close_time_ms": timestamp_ms(candle.close_time_utc),
        "open": canonical_decimal(candle.open.get()),
        "high": canonical_decimal(candle.high.get()),
        "low": canonical_decimal(candle.low.get()),
        "close": canonical_decimal(candle.close.get()),
        "volume": canonical_decimal(candle.volume.get()),
        "quote_turnover": canonical_decimal(candle.quote_turnover.get()),
        "completed": candle.completed
    }))
}

fn normalized_request_value(request: &CollectRequest) -> Result<serde_json::Value, LabError> {
    let mut markets: Vec<_> = request.markets.iter().map(MarketId::code).collect();
    markets.sort_unstable();
    Ok(serde_json::json!({
        "markets": markets,
        "range": [timestamp_ms(request.range.start()), timestamp_ms(request.range.end())],
        "data_resolution": enum_text(&request.data_resolution)?,
        "warmup_bars": request.warmup_bars,
        "completed_only": request.completed_only
    }))
}

fn timestamp_ms(value: super::UtcTimestamp) -> i64 {
    value.0.timestamp_millis()
}

fn canonical_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn enum_text(value: &impl Serialize) -> Result<String, LabError> {
    serde_json::to_value(value)
        .map_err(LabError::from)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| LabError::Internal("enum did not serialize as string".into()))
}
