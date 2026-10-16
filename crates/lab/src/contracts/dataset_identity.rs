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

// Field order preserves the lexical map order of the original json! payloads.
// Nested maps remain Value so their own canonical key ordering is unchanged.
#[derive(Serialize)]
struct SemanticIdentity<'a, T> {
    coverage: [i64; 2],
    gap_policy: &'a str,
    normalizer_version: &'a str,
    observations: &'a [T],
    origin: String,
    quality: &'a [serde_json::Value],
    request: &'a serde_json::Value,
    schema_version: &'a str,
    status: String,
}

#[derive(Serialize)]
struct ProvenanceIdentity<'a, T> {
    observation_sources: &'a [T],
    quality_issues: serde_json::Value,
    raw_objects: &'a [(&'a str, &'a str, &'a str)],
    semantic: &'a str,
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
    let normalized_request = ContentHash::of_value(&request)?;
    let mut observations: Vec<_> = snapshot
        .observations
        .iter()
        .map(|observation| {
            Ok((
                observation.candle.market.as_str(),
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
    let semantic = ContentHash::of_value(&SemanticIdentity {
        coverage: [
            timestamp_ms(snapshot.manifest.coverage.start()),
            timestamp_ms(snapshot.manifest.coverage.end()),
        ],
        gap_policy: &snapshot.manifest.gap_policy,
        normalizer_version: &snapshot.manifest.normalizer_version,
        observations: &observations,
        origin: enum_text(&snapshot.manifest.origin)?,
        quality: &quality,
        request: &request,
        schema_version: &snapshot.manifest.schema_version,
        status: enum_text(&snapshot.manifest.status)?,
    })?;
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
                observation.id.as_str(),
                observation
                    .raw_object_ids
                    .iter()
                    .map(super::RawObjectId::as_str)
                    .collect::<Vec<_>>(),
                observation
                    .constituent_ids
                    .iter()
                    .map(super::ObservationId::as_str)
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    observation_sources.sort_unstable();
    ContentHash::of_value(&ProvenanceIdentity {
        observation_sources: &observation_sources,
        quality_issues: serde_json::to_value(&snapshot.manifest.quality_issues)?,
        raw_objects: &raw,
        semantic: semantic.as_str(),
    })
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
