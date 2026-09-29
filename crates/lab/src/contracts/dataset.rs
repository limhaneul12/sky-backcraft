//! Collection inputs and immutable dataset/provenance contracts.

use super::{
    CandleInterval, CandleRecord, ContentHash, DatasetId, LabError, LimitItem, LimitReport,
    MAX_COLLECTION_EVALUATION_SECONDS, MarketDataOrigin, MarketId, ObservationId, RawObjectId,
    RequestId, RequestSize, UtcTimestamp, active_limits, evaluation_days_ceil,
};
use chrono::Duration;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const NORMALIZER_VERSION: &str = "upbit-candles-v2";
pub const RESAMPLE_VERSION: &str = "utc-ohlcv-sum-v1";
pub const MAX_DATASET_ROWS: usize = 30_000;
pub const MAX_COLLECTION_CALLS: u32 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "RangeInput")]
#[schemars(with = "RangeInput")]
pub struct UtcRange {
    start: UtcTimestamp,
    end: UtcTimestamp,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RangeInput {
    start: UtcTimestamp,
    end: UtcTimestamp,
}

impl TryFrom<RangeInput> for UtcRange {
    type Error = LabError;
    fn try_from(input: RangeInput) -> Result<Self, Self::Error> {
        Self::new(input.start, input.end)
    }
}

impl UtcRange {
    /// Construct a nonempty half-open UTC interval.
    /// # Errors
    /// Rejects equal or reversed endpoints.
    pub fn new(start: UtcTimestamp, end: UtcTimestamp) -> Result<Self, LabError> {
        if start >= end {
            return Err(LabError::InvalidConfig(
                "range must have start < end".into(),
            ));
        }
        Ok(Self { start, end })
    }
    #[must_use]
    pub fn start(self) -> UtcTimestamp {
        self.start
    }
    #[must_use]
    pub fn end(self) -> UtcTimestamp {
        self.end
    }
    #[must_use]
    pub fn contains(self, time: UtcTimestamp) -> bool {
        self.start <= time && time < self.end
    }
    /// Require UTC anchor alignment, including zero fractional seconds.
    /// # Errors
    /// Returns invalid configuration for misaligned boundaries.
    pub fn aligned(self, interval: CandleInterval) -> Result<(), LabError> {
        for value in [self.start, self.end] {
            if value.0.timestamp_subsec_nanos() != 0
                || value
                    .0
                    .timestamp()
                    .rem_euclid(interval.duration().num_seconds())
                    != 0
            {
                return Err(LabError::InvalidConfig(
                    "range boundaries must align to the UTC candle grid".into(),
                ));
            }
        }
        Ok(())
    }
    /// Return the exact grid row count.
    /// # Errors
    /// Rejects misaligned or unrepresentable ranges.
    pub fn bars(self, interval: CandleInterval) -> Result<usize, LabError> {
        self.aligned(interval)?;
        usize::try_from(
            (self.end.0 - self.start.0).num_seconds() / interval.duration().num_seconds(),
        )
        .map_err(|_| LabError::ResourceLimit("range row count exceeds platform capacity".into()))
    }
    /// Include explicitly requested warmup before evaluation.
    /// # Errors
    /// Rejects timestamp overflow.
    pub fn with_warmup(self, bars: u32, interval: CandleInterval) -> Result<Self, LabError> {
        let seconds = interval
            .duration()
            .num_seconds()
            .checked_mul(i64::from(bars))
            .ok_or_else(|| LabError::InvalidConfig("warmup duration overflow".into()))?;
        let start = self
            .start
            .0
            .checked_sub_signed(Duration::seconds(seconds))
            .ok_or_else(|| LabError::InvalidConfig("warmup timestamp overflow".into()))?;
        Self::new(UtcTimestamp(start), self.end)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectRequest {
    pub request_id: RequestId,
    pub markets: Vec<MarketId>,
    pub range: UtcRange,
    pub data_resolution: CandleInterval,
    /// Number of data-resolution bars before range.start (not decision bars).
    pub warmup_bars: u32,
    pub completed_only: bool,
}

impl CollectRequest {
    /// Validate the complete request and calculate its maximum expected grid size.
    /// # Errors
    /// Rejects duplicates, unavailable future bars, non-completed mode and resource excess.
    pub fn validate(&self, observed_now: UtcTimestamp) -> Result<usize, LabError> {
        if self.markets.is_empty() || self.markets.len() > 3 || !self.completed_only {
            return Err(LabError::InvalidConfig(
                "collection requires 1..3 supported markets and completed_only=true".into(),
            ));
        }
        let markets: BTreeSet<_> = self.markets.iter().map(MarketId::code).collect();
        if markets.len() != self.markets.len() {
            return Err(LabError::InvalidConfig(
                "duplicate collection market".into(),
            ));
        }
        if self.range.end() > observed_now {
            return Err(LabError::InvalidConfig(
                "collection end is in the future".into(),
            ));
        }
        let evaluation_seconds = u64::try_from(
            (self.range.end().0 - self.range.start().0).num_seconds(),
        )
        .map_err(|_| LabError::InvalidConfig("collection range duration is invalid".into()))?;
        let evaluation_days = evaluation_days_ceil(evaluation_seconds);
        if evaluation_seconds > MAX_COLLECTION_EVALUATION_SECONDS {
            return Err(LabError::RequestLimit(Box::new(LimitReport::exceeded(
                RequestSize {
                    evaluation_days: Some(evaluation_days),
                    evaluation_seconds: Some(evaluation_seconds),
                    ..RequestSize::default()
                },
                LimitItem::CollectionEvaluationSeconds,
                evaluation_seconds,
                MAX_COLLECTION_EVALUATION_SECONDS,
                &["shorten range so end-start is at most 366 days"],
            ))));
        }
        let rows = self
            .range
            .with_warmup(self.warmup_bars, self.data_resolution)?
            .bars(self.data_resolution)?
            .checked_mul(self.markets.len())
            .ok_or_else(|| LabError::ResourceLimit("collection row count overflow".into()))?;
        if rows > MAX_DATASET_ROWS {
            let requested_rows = u64::try_from(rows).unwrap_or(u64::MAX);
            return Err(LabError::RequestLimit(Box::new(LimitReport::exceeded(
                RequestSize {
                    evaluation_days: Some(evaluation_days),
                    evaluation_seconds: Some(evaluation_seconds),
                    collection_rows: Some(requested_rows),
                    ..RequestSize::default()
                },
                LimitItem::CollectionRows,
                requested_rows,
                active_limits().collection_rows,
                &[
                    "reduce the number of markets",
                    "shorten the range or warmup",
                    "select a coarser data resolution",
                ],
            ))));
        }
        Ok(rows)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RawObjectRef {
    pub id: RawObjectId,
    /// Relative object directory below the configured data root.
    pub relative_path: String,
    pub source_url: String,
    pub fetched_at: UtcTimestamp,
    pub persisted_at: UtcTimestamp,
    pub http_status: u16,
    pub remaining_req: Option<String>,
    pub raw_sha256: ContentHash,
    pub compressed_sha256: ContentHash,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub origin: MarketDataOrigin,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CandleObservation {
    pub id: ObservationId,
    pub candle: CandleRecord,
    pub content_digest: ContentHash,
    pub raw_object_ids: Vec<RawObjectId>,
    /// Resampling preserves constituent observation identity rather than fabricating prices.
    pub constituent_ids: Vec<ObservationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DatasetStatus {
    Ready,
    BlockedData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityKind {
    UnknownSourceGap,
    DuplicateConflict,
    InvalidOhlc,
    IncompleteCandle,
    InsufficientWarmup,
    DataUnavailable,
    SourceCorrupt,
    DuplicateIdentical,
    ZeroVolume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualitySeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QualityIssue {
    pub kind: QualityKind,
    pub severity: QualitySeverity,
    pub market: MarketId,
    pub start: UtcTimestamp,
    pub end: UtcTimestamp,
    pub count: u64,
    pub raw_object_ids: Vec<RawObjectId>,
    pub detail: String,
}

/// Retrieval bookkeeping for one collection that reused stored observations.
///
/// Deliberately excluded from every dataset digest: reuse changes how data was
/// obtained, never which economic values the snapshot contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionReuse {
    /// Observations taken from previously stored collections.
    pub reused_observations: u64,
    /// Observations fetched from the exchange for this request.
    pub fetched_observations: u64,
    /// Upbit API calls actually issued by this request.
    pub api_calls: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub schema_version: String,
    pub id: DatasetId,
    pub request: CollectRequest,
    pub coverage: UtcRange,
    pub status: DatasetStatus,
    pub row_count: u64,
    pub normalizer_version: String,
    pub gap_policy: String,
    pub semantic_digest: ContentHash,
    pub provenance_digest: ContentHash,
    pub origin: MarketDataOrigin,
    pub raw_objects: Vec<RawObjectRef>,
    pub quality_issues: Vec<QualityIssue>,
    /// Present when part of the coverage came from previously stored observations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reuse: Option<CollectionReuse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetSnapshot {
    pub manifest: DatasetManifest,
    /// Stable market-code/open-time order. READY requires the full expected grid.
    pub observations: Vec<CandleObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionPage {
    pub request_id: RequestId,
    pub market: MarketId,
    pub requested_to: UtcTimestamp,
    pub next_to: Option<UtcTimestamp>,
    pub raw_object: RawObjectRef,
    pub observations: Vec<CandleObservation>,
    pub page_index: u32,
}
