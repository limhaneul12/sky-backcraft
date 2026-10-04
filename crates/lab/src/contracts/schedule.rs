//! Persistent completed-candle collection schedules and freshness projections.

use super::{
    CandleInterval, ContentHash, DatasetId, FailureRecord, JobId,
    MAX_COLLECTION_EVALUATION_SECONDS, MAX_DATASET_ROWS, MarketId, RequestId, ResearchPage,
    ScheduleId, UtcRange, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_ACTIVE_SCHEDULES: usize = 8;
pub const MAX_SCHEDULE_MARKETS: usize = 3;
pub const MIN_SCHEDULE_CADENCE_SECONDS: u32 = 60;
pub const MAX_SCHEDULE_CADENCE_SECONDS: u32 = 86_400;
pub const MIN_SCHEDULE_RETRIES: u32 = 1;
pub const MAX_SCHEDULE_RETRIES: u32 = 5;
pub const MIN_SCHEDULE_BACKOFF_SECONDS: u32 = 60;
pub const MAX_SCHEDULE_BACKOFF_SECONDS: u32 = 3_600;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRetryPolicy {
    pub max_retries: u32,
    pub backoff_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionScheduleRequest {
    pub request_id: RequestId,
    pub markets: Vec<MarketId>,
    pub interval: CandleInterval,
    pub lookback_bars: u32,
    pub cadence_seconds: u32,
    pub retry: ScheduleRetryPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollectionScheduleStatus {
    Active,
    Paused,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleFireStatus {
    Planned,
    Queued,
    Running,
    RetryWait,
    Completed,
    Blocked,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleFire {
    pub schedule_id: ScheduleId,
    pub boundary: UtcTimestamp,
    pub request_id: RequestId,
    pub status: ScheduleFireStatus,
    pub job_id: Option<JobId>,
    pub attempt_id: Option<super::AttemptId>,
    pub dataset_id: Option<DatasetId>,
    pub created_at: UtcTimestamp,
    pub retry_count: u32,
    pub retry_at: Option<UtcTimestamp>,
    pub failure: Option<FailureRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionScheduleRecord {
    pub id: ScheduleId,
    pub request: CollectionScheduleRequest,
    pub input_digest: ContentHash,
    pub status: CollectionScheduleStatus,
    pub created_at: UtcTimestamp,
    pub next_action_at: UtcTimestamp,
    pub last_success_dataset_id: Option<DatasetId>,
    pub last_success_boundary: Option<UtcTimestamp>,
    pub last_success_coverage: Option<UtcRange>,
    pub failure: Option<FailureRecord>,
    pub in_flight: Option<ScheduleFire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessMissingReason {
    NeverCollected,
    ExpectedBoundaryMissing,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MarketFreshness {
    pub market: MarketId,
    pub expected_end: UtcTimestamp,
    pub latest_completed_end: Option<UtcTimestamp>,
    pub age_seconds: Option<u64>,
    pub gap_count: u64,
    pub missing: Option<FreshnessMissingReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionFreshness {
    pub schedule_id: ScheduleId,
    pub observed_at: UtcTimestamp,
    pub interval: CandleInterval,
    pub markets: Vec<MarketFreshness>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum CollectionScheduleAction {
    Create {
        request: Box<CollectionScheduleRequest>,
    },
    Get {
        schedule_id: ScheduleId,
    },
    List {
        offset: u64,
        limit: u32,
    },
    Pause {
        schedule_id: ScheduleId,
    },
    Resume {
        schedule_id: ScheduleId,
    },
    Freshness {
        schedule_id: ScheduleId,
    },
}

pub type CollectionSchedulePage = ResearchPage<CollectionScheduleRecord>;

impl CollectionScheduleRequest {
    /// Validate resource bounds before a schedule reaches persistence.
    ///
    /// # Errors
    /// Rejects duplicate/empty markets, an oversized lookback, and cadence or
    /// retry values outside the public contract.
    pub fn validate(&self) -> Result<(), super::LabError> {
        if self.markets.is_empty() || self.markets.len() > MAX_SCHEDULE_MARKETS {
            return Err(super::LabError::InvalidConfig(
                "collection schedule requires 1..=3 markets".into(),
            ));
        }
        let unique = self
            .markets
            .iter()
            .map(MarketId::code)
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != self.markets.len() {
            return Err(super::LabError::InvalidConfig(
                "collection schedule markets must be unique".into(),
            ));
        }
        if self.lookback_bars == 0 {
            return Err(super::LabError::InvalidConfig(
                "collection schedule lookback_bars must be positive".into(),
            ));
        }
        let duration_seconds = u64::try_from(self.interval.duration().num_seconds())
            .ok()
            .and_then(|seconds| seconds.checked_mul(u64::from(self.lookback_bars)))
            .ok_or_else(|| {
                super::LabError::ResourceLimit("schedule lookback duration overflow".into())
            })?;
        if duration_seconds > MAX_COLLECTION_EVALUATION_SECONDS {
            return Err(super::LabError::ResourceLimit(format!(
                "collection schedule lookback exceeds {MAX_COLLECTION_EVALUATION_SECONDS} seconds"
            )));
        }
        let rows = usize::try_from(self.lookback_bars)
            .ok()
            .and_then(|bars| bars.checked_mul(self.markets.len()))
            .ok_or_else(|| super::LabError::ResourceLimit("schedule lookback overflow".into()))?;
        if rows > MAX_DATASET_ROWS {
            return Err(super::LabError::ResourceLimit(format!(
                "collection schedule lookback exceeds {MAX_DATASET_ROWS} rows"
            )));
        }
        if !(MIN_SCHEDULE_CADENCE_SECONDS..=MAX_SCHEDULE_CADENCE_SECONDS)
            .contains(&self.cadence_seconds)
        {
            return Err(super::LabError::InvalidConfig(format!(
                "collection schedule cadence_seconds must be in {MIN_SCHEDULE_CADENCE_SECONDS}..={MAX_SCHEDULE_CADENCE_SECONDS}"
            )));
        }
        if !(MIN_SCHEDULE_RETRIES..=MAX_SCHEDULE_RETRIES).contains(&self.retry.max_retries) {
            return Err(super::LabError::InvalidConfig(format!(
                "collection schedule max_retries must be in {MIN_SCHEDULE_RETRIES}..={MAX_SCHEDULE_RETRIES}"
            )));
        }
        if !(MIN_SCHEDULE_BACKOFF_SECONDS..=MAX_SCHEDULE_BACKOFF_SECONDS)
            .contains(&self.retry.backoff_seconds)
        {
            return Err(super::LabError::InvalidConfig(format!(
                "collection schedule backoff_seconds must be in {MIN_SCHEDULE_BACKOFF_SECONDS}..={MAX_SCHEDULE_BACKOFF_SECONDS}"
            )));
        }
        Ok(())
    }
}

impl CollectionScheduleRecord {
    /// Derive the exact successful request coverage from its durable boundary.
    ///
    /// # Errors
    /// Returns timestamp/range overflow for corrupt persisted schedule geometry.
    pub fn derive_last_success_coverage(&self) -> Result<Option<UtcRange>, super::LabError> {
        self.last_success_boundary
            .map(|end| {
                let seconds = self
                    .request
                    .interval
                    .duration()
                    .num_seconds()
                    .checked_mul(i64::from(self.request.lookback_bars))
                    .ok_or_else(|| {
                        super::LabError::DataCorrupt(
                            "schedule success coverage duration overflow".into(),
                        )
                    })?;
                let start = end
                    .0
                    .checked_sub_signed(chrono::Duration::seconds(seconds))
                    .ok_or_else(|| {
                        super::LabError::DataCorrupt(
                            "schedule success coverage timestamp overflow".into(),
                        )
                    })?;
                UtcRange::new(UtcTimestamp(start), end).map_err(|_| {
                    super::LabError::DataCorrupt("invalid schedule success coverage".into())
                })
            })
            .transpose()
    }
}
