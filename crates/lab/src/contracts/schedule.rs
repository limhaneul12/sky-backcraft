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
    /// Optional freshness classification knobs; absent uses the interval defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_policy: Option<FreshnessPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FreshnessPolicy {
    /// A boundary younger than this is still publishing, never a gap.
    pub grace_seconds: u32,
    /// Maximum plausible source publication lag before a missing boundary
    /// becomes a candidate true gap.
    pub source_delay_seconds: u32,
    /// Consecutive absent historical boundaries (after the source delay)
    /// required before classifying `TRUE_GAP`.
    pub consecutive_gap_threshold: u32,
}

impl FreshnessPolicy {
    /// # Errors
    /// Rejects unbounded or inverted classification windows.
    pub fn validate(&self) -> Result<(), super::LabError> {
        if self.grace_seconds > 3_600 || self.source_delay_seconds > 86_400 {
            return Err(super::LabError::InvalidConfig(
                "freshness grace must be <= 3600s and source delay <= 86400s".into(),
            ));
        }
        if !(1..=100).contains(&self.consecutive_gap_threshold) {
            return Err(super::LabError::InvalidConfig(
                "consecutive gap threshold must be in 1..=100".into(),
            ));
        }
        if self.source_delay_seconds < self.grace_seconds {
            return Err(super::LabError::InvalidConfig(
                "source delay must not be shorter than the finalization grace".into(),
            ));
        }
        Ok(())
    }

    /// Conservative defaults derived from the candle width: half a bar of
    /// finalization grace and up to two bars of publication lag.
    #[must_use]
    pub fn for_interval(interval: CandleInterval) -> Self {
        let seconds = u32::try_from(interval.duration().num_seconds()).unwrap_or(86_400);
        Self {
            grace_seconds: (seconds / 2).clamp(30, 600),
            source_delay_seconds: seconds.saturating_mul(2).min(3_600),
            consecutive_gap_threshold: 2,
        }
    }
}

/// Classified freshness of one market boundary; `expected boundary missing`
/// alone never implies a true gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessState {
    Fresh,
    WaitingForFinalization,
    SourceDelay,
    CollectorDelay,
    TrueGap,
    Failed,
}

/// Result of an optional live source probe taken during freshness evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceProbeResult {
    pub attempted: bool,
    /// Whether the source API exposed the expected boundary candle.
    pub source_has_boundary: Option<bool>,
    pub note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollectionScheduleStatus {
    Active,
    Paused,
    Blocked,
}

/// Stable classification of the failure currently governing automatic
/// schedule recovery. Absence means the schedule has no recovery failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleFailureClass {
    Recoverable,
    OperatorRequired,
}

/// Durable scheduler recovery lifecycle. This is independent from the owner
/// pause status: pausing stops/cancels work while preserving the exact recovery
/// checkpoint for a later resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleRecoveryState {
    Active,
    Degraded,
    RecoveryWait,
    Probing,
    Backfilling,
    VerifyingFreshness,
    OperatorRequired,
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
    pub failure_class: Option<ScheduleFailureClass>,
    pub recovery_state: ScheduleRecoveryState,
    pub recovery_attempt_count: u32,
    pub last_probe_at: Option<UtcTimestamp>,
    pub last_recovery_at: Option<UtcTimestamp>,
    pub pending_gap: Option<UtcRange>,
    pub backfill_job_id: Option<JobId>,
    pub next_recovery_at: Option<UtcTimestamp>,
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
    /// Consecutive absent historical boundaries immediately before `expected_end`.
    pub consecutive_missing: u64,
    pub state: FreshnessState,
    pub state_reason: String,
    /// Present only when the freshness request asked for a live source probe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_probe: Option<SourceProbeResult>,
}

/// Distinguishable result of one schedule mutation (pause/resume).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScheduleMutationOutcome {
    /// The schedule was already in the requested state; nothing changed.
    NotApplied,
    /// The mutation was applied and acknowledged in this call.
    Applied,
    /// The mutation probably applied but its response was lost; confirmed by read-back.
    AppliedResponseLost,
    /// A conflicting mutation won; the read-back shows the requested state.
    Conflicted,
    /// Neither the prior nor the requested state was confirmed; reconciliation required.
    ReconciliationRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionFreshness {
    pub schedule_id: ScheduleId,
    pub observed_at: UtcTimestamp,
    pub interval: CandleInterval,
    pub markets: Vec<MarketFreshness>,
    pub failure_class: Option<ScheduleFailureClass>,
    pub recovery_state: ScheduleRecoveryState,
    pub recovery_attempt_count: u32,
    pub last_probe_at: Option<UtcTimestamp>,
    pub last_recovery_at: Option<UtcTimestamp>,
    pub pending_gap: Option<UtcRange>,
    pub backfill_job_id: Option<JobId>,
    pub next_recovery_at: Option<UtcTimestamp>,
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
        /// Probe the live source to distinguish collector delay from source delay.
        #[serde(default)]
        probe_source: bool,
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
        if let Some(policy) = &self.freshness_policy {
            policy.validate()?;
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
