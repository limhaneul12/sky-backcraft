//! Pure collection-schedule cadence and failure policies.

use crate::contracts::{
    CollectRequest, CollectionScheduleRecord, CollectionScheduleRequest, CollectionScheduleStatus,
    FailureRecord, FreshnessPolicy, FreshnessState, LabError, RequestId, ScheduleId,
    ScheduleRetryPolicy, SourceProbeResult, UtcRange, UtcTimestamp,
};
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub enum ScheduleTick {
    NotDue,
    NoNewBoundary {
        check_again_at: UtcTimestamp,
    },
    Admit {
        boundary: UtcTimestamp,
        submission: CollectRequest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleFailureAction {
    RetryAt {
        retry_count: u32,
        retry_at: UtcTimestamp,
    },
    Block,
}

/// Classified freshness outcome with its bounded, deterministic reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessClassification {
    pub state: FreshnessState,
    pub reason: String,
}

/// Classify one market boundary without assuming that a missing boundary is a
/// true gap: finalization grace, publication lag and consecutive absence are
/// separated before `TRUE_GAP` may be declared. `interval_seconds` is the
/// candle width; historical boundaries older than the current one only count
/// as a true gap once their own publication window has expired.
#[allow(
    clippy::too_many_arguments,
    reason = "one typed classification needs the boundary, lag, policy, probe and failure inputs"
)]
#[must_use]
pub fn classify_market_freshness(
    expected_end: UtcTimestamp,
    latest_completed_end: Option<UtcTimestamp>,
    observed_at: UtcTimestamp,
    consecutive_missing: u64,
    interval_seconds: u64,
    policy: &FreshnessPolicy,
    probe: Option<&SourceProbeResult>,
    schedule_failure: Option<&FailureRecord>,
) -> FreshnessClassification {
    if let Some(failure) = schedule_failure {
        return FreshnessClassification {
            state: FreshnessState::Failed,
            reason: format!("schedule blocked: {} ({})", failure.code, failure.message),
        };
    }
    if latest_completed_end.is_some_and(|latest| latest >= expected_end) {
        return FreshnessClassification {
            state: FreshnessState::Fresh,
            reason: "latest completed boundary covers the expected boundary".into(),
        };
    }
    if probe.is_some_and(|probe| probe.source_has_boundary == Some(true)) {
        return FreshnessClassification {
            state: FreshnessState::CollectorDelay,
            reason: "source exposes the expected boundary; the collector has not committed it"
                .into(),
        };
    }
    let missing_for =
        u64::try_from((observed_at.0 - expected_end.0).num_seconds()).unwrap_or(u64::MAX);
    if missing_for < u64::from(policy.grace_seconds) {
        return FreshnessClassification {
            state: FreshnessState::WaitingForFinalization,
            reason: format!(
                "boundary is within the {}s finalization grace",
                policy.grace_seconds
            ),
        };
    }
    // Boundaries before the current expected one; the expected boundary itself
    // may still be publishing, so only its predecessors prove a real hole.
    let historical_missing = consecutive_missing.saturating_sub(1);
    let oldest_historical_age =
        missing_for.saturating_add(historical_missing.saturating_mul(interval_seconds));
    let consecutive_gap = historical_missing >= u64::from(policy.consecutive_gap_threshold)
        && oldest_historical_age >= u64::from(policy.source_delay_seconds);
    if consecutive_gap {
        return FreshnessClassification {
            state: FreshnessState::TrueGap,
            reason: format!(
                "{historical_missing} consecutive historical boundaries are absent past the source delay"
            ),
        };
    }
    if missing_for < u64::from(policy.source_delay_seconds) {
        return FreshnessClassification {
            state: FreshnessState::SourceDelay,
            reason: format!(
                "boundary is within the {}s source publication window",
                policy.source_delay_seconds
            ),
        };
    }
    if consecutive_gap || historical_missing >= u64::from(policy.consecutive_gap_threshold) {
        return FreshnessClassification {
            state: FreshnessState::TrueGap,
            reason: format!(
                "{historical_missing} consecutive historical boundaries are absent past the source delay"
            ),
        };
    }
    FreshnessClassification {
        state: FreshnessState::SourceDelay,
        reason: format!(
            "{historical_missing} consecutive historical boundaries are absent but below the gap threshold"
        ),
    }
}

/// Freeze a validated public request into its initial durable state.
///
/// # Errors
/// Returns validation, serialization, or timestamp arithmetic failures.
pub fn create_schedule(
    request: CollectionScheduleRequest,
    now: UtcTimestamp,
) -> Result<CollectionScheduleRecord, LabError> {
    request.validate()?;
    let input_digest = crate::contracts::ContentHash::of_value(&request)?;
    let id = ScheduleId::from_seed(request.request_id.as_str());
    Ok(CollectionScheduleRecord {
        id,
        request,
        input_digest,
        status: CollectionScheduleStatus::Active,
        created_at: now,
        next_action_at: now,
        last_success_dataset_id: None,
        last_success_boundary: None,
        last_success_coverage: None,
        failure: None,
        in_flight: None,
    })
}

/// Project one scheduler tick without reading a clock or performing effects.
/// Missed cadence windows coalesce to the latest completed candle boundary.
///
/// # Errors
/// Returns timestamp/range arithmetic failures.
pub fn schedule_tick(
    schedule: &CollectionScheduleRecord,
    now: UtcTimestamp,
) -> Result<ScheduleTick, LabError> {
    if schedule.status != CollectionScheduleStatus::Active
        || schedule.in_flight.is_some()
        || now < schedule.next_action_at
    {
        return Ok(ScheduleTick::NotDue);
    }
    let boundary = latest_completed_boundary(now, schedule.request.interval)?;
    if schedule
        .last_success_boundary
        .is_some_and(|last| boundary <= last)
    {
        return Ok(ScheduleTick::NoNewBoundary {
            check_again_at: checked_add_seconds(now, schedule.request.cadence_seconds)?,
        });
    }
    let range = lookback_range(&schedule.request, boundary)?;
    let request_id = fire_request_id(&schedule.id, boundary);
    Ok(ScheduleTick::Admit {
        boundary,
        submission: CollectRequest {
            request_id,
            markets: schedule.request.markets.clone(),
            range,
            data_resolution: schedule.request.interval,
            warmup_bars: 0,
            completed_only: true,
        },
    })
}

/// Classify the durable worker failure representation used during reconciliation.
///
/// # Errors
/// Returns timestamp overflow while calculating the explicit retry instant.
pub fn classify_failure_record(
    failure: &FailureRecord,
    completed_retries: u32,
    retry: &ScheduleRetryPolicy,
    now: UtcTimestamp,
) -> Result<ScheduleFailureAction, LabError> {
    let transient = matches!(
        failure.code.as_str(),
        "NETWORK_UNAVAILABLE" | "RATE_LIMITED"
    );
    if !transient || completed_retries >= retry.max_retries {
        return Ok(ScheduleFailureAction::Block);
    }
    let retry_count = completed_retries
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("schedule retry count overflow".into()))?;
    Ok(ScheduleFailureAction::RetryAt {
        retry_count,
        retry_at: checked_add_seconds(now, retry.backoff_seconds)?,
    })
}

/// Advance cadence only after a completed fire or a duplicate-boundary check.
/// Queue-pressure deferral deliberately does not call this function.
///
/// # Errors
/// Returns timestamp overflow.
pub fn next_action_after_cadence(
    now: UtcTimestamp,
    cadence_seconds: u32,
) -> Result<UtcTimestamp, LabError> {
    checked_add_seconds(now, cadence_seconds)
}

/// UTC floor of `now` is the latest boundary whose preceding candle is complete.
///
/// # Errors
/// Returns invalid configuration for a nonpositive interval or timestamp overflow.
pub fn latest_completed_boundary(
    now: UtcTimestamp,
    interval: crate::contracts::CandleInterval,
) -> Result<UtcTimestamp, LabError> {
    let seconds = interval.duration().num_seconds();
    if seconds <= 0 {
        return Err(LabError::InvalidConfig(
            "schedule interval must be positive".into(),
        ));
    }
    let epoch = now.0.timestamp();
    let aligned = epoch - epoch.rem_euclid(seconds);
    DateTime::<Utc>::from_timestamp(aligned, 0)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("schedule boundary overflow".into()))
}

#[must_use]
pub fn fire_request_id(schedule_id: &ScheduleId, boundary: UtcTimestamp) -> RequestId {
    RequestId::from_seed(&format!(
        "collection-schedule:{}:{}",
        schedule_id.as_str(),
        boundary.to_rfc3339()
    ))
}

fn lookback_range(
    request: &CollectionScheduleRequest,
    end: UtcTimestamp,
) -> Result<UtcRange, LabError> {
    let seconds = request
        .interval
        .duration()
        .num_seconds()
        .checked_mul(i64::from(request.lookback_bars))
        .ok_or_else(|| LabError::InvalidConfig("schedule lookback duration overflow".into()))?;
    let start = end
        .0
        .checked_sub_signed(Duration::seconds(seconds))
        .ok_or_else(|| LabError::InvalidConfig("schedule lookback timestamp overflow".into()))?;
    UtcRange::new(UtcTimestamp(start), end)
}

fn checked_add_seconds(now: UtcTimestamp, seconds: u32) -> Result<UtcTimestamp, LabError> {
    now.0
        .checked_add_signed(Duration::seconds(i64::from(seconds)))
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::InvalidConfig("schedule timestamp overflow".into()))
}

#[cfg(test)]
mod tests;
