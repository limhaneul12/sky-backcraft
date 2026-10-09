//! Pure collection-schedule cadence and failure policies.

use crate::contracts::{
    CollectRequest, CollectionScheduleRecord, CollectionScheduleRequest, CollectionScheduleStatus,
    FailureRecord, FreshnessPolicy, FreshnessState, LabError, RequestId, ScheduleFailureClass,
    ScheduleId, ScheduleRecoveryState, ScheduleRetryPolicy, SourceProbeResult, UtcRange,
    UtcTimestamp,
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
    reason = "one typed classification needs the boundary, lag, policy, probe, failure and next_action inputs"
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
    next_action_at: Option<UtcTimestamp>,
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
    let within_cycle = next_action_at.is_some_and(|next| observed_at < next);
    let within_cycle_grace = next_action_at.is_some_and(|next| {
        let grace = u64::from(policy.grace_seconds);
        observed_at.0 <= next.0 + chrono::Duration::seconds(i64::try_from(grace).unwrap_or(0))
    });
    if let Some(probe) = probe
        && probe.source_has_boundary == Some(true)
    {
        if within_cycle_grace {
            return FreshnessClassification {
                state: FreshnessState::WaitingForFinalization,
                reason: "source exposes the expected boundary; awaiting scheduled collection cycle"
                    .into(),
            };
        }
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
    let consecutive_gap = !within_cycle
        && historical_missing >= u64::from(policy.consecutive_gap_threshold)
        && oldest_historical_age >= u64::from(policy.source_delay_seconds);
    if consecutive_gap {
        return FreshnessClassification {
            state: FreshnessState::TrueGap,
            reason: format!(
                "{historical_missing} consecutive historical boundaries are absent past the source delay"
            ),
        };
    }
    if within_cycle {
        return FreshnessClassification {
            state: FreshnessState::WaitingForFinalization,
            reason: format!(
                "boundary is within the scheduled collection cycle ({historical_missing} bars pending collection)"
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
    FreshnessClassification {
        state: FreshnessState::SourceDelay,
        reason: format!(
            "{historical_missing} consecutive historical boundaries are absent but below the gap threshold"
        ),
    }
}

/// Classify a durable failure record by its stable error code. Codes outside
/// the recoverable set are operator-required (fail closed, never retried
/// into an infinite loop).
#[must_use]
pub fn classify_schedule_failure(failure: &FailureRecord) -> ScheduleFailureClass {
    match failure.code.as_str() {
        "NETWORK_UNAVAILABLE" | "RATE_LIMITED" | "TEMPORARILY_BLOCKED" => {
            ScheduleFailureClass::Recoverable
        }
        _ => ScheduleFailureClass::OperatorRequired,
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
        failure_class: None,
        recovery_state: ScheduleRecoveryState::Active,
        recovery_attempt_count: 0,
        last_probe_at: None,
        last_recovery_at: None,
        pending_gap: None,
        backfill_job_id: None,
        next_recovery_at: None,
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
        || schedule.recovery_state != ScheduleRecoveryState::Active
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
        "NETWORK_UNAVAILABLE" | "RATE_LIMITED" | "TEMPORARILY_BLOCKED"
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

/// Freeze the exact outage range. A never-successful schedule recovers the
/// configured lookback; an established schedule starts at its last committed
/// boundary so every completed candle after the outage is covered.
///
/// # Errors
/// Returns invalid timestamp/range geometry.
pub fn recovery_gap(
    schedule: &CollectionScheduleRecord,
    target: UtcTimestamp,
) -> Result<Option<UtcRange>, LabError> {
    let start = if let Some(last) = schedule.last_success_boundary {
        last
    } else {
        UtcTimestamp(
            target
                .0
                .checked_sub_signed(
                    schedule
                        .request
                        .interval
                        .duration()
                        .checked_mul(i32::try_from(schedule.request.lookback_bars).map_err(
                            |_| LabError::ResourceLimit("schedule lookback exceeds i32".into()),
                        )?)
                        .ok_or_else(|| {
                            LabError::ResourceLimit("schedule recovery range overflow".into())
                        })?,
                )
                .ok_or_else(|| {
                    LabError::InvalidConfig("schedule recovery timestamp overflow".into())
                })?,
        )
    };
    if start >= target {
        return Ok(None);
    }
    UtcRange::new(start, target).map(Some)
}

/// Build one bounded, content-stable recovery chunk. Repeated calls for the
/// same schedule/target/index/generation produce the same ordinary job
/// identity. Generation advances only after the immutable 32-attempt job
/// history is exhausted.
///
/// # Errors
/// Rejects invalid persisted geometry or arithmetic overflow.
pub fn recovery_chunk_request(
    schedule: &CollectionScheduleRecord,
    target: UtcTimestamp,
    next_start: UtcTimestamp,
    chunk_index: u32,
    generation: u32,
) -> Result<CollectRequest, LabError> {
    if next_start >= target {
        return Err(LabError::InvalidConfig(
            "recovery chunk start must precede target".into(),
        ));
    }
    let width = schedule.request.interval.duration().num_seconds();
    if width <= 0 {
        return Err(LabError::InvalidConfig(
            "schedule interval must be positive".into(),
        ));
    }
    let market_count = i64::try_from(schedule.request.markets.len())
        .map_err(|_| LabError::ResourceLimit("recovery market count overflow".into()))?;
    let row_bars = i64::try_from(crate::contracts::MAX_DATASET_ROWS)
        .map_err(|_| LabError::ResourceLimit("recovery row limit overflow".into()))?
        / market_count;
    let duration_bars = i64::try_from(crate::contracts::MAX_COLLECTION_EVALUATION_SECONDS)
        .map_err(|_| LabError::ResourceLimit("recovery duration limit overflow".into()))?
        / width;
    let chunk_bars = row_bars.min(duration_bars).max(1);
    let candidate_end = next_start
        .0
        .checked_add_signed(Duration::seconds(
            width.checked_mul(chunk_bars).ok_or_else(|| {
                LabError::ResourceLimit("recovery chunk duration overflow".into())
            })?,
        ))
        .ok_or_else(|| LabError::InvalidConfig("recovery chunk end overflow".into()))?;
    let end = UtcTimestamp(candidate_end.min(target.0));
    let request_id = RequestId::from_seed(&format!(
        "collection-schedule-recovery:{}:{}:{}:{}",
        schedule.id.as_str(),
        target.to_rfc3339(),
        chunk_index,
        generation
    ));
    Ok(CollectRequest {
        request_id,
        markets: schedule.request.markets.clone(),
        range: UtcRange::new(next_start, end)?,
        data_resolution: schedule.request.interval,
        warmup_bars: 0,
        completed_only: true,
    })
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
