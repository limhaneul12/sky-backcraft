//! Network probe boundary for the durable scheduler recovery state machine.

use super::JobService;
use crate::contracts::{
    CollectionScheduleRecord, FailureRecord, LabError, ProbeCount, ScheduleRecoveryState,
    UtcTimestamp,
};

const RECOVERY_PROBE_SPACING: std::time::Duration = std::time::Duration::from_millis(110);

enum ProbeOutcome {
    Ready,
    Failed(FailureRecord),
    Cancelled,
}

impl JobService {
    pub(super) async fn recover_schedule(
        &self,
        candidate: CollectionScheduleRecord,
        now: UtcTimestamp,
    ) {
        if candidate.backfill_job_id.is_some() {
            self.retry_recovery_backfill(&candidate, now).await;
            return;
        }
        let Some(record) = self.claim_recovery_probe(&candidate, now).await else {
            return;
        };
        let target =
            match crate::scheduling::latest_completed_boundary(now, record.request.interval) {
                Ok(target) => target,
                Err(error) => {
                    self.persist_probe_failure(&record, super::super::failure_record(&error), now)
                        .await;
                    return;
                }
            };
        if !self.wait_probe_spacing().await {
            return;
        }
        match self.probe_all_markets(&record, target).await {
            ProbeOutcome::Ready => self.begin_recovery_backfill(&record, target, now).await,
            ProbeOutcome::Failed(failure) => {
                self.persist_probe_failure(&record, failure, now).await;
            }
            ProbeOutcome::Cancelled => {}
        }
    }

    async fn retry_recovery_backfill(
        &self,
        candidate: &CollectionScheduleRecord,
        now: UtcTimestamp,
    ) {
        let id = candidate.id.clone();
        match self
            .admitted("retry_schedule_recovery_backfill", move |store| {
                store.retry_schedule_recovery_backfill(&id, now)
            })
            .await
        {
            Ok(_) => self.inner.wake.notify_one(),
            Err(error @ LabError::CapacityExceeded(_)) => {
                self.persist_probe_failure(candidate, temporary_failure(&error), now)
                    .await;
            }
            Err(LabError::OutcomeUnknown(_)) => {
                if self.backfill_retry_confirmed(candidate).await {
                    self.inner.wake.notify_one();
                }
            }
            Err(LabError::Conflict(_)) => {}
            Err(error) => {
                self.persist_probe_failure(candidate, super::super::failure_record(&error), now)
                    .await;
            }
        }
    }

    async fn claim_recovery_probe(
        &self,
        candidate: &CollectionScheduleRecord,
        now: UtcTimestamp,
    ) -> Option<CollectionScheduleRecord> {
        let id = candidate.id.clone();
        match self
            .inner
            .database
            .call("claim_schedule_recovery_probe", move |store| {
                store.claim_schedule_recovery_probe(&id, now)
            })
            .await
        {
            Ok(record) if record.recovery_state == ScheduleRecoveryState::Probing => Some(record),
            Ok(_) | Err(LabError::Conflict(_)) => None,
            Err(error) => {
                tracing::warn!(
                    event = "schedule_recovery_claim_failed",
                    schedule_id = %candidate.id,
                    error = %error
                );
                None
            }
        }
    }

    async fn probe_all_markets(
        &self,
        record: &CollectionScheduleRecord,
        target: UtcTimestamp,
    ) -> ProbeOutcome {
        let count = match ProbeCount::try_from(1) {
            Ok(count) => count,
            Err(error) => return ProbeOutcome::Failed(super::super::failure_record(&error)),
        };
        for (index, market) in record.request.markets.iter().enumerate() {
            let result = self
                .inner
                .upbit
                .fetch_completed_candles(market, record.request.interval, count, None)
                .await;
            let outcome = probe_result(result, target, market.code().as_str());
            if !matches!(&outcome, ProbeOutcome::Ready) {
                return outcome;
            }
            if index + 1 < record.request.markets.len() && !self.wait_probe_spacing().await {
                return ProbeOutcome::Cancelled;
            }
        }
        ProbeOutcome::Ready
    }

    async fn begin_recovery_backfill(
        &self,
        record: &CollectionScheduleRecord,
        target: UtcTimestamp,
        now: UtcTimestamp,
    ) {
        let id = record.id.clone();
        match self
            .admitted("begin_schedule_recovery_backfill", move |store| {
                store.begin_schedule_recovery_backfill(&id, target, now)
            })
            .await
        {
            Ok(_) => {
                self.inner.wake.notify_one();
                tracing::info!(
                    event = "schedule_recovery_backfill_started",
                    schedule_id = %record.id,
                    target = %target
                );
            }
            Err(error @ LabError::CapacityExceeded(_)) => {
                self.persist_probe_failure(record, temporary_failure(&error), now)
                    .await;
            }
            Err(LabError::OutcomeUnknown(_)) => {
                if self.backfill_admission_confirmed(record).await {
                    self.inner.wake.notify_one();
                }
            }
            Err(LabError::Conflict(error)) => tracing::info!(
                event = "schedule_recovery_backfill_not_admitted",
                schedule_id = %record.id,
                reason = error
            ),
            Err(error) => {
                self.persist_probe_failure(record, super::super::failure_record(&error), now)
                    .await;
            }
        }
    }

    async fn wait_probe_spacing(&self) -> bool {
        tokio::select! {
            () = self.inner.stop.cancelled() => false,
            () = tokio::time::sleep(RECOVERY_PROBE_SPACING) => true
        }
    }

    async fn backfill_retry_confirmed(&self, candidate: &CollectionScheduleRecord) -> bool {
        let id = candidate.id.clone();
        self.inner
            .database
            .call("recovery_backfill_retry_readback", move |store| {
                Ok(store.get_collection_schedule(&id)?.is_some_and(|record| {
                    record.recovery_state == ScheduleRecoveryState::Backfilling
                        && record.backfill_job_id.is_some()
                        && record.next_recovery_at.is_none()
                }))
            })
            .await
            .unwrap_or(false)
    }

    async fn backfill_admission_confirmed(&self, record: &CollectionScheduleRecord) -> bool {
        let id = record.id.clone();
        self.inner
            .database
            .call("recovery_backfill_admission_readback", move |store| {
                Ok(store.get_collection_schedule(&id)?.is_some_and(|record| {
                    matches!(
                        record.recovery_state,
                        ScheduleRecoveryState::Backfilling
                            | ScheduleRecoveryState::VerifyingFreshness
                    )
                }))
            })
            .await
            .unwrap_or(false)
    }

    async fn persist_probe_failure(
        &self,
        record: &CollectionScheduleRecord,
        failure: FailureRecord,
        now: UtcTimestamp,
    ) {
        let id = record.id.clone();
        match self
            .inner
            .database
            .call("defer_schedule_recovery", move |store| {
                store.defer_schedule_recovery(&id, &failure, now)
            })
            .await
        {
            Ok(_) => tracing::info!(
                event = "schedule_recovery_deferred",
                schedule_id = %record.id
            ),
            Err(error) => tracing::error!(
                event = "schedule_recovery_defer_failed",
                schedule_id = %record.id,
                error = %error
            ),
        }
    }
}

fn temporary_failure(error: &LabError) -> FailureRecord {
    FailureRecord {
        code: "TEMPORARILY_BLOCKED".into(),
        message: error.to_string(),
    }
}

fn probe_result(
    result: Result<crate::contracts::ProbeReport, LabError>,
    target: UtcTimestamp,
    market: &str,
) -> ProbeOutcome {
    match result {
        Ok(report)
            if report
                .candles
                .iter()
                .any(|candle| candle.completed && candle.close_time_utc == target) =>
        {
            ProbeOutcome::Ready
        }
        Ok(_) => ProbeOutcome::Failed(FailureRecord {
            code: "TEMPORARILY_BLOCKED".into(),
            message: format!("source has not published target {target} for {market}"),
        }),
        Err(LabError::DataGap(message)) => ProbeOutcome::Failed(FailureRecord {
            code: "TEMPORARILY_BLOCKED".into(),
            message,
        }),
        Err(error) => ProbeOutcome::Failed(super::super::failure_record(&error)),
    }
}
