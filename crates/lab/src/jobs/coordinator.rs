//! The JobRuntime-owned coordinator schedules bounded producer and realtime work.

mod schedule_recovery;

use super::JobService;
use crate::contracts::{
    CandleInterval, CollectionScheduleRecord, CollectionScheduleStatus, JobPayload, JobSubmission,
    LabError, MarketId, PlanRequest, RequestId, RunRequest, ScheduleFireStatus, ScheduleId,
    SuiteCase, SuiteCaseStatus, SuiteId, SuitePhase, SuiteRecord, SuiteStatus, UtcTimestamp,
};
use crate::realtime::PublicMarketSubscription;
use crate::scheduling::ScheduleTick;
use crate::upbit::realtime::{PrivateAccountEvent, PrivateRealtimeConsumer};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

enum CoordinatorWake {
    Safety,
    MarketData,
    PublicHealth,
    PrivateHealth,
    PrivateEvent(Box<PrivateAccountEvent>),
    PrivateEventClosed,
    PrivateHealthClosed,
}

fn private_event_generation(event: &PrivateAccountEvent) -> u64 {
    match event {
        PrivateAccountEvent::Baseline { baseline, .. } => baseline.generation,
        PrivateAccountEvent::OrderObserved(observation) => observation.generation,
        PrivateAccountEvent::AssetObserved(observation) => observation.generation,
        PrivateAccountEvent::BootstrapComplete { generation, .. } => *generation,
    }
}

fn invalidate_private_generation(
    readiness: &crate::upbit::realtime::PrivateReadinessFence,
    generation: u64,
) -> Result<(), LabError> {
    match readiness.invalidate_for_recovery(generation) {
        Ok(_) | Err(crate::upbit::realtime::PrivateRealtimeError::StaleGeneration) => Ok(()),
        Err(error) => Err(LabError::Internal(format!(
            "private readiness invalidation failed: {error}"
        ))),
    }
}

fn private_recovery_error_is_transient(error: &LabError) -> bool {
    matches!(
        error,
        LabError::NetworkUnavailable(_)
            | LabError::RateLimited(_)
            | LabError::TemporarilyBlocked(_)
            | LabError::BlockedEvidence(_)
            | LabError::OutcomeUnknown(_)
    )
}

fn disable_private_lane(
    readiness: &crate::upbit::realtime::PrivateReadinessFence,
    generation: u64,
    failure: crate::upbit::realtime::PrivateRealtimeFailure,
    baseline_sequence: &mut Option<(u64, u64)>,
    private_events: &mut Option<tokio::sync::mpsc::Receiver<PrivateAccountEvent>>,
) {
    readiness.mark_owner_failure(generation, failure);
    *baseline_sequence = None;
    *private_events = None;
}

#[derive(Debug)]
enum Producer {
    Suite(Box<SuiteRecord>),
    Schedule(Box<CollectionScheduleRecord>),
}
impl Producer {
    fn order(&self) -> (UtcTimestamp, u8, &str) {
        match self {
            Self::Suite(record) => (record.next_action_at, 0, record.frozen.id.as_str()),
            Self::Schedule(record) => (record.next_action_at, 1, record.id.as_str()),
        }
    }
}

impl JobService {
    #[expect(
        clippy::too_many_lines,
        reason = "this one wake-select state machine owns the safety cadence and all producer subscriptions"
    )]
    pub(super) async fn coordinate(
        self,
        private_consumer: Option<PrivateRealtimeConsumer>,
    ) -> Result<(), LabError> {
        let mut poll = tokio::time::interval(std::time::Duration::from_secs(1));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let market_wake = self
            .inner
            .public_market_feed
            .as_ref()
            .map(crate::realtime::PublicMarketFeed::wake);
        let mut public_health = self
            .inner
            .public_market_feed
            .as_ref()
            .map(crate::realtime::PublicMarketFeed::subscribe_health);
        let (mut private_events, mut private_health, private_readiness) =
            private_consumer.map_or((None, None, None), |consumer| {
                (
                    Some(consumer.events),
                    Some(consumer.health),
                    Some(consumer.readiness),
                )
            });
        let mut active_subscriptions = BTreeSet::new();
        let mut private_baseline_sequence = None::<(u64, u64)>;
        loop {
            let wake = tokio::select! {
                () = self.inner.stop.cancelled() => return Ok(()),
                _ = poll.tick() => CoordinatorWake::Safety,
                () = async {
                    match market_wake.as_ref() {
                        Some(signal) => signal.notified().await,
                        None => std::future::pending().await,
                    }
                } => CoordinatorWake::MarketData,
                changed = async {
                    match public_health.as_mut() {
                        Some(health) => health.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    changed.map_err(|_| LabError::Internal("public feed health channel closed".into()))?;
                    CoordinatorWake::PublicHealth
                },
                changed = async {
                    match private_health.as_mut() {
                        Some(health) => health.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match changed {
                        Ok(()) => CoordinatorWake::PrivateHealth,
                        Err(_) => CoordinatorWake::PrivateHealthClosed,
                    }
                },
                event = async {
                    match private_events.as_mut() {
                        Some(events) => events.recv().await,
                        None => std::future::pending().await,
                    }
                } => match event {
                    Some(event) => CoordinatorWake::PrivateEvent(Box::new(event)),
                    None => CoordinatorWake::PrivateEventClosed,
                },
            };
            if !self.inner.admitting.load(Ordering::Acquire) {
                return Ok(());
            }
            match wake {
                CoordinatorWake::PublicHealth | CoordinatorWake::PrivateHealth => {
                    super::realtime_runtime::publish_status_if_changed(&self);
                }
                CoordinatorWake::PrivateEventClosed | CoordinatorWake::PrivateHealthClosed => {
                    if !self.inner.stop.is_cancelled()
                        && let Some(readiness) = private_readiness.as_ref()
                    {
                        let generation = readiness.snapshot().generation;
                        disable_private_lane(
                            readiness,
                            generation,
                            crate::upbit::realtime::PrivateRealtimeFailure::ConsumerClosed,
                            &mut private_baseline_sequence,
                            &mut private_events,
                        );
                        private_health = None;
                        super::realtime_runtime::publish_status_if_changed(&self);
                    }
                }
                CoordinatorWake::MarketData | CoordinatorWake::Safety => {
                    self.reconcile_active_sessions(&mut active_subscriptions)
                        .await?;
                }
                CoordinatorWake::PrivateEvent(event) => {
                    let Some(readiness) = private_readiness.as_ref() else {
                        return Err(LabError::Internal(
                            "private event arrived without an owned readiness fence".into(),
                        ));
                    };
                    self.handle_private_event(
                        *event,
                        readiness,
                        &mut private_baseline_sequence,
                        &mut private_events,
                        &mut active_subscriptions,
                    )
                    .await?;
                }
            }
        }
    }

    async fn reconcile_active_sessions(
        &self,
        active_subscriptions: &mut BTreeSet<(MarketId, CandleInterval)>,
    ) -> Result<(), LabError> {
        self.refresh_public_market_subscriptions(active_subscriptions)
            .await?;
        if let Some(feed) = &self.inner.public_market_feed {
            let count =
                crate::contracts::ProbeCount::try_from(crate::paper::MAX_PAPER_RECOVERY_BARS)?;
            if let Err(error) = feed
                .reconcile_one_subscription(count, &self.inner.stop)
                .await
            {
                if self.inner.stop.is_cancelled() {
                    return Ok(());
                }
                if matches!(error, LabError::BlockedEvidence(_)) {
                    tracing::debug!(
                        reason = %super::research_control::bounded_note(&error),
                        "public subscription input reconciliation awaiting evidence"
                    );
                } else {
                    tracing::warn!(
                        reason = %super::research_control::bounded_note(&error),
                        "public subscription input reconciliation deferred"
                    );
                }
            }
        }
        let now = UtcTimestamp::now();
        self.coordinate_once(now).await?;
        self.poll_due_paper_markets(now).await;
        Ok(())
    }

    async fn handle_private_event(
        &self,
        event: PrivateAccountEvent,
        readiness: &crate::upbit::realtime::PrivateReadinessFence,
        baseline_sequence: &mut Option<(u64, u64)>,
        private_events: &mut Option<tokio::sync::mpsc::Receiver<PrivateAccountEvent>>,
        active_subscriptions: &mut BTreeSet<(MarketId, CandleInterval)>,
    ) -> Result<(), LabError> {
        let generation = private_event_generation(&event);
        if readiness.snapshot().generation != generation {
            return Ok(());
        }
        match self.consume_private_stream_event(&event).await {
            Ok(Some(sequence)) => *baseline_sequence = Some((generation, sequence)),
            Ok(None) => {}
            Err(error) => {
                invalidate_private_generation(readiness, generation)?;
                *baseline_sequence = None;
                if private_recovery_error_is_transient(&error) {
                    tracing::warn!(
                        event = "private_stream_consumer_recovery_required",
                        error = %error,
                        readiness = "CLOSED_UNTIL_NEW_GENERATION_BASELINE"
                    );
                } else {
                    disable_private_lane(
                        readiness,
                        readiness.snapshot().generation,
                        crate::upbit::realtime::PrivateRealtimeFailure::Reconciliation,
                        baseline_sequence,
                        private_events,
                    );
                }
                super::realtime_runtime::publish_status_if_changed(self);
                return Ok(());
            }
        }
        if let PrivateAccountEvent::BootstrapComplete {
            generation,
            gap_reconciliation_required,
            known_order_readbacks_required,
            closed_order_history_complete,
        } = &event
            && let Err(error) = self
                .complete_private_stream_bootstrap_event(
                    *generation,
                    *gap_reconciliation_required,
                    *known_order_readbacks_required,
                    *closed_order_history_complete,
                    baseline_sequence,
                    readiness,
                )
                .await
        {
            invalidate_private_generation(readiness, *generation)?;
            *baseline_sequence = None;
            if private_recovery_error_is_transient(&error) {
                tracing::warn!(
                    event = "private_bootstrap_recovery_required",
                    error = %error,
                    readiness = "CLOSED_UNTIL_NEW_GENERATION_BASELINE"
                );
            } else {
                disable_private_lane(
                    readiness,
                    readiness.snapshot().generation,
                    crate::upbit::realtime::PrivateRealtimeFailure::Reconciliation,
                    baseline_sequence,
                    private_events,
                );
            }
            super::realtime_runtime::publish_status_if_changed(self);
            return Ok(());
        }
        self.reconcile_active_sessions(active_subscriptions).await?;
        super::realtime_runtime::publish_status_if_changed(self);
        Ok(())
    }

    async fn complete_private_stream_bootstrap_event(
        &self,
        generation: u64,
        gap_reconciliation_required: bool,
        known_order_readbacks_required: bool,
        closed_order_history_complete: bool,
        baseline_sequence: &mut Option<(u64, u64)>,
        readiness: &crate::upbit::realtime::PrivateReadinessFence,
    ) -> Result<(), LabError> {
        let Some((baseline_generation, sequence)) = *baseline_sequence else {
            return Err(LabError::DataCorrupt(
                "private bootstrap completed without its durable baseline".into(),
            ));
        };
        if baseline_generation != generation {
            return Err(LabError::DataCorrupt(
                "private bootstrap generation differs from its baseline".into(),
            ));
        }
        self.complete_private_stream_bootstrap(
            generation,
            gap_reconciliation_required,
            known_order_readbacks_required,
            closed_order_history_complete,
        )
        .await?;
        self.record_private_bootstrap_complete(
            generation,
            sequence,
            gap_reconciliation_required,
            known_order_readbacks_required,
            closed_order_history_complete,
        )
        .await?;
        if !readiness.mark_ready(generation) || !readiness.record_reconciled(generation) {
            invalidate_private_generation(readiness, generation)?;
            *baseline_sequence = None;
        }
        Ok(())
    }

    async fn refresh_public_market_subscriptions(
        &self,
        active: &mut BTreeSet<(MarketId, CandleInterval)>,
    ) -> Result<(), LabError> {
        let Some(feed) = &self.inner.public_market_feed else {
            return Ok(());
        };
        let requirements = self
            .inner
            .database
            .call("active_realtime_market_requirements", |store| {
                store.active_realtime_market_requirements()
            })
            .await?;
        if requirements == *active {
            return Ok(());
        }
        feed.replace_subscriptions(
            requirements
                .iter()
                .map(|(market, interval)| PublicMarketSubscription::new(market.clone(), *interval)),
        )?;
        *active = requirements;
        super::realtime_runtime::publish_status_if_changed(self);
        Ok(())
    }

    async fn coordinate_once(&self, now: UtcTimestamp) -> Result<(), LabError> {
        let (schedules, reconciled_recovery) = self
            .inner
            .database
            .call("schedule_reconciliation", move |store| {
                for id in store.collection_schedule_reconciliation_candidates(8)? {
                    store.reconcile_collection_schedule(&id, now)?;
                }
                let recovery_ids = store.schedule_recovery_reconciliation_candidates(8)?;
                let reconciled_recovery = !recovery_ids.is_empty();
                for id in recovery_ids {
                    store.reconcile_schedule_recovery(&id, now)?;
                }
                let _ = store.reconcile_running_paper_sessions(&[], now)?;
                Ok((store.due_collection_schedules(now, 8)?, reconciled_recovery))
            })
            .await?;
        if reconciled_recovery {
            self.inner.wake.notify_one();
        }
        let recoverable = self
            .inner
            .database
            .call("schedule_recovery_candidates", move |store| {
                store.schedule_recovery_candidates(now, 4)
            })
            .await?;
        for record in recoverable {
            self.recover_schedule(record, now).await;
        }
        let suites = self
            .inner
            .database
            .call("suite_reconciliation", move |store| {
                let mut records = Vec::new();
                for id in store.due_suite_ids(now, 8)? {
                    let record = store.reconcile_research_suite(&id, now)?;
                    if record.status == SuiteStatus::Running {
                        records.push(record);
                    }
                }
                Ok(records)
            })
            .await?;
        let mut producers = Vec::with_capacity(schedules.len() + suites.len());
        producers.extend(
            suites
                .into_iter()
                .map(|record| Producer::Suite(Box::new(record))),
        );
        producers.extend(
            schedules
                .into_iter()
                .map(|record| Producer::Schedule(Box::new(record))),
        );
        producers.sort_by(|a, b| a.order().cmp(&b.order()));
        for producer in producers {
            if self.inner.stop.is_cancelled() {
                return Ok(());
            }
            let admitted = match producer {
                Producer::Suite(record) => self.advance_suite_checked(*record, now).await?,
                Producer::Schedule(record) => self.advance_schedule_checked(*record, now).await?,
            };
            if admitted {
                self.inner.wake.notify_one();
                break;
            }
        }
        self.process_due_automation_sessions(now).await
    }

    async fn advance_suite_checked(
        &self,
        record: SuiteRecord,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let id = record.frozen.id.clone();
        let previous = record
            .cases
            .iter()
            .map(|case| (case.id.clone(), case.status, case.attempt_id.clone()))
            .collect::<Vec<_>>();
        match self.advance_suite(record, now).await {
            Ok(admitted) => Ok(admitted),
            Err(error) => {
                let key = id.clone();
                let current = self
                    .inner
                    .database
                    .call("suite_error_readback", move |store| {
                        store.get_research_suite(&key)
                    })
                    .await?;
                let Some(current) = current else {
                    return Ok(false);
                };
                if current.status != SuiteStatus::Running {
                    return Ok(false);
                }
                if matches!(error, LabError::Conflict(_))
                    && current.cases.iter().any(|case| {
                        previous
                            .iter()
                            .find(|(key, _, _)| *key == case.id)
                            .is_some_and(|(_, status, attempt)| {
                                *status != case.status || *attempt != case.attempt_id
                            })
                    })
                {
                    return Ok(false);
                }
                if matches!(error, LabError::OutcomeUnknown(_)) {
                    return Err(error);
                }
                let failure = super::failure_record(&error);
                self.admitted("block_research_suite", move |store| {
                    if store
                        .get_research_suite(&id)?
                        .is_some_and(|record| record.status == SuiteStatus::Running)
                    {
                        store.block_research_suite(&id, &failure, now)?;
                    }
                    Ok(())
                })
                .await?;
                Ok(false)
            }
        }
    }

    async fn advance_schedule_checked(
        &self,
        record: CollectionScheduleRecord,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let id = record.id.clone();
        let pin = record
            .in_flight
            .as_ref()
            .and_then(|fire| fire.attempt_id.clone());
        match self.advance_schedule(record, now).await {
            Ok(admitted) => Ok(admitted),
            Err(error) => {
                let key = id.clone();
                let current = self
                    .inner
                    .database
                    .call("schedule_error_readback", move |store| {
                        store.get_collection_schedule(&key)
                    })
                    .await?;
                let Some(current) = current else {
                    return Ok(false);
                };
                if current.status != CollectionScheduleStatus::Active {
                    return Ok(false);
                }
                let current_pin = current
                    .in_flight
                    .as_ref()
                    .and_then(|fire| fire.attempt_id.clone());
                if matches!(error, LabError::Conflict(_)) && current_pin != pin {
                    return Ok(false);
                }
                if matches!(error, LabError::OutcomeUnknown(_)) {
                    return Err(error);
                }
                let failure = super::failure_record(&error);
                self.admitted("block_collection_schedule", move |store| {
                    if store
                        .get_collection_schedule(&id)?
                        .is_some_and(|record| record.status == CollectionScheduleStatus::Active)
                    {
                        store.block_collection_schedule(&id, failure, now)?;
                    }
                    Ok(())
                })
                .await?;
                Ok(false)
            }
        }
    }

    async fn advance_suite(
        &self,
        mut record: SuiteRecord,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let suite_id = record.frozen.id.clone();
        if !self
            .commit_pending_fold_selections(&mut record, now)
            .await?
        {
            return Ok(false);
        }
        let id = suite_id.clone();
        let case = self
            .inner
            .database
            .call("next_suite_case", move |store| store.next_suite_case(&id))
            .await?;
        let Some(case) = case else {
            self.defer_suite(suite_id, now).await?;
            return Ok(false);
        };
        let admitted = if case.status == SuiteCaseStatus::RetryPending {
            self.retry_suite_case(&suite_id, &case, now).await?
        } else {
            self.admit_prepared_suite_case(&suite_id, &record, case, now)
                .await?
        };
        if !admitted {
            return Ok(false);
        }
        self.inner.wake.notify_one();
        self.defer_suite(suite_id, now).await?;
        Ok(true)
    }

    async fn commit_pending_fold_selections(
        &self,
        record: &mut SuiteRecord,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let suite_id = record.frozen.id.clone();
        for fold in record.folds.clone() {
            if fold.selection_digest.is_some() {
                continue;
            }
            let selection = record
                .cases
                .iter()
                .filter(|case| {
                    case.fold_index == Some(fold.index) && case.phase == SuitePhase::Selection
                })
                .collect::<Vec<_>>();
            if selection.is_empty()
                || selection.iter().any(|case| {
                    !matches!(
                        case.status,
                        SuiteCaseStatus::Completed | SuiteCaseStatus::Blocked
                    ) || case.run_id.is_none()
                })
            {
                continue;
            }
            let expected_runs = selection
                .iter()
                .map(|case| {
                    case.run_id
                        .clone()
                        .map(|run| (case.id.clone(), run))
                        .ok_or_else(|| {
                            LabError::DataCorrupt("completed selection case has no run".into())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut offset = 0;
            let mut rows = Vec::new();
            loop {
                let id = suite_id.clone();
                let page = self
                    .inner
                    .database
                    .call("selection_comparisons", move |store| {
                        store.list_suite_comparisons(&id, offset, 100)
                    })
                    .await?;
                rows.extend(
                    page.records
                        .into_iter()
                        .filter(|row| {
                            row.fold_index == Some(fold.index) && row.phase == SuitePhase::Selection
                        })
                        .map(|row| row.comparison),
                );
                if let Some(next) = page.next_offset {
                    offset = next;
                } else {
                    break;
                }
            }
            let outcome = crate::research::select_winner(record, fold.index, &rows)?;
            let id = suite_id.clone();
            self.admitted("commit_fold_selection", move |store| {
                store.commit_fold_selection(&id, fold.index, &expected_runs, &outcome)
            })
            .await?;
            let id = suite_id.clone();
            *record = self
                .inner
                .database
                .call("selected_suite", move |store| {
                    store.reconcile_research_suite(&id, now)
                })
                .await?;
            if record.status != SuiteStatus::Running {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn retry_suite_case(
        &self,
        suite_id: &SuiteId,
        case: &SuiteCase,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let case_id = case.id.clone();
        match self
            .admitted("retry_suite_case", move |store| {
                store.retry_suite_case(&case_id, now)
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(LabError::CapacityExceeded(_)) => Ok(false),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let id = suite_id.clone();
                let case_id = case.id.clone();
                let old_pin = case.attempt_id.clone();
                let linked = self
                    .inner
                    .database
                    .call("suite_retry_readback", move |store| {
                        Ok(store.get_research_suite(&id)?.is_some_and(|suite| {
                            suite.cases.iter().any(|case| {
                                case.id == case_id
                                    && case.attempt_id.is_some()
                                    && case.attempt_id != old_pin
                            })
                        }))
                    })
                    .await?;
                if linked { Ok(true) } else { Err(error) }
            }
            Err(error) => Err(error),
        }
    }

    async fn admit_prepared_suite_case(
        &self,
        suite_id: &SuiteId,
        record: &SuiteRecord,
        case: SuiteCase,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        let spec = crate::research::case_spec(record, &case)?;
        let plan_request_id = RequestId::from_seed(&format!(
            "research-plan:{}",
            crate::contracts::ContentHash::of_value(&spec)?
        ));
        let (plan, causal_digest) = crate::planning::prepare_with_causal_digest(
            &self.inner.database,
            PlanRequest {
                request_id: plan_request_id,
                spec,
            },
        )
        .await?;
        let request_id = RequestId::from_seed(case.id.as_str());
        let plan_id = plan.id.clone();
        let submission = JobSubmission {
            request_id: request_id.clone(),
            payload: JobPayload::Backtest {
                request: RunRequest {
                    request_id,
                    plan_id: plan.id,
                    input_digest: plan.input_digest,
                },
            },
        };
        let case_id = case.id.clone();
        let result = self
            .admitted("admit_suite_case", move |store| {
                store.admit_prepared_suite_case(
                    &case_id,
                    &plan_id,
                    &causal_digest,
                    &submission,
                    now,
                )
            })
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(LabError::CapacityExceeded(_)) => Ok(false),
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let id = suite_id.clone();
                let case_id = case.id;
                let linked = self
                    .inner
                    .database
                    .call("suite_admission_readback", move |store| {
                        let suite = store.get_research_suite(&id)?.ok_or_else(|| {
                            LabError::DataCorrupt("suite disappeared during readback".into())
                        })?;
                        Ok(suite.cases.iter().any(|candidate| {
                            candidate.id == case_id
                                && candidate.job_id.is_some()
                                && candidate.attempt_id.is_some()
                        }))
                    })
                    .await?;
                if linked { Ok(true) } else { Err(error) }
            }
            Err(error) => Err(error),
        }
    }

    async fn defer_suite(&self, id: SuiteId, now: UtcTimestamp) -> Result<(), LabError> {
        let next = UtcTimestamp(
            now.0
                .checked_add_signed(chrono::Duration::seconds(1))
                .ok_or_else(|| LabError::ResourceLimit("suite tick overflow".into()))?,
        );
        self.inner
            .database
            .call("defer_suite", move |store| {
                store.defer_research_suite(&id, next)
            })
            .await?;
        Ok(())
    }

    async fn advance_schedule(
        &self,
        record: CollectionScheduleRecord,
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        if record.status != CollectionScheduleStatus::Active {
            return Ok(false);
        }
        if record.in_flight.as_ref().is_some_and(|fire| {
            fire.status == ScheduleFireStatus::RetryWait
                && fire.retry_at.is_some_and(|at| at <= now)
        }) {
            let id = record.id.clone();
            let old_fire = record.in_flight.clone();
            match self
                .admitted("retry_schedule_fire", move |store| {
                    store.retry_collection_schedule_fire(&id, now)
                })
                .await
            {
                Ok(_) => return Ok(true),
                Err(LabError::CapacityExceeded(_)) => return Ok(false),
                Err(error @ LabError::OutcomeUnknown(_)) => {
                    let old = old_fire
                        .ok_or_else(|| LabError::DataCorrupt("retry fire disappeared".into()))?;
                    let job_id = old
                        .job_id
                        .ok_or_else(|| LabError::DataCorrupt("retry fire has no job".into()))?;
                    let linked = self
                        .inner
                        .database
                        .call("schedule_retry_readback", move |store| {
                            Ok(store.get_job(&job_id)?.is_some_and(|job| {
                                job.attempts.last().is_some_and(|attempt| {
                                    Some(&attempt.id) != old.attempt_id.as_ref()
                                })
                            }))
                        })
                        .await?;
                    if linked {
                        return Ok(true);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        match crate::scheduling::schedule_tick(&record, now)? {
            ScheduleTick::NotDue => Ok(false),
            ScheduleTick::NoNewBoundary { check_again_at } => {
                let id = record.id;
                self.inner
                    .database
                    .call("defer_schedule", move |store| {
                        store.defer_collection_schedule(&id, check_again_at)
                    })
                    .await?;
                Ok(false)
            }
            ScheduleTick::Admit {
                boundary,
                submission,
            } => {
                let id = record.id.clone();
                let result = self
                    .admitted("admit_schedule_fire", move |store| {
                        store.admit_collection_schedule_fire(&id, boundary, submission, now)
                    })
                    .await;
                match result {
                    Ok(_) => Ok(true),
                    Err(LabError::CapacityExceeded(_)) => Ok(false),
                    Err(error @ LabError::OutcomeUnknown(_)) => {
                        let id: ScheduleId = record.id;
                        let linked = self
                            .inner
                            .database
                            .call("schedule_admission_readback", move |store| {
                                let schedule =
                                    store.get_collection_schedule(&id)?.ok_or_else(|| {
                                        LabError::DataCorrupt(
                                            "schedule disappeared during readback".into(),
                                        )
                                    })?;
                                Ok(schedule.in_flight.is_some_and(|fire| {
                                    fire.boundary == boundary && fire.job_id.is_some()
                                }))
                            })
                            .await?;
                        if linked { Ok(true) } else { Err(error) }
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
