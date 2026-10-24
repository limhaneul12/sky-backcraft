use super::JobService;
use crate::contracts::{RealtimeConnectionState, RealtimeFeedStatus, RealtimeStatus, UtcTimestamp};
use crate::realtime::{PublicMarketFeed, PublicMarketFeedConfig, PublicMarketFeedRuntime};
use crate::upbit::client::UpbitPrivateClient;
use crate::upbit::realtime::{
    PrivateRealtimeConsumer, PrivateRealtimeError, PrivateRealtimeProducer,
    private_account_realtime_channels, run_private_account_stream,
};
use crate::{contracts::LabError, database::DatabaseHandle, market_data::UpbitClient};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(super) struct OwnedRealtimeRuntime {
    pub(super) market_owner: Option<PublicMarketFeedRuntime>,
    pub(super) private_consumer: Option<PrivateRealtimeConsumer>,
    pub(super) private_task: Option<JoinHandle<Result<(), PrivateRealtimeError>>>,
}

enum RuntimeCompletion {
    Worker(Result<Result<(), LabError>, tokio::task::JoinError>),
    Coordinator(Result<Result<(), LabError>, tokio::task::JoinError>),
    Market(Result<(), LabError>),
}

pub(super) fn initial_status() -> RealtimeStatus {
    RealtimeStatus {
        observed_at_utc: UtcTimestamp::now(),
        market: RealtimeFeedStatus::unavailable(),
        private: RealtimeFeedStatus::unavailable(),
    }
}

pub(super) fn status_channel() -> watch::Sender<RealtimeStatus> {
    watch::channel(initial_status()).0
}

pub(super) fn status_snapshot(service: &JobService) -> RealtimeStatus {
    let market = service
        .inner
        .public_market_feed
        .as_ref()
        .map_or_else(RealtimeFeedStatus::unavailable, |feed| feed.health().status);
    let private = if service.inner.automation_live_venue.is_live() {
        private_status(&service.inner.automation_live_venue)
    } else {
        RealtimeFeedStatus::unavailable()
    };
    RealtimeStatus {
        observed_at_utc: UtcTimestamp::now(),
        market,
        private,
    }
}

pub(super) fn publish_status_if_changed(service: &JobService) {
    let mut next = status_snapshot(service);
    let previous = service.inner.realtime_status.borrow().clone();
    let observed_at = UtcTimestamp::now();
    if same_material_health(&previous, &next)
        && observed_at
            .0
            .signed_duration_since(previous.observed_at_utc.0)
            < chrono::Duration::seconds(5)
    {
        return;
    }
    next.observed_at_utc = observed_at;
    service.inner.realtime_status.send_replace(next);
}

pub(super) fn live_private_readiness_required(
    service: &JobService,
    mode: crate::contracts::AutomationSessionMode,
) -> bool {
    mode == crate::contracts::AutomationSessionMode::Live
        && service.inner.automation_live_venue.is_live()
}

pub(super) fn live_private_readiness_is_open(
    service: &JobService,
    mode: crate::contracts::AutomationSessionMode,
) -> bool {
    !live_private_readiness_required(service, mode)
        || service
            .inner
            .automation_live_venue
            .private_readiness_fence()
            .is_ready()
}

pub(super) fn start_public_feed(
    upbit: UpbitClient,
    database: DatabaseHandle,
    config: PublicMarketFeedConfig,
) -> Result<(PublicMarketFeed, PublicMarketFeedRuntime), LabError> {
    PublicMarketFeed::start(upbit, database, config)
}

pub(super) fn private_channels(
    client: &UpbitPrivateClient,
) -> Option<(PrivateRealtimeProducer, PrivateRealtimeConsumer)> {
    client
        .is_live()
        .then(|| private_account_realtime_channels(client))
}

pub(super) fn spawn_private_stream(
    client: UpbitPrivateClient,
    stop: CancellationToken,
    producer: PrivateRealtimeProducer,
) -> JoinHandle<Result<(), PrivateRealtimeError>> {
    tokio::spawn(run_private_account_stream(client, stop, producer))
}

pub(super) fn private_task_error(
    result: &Result<Result<(), PrivateRealtimeError>, tokio::task::JoinError>,
) -> LabError {
    match result {
        Ok(Ok(())) => LabError::Internal("private realtime task exited before shutdown".into()),
        Ok(Err(error)) => {
            LabError::NetworkUnavailable(format!("private realtime task ended: {error}"))
        }
        Err(_) => LabError::Internal("private realtime task panicked or was aborted".into()),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one JobRuntime owner observes child exits, closes admission, cancels, and joins every owned task"
)]
pub(super) async fn run_owned_runtime(
    service: JobService,
    realtime: OwnedRealtimeRuntime,
) -> Result<(), LabError> {
    let OwnedRealtimeRuntime {
        market_owner,
        private_consumer,
        private_task,
    } = realtime;
    let owner = service.clone();
    let coordinator_service = service.clone();
    let supervisor = service.clone();
    let mut worker = tokio::spawn(async move { owner.run().await });
    let mut coordinator =
        tokio::spawn(async move { coordinator_service.coordinate(private_consumer).await });
    let mut market_owner = market_owner;
    let mut private_task = private_task;
    let mut private_result = None;
    let mut monitor = tokio::time::interval(std::time::Duration::from_millis(250));
    monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let completion = loop {
        tokio::select! {
            result = &mut worker => break RuntimeCompletion::Worker(result),
            result = &mut coordinator => break RuntimeCompletion::Coordinator(result),
            result = async {
                match private_task.as_mut() {
                    Some(task) => task.await,
                    None => std::future::pending().await,
                }
            }, if private_task.is_some() => {
                if supervisor.inner.stop.is_cancelled() {
                    private_result = Some(Ok(()));
                } else {
                    let error = private_task_error(&result);
                    let readiness = supervisor
                        .inner
                        .automation_live_venue
                        .private_readiness_fence();
                    let health = readiness.snapshot();
                    if health.state != crate::upbit::realtime::PrivateRealtimeState::Error {
                        readiness.mark_owner_failure(
                            health.generation,
                            crate::upbit::realtime::PrivateRealtimeFailure::InternalState,
                        );
                    }
                    tracing::error!(
                        event = "private_realtime_owner_failed",
                        error = %error,
                        disposition = "PRIVATE_LANE_CLOSED_PUBLIC_AND_MOCK_CONTINUE"
                    );
                    private_result = Some(Err(error));
                    publish_status_if_changed(&supervisor);
                }
                private_task = None;
            }
            _ = monitor.tick() => {
                if market_owner.as_ref().is_some_and(PublicMarketFeedRuntime::is_finished) {
                    let result = match market_owner.take() {
                        Some(owner) => owner.join().await,
                        None => Ok(()),
                    };
                    break RuntimeCompletion::Market(result);
                }
            }
        }
    };

    supervisor
        .inner
        .admitting
        .store(false, std::sync::atomic::Ordering::Release);
    supervisor.inner.stop.cancel();
    if let Some(owner) = market_owner.as_ref() {
        owner.cancel();
    }
    let (active, control_poisoned) = match supervisor.inner.active.lock() {
        Ok(mut active) => (active.take(), false),
        Err(poisoned) => (poisoned.into_inner().take(), true),
    };
    if let Some((id, token)) = active {
        let cancellation = supervisor
            .persist_cancel(id, super::CancellationAuthority::RuntimeOwner)
            .await;
        token.cancel();
        if cancellation.is_err() {
            tracing::error!(
                event = "runtime_cancel_unconfirmed",
                recovery = "DURABLE_READBACK_REQUIRED"
            );
        }
    }
    if let Ok(mut guard) = supervisor.inner.active.lock() {
        *guard = None;
    }

    let mut worker_result = None;
    let mut coordinator_result = None;
    let mut market_result = None;
    match completion {
        RuntimeCompletion::Worker(result) => {
            worker_result = Some(
                result
                    .map_err(|_| LabError::Internal("research runtime worker panicked".into()))
                    .and_then(std::convert::identity),
            );
        }
        RuntimeCompletion::Coordinator(result) => {
            coordinator_result = Some(
                result
                    .map_err(|_| LabError::Internal("research runtime coordinator panicked".into()))
                    .and_then(std::convert::identity),
            );
        }
        RuntimeCompletion::Market(result) => market_result = Some(result),
    }
    if worker_result.is_none() {
        worker_result = Some(
            worker
                .await
                .map_err(|_| LabError::Internal("research runtime worker panicked".into()))
                .and_then(std::convert::identity),
        );
    }
    if coordinator_result.is_none() {
        coordinator_result = Some(
            coordinator
                .await
                .map_err(|_| LabError::Internal("research runtime coordinator panicked".into()))
                .and_then(std::convert::identity),
        );
    }
    if private_result.is_none() {
        private_result = Some(if let Some(task) = private_task.take() {
            match task.await {
                Ok(Ok(())) => Ok(()),
                result => Err(private_task_error(&result)),
            }
        } else {
            Ok(())
        });
    }
    if market_result.is_none() {
        market_result = Some(if let Some(owner) = market_owner.take() {
            owner.shutdown().await
        } else {
            Ok(())
        });
    }
    realtime_status_after_join(&supervisor);

    supervisor
        .inner
        .runner_available
        .store(false, std::sync::atomic::Ordering::Release);
    {
        let _admitted = supervisor.inner.admission_gate.lock().await;
    }
    let worker_result = worker_result.unwrap_or(Ok(()));
    let coordinator_result = coordinator_result.unwrap_or(Ok(()));
    let private_result = private_result.unwrap_or(Ok(()));
    let market_result = market_result.unwrap_or(Ok(()));
    for (role, result) in [
        ("worker", &worker_result),
        ("coordinator", &coordinator_result),
        ("private_realtime", &private_result),
        ("public_realtime", &market_result),
    ] {
        if let Err(error) = result {
            tracing::error!(event = "research_runtime_child_failed", role, error = %error);
        }
    }
    let result = worker_result.and(coordinator_result).and(market_result);
    let result = if control_poisoned {
        Err(LabError::Internal(
            "job control lock poisoned; children joined".into(),
        ))
    } else {
        result
    };
    if result.is_err() {
        let recovered = supervisor
            .inner
            .database
            .call("recover_failed_runner", |store| {
                store.recover_interrupted(UtcTimestamp::now())
            })
            .await;
        if recovered.is_err() {
            tracing::error!(event = "runner_interruption_unconfirmed");
        }
    }
    result
}

fn realtime_status_after_join(service: &JobService) {
    publish_status_if_changed(service);
}

fn private_status(client: &UpbitPrivateClient) -> RealtimeFeedStatus {
    let health = client.private_readiness_fence().snapshot();
    let state = match health.state {
        crate::upbit::realtime::PrivateRealtimeState::Idle => RealtimeConnectionState::Idle,
        crate::upbit::realtime::PrivateRealtimeState::Connecting => {
            RealtimeConnectionState::Connecting
        }
        crate::upbit::realtime::PrivateRealtimeState::Recovering => {
            RealtimeConnectionState::Recovering
        }
        crate::upbit::realtime::PrivateRealtimeState::Ready => RealtimeConnectionState::Ready,
        crate::upbit::realtime::PrivateRealtimeState::Stopped => RealtimeConnectionState::Stopped,
        crate::upbit::realtime::PrivateRealtimeState::Error => RealtimeConnectionState::Error,
    };
    RealtimeFeedStatus {
        state,
        connection_count: health.connection_count,
        subscription_count: health.subscription_count,
        last_received_at_utc: health.last_received_at_utc,
        last_reconciled_at_utc: health.last_reconciled_at_utc,
        reconnect_count: health.reconnect_count,
        dropped_messages: health.dropped_messages,
        ready_for_decisions: health.ready_for_decisions,
        readiness_blockers: Vec::new(),
        subscription_readiness: Vec::new(),
    }
}

fn same_material_health(left: &RealtimeStatus, right: &RealtimeStatus) -> bool {
    same_feed_health(&left.market, &right.market) && same_feed_health(&left.private, &right.private)
}

fn same_feed_health(left: &RealtimeFeedStatus, right: &RealtimeFeedStatus) -> bool {
    left.state == right.state
        && left.connection_count == right.connection_count
        && left.subscription_count == right.subscription_count
        && left.last_reconciled_at_utc == right.last_reconciled_at_utc
        && left.reconnect_count == right.reconnect_count
        && left.dropped_messages == right.dropped_messages
        && left.ready_for_decisions == right.ready_for_decisions
        && left.readiness_blockers == right.readiness_blockers
        && left.subscription_readiness.len() == right.subscription_readiness.len()
        && left
            .subscription_readiness
            .iter()
            .zip(&right.subscription_readiness)
            .all(|(left, right)| {
                left.market == right.market
                    && left.interval == right.interval
                    && left.ready == right.ready
                    && left.blockers == right.blockers
                    && left.pending_rest_confirmation == right.pending_rest_confirmation
                    && left.revision_snapshot_count == right.revision_snapshot_count
                    && left.last_revision_open_time_utc == right.last_revision_open_time_utc
                    && left.confirmed_provider_absence_spans
                        == right.confirmed_provider_absence_spans
                    && left.provider_recovery_progress == right.provider_recovery_progress
            })
}
