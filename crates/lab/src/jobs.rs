//! Durable SQLite queue, one owned runner, and explicit cancellation acknowledgements.

mod coordinator;
mod research_control;

use crate::contracts::{
    AccountMark, ArtifactId, AttemptState, ContentHash, DatasetStatus, ENGINE_VERSION,
    FailureRecord, FillRecord, INDICATOR_VERSION, JobAttempt, JobControl, JobId, JobOutput,
    JobPayload, JobProgress, JobRecord, JobStatus, JobSubmission, LabError, MAX_RUN_EVENTS,
    MarketDataOrigin, ModelLedger, ModelStatus, OrderRecord, ProgressCountUnit, ROUNDING_VERSION,
    RunBundle, RunHeader, RunId, RunManifest, RunRequest, SCHEMA_VERSION, SignalRecord,
    UtcTimestamp, ValidationStatus,
};
use crate::database::DatabaseHandle;
use crate::market_data::UpbitClient;
use crate::storage::{AttemptPublication, ModelFactBatch, RootReservation};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

#[derive(Clone)]
pub struct JobService {
    inner: Arc<Inner>,
}
#[derive(Clone, Copy)]
enum CancellationAuthority {
    Client,
    RuntimeOwner,
}

struct Inner {
    database: DatabaseHandle,
    upbit: UpbitClient,
    root: PathBuf,
    revision: Option<String>,
    wake: Notify,
    stop: CancellationToken,
    admitting: AtomicBool,
    admission_gate: Arc<tokio::sync::Mutex<()>>,
    runner_available: AtomicBool,
    active: Mutex<Option<(JobId, CancellationToken)>>,
}

/// Point-in-time runtime ownership and persisted queue counts.
#[derive(Debug, serde::Serialize)]
pub struct JobServiceStatus {
    pub admission_open: bool,
    pub runner_available: bool,
    pub active_job_id: Option<JobId>,
    pub queued_attempts: u32,
    pub running_attempts: u32,
    pub compute_inflight_limit: u32,
    pub durable_queue_limit: u32,
}

struct PreparedOutcome {
    output: JobOutput,
    status: JobStatus,
    publication: AttemptPublication,
    reservation: Option<RootReservation>,
}

pub struct JobRuntime {
    service: JobService,
    runner: JoinHandle<Result<(), LabError>>,
}

impl JobRuntime {
    /// Recover interrupted attempts, then start the sole process-owned job runner.
    /// # Errors
    /// Reports persisted-state or storage-owner failures.
    pub async fn start(
        database: DatabaseHandle,
        upbit: UpbitClient,
        root: PathBuf,
        revision: Option<String>,
    ) -> Result<Self, LabError> {
        database
            .call("recover_interrupted", |store| {
                store.recover_interrupted(UtcTimestamp::now())
            })
            .await?;
        let service = JobService {
            inner: Arc::new(Inner {
                database,
                upbit,
                root,
                revision,
                wake: Notify::new(),
                stop: CancellationToken::new(),
                admitting: AtomicBool::new(true),
                admission_gate: Arc::new(tokio::sync::Mutex::new(())),
                runner_available: AtomicBool::new(true),
                active: Mutex::new(None),
            }),
        };
        let owner = service.clone();
        let producer = service.clone();
        let supervisor = service.clone();
        let runner = tokio::spawn(async move {
            let mut worker = tokio::spawn(async move { owner.run().await });
            let mut coordinator = tokio::spawn(async move { producer.coordinate().await });
            let (first, worker_finished) = tokio::select! {
                result = &mut worker => (result, true),
                result = &mut coordinator => (result, false),
            };
            supervisor.inner.admitting.store(false, Ordering::Release);
            supervisor.inner.stop.cancel();
            let (active, control_poisoned) = match supervisor.inner.active.lock() {
                Ok(active) => (active.clone(), false),
                Err(poisoned) => (poisoned.into_inner().clone(), true),
            };
            if let Some((id, token)) = active {
                let cancellation = supervisor
                    .persist_cancel(id, CancellationAuthority::RuntimeOwner)
                    .await;
                token.cancel();
                if cancellation.is_err() {
                    tracing::error!(
                        event = "runtime_cancel_unconfirmed",
                        recovery = "DURABLE_READBACK_REQUIRED"
                    );
                }
            }
            // A completed or failed child never detaches its sibling, including blocking work.
            let second = if worker_finished {
                coordinator.await
            } else {
                worker.await
            };
            supervisor
                .inner
                .runner_available
                .store(false, Ordering::Release);
            {
                let _admitted = supervisor.inner.admission_gate.lock().await;
            }
            let first = first
                .map_err(|_| LabError::Internal("research runtime child panicked".into()))
                .and_then(std::convert::identity);
            let second = second
                .map_err(|_| LabError::Internal("research runtime sibling panicked".into()))
                .and_then(std::convert::identity);
            if let Err(error) = &second {
                tracing::error!(event = "research_runtime_sibling_failed", error = %error);
            }
            let result = first.and(second);
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
        });
        Ok(Self { service, runner })
    }
    #[must_use]
    pub fn service(&self) -> JobService {
        self.service.clone()
    }
    /// Stop submissions, cancel active work cooperatively, and join the runner.
    /// Queued work remains durably queued for the next explicit server start.
    /// # Errors
    /// Reports worker failures/panics; this never treats an abort as cancellation proof.
    pub async fn shutdown(self) -> Result<(), LabError> {
        self.service.inner.admitting.store(false, Ordering::Release);
        {
            let _admitted = self.service.inner.admission_gate.lock().await;
        }
        self.service.inner.stop.cancel();
        let active = self
            .service
            .inner
            .active
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| LabError::Internal("job control lock poisoned".into()));
        let cancellation = match active {
            Ok(Some((job_id, token))) => {
                let persisted = self
                    .service
                    .persist_cancel(job_id, CancellationAuthority::RuntimeOwner)
                    .await;
                if persisted.is_ok() {
                    token.cancel();
                }
                persisted.map(|_| ())
            }
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        let mut runner = self.runner;
        // A started blocking computation cannot safely be aborted. On deadline,
        // report failed graceful shutdown, retain ownership, and finish joining.
        let (joined, timed_out) = if let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut runner).await
        {
            (result, false)
        } else {
            tracing::error!(
                event = "job_shutdown_timeout",
                outcome = "JOINING_BEFORE_FAILURE"
            );
            (runner.await, true)
        };
        joined.map_err(|_| LabError::Internal("job runner panicked".into()))??;
        cancellation?;
        if timed_out {
            return Err(LabError::ResourceLimit(
                "job shutdown exceeded 30 seconds; owner joined".into(),
            ));
        }
        Ok(())
    }
}

impl JobService {
    /// Read durable queue counts and current process ownership without returning job payloads.
    /// # Errors
    /// Reports storage or ownership-lock failure.
    pub async fn status(&self) -> Result<JobServiceStatus, LabError> {
        let queue = self
            .inner
            .database
            .call("job_queue_status", |store| store.job_queue_status())
            .await?;
        let active_job_id = self
            .inner
            .active
            .lock()
            .map_err(|_| LabError::Internal("job control lock poisoned".into()))?
            .as_ref()
            .map(|(id, _)| id.clone());
        Ok(JobServiceStatus {
            admission_open: self.inner.admitting.load(Ordering::Acquire),
            runner_available: self.inner.runner_available.load(Ordering::Acquire),
            active_job_id,
            queued_attempts: queue.queued_attempts,
            running_attempts: queue.running_attempts,
            compute_inflight_limit: 1,
            durable_queue_limit: 8,
        })
    }

    /// Enqueue a bounded idempotent request without awaiting computation.
    /// # Errors
    /// Rejects invalid/conflicting inputs, full queues or shutdown admission.
    pub async fn submit(&self, submission: JobSubmission) -> Result<JobRecord, LabError> {
        if !self.inner.admitting.load(Ordering::Acquire) {
            return Err(LabError::ResourceLimit("job submission stopped".into()));
        }
        match &submission.payload {
            JobPayload::Collect { request } => {
                request.validate(UtcTimestamp::now())?;
                if request.request_id != submission.request_id {
                    return Err(LabError::InvalidConfig(
                        "collection request identity mismatch".into(),
                    ));
                }
            }
            JobPayload::Backtest { request } => {
                if request.request_id != submission.request_id {
                    return Err(LabError::InvalidConfig(
                        "backtest request identity mismatch".into(),
                    ));
                }
                let id = request.plan_id.clone();
                let plan = self
                    .inner
                    .database
                    .call("admit_backtest", move |store| store.load_plan(&id))
                    .await?
                    .ok_or_else(|| LabError::InvalidConfig("unknown frozen plan".into()))?;
                if plan.resolved.input_digest != request.input_digest {
                    return Err(LabError::InputHashMismatch(
                        "submitted plan digest mismatch".into(),
                    ));
                }
            }
            JobPayload::Export { .. } | JobPayload::Verify { .. } => {}
        }
        let job = self
            .admitted("submit_job", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;
        self.inner.wake.notify_one();
        tracing::info!(event = "job_submitted", job_id = %job.id, attempts = job.attempts.len());
        Ok(job)
    }

    /// Query or explicitly cancel/retry one durable job.
    /// # Errors
    /// Reports unknown IDs, conflicts or storage failures.
    pub async fn control(&self, control: JobControl) -> Result<JobRecord, LabError> {
        match control {
            JobControl::Get { job_id } => self.get(job_id).await,
            JobControl::Cancel { job_id } => {
                let job = self
                    .persist_cancel(job_id.clone(), CancellationAuthority::Client)
                    .await?;
                let active = self
                    .inner
                    .active
                    .lock()
                    .map_err(|_| LabError::Internal("job control lock poisoned".into()))?;
                if let Some((active_id, token)) = active.as_ref().filter(|(id, _)| id == &job_id) {
                    tracing::info!(event = "job_cancel_requested", job_id = %active_id);
                    token.cancel();
                }
                Ok(job)
            }
            JobControl::Retry { job_id } => {
                if !self.inner.admitting.load(Ordering::Acquire) {
                    return Err(LabError::ResourceLimit("job retry stopped".into()));
                }
                let job = self
                    .admitted("retry_job", move |store| {
                        store.retry_job(&job_id, UtcTimestamp::now())
                    })
                    .await?;
                self.inner.wake.notify_one();
                Ok(job)
            }
        }
    }

    async fn persist_cancel(
        &self,
        job_id: JobId,
        authority: CancellationAuthority,
    ) -> Result<JobRecord, LabError> {
        let id = job_id.clone();
        match self
            .inner
            .database
            .call("cancel_job", move |store| match authority {
                CancellationAuthority::Client => store.request_cancel(&id, UtcTimestamp::now()),
                CancellationAuthority::RuntimeOwner => {
                    store.request_cancel_owned(&id, UtcTimestamp::now())
                }
            })
            .await
        {
            Err(error @ LabError::OutcomeUnknown(_)) => {
                let job = self.get(job_id).await?;
                let confirmed = job.attempts.last().is_some_and(|attempt| {
                    attempt.state.is_terminal()
                        || matches!(
                            attempt.state,
                            AttemptState::Running {
                                cancel_requested_at: Some(_),
                                ..
                            }
                        )
                });
                if confirmed { Ok(job) } else { Err(error) }
            }
            result => result,
        }
    }

    async fn get(&self, id: JobId) -> Result<JobRecord, LabError> {
        self.inner
            .database
            .call("get_job", move |store| store.get_job(&id))
            .await?
            .ok_or_else(|| LabError::InvalidConfig("unknown job ID".into()))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one owned claim, execute, cancellation reconciliation and terminal publication loop"
    )]
    async fn run(self) -> Result<(), LabError> {
        let mut poll = tokio::time::interval(std::time::Duration::from_secs(1));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.inner.stop.is_cancelled() {
                break;
            }
            let attempt = self
                .inner
                .database
                .call("claim_next", |store| store.claim_next(UtcTimestamp::now()))
                .await?;
            let Some(attempt) = attempt else {
                tokio::select! { () = self.inner.stop.cancelled() => break, () = self.inner.wake.notified() => {}, _tick = poll.tick() => {} }
                continue;
            };
            let cancellation = CancellationToken::new();
            *self
                .inner
                .active
                .lock()
                .map_err(|_| LabError::Internal("job control lock poisoned".into()))? =
                Some((attempt.job_id.clone(), cancellation.clone()));
            // Active token is installed BEFORE re-reading persisted cancellation,
            // closing the claim/cancel race without losing a durable request.
            let job = self.get(attempt.job_id.clone()).await?;
            if self.inner.stop.is_cancelled() {
                let id = job.id.clone();
                self.inner
                    .database
                    .call("cancel_claim_during_shutdown", move |store| {
                        store.request_cancel_owned(&id, UtcTimestamp::now())
                    })
                    .await?;
                cancellation.cancel();
            } else if job.attempts.last().is_some_and(|attempt| {
                matches!(
                    attempt.state,
                    AttemptState::Running {
                        cancel_requested_at: Some(_),
                        ..
                    }
                )
            }) {
                cancellation.cancel();
            }
            let span =
                tracing::info_span!("job_attempt", job_id = %job.id, attempt_id = %attempt.id);
            let result = self
                .execute(&job, &attempt, &cancellation)
                .instrument(span)
                .await;
            let ended_at = UtcTimestamp::now();
            let (terminal, publication, _reservation) = match result {
                Ok(prepared) => (
                    terminal_state(prepared.output, prepared.status, ended_at),
                    prepared.publication,
                    prepared.reservation,
                ),
                Err(LabError::Cancelled(message)) => (
                    AttemptState::Cancelled {
                        ended_at,
                        reason: message,
                    },
                    AttemptPublication::None,
                    None,
                ),
                Err(error) => (
                    AttemptState::Failed {
                        ended_at,
                        error: failure_record(&error),
                    },
                    AttemptPublication::None,
                    None,
                ),
            };
            let sealed_run_id = match &publication {
                AttemptPublication::Run { run_id, .. } => Some(run_id.clone()),
                _ => None,
            };
            let expected_terminal = ContentHash::of_value(&terminal)?;
            let id = attempt.id.clone();
            let finished = self
                .inner
                .database
                .call("finish_attempt", move |store| {
                    store.publish_attempt(&id, terminal, publication)
                })
                .await;
            if let Err(error) = finished {
                let refreshed = self.get(job.id.clone()).await?;
                let published = refreshed
                    .attempts
                    .last()
                    .filter(|record| record.id == attempt.id && record.state.is_terminal())
                    .map(|record| ContentHash::of_value(&record.state))
                    .transpose()?;
                if published.as_ref() == Some(&expected_terminal) {
                    tracing::warn!(event = "terminal_publication_reconciled", attempt_id = %attempt.id);
                } else if refreshed.attempts.last().is_some_and(|a| {
                    matches!(
                        a.state,
                        AttemptState::Running {
                            cancel_requested_at: Some(_),
                            ..
                        }
                    )
                }) {
                    let id = attempt.id.clone();
                    self.inner
                        .database
                        .call("acknowledge_cancel", move |store| {
                            store.finish_attempt(
                                &id,
                                AttemptState::Cancelled {
                                    ended_at: UtcTimestamp::now(),
                                    reason: "cancellation won terminal publication".into(),
                                },
                            )
                        })
                        .await?;
                } else {
                    return Err(error);
                }
            }
            // Seal the run's detailed ledger into content-addressed chunks and
            // compact the SQLite rows. A failure here never invalidates the
            // completed run: it stays DETAIL (rows intact) and the next
            // deployment's runner pass can seal it later.
            if let Some(run_id) = sealed_run_id {
                let sealed = self
                    .inner
                    .database
                    .call("seal_and_compact_run_ledger", move |store| {
                        store.seal_and_compact_run_ledger(&run_id)
                    })
                    .await;
                match sealed {
                    Ok(stats) => tracing::info!(
                        event = "run_ledger_sealed",
                        chunks = stats.chunks,
                        events = stats.events,
                        compressed_bytes = stats.compressed_bytes,
                        uncompressed_bytes = stats.uncompressed_bytes,
                    ),
                    Err(error) => tracing::warn!(
                        event = "run_ledger_seal_failed",
                        error = %error,
                        outcome = "run_kept_in_detail_state"
                    ),
                }
            }
            *self
                .inner
                .active
                .lock()
                .map_err(|_| LabError::Internal("job control lock poisoned".into()))? = None;
            tracing::info!(event = "job_attempt_finished", job_id = %job.id, attempt_id = %attempt.id);
        }
        Ok(())
    }

    async fn execute(
        &self,
        job: &JobRecord,
        attempt: &JobAttempt,
        cancellation: &CancellationToken,
    ) -> Result<PreparedOutcome, LabError> {
        check_cancel(cancellation)?;
        match &job.payload {
            JobPayload::Collect { request } => {
                let dataset = crate::collection::prepare_collection(
                    &self.inner.upbit,
                    &self.inner.database,
                    request.clone(),
                    cancellation,
                )
                .await?;
                let status = if dataset.manifest.status == DatasetStatus::Ready {
                    JobStatus::Completed
                } else {
                    JobStatus::Blocked
                };
                Ok(PreparedOutcome {
                    output: JobOutput::Dataset {
                        dataset_id: dataset.manifest.id.clone(),
                    },
                    status,
                    publication: AttemptPublication::Dataset(Box::new(dataset)),
                    reservation: None,
                })
            }
            JobPayload::Backtest { request } => {
                self.backtest(job, attempt, request, cancellation).await
            }
            JobPayload::Export { run_id, market } => {
                self.export(
                    run_id,
                    market.as_ref().map(|market| market.base.clone()),
                    cancellation,
                )
                .await
            }
            JobPayload::Verify {
                artifact_id,
                replay,
            } => self.verify(artifact_id, *replay, cancellation).await,
        }
    }

    async fn export(
        &self,
        run_id: &RunId,
        asset: Option<crate::contracts::Asset>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedOutcome, LabError> {
        let id = run_id.clone();
        let bundle = self
            .inner
            .database
            .call("export_input", move |store| store.load_run_bundle(&id))
            .await?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        let reservation = self
            .inner
            .database
            .call("reserve_export_capacity", |store| {
                store.reserve_root_bytes(crate::storage::MAX_EXPORT_RESERVATION_BYTES)
            })
            .await?;
        let root = self.inner.root.join("exports");
        let result = tokio::task::spawn_blocking(move || {
            crate::reporting::export_run(&bundle, &root, asset)
        })
        .await
        .map_err(|_| LabError::Internal("export worker panicked".into()))??;
        check_cancel(cancellation)?;
        let artifacts = result.into_catalog_artifacts()?;
        let artifact_ids = artifacts
            .iter()
            .map(|artifact| artifact.id.clone())
            .collect();
        Ok(PreparedOutcome {
            output: JobOutput::Artifacts { artifact_ids },
            status: JobStatus::Completed,
            publication: AttemptPublication::Artifacts(artifacts),
            reservation: Some(reservation),
        })
    }

    async fn verify(
        &self,
        artifact_id: &ArtifactId,
        replay: bool,
        cancellation: &CancellationToken,
    ) -> Result<PreparedOutcome, LabError> {
        let id = artifact_id.clone();
        let path = self
            .inner
            .database
            .call("resolve_artifact", move |store| {
                store.resolve_artifact_path(&id)
            })
            .await?;
        let directory = path
            .parent()
            .ok_or_else(|| LabError::DataCorrupt("artifact has no export parent".into()))?
            .to_path_buf();
        let replay_cancellation = cancellation.clone();
        let report = tokio::task::spawn_blocking(move || {
            let package = crate::reporting::export::read_export_package(&directory)?;
            if replay {
                crate::replay::replay_package_with_cancel(&package, &|| {
                    replay_cancellation.is_cancelled()
                })
            } else {
                Ok(package.validation)
            }
        })
        .await
        .map_err(|_| LabError::Internal("verification worker panicked".into()))??;
        check_cancel(cancellation)?;
        if report.status != ValidationStatus::Pass {
            return Err(LabError::AccountingInvariant(report.findings.join("; ")));
        }
        Ok(PreparedOutcome {
            output: JobOutput::Validation {
                report: report.clone(),
            },
            status: JobStatus::Completed,
            publication: AttemptPublication::Validation(report),
            reservation: None,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "ordered model computation and bounded durable publication share one cancellation owner"
    )]
    async fn backtest(
        &self,
        job: &JobRecord,
        attempt: &JobAttempt,
        request: &RunRequest,
        cancellation: &CancellationToken,
    ) -> Result<PreparedOutcome, LabError> {
        let id = request.plan_id.clone();
        let stored = self
            .inner
            .database
            .call("run_plan", move |store| store.load_plan(&id))
            .await?
            .ok_or_else(|| LabError::InvalidConfig("unknown plan".into()))?;
        if stored.resolved.input_digest != request.input_digest {
            return Err(LabError::InputHashMismatch("frozen plan mismatch".into()));
        }
        let (datasets, evidence) =
            crate::planning::load_inputs(&self.inner.database, &stored.resolved.spec).await?;
        let run_id = RunId::from_seed(attempt.id.as_str());
        let manifest = manifest(
            job,
            attempt,
            run_id.clone(),
            datasets[0].manifest.origin,
            self.inner.revision.clone(),
        )?;
        let header = RunHeader {
            manifest: manifest.clone(),
            plan_id: stored.resolved.id.clone(),
            state: JobStatus::Running,
            last_committed_event_seq: 0,
            semantic_digest: None,
        };
        self.inner
            .database
            .call("begin_run", move |store| store.begin_run(&header))
            .await?;
        let plan = Arc::new(stored.resolved);
        let datasets = Arc::new(datasets);
        let evidence = Arc::new(evidence);
        let mut models = Vec::new();
        let mut next_seq = 1_u64;
        let mut total_records = 0usize;
        for admission in &plan.admissions {
            check_cancel(cancellation)?;
            let worker_plan = plan.clone();
            let worker_data = datasets.clone();
            let worker_evidence = evidence.clone();
            let worker_run = run_id.clone();
            let worker_admission = admission.clone();
            let token = cancellation.clone();
            let (model, batches) = tokio::task::spawn_blocking(move || {
                let model = crate::engine::run_model(
                    &worker_plan,
                    &worker_data,
                    worker_evidence.as_ref().as_ref(),
                    &worker_run,
                    &worker_admission,
                    next_seq,
                    &|| token.is_cancelled(),
                )?;
                let batches = fact_batches(&model)?;
                Ok::<_, LabError>((model, batches))
            })
            .await
            .map_err(|_| LabError::Internal("model worker panicked".into()))??;
            total_records = total_records
                .checked_add(
                    model.signals.len()
                        + model.orders.len()
                        + model.order_events.len()
                        + model.fills.len()
                        + model.account_marks.len()
                        + model.episodes.len(),
                )
                .ok_or_else(|| LabError::ResourceLimit("run records overflow".into()))?;
            if total_records > MAX_RUN_EVENTS {
                return Err(LabError::ResourceLimit("run record budget exceeded".into()));
            }
            for batch in batches {
                check_cancel(cancellation)?;
                let run = run_id.clone();
                let model_id = model.model_id.clone();
                let attempt_id = attempt.id.clone();
                self.inner
                    .database
                    .call("append_facts", move |store| {
                        let seq = store.append_model_facts(&run, &model_id, &batch)?;
                        store.update_progress(
                            &attempt_id,
                            &JobProgress {
                                stage: "facts_committed".into(),
                                committed_records: Some(seq),
                                count_unit: Some(ProgressCountUnit::LedgerFacts),
                                last_committed_event_seq: Some(seq),
                            },
                        )?;
                        Ok(seq)
                    })
                    .await?;
            }
            let run = run_id.clone();
            let model = self
                .inner
                .database
                .call("finish_model", move |store| {
                    store.finish_model(&run, &model)?;
                    Ok(model)
                })
                .await?;
            next_seq = model
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| LabError::ResourceLimit("run sequence overflow".into()))?;
            models.push(model);
        }
        check_cancel(cancellation)?;
        let (digest, report, completed, blocked, count, comparisons) =
            tokio::task::spawn_blocking(move || {
                let mut bundle = RunBundle {
                    manifest,
                    plan: Arc::try_unwrap(plan).map_err(|_| {
                        LabError::Internal("plan worker reference remains after join".into())
                    })?,
                    datasets: Arc::try_unwrap(datasets).map_err(|_| {
                        LabError::Internal("dataset worker reference remains after join".into())
                    })?,
                    evidence: Arc::try_unwrap(evidence).map_err(|_| {
                        LabError::Internal("Evidence worker reference remains after join".into())
                    })?,
                    models,
                    semantic_digest: ContentHash::of_bytes(b"not-published"),
                };
                bundle.semantic_digest = crate::reporting::semantic_digest(&bundle)?;
                let report = crate::reporting::verify_run(&bundle);
                if report.status != ValidationStatus::Pass {
                    return Err(LabError::AccountingInvariant(report.findings.join("; ")));
                }
                let completed = bundle
                    .models
                    .iter()
                    .filter(|m| m.status == ModelStatus::Completed)
                    .count();
                let blocked = bundle.models.len() - completed;
                let count = u64::try_from(bundle.models.len())
                    .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?;
                let causal_digest = crate::research::causal_input_digest(
                    &bundle.plan,
                    &bundle.datasets,
                    bundle.evidence.as_ref(),
                )?;
                let comparisons = crate::reporting::build_comparisons(&bundle, &causal_digest)?;
                Ok((
                    bundle.semantic_digest,
                    report,
                    completed,
                    blocked,
                    count,
                    comparisons,
                ))
            })
            .await
            .map_err(|_| LabError::Internal("verification worker panicked".into()))??;
        check_cancel(cancellation)?;
        let status = if completed == 0 {
            JobStatus::Blocked
        } else if blocked == 0 {
            JobStatus::Completed
        } else {
            JobStatus::Partial
        };
        let output = JobOutput::Run {
            run_id: run_id.clone(),
            completed_models: u64::try_from(completed)
                .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?,
            blocked_models: u64::try_from(blocked)
                .map_err(|_| LabError::ResourceLimit("model count overflow".into()))?,
        };
        Ok(PreparedOutcome {
            output,
            status,
            publication: AttemptPublication::Run {
                run_id,
                semantic_digest: digest,
                expected_models: count,
                validation: report,
                comparisons,
            },
            reservation: None,
        })
    }
}

fn terminal_state(output: JobOutput, status: JobStatus, ended_at: UtcTimestamp) -> AttemptState {
    match status {
        JobStatus::Completed => AttemptState::Completed { ended_at, output },
        JobStatus::Partial => AttemptState::Partial {
            ended_at,
            output,
            warnings: vec![
                "some requested models are blocked or failed; inspect per-model states".into(),
            ],
        },
        JobStatus::Blocked => AttemptState::Blocked {
            ended_at,
            output: Some(output),
            reason: FailureRecord {
                code: "BLOCKED_INPUT".into(),
                message: "no admitted completed result".into(),
            },
        },
        _ => AttemptState::Failed {
            ended_at,
            error: FailureRecord {
                code: "INTERNAL".into(),
                message: "worker returned nonterminal outcome".into(),
            },
        },
    }
}

fn manifest(
    job: &JobRecord,
    attempt: &JobAttempt,
    run_id: RunId,
    origin: MarketDataOrigin,
    revision: Option<String>,
) -> Result<RunManifest, LabError> {
    Ok(RunManifest { schema_version: SCHEMA_VERSION.into(), run_id, job_id: job.id.clone(), attempt_id: attempt.id.clone(),
        created_at: UtcTimestamp::now(), code_revision: revision,
        source_digest: ContentHash::try_from(env!("SPOT_LAB_SOURCE_SHA256").to_owned())?,
        lockfile_digest: ContentHash::try_from(env!("SPOT_LAB_LOCK_SHA256").to_owned())?, toolchain: env!("SPOT_LAB_TOOLCHAIN").into(),
        engine_version: ENGINE_VERSION.into(), indicator_version: INDICATOR_VERSION.into(), rounding_version: ROUNDING_VERSION.into(),
        mode: "HISTORICAL_REPLAY".into(), historical_availability_model: "BAR_CLOSE_ASSUMED".into(), execution_origin: "SIMULATED_ONLY".into(),
        fill_observed: false, origin, numeric_tolerance: crate::contracts::NUMERIC_TOLERANCE.to_string(), metric_tolerance: 1e-10,
        start_policy: "START_FROM_CASH".into(), account_mode: "INDEPENDENT_ASSET_STRATEGY".into(),
        assumptions: vec!["fees, spread, slippage, impact and rules are explicit experiment inputs".into(),
            "DECISION_MARK_FIXED_BASE_QUANTITY / next-bar-open-v2-decision-mark-fixed-base-quantity; conservative rounding; no observed fills".into()] })
}

fn check_cancel(token: &CancellationToken) -> Result<(), LabError> {
    if token.is_cancelled() {
        Err(LabError::Cancelled(
            "job cancelled at a safe checkpoint".into(),
        ))
    } else {
        Ok(())
    }
}

fn failure_record(error: &LabError) -> FailureRecord {
    let code = match error {
        LabError::InvalidConfig(_) => "INVALID_CONFIG",
        LabError::InsufficientWarmup(_) => "INSUFFICIENT_WARMUP",
        LabError::DataGap(_) => "DATA_GAP",
        LabError::InputHashMismatch(_) => "INPUT_HASH_MISMATCH",
        LabError::BlockedEvidence(_) => "BLOCKED_EVIDENCE",
        LabError::NetworkUnavailable(_) => "NETWORK_UNAVAILABLE",
        LabError::RateLimited(_) => "RATE_LIMITED",
        LabError::TemporarilyBlocked(_) => "TEMPORARILY_BLOCKED",
        LabError::CapacityExceeded(_) => "CAPACITY_EXCEEDED",
        LabError::Conflict(_) => "CONFLICT",
        LabError::DataCorrupt(_) => "DATA_CORRUPT",
        LabError::ResourceLimit(_) | LabError::RequestLimit(_) => "RESOURCE_LIMIT",
        LabError::Cancelled(_) => "CANCELLED",
        LabError::UnverifiedMarketRules(_) => "UNVERIFIED_MARKET_RULES",
        LabError::AccountingInvariant(_) => "ACCOUNTING_INVARIANT_FAILURE",
        LabError::OutcomeUnknown(_) => "OUTCOME_UNKNOWN",
        LabError::ContractParse(_) => "CONTRACT_PARSE",
        LabError::Internal(_) => "INTERNAL",
    };
    // Durable local diagnostic; MCP projects the category and bounded message.
    FailureRecord {
        code: code.into(),
        message: error.to_string().chars().take(1024).collect(),
    }
}

fn fact_batches(model: &ModelLedger) -> Result<Vec<ModelFactBatch>, LabError> {
    enum Fact<'a> {
        Signal(&'a SignalRecord),
        Order(&'a OrderRecord),
        Fill(&'a FillRecord),
        Mark(&'a AccountMark),
    }
    let mut facts = Vec::new();
    facts.extend(
        model
            .signals
            .iter()
            .map(|r| (r.context.event_seq, Fact::Signal(r))),
    );
    facts.extend(
        model
            .order_events
            .iter()
            .map(|r| (r.context.event_seq, Fact::Order(r))),
    );
    facts.extend(
        model
            .fills
            .iter()
            .map(|r| (r.context.event_seq, Fact::Fill(r))),
    );
    facts.extend(
        model
            .account_marks
            .iter()
            .map(|r| (r.context.event_seq, Fact::Mark(r))),
    );
    facts.sort_by_key(|(seq, _)| *seq);
    let mut batches = Vec::new();
    let mut start = 0;
    while start < facts.len() {
        let mut end = (start + 256).min(facts.len());
        loop {
            let boundary = facts[end - 1].0;
            let crossing = facts[start..end].iter().position(
                |(_, fact)| matches!(fact, Fact::Fill(fill) if fill.accounting_mark_seq > boundary),
            );
            let Some(offset) = crossing else {
                break;
            };
            end = start + offset;
            if end == start {
                return Err(LabError::AccountingInvariant(
                    "fill/accounting pair exceeds bounded fact batch".into(),
                ));
            }
        }
        let mut batch = ModelFactBatch::default();
        for (_, fact) in &facts[start..end] {
            match fact {
                Fact::Signal(r) => batch.signals.push((*r).clone()),
                Fact::Order(r) => batch.order_events.push((*r).clone()),
                Fact::Fill(r) => batch.fills.push((*r).clone()),
                Fact::Mark(r) => batch.account_marks.push((*r).clone()),
            }
        }
        batches.push(batch);
        start = end;
    }
    Ok(batches)
}

#[cfg(test)]
mod tests;
