//! Domain adapters and admission for durable research producers.

use super::JobService;
use crate::contracts::{
    AutomationSessionCommandResponse, AutomationSessionQuery, AutomationSessionWrite,
    CollectionScheduleAction, JobId, LabError, PaperSessionCommandResponse, PaperSessionQuery,
    PaperSessionState, PaperSessionWrite, ProbeCount, ResearchSuiteAction, ResearchSuiteRequest,
    ScheduleId, ScheduleMutationOutcome, SourceProbeResult, StorageMaintenanceAction,
    StorageMaintenanceResult, SuiteId, UtcTimestamp, validate_research_page,
};
use crate::storage::Store;
use serde_json::Value;
use std::sync::atomic::Ordering;

impl JobService {
    /// The single admission gate protects producer writes and ordinary job submission.
    pub(super) async fn admitted<T, F>(
        &self,
        operation: &'static str,
        work: F,
    ) -> Result<T, LabError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, LabError> + Send + 'static,
    {
        let gate = self.inner.admission_gate.clone().lock_owned().await;
        let owner = self.inner.clone();
        self.inner
            .database
            .call(operation, move |store| {
                let _gate = gate;
                if !owner.admitting.load(Ordering::Acquire)
                    || !owner.runner_available.load(Ordering::Acquire)
                {
                    return Err(LabError::ResourceLimit(
                        "job runner admission closed".into(),
                    ));
                }
                work(store)
            })
            .await
    }

    async fn write_paper_session(
        &self,
        write: PaperSessionWrite,
        now: UtcTimestamp,
    ) -> Result<(PaperSessionCommandResponse, bool), LabError> {
        if let PaperSessionWrite::Resume {
            request_id,
            session_id,
            expected_sequence,
        } = &write
        {
            let readback_id = request_id.clone();
            let found_receipt = self
                .inner
                .database
                .call("paper_resume_receipt_preflight", move |store| {
                    Ok(store
                        .paper_readback_request(&readback_id, UtcTimestamp::now())?
                        .found)
                })
                .await?;
            if found_receipt {
                let response = self
                    .admitted("paper_session_write", move |store| {
                        store.paper_write(&write, UtcTimestamp::now())
                    })
                    .await?;
                return Ok((response, true));
            }

            let preflight_session = session_id.clone();
            let expected = *expected_sequence;
            let requirements = self
                .inner
                .database
                .call("paper_resume_feed_preflight", move |store| {
                    store.paper_recovery_poll_requirements(&preflight_session, expected)
                })
                .await?;
            if let Some(requirements) = requirements {
                let count = ProbeCount::try_from(crate::paper::MAX_PAPER_RECOVERY_BARS)?;
                let mut batches = Vec::with_capacity(requirements.len());
                for requirement in &requirements {
                    let batch = tokio::select! {
                        () = self.inner.stop.cancelled() => {
                            return Err(LabError::Cancelled(
                                "paper feed-gap recovery fetch cancelled".into(),
                            ));
                        }
                        result = self.inner.upbit.poll_paper_observations(
                            &requirement.market,
                            requirement.interval,
                            count,
                            &self.inner.database,
                        ) => result?,
                    };
                    batches.push(batch);
                }
                let response = self
                    .admitted("paper_session_write", move |store| {
                        store.paper_write_for_feed_gap_resume(
                            &write,
                            &requirements,
                            &batches,
                            UtcTimestamp::now(),
                        )
                    })
                    .await?;
                return Ok((response, true));
            }
        }

        let response = self
            .admitted("paper_session_write", move |store| {
                store.paper_write(&write, now)
            })
            .await?;
        Ok((response, false))
    }

    /// Create/control/query a durable suite without running a second computation worker.
    /// # Errors
    /// Rejects invalid inputs, closed admission and storage failures.
    pub async fn research_suite(&self, action: ResearchSuiteAction) -> Result<Value, LabError> {
        let value = match action {
            // Read-only admission preview; touches no durable state.
            ResearchSuiteAction::Plan { request } => {
                serde_json::to_value(crate::research::plan_suite(&request)?)?
            }
            ResearchSuiteAction::Create { request } => self.create_research_suite(*request).await?,
            ResearchSuiteAction::Get { suite_id } => {
                let record = self
                    .inner
                    .database
                    .call("get_research_suite", move |store| {
                        store.reconcile_research_suite(&suite_id, UtcTimestamp::now())
                    })
                    .await?;
                serde_json::to_value(crate::research::summarize(&record))?
            }
            ResearchSuiteAction::List { offset, limit } => {
                validate_research_page(limit)?;
                self.inner
                    .database
                    .call("list_research_suites", move |store| {
                        Ok(serde_json::to_value(
                            store.list_research_suites(offset, limit)?,
                        )?)
                    })
                    .await?
            }
            ResearchSuiteAction::Cases {
                suite_id,
                offset,
                limit,
            } => {
                validate_research_page(limit)?;
                self.inner
                    .database
                    .call("suite_cases", move |store| {
                        Ok(serde_json::to_value(
                            store.list_suite_cases(&suite_id, offset, limit)?,
                        )?)
                    })
                    .await?
            }
            ResearchSuiteAction::Comparisons {
                suite_id,
                offset,
                limit,
            } => {
                validate_research_page(limit)?;
                self.inner
                    .database
                    .call("suite_comparisons", move |store| {
                        Ok(serde_json::to_value(
                            store.list_suite_comparisons(&suite_id, offset, limit)?,
                        )?)
                    })
                    .await?
            }
            ResearchSuiteAction::Pause { suite_id } => self.pause_research_suite(suite_id).await?,
            ResearchSuiteAction::Resume { suite_id } => {
                self.resume_research_suite(suite_id).await?
            }
        };
        Ok(value)
    }

    async fn create_research_suite(
        &self,
        request: ResearchSuiteRequest,
    ) -> Result<Value, LabError> {
        let _geometry = crate::research::expand_geometry(&request)?;
        let _verified_inputs =
            crate::planning::load_inputs(&self.inner.database, &request.template).await?;
        let refs = request.template.policy_selections.clone();
        let policies = self
            .inner
            .database
            .call("suite_frozen_policies", move |store| {
                refs.iter()
                    .map(|reference| {
                        store
                            .load_policy_revision(reference)?
                            .map(|policy| policy.snapshot)
                            .ok_or_else(|| {
                                LabError::InvalidConfig("unknown suite policy revision".into())
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        let frozen = crate::research::freeze(request, policies)?;
        let cases = crate::research::initial_cases(&frozen)?;
        let record = self
            .admitted("create_research_suite", move |store| {
                store.create_research_suite(&frozen, &cases, UtcTimestamp::now())
            })
            .await?;
        Ok(serde_json::to_value(crate::research::summarize(&record))?)
    }

    async fn pause_research_suite(&self, suite_id: SuiteId) -> Result<Value, LabError> {
        let key = suite_id.clone();
        let paused = self
            .admitted("pause_research_suite", move |store| {
                store.pause_research_suite(&key, UtcTimestamp::now())
            })
            .await;
        let (record, jobs) = match paused {
            Ok(result) => result,
            Err(LabError::OutcomeUnknown(_)) => {
                self.admitted("pause_suite_readback", move |store| {
                    store.pause_research_suite(&suite_id, UtcTimestamp::now())
                })
                .await?
            }
            Err(error) => return Err(error),
        };
        self.signal_managed_cancellation(&jobs)?;
        Ok(serde_json::to_value(crate::research::summarize(&record))?)
    }

    async fn resume_research_suite(&self, suite_id: SuiteId) -> Result<Value, LabError> {
        let key = suite_id.clone();
        let resumed = self
            .admitted("resume_research_suite", move |store| {
                store.resume_research_suite(&key, UtcTimestamp::now())
            })
            .await;
        let record = match resumed {
            Ok(record) => record,
            Err(LabError::OutcomeUnknown(_)) => {
                self.admitted("resume_suite_readback", move |store| {
                    store.resume_research_suite(&suite_id, UtcTimestamp::now())
                })
                .await?
            }
            Err(error) => return Err(error),
        };
        Ok(serde_json::to_value(crate::research::summarize(&record))?)
    }

    /// Manage collection cadence; collection itself remains an ordinary durable job.
    /// # Errors
    /// Rejects invalid cadence, missing identities, closed admission or persistence failures.
    pub async fn collection_schedule(
        &self,
        action: CollectionScheduleAction,
    ) -> Result<Value, LabError> {
        let value = match action {
            CollectionScheduleAction::Create { request } => {
                request.validate()?;
                let record = crate::scheduling::create_schedule(*request, UtcTimestamp::now())?;
                self.admitted("create_collection_schedule", move |store| {
                    Ok(serde_json::to_value(
                        store.create_collection_schedule(&record)?,
                    )?)
                })
                .await?
            }
            CollectionScheduleAction::Get { schedule_id } => {
                self.inner
                    .database
                    .call("get_collection_schedule", move |store| {
                        Ok(serde_json::to_value(store.reconcile_collection_schedule(
                            &schedule_id,
                            UtcTimestamp::now(),
                        )?)?)
                    })
                    .await?
            }
            CollectionScheduleAction::List { offset, limit } => {
                validate_research_page(limit)?;
                self.inner
                    .database
                    .call("list_collection_schedules", move |store| {
                        Ok(serde_json::to_value(
                            store.list_collection_schedules(offset, limit)?,
                        )?)
                    })
                    .await?
            }
            CollectionScheduleAction::Pause { schedule_id } => {
                self.mutate_collection_schedule(schedule_id, ScheduleMutationTarget::Paused)
                    .await?
            }
            CollectionScheduleAction::Resume { schedule_id } => {
                self.mutate_collection_schedule(schedule_id, ScheduleMutationTarget::Active)
                    .await?
            }
            CollectionScheduleAction::Freshness {
                schedule_id,
                probe_source,
            } => {
                let probes = if probe_source {
                    self.probe_schedule_source(&schedule_id).await?
                } else {
                    std::collections::BTreeMap::new()
                };
                self.inner
                    .database
                    .call("collection_freshness", move |store| {
                        Ok(serde_json::to_value(store.collection_freshness(
                            &schedule_id,
                            UtcTimestamp::now(),
                            &probes,
                        )?)?)
                    })
                    .await?
            }
        };
        Ok(value)
    }

    /// Apply one schedule mutation with a distinguishable outcome receipt.
    ///
    /// The response keeps every record field and adds a bounded
    /// `mutation_outcome` projection, so existing readers stay compatible.
    /// # Errors
    /// Rejects unknown schedules, storage failures and unresolved conflicts.
    #[expect(
        clippy::too_many_lines,
        reason = "one mutation covers apply, lost-response read-back and conflict receipts"
    )]
    async fn mutate_collection_schedule(
        &self,
        schedule_id: ScheduleId,
        target: ScheduleMutationTarget,
    ) -> Result<Value, LabError> {
        let key = schedule_id.clone();
        let prior = self
            .inner
            .database
            .call("load_schedule_mutation_state", move |store| {
                store.reconcile_collection_schedule(&key, UtcTimestamp::now())
            })
            .await?;
        let already = matches!(
            (target, prior.status),
            (
                ScheduleMutationTarget::Paused,
                crate::contracts::CollectionScheduleStatus::Paused
            ) | (
                ScheduleMutationTarget::Active,
                crate::contracts::CollectionScheduleStatus::Active
            )
        );
        if already {
            return mutation_receipt(&prior, ScheduleMutationOutcome::NotApplied);
        }
        match target {
            ScheduleMutationTarget::Paused => {
                let key = schedule_id.clone();
                match self
                    .admitted("pause_collection_schedule", move |store| {
                        store.pause_collection_schedule(&key, UtcTimestamp::now())
                    })
                    .await
                {
                    Ok((record, jobs)) => {
                        self.signal_managed_cancellation(&jobs)?;
                        return mutation_receipt(&record, ScheduleMutationOutcome::Applied);
                    }
                    Err(LabError::OutcomeUnknown(_)) => {}
                    Err(conflict @ LabError::Conflict(_)) => {
                        let key = schedule_id.clone();
                        let record = self
                            .admitted("pause_schedule_conflict_readback", move |store| {
                                store.reconcile_collection_schedule(&key, UtcTimestamp::now())
                            })
                            .await?;
                        if record.status == crate::contracts::CollectionScheduleStatus::Paused {
                            return mutation_receipt(&record, ScheduleMutationOutcome::Conflicted);
                        }
                        return Err(conflict);
                    }
                    Err(error) => return Err(error),
                }
            }
            ScheduleMutationTarget::Active => {
                let key = schedule_id.clone();
                match self
                    .admitted("resume_collection_schedule", move |store| {
                        store.resume_collection_schedule(&key, UtcTimestamp::now())
                    })
                    .await
                {
                    Ok(record) => {
                        return mutation_receipt(&record, ScheduleMutationOutcome::Applied);
                    }
                    Err(LabError::OutcomeUnknown(_)) => {}
                    Err(conflict @ LabError::Conflict(_)) => {
                        let key = schedule_id.clone();
                        let record = self
                            .admitted("resume_schedule_conflict_readback", move |store| {
                                store.reconcile_collection_schedule(&key, UtcTimestamp::now())
                            })
                            .await?;
                        if record.status == crate::contracts::CollectionScheduleStatus::Active {
                            return mutation_receipt(&record, ScheduleMutationOutcome::Conflicted);
                        }
                        return Err(conflict);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        // The mutation outcome was lost: classify from a fresh read-back.
        let key = schedule_id.clone();
        let confirmed = self
            .inner
            .database
            .call("schedule_mutation_readback", move |store| {
                store.reconcile_collection_schedule(&key, UtcTimestamp::now())
            })
            .await?;
        let reached = matches!(
            (target, confirmed.status),
            (
                ScheduleMutationTarget::Paused,
                crate::contracts::CollectionScheduleStatus::Paused
            ) | (
                ScheduleMutationTarget::Active,
                crate::contracts::CollectionScheduleStatus::Active
            )
        );
        let outcome = if reached {
            ScheduleMutationOutcome::AppliedResponseLost
        } else {
            ScheduleMutationOutcome::ReconciliationRequired
        };
        mutation_receipt(&confirmed, outcome)
    }

    /// Probe the live source for every scheduled market to separate collector
    /// delay from source delay. Probe failures degrade to attempted probes
    /// with a note; freshness classification never fails because of them.
    /// # Errors
    /// Rejects unknown schedules or persistence failures.
    async fn probe_schedule_source(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<std::collections::BTreeMap<String, crate::contracts::SourceProbeResult>, LabError>
    {
        let key = schedule_id.clone();
        let record = self
            .inner
            .database
            .call("load_schedule_probe_state", move |store| {
                store.reconcile_collection_schedule(&key, UtcTimestamp::now())
            })
            .await?;
        let expected_end = crate::scheduling::latest_completed_boundary(
            UtcTimestamp::now(),
            record.request.interval,
        )?;
        let boundary_open = expected_end
            .0
            .checked_sub_signed(record.request.interval.duration())
            .ok_or_else(|| LabError::InvalidConfig("probe boundary overflow".into()))?;
        let mut probes = std::collections::BTreeMap::new();
        for market in &record.request.markets {
            let outcome =
                match self
                    .inner
                    .upbit
                    .fetch_completed_candles(
                        market,
                        record.request.interval,
                        ProbeCount::try_from(1)?,
                        None,
                    )
                    .await
                {
                    Ok(report) => SourceProbeResult {
                        attempted: true,
                        source_has_boundary: Some(report.candles.iter().any(|candle| {
                            candle.completed && candle.open_time_utc.0 == boundary_open
                        })),
                        note: format!(
                            "probe http_status={} candles={}",
                            report.http_status,
                            report.candles.len()
                        ),
                    },
                    Err(error) => SourceProbeResult {
                        attempted: true,
                        source_has_boundary: None,
                        note: format!("probe unavailable: {}", bounded_note(&error)),
                    },
                };
            probes.insert(market.code(), outcome);
        }
        Ok(probes)
    }

    /// Query bounded storage or produce a verified backup in its fixed namespace.
    /// # Errors
    /// Reports capacity, verification and storage errors without automatic deletion.
    pub async fn storage_maintenance(
        &self,
        action: StorageMaintenanceAction,
    ) -> Result<StorageMaintenanceResult, LabError> {
        match action {
            StorageMaintenanceAction::Usage => {
                self.inner
                    .database
                    .call("maintenance_usage", |store| {
                        Ok(StorageMaintenanceResult::Usage {
                            usage: store.maintenance_usage()?,
                        })
                    })
                    .await
            }
            StorageMaintenanceAction::Checkpoint { mode } => {
                self.refuse_under_active_jobs().await?;
                let outcome = self
                    .inner
                    .database
                    .call("checkpoint_wal", move |store| store.checkpoint_wal(mode))
                    .await?;
                Ok(StorageMaintenanceResult::Checkpoint { outcome })
            }
            StorageMaintenanceAction::Compact => {
                self.refuse_under_active_jobs().await?;
                let outcome = self
                    .admitted(
                        "compact_database",
                        super::super::storage::Store::compact_database,
                    )
                    .await?;
                Ok(StorageMaintenanceResult::Compact { outcome })
            }
            StorageMaintenanceAction::HardDeleteBatch { requests } => {
                if requests.is_empty() || requests.len() > crate::contracts::MAX_HARD_DELETE_BATCH {
                    return Err(LabError::InvalidConfig(format!(
                        "hard-delete batch requires 1..={} previewed requests",
                        crate::contracts::MAX_HARD_DELETE_BATCH
                    )));
                }
                self.refuse_under_active_jobs().await?;
                let outcomes = self
                    .admitted("hard_delete_batch", move |store| {
                        store.execute_hard_delete_batch(&requests, UtcTimestamp::now())
                    })
                    .await?;
                Ok(StorageMaintenanceResult::HardDeleteBatch { outcomes })
            }
            StorageMaintenanceAction::CreateBackup { request_id } => {
                self.admitted("create_managed_backup", move |store| {
                    Ok(StorageMaintenanceResult::Backup {
                        receipt: store.create_managed_backup(&request_id, UtcTimestamp::now())?,
                    })
                })
                .await
            }
            StorageMaintenanceAction::ListBackups => {
                self.inner
                    .database
                    .call("list_managed_backups", |store| {
                        Ok(StorageMaintenanceResult::Backups {
                            page: store.list_managed_backups()?,
                        })
                    })
                    .await
            }
            StorageMaintenanceAction::RetentionCandidates {
                cutoff,
                keep_recent,
                offset,
                limit,
            } => {
                validate_research_page(limit)?;
                self.inner
                    .database
                    .call("retention_candidates", move |store| {
                        Ok(StorageMaintenanceResult::RetentionCandidates {
                            page: store.retention_candidates(
                                cutoff,
                                keep_recent,
                                offset,
                                limit,
                                UtcTimestamp::now(),
                            )?,
                        })
                    })
                    .await
            }
        }
    }

    /// Checkpoint and compaction rewrite or fold the database file, so they
    /// refuse while a compute job or collection task may hold write transactions.
    async fn refuse_under_active_jobs(&self) -> Result<(), LabError> {
        let status = self.status().await?;
        if status.running_attempts > 0
            || status.queued_attempts > 0
            || status.active_job_id.is_some()
        {
            return Err(LabError::TemporarilyBlocked(
                "ACTIVE_JOB: a compute or collection job is running or queued and may hold write transactions; retry storage maintenance after it finishes"
                    .into(),
            ));
        }
        let has_active_schedule_fires = self
            .inner
            .database
            .call("check_active_schedule_fires", |store| {
                store.has_active_schedule_fires()
            })
            .await?;
        if has_active_schedule_fires {
            return Err(LabError::TemporarilyBlocked(
                "ACTIVE_SCHEDULE: a collection schedule fire is in-flight, queued, or waiting retry; retry storage maintenance after it finishes"
                    .into(),
            ));
        }
        Ok(())
    }

    fn signal_managed_cancellation(&self, jobs: &[JobId]) -> Result<(), LabError> {
        let active = self
            .inner
            .active
            .lock()
            .map_err(|_| LabError::Internal("job control lock poisoned".into()))?;
        if let Some((_, cancellation)) = active.as_ref().filter(|(job, _)| jobs.contains(job)) {
            cancellation.cancel();
        }
        Ok(())
    }

    /// Execute a persistent paper-session lifecycle mutation (`start`, `pause`, `resume`, `stop`).
    ///
    /// # Errors
    /// Propagates validation, CAS sequence conflict, idempotency conflict, or SQLite errors.
    pub async fn paper_session_write(
        &self,
        write: PaperSessionWrite,
    ) -> Result<PaperSessionCommandResponse, LabError> {
        let now = UtcTimestamp::now();
        let (mut response, feed_gap_resume_or_replay) =
            self.write_paper_session(write, now).await?;
        if response.session.state == PaperSessionState::Running && !feed_gap_resume_or_replay {
            self.poll_due_paper_markets(now).await;
            let session_id = response.session.session_id.clone();
            if let Ok(refreshed) = self
                .inner
                .database
                .call("paper_session_post_write_refresh", move |store| {
                    store.paper_get_session(&session_id, now)
                })
                .await
            {
                response.session = refreshed.session;
            }
        }
        Ok(response)
    }

    /// Query persistent paper sessions (`start_options`, `list`, `get`, `events`, `request`).
    ///
    /// # Errors
    /// Propagates unknown session IDs, invalid pagination limits, or SQLite errors.
    pub async fn paper_session_query(&self, query: PaperSessionQuery) -> Result<Value, LabError> {
        let now = UtcTimestamp::now();
        let needs_poll = matches!(
            query,
            PaperSessionQuery::List { .. }
                | PaperSessionQuery::Get { .. }
                | PaperSessionQuery::Events { .. }
        );
        let reconciled = needs_poll && self.poll_due_paper_markets(now).await;
        let op = match &query {
            PaperSessionQuery::StartOptions => "paper_start_options",
            PaperSessionQuery::List { .. } => "paper_list_sessions",
            PaperSessionQuery::Get { .. } => "paper_get_session",
            PaperSessionQuery::Events { .. } => "paper_list_events",
            PaperSessionQuery::ReviewEvents { .. } => "paper_review_events",
            PaperSessionQuery::Request { .. } => "paper_readback_request",
        };
        self.inner
            .database
            .call(op, move |store| {
                if needs_poll && !reconciled {
                    let _ = store.reconcile_running_paper_sessions(&[], now);
                }
                match query {
                    PaperSessionQuery::StartOptions => {
                        Ok(serde_json::to_value(store.paper_start_options()?)?)
                    }
                    PaperSessionQuery::List {
                        after_session_id,
                        limit,
                    } => Ok(serde_json::to_value(store.paper_list_sessions(
                        after_session_id.as_ref(),
                        limit,
                        now,
                    )?)?),
                    PaperSessionQuery::Get { session_id } => Ok(serde_json::to_value(
                        store.paper_get_session(&session_id, now)?,
                    )?),
                    PaperSessionQuery::Events {
                        session_id,
                        after_sequence,
                        limit,
                    } => Ok(serde_json::to_value(store.paper_list_events(
                        &session_id,
                        after_sequence,
                        limit,
                    )?)?),
                    PaperSessionQuery::ReviewEvents {
                        session_id,
                        after_sequence,
                        as_of_sequence,
                        limit,
                    } => Ok(serde_json::to_value(store.paper_review_events(
                        &session_id,
                        after_sequence,
                        as_of_sequence,
                        limit,
                    )?)?),
                    PaperSessionQuery::Request { request_id } => Ok(serde_json::to_value(
                        store.paper_readback_request(&request_id, now)?,
                    )?),
                }
            })
            .await
    }

    /// Query private strategy-automation projections without venue or lifecycle effects.
    /// # Errors
    /// Propagates bounded-page validation and durable storage failures.
    pub async fn automation_session_query(
        &self,
        query: AutomationSessionQuery,
    ) -> Result<Value, LabError> {
        match query {
            AutomationSessionQuery::Performance { session_id } => {
                let report = self
                    .bounded_automation_review(
                        "automation_review_performance",
                        session_id,
                        |snapshot| {
                            crate::reporting::automation_review::build_performance(&snapshot)
                        },
                    )
                    .await?;
                Ok(serde_json::to_value(report)?)
            }
            AutomationSessionQuery::ExportManifest { session_id } => {
                let manifest = self
                    .bounded_automation_review(
                        "automation_review_export_manifest",
                        session_id,
                        |snapshot| crate::reporting::automation_review::export_manifest(&snapshot),
                    )
                    .await?;
                Ok(serde_json::to_value(manifest)?)
            }
            AutomationSessionQuery::ExportRead {
                session_id,
                file,
                offset,
                expected_sha256,
                expected_snapshot_digest,
            } => {
                let chunk = self
                    .bounded_automation_review(
                        "automation_review_export_read",
                        session_id,
                        move |snapshot| {
                            crate::reporting::automation_review::export_read(
                                &snapshot,
                                file,
                                offset,
                                &expected_sha256,
                                &expected_snapshot_digest,
                            )
                        },
                    )
                    .await?;
                Ok(serde_json::to_value(chunk)?)
            }
            query => {
                let now = UtcTimestamp::now();
                let live_execution_policy = self.live_execution_policy();
                self.inner
                    .database
                    .call("automation_session_query", move |store| {
                        store.automation_query_with_policy(query, now, live_execution_policy)
                    })
                    .await
            }
        }
    }

    async fn bounded_automation_review<T, F>(
        &self,
        operation: &'static str,
        session_id: crate::contracts::AutomationSessionId,
        calculate: F,
    ) -> Result<T, LabError>
    where
        T: Send + 'static,
        F: FnOnce(crate::contracts::AutomationReviewSnapshot) -> Result<T, LabError>
            + Send
            + 'static,
    {
        self.bounded_saved_review(
            operation,
            move |store| store.automation_review_snapshot(&session_id),
            calculate,
        )
        .await
    }

    /// Apply a durable strategy-automation lifecycle command or owned MOCK cancellation.
    /// # Errors
    /// Propagates CAS/idempotency conflicts, safety denials, unresolved effects, or storage failures.
    pub async fn automation_session_write(
        &self,
        write: AutomationSessionWrite,
    ) -> Result<AutomationSessionCommandResponse, LabError> {
        self.write_automation_session(write).await
    }

    /// Best-effort completed-candle poll for `RUNNING` paper sessions whose latest boundary
    /// is not yet present in `candle_observations`.
    pub(super) async fn poll_due_paper_markets(&self, now: UtcTimestamp) -> bool {
        let due = match self
            .inner
            .database
            .call("due_paper_network_polls", move |store| {
                let _ = store.reconcile_running_paper_sessions(&[], now)?;
                store.due_paper_network_polls(now)
            })
            .await
        {
            Ok(due) => due,
            Err(error) => {
                tracing::warn!(reason = %bounded_note(&error), "paper completed-candle polling could not inspect durable state");
                return false;
            }
        };
        if due.is_empty() {
            return true;
        }
        let Ok(count) = ProbeCount::try_from(crate::paper::MAX_PAPER_RECOVERY_BARS) else {
            return true;
        };
        let mut batches = Vec::new();
        for req in due {
            if self.inner.stop.is_cancelled() {
                return true;
            }
            let batch = if let Some(feed) = &self.inner.public_market_feed {
                match feed
                    .read_or_bootstrap_observations(&req.market, req.interval, count)
                    .await
                {
                    Ok(batch) => match feed.ensure_ready(&req.market, req.interval) {
                        Ok(()) => Some(batch),
                        Err(error) => {
                            tracing::debug!(market = %req.market.code(), interval = ?req.interval, reason = %bounded_note(&error), "paper completed-candle input awaiting feed readiness");
                            None
                        }
                    },
                    Err(error @ LabError::BlockedEvidence(_)) => {
                        tracing::debug!(market = %req.market.code(), interval = ?req.interval, reason = %bounded_note(&error), "paper completed-candle input awaiting recovery evidence");
                        None
                    }
                    Err(error) => {
                        tracing::warn!(market = %req.market.code(), interval = ?req.interval, reason = %bounded_note(&error), "paper completed-candle input recovery failed");
                        None
                    }
                }
            } else {
                self.inner
                    .upbit
                    .poll_paper_observations(&req.market, req.interval, count, &self.inner.database)
                    .await
                    .ok()
            };
            if let Some(batch) = batch {
                batches.push(batch);
            }
        }
        if batches.is_empty() {
            return true;
        }
        if let Err(error) = self
            .inner
            .database
            .call("reconcile_running_paper_sessions", move |store| {
                store.reconcile_running_paper_sessions(&batches, now)
            })
            .await
        {
            tracing::warn!(reason = %bounded_note(&error), "paper completed-candle persistence or policy evaluation failed");
            return false;
        }
        true
    }
}

/// Requested target state of a schedule mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleMutationTarget {
    Paused,
    Active,
}

/// Wrap the schedule record with its bounded mutation outcome, keeping every
/// existing record field at the top level for backward-compatible readers.
/// # Errors
/// Propagates serialization failures.
fn mutation_receipt(
    record: &crate::contracts::CollectionScheduleRecord,
    outcome: ScheduleMutationOutcome,
) -> Result<Value, LabError> {
    let mut value = serde_json::to_value(record)?;
    if let serde_json::Value::Object(fields) = &mut value {
        fields.insert("mutation_outcome".into(), serde_json::to_value(outcome)?);
    }
    Ok(value)
}

/// Bounded single-line error note for probe diagnostics.
pub(super) fn bounded_note(error: &LabError) -> String {
    error.to_string().chars().take(200).collect()
}
