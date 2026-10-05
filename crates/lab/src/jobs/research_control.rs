//! Domain adapters and admission for durable research producers.

use super::JobService;
use crate::contracts::{
    CollectionScheduleAction, JobId, LabError, ProbeCount, ResearchSuiteAction,
    ResearchSuiteRequest, ScheduleId, ScheduleMutationOutcome, SourceProbeResult,
    StorageMaintenanceAction, StorageMaintenanceResult, SuiteId, UtcTimestamp,
    validate_research_page,
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
fn bounded_note(error: &LabError) -> String {
    error.to_string().chars().take(200).collect()
}
