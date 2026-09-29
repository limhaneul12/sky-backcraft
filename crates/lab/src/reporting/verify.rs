//! Engine-independent linkage, input-hash, and exact accounting verification.
//!
//! This module must not import strategy, execution, or accounting code. Its
//! calculations intentionally reconstruct cash, quantity, moving-average basis,
//! fees, equity, and drawdown from portable ledger facts alone.

use crate::contracts::{
    AccountMark, ContentHash, FillRecord, LabError, ModelLedger, PolicyProgram, RunBundle, RunId,
    Side, ValidationReport, ValidationStatus, experiment_config_digest, strategy_binding,
};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize)]
struct SemanticManifest<'a> {
    schema_version: &'a str,
    code_revision: &'a Option<String>,
    source_digest: &'a ContentHash,
    lockfile_digest: &'a ContentHash,
    toolchain: &'a str,
    engine_version: &'a str,
    indicator_version: &'a str,
    rounding_version: &'a str,
    mode: &'a str,
    historical_availability_model: &'a str,
    execution_origin: &'a str,
    fill_observed: bool,
    origin: crate::contracts::MarketDataOrigin,
    numeric_tolerance: &'a str,
    metric_tolerance: f64,
    start_policy: &'a str,
    account_mode: &'a str,
    assumptions: &'a [String],
}

#[derive(Serialize)]
struct SemanticProjection<'a> {
    manifest: SemanticManifest<'a>,
    plan: &'a crate::contracts::ResolvedPlan,
    datasets: &'a [crate::contracts::DatasetSnapshot],
    evidence: &'a Option<crate::contracts::EvidenceSnapshot>,
    models: Vec<ModelLedger>,
}

#[derive(Serialize)]
struct InputProjection<'a> {
    config_digest: &'a ContentHash,
    dataset_digests: &'a [(crate::contracts::DatasetId, ContentHash)],
    evidence_digest: &'a Option<ContentHash>,
}

/// Calculate the frozen input identity from the config and immutable input digests.
///
/// # Errors
/// Returns a contract error only if deterministic serialization fails.
pub fn input_digest(bundle: &RunBundle) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&InputProjection {
        config_digest: &bundle.plan.config_digest,
        dataset_digests: &bundle.plan.dataset_digests,
        evidence_digest: &bundle.plan.evidence_digest,
    })
}

/// Calculate the semantic result digest while explicitly excluding occurrence
/// metadata: run/job/attempt identity, run creation time, and embedded run IDs.
/// All input provenance times, decision/fill/accounting times, configuration,
/// rule/version fields, and economic facts remain in the projection.
///
/// # Errors
/// Returns a contract error only if deterministic serialization fails.
pub fn semantic_digest(bundle: &RunBundle) -> Result<ContentHash, LabError> {
    let semantic_run_id = RunId::from_seed("reporting-semantic-run");
    let mut models = bundle.models.clone();
    for model in &mut models {
        for signal in &mut model.signals {
            signal.context.run_id = semantic_run_id.clone();
        }
        for order in &mut model.orders {
            order.context.run_id = semantic_run_id.clone();
        }
        for order_event in &mut model.order_events {
            order_event.context.run_id = semantic_run_id.clone();
        }
        for fill in &mut model.fills {
            fill.context.run_id = semantic_run_id.clone();
        }
        for mark in &mut model.account_marks {
            mark.context.run_id = semantic_run_id.clone();
        }
        for episode in &mut model.episodes {
            episode.run_id = semantic_run_id.clone();
        }
    }
    ContentHash::of_value(&SemanticProjection {
        manifest: SemanticManifest {
            schema_version: &bundle.manifest.schema_version,
            code_revision: &bundle.manifest.code_revision,
            source_digest: &bundle.manifest.source_digest,
            lockfile_digest: &bundle.manifest.lockfile_digest,
            toolchain: &bundle.manifest.toolchain,
            engine_version: &bundle.manifest.engine_version,
            indicator_version: &bundle.manifest.indicator_version,
            rounding_version: &bundle.manifest.rounding_version,
            mode: &bundle.manifest.mode,
            historical_availability_model: &bundle.manifest.historical_availability_model,
            execution_origin: &bundle.manifest.execution_origin,
            fill_observed: bundle.manifest.fill_observed,
            origin: bundle.manifest.origin,
            numeric_tolerance: &bundle.manifest.numeric_tolerance,
            metric_tolerance: bundle.manifest.metric_tolerance,
            start_policy: &bundle.manifest.start_policy,
            account_mode: &bundle.manifest.account_mode,
            assumptions: &bundle.manifest.assumptions,
        },
        plan: &bundle.plan,
        datasets: &bundle.datasets,
        evidence: &bundle.evidence,
        models,
    })
}

/// Independently verify input identities, links, exact accounting, and sampled MDD.
/// The report is returned as `FAIL` with bounded findings rather than hiding the
/// first corruption behind an early error.
#[must_use]
pub fn verify_run(bundle: &RunBundle) -> ValidationReport {
    let expected_models = bundle
        .plan
        .admissions
        .iter()
        .map(|admission| admission.model_id.as_str().to_owned())
        .collect();
    verify_run_with_models(bundle, &expected_models, true)
}

pub(super) fn verify_run_subset(
    bundle: &RunBundle,
    expected_models: &BTreeSet<String>,
) -> ValidationReport {
    verify_run_with_models(bundle, expected_models, false)
}

fn verify_run_with_models(
    bundle: &RunBundle,
    expected_models: &BTreeSet<String>,
    require_complete_sequence: bool,
) -> ValidationReport {
    let mut verifier = Verifier::new(bundle);
    verifier.verify_inputs();
    verifier.verify_models(expected_models, require_complete_sequence);
    verifier.finish()
}

struct Verifier<'a> {
    bundle: &'a RunBundle,
    findings: Vec<String>,
    checked_fills: u64,
    checked_marks: u64,
}

impl<'a> Verifier<'a> {
    fn new(bundle: &'a RunBundle) -> Self {
        Self {
            bundle,
            findings: Vec::new(),
            checked_fills: 0,
            checked_marks: 0,
        }
    }

    fn finding(&mut self, finding: impl Into<String>) {
        const MAX_FINDINGS: usize = 256;
        if self.findings.len() < MAX_FINDINGS {
            self.findings.push(finding.into());
        }
    }

    fn verify_inputs(&mut self) {
        self.verify_datasets();
        if Decimal::from_str_exact(&self.bundle.manifest.numeric_tolerance).ok()
            != Some(crate::contracts::NUMERIC_TOLERANCE)
        {
            self.finding("run manifest numeric tolerance differs from the contract");
        }
        match experiment_config_digest(&self.bundle.plan.spec, &self.bundle.plan.policy_revisions) {
            Ok(digest) if digest != self.bundle.plan.config_digest => {
                self.finding("plan config_digest does not hash the exact frozen experiment");
            }
            Err(error) => self.finding(format!("invalid frozen policy/configuration: {error}")),
            Ok(_) => {}
        }

        let datasets: BTreeMap<_, _> = self
            .bundle
            .datasets
            .iter()
            .map(|dataset| {
                (
                    dataset.manifest.id.as_str(),
                    &dataset.manifest.semantic_digest,
                )
            })
            .collect();
        if datasets.len() != self.bundle.datasets.len() {
            self.finding("duplicate dataset IDs in RunBundle");
        }
        if self.bundle.plan.dataset_digests.len() != self.bundle.datasets.len() {
            self.finding("plan dataset digest catalog does not cover every bundled dataset");
        }
        let specified_datasets: BTreeSet<_> = self
            .bundle
            .plan
            .spec
            .dataset_ids
            .iter()
            .map(crate::contracts::DatasetId::as_str)
            .collect();
        if specified_datasets.len() != self.bundle.plan.spec.dataset_ids.len()
            || specified_datasets != datasets.keys().copied().collect()
        {
            self.finding("experiment dataset IDs do not exactly cover bundled snapshots");
        }
        for (id, digest) in &self.bundle.plan.dataset_digests {
            if datasets.get(id.as_str()).copied() != Some(digest) {
                self.finding(format!("dataset digest/reference mismatch for {id}"));
            }
        }

        match (&self.bundle.evidence, &self.bundle.plan.evidence_digest) {
            (Some(snapshot), Some(expected)) if &snapshot.digest != expected => {
                self.finding("evidence snapshot digest does not match frozen plan");
            }
            (Some(_), None) | (None, Some(_)) => {
                self.finding("evidence presence does not match frozen plan");
            }
            _ => {}
        }

        match input_digest(self.bundle) {
            Ok(digest) if digest != self.bundle.plan.input_digest => self.finding(
                "plan input_digest does not match config/dataset/evidence digest projection",
            ),
            Err(error) => self.finding(format!("could not calculate input digest: {error}")),
            Ok(_) => {}
        }
        match semantic_digest(self.bundle) {
            Ok(digest) if digest != self.bundle.semantic_digest => {
                self.finding("run semantic_digest mismatch");
            }
            Err(error) => self.finding(format!("could not calculate semantic digest: {error}")),
            Ok(_) => {}
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one independent boundary validates complete exported dataset identity and closure"
    )]
    fn verify_datasets(&mut self) {
        let mut observations: BTreeMap<String, Vec<&crate::contracts::CandleObservation>> =
            BTreeMap::new();
        let mut raw_objects: BTreeMap<String, ContentHash> = BTreeMap::new();
        for snapshot in &self.bundle.datasets {
            for raw in &snapshot.manifest.raw_objects {
                match ContentHash::of_value(raw) {
                    Ok(identity) => match raw_objects.entry(raw.id.as_str().to_owned()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(identity);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() != &identity =>
                        {
                            self.finding(format!(
                                "raw object {} has conflicting metadata across datasets",
                                raw.id
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    },
                    Err(error) => self.finding(format!(
                        "raw object {} metadata hashing failed: {error}",
                        raw.id
                    )),
                }
            }
            for observation in &snapshot.observations {
                observations
                    .entry(observation.id.as_str().to_owned())
                    .or_default()
                    .push(observation);
            }
        }
        for (id, candidates) in &observations {
            let canonical = candidates.first().map(|row| &row.content_digest);
            if candidates
                .iter()
                .any(|row| Some(&row.content_digest) != canonical)
            {
                self.finding(format!(
                    "observation {id} has conflicting economic identities across datasets"
                ));
            }
        }

        for snapshot in &self.bundle.datasets {
            match crate::contracts::dataset_digests(snapshot) {
                Ok(digests) => {
                    if snapshot.manifest.semantic_digest != digests.semantic
                        || snapshot.manifest.provenance_digest != digests.provenance
                        || snapshot.manifest.id != crate::contracts::dataset_id(&digests)
                    {
                        self.finding(format!(
                            "dataset {} canonical identity mismatch",
                            snapshot.manifest.id
                        ));
                    }
                }
                Err(error) => self.finding(format!(
                    "dataset {} canonical hashing failed: {error}",
                    snapshot.manifest.id
                )),
            }
            self.verify_snapshot_structure(snapshot, &observations);
        }
    }

    fn verify_snapshot_structure(
        &mut self,
        snapshot: &crate::contracts::DatasetSnapshot,
        observations: &BTreeMap<String, Vec<&crate::contracts::CandleObservation>>,
    ) {
        let manifest = &snapshot.manifest;
        let row_count = u64::try_from(snapshot.observations.len()).unwrap_or(u64::MAX);
        let stable_order = snapshot.observations.windows(2).all(|pair| {
            (&pair[0].candle.market, pair[0].candle.open_time_utc)
                < (&pair[1].candle.market, pair[1].candle.open_time_utc)
        });
        if manifest.row_count != row_count || !stable_order {
            self.finding(format!(
                "dataset {} row count or stable member order mismatch",
                manifest.id
            ));
        }
        if manifest.status == crate::contracts::DatasetStatus::Ready
            && manifest
                .quality_issues
                .iter()
                .any(|issue| issue.severity == crate::contracts::QualitySeverity::Error)
        {
            self.finding(format!(
                "READY dataset {} contains ERROR quality issues",
                manifest.id
            ));
        }
        let raw_ids: BTreeSet<_> = manifest
            .raw_objects
            .iter()
            .map(|object| object.id.as_str())
            .collect();
        if raw_ids.len() != manifest.raw_objects.len() {
            self.finding(format!(
                "dataset {} has duplicate raw object IDs",
                manifest.id
            ));
        }
        let coverage_valid = manifest.request.completed_only
            && manifest
                .request
                .range
                .with_warmup(
                    manifest.request.warmup_bars,
                    manifest.request.data_resolution,
                )
                .is_ok_and(|coverage| coverage == manifest.coverage);
        let raw_catalog_valid = manifest
            .raw_objects
            .iter()
            .all(|raw| raw.origin == manifest.origin)
            && manifest.quality_issues.iter().all(|issue| {
                issue
                    .raw_object_ids
                    .iter()
                    .all(|id| raw_ids.contains(id.as_str()))
            });
        if !coverage_valid || !raw_catalog_valid {
            self.finding(format!(
                "dataset {} coverage or raw catalog contract mismatch",
                manifest.id
            ));
        }
        let derived_rows = snapshot
            .observations
            .iter()
            .filter(|row| !row.constituent_ids.is_empty())
            .count();
        if derived_rows != 0 && derived_rows != snapshot.observations.len() {
            self.finding(format!(
                "dataset {} mixes source and derived observations",
                manifest.id
            ));
        }
        if derived_rows != 0
            && manifest.normalizer_version
                != format!(
                    "{}+{}",
                    crate::contracts::NORMALIZER_VERSION,
                    crate::contracts::RESAMPLE_VERSION
                )
        {
            self.finding(format!(
                "derived dataset {} has an unknown transform version",
                manifest.id
            ));
        }
        for row in &snapshot.observations {
            self.verify_observation(snapshot, row, &raw_ids, observations);
        }
        if manifest.status == crate::contracts::DatasetStatus::Ready {
            self.verify_ready_grid(snapshot);
        }
    }

    fn verify_observation(
        &mut self,
        snapshot: &crate::contracts::DatasetSnapshot,
        row: &crate::contracts::CandleObservation,
        raw_ids: &BTreeSet<&str>,
        observations: &BTreeMap<String, Vec<&crate::contracts::CandleObservation>>,
    ) {
        let candle = &row.candle;
        let close = candle
            .open_time_utc
            .0
            .checked_add_signed(candle.interval.duration())
            .map(crate::contracts::UtcTimestamp);
        let identity_valid = crate::contracts::observation_digest(candle).is_ok_and(|digest| {
            row.content_digest == digest
                && row.id == crate::contracts::ObservationId::from_seed(digest.as_str())
        });
        let shape_valid = candle.interval == snapshot.manifest.request.data_resolution
            && snapshot
                .manifest
                .request
                .markets
                .iter()
                .any(|market| market.code() == candle.market)
            && snapshot.manifest.coverage.contains(candle.open_time_utc)
            && close == Some(candle.close_time_utc)
            && candle.completed
            && candle.low <= candle.open
            && candle.low <= candle.close
            && candle.high >= candle.open
            && candle.high >= candle.close;
        let raw_valid = !row.raw_object_ids.is_empty()
            && row
                .raw_object_ids
                .iter()
                .all(|id| raw_ids.contains(id.as_str()));
        if !identity_valid || !shape_valid || !raw_valid {
            self.finding(format!(
                "dataset {} observation {} identity/shape/raw closure mismatch",
                snapshot.manifest.id, row.id
            ));
        }
        if !row.constituent_ids.is_empty() {
            self.verify_derived_observation(snapshot, row, observations);
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "derived-bar proof checks order, partition, exact aggregation and raw closure together"
    )]
    fn verify_derived_observation(
        &mut self,
        snapshot: &crate::contracts::DatasetSnapshot,
        derived: &crate::contracts::CandleObservation,
        observations: &BTreeMap<String, Vec<&crate::contracts::CandleObservation>>,
    ) {
        let mut seen = BTreeSet::new();
        let mut constituents = Vec::with_capacity(derived.constituent_ids.len());
        for id in &derived.constituent_ids {
            if id == &derived.id || !seen.insert(id.as_str()) {
                self.finding(format!(
                    "derived observation {} has self/duplicate constituents",
                    derived.id
                ));
                return;
            }
            let Some(row) = observations
                .get(id.as_str())
                .and_then(|candidates| candidates.first())
                .copied()
            else {
                self.finding(format!(
                    "derived observation {} constituent {} is absent from export",
                    derived.id, id
                ));
                return;
            };
            constituents.push(row);
        }
        let Some(first) = constituents.first().copied() else {
            return;
        };
        let Some(last) = constituents.last().copied() else {
            return;
        };
        let target_step = derived.candle.interval.duration().num_seconds();
        let source_step = first.candle.interval.duration().num_seconds();
        let expected_count = usize::try_from(target_step / source_step).ok();
        let partition_valid = source_step > 0
            && source_step < target_step
            && target_step % source_step == 0
            && expected_count == Some(constituents.len())
            && first.candle.open_time_utc == derived.candle.open_time_utc
            && last.candle.close_time_utc == derived.candle.close_time_utc
            && constituents.iter().all(|row| {
                row.candle.market == derived.candle.market
                    && row.candle.interval == first.candle.interval
                    && row.candle.completed
            })
            && constituents
                .windows(2)
                .all(|pair| pair[0].candle.close_time_utc == pair[1].candle.open_time_utc);
        let mut high = first.candle.high;
        let mut low = first.candle.low;
        let mut volume = Decimal::ZERO;
        let mut turnover = Decimal::ZERO;
        let derived_raw: BTreeSet<_> = derived
            .raw_object_ids
            .iter()
            .map(crate::contracts::RawObjectId::as_str)
            .collect();
        let mut constituent_raw = BTreeSet::new();
        let mut arithmetic_valid = true;
        for (id, row) in derived.constituent_ids.iter().zip(&constituents) {
            high = high.max(row.candle.high);
            low = low.min(row.candle.low);
            let Some(next_volume) = volume.checked_add(row.candle.volume.get()) else {
                arithmetic_valid = false;
                break;
            };
            volume = next_volume;
            let Some(next_turnover) = turnover.checked_add(row.candle.quote_turnover.get()) else {
                arithmetic_valid = false;
                break;
            };
            turnover = next_turnover;
            if let Some(candidate) = observations.get(id.as_str()).and_then(|candidates| {
                candidates.iter().copied().find(|candidate| {
                    candidate
                        .raw_object_ids
                        .iter()
                        .all(|raw| derived_raw.contains(raw.as_str()))
                })
            }) {
                constituent_raw.extend(
                    candidate
                        .raw_object_ids
                        .iter()
                        .map(crate::contracts::RawObjectId::as_str),
                );
            } else {
                arithmetic_valid = false;
            }
        }
        let aggregate_valid = arithmetic_valid
            && partition_valid
            && derived.candle.open == first.candle.open
            && derived.candle.close == last.candle.close
            && derived.candle.high == high
            && derived.candle.low == low
            && derived.candle.volume.get() == volume
            && derived.candle.quote_turnover.get() == turnover
            && constituent_raw == derived_raw;
        if !aggregate_valid {
            self.finding(format!(
                "derived observation {} ordered partition/aggregation/raw closure mismatch in dataset {}",
                derived.id, snapshot.manifest.id
            ));
        }
    }

    fn verify_ready_grid(&mut self, snapshot: &crate::contracts::DatasetSnapshot) {
        let manifest = &snapshot.manifest;
        let expected = manifest
            .coverage
            .bars(manifest.request.data_resolution)
            .ok()
            .and_then(|bars| bars.checked_mul(manifest.request.markets.len()));
        if expected != Some(snapshot.observations.len()) {
            self.finding(format!("READY dataset {} grid count mismatch", manifest.id));
            return;
        }
        let members: BTreeSet<_> = snapshot
            .observations
            .iter()
            .map(|row| (row.candle.market.as_str(), row.candle.open_time_utc))
            .collect();
        for market in &manifest.request.markets {
            let code = market.code();
            let mut cursor = manifest.coverage.start();
            while cursor < manifest.coverage.end() {
                if !members.contains(&(code.as_str(), cursor)) {
                    self.finding(format!(
                        "READY dataset {} has a UTC grid gap for {} at {}",
                        manifest.id, code, cursor
                    ));
                    break;
                }
                let Some(next) = cursor
                    .0
                    .checked_add_signed(manifest.request.data_resolution.duration())
                else {
                    self.finding(format!("dataset {} grid timestamp overflow", manifest.id));
                    break;
                };
                cursor = crate::contracts::UtcTimestamp(next);
            }
        }
    }

    fn verify_models(
        &mut self,
        expected_models: &BTreeSet<String>,
        require_complete_sequence: bool,
    ) {
        let observations: BTreeMap<_, _> = self
            .bundle
            .datasets
            .iter()
            .flat_map(|dataset| {
                dataset
                    .observations
                    .iter()
                    .map(|observation| (observation.id.as_str(), observation))
            })
            .collect();
        let observation_count: usize =
            self.bundle.datasets.iter().fold(0_usize, |total, dataset| {
                total.saturating_add(dataset.observations.len())
            });
        if observations.len() != observation_count {
            self.finding("duplicate observation IDs across bundled datasets");
        }
        let mut model_ids = BTreeSet::new();
        let mut global_event_sequences = BTreeSet::new();
        for model in &self.bundle.models {
            if !model_ids.insert(model.model_id.as_str()) {
                self.finding(format!("duplicate model ID {}", model.model_id));
            }
            let admission = self
                .bundle
                .plan
                .admissions
                .iter()
                .find(|admission| admission.model_id == model.model_id)
                .cloned();
            match admission {
                Some(admission) => self.verify_model_binding(model, &admission),
                None => self.finding(format!(
                    "{} ledger has no frozen model admission",
                    model.model_id
                )),
            }
            self.verify_model(
                model,
                &observations,
                &mut global_event_sequences,
                require_complete_sequence,
            );
        }
        let actual_models: BTreeSet<_> = model_ids.into_iter().map(str::to_owned).collect();
        if &actual_models != expected_models {
            self.finding("model ledgers do not exactly cover the required admission scope");
        }
    }

    fn verify_model_binding(
        &mut self,
        model: &ModelLedger,
        admission: &crate::contracts::ModelAdmission,
    ) {
        let binding_valid = admission.market == model.market
            && admission.strategy == model.strategy.kind()
            && admission.policy_ref.as_ref() == model.strategy.policy_ref()
            && admission_status_matches(admission.status, model.status);
        let frozen_valid = strategy_binding(&self.bundle.plan, admission).is_ok()
            && model.strategy.program(&self.bundle.plan).is_ok();
        if !binding_valid || !frozen_valid {
            self.finding(format!(
                "{} ledger does not match its exact frozen model admission",
                model.model_id
            ));
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one audit boundary checks all cross-record links before exact accounting"
    )]
    fn verify_model(
        &mut self,
        model: &ModelLedger,
        observations: &BTreeMap<&str, &crate::contracts::CandleObservation>,
        global_event_sequences: &mut BTreeSet<u64>,
        require_complete_sequence: bool,
    ) {
        let previous_model_last = global_event_sequences.last().copied().unwrap_or(0);
        let signals: BTreeMap<_, _> = model
            .signals
            .iter()
            .map(|record| (record.signal_id.as_str(), record))
            .collect();
        let orders: BTreeMap<_, _> = model
            .orders
            .iter()
            .map(|record| (record.order_id.as_str(), record))
            .collect();
        let fills: BTreeMap<_, _> = model
            .fills
            .iter()
            .map(|record| (record.fill_id.as_str(), record))
            .collect();
        let episodes: BTreeMap<_, _> = model
            .episodes
            .iter()
            .map(|record| (record.episode_id.as_str(), record))
            .collect();
        if signals.len() != model.signals.len() {
            self.finding(format!("{} has duplicate signal IDs", model.model_id));
        }
        if orders.len() != model.orders.len() {
            self.finding(format!("{} has duplicate order IDs", model.model_id));
        }
        if fills.len() != model.fills.len() {
            self.finding(format!("{} has duplicate fill IDs", model.model_id));
        }
        if episodes.len() != model.episodes.len() {
            self.finding(format!("{} has duplicate episode IDs", model.model_id));
        }
        let mut event_contexts: Vec<_> = model
            .signals
            .iter()
            .map(|record| &record.context)
            .chain(model.order_events.iter().map(|record| &record.context))
            .chain(model.fills.iter().map(|record| &record.context))
            .chain(model.account_marks.iter().map(|record| &record.context))
            .collect();
        event_contexts.sort_by_key(|context| context.event_seq);
        let sequence_shape_valid = if event_contexts.is_empty() {
            !require_complete_sequence || model.last_event_seq == previous_model_last
        } else {
            (!require_complete_sequence
                || event_contexts.first().is_some_and(|context| {
                    previous_model_last
                        .checked_add(1)
                        .is_some_and(|expected| context.event_seq == expected)
                }))
                && event_contexts.windows(2).all(|pair| {
                    pair[0]
                        .event_seq
                        .checked_add(1)
                        .is_some_and(|expected| pair[1].event_seq == expected)
                })
                && event_contexts
                    .last()
                    .is_some_and(|context| context.event_seq == model.last_event_seq)
        };
        if !sequence_shape_valid {
            self.finding(format!(
                "{} event sequence has gaps or last_event_seq mismatch",
                model.model_id
            ));
        }
        if event_contexts
            .windows(2)
            .any(|pair| pair[0].accounting_event_time > pair[1].accounting_event_time)
        {
            self.finding(format!(
                "{} accounting times regress in event order",
                model.model_id
            ));
        }
        if model.status == crate::contracts::ModelStatus::Completed
            && model.account_marks.is_empty()
        {
            self.finding(format!(
                "{} completed without account marks",
                model.model_id
            ));
        }
        let blocked_or_skipped = matches!(
            model.status,
            crate::contracts::ModelStatus::BlockedEvidence
                | crate::contracts::ModelStatus::BlockedData
                | crate::contracts::ModelStatus::Skipped
        );
        if blocked_or_skipped {
            let has_facts = !model.signals.is_empty()
                || !model.orders.is_empty()
                || !model.order_events.is_empty()
                || !model.fills.is_empty()
                || !model.episodes.is_empty()
                || !model.account_marks.is_empty();
            if has_facts
                || model
                    .status_reason
                    .as_deref()
                    .is_none_or(|reason| reason.is_empty() || reason.len() > 2_048)
            {
                self.finding(format!(
                    "{} blocked/skipped ledger must contain only a bounded status reason",
                    model.model_id
                ));
            }
        }

        let mut previous_policy_state: Option<&[crate::contracts::NamedPolicyValue]> = None;
        for signal in &model.signals {
            self.verify_context(model, &signal.context, global_event_sequences);
            if signal.source_bar_ids.is_empty()
                || signal
                    .source_bar_ids
                    .iter()
                    .any(|id| !observations.contains_key(id.as_str()))
            {
                self.finding(format!(
                    "{} signal {} has a missing source observation",
                    model.model_id, signal.signal_id
                ));
            }
            self.verify_signal_source(model, signal, observations);
            self.verify_policy_signal(model, signal, previous_policy_state);
            previous_policy_state = signal
                .policy_trace
                .as_ref()
                .map(|trace| trace.state_after.as_slice());
        }
        self.verify_order_history(model, &signals, &orders, &episodes, global_event_sequences);
        for order in &model.orders {
            self.verify_context_identity(model, &order.context);
            if !signals.contains_key(order.parent_signal_id.as_str()) {
                self.finding(format!(
                    "{} order {} has missing parent signal",
                    model.model_id, order.order_id
                ));
            }
            if order
                .episode_id
                .as_ref()
                .is_some_and(|id| !episodes.contains_key(id.as_str()))
            {
                self.finding(format!(
                    "{} order {} has a missing episode link",
                    model.model_id, order.order_id
                ));
            }
            if order.rule_snapshot_id != self.bundle.plan.spec.market_rules.id {
                self.finding(format!(
                    "{} order {} references an unfrozen rule snapshot",
                    model.model_id, order.order_id
                ));
            }
            if order.created_at > order.effective_at
                || order.effective_at > order.expires_at
                || order.expires_at > self.bundle.plan.spec.range.end()
                || !valid_order_status_quantity(order)
            {
                self.finding(format!(
                    "{} order {} has invalid lifecycle time or status quantity",
                    model.model_id, order.order_id
                ));
            }
            let filled = checked_sum(
                model
                    .fills
                    .iter()
                    .filter(|fill| fill.order_id == order.order_id)
                    .map(|fill| fill.qty.get()),
            );
            if filled.is_none_or(|value| {
                value != order.cumulative_filled_qty.get() || value > order.requested_qty.get()
            }) {
                self.finding(format!(
                    "{} order {} cumulative fill quantity mismatch",
                    model.model_id, order.order_id
                ));
            }
        }
        for fill in &model.fills {
            self.verify_context(model, &fill.context, global_event_sequences);
            self.checked_fills = self.checked_fills.saturating_add(1);
            if !orders.contains_key(fill.order_id.as_str())
                || !episodes.contains_key(fill.episode_id.as_str())
            {
                self.finding(format!(
                    "{} fill {} has an orphan order or episode link",
                    model.model_id, fill.fill_id
                ));
            }
            if !observations.contains_key(fill.source_bar_id.as_str()) {
                self.finding(format!(
                    "{} fill {} has a missing source observation",
                    model.model_id, fill.fill_id
                ));
            }
            self.verify_fill_causality(model, fill, &orders, &signals, observations);
            if fill.price.get().checked_mul(fill.qty.get()) != Some(fill.notional.get()) {
                self.finding(format!(
                    "{} fill {} notional is not price times quantity",
                    model.model_id, fill.fill_id
                ));
            }
            let expected_fee = fill
                .notional
                .get()
                .checked_mul(fill.fee_bps.get())
                .and_then(|value| value.checked_div(Decimal::from(10_000_u32)));
            if expected_fee != Some(fill.fee.get()) {
                self.finding(format!(
                    "{} fill {} fee does not match notional and fee rate",
                    model.model_id, fill.fill_id
                ));
            }
            if fill.fill_observed || fill.execution_origin != "SIMULATED_ONLY" {
                self.finding(format!(
                    "{} fill {} falsely claims observed execution",
                    model.model_id, fill.fill_id
                ));
            }
            if fill.accounting_mark_seq <= fill.context.event_seq {
                self.finding(format!(
                    "{} fill {} has a non-causal accounting mark sequence",
                    model.model_id, fill.fill_id
                ));
            }
            if let Some(episode) = episodes.get(fill.episode_id.as_str())
                && (!episode.fill_ids.contains(&fill.fill_id)
                    || !episode.order_ids.contains(&fill.order_id))
            {
                self.finding(format!(
                    "{} fill {} is absent from its episode links",
                    model.model_id, fill.fill_id
                ));
            }
        }
        for episode in &model.episodes {
            if episode.run_id != self.bundle.manifest.run_id
                || episode.model_id != model.model_id
                || episode.market != model.market
            {
                self.finding(format!(
                    "{} episode {} context mismatch",
                    model.model_id, episode.episode_id
                ));
            }
            if episode
                .fill_ids
                .iter()
                .any(|id| !fills.contains_key(id.as_str()))
                || episode
                    .order_ids
                    .iter()
                    .any(|id| !orders.contains_key(id.as_str()))
            {
                self.finding(format!(
                    "{} episode {} has orphan fact links",
                    model.model_id, episode.episode_id
                ));
            }
            if episode
                .realized_price_pnl
                .get()
                .checked_sub(episode.fees.get())
                .is_none_or(|net| !within_numeric_tolerance(episode.net_realized.get(), net))
            {
                self.finding(format!(
                    "{} episode {} net realized incorrectly accounts for fees",
                    model.model_id, episode.episode_id
                ));
            }
            let closing = if episode.status == crate::contracts::EpisodeStatus::Closed {
                episode
                    .fill_ids
                    .last()
                    .and_then(|id| fills.get(id.as_str()))
                    .copied()
                    .and_then(|fill| {
                        orders
                            .get(fill.order_id.as_str())
                            .copied()
                            .map(|order| (fill, order))
                    })
                    .and_then(|(fill, order)| {
                        signals
                            .get(order.parent_signal_id.as_str())
                            .copied()
                            .map(|signal| (fill, order, signal))
                    })
            } else {
                None
            };
            if let Err(error) = crate::reporting::episode_exit_details(episode, closing) {
                self.finding(format!(
                    "{} episode {} closing provenance mismatch: {error}",
                    model.model_id, episode.episode_id
                ));
            }
            self.verify_episode_accounting(model, episode, &fills);
        }
        for mark in &model.account_marks {
            self.verify_context(model, &mark.context, global_event_sequences);
            self.checked_marks = self.checked_marks.saturating_add(1);
            if !observations.contains_key(mark.source_bar_id.as_str()) {
                self.finding(format!(
                    "{} mark {} has a missing source observation",
                    model.model_id, mark.context.event_seq
                ));
            }
        }
        if model.status == crate::contracts::ModelStatus::Completed {
            self.verify_execution_close_marks(model, observations);
            self.verify_accounting(model);
        } else if !blocked_or_skipped {
            self.verify_accounting(model);
        }
    }

    fn verify_policy_signal(
        &mut self,
        model: &ModelLedger,
        signal: &crate::contracts::SignalRecord,
        previous_state: Option<&[crate::contracts::NamedPolicyValue]>,
    ) {
        if signal.strategy != model.strategy.kind()
            || signal.policy_ref.as_ref() != model.strategy.policy_ref()
        {
            self.finding(format!(
                "{} signal {} does not match its model policy binding",
                model.model_id, signal.signal_id
            ));
            return;
        }
        let Ok(program) = model.strategy.program(&self.bundle.plan) else {
            return;
        };
        let requires_evidence = model
            .strategy
            .policy_ref()
            .and_then(|reference| {
                self.bundle
                    .plan
                    .policy_revisions
                    .iter()
                    .find(|revision| revision.reference == *reference)
            })
            .is_some_and(|revision| revision.definition.requires_evidence());
        match (model.strategy.policy_ref(), &program, &signal.policy_trace) {
            (None, _, None) if signal.indicators.policy_values.is_empty() => {}
            (Some(_), PolicyProgram::Builtin { strategy }, Some(trace)) => {
                let state_ids = if builtin_has_latent_state(strategy) {
                    &["base_signal_state"][..]
                } else {
                    &[]
                };
                let valid_state = trace.matched_rule_id.is_none()
                    && exact_named_values(&trace.state_before, state_ids)
                    && exact_named_values(&trace.state_after, state_ids)
                    && trace
                        .state_before
                        .iter()
                        .chain(&trace.state_after)
                        .all(|item| is_builtin_state_value(item.value))
                    && signal.indicators.policy_values.is_empty();
                if !valid_state {
                    self.finding(format!(
                        "{} signal {} has an invalid builtin policy trace",
                        model.model_id, signal.signal_id
                    ));
                }
                self.verify_policy_trace_common(
                    model,
                    signal,
                    trace,
                    previous_state,
                    requires_evidence,
                );
            }
            (Some(_), PolicyProgram::Rules { program }, Some(trace)) => {
                let indicator_ids: Vec<_> = program
                    .indicators
                    .iter()
                    .map(|item| item.id.as_str())
                    .collect();
                let state_ids: Vec<_> =
                    program.states.iter().map(|item| item.id.as_str()).collect();
                let matched_valid = trace
                    .matched_rule_id
                    .as_deref()
                    .is_none_or(|matched| program.rules.iter().any(|rule| rule.id == matched));
                if !matched_valid
                    || !exact_named_values(&signal.indicators.policy_values, &indicator_ids)
                    || !exact_named_values(&trace.state_before, &state_ids)
                    || !exact_named_values(&trace.state_after, &state_ids)
                {
                    self.finding(format!(
                        "{} signal {} has an invalid rules policy trace",
                        model.model_id, signal.signal_id
                    ));
                }
                self.verify_policy_trace_common(
                    model,
                    signal,
                    trace,
                    previous_state,
                    requires_evidence,
                );
            }
            _ => self.finding(format!(
                "{} signal {} has missing or unexpected policy trace data",
                model.model_id, signal.signal_id
            )),
        }
    }

    fn verify_policy_trace_common(
        &mut self,
        model: &ModelLedger,
        signal: &crate::contracts::SignalRecord,
        trace: &crate::contracts::PolicyTrace,
        previous_state: Option<&[crate::contracts::NamedPolicyValue]>,
        requires_evidence: bool,
    ) {
        let artificial_terminal = signal
            .reasons
            .contains(&crate::contracts::ReasonCode::ArtificialTerminalExit);
        let continuity_target = if artificial_terminal {
            &trace.state_after
        } else {
            &trace.state_before
        };
        if previous_state.is_some_and(|previous| !same_named_values(previous, continuity_target)) {
            self.finding(format!(
                "{} signal {} policy state is not continuous",
                model.model_id, signal.signal_id
            ));
        }
        let evidence_valid = trace.evidence.as_ref().is_none_or(|evidence| {
            evidence.policy == self.bundle.plan.spec.pit_policy
                && evidence.snapshot_id == self.bundle.plan.spec.evidence_snapshot_id
                && if artificial_terminal {
                    evidence.used_at <= signal.decision_available_at
                } else {
                    evidence.used_at == signal.decision_available_at
                }
        });
        if trace.evidence.is_some() != requires_evidence || !evidence_valid {
            self.finding(format!(
                "{} signal {} policy evidence trace is not frozen to the plan",
                model.model_id, signal.signal_id
            ));
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one independent chronological episode reconstruction keeps conservation checks with its state"
    )]
    fn verify_episode_accounting(
        &mut self,
        model: &ModelLedger,
        episode: &crate::contracts::EpisodeRecord,
        fills: &BTreeMap<&str, &FillRecord>,
    ) {
        let mut qty = Decimal::ZERO;
        let mut basis = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        let mut sequential_realized = Decimal::ZERO;
        let mut sell_notional = Decimal::ZERO;
        let mut removed_basis_total = Decimal::ZERO;
        let mut previous_event_seq = None;
        let mut valid = true;
        for fill_id in &episode.fill_ids {
            let Some(fill) = fills.get(fill_id.as_str()).copied() else {
                valid = false;
                break;
            };
            if fill.episode_id != episode.episode_id
                || previous_event_seq.is_some_and(|sequence| sequence >= fill.context.event_seq)
            {
                valid = false;
                break;
            }
            previous_event_seq = Some(fill.context.event_seq);
            let Some(next_fees) = fees.checked_add(fill.fee.get()) else {
                valid = false;
                break;
            };
            fees = next_fees;
            match fill.side {
                Side::Buy => {
                    let Some(next_qty) = qty.checked_add(fill.qty.get()) else {
                        valid = false;
                        break;
                    };
                    let Some(next_basis) = basis.checked_add(fill.notional.get()) else {
                        valid = false;
                        break;
                    };
                    qty = next_qty;
                    basis = next_basis;
                }
                Side::Sell => {
                    if qty == Decimal::ZERO || fill.qty.get() > qty {
                        valid = false;
                        break;
                    }
                    let removed = if fill.qty.get() == qty {
                        basis
                    } else {
                        let Some(removed) = fill
                            .qty
                            .get()
                            .checked_div(qty)
                            .and_then(|fraction| basis.checked_mul(fraction))
                        else {
                            valid = false;
                            break;
                        };
                        removed
                    };
                    let Some(realized) = fill.notional.get().checked_sub(removed) else {
                        valid = false;
                        break;
                    };
                    let Some(next_realized) = sequential_realized.checked_add(realized) else {
                        valid = false;
                        break;
                    };
                    let Some(next_sell) = sell_notional.checked_add(fill.notional.get()) else {
                        valid = false;
                        break;
                    };
                    let Some(next_removed) = removed_basis_total.checked_add(removed) else {
                        valid = false;
                        break;
                    };
                    let Some(next_qty) = qty.checked_sub(fill.qty.get()) else {
                        valid = false;
                        break;
                    };
                    let Some(next_basis) = basis.checked_sub(removed) else {
                        valid = false;
                        break;
                    };
                    sequential_realized = next_realized;
                    sell_notional = next_sell;
                    removed_basis_total = next_removed;
                    qty = next_qty;
                    basis = if qty == Decimal::ZERO {
                        Decimal::ZERO
                    } else {
                        next_basis
                    };
                }
            }
        }
        let conserved_realized = sell_notional.checked_sub(removed_basis_total);
        if !valid
            || episode.residual_qty.get() != qty
            || episode.residual_basis.get() != basis
            || episode.fees.get() != fees
            || !within_numeric_tolerance(episode.realized_price_pnl.get(), sequential_realized)
            || conserved_realized.is_none_or(|conserved| {
                !within_numeric_tolerance(episode.realized_price_pnl.get(), conserved)
            })
        {
            self.finding(format!(
                "{} episode {} fill/basis/PnL conservation mismatch",
                model.model_id, episode.episode_id
            ));
        }
    }

    fn verify_execution_close_marks(
        &mut self,
        model: &ModelLedger,
        observations: &BTreeMap<&str, &crate::contracts::CandleObservation>,
    ) {
        let range = self.bundle.plan.spec.range;
        let mut required: BTreeMap<
            (
                String,
                crate::contracts::UtcTimestamp,
                crate::contracts::UtcTimestamp,
            ),
            BTreeSet<&str>,
        > = BTreeMap::new();
        for observation in observations.values().copied().filter(|observation| {
            observation.candle.market == model.market.code()
                && observation.candle.interval == self.bundle.plan.spec.execution_resolution
                && observation.candle.open_time_utc >= range.start()
                && observation.candle.close_time_utc <= range.end()
        }) {
            required
                .entry((
                    observation.candle.market.clone(),
                    observation.candle.open_time_utc,
                    observation.candle.close_time_utc,
                ))
                .or_default()
                .insert(observation.id.as_str());
        }
        for ((_, _, close_time), source_ids) in required {
            let covered = model.account_marks.iter().any(|mark| {
                mark.context.accounting_event_time == close_time
                    && source_ids.contains(mark.source_bar_id.as_str())
                    && matches!(
                        mark.kind,
                        crate::contracts::MarkKind::ExecutionClose
                            | crate::contracts::MarkKind::Terminal
                    )
            });
            if !covered {
                self.finding(format!(
                    "{} lacks execution-close mark at {}",
                    model.model_id, close_time
                ));
            }
        }
    }

    fn verify_signal_source(
        &mut self,
        model: &ModelLedger,
        signal: &crate::contracts::SignalRecord,
        observations: &BTreeMap<&str, &crate::contracts::CandleObservation>,
    ) {
        let decision_bar = signal
            .source_bar_ids
            .last()
            .and_then(|id| observations.get(id.as_str()));
        if decision_bar.is_none_or(|observation| {
            observation.candle.market != model.market.code()
                || observation.candle.interval != self.bundle.plan.spec.decision_interval
                || !observation.candle.completed
                || observation.candle.close_time_utc != signal.signal_time
                || observation.candle.close_time_utc != signal.decision_available_at
                || signal.context.accounting_event_time != signal.decision_available_at
        }) {
            self.finding(format!(
                "{} signal {} decision source/time mismatch",
                model.model_id, signal.signal_id
            ));
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one independent boundary cross-checks all exported fill causal references"
    )]
    fn verify_fill_causality(
        &mut self,
        model: &ModelLedger,
        fill: &FillRecord,
        orders: &BTreeMap<&str, &crate::contracts::OrderRecord>,
        signals: &BTreeMap<&str, &crate::contracts::SignalRecord>,
        observations: &BTreeMap<&str, &crate::contracts::CandleObservation>,
    ) {
        let Some(order) = orders.get(fill.order_id.as_str()).copied() else {
            return;
        };
        let Some(signal) = signals.get(order.parent_signal_id.as_str()).copied() else {
            return;
        };
        let Some(source) = observations.get(fill.source_bar_id.as_str()).copied() else {
            return;
        };
        let Some(liquidity_source) = observations
            .get(fill.liquidity_source_bar_id.as_str())
            .copied()
        else {
            self.finding(format!(
                "{} fill {} has missing liquidity source observation",
                model.model_id, fill.fill_id
            ));
            return;
        };
        let decision_bar = signal
            .source_bar_ids
            .last()
            .and_then(|id| observations.get(id.as_str()))
            .copied();
        let modeled_time = match (&fill.timing, order.order_type) {
            (
                crate::contracts::FillTiming::Exact { at },
                crate::contracts::SimulatedOrderType::Market,
            ) if (!fill.artificial_terminal_exit && *at == source.candle.open_time_utc)
                || (fill.artificial_terminal_exit && *at == source.candle.close_time_utc) =>
            {
                Some(*at)
            }
            (
                crate::contracts::FillTiming::Interval {
                    start,
                    end,
                    accounting_at,
                },
                crate::contracts::SimulatedOrderType::PassiveBuyLimit,
            ) if *start == source.candle.open_time_utc
                && *end == source.candle.close_time_utc
                && *accounting_at == *end =>
            {
                Some(*accounting_at)
            }
            _ => None,
        };
        let Some(fill_time) = modeled_time else {
            self.finding(format!(
                "{} fill {} timing does not match its execution source interval",
                model.model_id, fill.fill_id
            ));
            return;
        };
        let normal_time_valid = !fill.artificial_terminal_exit
            && match order.order_type {
                crate::contracts::SimulatedOrderType::Market => {
                    fill_time >= order.effective_at
                        && fill_time < order.expires_at
                        && fill_time < self.bundle.plan.spec.range.end()
                }
                crate::contracts::SimulatedOrderType::PassiveBuyLimit => {
                    source.candle.open_time_utc >= order.effective_at
                        && fill_time == order.expires_at
                        && source.candle.open_time_utc < self.bundle.plan.spec.range.end()
                        && fill_time <= self.bundle.plan.spec.range.end()
                }
            };
        let terminal_time_valid = fill.artificial_terminal_exit
            && self.bundle.plan.spec.terminal_policy
                == crate::contracts::TerminalPolicy::LiquidateScenario
            && fill_time >= order.effective_at
            && fill_time <= order.expires_at
            && fill_time == self.bundle.plan.spec.range.end();
        let source_valid = source.candle.market == model.market.code()
            && source.candle.interval == self.bundle.plan.spec.execution_resolution
            && source.candle.completed
            && (fill.artificial_terminal_exit
                || !signal.source_bar_ids.contains(&fill.source_bar_id))
            && fill.bar_open_proxy == source.candle.open
            && fill.context.accounting_event_time == fill_time;
        let decision_valid = decision_bar.is_some_and(|observation| {
            fill.decision_reference == observation.candle.close
                && observation.candle.close_time_utc == signal.decision_available_at
        });
        let liquidity_valid = liquidity_source.candle.market == model.market.code()
            && liquidity_source.candle.interval == self.bundle.plan.spec.execution_resolution
            && liquidity_source.candle.completed
            && liquidity_source.candle.close_time_utc == fill.liquidity_source_close_time
            && (fill.artificial_terminal_exit
                || fill.liquidity_source_close_time <= source.candle.open_time_utc);
        let order_valid = fill.side == order.side
            && order.episode_id.as_ref() == Some(&fill.episode_id)
            && fill.qty <= order.requested_qty;
        let transition_time_valid = model.order_events.iter().any(|event| {
            event.order_id == fill.order_id
                && event.context.accounting_event_time == fill_time
                && event.cumulative_filled_qty >= fill.qty
                && matches!(
                    event.status,
                    crate::contracts::OrderStatus::PartiallyFilled
                        | crate::contracts::OrderStatus::Filled
                )
        });
        if !source_valid
            || !decision_valid
            || !liquidity_valid
            || !order_valid
            || !transition_time_valid
            || !(normal_time_valid || terminal_time_valid)
        {
            self.finding(format!(
                "{} fill {} causal source/order/reference mismatch",
                model.model_id, fill.fill_id
            ));
        }
        if order.order_type == crate::contracts::SimulatedOrderType::PassiveBuyLimit {
            self.verify_passive_fill(model, fill, order, source, liquidity_source, observations);
        }
        if model.signals.iter().any(|candidate| {
            candidate.signal_id != signal.signal_id
                && candidate.context.accounting_event_time == fill_time
                && candidate.context.event_seq < fill.context.event_seq
        }) {
            self.finding(format!(
                "{} fill {} does not precede a same-time later decision",
                model.model_id, fill.fill_id
            ));
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "passive proof keeps eligibility, sizing and event order in one auditable boundary"
    )]
    fn verify_passive_fill(
        &mut self,
        model: &ModelLedger,
        fill: &FillRecord,
        order: &crate::contracts::OrderRecord,
        source: &crate::contracts::CandleObservation,
        liquidity_source: &crate::contracts::CandleObservation,
        observations: &BTreeMap<&str, &crate::contracts::CandleObservation>,
    ) {
        let crate::contracts::ExecutionPolicy::PassiveBuy {
            offset_bps,
            penetration_ticks,
            fill_fraction,
            ttl_execution_bars,
            participation_cap,
            ..
        } = &self.bundle.plan.spec.execution
        else {
            self.finding(format!(
                "{} passive fill {} was produced under a non-passive policy",
                model.model_id, fill.fill_id
            ));
            return;
        };
        let Some(limit) = order.requested_price else {
            self.finding(format!(
                "{} passive fill {} has no requested limit",
                model.model_id, fill.fill_id
            ));
            return;
        };
        let tick = self
            .bundle
            .plan
            .spec
            .market_rules
            .ticks
            .iter()
            .rev()
            .find(|band| limit.get() >= band.lower_bound.get())
            .map(|band| band.tick.get());
        let expected_limit = offset_bps
            .get()
            .checked_div(Decimal::from(10_000_u32))
            .and_then(|offset| Decimal::ONE.checked_sub(offset))
            .and_then(|multiplier| fill.decision_reference.get().checked_mul(multiplier))
            .and_then(|unrounded| {
                self.bundle
                    .plan
                    .spec
                    .market_rules
                    .ticks
                    .iter()
                    .rev()
                    .find(|band| unrounded >= band.lower_bound.get())
                    .and_then(|band| floor_decimal_step(unrounded, band.tick.get()))
            });
        let penetration = tick
            .and_then(|tick| tick.checked_mul(Decimal::from(*penetration_ticks)))
            .and_then(|amount| limit.get().checked_sub(amount));
        let price_path_valid = penetration.is_some_and(|threshold| {
            expected_limit == Some(limit.get())
                && source.candle.open > limit
                && source.candle.low.get() < threshold
                && fill.price == limit
        });
        let first_eligible = !observations.values().copied().any(|candidate| {
            candidate.candle.market == model.market.code()
                && candidate.candle.interval == self.bundle.plan.spec.execution_resolution
                && candidate.candle.completed
                && candidate.candle.open_time_utc >= order.effective_at
                && candidate.candle.open_time_utc < source.candle.open_time_utc
                && candidate.candle.close_time_utc <= order.expires_at
        });
        let step = self.bundle.plan.spec.market_rules.quantity_step.get();
        let fee_rate = self
            .bundle
            .plan
            .spec
            .costs
            .maker_fee_bps
            .get()
            .checked_div(Decimal::from(10_000_u32));
        let fraction_cap = order
            .requested_qty
            .get()
            .checked_mul((*fill_fraction).decimal());
        let volume_cap = liquidity_source
            .candle
            .volume
            .get()
            .checked_mul((*participation_cap).get());
        let reservation_cap = fee_rate
            .and_then(|rate| Decimal::ONE.checked_add(rate))
            .and_then(|multiplier| limit.get().checked_mul(multiplier))
            .and_then(|per_unit| order.reserved_cash.get().checked_div(per_unit));
        let expected_qty = fraction_cap.zip(volume_cap).zip(reservation_cap).and_then(
            |((fraction, volume), reservation)| {
                floor_decimal_step(fraction.min(volume).min(reservation), step)
            },
        );
        let size_valid = expected_qty == Some(fill.qty.get())
            && fill.qty.get() > Decimal::ZERO
            && model
                .fills
                .iter()
                .filter(|candidate| candidate.order_id == fill.order_id)
                .count()
                == 1;
        let after_fill = model.account_marks.iter().find(|mark| {
            mark.context.event_seq == fill.accounting_mark_seq
                && mark.kind == crate::contracts::MarkKind::AfterFill
        });
        let cancellation = model.order_events.iter().find(|event| {
            event.order_id == fill.order_id
                && event.status == crate::contracts::OrderStatus::Cancelled
        });
        let expected_close_seq = cancellation
            .map(|cancelled| cancelled.context.event_seq.saturating_add(1))
            .or_else(|| after_fill.map(|mark| mark.context.event_seq.saturating_add(1)));
        let execution_close = expected_close_seq.and_then(|expected| {
            model.account_marks.iter().find(|mark| {
                mark.kind == crate::contracts::MarkKind::ExecutionClose
                    && mark.source_bar_id == fill.source_bar_id
                    && mark.context.accounting_event_time == fill.context.accounting_event_time
                    && mark.context.event_seq == expected
            })
        });
        let remainder = order
            .requested_qty
            .get()
            .checked_sub(fill.qty.get())
            .unwrap_or(Decimal::ZERO);
        let partial_order_valid = if remainder > Decimal::ZERO {
            model.order_events.iter().any(|event| {
                event.order_id == fill.order_id
                    && event.status == crate::contracts::OrderStatus::PartiallyFilled
                    && event.cumulative_filled_qty == fill.qty
                    && event.context.event_seq < fill.context.event_seq
                    && event.context.accounting_event_time == fill.context.accounting_event_time
            }) && after_fill.is_some_and(|mark| {
                mark.context.event_seq == fill.context.event_seq.saturating_add(1)
            }) && cancellation.is_some_and(|cancelled| {
                cancelled.context.event_seq == fill.accounting_mark_seq.saturating_add(1)
                    && cancelled.context.accounting_event_time == fill.context.accounting_event_time
            }) && execution_close.is_some()
        } else {
            cancellation.is_none()
                && after_fill.is_some_and(|mark| {
                    mark.context.event_seq == fill.context.event_seq.saturating_add(1)
                })
                && execution_close.is_some()
        };
        let decision_order_valid = execution_close.is_none_or(|close_mark| {
            model.signals.iter().all(|signal| {
                signal.context.accounting_event_time != fill.context.accounting_event_time
                    || signal.context.event_seq > close_mark.context.event_seq
            })
        });
        if *ttl_execution_bars != 1
            || fill.side != Side::Buy
            || fill.fee_bps != self.bundle.plan.spec.costs.maker_fee_bps
            || !price_path_valid
            || !first_eligible
            || !size_valid
            || !partial_order_valid
            || !decision_order_valid
        {
            self.finding(format!(
                "{} passive fill {} eligibility/size/lifecycle mismatch",
                model.model_id, fill.fill_id
            ));
        }
    }

    fn verify_order_history(
        &mut self,
        model: &ModelLedger,
        signals: &BTreeMap<&str, &crate::contracts::SignalRecord>,
        orders: &BTreeMap<&str, &crate::contracts::OrderRecord>,
        episodes: &BTreeMap<&str, &crate::contracts::EpisodeRecord>,
        global_event_sequences: &mut BTreeSet<u64>,
    ) {
        if model
            .order_events
            .windows(2)
            .any(|pair| pair[0].context.event_seq >= pair[1].context.event_seq)
        {
            self.finding(format!(
                "{} order_events are not append-only event-sequence order",
                model.model_id
            ));
        }
        let mut histories: BTreeMap<&str, Vec<&crate::contracts::OrderRecord>> = BTreeMap::new();
        for event in &model.order_events {
            self.verify_context(model, &event.context, global_event_sequences);
            histories
                .entry(event.order_id.as_str())
                .or_default()
                .push(event);
            if !orders.contains_key(event.order_id.as_str()) {
                self.finding(format!(
                    "{} order event {} has no final projection",
                    model.model_id, event.order_id
                ));
            }
            if !valid_order_status_quantity(event) {
                self.finding(format!(
                    "{} order event {} has invalid status quantity",
                    model.model_id, event.order_id
                ));
            }
            if !signals.contains_key(event.parent_signal_id.as_str()) {
                self.finding(format!(
                    "{} order event {} has missing parent signal",
                    model.model_id, event.order_id
                ));
            }
            if event
                .episode_id
                .as_ref()
                .is_some_and(|id| !episodes.contains_key(id.as_str()))
            {
                self.finding(format!(
                    "{} order event {} has a missing episode link",
                    model.model_id, event.order_id
                ));
            }
        }
        for order in &model.orders {
            let Some(history) = histories.get(order.order_id.as_str()) else {
                self.finding(format!(
                    "{} order {} has no lifecycle events",
                    model.model_id, order.order_id
                ));
                continue;
            };
            self.verify_one_order_history(
                model,
                order,
                history,
                signals.get(order.parent_signal_id.as_str()).copied(),
            );
        }
    }

    fn verify_one_order_history(
        &mut self,
        model: &ModelLedger,
        projection: &crate::contracts::OrderRecord,
        history: &[&crate::contracts::OrderRecord],
        parent_signal: Option<&crate::contracts::SignalRecord>,
    ) {
        let Some(first) = history.first() else {
            return;
        };
        let Some(last) = history.last() else {
            return;
        };
        if first.status != crate::contracts::OrderStatus::Created
            || first.cumulative_filled_qty.get() != Decimal::ZERO
        {
            self.finding(format!(
                "{} order {} lifecycle does not start at CREATED with zero fills",
                model.model_id, projection.order_id
            ));
        }
        let expected_effective = i64::try_from(self.bundle.plan.spec.latency_ms)
            .ok()
            .and_then(|latency_ms| {
                first
                    .created_at
                    .0
                    .checked_add_signed(chrono::Duration::milliseconds(latency_ms))
                    .map(crate::contracts::UtcTimestamp)
            });
        if first.context.accounting_event_time != first.created_at
            || expected_effective != Some(first.effective_at)
            || parent_signal.is_none_or(|signal| {
                first.created_at != signal.decision_available_at
                    || first.context.event_seq <= signal.context.event_seq
            })
        {
            self.finding(format!(
                "{} order {} creation/latency does not follow its parent decision",
                model.model_id, projection.order_id
            ));
        }
        for pair in history.windows(2) {
            let before = pair[0];
            let after = pair[1];
            if !legal_order_transition(before.status, after.status)
                || after.cumulative_filled_qty < before.cumulative_filled_qty
                || (before.status == crate::contracts::OrderStatus::PartiallyFilled
                    && after.status == crate::contracts::OrderStatus::PartiallyFilled
                    && after.cumulative_filled_qty == before.cumulative_filled_qty)
                || after.cumulative_filled_qty > after.requested_qty
                || !same_order_identity(before, after)
                || (before.episode_id.is_some() && before.episode_id != after.episode_id)
            {
                self.finding(format!(
                    "{} order {} has an illegal or inconsistent lifecycle transition",
                    model.model_id, projection.order_id
                ));
            }
        }
        match (
            ContentHash::of_value(projection),
            ContentHash::of_value(*last),
        ) {
            (Ok(projected), Ok(final_event)) if projected == final_event => {}
            _ => {
                self.finding(format!(
                    "{} order {} final projection does not match its last lifecycle event",
                    model.model_id, projection.order_id
                ));
            }
        }
    }

    fn verify_context(
        &mut self,
        model: &ModelLedger,
        context: &crate::contracts::EventContext,
        sequences: &mut BTreeSet<u64>,
    ) {
        self.verify_context_identity(model, context);
        if context.event_seq == 0
            || context.event_seq > model.last_event_seq
            || !sequences.insert(context.event_seq)
        {
            self.finding(format!(
                "{} event sequence {} is invalid or duplicated",
                model.model_id, context.event_seq
            ));
        }
    }

    fn verify_context_identity(
        &mut self,
        model: &ModelLedger,
        context: &crate::contracts::EventContext,
    ) {
        if context.run_id != self.bundle.manifest.run_id
            || context.model_id != model.model_id
            || context.market != model.market
        {
            self.finding(format!(
                "{} event {} context mismatch",
                model.model_id, context.event_seq
            ));
        }
    }

    fn verify_accounting(&mut self, model: &ModelLedger) {
        let initial_cash = self.bundle.plan.spec.initial_cash.get();
        let mut state = ExactState::new(initial_cash);
        let mut events = Vec::with_capacity(
            model
                .fills
                .len()
                .saturating_add(model.order_events.len())
                .saturating_add(model.account_marks.len()),
        );
        events.extend(
            model
                .order_events
                .iter()
                .map(|order| (order.context.event_seq, LedgerEvent::Order(order))),
        );
        events.extend(
            model
                .fills
                .iter()
                .map(|fill| (fill.context.event_seq, LedgerEvent::Fill(fill))),
        );
        events.extend(
            model
                .account_marks
                .iter()
                .map(|mark| (mark.context.event_seq, LedgerEvent::Mark(mark))),
        );
        events.sort_by_key(|(sequence, _)| *sequence);
        for (_, event) in events {
            match event {
                LedgerEvent::Order(order) => {
                    if let Err(error) = state.apply_order_event(order) {
                        self.finding(format!(
                            "{} order {} reservation failure: {error}",
                            model.model_id, order.order_id
                        ));
                    }
                }
                LedgerEvent::Fill(fill) => {
                    if let Err(error) = state.apply(fill) {
                        self.finding(format!(
                            "{} fill {} accounting failure: {error}",
                            model.model_id, fill.fill_id
                        ));
                    }
                }
                LedgerEvent::Mark(mark) => self.verify_mark(model, mark, &mut state),
            }
        }
        for fill in &model.fills {
            if !model.account_marks.iter().any(|mark| {
                mark.context.event_seq == fill.accounting_mark_seq
                    && mark.kind == crate::contracts::MarkKind::AfterFill
            }) {
                self.finding(format!(
                    "{} fill {} lacks its committed after-fill mark",
                    model.model_id, fill.fill_id
                ));
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one mark boundary compares every exact account identity component"
    )]
    fn verify_mark(&mut self, model: &ModelLedger, mark: &AccountMark, state: &mut ExactState) {
        let Some(position) = state.qty.checked_mul(mark.mark_price.get()) else {
            self.finding(format!(
                "{} mark {} position overflow",
                model.model_id, mark.context.event_seq
            ));
            return;
        };
        let Some(unrealized) = position.checked_sub(state.basis) else {
            self.finding(format!(
                "{} mark {} unrealized PnL overflow",
                model.model_id, mark.context.event_seq
            ));
            return;
        };
        let Some(equity) = state.cash.checked_add(position) else {
            self.finding(format!(
                "{} mark {} equity overflow",
                model.model_id, mark.context.event_seq
            ));
            return;
        };
        state.peak = state.peak.max(equity);
        let drawdown = if state.peak == Decimal::ZERO {
            Decimal::ZERO
        } else {
            let Some(value) = state
                .peak
                .checked_sub(equity)
                .and_then(|value| value.checked_div(state.peak))
            else {
                self.finding(format!(
                    "{} mark {} drawdown overflow",
                    model.model_id, mark.context.event_seq
                ));
                return;
            };
            value
        };
        let Some(identity) = self
            .bundle
            .plan
            .spec
            .initial_cash
            .get()
            .checked_add(state.gross_realized)
            .and_then(|value| value.checked_add(unrealized))
            .and_then(|value| value.checked_sub(state.fees))
        else {
            self.finding(format!(
                "{} mark {} accounting identity overflow",
                model.model_id, mark.context.event_seq
            ));
            return;
        };
        let cash_partition = mark
            .state
            .cash_free
            .get()
            .checked_add(mark.state.cash_reserved.get());
        let expected_reserved = state.reserved_cash();
        let expected_free = expected_reserved.and_then(|reserved| state.cash.checked_sub(reserved));
        let checks = [
            (mark.state.cash_total.get() == state.cash, "cash_total"),
            (
                cash_partition == Some(mark.state.cash_total.get()),
                "cash free/reserved partition",
            ),
            (
                expected_reserved == Some(mark.state.cash_reserved.get()),
                "cash reservation lifecycle",
            ),
            (
                expected_free == Some(mark.state.cash_free.get()),
                "free cash after reservations",
            ),
            (mark.state.qty.get() == state.qty, "quantity"),
            (mark.state.price_basis.get() == state.basis, "price basis"),
            (
                mark.state.gross_realized.get() == state.gross_realized,
                "gross realized",
            ),
            (
                mark.state.cumulative_fees.get() == state.fees,
                "cumulative fees",
            ),
            (mark.position_value.get() == position, "position value"),
            (
                mark.gross_unrealized.get() == unrealized,
                "gross unrealized",
            ),
            (mark.equity.get() == equity, "equity"),
            (
                within_numeric_tolerance(mark.equity.get(), identity),
                "equity identity / fee debit exactly once",
            ),
            (mark.peak_equity.get() == state.peak, "peak equity"),
            (mark.drawdown.get() == drawdown, "sampled drawdown"),
        ];
        for (passed, label) in checks {
            if !passed {
                self.finding(format!(
                    "{} mark {} mismatched {label}",
                    model.model_id, mark.context.event_seq
                ));
            }
        }
    }

    fn finish(self) -> ValidationReport {
        ValidationReport {
            check_id: "F08_INDEPENDENT_LEDGER_V1".into(),
            status: if self.findings.is_empty() {
                ValidationStatus::Pass
            } else {
                ValidationStatus::Fail
            },
            run_id: Some(self.bundle.manifest.run_id.clone()),
            input_digest: self.bundle.plan.input_digest.clone(),
            checked_models: u64::try_from(self.bundle.models.len()).unwrap_or(u64::MAX),
            checked_fills: self.checked_fills,
            checked_marks: self.checked_marks,
            findings: self.findings,
        }
    }
}

fn checked_sum(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    values
        .into_iter()
        .try_fold(Decimal::ZERO, Decimal::checked_add)
}

fn floor_decimal_step(value: Decimal, step: Decimal) -> Option<Decimal> {
    if step <= Decimal::ZERO {
        return None;
    }
    value.checked_div(step)?.floor().checked_mul(step)
}

fn within_numeric_tolerance(left: Decimal, right: Decimal) -> bool {
    left.checked_sub(right)
        .is_some_and(|residual| residual.abs() <= crate::contracts::NUMERIC_TOLERANCE)
}

fn legal_order_transition(
    before: crate::contracts::OrderStatus,
    after: crate::contracts::OrderStatus,
) -> bool {
    use crate::contracts::OrderStatus::{
        Cancelled, Created, Expired, Filled, PartiallyFilled, Rejected,
    };
    matches!(
        (before, after),
        (
            Created,
            PartiallyFilled | Filled | Cancelled | Rejected | Expired
        ) | (
            PartiallyFilled,
            PartiallyFilled | Filled | Cancelled | Expired
        )
    )
}

fn same_order_identity(
    before: &crate::contracts::OrderRecord,
    after: &crate::contracts::OrderRecord,
) -> bool {
    before.order_id == after.order_id
        && before.parent_signal_id == after.parent_signal_id
        && before.side == after.side
        && before.order_type == after.order_type
        && before.requested_price == after.requested_price
        && before.requested_qty == after.requested_qty
        && before.created_at == after.created_at
        && before.effective_at == after.effective_at
        && before.expires_at == after.expires_at
        && before.rule_snapshot_id == after.rule_snapshot_id
        && before.policy_version == after.policy_version
        && before.order_origin == after.order_origin
}

fn valid_order_status_quantity(order: &crate::contracts::OrderRecord) -> bool {
    use crate::contracts::OrderStatus::{
        Cancelled, Created, Expired, Filled, PartiallyFilled, Rejected,
    };
    let cumulative = order.cumulative_filled_qty.get();
    let requested = order.requested_qty.get();
    match order.status {
        Created | Rejected => cumulative == Decimal::ZERO,
        PartiallyFilled => cumulative > Decimal::ZERO && cumulative < requested,
        Filled => cumulative == requested,
        Cancelled | Expired => cumulative <= requested,
    }
}

fn admission_status_matches(
    admission: crate::contracts::AdmissionStatus,
    model: crate::contracts::ModelStatus,
) -> bool {
    use crate::contracts::{AdmissionStatus, ModelStatus};
    match admission {
        AdmissionStatus::Eligible => matches!(
            model,
            ModelStatus::Completed | ModelStatus::Failed | ModelStatus::Skipped
        ),
        AdmissionStatus::BlockedEvidence => model == ModelStatus::BlockedEvidence,
        AdmissionStatus::BlockedData => model == ModelStatus::BlockedData,
    }
}

fn exact_named_values(values: &[crate::contracts::NamedPolicyValue], expected: &[&str]) -> bool {
    values.len() == expected.len()
        && values
            .windows(2)
            .all(|pair| pair[0].id.as_str() < pair[1].id.as_str())
        && values.iter().all(|value| {
            value.value.is_finite() && expected.iter().any(|expected_id| value.id == *expected_id)
        })
}

fn builtin_has_latent_state(strategy: &crate::contracts::StrategySpec) -> bool {
    matches!(
        strategy,
        crate::contracts::StrategySpec::S1 { .. }
            | crate::contracts::StrategySpec::S4 { .. }
            | crate::contracts::StrategySpec::S5 { .. }
            | crate::contracts::StrategySpec::S1CoverageControl { .. }
    )
}

#[allow(
    clippy::float_cmp,
    reason = "builtin signal state is an exact finite 0/1 enum encoding, not a measured statistic"
)]
fn is_builtin_state_value(value: f64) -> bool {
    value == 0.0 || value == 1.0
}

#[allow(
    clippy::float_cmp,
    reason = "state continuity requires byte-stable deterministic replay values, not tolerance"
)]
fn same_named_values(
    left: &[crate::contracts::NamedPolicyValue],
    right: &[crate::contracts::NamedPolicyValue],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.id == right.id && left.value == right.value)
}

enum LedgerEvent<'a> {
    Order(&'a crate::contracts::OrderRecord),
    Fill(&'a FillRecord),
    Mark(&'a AccountMark),
}

struct ExactState {
    cash: Decimal,
    qty: Decimal,
    basis: Decimal,
    gross_realized: Decimal,
    fees: Decimal,
    peak: Decimal,
    reservations: BTreeMap<String, Decimal>,
}

impl ExactState {
    fn new(initial_cash: Decimal) -> Self {
        Self {
            cash: initial_cash,
            qty: Decimal::ZERO,
            basis: Decimal::ZERO,
            gross_realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            peak: initial_cash,
            reservations: BTreeMap::new(),
        }
    }

    fn apply_order_event(
        &mut self,
        order: &crate::contracts::OrderRecord,
    ) -> Result<(), &'static str> {
        use crate::contracts::OrderStatus::{
            Cancelled, Created, Expired, Filled, PartiallyFilled, Rejected,
        };
        match order.status {
            Created => {
                if self
                    .reservations
                    .insert(
                        order.order_id.as_str().to_owned(),
                        order.reserved_cash.get(),
                    )
                    .is_some()
                {
                    return Err("duplicate CREATED reservation");
                }
            }
            PartiallyFilled => {
                if !self.reservations.contains_key(order.order_id.as_str()) {
                    return Err("partial fill without reservation");
                }
            }
            Filled | Cancelled | Rejected | Expired => {
                if self.reservations.remove(order.order_id.as_str()).is_none() {
                    return Err("terminal order without reservation");
                }
            }
        }
        Ok(())
    }

    fn reserved_cash(&self) -> Option<Decimal> {
        checked_sum(self.reservations.values().copied())
    }

    fn apply(&mut self, fill: &FillRecord) -> Result<(), &'static str> {
        match fill.side {
            Side::Buy => {
                let debit = fill
                    .notional
                    .get()
                    .checked_add(fill.fee.get())
                    .ok_or("buy debit overflow")?;
                self.consume_buy_reservation(fill.order_id.as_str(), debit)?;
                self.cash = self.cash.checked_sub(debit).ok_or("buy cash overflow")?;
                self.qty = self
                    .qty
                    .checked_add(fill.qty.get())
                    .ok_or("buy quantity overflow")?;
                self.basis = self
                    .basis
                    .checked_add(fill.notional.get())
                    .ok_or("buy basis overflow")?;
            }
            Side::Sell => {
                if fill.qty.get() > self.qty || self.qty == Decimal::ZERO {
                    return Err("sell exceeds position");
                }
                let removed_basis = fill
                    .qty
                    .get()
                    .checked_div(self.qty)
                    .and_then(|fraction| self.basis.checked_mul(fraction))
                    .ok_or("basis removal overflow")?;
                let credit = fill
                    .notional
                    .get()
                    .checked_sub(fill.fee.get())
                    .ok_or("sell credit overflow")?;
                self.cash = self.cash.checked_add(credit).ok_or("sell cash overflow")?;
                self.qty = self
                    .qty
                    .checked_sub(fill.qty.get())
                    .ok_or("sell quantity overflow")?;
                self.basis = self
                    .basis
                    .checked_sub(removed_basis)
                    .ok_or("sell basis overflow")?;
                self.gross_realized = self
                    .gross_realized
                    .checked_add(
                        fill.notional
                            .get()
                            .checked_sub(removed_basis)
                            .ok_or("realized PnL overflow")?,
                    )
                    .ok_or("realized PnL overflow")?;
                if self.qty == Decimal::ZERO {
                    self.basis = Decimal::ZERO;
                }
            }
        }
        self.fees = self
            .fees
            .checked_add(fill.fee.get())
            .ok_or("fee overflow")?;
        if self.cash < Decimal::ZERO || self.qty < Decimal::ZERO || self.basis < Decimal::ZERO {
            return Err("negative cash, quantity, or basis");
        }
        Ok(())
    }

    fn consume_buy_reservation(
        &mut self,
        order_id: &str,
        debit: Decimal,
    ) -> Result<(), &'static str> {
        if let Some(reserved) = self.reservations.get_mut(order_id) {
            if debit > *reserved {
                return Err("partial buy debit exceeds remaining reservation");
            }
            *reserved = reserved
                .checked_sub(debit)
                .ok_or("partial reservation consumption overflow")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod reservation_tests {
    use super::{ExactState, within_numeric_tolerance};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn partial_buy_consumes_exact_debit_before_cancellation_releases_remainder() {
        let mut state = ExactState::new(Decimal::from(10_000_000_u64));
        state.reservations.insert(
            "actual-g1-order".into(),
            Decimal::from_str("9999999.008233740000").expect("reservation"),
        );
        state
            .consume_buy_reservation(
                "actual-g1-order",
                Decimal::from_str("9999998.434857195").expect("fill debit"),
            )
            .expect("consume partial reservation");
        assert_eq!(
            state.reservations["actual-g1-order"],
            Decimal::from_str("0.573376545").expect("remainder")
        );
        assert_eq!(
            state
                .reservations
                .remove("actual-g1-order")
                .expect("cancelled remainder"),
            Decimal::from_str("0.573376545").expect("remainder")
        );
        assert!(state.reservations.is_empty());
    }

    #[test]
    fn aggregate_tolerance_accepts_boundary_and_rejects_larger_corruption() {
        let zero = Decimal::ZERO;
        let boundary = crate::contracts::NUMERIC_TOLERANCE;
        assert!(within_numeric_tolerance(zero, boundary));
        assert!(within_numeric_tolerance(
            zero,
            Decimal::from_str("0.0000000000000000000005").expect("measured residual")
        ));
        assert!(!within_numeric_tolerance(
            zero,
            Decimal::from_str("0.00000002").expect("corrupt residual")
        ));
    }
}
