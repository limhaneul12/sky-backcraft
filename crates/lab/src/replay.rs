//! Offline engine replay for verified portable export packages.
//!
//! Replay deliberately uses the engine and is separate from reporting's
//! engine-independent accounting verification.

use crate::{
    contracts::{
        ContentHash, ENGINE_VERSION, INDICATOR_VERSION, LabError, ModelLedger, ROUNDING_VERSION,
        RunBundle, SCHEMA_VERSION, ValidationReport, ValidationStatus, experiment_config_digest,
    },
    reporting::{ReadExportResult, semantic_digest},
};
use std::collections::BTreeSet;

const REPLAY_CHECK_ID: &str = "replay-report-v2";

/// Replay the complete frozen plan in original admission order and compare the
/// qualified package only after global event sequences have been reproduced.
///
/// # Errors
///
/// Returns an explicit incompatibility error for different compiled source,
/// lockfile, toolchain or algorithm versions. Engine/input/cancellation errors
/// are preserved. A deterministic result mismatch is returned as a `FAIL`
/// [`ValidationReport`].
pub fn replay_package(package: &ReadExportResult) -> Result<ValidationReport, LabError> {
    replay_package_with_cancel(package, &|| false)
}

/// Cancellation-aware form of [`replay_package`]. Cancellation is observed
/// between models and at the deterministic checkpoints inside each model.
///
/// # Errors
///
/// Has the same failure contract as [`replay_package`] and additionally returns
/// [`LabError::Cancelled`] when `cancelled` becomes true.
pub fn replay_package_with_cancel(
    package: &ReadExportResult,
    cancelled: &dyn Fn() -> bool,
) -> Result<ValidationReport, LabError> {
    validate_compiled_provenance(&package.bundle)?;
    validate_frozen_replay_inputs(package)?;
    let replayed_models = replay_models(&package.bundle, cancelled)?;
    let mut full_replay = RunBundle {
        manifest: package.bundle.manifest.clone(),
        plan: package.bundle.plan.clone(),
        datasets: package.bundle.datasets.clone(),
        evidence: package.bundle.evidence.clone(),
        models: replayed_models,
        semantic_digest: package.bundle.semantic_digest.clone(),
    };
    full_replay.semantic_digest = semantic_digest(&full_replay)?;

    let mut findings = Vec::new();
    if full_replay.semantic_digest != package.manifest.source_run_semantic_digest {
        findings.push("complete replay semantic digest differs from source run".to_owned());
    }

    let included: BTreeSet<&str> = package
        .manifest
        .included_model_ids
        .iter()
        .map(crate::contracts::ModelId::as_str)
        .collect();
    let mut qualified_replay = full_replay;
    qualified_replay
        .models
        .retain(|model| included.contains(model.model_id.as_str()));
    qualified_replay.semantic_digest = semantic_digest(&qualified_replay)?;

    if qualified_replay.semantic_digest != package.manifest.semantic_digest
        || qualified_replay.semantic_digest != package.bundle.semantic_digest
    {
        findings.push("qualified replay semantic digest differs from package".to_owned());
    }
    if ContentHash::of_value(&qualified_replay.models)?
        != ContentHash::of_value(&package.bundle.models)?
    {
        findings.push("replayed event identities or ordered model facts differ".to_owned());
    }

    let checked_models = usize_to_u64(qualified_replay.models.len(), "model count")?;
    let checked_fills = count_records(&qualified_replay.models, |model| model.fills.len(), "fill")?;
    let checked_marks = count_records(
        &qualified_replay.models,
        |model| model.account_marks.len(),
        "mark",
    )?;
    Ok(ValidationReport {
        check_id: REPLAY_CHECK_ID.to_owned(),
        status: if findings.is_empty() {
            ValidationStatus::Pass
        } else {
            ValidationStatus::Fail
        },
        run_id: Some(package.bundle.manifest.run_id.clone()),
        input_digest: package.manifest.input_digest.clone(),
        checked_models,
        checked_fills,
        checked_marks,
        findings,
    })
}

fn validate_frozen_replay_inputs(package: &ReadExportResult) -> Result<(), LabError> {
    let plan = &package.bundle.plan;
    let config_digest = experiment_config_digest(&plan.spec, &plan.policy_revisions)?;
    let policy_revisions: Vec<_> = plan
        .policy_revisions
        .iter()
        .map(|revision| revision.reference.clone())
        .collect();
    if config_digest != plan.config_digest || policy_revisions != package.manifest.policy_revisions
    {
        return Err(LabError::InputHashMismatch(
            "replay policy definitions or manifest lineage differ from the frozen plan".into(),
        ));
    }
    Ok(())
}

fn replay_models(
    source: &RunBundle,
    cancelled: &dyn Fn() -> bool,
) -> Result<Vec<ModelLedger>, LabError> {
    let mut models = Vec::with_capacity(source.plan.admissions.len());
    let mut next_sequence = 1_u64;
    for admission in &source.plan.admissions {
        check_cancelled(cancelled)?;
        let model = crate::engine::run_model(
            &source.plan,
            &source.datasets,
            source.evidence.as_ref(),
            &source.manifest.run_id,
            admission,
            next_sequence,
            cancelled,
        )?;
        next_sequence = model
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("replay sequence overflow".to_owned()))?;
        models.push(model);
    }
    Ok(models)
}

fn validate_compiled_provenance(source: &RunBundle) -> Result<(), LabError> {
    let compiled_source = ContentHash::try_from(env!("SPOT_LAB_SOURCE_SHA256").to_owned())?;
    let compiled_lock = ContentHash::try_from(env!("SPOT_LAB_LOCK_SHA256").to_owned())?;
    let manifest = &source.manifest;
    let mut incompatible = Vec::new();
    if manifest.source_digest != compiled_source {
        incompatible.push("source_digest");
    }
    if manifest.lockfile_digest != compiled_lock {
        incompatible.push("lockfile_digest");
    }
    if manifest.toolchain != env!("SPOT_LAB_TOOLCHAIN") {
        incompatible.push("toolchain");
    }
    if manifest.schema_version != SCHEMA_VERSION {
        incompatible.push("schema_version");
    }
    if manifest.engine_version != ENGINE_VERSION {
        incompatible.push("engine_version");
    }
    if manifest.indicator_version != INDICATOR_VERSION {
        incompatible.push("indicator_version");
    }
    if manifest.rounding_version != ROUNDING_VERSION {
        incompatible.push("rounding_version");
    }
    if manifest.mode != "HISTORICAL_REPLAY"
        || manifest.historical_availability_model != "BAR_CLOSE_ASSUMED"
        || manifest.fill_observed
    {
        incompatible.push("replay_mode");
    }
    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(LabError::InputHashMismatch(format!(
            "replay incompatible with compiled provenance: {}",
            incompatible.join(", ")
        )))
    }
}

fn check_cancelled(cancelled: &dyn Fn() -> bool) -> Result<(), LabError> {
    if cancelled() {
        Err(LabError::Cancelled(
            "offline replay cancelled at deterministic checkpoint".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn count_records(
    models: &[ModelLedger],
    count: impl Fn(&ModelLedger) -> usize,
    label: &str,
) -> Result<u64, LabError> {
    models.iter().try_fold(0_u64, |total, model| {
        total
            .checked_add(usize_to_u64(count(model), label)?)
            .ok_or_else(|| LabError::ResourceLimit(format!("replay {label} count overflow")))
    })
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::ResourceLimit(format!("replay {label} exceeds u64")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_runs_full_order_before_qualified_scope_filter() -> Result<(), LabError> {
        let mut bundle = crate::reporting::tests::multi_market_fixture();
        bundle.manifest.source_digest =
            ContentHash::try_from(env!("SPOT_LAB_SOURCE_SHA256").to_owned())?;
        bundle.manifest.lockfile_digest =
            ContentHash::try_from(env!("SPOT_LAB_LOCK_SHA256").to_owned())?;
        bundle.manifest.toolchain = env!("SPOT_LAB_TOOLCHAIN").to_owned();
        bundle.manifest.engine_version = ENGINE_VERSION.to_owned();
        bundle.manifest.indicator_version = INDICATOR_VERSION.to_owned();
        bundle.manifest.rounding_version = ROUNDING_VERSION.to_owned();
        bundle.models = replay_models(&bundle, &|| false)?;
        bundle.semantic_digest = semantic_digest(&bundle)?;

        let full_manifest = test_manifest(
            &bundle,
            crate::reporting::ExportScope::Full,
            bundle.models.iter(),
            Vec::new(),
        );
        let full = ReadExportResult {
            manifest: full_manifest,
            validation: crate::reporting::verify_run(&bundle),
            bundle: bundle.clone(),
        };
        let full_report = replay_package(&full)?;
        assert_eq!(full_report.status, ValidationStatus::Pass);
        assert!(matches!(
            replay_package_with_cancel(&full, &|| true),
            Err(LabError::Cancelled(_))
        ));

        let first_model = bundle.models[0].clone();
        let included_asset = first_model.market.base.clone();
        let excluded = bundle.models[1].model_id.clone();
        let source_digest = bundle.semantic_digest.clone();
        let mut scoped_bundle = bundle.clone();
        scoped_bundle.models = vec![first_model];
        scoped_bundle.semantic_digest = semantic_digest(&scoped_bundle)?;
        let mut scoped_manifest = test_manifest(
            &scoped_bundle,
            crate::reporting::ExportScope::Asset {
                asset: included_asset,
            },
            scoped_bundle.models.iter(),
            vec![excluded],
        );
        scoped_manifest.source_run_semantic_digest = source_digest;
        let scoped = ReadExportResult {
            manifest: scoped_manifest,
            validation: crate::reporting::verify_run(&scoped_bundle),
            bundle: scoped_bundle,
        };
        let scoped_report = replay_package(&scoped)?;
        assert_eq!(scoped_report.status, ValidationStatus::Pass);
        assert_eq!(scoped_report.checked_models, 1);
        Ok(())
    }

    #[test]
    fn replay_rejects_incompatible_compiled_provenance() {
        let mut bundle = crate::reporting::tests::multi_market_fixture();
        bundle.manifest.source_digest = ContentHash::of_bytes(b"different source");
        let manifest = test_manifest(
            &bundle,
            crate::reporting::ExportScope::Full,
            bundle.models.iter(),
            Vec::new(),
        );
        let package = ReadExportResult {
            manifest,
            validation: crate::reporting::verify_run(&bundle),
            bundle,
        };
        assert!(matches!(
            replay_package(&package),
            Err(LabError::InputHashMismatch(message)) if message.contains("source_digest")
        ));
    }

    #[test]
    fn replay_uses_only_frozen_policy_definition_and_exact_manifest_lineage() -> Result<(), LabError>
    {
        let mut bundle = crate::reporting::tests::policy_fixture();
        bundle.manifest.source_digest =
            ContentHash::try_from(env!("SPOT_LAB_SOURCE_SHA256").to_owned())?;
        bundle.manifest.lockfile_digest =
            ContentHash::try_from(env!("SPOT_LAB_LOCK_SHA256").to_owned())?;
        bundle.manifest.toolchain = env!("SPOT_LAB_TOOLCHAIN").to_owned();
        bundle.manifest.engine_version = ENGINE_VERSION.to_owned();
        bundle.manifest.indicator_version = INDICATOR_VERSION.to_owned();
        bundle.manifest.rounding_version = ROUNDING_VERSION.to_owned();
        bundle.models = replay_models(&bundle, &|| false)?;
        bundle.semantic_digest = semantic_digest(&bundle)?;

        let manifest = test_manifest(
            &bundle,
            crate::reporting::ExportScope::Full,
            bundle.models.iter(),
            Vec::new(),
        );
        let package = ReadExportResult {
            validation: crate::reporting::verify_run(&bundle),
            manifest,
            bundle,
        };
        assert_eq!(replay_package(&package)?.status, ValidationStatus::Pass);

        let mut wrong_lineage = package;
        wrong_lineage.manifest.policy_revisions.clear();
        assert!(matches!(
            replay_package(&wrong_lineage),
            Err(LabError::InputHashMismatch(message)) if message.contains("manifest lineage")
        ));
        Ok(())
    }

    fn test_manifest<'model>(
        bundle: &RunBundle,
        scope: crate::reporting::ExportScope,
        included: impl IntoIterator<Item = &'model ModelLedger>,
        excluded_model_ids: Vec<crate::contracts::ModelId>,
    ) -> crate::reporting::ExportManifest {
        let included_model_ids: Vec<_> = included
            .into_iter()
            .map(|model| model.model_id.clone())
            .collect();
        crate::reporting::ExportManifest {
            raw_fill_price_cost_unit: None,
            review_price_cost_unit: None,
            schema_version: "spot-lab-export-v2".to_owned(),
            run_id: bundle.manifest.run_id.clone(),
            scope,
            input_digest: bundle.plan.input_digest.clone(),
            source_run_semantic_digest: bundle.semantic_digest.clone(),
            semantic_digest: bundle.semantic_digest.clone(),
            replay_qualification: if excluded_model_ids.is_empty() {
                crate::reporting::ReplayQualification::FullRun
            } else {
                crate::reporting::ReplayQualification::QualifiedAssetSubset
            },
            included_model_ids,
            excluded_model_ids,
            dataset_ids: bundle
                .datasets
                .iter()
                .map(|dataset| dataset.manifest.id.clone())
                .collect(),
            policy_revisions: bundle
                .plan
                .policy_revisions
                .iter()
                .map(|revision| revision.reference.clone())
                .collect(),
            evidence_snapshot_id: bundle.evidence.as_ref().map(|evidence| evidence.id.clone()),
            raw_objects_embedded: false,
            raw_object_dependencies: Vec::new(),
            artifacts: Vec::new(),
        }
    }
}
