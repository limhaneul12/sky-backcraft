//! Deterministic, bounded, atomically promoted portable exports.

use super::metrics::{ReviewPayload, build_review};
use super::verify::{semantic_digest, verify_run, verify_run_subset};
use crate::contracts::{ArtifactId, ArtifactRef, Asset, ContentHash, LabError, RunBundle};
use flate2::{Compression, GzBuilder, bufread::GzDecoder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

const REVIEW_FILE: &str = "review.json";
const LEDGER_FILE: &str = "ledger.json.gz";
const MANIFEST_FILE: &str = "manifest.json";
const EXPORT_SCHEMA_VERSION: &str = "spot-lab-export-v4";
const LEGACY_EXPORT_SCHEMA_V3: &str = "spot-lab-export-v3";
const LEGACY_EXPORT_SCHEMA_V1: &str = "spot-lab-export-v1";
const LEGACY_EXPORT_SCHEMA_V2: &str = "spot-lab-export-v2";
const MAX_MANIFEST_BYTES: u64 = 1 << 20;
const MAX_REVIEW_BYTES: u64 = 64 << 20;
const MAX_COMPRESSED_LEDGER_BYTES: u64 = 128 << 20;
const MAX_DECODED_LEDGER_BYTES: u64 = 512 << 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ExportScope {
    Full,
    Asset { asset: Asset },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayQualification {
    FullRun,
    /// Replay the complete frozen plan in original model order, then compare
    /// only `included_model_ids`. Direct subset execution would renumber events.
    QualifiedAssetSubset,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExportManifest {
    pub schema_version: String,
    pub run_id: crate::contracts::RunId,
    pub scope: ExportScope,
    pub input_digest: ContentHash,
    /// Semantic identity of the complete source run before asset filtering.
    pub source_run_semantic_digest: ContentHash,
    /// Semantic identity of the ledger artifact in this package.
    pub semantic_digest: ContentHash,
    pub replay_qualification: ReplayQualification,
    pub included_model_ids: Vec<crate::contracts::ModelId>,
    pub excluded_model_ids: Vec<crate::contracts::ModelId>,
    pub dataset_ids: Vec<crate::contracts::DatasetId>,
    /// Exact immutable policy revisions required to interpret and replay the plan.
    #[serde(default)]
    pub policy_revisions: Vec<crate::contracts::PolicyRevisionRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_fill_price_cost_unit: Option<crate::contracts::PriceCostAttributionUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_price_cost_unit: Option<crate::contracts::PriceCostAttributionUnit>,
    pub evidence_snapshot_id: Option<crate::contracts::EvidenceSnapshotId>,
    pub raw_objects_embedded: bool,
    pub raw_object_dependencies: Vec<crate::contracts::RawObjectId>,
    /// Hashes every artifact except this manifest, avoiding a circular self-hash.
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Debug, Clone)]
pub struct ExportResult {
    pub directory: PathBuf,
    pub scope: ExportScope,
    pub manifest: ExportManifest,
    /// Includes `manifest.json`; the manifest itself contains only the other artifacts.
    pub artifacts: Vec<ArtifactRef>,
}

impl ExportResult {
    /// Qualify package paths for atomic catalog publication without changing any ID.
    /// # Errors
    /// Rejects a mismatched package namespace, reference set, or retrieval identity.
    pub fn into_catalog_artifacts(mut self) -> Result<Vec<ArtifactRef>, LabError> {
        let directory_name = export_directory_name(&self.manifest.run_id, &self.scope);
        if self.manifest.schema_version != EXPORT_SCHEMA_VERSION
            || self.scope != self.manifest.scope
            || self.directory.file_name().and_then(|name| name.to_str())
                != Some(directory_name.as_str())
            || self.artifacts.len() != 3
            || self.manifest.artifacts.len() != 2
        {
            return Err(LabError::DataCorrupt(
                "export publication namespace or file set mismatch".into(),
            ));
        }
        exact_artifact(&self.manifest, REVIEW_FILE)?;
        exact_artifact(&self.manifest, LEDGER_FILE)?;
        for reference in &self.manifest.artifacts {
            if self.artifacts.iter().find(|item| item.id == reference.id) != Some(reference) {
                return Err(LabError::InputHashMismatch(
                    "manifest reference differs from publication".into(),
                ));
            }
        }
        let mut names = BTreeSet::new();
        for artifact in &mut self.artifacts {
            if ![REVIEW_FILE, LEDGER_FILE, MANIFEST_FILE].contains(&artifact.relative_path.as_str())
                || !names.insert(artifact.relative_path.clone())
                || artifact.run_id != self.manifest.run_id
            {
                return Err(LabError::DataCorrupt(
                    "export publication has duplicate or unexpected files".into(),
                ));
            }
            let path = catalog_path(&artifact.run_id, &self.scope, &artifact.relative_path);
            if artifact.id != ArtifactId::for_file(&artifact.run_id, &path, &artifact.sha256) {
                return Err(LabError::InputHashMismatch(
                    "export artifact retrieval identity mismatch".into(),
                ));
            }
            artifact.relative_path = path;
        }
        Ok(self.artifacts)
    }
}

#[derive(Debug, Clone)]
pub struct ReadExportResult {
    pub manifest: ExportManifest,
    pub bundle: RunBundle,
    pub validation: crate::contracts::ValidationReport,
}

/// Export a full run, or one asset's model facts plus every frozen dependency.
///
/// The caller owns validation of `root`. Reporting creates only an attempt-scoped
/// child and does not duplicate storage's data-root/path policy.
///
/// # Errors
/// Fails closed on invalid input, existing destination, I/O, serialization, or bounds.
pub fn export_run(
    bundle: &RunBundle,
    root: &Path,
    asset_filter: Option<Asset>,
) -> Result<ExportResult, LabError> {
    let validation = verify_run(bundle);
    if validation.status != crate::contracts::ValidationStatus::Pass {
        return Err(LabError::AccountingInvariant(format!(
            "export requires independent PASS: {}",
            validation.findings.join("; ")
        )));
    }
    let scope = asset_filter.map_or(ExportScope::Full, |asset| ExportScope::Asset { asset });
    let exported = scoped_bundle(bundle, &scope)?;
    fs::create_dir_all(root).map_err(io_error)?;
    let name = export_name(bundle, &scope);
    let target = root.join(&name);
    let staging = root.join(format!(
        ".{name}.attempt-{}",
        bundle.manifest.attempt_id.as_str()
    ));
    if target.exists() {
        return read_existing_export(bundle, &exported, scope, &target);
    }
    if staging.exists() {
        return Err(LabError::Conflict(format!(
            "export destination already exists: {name}"
        )));
    }
    let review = build_review(&exported)?;
    fs::create_dir(&staging).map_err(io_error)?;
    let result = write_attempt(bundle, &exported, &review, scope.clone(), &staging);
    let (manifest, mut artifacts) = match result {
        Ok(value) => value,
        Err(error) => {
            let _cleanup_result = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };
    sync_directory(&staging)?;
    fs::rename(&staging, &target).map_err(io_error)?;
    sync_directory(root)?;
    let manifest_ref = file_artifact(
        bundle,
        &scope,
        &target,
        MANIFEST_FILE,
        "application/json",
        None,
    )?;
    artifacts.push(manifest_ref);
    Ok(ExportResult {
        directory: target,
        scope,
        manifest,
        artifacts,
    })
}

fn read_existing_export(
    source_bundle: &RunBundle,
    exported_bundle: &RunBundle,
    scope: ExportScope,
    target: &Path,
) -> Result<ExportResult, LabError> {
    let package = read_export_package(target)?;
    if package.manifest.schema_version != EXPORT_SCHEMA_VERSION
        || package.manifest.run_id != source_bundle.manifest.run_id
        || package.manifest.scope != scope
        || package.manifest.input_digest != source_bundle.plan.input_digest
        || package.manifest.source_run_semantic_digest != source_bundle.semantic_digest
        || package.manifest.semantic_digest != exported_bundle.semantic_digest
    {
        return Err(LabError::Conflict(
            "existing export has a different immutable identity".to_owned(),
        ));
    }
    let mut artifacts = package.manifest.artifacts.clone();
    artifacts.push(file_artifact(
        exported_bundle,
        &scope,
        target,
        MANIFEST_FILE,
        "application/json",
        None,
    )?);
    Ok(ExportResult {
        directory: target.to_path_buf(),
        scope,
        manifest: package.manifest,
        artifacts,
    })
}

/// Read an export only after verifying exact filenames, hashes, gzip shape,
/// resource bounds, schema links, and semantic/input identities. This convenience
/// result is available only for a complete run. Asset-scoped packages must use
/// Read the qualified manifest together with its verified ledger. Future replay
/// must use this API, execute the complete original plan/model order, and only
/// then filter included model IDs so global event sequences retain their meaning.
///
/// # Errors
/// Rejects corruption, incomplete scope catalogs, unknown models, and bound excess.
pub fn read_export_package(directory: &Path) -> Result<ReadExportResult, LabError> {
    let manifest: ExportManifest = read_json(&directory.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)?;
    let schema_version = manifest.schema_version.as_str();
    if ![
        LEGACY_EXPORT_SCHEMA_V1,
        LEGACY_EXPORT_SCHEMA_V2,
        LEGACY_EXPORT_SCHEMA_V3,
        EXPORT_SCHEMA_VERSION,
    ]
    .contains(&schema_version)
        || manifest.artifacts.len() != 2
    {
        return Err(LabError::DataCorrupt(
            "unsupported export manifest or artifact count".into(),
        ));
    }
    let review_ref = exact_artifact(&manifest, REVIEW_FILE)?;
    let ledger_ref = exact_artifact(&manifest, LEDGER_FILE)?;
    verify_file_hash(&directory.join(REVIEW_FILE), review_ref, MAX_REVIEW_BYTES)?;
    verify_file_hash(
        &directory.join(LEDGER_FILE),
        ledger_ref,
        MAX_COMPRESSED_LEDGER_BYTES,
    )?;
    let bundle = read_ledger(&directory.join(LEDGER_FILE), ledger_ref)?;
    if bundle.manifest.run_id != manifest.run_id
        || bundle.plan.input_digest != manifest.input_digest
        || bundle.semantic_digest != manifest.semantic_digest
    {
        return Err(LabError::InputHashMismatch(
            "export manifest does not identify decoded ledger".into(),
        ));
    }
    if semantic_digest(&bundle)? != bundle.semantic_digest {
        return Err(LabError::InputHashMismatch(
            "decoded ledger semantic digest mismatch".into(),
        ));
    }
    let expected_models = verify_manifest_dependencies(&manifest, &bundle)?;
    let validation = verify_run_subset(&bundle, &expected_models);
    if validation.status != crate::contracts::ValidationStatus::Pass {
        return Err(LabError::AccountingInvariant(
            validation.findings.join("; "),
        ));
    }
    let review: ReviewPayload = read_json(&directory.join(REVIEW_FILE), MAX_REVIEW_BYTES)?;
    if review.run_id != bundle.manifest.run_id
        || review.input_digest != bundle.plan.input_digest
        || review.semantic_digest != bundle.semantic_digest
    {
        return Err(LabError::InputHashMismatch(
            "review identity does not match decoded ledger".into(),
        ));
    }
    verify_review_projection(&manifest, &bundle, &review)?;
    Ok(ReadExportResult {
        manifest,
        bundle,
        validation,
    })
}

fn verify_review_projection(
    manifest: &ExportManifest,
    bundle: &RunBundle,
    review: &ReviewPayload,
) -> Result<(), LabError> {
    if matches!(
        manifest.schema_version.as_str(),
        EXPORT_SCHEMA_VERSION | LEGACY_EXPORT_SCHEMA_V3
    ) {
        let expected = build_review(bundle)?;
        if ContentHash::of_value(review)? != ContentHash::of_value(&expected)? {
            return Err(LabError::InputHashMismatch(
                "export review differs from deterministic ledger projection".into(),
            ));
        }
    } else if review.models.iter().any(|model| {
        model.costs.embedded_price_cost_attribution_unit
            != crate::contracts::PriceCostAttributionUnit::LegacySumKrwPerBaseUnit
    }) {
        return Err(LabError::InputHashMismatch(
            "legacy export review has non-legacy price-cost units".into(),
        ));
    }
    Ok(())
}

fn scoped_bundle(bundle: &RunBundle, scope: &ExportScope) -> Result<RunBundle, LabError> {
    let mut scoped = bundle.clone();
    if let ExportScope::Asset { asset } = scope {
        scoped.models.retain(|model| model.market.base == *asset);
        if scoped.models.is_empty() {
            return Err(LabError::InvalidConfig(format!(
                "run has no {} models",
                asset.code()
            )));
        }
    }
    scoped.semantic_digest = semantic_digest(&scoped)?;
    Ok(scoped)
}

fn write_attempt(
    source_bundle: &RunBundle,
    bundle: &RunBundle,
    review: &ReviewPayload,
    scope: ExportScope,
    directory: &Path,
) -> Result<(ExportManifest, Vec<ArtifactRef>), LabError> {
    let review_hash = write_json(&directory.join(REVIEW_FILE), review, MAX_REVIEW_BYTES)?;
    let ledger_hash = write_ledger(&directory.join(LEDGER_FILE), bundle)?;
    let review_ref = artifact(
        bundle,
        &scope,
        REVIEW_FILE,
        "application/json",
        review_hash,
        None,
    );
    let ledger_ref = artifact(
        bundle,
        &scope,
        LEDGER_FILE,
        "application/gzip",
        ledger_hash.compressed,
        Some(ledger_hash.uncompressed),
    );
    let raw_object_dependencies = bundle
        .datasets
        .iter()
        .flat_map(|dataset| {
            dataset
                .manifest
                .raw_objects
                .iter()
                .map(|object| object.id.clone())
        })
        .collect();
    let included_model_ids: Vec<_> = bundle
        .models
        .iter()
        .map(|model| model.model_id.clone())
        .collect();
    let included: BTreeSet<_> = included_model_ids
        .iter()
        .map(crate::contracts::ModelId::as_str)
        .collect();
    let excluded_model_ids = source_bundle
        .models
        .iter()
        .filter(|model| !included.contains(model.model_id.as_str()))
        .map(|model| model.model_id.clone())
        .collect();
    let replay_qualification = match &scope {
        ExportScope::Full => ReplayQualification::FullRun,
        ExportScope::Asset { .. } => ReplayQualification::QualifiedAssetSubset,
    };
    let manifest = ExportManifest {
        schema_version: EXPORT_SCHEMA_VERSION.into(),
        run_id: bundle.manifest.run_id.clone(),
        scope,
        input_digest: bundle.plan.input_digest.clone(),
        source_run_semantic_digest: source_bundle.semantic_digest.clone(),
        semantic_digest: bundle.semantic_digest.clone(),
        replay_qualification,
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
        raw_fill_price_cost_unit: Some(crate::contracts::PriceCostAttributionUnit::KrwPerBaseUnit),
        review_price_cost_unit: Some(crate::contracts::PriceCostAttributionUnit::Krw),
        evidence_snapshot_id: bundle.evidence.as_ref().map(|snapshot| snapshot.id.clone()),
        raw_objects_embedded: false,
        raw_object_dependencies,
        artifacts: vec![review_ref.clone(), ledger_ref.clone()],
    };
    write_json(
        &directory.join(MANIFEST_FILE),
        &manifest,
        MAX_MANIFEST_BYTES,
    )?;
    Ok((manifest, vec![review_ref, ledger_ref]))
}

struct LedgerHashes {
    compressed: FileHash,
    uncompressed: FileHash,
}

fn write_ledger(path: &Path, bundle: &RunBundle) -> Result<LedgerHashes, LabError> {
    let file = create_file(path)?;
    let compressed = HashingWriter::new(file, MAX_COMPRESSED_LEDGER_BYTES);
    let gzip = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(compressed, Compression::default());
    let mut uncompressed = HashingWriter::new(gzip, MAX_DECODED_LEDGER_BYTES);
    serde_json::to_writer(&mut uncompressed, bundle).map_err(json_error)?;
    uncompressed.flush().map_err(io_error)?;
    let (gzip, uncompressed_hash) = uncompressed.finish()?;
    let compressed = gzip.finish().map_err(io_error)?;
    let (file, compressed_hash) = compressed.finish()?;
    file.sync_all().map_err(io_error)?;
    Ok(LedgerHashes {
        compressed: compressed_hash,
        uncompressed: uncompressed_hash,
    })
}

fn read_ledger(path: &Path, artifact: &ArtifactRef) -> Result<RunBundle, LabError> {
    let file = File::open(path).map_err(io_error)?;
    let buffered = BufReader::new(file);
    let gzip = GzDecoder::new(buffered);
    let mut decoded = HashingReader::new(gzip, MAX_DECODED_LEDGER_BYTES);
    let bundle: RunBundle = serde_json::from_reader(&mut decoded).map_err(json_error)?;
    let (gzip, actual) = decoded.finish()?;
    if artifact.uncompressed_bytes != Some(actual.bytes)
        || artifact.uncompressed_sha256.as_ref() != Some(&actual.sha256)
    {
        return Err(LabError::InputHashMismatch(
            "decoded ledger length/hash mismatch".into(),
        ));
    }
    let mut buffered = gzip.into_inner();
    if !buffered.fill_buf().map_err(io_error)?.is_empty() {
        return Err(LabError::DataCorrupt(
            "ledger gzip has trailing bytes or another member".into(),
        ));
    }
    Ok(bundle)
}

fn write_json(path: &Path, value: &impl Serialize, limit: u64) -> Result<FileHash, LabError> {
    let file = create_file(path)?;
    let mut writer = HashingWriter::new(file, limit);
    serde_json::to_writer(&mut writer, value).map_err(json_error)?;
    writer.flush().map_err(io_error)?;
    let (file, hash) = writer.finish()?;
    file.sync_all().map_err(io_error)?;
    Ok(hash)
}

fn read_json<T: DeserializeOwned>(path: &Path, limit: u64) -> Result<T, LabError> {
    let file = File::open(path).map_err(io_error)?;
    let mut reader = BoundedReader::new(file, limit);
    serde_json::from_reader(&mut reader).map_err(json_error)
}

fn exact_artifact<'a>(
    manifest: &'a ExportManifest,
    path: &str,
) -> Result<&'a ArtifactRef, LabError> {
    let matches: Vec<_> = manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.relative_path == path)
        .collect();
    if matches.len() != 1
        || manifest.artifacts.iter().any(|artifact| {
            artifact.relative_path != REVIEW_FILE && artifact.relative_path != LEDGER_FILE
        })
    {
        return Err(LabError::DataCorrupt(
            "manifest contains missing, duplicate, or unknown artifact paths".into(),
        ));
    }
    let artifact = matches[0];
    if manifest.schema_version == EXPORT_SCHEMA_VERSION
        && artifact.id
            != ArtifactId::for_file(
                &manifest.run_id,
                &catalog_path(&manifest.run_id, &manifest.scope, path),
                &artifact.sha256,
            )
    {
        return Err(LabError::InputHashMismatch(
            "manifest artifact retrieval identity mismatch".into(),
        ));
    }
    let shape_valid = artifact.run_id == manifest.run_id
        && artifact.complete
        && match path {
            REVIEW_FILE => {
                artifact.media_type == "application/json"
                    && artifact.uncompressed_sha256.is_none()
                    && artifact.uncompressed_bytes.is_none()
            }
            LEDGER_FILE => {
                artifact.media_type == "application/gzip"
                    && artifact.uncompressed_sha256.is_some()
                    && artifact.uncompressed_bytes.is_some()
            }
            _ => false,
        };
    if !shape_valid {
        return Err(LabError::DataCorrupt(format!(
            "invalid artifact metadata for {path}"
        )));
    }
    Ok(artifact)
}

fn verify_manifest_dependencies(
    manifest: &ExportManifest,
    bundle: &RunBundle,
) -> Result<BTreeSet<String>, LabError> {
    let dataset_ids: Vec<_> = bundle
        .datasets
        .iter()
        .map(|dataset| dataset.manifest.id.clone())
        .collect();
    let evidence_id = bundle.evidence.as_ref().map(|snapshot| snapshot.id.clone());
    let policy_revisions: Vec<_> = bundle
        .plan
        .policy_revisions
        .iter()
        .map(|revision| revision.reference.clone())
        .collect();
    let raw_dependencies: Vec<_> = bundle
        .datasets
        .iter()
        .flat_map(|dataset| {
            dataset
                .manifest
                .raw_objects
                .iter()
                .map(|object| object.id.clone())
        })
        .collect();
    let scope_valid = match &manifest.scope {
        ExportScope::Full => manifest.replay_qualification == ReplayQualification::FullRun,
        ExportScope::Asset { asset } => {
            manifest.replay_qualification == ReplayQualification::QualifiedAssetSubset
                && !bundle.models.is_empty()
                && bundle
                    .models
                    .iter()
                    .all(|model| model.market.base == *asset)
        }
    };
    let included_ids: Vec<_> = bundle
        .models
        .iter()
        .map(|model| model.model_id.clone())
        .collect();
    let included: BTreeSet<_> = manifest
        .included_model_ids
        .iter()
        .map(|model_id| model_id.as_str().to_owned())
        .collect();
    let excluded: BTreeSet<_> = manifest
        .excluded_model_ids
        .iter()
        .map(|model_id| model_id.as_str().to_owned())
        .collect();
    let admitted: BTreeSet<_> = bundle
        .plan
        .admissions
        .iter()
        .map(|admission| admission.model_id.as_str().to_owned())
        .collect();
    let catalog_valid = included.len() == manifest.included_model_ids.len()
        && excluded.len() == manifest.excluded_model_ids.len()
        && included.is_disjoint(&excluded)
        && included.union(&excluded).cloned().collect::<BTreeSet<_>>() == admitted
        && manifest.included_model_ids == included_ids
        && match manifest.replay_qualification {
            ReplayQualification::FullRun => {
                excluded.is_empty()
                    && included == admitted
                    && manifest.source_run_semantic_digest == manifest.semantic_digest
            }
            ReplayQualification::QualifiedAssetSubset => true,
        };
    let schema_version = manifest.schema_version.as_str();
    let policy_catalog_valid = if schema_version == LEGACY_EXPORT_SCHEMA_V1 {
        manifest.policy_revisions.is_empty() && policy_revisions.is_empty()
    } else if matches!(
        schema_version,
        LEGACY_EXPORT_SCHEMA_V2 | LEGACY_EXPORT_SCHEMA_V3 | EXPORT_SCHEMA_VERSION
    ) {
        manifest.policy_revisions == policy_revisions
    } else {
        false
    };
    let presentation_units_valid = valid_presentation_units(manifest);
    if manifest.raw_objects_embedded
        || manifest.dataset_ids != dataset_ids
        || !policy_catalog_valid
        || !presentation_units_valid
        || manifest.evidence_snapshot_id != evidence_id
        || manifest.raw_object_dependencies != raw_dependencies
        || !scope_valid
        || !catalog_valid
    {
        return Err(LabError::InputHashMismatch(
            "export dependency catalog or scope does not match decoded ledger".into(),
        ));
    }
    Ok(included)
}

fn valid_presentation_units(manifest: &ExportManifest) -> bool {
    let schema_version = manifest.schema_version.as_str();
    if schema_version == LEGACY_EXPORT_SCHEMA_V1 || schema_version == LEGACY_EXPORT_SCHEMA_V2 {
        manifest.raw_fill_price_cost_unit.is_none() && manifest.review_price_cost_unit.is_none()
    } else if matches!(
        schema_version,
        LEGACY_EXPORT_SCHEMA_V3 | EXPORT_SCHEMA_VERSION
    ) {
        manifest.raw_fill_price_cost_unit
            == Some(crate::contracts::PriceCostAttributionUnit::KrwPerBaseUnit)
            && manifest.review_price_cost_unit
                == Some(crate::contracts::PriceCostAttributionUnit::Krw)
    } else {
        false
    }
}

fn verify_file_hash(path: &Path, artifact: &ArtifactRef, limit: u64) -> Result<(), LabError> {
    let metadata = fs::metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.len() > limit || metadata.len() != artifact.bytes {
        return Err(LabError::ResourceLimit(format!(
            "artifact size mismatch or limit exceeded: {}",
            artifact.relative_path
        )));
    }
    let mut file = File::open(path).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = hash_from_hasher(hasher)?;
    if actual != artifact.sha256 {
        return Err(LabError::InputHashMismatch(format!(
            "artifact hash mismatch: {}",
            artifact.relative_path
        )));
    }
    Ok(())
}

fn file_artifact(
    bundle: &RunBundle,
    scope: &ExportScope,
    directory: &Path,
    relative_path: &str,
    media_type: &str,
    uncompressed: Option<FileHash>,
) -> Result<ArtifactRef, LabError> {
    let path = directory.join(relative_path);
    let metadata = fs::metadata(&path).map_err(io_error)?;
    let mut file = File::open(path).map_err(io_error)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file, &mut HashSink(&mut hasher)).map_err(io_error)?;
    Ok(artifact(
        bundle,
        scope,
        relative_path,
        media_type,
        FileHash {
            bytes: metadata.len(),
            sha256: hash_from_hasher(hasher)?,
        },
        uncompressed,
    ))
}

fn artifact(
    bundle: &RunBundle,
    scope: &ExportScope,
    relative_path: &str,
    media_type: &str,
    stored: FileHash,
    uncompressed: Option<FileHash>,
) -> ArtifactRef {
    let storage_path = catalog_path(&bundle.manifest.run_id, scope, relative_path);
    ArtifactRef {
        id: ArtifactId::for_file(&bundle.manifest.run_id, &storage_path, &stored.sha256),
        run_id: bundle.manifest.run_id.clone(),
        relative_path: relative_path.into(),
        media_type: media_type.into(),
        bytes: stored.bytes,
        sha256: stored.sha256,
        uncompressed_sha256: uncompressed.as_ref().map(|hash| hash.sha256.clone()),
        uncompressed_bytes: uncompressed.map(|hash| hash.bytes),
        complete: true,
    }
}

fn export_name(bundle: &RunBundle, scope: &ExportScope) -> String {
    export_directory_name(&bundle.manifest.run_id, scope)
}

fn export_directory_name(run_id: &crate::contracts::RunId, scope: &ExportScope) -> String {
    let suffix = match scope {
        ExportScope::Full => "full",
        ExportScope::Asset { asset } => asset.code(),
    };
    format!("{run_id}-{suffix}-export-v4")
}

fn catalog_path(run_id: &crate::contracts::RunId, scope: &ExportScope, file_name: &str) -> String {
    format!(
        "exports/{}/{file_name}",
        export_directory_name(run_id, scope)
    )
}

fn create_file(path: &Path) -> Result<File, LabError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error)
}

fn sync_directory(path: &Path) -> Result<(), LabError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)
}

#[derive(Clone)]
struct FileHash {
    bytes: u64,
    sha256: ContentHash,
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
    limit: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
            limit,
        }
    }
    fn finish(self) -> Result<(W, FileHash), LabError> {
        Ok((
            self.inner,
            FileHash {
                bytes: self.bytes,
                sha256: hash_from_hasher(self.hasher)?,
            },
        ))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| io::Error::other("write length overflow"))?;
        if self
            .bytes
            .checked_add(length)
            .is_none_or(|total| total > self.limit)
        {
            return Err(io::Error::other("artifact exceeds configured byte limit"));
        }
        let written = self.inner.write(bytes)?;
        self.bytes +=
            u64::try_from(written).map_err(|_| io::Error::other("write length overflow"))?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
    bytes: u64,
    limit: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
            limit,
        }
    }
    fn finish(self) -> Result<(R, FileHash), LabError> {
        Ok((
            self.inner,
            FileHash {
                bytes: self.bytes,
                sha256: hash_from_hasher(self.hasher)?,
            },
        ))
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.bytes);
        if remaining == 0 {
            let mut probe = [0_u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => Err(io::Error::other(
                    "decoded artifact exceeds configured byte limit",
                )),
            };
        }
        let allowed = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| io::Error::other("read length overflow"))?;
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.bytes += u64::try_from(read).map_err(|_| io::Error::other("read length overflow"))?;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}

struct BoundedReader<R> {
    inner: R,
    bytes: u64,
    limit: u64,
}
impl<R> BoundedReader<R> {
    const fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            bytes: 0,
            limit,
        }
    }
}
impl<R: Read> Read for BoundedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.bytes);
        if remaining == 0 {
            let mut probe = [0_u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => Err(io::Error::other("JSON exceeds configured byte limit")),
            };
        }
        let allowed = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| io::Error::other("read length overflow"))?;
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.bytes += u64::try_from(read).map_err(|_| io::Error::other("read length overflow"))?;
        Ok(read)
    }
}

struct HashSink<'a>(&'a mut Sha256);
impl Write for HashSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hash_from_hasher(hasher: Sha256) -> Result<ContentHash, LabError> {
    ContentHash::try_from(format!("{:x}", hasher.finalize()))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err passes the owned source error"
)]
fn io_error(error: io::Error) -> LabError {
    LabError::Internal(format!("reporting I/O: {error}"))
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err passes the owned source error"
)]
fn json_error(error: serde_json::Error) -> LabError {
    LabError::ContractParse(format!("reporting JSON: {error}"))
}
