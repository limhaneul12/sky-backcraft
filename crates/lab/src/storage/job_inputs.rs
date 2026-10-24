use super::{RawObjectStore, Store, json_error, sql_error};
use crate::contracts::{
    ArtifactId, AttemptId, AttemptState, ContentHash, DatasetId, DeleteResource, JobId, JobOutput,
    JobPayload, LabError, PlanId, PlanRequest, RequestId, ResolvedPlan, RunId,
};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum JobInputRef {
    CollectionRequest(RequestId),
    Plan(PlanId),
    Resource(DeleteResource),
}

#[derive(Debug, Clone)]
pub(super) struct ActiveJobInputHold {
    pub job_id: JobId,
    pub inputs: Vec<JobInputRef>,
}

#[derive(serde::Serialize)]
struct InputProjection<'a> {
    config_digest: &'a ContentHash,
    dataset_digests: &'a [(DatasetId, ContentHash)],
    evidence_digest: &'a Option<ContentHash>,
}

/// Resolve a validated input set from one immutable payload inside the caller's Store operation.
///
/// # Errors
/// Rejects missing or corrupt input rows, stale digests, unknown payloads and SQLite failures.
pub(super) fn active_hold(
    connection: &Connection,
    job_id: JobId,
    payload: &JobPayload,
) -> Result<ActiveJobInputHold, LabError> {
    let mut inputs = vec![JobInputRef::Resource(DeleteResource::Job {
        job_id: job_id.clone(),
    })];
    let payload_inputs = match payload {
        JobPayload::Collect { request } => {
            vec![JobInputRef::CollectionRequest(request.request_id.clone())]
        }
        JobPayload::Backtest { request } => {
            plan_inputs(connection, &request.plan_id, &request.input_digest)?
        }
        JobPayload::PortfolioBacktest { request } => {
            request.portfolio.validate()?;
            plan_inputs(connection, &request.plan_id, &request.input_digest)?
        }
        JobPayload::Export { run_id, .. } => run_inputs(connection, run_id)?,
        JobPayload::Verify { artifact_id, .. } => artifact_package_inputs(connection, artifact_id)?,
    };
    inputs.extend(payload_inputs);
    Ok(ActiveJobInputHold { job_id, inputs })
}

pub(super) fn extend_with_terminal_outputs(hold: &mut ActiveJobInputHold, state: &AttemptState) {
    let output = match state {
        AttemptState::Completed { output, .. } | AttemptState::Partial { output, .. } => {
            Some(output)
        }
        AttemptState::Blocked {
            output: Some(output),
            ..
        } => Some(output),
        _ => None,
    };
    let Some(output) = output else {
        return;
    };
    match output {
        JobOutput::Dataset { dataset_id } => {
            push_unique(
                &mut hold.inputs,
                JobInputRef::Resource(DeleteResource::Dataset {
                    dataset_id: dataset_id.clone(),
                }),
            );
        }
        JobOutput::Run { run_id, .. } | JobOutput::Portfolio { run_id } => {
            push_unique(
                &mut hold.inputs,
                JobInputRef::Resource(DeleteResource::Run {
                    run_id: run_id.clone(),
                }),
            );
        }
        JobOutput::Artifacts { artifact_ids } => {
            for artifact_id in artifact_ids {
                push_unique(
                    &mut hold.inputs,
                    JobInputRef::Resource(DeleteResource::Artifact {
                        artifact_id: artifact_id.clone(),
                    }),
                );
            }
        }
        JobOutput::Validation { .. } => {}
    }
}

fn push_unique(inputs: &mut Vec<JobInputRef>, input: JobInputRef) {
    if !inputs.contains(&input) {
        inputs.push(input);
    }
}

/// Check referenced input files exist without duplicating the worker's full hash/decompression pass.
/// Database relationships are rechecked in the admission transaction by `active_hold`.
/// # Errors
/// Returns missing/corrupt raw objects or package files before work can be queued or claimed.
pub(super) fn verify_files(
    raw: &RawObjectStore,
    connection: &Connection,
    payload: &JobPayload,
) -> Result<(), LabError> {
    match payload {
        JobPayload::Collect { .. } => Ok(()),
        JobPayload::Backtest { request } => {
            verify_plan_raw_objects(connection, raw, &request.plan_id)
        }
        JobPayload::PortfolioBacktest { request } => {
            verify_plan_raw_objects(connection, raw, &request.plan_id)
        }
        JobPayload::Export { run_id, .. } => {
            let (plan_id, _) = run_plan_and_job_id(connection, run_id)?;
            verify_plan_raw_objects(connection, raw, &plan_id)
        }
        JobPayload::Verify { artifact_id, .. } => {
            verify_artifact_package_files(connection, raw, artifact_id)
        }
    }
}

fn plan_inputs(
    connection: &Connection,
    plan_id: &PlanId,
    expected_digest: &ContentHash,
) -> Result<Vec<JobInputRef>, LabError> {
    let row = connection
        .query_row(
            "SELECT original_request_json,resolved_plan_json,input_digest FROM plans WHERE id=?1",
            [plan_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig(format!("unknown frozen plan {plan_id}")))?;
    let request: PlanRequest = from_stored_json(&row.0, "plan request")?;
    let plan: ResolvedPlan = from_stored_json(&row.1, "resolved plan")?;
    validate_plan_shape(plan_id, &request, &plan, &row.2)?;
    if &plan.input_digest != expected_digest {
        return Err(LabError::InputHashMismatch(
            "submitted plan digest mismatch".into(),
        ));
    }

    let mut inputs = vec![JobInputRef::Plan(plan_id.clone())];
    validate_plan_datasets(connection, &plan, &mut inputs)?;
    validate_plan_evidence(connection, &plan, &mut inputs)?;
    validate_plan_policies(connection, plan_id, &plan, &mut inputs)?;
    Ok(inputs)
}

fn validate_plan_shape(
    plan_id: &PlanId,
    request: &PlanRequest,
    plan: &ResolvedPlan,
    stored_digest: &str,
) -> Result<(), LabError> {
    request.spec.validate()?;
    plan.spec.validate()?;
    let digest_projection = ContentHash::of_value(&InputProjection {
        config_digest: &plan.config_digest,
        dataset_digests: &plan.dataset_digests,
        evidence_digest: &plan.evidence_digest,
    })?;
    if &plan.id != plan_id
        || serde_json::to_vec(&request.spec).map_err(json_error)?
            != serde_json::to_vec(&plan.spec).map_err(json_error)?
        || crate::contracts::experiment_config_digest(&plan.spec, &plan.policy_revisions)?
            != plan.config_digest
        || digest_projection != plan.input_digest
        || plan.input_digest.as_str() != stored_digest
        || plan.spec.dataset_ids.len() != plan.dataset_digests.len()
        || plan
            .spec
            .dataset_ids
            .iter()
            .zip(&plan.dataset_digests)
            .any(|(expected, (actual, _))| expected != actual)
    {
        return Err(LabError::DataCorrupt(
            "stored frozen plan input identity or digest projection is inconsistent".into(),
        ));
    }
    Ok(())
}

fn validate_plan_datasets(
    connection: &Connection,
    plan: &ResolvedPlan,
    inputs: &mut Vec<JobInputRef>,
) -> Result<(), LabError> {
    let mut expected = Vec::with_capacity(plan.dataset_digests.len());
    for (dataset_id, digest) in &plan.dataset_digests {
        let row = connection
            .query_row(
                "SELECT semantic_digest,manifest_json FROM datasets WHERE id=?1",
                [dataset_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig(format!("plan references unknown dataset {dataset_id}"))
            })?;
        if row.0 != digest.as_str() {
            return Err(LabError::InputHashMismatch(format!(
                "plan dataset digest mismatch for {dataset_id}"
            )));
        }
        let manifest: crate::contracts::DatasetManifest =
            from_stored_json(&row.1, "dataset manifest")?;
        if manifest.id != *dataset_id || manifest.semantic_digest.as_str() != row.0 {
            return Err(LabError::DataCorrupt(format!(
                "stored dataset manifest identity differs from row {dataset_id}"
            )));
        }
        expected.push((dataset_id.as_str().to_owned(), digest.as_str().to_owned()));
        inputs.push(JobInputRef::Resource(DeleteResource::Dataset {
            dataset_id: dataset_id.clone(),
        }));
    }
    let mut statement = connection
        .prepare(
            "SELECT dataset_id,semantic_digest FROM plan_datasets WHERE plan_id=?1 ORDER BY position",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([plan.id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?;
    let stored = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
    if stored != expected {
        return Err(LabError::DataCorrupt(
            "frozen plan dataset closure differs from its stored relationship rows".into(),
        ));
    }
    Ok(())
}

fn validate_plan_evidence(
    connection: &Connection,
    plan: &ResolvedPlan,
    inputs: &mut Vec<JobInputRef>,
) -> Result<(), LabError> {
    match (&plan.spec.evidence_snapshot_id, &plan.evidence_digest) {
        (Some(id), Some(expected)) => {
            let stored = connection
                .query_row(
                    "SELECT digest FROM evidence_snapshots WHERE id=?1",
                    [id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    LabError::InvalidConfig(format!("plan references unknown evidence {id}"))
                })?;
            if stored != expected.as_str() {
                return Err(LabError::InputHashMismatch(
                    "plan evidence digest mismatch".into(),
                ));
            }
            inputs.push(JobInputRef::Resource(DeleteResource::EvidenceSnapshot {
                snapshot_id: id.clone(),
            }));
        }
        (None, None) => {}
        _ => {
            return Err(LabError::DataCorrupt(
                "frozen plan evidence ID and digest are inconsistent".into(),
            ));
        }
    }
    Ok(())
}

fn validate_plan_policies(
    connection: &Connection,
    plan_id: &PlanId,
    plan: &ResolvedPlan,
    inputs: &mut Vec<JobInputRef>,
) -> Result<(), LabError> {
    let mut statement = connection
        .prepare(
            "SELECT position,policy_id,revision_id,definition_digest,frozen_json \
             FROM plan_policy_revisions WHERE plan_id=?1 ORDER BY position",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([plan_id.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(sql_error)?;
    let stored = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
    if stored.len() != plan.policy_revisions.len() {
        return Err(LabError::DataCorrupt(
            "frozen plan policy closure cardinality mismatch".into(),
        ));
    }
    for (position, (row, frozen)) in stored.iter().zip(&plan.policy_revisions).enumerate() {
        let position = i64::try_from(position)
            .map_err(|_| LabError::ResourceLimit("policy position overflow".into()))?;
        let reference = &frozen.reference;
        let frozen_json = serde_json::to_string(frozen).map_err(json_error)?;
        if row.0 != position
            || row.1 != reference.policy_id.as_str()
            || row.2 != reference.revision_id.as_str()
            || row.3 != reference.definition_digest.as_str()
            || row.4 != frozen_json
        {
            return Err(LabError::DataCorrupt(
                "frozen plan policy body differs from its stored relationship".into(),
            ));
        }
        let (
            family,
            origin,
            revision_number,
            parent_revision_id,
            definition_json,
            definition_digest,
        ): (String, String, u32, Option<String>, String, String) = connection
            .query_row(
                "SELECT p.family,p.origin,r.revision_number,r.parent_revision_id,\
                        r.definition_json,r.definition_digest \
                 FROM policy_revisions r JOIN policies p ON p.policy_id=r.policy_id \
                 WHERE r.policy_id=?1 AND r.revision_id=?2",
                params![reference.policy_id.as_str(), reference.revision_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig("plan references unknown policy revision".into())
            })?;
        let definition: crate::contracts::PolicyDefinition =
            from_stored_json(&definition_json, "policy definition")?;
        definition.validate()?;
        let stored_snapshot = crate::contracts::FrozenPolicyRevision {
            reference: reference.clone(),
            revision_number,
            parent_revision_id: parent_revision_id
                .map(crate::contracts::PolicyRevisionId::new)
                .transpose()?,
            family: serde_json::from_value(serde_json::Value::String(family))
                .map_err(|_| LabError::DataCorrupt("stored policy family is invalid".into()))?,
            origin: serde_json::from_value(serde_json::Value::String(origin))
                .map_err(|_| LabError::DataCorrupt("stored policy origin is invalid".into()))?,
            definition: definition.clone(),
        };
        if definition_digest != reference.definition_digest.as_str()
            || ContentHash::of_value(&definition)? != reference.definition_digest
            || serde_json::to_vec(&stored_snapshot).map_err(json_error)?
                != serde_json::to_vec(frozen).map_err(json_error)?
        {
            return Err(LabError::InputHashMismatch(
                "frozen plan policy differs from immutable registry revision".into(),
            ));
        }
        inputs.push(JobInputRef::Resource(DeleteResource::Policy {
            policy_id: reference.policy_id.clone(),
        }));
    }
    Ok(())
}

fn run_inputs(connection: &Connection, run_id: &RunId) -> Result<Vec<JobInputRef>, LabError> {
    let (plan_id, producer_job_id) = run_plan_and_job_id(connection, run_id)?;
    let mut inputs = vec![
        JobInputRef::Resource(DeleteResource::Run {
            run_id: run_id.clone(),
        }),
        JobInputRef::Resource(DeleteResource::Job {
            job_id: producer_job_id,
        }),
    ];
    inputs.extend(plan_inputs_without_digest(connection, &plan_id)?);
    Ok(inputs)
}

fn run_plan_and_job_id(
    connection: &Connection,
    run_id: &RunId,
) -> Result<(PlanId, JobId), LabError> {
    let (plan_id, job_id, attempt_id, manifest_json): (String, String, String, String) = connection
        .query_row(
            "SELECT plan_id,job_id,attempt_id,manifest_json FROM runs WHERE id=?1",
            [run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig(format!("unknown run {run_id}")))?;
    let manifest: crate::contracts::RunManifest = from_stored_json(&manifest_json, "run manifest")?;
    let plan_id = PlanId::new(plan_id)?;
    let job_id = JobId::new(job_id)?;
    let attempt_id = AttemptId::new(attempt_id)?;
    if manifest.run_id != *run_id || manifest.job_id != job_id || manifest.attempt_id != attempt_id
    {
        return Err(LabError::DataCorrupt(
            "run manifest identity differs from its row".into(),
        ));
    }
    Ok((plan_id, job_id))
}

fn plan_inputs_without_digest(
    connection: &Connection,
    plan_id: &PlanId,
) -> Result<Vec<JobInputRef>, LabError> {
    let stored_digest: String = connection
        .query_row(
            "SELECT input_digest FROM plans WHERE id=?1",
            [plan_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig(format!("unknown frozen plan {plan_id}")))?;
    plan_inputs(connection, plan_id, &ContentHash::try_from(stored_digest)?)
}

fn artifact_package_inputs(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> Result<Vec<JobInputRef>, LabError> {
    let target = load_artifact(connection, artifact_id)?
        .ok_or_else(|| LabError::InvalidConfig("unknown artifact".into()))?;
    super::validate_relative_path(&target.relative_path)?;
    if !target.relative_path.starts_with("exports/") {
        return Err(LabError::DataCorrupt(
            "catalogued verification artifact escaped exports/".into(),
        ));
    }
    let directory = target
        .relative_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .filter(|parent| parent.starts_with("exports/"))
        .ok_or_else(|| LabError::DataCorrupt("artifact has no export package parent".into()))?;
    let prefix = format!("{directory}/");
    let mut statement = connection
        .prepare(
            "SELECT id,artifact_json FROM run_artifacts \
             WHERE substr(relative_path,1,length(?1))=?1 ORDER BY id",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([&prefix], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?;
    let mut inputs = Vec::new();
    let mut target_present = false;
    for row in rows {
        let (stored_id, artifact_json) = row.map_err(sql_error)?;
        let artifact: crate::contracts::ArtifactRef =
            from_stored_json(&artifact_json, "export artifact")?;
        super::validate_relative_path(&artifact.relative_path)?;
        if artifact.id.as_str() != stored_id
            || !artifact.relative_path.starts_with(&prefix)
            || artifact.run_id != target.run_id
        {
            return Err(LabError::DataCorrupt(
                "export package artifact row has inconsistent identity".into(),
            ));
        }
        target_present |= artifact.id == *artifact_id;
        inputs.push(JobInputRef::Resource(DeleteResource::Artifact {
            artifact_id: artifact.id,
        }));
    }
    if !target_present {
        return Err(LabError::DataCorrupt(
            "verification artifact is absent from its export package".into(),
        ));
    }
    Ok(inputs)
}

fn load_artifact(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> Result<Option<crate::contracts::ArtifactRef>, LabError> {
    connection
        .query_row(
            "SELECT artifact_json FROM run_artifacts WHERE id=?1",
            [artifact_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?
        .map(|json| from_stored_json(&json, "export artifact"))
        .transpose()
}

fn from_stored_json<T>(json: &str, label: &str) -> Result<T, LabError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(json)
        .map_err(|error| LabError::DataCorrupt(format!("stored {label} JSON is invalid: {error}")))
}

fn verify_plan_raw_objects(
    connection: &Connection,
    raw: &RawObjectStore,
    plan_id: &PlanId,
) -> Result<(), LabError> {
    let inputs = plan_inputs_without_digest(connection, plan_id)?;
    let mut verified_datasets = Vec::new();
    for input in inputs {
        let JobInputRef::Resource(DeleteResource::Dataset { dataset_id }) = input else {
            continue;
        };
        if verified_datasets.contains(&dataset_id) {
            continue;
        }
        verified_datasets.push(dataset_id.clone());
        verify_dataset_raw_objects(connection, raw, &dataset_id)?;
    }
    Ok(())
}

fn verify_dataset_raw_objects(
    connection: &Connection,
    raw: &RawObjectStore,
    dataset_id: &DatasetId,
) -> Result<(), LabError> {
    let manifest_json: String = connection
        .query_row(
            "SELECT manifest_json FROM datasets WHERE id=?1",
            [dataset_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig(format!("unknown dataset {dataset_id}")))?;
    let manifest: crate::contracts::DatasetManifest =
        from_stored_json(&manifest_json, "dataset manifest")?;
    if manifest.id != *dataset_id {
        return Err(LabError::DataCorrupt(
            "dataset manifest identity differs from row".into(),
        ));
    }
    for object in &manifest.raw_objects {
        super::validate_object_path(object)?;
        let directory = raw.root.join(&object.relative_path);
        let directory_metadata =
            raw.inspect_job_input_path(&directory, &format!("raw input {}", object.id))?;
        if !directory_metadata.is_dir() || directory_metadata.file_type().is_symlink() {
            return Err(LabError::DataCorrupt(format!(
                "raw input {} is not a regular directory",
                object.id
            )));
        }
        for (name, expected_bytes) in [
            (super::BODY_FILE, Some(object.compressed_bytes)),
            (super::META_FILE, None),
        ] {
            let path = directory.join(name);
            let metadata =
                raw.inspect_job_input_path(&path, &format!("raw input file {}", object.id))?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || expected_bytes.is_some_and(|bytes| metadata.len() != bytes)
            {
                return Err(LabError::DataCorrupt(format!(
                    "raw input file is malformed: {}",
                    object.id
                )));
            }
        }
    }
    Ok(())
}

fn verify_artifact_package_files(
    connection: &Connection,
    raw: &RawObjectStore,
    artifact_id: &ArtifactId,
) -> Result<(), LabError> {
    let inputs = artifact_package_inputs(connection, artifact_id)?;
    for input in inputs {
        let JobInputRef::Resource(DeleteResource::Artifact { artifact_id }) = input else {
            continue;
        };
        let artifact = load_artifact(connection, &artifact_id)?
            .ok_or_else(|| LabError::DataCorrupt("export package sibling disappeared".into()))?;
        super::validate_relative_path(&artifact.relative_path)?;
        if !artifact.relative_path.starts_with("exports/") {
            return Err(LabError::DataCorrupt(
                "catalogued artifact escaped exports/".into(),
            ));
        }
        let path = raw.root.join(&artifact.relative_path);
        let metadata =
            raw.inspect_job_input_path(&path, &format!("verification artifact {}", artifact.id))?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != artifact.bytes
        {
            return Err(LabError::DataCorrupt(format!(
                "verification package artifact is malformed: {}",
                artifact.id
            )));
        }
    }
    Ok(())
}

/// Release one process-local hold after its runner has completed terminal readback and post-work.
/// The caller must make this the final Store action for that attempt.
/// # Errors
/// Refuses release while the persisted attempt is still queued or running.
pub(super) fn release_terminal_hold(
    store: &mut Store,
    attempt_id: &AttemptId,
) -> Result<(), LabError> {
    let attempt = store.load_attempt(attempt_id)?;
    if !attempt.state.is_terminal() {
        return Err(LabError::Conflict(
            "cannot release input hold before attempt termination".into(),
        ));
    }
    store.active_job_input_holds.remove(attempt_id);
    Ok(())
}
