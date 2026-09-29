use super::{Store, json_error, sql_error, usize_to_i64};
use crate::contracts::{
    ContentHash, LabError, PlanId, PlanRequest, ResolvedPlan, experiment_config_digest,
    strategy_binding, validate_frozen_policy,
};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct StoredPlan {
    pub request: PlanRequest,
    pub resolved: ResolvedPlan,
}

#[derive(Serialize)]
struct InputProjection<'a> {
    config_digest: &'a ContentHash,
    dataset_digests: &'a [(crate::contracts::DatasetId, ContentHash)],
    evidence_digest: &'a Option<ContentHash>,
}

fn validate_plan_shape(request: &PlanRequest, plan: &ResolvedPlan) -> Result<(), LabError> {
    request.spec.validate()?;
    plan.spec.validate()?;
    if serde_json::to_vec(&request.spec).map_err(json_error)?
        != serde_json::to_vec(&plan.spec).map_err(json_error)?
        || experiment_config_digest(&plan.spec, &plan.policy_revisions)? != plan.config_digest
        || ContentHash::of_value(&InputProjection {
            config_digest: &plan.config_digest,
            dataset_digests: &plan.dataset_digests,
            evidence_digest: &plan.evidence_digest,
        })? != plan.input_digest
    {
        return Err(LabError::InputHashMismatch(
            "resolved plan does not match original config/input digests".into(),
        ));
    }
    if plan.spec.dataset_ids.len() != plan.dataset_digests.len()
        || plan
            .spec
            .dataset_ids
            .iter()
            .zip(&plan.dataset_digests)
            .any(|(expected, (actual, _))| expected != actual)
    {
        return Err(LabError::Conflict(
            "plan dataset digest catalog does not match configured datasets".into(),
        ));
    }
    let model_ids: BTreeSet<_> = plan
        .admissions
        .iter()
        .map(|value| value.model_id.as_str())
        .collect();
    if model_ids.len() != plan.admissions.len()
        || plan.admissions.iter().any(|admission| {
            !plan.spec.markets.contains(&admission.market)
                || strategy_binding(plan, admission).is_err()
        })
    {
        return Err(LabError::Conflict(
            "plan admissions contain duplicate IDs or out-of-config market/strategy".into(),
        ));
    }
    Ok(())
}

impl Store {
    /// Save one immutable original request and its resolved plan.
    ///
    /// # Errors
    /// Rejects invalid digests/references/identity conflicts or SQLite failure.
    pub fn save_plan(
        &mut self,
        request: &PlanRequest,
        plan: &ResolvedPlan,
    ) -> Result<PlanId, LabError> {
        validate_plan_shape(request, plan)?;
        let request_json = serde_json::to_string(request).map_err(json_error)?;
        let plan_json = serde_json::to_string(plan).map_err(json_error)?;
        if let Some(existing) = self.connection.query_row(
            "SELECT original_request_json,resolved_plan_json FROM plans WHERE id=?1 OR request_id=?2",
            params![plan.id.as_str(), request.request_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        ).optional().map_err(sql_error)? {
            return if existing == (request_json, plan_json) { Ok(plan.id.clone()) } else {
                Err(LabError::Conflict("plan or request identity already has different content".into()))
            };
        }
        self.validate_plan_references(plan)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction.execute(
            "INSERT INTO plans(id,request_id,config_digest,input_digest,evidence_snapshot_id,original_request_json,resolved_plan_json) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![plan.id.as_str(), request.request_id.as_str(), plan.config_digest.as_str(), plan.input_digest.as_str(), plan.spec.evidence_snapshot_id.as_ref().map(crate::contracts::EvidenceSnapshotId::as_str), request_json, plan_json],
        ).map_err(sql_error)?;
        for (position, (dataset, digest)) in plan.dataset_digests.iter().enumerate() {
            transaction.execute(
                "INSERT INTO plan_datasets(plan_id,dataset_id,semantic_digest,position) VALUES (?1,?2,?3,?4)",
                params![plan.id.as_str(), dataset.as_str(), digest.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
        for (position, policy) in plan.policy_revisions.iter().enumerate() {
            transaction.execute(
                "INSERT INTO plan_policy_revisions(plan_id,position,policy_id,revision_id,definition_digest,frozen_json) VALUES (?1,?2,?3,?4,?5,?6)",
                params![plan.id.as_str(), usize_to_i64(position)?, policy.reference.policy_id.as_str(), policy.reference.revision_id.as_str(), policy.reference.definition_digest.as_str(), serde_json::to_string(policy).map_err(json_error)?],
            ).map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(plan.id.clone())
    }

    fn validate_plan_references(&self, plan: &ResolvedPlan) -> Result<(), LabError> {
        for (dataset, digest) in &plan.dataset_digests {
            let stored: String = self
                .connection
                .query_row(
                    "SELECT semantic_digest FROM datasets WHERE id=?1",
                    [dataset.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    LabError::InvalidConfig(format!("plan references unknown dataset {dataset}"))
                })?;
            if stored != digest.as_str() {
                return Err(LabError::InputHashMismatch(format!(
                    "plan dataset digest mismatch for {dataset}"
                )));
            }
        }
        if let Some(evidence_id) = &plan.spec.evidence_snapshot_id {
            let stored: String = self
                .connection
                .query_row(
                    "SELECT digest FROM evidence_snapshots WHERE id=?1",
                    [evidence_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?
                .ok_or_else(|| {
                    LabError::InvalidConfig(format!(
                        "plan references unknown Evidence snapshot {evidence_id}"
                    ))
                })?;
            if plan
                .evidence_digest
                .as_ref()
                .is_none_or(|digest| digest.as_str() != stored)
            {
                return Err(LabError::InputHashMismatch(
                    "plan Evidence digest mismatch".into(),
                ));
            }
        } else if plan.evidence_digest.is_some() {
            return Err(LabError::Conflict(
                "plan Evidence digest exists without snapshot id".into(),
            ));
        }
        for policy in &plan.policy_revisions {
            validate_frozen_policy(policy)?;
            let stored = self
                .load_policy_revision(&policy.reference)?
                .ok_or_else(|| {
                    LabError::InvalidConfig("plan references unknown policy revision".into())
                })?;
            if serde_json::to_vec(&stored.snapshot).map_err(json_error)?
                != serde_json::to_vec(policy).map_err(json_error)?
            {
                return Err(LabError::InputHashMismatch(
                    "frozen plan policy differs from immutable registry revision".into(),
                ));
            }
        }
        Ok(())
    }

    /// Load one immutable original/resolved plan pair.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state or SQLite failure.
    pub fn load_plan(&self, id: &PlanId) -> Result<Option<StoredPlan>, LabError> {
        let stored = self
            .connection
            .query_row(
                "SELECT original_request_json,resolved_plan_json FROM plans WHERE id=?1",
                [id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .map(|(request, resolved)| {
                Ok::<StoredPlan, LabError>(StoredPlan {
                    request: serde_json::from_str(&request).map_err(json_error)?,
                    resolved: serde_json::from_str(&resolved).map_err(json_error)?,
                })
            })
            .transpose()?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.resolved.id != *id {
            return Err(LabError::DataCorrupt(
                "stored plan JSON id differs from row id".into(),
            ));
        }
        validate_plan_shape(&stored.request, &stored.resolved)?;
        self.validate_plan_references(&stored.resolved)?;
        self.validate_stored_plan_policies(id, &stored.resolved)?;
        Ok(Some(stored))
    }

    fn validate_stored_plan_policies(
        &self,
        id: &PlanId,
        plan: &ResolvedPlan,
    ) -> Result<(), LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT frozen_json FROM plan_policy_revisions WHERE plan_id=?1 ORDER BY position",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([id.as_str()], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        if rows.len() != plan.policy_revisions.len() {
            return Err(LabError::DataCorrupt(
                "stored plan policy closure cardinality mismatch".into(),
            ));
        }
        for (json, frozen) in rows.iter().zip(&plan.policy_revisions) {
            if serde_json::to_vec(frozen).map_err(json_error)? != json.as_bytes() {
                return Err(LabError::DataCorrupt(
                    "stored plan frozen policy body mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}
