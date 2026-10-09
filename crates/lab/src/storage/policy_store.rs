use super::{Store, enum_text, json_error, sql_error, timestamp_from_ms, timestamp_ms};
use crate::contracts::{
    ContentHash, FrozenPolicyRevision, HistoryPage, LabError, PolicyDefinition, PolicyHeader,
    PolicyId, PolicyOrigin, PolicyRevision, PolicyRevisionId, PolicyRevisionRef,
    PolicyRevisionSummary, PolicySweepResult, PolicyWrite, RequestId, StrategyKind, SweepCandidate,
    SweepPlan, UtcTimestamp,
};
use rusqlite::{OptionalExtension, Transaction, params};

const MAX_HISTORY_LIMIT: u32 = 100;

const BUILTIN_SEEDED_FLAG: &str = "builtin_policies_seeded";

/// One sweep candidate resolved to its persisted (or reused) revision.
struct ResolvedSweepRevision {
    reference: PolicyRevisionRef,
    revision_number: u32,
    parent_revision_id: Option<PolicyRevisionId>,
    name: String,
    request_id: RequestId,
    created_at: UtcTimestamp,
}

struct AdmittedSweep {
    plan: SweepPlan,
    request_digest: ContentHash,
}

impl Store {
    /// Read a persisted one-time marker from `application_metadata`.
    pub(crate) fn application_flag(&self, key: &str) -> Result<bool, LabError> {
        Ok(self
            .connection
            .query_row(
                "SELECT value FROM application_metadata WHERE key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
            .is_some_and(|value| value == "1"))
    }

    pub(crate) fn set_application_flag(&mut self, key: &str) -> Result<(), LabError> {
        self.connection
            .execute(
                "INSERT INTO application_metadata(key,value) VALUES (?1,'1') ON CONFLICT(key) DO UPDATE SET value='1'",
                params![key],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Seed built-in policies exactly once per data root.
    ///
    /// The persisted flag separates first-use seeding from explicit
    /// reinstallation: a deleted built-in policy is never silently re-created
    /// on restart. Explicit reseeding goes through `seed_builtin_policies`.
    ///
    /// # Errors
    /// Reports invalid seed definitions or SQLite failure.
    pub fn seed_builtin_policies_once(
        &mut self,
        definitions: &[(PolicyId, StrategyKind, PolicyDefinition)],
        now: UtcTimestamp,
    ) -> Result<bool, LabError> {
        if self.application_flag(BUILTIN_SEEDED_FLAG)? {
            return Ok(false);
        }
        self.seed_builtin_policies(definitions, now)?;
        self.set_application_flag(BUILTIN_SEEDED_FLAG)?;
        Ok(true)
    }

    /// Create or append one immutable policy revision with request idempotency and parent CAS.
    ///
    /// # Errors
    /// Rejects invalid definitions, conflicting request reuse, stale parents, or SQLite failure.
    #[expect(
        clippy::too_many_lines,
        reason = "single policy create/revise state transition keeps idempotency and parent CAS together"
    )]
    pub fn write_policy(
        &mut self,
        write: &PolicyWrite,
        now: UtcTimestamp,
    ) -> Result<PolicyRevision, LabError> {
        let request_digest = ContentHash::of_value(write)?;
        let request_id = policy_write_request_id(write);
        if let Some((digest, policy_id, revision_id)) = self.connection.query_row(
            "SELECT request_digest,policy_id,revision_id FROM policy_revisions WHERE request_id=?1",
            [request_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?)),
        ).optional().map_err(sql_error)? {
            if digest != request_digest.as_str() {
                return Err(LabError::Conflict(
                    "policy request id already has different normalized content".into(),
                ));
            }
            let reference = self.reference_for(&PolicyId::new(policy_id)?, &PolicyRevisionId::new(revision_id)?)?;
            return self.load_policy_revision(&reference)?.ok_or_else(|| {
                LabError::DataCorrupt("idempotent policy revision disappeared".into())
            });
        }
        match write {
            PolicyWrite::Sweep { .. } | PolicyWrite::Preflight { .. } => {
                Err(LabError::InvalidConfig(
                    "parameter sweep actions must use their dedicated read/materialization path"
                        .into(),
                ))
            }
            PolicyWrite::Create {
                request_id,
                definition,
            } => {
                definition.validate()?;
                let policy_id = PolicyId::from_seed(request_id.as_str());
                let exists = self
                    .connection
                    .query_row(
                        "SELECT 1 FROM policies WHERE policy_id=?1",
                        [policy_id.as_str()],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(sql_error)?;
                if exists.is_some() {
                    return Err(LabError::Conflict(
                        "derived policy id already exists for another request".into(),
                    ));
                }
                let transaction = self.connection.transaction().map_err(sql_error)?;
                let revision = insert_policy_revision(
                    &transaction,
                    &policy_id,
                    StrategyKind::Other,
                    PolicyOrigin::User,
                    request_id,
                    &request_digest,
                    None,
                    1,
                    definition,
                    now,
                    true,
                )?;
                transaction.commit().map_err(sql_error)?;
                Ok(revision)
            }
            PolicyWrite::Revise {
                request_id,
                policy_id,
                expected_parent_revision_id,
                definition,
            } => {
                definition.validate()?;
                let transaction = self.connection.transaction().map_err(sql_error)?;
                let (family, origin, head, count): (String, String, String, i64) = transaction
                    .query_row(
                        "SELECT family,origin,head_revision_id,revision_count FROM policies WHERE policy_id=?1",
                        [policy_id.as_str()],
                        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
                    )
                    .optional()
                    .map_err(sql_error)?
                    .ok_or_else(|| LabError::InvalidConfig("unknown policy".into()))?;
                if head != expected_parent_revision_id.as_str() {
                    return Err(LabError::Conflict(
                        "policy head changed before revision".into(),
                    ));
                }
                let revision_number = u32::try_from(count)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| LabError::ResourceLimit("policy revision overflow".into()))?;
                let family = enum_from_text(&family)?;
                let origin = enum_from_text(&origin)?;
                let revision = insert_policy_revision(
                    &transaction,
                    policy_id,
                    family,
                    origin,
                    request_id,
                    &request_digest,
                    Some(expected_parent_revision_id),
                    revision_number,
                    definition,
                    now,
                    false,
                )?;
                let changed = transaction.execute(
                    "UPDATE policies SET head_revision_id=?1,revision_count=?2,name=?3 WHERE policy_id=?4 AND head_revision_id=?5 AND revision_count=?6",
                    params![revision.snapshot.reference.revision_id.as_str(), revision_number, definition.name, policy_id.as_str(), expected_parent_revision_id.as_str(), count],
                ).map_err(sql_error)?;
                if changed != 1 {
                    return Err(LabError::Conflict("policy revision CAS failed".into()));
                }
                transaction.commit().map_err(sql_error)?;
                Ok(revision)
            }
        }
    }

    /// Expand a parameter sweep into one immutable policy per candidate.
    ///
    /// Identical parameter sets reuse the existing policy (deterministic ids
    /// derived from the definition digest), so reruns never multiply revisions.
    /// # Errors
    /// Rejects invalid sweeps, conflicting request reuse or SQLite failure.
    pub fn sweep_policy(
        &mut self,
        request_id: &RequestId,
        family: StrategyKind,
        template: &crate::contracts::StrategySpec,
        mode: &crate::contracts::ParameterSweepMode,
        research: Option<&crate::contracts::SweepResearchContext>,
        now: UtcTimestamp,
    ) -> Result<PolicySweepResult, LabError> {
        let admitted = admit_sweep(request_id, family, template, mode, research)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let resolved = admitted
            .plan
            .candidates
            .iter()
            .map(|candidate| {
                resolve_sweep_candidate(
                    &transaction,
                    request_id,
                    family,
                    candidate,
                    &admitted.request_digest,
                    now,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().map_err(sql_error)?;
        let revisions = resolved
            .into_iter()
            .map(|row| PolicyRevisionSummary {
                reference: row.reference,
                revision_number: row.revision_number,
                parent_revision_id: row.parent_revision_id,
                name: row.name,
                created_at: row.created_at,
                request_id: row.request_id,
            })
            .collect();
        Ok(PolicySweepResult {
            plan: admitted.plan,
            revisions,
        })
    }

    /// Seed canonical built-ins once without replacing an existing edited head.
    ///
    /// # Errors
    /// Rejects invalid definitions/identities or SQLite failure.
    pub fn seed_builtin_policies(
        &mut self,
        definitions: &[(PolicyId, StrategyKind, PolicyDefinition)],
        now: UtcTimestamp,
    ) -> Result<(), LabError> {
        for (policy_id, family, definition) in definitions {
            if *family == StrategyKind::Other {
                return Err(LabError::InvalidConfig(
                    "built-in seed requires a fixed strategy family".into(),
                ));
            }
            definition.validate()?;
            let exists = self
                .connection
                .query_row(
                    "SELECT 1 FROM policies WHERE policy_id=?1",
                    [policy_id.as_str()],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sql_error)?;
            if exists.is_some() {
                continue;
            }
            let request_id = RequestId::from_seed(&format!("builtin:{}:v1", policy_id.as_str()));
            let request_digest =
                ContentHash::of_value(&("builtin-seed-v1", policy_id, family, definition))?;
            let transaction = self.connection.transaction().map_err(sql_error)?;
            insert_policy_revision(
                &transaction,
                policy_id,
                *family,
                PolicyOrigin::Builtin,
                &request_id,
                &request_digest,
                None,
                1,
                definition,
                now,
                true,
            )?;
            transaction.commit().map_err(sql_error)?;
        }
        Ok(())
    }

    /// Load one exact immutable policy body by full revision reference.
    ///
    /// # Errors
    /// Returns an error for corrupt identity/body state or SQLite failure.
    pub fn load_policy_revision(
        &self,
        reference: &PolicyRevisionRef,
    ) -> Result<Option<PolicyRevision>, LabError> {
        let row = self.connection.query_row(
            "SELECT p.family,p.origin,r.revision_number,r.parent_revision_id,r.request_id,r.created_at_ms,r.definition_json,r.definition_digest FROM policy_revisions r JOIN policies p ON p.policy_id=r.policy_id WHERE r.policy_id=?1 AND r.revision_id=?2",
            params![reference.policy_id.as_str(), reference.revision_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, u32>(2)?,row.get::<_, Option<String>>(3)?,row.get::<_, String>(4)?,row.get::<_, i64>(5)?,row.get::<_, String>(6)?,row.get::<_, String>(7)?)),
        ).optional().map_err(sql_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.7 != reference.definition_digest.as_str() {
            return Err(LabError::InputHashMismatch(
                "policy reference definition digest mismatch".into(),
            ));
        }
        let definition: PolicyDefinition = serde_json::from_str(&row.6).map_err(json_error)?;
        definition.validate()?;
        if ContentHash::of_value(&definition)? != reference.definition_digest {
            return Err(LabError::DataCorrupt(
                "stored policy body digest mismatch".into(),
            ));
        }
        Ok(Some(PolicyRevision {
            snapshot: FrozenPolicyRevision {
                reference: reference.clone(),
                revision_number: row.2,
                parent_revision_id: row.3.map(PolicyRevisionId::new).transpose()?,
                family: enum_from_text(&row.0)?,
                origin: enum_from_text(&row.1)?,
                definition,
            },
            request_id: RequestId::new(row.4)?,
            created_at: timestamp_from_ms(row.5)?,
        }))
    }

    /// List policy heads in stable policy-id order.
    ///
    /// # Errors
    /// Rejects invalid limits/cursors or corrupt SQLite state.
    pub fn list_policies(
        &self,
        after: Option<&PolicyId>,
        limit: u32,
    ) -> Result<HistoryPage<PolicyHeader, PolicyId>, LabError> {
        validate_history_limit(limit)?;
        let total = count_u64(&self.connection, "SELECT COUNT(*) FROM policies", [])?;
        let mut statement = self.connection.prepare(
            "SELECT policy_id,family,origin,created_at_ms,name,head_revision_id,revision_count FROM policies WHERE (?1 IS NULL OR policy_id>?1) ORDER BY policy_id LIMIT ?2",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![after.map(PolicyId::as_str), i64::from(limit) + 1],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, u32>(6)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut items = Vec::new();
        for row in rows {
            let row = row.map_err(sql_error)?;
            let policy_id = PolicyId::new(row.0)?;
            let revision_id = PolicyRevisionId::new(row.5)?;
            let reference = self.reference_for(&policy_id, &revision_id)?;
            items.push(PolicyHeader {
                policy_id,
                family: enum_from_text(&row.1)?,
                origin: enum_from_text(&row.2)?,
                created_at: timestamp_from_ms(row.3)?,
                name: row.4,
                head: reference,
                revision_count: row.6,
            });
        }
        page_by_extra(items, limit, total, |item| item.policy_id.clone())
    }

    /// List immutable revision summaries newest-first without definition bodies.
    ///
    /// # Errors
    /// Rejects invalid limits/cursors or corrupt SQLite state.
    pub fn policy_history(
        &self,
        policy_id: &PolicyId,
        before: Option<u32>,
        limit: u32,
    ) -> Result<HistoryPage<PolicyRevisionSummary, u32>, LabError> {
        validate_history_limit(limit)?;
        let total = count_u64(
            &self.connection,
            "SELECT COUNT(*) FROM policy_revisions WHERE policy_id=?1",
            [policy_id.as_str()],
        )?;
        let mut statement = self.connection.prepare(
            "SELECT revision_id,definition_digest,revision_number,parent_revision_id,name,created_at_ms,request_id FROM policy_revisions WHERE policy_id=?1 AND (?2 IS NULL OR revision_number<?2) ORDER BY revision_number DESC LIMIT ?3",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![policy_id.as_str(), before, i64::from(limit) + 1],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut items = Vec::new();
        for row in rows {
            let row = row.map_err(sql_error)?;
            items.push(PolicyRevisionSummary {
                reference: PolicyRevisionRef {
                    policy_id: policy_id.clone(),
                    revision_id: PolicyRevisionId::new(row.0)?,
                    definition_digest: ContentHash::try_from(row.1)?,
                },
                revision_number: row.2,
                parent_revision_id: row.3.map(PolicyRevisionId::new).transpose()?,
                name: row.4,
                created_at: timestamp_from_ms(row.5)?,
                request_id: RequestId::new(row.6)?,
            });
        }
        page_by_extra(items, limit, total, |item| item.revision_number)
    }

    pub(super) fn reference_for(
        &self,
        policy_id: &PolicyId,
        revision_id: &PolicyRevisionId,
    ) -> Result<PolicyRevisionRef, LabError> {
        let digest: String = self.connection.query_row(
            "SELECT definition_digest FROM policy_revisions WHERE policy_id=?1 AND revision_id=?2",
            params![policy_id.as_str(), revision_id.as_str()],
            |row| row.get(0),
        ).map_err(sql_error)?;
        Ok(PolicyRevisionRef {
            policy_id: policy_id.clone(),
            revision_id: revision_id.clone(),
            definition_digest: ContentHash::try_from(digest)?,
        })
    }
}

fn admit_sweep(
    request_id: &RequestId,
    family: StrategyKind,
    template: &crate::contracts::StrategySpec,
    mode: &crate::contracts::ParameterSweepMode,
    research: Option<&crate::contracts::SweepResearchContext>,
) -> Result<AdmittedSweep, LabError> {
    let prepared =
        crate::contracts::sweep::prepare_parameter_sweep(family, template, mode, research)?;
    if !prepared.report.resource_admissible {
        return Err(LabError::ResourceLimit(
            prepared
                .report
                .reject_reason
                .unwrap_or_else(|| "parameter sweep was not admitted".into()),
        ));
    }
    let plan = prepared
        .plan
        .ok_or_else(|| LabError::Internal("admitted sweep lost its candidate plan".into()))?;
    let request_digest = ContentHash::of_value(&(
        "policy-sweep-v2",
        request_id,
        family,
        template,
        mode,
        research,
    ))?;
    Ok(AdmittedSweep {
        plan,
        request_digest,
    })
}

fn resolve_sweep_candidate(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    family: StrategyKind,
    candidate: &SweepCandidate,
    request_digest: &ContentHash,
    now: UtcTimestamp,
) -> Result<ResolvedSweepRevision, LabError> {
    let policy_id = PolicyId::from_seed(&format!("sweep:{}", candidate.definition_digest.as_str()));
    if let Some(existing) = load_exact_sweep_revision(transaction, &policy_id, candidate)? {
        return Ok(existing);
    }
    let policy_exists = transaction
        .query_row(
            "SELECT 1 FROM policies WHERE policy_id=?1",
            [policy_id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(sql_error)?
        .is_some();
    if policy_exists {
        return Err(LabError::DataCorrupt(
            "sweep policy identity has no exact definition revision".into(),
        ));
    }
    let candidate_request_id =
        RequestId::from_seed(&format!("{request_id}:{}", candidate.definition_digest));
    let revision = insert_policy_revision(
        transaction,
        &policy_id,
        family,
        PolicyOrigin::User,
        &candidate_request_id,
        request_digest,
        None,
        1,
        &candidate.definition,
        now,
        true,
    )?;
    Ok(ResolvedSweepRevision {
        reference: revision.snapshot.reference.clone(),
        revision_number: revision.snapshot.revision_number,
        parent_revision_id: revision.snapshot.parent_revision_id.clone(),
        name: revision.snapshot.definition.name.clone(),
        request_id: revision.request_id.clone(),
        created_at: revision.created_at,
    })
}

fn load_exact_sweep_revision(
    transaction: &Transaction<'_>,
    policy_id: &PolicyId,
    candidate: &SweepCandidate,
) -> Result<Option<ResolvedSweepRevision>, LabError> {
    let exact: Option<(String, i64, String, String, i64, String)> = transaction
        .query_row(
            "SELECT revision_id,revision_number,definition_digest,name,\
             created_at_ms,request_id \
             FROM policy_revisions WHERE policy_id=?1 AND definition_digest=?2 \
             ORDER BY revision_number DESC LIMIT 1",
            params![policy_id.as_str(), candidate.definition_digest.as_str()],
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
        .map_err(sql_error)?;
    let Some((revision_id, revision_number, definition_digest, name, created_ms, request)) = exact
    else {
        return Ok(None);
    };
    if definition_digest != candidate.definition_digest.as_str() {
        return Err(LabError::DataCorrupt(
            "sweep policy identity resolved to a different definition".into(),
        ));
    }
    Ok(Some(ResolvedSweepRevision {
        reference: PolicyRevisionRef {
            policy_id: policy_id.clone(),
            revision_id: PolicyRevisionId::new(revision_id)?,
            definition_digest: ContentHash::try_from(definition_digest)?,
        },
        revision_number: u32::try_from(revision_number)
            .map_err(|_| LabError::DataCorrupt("policy revision number overflow".into()))?,
        parent_revision_id: None,
        name,
        request_id: RequestId::new(request)?,
        created_at: timestamp_from_ms(created_ms)?,
    }))
}

#[expect(
    clippy::too_many_arguments,
    reason = "single immutable revision insertion keeps lineage and policy head initialization atomic"
)]
fn insert_policy_revision(
    transaction: &Transaction<'_>,
    policy_id: &PolicyId,
    family: StrategyKind,
    origin: PolicyOrigin,
    request_id: &RequestId,
    request_digest: &ContentHash,
    parent: Option<&PolicyRevisionId>,
    revision_number: u32,
    definition: &PolicyDefinition,
    created_at: UtcTimestamp,
    create_policy: bool,
) -> Result<PolicyRevision, LabError> {
    // Return the same clock precision persisted by SQLite so admitted retries
    // read back byte-identical revision JSON, including submillisecond callers.
    let created_at = timestamp_from_ms(timestamp_ms(created_at))?;
    let definition_digest = ContentHash::of_value(definition)?;
    let revision_id = PolicyRevisionId::from_seed(&format!(
        "{}:{revision_number}:{}",
        policy_id.as_str(),
        definition_digest.as_str()
    ));
    let reference = PolicyRevisionRef {
        policy_id: policy_id.clone(),
        revision_id: revision_id.clone(),
        definition_digest: definition_digest.clone(),
    };
    if create_policy {
        transaction.execute(
            "INSERT INTO policies(policy_id,family,origin,created_at_ms,head_revision_id,revision_count,name) VALUES (?1,?2,?3,?4,?5,1,?6)",
            params![policy_id.as_str(), enum_text(&family)?, enum_text(&origin)?, timestamp_ms(created_at), revision_id.as_str(), definition.name],
        ).map_err(sql_error)?;
    }
    transaction.execute(
        "INSERT INTO policy_revisions(policy_id,revision_id,revision_number,parent_revision_id,request_id,request_digest,definition_digest,created_at_ms,name,definition_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![policy_id.as_str(), revision_id.as_str(), revision_number, parent.map(PolicyRevisionId::as_str), request_id.as_str(), request_digest.as_str(), definition_digest.as_str(), timestamp_ms(created_at), definition.name, serde_json::to_string(definition).map_err(json_error)?],
    ).map_err(sql_error)?;
    Ok(PolicyRevision {
        snapshot: FrozenPolicyRevision {
            reference,
            revision_number,
            parent_revision_id: parent.cloned(),
            family,
            origin,
            definition: definition.clone(),
        },
        request_id: request_id.clone(),
        created_at,
    })
}

fn policy_write_request_id(write: &PolicyWrite) -> &RequestId {
    match write {
        PolicyWrite::Create { request_id, .. }
        | PolicyWrite::Revise { request_id, .. }
        | PolicyWrite::Sweep { request_id, .. }
        | PolicyWrite::Preflight { request_id, .. } => request_id,
    }
}

fn validate_history_limit(limit: u32) -> Result<(), LabError> {
    if !(1..=MAX_HISTORY_LIMIT).contains(&limit) {
        return Err(LabError::ResourceLimit(
            "history limit must be 1..=100".into(),
        ));
    }
    Ok(())
}

fn enum_from_text<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, LabError> {
    serde_json::from_value(serde_json::Value::String(text.into())).map_err(json_error)
}

fn count_u64<P: rusqlite::Params>(
    connection: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<u64, LabError> {
    let value: i64 = connection
        .query_row(sql, params, |row| row.get(0))
        .map_err(sql_error)?;
    u64::try_from(value).map_err(|_| LabError::DataCorrupt("negative history count".into()))
}

fn page_by_extra<T, C>(
    mut items: Vec<T>,
    limit: u32,
    total_count: u64,
    cursor: impl Fn(&T) -> C,
) -> Result<HistoryPage<T, C>, LabError> {
    let limit = usize::try_from(limit)
        .map_err(|_| LabError::ResourceLimit("history limit exceeds platform capacity".into()))?;
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    let next_cursor = if has_more {
        items.last().map(cursor)
    } else {
        None
    };
    let returned_count = u64::try_from(items.len())
        .map_err(|_| LabError::ResourceLimit("history result count overflow".into()))?;
    Ok(HistoryPage {
        returned_count,
        items,
        total_count,
        next_cursor,
        truncated_reason: has_more.then(|| "LIMIT".into()),
    })
}
