//! Durable policy-only deletion receipts.
//!
//! This path deliberately does not call the generic hard-delete executor. It
//! recomputes the existing closure under an immediate transaction and admits
//! an effect only when the policy and its owned revisions are the entire
//! positive deletion scope.

use super::{DeleteScope, Store, scope_digest};
use crate::contracts::{
    ContentHash, DeleteBlockerClass, DeleteResource, LabError, MAX_POLICY_DELETE_ITEMS,
    PolicyDeleteCounts, PolicyDeleteEffect, PolicyDeleteItemReadback, PolicyDeleteItemStatus,
    PolicyDeletePreviewEcho, PolicyDeleteReadback, PolicyDeleteReasonCode,
    PolicyDeleteRemediationCode, PolicyDeleteRequestState, PolicyDeleteWriteError,
    PolicyDeleteWriteErrorCode, PolicyDeleteWriteErrorDetail, PolicyDeleteWriteRequest, PolicyId,
    RequestId, UtcTimestamp,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;

const POLICY_DELETE_TTL_MS: i64 = 600_000;

#[derive(Debug)]
pub(crate) enum PolicyDeleteAdmission {
    Started,
    Existing(PolicyDeleteReadback),
    Rejected(PolicyDeleteWriteError),
}

#[derive(Debug)]
struct StoredItem {
    preview_scope_digest: Option<ContentHash>,
    preview_digest: ContentHash,
    expires_at_ms: Option<i64>,
    confirmation_token: Option<ContentHash>,
    status: PolicyDeleteItemStatus,
}

#[derive(Debug)]
struct StoredRequestItem {
    item: StoredItem,
    confirmation_text: String,
    requested_count: i64,
}

#[derive(Debug)]
struct CurrentPolicy {
    origin: String,
    head_revision_id: String,
    revision_count: i64,
    name: String,
}

#[derive(Debug)]
struct PreviewProof {
    scope_digest: ContentHash,
    expires_at_ms: i64,
    confirmation_token: ContentHash,
}

#[derive(Debug)]
enum PolicyDeleteDecision {
    Terminal(TerminalItem),
    Delete { revision_count: i64 },
}

#[derive(Debug, Clone, Copy)]
struct TerminalItem {
    status: PolicyDeleteItemStatus,
    reason: Option<PolicyDeleteReasonCode>,
    remediation: Option<PolicyDeleteRemediationCode>,
    deleted_revision_count: u64,
    retained: bool,
    effect: PolicyDeleteEffect,
}

impl Store {
    /// Admit one bounded explicit policy deletion request or return its durable readback.
    pub(crate) fn begin_policy_delete_request(
        &mut self,
        request: &PolicyDeleteWriteRequest,
    ) -> Result<PolicyDeleteAdmission, LabError> {
        if request.items.len() > MAX_POLICY_DELETE_ITEMS {
            return Ok(rejected(
                request,
                None,
                None,
                PolicyDeleteWriteErrorCode::SelectionLimit,
                None,
                None,
                PolicyDeleteEffect::NotApplied,
            ));
        }
        let submitted_digest = request_digest(request)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(super::sql_error)?;
        if let Some(existing) =
            existing_request_admission(&transaction, request, &submitted_digest)?
        {
            return Ok(existing);
        }
        if let Some(rejection) = selection_rejection(request, Some(submitted_digest.clone())) {
            return Ok(rejection);
        }
        if !confirmation_matches(&transaction, request)? {
            return Ok(confirmation_rejection(request, submitted_digest));
        }
        let now_ms = timestamp_ms(UtcTimestamp::now());
        insert_pending_request(&transaction, request, &submitted_digest, now_ms)?;
        transaction.commit().map_err(super::sql_error)?;
        Ok(PolicyDeleteAdmission::Started)
    }

    /// Revalidate and process one pending item in one immediate SQL transaction.
    pub(crate) fn apply_policy_delete_item(
        &mut self,
        request_id: &RequestId,
        policy_id: &PolicyId,
    ) -> Result<(), LabError> {
        let transaction =
            Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)
                .map_err(super::sql_error)?;
        // This clock is sampled only after BEGIN IMMEDIATE has obtained its lock.
        let now_ms = timestamp_ms(UtcTimestamp::now());
        let stored = load_request_item(&transaction, request_id, policy_id)?;
        if stored.item.status != PolicyDeleteItemStatus::Pending {
            return Ok(());
        }
        let decision = self.deletion_decision(&transaction, policy_id, &stored, now_ms)?;
        let outcome = match decision {
            PolicyDeleteDecision::Terminal(outcome) => outcome,
            PolicyDeleteDecision::Delete { revision_count } => {
                delete_policy_rows(&transaction, policy_id, revision_count)?
            }
        };
        finish_item(&transaction, request_id, policy_id, outcome, now_ms)?;
        transaction.commit().map_err(super::sql_error)
    }

    fn deletion_decision(
        &self,
        transaction: &Transaction<'_>,
        policy_id: &PolicyId,
        stored: &StoredRequestItem,
        now_ms: i64,
    ) -> Result<PolicyDeleteDecision, LabError> {
        let Some(current) = load_current_policy(transaction, policy_id)? else {
            return Ok(PolicyDeleteDecision::Terminal(stale(
                PolicyDeleteReasonCode::PolicyNotFound,
                PolicyDeleteRemediationCode::FreshPreviewRequired,
                false,
            )));
        };
        if current.origin == "BUILTIN" {
            return Ok(PolicyDeleteDecision::Terminal(blocked(
                PolicyDeleteReasonCode::BuiltinPolicy,
                Some(PolicyDeleteRemediationCode::BuiltinReadOnly),
                true,
            )));
        }
        if current.origin != "USER" {
            return Ok(PolicyDeleteDecision::Terminal(blocked(
                PolicyDeleteReasonCode::UnknownOrigin,
                Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
                true,
            )));
        }
        let proof = match validate_preview_echo(policy_id, &stored.item, now_ms)? {
            Ok(proof) => proof,
            Err(outcome) => return Ok(PolicyDeleteDecision::Terminal(outcome)),
        };
        self.validate_current_policy_scope(transaction, policy_id, &current, stored, &proof)
    }

    fn validate_current_policy_scope(
        &self,
        transaction: &Transaction<'_>,
        policy_id: &PolicyId,
        current: &CurrentPolicy,
        stored: &StoredRequestItem,
        proof: &PreviewProof,
    ) -> Result<PolicyDeleteDecision, LabError> {
        let scope = self.compute_delete_scope(&DeleteResource::Policy {
            policy_id: policy_id.clone(),
        })?;
        let current_digest = scope_digest(&scope)?;
        if current_digest != proof.scope_digest {
            return Ok(PolicyDeleteDecision::Terminal(stale(
                PolicyDeleteReasonCode::ScopeChanged,
                PolicyDeleteRemediationCode::FreshPreviewRequired,
                true,
            )));
        }
        let cascade_required = scope
            .blockers
            .iter()
            .any(|blocker| blocker.class == DeleteBlockerClass::LiveReference);
        let expected_token = ContentHash::of_value(&serde_json::json!([
            current_digest.as_str(),
            proof.expires_at_ms,
            cascade_required
        ]))?;
        if expected_token != proof.confirmation_token {
            return Ok(PolicyDeleteDecision::Terminal(stale(
                PolicyDeleteReasonCode::TokenMismatch,
                PolicyDeleteRemediationCode::FreshPreviewRequired,
                true,
            )));
        }
        if !single_confirmation_still_matches(
            transaction,
            &stored.confirmation_text,
            stored.requested_count,
            policy_id,
            &current.name,
        )? {
            return Ok(PolicyDeleteDecision::Terminal(stale(
                PolicyDeleteReasonCode::ScopeChanged,
                PolicyDeleteRemediationCode::FreshPreviewRequired,
                true,
            )));
        }
        if !revision_head_is_consistent(
            transaction,
            policy_id,
            &current.head_revision_id,
            current.revision_count,
        )? {
            return Ok(PolicyDeleteDecision::Terminal(stale(
                PolicyDeleteReasonCode::ScopeChanged,
                PolicyDeleteRemediationCode::FreshPreviewRequired,
                true,
            )));
        }
        if let Some(outcome) = protected_scope_outcome(&scope, policy_id, current.revision_count)? {
            return Ok(PolicyDeleteDecision::Terminal(outcome));
        }
        Ok(PolicyDeleteDecision::Delete {
            revision_count: current.revision_count,
        })
    }

    /// Read the durable request receipt without touching policies or resuming work.
    pub(crate) fn policy_delete_readback(
        &self,
        request_id: &RequestId,
    ) -> Result<PolicyDeleteReadback, LabError> {
        readback_from_connection(&self.connection, request_id)
    }
}

fn existing_request_admission(
    connection: &Connection,
    request: &PolicyDeleteWriteRequest,
    submitted_digest: &ContentHash,
) -> Result<Option<PolicyDeleteAdmission>, LabError> {
    let existing = connection
        .query_row(
            "SELECT request_digest FROM policy_delete_requests WHERE request_id=?1",
            [request.request_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(super::sql_error)?;
    let Some(existing_digest) = existing else {
        return Ok(None);
    };
    let existing_digest = parse_hash(existing_digest)?;
    if submitted_digest == &existing_digest {
        let readback = readback_from_connection(connection, &request.request_id)?;
        return Ok(Some(PolicyDeleteAdmission::Existing(readback)));
    }
    Ok(Some(rejected(
        request,
        Some(submitted_digest.clone()),
        Some(existing_digest),
        PolicyDeleteWriteErrorCode::RequestIdConflict,
        None,
        None,
        PolicyDeleteEffect::NoNewEffect,
    )))
}

fn selection_rejection(
    request: &PolicyDeleteWriteRequest,
    submitted_digest: Option<ContentHash>,
) -> Option<PolicyDeleteAdmission> {
    if request.items.is_empty() {
        return Some(rejected(
            request,
            submitted_digest,
            None,
            PolicyDeleteWriteErrorCode::InvalidSelection,
            None,
            None,
            PolicyDeleteEffect::NotApplied,
        ));
    }
    let mut seen = BTreeSet::new();
    request
        .items
        .iter()
        .enumerate()
        .find(|(_, item)| !seen.insert(item.policy_id.clone()))
        .map(|(index, item)| {
            rejected(
                request,
                submitted_digest,
                None,
                PolicyDeleteWriteErrorCode::DuplicatePolicyId,
                Some(index),
                Some(item.policy_id.clone()),
                PolicyDeleteEffect::NotApplied,
            )
        })
}

fn confirmation_rejection(
    request: &PolicyDeleteWriteRequest,
    submitted_digest: ContentHash,
) -> PolicyDeleteAdmission {
    let (index, policy_id) = if request.items.len() == 1 {
        (Some(0), Some(request.items[0].policy_id.clone()))
    } else {
        (None, None)
    };
    rejected(
        request,
        Some(submitted_digest),
        None,
        PolicyDeleteWriteErrorCode::ConfirmationMismatch,
        index,
        policy_id,
        PolicyDeleteEffect::NotApplied,
    )
}

fn insert_pending_request(
    transaction: &Transaction<'_>,
    request: &PolicyDeleteWriteRequest,
    digest: &ContentHash,
    now_ms: i64,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO policy_delete_requests(request_id,request_digest,confirmation_text,requested_count,state,created_at_ms,updated_at_ms) VALUES (?1,?2,?3,?4,'PENDING',?5,?5)",
            params![
                request.request_id.as_str(),
                digest.as_str(),
                request.confirmation_text,
                usize_to_i64(request.items.len())?,
                now_ms,
            ],
        )
        .map_err(super::sql_error)?;
    let mut items = request.items.iter().collect::<Vec<_>>();
    items.sort_by(|lhs, rhs| lhs.policy_id.cmp(&rhs.policy_id));
    for (position, item) in items.into_iter().enumerate() {
        let preview = item.preview.as_ref();
        let scope_digest = preview.and_then(|echo| echo.scope_digest.as_ref());
        let expires_at_ms = preview.and_then(|echo| echo.expires_at).map(timestamp_ms);
        let confirmation_token = preview.and_then(|echo| echo.confirmation_token.as_ref());
        let preview_digest = preview_digest(&item.policy_id, preview)?;
        let retained: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM policies WHERE policy_id=?1)",
                [item.policy_id.as_str()],
                |row| row.get(0),
            )
            .map_err(super::sql_error)?;
        transaction
            .execute(
                "INSERT INTO policy_delete_request_items(request_id,position,policy_id,preview_scope_digest,preview_digest,preview_expires_at_ms,preview_confirmation_token,status,reason_code,remediation_code,deleted_revision_count,retained,effect) VALUES (?1,?2,?3,?4,?5,?6,?7,'PENDING',NULL,NULL,0,?8,'NOT_APPLIED')",
                params![
                    request.request_id.as_str(),
                    usize_to_i64(position)?,
                    item.policy_id.as_str(),
                    scope_digest.map(ContentHash::as_str),
                    preview_digest.as_str(),
                    expires_at_ms,
                    confirmation_token.map(ContentHash::as_str),
                    retained,
                ],
            )
            .map_err(super::sql_error)?;
    }
    Ok(())
}

fn load_request_item(
    connection: &Connection,
    request_id: &RequestId,
    policy_id: &PolicyId,
) -> Result<StoredRequestItem, LabError> {
    let stored = connection
        .query_row(
            "SELECT i.preview_scope_digest,i.preview_digest,i.preview_expires_at_ms,i.preview_confirmation_token,i.status,r.confirmation_text,r.requested_count FROM policy_delete_request_items i JOIN policy_delete_requests r USING(request_id) WHERE i.request_id=?1 AND i.policy_id=?2",
            params![request_id.as_str(), policy_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()
        .map_err(super::sql_error)?;
    let Some((scope, digest, expires_at_ms, token, status, confirmation_text, requested_count)) =
        stored
    else {
        return Err(LabError::InvalidConfig(
            "policy delete item is not part of the durable request".into(),
        ));
    };
    Ok(StoredRequestItem {
        item: StoredItem {
            preview_scope_digest: scope.map(parse_hash).transpose()?,
            preview_digest: parse_hash(digest)?,
            expires_at_ms,
            confirmation_token: token.map(parse_hash).transpose()?,
            status: parse_enum(&status)?,
        },
        confirmation_text,
        requested_count,
    })
}

fn load_current_policy(
    connection: &Connection,
    policy_id: &PolicyId,
) -> Result<Option<CurrentPolicy>, LabError> {
    connection
        .query_row(
            "SELECT origin,head_revision_id,revision_count,name FROM policies WHERE policy_id=?1",
            [policy_id.as_str()],
            |row| {
                Ok(CurrentPolicy {
                    origin: row.get(0)?,
                    head_revision_id: row.get(1)?,
                    revision_count: row.get(2)?,
                    name: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(super::sql_error)
}

fn validate_preview_echo(
    policy_id: &PolicyId,
    stored: &StoredItem,
    now_ms: i64,
) -> Result<Result<PreviewProof, TerminalItem>, LabError> {
    let Some(expires_at_ms) = stored.expires_at_ms else {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewMissing,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    };
    let Some(scope_digest) = stored.preview_scope_digest.as_ref() else {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewIncomplete,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    };
    let Some(confirmation_token) = stored.confirmation_token.as_ref() else {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewIncomplete,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    };
    if now_ms >= expires_at_ms {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewExpired,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    }
    if expires_at_ms > now_ms.saturating_add(POLICY_DELETE_TTL_MS) {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewIncomplete,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    }
    let expected_digest = preview_digest(
        policy_id,
        Some(&PolicyDeletePreviewEcho {
            scope_digest: Some(scope_digest.clone()),
            expires_at: Some(timestamp_from_ms(expires_at_ms)?),
            confirmation_token: Some(confirmation_token.clone()),
        }),
    )?;
    if expected_digest != stored.preview_digest {
        return Ok(Err(stale(
            PolicyDeleteReasonCode::PreviewIncomplete,
            PolicyDeleteRemediationCode::FreshPreviewRequired,
            true,
        )));
    }
    Ok(Ok(PreviewProof {
        scope_digest: scope_digest.clone(),
        expires_at_ms,
        confirmation_token: confirmation_token.clone(),
    }))
}

fn delete_policy_rows(
    transaction: &Transaction<'_>,
    policy_id: &PolicyId,
    revision_count: i64,
) -> Result<TerminalItem, LabError> {
    transaction
        .execute_batch("SAVEPOINT policy_delete_effect")
        .map_err(super::sql_error)?;
    let deleted_revisions = match transaction.execute(
        "DELETE FROM policy_revisions WHERE policy_id=?1",
        [policy_id.as_str()],
    ) {
        Ok(deleted) => deleted,
        Err(error) if is_constraint_violation(&error) => {
            rollback_policy_effect(transaction)?;
            return Ok(blocked(
                PolicyDeleteReasonCode::OtherResourceScope,
                Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
                true,
            ));
        }
        Err(error) => return Err(super::sql_error(error)),
    };
    let expected_revisions = usize::try_from(revision_count)
        .map_err(|_| LabError::DataCorrupt("negative policy revision count".into()))?;
    if deleted_revisions != expected_revisions {
        rollback_policy_effect(transaction)?;
        return Ok(storage_failure(true));
    }
    let deleted_policy = match transaction.execute(
        "DELETE FROM policies WHERE policy_id=?1 AND origin='USER'",
        [policy_id.as_str()],
    ) {
        Ok(deleted) => deleted,
        Err(error) if is_constraint_violation(&error) => {
            rollback_policy_effect(transaction)?;
            return Ok(blocked(
                PolicyDeleteReasonCode::OtherResourceScope,
                Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
                true,
            ));
        }
        Err(error) => return Err(super::sql_error(error)),
    };
    if deleted_policy != 1 {
        rollback_policy_effect(transaction)?;
        return Ok(storage_failure(true));
    }
    transaction
        .execute_batch("RELEASE policy_delete_effect")
        .map_err(super::sql_error)?;
    Ok(TerminalItem {
        status: PolicyDeleteItemStatus::Deleted,
        reason: None,
        remediation: None,
        deleted_revision_count: u64::try_from(deleted_revisions)
            .map_err(|_| LabError::DataCorrupt("negative delete row count".into()))?,
        retained: false,
        effect: PolicyDeleteEffect::Applied,
    })
}

fn storage_failure(retained: bool) -> TerminalItem {
    TerminalItem {
        status: PolicyDeleteItemStatus::Failed,
        reason: Some(PolicyDeleteReasonCode::StorageFailure),
        remediation: Some(PolicyDeleteRemediationCode::FreshPreviewRequired),
        deleted_revision_count: 0,
        retained,
        effect: PolicyDeleteEffect::NotApplied,
    }
}

fn rollback_policy_effect(transaction: &Transaction<'_>) -> Result<(), LabError> {
    transaction
        .execute_batch("ROLLBACK TO policy_delete_effect; RELEASE policy_delete_effect")
        .map_err(super::sql_error)
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

/// Terminalize work admitted by a previous process. This intentionally performs
/// no policy mutation and leaves all already-terminal receipts unchanged.
pub(crate) fn recover_interrupted_policy_deletes(
    connection: &mut Connection,
) -> Result<(), LabError> {
    let pending = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM policy_delete_request_items WHERE status='PENDING')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(super::sql_error)?;
    if !pending {
        return Ok(());
    }
    let now_ms = timestamp_ms(UtcTimestamp::now());
    let transaction = connection.transaction().map_err(super::sql_error)?;
    transaction
        .execute(
            "UPDATE policy_delete_request_items SET status='INTERRUPTED_NOT_APPLIED',reason_code='PROCESS_INTERRUPTED',remediation_code='FRESH_PREVIEW_REQUIRED',deleted_revision_count=0,retained=EXISTS(SELECT 1 FROM policies p WHERE p.policy_id=policy_delete_request_items.policy_id),effect='NOT_APPLIED' WHERE status='PENDING'",
            [],
        )
        .map_err(super::sql_error)?;
    transaction
        .execute(
            "UPDATE policy_delete_requests SET state=CASE \
                WHEN EXISTS(SELECT 1 FROM policy_delete_request_items i WHERE i.request_id=policy_delete_requests.request_id AND i.status='PENDING') THEN 'PENDING' \
                WHEN EXISTS(SELECT 1 FROM policy_delete_request_items i WHERE i.request_id=policy_delete_requests.request_id AND i.status='INTERRUPTED_NOT_APPLIED') THEN 'INTERRUPTED' \
                WHEN NOT EXISTS(SELECT 1 FROM policy_delete_request_items i WHERE i.request_id=policy_delete_requests.request_id AND i.status<>'DELETED') THEN 'COMPLETE' \
                ELSE 'PARTIAL' END,updated_at_ms=?1 WHERE EXISTS(SELECT 1 FROM policy_delete_request_items i WHERE i.request_id=policy_delete_requests.request_id AND i.status='INTERRUPTED_NOT_APPLIED')",
            [now_ms],
        )
        .map_err(super::sql_error)?;
    transaction.commit().map_err(super::sql_error)
}

fn rejected(
    request: &PolicyDeleteWriteRequest,
    submitted: Option<ContentHash>,
    existing: Option<ContentHash>,
    code: PolicyDeleteWriteErrorCode,
    item_index: Option<usize>,
    policy_id: Option<PolicyId>,
    effect: PolicyDeleteEffect,
) -> PolicyDeleteAdmission {
    PolicyDeleteAdmission::Rejected(PolicyDeleteWriteError {
        error: PolicyDeleteWriteErrorDetail {
            request_id: request.request_id.clone(),
            submitted_request_digest: submitted,
            existing_request_digest: existing,
            code,
            item_index: item_index.and_then(|index| u32::try_from(index).ok()),
            policy_id,
            effect,
        },
    })
}

fn request_digest(request: &PolicyDeleteWriteRequest) -> Result<ContentHash, LabError> {
    let mut items = request.items.iter().collect::<Vec<_>>();
    items.sort_by(|lhs, rhs| lhs.policy_id.cmp(&rhs.policy_id));
    let tuples = items
        .into_iter()
        .map(|item| preview_tuple(&item.policy_id, item.preview.as_ref()))
        .collect::<Vec<_>>();
    ContentHash::of_value(&serde_json::json!({
        "version": "policy-delete-v1",
        "items": tuples,
        "confirmation_text": request.confirmation_text,
    }))
}

fn preview_digest(
    policy_id: &PolicyId,
    preview: Option<&PolicyDeletePreviewEcho>,
) -> Result<ContentHash, LabError> {
    ContentHash::of_value(&preview_tuple(policy_id, preview))
}

fn preview_tuple(
    policy_id: &PolicyId,
    preview: Option<&PolicyDeletePreviewEcho>,
) -> serde_json::Value {
    let scope = preview.and_then(|echo| echo.scope_digest.as_ref());
    let expiry = preview.and_then(|echo| echo.expires_at).map(timestamp_ms);
    let token = preview.and_then(|echo| echo.confirmation_token.as_ref());
    serde_json::json!([
        policy_id.as_str(),
        scope.map(ContentHash::as_str),
        expiry,
        token.map(ContentHash::as_str)
    ])
}

fn confirmation_matches(
    connection: &Connection,
    request: &PolicyDeleteWriteRequest,
) -> Result<bool, LabError> {
    if request.items.len() != 1 {
        return Ok(request.confirmation_text == format!("DELETE {} POLICIES", request.items.len()));
    }
    let item = &request.items[0];
    if request.confirmation_text == item.policy_id.as_str() {
        return Ok(true);
    }
    let current_name = connection
        .query_row(
            "SELECT name FROM policies WHERE policy_id=?1",
            [item.policy_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(super::sql_error)?;
    let Some(current_name) = current_name else {
        return Ok(false);
    };
    if current_name != request.confirmation_text {
        return Ok(false);
    }
    let matches = connection
        .query_row(
            "SELECT COUNT(*) FROM policies WHERE name=?1",
            [&request.confirmation_text],
            |row| row.get::<_, i64>(0),
        )
        .map_err(super::sql_error)?;
    Ok(matches == 1)
}

fn single_confirmation_still_matches(
    connection: &Connection,
    confirmation: &str,
    requested_count: i64,
    policy_id: &PolicyId,
    current_name: &str,
) -> Result<bool, LabError> {
    if requested_count != 1 {
        return Ok(confirmation == format!("DELETE {requested_count} POLICIES"));
    }
    if confirmation == policy_id.as_str() {
        return Ok(true);
    }
    if confirmation != current_name {
        return Ok(false);
    }
    let matches = connection
        .query_row(
            "SELECT COUNT(*) FROM policies WHERE name=?1",
            [confirmation],
            |row| row.get::<_, i64>(0),
        )
        .map_err(super::sql_error)?;
    Ok(matches == 1)
}

fn revision_head_is_consistent(
    connection: &Connection,
    policy_id: &PolicyId,
    expected_head: &str,
    expected_count: i64,
) -> Result<bool, LabError> {
    if expected_count <= 0 {
        return Ok(false);
    }
    let mut statement = connection
        .prepare(
            "SELECT revision_id,revision_number,parent_revision_id FROM policy_revisions WHERE policy_id=?1 ORDER BY revision_number",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([policy_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    let expected_len = usize::try_from(expected_count)
        .map_err(|_| LabError::DataCorrupt("negative policy revision count".into()))?;
    if rows.len() != expected_len || rows.last().map(|row| row.0.as_str()) != Some(expected_head) {
        return Ok(false);
    }
    let mut previous: Option<&str> = None;
    for (index, (revision_id, revision_number, parent_revision_id)) in rows.iter().enumerate() {
        let expected_number = i64::try_from(index + 1)
            .map_err(|_| LabError::DataCorrupt("policy revision number overflow".into()))?;
        if *revision_number != expected_number || parent_revision_id.as_deref() != previous {
            return Ok(false);
        }
        previous = Some(revision_id);
    }
    Ok(true)
}

fn protected_scope_outcome(
    scope: &DeleteScope,
    policy_id: &PolicyId,
    revision_count: i64,
) -> Result<Option<TerminalItem>, LabError> {
    if let Some(blocker) = scope
        .blockers
        .iter()
        .find(|blocker| blocker.reference.starts_with("paper:"))
    {
        let _ = blocker;
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::ReferencedPaper,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    if let Some(blocker) = scope
        .blockers
        .iter()
        .find(|blocker| blocker.reference.starts_with("automation:"))
    {
        let _ = blocker;
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::ReferencedAutomation,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    if scope
        .blockers
        .iter()
        .any(|blocker| blocker.class == DeleteBlockerClass::ActiveJob)
    {
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::ActiveJob,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    let has_historical_scope = !scope.plans.is_empty()
        || !scope.jobs.is_empty()
        || !scope.attempts.is_empty()
        || !scope.runs.is_empty()
        || !scope.evidence_snapshots.is_empty()
        || !scope.suites.is_empty()
        || !scope.suite_cases.is_empty()
        || !scope.schedules.is_empty()
        || !scope.schedule_fires.is_empty()
        || !scope.datasets.is_empty()
        || !scope.artifacts.is_empty()
        || !scope.backups.is_empty();
    if has_historical_scope {
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::ReferencedHistorical,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    if !scope.blockers.is_empty() {
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::OtherResourceScope,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    let allowed_policy = scope.policies.as_slice() == [policy_id.as_str()];
    let allowed_revisions = i64::try_from(scope.policy_revisions.len())
        .map_err(|_| LabError::DataCorrupt("policy revision count overflow".into()))?
        == revision_count;
    let other_rows = scope
        .counts
        .iter()
        .try_fold(0_u64, |sum, (_, value)| sum.checked_add(*value))
        .ok_or_else(|| LabError::DataCorrupt("delete scope count overflow".into()))?;
    let expected_rows = u64::try_from(revision_count)
        .map_err(|_| LabError::DataCorrupt("negative policy revision count".into()))?
        .checked_add(1)
        .ok_or_else(|| LabError::DataCorrupt("delete scope row count overflow".into()))?;
    let no_files = scope.artifact_files.is_empty()
        && scope.ledger_files.is_empty()
        && scope.raw_files.is_empty()
        && scope.backup_files.is_empty();
    if !allowed_policy
        || !allowed_revisions
        || other_rows != expected_rows
        || !no_files
        || scope.observations != 0
        || !scope.policy_revisions.iter().all(|id| !id.is_empty())
    {
        return Ok(Some(blocked(
            PolicyDeleteReasonCode::OtherResourceScope,
            Some(PolicyDeleteRemediationCode::ArchiveDeactivateUnavailable),
            true,
        )));
    }
    Ok(None)
}

fn blocked(
    reason: PolicyDeleteReasonCode,
    remediation: Option<PolicyDeleteRemediationCode>,
    retained: bool,
) -> TerminalItem {
    TerminalItem {
        status: PolicyDeleteItemStatus::Blocked,
        reason: Some(reason),
        remediation,
        deleted_revision_count: 0,
        retained,
        effect: PolicyDeleteEffect::NotApplied,
    }
}

fn stale(
    reason: PolicyDeleteReasonCode,
    remediation: PolicyDeleteRemediationCode,
    retained: bool,
) -> TerminalItem {
    TerminalItem {
        status: PolicyDeleteItemStatus::Stale,
        reason: Some(reason),
        remediation: Some(remediation),
        deleted_revision_count: 0,
        retained,
        effect: PolicyDeleteEffect::NotApplied,
    }
}

fn finish_item(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    policy_id: &PolicyId,
    terminal: TerminalItem,
    now_ms: i64,
) -> Result<(), LabError> {
    transaction
        .execute(
            "UPDATE policy_delete_request_items SET status=?3,reason_code=?4,remediation_code=?5,deleted_revision_count=?6,retained=?7,effect=?8 WHERE request_id=?1 AND policy_id=?2 AND status='PENDING'",
            params![
                request_id.as_str(),
                policy_id.as_str(),
                enum_text(&terminal.status)?,
                terminal.reason.map(|value| enum_text(&value)).transpose()?,
                terminal
                    .remediation
                    .map(|value| enum_text(&value))
                    .transpose()?,
                u64_to_i64(terminal.deleted_revision_count)?,
                terminal.retained,
                enum_text(&terminal.effect)?,
            ],
        )
        .map_err(super::sql_error)?;
    update_request_state(transaction, request_id, now_ms)
}

fn update_request_state(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    now_ms: i64,
) -> Result<(), LabError> {
    let requested_count = transaction
        .query_row(
            "SELECT requested_count FROM policy_delete_requests WHERE request_id=?1",
            [request_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(super::sql_error)?;
    let mut counts = PolicyDeleteCounts::default();
    let mut statement = transaction
        .prepare("SELECT status,retained FROM policy_delete_request_items WHERE request_id=?1")
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([request_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    drop(statement);
    for (status, retained) in &rows {
        match parse_enum::<PolicyDeleteItemStatus>(status)? {
            PolicyDeleteItemStatus::Pending => counts.pending += 1,
            PolicyDeleteItemStatus::Deleted => counts.deleted += 1,
            PolicyDeleteItemStatus::Blocked => counts.blocked += 1,
            PolicyDeleteItemStatus::Stale => counts.stale += 1,
            PolicyDeleteItemStatus::Failed => counts.failed += 1,
            PolicyDeleteItemStatus::InterruptedNotApplied => {
                counts.interrupted_not_applied += 1;
            }
        }
        if *retained {
            counts.retained += 1;
        }
    }
    if partition_count(&counts)? != requested_count {
        return Err(LabError::DataCorrupt(
            "policy delete receipt status partition disagrees with requested_count".into(),
        ));
    }
    let state = request_state(&counts);
    transaction
        .execute(
            "UPDATE policy_delete_requests SET state=?2,updated_at_ms=?3 WHERE request_id=?1",
            params![request_id.as_str(), enum_text(&state)?, now_ms],
        )
        .map_err(super::sql_error)?;
    Ok(())
}

fn readback_from_connection(
    connection: &Connection,
    request_id: &RequestId,
) -> Result<PolicyDeleteReadback, LabError> {
    let request = connection
        .query_row(
            "SELECT request_digest,requested_count,state FROM policy_delete_requests WHERE request_id=?1",
            [request_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(super::sql_error)?;
    let Some((request_digest, requested_count, state_text)) = request else {
        return Ok(PolicyDeleteReadback {
            request_id: request_id.clone(),
            found: false,
            request_digest: None,
            state: None,
            requested_count: None,
            items: None,
            counts: None,
        });
    };
    let items = readback_items(connection, request_id)?;
    let counts = readback_counts(&items);
    if partition_count(&counts)? != requested_count {
        return Err(LabError::DataCorrupt(
            "policy delete item count does not match request requested_count".into(),
        ));
    }
    let state = request_state(&counts);
    let stored_state = parse_enum::<PolicyDeleteRequestState>(&state_text)?;
    if stored_state != state {
        return Err(LabError::DataCorrupt(
            "stored policy delete request state disagrees with item statuses".into(),
        ));
    }
    Ok(PolicyDeleteReadback {
        request_id: request_id.clone(),
        found: true,
        request_digest: Some(parse_hash(request_digest)?),
        state: Some(state),
        requested_count: Some(
            u64::try_from(requested_count).map_err(|_| {
                LabError::DataCorrupt("negative policy delete requested_count".into())
            })?,
        ),
        items: Some(items),
        counts: Some(counts),
    })
}

fn readback_items(
    connection: &Connection,
    request_id: &RequestId,
) -> Result<Vec<PolicyDeleteItemReadback>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT policy_id,preview_scope_digest,preview_digest,status,reason_code,remediation_code,deleted_revision_count,retained,effect FROM policy_delete_request_items WHERE request_id=?1 ORDER BY policy_id",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([request_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, bool>(7)?,
                row.get::<_, String>(8)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    drop(statement);
    let mut items = Vec::with_capacity(rows.len());
    for (
        id,
        preview_scope,
        preview_digest,
        status,
        reason,
        remediation,
        deleted,
        retained,
        effect,
    ) in rows
    {
        let policy_id = PolicyId::new(id).map_err(|error| {
            LabError::DataCorrupt(format!("invalid policy ID in receipt: {error}"))
        })?;
        let status = parse_enum::<PolicyDeleteItemStatus>(&status)?;
        let reason = reason.as_deref().map(parse_enum).transpose()?;
        let remediation = remediation.as_deref().map(parse_enum).transpose()?;
        let effect = parse_enum::<PolicyDeleteEffect>(&effect)?;
        let deleted_revision_count = u64::try_from(deleted)
            .map_err(|_| LabError::DataCorrupt("negative policy deletion count".into()))?;
        items.push(PolicyDeleteItemReadback {
            policy_id,
            preview_scope_digest: preview_scope.map(parse_hash).transpose()?,
            preview_digest: parse_hash(preview_digest)?,
            status,
            reason_code: reason,
            remediation_code: remediation,
            deleted_revision_count,
            retained,
            effect,
        });
    }
    Ok(items)
}

fn readback_counts(items: &[PolicyDeleteItemReadback]) -> PolicyDeleteCounts {
    let mut counts = PolicyDeleteCounts::default();
    for item in items {
        match item.status {
            PolicyDeleteItemStatus::Pending => counts.pending += 1,
            PolicyDeleteItemStatus::Deleted => counts.deleted += 1,
            PolicyDeleteItemStatus::Blocked => counts.blocked += 1,
            PolicyDeleteItemStatus::Stale => counts.stale += 1,
            PolicyDeleteItemStatus::Failed => counts.failed += 1,
            PolicyDeleteItemStatus::InterruptedNotApplied => {
                counts.interrupted_not_applied += 1;
            }
        }
        if item.retained {
            counts.retained += 1;
        }
    }
    counts
}

fn partition_count(counts: &PolicyDeleteCounts) -> Result<i64, LabError> {
    let value = counts
        .pending
        .checked_add(counts.deleted)
        .and_then(|sum| sum.checked_add(counts.blocked))
        .and_then(|sum| sum.checked_add(counts.stale))
        .and_then(|sum| sum.checked_add(counts.failed))
        .and_then(|sum| sum.checked_add(counts.interrupted_not_applied))
        .ok_or_else(|| LabError::DataCorrupt("policy delete count overflow".into()))?;
    i64::try_from(value).map_err(|_| LabError::DataCorrupt("policy delete count overflow".into()))
}

fn request_state(counts: &PolicyDeleteCounts) -> PolicyDeleteRequestState {
    if counts.pending > 0 {
        PolicyDeleteRequestState::Pending
    } else if counts.interrupted_not_applied > 0 {
        PolicyDeleteRequestState::Interrupted
    } else if counts.deleted > 0 && counts.blocked == 0 && counts.stale == 0 && counts.failed == 0 {
        PolicyDeleteRequestState::Complete
    } else {
        PolicyDeleteRequestState::Partial
    }
}

fn parse_hash(value: String) -> Result<ContentHash, LabError> {
    ContentHash::try_from(value).map_err(|error| {
        LabError::DataCorrupt(format!("invalid hash in policy delete receipt: {error}"))
    })
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T, LabError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|error| {
        LabError::DataCorrupt(format!("invalid policy delete receipt enum: {error}"))
    })
}

fn enum_text(value: &impl Serialize) -> Result<String, LabError> {
    serde_json::to_value(value)
        .map_err(super::json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            LabError::Internal("policy delete enum did not serialize as a string".into())
        })
}

fn timestamp_ms(value: UtcTimestamp) -> i64 {
    value.0.timestamp_millis()
}

fn timestamp_from_ms(value: i64) -> Result<UtcTimestamp, LabError> {
    chrono::DateTime::from_timestamp_millis(value)
        .map(UtcTimestamp)
        .ok_or_else(|| LabError::DataCorrupt(format!("invalid policy delete timestamp: {value}")))
}

fn usize_to_i64(value: usize) -> Result<i64, LabError> {
    i64::try_from(value)
        .map_err(|_| LabError::ResourceLimit("policy delete value exceeds SQLite integer".into()))
}

fn u64_to_i64(value: u64) -> Result<i64, LabError> {
    i64::try_from(value)
        .map_err(|_| LabError::ResourceLimit("policy delete value exceeds SQLite integer".into()))
}
