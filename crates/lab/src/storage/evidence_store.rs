use super::{Store, enum_text, json_error, sql_error, timestamp_ms, usize_to_i64};
use crate::contracts::{EvidenceImport, EvidenceSnapshot, EvidenceSnapshotId, LabError};
use rusqlite::{OptionalExtension, params};

impl Store {
    /// Register one canonical immutable Evidence snapshot and its version lineage.
    ///
    /// # Errors
    /// Rejects noncanonical/conflicting snapshots or SQLite failure.
    pub fn register_evidence_snapshot(
        &mut self,
        snapshot: &EvidenceSnapshot,
    ) -> Result<EvidenceSnapshotId, LabError> {
        let canonical = crate::evidence::build_snapshot(EvidenceImport {
            public_non_sensitive_ack: true,
            versions: snapshot.versions.clone(),
        })?;
        if canonical.id != snapshot.id || canonical.digest != snapshot.digest {
            return Err(LabError::InputHashMismatch(
                "Evidence snapshot is not the canonical version projection".into(),
            ));
        }
        let snapshot_json = serde_json::to_string(snapshot).map_err(json_error)?;
        if let Some(existing) = self
            .connection
            .query_row(
                "SELECT snapshot_json FROM evidence_snapshots WHERE id=?1",
                [snapshot.id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
        {
            return if existing == snapshot_json {
                Ok(snapshot.id.clone())
            } else {
                Err(LabError::Conflict(
                    "Evidence snapshot id already has different content".into(),
                ))
            };
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for version in &snapshot.versions {
            let json = serde_json::to_string(version).map_err(json_error)?;
            let existing = transaction
                .query_row(
                    "SELECT version_json FROM evidence_versions WHERE revision_id=?1",
                    [version.revision_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_error)?;
            match existing {
                Some(existing) if existing == json => {}
                Some(_) => {
                    return Err(LabError::Conflict(
                        "Evidence revision id already has different content".into(),
                    ));
                }
                None => {
                    transaction.execute(
                        "INSERT INTO evidence_versions(revision_id,evidence_id,event_id,purpose,category,body,declared_available_at_ms,registered_at_ms,valid_until_ms,body_hash,mapping_version,supersedes_revision_id,version_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                        params![version.revision_id.as_str(), version.evidence_id.as_str(), version.event_id, enum_text(&version.purpose)?, version.category, version.body, timestamp_ms(version.declared_available_at), timestamp_ms(version.registered_at), timestamp_ms(version.valid_until), version.body_hash.as_str(), version.mapping_version, version.supersedes_revision_id.as_ref().map(crate::contracts::EvidenceRevisionId::as_str), json],
                    ).map_err(sql_error)?;
                    for (position, market) in version.markets.iter().enumerate() {
                        transaction.execute(
                            "INSERT INTO evidence_version_markets(revision_id,market,position) VALUES (?1,?2,?3)",
                            params![version.revision_id.as_str(), market.code(), usize_to_i64(position)?],
                        ).map_err(sql_error)?;
                    }
                    for (position, source) in version.source_refs.iter().enumerate() {
                        transaction.execute(
                            "INSERT INTO evidence_version_sources(revision_id,source_ref,position) VALUES (?1,?2,?3)",
                            params![version.revision_id.as_str(), source, usize_to_i64(position)?],
                        ).map_err(sql_error)?;
                    }
                }
            }
        }
        transaction
            .execute(
                "INSERT INTO evidence_snapshots(id,digest,snapshot_json) VALUES (?1,?2,?3)",
                params![
                    snapshot.id.as_str(),
                    snapshot.digest.as_str(),
                    snapshot_json
                ],
            )
            .map_err(sql_error)?;
        for (position, version) in snapshot.versions.iter().enumerate() {
            transaction.execute(
                "INSERT INTO evidence_snapshot_members(snapshot_id,revision_id,position) VALUES (?1,?2,?3)",
                params![snapshot.id.as_str(), version.revision_id.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(snapshot.id.clone())
    }

    /// Load one immutable Evidence snapshot.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state or SQLite failure.
    pub fn load_evidence_snapshot(
        &self,
        id: &EvidenceSnapshotId,
    ) -> Result<Option<EvidenceSnapshot>, LabError> {
        let json = self
            .connection
            .query_row(
                "SELECT snapshot_json FROM evidence_snapshots WHERE id=?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(json) = json else {
            return Ok(None);
        };
        let snapshot: EvidenceSnapshot = serde_json::from_str(&json).map_err(json_error)?;
        let canonical = crate::evidence::build_snapshot(EvidenceImport {
            public_non_sensitive_ack: true,
            versions: snapshot.versions.clone(),
        })?;
        if canonical.id != snapshot.id || canonical.digest != snapshot.digest {
            return Err(LabError::DataCorrupt(
                "stored Evidence snapshot is noncanonical".into(),
            ));
        }
        Ok(Some(snapshot))
    }
}
