use super::{
    Store, directory_size, io_error, json_error, reject_symlink, sql_error, timestamp_from_ms,
    timestamp_ms,
};
use crate::contracts::{
    BackupId, ContentHash, DeleteResource, LabError, MAX_MANAGED_BACKUP_BYTES,
    MAX_MANAGED_BACKUP_SOURCE_BYTES, MAX_MANAGED_BACKUPS, MAX_RETENTION_PAGE, ManagedBackupReceipt,
    MetricValue, NullReason, RequestId, ResearchPage, RetentionCandidate, SqliteStorageUsage,
    StorageUsage, UtcTimestamp,
};
use rusqlite::{OptionalExtension, params};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct VerifiedBackup {
    bytes: u64,
    manifest_digest: ContentHash,
    source_identity_digest: ContentHash,
}

#[derive(Debug)]
struct CandidateSeed {
    resource: DeleteResource,
    created_at: UtcTimestamp,
}

impl Store {
    /// Typed storage accounting including the separately capped backup namespace.
    ///
    /// # Errors
    /// Returns corrupt-accounting, unsafe-filesystem or SQLite failures.
    pub fn maintenance_usage(&self) -> Result<StorageUsage, LabError> {
        let accounting = self.storage_accounting()?;
        let sqlite = accounting
            .get("sqlite")
            .ok_or_else(|| LabError::DataCorrupt("storage accounting omitted sqlite".into()))?;
        let allocated = json_u64(sqlite, "allocated_bytes")?;
        let live = json_u64(sqlite, "live_bytes")?;
        let reusable = json_u64(sqlite, "reusable_bytes")?;
        let wal = json_u64(sqlite, "wal_bytes")?;
        let limit = json_u64(sqlite, "limit_bytes")?;
        let raw = json_u64(&accounting, "raw_bytes")?;
        let ledgers = json_u64(&accounting, "sealed_ledger_bytes")?;
        let exports = json_u64(&accounting, "exports_bytes")?;
        let total = json_u64(&accounting, "total_managed_bytes")?;
        let (backup_count, backup_bytes) =
            inspect_backup_namespace(&managed_backup_root(&self.raw.root)?, false)?;
        let effective = allocated.saturating_add(wal);
        Ok(StorageUsage {
            sqlite: SqliteStorageUsage {
                allocated_bytes: allocated,
                live_bytes: live,
                reusable_bytes: reusable,
                wal_bytes: wal,
                limit_bytes: limit,
                utilization: ratio(effective, limit),
                main_db_utilization: ratio(allocated, limit),
                effective_db_bytes: effective,
                effective_utilization: ratio(effective, limit),
            },
            raw_bytes: raw,
            sealed_ledger_bytes: ledgers,
            exports_bytes: exports,
            managed_backup_bytes: backup_bytes,
            managed_backup_count: backup_count,
            managed_backup_limit_bytes: MAX_MANAGED_BACKUP_BYTES,
            managed_backup_limit_count: MAX_MANAGED_BACKUPS,
            total_managed_bytes: total.saturating_add(backup_bytes),
            data_root_utilization: ratio(total, MAX_MANAGED_BACKUP_SOURCE_BYTES),
            managed_backup_utilization: ratio(backup_bytes, MAX_MANAGED_BACKUP_BYTES),
        })
    }

    /// Fold the WAL into the main database with one explicit checkpoint.
    ///
    /// Rows are never deleted and the database file is never rewritten; busy
    /// readers only leave frames unconvered and are reported, not forced.
    /// # Errors
    /// Returns corrupt-accounting or SQLite failures.
    pub fn checkpoint_wal(
        &self,
        mode: crate::contracts::CheckpointMode,
    ) -> Result<crate::contracts::WalCheckpointOutcome, LabError> {
        let before = self.maintenance_usage()?.sqlite;
        let (busy, log, checkpointed): (i64, i64, i64) = self
            .connection
            .query_row(
                &format!("PRAGMA wal_checkpoint({});", mode.as_str()),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(sql_error)?;
        let after = self.maintenance_usage()?.sqlite;
        let to_u64 = |value: i64| u64::try_from(value.max(0)).unwrap_or(u64::MAX);
        Ok(crate::contracts::WalCheckpointOutcome {
            mode,
            busy: busy != 0,
            busy_reason: (busy != 0).then_some(crate::contracts::CheckpointBusyReason::SqliteBusy),
            log_frames: to_u64(log),
            checkpointed_frames: to_u64(checkpointed),
            before,
            after,
        })
    }

    /// Explicit physical compaction: checkpoint, `VACUUM`, final checkpoint
    /// and an integrity check. Deletion stays a separate operation so batch
    /// cleanups never amplify WAL and I/O per deleted resource.
    /// # Errors
    /// Returns corrupt-accounting, integrity or SQLite failures.
    pub fn compact_database(&mut self) -> Result<crate::contracts::CompactOutcome, LabError> {
        let before = self.maintenance_usage()?.sqlite;
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(sql_error)?;
        self.connection
            .execute_batch("VACUUM;")
            .map_err(sql_error)?;
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(sql_error)?;
        let integrity: String = self
            .connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(sql_error)?;
        if integrity != "ok" {
            return Err(LabError::DataCorrupt(format!(
                "post-compaction integrity check failed: {integrity}"
            )));
        }
        let after = self.maintenance_usage()?.sqlite;
        Ok(crate::contracts::CompactOutcome {
            reclaimed_bytes: before
                .effective_db_bytes
                .saturating_sub(after.effective_db_bytes),
            before,
            after,
            integrity,
        })
    }

    /// Create or reconcile one verified managed backup using a deterministic opaque ID.
    ///
    /// # Errors
    /// Rejects capacity, identity or verification failures and propagates storage I/O errors.
    pub fn create_managed_backup(
        &mut self,
        request_id: &RequestId,
        now: UtcTimestamp,
    ) -> Result<ManagedBackupReceipt, LabError> {
        let input_digest = ContentHash::of_value(&serde_json::json!({
            "format_version": "managed-backup-v1",
            "max_source_bytes": MAX_MANAGED_BACKUP_SOURCE_BYTES,
            "max_backup_count": MAX_MANAGED_BACKUPS,
            "max_backup_bytes": MAX_MANAGED_BACKUP_BYTES,
        }))?;
        if let Some((stored_digest, receipt)) = self.managed_backup_by_request(request_id)? {
            if stored_digest != input_digest {
                return Err(LabError::Conflict(
                    "backup request ID already names a different input".into(),
                ));
            }
            self.verify_receipt_path(&receipt)?;
            return Ok(receipt);
        }

        let id = BackupId::from_seed(request_id.as_str());
        let root = managed_backup_root(&self.raw.root)?;
        let destination = root.join(id.as_str());
        if destination
            .try_exists()
            .map_err(io_error("inspect managed backup"))?
        {
            let verified = Self::verified_backup(&destination)?;
            enforce_published_namespace_caps(&root)?;
            return self.persist_backup_receipt(&id, request_id, &input_digest, now, &verified);
        }

        let usage = self.maintenance_usage()?;
        let source_bytes = usage
            .total_managed_bytes
            .saturating_sub(usage.managed_backup_bytes);
        if source_bytes > MAX_MANAGED_BACKUP_SOURCE_BYTES {
            return Err(LabError::ResourceLimit(format!(
                "managed backup source exceeds {MAX_MANAGED_BACKUP_SOURCE_BYTES} bytes"
            )));
        }
        let (directory_count, directory_bytes) = inspect_backup_namespace(&root, true)?;
        if directory_count >= MAX_MANAGED_BACKUPS {
            return Err(LabError::ResourceLimit(format!(
                "managed backup count limit is {MAX_MANAGED_BACKUPS}"
            )));
        }
        if directory_bytes.saturating_add(source_bytes) > MAX_MANAGED_BACKUP_BYTES {
            return Err(LabError::ResourceLimit(format!(
                "managed backup byte limit is {MAX_MANAGED_BACKUP_BYTES}"
            )));
        }
        self.backup(&destination)?;
        let verified = Self::verified_backup(&destination)?;
        enforce_published_namespace_caps(&root)?;
        self.persist_backup_receipt(&id, request_id, &input_digest, now, &verified)
    }

    /// List the complete bounded backup catalog in deterministic creation order.
    ///
    /// # Errors
    /// Rejects missing, corrupt or mismatched backup receipts and filesystem failures.
    pub fn list_managed_backups(&self) -> Result<ResearchPage<ManagedBackupReceipt>, LabError> {
        let mut statement = self
            .connection
            .prepare("SELECT receipt_json FROM managed_backups ORDER BY created_at_ms,id")
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        if rows.len() > usize::try_from(MAX_MANAGED_BACKUPS).unwrap_or(usize::MAX) {
            return Err(LabError::DataCorrupt(
                "managed backup catalog exceeds its fixed bound".into(),
            ));
        }
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let receipt: ManagedBackupReceipt = serde_json::from_str(&row).map_err(json_error)?;
            self.verify_receipt_path(&receipt)?;
            records.push(receipt);
        }
        Ok(ResearchPage {
            total_count: u64::try_from(records.len())
                .map_err(|_| LabError::ResourceLimit("backup count overflow".into()))?,
            records,
            next_offset: None,
        })
    }

    /// Return paged advisory candidates. This method never deletes or mutates state.
    ///
    /// # Errors
    /// Rejects invalid paging, corrupt references, unsafe previews or SQLite failures.
    pub fn retention_candidates(
        &self,
        cutoff: UtcTimestamp,
        keep_recent: u32,
        offset: u64,
        limit: u32,
        now: UtcTimestamp,
    ) -> Result<ResearchPage<RetentionCandidate>, LabError> {
        if keep_recent == 0 {
            return Err(LabError::InvalidConfig(
                "retention keep_recent must be at least 1".into(),
            ));
        }
        if !(1..=MAX_RETENTION_PAGE).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "retention limit must be in 1..={MAX_RETENTION_PAGE}"
            )));
        }
        let (seeds, total_count) = self.retention_seeds(cutoff, keep_recent, offset, limit)?;
        let mut records = Vec::with_capacity(seeds.len());
        for seed in &seeds {
            records.push(RetentionCandidate {
                resource: seed.resource.clone(),
                created_at: seed.created_at,
                preview: self.delete_preview(&seed.resource, now)?,
            });
        }
        let next = offset
            .checked_add(u64::try_from(records.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| LabError::ResourceLimit("retention next offset overflow".into()))?;
        Ok(ResearchPage {
            records,
            total_count,
            next_offset: (next < total_count).then_some(next),
        })
    }

    fn persist_backup_receipt(
        &mut self,
        id: &BackupId,
        request_id: &RequestId,
        input_digest: &ContentHash,
        now: UtcTimestamp,
        verified: &VerifiedBackup,
    ) -> Result<ManagedBackupReceipt, LabError> {
        let receipt = ManagedBackupReceipt {
            id: id.clone(),
            request_id: request_id.clone(),
            source_identity_digest: verified.source_identity_digest.clone(),
            created_at: now,
            bytes: verified.bytes,
            manifest_digest: verified.manifest_digest.clone(),
        };
        let receipt_json = serde_json::to_string(&receipt).map_err(json_error)?;
        self.connection.execute(
            "INSERT INTO managed_backups(id,request_id,input_digest,source_identity_digest,created_at_ms,relative_path,bytes,manifest_digest,receipt_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                id.as_str(),
                request_id.as_str(),
                input_digest.as_str(),
                verified.source_identity_digest.as_str(),
                timestamp_ms(now),
                id.as_str(),
                u64_to_i64(verified.bytes)?,
                verified.manifest_digest.as_str(),
                receipt_json
            ],
        ).map_err(sql_error)?;
        Ok(receipt)
    }

    fn managed_backup_by_request(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<(ContentHash, ManagedBackupReceipt)>, LabError> {
        let row = self
            .connection
            .query_row(
                "SELECT input_digest,receipt_json FROM managed_backups WHERE request_id=?1",
                [request_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|(digest, receipt)| {
            Ok((
                ContentHash::try_from(digest)?,
                serde_json::from_str(&receipt).map_err(json_error)?,
            ))
        })
        .transpose()
    }

    fn verify_receipt_path(&self, receipt: &ManagedBackupReceipt) -> Result<(), LabError> {
        let root = managed_backup_root(&self.raw.root)?;
        let verified = Self::verified_backup(&root.join(receipt.id.as_str()))?;
        if verified.bytes != receipt.bytes
            || verified.manifest_digest != receipt.manifest_digest
            || verified.source_identity_digest != receipt.source_identity_digest
        {
            return Err(LabError::DataCorrupt(format!(
                "managed backup receipt no longer matches {}",
                receipt.id
            )));
        }
        Ok(())
    }

    fn verified_backup(path: &Path) -> Result<VerifiedBackup, LabError> {
        let (manifest_digest, source_identity_digest, bytes) =
            Store::verify_backup_directory(path)?;
        Ok(VerifiedBackup {
            bytes,
            manifest_digest,
            source_identity_digest,
        })
    }

    fn retention_seeds(
        &self,
        cutoff: UtcTimestamp,
        keep_recent: u32,
        offset: u64,
        limit: u32,
    ) -> Result<(Vec<CandidateSeed>, u64), LabError> {
        let cutoff = timestamp_ms(cutoff);
        let keep_recent = i64::from(keep_recent);
        let total_sql = format!("{RETENTION_CTE} SELECT COUNT(*) FROM candidates");
        let total: i64 = self
            .connection
            .query_row(&total_sql, params![keep_recent, cutoff], |row| row.get(0))
            .map_err(sql_error)?;
        let total = u64::try_from(total)
            .map_err(|_| LabError::DataCorrupt("negative retention count".into()))?;
        let page_sql = format!(
            "{RETENTION_CTE} SELECT kind,id,created_at_ms FROM candidates ORDER BY created_at_ms,id,kind LIMIT ?3 OFFSET ?4"
        );
        let mut statement = self.connection.prepare(&page_sql).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![keep_recent, cutoff, i64::from(limit), u64_to_i64(offset)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut seeds = Vec::with_capacity(usize::try_from(limit).unwrap_or(0));
        for row in rows {
            let (kind, id, created_at) = row.map_err(sql_error)?;
            let resource = match kind.as_str() {
                "job" => DeleteResource::Job {
                    job_id: crate::contracts::JobId::new(id)?,
                },
                "dataset" => DeleteResource::Dataset {
                    dataset_id: crate::contracts::DatasetId::new(id)?,
                },
                "suite" => DeleteResource::Suite {
                    suite_id: crate::contracts::SuiteId::new(id)?,
                },
                "schedule" => DeleteResource::Schedule {
                    schedule_id: crate::contracts::ScheduleId::new(id)?,
                },
                "backup" => DeleteResource::Backup {
                    backup_id: BackupId::new(id)?,
                },
                _ => {
                    return Err(LabError::DataCorrupt(format!(
                        "unknown retention candidate kind: {kind}"
                    )));
                }
            };
            seeds.push(CandidateSeed {
                resource,
                created_at: timestamp_from_ms(created_at)?,
            });
        }
        Ok((seeds, total))
    }
}

const RETENTION_CTE: &str = "WITH
job_ranked AS (
    SELECT j.id,j.created_at_ms,
           ROW_NUMBER() OVER (ORDER BY j.created_at_ms DESC,j.id DESC) AS recent_rank
    FROM jobs j
    WHERE j.current_status NOT IN ('QUEUED','RUNNING')
      AND NOT EXISTS (SELECT 1 FROM research_suite_cases c WHERE c.job_id=j.id)
      AND NOT EXISTS (SELECT 1 FROM schedule_fires f WHERE f.job_id=j.id)
),
dataset_dated AS (
    SELECT d.id,MAX(r.persisted_at_ms) AS created_at_ms
    FROM datasets d
    JOIN dataset_raw_objects dr ON dr.dataset_id=d.id
    JOIN raw_objects r ON r.id=dr.raw_object_id
    WHERE NOT EXISTS (SELECT 1 FROM plan_datasets p WHERE p.dataset_id=d.id)
      AND NOT EXISTS (SELECT 1 FROM collection_schedules s WHERE s.last_success_dataset_id=d.id)
      AND NOT EXISTS (SELECT 1 FROM schedule_fires f WHERE f.dataset_id=d.id)
      AND NOT EXISTS (
        SELECT 1 FROM job_attempt_dataset_outputs o
        JOIN job_attempts a ON a.id=o.attempt_id
        WHERE o.dataset_id=d.id AND a.status IN ('QUEUED','RUNNING')
      )
    GROUP BY d.id
),
dataset_ranked AS (
    SELECT id,created_at_ms,
           ROW_NUMBER() OVER (ORDER BY created_at_ms DESC,id DESC) AS recent_rank
    FROM dataset_dated
),
suite_ranked AS (
    SELECT id,created_at_ms,
           ROW_NUMBER() OVER (ORDER BY created_at_ms DESC,id DESC) AS recent_rank
    FROM research_suites WHERE status IN ('completed','blocked')
),
schedule_ranked AS (
    SELECT id,created_at_ms,
           ROW_NUMBER() OVER (ORDER BY created_at_ms DESC,id DESC) AS recent_rank
    FROM collection_schedules WHERE status IN ('paused','blocked')
),
backup_ranked AS (
    SELECT id,created_at_ms,
           ROW_NUMBER() OVER (ORDER BY created_at_ms DESC,id DESC) AS recent_rank
    FROM managed_backups
),
candidates(kind,id,created_at_ms) AS (
    SELECT 'job',id,created_at_ms FROM job_ranked WHERE recent_rank>?1 AND created_at_ms<?2
    UNION ALL SELECT 'dataset',id,created_at_ms FROM dataset_ranked WHERE recent_rank>?1 AND created_at_ms<?2
    UNION ALL SELECT 'suite',id,created_at_ms FROM suite_ranked WHERE recent_rank>?1 AND created_at_ms<?2
    UNION ALL SELECT 'schedule',id,created_at_ms FROM schedule_ranked WHERE recent_rank>?1 AND created_at_ms<?2
    UNION ALL SELECT 'backup',id,created_at_ms FROM backup_ranked WHERE recent_rank>?1 AND created_at_ms<?2
)";

pub(super) fn managed_backup_root(data_root: &Path) -> Result<PathBuf, LabError> {
    let parent = data_root.parent().ok_or_else(|| {
        LabError::InvalidConfig("data root must have a parent for managed backups".into())
    })?;
    let namespace = ContentHash::of_bytes(data_root.to_string_lossy().as_bytes());
    Ok(parent.join(format!(
        ".sky-backcraft-managed-backups-{}",
        &namespace.as_str()[..16]
    )))
}

fn inspect_backup_namespace(root: &Path, create: bool) -> Result<(u64, u64), LabError> {
    if !root
        .try_exists()
        .map_err(io_error("inspect managed backup root"))?
    {
        if create {
            fs::create_dir(root).map_err(io_error("create managed backup root"))?;
        }
        return Ok((0, 0));
    }
    reject_symlink(root)?;
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root).map_err(io_error("read managed backup root"))? {
        let entry = entry.map_err(io_error("read managed backup entry"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        BackupId::new(name.clone()).map_err(|_| {
            LabError::DataCorrupt(format!("unexpected managed backup entry: {name}"))
        })?;
        let metadata = entry
            .file_type()
            .map_err(io_error("inspect managed backup entry"))?;
        if !metadata.is_dir() || metadata.is_symlink() {
            return Err(LabError::DataCorrupt(format!(
                "managed backup entry is not a regular directory: {name}"
            )));
        }
        count = count.saturating_add(1);
        bytes = bytes.saturating_add(directory_size(&entry.path())?);
    }
    Ok((count, bytes))
}

fn enforce_published_namespace_caps(root: &Path) -> Result<(), LabError> {
    let (count, bytes) = inspect_backup_namespace(root, false)?;
    if count > MAX_MANAGED_BACKUPS || bytes > MAX_MANAGED_BACKUP_BYTES {
        return Err(LabError::OutcomeUnknown(format!(
            "published managed backup namespace exceeds its receipt cap: count={count}/{MAX_MANAGED_BACKUPS}, bytes={bytes}/{MAX_MANAGED_BACKUP_BYTES}; no receipt was recorded"
        )));
    }
    Ok(())
}

fn json_u64(value: &serde_json::Value, key: &str) -> Result<u64, LabError> {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| LabError::DataCorrupt(format!("storage accounting field is invalid: {key}")))
}

fn u64_to_i64(value: u64) -> Result<i64, LabError> {
    i64::try_from(value).map_err(|_| LabError::ResourceLimit("byte count overflow".into()))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "storage byte contracts are capped below 2^53, so these integer casts are exact"
)]
fn ratio(numerator: u64, denominator: u64) -> MetricValue {
    if denominator == 0 {
        return MetricValue::null(NullReason::ZeroDenominator);
    }
    MetricValue::value(numerator as f64 / denominator as f64)
}

#[cfg(test)]
mod tests;
