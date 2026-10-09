//! Bounded storage accounting, managed backups and advisory retention.

use super::{
    BackupId, ContentHash, DeleteOutcome, DeletePreview, DeleteResource, MetricValue, RequestId,
    ResearchPage, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_MANAGED_BACKUPS: u64 = 8;
pub const MAX_MANAGED_BACKUP_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const MAX_MANAGED_BACKUP_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_RETENTION_PAGE: u32 = 100;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageMaintenanceAction {
    Usage,
    CreateBackup {
        request_id: RequestId,
    },
    ListBackups,
    RetentionCandidates {
        cutoff: UtcTimestamp,
        keep_recent: u32,
        offset: u64,
        limit: u32,
    },
    /// Fold the write-ahead log back into the main database. Never deletes
    /// rows and never rewrites the database file.
    Checkpoint {
        mode: CheckpointMode,
    },
    /// Explicit physical compaction: checkpoint, `VACUUM`, final checkpoint
    /// and an integrity check. Separated from deletion so batch cleanups do
    /// not amplify WAL and I/O per deleted resource.
    Compact,
    /// Execute a bounded batch of previously previewed hard deletions in one
    /// call. Every request echoes its own exact preview; nothing is deleted
    /// without it, and the batch aborts before the first failure.
    HardDeleteBatch {
        requests: Vec<super::HardDeleteRequest>,
    },
}

/// Hard-deletion batches are bounded so one operator call can never run away.
pub const MAX_HARD_DELETE_BATCH: usize = 8;

/// SQLite WAL checkpoint aggressiveness, mirroring `PRAGMA wal_checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckpointMode {
    Passive,
    Restart,
    Truncate,
}

impl CheckpointMode {
    /// `PRAGMA wal_checkpoint` argument spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// SQLite reports that the requested checkpoint could not finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckpointBusyReason {
    SqliteBusy,
}

/// Result of one explicit WAL checkpoint with usage on both sides.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WalCheckpointOutcome {
    pub mode: CheckpointMode,
    /// True when some frames could not be checkpointed (busy readers/writers).
    pub busy: bool,
    /// SQLite could not finish the requested checkpoint because the database
    /// was busy. No reader or writer is forcibly interrupted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_reason: Option<CheckpointBusyReason>,
    pub log_frames: u64,
    pub checkpointed_frames: u64,
    pub before: SqliteStorageUsage,
    pub after: SqliteStorageUsage,
}

/// Result of one explicit physical compaction (`VACUUM` + checkpoints).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompactOutcome {
    pub before: SqliteStorageUsage,
    pub after: SqliteStorageUsage,
    /// `before.effective_db_bytes - after.effective_db_bytes`.
    pub reclaimed_bytes: u64,
    /// `PRAGMA integrity_check` verdict; anything but `ok` fails the call.
    pub integrity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteStorageUsage {
    pub allocated_bytes: u64,
    pub live_bytes: u64,
    pub reusable_bytes: u64,
    pub wal_bytes: u64,
    pub limit_bytes: u64,
    /// `(allocated + wal) / limit`; kept for backward compatibility and
    /// identical to [`Self::effective_utilization`].
    pub utilization: MetricValue,
    /// `allocated / limit` — the main database file alone against its cap.
    pub main_db_utilization: MetricValue,
    /// `allocated + wal` — the total SQLite footprint.
    pub effective_db_bytes: u64,
    /// `effective_db_bytes / limit` — the enforced admission metric.
    pub effective_utilization: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StorageUsage {
    pub sqlite: SqliteStorageUsage,
    pub raw_bytes: u64,
    pub sealed_ledger_bytes: u64,
    pub exports_bytes: u64,
    pub managed_backup_bytes: u64,
    pub managed_backup_count: u64,
    pub managed_backup_limit_bytes: u64,
    pub managed_backup_limit_count: u64,
    pub total_managed_bytes: u64,
    pub data_root_utilization: MetricValue,
    pub managed_backup_utilization: MetricValue,
}

/// Durable proof that a backup under the server-owned namespace was verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedBackupReceipt {
    pub id: BackupId,
    pub request_id: RequestId,
    pub source_identity_digest: ContentHash,
    pub created_at: UtcTimestamp,
    pub bytes: u64,
    pub manifest_digest: ContentHash,
}

/// Advisory candidate. The embedded preview remains the only deletion authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionCandidate {
    pub resource: DeleteResource,
    pub created_at: UtcTimestamp,
    pub preview: DeletePreview,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageMaintenanceResult {
    Usage {
        usage: StorageUsage,
    },
    Checkpoint {
        outcome: WalCheckpointOutcome,
    },
    Compact {
        outcome: CompactOutcome,
    },
    HardDeleteBatch {
        outcomes: Vec<DeleteOutcome>,
    },
    Backup {
        receipt: ManagedBackupReceipt,
    },
    Backups {
        page: ResearchPage<ManagedBackupReceipt>,
    },
    RetentionCandidates {
        page: ResearchPage<RetentionCandidate>,
    },
}
