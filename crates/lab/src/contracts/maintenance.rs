//! Bounded storage accounting, managed backups and advisory retention.

use super::{
    BackupId, ContentHash, DeletePreview, DeleteResource, MetricValue, RequestId, ResearchPage,
    UtcTimestamp,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteStorageUsage {
    pub allocated_bytes: u64,
    pub live_bytes: u64,
    pub reusable_bytes: u64,
    pub wal_bytes: u64,
    pub limit_bytes: u64,
    pub utilization: MetricValue,
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
