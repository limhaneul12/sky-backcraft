//! Synchronous SQLite and raw-object persistence core.
//!
//! The application owns this value from one dedicated storage thread. This
//! module deliberately contains no async runtime bridge or hidden worker pool.

mod artifact_read;
mod delete_store;
mod evidence_store;
mod history_store;
mod job_store;
mod ledger_seal;
mod maintenance_store;
mod migrations;
mod plan_store;
mod policy_store;
mod portfolio_store;
mod query_read;
mod research_store;
mod run_store;
mod schedule_store;
pub use crate::contracts::{
    DatasetDigests, dataset_digests, dataset_id, normalized_request_digest, observation_digest,
};
pub use artifact_read::ArtifactChunkRead;
pub use job_store::AttemptPublication;
pub use ledger_seal::RunLedgerSealStats;
pub use plan_store::StoredPlan;
pub use query_read::{
    JobQueueStatus, ModelCostSummary, ModelResultSummary, RunResultSummary, ValidationResultSummary,
};
pub use run_store::{ModelFactBatch, ResultPage};

use crate::contracts::{
    CandleObservation, CollectRequest, CollectionPage, ContentHash, DatasetId, DatasetSnapshot,
    DatasetStatus, LabError, MAX_COLLECTION_CALLS, MAX_DATASET_ROWS, MarketDataOrigin, MarketId,
    ObservationId, QualitySeverity, RawObjectId, RawObjectRef, RequestId, UtcTimestamp,
};
use flate2::{Compression, GzBuilder};
use rusqlite::{Connection, MAIN_DB, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DATABASE_FILE: &str = "lab.sqlite";
const RAW_DIR: &str = "raw";
const BODY_FILE: &str = "body.json.gz";
const META_FILE: &str = "metadata.json";
const OWNER_LOCK_FILE: &str = ".lab-owner.lock";
pub const MAX_RAW_OBJECT_BYTES: usize = 1024 * 1024;
pub const MAX_COLLECTION_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_DATA_ROOT_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_EXPORT_RESERVATION_BYTES: u64 = 193 * 1024 * 1024;
const MAX_DATABASE_BYTES: u64 = 512 * 1024 * 1024;
const DATABASE_WAL_HEADROOM_BYTES: u64 = 260 * 1024 * 1024;
const DATABASE_PAGE_BYTES: i64 = 4_096;
const WAL_AUTOCHECKPOINT_PAGES: i64 = 1_000;
const WAL_JOURNAL_LIMIT_BYTES: i64 = 64 * 1024 * 1024;
/// Effective-utilization percent at which submissions log a pressure warning.
const DB_SOFT_PRESSURE_PERCENT: u64 = 85;
/// Effective-utilization percent at which mutating submissions are refused.
const DB_HARD_PRESSURE_PERCENT: u64 = 95;
/// WAL size at which submissions are refused until an explicit checkpoint.
const WAL_PRESSURE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PAGE_OBSERVATIONS: usize = 200;
const MAX_SNAPSHOT_LINKS: usize = 120_000;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct RawObjectInput {
    pub source_url: String,
    pub fetched_at: UtcTimestamp,
    pub persisted_at: UtcTimestamp,
    pub http_status: u16,
    pub remaining_req: Option<String>,
    pub origin: MarketDataOrigin,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct PublishedRaw {
    pub object: RawObjectRef,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct RawObjectStore {
    root: PathBuf,
    capacity: RootCapacity,
}

#[derive(Debug, Default)]
struct ReservationState {
    reserved_bytes: u64,
}

#[derive(Debug, Clone)]
struct RootCapacity {
    root: PathBuf,
    reservations: Arc<Mutex<ReservationState>>,
}

#[derive(Debug)]
#[must_use = "holding the guard keeps root capacity reserved"]
pub struct RootReservation {
    reservations: Arc<Mutex<ReservationState>>,
    bytes: u64,
}

impl Drop for RootReservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.reservations.lock() {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.bytes);
        }
    }
}

impl RootCapacity {
    fn reserve(&self, bytes: u64) -> Result<RootReservation, LabError> {
        if bytes == 0 || bytes > MAX_DATA_ROOT_BYTES {
            return Err(LabError::InvalidConfig(
                "root reservation must be within 1..=root limit".into(),
            ));
        }
        let mut state = self
            .reservations
            .lock()
            .map_err(|_| LabError::Internal("root reservation state is poisoned".into()))?;
        self.ensure_capacity(state.reserved_bytes, bytes)?;
        state.reserved_bytes = state
            .reserved_bytes
            .checked_add(bytes)
            .ok_or_else(|| LabError::ResourceLimit("root reservation overflow".into()))?;
        Ok(RootReservation {
            reservations: Arc::clone(&self.reservations),
            bytes,
        })
    }

    fn admit(&self, bytes: u64) -> Result<(), LabError> {
        let state = self
            .reservations
            .lock()
            .map_err(|_| LabError::Internal("root reservation state is poisoned".into()))?;
        self.ensure_capacity(state.reserved_bytes, bytes)
    }

    fn ensure_capacity(&self, reserved: u64, requested: u64) -> Result<(), LabError> {
        let actual = directory_size(&self.root)?;
        let database_bytes = file_size(&self.root.join(DATABASE_FILE))?;
        let wal_bytes = file_size(&self.root.join(format!("{DATABASE_FILE}-wal")))?;
        let database_headroom = MAX_DATABASE_BYTES.saturating_sub(database_bytes);
        let wal_headroom = DATABASE_WAL_HEADROOM_BYTES.saturating_sub(wal_bytes);
        let admitted = actual
            .checked_add(database_headroom)
            .and_then(|value| value.checked_add(wal_headroom))
            .and_then(|value| value.checked_add(reserved))
            .and_then(|value| value.checked_add(requested))
            .ok_or_else(|| {
                LabError::ResourceLimit("data root capacity arithmetic overflow".into())
            })?;
        if admitted > MAX_DATA_ROOT_BYTES {
            return Err(LabError::ResourceLimit(format!(
                "data root admission would exceed {MAX_DATA_ROOT_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

/// Storage format marker for slim collection page rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredPageFormat {
    SlimV1,
}

/// New-format persisted collection page: cursor, order, raw link and version
/// only. Candle bodies live once in `candle_observations` and are restored
/// through `collection_page_members`; legacy rows keep their full-body JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SlimCollectionPage {
    format: StoredPageFormat,
    request_id: RequestId,
    market: MarketId,
    requested_to: UtcTimestamp,
    next_to: Option<UtcTimestamp>,
    raw_object: RawObjectRef,
    page_index: u32,
    observation_count: u32,
}

impl SlimCollectionPage {
    fn from_collection_page(page: &CollectionPage) -> Result<Self, LabError> {
        Ok(Self {
            format: StoredPageFormat::SlimV1,
            request_id: page.request_id.clone(),
            market: page.market.clone(),
            requested_to: page.requested_to,
            next_to: page.next_to,
            raw_object: page.raw_object.clone(),
            page_index: page.page_index,
            observation_count: u32::try_from(page.observations.len())
                .map_err(|_| LabError::ResourceLimit("page observation count overflow".into()))?,
        })
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetDerivation {
    pub derived_dataset_id: DatasetId,
    pub source_dataset_id: DatasetId,
    pub transform_version: String,
    pub target_interval: crate::contracts::CandleInterval,
    pub closure_digest: ContentHash,
}

pub(super) struct PendingDerivation {
    source_dataset_id: DatasetId,
    transform_version: String,
    target_interval: crate::contracts::CandleInterval,
    closure_digest: ContentHash,
}

pub(super) struct PreparedDatasetPublication {
    digests: DatasetDigests,
}

#[derive(Clone, Copy)]
enum RestoreRootPolicy {
    #[cfg(test)]
    StrictEmpty,
    HeldOwnerLock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupManifest {
    schema_version: i64,
    raw_objects: Vec<BackupRaw>,
    artifacts: Vec<BackupArtifact>,
    datasets: Vec<BackupDataset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupRaw {
    id: String,
    relative_path: String,
    raw_sha256: String,
    compressed_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupDataset {
    id: String,
    semantic_digest: String,
    provenance_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupArtifact {
    id: String,
    relative_path: String,
    bytes: u64,
    sha256: String,
}

pub struct Store {
    connection: Connection,
    raw: RawObjectStore,
}

impl RawObjectStore {
    /// Open or create the single raw-object publication root.
    fn open(data_root: impl AsRef<Path>) -> Result<Self, LabError> {
        let root = data_root.as_ref().to_path_buf();
        ensure_root(&root)?;
        let raw_root = root.join(RAW_DIR);
        create_directory(&raw_root)?;
        reject_symlink(&raw_root)?;
        sync_directory(&raw_root)?;
        sync_directory(&root)?;
        Ok(Self {
            capacity: RootCapacity {
                root: root.clone(),
                reservations: Arc::new(Mutex::new(ReservationState::default())),
            },
            root,
        })
    }

    /// Publish bounded source bytes by durable same-filesystem directory rename.
    fn publish(&self, input: RawObjectInput) -> Result<PublishedRaw, LabError> {
        if input.source_url.is_empty()
            || input.source_url.len() > 2_048
            || input
                .remaining_req
                .as_ref()
                .is_some_and(|value| value.len() > 4_096)
            || !(100..=599).contains(&input.http_status)
        {
            return Err(LabError::InvalidConfig(
                "raw response metadata exceeds bounds or has invalid HTTP status".into(),
            ));
        }
        if input.body.len() > MAX_RAW_OBJECT_BYTES {
            return Err(LabError::ResourceLimit(format!(
                "raw response is {} bytes; limit is {MAX_RAW_OBJECT_BYTES}",
                input.body.len()
            )));
        }
        let raw_sha256 = ContentHash::of_bytes(&input.body);
        let compressed = deterministic_gzip(&input.body)?;
        let compressed_sha256 = ContentHash::of_bytes(&compressed);
        let seed = serde_json::to_string(&(
            raw_sha256.as_str(),
            compressed_sha256.as_str(),
            &input.source_url,
            input.fetched_at,
            input.http_status,
            &input.remaining_req,
            input.origin,
        ))
        .map_err(json_error)?;
        let id = RawObjectId::from_seed(&seed);
        let shard = &raw_sha256.as_str()[..2];
        let relative_path = format!("{RAW_DIR}/{shard}/{id}");
        validate_relative_path(&relative_path)?;
        let parent = self.root.join(RAW_DIR).join(shard);
        create_directory(&parent)?;
        reject_symlink(&parent)?;
        sync_directory(&self.root.join(RAW_DIR))?;
        let destination = self.root.join(&relative_path);
        let object = RawObjectRef {
            id,
            relative_path,
            source_url: input.source_url,
            fetched_at: input.fetched_at,
            persisted_at: input.persisted_at,
            http_status: input.http_status,
            remaining_req: input.remaining_req,
            raw_sha256,
            compressed_sha256,
            raw_bytes: input.body.len() as u64,
            compressed_bytes: compressed.len() as u64,
            origin: input.origin,
        };

        if destination.exists() {
            let stored: RawObjectRef = serde_json::from_slice(
                &fs::read(destination.join(META_FILE))
                    .map_err(io_error("read existing raw metadata"))?,
            )
            .map_err(json_error)?;
            self.verify(&stored)?;
            return Ok(PublishedRaw {
                object: stored,
                body: input.body,
            });
        }
        let metadata = serde_json::to_vec_pretty(&object).map_err(json_error)?;
        let additional =
            u64::try_from(compressed.len().saturating_add(metadata.len())).map_err(|_| {
                LabError::ResourceLimit("raw publication size exceeds platform capacity".into())
            })?;
        self.capacity.admit(additional)?;

        let temporary = parent.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&temporary).map_err(io_error("create raw temporary directory"))?;
        let publication = (|| {
            write_synced(&temporary.join(BODY_FILE), &compressed)?;
            write_synced(&temporary.join(META_FILE), &metadata)?;
            sync_directory(&temporary)?;
            fs::rename(&temporary, &destination).map_err(io_error("publish raw object"))?;
            sync_directory(&parent)
        })();
        if publication.is_err() && temporary.exists() {
            let _ignored = fs::remove_dir_all(&temporary);
        }
        publication?;
        Ok(PublishedRaw {
            object,
            body: input.body,
        })
    }

    fn verify(&self, object: &RawObjectRef) -> Result<(), LabError> {
        self.read_verified(object)?;
        Ok(())
    }

    /// Read raw bytes only after path, length, compressed hash, and raw hash verification.
    ///
    /// # Errors
    /// Returns an error for unsafe paths, missing/corrupt content, or invalid metadata.
    pub fn read_verified(&self, object: &RawObjectRef) -> Result<Vec<u8>, LabError> {
        validate_object_path(object)?;
        let directory = self.root.join(&object.relative_path);
        reject_symlink_chain(&self.root, &directory)?;
        let metadata = fs::symlink_metadata(&directory).map_err(io_error("inspect raw object"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(LabError::DataCorrupt(format!(
                "raw object {} is not a regular directory",
                object.id
            )));
        }
        reject_symlink(&directory.join(BODY_FILE))?;
        reject_symlink(&directory.join(META_FILE))?;
        let compressed = fs::read(directory.join(BODY_FILE)).map_err(io_error("read raw body"))?;
        if compressed.len() as u64 != object.compressed_bytes
            || ContentHash::of_bytes(&compressed) != object.compressed_sha256
        {
            return Err(LabError::DataCorrupt(format!(
                "compressed raw object hash mismatch: {}",
                object.id
            )));
        }
        let body = bounded_gunzip(&compressed, object.raw_bytes)?;
        if ContentHash::of_bytes(&body) != object.raw_sha256 {
            return Err(LabError::DataCorrupt(format!(
                "uncompressed raw object hash mismatch: {}",
                object.id
            )));
        }
        let stored: RawObjectRef = serde_json::from_slice(
            &fs::read(directory.join(META_FILE)).map_err(io_error("read raw metadata"))?,
        )
        .map_err(json_error)?;
        if serde_json::to_vec(&stored).map_err(json_error)?
            != serde_json::to_vec(object).map_err(json_error)?
        {
            return Err(LabError::DataCorrupt(format!(
                "raw object metadata mismatch: {}",
                object.id
            )));
        }
        Ok(body)
    }
}

impl Store {
    /// Open the database, configure connection invariants, and apply migrations.
    ///
    /// # Errors
    /// Returns a typed storage error for unsafe paths, SQLite failures, or invalid migrations.
    pub fn open(data_root: impl AsRef<Path>) -> Result<Self, LabError> {
        let root = data_root.as_ref().to_path_buf();
        ensure_root(&root)?;
        let raw = RawObjectStore::open(&root)?;
        let database_path = root.join(DATABASE_FILE);
        if database_path.exists() {
            reject_symlink(&database_path)?;
        }
        let mut connection = Connection::open(database_path).map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_secs(1))
            .map_err(sql_error)?;
        configure_database_capacity(&connection)?;
        migrations::apply(&mut connection)?;
        delete_store::recover_pending_file_removals(&mut connection, &root)?;
        Ok(Self { connection, raw })
    }

    /// Persist one bounded raw response before any database reference is created.
    ///
    /// # Errors
    /// Returns an error for capacity, path, compression, or durable publication failure.
    pub fn publish_raw(&self, input: RawObjectInput) -> Result<PublishedRaw, LabError> {
        self.raw.publish(input)
    }

    /// Reserve worst-case bytes for an out-of-owner export until its files are catalogued.
    ///
    /// The returned guard is `Send`; dropping it releases the in-process reservation.
    ///
    /// # Errors
    /// Rejects zero/oversized reservations, poisoned state, or root-capacity excess.
    pub fn reserve_root_bytes(&self, bytes: u64) -> Result<RootReservation, LabError> {
        self.raw.capacity.reserve(bytes)
    }

    #[must_use]
    pub fn raw_objects(&self) -> &RawObjectStore {
        &self.raw
    }

    /// Catalog a previously published and verified raw object without a collection page.
    ///
    /// # Errors
    /// Rejects missing/corrupt evidence, immutable identity conflicts, or SQLite failure.
    pub fn catalog_raw(&mut self, object: &RawObjectRef) -> Result<(), LabError> {
        self.raw.verify(object)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_raw(&transaction, object)?;
        transaction.commit().map_err(sql_error)
    }

    /// Catalog every fetched response against its collection request.
    ///
    /// This includes responses rejected before parsing or page commitment, so
    /// retry/error traffic consumes the same durable 64 MiB request budget.
    ///
    /// # Errors
    /// Rejects unknown requests, corrupt evidence, budget excess, conflicts, or SQLite failure.
    pub fn catalog_collection_raw(
        &mut self,
        request_id: &RequestId,
        object: &RawObjectRef,
    ) -> Result<(), LabError> {
        self.raw.verify(object)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let request_exists = transaction
            .query_row(
                "SELECT 1 FROM collections WHERE request_id=?1",
                [request_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_error)?;
        if request_exists.is_none() {
            return Err(LabError::InvalidConfig(
                "collection request must be registered before cataloging raw responses".into(),
            ));
        }
        insert_raw(&transaction, object)?;
        link_collection_raw(&transaction, request_id, object)?;
        transaction.commit().map_err(sql_error)
    }

    /// Load all fetched raw responses for one collection in durable link order.
    ///
    /// # Errors
    /// Returns an error for corrupt metadata/evidence or SQLite failure.
    pub fn load_collection_raw_objects(
        &self,
        request_id: &RequestId,
    ) -> Result<Vec<RawObjectRef>, LabError> {
        let mut statement = self.connection.prepare(
            "SELECT r.object_json FROM collection_raw_objects c JOIN raw_objects r ON r.id=c.raw_object_id WHERE c.request_id=?1 ORDER BY c.linked_index",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([request_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut objects = Vec::new();
        for row in rows {
            let object: RawObjectRef =
                serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?;
            self.raw.verify(&object)?;
            objects.push(object);
        }
        Ok(objects)
    }

    /// Total uncompressed bytes linked to one collection request.
    ///
    /// # Errors
    /// Returns an error for negative/corrupt stored sizes or SQLite failure.
    pub fn collection_raw_bytes(&self, request_id: &RequestId) -> Result<u64, LabError> {
        query_collection_raw_bytes(&self.connection, request_id)
    }

    /// Register a normalized collection request before any page is committed.
    ///
    /// # Errors
    /// Rejects a reused request id whose normalized input differs, or a SQLite failure.
    pub fn begin_collection(&mut self, request: &CollectRequest) -> Result<ContentHash, LabError> {
        let digest = normalized_request_digest(request)?;
        let request_json = serde_json::to_string(request).map_err(json_error)?;
        if let Some(existing) = self
            .connection
            .query_row(
                "SELECT normalized_request_digest FROM collections WHERE request_id=?1",
                [request.request_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
        {
            return if existing == digest.as_str() {
                Ok(digest)
            } else {
                Err(LabError::Conflict(
                    "request id already names different normalized collection input".into(),
                ))
            };
        }
        self.connection.execute(
            "INSERT INTO collections(request_id,normalized_request_digest,request_json) VALUES (?1,?2,?3)",
            params![request.request_id.as_str(), digest.as_str(), request_json],
        ).map_err(sql_error)?;
        Ok(digest)
    }

    /// Commit one published raw reference, normalized rows, and resume cursor atomically.
    ///
    /// The candle bodies live once in `candle_observations`; the page row stores
    /// only the slim `slim_v1` header (cursor, order, raw link, version), and
    /// readers rebuild bodies through `collection_page_members`. Legacy
    /// full-body rows written by older builds stay readable as stored.
    ///
    /// # Errors
    /// Rejects unregistered/conflicting input, corrupt raw evidence, limits, or SQLite failure.
    pub fn commit_page(&mut self, page: &CollectionPage) -> Result<(), LabError> {
        let registered = self
            .connection
            .query_row(
                "SELECT request_json FROM collections WHERE request_id=?1",
                [page.request_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(registered) = registered else {
            return Err(LabError::InvalidConfig(
                "collection request must be registered before committing pages".into(),
            ));
        };
        let request: CollectRequest = serde_json::from_str(&registered).map_err(json_error)?;
        if !request.markets.contains(&page.market) {
            return Err(LabError::Conflict(
                "collection page market is absent from its registered request".into(),
            ));
        }
        if page.page_index >= MAX_COLLECTION_CALLS {
            return Err(LabError::ResourceLimit(format!(
                "collection page index exceeds {MAX_COLLECTION_CALLS} call limit"
            )));
        }
        if page.observations.len() > MAX_PAGE_OBSERVATIONS {
            return Err(LabError::ResourceLimit(format!(
                "collection page exceeds {MAX_PAGE_OBSERVATIONS} observations"
            )));
        }
        for observation in &page.observations {
            if observation.candle.market != page.market.code()
                || !observation
                    .raw_object_ids
                    .iter()
                    .any(|id| id == &page.raw_object.id)
            {
                return Err(LabError::Conflict(
                    "collection page observation market/raw provenance mismatch".into(),
                ));
            }
        }
        self.raw.verify(&page.raw_object)?;
        let stored_json = serde_json::to_string(&SlimCollectionPage::from_collection_page(page)?)
            .map_err(json_error)?;
        let page_digest = ContentHash::of_bytes(stored_json.as_bytes());
        if let Some(existing) = self.connection.query_row(
            "SELECT page_digest FROM collection_pages WHERE request_id=?1 AND market=?2 AND page_index=?3",
            params![page.request_id.as_str(), page.market.code(), page.page_index],
            |row| row.get::<_, String>(0),
        ).optional().map_err(sql_error)? {
            return if existing == page_digest.as_str() { Ok(()) } else {
                Err(LabError::Conflict("collection page identity already has different content".into()))
            };
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_raw(&transaction, &page.raw_object)?;
        link_collection_raw(&transaction, &page.request_id, &page.raw_object)?;
        for observation in &page.observations {
            insert_observation(&transaction, observation)?;
        }
        transaction.execute(
            "INSERT INTO collection_pages(request_id,market,page_index,requested_to_ms,next_to_ms,raw_object_id,page_digest,page_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![page.request_id.as_str(), page.market.code(), page.page_index, timestamp_ms(page.requested_to), page.next_to.map(timestamp_ms), page.raw_object.id.as_str(), page_digest.as_str(), stored_json],
        ).map_err(sql_error)?;
        for (position, observation) in page.observations.iter().enumerate() {
            transaction.execute(
                "INSERT INTO collection_page_members(request_id,market,page_index,observation_id,position) VALUES (?1,?2,?3,?4,?5)",
                params![page.request_id.as_str(), page.market.code(), page.page_index, observation.id.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)
    }

    /// Read committed pages in page-index order to rebuild collection state after restart.
    ///
    /// Slim `slim_v1` pages rebuild their candle bodies through one batched
    /// members join; legacy full-body rows are read exactly as stored.
    ///
    /// # Errors
    /// Returns an error for corrupt/oversized persisted pages, raw evidence, or SQLite failure.
    pub fn load_collection_pages(
        &self,
        request: &RequestId,
        market: &MarketId,
    ) -> Result<Vec<CollectionPage>, LabError> {
        let mut statement = self.connection.prepare(
            "SELECT page_index,page_json FROM collection_pages WHERE request_id=?1 AND market=?2 ORDER BY page_index LIMIT ?3",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    request.as_str(),
                    market.code(),
                    i64::from(MAX_COLLECTION_CALLS) + 1
                ],
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(sql_error)?;
        let mut pages = Vec::new();
        let mut slim_pages = Vec::new();
        for row in rows {
            let (page_index, page_json) = row.map_err(sql_error)?;
            let value: serde_json::Value = serde_json::from_str(&page_json).map_err(json_error)?;
            if value.get("format").and_then(|format| format.as_str()) == Some("slim_v1") {
                let slim: SlimCollectionPage = serde_json::from_value(value).map_err(json_error)?;
                if page_index != slim.page_index {
                    return Err(LabError::DataCorrupt(
                        "slim collection page index disagrees with its row".into(),
                    ));
                }
                self.raw.verify(&slim.raw_object)?;
                slim_pages.push((page_index, slim));
            } else {
                let page: CollectionPage = serde_json::from_value(value).map_err(json_error)?;
                if page.page_index != page_index {
                    return Err(LabError::DataCorrupt(
                        "collection page index disagrees with its row".into(),
                    ));
                }
                self.raw.verify(&page.raw_object)?;
                for observation in &page.observations {
                    validate_observation_identity(observation)?;
                }
                pages.push(page);
            }
        }
        drop(statement);
        if !slim_pages.is_empty() {
            pages.extend(self.rebuild_slim_pages(request, market, &slim_pages)?);
        }
        pages.sort_by_key(|page| page.page_index);
        let page_limit = usize::try_from(MAX_COLLECTION_CALLS)
            .map_err(|_| LabError::ResourceLimit("page limit exceeds platform capacity".into()))?;
        if pages.len() > page_limit {
            return Err(LabError::DataCorrupt(
                "stored collection exceeds page limit".into(),
            ));
        }
        Ok(pages)
    }

    /// Rebuild slim pages' candle bodies with one batched members join.
    ///
    /// Page-time provenance is preserved: every observation committed to a page
    /// carried exactly that page's raw object, so reconstruction re-pins each
    /// body to the header's raw reference instead of reading the global
    /// observation projection, which later duplicate commits may have extended.
    fn rebuild_slim_pages(
        &self,
        request: &RequestId,
        market: &MarketId,
        slim_pages: &[(u32, SlimCollectionPage)],
    ) -> Result<Vec<CollectionPage>, LabError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT pm.page_index, o.observation_json \
             FROM collection_page_members pm \
             JOIN candle_observations o ON o.id=pm.observation_id \
             WHERE pm.request_id=?1 AND pm.market=?2 \
             ORDER BY pm.page_index, pm.position",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![request.as_str(), market.code()], |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let slim_raw_objects: BTreeMap<u32, &RawObjectRef> = slim_pages
            .iter()
            .map(|(index, slim)| (*index, &slim.raw_object))
            .collect();
        let mut bodies: BTreeMap<u32, Vec<CandleObservation>> = BTreeMap::new();
        for (page_index, observation_json) in rows {
            let Some(raw_object) = slim_raw_objects.get(&page_index) else {
                continue;
            };
            let mut observation: CandleObservation =
                serde_json::from_str(&observation_json).map_err(json_error)?;
            observation.raw_object_ids = vec![raw_object.id.clone()];
            observation.constituent_ids = Vec::new();
            validate_observation_identity(&observation)?;
            bodies.entry(page_index).or_default().push(observation);
        }
        slim_pages
            .iter()
            .map(|(index, slim)| {
                let observations = bodies.remove(index).unwrap_or_default();
                if u32::try_from(observations.len())
                    .map_err(|_| LabError::ResourceLimit("slim page body count overflow".into()))?
                    != slim.observation_count
                {
                    return Err(LabError::DataCorrupt(format!(
                        "slim collection page {index} stored {} bodies but its header declares {}",
                        observations.len(),
                        slim.observation_count
                    )));
                }
                Ok(CollectionPage {
                    request_id: slim.request_id.clone(),
                    market: slim.market.clone(),
                    requested_to: slim.requested_to,
                    next_to: slim.next_to,
                    raw_object: slim.raw_object.clone(),
                    observations,
                    page_index: slim.page_index,
                })
            })
            .collect()
    }

    /// Validate and freeze an immutable dataset. Repeated identical calls are idempotent.
    ///
    /// # Errors
    /// Rejects invalid identities, conflicting requests, corrupt evidence, or SQLite failure.
    pub fn finish_dataset(&mut self, snapshot: &DatasetSnapshot) -> Result<DatasetId, LabError> {
        self.finish_dataset_internal(snapshot, None)
    }

    /// Atomically publish one exact UTC-resampled snapshot and its source lineage.
    ///
    /// # Errors
    /// Rejects invalid source/target closure, incomplete buckets, identity conflicts, or SQLite failure.
    pub fn finish_derived_dataset(
        &mut self,
        source_dataset_id: &DatasetId,
        snapshot: &DatasetSnapshot,
        transform_version: &str,
    ) -> Result<DatasetId, LabError> {
        let source = self.load_dataset(source_dataset_id)?.ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown source dataset {source_dataset_id}"))
        })?;
        let pending = validate_derived_snapshot(&source, snapshot, transform_version)?;
        self.finish_dataset_internal(snapshot, Some(&pending))
    }

    fn finish_dataset_internal(
        &mut self,
        snapshot: &DatasetSnapshot,
        derivation: Option<&PendingDerivation>,
    ) -> Result<DatasetId, LabError> {
        self.begin_collection(&snapshot.manifest.request)?;
        let prepared = self.prepare_dataset_publication(snapshot)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        insert_dataset_rows(&transaction, snapshot, &prepared, derivation)?;
        transaction.commit().map_err(sql_error)?;
        Ok(snapshot.manifest.id.clone())
    }

    pub(super) fn prepare_dataset_publication(
        &self,
        snapshot: &DatasetSnapshot,
    ) -> Result<PreparedDatasetPublication, LabError> {
        validate_snapshot(snapshot)?;
        let digests = dataset_digests(snapshot)?;
        let registered: String = self
            .connection
            .query_row(
                "SELECT normalized_request_digest FROM collections WHERE request_id=?1",
                [snapshot.manifest.request.request_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::InvalidConfig(
                    "dataset request must be registered before publication".into(),
                )
            })?;
        if registered != digests.normalized_request.as_str()
            || snapshot.manifest.semantic_digest != digests.semantic
            || snapshot.manifest.provenance_digest != digests.provenance
            || snapshot.manifest.id != dataset_id(&digests)
        {
            return Err(LabError::InputHashMismatch(
                "dataset request/id/digests do not match immutable snapshot".into(),
            ));
        }
        for raw in &snapshot.manifest.raw_objects {
            self.raw.verify(raw)?;
        }
        validate_dataset_identity_conflict(&self.connection, snapshot, &digests)?;
        Ok(PreparedDatasetPublication { digests })
    }

    /// Load explicit local lineage for one derived dataset. Test-visible
    /// accessor; the write path owns derivation validation.
    ///
    /// # Errors
    /// Returns an error for corrupt lineage fields or SQLite failure.
    #[cfg(test)]
    pub fn load_dataset_derivation(
        &self,
        derived_dataset_id: &DatasetId,
    ) -> Result<Option<DatasetDerivation>, LabError> {
        self.connection.query_row(
            "SELECT source_dataset_id,transform_version,target_interval,closure_digest FROM dataset_derivations WHERE derived_dataset_id=?1",
            [derived_dataset_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?)),
        ).optional().map_err(sql_error)?.map(|row| Ok(DatasetDerivation {
            derived_dataset_id: derived_dataset_id.clone(),
            source_dataset_id: DatasetId::new(row.0)?,
            transform_version: row.1,
            target_interval: crate::contracts::CandleInterval::parse_code(&row.2)?,
            closure_digest: ContentHash::try_from(row.3)?,
        })).transpose()
    }

    /// Load one immutable dataset snapshot in its stored member order.
    ///
    /// # Errors
    /// Returns an error for corrupt serialized state, invalid identities, or SQLite failure.
    pub fn load_dataset(&self, id: &DatasetId) -> Result<Option<DatasetSnapshot>, LabError> {
        let manifest = self
            .connection
            .query_row(
                "SELECT manifest_json FROM datasets WHERE id=?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };
        let manifest = serde_json::from_str(&manifest).map_err(json_error)?;
        let mut statement = self.connection.prepare("SELECT o.observation_json FROM dataset_members m JOIN candle_observations o ON o.id=m.observation_id WHERE m.dataset_id=?1 ORDER BY m.position").map_err(sql_error)?;
        let rows = statement
            .query_map([id.as_str()], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut observations = Vec::new();
        for row in rows {
            let mut observation: CandleObservation =
                serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?;
            let mut raw_statement = self.connection.prepare(
                "SELECT raw_object_id FROM dataset_observation_raw_objects WHERE dataset_id=?1 AND observation_id=?2 ORDER BY position",
            ).map_err(sql_error)?;
            let raw_rows = raw_statement
                .query_map(params![id.as_str(), observation.id.as_str()], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_error)?;
            observation.raw_object_ids = raw_rows
                .map(|raw| RawObjectId::new(raw.map_err(sql_error)?))
                .collect::<Result<_, _>>()?;
            let mut constituent_statement = self.connection.prepare(
                "SELECT constituent_id FROM dataset_observation_constituents WHERE dataset_id=?1 AND observation_id=?2 ORDER BY position",
            ).map_err(sql_error)?;
            let constituent_rows = constituent_statement
                .query_map(params![id.as_str(), observation.id.as_str()], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_error)?;
            observation.constituent_ids = constituent_rows
                .map(|constituent| ObservationId::new(constituent.map_err(sql_error)?))
                .collect::<Result<_, _>>()?;
            observations.push(observation);
        }
        Ok(Some(DatasetSnapshot {
            manifest,
            observations,
        }))
    }

    /// Load stored completed observations eligible for incremental reuse.
    ///
    /// Returns every previously stored completed observation for the markets and
    /// interval inside the half-open range, with its durable raw provenance, so a
    /// new collection request can reuse overlapping data instead of refetching.
    ///
    /// # Errors
    /// Returns an error for corrupt stored identities or SQLite failure.
    pub fn load_reusable_observations(
        &self,
        markets: &[MarketId],
        interval: crate::contracts::CandleInterval,
        start: UtcTimestamp,
        end: UtcTimestamp,
    ) -> Result<Vec<CandleObservation>, LabError> {
        if markets.is_empty() {
            return Ok(Vec::new());
        }
        let mut codes = Vec::new();
        for market in markets {
            codes.push(market.code());
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT observation_json FROM candle_observations \
             WHERE market IN (SELECT value FROM json_each(?1)) AND interval=?2 AND completed=1 \
             AND open_time_ms>=?3 AND open_time_ms<?4 \
             ORDER BY market, open_time_ms",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    serde_json::to_string(&codes).map_err(json_error)?,
                    enum_text(&interval)?,
                    timestamp_ms(start),
                    timestamp_ms(end)
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut observations = Vec::new();
        for row in rows {
            let observation: CandleObservation =
                serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?;
            validate_observation_identity(&observation)?;
            observations.push(observation);
        }
        Ok(observations)
    }

    /// Load the raw-object provenance rows referenced by reusable observations.
    ///
    /// # Errors
    /// Returns an error for corrupt metadata or SQLite failure.
    pub fn load_reusable_raw_objects(
        &self,
        markets: &[MarketId],
        interval: crate::contracts::CandleInterval,
        start: UtcTimestamp,
        end: UtcTimestamp,
    ) -> Result<Vec<RawObjectRef>, LabError> {
        if markets.is_empty() {
            return Ok(Vec::new());
        }
        let codes: Vec<String> = markets.iter().map(|market| market.code().clone()).collect();
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT r.object_json FROM observation_raw_objects o \
             JOIN candle_observations c ON c.id=o.observation_id \
             JOIN raw_objects r ON r.id=o.raw_object_id \
             WHERE c.market IN (SELECT value FROM json_each(?1)) AND c.interval=?2 AND c.completed=1 \
             AND c.open_time_ms>=?3 AND c.open_time_ms<?4",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    serde_json::to_string(&codes).map_err(json_error)?,
                    enum_text(&interval)?,
                    timestamp_ms(start),
                    timestamp_ms(end)
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut objects = Vec::new();
        for row in rows {
            let object: RawObjectRef =
                serde_json::from_str(&row.map_err(sql_error)?).map_err(json_error)?;
            objects.push(object);
        }
        Ok(objects)
    }

    /// List bounded dataset identities, newest first.
    ///
    /// # Errors
    /// Rejects an out-of-range limit or corrupt/failed SQLite results.
    pub fn list_datasets(&self, limit: usize) -> Result<Vec<DatasetId>, LabError> {
        if limit == 0 || limit > 1_000 {
            return Err(LabError::ResourceLimit(
                "dataset list limit must be 1..=1000".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare("SELECT id FROM datasets ORDER BY rowid DESC LIMIT ?1")
            .map_err(sql_error)?;
        let rows = statement
            .query_map([usize_to_i64(limit)?], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        rows.map(|row| DatasetId::new(row.map_err(sql_error)?))
            .collect()
    }

    /// List published raw directories that have no database reference. Never deletes them.
    /// Test-visible diagnostic; production cleanup goes through hard delete.
    ///
    /// # Errors
    /// Returns an error for unsafe filesystem entries or failed filesystem/SQLite reads.
    #[cfg(test)]
    pub fn scan_orphans(&self) -> Result<Vec<String>, LabError> {
        let mut known = BTreeSet::new();
        let mut statement = self
            .connection
            .prepare("SELECT relative_path FROM raw_objects")
            .map_err(sql_error)?;
        for row in statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
        {
            known.insert(row.map_err(sql_error)?);
        }
        let mut found = Vec::new();
        let raw_root = self.raw.root.join(RAW_DIR);
        for shard in read_directories(&raw_root)? {
            for object in read_directories(&shard)? {
                let relative = object
                    .strip_prefix(&self.raw.root)
                    .map_err(|error| LabError::Internal(format!("orphan path: {error}")))?
                    .to_string_lossy()
                    .replace('\\', "/");
                if !known.contains(&relative) {
                    found.push(relative);
                }
            }
        }
        found.sort();
        Ok(found)
    }

    /// Create a consistent SQLite/raw backup in a new directory.
    ///
    /// # Errors
    /// Rejects an existing destination or corrupt evidence and propagates backup I/O failures.
    /// Categorized storage accounting for status surfaces.
    ///
    /// SQLite numbers come from the live connection pragmas (allocated =
    /// `page_count * page_size`, reusable = `freelist * page_size`); WAL, raw,
    /// sealed ledgers and exports are separate on-disk observations.
    ///
    /// # Errors
    /// Reports SQLite or filesystem read failures.
    pub fn storage_accounting(&self) -> Result<serde_json::Value, LabError> {
        let page_size: i64 = self
            .connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(sql_error)?;
        let page_count: i64 = self
            .connection
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .map_err(sql_error)?;
        let freelist: i64 = self
            .connection
            .pragma_query_value(None, "freelist_count", |row| row.get(0))
            .map_err(sql_error)?;
        let allocated = page_count.saturating_mul(page_size);
        let reusable = freelist.saturating_mul(page_size);
        let wal_path = self.raw.root.join(format!("{DATABASE_FILE}-wal"));
        let wal_bytes: i64 = if wal_path
            .try_exists()
            .map_err(io_error("inspect SQLite WAL"))?
        {
            i64::try_from(file_size(&wal_path)?).unwrap_or(i64::MAX)
        } else {
            0
        };
        let directory = |relative: &str| -> Result<u64, LabError> {
            let path = self.raw.root.join(relative);
            if path
                .try_exists()
                .map_err(io_error("inspect storage directory"))?
            {
                directory_size(&path)
            } else {
                Ok(0)
            }
        };
        let to_i64 = |bytes: u64| i64::try_from(bytes).unwrap_or(i64::MAX);
        let raw_bytes = to_i64(directory("raw")?);
        let sealed_bytes = to_i64(directory("ledgers")?);
        let exports_bytes = to_i64(directory("exports")?);
        Ok(serde_json::json!({
            "sqlite": {
                "allocated_bytes": allocated,
                "live_bytes": allocated.saturating_sub(reusable),
                "reusable_bytes": reusable,
                "wal_bytes": wal_bytes,
                "limit_bytes": MAX_DATABASE_BYTES,
            },
            "raw_bytes": raw_bytes,
            "sealed_ledger_bytes": sealed_bytes,
            "exports_bytes": exports_bytes,
            "total_managed_bytes": allocated
                .saturating_add(wal_bytes)
                .saturating_add(raw_bytes)
                .saturating_add(sealed_bytes)
                .saturating_add(exports_bytes),
        }))
    }

    /// Write one verifiable online backup of this store into a new directory.
    ///
    /// # Errors
    /// Reports filesystem, SQLite, or verification failures.
    pub fn backup(&self, destination: impl AsRef<Path>) -> Result<(), LabError> {
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(LabError::Conflict(
                "backup destination already exists".into(),
            ));
        }
        let parent = destination
            .parent()
            .ok_or_else(|| LabError::InvalidConfig("backup destination needs a parent".into()))?;
        create_directory(parent)?;
        reject_symlink(parent)?;
        let temporary = parent.join(format!(
            ".backup-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&temporary).map_err(io_error("create backup temporary directory"))?;
        let result = (|| {
            self.connection
                .backup(MAIN_DB, temporary.join(DATABASE_FILE), None)
                .map_err(sql_error)?;
            let manifest = self.backup_manifest()?;
            for raw in &manifest.raw_objects {
                let object: RawObjectRef = self
                    .connection
                    .query_row(
                        "SELECT object_json FROM raw_objects WHERE id=?1",
                        [&raw.id],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(sql_error)
                    .and_then(|json| serde_json::from_str(&json).map_err(json_error))?;
                self.raw.verify(&object)?;
                copy_tree(
                    &self.raw.root.join(&object.relative_path),
                    &temporary.join(&object.relative_path),
                )?;
            }
            for artifact in &manifest.artifacts {
                validate_relative_path(&artifact.relative_path)?;
                let source = self.raw.root.join(&artifact.relative_path);
                reject_symlink_chain(&self.raw.root, &source)?;
                copy_file_synced(&source, &temporary.join(&artifact.relative_path))?;
            }
            // Sealed ledger chunks are part of the durable run payload.
            let ledgers_dir = self.raw.root.join("ledgers");
            if ledgers_dir.exists() {
                copy_tree(&ledgers_dir, &temporary.join("ledgers"))?;
            }
            write_synced(
                &temporary.join("backup-manifest.json"),
                &serde_json::to_vec_pretty(&manifest).map_err(json_error)?,
            )?;
            sync_directory(&temporary)?;
            fs::rename(&temporary, destination).map_err(io_error("publish backup"))?;
            sync_directory(parent)
        })();
        if result.is_err() && temporary.exists() {
            let _ignored = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Restore a verified backup into a new, empty data root.
    ///
    /// # Errors
    /// Rejects nonempty/unsafe roots, corrupt manifests or hashes, and failed copy/SQLite work.
    #[cfg(test)]
    pub fn restore(backup: impl AsRef<Path>, new_root: impl AsRef<Path>) -> Result<Self, LabError> {
        Self::restore_impl(
            backup.as_ref(),
            new_root.as_ref(),
            RestoreRootPolicy::StrictEmpty,
        )
    }

    /// Restore while the caller holds the root's exclusive `.lab-owner.lock`.
    ///
    /// The root must contain exactly that regular lock file and nothing else.
    /// The caller retains lock ownership across this call and the returned Store.
    ///
    /// # Errors
    /// Rejects missing/unsafe lock ownership, other root entries, corrupt backup data,
    /// failed independent run verification, or filesystem/SQLite failure.
    pub(crate) fn restore_locked(
        backup: impl AsRef<Path>,
        new_root: impl AsRef<Path>,
    ) -> Result<Self, LabError> {
        Self::restore_impl(
            backup.as_ref(),
            new_root.as_ref(),
            RestoreRootPolicy::HeldOwnerLock,
        )
    }

    fn restore_impl(
        backup: &Path,
        new_root: &Path,
        root_policy: RestoreRootPolicy,
    ) -> Result<Self, LabError> {
        reject_symlink_chain(backup, backup)?;
        validate_restore_root(new_root, root_policy)?;
        ensure_root(new_root)?;
        reject_symlink(&backup.join("backup-manifest.json"))?;
        reject_symlink(&backup.join(DATABASE_FILE))?;
        let manifest: BackupManifest = serde_json::from_slice(
            &fs::read(backup.join("backup-manifest.json"))
                .map_err(io_error("read backup manifest"))?,
        )
        .map_err(json_error)?;
        fs::copy(backup.join(DATABASE_FILE), new_root.join(DATABASE_FILE))
            .map_err(io_error("copy backup database"))?;
        for raw in &manifest.raw_objects {
            validate_backup_raw(raw)?;
            reject_symlink_chain(backup, &backup.join(&raw.relative_path))?;
            copy_tree(
                &backup.join(&raw.relative_path),
                &new_root.join(&raw.relative_path),
            )?;
        }
        for artifact in &manifest.artifacts {
            validate_relative_path(&artifact.relative_path)?;
            let source = backup.join(&artifact.relative_path);
            reject_symlink_chain(backup, &source)?;
            copy_file_synced(&source, &new_root.join(&artifact.relative_path))?;
        }
        // Sealed ledger chunks ride along with the backup when present.
        let backup_ledgers = backup.join("ledgers");
        if backup_ledgers.exists() {
            copy_tree(&backup_ledgers, &new_root.join("ledgers"))?;
        }
        sync_directory(new_root)?;
        let store = Self::open(new_root)?;
        store.verify_backup_contents(&manifest)?;
        Ok(store)
    }

    fn verify_backup_contents(&self, manifest: &BackupManifest) -> Result<(), LabError> {
        self.verify_database_integrity()?;
        let restored = self.backup_manifest()?;
        if serde_json::to_vec(manifest).map_err(json_error)?
            != serde_json::to_vec(&restored).map_err(json_error)?
        {
            return Err(LabError::DataCorrupt(
                "restored backup manifest does not match".into(),
            ));
        }
        for raw in &manifest.raw_objects {
            validate_backup_raw(raw)?;
            let object_json: String = self
                .connection
                .query_row(
                    "SELECT object_json FROM raw_objects WHERE id=?1",
                    [&raw.id],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            self.raw
                .verify(&serde_json::from_str(&object_json).map_err(json_error)?)?;
        }
        for artifact in &manifest.artifacts {
            validate_relative_path(&artifact.relative_path)?;
            reject_symlink_chain(&self.raw.root, &self.raw.root.join(&artifact.relative_path))?;
            let bytes = fs::read(self.raw.root.join(&artifact.relative_path))
                .map_err(io_error("read restored artifact"))?;
            if bytes.len() as u64 != artifact.bytes
                || ContentHash::of_bytes(&bytes).as_str() != artifact.sha256
            {
                return Err(LabError::DataCorrupt(format!(
                    "restored artifact hash mismatch: {}",
                    artifact.id
                )));
            }
        }
        for dataset in &manifest.datasets {
            let id = DatasetId::new(dataset.id.clone())?;
            let snapshot = self.load_dataset(&id)?.ok_or_else(|| {
                LabError::DataCorrupt(format!("restored dataset is missing: {id}"))
            })?;
            let digests = dataset_digests(&snapshot)?;
            if digests.semantic.as_str() != dataset.semantic_digest
                || digests.provenance.as_str() != dataset.provenance_digest
                || snapshot.manifest.id != dataset_id(&digests)
            {
                return Err(LabError::DataCorrupt(format!(
                    "restored dataset digest mismatch: {id}"
                )));
            }
        }
        self.verify_restored_runs()?;
        Ok(())
    }

    /// Verify an immutable backup without opening a writable Store or changing it.
    /// # Errors
    /// Rejects unsafe paths, oversized/incomplete snapshots and integrity failures.
    pub(super) fn verify_backup_directory(
        backup: &Path,
    ) -> Result<(ContentHash, ContentHash, u64), LabError> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        const HEX: &[u8; 16] = b"0123456789ABCDEF";

        reject_symlink_chain(backup, backup)?;
        let manifest_path = backup.join("backup-manifest.json");
        let database_path = backup.join(DATABASE_FILE);
        reject_symlink(&manifest_path)?;
        reject_symlink(&database_path)?;
        if file_size(&manifest_path)? > 16 * 1024 * 1024
            || file_size(&database_path)? > MAX_DATABASE_BYTES
        {
            return Err(LabError::ResourceLimit(
                "backup verification input exceeds bounds".into(),
            ));
        }
        let bytes = directory_size(backup)?;
        if bytes > MAX_DATA_ROOT_BYTES {
            return Err(LabError::ResourceLimit(
                "backup exceeds the source root bound".into(),
            ));
        }
        let encoded = fs::read(&manifest_path).map_err(io_error("read backup manifest"))?;
        let manifest: BackupManifest = serde_json::from_slice(&encoded).map_err(json_error)?;
        // Online Backup API publishes a self-contained immutable snapshot. The URI
        // mode prevents even read-only WAL connections from creating -shm files.
        let mut uri = String::from("file:");
        for byte in database_path.as_os_str().as_encoded_bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~".contains(byte) {
                uri.push(char::from(*byte));
            } else {
                uri.push('%');
                uri.push(char::from(HEX[usize::from(*byte >> 4)]));
                uri.push(char::from(HEX[usize::from(*byte & 15)]));
            }
        }
        uri.push_str("?immutable=1");
        let connection = Connection::open_with_flags(
            &uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(sql_error)?;
        let verified = Self {
            connection,
            raw: RawObjectStore {
                root: backup.to_path_buf(),
                capacity: RootCapacity {
                    root: backup.to_path_buf(),
                    reservations: Arc::new(Mutex::new(ReservationState::default())),
                },
            },
        };
        verified.verify_backup_contents(&manifest)?;
        let mut file =
            fs::File::open(&database_path).map_err(io_error("read verified backup database"))?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 8_192];
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(io_error("hash verified backup database"))?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        let database_digest = ContentHash::try_from(format!("{:x}", digest.finalize()))?;
        let manifest_digest = ContentHash::of_value(&manifest)?;
        let source_identity = ContentHash::of_value(&(&manifest_digest, &database_digest))?;
        Ok((manifest_digest, source_identity, bytes))
    }

    fn verify_restored_runs(&self) -> Result<(), LabError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM runs WHERE semantic_digest IS NOT NULL ORDER BY id")
            .map_err(sql_error)?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        for id in ids {
            let run_id = crate::contracts::RunId::new(id)?;
            let bundle = self.load_run_bundle(&run_id)?.ok_or_else(|| {
                LabError::DataCorrupt(format!("restored finalized run is missing: {run_id}"))
            })?;
            if crate::reporting::semantic_digest(&bundle)? != bundle.semantic_digest {
                return Err(LabError::DataCorrupt(format!(
                    "restored run semantic digest mismatch: {run_id}"
                )));
            }
            let report = crate::reporting::verify_run(&bundle);
            if report.status != crate::contracts::ValidationStatus::Pass {
                return Err(LabError::DataCorrupt(format!(
                    "restored run independent verification failed: {run_id}: {}",
                    report.findings.join("; ")
                )));
            }
        }
        Ok(())
    }

    fn backup_manifest(&self) -> Result<BackupManifest, LabError> {
        let schema_version = self
            .connection
            .query_row(
                "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let raw_objects = query_backup_raw(&self.connection)?;
        let artifacts = query_backup_artifacts(&self.connection)?;
        let datasets = query_backup_datasets(&self.connection)?;
        Ok(BackupManifest {
            schema_version,
            raw_objects,
            artifacts,
            datasets,
        })
    }

    fn verify_database_integrity(&self) -> Result<(), LabError> {
        let integrity: String = self
            .connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(sql_error)?;
        if integrity != "ok" {
            return Err(LabError::DataCorrupt(format!(
                "sqlite integrity check failed: {integrity}"
            )));
        }
        let foreign_key_failure = self
            .connection
            .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
            .optional()
            .map_err(sql_error)?;
        if foreign_key_failure.is_some() {
            return Err(LabError::DataCorrupt(
                "sqlite foreign key check failed".into(),
            ));
        }
        Ok(())
    }

    /// Checkpoint WAL state and explicitly close the owned SQLite connection.
    ///
    /// # Errors
    /// Returns the SQLite checkpoint or close error.
    pub fn close(self) -> Result<(), LabError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(sql_error)?;
        self.connection
            .close()
            .map_err(|(_connection, error)| sql_error(error))
    }
}

fn validate_observation_identity(observation: &CandleObservation) -> Result<(), LabError> {
    let digest = observation_digest(&observation.candle)?;
    if observation.content_digest != digest
        || observation.id != ObservationId::from_seed(digest.as_str())
    {
        return Err(LabError::InputHashMismatch(
            "observation id or content digest is not its canonical economic identity".into(),
        ));
    }
    Ok(())
}

fn validate_derived_snapshot(
    source: &DatasetSnapshot,
    derived: &DatasetSnapshot,
    transform_version: &str,
) -> Result<PendingDerivation, LabError> {
    if source.manifest.status != DatasetStatus::Ready
        || transform_version.is_empty()
        || transform_version.len() > 96
    {
        return Err(LabError::InvalidConfig(
            "derived publication requires READY source and bounded transform version".into(),
        ));
    }
    let source_interval = source.manifest.request.data_resolution;
    let target = derived.manifest.request.data_resolution;
    let source_seconds = source_interval.duration().num_seconds();
    let target_seconds = target.duration().num_seconds();
    if target_seconds <= source_seconds || target_seconds % source_seconds != 0 {
        return Err(LabError::InvalidConfig(
            "derived resolution must be strictly coarser and exactly divisible".into(),
        ));
    }
    derived.manifest.request.range.aligned(target)?;
    if derived.manifest.request.markets != source.manifest.request.markets
        || derived.manifest.request.range != source.manifest.request.range
        || derived.manifest.request.completed_only != source.manifest.request.completed_only
        || derived.manifest.schema_version != source.manifest.schema_version
        || derived.manifest.status != DatasetStatus::Ready
        || derived.manifest.gap_policy != source.manifest.gap_policy
        || derived.manifest.origin != source.manifest.origin
        || derived.manifest.normalizer_version
            != format!(
                "{}+{transform_version}",
                crate::contracts::NORMALIZER_VERSION
            )
    {
        return Err(LabError::Conflict(
            "derived snapshot metadata differs from immutable source contract".into(),
        ));
    }
    let request_seed =
        serde_json::to_string(&(source.manifest.id.clone(), target, transform_version))
            .map_err(json_error)?;
    if derived.manifest.request.request_id != RequestId::from_seed(&request_seed) {
        return Err(LabError::InputHashMismatch(
            "derived request id does not match source/target/transform identity".into(),
        ));
    }
    let warmup_seconds = (source.manifest.request.range.start().0
        - source.manifest.coverage.start().0)
        .num_seconds();
    if warmup_seconds < 0 {
        return Err(LabError::DataCorrupt(
            "source coverage begins after evaluation range".into(),
        ));
    }
    let warmup_bars = u32::try_from(warmup_seconds / target_seconds)
        .map_err(|_| LabError::ResourceLimit("derived warmup count overflow".into()))?;
    let expected_coverage = derived
        .manifest
        .request
        .range
        .with_warmup(warmup_bars, target)?;
    if derived.manifest.request.warmup_bars != warmup_bars
        || derived.manifest.coverage != expected_coverage
    {
        return Err(LabError::Conflict(
            "derived warmup/coverage is not the complete target-grid floor".into(),
        ));
    }
    if serde_json::to_vec(&derived.manifest.raw_objects).map_err(json_error)?
        != serde_json::to_vec(&source.manifest.raw_objects).map_err(json_error)?
    {
        return Err(LabError::Conflict(
            "derived snapshot must preserve the full ordered source raw catalog".into(),
        ));
    }
    validate_derived_closure(source, derived, target, expected_coverage)?;
    let digests = dataset_digests(derived)?;
    let closure_digest = ContentHash::of_value(&serde_json::json!({
        "source_dataset_id": source.manifest.id,
        "source_semantic_digest": source.manifest.semantic_digest,
        "source_provenance_digest": source.manifest.provenance_digest,
        "derived_dataset_id": derived.manifest.id,
        "derived_semantic_digest": digests.semantic,
        "derived_provenance_digest": digests.provenance,
        "transform_version": transform_version,
        "target_interval": enum_text(&target)?
    }))?;
    Ok(PendingDerivation {
        source_dataset_id: source.manifest.id.clone(),
        transform_version: transform_version.into(),
        target_interval: target,
        closure_digest,
    })
}

fn validate_derived_closure(
    source: &DatasetSnapshot,
    derived: &DatasetSnapshot,
    target: crate::contracts::CandleInterval,
    expected_coverage: crate::contracts::UtcRange,
) -> Result<(), LabError> {
    let source_rows = source
        .observations
        .iter()
        .filter(|row| {
            expected_coverage.contains(row.candle.open_time_utc)
                && row.candle.close_time_utc <= expected_coverage.end()
        })
        .cloned()
        .collect::<Vec<_>>();
    let expected_rows = crate::collection::resample(&source_rows, target)?;
    if serde_json::to_vec(&expected_rows).map_err(json_error)?
        != serde_json::to_vec(&derived.observations).map_err(json_error)?
        || derived.manifest.row_count != derived.observations.len() as u64
    {
        return Err(LabError::InputHashMismatch(
            "derived observations are not the exact complete resampling closure".into(),
        ));
    }
    let expected_quality = source
        .manifest
        .quality_issues
        .iter()
        .filter(|issue| {
            derived.manifest.request.markets.contains(&issue.market)
                && issue.start < expected_coverage.end()
                && issue.end > expected_coverage.start()
        })
        .cloned()
        .collect::<Vec<_>>();
    if serde_json::to_vec(&expected_quality).map_err(json_error)?
        != serde_json::to_vec(&derived.manifest.quality_issues).map_err(json_error)?
    {
        return Err(LabError::Conflict(
            "derived quality provenance does not match overlapping source issues".into(),
        ));
    }
    Ok(())
}

fn validate_existing_derivation(
    connection: &Connection,
    derived_dataset_id: &DatasetId,
    expected: &PendingDerivation,
) -> Result<(), LabError> {
    let row = connection
        .query_row(
            "SELECT source_dataset_id,transform_version,target_interval,closure_digest FROM dataset_derivations WHERE derived_dataset_id=?1",
            [derived_dataset_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let expected_interval = enum_text(&expected.target_interval)?;
    match row {
        Some((source, version, interval, digest))
            if source == expected.source_dataset_id.as_str()
                && version == expected.transform_version
                && interval == expected_interval
                && digest == expected.closure_digest.as_str() =>
        {
            Ok(())
        }
        Some(_) => Err(LabError::Conflict(
            "derived dataset identity already has different lineage".into(),
        )),
        None => Err(LabError::DataCorrupt(
            "derived dataset exists without atomic lineage".into(),
        )),
    }
}

fn validate_snapshot(snapshot: &DatasetSnapshot) -> Result<(), LabError> {
    validate_snapshot_bounds(snapshot)?;
    if snapshot.manifest.row_count != snapshot.observations.len() as u64 {
        return Err(LabError::DataCorrupt(
            "dataset row_count does not match members".into(),
        ));
    }
    if snapshot.manifest.status == DatasetStatus::Ready && snapshot.observations.is_empty() {
        return Err(LabError::DataCorrupt(
            "READY dataset must contain observations".into(),
        ));
    }
    if snapshot.manifest.status == DatasetStatus::Ready
        && snapshot
            .manifest
            .quality_issues
            .iter()
            .any(|issue| issue.severity == QualitySeverity::Error)
    {
        return Err(LabError::DataCorrupt(
            "READY dataset cannot contain ERROR quality issues".into(),
        ));
    }
    let raw_ids: BTreeSet<_> = snapshot
        .manifest
        .raw_objects
        .iter()
        .map(|raw| raw.id.as_str())
        .collect();
    if raw_ids.len() != snapshot.manifest.raw_objects.len() {
        return Err(LabError::Conflict(
            "duplicate raw object in dataset manifest".into(),
        ));
    }
    validate_snapshot_observations(snapshot, &raw_ids)?;
    validate_quality_sources(snapshot, &raw_ids)
}

fn validate_snapshot_bounds(snapshot: &DatasetSnapshot) -> Result<(), LabError> {
    let raw_limit = usize::try_from(MAX_COLLECTION_CALLS).map_err(|_| {
        LabError::ResourceLimit("raw-object limit exceeds platform capacity".into())
    })?;
    if snapshot.observations.len() > MAX_DATASET_ROWS
        || snapshot.manifest.raw_objects.len() > raw_limit
    {
        return Err(LabError::ResourceLimit(
            "dataset exceeds row or raw-object bounds".into(),
        ));
    }
    let observation_links = snapshot
        .observations
        .iter()
        .try_fold(0_usize, |total, observation| {
            total
                .checked_add(observation.raw_object_ids.len())
                .and_then(|value| value.checked_add(observation.constituent_ids.len()))
        })
        .ok_or_else(|| LabError::ResourceLimit("dataset provenance link count overflow".into()))?;
    let link_count = snapshot
        .manifest
        .quality_issues
        .iter()
        .try_fold(observation_links, |total, issue| {
            total.checked_add(issue.raw_object_ids.len())
        })
        .ok_or_else(|| LabError::ResourceLimit("dataset provenance link count overflow".into()))?;
    if link_count > MAX_SNAPSHOT_LINKS {
        return Err(LabError::ResourceLimit(format!(
            "dataset exceeds {MAX_SNAPSHOT_LINKS} provenance links"
        )));
    }
    Ok(())
}

fn validate_snapshot_observations(
    snapshot: &DatasetSnapshot,
    raw_ids: &BTreeSet<&str>,
) -> Result<(), LabError> {
    let request_markets: BTreeSet<_> = snapshot
        .manifest
        .request
        .markets
        .iter()
        .map(MarketId::code)
        .collect();
    let mut ids = BTreeSet::new();
    let mut previous_key = None;
    for observation in &snapshot.observations {
        validate_observation_identity(observation)?;
        if !request_markets.contains(observation.candle.market.as_str())
            || observation.candle.interval != snapshot.manifest.request.data_resolution
        {
            return Err(LabError::Conflict(
                "dataset observation is outside its requested market/resolution".into(),
            ));
        }
        if observation.raw_object_ids.is_empty()
            || observation
                .raw_object_ids
                .iter()
                .any(|raw| !raw_ids.contains(raw.as_str()))
        {
            return Err(LabError::DataCorrupt(
                "dataset observation provenance is absent from its raw manifest".into(),
            ));
        }
        let source_count = observation
            .raw_object_ids
            .iter()
            .map(RawObjectId::as_str)
            .collect::<BTreeSet<_>>()
            .len();
        if source_count != observation.raw_object_ids.len() {
            return Err(LabError::Conflict(
                "duplicate raw source on observation".into(),
            ));
        }
        if !ids.insert(observation.id.as_str()) {
            return Err(LabError::Conflict(
                "duplicate observation id in dataset".into(),
            ));
        }
        let key = (
            observation.candle.market.as_str(),
            enum_text(&observation.candle.interval)?,
            timestamp_ms(observation.candle.open_time_utc),
            observation.id.as_str(),
        );
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous > &key)
        {
            return Err(LabError::InvalidConfig(
                "dataset observations must use stable market/interval/open-time order".into(),
            ));
        }
        previous_key = Some(key);
    }
    Ok(())
}

fn validate_quality_sources(
    snapshot: &DatasetSnapshot,
    raw_ids: &BTreeSet<&str>,
) -> Result<(), LabError> {
    for issue in &snapshot.manifest.quality_issues {
        let issue_source_count = issue
            .raw_object_ids
            .iter()
            .map(RawObjectId::as_str)
            .collect::<BTreeSet<_>>()
            .len();
        if issue_source_count != issue.raw_object_ids.len() {
            return Err(LabError::Conflict(
                "duplicate raw source on quality issue".into(),
            ));
        }
        if issue
            .raw_object_ids
            .iter()
            .any(|raw| !raw_ids.contains(raw.as_str()))
        {
            return Err(LabError::DataCorrupt(
                "quality issue references raw evidence outside its dataset".into(),
            ));
        }
    }
    Ok(())
}

fn validate_dataset_identity_conflict(
    connection: &Connection,
    snapshot: &DatasetSnapshot,
    digests: &DatasetDigests,
) -> Result<(), LabError> {
    let existing = connection.query_row(
        "SELECT id,normalized_request_digest,semantic_digest,provenance_digest FROM datasets WHERE request_id=?1",
        [snapshot.manifest.request.request_id.as_str()],
        |row| Ok((row.get::<_, String>(0)?,row.get::<_, String>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?)),
    ).optional().map_err(sql_error)?;
    match existing {
        Some((id, request, semantic, provenance))
            if id == snapshot.manifest.id.as_str()
                && request == digests.normalized_request.as_str()
                && semantic == digests.semantic.as_str()
                && provenance == digests.provenance.as_str() =>
        {
            Ok(())
        }
        Some(_) => Err(LabError::Conflict(
            "request id already names a different normalized dataset".into(),
        )),
        None => Ok(()),
    }
}

pub(super) fn insert_dataset_rows(
    transaction: &Transaction<'_>,
    snapshot: &DatasetSnapshot,
    prepared: &PreparedDatasetPublication,
    derivation: Option<&PendingDerivation>,
) -> Result<(), LabError> {
    let exists = transaction
        .query_row(
            "SELECT 1 FROM datasets WHERE id=?1",
            [snapshot.manifest.id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(sql_error)?;
    if exists.is_some() {
        if let Some(derivation) = derivation {
            validate_existing_derivation(transaction, &snapshot.manifest.id, derivation)?;
        }
        return Ok(());
    }
    for raw in &snapshot.manifest.raw_objects {
        insert_raw(transaction, raw)?;
    }
    for observation in &snapshot.observations {
        insert_observation(transaction, observation)?;
    }
    let manifest_json = serde_json::to_string(&snapshot.manifest).map_err(json_error)?;
    transaction.execute(
        "INSERT INTO datasets(id,request_id,normalized_request_digest,schema_version,status,coverage_start_ms,coverage_end_ms,row_count,normalizer_version,gap_policy,semantic_digest,provenance_digest,origin,manifest_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![snapshot.manifest.id.as_str(), snapshot.manifest.request.request_id.as_str(), prepared.digests.normalized_request.as_str(), snapshot.manifest.schema_version, enum_text(&snapshot.manifest.status)?, timestamp_ms(snapshot.manifest.coverage.start()), timestamp_ms(snapshot.manifest.coverage.end()), u64_to_i64(snapshot.manifest.row_count)?, snapshot.manifest.normalizer_version, snapshot.manifest.gap_policy, prepared.digests.semantic.as_str(), prepared.digests.provenance.as_str(), enum_text(&snapshot.manifest.origin)?, manifest_json],
    ).map_err(sql_error)?;
    insert_dataset_members(transaction, snapshot)?;
    insert_dataset_quality(transaction, snapshot)?;
    if let Some(derivation) = derivation {
        transaction.execute(
            "INSERT INTO dataset_derivations(derived_dataset_id,source_dataset_id,transform_kind,transform_version,target_interval,closure_digest) VALUES (?1,?2,'UTC_RESAMPLE',?3,?4,?5)",
            params![snapshot.manifest.id.as_str(), derivation.source_dataset_id.as_str(), derivation.transform_version, enum_text(&derivation.target_interval)?, derivation.closure_digest.as_str()],
        ).map_err(sql_error)?;
    }
    Ok(())
}

fn insert_dataset_members(
    transaction: &Transaction<'_>,
    snapshot: &DatasetSnapshot,
) -> Result<(), LabError> {
    for (position, observation) in snapshot.observations.iter().enumerate() {
        transaction
            .execute(
                "INSERT INTO dataset_members(dataset_id,observation_id,position) VALUES (?1,?2,?3)",
                params![
                    snapshot.manifest.id.as_str(),
                    observation.id.as_str(),
                    usize_to_i64(position)?
                ],
            )
            .map_err(sql_error)?;
    }
    for (position, raw) in snapshot.manifest.raw_objects.iter().enumerate() {
        transaction.execute("INSERT INTO dataset_raw_objects(dataset_id,raw_object_id,position) VALUES (?1,?2,?3)", params![snapshot.manifest.id.as_str(), raw.id.as_str(), usize_to_i64(position)?]).map_err(sql_error)?;
    }
    for observation in &snapshot.observations {
        for (position, raw) in observation.raw_object_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO dataset_observation_raw_objects(dataset_id,observation_id,raw_object_id,position) VALUES (?1,?2,?3,?4)",
                params![snapshot.manifest.id.as_str(), observation.id.as_str(), raw.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
        for (position, constituent) in observation.constituent_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO dataset_observation_constituents(dataset_id,observation_id,constituent_id,position) VALUES (?1,?2,?3,?4)",
                params![snapshot.manifest.id.as_str(), observation.id.as_str(), constituent.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
    }
    Ok(())
}

fn insert_dataset_quality(
    transaction: &Transaction<'_>,
    snapshot: &DatasetSnapshot,
) -> Result<(), LabError> {
    for (index, issue) in snapshot.manifest.quality_issues.iter().enumerate() {
        transaction.execute(
            "INSERT INTO quality_issues(dataset_id,issue_index,kind,severity,market,start_ms,end_ms,issue_count,detail,issue_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![snapshot.manifest.id.as_str(), usize_to_i64(index)?, enum_text(&issue.kind)?, enum_text(&issue.severity)?, issue.market.code(), timestamp_ms(issue.start), timestamp_ms(issue.end), u64_to_i64(issue.count)?, issue.detail, serde_json::to_string(issue).map_err(json_error)?],
        ).map_err(sql_error)?;
        for (position, raw) in issue.raw_object_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO quality_issue_raw_objects(dataset_id,issue_index,raw_object_id,position) VALUES (?1,?2,?3,?4)",
                params![snapshot.manifest.id.as_str(), usize_to_i64(index)?, raw.as_str(), usize_to_i64(position)?],
            ).map_err(sql_error)?;
        }
    }
    Ok(())
}

fn insert_raw(transaction: &Transaction<'_>, object: &RawObjectRef) -> Result<(), LabError> {
    let json = serde_json::to_string(object).map_err(json_error)?;
    let existing = transaction
        .query_row(
            "SELECT object_json FROM raw_objects WHERE id=?1",
            [object.id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(existing) = existing {
        return if existing == json {
            Ok(())
        } else {
            Err(LabError::Conflict(
                "raw object id already has different metadata".into(),
            ))
        };
    }
    transaction.execute(
        "INSERT INTO raw_objects(id,relative_path,source_url,fetched_at_ms,persisted_at_ms,http_status,remaining_req,raw_sha256,compressed_sha256,raw_bytes,compressed_bytes,origin,object_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![object.id.as_str(), object.relative_path, object.source_url, timestamp_ms(object.fetched_at), timestamp_ms(object.persisted_at), object.http_status, object.remaining_req, object.raw_sha256.as_str(), object.compressed_sha256.as_str(), u64_to_i64(object.raw_bytes)?, u64_to_i64(object.compressed_bytes)?, enum_text(&object.origin)?, json],
    ).map_err(sql_error)?;
    Ok(())
}

fn link_collection_raw(
    transaction: &Transaction<'_>,
    request_id: &RequestId,
    object: &RawObjectRef,
) -> Result<(), LabError> {
    let existing = transaction
        .query_row(
            "SELECT 1 FROM collection_raw_objects WHERE request_id=?1 AND raw_object_id=?2",
            params![request_id.as_str(), object.id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(sql_error)?;
    if existing.is_some() {
        return Ok(());
    }
    let used = query_collection_raw_bytes(transaction, request_id)?;
    if used.saturating_add(object.raw_bytes) > MAX_COLLECTION_BYTES {
        return Err(LabError::ResourceLimit(format!(
            "collection raw bytes exceed {MAX_COLLECTION_BYTES}"
        )));
    }
    let next: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(linked_index),-1)+1 FROM collection_raw_objects WHERE request_id=?1",
            [request_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    transaction
        .execute(
            "INSERT INTO collection_raw_objects(request_id,raw_object_id,linked_index) VALUES (?1,?2,?3)",
            params![request_id.as_str(), object.id.as_str(), next],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn query_collection_raw_bytes(
    connection: &rusqlite::Connection,
    request_id: &RequestId,
) -> Result<u64, LabError> {
    let used: i64 = connection
        .query_row(
            "SELECT COALESCE(SUM(r.raw_bytes),0) FROM collection_raw_objects c JOIN raw_objects r ON r.id=c.raw_object_id WHERE c.request_id=?1",
            [request_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    u64::try_from(used).map_err(|_| LabError::DataCorrupt("negative collection byte usage".into()))
}

fn insert_observation(
    transaction: &Transaction<'_>,
    observation: &CandleObservation,
) -> Result<(), LabError> {
    validate_observation_identity(observation)?;
    let json = serde_json::to_string(observation).map_err(json_error)?;
    let existing = transaction
        .query_row(
            "SELECT content_digest FROM candle_observations WHERE id=?1",
            [observation.id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(existing) = existing {
        if existing != observation.content_digest.as_str() {
            return Err(LabError::Conflict(
                "observation id already has different content".into(),
            ));
        }
        insert_observation_links(transaction, observation)?;
        return Ok(());
    }
    let candle = &observation.candle;
    transaction.execute(
        "INSERT INTO candle_observations(id,market,interval,open_time_ms,close_time_ms,open_decimal,high_decimal,low_decimal,close_decimal,volume_decimal,quote_turnover_decimal,completed,content_digest,observation_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![observation.id.as_str(), candle.market, enum_text(&candle.interval)?, timestamp_ms(candle.open_time_utc), timestamp_ms(candle.close_time_utc), canonical_decimal(candle.open.get()), canonical_decimal(candle.high.get()), canonical_decimal(candle.low.get()), canonical_decimal(candle.close.get()), canonical_decimal(candle.volume.get()), canonical_decimal(candle.quote_turnover.get()), candle.completed, observation.content_digest.as_str(), json],
    ).map_err(sql_error)?;
    insert_observation_links(transaction, observation)
}

fn insert_observation_links(
    transaction: &Transaction<'_>,
    observation: &CandleObservation,
) -> Result<(), LabError> {
    let raw_offset: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM observation_raw_objects WHERE observation_id=?1",
            [observation.id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    for (position, raw) in observation.raw_object_ids.iter().enumerate() {
        transaction.execute(
            "INSERT OR IGNORE INTO observation_raw_objects(observation_id,raw_object_id,position) VALUES (?1,?2,?3)",
            params![observation.id.as_str(), raw.as_str(), raw_offset + usize_to_i64(position)?],
        ).map_err(sql_error)?;
    }
    let constituents: Vec<String> = {
        let mut statement = transaction.prepare(
            "SELECT constituent_id FROM observation_constituents WHERE observation_id=?1 ORDER BY position",
        ).map_err(sql_error)?;
        statement
            .query_map([observation.id.as_str()], |row| row.get(0))
            .map_err(sql_error)?
            .collect::<Result<_, _>>()
            .map_err(sql_error)?
    };
    let requested: Vec<_> = observation
        .constituent_ids
        .iter()
        .map(ToString::to_string)
        .collect();
    if !constituents.is_empty() && constituents != requested {
        return Err(LabError::Conflict(
            "observation identity has different constituents".into(),
        ));
    }
    if constituents.is_empty() {
        for (position, constituent) in observation.constituent_ids.iter().enumerate() {
            transaction.execute("INSERT INTO observation_constituents(observation_id,constituent_id,position) VALUES (?1,?2,?3)", params![observation.id.as_str(), constituent.as_str(), usize_to_i64(position)?]).map_err(sql_error)?;
        }
    }
    Ok(())
}

fn query_backup_raw(connection: &Connection) -> Result<Vec<BackupRaw>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT id,relative_path,raw_sha256,compressed_sha256 FROM raw_objects ORDER BY id",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok(BackupRaw {
                id: row.get(0)?,
                relative_path: row.get(1)?,
                raw_sha256: row.get(2)?,
                compressed_sha256: row.get(3)?,
            })
        })
        .map_err(sql_error)?;
    rows.map(|row| row.map_err(sql_error)).collect()
}

fn query_backup_datasets(connection: &Connection) -> Result<Vec<BackupDataset>, LabError> {
    let mut statement = connection
        .prepare("SELECT id,semantic_digest,provenance_digest FROM datasets ORDER BY id")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok(BackupDataset {
                id: row.get(0)?,
                semantic_digest: row.get(1)?,
                provenance_digest: row.get(2)?,
            })
        })
        .map_err(sql_error)?;
    rows.map(|row| row.map_err(sql_error)).collect()
}

fn query_backup_artifacts(connection: &Connection) -> Result<Vec<BackupArtifact>, LabError> {
    let mut statement = connection
        .prepare("SELECT id,relative_path,bytes,sha256 FROM run_artifacts ORDER BY id")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(sql_error)?;
    rows.map(|row| {
        let (id, relative_path, bytes, sha256) = row.map_err(sql_error)?;
        Ok(BackupArtifact {
            id,
            relative_path,
            bytes: u64::try_from(bytes)
                .map_err(|_| LabError::DataCorrupt("negative artifact byte count".into()))?,
            sha256,
        })
    })
    .collect()
}

fn validate_backup_raw(raw: &BackupRaw) -> Result<(), LabError> {
    validate_relative_path(&raw.relative_path)?;
    let id = RawObjectId::new(raw.id.clone())?;
    let raw_hash = ContentHash::try_from(raw.raw_sha256.clone())?;
    let _compressed_hash = ContentHash::try_from(raw.compressed_sha256.clone())?;
    let expected = format!("{RAW_DIR}/{}/{}", &raw_hash.as_str()[..2], id);
    if raw.relative_path != expected {
        return Err(LabError::DataCorrupt(
            "backup raw path does not match its identity".into(),
        ));
    }
    Ok(())
}

fn deterministic_gzip(body: &[u8]) -> Result<Vec<u8>, LabError> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(Vec::new(), Compression::default());
    encoder
        .write_all(body)
        .map_err(io_error("compress raw body"))?;
    encoder.finish().map_err(io_error("finish raw compression"))
}

fn bounded_gunzip(compressed: &[u8], expected: u64) -> Result<Vec<u8>, LabError> {
    if expected > MAX_RAW_OBJECT_BYTES as u64 {
        return Err(LabError::DataCorrupt(
            "raw object declares oversized body".into(),
        ));
    }
    let cursor = std::io::Cursor::new(compressed);
    let mut decoder = flate2::bufread::GzDecoder::new(cursor);
    let capacity = usize::try_from(expected)
        .map_err(|_| LabError::DataCorrupt("raw object size exceeds platform capacity".into()))?;
    let mut body = Vec::with_capacity(capacity);
    {
        decoder
            .by_ref()
            .take(MAX_RAW_OBJECT_BYTES as u64 + 1)
            .read_to_end(&mut body)
            .map_err(io_error("decompress raw body"))?;
    }
    let consumed = decoder.into_inner().position();
    if consumed != compressed.len() as u64 {
        return Err(LabError::DataCorrupt(
            "raw object contains trailing or concatenated gzip data".into(),
        ));
    }
    if body.len() as u64 != expected {
        return Err(LabError::DataCorrupt(
            "raw object uncompressed length mismatch".into(),
        ));
    }
    Ok(body)
}

fn ensure_root(root: &Path) -> Result<(), LabError> {
    if root.as_os_str().is_empty() {
        return Err(LabError::InvalidConfig(
            "data root must not be empty".into(),
        ));
    }
    create_directory(root)?;
    reject_symlink(root)
}

fn validate_restore_root(root: &Path, policy: RestoreRootPolicy) -> Result<(), LabError> {
    if !root.exists() {
        return match policy {
            #[cfg(test)]
            RestoreRootPolicy::StrictEmpty => Ok(()),
            RestoreRootPolicy::HeldOwnerLock => Err(LabError::Conflict(
                "locked restore root does not exist".into(),
            )),
        };
    }
    reject_symlink(root)?;
    let entries = fs::read_dir(root)
        .map_err(io_error("inspect restore root"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error("inspect restore root entry"))?;
    match policy {
        #[cfg(test)]
        RestoreRootPolicy::StrictEmpty if entries.is_empty() => Ok(()),
        #[cfg(test)]
        RestoreRootPolicy::StrictEmpty => Err(LabError::Conflict(
            "restore root must be new or empty".into(),
        )),
        RestoreRootPolicy::HeldOwnerLock if entries.len() == 1 => {
            let entry = &entries[0];
            if entry.file_name() != std::ffi::OsStr::new(OWNER_LOCK_FILE) {
                return Err(LabError::Conflict(
                    "locked restore root contains an unexpected entry".into(),
                ));
            }
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(io_error("inspect restore owner lock"))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(LabError::Conflict(
                    "restore owner lock must be a regular file".into(),
                ));
            }
            Ok(())
        }
        RestoreRootPolicy::HeldOwnerLock => Err(LabError::Conflict(
            "locked restore root must contain only .lab-owner.lock".into(),
        )),
    }
}

fn create_directory(path: &Path) -> Result<(), LabError> {
    fs::create_dir_all(path).map_err(io_error("create directory"))
}

fn reject_symlink(path: &Path) -> Result<(), LabError> {
    let metadata = fs::symlink_metadata(path).map_err(io_error("inspect path"))?;
    if metadata.file_type().is_symlink() {
        Err(LabError::InvalidConfig(format!(
            "symlink path is not allowed: {}",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn reject_symlink_chain(root: &Path, path: &Path) -> Result<(), LabError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| LabError::InvalidConfig("path escapes data root".into()))?;
    reject_symlink(root)?;
    let mut cursor = root.to_path_buf();
    for component in relative.components() {
        cursor.push(component);
        if cursor.exists() {
            reject_symlink(&cursor)?;
        }
    }
    Ok(())
}

fn validate_object_path(object: &RawObjectRef) -> Result<(), LabError> {
    validate_relative_path(&object.relative_path)?;
    let expected = format!(
        "{RAW_DIR}/{}/{}",
        &object.raw_sha256.as_str()[..2],
        object.id
    );
    if object.relative_path != expected {
        return Err(LabError::DataCorrupt(
            "raw object relative path does not match identity".into(),
        ));
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), LabError> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(LabError::InvalidConfig(
            "relative object path contains traversal".into(),
        ));
    }
    Ok(())
}

fn directory_size(root: &Path) -> Result<u64, LabError> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).map_err(io_error("scan data root"))? {
            let entry = entry.map_err(io_error("scan data root entry"))?;
            let metadata = entry
                .metadata()
                .map_err(io_error("inspect data root entry"))?;
            if entry
                .file_type()
                .map_err(io_error("inspect data root type"))?
                .is_symlink()
            {
                return Err(LabError::InvalidConfig(
                    "symlink inside data root is not allowed".into(),
                ));
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

fn file_size(path: &Path) -> Result<u64, LabError> {
    if !path.exists() {
        return Ok(0);
    }
    reject_symlink(path)?;
    let metadata = fs::metadata(path).map_err(io_error("inspect capacity file"))?;
    if metadata.is_file() {
        Ok(metadata.len())
    } else {
        Err(LabError::DataCorrupt(format!(
            "capacity path is not a regular file: {}",
            path.display()
        )))
    }
}

/// Classify SQLite storage pressure from measured byte counts.
///
/// Refusals carry stable sub-reasons (`DB_STORAGE_PRESSURE` /
/// `WAL_STORAGE_PRESSURE`); the soft band is allowed and only reported.
///
/// # Errors
/// Returns [`LabError::StoragePressure`] in the hard and WAL bands and
/// [`LabError::DataCorrupt`] for a zero limit.
pub fn classify_storage_pressure(
    allocated_bytes: u64,
    wal_bytes: u64,
    limit_bytes: u64,
) -> Result<(), LabError> {
    if limit_bytes == 0 {
        return Err(LabError::DataCorrupt(
            "sqlite byte limit must be positive".into(),
        ));
    }
    if wal_bytes > WAL_PRESSURE_BYTES {
        return Err(LabError::StoragePressure(format!(
            "WAL_STORAGE_PRESSURE: wal_bytes={wal_bytes} limit={WAL_PRESSURE_BYTES}              remedy=run storage_maintenance action=checkpoint mode=TRUNCATE"
        )));
    }
    let effective = allocated_bytes.saturating_add(wal_bytes);
    let percent = effective.saturating_mul(100) / limit_bytes;
    if percent >= DB_HARD_PRESSURE_PERCENT {
        return Err(LabError::StoragePressure(format!(
            "DB_STORAGE_PRESSURE: effective_bytes={effective} (main={allocated_bytes} wal={wal_bytes})              limit={limit_bytes} utilization={percent}%              remedy=storage_maintenance action=compact or explicit hard-delete of finished resources"
        )));
    }
    if percent >= DB_SOFT_PRESSURE_PERCENT {
        tracing::warn!(
            event = "db_storage_pressure_soft",
            effective_bytes = effective,
            limit_bytes = limit_bytes,
            utilization_percent = percent,
            remedy = "plan storage_maintenance action=compact"
        );
    }
    Ok(())
}

impl Store {
    /// Refuse mutating submissions under hard SQLite storage pressure.
    ///
    /// Cheap by design: two PRAGMAs and one WAL file stat — never a
    /// directory scan. Soft pressure passes and is only logged.
    /// # Errors
    /// Returns [`LabError::StoragePressure`] with a stable sub-reason.
    pub fn enforce_storage_pressure(&self) -> Result<(), LabError> {
        let page_size: i64 = self
            .connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(sql_error)?;
        let page_count: i64 = self
            .connection
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .map_err(sql_error)?;
        let allocated = u64::try_from(page_count.saturating_mul(page_size)).unwrap_or(u64::MAX);
        let wal_path = self.raw.root.join(format!("{DATABASE_FILE}-wal"));
        let wal_bytes = if wal_path
            .try_exists()
            .map_err(io_error("inspect SQLite WAL"))?
        {
            file_size(&wal_path)?
        } else {
            0
        };
        classify_storage_pressure(allocated, wal_bytes, MAX_DATABASE_BYTES)
    }
}

fn configure_database_capacity(connection: &Connection) -> Result<(), LabError> {
    connection
        .pragma_update(None, "page_size", DATABASE_PAGE_BYTES)
        .map_err(sql_error)?;
    let page_size: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(sql_error)?;
    if page_size != DATABASE_PAGE_BYTES {
        return Err(LabError::DataCorrupt(format!(
            "sqlite page_size must be {DATABASE_PAGE_BYTES}, found {page_size}"
        )));
    }
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(sql_error)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(LabError::Internal(format!(
            "sqlite refused WAL mode: {journal_mode}"
        )));
    }
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(sql_error)?;
    connection
        .pragma_update(None, "wal_autocheckpoint", WAL_AUTOCHECKPOINT_PAGES)
        .map_err(sql_error)?;
    connection
        .pragma_update(None, "journal_size_limit", WAL_JOURNAL_LIMIT_BYTES)
        .map_err(sql_error)?;
    let page_count: i64 = connection
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .map_err(sql_error)?;
    let database_max_pages =
        i64::try_from(MAX_DATABASE_BYTES / u64::try_from(DATABASE_PAGE_BYTES).unwrap_or(1))
            .map_err(|_| LabError::Internal("database page budget overflow".into()))?;
    if page_count > database_max_pages {
        return Err(LabError::ResourceLimit(format!(
            "sqlite database already exceeds {MAX_DATABASE_BYTES} byte cap"
        )));
    }
    let max_page_count: i64 = connection
        .query_row(
            &format!("PRAGMA max_page_count={database_max_pages}"),
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if max_page_count != database_max_pages {
        return Err(LabError::Internal(format!(
            "sqlite max_page_count mismatch: {max_page_count}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn read_directories(path: &Path) -> Result<Vec<PathBuf>, LabError> {
    let mut result = Vec::new();
    for entry in fs::read_dir(path).map_err(io_error("scan raw directories"))? {
        let entry = entry.map_err(io_error("scan raw directory entry"))?;
        let kind = entry
            .file_type()
            .map_err(io_error("inspect raw directory entry"))?;
        if kind.is_symlink() {
            return Err(LabError::DataCorrupt("symlink inside raw store".into()));
        }
        if kind.is_dir() {
            result.push(entry.path());
        }
    }
    Ok(result)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), LabError> {
    reject_symlink(source)?;
    fs::create_dir_all(destination).map_err(io_error("create copied directory"))?;
    for entry in fs::read_dir(source).map_err(io_error("read copied directory"))? {
        let entry = entry.map_err(io_error("read copied entry"))?;
        let kind = entry
            .file_type()
            .map_err(io_error("inspect copied entry"))?;
        if kind.is_symlink() {
            return Err(LabError::DataCorrupt("symlink in copied tree".into()));
        }
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target).map_err(io_error("copy file"))?;
            File::open(&target)
                .and_then(|file| file.sync_all())
                .map_err(io_error("sync copied file"))?;
        }
    }
    sync_directory(destination)
}

fn copy_file_synced(source: &Path, destination: &Path) -> Result<(), LabError> {
    if let Some(parent) = destination.parent() {
        create_directory(parent)?;
    }
    fs::copy(source, destination).map_err(io_error("copy file"))?;
    File::open(destination)
        .and_then(|file| file.sync_all())
        .map_err(io_error("sync copied file"))?;
    if let Some(parent) = destination.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), LabError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error("create file"))?;
    file.write_all(bytes).map_err(io_error("write file"))?;
    file.sync_all().map_err(io_error("sync file"))
}

fn sync_directory(path: &Path) -> Result<(), LabError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error("sync directory"))
}

fn timestamp_ms(value: UtcTimestamp) -> i64 {
    value.0.timestamp_millis()
}

fn timestamp_from_ms(value: i64) -> Result<UtcTimestamp, LabError> {
    chrono::DateTime::from_timestamp_millis(value)
        .map(UtcTimestamp)
        .ok_or_else(|| {
            LabError::DataCorrupt(format!("invalid stored UTC millisecond timestamp: {value}"))
        })
}

fn canonical_decimal(value: rust_decimal::Decimal) -> String {
    value.normalize().to_string()
}

fn enum_text(value: &impl Serialize) -> Result<String, LabError> {
    serde_json::to_value(value)
        .map_err(json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| LabError::Internal("enum did not serialize as string".into()))
}

fn usize_to_i64(value: usize) -> Result<i64, LabError> {
    i64::try_from(value).map_err(|_| LabError::ResourceLimit("index exceeds sqlite integer".into()))
}

fn u64_to_i64(value: u64) -> Result<i64, LabError> {
    i64::try_from(value).map_err(|_| LabError::ResourceLimit("value exceeds sqlite integer".into()))
}
#[allow(clippy::needless_pass_by_value)]
fn sql_error(error: rusqlite::Error) -> LabError {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        LabError::StoragePressure(format!(
            "sqlite_max_database_bytes reached at stage=write: allowed={MAX_DATABASE_BYTES} \
             unit=bytes remedy=hard-delete finished runs, datasets, jobs or exports; freed pages \
             restore write capacity immediately and the post-delete VACUUM shrinks the file"
        ))
    } else {
        LabError::Internal(format!("sqlite storage: {error}"))
    }
}
#[allow(clippy::needless_pass_by_value)]
fn json_error(error: serde_json::Error) -> LabError {
    LabError::Internal(format!("storage json: {error}"))
}
fn io_error(context: &'static str) -> impl FnOnce(std::io::Error) -> LabError {
    move |error| LabError::Internal(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests;
