//! Run ledger sealing: verified run ledgers move from SQLite rows into
//! content-addressed gzip NDJSON chunk files, and the detail rows compact.
//!
//! Lifecycle per run: `DETAIL` (SQLite rows are the only copy) -> `SEALED`
//! (chunk files published and catalogued, detail rows still present) ->
//! `COMPACTED` (detail rows removed; chunk files are the only copy). Readers
//! fall back to chunk files whenever detail rows are absent, so queries,
//! export, replay, accounting verification and hard delete keep working.
//!
//! Chunk files are immutable and content-addressed by the SHA-256 of their
//! compressed bytes, mirroring the raw-object store layout.

use super::{
    Store, io_error, json_error, reject_symlink_chain, sql_error, sync_directory, u64_to_i64,
    validate_relative_path,
};
use crate::contracts::{
    AccountMark, ContentHash, EpisodeRecord, FillRecord, LabError, LedgerSection, ModelId,
    OrderRecord, RunId, SignalRecord,
};
use flate2::{Compression, GzBuilder, read::GzDecoder};
use rusqlite::{OptionalExtension, params};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;

/// Storage format identifier recorded per chunk row.
pub(crate) const LEDGER_CHUNK_FORMAT: &str = "ndjson-gzip-v1";
/// Maximum detailed ledger events per sealed chunk file.
pub(crate) const LEDGER_CHUNK_EVENTS: usize = 10_000;
const LEDGERS_DIR: &str = "ledgers";
const LEDGER_FILE_SUFFIX: &str = ".json.gz";
/// Hard upper bound per decompressed chunk (10k events, generous record size).
const MAX_CHUNK_DECOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
/// Hard upper bound per compressed chunk file.
const MAX_CHUNK_COMPRESSED_BYTES: u64 = 128 * 1024 * 1024;

/// Lifecycle of a run's detailed ledger payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum RunLedgerState {
    Detail,
    Sealed,
    Compacted,
}

impl RunLedgerState {
    fn from_text(value: &str) -> Result<Self, LabError> {
        serde_json::from_value(serde_json::Value::String(value.to_owned()))
            .map_err(|error| LabError::DataCorrupt(format!("run ledger state: {error}")))
    }
}

/// One detailed ledger record line inside a sealed chunk.
#[derive(Debug)]
pub(crate) struct LedgerLine {
    pub section: LedgerSection,
    pub model_id: ModelId,
    pub event_seq: u64,
    pub accounting_event_time_ms: i64,
    /// Denormalized fills columns cross-checked on read.
    pub liquidity_source_bar_id: Option<String>,
    pub liquidity_source_close_time_ms: Option<i64>,
    pub record: serde_json::Value,
}

/// One compressed chunk staged for publication and its catalogue row.
#[derive(Debug)]
struct PreparedChunk {
    chunk_index: u64,
    relative_path: String,
    sha256: String,
    event_seq_start: u64,
    event_seq_end: u64,
    event_count: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

impl LedgerLine {
    pub(crate) fn kind(&self) -> &'static str {
        match self.section {
            LedgerSection::Signals => "SIGNAL",
            LedgerSection::Orders => "ORDER",
            LedgerSection::OrderEvents => "ORDER_EVENT",
            LedgerSection::Fills => "FILL",
            LedgerSection::Equity => "ACCOUNT_MARK",
            LedgerSection::Episodes => "EPISODE",
        }
    }

    fn to_line(&self) -> Result<String, LabError> {
        let line = serde_json::json!({
            "kind": self.kind(),
            "model": self.model_id.as_str(),
            "seq": self.event_seq,
            "t": self.accounting_event_time_ms,
            "lsb": self.liquidity_source_bar_id,
            "lst": self.liquidity_source_close_time_ms,
            "r": self.record,
        });
        serde_json::to_string(&line).map_err(json_error)
    }

    fn from_line(line: &str) -> Result<Self, LabError> {
        let value: serde_json::Value = serde_json::from_str(line).map_err(json_error)?;
        let kind = value
            .get("kind")
            .and_then(|kind| kind.as_str())
            .ok_or_else(|| LabError::DataCorrupt("sealed chunk line lacks kind".into()))?;
        let section = match kind {
            "SIGNAL" => LedgerSection::Signals,
            "ORDER" => LedgerSection::Orders,
            "ORDER_EVENT" => LedgerSection::OrderEvents,
            "FILL" => LedgerSection::Fills,
            "ACCOUNT_MARK" => LedgerSection::Equity,
            other => {
                return Err(LabError::DataCorrupt(format!(
                    "sealed chunk line has unknown kind {other}"
                )));
            }
        };
        let model = value
            .get("model")
            .and_then(|model| model.as_str())
            .ok_or_else(|| LabError::DataCorrupt("sealed chunk line lacks model".into()))?;
        Ok(Self {
            section,
            model_id: ModelId::new(model)?,
            event_seq: parse_u64(value.get("seq"), "seq")?,
            accounting_event_time_ms: value
                .get("t")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| LabError::DataCorrupt("sealed chunk line lacks time".into()))?,
            liquidity_source_bar_id: value.get("lsb").and_then(|v| v.as_str()).map(str::to_owned),
            liquidity_source_close_time_ms: value.get("lst").and_then(serde_json::Value::as_i64),
            record: value
                .get("r")
                .cloned()
                .ok_or_else(|| LabError::DataCorrupt("sealed chunk line lacks record".into()))?,
        })
    }
}

fn parse_u64(value: Option<&serde_json::Value>, label: &str) -> Result<u64, LabError> {
    value
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| LabError::DataCorrupt(format!("sealed chunk line lacks {label}")))
}

/// Counts reported by a successful sealing pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLedgerSealStats {
    pub chunks: u64,
    pub events: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub already_sealed: bool,
}

fn chunk_line(
    section: LedgerSection,
    model_id: &str,
    event_seq: i64,
    time_ms: i64,
    record_json: &str,
) -> Result<LedgerLine, LabError> {
    Ok(LedgerLine {
        section,
        model_id: ModelId::new(model_id)?,
        event_seq: u64::try_from(event_seq)
            .map_err(|_| LabError::DataCorrupt("negative ledger event_seq".into()))?,
        accounting_event_time_ms: time_ms,
        liquidity_source_bar_id: None,
        liquidity_source_close_time_ms: None,
        record: serde_json::from_str(record_json).map_err(json_error)?,
    })
}

fn section_entries(
    store: &Store,
    run_id: &RunId,
    section: LedgerSection,
) -> Result<Vec<LedgerLine>, LabError> {
    // The join mirrors QueryTable::source(): event-keyed sections take their
    // accounting time from run_events; Orders carries it denormalized.
    let (source, time_column, extra_columns, has_join) = match section {
        LedgerSection::Signals => ("signals s", "e.accounting_event_time_ms", "", true),
        LedgerSection::OrderEvents => ("order_events s", "e.accounting_event_time_ms", "", true),
        LedgerSection::Fills => (
            "fills s",
            "e.accounting_event_time_ms",
            ",s.liquidity_source_bar_id,s.liquidity_source_close_time_ms",
            true,
        ),
        LedgerSection::Orders => ("orders s", "s.accounting_event_time_ms", "", false),
        LedgerSection::Equity => ("account_marks s", "e.accounting_event_time_ms", "", true),
        LedgerSection::Episodes => unreachable!("episodes stay in the catalog"),
    };
    let join = if has_join {
        " JOIN run_events e ON e.run_id=s.run_id AND e.event_seq=s.event_seq"
    } else {
        ""
    };
    let sql = format!(
        "SELECT s.model_id,s.event_seq,{time_column},s.record_json{extra_columns} \
         FROM {source}{join} WHERE s.run_id=?1 \
         ORDER BY s.event_seq,s.model_id"
    );
    let mut statement = store.connection.prepare(&sql).map_err(sql_error)?;
    let read_fill_extras = section == LedgerSection::Fills;
    let rows = statement
        .query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                if read_fill_extras {
                    row.get::<_, Option<String>>(4)?
                } else {
                    None
                },
                if read_fill_extras {
                    row.get::<_, Option<i64>>(5)?
                } else {
                    None
                },
            ))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let mut lines = Vec::with_capacity(rows.len());
    for (model_id, event_seq, time_ms, record_json, lsb, lst) in rows {
        let mut line = chunk_line(section, &model_id, event_seq, time_ms, &record_json)?;
        line.liquidity_source_bar_id = lsb;
        line.liquidity_source_close_time_ms = lst;
        lines.push(line);
    }
    Ok(lines)
}

fn gzip_bytes(payload: &[u8]) -> Result<Vec<u8>, LabError> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(Vec::new(), Compression::default());
    encoder
        .write_all(payload)
        .map_err(|error| LabError::Internal(format!("chunk gzip write: {error}")))?;
    encoder
        .finish()
        .map_err(|error| LabError::Internal(format!("chunk gzip finish: {error}")))
}

fn gunzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, LabError> {
    let decoder = GzDecoder::new(bytes);
    let mut payload = Vec::new();
    decoder
        .take(MAX_CHUNK_DECOMPRESSED_BYTES)
        .read_to_end(&mut payload)
        .map_err(|error| LabError::DataCorrupt(format!("sealed chunk decode: {error}")))?;
    Ok(payload)
}

fn publish_chunk_file(
    store: &Store,
    compressed: &[u8],
    sha256: &ContentHash,
) -> Result<PathBuf, LabError> {
    let relative_path = format!(
        "{LEDGERS_DIR}/{}/{}{LEDGER_FILE_SUFFIX}",
        &sha256.as_str()[..2],
        sha256.as_str()
    );
    validate_relative_path(&relative_path)?;
    let destination = store.raw_objects().root.join(&relative_path);
    if destination.exists() {
        // Content-addressed: identical bytes already published are a no-op.
        return Ok(PathBuf::from(relative_path));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| LabError::Internal("chunk path has no parent".into()))?
        .to_path_buf();
    fs::create_dir_all(&parent).map_err(io_error("create ledger chunk directory"))?;
    reject_symlink_chain(&store.raw_objects().root, &destination)?;
    let temporary = parent.join(format!(
        ".tmp-chunk-{}-{}",
        std::process::id(),
        super::TEMP_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let write = (|| {
        let mut file = fs::File::create(&temporary).map_err(io_error("create chunk temp"))?;
        file.write_all(compressed)
            .map_err(io_error("write chunk bytes"))?;
        file.sync_all().map_err(io_error("sync chunk bytes"))
    })();
    if let Err(error) = write {
        let _ignored = fs::remove_file(&temporary);
        return Err(error);
    }
    fs::rename(&temporary, &destination).map_err(io_error("publish ledger chunk"))?;
    sync_directory(&parent)?;
    Ok(PathBuf::from(relative_path))
}

/// Read and verify one published chunk file, returning its decoded payload.
fn read_verified_chunk(
    store: &Store,
    relative_path: &str,
    sha256: &str,
    event_count: u64,
    uncompressed_bytes: u64,
) -> Result<Vec<u8>, LabError> {
    validate_relative_path(relative_path)?;
    let path = store.raw_objects().root.join(relative_path);
    reject_symlink_chain(&store.raw_objects().root, &path)?;
    let compressed = fs::read(&path).map_err(io_error("read sealed chunk"))?;
    let compressed_bytes = u64::try_from(compressed.len())
        .map_err(|_| LabError::ResourceLimit("sealed chunk exceeds u64".into()))?;
    if compressed_bytes > MAX_CHUNK_COMPRESSED_BYTES {
        return Err(LabError::DataCorrupt(
            "sealed chunk exceeds size bound".into(),
        ));
    }
    let actual_sha = ContentHash::of_bytes(&compressed);
    if actual_sha.as_str() != sha256 {
        return Err(LabError::InputHashMismatch(
            "sealed chunk compressed hash mismatch".into(),
        ));
    }
    let payload = gunzip_bytes(&compressed)?;
    let payload_bytes = u64::try_from(payload.len())
        .map_err(|_| LabError::ResourceLimit("sealed payload exceeds u64".into()))?;
    if payload_bytes != uncompressed_bytes {
        return Err(LabError::InputHashMismatch(
            "sealed chunk decompressed length mismatch".into(),
        ));
    }
    let lines = payload
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.strip_suffix(b"\r").unwrap_or(line).is_empty())
        .count();
    let lines = u64::try_from(lines)
        .map_err(|_| LabError::ResourceLimit("sealed line count exceeds u64".into()))?;
    if lines != event_count {
        return Err(LabError::InputHashMismatch(
            "sealed chunk event count mismatch".into(),
        ));
    }
    Ok(payload)
}

fn stored_u64(value: i64, label: &str) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| LabError::DataCorrupt(format!("negative stored {label}")))
}

fn prepare_run_chunks(
    store: &Store,
    run_id: &RunId,
    last_seq: i64,
) -> Result<(Vec<PreparedChunk>, u64), LabError> {
    let mut lines = Vec::new();
    for section in [
        LedgerSection::Signals,
        LedgerSection::OrderEvents,
        LedgerSection::Fills,
        LedgerSection::Orders,
        LedgerSection::Equity,
    ] {
        lines.extend(section_entries(store, run_id, section)?);
    }
    lines.sort_by(|left, right| {
        (left.event_seq, left.model_id.as_str(), left.kind()).cmp(&(
            right.event_seq,
            right.model_id.as_str(),
            right.kind(),
        ))
    });
    let total_events = u64::try_from(lines.len())
        .map_err(|_| LabError::ResourceLimit("sealed event count exceeds u64".into()))?;
    let expected_last = stored_u64(last_seq, "run last sequence")?;
    if let Some(last_line) = lines.last()
        && last_line.event_seq > expected_last
    {
        return Err(LabError::DataCorrupt(
            "run ledger extends past its committed sequence".into(),
        ));
    }

    let mut chunks = Vec::new();
    for (chunk_index, group) in lines.chunks(LEDGER_CHUNK_EVENTS).enumerate() {
        let mut payload = Vec::new();
        let mut seq_start = u64::MAX;
        let mut seq_end = 0_u64;
        for line in group {
            payload.extend_from_slice(line.to_line()?.as_bytes());
            payload.push(b'\n');
            seq_start = seq_start.min(line.event_seq);
            seq_end = seq_end.max(line.event_seq);
        }
        let compressed = gzip_bytes(&payload)?;
        let sha = ContentHash::of_bytes(&compressed);
        let relative_path = publish_chunk_file(store, &compressed, &sha)?;
        let event_count = u64::try_from(group.len())
            .map_err(|_| LabError::ResourceLimit("chunk event count exceeds u64".into()))?;
        let uncompressed_bytes = u64::try_from(payload.len())
            .map_err(|_| LabError::ResourceLimit("chunk payload exceeds u64".into()))?;
        read_verified_chunk(
            store,
            relative_path.to_string_lossy().as_ref(),
            sha.as_str(),
            event_count,
            uncompressed_bytes,
        )?;
        chunks.push(PreparedChunk {
            chunk_index: u64::try_from(chunk_index)
                .map_err(|_| LabError::Internal("chunk index overflow".into()))?,
            relative_path: relative_path.to_string_lossy().into_owned(),
            sha256: sha.as_str().to_owned(),
            event_seq_start: seq_start,
            event_seq_end: seq_end,
            event_count,
            compressed_bytes: u64::try_from(compressed.len())
                .map_err(|_| LabError::ResourceLimit("compressed chunk exceeds u64".into()))?,
            uncompressed_bytes,
        });
    }
    Ok((chunks, total_events))
}

impl Store {
    /// Seal a finalized run's detailed ledger into content-addressed chunks.
    ///
    /// Idempotent: a run already SEALED or COMPACTED reports its stats without
    /// rewriting. Any failure before the catalog commit leaves the run DETAIL
    /// with its SQLite ledger untouched.
    ///
    /// # Errors
    /// Reports unknown runs, unverified chunk files, catalog conflicts, or SQLite failure.
    pub fn seal_run_ledger(&mut self, run_id: &RunId) -> Result<RunLedgerSealStats, LabError> {
        let (state, last_seq): (String, i64) = self
            .connection
            .query_row(
                "SELECT ledger_state,last_committed_event_seq FROM runs WHERE id=?1",
                [run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run for sealing".into()))?;
        if RunLedgerState::from_text(&state)? != RunLedgerState::Detail {
            return self
                .sealed_run_stats(run_id)
                .map(|stats| RunLedgerSealStats {
                    already_sealed: true,
                    ..stats
                });
        }
        let (chunks, total_events) = prepare_run_chunks(self, run_id, last_seq)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for chunk in &chunks {
            transaction
                .execute(
                    "INSERT INTO run_ledger_chunks(run_id,chunk_index,event_seq_start,event_seq_end,event_count,sha256,uncompressed_bytes,compressed_bytes,format_version,relative_path) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        run_id.as_str(),
                        u64_to_i64(chunk.chunk_index)?,
                        u64_to_i64(chunk.event_seq_start)?,
                        u64_to_i64(chunk.event_seq_end)?,
                        u64_to_i64(chunk.event_count)?,
                        chunk.sha256,
                        u64_to_i64(chunk.uncompressed_bytes)?,
                        u64_to_i64(chunk.compressed_bytes)?,
                        LEDGER_CHUNK_FORMAT,
                        &chunk.relative_path
                    ],
                )
                .map_err(sql_error)?;
        }
        let changed = transaction
            .execute(
                "UPDATE runs SET ledger_state='SEALED' WHERE id=?1 AND ledger_state='DETAIL'",
                params![run_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "run ledger state changed during sealing".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(RunLedgerSealStats {
            chunks: u64::try_from(chunks.len())
                .map_err(|_| LabError::ResourceLimit("sealed chunk count exceeds u64".into()))?,
            events: total_events,
            compressed_bytes: chunks.iter().map(|c| c.compressed_bytes).sum(),
            uncompressed_bytes: chunks.iter().map(|c| c.uncompressed_bytes).sum(),
            already_sealed: false,
        })
    }

    fn sealed_run_stats(&self, run_id: &RunId) -> Result<RunLedgerSealStats, LabError> {
        let row = self.connection.query_row(
            "SELECT COUNT(*),COALESCE(SUM(event_count),0),COALESCE(SUM(compressed_bytes),0),COALESCE(SUM(uncompressed_bytes),0) FROM run_ledger_chunks WHERE run_id=?1",
            [run_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        ).map_err(sql_error)?;
        Ok(RunLedgerSealStats {
            chunks: stored_u64(row.0, "sealed chunk count")?,
            events: stored_u64(row.1, "sealed event count")?,
            compressed_bytes: stored_u64(row.2, "sealed compressed byte count")?,
            uncompressed_bytes: stored_u64(row.3, "sealed uncompressed byte count")?,
            already_sealed: true,
        })
    }

    /// Seal a finalized run, then compact its SQLite detail rows.
    ///
    /// Single entry point for the job runner: sealing is idempotent and
    /// compaction only proceeds on a SEALED run, so a failure anywhere leaves
    /// the run in DETAIL (unsealed) or SEALED (rows intact) — never a state
    /// where the SQLite ledger is gone without a verified chunk file.
    ///
    /// # Errors
    /// Reports unknown runs, chunk verification failures, or SQLite failure.
    pub fn seal_and_compact_run_ledger(
        &mut self,
        run_id: &RunId,
    ) -> Result<RunLedgerSealStats, LabError> {
        let stats = self.seal_run_ledger(run_id)?;
        self.compact_run_ledger(run_id)?;
        Ok(stats)
    }

    /// Remove a sealed run's detail rows after its chunks are catalogued.
    ///
    /// Episode exit details are materialized inside the same transaction while
    /// the detail rows still exist, so episode pages stay byte-identical after
    /// compaction. The whole compaction is one transaction: an interrupted
    /// attempt leaves the run SEALED with detail rows intact.
    ///
    /// # Errors
    /// Rejects unsealed runs, sealed chunk verification failures, or SQLite failure.
    pub fn compact_run_ledger(&mut self, run_id: &RunId) -> Result<u64, LabError> {
        let state: String = self
            .connection
            .query_row(
                "SELECT ledger_state FROM runs WHERE id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run for compaction".into()))?;
        match RunLedgerState::from_text(&state)? {
            RunLedgerState::Compacted => return Ok(0),
            RunLedgerState::Sealed => {}
            RunLedgerState::Detail => {
                return Err(LabError::Conflict(
                    "run ledger must be sealed before compaction".into(),
                ));
            }
        }
        // Every referenced chunk file must verify before any row is removed.
        let files: Vec<(String, String, i64, i64)> = self.connection.prepare(
            "SELECT relative_path,sha256,event_count,uncompressed_bytes FROM run_ledger_chunks WHERE run_id=?1 ORDER BY chunk_index",
        ).map_err(sql_error)?
        .query_map([run_id.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
        if files.is_empty() {
            return Err(LabError::DataCorrupt(
                "sealed run has no ledger chunks".into(),
            ));
        }
        for (relative_path, sha256, event_count, uncompressed_bytes) in &files {
            read_verified_chunk(
                self,
                relative_path,
                sha256,
                stored_u64(*event_count, "sealed event count")?,
                stored_u64(*uncompressed_bytes, "sealed uncompressed byte count")?,
            )?;
        }
        // Materialize episode exit details before opening the transaction:
        // they are deterministic functions of the immutable detail rows, and
        // computing first keeps the single-owner borrow rules simple.
        let mut materialized: Vec<(String, String, String)> = Vec::new();
        {
            let closed: Vec<(String, String, String)> = self
                .connection
                .prepare(
                    "SELECT model_id,episode_id,record_json FROM episodes WHERE run_id=?1 AND status='CLOSED' AND exit_details_json IS NULL ORDER BY model_id,episode_id",
                )
                .map_err(sql_error)?
                .query_map([run_id.as_str()], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(sql_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_error)?;
            for (model_id, episode_id, episode_record) in closed {
                let episode: EpisodeRecord =
                    serde_json::from_str(&episode_record).map_err(json_error)?;
                let details = self.episode_exit_details(&episode)?;
                materialized.push((
                    model_id,
                    episode_id,
                    serde_json::to_string(&details).map_err(json_error)?,
                ));
            }
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        for (model_id, episode_id, details_json) in &materialized {
            transaction
                .execute(
                    "UPDATE episodes SET exit_details_json=?1 WHERE run_id=?2 AND model_id=?3 AND episode_id=?4",
                    params![details_json, run_id.as_str(), model_id, episode_id],
                )
                .map_err(sql_error)?;
        }
        // Child links and identity tables first, then the event-keyed details.
        for sql in COMPACT_DELETES {
            transaction
                .execute(sql, params![run_id.as_str()])
                .map_err(sql_error)?;
        }
        let changed = transaction
            .execute(
                "UPDATE runs SET ledger_state='COMPACTED' WHERE id=?1 AND ledger_state='SEALED'",
                params![run_id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "run ledger state changed during compaction".into(),
            ));
        }
        let removed = transaction.changes();
        transaction.commit().map_err(sql_error)?;
        Ok(removed)
    }

    /// Detailed-ledger state of one run. Test-visible accessor; production
    /// readers branch on `run_ledger_source` instead.
    #[cfg(test)]
    pub(crate) fn run_ledger_state(&self, run_id: &RunId) -> Result<RunLedgerState, LabError> {
        let state: String = self
            .connection
            .query_row(
                "SELECT ledger_state FROM runs WHERE id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
        RunLedgerState::from_text(&state)
    }

    /// Decompressed detailed ledger lines for a sealed run, in commit order.
    ///
    /// Returns `None` when the run still keeps its detail rows in SQLite.
    ///
    /// # Errors
    /// Reports corrupt or tampered chunk files.
    pub(crate) fn load_run_ledger_lines(
        &self,
        run_id: &RunId,
    ) -> Result<Option<Vec<LedgerLine>>, LabError> {
        let rows: Vec<(String, String, i64, i64)> = self.connection.prepare(
            "SELECT relative_path,sha256,event_count,uncompressed_bytes FROM run_ledger_chunks WHERE run_id=?1 ORDER BY chunk_index",
        ).map_err(sql_error)?
        .query_map([run_id.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut lines = Vec::new();
        for (relative_path, sha256, event_count, uncompressed_bytes) in rows {
            let payload = read_verified_chunk(
                self,
                &relative_path,
                &sha256,
                stored_u64(event_count, "sealed event count")?,
                stored_u64(uncompressed_bytes, "sealed uncompressed byte count")?,
            )?;
            for line in BufReader::new(payload.as_slice()).lines() {
                let line = line.map_err(|error| {
                    LabError::DataCorrupt(format!("sealed chunk line read: {error}"))
                })?;
                if line.is_empty() {
                    continue;
                }
                lines.push(LedgerLine::from_line(&line)?);
            }
        }
        Ok(Some(lines))
    }
}

/// Child-first compaction deletes over the run scope. Episode rows stay (with
/// their materialized exit details); episode link rows go because their fill
/// and order targets are sealed away.
const COMPACT_DELETES: &[&str] = &[
    "DELETE FROM signal_source_bars WHERE run_id=?1",
    "DELETE FROM episode_fills WHERE run_id=?1",
    "DELETE FROM fills WHERE run_id=?1",
    "DELETE FROM order_events WHERE run_id=?1",
    "DELETE FROM orders WHERE run_id=?1",
    "DELETE FROM episode_orders WHERE run_id=?1",
    "DELETE FROM order_identities WHERE run_id=?1",
    "DELETE FROM signals WHERE run_id=?1",
    "DELETE FROM account_marks WHERE run_id=?1",
    "DELETE FROM run_events WHERE run_id=?1",
];

/// Where a run's detailed ledger rows currently live.
#[derive(Debug)]
pub(crate) enum RunLedgerSource {
    Detail,
    Sealed(Vec<LedgerLine>),
}

/// Resolve whether a run's detail rows are still in SQLite or sealed away.
///
/// # Errors
/// Reports corrupt stored state or chunk-file verification failures.
pub(crate) fn run_ledger_source(
    store: &Store,
    run_id: &RunId,
) -> Result<RunLedgerSource, LabError> {
    let state: String = store
        .connection
        .query_row(
            "SELECT ledger_state FROM runs WHERE id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| LabError::InvalidConfig("unknown run".into()))?;
    match RunLedgerState::from_text(&state)? {
        RunLedgerState::Compacted => store
            .load_run_ledger_lines(run_id)?
            .map(RunLedgerSource::Sealed)
            .ok_or_else(|| LabError::DataCorrupt("compacted run has no ledger chunks".into())),
        _ => Ok(RunLedgerSource::Detail),
    }
}

pub(crate) fn records_of_kind<T: serde::de::DeserializeOwned>(
    lines: &[LedgerLine],
    model_id: &ModelId,
    section: LedgerSection,
) -> Result<Vec<T>, LabError> {
    let mut records: Vec<(u64, T)> = lines
        .iter()
        .filter(|line| line.section == section && &line.model_id == model_id)
        .map(|line| {
            Ok((
                line.event_seq,
                serde_json::from_value(line.record.clone()).map_err(json_error)?,
            ))
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    records.sort_by_key(|(seq, _)| *seq);
    Ok(records.into_iter().map(|(_, record)| record).collect())
}

/// Rebuild the four event-keyed fact vectors for one model from sealed lines.
pub(crate) struct ModelFactVectors {
    pub signals: Vec<SignalRecord>,
    pub order_events: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub account_marks: Vec<AccountMark>,
}

pub(crate) fn sequenced_from_lines(
    lines: &[LedgerLine],
    model_id: &ModelId,
) -> Result<ModelFactVectors, LabError> {
    let signals: Vec<SignalRecord> = records_of_kind(lines, model_id, LedgerSection::Signals)?;
    let order_events: Vec<OrderRecord> =
        records_of_kind(lines, model_id, LedgerSection::OrderEvents)?;
    let mut fills: Vec<(u64, FillRecord)> = lines
        .iter()
        .filter(|line| line.section == LedgerSection::Fills && &line.model_id == model_id)
        .map(|line| {
            let fill: FillRecord =
                serde_json::from_value(line.record.clone()).map_err(json_error)?;
            // Preserve load_fills' denormalized-column cross-check semantics:
            // the sealed line re-records the columns the writer verified.
            if line.liquidity_source_bar_id.as_deref()
                != Some(fill.liquidity_source_bar_id.as_str())
            {
                return Err(LabError::DataCorrupt(
                    "sealed fill liquidity source mismatch".into(),
                ));
            }
            if line.liquidity_source_close_time_ms
                != Some(fill.liquidity_source_close_time.0.timestamp_millis())
            {
                return Err(LabError::DataCorrupt(
                    "sealed fill liquidity close time mismatch".into(),
                ));
            }
            Ok((line.event_seq, fill))
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    fills.sort_by_key(|(seq, _)| *seq);
    let marks: Vec<AccountMark> = records_of_kind(lines, model_id, LedgerSection::Equity)?;
    Ok(ModelFactVectors {
        signals,
        order_events,
        fills: fills.into_iter().map(|(_, fill)| fill).collect(),
        account_marks: marks,
    })
}

/// True when the line's accounting time is inside the half-open UTC range.
pub(crate) fn line_in_range(line: &LedgerLine, range: Option<&crate::contracts::UtcRange>) -> bool {
    match range {
        None => true,
        Some(range) => {
            let ms = line.accounting_event_time_ms;
            ms >= range.start().0.timestamp_millis() && ms < range.end().0.timestamp_millis()
        }
    }
}
