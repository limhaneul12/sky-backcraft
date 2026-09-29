-- Run ledger sealing: detailed ledger payloads move to content-addressed
-- compressed files once a run is verified; SQLite keeps catalog metadata.
-- `runs.ledger_state` lifecycle: DETAIL (rows here) -> SEALED (chunks on disk,
-- detail rows still present) -> COMPACTED (detail rows removed, file is the
-- only copy). Readers fall back to chunk files when detail rows are absent.

ALTER TABLE runs ADD COLUMN ledger_state TEXT NOT NULL DEFAULT 'DETAIL';

-- Exit details for closed episodes are materialized while the detail rows
-- still exist (during compaction), so episode pages stay identical after the
-- fills/orders/signals payloads move to sealed chunk files. Open episodes
-- always compute live.
ALTER TABLE episodes ADD COLUMN exit_details_json TEXT;

CREATE TABLE run_ledger_chunks (
    run_id TEXT NOT NULL REFERENCES runs(id),
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    event_seq_start INTEGER NOT NULL CHECK (event_seq_start > 0),
    event_seq_end INTEGER NOT NULL CHECK (event_seq_end >= event_seq_start),
    event_count INTEGER NOT NULL CHECK (event_count > 0),
    sha256 TEXT NOT NULL,
    uncompressed_bytes INTEGER NOT NULL CHECK (uncompressed_bytes >= 0),
    compressed_bytes INTEGER NOT NULL CHECK (compressed_bytes >= 0),
    format_version TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    PRIMARY KEY (run_id, chunk_index)
) STRICT;
