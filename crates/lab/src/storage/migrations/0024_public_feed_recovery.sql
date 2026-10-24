CREATE TABLE provider_absence_pending_reads (
    market TEXT NOT NULL,
    interval TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    pre_anchor_id TEXT NOT NULL,
    pre_anchor_digest TEXT NOT NULL CHECK (length(pre_anchor_digest) = 64),
    post_anchor_id TEXT NOT NULL,
    post_anchor_digest TEXT NOT NULL CHECK (length(post_anchor_digest) = 64),
    first_raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    first_fetched_at_ms INTEGER NOT NULL,
    first_body_digest TEXT NOT NULL CHECK (length(first_body_digest) = 64),
    first_request_scope_digest TEXT NOT NULL CHECK (length(first_request_scope_digest) = 64),
    conflict_raw_object_id TEXT REFERENCES raw_objects(id),
    conflict_observation_id TEXT,
    conflict_digest TEXT CHECK (conflict_digest IS NULL OR length(conflict_digest) = 64),
    CHECK (start_ms < end_ms),
    CHECK (
        (conflict_raw_object_id IS NULL AND conflict_observation_id IS NULL AND conflict_digest IS NULL)
        OR
        (conflict_raw_object_id IS NOT NULL AND conflict_observation_id IS NOT NULL AND conflict_digest IS NOT NULL)
    ),
    PRIMARY KEY (market, interval, start_ms, end_ms)
) STRICT;

CREATE TABLE provider_absence_spans (
    proof_id TEXT PRIMARY KEY CHECK (length(proof_id) = 64),
    market TEXT NOT NULL,
    interval TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    first_raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    second_raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    proof_json TEXT NOT NULL CHECK (json_valid(proof_json)),
    proof_digest TEXT NOT NULL CHECK (length(proof_digest) = 64),
    CHECK (start_ms < end_ms),
    CHECK (first_raw_object_id <> second_raw_object_id),
    UNIQUE (market, interval, start_ms, end_ms)
) STRICT;

CREATE INDEX provider_absence_spans_range
    ON provider_absence_spans(market, interval, start_ms, end_ms);

CREATE TABLE provider_absence_conflicts (
    proof_id TEXT NOT NULL REFERENCES provider_absence_spans(proof_id),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    observation_id TEXT NOT NULL,
    observation_digest TEXT NOT NULL CHECK (length(observation_digest) = 64),
    observed_at_ms INTEGER NOT NULL,
    conflict_digest TEXT NOT NULL CHECK (length(conflict_digest) = 64),
    PRIMARY KEY (proof_id, raw_object_id, observation_id)
) STRICT;

CREATE TABLE provider_feed_recovery_cursors (
    market TEXT NOT NULL,
    interval TEXT NOT NULL,
    accepted_anchor_json TEXT CHECK (accepted_anchor_json IS NULL OR json_valid(accepted_anchor_json)),
    last_recovered_tip_json TEXT CHECK (last_recovered_tip_json IS NULL OR json_valid(last_recovered_tip_json)),
    next_page_to_ms INTEGER,
    pages_scanned INTEGER NOT NULL CHECK (pages_scanned BETWEEN 0 AND 64),
    rows_scanned INTEGER NOT NULL CHECK (rows_scanned BETWEEN 0 AND 12800),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 199),
    pending_absence_confirmation INTEGER NOT NULL CHECK (pending_absence_confirmation IN (0, 1)),
    started_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= started_at_ms),
    PRIMARY KEY (market, interval)
) STRICT;

CREATE TABLE provider_feed_recovery_pages (
    market TEXT NOT NULL,
    interval TEXT NOT NULL,
    recovery_started_at_ms INTEGER NOT NULL,
    page_index INTEGER NOT NULL CHECK (page_index BETWEEN 0 AND 63),
    repeat_of_page_index INTEGER CHECK (
        repeat_of_page_index IS NULL
        OR (repeat_of_page_index >= 0 AND repeat_of_page_index < page_index)
    ),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    requested_to_ms INTEGER,
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 200),
    observations_json TEXT NOT NULL CHECK (json_valid(observations_json)),
    page_digest TEXT NOT NULL CHECK (length(page_digest) = 64),
    PRIMARY KEY (market, interval, recovery_started_at_ms, page_index)
) STRICT;
