CREATE TABLE application_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE raw_objects (
    id TEXT PRIMARY KEY,
    relative_path TEXT NOT NULL UNIQUE,
    source_url TEXT NOT NULL,
    fetched_at_ms INTEGER NOT NULL,
    persisted_at_ms INTEGER NOT NULL,
    http_status INTEGER NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    remaining_req TEXT,
    raw_sha256 TEXT NOT NULL,
    compressed_sha256 TEXT NOT NULL,
    raw_bytes INTEGER NOT NULL CHECK (raw_bytes >= 0),
    compressed_bytes INTEGER NOT NULL CHECK (compressed_bytes >= 0),
    origin TEXT NOT NULL,
    object_json TEXT NOT NULL
) STRICT;

CREATE TABLE candle_observations (
    id TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time_ms INTEGER NOT NULL,
    close_time_ms INTEGER NOT NULL,
    open_decimal TEXT NOT NULL,
    high_decimal TEXT NOT NULL,
    low_decimal TEXT NOT NULL,
    close_decimal TEXT NOT NULL,
    volume_decimal TEXT NOT NULL,
    quote_turnover_decimal TEXT NOT NULL,
    completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
    content_digest TEXT NOT NULL,
    observation_json TEXT NOT NULL
) STRICT;

CREATE INDEX candle_observations_slot
ON candle_observations(market, interval, open_time_ms);

CREATE TABLE observation_raw_objects (
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (observation_id, raw_object_id),
    UNIQUE (observation_id, position)
) STRICT;

CREATE TABLE observation_constituents (
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    constituent_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (observation_id, constituent_id),
    UNIQUE (observation_id, position)
) STRICT;

CREATE TABLE collections (
    request_id TEXT PRIMARY KEY,
    normalized_request_digest TEXT NOT NULL,
    request_json TEXT NOT NULL
) STRICT;

CREATE TABLE collection_pages (
    request_id TEXT NOT NULL REFERENCES collections(request_id),
    market TEXT NOT NULL,
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    requested_to_ms INTEGER NOT NULL,
    next_to_ms INTEGER,
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    page_digest TEXT NOT NULL,
    page_json TEXT NOT NULL,
    PRIMARY KEY (request_id, market, page_index),
    UNIQUE (raw_object_id)
) STRICT;

CREATE INDEX collection_pages_checkpoint
ON collection_pages(request_id, market, page_index DESC);

CREATE TABLE collection_page_members (
    request_id TEXT NOT NULL,
    market TEXT NOT NULL,
    page_index INTEGER NOT NULL,
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (request_id, market, page_index, observation_id),
    UNIQUE (request_id, market, page_index, position),
    FOREIGN KEY (request_id, market, page_index)
        REFERENCES collection_pages(request_id, market, page_index)
) STRICT;

CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES collections(request_id),
    normalized_request_digest TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    status TEXT NOT NULL,
    coverage_start_ms INTEGER NOT NULL,
    coverage_end_ms INTEGER NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    normalizer_version TEXT NOT NULL,
    gap_policy TEXT NOT NULL,
    semantic_digest TEXT NOT NULL,
    provenance_digest TEXT NOT NULL,
    origin TEXT NOT NULL,
    manifest_json TEXT NOT NULL
) STRICT;

CREATE TABLE dataset_members (
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (dataset_id, observation_id),
    UNIQUE (dataset_id, position)
) STRICT;

CREATE TABLE dataset_raw_objects (
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (dataset_id, raw_object_id),
    UNIQUE (dataset_id, position)
) STRICT;

CREATE TABLE dataset_observation_raw_objects (
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (dataset_id, observation_id, raw_object_id),
    UNIQUE (dataset_id, observation_id, position)
) STRICT;

CREATE TABLE dataset_observation_constituents (
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    constituent_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (dataset_id, observation_id, constituent_id),
    UNIQUE (dataset_id, observation_id, position)
) STRICT;

CREATE TABLE quality_issues (
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    issue_index INTEGER NOT NULL CHECK (issue_index >= 0),
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    market TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    issue_count INTEGER NOT NULL CHECK (issue_count >= 0),
    detail TEXT NOT NULL,
    issue_json TEXT NOT NULL,
    PRIMARY KEY (dataset_id, issue_index)
) STRICT;

CREATE TABLE quality_issue_raw_objects (
    dataset_id TEXT NOT NULL,
    issue_index INTEGER NOT NULL,
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (dataset_id, issue_index, raw_object_id),
    UNIQUE (dataset_id, issue_index, position),
    FOREIGN KEY (dataset_id, issue_index)
        REFERENCES quality_issues(dataset_id, issue_index)
) STRICT;
