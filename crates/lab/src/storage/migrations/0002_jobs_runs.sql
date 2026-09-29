CREATE TABLE evidence_versions (
    revision_id TEXT PRIMARY KEY,
    evidence_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    category TEXT NOT NULL,
    body TEXT NOT NULL,
    declared_available_at_ms INTEGER NOT NULL,
    registered_at_ms INTEGER NOT NULL,
    valid_until_ms INTEGER NOT NULL,
    body_hash TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    supersedes_revision_id TEXT REFERENCES evidence_versions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    version_json TEXT NOT NULL
) STRICT;

CREATE INDEX evidence_versions_identity
ON evidence_versions(evidence_id, declared_available_at_ms, revision_id);

CREATE TABLE evidence_version_markets (
    revision_id TEXT NOT NULL REFERENCES evidence_versions(revision_id),
    market TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (revision_id, market),
    UNIQUE (revision_id, position)
) STRICT;

CREATE TABLE evidence_version_sources (
    revision_id TEXT NOT NULL REFERENCES evidence_versions(revision_id),
    source_ref TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (revision_id, source_ref),
    UNIQUE (revision_id, position)
) STRICT;

CREATE TABLE evidence_snapshots (
    id TEXT PRIMARY KEY,
    digest TEXT NOT NULL UNIQUE,
    snapshot_json TEXT NOT NULL
) STRICT;

CREATE TABLE evidence_snapshot_members (
    snapshot_id TEXT NOT NULL REFERENCES evidence_snapshots(id),
    revision_id TEXT NOT NULL REFERENCES evidence_versions(revision_id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (snapshot_id, revision_id),
    UNIQUE (snapshot_id, position)
) STRICT;

CREATE TABLE plans (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    config_digest TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    evidence_snapshot_id TEXT REFERENCES evidence_snapshots(id),
    original_request_json TEXT NOT NULL,
    resolved_plan_json TEXT NOT NULL
) STRICT;

CREATE TABLE plan_datasets (
    plan_id TEXT NOT NULL REFERENCES plans(id),
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    semantic_digest TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (plan_id, dataset_id),
    UNIQUE (plan_id, position)
) STRICT;

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    normalized_input_digest TEXT NOT NULL,
    payload_kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    current_attempt_number INTEGER NOT NULL CHECK (current_attempt_number > 0),
    current_status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE job_attempts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    input_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL,
    progress_stage TEXT NOT NULL,
    committed_records INTEGER NOT NULL CHECK (committed_records >= 0),
    last_committed_event_seq INTEGER NOT NULL CHECK (last_committed_event_seq >= 0),
    queued_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    ended_at_ms INTEGER,
    cancel_requested_at_ms INTEGER,
    UNIQUE (job_id, attempt_number)
) STRICT;

CREATE INDEX job_attempts_queue
ON job_attempts(status, queued_at_ms, job_id, attempt_number);

CREATE TABLE job_attempt_dataset_outputs (
    attempt_id TEXT PRIMARY KEY REFERENCES job_attempts(id),
    dataset_id TEXT NOT NULL REFERENCES datasets(id)
) STRICT;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_id TEXT NOT NULL UNIQUE REFERENCES job_attempts(id),
    plan_id TEXT NOT NULL REFERENCES plans(id),
    state TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    last_committed_event_seq INTEGER NOT NULL CHECK (last_committed_event_seq >= 0),
    semantic_digest TEXT,
    manifest_json TEXT NOT NULL
) STRICT;

CREATE TABLE job_attempt_run_outputs (
    attempt_id TEXT PRIMARY KEY REFERENCES job_attempts(id),
    run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
    completed_models INTEGER NOT NULL CHECK (completed_models >= 0),
    blocked_models INTEGER NOT NULL CHECK (blocked_models >= 0)
) STRICT;

CREATE TABLE run_models (
    run_id TEXT NOT NULL REFERENCES runs(id),
    model_id TEXT NOT NULL,
    market TEXT NOT NULL,
    strategy_kind TEXT NOT NULL,
    strategy_json TEXT NOT NULL,
    status TEXT,
    status_reason TEXT,
    last_committed_event_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_committed_event_seq >= 0),
    signal_count INTEGER NOT NULL DEFAULT 0 CHECK (signal_count >= 0),
    order_event_count INTEGER NOT NULL DEFAULT 0 CHECK (order_event_count >= 0),
    fill_count INTEGER NOT NULL DEFAULT 0 CHECK (fill_count >= 0),
    account_mark_count INTEGER NOT NULL DEFAULT 0 CHECK (account_mark_count >= 0),
    episode_count INTEGER NOT NULL DEFAULT 0 CHECK (episode_count >= 0),
    fact_digest TEXT,
    final_digest TEXT,
    PRIMARY KEY (run_id, model_id)
) STRICT;

CREATE TABLE run_events (
    run_id TEXT NOT NULL REFERENCES runs(id),
    event_seq INTEGER NOT NULL CHECK (event_seq > 0),
    model_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    accounting_event_time_ms INTEGER NOT NULL,
    record_id TEXT NOT NULL,
    PRIMARY KEY (run_id, event_seq),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE INDEX run_events_model
ON run_events(run_id, model_id, event_seq);

CREATE INDEX run_events_time
ON run_events(run_id, model_id, accounting_event_time_ms, event_seq);

CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    market TEXT NOT NULL,
    signal_time_ms INTEGER NOT NULL,
    decision_available_at_ms INTEGER NOT NULL,
    valid_until_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    UNIQUE (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE order_identities (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    parent_signal_id TEXT NOT NULL REFERENCES signals(signal_id),
    PRIMARY KEY (run_id, order_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE signal_source_bars (
    signal_id TEXT NOT NULL REFERENCES signals(signal_id),
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (signal_id, observation_id),
    UNIQUE (signal_id, position)
) STRICT;

CREATE TABLE order_events (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    effective_at_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, order_id) REFERENCES order_identities(run_id, order_id)
) STRICT;

CREATE TABLE episode_identities (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    PRIMARY KEY (run_id, episode_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    accounting_mark_seq INTEGER NOT NULL,
    price_decimal TEXT NOT NULL,
    qty_decimal TEXT NOT NULL,
    notional_decimal TEXT NOT NULL,
    fee_decimal TEXT NOT NULL,
    source_bar_id TEXT NOT NULL REFERENCES candle_observations(id),
    record_json TEXT NOT NULL,
    UNIQUE (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, order_id) REFERENCES order_identities(run_id, order_id),
    FOREIGN KEY (run_id, episode_id) REFERENCES episode_identities(run_id, episode_id),
    FOREIGN KEY (run_id, accounting_mark_seq) REFERENCES run_events(run_id, event_seq)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE account_marks (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    cash_total_decimal TEXT NOT NULL,
    cash_free_decimal TEXT NOT NULL,
    cash_reserved_decimal TEXT NOT NULL,
    qty_decimal TEXT NOT NULL,
    price_basis_decimal TEXT NOT NULL,
    cumulative_fees_decimal TEXT NOT NULL,
    equity_decimal TEXT NOT NULL,
    source_bar_id TEXT NOT NULL REFERENCES candle_observations(id),
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE orders (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    final_status TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    accounting_event_time_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, order_id),
    FOREIGN KEY (run_id, order_id) REFERENCES order_identities(run_id, order_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE INDEX orders_query
ON orders(run_id, model_id, accounting_event_time_ms, event_seq);

CREATE TABLE episodes (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    net_realized_decimal TEXT NOT NULL,
    fees_decimal TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, episode_id),
    FOREIGN KEY (run_id, episode_id) REFERENCES episode_identities(run_id, episode_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE INDEX episodes_query
ON episodes(run_id, model_id, opened_at_ms, episode_id);

CREATE TABLE episode_fills (
    run_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    fill_id TEXT NOT NULL REFERENCES fills(fill_id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (run_id, episode_id, fill_id),
    UNIQUE (run_id, episode_id, position),
    FOREIGN KEY (run_id, episode_id) REFERENCES episodes(run_id, episode_id)
) STRICT;

CREATE TABLE episode_orders (
    run_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (run_id, episode_id, order_id),
    UNIQUE (run_id, episode_id, position),
    FOREIGN KEY (run_id, episode_id) REFERENCES episodes(run_id, episode_id),
    FOREIGN KEY (run_id, order_id) REFERENCES order_identities(run_id, order_id)
) STRICT;

CREATE TABLE validation_results (
    identity_key TEXT PRIMARY KEY,
    check_id TEXT NOT NULL,
    run_id TEXT REFERENCES runs(id),
    status TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    report_json TEXT NOT NULL
) STRICT;

CREATE TABLE run_artifacts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    relative_path TEXT NOT NULL UNIQUE,
    media_type TEXT NOT NULL,
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    sha256 TEXT NOT NULL,
    uncompressed_sha256 TEXT,
    uncompressed_bytes INTEGER,
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    artifact_json TEXT NOT NULL
) STRICT;

CREATE TABLE job_attempt_artifact_outputs (
    attempt_id TEXT NOT NULL REFERENCES job_attempts(id),
    artifact_id TEXT NOT NULL REFERENCES run_artifacts(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (attempt_id, artifact_id),
    UNIQUE (attempt_id, position)
) STRICT;
