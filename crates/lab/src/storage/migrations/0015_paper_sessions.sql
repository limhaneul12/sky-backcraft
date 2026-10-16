CREATE TABLE IF NOT EXISTS paper_sessions (
    session_id TEXT PRIMARY KEY,
    origin_request_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'PAUSED', 'STOPPED', 'RECOVERY_REQUIRED')),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    schema_version TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_revision_id TEXT NOT NULL,
    policy_ref_json TEXT NOT NULL CHECK (json_valid(policy_ref_json)),
    decision_interval TEXT NOT NULL,
    execution_resolution TEXT NOT NULL,
    latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
    execution_json TEXT NOT NULL CHECK (json_valid(execution_json)),
    markets_json TEXT NOT NULL CHECK (json_valid(markets_json)),
    created_at_utc TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    paused_at_utc TEXT,
    stopped_at_utc TEXT,
    last_evaluated_at_utc TEXT,
    event_count INTEGER NOT NULL CHECK (event_count >= 0),
    recovery_reason TEXT,
    view_json TEXT NOT NULL CHECK (json_valid(view_json)),
    FOREIGN KEY (policy_id, policy_revision_id)
        REFERENCES policy_revisions(policy_id, revision_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_paper_sessions_state_created
    ON paper_sessions (state, created_at_utc DESC, session_id ASC);

CREATE INDEX IF NOT EXISTS idx_paper_sessions_policy_id
    ON paper_sessions (policy_id, session_id ASC);

CREATE TABLE IF NOT EXISTS paper_market_checkpoints (
    session_id TEXT NOT NULL,
    market_order INTEGER NOT NULL CHECK (market_order >= 0),
    market TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    schema_version TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    checkpoint_digest TEXT NOT NULL,
    checkpoint_json TEXT NOT NULL CHECK (json_valid(checkpoint_json)),
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (session_id, market),
    FOREIGN KEY (session_id) REFERENCES paper_sessions(session_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_paper_market_checkpoints_order
    ON paper_market_checkpoints (session_id, market_order ASC);

CREATE TABLE IF NOT EXISTS paper_session_events (
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    market TEXT,
    kind TEXT NOT NULL,
    occurred_at_utc TEXT NOT NULL,
    event_json TEXT NOT NULL CHECK (json_valid(event_json)),
    PRIMARY KEY (session_id, sequence),
    FOREIGN KEY (session_id) REFERENCES paper_sessions(session_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_paper_session_events_market
    ON paper_session_events (session_id, market, sequence DESC);

CREATE TABLE IF NOT EXISTS paper_request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    session_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    receipt_json TEXT NOT NULL CHECK (json_valid(receipt_json))
) STRICT;
