PRAGMA defer_foreign_keys=ON;

CREATE TABLE policies (
    policy_id TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    origin TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    head_revision_id TEXT NOT NULL,
    revision_count INTEGER NOT NULL CHECK (revision_count > 0),
    name TEXT NOT NULL
) STRICT;

CREATE TABLE policy_revisions (
    policy_id TEXT NOT NULL REFERENCES policies(policy_id),
    revision_id TEXT NOT NULL,
    revision_number INTEGER NOT NULL CHECK (revision_number > 0),
    parent_revision_id TEXT,
    request_id TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    PRIMARY KEY (policy_id, revision_id),
    UNIQUE (policy_id, revision_number),
    UNIQUE (policy_id, revision_id, definition_digest),
    FOREIGN KEY (policy_id, parent_revision_id)
        REFERENCES policy_revisions(policy_id, revision_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE INDEX policies_history
ON policies(policy_id);
CREATE INDEX policy_revisions_history
ON policy_revisions(policy_id, revision_number DESC);

CREATE TABLE plan_policy_revisions (
    plan_id TEXT NOT NULL REFERENCES plans(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    policy_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    frozen_json TEXT NOT NULL,
    PRIMARY KEY (plan_id, position),
    UNIQUE (plan_id, policy_id, revision_id),
    FOREIGN KEY (policy_id, revision_id, definition_digest)
        REFERENCES policy_revisions(policy_id, revision_id, definition_digest)
) STRICT;

CREATE TABLE run_models_v7 (
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
    policy_id TEXT,
    policy_revision_id TEXT,
    policy_definition_digest TEXT,
    PRIMARY KEY (run_id, model_id),
    FOREIGN KEY (policy_id, policy_revision_id, policy_definition_digest)
        REFERENCES policy_revisions(policy_id, revision_id, definition_digest),
    CHECK (
        (policy_id IS NULL AND policy_revision_id IS NULL AND policy_definition_digest IS NULL)
        OR
        (policy_id IS NOT NULL AND policy_revision_id IS NOT NULL AND policy_definition_digest IS NOT NULL)
    )
) STRICT;

INSERT INTO run_models_v7(
    run_id,model_id,market,strategy_kind,strategy_json,status,status_reason,
    last_committed_event_seq,signal_count,order_event_count,fill_count,
    account_mark_count,episode_count,fact_digest,final_digest
)
SELECT run_id,model_id,market,strategy_kind,strategy_json,status,status_reason,
    last_committed_event_seq,signal_count,order_event_count,fill_count,
    account_mark_count,episode_count,fact_digest,final_digest
FROM run_models;

DROP TABLE run_models;
ALTER TABLE run_models_v7 RENAME TO run_models;

CREATE INDEX run_models_policy_history
ON run_models(policy_id, policy_revision_id, policy_definition_digest, run_id, model_id);
