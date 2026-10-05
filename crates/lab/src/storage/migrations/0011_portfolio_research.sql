-- Shared-capital portfolio runs keep their spec immutable and their ledger
-- facts append-only; report projections derive from facts, never rewrite them.

CREATE TABLE portfolio_runs (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    plan_id TEXT NOT NULL REFERENCES plans(id),
    input_digest TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed')),
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    error_json TEXT,
    totals_json TEXT,
    benchmarks_json TEXT
) STRICT;

CREATE INDEX portfolio_runs_due
ON portfolio_runs(status, created_at_ms, id);

CREATE TABLE portfolio_facts (
    run_id TEXT NOT NULL REFERENCES portfolio_runs(id) ON DELETE CASCADE,
    event_seq INTEGER NOT NULL CHECK (event_seq >= 0),
    kind TEXT NOT NULL CHECK (kind IN ('intent', 'fill', 'rejection', 'mark')),
    fact_json TEXT NOT NULL,
    PRIMARY KEY (run_id, event_seq)
) STRICT;

CREATE INDEX portfolio_facts_kind
ON portfolio_facts(run_id, kind, event_seq);
