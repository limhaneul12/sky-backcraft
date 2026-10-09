-- Exact side projections for shared-portfolio research. Legacy ledgers remain
-- readable through portfolio_facts and are explicitly unavailable here.

CREATE TABLE portfolio_projection_meta (
    run_id TEXT PRIMARY KEY REFERENCES portfolio_runs(id) ON DELETE CASCADE,
    request_digest TEXT NOT NULL,
    ledger_digest TEXT NOT NULL,
    benchmarks_digest TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    contributions_json TEXT NOT NULL,
    regime_configured INTEGER NOT NULL CHECK (regime_configured IN (0, 1)),
    regime_summary_json TEXT
) STRICT;

CREATE TABLE portfolio_projection_rows (
    run_id TEXT NOT NULL REFERENCES portfolio_projection_meta(run_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('equity', 'allocation', 'rebalance', 'regime')),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    row_json TEXT NOT NULL,
    PRIMARY KEY (run_id, kind, ordinal)
) STRICT;

CREATE INDEX portfolio_projection_rows_kind
ON portfolio_projection_rows(run_id, kind, ordinal);
