-- Immutable, account-scoped activation loss anchors from current local account
-- observations. This never claims provider-owned midnight/as-of evidence.
CREATE TABLE automation_live_activation_references (
    account_identity TEXT PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode = 'LIVE'),
    source TEXT NOT NULL CHECK (source = 'CURRENT_LOCAL_OBSERVED'),
    snapshot_sequence INTEGER NOT NULL UNIQUE
        REFERENCES automation_account_snapshots(snapshot_sequence) ON DELETE RESTRICT,
    control_sequence INTEGER NOT NULL CHECK (control_sequence > 0),
    reference_max_feed_age_seconds INTEGER NOT NULL CHECK (reference_max_feed_age_seconds BETWEEN 1 AND 86400),
    reference_observed_at_utc TEXT NOT NULL,
    reference_received_at_utc TEXT NOT NULL,
    valuation_recorded_at_utc TEXT NOT NULL,
    balances_json TEXT NOT NULL CHECK (json_valid(balances_json)),
    balances_digest TEXT NOT NULL CHECK (length(balances_digest) = 64),
    valuation_state TEXT NOT NULL CHECK (valuation_state IN ('PENDING_MARKS','VERIFIED')),
    valuation_json TEXT NOT NULL CHECK (json_valid(valuation_json)),
    valuation_digest TEXT NOT NULL CHECK (length(valuation_digest) = 64),
    reference_equity_krw TEXT,
    created_at_utc TEXT NOT NULL,
    CHECK (reference_observed_at_utc <= reference_received_at_utc),
    CHECK (
        (valuation_state = 'VERIFIED' AND reference_equity_krw IS NOT NULL AND length(reference_equity_krw) > 0)
        OR (valuation_state = 'PENDING_MARKS' AND reference_equity_krw IS NULL)
    )
) STRICT;
