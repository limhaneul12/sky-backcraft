-- Durable account-scoped daily starting baseline contract and verification ledger.
-- Distinguishes operator-attested inputs from exchange-proven facts and preserves
-- insufficient evidence in BLOCKED_INSUFFICIENT_EVIDENCE without enabling risk admission.

CREATE TABLE automation_account_day_baselines (
    baseline_id TEXT PRIMARY KEY,
    account_identity TEXT NOT NULL,
    day_utc TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('MOCK', 'LIVE')),
    reference_time_utc TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN (
        'OPERATOR_ATTESTED',
        'EXCHANGE_OBSERVED_DAY_START',
        'PRIOR_DAY_VERIFIED_LEDGER_ROLLOVER',
        'DURABLE_MOCK_VENUE_SEED'
    )),
    verification_state TEXT NOT NULL CHECK (verification_state IN (
        'VERIFIED',
        'BLOCKED_INSUFFICIENT_EVIDENCE'
    )),
    sufficient_for_risk_admission INTEGER NOT NULL CHECK (
        sufficient_for_risk_admission IN (0, 1)
    ),
    total_equity_krw TEXT,
    baseline_balances_digest TEXT NOT NULL,
    baseline_balances_json TEXT NOT NULL CHECK (json_valid(baseline_balances_json)),
    valuation_evidence_json TEXT NOT NULL CHECK (json_valid(valuation_evidence_json)),
    block_reasons_json TEXT NOT NULL CHECK (json_valid(block_reasons_json)),
    baseline_digest TEXT NOT NULL,
    request_id TEXT,
    record_json TEXT NOT NULL CHECK (json_valid(record_json)),
    created_at_utc TEXT NOT NULL,
    UNIQUE (account_identity, day_utc),
    CHECK (
        (sufficient_for_risk_admission = 1 AND verification_state = 'VERIFIED' AND total_equity_krw IS NOT NULL)
        OR (sufficient_for_risk_admission = 0 AND verification_state = 'BLOCKED_INSUFFICIENT_EVIDENCE')
    )
) STRICT;

CREATE INDEX automation_account_day_baselines_lookup
    ON automation_account_day_baselines(account_identity, day_utc);

CREATE TABLE automation_session_requests_v2 (
    request_id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES automation_sessions(session_id),
    account_identity TEXT,
    action TEXT NOT NULL CHECK (action IN (
        'START', 'PAUSE', 'RESUME', 'STOP', 'CANCEL_ORDER', 'RECORD_BASELINE'
    )),
    payload_digest TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'APPLIED', 'REJECTED')),
    response_json TEXT CHECK (response_json IS NULL OR json_valid(response_json)),
    reason TEXT,
    created_at_utc TEXT NOT NULL
) STRICT;

INSERT INTO automation_session_requests_v2 (
    request_id, session_id, account_identity, action, payload_digest,
    request_json, state, response_json, reason, created_at_utc
)
SELECT
    request_id, session_id, account_identity, action, payload_digest,
    request_json, state, response_json, reason, created_at_utc
FROM automation_session_requests;

DROP TABLE automation_session_requests;
ALTER TABLE automation_session_requests_v2 RENAME TO automation_session_requests;

CREATE TABLE automation_session_events_v2 (
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    kind TEXT NOT NULL CHECK (kind IN (
        'STARTED', 'PAUSED', 'RESUMED', 'STOPPED', 'CANDLE_OBSERVED',
        'SIGNAL_OBSERVED', 'INTENT_REJECTED', 'ORDER_ADMITTED',
        'ORDER_SEND_CLAIMED', 'ORDER_ACKNOWLEDGED', 'ORDER_CANCEL_REQUESTED',
        'ORDER_RECONCILED', 'FILL_RECORDED', 'NEEDS_RECONCILIATION',
        'RECOVERY_BLOCKED', 'BASELINE_RECORDED', 'BASELINE_BLOCKED'
    )),
    at_utc TEXT NOT NULL,
    detail_json TEXT NOT NULL CHECK (json_valid(detail_json)),
    PRIMARY KEY (session_id, sequence)
) STRICT;

INSERT INTO automation_session_events_v2 (
    session_id, sequence, kind, at_utc, detail_json
)
SELECT
    session_id, sequence, kind, at_utc, detail_json
FROM automation_session_events;

DROP TABLE automation_session_events;
ALTER TABLE automation_session_events_v2 RENAME TO automation_session_events;
