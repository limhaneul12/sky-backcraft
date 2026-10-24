-- Extend the durable automation event vocabulary without changing prior migrations.
-- The table has no external references; retain its original FK, key, checks and payloads.
CREATE TABLE automation_session_events_v3 (
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    kind TEXT NOT NULL CHECK (kind IN (
        'STARTED', 'PAUSED', 'RESUMED', 'STOPPED', 'CANDLE_OBSERVED',
        'SIGNAL_OBSERVED', 'INTENT_REJECTED', 'ORDER_ADMITTED',
        'ORDER_SEND_CLAIMED', 'ORDER_ACKNOWLEDGED', 'ORDER_CANCEL_REQUESTED',
        'ORDER_RECONCILED', 'FILL_RECORDED', 'NEEDS_RECONCILIATION',
        'RECOVERY_BLOCKED', 'BASELINE_RECORDED', 'BASELINE_BLOCKED',
        'DECISION_DEFERRED'
    )),
    at_utc TEXT NOT NULL,
    detail_json TEXT NOT NULL CHECK (json_valid(detail_json)),
    PRIMARY KEY (session_id, sequence)
) STRICT;

INSERT INTO automation_session_events_v3 (
    session_id, sequence, kind, at_utc, detail_json
)
SELECT
    session_id, sequence, kind, at_utc, detail_json
FROM automation_session_events;

DROP TABLE automation_session_events;
ALTER TABLE automation_session_events_v3 RENAME TO automation_session_events;

CREATE INDEX automation_events_deferred_lookup
    ON automation_session_events(session_id, sequence)
    WHERE kind = 'DECISION_DEFERRED';
