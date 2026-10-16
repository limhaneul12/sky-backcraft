CREATE TABLE trading_control_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state TEXT NOT NULL CHECK (state IN ('STANDBY', 'RUNNING', 'PAUSED', 'EMERGENCY_STOP_ACTIVE')),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    order_admission_blocked INTEGER NOT NULL CHECK (order_admission_blocked IN (0, 1)),
    emergency_latched INTEGER NOT NULL CHECK (emergency_latched IN (0, 1)),
    updated_at_utc TEXT NOT NULL
) STRICT;

INSERT INTO trading_control_state(
    singleton, state, sequence, order_admission_blocked, emergency_latched, updated_at_utc
) VALUES (1, 'STANDBY', 0, 0, 0, '1970-01-01T00:00:00Z');

CREATE TABLE trading_control_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL CHECK (action IN ('START', 'RESUME', 'PAUSE', 'EMERGENCY_STOP')),
    expected_sequence INTEGER NOT NULL CHECK (expected_sequence >= 0),
    resulting_sequence INTEGER NOT NULL CHECK (resulting_sequence >= 0),
    outcome_json TEXT NOT NULL CHECK (json_valid(outcome_json)),
    created_at_utc TEXT NOT NULL
) STRICT;
