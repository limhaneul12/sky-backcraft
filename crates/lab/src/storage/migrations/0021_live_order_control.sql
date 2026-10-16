CREATE TABLE live_order_control_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    boot_epoch TEXT NOT NULL CHECK (length(boot_epoch) BETWEEN 1 AND 128),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    live_trading_enabled INTEGER NOT NULL CHECK (live_trading_enabled IN (0, 1)),
    available INTEGER NOT NULL CHECK (available IN (0, 1)),
    updated_at_utc TEXT NOT NULL
) STRICT;

INSERT INTO live_order_control_state(
    singleton, boot_epoch, sequence, live_trading_enabled, available, updated_at_utc
) VALUES (1, 'uninitialized', 0, 0, 0, '1970-01-01T00:00:00Z');

CREATE TABLE live_order_control_intents (
    request_id TEXT PRIMARY KEY CHECK (length(request_id) BETWEEN 1 AND 128),
    action TEXT NOT NULL CHECK (action IN ('ENABLE', 'DISABLE')),
    expected_sequence INTEGER NOT NULL CHECK (expected_sequence >= 0),
    expected_epoch TEXT NOT NULL CHECK (length(expected_epoch) BETWEEN 1 AND 128),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    boot_epoch TEXT NOT NULL CHECK (length(boot_epoch) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'ACKNOWLEDGED', 'DENIED')),
    observed_at_utc TEXT NOT NULL,
    denied_code TEXT,
    CHECK ((status = 'DENIED' AND denied_code IS NOT NULL)
        OR (status != 'DENIED' AND denied_code IS NULL))
) STRICT;

CREATE INDEX live_order_control_intents_status
    ON live_order_control_intents(status, observed_at_utc, request_id);

CREATE TABLE live_order_control_fail_safe_closures (
    boot_epoch TEXT NOT NULL CHECK (length(boot_epoch) BETWEEN 1 AND 128),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
    reason TEXT NOT NULL CHECK (reason IN ('REPLAYED_OFF', 'IDENTITY_CONFLICT_OFF')),
    observed_at_utc TEXT NOT NULL,
    PRIMARY KEY (boot_epoch, sequence)
) STRICT;

CREATE INDEX live_order_control_fail_safe_closures_request
    ON live_order_control_fail_safe_closures(request_id, observed_at_utc);
