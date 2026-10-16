CREATE TABLE automation_sessions (
    session_id TEXT PRIMARY KEY,
    start_request_id TEXT NOT NULL UNIQUE,
    account_identity TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('MOCK', 'LIVE')),
    state TEXT NOT NULL CHECK (state IN (
        'RUNNING', 'PAUSED', 'STOPPED', 'NEEDS_RECONCILIATION',
        'BLOCKED_APPROVAL', 'FAULTED'
    )),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    policy_ref_json TEXT NOT NULL CHECK (json_valid(policy_ref_json)),
    frozen_policy_json TEXT NOT NULL CHECK (json_valid(frozen_policy_json)),
    market TEXT NOT NULL,
    decision_interval TEXT NOT NULL,
    risk_json TEXT NOT NULL CHECK (json_valid(risk_json)),
    checkpoint_json TEXT NOT NULL CHECK (json_valid(checkpoint_json)),
    last_finalized_candle_at_utc TEXT,
    last_feed_received_at_utc TEXT,
    last_balance_observed_at_utc TEXT,
    unresolved_order_count INTEGER NOT NULL DEFAULT 0 CHECK (unresolved_order_count >= 0),
    fill_count INTEGER NOT NULL DEFAULT 0 CHECK (fill_count >= 0),
    recovery_reason TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
) STRICT;

CREATE INDEX automation_sessions_state_due
    ON automation_sessions(state, updated_at_utc, session_id);

CREATE TABLE automation_session_requests (
    request_id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES automation_sessions(session_id),
    account_identity TEXT,
    action TEXT NOT NULL CHECK (action IN ('START', 'PAUSE', 'RESUME', 'STOP', 'CANCEL_ORDER')),
    payload_digest TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    state TEXT NOT NULL CHECK (state IN ('PENDING','APPLIED','REJECTED')),
    response_json TEXT CHECK (response_json IS NULL OR json_valid(response_json)),
    reason TEXT,
    created_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_session_events (
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    kind TEXT NOT NULL CHECK (kind IN (
        'STARTED', 'PAUSED', 'RESUMED', 'STOPPED', 'CANDLE_OBSERVED',
        'SIGNAL_OBSERVED', 'INTENT_REJECTED', 'ORDER_ADMITTED',
        'ORDER_SEND_CLAIMED', 'ORDER_ACKNOWLEDGED', 'ORDER_CANCEL_REQUESTED', 'ORDER_RECONCILED', 'FILL_RECORDED',
        'NEEDS_RECONCILIATION', 'RECOVERY_BLOCKED'
    )),
    at_utc TEXT NOT NULL,
    detail_json TEXT NOT NULL CHECK (json_valid(detail_json)),
    PRIMARY KEY (session_id, sequence)
) STRICT;

CREATE TABLE automation_candles (
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    observation_id TEXT NOT NULL,
    close_time_utc TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    candle_json TEXT NOT NULL CHECK (json_valid(candle_json)),
    checkpoint_json TEXT NOT NULL CHECK (json_valid(checkpoint_json)),
    PRIMARY KEY (session_id, observation_id),
    UNIQUE (session_id, close_time_utc)
) STRICT;

CREATE TABLE automation_intents (
    intent_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    observation_id TEXT NOT NULL,
    decision_time_utc TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('BUY', 'SELL', 'HOLD')),
    state TEXT NOT NULL CHECK (state IN (
        'NO_ACTION', 'PENDING', 'REJECTED', 'ORDER_RESERVED', 'ORDER_SUBMITTED',
        'ORDER_UNKNOWN', 'ORDER_RECONCILED'
    )),
    evaluation_json TEXT NOT NULL CHECK (json_valid(evaluation_json)),
    created_at_utc TEXT NOT NULL,
    UNIQUE (session_id, observation_id),
    FOREIGN KEY (session_id, observation_id)
        REFERENCES automation_candles(session_id, observation_id)
) STRICT;

CREATE TABLE automation_orders (
    client_identifier TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE REFERENCES automation_intents(intent_id),
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    account_identity TEXT NOT NULL,
    order_request_json TEXT NOT NULL CHECK (json_valid(order_request_json)),
    rules_snapshot_json TEXT NOT NULL CHECK (json_valid(rules_snapshot_json)),
    request_digest TEXT NOT NULL,
    market TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BID', 'ASK')),
    state TEXT NOT NULL CHECK (state IN (
        'RESERVED', 'SUBMITTED_UNKNOWN', 'OPEN', 'PARTIALLY_FILLED',
        'FILLED', 'CANCELLED', 'REJECTED', 'NEEDS_RECONCILIATION'
    )),
    venue_order_uuid TEXT UNIQUE,
    requested_notional_krw TEXT NOT NULL,
    decision_mark_krw TEXT NOT NULL,
    reserve_quote_krw TEXT NOT NULL,
    reserve_asset TEXT NOT NULL,
    reserved_asset_volume TEXT NOT NULL,
    reserved_fee_krw TEXT NOT NULL,
    remaining_fee_krw TEXT NOT NULL,
    executed_volume TEXT NOT NULL,
    remaining_volume TEXT NOT NULL,
    paid_fee_krw TEXT NOT NULL,
    locked_amount TEXT NOT NULL,
    locked_currency TEXT NOT NULL,
    trades_count INTEGER NOT NULL CHECK (trades_count >= 0),
    venue_created_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    detail_json TEXT CHECK (detail_json IS NULL OR json_valid(detail_json))
) STRICT;

CREATE INDEX automation_orders_account_state
    ON automation_orders(account_identity, state, created_at_utc, client_identifier);

CREATE TABLE automation_risk_reservations (
    client_identifier TEXT PRIMARY KEY REFERENCES automation_orders(client_identifier),
    account_identity TEXT NOT NULL,
    market TEXT NOT NULL,
    quote_reserved_krw TEXT NOT NULL,
    asset_reserved TEXT NOT NULL,
    asset_reserved_volume TEXT NOT NULL,
    order_notional_krw TEXT NOT NULL,
    order_day_utc TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RELEASED')),
    created_at_utc TEXT NOT NULL,
    released_at_utc TEXT
) STRICT;

CREATE INDEX automation_risk_reservations_active
    ON automation_risk_reservations(account_identity,state, order_day_utc, market);

CREATE TABLE automation_account_snapshots (
    snapshot_sequence INTEGER PRIMARY KEY,
    account_identity TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    source_mode TEXT NOT NULL CHECK (source_mode IN ('MOCK', 'LIVE')),
    balances_json TEXT NOT NULL CHECK (json_valid(balances_json)),
    balances_digest TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_account_days (
    account_identity TEXT NOT NULL,
    day_utc TEXT NOT NULL,
    baseline_equity_krw TEXT NOT NULL,
    baseline_balances_digest TEXT NOT NULL,
    baseline_balances_json TEXT NOT NULL CHECK (json_valid(baseline_balances_json)),
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY (account_identity, day_utc)
) STRICT;

CREATE TABLE automation_fills (
    trade_uuid TEXT PRIMARY KEY,
    client_identifier TEXT NOT NULL REFERENCES automation_orders(client_identifier),
    session_id TEXT NOT NULL REFERENCES automation_sessions(session_id),
    market TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BID', 'ASK')),
    price_krw TEXT NOT NULL,
    volume TEXT NOT NULL,
    funds_krw TEXT NOT NULL,
    trade_time_utc TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
) STRICT;

CREATE INDEX automation_fills_session_order
    ON automation_fills(session_id, client_identifier, trade_time_utc, trade_uuid);

CREATE TABLE automation_fee_accruals (
    client_identifier TEXT NOT NULL REFERENCES automation_orders(client_identifier),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    delta_paid_fee_krw TEXT NOT NULL,
    cumulative_paid_fee_krw TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    PRIMARY KEY (client_identifier, sequence)
) STRICT;

-- The deterministic mock venue is a separate fact authority, but shares the
-- single DatabaseOwner so mock ACK/fill facts survive process restart.
CREATE TABLE automation_mock_venue_seed (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    seed_version INTEGER NOT NULL CHECK (seed_version = 1),
    created_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_mock_venue_accounts (
    currency TEXT PRIMARY KEY,
    balance TEXT NOT NULL,
    locked TEXT NOT NULL,
    avg_buy_price TEXT NOT NULL,
    avg_buy_price_modified INTEGER NOT NULL CHECK (avg_buy_price_modified IN (0,1)),
    unit_currency TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_mock_venue_identifiers (
    client_identifier TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    venue_uuid TEXT NOT NULL UNIQUE,
    created_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_mock_venue_orders (
    client_identifier TEXT PRIMARY KEY REFERENCES automation_mock_venue_identifiers(client_identifier),
    venue_uuid TEXT NOT NULL UNIQUE,
    detail_json TEXT NOT NULL CHECK (json_valid(detail_json)),
    updated_at_utc TEXT NOT NULL
) STRICT;

CREATE TABLE automation_mock_venue_trades (
    trade_uuid TEXT PRIMARY KEY,
    client_identifier TEXT NOT NULL REFERENCES automation_mock_venue_identifiers(client_identifier),
    trade_json TEXT NOT NULL CHECK (json_valid(trade_json)),
    created_at_utc TEXT NOT NULL
) STRICT;
