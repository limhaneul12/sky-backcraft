-- Transport observations are distinct from authoritative automation fills/orders.
CREATE TABLE private_stream_observations (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    account_identity TEXT NOT NULL,
    event_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    kind TEXT NOT NULL CHECK (kind IN (
        'BASELINE','ORDER_OBSERVED','ASSET_OBSERVED','BOOTSTRAP_COMPLETE','REST_RECONCILED','REST_ORDER_OBSERVED'
    )),
    provider_at_utc TEXT,
    received_at_utc TEXT NOT NULL,
    transport_json TEXT CHECK (transport_json IS NULL OR json_valid(transport_json)),
    facts_digest TEXT NOT NULL,
    record_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    UNIQUE (account_identity, event_key)
) STRICT;

CREATE INDEX private_stream_observations_account_sequence
    ON private_stream_observations(account_identity, sequence);
