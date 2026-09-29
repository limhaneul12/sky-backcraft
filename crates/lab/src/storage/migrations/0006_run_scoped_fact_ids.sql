PRAGMA defer_foreign_keys=ON;

CREATE TABLE signals_v6 (
    signal_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    market TEXT NOT NULL,
    signal_time_ms INTEGER NOT NULL,
    decision_available_at_ms INTEGER NOT NULL,
    valid_until_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id, signal_id),
    UNIQUE (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE signal_source_bars_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    observation_id TEXT NOT NULL REFERENCES candle_observations(id),
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (run_id, model_id, signal_id, observation_id),
    UNIQUE (run_id, model_id, signal_id, position),
    FOREIGN KEY (run_id, model_id, signal_id)
        REFERENCES signals_v6(run_id, model_id, signal_id)
) STRICT;

CREATE TABLE order_identities_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    parent_signal_id TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id, order_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id),
    FOREIGN KEY (run_id, model_id, parent_signal_id)
        REFERENCES signals_v6(run_id, model_id, signal_id)
) STRICT;

CREATE TABLE order_events_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    effective_at_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, model_id, order_id)
        REFERENCES order_identities_v6(run_id, model_id, order_id)
) STRICT;

CREATE TABLE episode_identities_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id, episode_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE fills_v6 (
    fill_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    accounting_mark_seq INTEGER NOT NULL,
    price_decimal TEXT NOT NULL,
    qty_decimal TEXT NOT NULL,
    notional_decimal TEXT NOT NULL,
    fee_decimal TEXT NOT NULL,
    source_bar_id TEXT NOT NULL REFERENCES candle_observations(id),
    record_json TEXT NOT NULL,
    liquidity_source_bar_id TEXT REFERENCES candle_observations(id),
    liquidity_source_close_time_ms INTEGER,
    PRIMARY KEY (run_id, model_id, fill_id),
    UNIQUE (run_id, event_seq),
    FOREIGN KEY (run_id, event_seq) REFERENCES run_events(run_id, event_seq),
    FOREIGN KEY (run_id, model_id, order_id)
        REFERENCES order_identities_v6(run_id, model_id, order_id),
    FOREIGN KEY (run_id, model_id, episode_id)
        REFERENCES episode_identities_v6(run_id, model_id, episode_id),
    FOREIGN KEY (run_id, accounting_mark_seq) REFERENCES run_events(run_id, event_seq)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE orders_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    final_status TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    accounting_event_time_ms INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id, order_id),
    FOREIGN KEY (run_id, model_id, order_id)
        REFERENCES order_identities_v6(run_id, model_id, order_id)
) STRICT;

CREATE TABLE episodes_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    net_realized_decimal TEXT NOT NULL,
    fees_decimal TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id, episode_id),
    FOREIGN KEY (run_id, model_id, episode_id)
        REFERENCES episode_identities_v6(run_id, model_id, episode_id)
) STRICT;

CREATE TABLE episode_fills_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (run_id, model_id, episode_id, fill_id),
    UNIQUE (run_id, model_id, episode_id, position),
    FOREIGN KEY (run_id, model_id, episode_id)
        REFERENCES episodes_v6(run_id, model_id, episode_id),
    FOREIGN KEY (run_id, model_id, fill_id)
        REFERENCES fills_v6(run_id, model_id, fill_id)
) STRICT;

CREATE TABLE episode_orders_v6 (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (run_id, model_id, episode_id, order_id),
    UNIQUE (run_id, model_id, episode_id, position),
    FOREIGN KEY (run_id, model_id, episode_id)
        REFERENCES episodes_v6(run_id, model_id, episode_id),
    FOREIGN KEY (run_id, model_id, order_id)
        REFERENCES order_identities_v6(run_id, model_id, order_id)
) STRICT;

INSERT INTO signals_v6 SELECT * FROM signals;
INSERT INTO signal_source_bars_v6
SELECT s.run_id,s.model_id,b.signal_id,b.observation_id,b.position
FROM signal_source_bars b JOIN signals s ON s.signal_id=b.signal_id;
INSERT INTO order_identities_v6 SELECT run_id,model_id,order_id,parent_signal_id FROM order_identities;
INSERT INTO order_events_v6 SELECT * FROM order_events;
INSERT INTO episode_identities_v6 SELECT run_id,model_id,episode_id FROM episode_identities;
INSERT INTO fills_v6 SELECT fill_id,run_id,model_id,event_seq,order_id,episode_id,
    accounting_mark_seq,price_decimal,qty_decimal,notional_decimal,fee_decimal,
    source_bar_id,record_json,liquidity_source_bar_id,liquidity_source_close_time_ms
FROM fills;
INSERT INTO orders_v6 SELECT * FROM orders;
INSERT INTO episodes_v6 SELECT * FROM episodes;
INSERT INTO episode_fills_v6
SELECT e.run_id,e.model_id,l.episode_id,l.fill_id,l.position
FROM episode_fills l JOIN episodes e
  ON e.run_id=l.run_id AND e.episode_id=l.episode_id;
INSERT INTO episode_orders_v6
SELECT e.run_id,e.model_id,l.episode_id,l.order_id,l.position
FROM episode_orders l JOIN episodes e
  ON e.run_id=l.run_id AND e.episode_id=l.episode_id;

DROP TABLE signal_source_bars;
DROP TABLE order_events;
DROP TABLE episode_fills;
DROP TABLE episode_orders;
DROP TABLE fills;
DROP TABLE orders;
DROP TABLE episodes;
DROP TABLE order_identities;
DROP TABLE episode_identities;
DROP TABLE signals;

ALTER TABLE signals_v6 RENAME TO signals;
ALTER TABLE signal_source_bars_v6 RENAME TO signal_source_bars;
ALTER TABLE order_identities_v6 RENAME TO order_identities;
ALTER TABLE order_events_v6 RENAME TO order_events;
ALTER TABLE episode_identities_v6 RENAME TO episode_identities;
ALTER TABLE fills_v6 RENAME TO fills;
ALTER TABLE orders_v6 RENAME TO orders;
ALTER TABLE episodes_v6 RENAME TO episodes;
ALTER TABLE episode_fills_v6 RENAME TO episode_fills;
ALTER TABLE episode_orders_v6 RENAME TO episode_orders;

CREATE INDEX fills_liquidity_source
ON fills(run_id, model_id, liquidity_source_close_time_ms, event_seq);
CREATE INDEX orders_query
ON orders(run_id, model_id, accounting_event_time_ms, event_seq);
CREATE INDEX episodes_query
ON episodes(run_id, model_id, opened_at_ms, episode_id);
