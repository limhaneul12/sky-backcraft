ALTER TABLE fills ADD COLUMN liquidity_source_bar_id TEXT
    REFERENCES candle_observations(id);

ALTER TABLE fills ADD COLUMN liquidity_source_close_time_ms INTEGER;

CREATE INDEX fills_liquidity_source
ON fills(run_id, model_id, liquidity_source_close_time_ms, event_seq);
