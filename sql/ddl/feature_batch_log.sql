CREATE TABLE IF NOT EXISTS feature_batch_log (
    run_id TEXT PRIMARY KEY,
    feature_version TEXT NOT NULL,
    run_start_ts TIMESTAMPTZ NOT NULL,
    run_end_ts TIMESTAMPTZ NOT NULL,
    zone_filter TEXT,
    lag_null_policy TEXT NOT NULL,
    source_min_ts TIMESTAMPTZ,
    source_max_ts TIMESTAMPTZ,
    row_count BIGINT,
    state TEXT NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_feature_batch_log_state
    ON feature_batch_log (state, started_at DESC);
