CREATE TABLE IF NOT EXISTS eda_zone_sparsity_summary (
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    total_buckets BIGINT NOT NULL,
    expected_buckets BIGINT NOT NULL,
    nonzero_buckets BIGINT NOT NULL,
    nonzero_ratio DOUBLE PRECISION NOT NULL,
    avg_pickup_count DOUBLE PRECISION NOT NULL,
    median_pickup_count DOUBLE PRECISION NOT NULL,
    std_pickup_count DOUBLE PRECISION,
    max_consecutive_zero_buckets BIGINT NOT NULL,
    active_days BIGINT NOT NULL,
    coverage_ratio DOUBLE PRECISION NOT NULL,
    sparsity_class TEXT,
    data_start_ts TIMESTAMPTZ NOT NULL,
    data_end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, zone_id)
);

CREATE INDEX IF NOT EXISTS idx_eda_zone_sparsity_summary_zone
    ON eda_zone_sparsity_summary (zone_id);
