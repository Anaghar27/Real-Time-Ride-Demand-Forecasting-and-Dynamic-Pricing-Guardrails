CREATE TABLE IF NOT EXISTS eda_time_profile_summary (
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    profile_type TEXT NOT NULL,
    profile_key TEXT NOT NULL,
    mean_pickup_count DOUBLE PRECISION NOT NULL,
    median_pickup_count DOUBLE PRECISION NOT NULL,
    p90_pickup_count DOUBLE PRECISION NOT NULL,
    row_count BIGINT NOT NULL,
    data_start_ts TIMESTAMPTZ NOT NULL,
    data_end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, profile_type, profile_key)
);

CREATE TABLE IF NOT EXISTS eda_zone_profile_summary (
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    total_pickups BIGINT NOT NULL,
    mean_pickup_count DOUBLE PRECISION NOT NULL,
    variance_pickup_count DOUBLE PRECISION NOT NULL,
    coeff_variation DOUBLE PRECISION,
    zero_demand_ratio DOUBLE PRECISION NOT NULL,
    peak_hour_concentration DOUBLE PRECISION NOT NULL,
    intraday_periodicity_score DOUBLE PRECISION,
    weekly_periodicity_score DOUBLE PRECISION,
    seasonality_strength_index DOUBLE PRECISION,
    data_start_ts TIMESTAMPTZ NOT NULL,
    data_end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, zone_id)
);

CREATE TABLE IF NOT EXISTS eda_seasonality_summary (
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_scope TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_rank BIGINT,
    details JSONB,
    data_start_ts TIMESTAMPTZ NOT NULL,
    data_end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, metric_name, metric_scope)
);

CREATE INDEX IF NOT EXISTS idx_eda_time_profile_summary_run
    ON eda_time_profile_summary (run_id, profile_type, profile_key);

CREATE INDEX IF NOT EXISTS idx_eda_zone_profile_summary_zone
    ON eda_zone_profile_summary (zone_id);

CREATE INDEX IF NOT EXISTS idx_eda_seasonality_summary_run
    ON eda_seasonality_summary (run_id, metric_name);
