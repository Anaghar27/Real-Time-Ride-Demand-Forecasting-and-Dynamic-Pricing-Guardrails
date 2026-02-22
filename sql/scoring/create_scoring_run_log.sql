CREATE TABLE IF NOT EXISTS scoring_run_log (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    failure_reason TEXT,
    model_name TEXT NOT NULL,
    model_version TEXT,
    model_stage TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    forecast_run_key TEXT,
    scoring_created_at TIMESTAMPTZ NOT NULL,
    forecast_start_ts TIMESTAMPTZ NOT NULL,
    forecast_end_ts TIMESTAMPTZ NOT NULL,
    horizon_buckets INTEGER NOT NULL,
    bucket_minutes INTEGER NOT NULL,
    zone_count INTEGER,
    row_count INTEGER,
    latency_ms DOUBLE PRECISION,
    confidence_reference_updated_at TIMESTAMPTZ,
    config_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scoring_run_log_started_at
    ON scoring_run_log (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_scoring_run_log_status
    ON scoring_run_log (status);

