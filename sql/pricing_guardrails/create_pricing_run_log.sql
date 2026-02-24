CREATE TABLE IF NOT EXISTS pricing_run_log (
    run_id TEXT PRIMARY KEY,
    pricing_run_key TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    failure_reason TEXT,
    pricing_policy_version TEXT NOT NULL,
    forecast_run_id TEXT,
    target_bucket_start TIMESTAMPTZ,
    target_bucket_end TIMESTAMPTZ,
    zone_count INTEGER,
    row_count INTEGER,
    cap_applied_count INTEGER,
    rate_limited_count INTEGER,
    low_confidence_count INTEGER,
    latency_ms DOUBLE PRECISION,
    config_snapshot JSONB NOT NULL,
    check_summary JSONB,
    artifacts_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pricing_run_log_started_at
    ON pricing_run_log (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pricing_run_log_status
    ON pricing_run_log (status);
