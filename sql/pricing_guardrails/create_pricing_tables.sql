CREATE TABLE IF NOT EXISTS pricing_decisions (
    pricing_run_key TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    pricing_created_at TIMESTAMPTZ NOT NULL,
    horizon_index INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    forecast_run_id TEXT NOT NULL,
    forecast_created_at TIMESTAMPTZ NOT NULL,
    y_pred DOUBLE PRECISION NOT NULL,
    y_pred_lower DOUBLE PRECISION NOT NULL,
    y_pred_upper DOUBLE PRECISION NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    uncertainty_band TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    model_stage TEXT NOT NULL,
    feature_version TEXT NOT NULL,

    baseline_expected_demand DOUBLE PRECISION NOT NULL,
    baseline_reference_level TEXT NOT NULL,
    demand_ratio DOUBLE PRECISION NOT NULL,
    raw_multiplier DOUBLE PRECISION NOT NULL,
    pre_cap_multiplier DOUBLE PRECISION NOT NULL,
    post_cap_multiplier DOUBLE PRECISION NOT NULL,
    candidate_multiplier_before_rate_limit DOUBLE PRECISION NOT NULL,
    final_multiplier DOUBLE PRECISION NOT NULL,

    cap_applied BOOLEAN NOT NULL DEFAULT FALSE,
    cap_type TEXT,
    cap_reason TEXT,
    cap_value DOUBLE PRECISION,
    rate_limit_applied BOOLEAN NOT NULL DEFAULT FALSE,
    rate_limit_direction TEXT NOT NULL DEFAULT 'none',
    previous_final_multiplier DOUBLE PRECISION,
    smoothing_applied BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_applied BOOLEAN NOT NULL DEFAULT FALSE,

    primary_reason_code TEXT NOT NULL,
    reason_codes_json JSONB NOT NULL,
    reason_summary TEXT NOT NULL,
    pricing_policy_version TEXT NOT NULL,

    run_id TEXT NOT NULL,
    status TEXT NOT NULL,

    CONSTRAINT pk_pricing_decisions PRIMARY KEY (pricing_run_key, zone_id, bucket_start_ts),
    CONSTRAINT chk_pricing_confidence_score CHECK (confidence_score >= 0 AND confidence_score <= 1),
    CONSTRAINT chk_pricing_pred_nonnegative CHECK (y_pred >= 0),
    CONSTRAINT chk_pricing_final_multiplier_nonnegative CHECK (final_multiplier >= 0),
    CONSTRAINT chk_pricing_rate_limit_direction CHECK (rate_limit_direction IN ('up', 'down', 'none'))
);

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_zone_bucket
    ON pricing_decisions (zone_id, bucket_start_ts);

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_bucket
    ON pricing_decisions (bucket_start_ts);

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_created_at
    ON pricing_decisions (pricing_created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_run_id
    ON pricing_decisions (run_id);

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_primary_reason
    ON pricing_decisions (primary_reason_code);
