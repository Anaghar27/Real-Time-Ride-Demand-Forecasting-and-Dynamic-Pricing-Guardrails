CREATE TABLE IF NOT EXISTS demand_forecast (
    forecast_run_key TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    forecast_created_at TIMESTAMPTZ NOT NULL,
    horizon_index INTEGER NOT NULL,
    y_pred DOUBLE PRECISION NOT NULL,
    y_pred_lower DOUBLE PRECISION NOT NULL,
    y_pred_upper DOUBLE PRECISION NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    uncertainty_band TEXT NOT NULL,
    used_recursive_features BOOLEAN NOT NULL DEFAULT FALSE,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    model_stage TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    scoring_window_start TIMESTAMPTZ NOT NULL,
    scoring_window_end TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_demand_forecast PRIMARY KEY (forecast_run_key, zone_id, bucket_start_ts),
    CONSTRAINT chk_forecast_pred_nonnegative CHECK (y_pred >= 0),
    CONSTRAINT chk_forecast_confidence_score CHECK (confidence_score >= 0 AND confidence_score <= 1),
    CONSTRAINT chk_forecast_bounds CHECK (
        y_pred_lower <= y_pred_upper
        AND y_pred >= y_pred_lower
        AND y_pred <= y_pred_upper
    )
);

CREATE INDEX IF NOT EXISTS idx_demand_forecast_zone_bucket
    ON demand_forecast (zone_id, bucket_start_ts);

CREATE INDEX IF NOT EXISTS idx_demand_forecast_created_at
    ON demand_forecast (forecast_created_at);

CREATE INDEX IF NOT EXISTS idx_demand_forecast_run_key
    ON demand_forecast (forecast_run_key);

