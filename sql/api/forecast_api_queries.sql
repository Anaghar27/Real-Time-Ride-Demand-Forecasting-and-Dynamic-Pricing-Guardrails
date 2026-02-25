-- Forecast API query pack.
-- These templates mirror the service-layer SQL used by versioned forecast endpoints.

-- Latest successful forecast run id.
SELECT run_id
FROM scoring_run_log
WHERE LOWER(status) = 'success'
ORDER BY started_at DESC
LIMIT 1;

-- Latest forecast rows for a run with deterministic sort and pagination.
SELECT
    f.zone_id,
    f.bucket_start_ts,
    f.forecast_run_key,
    f.run_id,
    f.horizon_index,
    z.zone AS zone_name,
    z.borough,
    z.service_zone,
    f.y_pred,
    f.y_pred_lower,
    f.y_pred_upper,
    f.confidence_score,
    f.uncertainty_band,
    f.used_recursive_features,
    f.model_name,
    f.model_version,
    f.model_stage,
    f.feature_version
FROM demand_forecast f
LEFT JOIN dim_zone z ON z.location_id = f.zone_id
WHERE f.run_id = :run_id
ORDER BY f.bucket_start_ts DESC, f.zone_id ASC
LIMIT :limit OFFSET :offset;

-- Forecast run summary.
SELECT
    run_id,
    status,
    started_at,
    ended_at,
    failure_reason,
    model_name,
    model_version,
    model_stage,
    feature_version,
    forecast_run_key,
    forecast_start_ts,
    forecast_end_ts,
    horizon_buckets,
    bucket_minutes,
    zone_count,
    row_count,
    latency_ms
FROM scoring_run_log
WHERE run_id = :run_id;
