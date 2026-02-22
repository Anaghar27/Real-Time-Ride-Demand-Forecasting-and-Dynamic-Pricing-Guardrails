-- Latest scoring runs
SELECT *
FROM scoring_run_log
ORDER BY started_at DESC
LIMIT 20;

-- Latest forecast run key (most recent successful scoring run)
SELECT forecast_run_key
FROM scoring_run_log
WHERE status = 'succeeded'
ORDER BY started_at DESC
LIMIT 1;

-- Latest forecast rows per zone and bucket (useful for downstream consumers)
SELECT DISTINCT ON (zone_id, bucket_start_ts)
    zone_id,
    bucket_start_ts,
    y_pred,
    y_pred_lower,
    y_pred_upper,
    confidence_score,
    uncertainty_band,
    model_name,
    model_version,
    model_stage,
    forecast_created_at
FROM demand_forecast
WHERE bucket_start_ts >= NOW()
ORDER BY zone_id, bucket_start_ts, forecast_created_at DESC;

-- Coverage summary by horizon for the latest successful run
WITH latest_run AS (
    SELECT forecast_run_key
    FROM scoring_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    horizon_index,
    COUNT(*) AS rows,
    COUNT(DISTINCT zone_id) AS zones
FROM demand_forecast
WHERE forecast_run_key = (SELECT forecast_run_key FROM latest_run)
GROUP BY horizon_index
ORDER BY horizon_index;

-- Confidence distribution for the latest successful run
WITH latest_run AS (
    SELECT forecast_run_key
    FROM scoring_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    uncertainty_band,
    COUNT(*) AS rows,
    AVG(confidence_score) AS avg_confidence
FROM demand_forecast
WHERE forecast_run_key = (SELECT forecast_run_key FROM latest_run)
GROUP BY uncertainty_band
ORDER BY rows DESC;

