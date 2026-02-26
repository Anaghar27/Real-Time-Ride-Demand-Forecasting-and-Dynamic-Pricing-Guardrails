CREATE OR REPLACE VIEW view_scoring_coverage_by_run AS
SELECT
    run_id,
    started_at,
    status,
    zone_count,
    horizon_buckets,
    row_count,
    CASE
        WHEN COALESCE(zone_count, 0) > 0 AND COALESCE(horizon_buckets, 0) > 0
            THEN 100.0 * row_count::DOUBLE PRECISION / NULLIF(zone_count * horizon_buckets, 0)
        ELSE NULL
    END AS coverage_percent
FROM scoring_run_log
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW view_scoring_confidence_band_over_time AS
SELECT
    DATE_TRUNC('hour', bucket_start_ts) AS time_bucket,
    uncertainty_band,
    COUNT(*) AS row_count,
    AVG(confidence_score) AS avg_confidence_score
FROM demand_forecast
GROUP BY DATE_TRUNC('hour', bucket_start_ts), uncertainty_band
ORDER BY time_bucket ASC;

CREATE OR REPLACE VIEW view_scoring_interval_width_percentiles_over_time AS
SELECT
    DATE_TRUNC('hour', bucket_start_ts) AS time_bucket,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (y_pred_upper - y_pred_lower)) AS p50_interval_width,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (y_pred_upper - y_pred_lower)) AS p90_interval_width,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY (y_pred_upper - y_pred_lower)) AS p99_interval_width
FROM demand_forecast
GROUP BY DATE_TRUNC('hour', bucket_start_ts)
ORDER BY time_bucket ASC;

CREATE OR REPLACE VIEW view_scoring_latency_over_time AS
SELECT
    started_at,
    latency_ms,
    status
FROM scoring_run_log
ORDER BY started_at ASC;

CREATE OR REPLACE VIEW view_scoring_expected_vs_actual_rows AS
SELECT
    run_id,
    started_at,
    zone_count,
    horizon_buckets,
    row_count AS actual_row_count,
    (COALESCE(zone_count, 0) * COALESCE(horizon_buckets, 0)) AS expected_row_count,
    row_count - (COALESCE(zone_count, 0) * COALESCE(horizon_buckets, 0)) AS row_count_delta
FROM scoring_run_log
ORDER BY started_at DESC;
