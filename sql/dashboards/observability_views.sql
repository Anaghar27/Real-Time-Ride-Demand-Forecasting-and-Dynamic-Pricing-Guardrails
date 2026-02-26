CREATE OR REPLACE VIEW view_latest_scoring_run AS
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
    zone_count,
    row_count,
    latency_ms,
    EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at)) / 60.0 AS duration_minutes,
    EXTRACT(EPOCH FROM (NOW() - COALESCE(ended_at, started_at))) / 60.0 AS minutes_since_last_update,
    CASE
        WHEN COALESCE(zone_count, 0) > 0 AND COALESCE(horizon_buckets, 0) > 0
            THEN row_count::DOUBLE PRECISION / NULLIF(zone_count * horizon_buckets, 0)
        ELSE NULL
    END AS coverage_ratio
FROM scoring_run_log
ORDER BY started_at DESC
LIMIT 1;

CREATE OR REPLACE VIEW view_latest_pricing_run AS
SELECT
    run_id,
    status,
    started_at,
    ended_at,
    failure_reason,
    pricing_policy_version,
    forecast_run_id,
    zone_count,
    row_count,
    cap_applied_count,
    rate_limited_count,
    low_confidence_count,
    latency_ms,
    EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at)) / 60.0 AS duration_minutes,
    EXTRACT(EPOCH FROM (NOW() - COALESCE(ended_at, started_at))) / 60.0 AS minutes_since_last_update,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN cap_applied_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS cap_applied_rate,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN rate_limited_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS rate_limited_rate
FROM pricing_run_log
ORDER BY started_at DESC
LIMIT 1;

CREATE OR REPLACE VIEW view_scoring_run_trends AS
SELECT
    run_id,
    started_at,
    ended_at,
    status,
    zone_count,
    row_count,
    latency_ms,
    CASE
        WHEN COALESCE(zone_count, 0) > 0 AND COALESCE(horizon_buckets, 0) > 0
            THEN row_count::DOUBLE PRECISION / NULLIF(zone_count * horizon_buckets, 0)
        ELSE NULL
    END AS coverage_ratio
FROM scoring_run_log
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW view_pricing_guardrail_trends AS
SELECT
    run_id,
    started_at,
    ended_at,
    status,
    row_count,
    cap_applied_count,
    rate_limited_count,
    low_confidence_count,
    latency_ms,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN cap_applied_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS cap_applied_rate,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN rate_limited_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS rate_limited_rate
FROM pricing_run_log
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW view_reason_code_counts AS
SELECT
    p.run_id,
    p.primary_reason_code,
    COALESCE(r.description, 'Description unavailable') AS reason_description,
    COUNT(*) AS row_count
FROM pricing_decisions p
LEFT JOIN reason_code_reference r
    ON r.reason_code = p.primary_reason_code
GROUP BY p.run_id, p.primary_reason_code, COALESCE(r.description, 'Description unavailable');

CREATE OR REPLACE VIEW view_forecast_confidence_distribution AS
SELECT
    DATE_TRUNC('hour', bucket_start_ts) AS time_bucket,
    run_id,
    uncertainty_band,
    COUNT(*) AS row_count,
    AVG(confidence_score) AS avg_confidence_score
FROM demand_forecast
GROUP BY DATE_TRUNC('hour', bucket_start_ts), run_id, uncertainty_band;
