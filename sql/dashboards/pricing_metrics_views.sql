CREATE OR REPLACE VIEW view_pricing_cap_rate_by_run AS
SELECT
    run_id,
    started_at,
    status,
    row_count,
    cap_applied_count,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN 100.0 * cap_applied_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS cap_applied_percent
FROM pricing_run_log
ORDER BY started_at ASC;

CREATE OR REPLACE VIEW view_pricing_rate_limit_rate_by_run AS
SELECT
    run_id,
    started_at,
    status,
    row_count,
    rate_limited_count,
    CASE
        WHEN COALESCE(row_count, 0) > 0
            THEN 100.0 * rate_limited_count::DOUBLE PRECISION / NULLIF(row_count, 0)
        ELSE 0.0
    END AS rate_limited_percent
FROM pricing_run_log
ORDER BY started_at ASC;

CREATE OR REPLACE VIEW view_pricing_multiplier_distribution AS
SELECT
    DATE_TRUNC('hour', bucket_start_ts) AS time_bucket,
    WIDTH_BUCKET(final_multiplier, 0.50, 2.50, 20) AS multiplier_bucket,
    COUNT(*) AS row_count
FROM pricing_decisions
GROUP BY DATE_TRUNC('hour', bucket_start_ts), WIDTH_BUCKET(final_multiplier, 0.50, 2.50, 20)
ORDER BY time_bucket ASC, multiplier_bucket ASC;

CREATE OR REPLACE VIEW view_pricing_reason_code_top AS
SELECT
    primary_reason_code,
    COUNT(*) AS row_count
FROM pricing_decisions
GROUP BY primary_reason_code
ORDER BY row_count DESC;

CREATE OR REPLACE VIEW view_pricing_average_multiplier_by_borough AS
SELECT
    DATE_TRUNC('hour', p.bucket_start_ts) AS time_bucket,
    z.borough,
    AVG(p.final_multiplier) AS avg_final_multiplier
FROM pricing_decisions p
LEFT JOIN dim_zone z
    ON z.location_id = p.zone_id
GROUP BY DATE_TRUNC('hour', p.bucket_start_ts), z.borough
ORDER BY time_bucket ASC, z.borough ASC;
