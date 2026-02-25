-- Diagnostics API query pack.
-- These templates mirror the service-layer SQL used by diagnostics endpoints.

-- Coverage summary for latest pricing run.
SELECT
    COUNT(DISTINCT zone_id) AS pricing_zone_count,
    COUNT(*) AS pricing_row_count
FROM pricing_decisions
WHERE run_id = :pricing_run_id;

-- Coverage summary for latest forecast run.
SELECT
    COUNT(DISTINCT zone_id) AS forecast_zone_count,
    COUNT(*) AS forecast_row_count
FROM demand_forecast
WHERE run_id = :forecast_run_id;

-- Guardrail usage summary.
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN cap_applied THEN 1 ELSE 0 END) AS cap_applied_rows,
    SUM(CASE WHEN rate_limit_applied THEN 1 ELSE 0 END) AS rate_limited_rows,
    SUM(CASE WHEN smoothing_applied THEN 1 ELSE 0 END) AS smoothing_applied_rows
FROM pricing_decisions
WHERE run_id = :pricing_run_id;

-- Confidence distribution summary.
SELECT
    uncertainty_band,
    COUNT(*) AS row_count,
    AVG(confidence_score) AS avg_confidence_score
FROM demand_forecast
WHERE run_id = :forecast_run_id
GROUP BY uncertainty_band
ORDER BY uncertainty_band ASC;
