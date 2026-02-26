CREATE OR REPLACE VIEW view_api_request_counts AS
SELECT
    DATE_TRUNC('minute', created_at) AS time_bucket,
    COUNT(*) AS request_count
FROM api_request_log
GROUP BY DATE_TRUNC('minute', created_at)
ORDER BY time_bucket ASC;

CREATE OR REPLACE VIEW view_api_latency_percentiles AS
SELECT
    DATE_TRUNC('minute', created_at) AS time_bucket,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms) AS latency_p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS latency_p95_ms
FROM api_request_log
GROUP BY DATE_TRUNC('minute', created_at)
ORDER BY time_bucket ASC;

CREATE OR REPLACE VIEW view_api_error_rate AS
SELECT
    DATE_TRUNC('minute', created_at) AS time_bucket,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS client_or_server_errors,
    CASE
        WHEN COUNT(*) > 0
            THEN 100.0 * SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)::DOUBLE PRECISION / COUNT(*)
        ELSE 0.0
    END AS error_rate_percent
FROM api_request_log
GROUP BY DATE_TRUNC('minute', created_at)
ORDER BY time_bucket ASC;

CREATE OR REPLACE VIEW view_api_top_endpoints AS
SELECT
    path,
    method,
    COUNT(*) AS request_count,
    AVG(duration_ms) AS avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms
FROM api_request_log
GROUP BY path, method
ORDER BY request_count DESC;
