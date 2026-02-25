-- Pricing API query pack.
-- These templates mirror the service-layer SQL used by versioned pricing endpoints.

-- Latest successful pricing run id.
SELECT run_id
FROM pricing_run_log
WHERE LOWER(status) = 'success'
ORDER BY started_at DESC
LIMIT 1;

-- Latest pricing rows for a run with deterministic sort and pagination.
SELECT
    p.zone_id,
    p.bucket_start_ts,
    p.pricing_run_key,
    p.run_id,
    p.forecast_run_id,
    z.zone AS zone_name,
    z.borough,
    z.service_zone,
    p.final_multiplier,
    p.raw_multiplier,
    p.pre_cap_multiplier,
    p.post_cap_multiplier,
    p.confidence_score,
    p.uncertainty_band,
    p.y_pred,
    p.y_pred_lower,
    p.y_pred_upper,
    p.cap_applied,
    p.cap_type,
    p.cap_reason,
    p.rate_limit_applied,
    p.rate_limit_direction,
    p.smoothing_applied,
    p.primary_reason_code,
    p.reason_codes_json,
    p.reason_summary,
    p.pricing_policy_version
FROM pricing_decisions p
LEFT JOIN dim_zone z ON z.location_id = p.zone_id
WHERE p.run_id = :run_id
ORDER BY p.bucket_start_ts DESC, p.zone_id ASC
LIMIT :limit OFFSET :offset;

-- Pricing run summary.
SELECT
    run_id,
    status,
    started_at,
    ended_at,
    failure_reason,
    pricing_policy_version,
    forecast_run_id,
    target_bucket_start,
    target_bucket_end,
    zone_count,
    row_count,
    cap_applied_count,
    rate_limited_count,
    low_confidence_count,
    latency_ms
FROM pricing_run_log
WHERE run_id = :run_id;
