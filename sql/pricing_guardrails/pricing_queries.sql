-- Latest pricing run summaries
SELECT *
FROM pricing_run_log
ORDER BY started_at DESC
LIMIT 20;

-- Latest pricing decision per zone and bucket
SELECT DISTINCT ON (zone_id, bucket_start_ts)
    zone_id,
    bucket_start_ts,
    final_multiplier,
    cap_applied,
    rate_limit_applied,
    primary_reason_code,
    pricing_created_at
FROM pricing_decisions
ORDER BY zone_id, bucket_start_ts, pricing_created_at DESC;

-- Zones with cap applied in latest successful run
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT zone_id, bucket_start_ts, pre_cap_multiplier, post_cap_multiplier, cap_type, cap_value, cap_reason
FROM pricing_decisions
WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
  AND cap_applied = TRUE
ORDER BY zone_id, bucket_start_ts;

-- Zones with rate limit applied in latest successful run
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT zone_id, bucket_start_ts, previous_final_multiplier, candidate_multiplier_before_rate_limit, final_multiplier, rate_limit_direction
FROM pricing_decisions
WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
  AND rate_limit_applied = TRUE
ORDER BY zone_id, bucket_start_ts;

-- Reason code distribution for latest successful run
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
exploded AS (
    SELECT jsonb_array_elements_text(reason_codes_json) AS reason_code
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
)
SELECT reason_code, COUNT(*) AS row_count
FROM exploded
GROUP BY reason_code
ORDER BY row_count DESC;

-- Final multiplier trend for a selected zone (replace :zone_id)
SELECT
    zone_id,
    bucket_start_ts,
    final_multiplier,
    cap_applied,
    rate_limit_applied,
    primary_reason_code,
    pricing_created_at
FROM pricing_decisions
WHERE zone_id = :zone_id
ORDER BY bucket_start_ts, pricing_created_at DESC;
