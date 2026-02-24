-- Pricing guardrails market evaluation query pack
--
-- Purpose:
-- 1) Evaluate forecast signal quality vs realized demand.
-- 2) Estimate pricing and guardrail lift proxies (non-causal).
-- 3) Measure customer shock via multiplier step changes.
-- 4) Evaluate fairness slices by zone class.
--
-- Notes:
-- - These are retrospective diagnostics, not causal impact estimates.
-- - Revenue lift here is a proxy index using realized demand and applied multipliers.

\echo ''
\echo '1. Latest Successful Pricing Run Context'
\echo 'Indicates run scope, policy version, window, and guardrail counters.'

-- 1) Latest successful pricing run context
WITH latest_run AS (
    SELECT
        run_id,
        pricing_run_key,
        started_at,
        pricing_policy_version,
        forecast_run_id,
        target_bucket_start,
        target_bucket_end,
        row_count,
        zone_count,
        cap_applied_count,
        rate_limited_count,
        low_confidence_count
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT *
FROM latest_run;

\echo ''
\echo '2. Data Readiness and Join Coverage'
\echo 'Shows whether pricing rows can be matched to realized demand and fallback prevalence.'

-- 2) Data readiness and join coverage (pricing rows with realized demand)
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
)
SELECT
    COUNT(*) AS pricing_rows,
    COUNT(*) FILTER (WHERE r.actual_pickup_count IS NOT NULL) AS rows_with_realized_demand,
    ROUND(
        COUNT(*) FILTER (WHERE r.actual_pickup_count IS NOT NULL)::NUMERIC
        / NULLIF(COUNT(*), 0),
        4
    ) AS realized_join_coverage,
    COUNT(*) FILTER (WHERE d.reason_codes_json ? 'MISSING_BASELINE_REFERENCE_FALLBACK') AS fallback_rows
FROM latest_decisions d
LEFT JOIN realized_demand r
  ON d.zone_id = r.zone_id
 AND d.bucket_start_ts = r.bucket_start_ts;

\echo ''
\echo '3.  Forecast Error vs Baseline Reference'
\echo 'Compares forecast error against baseline error on realized demand (lower is better).'

-- 3) Forecast error vs baseline reference (signal quality)
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
),
joined AS (
    SELECT
        d.zone_id,
        d.bucket_start_ts,
        d.y_pred,
        d.baseline_expected_demand,
        r.actual_pickup_count
    FROM latest_decisions d
    JOIN realized_demand r
      ON d.zone_id = r.zone_id
     AND d.bucket_start_ts = r.bucket_start_ts
)
SELECT
    COUNT(*) AS evaluated_rows,
    ROUND(AVG(ABS(y_pred - actual_pickup_count))::NUMERIC, 4) AS forecast_mae,
    ROUND(AVG(ABS(baseline_expected_demand - actual_pickup_count))::NUMERIC, 4) AS baseline_mae,
    ROUND(
        (
            SUM(ABS(y_pred - actual_pickup_count))
            / NULLIF(SUM(actual_pickup_count), 0)
        )::NUMERIC,
        4
    ) AS forecast_wape,
    ROUND(
        (
            SUM(ABS(baseline_expected_demand - actual_pickup_count))
            / NULLIF(SUM(actual_pickup_count), 0)
        )::NUMERIC,
        4
    ) AS baseline_wape,
    ROUND(
        (
            1 - (
                SUM(ABS(y_pred - actual_pickup_count))
                / NULLIF(SUM(ABS(baseline_expected_demand - actual_pickup_count)), 0)
            )
        )::NUMERIC,
        4
    ) AS forecast_error_improvement_vs_baseline
FROM joined;

\echo ''
\echo '4. Lift Proxies and Guardrail Impact (Non-Causal)'
\echo 'Compares final/post-cap/raw/no-surge proxy indices and guardrail reduction vs raw.'

-- 4) Lift proxies (non-causal) and guardrail impact decomposition
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
),
joined AS (
    SELECT
        d.zone_id,
        d.bucket_start_ts,
        d.raw_multiplier,
        d.post_cap_multiplier,
        d.final_multiplier,
        COALESCE(r.actual_pickup_count, 0.0) AS actual_pickup_count
    FROM latest_decisions d
    LEFT JOIN realized_demand r
      ON d.zone_id = r.zone_id
     AND d.bucket_start_ts = r.bucket_start_ts
)
SELECT
    ROUND(SUM(actual_pickup_count * final_multiplier)::NUMERIC, 4) AS final_revenue_index,
    ROUND(SUM(actual_pickup_count * post_cap_multiplier)::NUMERIC, 4) AS post_cap_revenue_index,
    ROUND(SUM(actual_pickup_count * raw_multiplier)::NUMERIC, 4) AS raw_revenue_index,
    ROUND(SUM(actual_pickup_count * 1.0)::NUMERIC, 4) AS no_surge_revenue_index,
    ROUND(
        (
            (
                SUM(actual_pickup_count * final_multiplier)
                - SUM(actual_pickup_count * 1.0)
            ) / NULLIF(SUM(actual_pickup_count * 1.0), 0)
        )::NUMERIC,
        4
    ) AS final_lift_pct_vs_no_surge,
    ROUND(
        (
            (
                SUM(actual_pickup_count * raw_multiplier)
                - SUM(actual_pickup_count * final_multiplier)
            ) / NULLIF(SUM(actual_pickup_count * raw_multiplier), 0)
        )::NUMERIC,
        4
    ) AS guardrail_reduction_pct_vs_raw
FROM joined;

\echo ''
\echo '5. Customer Shock Metrics'
\echo 'Reports step-change deltas in multipliers and demand-weighted shock intensity.'

-- 5) Customer shock metrics (step-change distribution)
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
),
shock_frame AS (
    SELECT
        d.zone_id,
        d.bucket_start_ts,
        d.previous_final_multiplier,
        d.final_multiplier,
        ABS(d.final_multiplier - d.previous_final_multiplier) AS absolute_delta,
        CASE
            WHEN d.previous_final_multiplier IS NULL OR d.previous_final_multiplier = 0 THEN NULL
            ELSE ABS((d.final_multiplier - d.previous_final_multiplier) / d.previous_final_multiplier)
        END AS pct_delta,
        d.rate_limit_applied,
        COALESCE(r.actual_pickup_count, 0.0) AS actual_pickup_count
    FROM latest_decisions d
    LEFT JOIN realized_demand r
      ON d.zone_id = r.zone_id
     AND d.bucket_start_ts = r.bucket_start_ts
)
SELECT
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY absolute_delta)::NUMERIC, 4) AS p50_abs_delta,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY absolute_delta)::NUMERIC, 4) AS p90_abs_delta,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY absolute_delta)::NUMERIC, 4) AS p99_abs_delta,
    ROUND(AVG(absolute_delta)::NUMERIC, 4) AS avg_abs_delta,
    ROUND(AVG(pct_delta)::NUMERIC, 4) AS avg_pct_delta,
    ROUND(
        (
            SUM(absolute_delta * actual_pickup_count)
            / NULLIF(SUM(actual_pickup_count), 0)
        )::NUMERIC,
        4
    ) AS demand_weighted_abs_delta,
    COUNT(*) FILTER (WHERE rate_limit_applied) AS rate_limited_rows,
    COUNT(*) AS total_rows
FROM shock_frame;

\echo ''
\echo '6. Fairness and Safety Slices by Zone Class'
\echo 'Compares multiplier levels and guardrail rates across zone classes.'

-- 6) Fairness and safety slices by zone class
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
zone_class_map AS (
    SELECT DISTINCT ON (zone_id)
        zone_id,
        sparsity_class AS zone_class
    FROM zone_fallback_policy
    ORDER BY zone_id, effective_from DESC
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
),
joined AS (
    SELECT
        COALESCE(z.zone_class, 'unknown') AS zone_class,
        d.final_multiplier,
        d.cap_applied,
        d.rate_limit_applied,
        d.reason_codes_json,
        COALESCE(r.actual_pickup_count, 0.0) AS actual_pickup_count
    FROM latest_decisions d
    LEFT JOIN zone_class_map z
      ON d.zone_id = z.zone_id
    LEFT JOIN realized_demand r
      ON d.zone_id = r.zone_id
     AND d.bucket_start_ts = r.bucket_start_ts
)
SELECT
    zone_class,
    COUNT(*) AS rows,
    ROUND(AVG(final_multiplier)::NUMERIC, 4) AS avg_final_multiplier,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY final_multiplier)::NUMERIC, 4) AS p90_final_multiplier,
    ROUND(AVG(CASE WHEN cap_applied THEN 1 ELSE 0 END)::NUMERIC, 4) AS cap_applied_rate,
    ROUND(AVG(CASE WHEN rate_limit_applied THEN 1 ELSE 0 END)::NUMERIC, 4) AS rate_limited_rate,
    ROUND(AVG(CASE WHEN reason_codes_json ? 'MISSING_BASELINE_REFERENCE_FALLBACK' THEN 1 ELSE 0 END)::NUMERIC, 4) AS missing_baseline_rate,
    ROUND(AVG(CASE WHEN reason_codes_json ? 'LOW_CONFIDENCE_DAMPENING' THEN 1 ELSE 0 END)::NUMERIC, 4) AS low_confidence_dampening_rate,
    ROUND(
        (
            SUM(final_multiplier * actual_pickup_count)
            / NULLIF(SUM(actual_pickup_count), 0)
        )::NUMERIC,
        4
    ) AS demand_weighted_final_multiplier
FROM joined
GROUP BY zone_class
ORDER BY zone_class;

\echo ''
\echo '7. Top Shock Zones'
\echo 'Lists zones with highest demand-weighted multiplier movement.'

-- 7) Top shock zones by demand-weighted absolute multiplier delta
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
),
shock_frame AS (
    SELECT
        d.zone_id,
        ABS(d.final_multiplier - d.previous_final_multiplier) AS absolute_delta,
        COALESCE(r.actual_pickup_count, 0.0) AS actual_pickup_count
    FROM latest_decisions d
    LEFT JOIN realized_demand r
      ON d.zone_id = r.zone_id
     AND d.bucket_start_ts = r.bucket_start_ts
)
SELECT
    zone_id,
    ROUND(AVG(absolute_delta)::NUMERIC, 4) AS avg_abs_delta,
    ROUND(SUM(absolute_delta * actual_pickup_count)::NUMERIC, 4) AS demand_weighted_delta_sum,
    ROUND(AVG(actual_pickup_count)::NUMERIC, 4) AS avg_actual_pickups,
    COUNT(*) AS rows
FROM shock_frame
GROUP BY zone_id
ORDER BY demand_weighted_delta_sum DESC, avg_abs_delta DESC
LIMIT 20;

\echo ''
\echo '8. Revenue Proxy Trend by Bucket'
\echo 'Bucket-level view of actual pickups, multiplier intensity, and guardrail rates.'

-- 8) Revenue proxy by bucket (trend view)
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_demand AS (
    SELECT
        zone_id,
        bucket_start_ts,
        pickup_count::DOUBLE PRECISION AS actual_pickup_count
    FROM fact_demand_features
)
SELECT
    d.bucket_start_ts,
    ROUND(SUM(COALESCE(r.actual_pickup_count, 0.0))::NUMERIC, 2) AS actual_pickups,
    ROUND(SUM(COALESCE(r.actual_pickup_count, 0.0) * d.final_multiplier)::NUMERIC, 2) AS final_revenue_index,
    ROUND(SUM(COALESCE(r.actual_pickup_count, 0.0) * 1.0)::NUMERIC, 2) AS no_surge_revenue_index,
    ROUND(AVG(d.final_multiplier)::NUMERIC, 4) AS avg_final_multiplier,
    ROUND(AVG(CASE WHEN d.rate_limit_applied THEN 1 ELSE 0 END)::NUMERIC, 4) AS rate_limited_rate,
    ROUND(AVG(CASE WHEN d.cap_applied THEN 1 ELSE 0 END)::NUMERIC, 4) AS capped_rate
FROM latest_decisions d
LEFT JOIN realized_demand r
  ON d.zone_id = r.zone_id
 AND d.bucket_start_ts = r.bucket_start_ts
GROUP BY d.bucket_start_ts
ORDER BY d.bucket_start_ts;

\echo ''
\echo '9. Realized Fare Exposure (Optional)'
\echo 'Sanity check using raw trip totals and multiplier-weighted amount proxy.'

-- 9) Realized fare amount exposure (optional check from raw_trips)
-- This helps inspect whether high-multiplier buckets align with higher historical fare pools.
WITH latest_run AS (
    SELECT pricing_run_key
    FROM pricing_run_log
    WHERE status = 'succeeded'
    ORDER BY started_at DESC
    LIMIT 1
),
latest_decisions AS (
    SELECT *
    FROM pricing_decisions
    WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
),
realized_fares AS (
    SELECT
        pickup_location_id AS zone_id,
        (
            date_trunc('hour', pickup_datetime)
            + (FLOOR(EXTRACT(MINUTE FROM pickup_datetime) / 15.0)::INT * INTERVAL '15 minutes')
        ) AS bucket_start_ts,
        COUNT(*)::DOUBLE PRECISION AS trip_count,
        SUM(COALESCE(total_amount, 0.0))::DOUBLE PRECISION AS total_amount
    FROM raw_trips
    GROUP BY 1, 2
)
SELECT
    d.bucket_start_ts,
    ROUND(SUM(COALESCE(f.trip_count, 0.0))::NUMERIC, 2) AS realized_trip_count,
    ROUND(SUM(COALESCE(f.total_amount, 0.0))::NUMERIC, 2) AS realized_total_amount,
    ROUND(SUM(COALESCE(f.total_amount, 0.0) * d.final_multiplier)::NUMERIC, 2) AS multiplier_weighted_amount_proxy
FROM latest_decisions d
LEFT JOIN realized_fares f
  ON d.zone_id = f.zone_id
 AND d.bucket_start_ts = f.bucket_start_ts
GROUP BY d.bucket_start_ts
ORDER BY d.bucket_start_ts;
