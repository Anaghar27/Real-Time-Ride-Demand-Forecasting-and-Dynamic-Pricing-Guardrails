WITH scoped AS (
    SELECT *
    FROM fct_zone_demand_features_stage_15m
    WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
      AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
      AND (
          :zone_ids IS NULL
          OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
      )
),
expected AS (
    SELECT COUNT(*)::BIGINT AS expected_rows
    FROM fct_zone_time_spine_15m
    WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
      AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
      AND (
          :zone_ids IS NULL
          OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
      )
),
dupes AS (
    SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT AS duplicate_rows
    FROM (
        SELECT zone_id, bucket_start_ts, COUNT(*) AS cnt
        FROM scoped
        GROUP BY zone_id, bucket_start_ts
        HAVING COUNT(*) > 1
    ) d
),
nulls AS (
    SELECT
        SUM(CASE WHEN pickup_count IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_pickup_count,
        SUM(CASE WHEN hour_of_day IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_hour_of_day,
        SUM(CASE WHEN day_of_week IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_day_of_week,
        SUM(CASE WHEN lag_1 IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_lag_1,
        COUNT(*)::BIGINT AS total_rows
    FROM scoped
),
freshness AS (
    SELECT
        (
            SELECT MAX(rt.pickup_datetime)
            FROM raw_trips rt
            WHERE rt.pickup_datetime < CAST(:run_end_ts AS TIMESTAMPTZ)
              AND (
                  :zone_ids IS NULL
                  OR rt.pickup_location_id = ANY(CAST(:zone_ids AS INTEGER[]))
              )
        ) AS latest_raw_pickup_ts,
        (
            SELECT MAX(sc.bucket_start_ts)
            FROM scoped sc
        ) AS latest_feature_bucket_ts
),
distribution AS (
    SELECT
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pickup_count) AS pickup_p50,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY pickup_count) AS pickup_p95,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY pickup_count) AS pickup_p99
    FROM scoped
),
assembled AS (
    SELECT
        'duplicate_key'::TEXT AS check_name,
        'critical'::TEXT AS severity,
        (duplicate_rows = 0) AS passed,
        duplicate_rows::DOUBLE PRECISION AS metric_value,
        0.0::DOUBLE PRECISION AS threshold_value,
        jsonb_build_object('duplicate_rows', duplicate_rows) AS details
    FROM dupes

    UNION ALL

    SELECT
        'row_count_expectation',
        'critical',
        ((SELECT COUNT(*) FROM scoped) = expected.expected_rows) AS passed,
        (SELECT COUNT(*) FROM scoped)::DOUBLE PRECISION,
        expected.expected_rows::DOUBLE PRECISION,
        jsonb_build_object(
            'actual_rows', (SELECT COUNT(*) FROM scoped),
            'expected_rows', expected.expected_rows
        )
    FROM expected

    UNION ALL

    SELECT
        'null_threshold_core',
        'critical',
        (
            null_pickup_count = 0
            AND null_hour_of_day = 0
            AND null_day_of_week = 0
            AND (
                :lag_null_policy = 'keep_nulls'
                OR null_lag_1 = 0
            )
        ) AS passed,
        (null_pickup_count + null_hour_of_day + null_day_of_week + null_lag_1)::DOUBLE PRECISION,
        0.0::DOUBLE PRECISION,
        jsonb_build_object(
            'null_pickup_count', null_pickup_count,
            'null_hour_of_day', null_hour_of_day,
            'null_day_of_week', null_day_of_week,
            'null_lag_1', null_lag_1,
            'total_rows', total_rows
        )
    FROM nulls

    UNION ALL

    SELECT
        'freshness_vs_raw',
        'warning',
        (fresh.latest_raw_pickup_ts IS NULL OR fresh.latest_feature_bucket_ts >= fresh.latest_raw_pickup_ts - INTERVAL '1 hour') AS passed,
        EXTRACT(EPOCH FROM (fresh.latest_raw_pickup_ts - fresh.latest_feature_bucket_ts)) / 60.0,
        60.0,
        jsonb_build_object(
            'latest_raw_pickup_ts', fresh.latest_raw_pickup_ts,
            'latest_feature_bucket_ts', fresh.latest_feature_bucket_ts
        )
    FROM freshness fresh

    UNION ALL

    SELECT
        'pickup_distribution_sanity',
        'warning',
        (pickup_p95 >= pickup_p50 AND pickup_p99 >= pickup_p95) AS passed,
        pickup_p99,
        pickup_p95,
        jsonb_build_object(
            'pickup_p50', pickup_p50,
            'pickup_p95', pickup_p95,
            'pickup_p99', pickup_p99
        )
    FROM distribution
)
INSERT INTO feature_check_results (
    run_id,
    check_name,
    severity,
    passed,
    metric_value,
    threshold_value,
    details,
    reason_code,
    created_at
)
SELECT
    CAST(:run_id AS TEXT),
    a.check_name,
    a.severity,
    a.passed,
    a.metric_value,
    a.threshold_value,
    a.details,
    CASE WHEN a.passed THEN 'ok' ELSE CONCAT('failed_', a.check_name) END AS reason_code,
    NOW()
FROM assembled a;
