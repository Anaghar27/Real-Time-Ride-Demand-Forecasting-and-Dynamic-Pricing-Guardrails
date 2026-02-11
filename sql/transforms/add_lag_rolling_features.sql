CREATE TABLE IF NOT EXISTS fct_zone_demand_features_stage_15m (
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    pickup_count INTEGER NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    quarter_hour_index SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    lag_1 DOUBLE PRECISION,
    lag_2 DOUBLE PRECISION,
    lag_4 DOUBLE PRECISION,
    lag_96 DOUBLE PRECISION,
    lag_672 DOUBLE PRECISION,
    roll_mean_4 DOUBLE PRECISION,
    roll_mean_8 DOUBLE PRECISION,
    roll_std_8 DOUBLE PRECISION,
    roll_max_16 DOUBLE PRECISION,
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zone_id, bucket_start_ts)
);

DELETE FROM fct_zone_demand_features_stage_15m
WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

WITH demand_w_history AS (
    SELECT
        c.zone_id,
        c.bucket_start_ts,
        c.pickup_count,
        c.hour_of_day,
        c.quarter_hour_index,
        c.day_of_week,
        c.is_weekend,
        c.week_of_year,
        c.month,
        c.is_holiday,
        LAG(c.pickup_count, 1) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
        )::DOUBLE PRECISION AS lag_1,
        LAG(c.pickup_count, 2) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
        )::DOUBLE PRECISION AS lag_2,
        LAG(c.pickup_count, 4) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
        )::DOUBLE PRECISION AS lag_4,
        LAG(c.pickup_count, 96) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
        )::DOUBLE PRECISION AS lag_96,
        LAG(c.pickup_count, 672) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
        )::DOUBLE PRECISION AS lag_672,
        AVG(c.pickup_count) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        )::DOUBLE PRECISION AS roll_mean_4,
        AVG(c.pickup_count) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
            ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING
        )::DOUBLE PRECISION AS roll_mean_8,
        STDDEV_SAMP(c.pickup_count) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
            ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING
        )::DOUBLE PRECISION AS roll_std_8,
        MAX(c.pickup_count) OVER (
            PARTITION BY c.zone_id
            ORDER BY c.bucket_start_ts ASC
            ROWS BETWEEN 16 PRECEDING AND 1 PRECEDING
        )::DOUBLE PRECISION AS roll_max_16
    FROM fct_zone_demand_calendar_15m c
    WHERE c.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
      AND c.bucket_start_ts >= CAST(:history_start_ts AS TIMESTAMPTZ)
      AND (
          :zone_ids IS NULL
          OR c.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
      )
)
INSERT INTO fct_zone_demand_features_stage_15m (
    zone_id,
    bucket_start_ts,
    pickup_count,
    hour_of_day,
    quarter_hour_index,
    day_of_week,
    is_weekend,
    week_of_year,
    month,
    is_holiday,
    lag_1,
    lag_2,
    lag_4,
    lag_96,
    lag_672,
    roll_mean_4,
    roll_mean_8,
    roll_std_8,
    roll_max_16,
    run_id,
    feature_version,
    created_at
)
SELECT
    d.zone_id,
    d.bucket_start_ts,
    d.pickup_count,
    d.hour_of_day,
    d.quarter_hour_index,
    d.day_of_week,
    d.is_weekend,
    d.week_of_year,
    d.month,
    d.is_holiday,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.lag_1, 0.0) ELSE d.lag_1 END AS lag_1,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.lag_2, 0.0) ELSE d.lag_2 END AS lag_2,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.lag_4, 0.0) ELSE d.lag_4 END AS lag_4,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.lag_96, 0.0) ELSE d.lag_96 END AS lag_96,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.lag_672, 0.0) ELSE d.lag_672 END AS lag_672,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.roll_mean_4, 0.0) ELSE d.roll_mean_4 END AS roll_mean_4,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.roll_mean_8, 0.0) ELSE d.roll_mean_8 END AS roll_mean_8,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.roll_std_8, 0.0) ELSE d.roll_std_8 END AS roll_std_8,
    CASE WHEN :lag_null_policy = 'zero' THEN COALESCE(d.roll_max_16, 0.0) ELSE d.roll_max_16 END AS roll_max_16,
    CAST(:run_id AS TEXT) AS run_id,
    CAST(:feature_version AS TEXT) AS feature_version,
    NOW() AS created_at
FROM demand_w_history d
WHERE d.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND d.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR d.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );
