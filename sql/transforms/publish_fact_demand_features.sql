DELETE FROM fact_demand_features
WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

INSERT INTO fact_demand_features (
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
    feature_version,
    created_at,
    run_id,
    source_min_ts,
    source_max_ts
)
SELECT
    s.zone_id,
    s.bucket_start_ts,
    s.pickup_count,
    s.hour_of_day,
    s.quarter_hour_index,
    s.day_of_week,
    s.is_weekend,
    s.week_of_year,
    s.month,
    s.is_holiday,
    s.lag_1,
    s.lag_2,
    s.lag_4,
    s.lag_96,
    s.lag_672,
    s.roll_mean_4,
    s.roll_mean_8,
    s.roll_std_8,
    s.roll_max_16,
    CAST(:feature_version AS TEXT) AS feature_version,
    NOW() AS created_at,
    CAST(:run_id AS TEXT) AS run_id,
    CAST(:source_min_ts AS TIMESTAMPTZ) AS source_min_ts,
    CAST(:source_max_ts AS TIMESTAMPTZ) AS source_max_ts
FROM fct_zone_demand_features_stage_15m s
WHERE s.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND s.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR s.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  )
ON CONFLICT (zone_id, bucket_start_ts)
DO UPDATE SET
    pickup_count = EXCLUDED.pickup_count,
    hour_of_day = EXCLUDED.hour_of_day,
    quarter_hour_index = EXCLUDED.quarter_hour_index,
    day_of_week = EXCLUDED.day_of_week,
    is_weekend = EXCLUDED.is_weekend,
    week_of_year = EXCLUDED.week_of_year,
    month = EXCLUDED.month,
    is_holiday = EXCLUDED.is_holiday,
    lag_1 = EXCLUDED.lag_1,
    lag_2 = EXCLUDED.lag_2,
    lag_4 = EXCLUDED.lag_4,
    lag_96 = EXCLUDED.lag_96,
    lag_672 = EXCLUDED.lag_672,
    roll_mean_4 = EXCLUDED.roll_mean_4,
    roll_mean_8 = EXCLUDED.roll_mean_8,
    roll_std_8 = EXCLUDED.roll_std_8,
    roll_max_16 = EXCLUDED.roll_max_16,
    created_at = EXCLUDED.created_at,
    run_id = EXCLUDED.run_id,
    source_min_ts = EXCLUDED.source_min_ts,
    source_max_ts = EXCLUDED.source_max_ts;
