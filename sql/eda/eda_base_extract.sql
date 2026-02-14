SELECT
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
    feature_version
FROM fact_demand_features
WHERE bucket_start_ts >= CAST(:data_start_ts AS TIMESTAMPTZ)
  AND bucket_start_ts < CAST(:data_end_ts AS TIMESTAMPTZ)
  AND feature_version = CAST(:feature_version AS TEXT)
  AND (
      :zone_ids IS NULL
      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  )
ORDER BY zone_id, bucket_start_ts;
