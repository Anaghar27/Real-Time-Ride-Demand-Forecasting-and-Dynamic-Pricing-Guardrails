WITH latest_policy AS (
    SELECT DISTINCT ON (zone_id)
        zone_id,
        sparsity_class,
        policy_version,
        effective_from
    FROM zone_fallback_policy
    WHERE policy_version = :policy_version
    ORDER BY zone_id, effective_from DESC
)
SELECT
    f.zone_id,
    f.bucket_start_ts,
    f.pickup_count,
    f.hour_of_day,
    f.quarter_hour_index,
    f.day_of_week,
    f.is_weekend,
    f.week_of_year,
    f.month,
    f.is_holiday,
    f.lag_1,
    f.lag_2,
    f.lag_4,
    f.lag_96,
    f.lag_672,
    f.roll_mean_4,
    f.roll_mean_8,
    f.roll_std_8,
    f.roll_max_16,
    f.feature_version,
    COALESCE(p.sparsity_class, 'unknown') AS sparsity_class
FROM fact_demand_features f
LEFT JOIN latest_policy p
  ON f.zone_id = p.zone_id
WHERE f.bucket_start_ts >= :start_ts
  AND f.bucket_start_ts < :end_ts
  AND f.feature_version = :feature_version
  AND (
      :zone_ids IS NULL
      OR f.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  )
ORDER BY f.bucket_start_ts, f.zone_id;
