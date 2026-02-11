CREATE TABLE IF NOT EXISTS fct_zone_demand_15m (
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    bucket_end_ts TIMESTAMPTZ NOT NULL,
    pickup_count INTEGER NOT NULL,
    avg_fare_amount DOUBLE PRECISION,
    avg_trip_distance DOUBLE PRECISION,
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zone_id, bucket_start_ts)
);

DELETE FROM fct_zone_demand_15m
WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

WITH pickups_agg AS (
    SELECT
        rt.pickup_location_id AS zone_id,
        (
            (
                date_trunc('hour', timezone(:feature_tz, rt.pickup_datetime))
                + make_interval(
                    mins => (FLOOR(EXTRACT(MINUTE FROM timezone(:feature_tz, rt.pickup_datetime)) / 15)::INT * 15)
                )
            ) AT TIME ZONE :feature_tz
        ) AS bucket_start_ts,
        COUNT(*)::INTEGER AS pickup_count,
        AVG(rt.fare_amount)::DOUBLE PRECISION AS avg_fare_amount,
        AVG(rt.trip_distance)::DOUBLE PRECISION AS avg_trip_distance
    FROM raw_trips rt
    WHERE rt.pickup_datetime >= CAST(:run_start_ts AS TIMESTAMPTZ)
      AND rt.pickup_datetime < CAST(:run_end_ts AS TIMESTAMPTZ)
      AND rt.pickup_location_id IS NOT NULL
      AND (
          :zone_ids IS NULL
          OR rt.pickup_location_id = ANY(CAST(:zone_ids AS INTEGER[]))
      )
    GROUP BY rt.pickup_location_id, 2
)
INSERT INTO fct_zone_demand_15m (
    zone_id,
    bucket_start_ts,
    bucket_end_ts,
    pickup_count,
    avg_fare_amount,
    avg_trip_distance,
    run_id,
    feature_version,
    created_at
)
SELECT
    zts.zone_id,
    zts.bucket_start_ts,
    zts.bucket_end_ts,
    COALESCE(pa.pickup_count, 0) AS pickup_count,
    pa.avg_fare_amount,
    pa.avg_trip_distance,
    CAST(:run_id AS TEXT) AS run_id,
    CAST(:feature_version AS TEXT) AS feature_version,
    NOW() AS created_at
FROM fct_zone_time_spine_15m zts
LEFT JOIN pickups_agg pa
    ON pa.zone_id = zts.zone_id
   AND pa.bucket_start_ts = zts.bucket_start_ts
WHERE zts.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND zts.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zts.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

CREATE INDEX IF NOT EXISTS idx_fct_zone_demand_15m_zone_bucket
    ON fct_zone_demand_15m (zone_id, bucket_start_ts);
