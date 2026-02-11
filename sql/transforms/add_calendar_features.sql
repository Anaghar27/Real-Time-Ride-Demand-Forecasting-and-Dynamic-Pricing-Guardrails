CREATE TABLE IF NOT EXISTS dim_holiday (
    holiday_date DATE PRIMARY KEY,
    holiday_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    city_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_zone_demand_calendar_15m (
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    pickup_count INTEGER NOT NULL,
    avg_fare_amount DOUBLE PRECISION,
    avg_trip_distance DOUBLE PRECISION,
    hour_of_day SMALLINT NOT NULL,
    quarter_hour_index SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    sin_hour DOUBLE PRECISION,
    cos_hour DOUBLE PRECISION,
    sin_dow DOUBLE PRECISION,
    cos_dow DOUBLE PRECISION,
    run_id TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zone_id, bucket_start_ts)
);

DELETE FROM fct_zone_demand_calendar_15m
WHERE bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

INSERT INTO fct_zone_demand_calendar_15m (
    zone_id,
    bucket_start_ts,
    pickup_count,
    avg_fare_amount,
    avg_trip_distance,
    hour_of_day,
    quarter_hour_index,
    day_of_week,
    is_weekend,
    week_of_year,
    month,
    is_holiday,
    sin_hour,
    cos_hour,
    sin_dow,
    cos_dow,
    run_id,
    feature_version,
    created_at
)
SELECT
    d.zone_id,
    d.bucket_start_ts,
    d.pickup_count,
    d.avg_fare_amount,
    d.avg_trip_distance,
    EXTRACT(HOUR FROM timezone(:feature_tz, d.bucket_start_ts))::SMALLINT AS hour_of_day,
    (
        EXTRACT(HOUR FROM timezone(:feature_tz, d.bucket_start_ts))::INT * 4
        + FLOOR(EXTRACT(MINUTE FROM timezone(:feature_tz, d.bucket_start_ts)) / 15)::INT
    )::SMALLINT AS quarter_hour_index,
    EXTRACT(ISODOW FROM timezone(:feature_tz, d.bucket_start_ts))::SMALLINT AS day_of_week,
    (EXTRACT(ISODOW FROM timezone(:feature_tz, d.bucket_start_ts))::INT IN (6, 7)) AS is_weekend,
    EXTRACT(WEEK FROM timezone(:feature_tz, d.bucket_start_ts))::SMALLINT AS week_of_year,
    EXTRACT(MONTH FROM timezone(:feature_tz, d.bucket_start_ts))::SMALLINT AS month,
    (h.holiday_date IS NOT NULL) AS is_holiday,
    SIN(2 * PI() * EXTRACT(HOUR FROM timezone(:feature_tz, d.bucket_start_ts)) / 24.0) AS sin_hour,
    COS(2 * PI() * EXTRACT(HOUR FROM timezone(:feature_tz, d.bucket_start_ts)) / 24.0) AS cos_hour,
    SIN(2 * PI() * EXTRACT(ISODOW FROM timezone(:feature_tz, d.bucket_start_ts)) / 7.0) AS sin_dow,
    COS(2 * PI() * EXTRACT(ISODOW FROM timezone(:feature_tz, d.bucket_start_ts)) / 7.0) AS cos_dow,
    CAST(:run_id AS TEXT) AS run_id,
    CAST(:feature_version AS TEXT) AS feature_version,
    NOW() AS created_at
FROM fct_zone_demand_15m d
LEFT JOIN dim_holiday h
    ON h.holiday_date = CAST(timezone(:feature_tz, d.bucket_start_ts) AS DATE)
WHERE d.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND d.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR d.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );
