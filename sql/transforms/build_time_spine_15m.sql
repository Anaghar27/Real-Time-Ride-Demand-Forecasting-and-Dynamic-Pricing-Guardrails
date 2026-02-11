CREATE TABLE IF NOT EXISTS dim_time_15m (
    bucket_start_ts TIMESTAMPTZ PRIMARY KEY,
    bucket_end_ts TIMESTAMPTZ NOT NULL,
    date_key DATE NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    quarter_hour_index SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fct_zone_time_spine_15m (
    zone_id INTEGER NOT NULL,
    bucket_start_ts TIMESTAMPTZ NOT NULL,
    bucket_end_ts TIMESTAMPTZ NOT NULL,
    date_key DATE NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    quarter_hour_index SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    PRIMARY KEY (zone_id, bucket_start_ts)
);

WITH generated_buckets AS (
    SELECT gs AS bucket_start_ts
    FROM generate_series(
        CAST(:run_start_ts AS TIMESTAMPTZ),
        CAST(:run_end_ts AS TIMESTAMPTZ) - INTERVAL '15 minutes',
        INTERVAL '15 minutes'
    ) AS gs
),
upsert_time AS (
    INSERT INTO dim_time_15m (
        bucket_start_ts,
        bucket_end_ts,
        date_key,
        hour_of_day,
        quarter_hour_index,
        day_of_week,
        week_of_year,
        month,
        year
    )
    SELECT
        gb.bucket_start_ts,
        gb.bucket_start_ts + INTERVAL '15 minutes' AS bucket_end_ts,
        CAST(timezone(:feature_tz, gb.bucket_start_ts) AS DATE) AS date_key,
        EXTRACT(HOUR FROM timezone(:feature_tz, gb.bucket_start_ts))::SMALLINT AS hour_of_day,
        (
            EXTRACT(HOUR FROM timezone(:feature_tz, gb.bucket_start_ts))::INT * 4
            + FLOOR(EXTRACT(MINUTE FROM timezone(:feature_tz, gb.bucket_start_ts)) / 15)::INT
        )::SMALLINT AS quarter_hour_index,
        EXTRACT(ISODOW FROM timezone(:feature_tz, gb.bucket_start_ts))::SMALLINT AS day_of_week,
        EXTRACT(WEEK FROM timezone(:feature_tz, gb.bucket_start_ts))::SMALLINT AS week_of_year,
        EXTRACT(MONTH FROM timezone(:feature_tz, gb.bucket_start_ts))::SMALLINT AS month,
        EXTRACT(YEAR FROM timezone(:feature_tz, gb.bucket_start_ts))::SMALLINT AS year
    FROM generated_buckets gb
    ON CONFLICT (bucket_start_ts) DO UPDATE SET
        bucket_end_ts = EXCLUDED.bucket_end_ts,
        date_key = EXCLUDED.date_key,
        hour_of_day = EXCLUDED.hour_of_day,
        quarter_hour_index = EXCLUDED.quarter_hour_index,
        day_of_week = EXCLUDED.day_of_week,
        week_of_year = EXCLUDED.week_of_year,
        month = EXCLUDED.month,
        year = EXCLUDED.year
    RETURNING bucket_start_ts
)
DELETE FROM fct_zone_time_spine_15m zts
WHERE zts.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND zts.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR zts.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
  );

INSERT INTO fct_zone_time_spine_15m (
    zone_id,
    bucket_start_ts,
    bucket_end_ts,
    date_key,
    hour_of_day,
    quarter_hour_index,
    day_of_week,
    week_of_year,
    month,
    year
)
SELECT
    dz.location_id AS zone_id,
    dt.bucket_start_ts,
    dt.bucket_end_ts,
    dt.date_key,
    dt.hour_of_day,
    dt.quarter_hour_index,
    dt.day_of_week,
    dt.week_of_year,
    dt.month,
    dt.year
FROM dim_zone dz
CROSS JOIN dim_time_15m dt
WHERE dt.bucket_start_ts >= CAST(:run_start_ts AS TIMESTAMPTZ)
  AND dt.bucket_start_ts < CAST(:run_end_ts AS TIMESTAMPTZ)
  AND (
      :zone_ids IS NULL
      OR dz.location_id = ANY(CAST(:zone_ids AS INTEGER[]))
  )
ON CONFLICT (zone_id, bucket_start_ts) DO UPDATE SET
    bucket_end_ts = EXCLUDED.bucket_end_ts,
    date_key = EXCLUDED.date_key,
    hour_of_day = EXCLUDED.hour_of_day,
    quarter_hour_index = EXCLUDED.quarter_hour_index,
    day_of_week = EXCLUDED.day_of_week,
    week_of_year = EXCLUDED.week_of_year,
    month = EXCLUDED.month,
    year = EXCLUDED.year;

CREATE INDEX IF NOT EXISTS idx_fct_zone_time_spine_15m_bucket
    ON fct_zone_time_spine_15m (bucket_start_ts);
