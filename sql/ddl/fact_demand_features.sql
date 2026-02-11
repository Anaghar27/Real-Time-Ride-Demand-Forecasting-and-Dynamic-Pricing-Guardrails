CREATE TABLE IF NOT EXISTS fact_demand_features (
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
    feature_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id TEXT NOT NULL,
    source_min_ts TIMESTAMPTZ NOT NULL,
    source_max_ts TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_pickup_count_nonnegative CHECK (pickup_count >= 0)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'fact_demand_features'
          AND constraint_name = 'pk_fact_demand_features'
          AND constraint_type = 'PRIMARY KEY'
    ) THEN
        ALTER TABLE fact_demand_features DROP CONSTRAINT pk_fact_demand_features;
    END IF;
EXCEPTION
    WHEN undefined_object THEN
        NULL;
END $$;

DELETE FROM fact_demand_features f
USING (
    SELECT ctid
    FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY zone_id, bucket_start_ts
                ORDER BY created_at DESC, run_id DESC
            ) AS rn
        FROM fact_demand_features
    ) ranked
    WHERE ranked.rn > 1
) dupes
WHERE f.ctid = dupes.ctid;

ALTER TABLE fact_demand_features
    ADD CONSTRAINT pk_fact_demand_features PRIMARY KEY (zone_id, bucket_start_ts);

CREATE INDEX IF NOT EXISTS idx_fact_demand_features_zone_bucket
    ON fact_demand_features (zone_id, bucket_start_ts);

CREATE INDEX IF NOT EXISTS idx_fact_demand_features_bucket
    ON fact_demand_features (bucket_start_ts);
