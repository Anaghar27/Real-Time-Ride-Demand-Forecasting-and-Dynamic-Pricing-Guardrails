CREATE TABLE IF NOT EXISTS dim_zone (
    location_id INTEGER PRIMARY KEY,
    borough TEXT NOT NULL,
    zone TEXT NOT NULL,
    service_zone TEXT,
    ingested_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS zone_join_coverage_report (
    id BIGSERIAL PRIMARY KEY,
    reported_at TIMESTAMPTZ NOT NULL,
    total_rows BIGINT NOT NULL,
    pickup_coverage_pct DOUBLE PRECISION NOT NULL,
    dropoff_coverage_pct DOUBLE PRECISION NOT NULL
);
