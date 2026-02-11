CREATE TABLE IF NOT EXISTS stg_raw_trips (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMPTZ,
    dropoff_datetime TIMESTAMPTZ,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    rate_code_id INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_type INTEGER,
    store_and_fwd_flag TEXT,
    ingest_batch_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_raw_trips_batch
    ON stg_raw_trips (ingest_batch_id);
