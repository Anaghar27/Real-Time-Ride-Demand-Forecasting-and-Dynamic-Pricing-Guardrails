CREATE TABLE IF NOT EXISTS raw_trips (
    id BIGSERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ NOT NULL,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    rate_code_id INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_type INTEGER,
    store_and_fwd_flag TEXT,
    ingest_batch_id TEXT NOT NULL REFERENCES ingestion_batch_log(batch_id),
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_raw_trips_source_row UNIQUE (source_file, source_row_number)
);

CREATE INDEX IF NOT EXISTS idx_raw_trips_pickup_datetime
    ON raw_trips (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_raw_trips_pickup_location_id
    ON raw_trips (pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_raw_trips_ingest_batch_id
    ON raw_trips (ingest_batch_id);
