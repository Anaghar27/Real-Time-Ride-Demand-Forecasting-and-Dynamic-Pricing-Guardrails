CREATE TABLE IF NOT EXISTS ingestion_watermark (
    dataset_name TEXT PRIMARY KEY,
    latest_successful_period TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
