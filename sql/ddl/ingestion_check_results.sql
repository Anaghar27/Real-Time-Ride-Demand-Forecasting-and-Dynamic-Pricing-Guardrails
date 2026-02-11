CREATE TABLE IF NOT EXISTS ingestion_check_results (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES ingestion_batch_log(batch_id),
    check_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    metric_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_check_results_batch
    ON ingestion_check_results (batch_id, check_name);

CREATE TABLE IF NOT EXISTS ingestion_rejects (
    id BIGSERIAL PRIMARY KEY,
    ingest_batch_id TEXT NOT NULL REFERENCES ingestion_batch_log(batch_id),
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    check_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_rejects_batch
    ON ingestion_rejects (ingest_batch_id);
