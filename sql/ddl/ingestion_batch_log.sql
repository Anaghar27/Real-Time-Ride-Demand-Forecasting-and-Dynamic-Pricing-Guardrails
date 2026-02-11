CREATE TABLE IF NOT EXISTS ingestion_batch_log (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    checksum TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('discovered', 'running', 'failed', 'succeeded')),
    rows_read INTEGER DEFAULT 0,
    rows_valid INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    load_duration_sec DOUBLE PRECISION DEFAULT 0,
    check_pass_rate DOUBLE PRECISION DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_batch_log_state
    ON ingestion_batch_log (state);

CREATE INDEX IF NOT EXISTS idx_ingestion_batch_log_source_file
    ON ingestion_batch_log (source_file);
