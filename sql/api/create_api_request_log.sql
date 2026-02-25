CREATE TABLE IF NOT EXISTS api_request_log (
    request_id TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_request_log_created_at
    ON api_request_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_request_log_path_method
    ON api_request_log (path, method);
