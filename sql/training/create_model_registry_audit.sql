CREATE TABLE IF NOT EXISTS model_registry_audit (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    stage TEXT NOT NULL,
    mlflow_run_id TEXT,
    status TEXT NOT NULL,
    reason_code TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_registry_audit_run
    ON model_registry_audit (run_id, created_at DESC);
