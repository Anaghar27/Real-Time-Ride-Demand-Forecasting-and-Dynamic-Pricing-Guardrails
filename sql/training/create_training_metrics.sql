CREATE TABLE IF NOT EXISTS training_metrics (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_role TEXT NOT NULL,
    split_id TEXT NOT NULL,
    mae DOUBLE PRECISION NOT NULL,
    rmse DOUBLE PRECISION NOT NULL,
    wape DOUBLE PRECISION NOT NULL,
    smape DOUBLE PRECISION NOT NULL,
    latency_ms DOUBLE PRECISION,
    model_size_bytes BIGINT,
    mlflow_run_id TEXT,
    extra_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS training_slice_metrics (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    split_id TEXT NOT NULL,
    slice_name TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    mae DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    wape DOUBLE PRECISION,
    smape DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_training_metrics_run_model
    ON training_metrics (run_id, model_name);

CREATE INDEX IF NOT EXISTS idx_training_slice_metrics_run_slice
    ON training_slice_metrics (run_id, slice_name);
