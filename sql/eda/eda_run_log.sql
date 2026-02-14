CREATE TABLE IF NOT EXISTS eda_run_log (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    data_start_ts TIMESTAMPTZ NOT NULL,
    data_end_ts TIMESTAMPTZ NOT NULL,
    feature_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS eda_check_results (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES eda_run_log(run_id),
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eda_check_results_run
    ON eda_check_results (run_id, check_name);
