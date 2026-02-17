CREATE TABLE IF NOT EXISTS training_run_log (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    split_policy_version TEXT NOT NULL,
    train_start_ts TIMESTAMPTZ,
    train_end_ts TIMESTAMPTZ,
    val_start_ts TIMESTAMPTZ,
    val_end_ts TIMESTAMPTZ,
    test_start_ts TIMESTAMPTZ,
    test_end_ts TIMESTAMPTZ,
    config_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_training_run_log_started_at
    ON training_run_log (started_at DESC);
