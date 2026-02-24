CREATE TABLE IF NOT EXISTS reason_code_reference (
    reason_code TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reason_code_reference_category
    ON reason_code_reference (category, active_flag);
