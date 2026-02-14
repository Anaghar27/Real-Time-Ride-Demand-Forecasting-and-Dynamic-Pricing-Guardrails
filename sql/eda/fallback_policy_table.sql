CREATE TABLE IF NOT EXISTS zone_fallback_policy (
    zone_id INTEGER NOT NULL,
    sparsity_class TEXT NOT NULL,
    fallback_method TEXT NOT NULL,
    fallback_priority INTEGER NOT NULL,
    confidence_band TEXT NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    policy_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (zone_id, policy_version, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_zone_fallback_policy_zone
    ON zone_fallback_policy (zone_id, policy_version);
