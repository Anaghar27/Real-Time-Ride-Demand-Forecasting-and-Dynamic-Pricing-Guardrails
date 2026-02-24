CREATE TABLE IF NOT EXISTS pricing_policy_snapshot (
    policy_version TEXT NOT NULL,
    config_json JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (policy_version, effective_from)
);

CREATE TABLE IF NOT EXISTS multiplier_rule_snapshot (
    policy_version TEXT NOT NULL,
    config_json JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (policy_version, effective_from)
);

CREATE TABLE IF NOT EXISTS rate_limit_rule_snapshot (
    policy_version TEXT NOT NULL,
    config_json JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (policy_version, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_pricing_policy_snapshot_active
    ON pricing_policy_snapshot (policy_version, active_flag);

CREATE INDEX IF NOT EXISTS idx_multiplier_rule_snapshot_active
    ON multiplier_rule_snapshot (policy_version, active_flag);

CREATE INDEX IF NOT EXISTS idx_rate_limit_rule_snapshot_active
    ON rate_limit_rule_snapshot (policy_version, active_flag);
