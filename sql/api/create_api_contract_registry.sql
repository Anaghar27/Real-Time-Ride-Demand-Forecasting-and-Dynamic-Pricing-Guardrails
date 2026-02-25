CREATE TABLE IF NOT EXISTS api_contract_registry (
    id BIGSERIAL PRIMARY KEY,
    api_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    endpoint_path TEXT NOT NULL,
    response_model_name TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_contract_registry_version
    ON api_contract_registry (api_version, schema_version, active_flag);

CREATE UNIQUE INDEX IF NOT EXISTS uq_api_contract_registry_unique_contract
    ON api_contract_registry (api_version, schema_version, endpoint_path, response_model_name, contract_hash);
