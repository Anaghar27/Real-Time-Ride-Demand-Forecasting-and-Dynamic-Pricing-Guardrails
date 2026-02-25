# API Versioning and Backward Compatibility Policy

## Versioning Strategy
- Path-based API versioning is used.
- Current version path: `/api/v1`
- Response schema version field: `schema_version` (example: `1.0.0`)

## Compatibility Rules
### Non-breaking changes
- Add optional fields to existing responses.
- Add new endpoints.
- Add new reason codes.

### Breaking changes
- Rename existing fields.
- Remove existing fields.
- Change field types.
- Change response nesting shape.

## Required Action for Breaking Changes
- Any breaking change requires a new API path version (for example `/api/v2`).
- Breaking changes must not be shipped under `/api/v1`.

## Contract Enforcement
- Contract snapshot: `reports/api/contract_checks/latest_contract_snapshot.json`
- Diff report: `reports/api/contract_checks/contract_diff_report.md`
- Validation command:
```bash
make api-contract-check
```

## Contract Registry Table
- DDL: `sql/api/create_api_contract_registry.sql`
- Table: `api_contract_registry`
- Core fields: `api_version`, `schema_version`, `endpoint_path`, `response_model_name`, `contract_hash`, `active_flag`, `created_at`
