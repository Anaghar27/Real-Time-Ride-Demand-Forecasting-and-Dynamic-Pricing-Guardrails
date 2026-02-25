# Phase 7 API Overview

## Purpose
Phase 7 exposes forecasting and pricing outputs as a versioned FastAPI service so downstream systems, dashboards, and analysts can consume outputs programmatically.

## Base URLs
- Local API root: `http://localhost:8000`
- Versioned base path: `http://localhost:8000/api/v1`
- OpenAPI docs: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

## Endpoint Catalog
### Health
- `GET /health`
- `GET /ready`
- `GET /version`

### Pricing (`/api/v1/pricing`)
- `GET /latest`
- `GET /window`
- `GET /zone/{zone_id}`
- `GET /runs/latest`
- `GET /runs/{run_id}`

### Forecast (`/api/v1/forecast`)
- `GET /latest`
- `GET /window`
- `GET /zone/{zone_id}`
- `GET /runs/latest`
- `GET /runs/{run_id}`

### Metadata (`/api/v1/metadata`)
- `GET /zones`
- `GET /reason-codes`
- `GET /policy/current`
- `GET /schema`

### Diagnostics (`/api/v1/diagnostics`)
- `GET /coverage/latest`
- `GET /guardrails/latest`
- `GET /confidence/latest`

## Deterministic Pagination and Sorting
- Pagination inputs: `page`, `page_size` (or `limit` alias)
- Max page size enforced by config (`API_MAX_PAGE_SIZE`)
- Sort input format: `sort=<field>:asc|desc`
- Stable tie breakers are applied server-side (`zone_id`, `bucket_start_ts`)

## Local Run Commands
```bash
make api-dev
make api-show-urls
make api-test
make api-contract-check
```

## Troubleshooting Quick Notes
- DB connection errors: verify `DATABASE_URL`, then run `make db-shell`.
- No latest run found: ensure Phase 5 and Phase 6 runs completed and wrote run logs.
- Schema mismatch: run `make api-contract-check` and inspect `reports/api/contract_checks/contract_diff_report.md`.
- Invalid query params: API returns structured errors with `error_code`, `message`, and `request_id`.
- Slow endpoints: use indexed filters first (`zone_id`, `bucket_start_ts`, `run_id`) and avoid large `page_size` values.
