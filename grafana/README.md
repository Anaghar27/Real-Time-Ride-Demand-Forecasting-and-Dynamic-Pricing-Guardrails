# Grafana Dashboards (Phase 8)

This folder contains technical observability dashboards for scoring, pricing, and API health.

## Structure
- `provisioning/datasources/postgres.yaml`: Postgres datasource provisioning.
- `provisioning/dashboards/dashboards.yaml`: file-based dashboard loader.
- `dashboards/*.json`: dashboard definitions imported automatically by Grafana.

## Local workflow
1. Apply SQL views in `sql/dashboards/*.sql`.
2. Run `make dashboard-tech-provision`.
3. Run `make dashboard-tech-up`.
4. Open Grafana with `make dashboard-tech-open`.

## Credentials
Default local credentials are `admin` / `admin` unless overridden in environment variables.
