# Real_Time_Ride_Demand_Forecasting_and_Dynamic_Pricing_Guardrails

## Project overview
Production-style local-first ML/MLOps platform for NYC TLC ride-demand forecasting and dynamic pricing guardrails.

## Tech stack
- Python 3.11
- FastAPI
- PostgreSQL + SQLAlchemy
- MLflow
- Prefect
- Prometheus + Grafana
- pytest, Ruff, Black, mypy
- Docker + Docker Compose
- GitHub Actions

## Phase 0 (completed)
- Repository scaffold and packaging
- Local developer environment and pinned dependencies
- Docker Compose platform services
- Makefile workflow
- CI checks (lint, typecheck, tests)

## Phase 1 ingestion scope
1. Step 1.1 sample download and manifest
2. Step 1.2 raw loader and schema normalization
3. Step 1.3 zone dimension load and coverage report
4. Step 1.4 hard-gated ingestion checks
5. Step 1.5 idempotent rerun-safe ingestion
6. Step 1.6 historical backfill (strictly locked behind gate)

## Strict gate policy for Step 1.6
Backfill commands always enforce `scripts/check_phase1_gate.py` first. Step 1.6 aborts if:
- Step 1.1-1.5 tests are not passing
- successful sample batches are below threshold
- unresolved failed batches exist

## Prerequisites
- Python 3.11
- Docker Desktop / Docker Engine with Compose
- GNU Make
- curl

## Quickstart
1. Clone repository.
2. Review `.env`.
3. Bootstrap:
```bash
make setup
```
4. Start platform:
```bash
make up
```
5. Smoke check:
```bash
make smoke
```

## Service URLs
- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- MLflow: `http://localhost:5001`
- Prefect: `http://localhost:4200`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Phase 1 commands (required order)
1. `make ingest-sample-download`
2. `make ingest-zone-dim`
3. `make ingest-load-sample`
4. `make ingest-validate`
5. `make ingest-run-sample`
6. `make ingest-rerun-sample`
7. `make ingest-gate-check`
8. `make ingest-backfill-pilot`
9. `make ingest-backfill-full`
10. `make ingest-incremental`

## Common commands
- `make help`
- `make check`
- `make logs`
- `make ps`
- `make db-shell`
- `make urls`

## Troubleshooting
- Port conflicts:
  - Update `.env` ports and run `make restart`.
- Docker not running:
  - Start Docker then `make up`.
- API/DB connectivity:
  - Check `make ps`, `make logs`, and `DATABASE_URL`.
- Ingestion check failures:
  - Query `ingestion_check_results` and `ingestion_rejects` by `batch_id`.
- Idempotent rerun validation:
  - Run `make ingest-rerun-sample` and confirm `inserted_or_updated=0` for already succeeded files.
- Gate failures:
  - Run `make ingest-gate-check` and follow reported reasons.
- Backfill resume:
  - Inspect `ingestion_watermark`; rerun `make ingest-incremental` after resolving failures.
