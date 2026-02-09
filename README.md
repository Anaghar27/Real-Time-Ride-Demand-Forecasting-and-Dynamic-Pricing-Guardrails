# Real_Time_Ride_Demand_Forecasting_and_Dynamic_Pricing_Guardrails

## Project overview
This repository provides a production-style Phase 0 foundation for a local-first ML and MLOps platform focused on real-time ride demand forecasting and dynamic pricing guardrails.

## Tech stack
- Python 3.11
- FastAPI
- PostgreSQL + SQLAlchemy
- MLflow
- Prefect
- Prometheus + Grafana
- Evidently
- Docker + Docker Compose
- pytest, Ruff, Black, mypy
- GitHub Actions

## Phase 0 scope
- Repository and package structure
- Local environment bootstrap and pinned dependencies
- Docker Compose platform for core services
- Makefile-based developer workflow
- CI workflow for lint, typecheck, tests
- Initial API and configuration tests

## Prerequisites
- Python 3.11
- Docker and Docker Compose
- GNU Make
- curl

## Quickstart
1. Clone the repository.
2. Review and adjust `.env` values as needed for your machine.
3. Run local bootstrap:
   ```bash
   make setup
   ```
4. Start the local platform:
   ```bash
   make up
   ```
5. Run smoke checks:
   ```bash
   make smoke
   ```

## Service URLs
- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- MLflow: `http://localhost:5000`
- Prefect: `http://localhost:4200`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

## Common commands
- `make help` - list available commands
- `make setup` - create virtualenv and install dependencies
- `make up` - start all local services
- `make down` - stop all local services
- `make ps` - list compose services
- `make logs` - tail service logs
- `make api` - run API locally outside Docker
- `make check` - run lint, typecheck, tests
- `make db-shell` - open psql shell in postgres container
- `make urls` - print local service URLs

## Troubleshooting
- Port conflicts:
  - Update relevant ports in `.env` and restart with `make restart`.
- Docker daemon not running:
  - Start Docker Desktop or Docker Engine and rerun `make up`.
- DB connection issues:
  - Check `make ps` for postgres health, then inspect logs with `make logs`.
  - Verify `DATABASE_URL` in `.env` matches runtime mode (local API vs Docker API).
- Healthcheck failures:
  - Wait for initial service startup (first run can take longer).
  - Re-run `make smoke`; if it fails, inspect `make logs` output for the failing service.

## Ready for Phase 1
With this baseline, the repository is prepared for Phase 1 ingestion pipelines, feature transforms, and orchestration workflows.
