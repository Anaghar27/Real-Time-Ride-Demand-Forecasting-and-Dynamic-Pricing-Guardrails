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

## Phase 2 feature pipeline scope
- Purpose: generate leakage-safe, model-ready demand features at exact 15-minute grain (`zone_id`, `bucket_start_ts`).
- Target definition: `pickup_count` from trip `pickup_datetime` aggregated by pickup zone.
- Timezone policy: all storage timestamps are UTC `TIMESTAMPTZ`; calendar derivations use `FEATURE_TIMEZONE`.
- Leakage prevention guarantees:
  - lags computed with strict partition/order by `zone_id`, `bucket_start_ts`
  - rolling frames exclude current row (`ROWS BETWEEN N PRECEDING AND 1 PRECEDING`)
  - no feature uses future records
- Stable output contract: `fact_demand_features` with deterministic schema and unique key on (`zone_id`, `bucket_start_ts`).

### Feature list
- Keys: `zone_id`, `bucket_start_ts`
- Target: `pickup_count`
- Calendar: `hour_of_day`, `quarter_hour_index`, `day_of_week`, `is_weekend`, `week_of_year`, `month`, `is_holiday`
- Lags: `lag_1`, `lag_2`, `lag_4`, `lag_96`, `lag_672`
- Rolling stats: `roll_mean_4`, `roll_mean_8`, `roll_std_8`, `roll_max_16`
- Metadata: `feature_version`, `created_at`, `run_id`, `source_min_ts`, `source_max_ts`

## Phase 3 EDA and governance scope
- Objective: reproducible time-series EDA to guide sparse-segment modeling and fallback-safe scoring behavior.
- Inputs: `fact_demand_features` at `zone_id x 15-minute bucket_start_ts`.
- Core outputs:
  - `eda_time_profile_summary`
  - `eda_zone_profile_summary`
  - `eda_seasonality_summary`
  - `eda_zone_sparsity_summary`
  - `zone_fallback_policy`
  - `eda_run_log`, `eda_check_results`
- Governance docs:
  - `docs/eda/phase3_report.md`
  - `docs/eda/assumptions_register.yaml`
  - `docs/eda/fallback_policy.md`
  - `docs/eda/data_dictionary_addendum.md`

## Phase 4 training scope
- Objective: leakage-safe, reproducible model training and champion registration with MLflow traceability.
- Inputs: `fact_demand_features` joined with `zone_fallback_policy` by configured `policy_version`.
- Split policy: strict chronological holdout (`train`, `validation`, `test`) with optional rolling-origin folds (explicit timestamps or auto-derived windows in `configs/split_policy.yaml`).
- Baselines (must run first):
  - `naive_previous_day` using `lag_96`
  - `naive_previous_week` using `lag_672`
  - `linear_baseline` (ridge)
- Candidate models:
  - LightGBM
  - CatBoost
  - XGBoost
  - ElasticNet challenger
- Metrics:
  - MAE, RMSE, WAPE, sMAPE
  - required slices: peak/off-peak, weekday/weekend, robust/sparse zones
- Tracking: all runs log params, metrics, tags, and artifacts to MLflow.
- Champion gate:
  - minimum improvement over best baseline on primary metric
  - no critical sparse-zone regression
  - latency and stability thresholds
  - complete MLflow metadata
- Registration:
  - Staging by default
  - Production only when champion policy explicitly allows it

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

## Phase 2 commands (required order)
1. `make features-time-buckets`
2. `make features-aggregate`
3. `make features-calendar`
4. `make features-lag-roll`
5. `make features-validate`
6. `make features-publish`
7. `make features-build`

## Phase 3 commands (required order)
1. `make eda-seasonality`
2. `make eda-sparsity`
3. `make eda-fallback-policy`
4. `make eda-docs`
5. `make eda-validate`
6. `make eda-run`

## Phase 4 commands (required order)
1. `make train-prepare-data`
2. `make train-show-splits`
3. `make train-baseline`
4. `make train-candidates`
5. `make train-compare`
6. `make train-track`
7. `make train-select-champion`
8. `make train-register-staging`
9. `make train-register-production` (only when policy allows)
10. `make train-run-all`
11. `make train-auto` (build features + preflight coverage checks + train-run-all)

Phase 4 configuration:
- `configs/training.yaml`: feature/training window (fixed `start_date`/`end_date` or auto-derived via `data.auto_window`).
- `configs/split_policy.yaml`: holdout/rolling split policy (explicit timestamps or auto-derived windows).

Phase 3 configuration:
- `configs/eda.yaml`: analysis window, top/bottom zone settings, report output paths.
- `configs/eda_thresholds.yaml`: sparsity thresholds and fallback policy mapping.
- Optional Make overrides:
  - `EDA_START_DATE`, `EDA_END_DATE`
  - `EDA_FEATURE_VERSION`, `EDA_POLICY_VERSION`
  - `EDA_ZONES`
  - `EDA_RUN_ID`

Sparsity classes:
- `robust`: high non-zero activity and coverage, zone model preferred.
- `medium`: moderate activity, zone model with conservative smoothing.
- `sparse`: limited activity, borough seasonal baseline fallback.
- `ultra_sparse`: minimal activity, city seasonal baseline fallback.

Use `.env` or Make overrides for runtime scope:
- `FEATURE_START_DATE` (inclusive)
- `FEATURE_END_DATE` (inclusive)
- `FEATURE_VERSION`
- `FEATURE_ZONES` (optional comma-separated list)
- `FEATURE_TIMEZONE`
- `FEATURE_LAG_NULL_POLICY` (`zero` or `keep_nulls`)
- `HOLIDAY_REFERENCE_FILE`

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
- Feature quality failures:
  - Inspect `feature_check_results` by `run_id` and resolve `severity='critical'` failures before publish.
- Feature rerun determinism:
  - Re-run the same date interval + `FEATURE_VERSION` and compare row count and aggregate metrics in `fact_demand_features`.
- EDA run failure:
  - Query `eda_check_results` for failed critical checks by `run_id`.
- Missing fallback assignments:
  - Re-run `make eda-sparsity` then `make eda-fallback-policy`; verify `zone_fallback_policy` coverage equals sparsity zone count.
- Report section check failures:
  - Regenerate docs with `make eda-docs` and verify required sections in `docs/eda/phase3_report.md`.
- Training split issues:
  - Run `make train-show-splits` and confirm strict temporal ordering with no overlaps.
- Training gate failures:
  - Check `reports/training/<run_id>/champion_decision.json` for reason codes.
- Registration blocked:
  - Verify champion gate pass and `configs/champion_policy.yaml` `registration.allow_production`.
