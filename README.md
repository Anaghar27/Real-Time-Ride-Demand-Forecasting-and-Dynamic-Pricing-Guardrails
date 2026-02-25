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

## Phase 5 scoring scope
- Objective: operational, leakage-safe scoring for future demand forecasts with uncertainty estimates.
- Inputs:
  - champion model from MLflow Model Registry (by `model_name` + `model_stage`)
  - historical demand features from `fact_demand_features`
  - optional sparse-zone policy from `zone_fallback_policy` to adjust confidence
- Outputs:
  - `demand_forecast` contract table for downstream pricing
  - `scoring_run_log` audit table (status, window, model version, counts, latency)
  - `confidence_reference` table (residual quantiles for prediction intervals)
  - run artifacts in `reports/scoring/<run_id>/`
- Scheduling: Prefect deployment on an interval; overlap protected by a Postgres advisory lock.

## Phase 6 pricing guardrails scope
- Objective: transform Phase 5 forecasts into safe, explainable, and rerun-safe pricing decisions.
- Inputs:
  - `demand_forecast` output rows from Phase 5 (`zone_id`, `bucket_start_ts`, `y_pred`, confidence fields, provenance)
  - config-driven policy YAML files for multiplier logic, caps, rate limits, and reason taxonomy
  - optional sparse-zone classes from `zone_fallback_policy`
- Outputs:
  - `pricing_decisions` contract table (raw + guarded multipliers, diagnostics, reason codes)
  - `pricing_run_log` audit table (status, counts, latency, policy version, failure reason)
  - policy snapshot tables (`pricing_policy_snapshot`, `multiplier_rule_snapshot`, `rate_limit_rule_snapshot`)
  - reason code reference table (`reason_code_reference`)
  - run artifacts in `reports/pricing_guardrails/<run_id>/`
- Scheduling: Prefect deployment with retries; run overlap blocked by Postgres advisory lock.

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

## Phase 5 commands
- One-off scoring run (current time window): `make score-run`
- Validate-only (checks + artifacts, no DB write): `make score-validate`
- Backfill a specific forecast window (end-exclusive):
  - `make score-run-window SCORE_FORECAST_START_TS='2025-11-03T00:00:00+00:00' SCORE_FORECAST_END_TS='2025-11-03T01:00:00+00:00'`
- Schedule scoring with Prefect (register deployment + start worker): `make score-schedule`
- Show URLs: `make score-show-urls`

## Phase 6 commands
- Load and validate policy files: `make pricing-load-policy`
- Compute raw multipliers only: `make pricing-compute-raw`
- Apply cap guardrails only: `make pricing-apply-caps`
- Apply rate limiter only: `make pricing-apply-rate-limit`
- Generate reason codes only: `make pricing-reason-codes`
- Validate pricing checks (no save): `make pricing-validate`
- Save pricing decisions (full run): `make pricing-save`
- One-off pricing run (latest mode): `make pricing-run`
- Replay/backfill explicit window (end-exclusive):
  - `make pricing-run-window PRICE_FORECAST_START_TS='2025-11-03T00:00:00+00:00' PRICE_FORECAST_END_TS='2025-11-03T01:00:00+00:00'`
  - optional run pin: add `PRICE_FORECAST_RUN_ID='<forecast_run_id>'`
- Ordered chain run (`load-policy -> compute-raw -> apply-caps -> apply-rate-limit -> reason-codes -> validate -> save`):
  - `make pricing-run-all`
- Run market evaluation query pack (latest succeeded pricing run):
  - `make pricing-evaluate`
- Schedule pricing with Prefect (register deployment + start worker): `make pricing-schedule`
- Show URLs: `make pricing-show-urls`

## Phase 7 API commands
- Run API (prod-style): `make api-run`
- Run API (dev reload): `make api-dev`
- Show API URLs: `make api-show-urls`
- Run API tests: `make api-test`
- Run API lint: `make api-lint`
- Format API code: `make api-format`
- Check schema contracts: `make api-contract-check`
- Check plain-language mappings: `make api-plain-language-check`
- Export OpenAPI JSON: `make api-openapi-export`

### Phase 7 endpoint catalog
- Health:
  - `GET /health`
  - `GET /ready`
  - `GET /version`
- Pricing (`/api/v1/pricing`):
  - `GET /latest`
  - `GET /window`
  - `GET /zone/{zone_id}`
  - `GET /runs/latest`
  - `GET /runs/{run_id}`
- Forecast (`/api/v1/forecast`):
  - `GET /latest`
  - `GET /window`
  - `GET /zone/{zone_id}`
  - `GET /runs/latest`
  - `GET /runs/{run_id}`
- Metadata (`/api/v1/metadata`):
  - `GET /zones`
  - `GET /reason-codes`
  - `GET /policy/current`
  - `GET /schema`
- Diagnostics (`/api/v1/diagnostics`):
  - `GET /coverage/latest`
  - `GET /guardrails/latest`
  - `GET /confidence/latest`

### Phase 7 examples
- Health:
  - `curl -s http://localhost:8000/health | jq`
- Pricing latest:
  - `curl -s "http://localhost:8000/api/v1/pricing/latest?page=1&page_size=5&sort=bucket_start_ts:desc" | jq`
- Pricing window:
  - `curl -s "http://localhost:8000/api/v1/pricing/window?start_ts=2026-02-25T10:00:00Z&end_ts=2026-02-25T11:00:00Z&borough=Manhattan&page=1&page_size=5&sort=zone_id:asc" | jq`
- Forecast latest:
  - `curl -s "http://localhost:8000/api/v1/forecast/latest?page=1&page_size=5&sort=bucket_start_ts:desc" | jq`
- Metadata reason codes:
  - `curl -s "http://localhost:8000/api/v1/metadata/reason-codes?page=1&page_size=20&sort=reason_code:asc" | jq`

### Phase 7 troubleshooting
- DB connection errors:
  - verify `DATABASE_URL` in `.env`
  - run `make db-shell`
- No latest run found:
  - run Phase 5 (`make score-run`) and Phase 6 (`make pricing-run`) to populate run logs
- Schema mismatch:
  - run `make api-contract-check`
  - inspect `reports/api/contract_checks/contract_diff_report.md`
- Invalid query params:
  - API returns structured errors with `error_code`, `message`, `details`, and `request_id`
- Slow endpoints:
  - filter by `zone_id`, `bucket_start_ts`, and run endpoints where possible
  - keep `page_size` under configured limits

Phase 4 configuration:
- `configs/training.yaml`: feature/training window (fixed `start_date`/`end_date` or auto-derived via `data.auto_window`).
- `configs/split_policy.yaml`: holdout/rolling split policy (explicit timestamps or auto-derived windows).

Phase 5 configuration:
- Environment variables (optional):
  - `SCORING_HORIZON_BUCKETS` (default `4`)
  - `SCORING_FREQUENCY_MINUTES` (default `15`)
  - `RIDE_DEMAND_MODEL_NAME` / `RIDE_DEMAND_MODEL_STAGE` (default stage `Staging`)
  - `SCORING_FEATURE_VERSION` / `SCORING_POLICY_VERSION`
  - `SCORING_STALE_DATA_FALLBACK_ENABLED` (`true/false`, default `false`)
  - `SCORING_STALE_DATA_FLOOR_START_TS` (ISO8601 UTC floor used only when stale fallback is enabled)

Phase 6 configuration:
- YAML files:
  - `configs/pricing_policy.yaml`
  - `configs/multiplier_rules.yaml`
  - `configs/rate_limit_rules.yaml`
  - `configs/reason_codes.yaml`
- Environment variables (optional):
  - `PRICING_POLICY_VERSION`
  - `PRICING_FORECAST_SELECTION_MODE` (`latest_run`, `explicit_run_id`, `explicit_window`)
  - `PRICING_FORECAST_RUN_ID`
  - `PRICING_FORECAST_START_TS`, `PRICING_FORECAST_END_TS`
  - `PRICING_DEFAULT_FLOOR_MULTIPLIER`, `PRICING_GLOBAL_CAP_MULTIPLIER`
  - `PRICING_MAX_INCREASE_PER_BUCKET`, `PRICING_MAX_DECREASE_PER_BUCKET`
  - `PRICING_SMOOTHING_ENABLED`, `PRICING_SMOOTHING_ALPHA`
  - `PRICING_STRICT_CHECKS`
  - `PRICING_PREFECT_WORK_POOL`, `PRICING_PREFECT_WORK_QUEUE`

### Phase 5 troubleshooting
- **Model not found in registry stage**: confirm MLflow is up (`make smoke`) and the stage has a version. In MLflow UI (`make mlflow-ui`), check `Models -> ride-demand-forecast-model -> Versions`. Then set `RIDE_DEMAND_MODEL_NAME` / `RIDE_DEMAND_MODEL_STAGE` and rerun `make score-run`.
- **Feature schema mismatch**: scoring expects the Phase 2 contract columns from `fact_demand_features` (calendar + lags + rollings). Rebuild features for the latest window (`make features-build`) and ensure `SCORING_FEATURE_VERSION` matches the published `feature_version`.
- **Not enough history for lags/rollings**: increase `SCORING_HISTORY_DAYS` and/or backfill features further back in time. For weekly lag (`lag_672`) you typically want at least 8–14 days of history to avoid sparse/null lags.
- **Using temporary historical scoring while upstream data catches up**: set `SCORING_STALE_DATA_FALLBACK_ENABLED=true` and `SCORING_STALE_DATA_FLOOR_START_TS=2025-11-02T00:00:00+00:00`. When feature freshness is stale, scoring starts from the latest observed feature bucket (not earlier than the configured floor). Once features are fresh again, scoring automatically returns to current-time scheduling.
- **`zone_fallback_policy` missing**: scoring still runs, but confidence will not be segment-adjusted. Run Phase 3 (`make eda-run`) or create the policy table, then rerun scoring.
- **Prefect schedule not running**: `make score-schedule` starts a local Prefect worker and blocks. In Prefect UI (`make urls`), verify the deployment exists and is scheduled, and that the worker is online.
- **Duplicate rows / idempotency confusion**: `demand_forecast` uses `(forecast_run_key, zone_id, bucket_start_ts)` as the primary key and upserts on conflicts. If you change the horizon or model version, you will get a new `forecast_run_key` by design.

### Phase 6 troubleshooting
- **Missing forecast rows**: verify `demand_forecast` has rows for the requested selection mode and window. For explicit window runs, confirm `PRICE_FORECAST_START_TS` and `PRICE_FORECAST_END_TS` are UTC ISO8601 and end-exclusive.
- **Invalid policy config**: run `make pricing-load-policy` and fix reported missing keys/version mismatch across `pricing_policy.yaml`, `multiplier_rules.yaml`, `rate_limit_rules.yaml`, and `reason_codes.yaml`.
- **Rate limiter violations**: run `make pricing-validate`, then inspect `reports/pricing_guardrails/<run_id>/run_summary.json` and `guardrail_stats.csv` for delta-bound failures.
- **Duplicate writes due to key mismatch**: verify `pricing_run_key` inputs are stable (`pricing_policy_version + forecast_run_id + target_bucket_start + target_bucket_end`) and confirm table unique key is `(pricing_run_key, zone_id, bucket_start_ts)`.
- **Missing previous multiplier cold-starts**: expected for new zones or first pricing run; rows should include `NO_PREVIOUS_MULTIPLIER_COLD_START` and use configured cold-start multiplier.

### Phase 6 market evaluation
- Query pack file: `sql/pricing_guardrails/market_evaluation_queries.sql`
- Guide: `docs/pricing_guardrails/market_evaluation_queries.md`
- Scope:
  - forecast error vs baseline demand reference
  - non-causal lift proxies (raw vs guarded vs no-surge)
  - customer shock metrics (multiplier deltas)
  - fairness slices by zone class

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
