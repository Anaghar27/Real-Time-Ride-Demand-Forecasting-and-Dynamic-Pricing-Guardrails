SHELL := /bin/bash
.DEFAULT_GOAL := help

ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
VENV_PYTHON := .venv/bin/python
VENV_PIP := .venv/bin/pip
VENV_UVICORN := .venv/bin/uvicorn
VENV_PREFECT := .venv/bin/prefect
VENV_STREAMLIT := .venv/bin/streamlit

.PHONY: help setup up down restart logs ps api api-run api-dev api-test api-contract-check api-lint api-format api-show-urls api-smoke api-openapi-export api-plain-language-check test lint format typecheck check clean db-shell smoke mlflow-ui urls ingest-sample-download ingest-zone-dim ingest-load-sample ingest-validate ingest-run-sample ingest-rerun-sample ingest-gate-check ingest-backfill-pilot ingest-backfill-full ingest-incremental features-time-buckets features-aggregate features-calendar features-lag-roll features-validate features-publish features-build eda-seasonality eda-sparsity eda-fallback-policy eda-docs eda-validate eda-run train-prepare-data train-show-splits train-baseline train-candidates train-compare train-track train-select-champion train-register train-register-staging train-register-production train-run-all train-auto score-run score-run-window score-validate score-schedule score-show-urls pricing-load-policy pricing-compute-raw pricing-apply-caps pricing-apply-rate-limit pricing-reason-codes pricing-save pricing-validate pricing-run pricing-run-window pricing-run-all pricing-schedule pricing-show-urls pricing-evaluate dashboard-user-run dashboard-user-dev dashboard-user-test dashboard-tech-up dashboard-tech-down dashboard-tech-open dashboard-tech-provision run-all-phases

FEATURE_START_DATE ?= 2024-01-01
FEATURE_END_DATE ?= 2024-01-07
FEATURE_VERSION ?= v1
FEATURE_ZONES ?=
EDA_START_DATE ?= 2024-01-01
EDA_END_DATE ?= 2024-01-07
EDA_FEATURE_VERSION ?= v1
EDA_POLICY_VERSION ?= p1
EDA_ZONES ?=
EDA_RUN_ID ?=
SCORE_FORECAST_START_TS ?=
SCORE_FORECAST_END_TS ?=
PRICE_FORECAST_START_TS ?=
PRICE_FORECAST_END_TS ?=
PRICE_FORECAST_RUN_ID ?=

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'

setup: ## Install dependencies and bootstrap local environment
	@bash scripts/bootstrap_local.sh

up: ## Start local platform with Docker Compose
	@$(COMPOSE) up -d --build

down: ## Stop local platform
	@$(COMPOSE) down

restart: ## Restart local platform
	@$(MAKE) down
	@$(MAKE) up

logs: ## Tail logs for all services
	@$(COMPOSE) logs -f --tail=200

ps: ## Show running services
	@$(COMPOSE) ps

api: api-dev ## Alias for development API server

api-run: ## Run versioned API without auto-reload
	@$(VENV_UVICORN) src.api.main:app --host $(API_HOST) --port $(API_PORT)

api-dev: ## Run versioned API with auto-reload
	@$(VENV_UVICORN) src.api.main:app --host $(API_HOST) --port $(API_PORT) --reload

api-test: ## Run API-focused test suite
	@$(VENV_PYTHON) -m pytest tests/api

api-contract-check: ## Validate API contracts and update snapshot/diff artifacts
	@$(VENV_PYTHON) scripts/check_api_contracts.py

api-lint: ## Run lint checks for API modules and tests
	@$(VENV_PYTHON) -m ruff check src/api tests/api scripts/check_api_contracts.py scripts/check_plain_language_fields.py

api-format: ## Format API modules and tests
	@$(VENV_PYTHON) -m black src/api tests/api scripts/check_api_contracts.py scripts/check_plain_language_fields.py

api-show-urls: ## Print API URLs and key endpoint paths
	@echo "API Base:        http://localhost:$(API_PORT)"
	@echo "OpenAPI JSON:    http://localhost:$(API_PORT)/openapi.json"
	@echo "Swagger UI:      http://localhost:$(API_PORT)/docs"
	@echo "Health:          http://localhost:$(API_PORT)/health"
	@echo "Readiness:       http://localhost:$(API_PORT)/ready"
	@echo "Version:         http://localhost:$(API_PORT)/version"
	@echo "Pricing Latest:  http://localhost:$(API_PORT)/api/v1/pricing/latest"
	@echo "Forecast Latest: http://localhost:$(API_PORT)/api/v1/forecast/latest"
	@echo "Metadata Zones:  http://localhost:$(API_PORT)/api/v1/metadata/zones"

api-smoke: ## Run API-only smoke checks
	@set -euo pipefail; \
	curl -fsS "http://localhost:$(API_PORT)/health" >/dev/null; \
	curl -fsS "http://localhost:$(API_PORT)/ready" >/dev/null; \
	curl -fsS "http://localhost:$(API_PORT)/version" >/dev/null; \
	curl -fsS "http://localhost:$(API_PORT)/api/v1/metadata/schema" >/dev/null; \
	echo "API smoke checks passed."

api-openapi-export: ## Export OpenAPI schema to docs/api/openapi_v1.json
	@$(VENV_PYTHON) -c "import json; from src.api.app import app; from pathlib import Path; p=Path('docs/api/openapi_v1.json'); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(app.openapi(), indent=2, sort_keys=True), encoding='utf-8')"

api-plain-language-check: ## Validate deterministic plain-language mappings
	@$(VENV_PYTHON) scripts/check_plain_language_fields.py

test: ## Run tests with coverage summary
	@$(VENV_PYTHON) -m pytest

lint: ## Run static lint checks
	@$(VENV_PYTHON) -m ruff check src tests

format: ## Auto-format code
	@$(VENV_PYTHON) -m black src tests

typecheck: ## Run mypy type checks
	@$(VENV_PYTHON) -m mypy src

check: ## Run lint, typecheck, and tests
	@$(MAKE) lint
	@$(MAKE) typecheck
	@$(MAKE) test

clean: ## Remove local caches and temporary files
	@find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	@rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov

db-shell: ## Open psql shell in postgres container
	@$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

smoke: ## Run smoke checks against API and core infra
	@set -euo pipefail; \
	check_url() { \
		local name="$$1"; \
		local url="$$2"; \
		local retries="$${3:-30}"; \
		local delay="$${4:-2}"; \
		echo "Checking $$name..."; \
		for _ in $$(seq 1 "$$retries"); do \
			if curl -fsS "$$url" >/dev/null 2>&1; then \
				echo "$$name is healthy."; \
				return 0; \
			fi; \
			sleep "$$delay"; \
		done; \
		echo "$$name failed health check at $$url" >&2; \
		return 1; \
	}; \
	check_url "API health" "http://localhost:$(API_PORT)/health"; \
	check_url "API readiness" "http://localhost:$(API_PORT)/ready"; \
	check_url "MLflow" "http://localhost:5001" 120 2; \
	check_url "Prefect" "http://localhost:4200/api/health"; \
	check_url "Prometheus" "http://localhost:$(PROMETHEUS_PORT)/-/healthy"; \
	check_url "Grafana" "http://localhost:$(GRAFANA_PORT)/api/health"; \
	echo "Smoke checks passed."

mlflow-ui: ## Print MLflow URL
	@echo "MLflow UI: http://localhost:5001"

urls: ## Print local service URLs
	@echo "API:         http://localhost:$(API_PORT)"
	@echo "API Docs:    http://localhost:$(API_PORT)/docs"
	@echo "MLflow:      http://localhost:5001"
	@echo "Prefect:     http://localhost:4200"
	@echo "Prometheus:  http://localhost:$(PROMETHEUS_PORT)"
	@echo "Grafana:     http://localhost:$(GRAFANA_PORT)"

ingest-sample-download: ## Step 1.1 download sample TLC and reference files
	@$(VENV_PYTHON) scripts/fetch_tlc_sample.py

ingest-zone-dim: ## Step 1.3 load zone dimension and coverage report
	@$(VENV_PYTHON) -m src.ingestion.load_zone_dim

ingest-load-sample: ## Step 1.2 load normalized sample trips
	@$(VENV_PYTHON) -m src.ingestion.load_raw_trips

ingest-validate: ## Step 1.4 run ingestion checks with hard gate
	@$(VENV_PYTHON) -m src.ingestion.validate_ingestion

ingest-run-sample: ## Step 1.5 execute idempotent sample ingestion run
	@$(VENV_PYTHON) -m src.ingestion.load_raw_trips

ingest-rerun-sample: ## Step 1.5 rerun sample ingestion to confirm idempotency
	@$(VENV_PYTHON) -m src.ingestion.load_raw_trips

ingest-gate-check: ## Verify Phase 1 gate before historical backfill
	@$(VENV_PYTHON) scripts/check_phase1_gate.py

ingest-backfill-pilot: ## Step 1.6 pilot backfill (gated)
	@$(MAKE) ingest-gate-check
	@$(VENV_PYTHON) -m src.ingestion.backfill_historical --mode pilot

ingest-backfill-full: ## Step 1.6 full backfill (gated)
	@$(MAKE) ingest-gate-check
	@$(VENV_PYTHON) -m src.ingestion.backfill_historical --mode full

ingest-incremental: ## Step 1.6 incremental backfill (gated)
	@$(MAKE) ingest-gate-check
	@$(VENV_PYTHON) -m src.ingestion.backfill_historical --mode incremental

features-time-buckets: ## Step 2.1 build 15-minute time buckets and zone-time spine
	@$(VENV_PYTHON) -m src.features.time_buckets --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)"

features-aggregate: ## Step 2.2 aggregate pickups and zero-fill target table
	@$(VENV_PYTHON) -m src.features.aggregate_pickups --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)"

features-calendar: ## Step 2.3 add deterministic calendar and holiday features
	@$(VENV_PYTHON) -m src.features.calendar_features --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)"

features-lag-roll: ## Step 2.4 add lag and rolling features
	@$(VENV_PYTHON) -m src.features.lag_rolling_features --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)"

features-validate: ## Step 2.5 quality checks (critical checks hard-fail)
	@RUN_ID=$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())"); \
	$(VENV_PYTHON) -m src.features.build_feature_pipeline --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)" --run-id $$RUN_ID --dry-run

features-publish: ## Step 2.5 publish fact_demand_features
	@RUN_ID=$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())"); \
	$(VENV_PYTHON) -m src.features.build_feature_pipeline --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)" --run-id $$RUN_ID

features-build: ## Full chain: 2.1 to 2.5 (build, validate, publish)
	@$(VENV_PYTHON) -m src.features.build_feature_pipeline --start-date $(FEATURE_START_DATE) --end-date $(FEATURE_END_DATE) --feature-version $(FEATURE_VERSION) --zones "$(FEATURE_ZONES)"

eda-seasonality: ## Step 3.1 profile seasonal patterns and zone behavior
	@RUN_ID=$${EDA_RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.eda.profile_seasonality --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)" --run-id $$RUN_ID

eda-sparsity: ## Step 3.2 classify sparse zones with config-driven thresholds
	@RUN_ID=$${EDA_RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.eda.zone_sparsity --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)" --run-id $$RUN_ID

eda-fallback-policy: ## Step 3.2 assign fallback policy by sparsity class
	@$(VENV_PYTHON) -m src.eda.fallback_policy --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)" --run-id "$(EDA_RUN_ID)"

eda-docs: ## Step 3.3 generate assumptions register and governance docs
	@$(VENV_PYTHON) -m src.eda.assumptions_registry --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)" --run-id "$(EDA_RUN_ID)"

eda-validate: ## Validate full EDA flow and critical checks
	@$(VENV_PYTHON) -m src.eda.eda_orchestrator --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)"

eda-run: ## Full Phase 3 orchestration with persisted outputs and docs
	@$(VENV_PYTHON) -m src.eda.eda_orchestrator --start-date $(EDA_START_DATE) --end-date $(EDA_END_DATE) --feature-version $(EDA_FEATURE_VERSION) --policy-version $(EDA_POLICY_VERSION) --zones "$(EDA_ZONES)" --run-id "$(EDA_RUN_ID)"

train-prepare-data: ## Phase 4 prepare training dataset and split manifest
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step prepare-data --run-id $$RUN_ID

train-show-splits: ## Phase 4 print deterministic split manifest
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step show-splits --run-id $$RUN_ID

train-baseline: ## Phase 4 run baseline models
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step baseline --run-id $$RUN_ID

train-candidates: ## Phase 4 train and tune candidate models
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step candidates --run-id $$RUN_ID

train-compare: ## Phase 4 compare baseline and candidate leaderboard
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step compare --run-id $$RUN_ID

train-track: ## Phase 4 list tracked artifacts for run
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step track --run-id $$RUN_ID

train-select-champion: ## Phase 4 evaluate champion gate policy
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step select-champion --run-id $$RUN_ID

train-register: ## Phase 4 register champion to default stage
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step register --run-id $$RUN_ID

train-register-staging: ## Phase 4 register champion directly to Staging
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step register-staging --run-id $$RUN_ID

train-register-production: ## Phase 4 register champion and promote to Production if policy allows
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step register-production --run-id $$RUN_ID --promote-production

train-run-all: ## Phase 4 full chain prepare->baseline->candidates->compare->select->register-staging
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.training_orchestrator --step run-all --run-id $$RUN_ID

train-auto: ## Automated Phase 2 build -> preflight checks -> Phase 4 run-all
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.training.auto_pipeline --run-id $$RUN_ID

score-run: ## Phase 5 run scoring once for current time window
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.scoring.scoring_orchestrator --run-id $$RUN_ID

score-run-window: ## Phase 5 run scoring for explicit forecast start/end overrides
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	if [ -z "$(SCORE_FORECAST_START_TS)" ] || [ -z "$(SCORE_FORECAST_END_TS)" ]; then \
		echo "Set SCORE_FORECAST_START_TS and SCORE_FORECAST_END_TS (ISO8601, end-exclusive)"; \
		exit 1; \
	fi; \
	$(VENV_PYTHON) -m src.scoring.scoring_orchestrator --run-id $$RUN_ID --forecast-start-ts "$(SCORE_FORECAST_START_TS)" --forecast-end-ts "$(SCORE_FORECAST_END_TS)"

score-validate: ## Phase 5 run scoring checks without writing forecasts
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.scoring.scoring_orchestrator --run-id $$RUN_ID --validate-only

score-schedule: ## Phase 5 register Prefect deployment and start a local worker (blocks)
	@$(VENV_PREFECT) work-pool inspect "$${SCORING_PREFECT_WORK_POOL:-scoring-process}" >/dev/null 2>&1 || \
		$(VENV_PREFECT) work-pool create "$${SCORING_PREFECT_WORK_POOL:-scoring-process}" --type process; \
	$(VENV_PREFECT) work-queue create "$${SCORING_PREFECT_WORK_QUEUE:-scoring}" --pool "$${SCORING_PREFECT_WORK_POOL:-scoring-process}" >/dev/null 2>&1 || true; \
	$(VENV_PREFECT) work-queue set-concurrency-limit "$${SCORING_PREFECT_WORK_QUEUE:-scoring}" 1 --pool "$${SCORING_PREFECT_WORK_POOL:-scoring-process}" >/dev/null 2>&1 || true; \
	$(VENV_PYTHON) -m src.scoring.scoring_job --apply-deployment; \
	$(VENV_PREFECT) worker start --pool "$${SCORING_PREFECT_WORK_POOL:-scoring-process}" --work-queue "$${SCORING_PREFECT_WORK_QUEUE:-scoring}"

score-show-urls: ## Print scoring-relevant local URLs
	@echo "MLflow:      http://localhost:5001"
	@echo "Prefect:     http://localhost:4200"
	@echo "API:         http://localhost:$(API_PORT)"
	@echo "Prometheus:  http://localhost:$(PROMETHEUS_PORT)"
	@echo "Grafana:     http://localhost:$(GRAFANA_PORT)"

pricing-load-policy: ## Phase 6 load and validate pricing policy files and snapshots
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step load-policy

pricing-compute-raw: ## Phase 6 compute raw multipliers from forecast + baseline
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step compute-raw

pricing-apply-caps: ## Phase 6 apply floor and cap guardrails
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step apply-caps

pricing-apply-rate-limit: ## Phase 6 apply rate limiter and optional smoothing
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step apply-rate-limit

pricing-reason-codes: ## Phase 6 generate reason codes and summaries
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step reason-codes

pricing-save: ## Phase 6 save pricing decisions (idempotent upsert)
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step save

pricing-validate: ## Phase 6 run quality checks without writing pricing decisions
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step validate

pricing-run: ## Phase 6 run pricing once for latest forecast selection mode
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step save

pricing-run-window: ## Phase 6 run pricing for explicit forecast window (end-exclusive)
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	if [ -z "$(PRICE_FORECAST_START_TS)" ] || [ -z "$(PRICE_FORECAST_END_TS)" ]; then \
		echo "Set PRICE_FORECAST_START_TS and PRICE_FORECAST_END_TS (ISO8601, end-exclusive)"; \
		exit 1; \
	fi; \
	PRICING_FORECAST_SELECTION_MODE=explicit_window \
	PRICING_FORECAST_START_TS="$(PRICE_FORECAST_START_TS)" \
	PRICING_FORECAST_END_TS="$(PRICE_FORECAST_END_TS)" \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step save --forecast-run-id "$(PRICE_FORECAST_RUN_ID)" --forecast-start-ts "$(PRICE_FORECAST_START_TS)" --forecast-end-ts "$(PRICE_FORECAST_END_TS)"

pricing-run-all: ## Phase 6 ordered chain: load-policy -> compute-raw -> apply-caps -> apply-rate-limit -> reason-codes -> validate -> save
	@RUN_ID=$${RUN_ID:-$$($(VENV_PYTHON) -c "import uuid; print(uuid.uuid4())")}; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step load-policy; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step compute-raw; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step apply-caps; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step apply-rate-limit; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step reason-codes; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step validate; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_orchestrator --run-id $$RUN_ID --step save

pricing-schedule: ## Phase 6 register pricing deployment and start local worker (blocks)
	@$(VENV_PREFECT) work-pool inspect "$${PRICING_PREFECT_WORK_POOL:-pricing-process}" >/dev/null 2>&1 || \
		$(VENV_PREFECT) work-pool create "$${PRICING_PREFECT_WORK_POOL:-pricing-process}" --type process; \
	$(VENV_PREFECT) work-queue create "$${PRICING_PREFECT_WORK_QUEUE:-pricing}" --pool "$${PRICING_PREFECT_WORK_POOL:-pricing-process}" >/dev/null 2>&1 || true; \
	$(VENV_PREFECT) work-queue set-concurrency-limit "$${PRICING_PREFECT_WORK_QUEUE:-pricing}" 1 --pool "$${PRICING_PREFECT_WORK_POOL:-pricing-process}" >/dev/null 2>&1 || true; \
	$(VENV_PYTHON) -m src.pricing_guardrails.pricing_job --apply-deployment; \
	$(VENV_PREFECT) worker start --pool "$${PRICING_PREFECT_WORK_POOL:-pricing-process}" --work-queue "$${PRICING_PREFECT_WORK_QUEUE:-pricing}"

pricing-show-urls: ## Print pricing-relevant local URLs
	@echo "MLflow:      http://localhost:5001"
	@echo "Prefect:     http://localhost:4200"
	@echo "API:         http://localhost:$(API_PORT)"
	@echo "Prometheus:  http://localhost:$(PROMETHEUS_PORT)"
	@echo "Grafana:     http://localhost:$(GRAFANA_PORT)"

pricing-evaluate: ## Phase 6 run market evaluation query pack for latest successful pricing run
	@cat sql/pricing_guardrails/market_evaluation_queries.sql | \
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

dashboard-user-run: ## Run Streamlit user decision-support dashboard
	@$(VENV_PYTHON) -m streamlit run src/dashboard_user/app.py

dashboard-user-dev: ## Run Streamlit dashboard in dev-friendly mode
	@STREAMLIT_SERVER_RUN_ON_SAVE=true $(VENV_PYTHON) -m streamlit run src/dashboard_user/app.py

dashboard-user-test: ## Run dashboard user test suite
	@$(VENV_PYTHON) -m pytest tests/dashboard_user

dashboard-tech-up: ## Start Grafana technical dashboard service and apply latest provisioning
	@$(MAKE) dashboard-tech-provision
	@$(COMPOSE) up -d --force-recreate grafana

dashboard-tech-down: ## Stop Grafana technical dashboard service
	@$(COMPOSE) stop grafana

dashboard-tech-open: ## Print Grafana URL and default local credentials
	@echo "Grafana URL:  http://localhost:$(GRAFANA_PORT)"
	@echo "Username:     admin"
	@echo "Password:     admin"

dashboard-tech-provision: ## Sync Grafana dashboards and apply SQL view helpers
	@mkdir -p infra/grafana/provisioning/datasources
	@mkdir -p infra/grafana/provisioning/dashboards
	@cp -f grafana/provisioning/datasources/postgres.yaml infra/grafana/provisioning/datasources/postgres.yaml
	@cp -f grafana/provisioning/dashboards/dashboards.yaml infra/grafana/provisioning/dashboards/dashboards.yaml
	@cp -f grafana/dashboards/*.json infra/grafana/provisioning/dashboards/
	@$(COMPOSE) up -d postgres
	@cat sql/dashboards/observability_views.sql | $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
	@cat sql/dashboards/scoring_metrics_views.sql | $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
	@cat sql/dashboards/pricing_metrics_views.sql | $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
	@cat sql/api/create_api_request_log.sql | $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
	@cat sql/dashboards/api_metrics_views.sql | $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
	@$(COMPOSE) restart grafana >/dev/null 2>&1 || true
	@echo "Dashboard provisioning synced and SQL views applied."

run-all-phases: ## Run Phase 1 through Phase 8 in one ordered command chain (non-blocking)
	@$(MAKE) setup
	@$(MAKE) up
	@$(MAKE) ingest-sample-download
	@$(MAKE) ingest-zone-dim
	@$(MAKE) ingest-load-sample
	@$(MAKE) ingest-validate
	@$(MAKE) ingest-run-sample
	@$(MAKE) ingest-rerun-sample
	@$(MAKE) ingest-gate-check
	@$(MAKE) features-build
	@$(MAKE) eda-run
	@$(MAKE) train-run-all
	@$(MAKE) score-run
	@$(MAKE) pricing-run
	@$(MAKE) api-smoke
	@$(MAKE) dashboard-tech-provision
	@$(MAKE) dashboard-tech-up
	@$(MAKE) dashboard-user-test
	@echo "All phases completed. Launch user dashboard with: make dashboard-user-run"
