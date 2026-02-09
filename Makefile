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

.PHONY: help setup up down restart logs ps api test lint format typecheck check clean db-shell smoke mlflow-ui urls

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

api: ## Run API locally without Docker
	@$(VENV_UVICORN) src.api.main:app --host $(API_HOST) --port $(API_PORT) --reload

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
