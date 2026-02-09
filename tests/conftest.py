"""Shared test configuration."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def base_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure required environment variables are present during tests."""

    defaults = {
        "PROJECT_NAME": "test-project",
        "ENV": "test",
        "LOG_LEVEL": "INFO",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "ride_demand",
        "POSTGRES_USER": "ride_user",
        "POSTGRES_PASSWORD": "ride_password",
        "DATABASE_URL": "postgresql+psycopg2://ride_user:ride_password@localhost:5432/ride_demand",
        "MLFLOW_TRACKING_URI": "http://localhost:5000",
        "API_HOST": "0.0.0.0",
        "API_PORT": "8000",
        "PREFECT_API_URL": "http://localhost:4200/api",
        "PROMETHEUS_PORT": "9090",
        "GRAFANA_PORT": "3000",
    }

    for key, value in defaults.items():
        if os.getenv(key) is None:
            monkeypatch.setenv(key, value)
