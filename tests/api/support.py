# This file provides shared helpers for API endpoint tests.
# It exists so tests can override service dependencies without touching real databases.
# The helpers build consistent config objects and scoped TestClient contexts.
# Centralized test wiring keeps API tests small and focused on behavior.

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from fastapi.testclient import TestClient

from src.api.api_config import ApiConfig
from src.api.app import app
from src.api.dependencies import (
    get_config,
    get_database_client,
    get_diagnostics_service,
    get_forecast_service,
    get_metadata_service,
    get_pricing_service,
)


def build_test_config(*, include_plain_language_fields: bool = True) -> ApiConfig:
    """Create deterministic API config for tests."""

    allowed_names = {
        "pricing_decisions",
        "demand_forecast",
        "dim_zone",
        "reason_code_reference",
        "pricing_run_log",
        "scoring_run_log",
        "pricing_policy_snapshot",
        "api_request_log",
        "api_contract_registry",
    }

    return ApiConfig(
        api_name="Test Ride API",
        api_version_path="/api/v1",
        schema_version="1.0.0",
        host="0.0.0.0",
        port=8000,
        environment="test",
        database_url="postgresql+psycopg2://test:test@localhost:5432/test",
        default_page_size=2,
        max_page_size=5,
        default_sort_order="bucket_start_ts:desc",
        request_timeout_seconds=30,
        enable_request_logging=False,
        include_plain_language_fields=include_plain_language_fields,
        allowed_origins=[],
        pricing_table_name="pricing_decisions",
        forecast_table_name="demand_forecast",
        zone_table_name="dim_zone",
        reason_code_table_name="reason_code_reference",
        pricing_run_log_table_name="pricing_run_log",
        forecast_run_log_table_name="scoring_run_log",
        pricing_policy_snapshot_table_name="pricing_policy_snapshot",
        request_log_table_name="api_request_log",
        contract_registry_table_name="api_contract_registry",
        app_version="0.1.0",
        allowed_table_names=allowed_names,
    )


class FakeDBClient:
    """Simple fake DB dependency for health/readiness endpoint tests."""

    def __init__(self, *, connected: bool = True, existing_tables: set[str] | None = None) -> None:
        self._connected = connected
        self._tables = existing_tables or {
            "pricing_decisions",
            "demand_forecast",
            "dim_zone",
            "reason_code_reference",
        }

    def can_connect(self) -> bool:
        return self._connected

    def table_exists(self, table_name: str) -> bool:
        return self._connected and table_name in self._tables

    def log_request(self, **_: Any) -> None:
        return None


@contextmanager
def api_test_client(
    *,
    config: ApiConfig | None = None,
    db_client: Any | None = None,
    pricing_service: Any | None = None,
    forecast_service: Any | None = None,
    metadata_service: Any | None = None,
    diagnostics_service: Any | None = None,
) -> Iterator[TestClient]:
    """Yield a TestClient with scoped dependency overrides."""

    resolved_config = config or build_test_config()

    app.dependency_overrides[get_config] = lambda: resolved_config
    if db_client is not None:
        app.dependency_overrides[get_database_client] = lambda: db_client
    if pricing_service is not None:
        app.dependency_overrides[get_pricing_service] = lambda: pricing_service
    if forecast_service is not None:
        app.dependency_overrides[get_forecast_service] = lambda: forecast_service
    if metadata_service is not None:
        app.dependency_overrides[get_metadata_service] = lambda: metadata_service
    if diagnostics_service is not None:
        app.dependency_overrides[get_diagnostics_service] = lambda: diagnostics_service

    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
