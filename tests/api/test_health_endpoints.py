# This file tests API health, readiness, and version endpoints.
# It exists to validate operational contracts used by orchestration and monitoring.
# The tests confirm request IDs and version metadata are always returned.
# Keeping these checks stable helps prevent accidental regressions in base API availability.

from __future__ import annotations

from tests.api.support import FakeDBClient, api_test_client, build_test_config


def test_health_endpoint_returns_expected_fields() -> None:
    config = build_test_config()
    with api_test_client(config=config, db_client=FakeDBClient()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["environment"] == "test"
    assert payload["service_name"] == config.api_name
    assert payload["api_version"] == "v1"
    assert payload["schema_version"] == config.schema_version
    assert payload["request_id"]
    assert "timestamp" in payload


def test_ready_endpoint_reflects_source_status() -> None:
    config = build_test_config()
    db_client = FakeDBClient(connected=True)
    with api_test_client(config=config, db_client=db_client) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    payload = response.json()
    assert payload["db_connected"] is True
    assert payload["pricing_source_ready"] is True
    assert payload["forecast_source_ready"] is True
    assert payload["ready"] is True


def test_version_endpoint_returns_version_metadata() -> None:
    config = build_test_config()
    with api_test_client(config=config, db_client=FakeDBClient()) as client:
        response = client.get("/version")

    assert response.status_code == 200
    payload = response.json()
    assert payload["api_version_path"] == "/api/v1"
    assert payload["schema_version"] == config.schema_version
    assert payload["app_version"] == config.app_version
    assert payload["project"] == config.api_name
    assert payload["version"] == config.app_version
