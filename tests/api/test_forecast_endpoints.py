# This file tests forecast endpoints for latest rows, windows, zone timelines, and run summaries.
# It exists to protect the forecast API contract expected by downstream pricing and dashboards.
# The test suite verifies plain-language fields and deterministic envelope metadata.
# These checks reduce risk of accidental behavior regressions during future refactors.

from __future__ import annotations

from datetime import UTC, datetime

from src.api.error_handlers import APIError
from tests.api.support import FakeDBClient, api_test_client, build_test_config


class FakeForecastService:
    def get_latest_forecast(self, **_: object) -> dict[str, object]:
        return {
            "rows": [
                {
                    "zone_id": 101,
                    "bucket_start_ts": datetime(2026, 2, 25, 10, 0, tzinfo=UTC),
                    "forecast_run_key": "fk1",
                    "run_id": "run-fc-1",
                    "horizon_index": 1,
                    "zone_name": "Alphabet City",
                    "borough": "Manhattan",
                    "service_zone": "Yellow Zone",
                    "y_pred": 24.0,
                    "y_pred_lower": 20.0,
                    "y_pred_upper": 28.0,
                    "confidence_score": 0.77,
                    "uncertainty_band": "medium",
                    "used_recursive_features": False,
                    "model_name": "lgbm",
                    "model_version": "1",
                    "model_stage": "Production",
                    "feature_version": "v1",
                    "demand_outlook_label": "elevated",
                    "confidence_note": "Medium confidence forecast with medium uncertainty band.",
                    "forecast_range_summary": "Expected demand range is 20.00 to 28.00.",
                }
            ],
            "total_count": 1,
            "warnings": None,
        }

    def get_forecast_window(self, **_: object) -> dict[str, object]:
        return self.get_latest_forecast()

    def get_zone_timeline(self, *, zone_id: int, **_: object) -> dict[str, object]:
        if zone_id == 9999:
            raise APIError(status_code=404, error_code="ZONE_NOT_FOUND", message="Unknown zone")
        return self.get_latest_forecast()

    def get_latest_run_summary(self) -> dict[str, object] | None:
        return {
            "run_id": "run-fc-1",
            "status": "success",
            "started_at": datetime(2026, 2, 25, 9, 0, tzinfo=UTC),
            "ended_at": datetime(2026, 2, 25, 9, 1, tzinfo=UTC),
            "failure_reason": None,
            "model_name": "lgbm",
            "model_version": "1",
            "model_stage": "Production",
            "feature_version": "v1",
            "forecast_run_key": "fk1",
            "forecast_start_ts": datetime(2026, 2, 25, 10, 0, tzinfo=UTC),
            "forecast_end_ts": datetime(2026, 2, 25, 11, 0, tzinfo=UTC),
            "horizon_buckets": 4,
            "bucket_minutes": 15,
            "zone_count": 2,
            "row_count": 8,
            "latency_ms": 200.0,
        }

    def get_run_summary(self, *, run_id: str) -> dict[str, object] | None:
        if run_id == "missing":
            return None
        return self.get_latest_run_summary()


def test_forecast_latest_endpoint_contract() -> None:
    with api_test_client(
        config=build_test_config(include_plain_language_fields=True),
        db_client=FakeDBClient(),
        forecast_service=FakeForecastService(),
    ) as client:
        response = client.get("/api/v1/forecast/latest?page=1&page_size=2&sort=bucket_start_ts:desc")

    assert response.status_code == 200
    payload = response.json()
    assert payload["api_version"] == "v1"
    assert payload["schema_version"] == "1.0.0"
    assert payload["pagination"]["total_count"] == 1
    assert payload["data"][0]["demand_outlook_label"] == "elevated"


def test_forecast_window_invalid_time_returns_400() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        forecast_service=FakeForecastService(),
    ) as client:
        response = client.get(
            "/api/v1/forecast/window?start_ts=2026-02-26T00:00:00Z&end_ts=2026-02-25T00:00:00Z"
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["error_code"] == "INVALID_TIME_WINDOW"


def test_forecast_zone_unknown_returns_404() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        forecast_service=FakeForecastService(),
    ) as client:
        response = client.get(
            "/api/v1/forecast/zone/9999?start_ts=2026-02-25T00:00:00Z&end_ts=2026-02-25T01:00:00Z"
        )

    assert response.status_code == 404
    payload = response.json()
    assert payload["error_code"] == "ZONE_NOT_FOUND"


def test_forecast_runs_endpoints() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        forecast_service=FakeForecastService(),
    ) as client:
        latest = client.get("/api/v1/forecast/runs/latest")
        specific = client.get("/api/v1/forecast/runs/run-fc-1")
        missing = client.get("/api/v1/forecast/runs/missing")

    assert latest.status_code == 200
    assert specific.status_code == 200
    assert latest.json()["data"]["run_id"] == "run-fc-1"
    assert missing.status_code == 404
