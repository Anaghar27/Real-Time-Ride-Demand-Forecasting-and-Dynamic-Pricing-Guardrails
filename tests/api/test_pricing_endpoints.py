# This file tests pricing endpoints for list retrieval, filtering behavior, and run summaries.
# It exists to confirm pricing routes expose stable machine-readable contracts.
# The tests also verify request validation and error payload consistency.
# This coverage helps catch contract regressions before deployment.

from __future__ import annotations

from datetime import UTC, datetime

from src.api.error_handlers import APIError
from tests.api.support import FakeDBClient, api_test_client, build_test_config


class FakePricingService:
    def get_latest_pricing(self, **_: object) -> dict[str, object]:
        return {
            "rows": [
                {
                    "zone_id": 101,
                    "bucket_start_ts": datetime(2026, 2, 25, 10, 0, tzinfo=UTC),
                    "pricing_run_key": "pk1",
                    "run_id": "run-pr-1",
                    "forecast_run_id": "run-fc-1",
                    "zone_name": "Alphabet City",
                    "borough": "Manhattan",
                    "service_zone": "Yellow Zone",
                    "final_multiplier": 1.08,
                    "raw_multiplier": 1.1,
                    "pre_cap_multiplier": 1.1,
                    "post_cap_multiplier": 1.08,
                    "confidence_score": 0.82,
                    "uncertainty_band": "low",
                    "y_pred": 25.0,
                    "y_pred_lower": 21.0,
                    "y_pred_upper": 29.0,
                    "cap_applied": True,
                    "cap_type": "contextual",
                    "cap_reason": "confidence",
                    "rate_limit_applied": False,
                    "rate_limit_direction": "none",
                    "smoothing_applied": False,
                    "primary_reason_code": "CAP_APPLIED_CONFIDENCE",
                    "reason_codes": ["CAP_APPLIED_CONFIDENCE"],
                    "reason_summary": "Cap applied due to confidence band.",
                    "pricing_policy_version": "pr1",
                    "recommended_price_action": "Small increase",
                    "why_this_price": "Small increase was recommended and then adjusted by guardrails.",
                    "guardrail_note": "A pricing cap was applied due to confidence.",
                    "confidence_note": "High confidence forecast with low uncertainty band.",
                }
            ],
            "total_count": 1,
            "warnings": None,
        }

    def get_pricing_window(self, **_: object) -> dict[str, object]:
        return self.get_latest_pricing()

    def get_zone_timeline(self, *, zone_id: int, **_: object) -> dict[str, object]:
        if zone_id == 9999:
            raise APIError(status_code=404, error_code="ZONE_NOT_FOUND", message="Unknown zone")
        return self.get_latest_pricing()

    def get_latest_run_summary(self) -> dict[str, object] | None:
        return {
            "run_id": "run-pr-1",
            "status": "success",
            "started_at": datetime(2026, 2, 25, 9, 0, tzinfo=UTC),
            "ended_at": datetime(2026, 2, 25, 9, 1, tzinfo=UTC),
            "failure_reason": None,
            "pricing_policy_version": "pr1",
            "forecast_run_id": "run-fc-1",
            "target_bucket_start": datetime(2026, 2, 25, 9, 0, tzinfo=UTC),
            "target_bucket_end": datetime(2026, 2, 25, 10, 0, tzinfo=UTC),
            "zone_count": 2,
            "row_count": 8,
            "cap_applied_count": 3,
            "rate_limited_count": 1,
            "low_confidence_count": 0,
            "latency_ms": 221.0,
        }

    def get_run_summary(self, *, run_id: str) -> dict[str, object] | None:
        if run_id == "missing":
            return None
        return self.get_latest_run_summary()


def test_pricing_latest_endpoint_contract() -> None:
    with api_test_client(
        config=build_test_config(include_plain_language_fields=True),
        db_client=FakeDBClient(),
        pricing_service=FakePricingService(),
    ) as client:
        response = client.get("/api/v1/pricing/latest?page=1&page_size=2&sort=bucket_start_ts:desc")

    assert response.status_code == 200
    payload = response.json()
    assert payload["api_version"] == "v1"
    assert payload["schema_version"] == "1.0.0"
    assert payload["pagination"]["total_count"] == 1
    assert payload["data"][0]["zone_id"] == 101
    assert payload["data"][0]["recommended_price_action"] == "Small increase"


def test_pricing_window_invalid_time_returns_400() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=FakePricingService(),
    ) as client:
        response = client.get(
            "/api/v1/pricing/window?start_ts=2026-02-26T00:00:00Z&end_ts=2026-02-25T00:00:00Z"
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["error_code"] == "INVALID_TIME_WINDOW"


def test_pricing_zone_unknown_returns_404() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=FakePricingService(),
    ) as client:
        response = client.get(
            "/api/v1/pricing/zone/9999?start_ts=2026-02-25T00:00:00Z&end_ts=2026-02-25T01:00:00Z"
        )

    assert response.status_code == 404
    payload = response.json()
    assert payload["error_code"] == "ZONE_NOT_FOUND"


def test_pricing_runs_endpoints() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=FakePricingService(),
    ) as client:
        latest = client.get("/api/v1/pricing/runs/latest")
        specific = client.get("/api/v1/pricing/runs/run-pr-1")
        missing = client.get("/api/v1/pricing/runs/missing")

    assert latest.status_code == 200
    assert specific.status_code == 200
    assert latest.json()["data"]["run_id"] == "run-pr-1"
    assert missing.status_code == 404


def test_pricing_invalid_sort_returns_400() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=FakePricingService(),
    ) as client:
        response = client.get("/api/v1/pricing/latest?sort=invalid_field:desc")

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_QUERY_PARAM"
