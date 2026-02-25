# This file tests metadata endpoints for zones, reason codes, policy, and schema catalog data.
# It exists to protect contracts used by dashboards and non-technical consumers.
# The tests verify deterministic pagination metadata for catalog responses.
# They also ensure schema policy payloads are reachable under the versioned path.

from __future__ import annotations

from datetime import UTC, datetime

from tests.api.support import FakeDBClient, api_test_client, build_test_config


class FakeMetadataService:
    def get_zones(self, **_: object) -> dict[str, object]:
        return {
            "rows": [
                {
                    "zone_id": 101,
                    "zone_name": "Alphabet City",
                    "borough": "Manhattan",
                    "service_zone": "Yellow Zone",
                }
            ],
            "total_count": 1,
            "warnings": None,
        }

    def get_reason_codes(self, **_: object) -> dict[str, object]:
        return {
            "rows": [
                {
                    "reason_code": "CAP_APPLIED_CONFIDENCE",
                    "category": "guardrail",
                    "description": "Cap was applied due to confidence band.",
                    "active_flag": True,
                }
            ],
            "total_count": 1,
            "warnings": None,
        }

    def get_current_policy(self) -> dict[str, object]:
        return {
            "policy_version": "pr1",
            "effective_from": datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            "active_flag": True,
            "policy_summary": {"global_cap_multiplier": 2.0},
        }

    def get_schema_catalog(self) -> dict[str, object]:
        return {
            "api_version_path": "/api/v1",
            "schema_version": "1.0.0",
            "compatibility_policy": {
                "non_breaking_changes": ["adding optional fields"],
                "breaking_changes": ["removing fields"],
            },
            "endpoints": [
                {
                    "endpoint_path": "/api/v1/pricing/latest",
                    "method": "GET",
                    "response_model_name": "PricingDecisionListResponseV1",
                }
            ],
        }


class FakeDiagnosticsService:
    def get_latest_coverage_summary(self) -> dict[str, object]:
        return {
            "pricing_run_id": "run-pr-1",
            "forecast_run_id": "run-fc-1",
            "pricing_zone_count": 2,
            "forecast_zone_count": 2,
            "pricing_row_count": 8,
            "forecast_row_count": 8,
        }

    def get_latest_guardrail_summary(self) -> dict[str, object]:
        return {
            "pricing_run_id": "run-pr-1",
            "total_rows": 8,
            "cap_applied_rows": 2,
            "rate_limited_rows": 1,
            "smoothing_applied_rows": 1,
            "cap_applied_rate": 0.25,
            "rate_limited_rate": 0.125,
        }

    def get_latest_confidence_summary(self) -> dict[str, object]:
        return {
            "forecast_run_id": "run-fc-1",
            "bands": [
                {
                    "uncertainty_band": "low",
                    "row_count": 6,
                    "avg_confidence_score": 0.82,
                },
                {
                    "uncertainty_band": "medium",
                    "row_count": 2,
                    "avg_confidence_score": 0.64,
                },
            ],
        }


def test_metadata_zones_endpoint() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        metadata_service=FakeMetadataService(),
    ) as client:
        response = client.get("/api/v1/metadata/zones?page=1&page_size=2&sort=zone_id:asc")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["zone_id"] == 101
    assert payload["pagination"]["total_count"] == 1


def test_metadata_reason_codes_endpoint() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        metadata_service=FakeMetadataService(),
    ) as client:
        response = client.get("/api/v1/metadata/reason-codes")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["reason_code"] == "CAP_APPLIED_CONFIDENCE"


def test_metadata_policy_and_schema_endpoints() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        metadata_service=FakeMetadataService(),
    ) as client:
        policy = client.get("/api/v1/metadata/policy/current")
        schema = client.get("/api/v1/metadata/schema")

    assert policy.status_code == 200
    assert schema.status_code == 200
    assert policy.json()["data"]["policy_version"] == "pr1"
    assert schema.json()["data"]["api_version_path"] == "/api/v1"


def test_diagnostics_endpoints() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        metadata_service=FakeMetadataService(),
        diagnostics_service=FakeDiagnosticsService(),
    ) as client:
        coverage = client.get("/api/v1/diagnostics/coverage/latest")
        guardrails = client.get("/api/v1/diagnostics/guardrails/latest")
        confidence = client.get("/api/v1/diagnostics/confidence/latest")

    assert coverage.status_code == 200
    assert guardrails.status_code == 200
    assert confidence.status_code == 200
    assert coverage.json()["data"]["pricing_row_count"] == 8
    assert guardrails.json()["data"]["cap_applied_rows"] == 2
    assert confidence.json()["data"]["bands"][0]["uncertainty_band"] == "low"
