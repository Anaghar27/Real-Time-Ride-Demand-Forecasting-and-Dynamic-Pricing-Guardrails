# This file tests plain-language mapping functions and endpoint field toggling.
# It exists to ensure user-facing summaries stay consistent with technical flags.
# The checks cover deterministic threshold mappings required by the API contract.
# This helps prevent contradictory text in downstream dashboards.

from __future__ import annotations

from datetime import UTC, datetime

from src.api.plain_language import (
    confidence_note,
    demand_outlook_label,
    guardrail_note,
    price_action_label,
    pricing_plain_fields,
)
from tests.api.support import FakeDBClient, api_test_client, build_test_config


class PlainLanguagePricingService:
    def get_latest_pricing(self, *, include_plain_language_fields: bool, **_: object) -> dict[str, object]:
        row = {
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
        }
        if include_plain_language_fields:
            row.update(pricing_plain_fields(row))
        return {"rows": [row], "total_count": 1, "warnings": None}

    def get_pricing_window(self, **kwargs: object) -> dict[str, object]:
        return self.get_latest_pricing(**kwargs)

    def get_zone_timeline(self, **kwargs: object) -> dict[str, object]:
        return self.get_latest_pricing(**kwargs)

    def get_latest_run_summary(self) -> dict[str, object] | None:
        return None

    def get_run_summary(self, *, run_id: str) -> dict[str, object] | None:
        return None


def test_plain_language_mapping_thresholds() -> None:
    assert price_action_label(1.0) == "No price change"
    assert price_action_label(1.05) == "Small increase"
    assert price_action_label(1.15) == "Moderate increase"
    assert price_action_label(1.4) == "Larger increase"

    note = guardrail_note(
        cap_applied=True,
        rate_limit_applied=False,
        cap_reason="confidence",
        cap_type="contextual",
    )
    assert "cap" in note.lower()

    assert demand_outlook_label(2.0) == "low"
    assert demand_outlook_label(10.0) == "normal"
    assert demand_outlook_label(20.0) == "elevated"
    assert demand_outlook_label(35.0) == "high"

    assert "High confidence" in confidence_note(0.9, "low")


def test_plain_language_fields_present_when_enabled() -> None:
    with api_test_client(
        config=build_test_config(include_plain_language_fields=True),
        db_client=FakeDBClient(),
        pricing_service=PlainLanguagePricingService(),
    ) as client:
        response = client.get("/api/v1/pricing/latest")

    assert response.status_code == 200
    row = response.json()["data"][0]
    assert row["recommended_price_action"] == "Small increase"
    assert "guardrails" in row["why_this_price"].lower()
    assert "cap" in row["guardrail_note"].lower()


def test_plain_language_fields_absent_when_disabled() -> None:
    with api_test_client(
        config=build_test_config(include_plain_language_fields=False),
        db_client=FakeDBClient(),
        pricing_service=PlainLanguagePricingService(),
    ) as client:
        response = client.get("/api/v1/pricing/latest")

    assert response.status_code == 200
    row = response.json()["data"][0]
    assert "recommended_price_action" not in row
    assert "why_this_price" not in row
    assert "guardrail_note" not in row
