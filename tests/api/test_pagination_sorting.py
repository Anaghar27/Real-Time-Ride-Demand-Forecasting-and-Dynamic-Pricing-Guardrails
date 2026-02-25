# This file tests pagination and sorting behavior for deterministic list responses.
# It exists to validate both helper-level normalization and endpoint-level metadata output.
# The tests cover partial-page behavior and invalid sort/page size validation.
# This keeps list contracts reliable for consumers that paginate programmatically.

from __future__ import annotations

from datetime import UTC, datetime

from src.api.pagination import normalize_pagination, parse_sort
from tests.api.support import FakeDBClient, api_test_client, build_test_config


class CapturingPricingService:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] = {}

    def get_latest_pricing(self, **kwargs: object) -> dict[str, object]:
        self.last_kwargs = kwargs
        page = int(kwargs["page"])
        if page == 2:
            rows = [
                {
                    "zone_id": 102,
                    "bucket_start_ts": datetime(2026, 2, 25, 10, 15, tzinfo=UTC),
                    "pricing_run_key": "pk2",
                    "run_id": "run-pr-1",
                    "forecast_run_id": "run-fc-1",
                    "zone_name": "Astoria",
                    "borough": "Queens",
                    "service_zone": "Yellow Zone",
                    "final_multiplier": 1.0,
                    "raw_multiplier": 1.0,
                    "pre_cap_multiplier": 1.0,
                    "post_cap_multiplier": 1.0,
                    "confidence_score": 0.7,
                    "uncertainty_band": "medium",
                    "y_pred": 12.0,
                    "y_pred_lower": 10.0,
                    "y_pred_upper": 15.0,
                    "cap_applied": False,
                    "cap_type": None,
                    "cap_reason": None,
                    "rate_limit_applied": False,
                    "rate_limit_direction": "none",
                    "smoothing_applied": False,
                    "primary_reason_code": "NORMAL_DEMAND_BASELINE",
                    "reason_codes": ["NORMAL_DEMAND_BASELINE"],
                    "reason_summary": "Normal demand baseline.",
                    "pricing_policy_version": "pr1",
                    "recommended_price_action": "No price change",
                    "why_this_price": "No price change was recommended from forecasted demand signals.",
                    "guardrail_note": "No cap or rate limit guardrail was applied.",
                    "confidence_note": "Medium confidence forecast with medium uncertainty band.",
                }
            ]
        else:
            rows = []
        return {"rows": rows, "total_count": 3, "warnings": None}

    def get_pricing_window(self, **kwargs: object) -> dict[str, object]:
        return self.get_latest_pricing(**kwargs)

    def get_zone_timeline(self, **kwargs: object) -> dict[str, object]:
        return self.get_latest_pricing(**kwargs)

    def get_latest_run_summary(self) -> dict[str, object] | None:
        return None

    def get_run_summary(self, *, run_id: str) -> dict[str, object] | None:
        return None


def test_normalize_pagination_and_sort_helpers() -> None:
    pagination = normalize_pagination(
        page=2,
        page_size=2,
        limit=None,
        default_page_size=10,
        max_page_size=100,
    )
    sort = parse_sort(
        requested_sort="bucket_start_ts:desc",
        default_sort="zone_id:asc",
        allowed_fields={"bucket_start_ts", "zone_id"},
    )

    assert pagination.page == 2
    assert pagination.page_size == 2
    assert pagination.offset == 2
    assert sort.field == "bucket_start_ts"
    assert sort.order == "desc"


def test_partial_page_pagination_metadata_is_deterministic() -> None:
    fake_service = CapturingPricingService()
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=fake_service,
    ) as client:
        response = client.get("/api/v1/pricing/latest?page=2&page_size=2&sort=zone_id:asc")

    assert response.status_code == 200
    payload = response.json()
    assert payload["pagination"]["page"] == 2
    assert payload["pagination"]["page_size"] == 2
    assert payload["pagination"]["total_count"] == 3
    assert payload["pagination"]["total_pages"] == 2
    assert payload["pagination"]["sort"] == "zone_id:asc"
    assert fake_service.last_kwargs["page"] == 2
    assert fake_service.last_kwargs["page_size"] == 2


def test_invalid_page_size_returns_400() -> None:
    with api_test_client(
        config=build_test_config(),
        db_client=FakeDBClient(),
        pricing_service=CapturingPricingService(),
    ) as client:
        response = client.get("/api/v1/pricing/latest?page_size=99")

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_QUERY_PARAM"
