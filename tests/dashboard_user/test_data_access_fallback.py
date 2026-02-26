# This test file verifies API-first behavior and DB fallback in the dashboard data access layer.
# It exists so user pages remain functional even when the API is unavailable.
# The checks also confirm plain-language fields are generated during DB fallback mode.
# These tests protect the core resilience requirement of the decision-support dashboard.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.dashboard_user.api_client import ApiUnavailableError
from src.dashboard_user.dashboard_config import DashboardConfig, DashboardFilters
from src.dashboard_user.data_access import DashboardDataAccess


class _ApiDownClient:
    def get_pricing_window(self, **_: object) -> list[dict[str, object]]:
        raise ApiUnavailableError("api unavailable")

    def get_forecast_window(self, **_: object) -> list[dict[str, object]]:
        raise ApiUnavailableError("api unavailable")

    def get_zones(self, **_: object) -> list[dict[str, object]]:
        raise ApiUnavailableError("api unavailable")

    def get_reason_codes(self, **_: object) -> list[dict[str, object]]:
        raise ApiUnavailableError("api unavailable")

    def get_latest_pricing_run(self) -> dict[str, object] | None:
        raise ApiUnavailableError("api unavailable")

    def get_latest_forecast_run(self) -> dict[str, object] | None:
        raise ApiUnavailableError("api unavailable")


class _StubDbClient:
    def get_zone_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            [{"zone_id": 1, "zone_name": "Test Zone", "borough": "Queens", "service_zone": "Boro"}]
        )

    def get_reason_code_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "reason_code": "HIGH_DEMAND",
                    "category": "demand",
                    "description": "Demand is above baseline.",
                    "active_flag": True,
                }
            ]
        )

    def get_latest_pricing_run(self) -> dict[str, object] | None:
        return {"run_id": "pricing-run-1", "status": "success"}

    def get_latest_forecast_run(self) -> dict[str, object] | None:
        return {"run_id": "forecast-run-1", "status": "success"}

    def get_recent_pricing_runs(self, *, limit: int) -> pd.DataFrame:
        return pd.DataFrame(
            [{"run_id": "pricing-run-1", "status": "success", "started_at": "2026-02-25T10:00:00Z"}]
        ).head(limit)

    def get_pricing_window(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "zone_id": 1,
                    "zone_name": "Test Zone",
                    "borough": "Queens",
                    "service_zone": "Boro",
                    "bucket_start_ts": "2026-02-25T10:00:00Z",
                    "pricing_created_at": "2026-02-25T09:55:00Z",
                    "run_id": "pricing-run-1",
                    "pricing_run_key": "pk-1",
                    "forecast_run_id": "forecast-run-1",
                    "final_multiplier": 1.15,
                    "raw_multiplier": 1.22,
                    "pre_cap_multiplier": 1.22,
                    "post_cap_multiplier": 1.15,
                    "confidence_score": 0.55,
                    "uncertainty_band": "low",
                    "y_pred": 25.0,
                    "y_pred_lower": 20.0,
                    "y_pred_upper": 30.0,
                    "cap_applied": True,
                    "cap_type": "upper",
                    "cap_reason": "policy_cap",
                    "rate_limit_applied": False,
                    "rate_limit_direction": "none",
                    "smoothing_applied": False,
                    "primary_reason_code": "HIGH_DEMAND",
                    "reason_codes": ["HIGH_DEMAND"],
                    "reason_summary": "High demand relative to baseline.",
                    "pricing_policy_version": "p1",
                }
            ]
        )

    def get_forecast_window(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "zone_id": 1,
                    "zone_name": "Test Zone",
                    "borough": "Queens",
                    "service_zone": "Boro",
                    "bucket_start_ts": "2026-02-25T10:00:00Z",
                    "forecast_created_at": "2026-02-25T09:50:00Z",
                    "run_id": "forecast-run-1",
                    "forecast_run_key": "fk-1",
                    "horizon_index": 1,
                    "y_pred": 12.0,
                    "y_pred_lower": 9.0,
                    "y_pred_upper": 15.0,
                    "confidence_score": 0.58,
                    "uncertainty_band": "low",
                    "used_recursive_features": False,
                    "model_name": "champion",
                    "model_version": "1",
                    "model_stage": "Production",
                    "feature_version": "v1",
                }
            ]
        )


def _build_filters() -> DashboardFilters:
    return DashboardFilters(
        start_ts=datetime(2026, 2, 25, 9, 0, tzinfo=UTC),
        end_ts=datetime(2026, 2, 25, 12, 0, tzinfo=UTC),
        borough=None,
        zone_id=None,
        uncertainty_band=None,
        cap_only=False,
        rate_limit_only=False,
        low_confidence_only=False,
        pricing_run_mode="latest",
        pricing_run_id=None,
        page_size=200,
    )


def test_pricing_falls_back_to_db_and_generates_plain_language() -> None:
    config = DashboardConfig(
        api_base_url="http://localhost:8000/api/v1",
        database_url="postgresql://unused",
        request_timeout_seconds=5,
        default_page_size=200,
        max_page_size=1000,
        metadata_cache_ttl_seconds=60,
        query_cache_ttl_seconds=60,
        low_confidence_threshold=0.6,
        default_hours_back=4,
        default_hours_forward=2,
        max_run_selector_options=20,
    )
    access = DashboardDataAccess(
        config=config, api_client=_ApiDownClient(), db_client=_StubDbClient()
    )

    dataframe, source = access.get_pricing_data(_build_filters())

    assert source == "db"
    assert not dataframe.empty
    assert dataframe.iloc[0]["why_this_price"]
    assert dataframe.iloc[0]["guardrail_note"]
    assert dataframe.iloc[0]["confidence_note"]
