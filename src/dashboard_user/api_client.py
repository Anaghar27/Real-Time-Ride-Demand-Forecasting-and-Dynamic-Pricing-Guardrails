# This file implements the API-first data client used by the Streamlit dashboard.
# It exists so dashboard pages can call stable Phase 7 endpoints without embedding request details everywhere.
# The client normalizes envelope parsing and converts transport failures into one clear exception type.
# Keeping API calls here makes fallback logic in data_access.py much cleaner.

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests


class ApiUnavailableError(RuntimeError):
    """Raised when the API cannot be reached or responds with server errors."""


class DashboardApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: int = 8,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def get_zones(self, *, page_size: int = 5000) -> list[dict[str, Any]]:
        payload = self._request_json(
            "/metadata/zones",
            params={"page": 1, "page_size": page_size, "sort": "zone_id:asc"},
        )
        return list(payload.get("data", []))

    def get_reason_codes(self, *, page_size: int = 1000) -> list[dict[str, Any]]:
        payload = self._request_json(
            "/metadata/reason-codes",
            params={"active_only": True, "page": 1, "page_size": page_size},
        )
        return list(payload.get("data", []))

    def get_latest_pricing_run(self) -> dict[str, Any] | None:
        return self._safe_run_summary("/pricing/runs/latest")

    def get_pricing_run(self, run_id: str) -> dict[str, Any] | None:
        return self._safe_run_summary(f"/pricing/runs/{run_id}")

    def get_latest_forecast_run(self) -> dict[str, Any] | None:
        return self._safe_run_summary("/forecast/runs/latest")

    def get_forecast_run(self, run_id: str) -> dict[str, Any] | None:
        return self._safe_run_summary(f"/forecast/runs/{run_id}")

    def get_pricing_window(
        self,
        *,
        start_ts: datetime,
        end_ts: datetime,
        zone_id: int | None,
        borough: str | None,
        uncertainty_band: str | None,
        cap_applied: bool | None,
        rate_limit_applied: bool | None,
        page_size: int,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "page": 1,
            "page_size": page_size,
            "sort": "bucket_start_ts:desc",
        }
        if zone_id is not None:
            params["zone_id"] = zone_id
        if borough:
            params["borough"] = borough
        if uncertainty_band:
            params["uncertainty_band"] = uncertainty_band
        if cap_applied is not None:
            params["cap_applied"] = cap_applied
        if rate_limit_applied is not None:
            params["rate_limit_applied"] = rate_limit_applied

        payload = self._request_json("/pricing/window", params=params)
        return list(payload.get("data", []))

    def get_forecast_window(
        self,
        *,
        start_ts: datetime,
        end_ts: datetime,
        zone_id: int | None,
        borough: str | None,
        page_size: int,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "page": 1,
            "page_size": page_size,
            "sort": "bucket_start_ts:asc",
        }
        if zone_id is not None:
            params["zone_id"] = zone_id
        if borough:
            params["borough"] = borough

        payload = self._request_json("/forecast/window", params=params)
        return list(payload.get("data", []))

    def _safe_run_summary(self, path: str) -> dict[str, Any] | None:
        try:
            payload = self._request_json(path, params=None)
        except ApiUnavailableError:
            raise
        except ValueError:
            return None
        data = payload.get("data")
        return dict(data) if isinstance(data, dict) else None

    def _request_json(self, path: str, params: dict[str, Any] | None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise ApiUnavailableError(f"API request failed for {url}: {exc}") from exc

        if response.status_code == 404:
            raise ValueError(f"Endpoint returned 404 for {url}")
        if response.status_code >= 500:
            raise ApiUnavailableError(
                f"API request failed with status {response.status_code} for {url}"
            )
        if response.status_code >= 400:
            raise ValueError(
                f"API request was rejected with status {response.status_code} for {url}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiUnavailableError(f"API did not return valid JSON for {url}") from exc

        if not isinstance(payload, dict):
            raise ApiUnavailableError(f"Unexpected payload shape from {url}")
        return payload
