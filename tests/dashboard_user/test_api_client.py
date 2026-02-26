# This test file validates the dashboard API client behavior against expected envelope patterns.
# It exists so request parsing and error handling stay stable as endpoints evolve.
# The tests focus on success payload extraction and clear failure modes.
# Keeping these checks small makes dashboard integration regressions easier to catch.

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import requests

from src.dashboard_user.api_client import ApiUnavailableError, DashboardApiClient


class _FakeResponse:
    def __init__(self, *, status_code: int, payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(
        self, responses: list[_FakeResponse] | None = None, raise_error: Exception | None = None
    ) -> None:
        self.responses = responses or []
        self.raise_error = raise_error
        self.calls: list[tuple[str, dict[str, Any] | None, int]] = []

    def get(self, url: str, params: dict[str, Any] | None, timeout: int) -> _FakeResponse:
        self.calls.append((url, params, timeout))
        if self.raise_error is not None:
            raise self.raise_error
        return self.responses.pop(0)


def test_get_pricing_window_parses_rows() -> None:
    session = _FakeSession(
        responses=[
            _FakeResponse(
                status_code=200,
                payload={
                    "data": [
                        {
                            "zone_id": 1,
                            "bucket_start_ts": "2026-02-25T10:00:00Z",
                            "final_multiplier": 1.12,
                        }
                    ]
                },
            )
        ]
    )
    client = DashboardApiClient(base_url="http://localhost:8000/api/v1", session=session)

    rows = client.get_pricing_window(
        start_ts=datetime(2026, 2, 25, 9, 0, tzinfo=UTC),
        end_ts=datetime(2026, 2, 25, 11, 0, tzinfo=UTC),
        zone_id=None,
        borough=None,
        uncertainty_band=None,
        cap_applied=None,
        rate_limit_applied=None,
        page_size=50,
    )

    assert len(rows) == 1
    assert rows[0]["zone_id"] == 1


def test_get_latest_pricing_run_returns_none_on_404() -> None:
    session = _FakeSession(
        responses=[_FakeResponse(status_code=404, payload={"detail": "not found"})]
    )
    client = DashboardApiClient(base_url="http://localhost:8000/api/v1", session=session)

    result = client.get_latest_pricing_run()

    assert result is None


def test_api_transport_error_raises_unavailable() -> None:
    session = _FakeSession(raise_error=requests.ConnectionError("api down"))
    client = DashboardApiClient(base_url="http://localhost:8000/api/v1", session=session)

    with pytest.raises(ApiUnavailableError):
        client.get_zones()
