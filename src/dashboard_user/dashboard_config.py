# This file defines runtime configuration and filter state for the user dashboard.
# It exists so API settings, cache policies, and UI defaults can be tuned through environment variables.
# Keeping these values centralized avoids hard-coded behavior scattered across the app.
# The dataclasses also make filter inputs explicit and easy to test.

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv


@dataclass(frozen=True)
class DashboardConfig:
    api_base_url: str
    database_url: str | None
    request_timeout_seconds: int
    default_page_size: int
    max_page_size: int
    metadata_cache_ttl_seconds: int
    query_cache_ttl_seconds: int
    low_confidence_threshold: float
    default_hours_back: int
    default_hours_forward: int
    max_run_selector_options: int

    def clamp_page_size(self, requested_page_size: int | None) -> int:
        if requested_page_size is None:
            return self.default_page_size
        return max(1, min(int(requested_page_size), self.max_page_size))


@dataclass(frozen=True)
class DashboardFilters:
    start_ts: datetime
    end_ts: datetime
    borough: str | None
    zone_id: int | None
    uncertainty_band: str | None
    cap_only: bool
    rate_limit_only: bool
    low_confidence_only: bool
    pricing_run_mode: str
    pricing_run_id: str | None
    page_size: int


def default_time_window(config: DashboardConfig) -> tuple[datetime, datetime]:
    now = datetime.now(tz=UTC)
    return now - timedelta(hours=config.default_hours_back), now + timedelta(
        hours=config.default_hours_forward
    )


def load_dashboard_config(*, load_env: bool = True) -> DashboardConfig:
    if load_env:
        load_dotenv()

    api_base_url = os.getenv("DASHBOARD_API_BASE_URL")
    if not api_base_url:
        api_host = os.getenv("API_HOST", "localhost")
        api_port = os.getenv("API_PORT", "8000")
        api_base_url = f"http://{api_host}:{api_port}/api/v1"

    database_url = os.getenv("DASHBOARD_DATABASE_URL") or os.getenv("DATABASE_URL") or None

    return DashboardConfig(
        api_base_url=api_base_url.rstrip("/"),
        database_url=database_url,
        request_timeout_seconds=int(os.getenv("DASHBOARD_REQUEST_TIMEOUT_SECONDS", "8")),
        default_page_size=int(os.getenv("DASHBOARD_DEFAULT_PAGE_SIZE", "500")),
        max_page_size=int(os.getenv("DASHBOARD_MAX_PAGE_SIZE", "2000")),
        metadata_cache_ttl_seconds=int(os.getenv("DASHBOARD_METADATA_CACHE_TTL_SECONDS", "300")),
        query_cache_ttl_seconds=int(os.getenv("DASHBOARD_QUERY_CACHE_TTL_SECONDS", "90")),
        low_confidence_threshold=float(os.getenv("DASHBOARD_LOW_CONFIDENCE_THRESHOLD", "0.6")),
        default_hours_back=int(os.getenv("DASHBOARD_DEFAULT_HOURS_BACK", "6")),
        default_hours_forward=int(os.getenv("DASHBOARD_DEFAULT_HOURS_FORWARD", "3")),
        max_run_selector_options=int(os.getenv("DASHBOARD_MAX_RUN_SELECTOR_OPTIONS", "50")),
    )
