"""
Integration tests for zone coverage.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

import os

import pytest

from src.common.db import test_connection
from src.ingestion.load_zone_dim import load_zone_dim

if os.getenv("RUN_INGESTION_INTEGRATION") != "1":
    pytest.skip("Set RUN_INGESTION_INTEGRATION=1 to run ingestion integration tests", allow_module_level=True)


@pytest.mark.integration
def test_zone_coverage_report_persists() -> None:
    if not test_connection():
        pytest.skip("Postgres unavailable in local test environment")

    result = load_zone_dim()
    assert "pickup_coverage_pct" in result
    assert "dropoff_coverage_pct" in result
