"""
Integration tests for rerun idempotency.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok
from src.features.build_feature_pipeline import build_feature_pipeline

if os.getenv("RUN_FEATURE_INTEGRATION") != "1":
    pytest.skip("Set RUN_FEATURE_INTEGRATION=1 to run feature integration tests", allow_module_level=True)


@pytest.mark.integration
def test_rerun_idempotency() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    build_feature_pipeline(
        start_date="2024-01-01",
        end_date="2024-01-01",
        zones=None,
        feature_version="it_v1_rerun",
        dry_run=False,
    )

    with engine.begin() as connection:
        before = connection.execute(
            text(
                """
                SELECT
                    COUNT(*) AS row_count,
                    COALESCE(SUM(pickup_count), 0) AS pickup_sum,
                    COALESCE(SUM(lag_1), 0) AS lag_1_sum,
                    COALESCE(SUM(roll_mean_4), 0) AS roll_mean_4_sum
                FROM fact_demand_features
                WHERE feature_version = 'it_v1_rerun'
                """
            )
        ).mappings().one()

    build_feature_pipeline(
        start_date="2024-01-01",
        end_date="2024-01-01",
        zones=None,
        feature_version="it_v1_rerun",
        dry_run=False,
    )

    with engine.begin() as connection:
        after = connection.execute(
            text(
                """
                SELECT
                    COUNT(*) AS row_count,
                    COALESCE(SUM(pickup_count), 0) AS pickup_sum,
                    COALESCE(SUM(lag_1), 0) AS lag_1_sum,
                    COALESCE(SUM(roll_mean_4), 0) AS roll_mean_4_sum
                FROM fact_demand_features
                WHERE feature_version = 'it_v1_rerun'
                """
            )
        ).mappings().one()

    assert int(before["row_count"]) == int(after["row_count"])
    assert float(before["pickup_sum"]) == float(after["pickup_sum"])
    assert float(before["lag_1_sum"]) == float(after["lag_1_sum"])
    assert float(before["roll_mean_4_sum"]) == pytest.approx(float(after["roll_mean_4_sum"]), abs=1e-9)
