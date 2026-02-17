"""
Integration tests for feature table uniqueness.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok

if os.getenv("RUN_FEATURE_INTEGRATION") != "1":
    pytest.skip("Set RUN_FEATURE_INTEGRATION=1 to run feature integration tests", allow_module_level=True)


@pytest.mark.integration
def test_feature_table_uniqueness() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    with engine.begin() as connection:
        duplicates = int(
            connection.execute(
                text(
                    """
                    SELECT COALESCE(SUM(cnt - 1), 0)
                    FROM (
                        SELECT zone_id, bucket_start_ts, COUNT(*) AS cnt
                        FROM fact_demand_features
                        GROUP BY zone_id, bucket_start_ts
                        HAVING COUNT(*) > 1
                    ) d
                    """
                )
            ).scalar()
            or 0
        )

    assert duplicates == 0
