from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok

if os.getenv("RUN_FEATURE_INTEGRATION") != "1":
    pytest.skip("Set RUN_FEATURE_INTEGRATION=1 to run feature integration tests", allow_module_level=True)


@pytest.mark.integration
def test_zero_fill_completeness() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    COUNT(*) AS actual_rows,
                    (
                        SELECT COUNT(*) FROM fct_zone_time_spine_15m
                    ) AS expected_rows
                FROM fct_zone_demand_15m
                """
            )
        ).mappings().one()

    assert int(row["actual_rows"]) == int(row["expected_rows"])
