"""
Unit tests for watermark.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok
from src.ingestion.ddl import apply_ingestion_ddl


def test_watermark_upsert_round_trip() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    apply_ingestion_ddl(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ingestion_watermark (dataset_name, latest_successful_period, updated_at)
                VALUES ('yellow_taxi', '2024-01', :updated_at)
                ON CONFLICT (dataset_name)
                DO UPDATE SET latest_successful_period = EXCLUDED.latest_successful_period,
                              updated_at = EXCLUDED.updated_at
                """
            ),
            {"updated_at": datetime.now(tz=UTC)},
        )
        current = connection.execute(
            text(
                "SELECT latest_successful_period FROM ingestion_watermark WHERE dataset_name = 'yellow_taxi'"
            )
        ).scalar()

    assert current == "2024-01"
