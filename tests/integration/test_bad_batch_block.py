"""
Integration tests for bad batch block.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

import os

import pandas as pd
import pytest

from src.common.db import engine, test_connection
from src.ingestion.checks import run_ingestion_checks

if os.getenv("RUN_INGESTION_INTEGRATION") != "1":
    pytest.skip("Set RUN_INGESTION_INTEGRATION=1 to run ingestion integration tests", allow_module_level=True)


@pytest.mark.integration
def test_bad_batch_fails_checks() -> None:
    if not test_connection():
        pytest.skip("Postgres unavailable in local test environment")

    bad_df = pd.DataFrame(
        {
            "pickup_datetime": [pd.Timestamp("2024-01-01 11:00:00")],
            "dropoff_datetime": [pd.Timestamp("2024-01-01 10:59:00")],
            "pickup_location_id": [100],
            "dropoff_location_id": [101],
            "fare_amount": [4.0],
            "trip_distance": [1.0],
            "source_file": ["bad.parquet"],
            "source_row_number": [1],
            "ingest_batch_id": ["bad-batch"],
        }
    )

    passed, _, _ = run_ingestion_checks(bad_df, engine)
    assert passed is False
