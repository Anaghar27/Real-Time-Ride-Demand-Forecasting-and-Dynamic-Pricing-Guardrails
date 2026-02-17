"""
Unit tests for checks.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

import pandas as pd
import pytest

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok
from src.ingestion.checks import run_ingestion_checks


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pickup_datetime": [pd.Timestamp("2024-01-01 10:00:00")],
            "dropoff_datetime": [pd.Timestamp("2024-01-01 10:15:00")],
            "pickup_location_id": [100],
            "dropoff_location_id": [101],
            "fare_amount": [5.5],
            "trip_distance": [1.2],
            "source_file": ["sample.parquet"],
            "source_row_number": [1],
            "ingest_batch_id": ["batch-1"],
        }
    )


def test_checks_pass_for_valid_data() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    passed, results, rejects = run_ingestion_checks(_valid_frame(), engine)

    assert passed is True
    assert len(results) > 0
    assert rejects.empty


def test_checks_fail_for_negative_fare() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    frame = _valid_frame()
    frame.loc[0, "fare_amount"] = -1.0

    passed, _, rejects = run_ingestion_checks(frame, engine)

    assert passed is False
    assert len(rejects) == 1
