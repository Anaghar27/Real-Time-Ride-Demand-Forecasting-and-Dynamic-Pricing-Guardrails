"""
Unit tests for schema map.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from pathlib import Path

import pandas as pd

from src.common.schema_map import TRIP_COLUMNS_WITH_META, normalize_trip_dataframe


def test_normalize_trip_dataframe_schema() -> None:
    sample = pd.DataFrame(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": ["2024-01-01 00:00:00"],
            "tpep_dropoff_datetime": ["2024-01-01 00:20:00"],
            "PULocationID": [132],
            "DOLocationID": [231],
            "trip_distance": [2.1],
            "fare_amount": [10.5],
            "total_amount": [14.2],
        }
    )
    normalized = normalize_trip_dataframe(sample, Path("sample.parquet"), "batch-1")

    assert list(normalized.columns) == TRIP_COLUMNS_WITH_META
    assert normalized.iloc[0]["pickup_location_id"] == 132
    assert normalized.iloc[0]["source_row_number"] == 1
