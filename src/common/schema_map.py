"""
Schema normalization for NYC TLC trip datasets.
It centralizes cross-cutting concerns like settings, logging, and database access used by the pipelines.
Keeping these helpers isolated reduces duplication and keeps domain modules focused on business logic.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

COLUMN_ALIASES = {
    "vendorid": "vendor_id",
    "vendor_id": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "pickup_datetime": "pickup_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "dropoff_datetime": "dropoff_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "pulocationid": "pickup_location_id",
    "pickup_location_id": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "dropoff_location_id": "dropoff_location_id",
    "ratecodeid": "rate_code_id",
    "rate_code_id": "rate_code_id",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "total_amount": "total_amount",
    "payment_type": "payment_type",
    "store_and_fwd_flag": "store_and_fwd_flag",
}

CANONICAL_TRIP_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
    "store_and_fwd_flag",
]

TRIP_COLUMNS_WITH_META = CANONICAL_TRIP_COLUMNS + [
    "ingest_batch_id",
    "source_file",
    "source_row_number",
    "ingested_at",
]


def _to_snake_case(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_")
    return normalized.lower()


def normalize_trip_dataframe(df: pd.DataFrame, source_file: Path, ingest_batch_id: str) -> pd.DataFrame:
    """Normalize TLC trip dataframe to canonical schema and metadata."""

    renamed_columns = {
        column: COLUMN_ALIASES.get(_to_snake_case(column), _to_snake_case(column)) for column in df.columns
    }
    normalized = df.rename(columns=renamed_columns).copy()

    for column in CANONICAL_TRIP_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized["pickup_datetime"] = pd.to_datetime(normalized["pickup_datetime"], errors="coerce")
    normalized["dropoff_datetime"] = pd.to_datetime(normalized["dropoff_datetime"], errors="coerce")

    int_columns = [
        "vendor_id",
        "pickup_location_id",
        "dropoff_location_id",
        "rate_code_id",
        "passenger_count",
        "payment_type",
    ]
    for column in int_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    float_columns = ["trip_distance", "fare_amount", "total_amount"]
    for column in float_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["store_and_fwd_flag"] = normalized["store_and_fwd_flag"].astype("string")

    normalized["ingest_batch_id"] = ingest_batch_id
    normalized["source_file"] = str(source_file)
    normalized["source_row_number"] = pd.Series(range(1, len(normalized) + 1), dtype="int64")
    normalized["ingested_at"] = datetime.now(tz=UTC)

    return normalized[TRIP_COLUMNS_WITH_META]
