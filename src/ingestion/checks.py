"""
Batch-level data quality checks for ingestion pipeline.
It supports an idempotent ingestion workflow that loads raw TLC data and reference tables into Postgres.
It is typically invoked via the Phase 1 Make targets and should be safe to re-run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

KEY_COLUMNS = ["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id"]


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    metric_value: float
    threshold_value: float
    details: dict[str, Any]


def _table_exists(engine: Engine, table_name: str) -> bool:
    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        )
        """
    )
    with engine.begin() as connection:
        return bool(connection.execute(query, {"table_name": table_name}).scalar())


def run_ingestion_checks(
    df: pd.DataFrame,
    engine: Engine,
    *,
    null_threshold: float = 0.05,
    duplicate_threshold: float = 0.0,
    timestamp_invalid_threshold: float = 0.05,
    negative_value_threshold: float = 0.03,
    zone_invalid_threshold: float = 0.05,
) -> tuple[bool, list[CheckResult], pd.DataFrame]:
    """Execute required quality checks and return pass/fail plus rejects."""

    check_results: list[CheckResult] = []
    reject_reasons: dict[int, list[str]] = {}

    missing_required = [column for column in KEY_COLUMNS if column not in df.columns]
    check_results.append(
        CheckResult(
            check_name="required_column_presence",
            passed=len(missing_required) == 0,
            metric_value=float(len(missing_required)),
            threshold_value=0.0,
            details={"missing_columns": missing_required},
        )
    )
    if missing_required:
        return False, check_results, pd.DataFrame()

    invalid_timestamp_mask = (
        df["pickup_datetime"].isna()
        | df["dropoff_datetime"].isna()
        | (df["dropoff_datetime"] < df["pickup_datetime"])
    )
    invalid_timestamp_rate = float(invalid_timestamp_mask.mean()) if len(df) else 0.0
    check_results.append(
        CheckResult(
            check_name="pickup_dropoff_timestamp_validity",
            passed=invalid_timestamp_rate <= timestamp_invalid_threshold,
            metric_value=invalid_timestamp_rate,
            threshold_value=timestamp_invalid_threshold,
            details={"invalid_rows": int(invalid_timestamp_mask.sum())},
        )
    )

    for column in KEY_COLUMNS:
        null_rate = float(df[column].isna().mean()) if len(df) else 0.0
        check_results.append(
            CheckResult(
                check_name=f"null_threshold_{column}",
                passed=null_rate <= null_threshold,
                metric_value=null_rate,
                threshold_value=null_threshold,
                details={"null_rows": int(df[column].isna().sum())},
            )
        )

    negative_mask = (df["fare_amount"].fillna(0) < 0) | (df["trip_distance"].fillna(0) < 0)
    negative_rate = float(negative_mask.mean()) if len(df) else 0.0
    check_results.append(
        CheckResult(
            check_name="nonnegative_fare_and_distance",
            passed=negative_rate <= negative_value_threshold,
            metric_value=negative_rate,
            threshold_value=negative_value_threshold,
            details={"invalid_rows": int(negative_mask.sum())},
        )
    )

    duplicate_rate = (
        float(df.duplicated(subset=["source_file", "source_row_number"]).mean()) if len(df) else 0.0
    )
    check_results.append(
        CheckResult(
            check_name="duplicate_threshold",
            passed=duplicate_rate <= duplicate_threshold,
            metric_value=duplicate_rate,
            threshold_value=duplicate_threshold,
            details={"duplicate_rows": int(df.duplicated(subset=["source_file", "source_row_number"]).sum())},
        )
    )

    zone_check_passed = True
    zone_details: dict[str, Any] = {"checked": False}
    if _table_exists(engine, "dim_zone"):
        with engine.begin() as connection:
            zone_count = int(connection.execute(text("SELECT COUNT(*) FROM dim_zone")).scalar() or 0)
        if zone_count > 0:
            zone_details["checked"] = True
            valid_zone_ids = set(
                pd.read_sql("SELECT location_id FROM dim_zone", con=engine)["location_id"].astype("Int64")
            )
            pickup_invalid = ~df["pickup_location_id"].astype("Int64").isin(valid_zone_ids)
            dropoff_invalid = ~df["dropoff_location_id"].astype("Int64").isin(valid_zone_ids)
            invalid_zone_rate = float((pickup_invalid | dropoff_invalid).mean()) if len(df) else 0.0
            zone_check_passed = invalid_zone_rate <= zone_invalid_threshold
            zone_details.update(
                {
                    "invalid_zone_rate": invalid_zone_rate,
                    "invalid_rows": int((pickup_invalid | dropoff_invalid).sum()),
                }
            )
            for row_index in df.index[pickup_invalid | dropoff_invalid].tolist():
                reject_reasons.setdefault(int(row_index), []).append("zone_id_validity")

    check_results.append(
        CheckResult(
            check_name="zone_id_validity",
            passed=zone_check_passed,
            metric_value=float(zone_details.get("invalid_zone_rate", 0.0)),
            threshold_value=zone_invalid_threshold,
            details=zone_details,
        )
    )

    row_level_masks = {
        "pickup_dropoff_timestamp_validity": invalid_timestamp_mask,
        "nonnegative_fare_and_distance": negative_mask,
    }
    for reason, mask in row_level_masks.items():
        for row_index in df.index[mask].tolist():
            reject_reasons.setdefault(int(row_index), []).append(reason)

    rejects = []
    for row_index, reasons in reject_reasons.items():
        row = df.loc[row_index]
        rejects.append(
            {
                "ingest_batch_id": row["ingest_batch_id"],
                "source_file": row["source_file"],
                "source_row_number": int(row["source_row_number"]),
                "check_name": ";".join(sorted(set(reasons))),
                "reason": "validation_failed",
                "raw_payload": row.to_json(date_format="iso"),
            }
        )

    rejects_df = pd.DataFrame(rejects)
    overall_passed = all(result.passed for result in check_results)
    return overall_passed, check_results, rejects_df
