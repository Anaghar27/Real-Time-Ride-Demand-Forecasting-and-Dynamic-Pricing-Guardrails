"""Sample ingestion loader from TLC landing data into raw tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.common.schema_map import normalize_trip_dataframe
from src.ingestion.checks import CheckResult, run_ingestion_checks
from src.ingestion.ddl import apply_ingestion_ddl
from src.ingestion.utils import sha256sum

DEFAULT_INPUT_GLOB = "data/landing/tlc/year=*/month=*/*.parquet"


def _batch_key_for_file(source_file: Path, checksum: str) -> str:
    return hashlib.sha256(f"{source_file}:{checksum}".encode()).hexdigest()


def _upsert_batch_discovered(batch_id: str, batch_key: str, source_file: Path, checksum: str) -> str:
    with engine.begin() as connection:
        existing = connection.execute(
            text(
                "SELECT state FROM ingestion_batch_log WHERE batch_key = :batch_key"
            ),
            {"batch_key": batch_key},
        ).scalar()

        if existing == "succeeded":
            return "already_succeeded"

        if existing is None:
            connection.execute(
                text(
                    """
                    INSERT INTO ingestion_batch_log (
                        batch_id,
                        batch_key,
                        source_name,
                        source_file,
                        checksum,
                        state,
                        created_at
                    )
                    VALUES (
                        :batch_id,
                        :batch_key,
                        :source_name,
                        :source_file,
                        :checksum,
                        'discovered',
                        :created_at
                    )
                    """
                ),
                {
                    "batch_id": batch_id,
                    "batch_key": batch_key,
                    "source_name": "yellow_taxi",
                    "source_file": str(source_file),
                    "checksum": checksum,
                    "created_at": datetime.now(tz=UTC),
                },
            )
        else:
            connection.execute(
                text(
                    """
                    UPDATE ingestion_batch_log
                    SET state = 'discovered',
                        error_message = NULL,
                        created_at = COALESCE(created_at, :created_at)
                    WHERE batch_key = :batch_key
                    """
                ),
                {"batch_key": batch_key, "created_at": datetime.now(tz=UTC)},
            )
    return "ready"


def _set_batch_state(batch_key: str, state: str, **metrics: Any) -> None:
    payload = {
        "batch_key": batch_key,
        "state": state,
        "completed_at": datetime.now(tz=UTC) if state in {"failed", "succeeded"} else None,
        "started_at": datetime.now(tz=UTC) if state == "running" else None,
        **metrics,
    }

    assignments = ["state = :state"]
    if payload.get("started_at"):
        assignments.append("started_at = :started_at")
    if payload.get("completed_at"):
        assignments.append("completed_at = :completed_at")
    for field in [
        "rows_read",
        "rows_valid",
        "rows_rejected",
        "load_duration_sec",
        "check_pass_rate",
        "error_message",
    ]:
        if field in payload:
            assignments.append(f"{field} = :{field}")

    sql = text(
        f"UPDATE ingestion_batch_log SET {', '.join(assignments)} WHERE batch_key = :batch_key"
    )
    with engine.begin() as connection:
        connection.execute(sql, payload)


def _persist_check_results(batch_id: str, results: list[CheckResult]) -> None:
    with engine.begin() as connection:
        for result in results:
            connection.execute(
                text(
                    """
                    INSERT INTO ingestion_check_results (
                        batch_id,
                        check_name,
                        passed,
                        metric_value,
                        threshold_value,
                        details,
                        created_at
                    )
                    VALUES (
                        :batch_id,
                        :check_name,
                        :passed,
                        :metric_value,
                        :threshold_value,
                        CAST(:details AS JSONB),
                        :created_at
                    )
                    """
                ),
                {
                    "batch_id": batch_id,
                    "check_name": result.check_name,
                    "passed": result.passed,
                    "metric_value": result.metric_value,
                    "threshold_value": result.threshold_value,
                    "details": json.dumps(result.details),
                    "created_at": datetime.now(tz=UTC),
                },
            )


def _persist_rejects(rejects_df: pd.DataFrame) -> None:
    if rejects_df.empty:
        return

    with engine.begin() as connection:
        for record in rejects_df.to_dict(orient="records"):
            connection.execute(
                text(
                    """
                    INSERT INTO ingestion_rejects (
                        ingest_batch_id,
                        source_file,
                        source_row_number,
                        check_name,
                        reason,
                        raw_payload,
                        created_at
                    )
                    VALUES (
                        :ingest_batch_id,
                        :source_file,
                        :source_row_number,
                        :check_name,
                        :reason,
                        CAST(:raw_payload AS JSONB),
                        :created_at
                    )
                    """
                ),
                {
                    **record,
                    "created_at": datetime.now(tz=UTC),
                },
            )


def _merge_staging(batch_id: str) -> int:
    merge_sql = text(
        """
        INSERT INTO raw_trips (
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            pickup_location_id,
            dropoff_location_id,
            rate_code_id,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount,
            payment_type,
            store_and_fwd_flag,
            ingest_batch_id,
            source_file,
            source_row_number,
            ingested_at
        )
        SELECT
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            pickup_location_id,
            dropoff_location_id,
            rate_code_id,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount,
            payment_type,
            store_and_fwd_flag,
            ingest_batch_id,
            source_file,
            source_row_number,
            ingested_at
        FROM stg_raw_trips
        WHERE ingest_batch_id = :batch_id
          AND source_row_number NOT IN (
              SELECT source_row_number
              FROM ingestion_rejects
              WHERE ingest_batch_id = :batch_id
          )
        ON CONFLICT (source_file, source_row_number)
        DO UPDATE SET
            vendor_id = EXCLUDED.vendor_id,
            pickup_datetime = EXCLUDED.pickup_datetime,
            dropoff_datetime = EXCLUDED.dropoff_datetime,
            pickup_location_id = EXCLUDED.pickup_location_id,
            dropoff_location_id = EXCLUDED.dropoff_location_id,
            rate_code_id = EXCLUDED.rate_code_id,
            passenger_count = EXCLUDED.passenger_count,
            trip_distance = EXCLUDED.trip_distance,
            fare_amount = EXCLUDED.fare_amount,
            total_amount = EXCLUDED.total_amount,
            payment_type = EXCLUDED.payment_type,
            store_and_fwd_flag = EXCLUDED.store_and_fwd_flag,
            ingest_batch_id = EXCLUDED.ingest_batch_id,
            ingested_at = EXCLUDED.ingested_at
        """
    )
    with engine.begin() as connection:
        result = connection.execute(merge_sql, {"batch_id": batch_id})
    return int(result.rowcount or 0)


def process_trip_file(
    source_file: Path, validate_only: bool = False, max_rows_per_file: int | None = None
) -> dict[str, Any]:
    """Ingest a single trip file with checks and idempotent merge."""

    checksum = sha256sum(source_file)
    batch_key = _batch_key_for_file(source_file, checksum)
    batch_id = str(uuid.uuid5(uuid.NAMESPACE_URL, batch_key))

    init_state = _upsert_batch_discovered(batch_id, batch_key, source_file, checksum)
    if init_state == "already_succeeded":
        return {
            "batch_id": batch_id,
            "source_file": str(source_file),
            "state": "succeeded",
            "rows_read": 0,
            "rows_valid": 0,
            "rows_rejected": 0,
            "inserted_or_updated": 0,
            "message": "batch already succeeded; skipped",
        }

    start_time = time.perf_counter()
    _set_batch_state(batch_key, "running")

    try:
        trip_df = pd.read_parquet(source_file)
        if max_rows_per_file is not None and max_rows_per_file > 0:
            trip_df = trip_df.head(max_rows_per_file)
        normalized_df = normalize_trip_dataframe(trip_df, source_file, batch_id)

        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM stg_raw_trips WHERE ingest_batch_id = :batch_id"),
                {"batch_id": batch_id},
            )
        normalized_df.to_sql("stg_raw_trips", con=engine, if_exists="append", index=False, method="multi")

        check_passed, check_results, rejects_df = run_ingestion_checks(normalized_df, engine)
        _persist_check_results(batch_id, check_results)
        _persist_rejects(rejects_df)

        rows_read = len(normalized_df)
        rows_rejected = len(rejects_df)
        rows_valid = rows_read - rows_rejected
        check_pass_rate = sum(1 for result in check_results if result.passed) / max(len(check_results), 1)

        if not check_passed:
            _set_batch_state(
                batch_key,
                "failed",
                rows_read=rows_read,
                rows_valid=rows_valid,
                rows_rejected=rows_rejected,
                load_duration_sec=time.perf_counter() - start_time,
                check_pass_rate=check_pass_rate,
                error_message="ingestion checks failed; merge blocked",
            )
            return {
                "batch_id": batch_id,
                "source_file": str(source_file),
                "state": "failed",
                "rows_read": rows_read,
                "rows_valid": rows_valid,
                "rows_rejected": rows_rejected,
                "inserted_or_updated": 0,
                "message": "validation failed",
            }

        inserted_or_updated = 0
        if not validate_only:
            inserted_or_updated = _merge_staging(batch_id)

        _set_batch_state(
            batch_key,
            "succeeded",
            rows_read=rows_read,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            load_duration_sec=time.perf_counter() - start_time,
            check_pass_rate=check_pass_rate,
        )
        return {
            "batch_id": batch_id,
            "source_file": str(source_file),
            "state": "succeeded",
            "rows_read": rows_read,
            "rows_valid": rows_valid,
            "rows_rejected": rows_rejected,
            "inserted_or_updated": inserted_or_updated,
            "message": "validated and merged" if not validate_only else "validated only",
            "max_rows_per_file": max_rows_per_file,
        }

    except Exception as exc:
        _set_batch_state(
            batch_key,
            "failed",
            load_duration_sec=time.perf_counter() - start_time,
            check_pass_rate=0.0,
            error_message=str(exc),
        )
        raise


def run_sample_ingestion(
    input_glob: str, validate_only: bool = False, max_rows_per_file: int | None = None
) -> list[dict[str, Any]]:
    """Run ingestion for every matching sample file."""

    apply_ingestion_ddl(engine)
    source_files = sorted(Path().glob(input_glob))
    if not source_files:
        raise FileNotFoundError(f"No source files found for pattern: {input_glob}")

    summaries = []
    for source_file in source_files:
        summaries.append(
            process_trip_file(
                source_file,
                validate_only=validate_only,
                max_rows_per_file=max_rows_per_file,
            )
        )
    return summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load normalized sample trips into raw tables")
    parser.add_argument("--input-glob", default=DEFAULT_INPUT_GLOB)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=int(os.getenv("INGEST_SAMPLE_MAX_ROWS", "200000")),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summaries = run_sample_ingestion(
        args.input_glob,
        validate_only=args.validate_only,
        max_rows_per_file=args.max_rows_per_file,
    )
    print(json.dumps({"batches": summaries}, indent=2, default=str))


if __name__ == "__main__":
    main()
