"""Historical backfill runner with strict Phase 1 gate enforcement."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

import requests
from sqlalchemy import text

from src.common.db import engine
from src.ingestion.ddl import apply_ingestion_ddl
from src.ingestion.fetch import download_month_file
from src.ingestion.gate import evaluate_phase1_gate
from src.ingestion.load_raw_trips import process_trip_file

Mode = Literal["pilot", "full", "incremental"]


@dataclass
class BackfillResult:
    period: str
    state: str
    attempts: int
    reason_code: str | None


class BackfillRunError(RuntimeError):
    """Raised when backfill cannot continue and should fail gracefully."""

    def __init__(self, message: str, payload: dict[str, Any]) -> None:
        super().__init__(message)
        self.payload = payload


def _parse_period(period: str) -> date:
    year, month = period.split("-")
    return date(int(year), int(month), 1)


def _format_period(value: date) -> str:
    return value.strftime("%Y-%m")


def _iter_months(start: date, end: date) -> list[str]:
    months: list[str] = []
    current = date(start.year, start.month, 1)
    boundary = date(end.year, end.month, 1)
    while current <= boundary:
        months.append(_format_period(current))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _latest_complete_month() -> date:
    now = datetime.now(tz=UTC)
    if now.month == 1:
        return date(now.year - 1, 12, 1)
    return date(now.year, now.month - 1, 1)


def _get_watermark(dataset_name: str) -> str | None:
    with engine.begin() as connection:
        value = connection.execute(
            text(
                "SELECT latest_successful_period FROM ingestion_watermark WHERE dataset_name = :dataset_name"
            ),
            {"dataset_name": dataset_name},
        ).scalar()
    return str(value) if value else None


def _set_watermark(dataset_name: str, period: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ingestion_watermark (dataset_name, latest_successful_period, updated_at)
                VALUES (:dataset_name, :period, :updated_at)
                ON CONFLICT (dataset_name)
                DO UPDATE SET
                    latest_successful_period = EXCLUDED.latest_successful_period,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "dataset_name": dataset_name,
                "period": period,
                "updated_at": datetime.now(tz=UTC),
            },
        )


def _resolve_periods(mode: Mode, full_months: int, pilot_months: list[str], dataset_name: str) -> list[str]:
    if mode == "pilot":
        return pilot_months

    latest_month = _latest_complete_month()
    if mode == "full":
        end_period = latest_month
        start_year = end_period.year
        start_month = end_period.month - (full_months - 1)
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        return _iter_months(date(start_year, start_month, 1), end_period)

    watermark = _get_watermark(dataset_name)
    if watermark is None:
        raise RuntimeError("No watermark found for incremental mode. Run pilot/full first.")

    watermark_date = _parse_period(watermark)
    if watermark_date >= latest_month:
        return []

    if watermark_date.month == 12:
        next_month = date(watermark_date.year + 1, 1, 1)
    else:
        next_month = date(watermark_date.year, watermark_date.month + 1, 1)
    return _iter_months(next_month, latest_month)


def _assert_gate() -> None:
    gate_passed, details = evaluate_phase1_gate(engine, min_successful_batches=2, run_tests=True)
    if not gate_passed:
        raise RuntimeError(
            f"Phase 1 gate failed. Step 1.6 is blocked. Reasons: {details.get('reasons', [])}"
        )


def run_backfill(
    mode: Mode,
    *,
    full_months: int,
    max_retries: int,
    retry_delay_seconds: int,
    max_rows_per_file: int | None,
) -> dict[str, object]:
    """Run pilot/full/incremental backfill using common ingestion pipeline."""

    apply_ingestion_ddl(engine)
    _assert_gate()

    dataset_name = "yellow_taxi"
    pilot_months = ["2024-01", "2024-02", "2024-03"]
    periods = _resolve_periods(mode, full_months, pilot_months, dataset_name)

    if not periods:
        return {"mode": mode, "message": "no periods to process", "results": []}

    results: list[BackfillResult] = []
    unavailable_periods: list[str] = []
    for period in periods:
        try:
            source_file = download_month_file(period)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in {403, 404}:
                unavailable_periods.append(period)
                results.append(
                    BackfillResult(
                        period=period,
                        state="failed",
                        attempts=0,
                        reason_code="source_unavailable",
                    )
                )
                continue
            raise

        attempts = 0
        completed = False
        reason_code: str | None = None
        last_error: str | None = None

        while attempts < max_retries and not completed:
            attempts += 1
            try:
                summary = process_trip_file(
                    source_file,
                    validate_only=False,
                    max_rows_per_file=max_rows_per_file,
                )
                if summary["state"] != "succeeded":
                    reason_code = "validation_failure"
                    last_error = summary.get("message", "validation checks failed")
                    break

                _set_watermark(dataset_name, period)
                results.append(
                    BackfillResult(period=period, state="succeeded", attempts=attempts, reason_code=None)
                )
                completed = True
            except Exception as exc:  # pragma: no cover - exercised in integration paths
                last_error = str(exc)
                if attempts >= max_retries:
                    reason_code = "permanent_failure"
                    break
                reason_code = "transient_retry"
                time.sleep(retry_delay_seconds)

        if not completed:
            results.append(
                BackfillResult(period=period, state="failed", attempts=attempts, reason_code=reason_code)
            )
            payload = {
                "status": "failed",
                "message": f"Backfill failed for {period} due to {reason_code}",
                "mode": mode,
                "failed_period": period,
                "reason_code": reason_code,
                "error": last_error,
                "watermark": _get_watermark(dataset_name),
                "results": [result.__dict__ for result in results],
            }
            raise BackfillRunError(
                f"Backfill failed for {period} with reason_code={reason_code}",
                payload,
            )

    status = "succeeded_with_warnings" if unavailable_periods else "succeeded"
    message = (
        f"completed with unavailable months: {', '.join(unavailable_periods)}"
        if unavailable_periods
        else "completed"
    )
    return {
        "status": status,
        "message": message,
        "mode": mode,
        "periods": periods,
        "unavailable_periods": unavailable_periods,
        "watermark": _get_watermark(dataset_name),
        "results": [result.__dict__ for result in results],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Historical backfill ingestion with gate enforcement")
    parser.add_argument("--mode", choices=["pilot", "full", "incremental"], required=True)
    parser.add_argument("--full-months", type=int, default=12)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=int, default=5)
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=int(os.getenv("INGEST_SAMPLE_MAX_ROWS", "200000")),
        help="Use 0 for full-file ingestion",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_rows = None if args.max_rows_per_file == 0 else args.max_rows_per_file
    try:
        result = run_backfill(
            args.mode,
            full_months=args.full_months,
            max_retries=args.max_retries,
            retry_delay_seconds=args.retry_delay_seconds,
            max_rows_per_file=max_rows,
        )
        print(json.dumps(result, indent=2))
    except BackfillRunError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(json.dumps(exc.payload, indent=2), file=sys.stderr)
        raise SystemExit(1) from None
    except Exception as exc:  # pragma: no cover - defensive fallback for CLI robustness
        payload = {
            "status": "failed",
            "message": "Backfill failed due to an unexpected error",
            "mode": args.mode,
            "reason_code": "unexpected_error",
            "error": str(exc),
        }
        print(f"ERROR: {payload['message']}: {exc}", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
