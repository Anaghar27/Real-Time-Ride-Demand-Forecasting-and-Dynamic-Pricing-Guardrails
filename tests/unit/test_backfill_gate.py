"""
Unit tests for backfill gate.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from unittest.mock import patch

import pytest
import requests

from src.ingestion.backfill_historical import BackfillRunError, _resolve_periods, run_backfill


@pytest.fixture(autouse=True)
def _mock_apply_ingestion_ddl() -> None:
    """Keep unit tests DB-independent in CI."""
    with patch("src.ingestion.backfill_historical.apply_ingestion_ddl"):
        yield


def test_incremental_period_resolution_from_watermark() -> None:
    with patch("src.ingestion.backfill_historical._get_watermark", return_value="2024-02"), patch(
        "src.ingestion.backfill_historical._latest_complete_month"
    ) as latest_month:
        latest_month.return_value = __import__("datetime").date(2024, 4, 1)
        periods = _resolve_periods("incremental", full_months=12, pilot_months=["2024-01"], dataset_name="yellow_taxi")

    assert periods == ["2024-03", "2024-04"]


def test_backfill_gate_fail_blocks_execution() -> None:
    with patch("src.ingestion.backfill_historical.evaluate_phase1_gate", return_value=(False, {"reasons": ["x"]})):
        with pytest.raises(RuntimeError, match="Phase 1 gate failed"):
            run_backfill(
                "pilot",
                full_months=12,
                max_retries=1,
                retry_delay_seconds=0,
                max_rows_per_file=100,
            )


def test_backfill_gate_pass_allows_execution_path() -> None:
    with patch("src.ingestion.backfill_historical.evaluate_phase1_gate", return_value=(True, {})), patch(
        "src.ingestion.backfill_historical.download_month_file"
    ) as download_month_file, patch("src.ingestion.backfill_historical.process_trip_file") as process_trip_file, patch(
        "src.ingestion.backfill_historical._set_watermark"
    ) as set_watermark, patch("src.ingestion.backfill_historical._get_watermark", return_value="2024-03"):
        download_month_file.return_value = __import__("pathlib").Path(
            "data/landing/tlc/year=2024/month=01/yellow_tripdata_2024-01.parquet"
        )
        process_trip_file.return_value = {"state": "succeeded"}

        result = run_backfill(
            "pilot",
            full_months=12,
            max_retries=1,
            retry_delay_seconds=0,
            max_rows_per_file=100,
        )

    assert result["mode"] == "pilot"
    assert set_watermark.call_count >= 1


def test_backfill_validation_failure_raises_structured_error() -> None:
    with patch("src.ingestion.backfill_historical.evaluate_phase1_gate", return_value=(True, {})), patch(
        "src.ingestion.backfill_historical.download_month_file"
    ) as download_month_file, patch("src.ingestion.backfill_historical.process_trip_file") as process_trip_file, patch(
        "src.ingestion.backfill_historical._get_watermark", return_value="2024-01"
    ):
        download_month_file.return_value = __import__("pathlib").Path(
            "data/landing/tlc/year=2024/month=02/yellow_tripdata_2024-02.parquet"
        )
        process_trip_file.return_value = {"state": "failed", "message": "check failure"}

        with pytest.raises(BackfillRunError) as exc:
            run_backfill(
                "pilot",
                full_months=12,
                max_retries=1,
                retry_delay_seconds=0,
                max_rows_per_file=100,
            )

    assert exc.value.payload["status"] == "failed"
    assert exc.value.payload["reason_code"] == "validation_failure"
    assert exc.value.payload["failed_period"] == "2024-01"


def test_backfill_skips_unavailable_source_month() -> None:
    http_error = requests.HTTPError("403 forbidden")
    response = requests.Response()
    response.status_code = 403
    http_error.response = response

    with patch("src.ingestion.backfill_historical.evaluate_phase1_gate", return_value=(True, {})), patch(
        "src.ingestion.backfill_historical._resolve_periods", return_value=["2025-12"]
    ), patch("src.ingestion.backfill_historical.download_month_file", side_effect=http_error), patch(
        "src.ingestion.backfill_historical._get_watermark", return_value="2025-11"
    ), patch("src.ingestion.backfill_historical.process_trip_file") as process_trip_file:
        result = run_backfill(
            "full",
            full_months=12,
            max_retries=1,
            retry_delay_seconds=0,
            max_rows_per_file=100,
        )

    assert result["status"] == "succeeded_with_warnings"
    assert result["unavailable_periods"] == ["2025-12"]
    assert result["results"][0]["reason_code"] == "source_unavailable"
    process_trip_file.assert_not_called()
