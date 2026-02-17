"""
Phase 1 gate checks prior to historical backfill.
It supports an idempotent ingestion workflow that loads raw TLC data and reference tables into Postgres.
It is typically invoked via the Phase 1 Make targets and should be safe to re-run.
"""

from __future__ import annotations

import subprocess
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _run_phase1_tests() -> tuple[bool, str]:
    command = [".venv/bin/python", "-m", "pytest", "tests/unit", "tests/integration", "-q"]
    result = subprocess.run(command, capture_output=True, text=True)
    output = (result.stdout + "\n" + result.stderr).strip()
    return result.returncode == 0, output


def evaluate_phase1_gate(
    engine: Engine,
    *,
    min_successful_batches: int = 2,
    run_tests: bool = True,
) -> tuple[bool, dict[str, Any]]:
    """Evaluate whether Phase 1.1-1.5 criteria are met."""

    details: dict[str, Any] = {
        "tests_passed": None,
        "successful_sample_batches": 0,
        "min_successful_batches": min_successful_batches,
        "open_failed_batches": 0,
        "reasons": [],
    }

    with engine.begin() as connection:
        details["successful_sample_batches"] = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM ingestion_batch_log
                    WHERE source_name = 'yellow_taxi' AND state = 'succeeded'
                    """
                )
            ).scalar()
            or 0
        )
        details["open_failed_batches"] = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM ingestion_batch_log
                    WHERE state = 'failed'
                    """
                )
            ).scalar()
            or 0
        )

    if run_tests:
        tests_passed, test_output = _run_phase1_tests()
        details["tests_passed"] = tests_passed
        details["test_output"] = test_output
        if not tests_passed:
            details["reasons"].append("step_1_1_to_1_5_tests_failed")

    if details["successful_sample_batches"] < min_successful_batches:
        details["reasons"].append("insufficient_successful_sample_batches")
    if details["open_failed_batches"] > 0:
        details["reasons"].append("open_failed_batches_present")

    passed = len(details["reasons"]) == 0
    return passed, details
