"""Step 2.5 quality checks and gating for feature publish."""

from __future__ import annotations

import argparse
import json

from sqlalchemy import text

from src.common.db import engine
from src.features.runtime import (
    FeatureParams,
    build_feature_params,
    exec_sql_file,
    resolve_sql_file,
)

TRANSFORM_SQL = resolve_sql_file(
    [
        "sql/transforms/206_feature_quality_checks.sql",
        "sql/transforms/feature_quality_checks.sql",
    ]
)


class FeatureQualityError(RuntimeError):
    """Raised when critical quality checks fail."""


def run_feature_quality_checks(params: FeatureParams) -> dict[str, object]:
    """Execute quality checks, persist results, and enforce critical hard-fail."""

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM feature_check_results WHERE run_id = :run_id"), {"run_id": params.run_id})

    exec_sql_file(
        engine,
        TRANSFORM_SQL,
        {
            "run_id": params.run_id,
            "run_start_ts": params.run_start_ts,
            "run_end_ts": params.run_end_ts,
            "lag_null_policy": params.lag_null_policy,
            "zone_ids": params.zone_ids,
        },
    )

    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT check_name, severity, passed, reason_code
                FROM feature_check_results
                WHERE run_id = :run_id
                ORDER BY
                    CASE severity WHEN 'critical' THEN 1 ELSE 2 END,
                    check_name
                """
            ),
            {"run_id": params.run_id},
        ).mappings().all()

    check_rows: list[dict[str, object]] = [dict(row) for row in rows]
    critical_failures: list[dict[str, object]] = [
        row for row in check_rows if row["severity"] == "critical" and not row["passed"]
    ]
    summary: dict[str, object] = {
        "run_id": params.run_id,
        "checks": check_rows,
        "critical_failures": critical_failures,
    }

    if critical_failures:
        raise FeatureQualityError(json.dumps(summary, default=str))

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality checks for feature batch")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    params = build_feature_params(
        start_date=args.start_date,
        end_date=args.end_date,
        feature_version=args.feature_version,
        zones_arg=args.zones,
        run_id=args.run_id,
    )
    result = run_feature_quality_checks(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
