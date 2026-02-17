"""
Step 2.1 Build 15-minute time buckets and zone-time spine.
It contributes to a leakage-safe, 15-minute demand feature set materialized in Postgres.
Run it via the Phase 2 Make targets or through `src.features.build_feature_pipeline`.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

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
        "sql/transforms/201_build_time_spine_15m.sql",
        "sql/transforms/build_time_spine_15m.sql",
    ]
)


def floor_timestamp_to_15m(ts: datetime) -> datetime:
    """Floor a datetime to its canonical 15-minute boundary."""

    if ts.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    ts_utc = ts.astimezone(UTC)
    minute = (ts_utc.minute // 15) * 15
    return ts_utc.replace(minute=minute, second=0, microsecond=0)


def build_time_buckets(params: FeatureParams) -> dict[str, int | str]:
    """Run SQL transform for time and zone-time spine creation."""

    exec_sql_file(
        engine,
        TRANSFORM_SQL,
        {
            "run_start_ts": params.run_start_ts,
            "run_end_ts": params.run_end_ts,
            "feature_tz": params.feature_tz,
            "zone_ids": params.zone_ids,
        },
    )

    with engine.begin() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM dim_time_15m
                     WHERE bucket_start_ts >= :run_start_ts
                       AND bucket_start_ts < :run_end_ts) AS bucket_count,
                    (SELECT COUNT(*) FROM fct_zone_time_spine_15m
                     WHERE bucket_start_ts >= :run_start_ts
                       AND bucket_start_ts < :run_end_ts
                       AND (:zone_ids IS NULL OR zone_id = ANY(CAST(:zone_ids AS INTEGER[])))) AS zone_bucket_count,
                    (SELECT COUNT(*) FROM dim_zone
                     WHERE (:zone_ids IS NULL OR location_id = ANY(CAST(:zone_ids AS INTEGER[])))) AS zone_count
                """
            ),
            {
                "run_start_ts": params.run_start_ts,
                "run_end_ts": params.run_end_ts,
                "zone_ids": params.zone_ids,
            },
        ).mappings().one()

    return {
        "run_id": params.run_id,
        "bucket_count": int(counts["bucket_count"]),
        "zone_bucket_count": int(counts["zone_bucket_count"]),
        "zone_count": int(counts["zone_count"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build 15-minute time and zone-time spine")
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--zones", default=None, help="Optional comma-separated zone ids")
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--run-id", default=None)
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
    result = build_time_buckets(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
