"""
Step 2.5 Publish stable feature table contract.
It contributes to a leakage-safe, 15-minute demand feature set materialized in Postgres.
Run it via the Phase 2 Make targets or through `src.features.build_feature_pipeline`.
"""

from __future__ import annotations

import argparse
import json

from sqlalchemy import text

from src.common.db import engine
from src.features.runtime import (
    FeatureParams,
    build_feature_params,
    exec_sql_file,
    get_source_bounds,
    resolve_sql_file,
)

TRANSFORM_SQL = resolve_sql_file(
    [
        "sql/transforms/205_publish_fact_demand_features.sql",
        "sql/transforms/publish_fact_demand_features.sql",
    ]
)


def publish_features(params: FeatureParams) -> dict[str, int | str]:
    """Publish stage features into fact_demand_features."""

    source_min_ts, source_max_ts = get_source_bounds(engine, params)
    exec_sql_file(
        engine,
        TRANSFORM_SQL,
        {
            "run_id": params.run_id,
            "feature_version": params.feature_version,
            "run_start_ts": params.run_start_ts,
            "run_end_ts": params.run_end_ts,
            "source_min_ts": source_min_ts,
            "source_max_ts": source_max_ts,
            "zone_ids": params.zone_ids,
        },
    )

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT COUNT(*) AS row_count
                FROM fact_demand_features
                WHERE bucket_start_ts >= :run_start_ts
                  AND bucket_start_ts < :run_end_ts
                  AND feature_version = :feature_version
                  AND (
                      :zone_ids IS NULL
                      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
                  )
                """
            ),
            {
                "run_start_ts": params.run_start_ts,
                "run_end_ts": params.run_end_ts,
                "feature_version": params.feature_version,
                "zone_ids": params.zone_ids,
            },
        ).mappings().one()

    return {"run_id": params.run_id, "row_count": int(row["row_count"])}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish final fact_demand_features table")
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
    result = publish_features(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
