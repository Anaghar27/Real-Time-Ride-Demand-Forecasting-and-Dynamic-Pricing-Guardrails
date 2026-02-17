"""
Step 2.4 Add lag and rolling demand features with leakage guards.
It contributes to a leakage-safe, 15-minute demand feature set materialized in Postgres.
Run it via the Phase 2 Make targets or through `src.features.build_feature_pipeline`.
"""

from __future__ import annotations

import argparse
import json

import pandas as pd
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
        "sql/transforms/204_add_lag_rolling_features.sql",
        "sql/transforms/add_lag_rolling_features.sql",
    ]
)


def add_lag_features_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Reference lag implementation for deterministic unit tests."""

    out = df.sort_values(["zone_id", "bucket_start_ts"]).copy()
    out["lag_1"] = out.groupby("zone_id")["pickup_count"].shift(1)
    out["lag_2"] = out.groupby("zone_id")["pickup_count"].shift(2)
    out["lag_4"] = out.groupby("zone_id")["pickup_count"].shift(4)
    out["lag_96"] = out.groupby("zone_id")["pickup_count"].shift(96)
    out["lag_672"] = out.groupby("zone_id")["pickup_count"].shift(672)
    return out


def add_rolling_features_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Reference rolling implementation that excludes current row."""

    out = df.sort_values(["zone_id", "bucket_start_ts"]).copy()
    shifted = out.groupby("zone_id")["pickup_count"].shift(1)
    out["roll_mean_4"] = shifted.groupby(out["zone_id"]).rolling(4).mean().reset_index(level=0, drop=True)
    out["roll_mean_8"] = shifted.groupby(out["zone_id"]).rolling(8).mean().reset_index(level=0, drop=True)
    out["roll_std_8"] = shifted.groupby(out["zone_id"]).rolling(8).std().reset_index(level=0, drop=True)
    out["roll_max_16"] = shifted.groupby(out["zone_id"]).rolling(16).max().reset_index(level=0, drop=True)
    return out


def apply_null_policy(df: pd.DataFrame, policy: str) -> pd.DataFrame:
    """Apply lag null policy consistently for training and inference."""

    if policy == "keep_nulls":
        return df
    if policy == "zero":
        lag_cols = [
            "lag_1",
            "lag_2",
            "lag_4",
            "lag_96",
            "lag_672",
            "roll_mean_4",
            "roll_mean_8",
            "roll_std_8",
            "roll_max_16",
        ]
        output = df.copy()
        output[lag_cols] = output[lag_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        return output
    raise ValueError("Unsupported null policy")


def add_lag_rolling_features(params: FeatureParams) -> dict[str, int | str]:
    """Populate lag and rolling feature stage table."""

    exec_sql_file(
        engine,
        TRANSFORM_SQL,
        {
            "run_id": params.run_id,
            "feature_version": params.feature_version,
            "run_start_ts": params.run_start_ts,
            "run_end_ts": params.run_end_ts,
            "history_start_ts": params.history_start_ts,
            "lag_null_policy": params.lag_null_policy,
            "zone_ids": params.zone_ids,
        },
    )

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT COUNT(*) AS row_count
                FROM fct_zone_demand_features_stage_15m
                WHERE bucket_start_ts >= :run_start_ts
                  AND bucket_start_ts < :run_end_ts
                  AND (
                      :zone_ids IS NULL
                      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
                  )
                """
            ),
            {
                "run_start_ts": params.run_start_ts,
                "run_end_ts": params.run_end_ts,
                "zone_ids": params.zone_ids,
            },
        ).mappings().one()

    return {"run_id": params.run_id, "row_count": int(row["row_count"])}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add lag and rolling demand features")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--zones", default=None)
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
    result = add_lag_rolling_features(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
