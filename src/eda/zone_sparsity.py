"""Phase 3.2 zone sparsity classification."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.eda.utils import EDAParams, build_eda_params, ensure_eda_tables, fetch_eda_base, load_yaml


def max_consecutive_zeros(values: pd.Series) -> int:
    longest = 0
    current = 0
    for value in values.astype(float).tolist():
        if value == 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return int(longest)


def classify_sparsity(record: dict[str, float], thresholds: dict[str, Any]) -> str:
    if (
        record["nonzero_ratio"] >= thresholds["robust"]["min_nonzero_ratio"]
        and record["active_days"] >= thresholds["robust"]["min_active_days"]
        and record["coverage_ratio"] >= thresholds["robust"]["min_coverage_ratio"]
    ):
        return "robust"
    if (
        record["nonzero_ratio"] >= thresholds["medium"]["min_nonzero_ratio"]
        and record["active_days"] >= thresholds["medium"]["min_active_days"]
        and record["coverage_ratio"] >= thresholds["medium"]["min_coverage_ratio"]
    ):
        return "medium"
    if (
        record["nonzero_ratio"] >= thresholds["sparse"]["min_nonzero_ratio"]
        and record["active_days"] >= thresholds["sparse"]["min_active_days"]
        and record["coverage_ratio"] >= thresholds["sparse"]["min_coverage_ratio"]
    ):
        return "sparse"
    return "ultra_sparse"


def run_zone_sparsity(params: EDAParams, thresholds_cfg: dict[str, Any]) -> dict[str, Any]:
    ensure_eda_tables(engine)
    df = fetch_eda_base(params)
    if df.empty:
        raise ValueError("No fact_demand_features data found for sparsity analysis")

    df["bucket_start_ts"] = pd.to_datetime(df["bucket_start_ts"], utc=True)
    df["date"] = df["bucket_start_ts"].dt.date
    expected_buckets = int(df["bucket_start_ts"].nunique())

    summary = df.groupby("zone_id").agg(
        total_buckets=("pickup_count", "count"),
        nonzero_buckets=("pickup_count", lambda x: int((x > 0).sum())),
        avg_pickup_count=("pickup_count", "mean"),
        median_pickup_count=("pickup_count", "median"),
        std_pickup_count=("pickup_count", "std"),
        active_days=("date", lambda x: int(x[df.loc[x.index, "pickup_count"] > 0].nunique())),
    ).reset_index()

    summary["std_pickup_count"] = summary["std_pickup_count"].fillna(0.0)
    summary["expected_buckets"] = expected_buckets
    summary["coverage_ratio"] = summary["total_buckets"] / summary["expected_buckets"]
    summary["nonzero_ratio"] = np.where(summary["total_buckets"] > 0, summary["nonzero_buckets"] / summary["total_buckets"], 0.0)

    zero_streak = (
        df.sort_values(["zone_id", "bucket_start_ts"]).groupby("zone_id")["pickup_count"].apply(max_consecutive_zeros).reset_index(name="max_consecutive_zero_buckets")
    )
    summary = summary.merge(zero_streak, on="zone_id", how="left")

    thresholds = dict(thresholds_cfg["sparsity_thresholds"])
    summary["sparsity_class"] = summary.apply(lambda row: classify_sparsity(row.to_dict(), thresholds), axis=1)

    summary["run_id"] = params.run_id
    summary["feature_version"] = params.feature_version
    summary["data_start_ts"] = params.data_start_ts
    summary["data_end_ts"] = params.data_end_ts
    summary["created_at"] = pd.Timestamp.utcnow()

    summary = summary[
        [
            "run_id",
            "feature_version",
            "zone_id",
            "total_buckets",
            "expected_buckets",
            "nonzero_buckets",
            "nonzero_ratio",
            "avg_pickup_count",
            "median_pickup_count",
            "std_pickup_count",
            "max_consecutive_zero_buckets",
            "active_days",
            "coverage_ratio",
            "sparsity_class",
            "data_start_ts",
            "data_end_ts",
            "created_at",
        ]
    ]

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM eda_zone_sparsity_summary WHERE run_id = :run_id"), {"run_id": params.run_id})
    summary.to_sql("eda_zone_sparsity_summary", con=engine, if_exists="append", index=False, method="multi")

    class_counts = summary.groupby("sparsity_class").size().to_dict()
    demand_by_class = (
        summary.groupby("sparsity_class")["avg_pickup_count"].sum() / max(float(summary["avg_pickup_count"].sum()), 1.0)
    ).to_dict()

    return {
        "run_id": params.run_id,
        "zone_rows": int(len(summary)),
        "class_counts": {str(k): int(v) for k, v in class_counts.items()},
        "demand_share_by_class": {str(k): float(v) for k, v in demand_by_class.items()},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute zone sparsity classes")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--feature-version", required=False)
    parser.add_argument("--policy-version", required=False)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default="configs/eda.yaml")
    parser.add_argument("--threshold-config", default="configs/eda_thresholds.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yaml(Path(args.config))
    threshold_cfg = load_yaml(Path(args.threshold_config))
    window_cfg = dict(cfg.get("analysis_window", {}))
    params = build_eda_params(
        run_id=args.run_id,
        start_date=args.start_date or str(window_cfg.get("start_date")),
        end_date=args.end_date or str(window_cfg.get("end_date")),
        feature_version=args.feature_version or str(cfg.get("feature_version", "v1")),
        policy_version=args.policy_version or str(cfg.get("policy_version", "p1")),
        zones=args.zones,
        config=cfg,
    )
    result = run_zone_sparsity(params, threshold_cfg)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
