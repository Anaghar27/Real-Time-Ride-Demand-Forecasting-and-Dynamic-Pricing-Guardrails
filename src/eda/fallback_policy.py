"""
Phase 3.2 fallback policy assignment from sparsity classes.
It reads from the feature tables and produces reproducible summaries and governance artifacts.
Run it via `make eda-*` targets or through the EDA orchestrator for a full end-to-end report.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.eda.utils import EDAParams, build_eda_params, ensure_eda_tables, load_yaml


def assign_fallback_policy(sparsity_df: pd.DataFrame, thresholds_cfg: dict[str, Any]) -> pd.DataFrame:
    mapping = dict(thresholds_cfg["fallback_policy_mapping"])

    def _map_row(row: pd.Series) -> pd.Series:
        policy = dict(mapping[row["sparsity_class"]])
        return pd.Series(
            {
                "fallback_method": str(policy["fallback_method"]),
                "fallback_priority": int(policy["fallback_priority"]),
                "confidence_band": str(policy["confidence_band"]),
            }
        )

    assigned = sparsity_df.copy()
    assigned[["fallback_method", "fallback_priority", "confidence_band"]] = assigned.apply(_map_row, axis=1)
    return assigned


def run_fallback_policy(params: EDAParams, thresholds_cfg: dict[str, Any]) -> dict[str, Any]:
    ensure_eda_tables(engine)
    sparsity_df = pd.read_sql(
        text(
            """
            SELECT zone_id, sparsity_class
            FROM eda_zone_sparsity_summary
            WHERE run_id = :run_id
            ORDER BY zone_id
            """
        ),
        con=engine,
        params={"run_id": params.run_id},
    )
    if sparsity_df.empty:
        raise ValueError("No sparsity summary rows found for run_id")

    policy_df = assign_fallback_policy(sparsity_df, thresholds_cfg)
    policy_df["effective_from"] = params.data_start_ts
    policy_df["effective_to"] = None
    policy_df["created_at"] = pd.Timestamp.utcnow()
    policy_df["policy_version"] = params.policy_version
    policy_df["run_id"] = params.run_id

    policy_df = policy_df[
        [
            "zone_id",
            "sparsity_class",
            "fallback_method",
            "fallback_priority",
            "confidence_band",
            "effective_from",
            "effective_to",
            "created_at",
            "policy_version",
            "run_id",
        ]
    ]

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM zone_fallback_policy
                WHERE run_id = :run_id
                   OR (policy_version = :policy_version AND effective_from = :effective_from)
                """
            ),
            {
                "run_id": params.run_id,
                "policy_version": params.policy_version,
                "effective_from": params.data_start_ts,
            },
        )
    policy_df.to_sql("zone_fallback_policy", con=engine, if_exists="append", index=False, method="multi")

    class_counts = policy_df.groupby("sparsity_class").size().to_dict()
    return {
        "run_id": params.run_id,
        "policy_rows": int(len(policy_df)),
        "class_counts": {str(k): int(v) for k, v in class_counts.items()},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assign fallback policy by sparsity class")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--feature-version", required=False)
    parser.add_argument("--policy-version", required=False)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--run-id", required=False)
    parser.add_argument("--config", default="configs/eda.yaml")
    parser.add_argument("--threshold-config", default="configs/eda_thresholds.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yaml(Path(args.config))
    threshold_cfg = load_yaml(Path(args.threshold_config))
    window_cfg = dict(cfg.get("analysis_window", {}))
    run_id = args.run_id or None
    if run_id is None:
        latest = pd.read_sql(
            text("SELECT run_id FROM eda_zone_sparsity_summary ORDER BY created_at DESC LIMIT 1"),
            con=engine,
        )
        if latest.empty:
            raise ValueError("No sparsity run available. Run eda-sparsity first or pass --run-id.")
        run_id = str(latest.iloc[0]["run_id"])

    params = build_eda_params(
        run_id=run_id,
        start_date=args.start_date or str(window_cfg.get("start_date")),
        end_date=args.end_date or str(window_cfg.get("end_date")),
        feature_version=args.feature_version or str(cfg.get("feature_version", "v1")),
        policy_version=args.policy_version or str(cfg.get("policy_version", "p1")),
        zones=args.zones,
        config=cfg,
    )
    result = run_fallback_policy(params, threshold_cfg)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
