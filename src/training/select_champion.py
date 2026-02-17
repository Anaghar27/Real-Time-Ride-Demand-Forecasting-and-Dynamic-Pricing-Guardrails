"""
Champion selection gate from leaderboard and policy.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import pandas as pd

from src.training.training_config import ensure_run_dir, load_training_bundle

REASON_BASELINE = "did_not_beat_baseline"
REASON_SPARSE = "sparse_zone_regression"
REASON_STABILITY = "stability_threshold_failed"
REASON_LATENCY = "latency_exceeds_limit"
REASON_METADATA = "missing_required_metadata"


def _best_baseline_row(df: pd.DataFrame, primary_metric: str) -> pd.Series:
    baselines = df.loc[df["model_role"] == "baseline"]
    if baselines.empty:
        raise ValueError("leaderboard missing baseline rows")
    return baselines.sort_values(primary_metric, ascending=True).iloc[0]


def _best_candidate_row(df: pd.DataFrame, primary_metric: str) -> pd.Series:
    candidates = df.loc[df["model_role"] == "candidate"]
    if candidates.empty:
        raise ValueError("leaderboard missing candidate rows")
    return candidates.sort_values(primary_metric, ascending=True).iloc[0]


def evaluate_champion_gate(
    *,
    leaderboard: pd.DataFrame,
    slice_metrics: pd.DataFrame,
    policy: dict[str, Any],
) -> dict[str, Any]:
    primary_metric = str(policy.get("primary_metric", "wape"))
    min_improve_pct = float(policy.get("min_improvement_over_baseline_pct", 0.02))
    max_sparse_regression_pct = float(policy.get("max_sparse_wape_regression_pct", 0.03))
    max_latency_ms = float(policy.get("max_latency_ms_per_row", 5.0))
    max_stability_std = float(policy.get("max_stability_std_wape", 0.02))

    best_baseline = _best_baseline_row(leaderboard, primary_metric)
    best_candidate = _best_candidate_row(leaderboard, primary_metric)

    reasons: list[str] = []

    baseline_value = float(best_baseline[primary_metric])
    candidate_value = float(best_candidate[primary_metric])
    required_value = baseline_value * (1.0 - min_improve_pct)
    if candidate_value > required_value:
        reasons.append(REASON_BASELINE)

    sparse_baseline = slice_metrics.loc[
        (slice_metrics["model_name"] == best_baseline["model_name"]) & (slice_metrics["slice_name"] == "sparse_zones")
    ]
    sparse_candidate = slice_metrics.loc[
        (slice_metrics["model_name"] == best_candidate["model_name"]) & (slice_metrics["slice_name"] == "sparse_zones")
    ]
    if not sparse_baseline.empty and not sparse_candidate.empty:
        b_wape = float(sparse_baseline.iloc[0]["wape"])
        c_wape = float(sparse_candidate.iloc[0]["wape"])
        if c_wape > b_wape * (1.0 + max_sparse_regression_pct):
            reasons.append(REASON_SPARSE)

    stability_value = float(best_candidate.get("stability_std_wape", 0.0) or 0.0)
    if stability_value > max_stability_std:
        reasons.append(REASON_STABILITY)

    latency_value = float(best_candidate.get("latency_ms", 0.0) or 0.0)
    if latency_value > max_latency_ms:
        reasons.append(REASON_LATENCY)

    if not isinstance(best_candidate.get("mlflow_run_id"), str) or not str(best_candidate.get("mlflow_run_id")).strip():
        reasons.append(REASON_METADATA)

    passed = len(reasons) == 0
    return {
        "passed": passed,
        "reasons": reasons,
        "primary_metric": primary_metric,
        "best_baseline": best_baseline.to_dict(),
        "best_candidate": best_candidate.to_dict(),
    }


def run_selection(context: Any, champion_cfg: dict[str, Any]) -> dict[str, Any]:
    run_dir = ensure_run_dir(context)
    leaderboard_path = run_dir / "metrics_summary.csv"
    slice_path = run_dir / "slice_metrics.csv"

    if not leaderboard_path.exists():
        raise FileNotFoundError(f"leaderboard not found: {leaderboard_path}")
    if not slice_path.exists():
        raise FileNotFoundError(f"slice metrics not found: {slice_path}")

    leaderboard = pd.read_csv(leaderboard_path)
    slice_metrics = pd.read_csv(slice_path)

    gate_cfg = dict(champion_cfg.get("gate", {}))
    decision = evaluate_champion_gate(leaderboard=leaderboard, slice_metrics=slice_metrics, policy=gate_cfg)

    decision_path = run_dir / "champion_decision.json"
    comparison_path = run_dir / "challenger_comparison.csv"

    decision_payload = {
        "run_id": context.run_id,
        "gate_passed": decision["passed"],
        "reason_codes": decision["reasons"],
        "primary_metric": decision["primary_metric"],
        "best_baseline": decision["best_baseline"],
        "best_candidate": decision["best_candidate"],
    }
    decision_path.write_text(json.dumps(decision_payload, indent=2, default=str), encoding="utf-8")

    comparison = leaderboard.sort_values(["model_role", gate_cfg.get("primary_metric", "wape")], ascending=[True, True])
    comparison.to_csv(comparison_path, index=False)

    return {
        "run_id": context.run_id,
        "decision_path": str(decision_path),
        "comparison_path": str(comparison_path),
        "gate_passed": decision["passed"],
        "reason_codes": decision["reasons"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select champion model via policy gate")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--training-config", default="configs/training.yaml")
    parser.add_argument("--split-policy", default="configs/split_policy.yaml")
    parser.add_argument("--model-search-space", default="configs/model_search_space.yaml")
    parser.add_argument("--champion-policy", default="configs/champion_policy.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context, _, _, _, champion_cfg = load_training_bundle(
        training_config_path=args.training_config,
        split_policy_path=args.split_policy,
        model_search_path=args.model_search_space,
        champion_policy_path=args.champion_policy,
        run_id=args.run_id,
    )
    output = run_selection(context, champion_cfg)
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
