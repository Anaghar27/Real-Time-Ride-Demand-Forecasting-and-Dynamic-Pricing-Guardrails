from __future__ import annotations

import pandas as pd

from src.training.select_champion import REASON_BASELINE, evaluate_champion_gate


def test_champion_gate_fails_when_not_beating_baseline() -> None:
    leaderboard = pd.DataFrame(
        [
            {"model_name": "naive_previous_day", "model_role": "baseline", "wape": 0.20, "latency_ms": 0.1, "stability_std_wape": 0.0, "mlflow_run_id": "b1"},
            {"model_name": "xgboost", "model_role": "candidate", "wape": 0.199, "latency_ms": 0.1, "stability_std_wape": 0.0, "mlflow_run_id": "c1"},
        ]
    )
    slices = pd.DataFrame(
        [
            {"model_name": "naive_previous_day", "slice_name": "sparse_zones", "wape": 0.30},
            {"model_name": "xgboost", "slice_name": "sparse_zones", "wape": 0.30},
        ]
    )
    policy = {
        "primary_metric": "wape",
        "min_improvement_over_baseline_pct": 0.02,
        "max_sparse_wape_regression_pct": 0.03,
        "max_stability_std_wape": 0.03,
        "max_latency_ms_per_row": 2.0,
    }

    result = evaluate_champion_gate(leaderboard=leaderboard, slice_metrics=slices, policy=policy)
    assert result["passed"] is False
    assert REASON_BASELINE in result["reasons"]
