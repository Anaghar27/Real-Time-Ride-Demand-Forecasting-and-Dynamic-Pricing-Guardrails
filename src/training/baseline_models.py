"""
Baseline model training for Phase 4.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from src.training.dataset_builder import build_split_manifest, prepare_dataset
from src.training.evaluate_models import (
    compute_global_metrics,
    compute_slice_metrics,
    persist_leaderboard,
    persist_metrics_to_db,
    persist_prediction_artifacts,
    persist_slice_csv,
    persist_slice_metrics_to_db,
)
from src.training.mlflow_tracking import build_run_name, log_run
from src.training.training_config import ensure_run_dir, load_training_bundle

CORE_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_holiday",
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

REQUIRED_BASELINE_COLUMNS = {"lag_96", "lag_672"}


def _build_scored_frame(test_df: pd.DataFrame, preds: np.ndarray) -> pd.DataFrame:
    scored = test_df.copy()
    scored["prediction"] = preds
    scored["residual"] = scored["pickup_count"].astype(float) - scored["prediction"].astype(float)
    return scored


def run_baselines(
    context: Any,
    training_cfg: dict[str, Any],
    split_cfg: dict[str, Any],
) -> dict[str, Any]:
    prepared = prepare_dataset(context, split_cfg)
    split = prepared.holdout
    frame = prepared.frame.sort_values(["bucket_start_ts", "zone_id"]).reset_index(drop=True)

    missing = REQUIRED_BASELINE_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"missing required lag columns for baseline models: {sorted(missing)}")

    train_df = frame.loc[split.train_mask]
    test_df = frame.loc[split.test_mask]
    if train_df.empty or test_df.empty:
        raise ValueError("train/test split yielded an empty partition")

    y_test = test_df["pickup_count"].to_numpy(dtype=float)
    slices = list(
        dict(training_cfg.get("evaluation", {})).get(
            "slice_definitions",
            ["peak_hours", "off_peak_hours", "weekday", "weekend", "robust_zones", "sparse_zones"],
        )
    )

    results: list[dict[str, Any]] = []
    all_slice_rows: list[dict[str, Any]] = []

    naive_day_pred = test_df["lag_96"].fillna(0.0).to_numpy(dtype=float)
    naive_week_pred = test_df["lag_672"].fillna(0.0).to_numpy(dtype=float)

    model_specs = [
        ("naive_previous_day", naive_day_pred, None),
        ("naive_previous_week", naive_week_pred, None),
    ]

    x_train = train_df[CORE_FEATURES].fillna(0.0)
    y_train = train_df["pickup_count"].to_numpy(dtype=float)
    x_test = test_df[CORE_FEATURES].fillna(0.0)

    alpha = float(dict(training_cfg.get("models", {})).get("linear_baseline_alpha", 1.0))
    ridge = Ridge(alpha=alpha, random_state=42)
    ridge.fit(x_train, y_train)
    ridge_pred = np.clip(ridge.predict(x_test), 0.0, None)
    model_specs.append(("linear_baseline", ridge_pred, ridge))

    run_dir = ensure_run_dir(context)
    config_snapshot = run_dir / "training_config_snapshot.json"
    config_snapshot.write_text(
        json.dumps(
            {
                "training": training_cfg,
                "split": split_cfg,
                "split_manifest": build_split_manifest(prepared, context),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    for model_name, preds, model_obj in model_specs:
        metrics = compute_global_metrics(y_test, preds)
        slices_rows = compute_slice_metrics(test_df, y_test, preds, slices)
        all_slice_rows.extend([{"model_name": model_name, **row} for row in slices_rows])

        scored = _build_scored_frame(test_df, preds)
        artifacts = persist_prediction_artifacts(context=context, scored_df=scored, model_name=model_name)
        artifacts["config_snapshot"] = config_snapshot

        run_id = log_run(
            context=context,
            run_name=build_run_name(
                context=context,
                model_role="baseline",
                model_name=model_name,
                split_id=split.split_id,
            ),
            model_name=model_name,
            params={
                "model_type": model_name,
                "split_id": split.split_id,
                "feature_version": context.feature_version,
                "policy_version": context.policy_version,
                "run_id": context.run_id,
            },
            metrics={f"test_{k}": v for k, v in metrics.items()},
            slice_metrics=slices_rows,
            artifacts=artifacts,
            tags={"stage": "baseline"},
            model=model_obj,
        )

        persist_metrics_to_db(
            context=context,
            model_name=model_name,
            role="baseline",
            split_id=split.split_id,
            metrics=metrics,
            latency_ms=None,
            model_size_bytes=None,
            mlflow_run_id=run_id,
            metadata={"type": "baseline"},
        )
        persist_slice_metrics_to_db(
            context=context,
            model_name=model_name,
            split_id=split.split_id,
            slice_rows=slices_rows,
        )

        row = {
            "model_name": model_name,
            "model_role": "baseline",
            **metrics,
            "latency_ms": np.nan,
            "model_size_bytes": np.nan,
            "mlflow_run_id": run_id,
            "stability_std_wape": np.nan,
        }
        results.append(row)

    leaderboard = pd.DataFrame(results).sort_values("wape", ascending=True).reset_index(drop=True)
    metrics_path = persist_leaderboard(context, leaderboard)
    slice_path = persist_slice_csv(context, all_slice_rows)

    return {
        "run_id": context.run_id,
        "rows": int(len(frame)),
        "models": results,
        "leaderboard_path": str(metrics_path),
        "slice_metrics_path": str(slice_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run baseline training")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--training-config", default="configs/training.yaml")
    parser.add_argument("--split-policy", default="configs/split_policy.yaml")
    parser.add_argument("--model-search-space", default="configs/model_search_space.yaml")
    parser.add_argument("--champion-policy", default="configs/champion_policy.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context, training_cfg, split_cfg, _, _ = load_training_bundle(
        training_config_path=args.training_config,
        split_policy_path=args.split_policy,
        model_search_path=args.model_search_space,
        champion_policy_path=args.champion_policy,
        run_id=args.run_id,
    )
    output = run_baselines(context, training_cfg, split_cfg)
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
