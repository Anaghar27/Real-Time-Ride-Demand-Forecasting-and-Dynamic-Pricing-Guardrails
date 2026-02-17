"""
Evaluation helpers for baseline and challenger models.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import json
import math
import pickle
import time
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.training.training_config import TrainingContext, ensure_run_dir

EPSILON = 1e-6


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean(np.square(y_true - y_pred))))


def wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = float(np.sum(np.abs(y_true)))
    return float(np.sum(np.abs(y_true - y_pred)) / max(denom, EPSILON))


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.abs(y_true) + np.abs(y_pred)
    ratio = 2.0 * np.abs(y_pred - y_true) / np.maximum(denom, EPSILON)
    return float(np.mean(ratio))


def compute_global_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": mae(y_true, y_pred),
        "rmse": rmse(y_true, y_pred),
        "wape": wape(y_true, y_pred),
        "smape": smape(y_true, y_pred),
    }


def _slice_mask(df: pd.DataFrame, slice_name: str) -> pd.Series:
    if slice_name == "peak_hours":
        return df["hour_of_day"].isin([7, 8, 9, 16, 17, 18, 19])
    if slice_name == "off_peak_hours":
        return ~df["hour_of_day"].isin([7, 8, 9, 16, 17, 18, 19])
    if slice_name == "weekday":
        return ~df["is_weekend"].astype(bool)
    if slice_name == "weekend":
        return df["is_weekend"].astype(bool)
    if slice_name == "robust_zones":
        return df["sparsity_class"].fillna("").eq("robust")
    if slice_name == "sparse_zones":
        return df["sparsity_class"].fillna("").isin(["sparse", "ultra_sparse"])
    raise ValueError(f"unknown slice_name: {slice_name}")


def compute_slice_metrics(
    frame: pd.DataFrame,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    slices: list[str],
) -> list[dict[str, Any]]:
    scored = frame.copy()
    scored["_y_true"] = y_true
    scored["_y_pred"] = y_pred

    rows: list[dict[str, Any]] = []
    for slice_name in slices:
        mask = _slice_mask(scored, slice_name)
        if not mask.any():
            rows.append(
                {
                    "slice_name": slice_name,
                    "rows": 0,
                    "mae": math.nan,
                    "rmse": math.nan,
                    "wape": math.nan,
                    "smape": math.nan,
                }
            )
            continue

        part = scored.loc[mask]
        metrics = compute_global_metrics(part["_y_true"].to_numpy(dtype=float), part["_y_pred"].to_numpy(dtype=float))
        rows.append({"slice_name": slice_name, "rows": int(len(part)), **metrics})

    return rows


def estimate_inference_latency_ms(model: Any, x_test: pd.DataFrame) -> float:
    start = time.perf_counter()
    _ = model.predict(x_test)
    elapsed = (time.perf_counter() - start) * 1000.0
    return float(elapsed / max(len(x_test), 1))


def estimate_model_size_bytes(model: Any) -> int:
    return int(len(pickle.dumps(model)))


def persist_metrics_to_db(
    *,
    context: TrainingContext,
    model_name: str,
    role: str,
    split_id: str,
    metrics: dict[str, float],
    latency_ms: float | None,
    model_size_bytes: int | None,
    mlflow_run_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    payload = {
        "run_id": context.run_id,
        "model_name": model_name,
        "model_role": role,
        "split_id": split_id,
        "mae": metrics["mae"],
        "rmse": metrics["rmse"],
        "wape": metrics["wape"],
        "smape": metrics["smape"],
        "latency_ms": latency_ms,
        "model_size_bytes": model_size_bytes,
        "mlflow_run_id": mlflow_run_id,
        "extra_metadata": json.dumps(metadata or {}),
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO training_metrics (
                    run_id,
                    model_name,
                    model_role,
                    split_id,
                    mae,
                    rmse,
                    wape,
                    smape,
                    latency_ms,
                    model_size_bytes,
                    mlflow_run_id,
                    extra_metadata
                ) VALUES (
                    :run_id,
                    :model_name,
                    :model_role,
                    :split_id,
                    :mae,
                    :rmse,
                    :wape,
                    :smape,
                    :latency_ms,
                    :model_size_bytes,
                    :mlflow_run_id,
                    CAST(:extra_metadata AS JSONB)
                )
                """
            ),
            payload,
        )


def persist_slice_metrics_to_db(
    *,
    context: TrainingContext,
    model_name: str,
    split_id: str,
    slice_rows: list[dict[str, Any]],
) -> None:
    with engine.begin() as connection:
        for row in slice_rows:
            connection.execute(
                text(
                    """
                    INSERT INTO training_slice_metrics (
                        run_id,
                        model_name,
                        split_id,
                        slice_name,
                        row_count,
                        mae,
                        rmse,
                        wape,
                        smape
                    ) VALUES (
                        :run_id,
                        :model_name,
                        :split_id,
                        :slice_name,
                        :row_count,
                        :mae,
                        :rmse,
                        :wape,
                        :smape
                    )
                    """
                ),
                {
                    "run_id": context.run_id,
                    "model_name": model_name,
                    "split_id": split_id,
                    "slice_name": row["slice_name"],
                    "row_count": row["rows"],
                    "mae": row["mae"],
                    "rmse": row["rmse"],
                    "wape": row["wape"],
                    "smape": row["smape"],
                },
            )


def persist_prediction_artifacts(
    *,
    context: TrainingContext,
    scored_df: pd.DataFrame,
    model_name: str,
    top_n: int = 500,
) -> dict[str, Path]:
    run_dir = ensure_run_dir(context)

    sample_path = run_dir / "prediction_sample.csv"
    residual_path = run_dir / "residual_plot.png"

    sample = scored_df[["zone_id", "bucket_start_ts", "pickup_count", "prediction", "residual"]].head(top_n)
    sample.to_csv(sample_path, index=False)

    plt.figure(figsize=(8, 4))
    plt.scatter(scored_df["prediction"].head(3000), scored_df["residual"].head(3000), s=6, alpha=0.5)
    plt.axhline(0.0, color="black", linestyle="--", linewidth=1)
    plt.title(f"Residuals: {model_name}")
    plt.xlabel("Prediction")
    plt.ylabel("Residual")
    plt.tight_layout()
    plt.savefig(residual_path)
    plt.close()

    return {"prediction_sample": sample_path, "residual_plot": residual_path}


def persist_leaderboard(context: TrainingContext, leaderboard: pd.DataFrame) -> Path:
    run_dir = ensure_run_dir(context)
    path = run_dir / "metrics_summary.csv"
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, leaderboard], axis=0, ignore_index=True)
        combined = combined.drop_duplicates(subset=["model_name", "model_role"], keep="last")
        combined.to_csv(path, index=False)
    else:
        leaderboard.to_csv(path, index=False)
    return path


def persist_slice_csv(context: TrainingContext, slice_rows: list[dict[str, Any]]) -> Path:
    run_dir = ensure_run_dir(context)
    path = run_dir / "slice_metrics.csv"
    incoming = pd.DataFrame(slice_rows)
    if path.exists():
        existing = pd.read_csv(path)
        merged = pd.concat([existing, incoming], axis=0, ignore_index=True)
        merged.to_csv(path, index=False)
    else:
        incoming.to_csv(path, index=False)
    return path
