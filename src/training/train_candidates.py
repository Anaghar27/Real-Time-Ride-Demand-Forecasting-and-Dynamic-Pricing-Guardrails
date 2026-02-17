"""
Train and tune candidate models against a fixed chronological split.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import argparse
import itertools
import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet

from src.training.dataset_builder import build_split_manifest, prepare_dataset
from src.training.evaluate_models import (
    compute_global_metrics,
    compute_slice_metrics,
    estimate_inference_latency_ms,
    estimate_model_size_bytes,
    persist_leaderboard,
    persist_metrics_to_db,
    persist_prediction_artifacts,
    persist_slice_csv,
    persist_slice_metrics_to_db,
)
from src.training.mlflow_tracking import build_run_name, log_run
from src.training.training_config import ensure_run_dir, load_training_bundle

_LGBMRegressor: type[Any] | None = None
_LIGHTGBM_IMPORT_ERROR: Exception | None = None
try:
    from lightgbm import LGBMRegressor as _LGBMRegressorImported
except Exception as exc:  # noqa: BLE001
    _LIGHTGBM_IMPORT_ERROR = exc
else:
    _LGBMRegressor = _LGBMRegressorImported

_CatBoostRegressor: type[Any] | None = None
_CATBOOST_IMPORT_ERROR: Exception | None = None
try:
    from catboost import CatBoostRegressor as _CatBoostRegressorImported
except Exception as exc:  # noqa: BLE001
    _CATBOOST_IMPORT_ERROR = exc
else:
    _CatBoostRegressor = _CatBoostRegressorImported

_XGBRegressor: type[Any] | None = None
_XGBOOST_IMPORT_ERROR: Exception | None = None
try:
    from xgboost import XGBRegressor as _XGBRegressorImported
except Exception as exc:  # noqa: BLE001
    _XGBOOST_IMPORT_ERROR = exc
else:
    _XGBRegressor = _XGBRegressorImported

FEATURE_COLUMNS = [
    "hour_of_day",
    "quarter_hour_index",
    "day_of_week",
    "is_weekend",
    "week_of_year",
    "month",
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


@dataclass(frozen=True)
class CandidateResult:
    model_name: str
    best_params: dict[str, Any]
    validation_metrics: dict[str, float]
    test_metrics: dict[str, float]
    latency_ms: float
    model_size_bytes: int
    mlflow_run_id: str


def _search_space_to_grid(space: dict[str, Any], quick_mode: bool, quick_trials: int, full_trials: int) -> list[dict[str, Any]]:
    keys = sorted(space.keys())
    values = [list(space[key]) for key in keys]
    all_combos = [dict(zip(keys, combo, strict=False)) for combo in itertools.product(*values)]
    limit = quick_trials if quick_mode else full_trials
    return all_combos[: min(limit, len(all_combos))]


def _target_has_variation(y: np.ndarray) -> bool:
    if y.size == 0:
        return False
    return float(np.min(y)) != float(np.max(y))


def _fit_predict(model_name: str, params: dict[str, Any], x_train: pd.DataFrame, y_train: np.ndarray, x_scored: pd.DataFrame) -> tuple[Any, np.ndarray]:
    if model_name == "lightgbm":
        if _LGBMRegressor is None:
            raise ImportError(f"lightgbm unavailable: {_LIGHTGBM_IMPORT_ERROR}")
        model = _LGBMRegressor(**params)
        model.fit(x_train, y_train)
        return model, np.clip(model.predict(x_scored), 0.0, None)

    if model_name == "catboost":
        if _CatBoostRegressor is None:
            raise ImportError(f"catboost unavailable: {_CATBOOST_IMPORT_ERROR}")
        model = _CatBoostRegressor(**params)
        model.fit(x_train, y_train, verbose=False)
        return model, np.clip(model.predict(x_scored), 0.0, None)

    if model_name == "xgboost":
        if _XGBRegressor is None:
            raise ImportError(f"xgboost unavailable: {_XGBOOST_IMPORT_ERROR}")
        model = _XGBRegressor(**params)
        model.fit(x_train, y_train)
        return model, np.clip(model.predict(x_scored), 0.0, None)

    if model_name == "linear_elasticnet":
        model = ElasticNet(**params)
        model.fit(x_train, y_train)
        return model, np.clip(model.predict(x_scored), 0.0, None)

    raise ValueError(f"unsupported candidate model: {model_name}")


def _select_best_params(
    *,
    model_name: str,
    search_space: dict[str, Any],
    primary_metric: str,
    quick_mode: bool,
    quick_trials: int,
    full_trials: int,
    x_train: pd.DataFrame,
    y_train: np.ndarray,
    x_val: pd.DataFrame,
    y_val: np.ndarray,
) -> tuple[dict[str, Any], dict[str, float]]:
    grid = _search_space_to_grid(search_space, quick_mode, quick_trials, full_trials)
    if not grid:
        raise ValueError(f"empty search grid for model {model_name}")

    best_params: dict[str, Any] | None = None
    best_metrics: dict[str, float] | None = None
    best_score = float("inf")
    last_exc: Exception | None = None
    for params in grid:
        try:
            _, val_pred = _fit_predict(model_name, params, x_train, y_train, x_val)
            metrics = compute_global_metrics(y_val, val_pred)
            score = float(metrics[primary_metric])
            if score < best_score:
                best_score = score
                best_params = params
                best_metrics = metrics
        except ImportError:
            raise
        except Exception as exc:
            last_exc = exc
            continue

    if best_params is None or best_metrics is None:
        suffix = f"; last_error={last_exc!r}" if last_exc else ""
        raise RuntimeError(f"failed to find best params for {model_name}{suffix}")
    return best_params, best_metrics


def run_candidates(
    context: Any,
    training_cfg: dict[str, Any],
    split_cfg: dict[str, Any],
    search_cfg: dict[str, Any],
) -> dict[str, Any]:
    prepared = prepare_dataset(context, split_cfg)
    split = prepared.holdout
    frame = prepared.frame.sort_values(["bucket_start_ts", "zone_id"]).reset_index(drop=True)

    train_df = frame.loc[split.train_mask]
    val_df = frame.loc[split.val_mask]
    test_df = frame.loc[split.test_mask]

    x_train = train_df[FEATURE_COLUMNS].fillna(0.0)
    y_train = train_df["pickup_count"].to_numpy(dtype=float)
    x_val = val_df[FEATURE_COLUMNS].fillna(0.0)
    y_val = val_df["pickup_count"].to_numpy(dtype=float)
    x_test = test_df[FEATURE_COLUMNS].fillna(0.0)
    y_test = test_df["pickup_count"].to_numpy(dtype=float)

    eval_cfg = dict(training_cfg.get("evaluation", {}))
    slices = list(eval_cfg.get("slice_definitions", ["peak_hours", "off_peak_hours", "weekday", "weekend", "robust_zones", "sparse_zones"]))
    primary_metric = str(eval_cfg.get("primary_metric", "wape"))

    search_runtime = dict(search_cfg.get("search_runtime", {}))
    quick_trials = int(search_runtime.get("quick_trials_per_model", 8))
    full_trials = int(search_runtime.get("full_trials_per_model", 24))

    candidate_spaces = dict(search_cfg.get("candidates", {}))
    results: list[CandidateResult] = []
    slice_rows_all: list[dict[str, Any]] = []
    leaderboard_rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    run_dir = ensure_run_dir(context)
    config_snapshot = run_dir / "training_config_snapshot.json"
    config_snapshot.write_text(
        json.dumps({"training": training_cfg, "split": split_cfg, "search": search_cfg, "manifest": build_split_manifest(prepared, context)}, indent=2, default=str),
        encoding="utf-8",
    )

    if not _target_has_variation(y_train):
        raise ValueError("candidate training targets are constant in holdout train split")

    for model_name, search_space in candidate_spaces.items():
        try:
            best_params, val_metrics = _select_best_params(
                model_name=model_name,
                search_space=dict(search_space),
                primary_metric=primary_metric,
                quick_mode=context.quick_mode,
                quick_trials=quick_trials,
                full_trials=full_trials,
                x_train=x_train,
                y_train=y_train,
                x_val=x_val,
                y_val=y_val,
            )
        except ImportError as exc:
            failures.append(
                {
                    "model_name": model_name,
                    "reason_code": "dependency_missing",
                    "message": str(exc),
                }
            )
            continue
        except Exception as exc:
            failures.append(
                {
                    "model_name": model_name,
                    "reason_code": "param_search_failed",
                    "message": str(exc),
                }
            )
            continue

        train_val_df = pd.concat([train_df, val_df], axis=0, ignore_index=True)
        x_train_val = train_val_df[FEATURE_COLUMNS].fillna(0.0)
        y_train_val = train_val_df["pickup_count"].to_numpy(dtype=float)

        if not _target_has_variation(y_train_val):
            failures.append(
                {
                    "model_name": model_name,
                    "reason_code": "constant_target",
                    "message": "train+validation targets are constant; skipping model",
                }
            )
            continue

        try:
            model, test_pred = _fit_predict(model_name, best_params, x_train_val, y_train_val, x_test)
        except Exception as exc:
            failures.append(
                {
                    "model_name": model_name,
                    "reason_code": "fit_failed",
                    "message": str(exc),
                }
            )
            continue
        test_metrics = compute_global_metrics(y_test, test_pred)
        slice_rows = compute_slice_metrics(test_df, y_test, test_pred, slices)
        slice_rows_all.extend([{"model_name": model_name, **row} for row in slice_rows])

        fold_wapes: list[float] = []
        for rolling in prepared.rolling:
            fold_train = frame.loc[rolling.train_mask]
            fold_test = frame.loc[rolling.test_mask]
            if fold_train.empty or fold_test.empty:
                continue
            fold_x_train = fold_train[FEATURE_COLUMNS].fillna(0.0)
            fold_y_train = fold_train["pickup_count"].to_numpy(dtype=float)
            if not _target_has_variation(fold_y_train):
                continue
            fold_x_test = fold_test[FEATURE_COLUMNS].fillna(0.0)
            fold_y_test = fold_test["pickup_count"].to_numpy(dtype=float)
            try:
                _, fold_pred = _fit_predict(model_name, best_params, fold_x_train, fold_y_train, fold_x_test)
            except Exception:
                continue
            fold_metrics = compute_global_metrics(fold_y_test, fold_pred)
            fold_wapes.append(float(fold_metrics["wape"]))

        stability_std_wape = float(np.std(fold_wapes)) if len(fold_wapes) >= 2 else float("nan")
        latency_ms = estimate_inference_latency_ms(model, x_test)
        model_size_bytes = estimate_model_size_bytes(model)

        scored = test_df.copy()
        scored["prediction"] = test_pred
        scored["residual"] = scored["pickup_count"].astype(float) - scored["prediction"].astype(float)
        artifacts = persist_prediction_artifacts(context=context, scored_df=scored, model_name=model_name)
        artifacts["config_snapshot"] = config_snapshot

        mlflow_run_id = log_run(
            context=context,
            run_name=build_run_name(
                context=context,
                model_role="candidate",
                model_name=model_name,
                split_id=split.split_id,
            ),
            model_name=model_name,
            params={
                "model_type": model_name,
                **best_params,
                "split_id": split.split_id,
                "feature_version": context.feature_version,
                "policy_version": context.policy_version,
                "run_id": context.run_id,
            },
            metrics={
                **{f"validation_{k}": v for k, v in val_metrics.items()},
                **{f"test_{k}": v for k, v in test_metrics.items()},
                "latency_ms_per_row": latency_ms,
                "model_size_bytes": float(model_size_bytes),
                "stability_std_wape": stability_std_wape,
            },
            slice_metrics=slice_rows,
            artifacts=artifacts,
            tags={"stage": "candidate"},
            model=model,
        )

        persist_metrics_to_db(
            context=context,
            model_name=model_name,
            role="candidate",
            split_id=split.split_id,
            metrics=test_metrics,
            latency_ms=latency_ms,
            model_size_bytes=model_size_bytes,
            mlflow_run_id=mlflow_run_id,
            metadata={"best_params": best_params, "validation_metrics": val_metrics},
        )
        persist_slice_metrics_to_db(context=context, model_name=model_name, split_id=split.split_id, slice_rows=slice_rows)

        results.append(
            CandidateResult(
                model_name=model_name,
                best_params=best_params,
                validation_metrics=val_metrics,
                test_metrics=test_metrics,
                latency_ms=latency_ms,
                model_size_bytes=model_size_bytes,
                mlflow_run_id=mlflow_run_id,
            )
        )
        leaderboard_rows.append(
            {
                "model_name": model_name,
                "model_role": "candidate",
                **test_metrics,
                "latency_ms": latency_ms,
                "model_size_bytes": model_size_bytes,
                "mlflow_run_id": mlflow_run_id,
                "stability_std_wape": stability_std_wape,
            }
        )

    if not leaderboard_rows:
        detail = json.dumps(failures, indent=2) if failures else "[]"
        raise RuntimeError(
            "No candidate models trained. Check model dependencies, feature coverage, and data window. "
            f"Failures: {detail}"
        )

    leaderboard = pd.DataFrame(leaderboard_rows).sort_values(primary_metric, ascending=True).reset_index(drop=True)
    leaderboard_path = persist_leaderboard(context, leaderboard)
    slice_path = persist_slice_csv(context, slice_rows_all)

    return {
        "run_id": context.run_id,
        "primary_metric": primary_metric,
        "leaderboard_path": str(leaderboard_path),
        "slice_metrics_path": str(slice_path),
        "failures": failures,
        "results": [
            {
                "model_name": row.model_name,
                "best_params": row.best_params,
                "validation_metrics": row.validation_metrics,
                "test_metrics": row.test_metrics,
                "latency_ms": row.latency_ms,
                "model_size_bytes": row.model_size_bytes,
                "mlflow_run_id": row.mlflow_run_id,
            }
            for row in results
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and tune candidate models")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--training-config", default="configs/training.yaml")
    parser.add_argument("--split-policy", default="configs/split_policy.yaml")
    parser.add_argument("--model-search-space", default="configs/model_search_space.yaml")
    parser.add_argument("--champion-policy", default="configs/champion_policy.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context, training_cfg, split_cfg, search_cfg, _ = load_training_bundle(
        training_config_path=args.training_config,
        split_policy_path=args.split_policy,
        model_search_path=args.model_search_space,
        champion_policy_path=args.champion_policy,
        run_id=args.run_id,
    )
    output = run_candidates(context, training_cfg, split_cfg, search_cfg)
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
