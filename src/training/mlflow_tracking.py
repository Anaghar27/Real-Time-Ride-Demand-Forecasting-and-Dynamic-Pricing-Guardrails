"""MLflow logging utilities for Phase 4 training."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from datetime import tzinfo as dt_tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import mlflow

from src.common.settings import get_settings
from src.training.training_config import TrainingContext


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return None


def configure_mlflow(experiment_name: str) -> None:
    settings = get_settings()
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        client.create_experiment(experiment_name, artifact_location="mlflow-artifacts:/")
    else:
        if str(exp.lifecycle_stage).lower() == "deleted":
            client.restore_experiment(exp.experiment_id)
            exp = client.get_experiment(exp.experiment_id)
        artifact_location = str(exp.artifact_location or "")
        if artifact_location.startswith("/") or artifact_location.startswith("file:/"):
            legacy_name = f"{experiment_name}_legacy_{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"
            client.rename_experiment(exp.experiment_id, legacy_name)
            client.create_experiment(experiment_name, artifact_location="mlflow-artifacts:/")
    mlflow.set_experiment(experiment_name)


def build_run_name(*, context: TrainingContext, model_role: str, model_name: str, split_id: str | None = None) -> str:
    run_id_short = str(context.run_id).split("-", maxsplit=1)[0]

    try:
        tz: dt_tzinfo = ZoneInfo(str(context.timezone))
    except Exception:  # noqa: BLE001
        tz = UTC

    start_local = context.start_ts.astimezone(tz)
    end_local_inclusive = (context.end_ts - timedelta(seconds=1)).astimezone(tz)
    interval = f"{start_local.date().isoformat()}..{end_local_inclusive.date().isoformat()}"

    split_suffix = f" split={split_id}" if split_id else ""
    return (
        f"{model_role}.{model_name}{split_suffix} | {interval} | "
        f"fv={context.feature_version} pv={context.policy_version} sp={context.split_policy_version} | "
        f"run={run_id_short}"
    )


def log_run(
    *,
    context: TrainingContext,
    run_name: str,
    model_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    slice_metrics: list[dict[str, Any]],
    artifacts: dict[str, Path],
    tags: dict[str, str],
    model: Any | None = None,
) -> str:
    configure_mlflow(context.experiment_name)

    run_tags = {
        "run_id": context.run_id,
        "model_name": model_name,
        "feature_version": context.feature_version,
        "policy_version": context.policy_version,
        "split_policy_version": context.split_policy_version,
        "data_start_ts": context.start_ts.isoformat(),
        "data_end_ts": context.end_ts.isoformat(),
        "data_timezone": str(context.timezone),
        "zone_scope": "all" if context.zone_ids is None else ",".join(str(value) for value in context.zone_ids),
        "quick_mode": str(bool(context.quick_mode)).lower(),
        **tags,
    }
    commit = _git_commit()
    if commit:
        run_tags["git_commit"] = commit

    with mlflow.start_run(run_name=run_name, tags=run_tags) as active_run:
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        for row in slice_metrics:
            prefix = row["slice_name"]
            for key in ["mae", "rmse", "wape", "smape"]:
                value = row.get(key)
                if value is not None and value == value:
                    mlflow.log_metric(f"slice.{prefix}.{key}", float(value))

        for artifact_name, artifact_path in artifacts.items():
            if artifact_path.exists():
                mlflow.log_artifact(str(artifact_path), artifact_path=artifact_name)

        config_snapshot = artifacts.get("config_snapshot")
        if config_snapshot and config_snapshot.exists():
            mlflow.log_artifact(str(config_snapshot), artifact_path="config")

        if model is not None:
            mlflow.sklearn.log_model(model, artifact_path="model")

        mlflow.log_text(json.dumps({"params": params, "metrics": metrics}, indent=2), "run_summary.json")
        return str(active_run.info.run_id)
