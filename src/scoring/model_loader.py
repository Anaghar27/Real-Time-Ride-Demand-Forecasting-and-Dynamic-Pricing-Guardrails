# This module loads the currently promoted forecasting model from the MLflow Model Registry.
# It exists to decouple scoring from training details and to make “which model did we score with?” explicit.
# The loader resolves a concrete model version from a registry stage and validates the expected feature inputs.
# Failing fast here prevents silent schema drift and broken downstream forecasts.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient


@dataclass(frozen=True)
class LoadedModel:
    model_name: str
    model_stage: str
    model_version: str
    mlflow_run_id: str
    model: Any


def resolve_model_version(*, model_name: str, model_stage: str) -> tuple[str, str]:
    client = MlflowClient()
    stage = model_stage.strip()
    latest = client.get_latest_versions(model_name, stages=[stage])
    if not latest:
        raise RuntimeError(f"No model versions found for model={model_name!r} stage={stage!r}")
    chosen = latest[0]
    return str(chosen.version), str(chosen.run_id)


def _validate_predict_signature(model: Any, required_columns: list[str]) -> None:
    sample = pd.DataFrame([{name: 0.0 for name in required_columns}])
    try:
        pred = model.predict(sample)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Model predict failed on required columns: {required_columns}") from exc
    if getattr(pred, "shape", None) is None:
        raise RuntimeError("Model predict returned non-array output")


def load_champion_model(
    *,
    tracking_uri: str,
    model_name: str,
    model_stage: str,
    required_feature_columns: list[str],
) -> LoadedModel:
    mlflow.set_tracking_uri(tracking_uri)

    model_version, mlflow_run_id = resolve_model_version(model_name=model_name, model_stage=model_stage)
    model_uri = f"models:/{model_name}/{model_version}"

    model = mlflow.pyfunc.load_model(model_uri)
    _validate_predict_signature(model, required_feature_columns)

    return LoadedModel(
        model_name=model_name,
        model_stage=model_stage,
        model_version=model_version,
        mlflow_run_id=mlflow_run_id,
        model=model,
    )

