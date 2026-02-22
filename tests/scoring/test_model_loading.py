# This test module validates the MLflow model loading logic used in Phase 5 scoring.
# It exists to ensure we can resolve a concrete model version from a registry stage without contacting a real server.
# The tests use small stubs/mocks so they run fast in CI and fail clearly when the loader contract changes.
# This guards against silent breakage when MLflow APIs or registry behavior evolves.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import mlflow
import numpy as np

from src.scoring import model_loader


@dataclass(frozen=True)
class _ModelVersionStub:
    version: str
    run_id: str


class _ClientStub:
    def get_latest_versions(self, name: str, stages: list[str]) -> list[_ModelVersionStub]:
        assert name == "ride-demand-forecast-model"
        assert stages == ["Staging"]
        return [_ModelVersionStub(version="7", run_id="run_abc")]


class _ModelStub:
    def predict(self, x: Any) -> np.ndarray:
        return np.zeros(len(x), dtype=float)


def test_load_champion_model_resolves_version_and_loads(monkeypatch: Any) -> None:
    called: dict[str, str] = {}

    monkeypatch.setattr(model_loader, "MlflowClient", lambda: _ClientStub())
    monkeypatch.setattr(model_loader.mlflow, "set_tracking_uri", lambda _: None)
    monkeypatch.setattr(mlflow, "set_tracking_uri", lambda _: None)

    def _load_model(uri: str) -> _ModelStub:
        called["uri"] = uri
        return _ModelStub()

    monkeypatch.setattr(model_loader.mlflow.pyfunc, "load_model", _load_model)
    monkeypatch.setattr(mlflow.pyfunc, "load_model", _load_model)

    loaded = model_loader.load_champion_model(
        tracking_uri="http://example",
        model_name="ride-demand-forecast-model",
        model_stage="Staging",
        required_feature_columns=["hour_of_day", "lag_1"],
    )

    assert loaded.model_version == "7"
    assert loaded.mlflow_run_id == "run_abc"
    assert called["uri"] == "models:/ride-demand-forecast-model/7"
