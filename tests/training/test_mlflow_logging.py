"""
Tests for mlflow logging.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

from pathlib import Path

from src.training.mlflow_tracking import log_run
from src.training.training_config import TrainingContext


class StubRun:
    def __init__(self) -> None:
        self.info = type("Info", (), {"run_id": "stub-run-id"})()

    def __enter__(self) -> StubRun:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_mlflow_logging_completeness(monkeypatch, tmp_path: Path) -> None:
    logged = {"params": None, "metrics": None, "tags": None, "artifacts": []}

    monkeypatch.setattr("src.training.mlflow_tracking.configure_mlflow", lambda experiment_name: None)
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.start_run", lambda run_name, tags: StubRun())
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.log_params", lambda p: logged.__setitem__("params", p))
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.log_metrics", lambda m: logged.__setitem__("metrics", m))
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.log_metric", lambda name, value: None)
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.log_artifact", lambda path, artifact_path=None: logged["artifacts"].append((path, artifact_path)))
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.log_text", lambda text, artifact_file: None)
    monkeypatch.setattr("src.training.mlflow_tracking.mlflow.sklearn.log_model", lambda model, artifact_path: None)

    artifact = tmp_path / "dummy.txt"
    artifact.write_text("x", encoding="utf-8")

    context = TrainingContext(
        run_id="r1",
        experiment_name="exp",
        feature_version="v1",
        policy_version="p1",
        split_policy_version="sp1",
        start_ts=__import__("datetime").datetime.now(__import__("datetime").UTC),
        end_ts=__import__("datetime").datetime.now(__import__("datetime").UTC),
        zone_ids=None,
        timezone="UTC",
        output_dir=tmp_path,
        quick_mode=True,
    )

    run_id = log_run(
        context=context,
        run_name="test",
        model_name="linear",
        params={"alpha": 1.0},
        metrics={"test_wape": 0.1},
        slice_metrics=[{"slice_name": "weekday", "mae": 1.0, "rmse": 1.0, "wape": 0.1, "smape": 0.1}],
        artifacts={"dummy": artifact},
        tags={"stage": "unit"},
        model=None,
    )

    assert run_id == "stub-run-id"
    assert logged["params"] == {"alpha": 1.0}
    assert logged["metrics"] == {"test_wape": 0.1}
    assert logged["artifacts"]
