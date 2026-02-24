# This test module provides a lightweight smoke test for the Phase 5 scheduled scoring job wiring.
# It exists to ensure the Prefect flow can run locally and that deployment registration code is callable in CI.
# External systems (Prefect server, Postgres, MLflow) are mocked so the tests remain fast and deterministic.
# This catches accidental import or API changes before they break scheduling in production.

from __future__ import annotations

from typing import Any

from src.scoring import scoring_job


def test_scoring_flow_runs_with_mocked_orchestrator(monkeypatch: Any) -> None:
    class _LoggerStub:
        def info(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    monkeypatch.setattr(scoring_job, "run_scoring", lambda: {"status": "succeeded", "run_id": "test"})
    monkeypatch.setattr(scoring_job, "get_run_logger", lambda: _LoggerStub())
    # Call the underlying function to avoid creating a Prefect flow run in CI.
    result = scoring_job.scoring_flow.fn()
    assert result["status"] == "succeeded"
    assert result["run_id"] == "test"


def test_apply_deployment_builds_from_flow(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    applied = {"called": False}

    class _DeploymentStub:
        def apply(self) -> None:
            applied["called"] = True

    def _build_from_flow(**kwargs: Any) -> _DeploymentStub:
        captured.update(kwargs)
        return _DeploymentStub()

    monkeypatch.setattr(scoring_job.Deployment, "build_from_flow", staticmethod(_build_from_flow))
    scoring_job.apply_deployment(every_minutes=15, work_pool="scoring-process", work_queue="scoring")

    assert captured["name"] == "scheduled"
    assert captured["work_pool_name"] == "scoring-process"
    assert captured["work_queue_name"] == "scoring"
    assert applied["called"] is True
