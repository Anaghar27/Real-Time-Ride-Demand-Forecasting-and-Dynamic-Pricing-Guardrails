# This test file provides a smoke check for pricing scheduling job wiring.
# It exists to catch import and deployment API regressions before runtime.
# Prefect and orchestrator side effects are mocked so tests remain deterministic.
# The suite verifies both flow execution and deployment registration arguments.

from __future__ import annotations

from typing import Any

from src.pricing_guardrails import pricing_job


def test_pricing_flow_runs_with_mocked_orchestrator(monkeypatch: Any) -> None:
    class _LoggerStub:
        def info(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    monkeypatch.setattr(pricing_job, "run_pricing", lambda step="save": {"status": "succeeded", "run_id": "test"})
    monkeypatch.setattr(pricing_job, "get_run_logger", lambda: _LoggerStub())

    result = pricing_job.pricing_flow.fn()
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

    monkeypatch.setattr(pricing_job.Deployment, "build_from_flow", staticmethod(_build_from_flow))
    pricing_job.apply_deployment(every_minutes=15, work_pool="pricing-process", work_queue="pricing")

    assert captured["name"] == "scheduled"
    assert captured["work_pool_name"] == "pricing-process"
    assert captured["work_queue_name"] == "pricing"
    assert applied["called"] is True
