"""Register champion model in MLflow Model Registry with audit trail."""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import mlflow
from sqlalchemy import text

from src.common.db import engine
from src.common.settings import get_settings
from src.training.training_config import ensure_run_dir, load_training_bundle


class ChampionGateFailedError(RuntimeError):
    def __init__(self, message: str, *, decision: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.decision = decision or {}


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _load_decision(run_dir: Path) -> dict[str, Any]:
    decision_path = run_dir / "champion_decision.json"
    if not decision_path.exists():
        raise FileNotFoundError(f"champion_decision.json missing at {decision_path}")
    return dict(json.loads(decision_path.read_text(encoding="utf-8")))


def _set_stage(model_name: str, version: str, stage: str) -> None:
    client = mlflow.tracking.MlflowClient()
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=stage,
        archive_existing_versions=(stage == "Production"),
    )


def _persist_audit(
    *,
    run_id: str,
    model_name: str,
    model_version: str,
    stage: str,
    mlflow_run_id: str,
    status: str,
    reason_code: str | None,
    metadata: dict[str, Any],
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO model_registry_audit (
                    run_id,
                    model_name,
                    model_version,
                    stage,
                    mlflow_run_id,
                    status,
                    reason_code,
                    metadata,
                    created_at
                ) VALUES (
                    :run_id,
                    :model_name,
                    :model_version,
                    :stage,
                    :mlflow_run_id,
                    :status,
                    :reason_code,
                    CAST(:metadata AS JSONB),
                    :created_at
                )
                """
            ),
            {
                "run_id": run_id,
                "model_name": model_name,
                "model_version": model_version,
                "stage": stage,
                "mlflow_run_id": mlflow_run_id,
                "status": status,
                "reason_code": reason_code,
                "metadata": json.dumps(_sanitize_json_payload(metadata), default=str, allow_nan=False),
                "created_at": datetime.now(tz=UTC),
            },
        )


def register_champion(
    *,
    context: Any,
    training_cfg: dict[str, Any],
    champion_cfg: dict[str, Any],
    promote_to_production: bool,
) -> dict[str, Any]:
    run_dir = ensure_run_dir(context)
    decision = _load_decision(run_dir)
    if not bool(decision.get("gate_passed", False)):
        _persist_audit(
            run_id=context.run_id,
            model_name=str(dict(training_cfg.get("registry", {})).get("model_name", "ride-demand-forecast")),
            model_version="n/a",
            stage="None",
            mlflow_run_id=str(decision.get("best_candidate", {}).get("mlflow_run_id", "")),
            status="failed",
            reason_code="gate_failed",
            metadata=decision,
        )
        raise ChampionGateFailedError("Champion gate failed. Registration blocked.", decision=decision)

    best_candidate = dict(decision["best_candidate"])
    mlflow_run_id = str(best_candidate["mlflow_run_id"])

    settings = get_settings()
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)

    model_name = str(dict(training_cfg.get("registry", {})).get("model_name", "ride-demand-forecast"))
    model_uri = f"runs:/{mlflow_run_id}/model"
    registration = mlflow.register_model(model_uri=model_uri, name=model_name)
    model_version = str(registration.version)

    gate_cfg = dict(champion_cfg.get("registration", {}))
    allow_production = bool(gate_cfg.get("allow_production", False))
    if promote_to_production and not allow_production:
        raise RuntimeError("Production promotion blocked by champion policy")

    stage = "Production" if promote_to_production and allow_production else "Staging"
    _set_stage(model_name, model_version, stage)

    metadata = {
        "feature_version": context.feature_version,
        "policy_version": context.policy_version,
        "training_interval": {
            "start_ts": context.start_ts.isoformat(),
            "end_ts": context.end_ts.isoformat(),
        },
        "evaluation_summary": {
            "primary_metric": decision.get("primary_metric"),
            "best_candidate_metric": best_candidate.get(decision.get("primary_metric", "wape")),
        },
        "sparse_zone_policy_version": context.policy_version,
    }

    _persist_audit(
        run_id=context.run_id,
        model_name=model_name,
        model_version=model_version,
        stage=stage,
        mlflow_run_id=mlflow_run_id,
        status="registered",
        reason_code=None,
        metadata=metadata,
    )

    return {
        "run_id": context.run_id,
        "model_name": model_name,
        "model_version": model_version,
        "stage": stage,
        "mlflow_run_id": mlflow_run_id,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register champion model")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--promote-production", action="store_true")
    parser.add_argument("--training-config", default="configs/training.yaml")
    parser.add_argument("--split-policy", default="configs/split_policy.yaml")
    parser.add_argument("--model-search-space", default="configs/model_search_space.yaml")
    parser.add_argument("--champion-policy", default="configs/champion_policy.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context, training_cfg, _, _, champion_cfg = load_training_bundle(
        training_config_path=args.training_config,
        split_policy_path=args.split_policy,
        model_search_path=args.model_search_space,
        champion_policy_path=args.champion_policy,
        run_id=args.run_id,
    )
    result = register_champion(
        context=context,
        training_cfg=training_cfg,
        champion_cfg=champion_cfg,
        promote_to_production=args.promote_production,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
