# This module defines the scheduled pricing guardrails job using Prefect.
# It exists so pricing decisions can be refreshed continuously without manual CLI execution.
# The flow delegates logic to the pricing orchestrator, which handles overlap locks and idempotent writes.
# A small CLI helper is included to apply deployments for local or managed scheduling.

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

from src.pricing_guardrails.pricing_config import load_pricing_config
from src.pricing_guardrails.pricing_orchestrator import run_pricing


@flow(name="ride-demand-pricing-guardrails", retries=2, retry_delay_seconds=60)
def pricing_flow() -> dict[str, Any]:
    logger = get_run_logger()
    result = run_pricing(step="save")
    logger.info("pricing completed status=%s run_id=%s", result.get("status"), result.get("run_id"))
    return result


def apply_deployment(*, every_minutes: int, work_pool: str, work_queue: str) -> None:
    schedule = IntervalSchedule(interval=cast(Any, timedelta(minutes=every_minutes)))
    repo_root = str(Path(__file__).resolve().parents[2])
    deployment = cast(
        Any,
        Deployment.build_from_flow(
            flow=pricing_flow,
            name="scheduled",
            schedule=schedule,
            work_pool_name=work_pool,
            work_queue_name=work_queue,
            tags=["pricing", "guardrails"],
            path=repo_root,
            entrypoint="src/pricing_guardrails/pricing_job.py:pricing_flow",
            load_existing=False,
        ),
    )
    deployment.apply()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prefect pricing job utilities")
    parser.add_argument("--apply-deployment", action="store_true", help="Register or refresh scheduled deployment")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_pricing_config()
    if args.apply_deployment:
        apply_deployment(
            every_minutes=cfg.prefect_schedule_minutes,
            work_pool=cfg.prefect_work_pool,
            work_queue=cfg.prefect_work_queue,
        )
        print(
            "Deployment applied. Start a worker with: "
            f"prefect worker start --pool {cfg.prefect_work_pool} --work-queue {cfg.prefect_work_queue}"
        )


if __name__ == "__main__":
    main()
