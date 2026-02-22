# This module defines the scheduled scoring job using Prefect.
# It exists so demand forecasts can be produced automatically at a fixed cadence without manual CLI runs.
# The Prefect flow delegates the heavy lifting to the scoring orchestrator and relies on Postgres locking to avoid overlap.
# A small CLI is included to register a deployment and help operators start a worker for local scheduling.

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

from src.scoring.scoring_config import load_scoring_config
from src.scoring.scoring_orchestrator import run_scoring


@flow(name="ride-demand-scoring", retries=2, retry_delay_seconds=60)
def scoring_flow() -> dict[str, Any]:
    logger = get_run_logger()
    result = run_scoring()
    logger.info("scoring completed status=%s run_id=%s", result.get("status"), result.get("run_id"))
    return result


def apply_deployment(*, every_minutes: int, work_pool: str, work_queue: str) -> None:
    schedule = IntervalSchedule(interval=cast(Any, timedelta(minutes=every_minutes)))
    repo_root = str(Path(__file__).resolve().parents[2])
    deployment = cast(
        Any,
        Deployment.build_from_flow(
            flow=scoring_flow,
            name="scheduled",
            schedule=schedule,
            work_pool_name=work_pool,
            work_queue_name=work_queue,
            tags=["scoring"],
            # Avoid stale path metadata when the repository is moved/renamed.
            path=repo_root,
            entrypoint="src/scoring/scoring_job.py:scoring_flow",
            load_existing=False,
        ),
    )
    deployment.apply()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prefect scoring job utilities")
    parser.add_argument("--apply-deployment", action="store_true", help="Register/refresh the scheduled deployment")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_scoring_config()
    if args.apply_deployment:
        apply_deployment(
            every_minutes=cfg.scoring_frequency_minutes,
            work_pool=cfg.prefect_work_pool,
            work_queue=cfg.prefect_work_queue,
        )
        print(
            f"Deployment applied. Start a worker with: prefect worker start --pool {cfg.prefect_work_pool} --work-queue {cfg.prefect_work_queue}"
        )


if __name__ == "__main__":
    main()
