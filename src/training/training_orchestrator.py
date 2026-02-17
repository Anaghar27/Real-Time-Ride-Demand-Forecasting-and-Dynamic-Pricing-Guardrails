"""
Phase 4 training orchestration entrypoint.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.training.baseline_models import run_baselines
from src.training.dataset_builder import (
    build_split_manifest,
    persist_split_manifest,
    prepare_dataset,
)
from src.training.register_model import ChampionGateFailedError, register_champion
from src.training.select_champion import run_selection
from src.training.train_candidates import run_candidates
from src.training.training_config import ensure_run_dir, load_training_bundle

TRAINING_SQL_ORDER = [
    "sql/training/create_training_run_log.sql",
    "sql/training/create_training_metrics.sql",
    "sql/training/create_model_registry_audit.sql",
]


def apply_training_sql() -> None:
    with engine.begin() as connection:
        for sql_file in TRAINING_SQL_ORDER:
            sql_text = Path(sql_file).read_text(encoding="utf-8")
            connection.exec_driver_sql(sql_text)


def _upsert_run_log(
    *,
    context: Any,
    status: str,
    split_manifest: dict[str, Any] | None,
    config_snapshot: dict[str, Any],
    error_message: str | None = None,
) -> None:
    holdout = split_manifest.get("holdout", {}) if split_manifest else {}
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO training_run_log (
                    run_id,
                    started_at,
                    ended_at,
                    status,
                    feature_version,
                    policy_version,
                    split_policy_version,
                    train_start_ts,
                    train_end_ts,
                    val_start_ts,
                    val_end_ts,
                    test_start_ts,
                    test_end_ts,
                    config_snapshot,
                    error_message
                ) VALUES (
                    :run_id,
                    :started_at,
                    :ended_at,
                    :status,
                    :feature_version,
                    :policy_version,
                    :split_policy_version,
                    :train_start_ts,
                    :train_end_ts,
                    :val_start_ts,
                    :val_end_ts,
                    :test_start_ts,
                    :test_end_ts,
                    CAST(:config_snapshot AS JSONB),
                    :error_message
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    ended_at = EXCLUDED.ended_at,
                    status = EXCLUDED.status,
                    train_start_ts = EXCLUDED.train_start_ts,
                    train_end_ts = EXCLUDED.train_end_ts,
                    val_start_ts = EXCLUDED.val_start_ts,
                    val_end_ts = EXCLUDED.val_end_ts,
                    test_start_ts = EXCLUDED.test_start_ts,
                    test_end_ts = EXCLUDED.test_end_ts,
                    config_snapshot = EXCLUDED.config_snapshot,
                    error_message = EXCLUDED.error_message
                """
            ),
            {
                "run_id": context.run_id,
                "started_at": datetime.now(tz=UTC),
                "ended_at": datetime.now(tz=UTC) if status in {"failed", "succeeded"} else None,
                "status": status,
                "feature_version": context.feature_version,
                "policy_version": context.policy_version,
                "split_policy_version": context.split_policy_version,
                "train_start_ts": holdout.get("train_start"),
                "train_end_ts": holdout.get("train_end"),
                "val_start_ts": holdout.get("val_start"),
                "val_end_ts": holdout.get("val_end"),
                "test_start_ts": holdout.get("test_start"),
                "test_end_ts": holdout.get("test_end"),
                "config_snapshot": json.dumps(config_snapshot, default=str),
                "error_message": error_message,
            },
        )


def _compare(context: Any) -> dict[str, Any]:
    run_dir = ensure_run_dir(context)
    leaderboard = pd.read_csv(run_dir / "metrics_summary.csv")
    slices = pd.read_csv(run_dir / "slice_metrics.csv") if (run_dir / "slice_metrics.csv").exists() else pd.DataFrame()

    best_model = leaderboard.sort_values("wape", ascending=True).iloc[0]
    rationale = {
        "winner": str(best_model["model_name"]),
        "reason": "lowest_wape_on_common_test_window",
        "wape": float(best_model["wape"]),
        "mae": float(best_model["mae"]),
        "rmse": float(best_model["rmse"]),
        "latency_ms": float(best_model.get("latency_ms", 0.0) or 0.0),
    }
    output_path = run_dir / "comparison_rationale.json"
    output_path.write_text(json.dumps(rationale, indent=2), encoding="utf-8")

    report_path = run_dir / "training_report.md"
    leaderboard_csv = leaderboard.to_csv(index=False)
    slice_csv = slices.head(100).to_csv(index=False) if not slices.empty else ""
    report_lines = [
        "# Training Report",
        "",
        f"- run_id: {context.run_id}",
        f"- winner: {rationale['winner']}",
        f"- reason: {rationale['reason']}",
        f"- wape: {rationale['wape']:.6f}",
        f"- mae: {rationale['mae']:.6f}",
        f"- rmse: {rationale['rmse']:.6f}",
        "",
        "## Leaderboard",
        "",
        "```csv",
        leaderboard_csv.strip(),
        "```",
        "",
        "## Slice Metrics",
        "",
    ]
    if slices.empty:
        report_lines.append("No slice metrics available.")
    else:
        report_lines.extend(["```csv", slice_csv.strip(), "```"])
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    return {"comparison_path": str(output_path), "report_path": str(report_path), **rationale}


def run_step(step: str, *, run_id: str | None, promote_production: bool = False) -> dict[str, Any]:
    context, training_cfg, split_cfg, search_cfg, champion_cfg = load_training_bundle(run_id=run_id)
    apply_training_sql()

    prepared = prepare_dataset(context, split_cfg)
    manifest = build_split_manifest(prepared, context)
    persist_split_manifest(context, manifest)

    config_snapshot = {
        "training": training_cfg,
        "split": split_cfg,
        "search": search_cfg,
        "champion": champion_cfg,
    }

    _upsert_run_log(context=context, status="running", split_manifest=manifest, config_snapshot=config_snapshot)

    try:
        if step == "prepare-data":
            return {"run_id": context.run_id, "manifest": manifest}

        if step == "show-splits":
            return {"run_id": context.run_id, "manifest": manifest}

        if step == "baseline":
            result = run_baselines(context, training_cfg, split_cfg)
            _upsert_run_log(context=context, status="running", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "candidates":
            result = run_candidates(context, training_cfg, split_cfg, search_cfg)
            _upsert_run_log(context=context, status="running", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "compare":
            return _compare(context)

        if step == "track":
            run_dir = ensure_run_dir(context)
            return {
                "run_id": context.run_id,
                "artifacts": [
                    str(run_dir / "split_manifest.json"),
                    str(run_dir / "metrics_summary.csv"),
                    str(run_dir / "slice_metrics.csv"),
                ],
            }

        if step == "select-champion":
            result = run_selection(context, champion_cfg)
            _upsert_run_log(context=context, status="running", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "register-staging":
            result = register_champion(
                context=context,
                training_cfg=training_cfg,
                champion_cfg=champion_cfg,
                promote_to_production=False,
            )
            _upsert_run_log(context=context, status="succeeded", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "register-production":
            result = register_champion(
                context=context,
                training_cfg=training_cfg,
                champion_cfg=champion_cfg,
                promote_to_production=promote_production,
            )
            _upsert_run_log(context=context, status="succeeded", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "register":
            result = register_champion(
                context=context,
                training_cfg=training_cfg,
                champion_cfg=champion_cfg,
                promote_to_production=False,
            )
            _upsert_run_log(context=context, status="succeeded", split_manifest=manifest, config_snapshot=config_snapshot)
            return result

        if step == "run-all":
            baseline = run_baselines(context, training_cfg, split_cfg)
            candidates = run_candidates(context, training_cfg, split_cfg, search_cfg)
            compare = _compare(context)
            selection = run_selection(context, champion_cfg)
            try:
                registration: dict[str, Any] = register_champion(
                    context=context,
                    training_cfg=training_cfg,
                    champion_cfg=champion_cfg,
                    promote_to_production=False,
                )
            except ChampionGateFailedError as exc:
                run_dir = ensure_run_dir(context)
                registration = {
                    "run_id": context.run_id,
                    "status": "blocked",
                    "reason_code": "gate_failed",
                    "message": str(exc),
                    "decision_path": str(run_dir / "champion_decision.json"),
                    "decision": exc.decision,
                }
            _upsert_run_log(context=context, status="succeeded", split_manifest=manifest, config_snapshot=config_snapshot)
            return {
                "run_id": context.run_id,
                "baseline": baseline,
                "candidates": candidates,
                "compare": compare,
                "selection": selection,
                "registration": registration,
            }

        raise ValueError(f"unsupported step: {step}")
    except Exception as exc:  # noqa: BLE001
        _upsert_run_log(
            context=context,
            status="failed",
            split_manifest=manifest,
            config_snapshot=config_snapshot,
            error_message=str(exc),
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Training pipeline orchestrator")
    parser.add_argument(
        "--step",
        required=True,
        choices=[
            "prepare-data",
            "show-splits",
            "baseline",
            "candidates",
            "compare",
            "track",
            "select-champion",
            "register",
            "register-staging",
            "register-production",
            "run-all",
        ],
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--promote-production", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_step(args.step, run_id=args.run_id, promote_production=args.promote_production)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
