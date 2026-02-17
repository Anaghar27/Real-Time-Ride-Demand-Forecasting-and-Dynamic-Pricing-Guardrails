"""
Automated Phase 2->Phase 4 pipeline with preflight checks.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

import argparse
import json
import uuid
from typing import Any

from src.features.build_feature_pipeline import build_feature_pipeline
from src.training.dataset_builder import prepare_dataset
from src.training.training_config import load_training_bundle
from src.training.training_orchestrator import run_step


def _nonzero_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def run_auto_pipeline(*, run_id: str | None = None) -> dict[str, Any]:
    context, training_cfg, split_cfg, _, _ = load_training_bundle(run_id=run_id)
    data_cfg = dict(training_cfg.get("data", {}))
    auto_cfg = dict(training_cfg.get("automation", {}))

    feature_build = build_feature_pipeline(
        start_date=str(data_cfg.get("start_date")),
        end_date=str(data_cfg.get("end_date")),
        zones=str(data_cfg.get("zones") or "") or None,
        feature_version=str(data_cfg.get("feature_version", "v1")),
        dry_run=False,
        run_id=str(uuid.uuid4()),
    )

    prepared = prepare_dataset(context, split_cfg)
    frame = prepared.frame
    holdout = prepared.holdout
    val_df = frame.loc[holdout.val_mask]
    test_df = frame.loc[holdout.test_mask]

    min_nonzero_ratio = float(auto_cfg.get("min_nonzero_ratio_per_holdout_split", 0.01))
    val_nonzero = int((val_df["pickup_count"] > 0).sum()) if not val_df.empty else 0
    test_nonzero = int((test_df["pickup_count"] > 0).sum()) if not test_df.empty else 0
    val_ratio = _nonzero_ratio(val_nonzero, int(len(val_df)))
    test_ratio = _nonzero_ratio(test_nonzero, int(len(test_df)))

    if val_ratio < min_nonzero_ratio or test_ratio < min_nonzero_ratio:
        return {
            "run_id": context.run_id,
            "status": "skipped",
            "reason_code": "low_holdout_nonzero_coverage",
            "message": (
                "Auto pipeline skipped due to low holdout nonzero coverage. "
                "Adjust split window or build a richer feature interval."
            ),
            "feature_build": feature_build,
            "preflight": {
                "min_nonzero_ratio_required": min_nonzero_ratio,
                "validation_nonzero_ratio": round(val_ratio, 6),
                "test_nonzero_ratio": round(test_ratio, 6),
                "validation_rows": int(len(val_df)),
                "test_rows": int(len(test_df)),
            },
        }

    training_result = run_step("run-all", run_id=context.run_id, promote_production=False)
    warning_codes: list[str] = []
    message: str | None = None
    if isinstance(training_result, dict):
        registration = training_result.get("registration")
        if (
            isinstance(registration, dict)
            and registration.get("status") == "blocked"
            and registration.get("reason_code") == "gate_failed"
        ):
            warning_codes.append("champion_gate_failed")
            message = "Training completed but champion gate failed; registration skipped."
    return {
        "run_id": context.run_id,
        "status": "completed",
        "warning_codes": warning_codes,
        "message": message,
        "feature_build": feature_build,
        "preflight": {
            "min_nonzero_ratio_required": min_nonzero_ratio,
            "validation_nonzero_ratio": round(val_ratio, 6),
            "test_nonzero_ratio": round(test_ratio, 6),
            "validation_rows": int(len(val_df)),
            "test_rows": int(len(test_df)),
        },
        "training": training_result,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run automated Phase 2->Phase 4 pipeline")
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_auto_pipeline(run_id=args.run_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
