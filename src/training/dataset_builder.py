"""Dataset extraction and split manifests for training."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.training.split_strategy import (
    SplitResult,
    build_chronological_split,
    build_rolling_origin_splits,
)
from src.training.training_config import TrainingContext, ensure_run_dir, load_training_bundle

PROJECT_ROOT = Path(__file__).resolve().parents[2]

REQUIRED_COLUMNS = {
    "zone_id",
    "bucket_start_ts",
    "pickup_count",
    "day_of_week",
    "hour_of_day",
    "is_weekend",
    "lag_1",
    "lag_2",
    "lag_4",
    "lag_96",
    "lag_672",
    "roll_mean_4",
    "roll_mean_8",
    "roll_std_8",
    "roll_max_16",
}


@dataclass(frozen=True)
class PreparedDataset:
    frame: pd.DataFrame
    holdout: SplitResult
    rolling: list[SplitResult]


def _resolve_sql_file() -> Path:
    sql_path = PROJECT_ROOT / "sql/training/training_dataset_extract.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"Training dataset SQL not found: {sql_path}")
    return sql_path


def extract_training_frame(context: TrainingContext) -> pd.DataFrame:
    sql_text = _resolve_sql_file().read_text(encoding="utf-8")
    frame = pd.read_sql(
        text(sql_text),
        con=engine,
        params={
            "start_ts": context.start_ts,
            "end_ts": context.end_ts,
            "feature_version": context.feature_version,
            "policy_version": context.policy_version,
            "zone_ids": context.zone_ids,
        },
        parse_dates=["bucket_start_ts"],
    )

    if frame.empty:
        raise ValueError("training extract returned 0 rows")

    missing = sorted(REQUIRED_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"training dataset missing required columns: {missing}")

    frame = frame.sort_values(["bucket_start_ts", "zone_id"]).reset_index(drop=True)
    return frame


def prepare_dataset(context: TrainingContext, split_cfg: dict[str, Any]) -> PreparedDataset:
    frame = extract_training_frame(context)
    holdout = build_chronological_split(frame, split_cfg)
    rolling = build_rolling_origin_splits(frame, split_cfg)
    train_rows = int(holdout.train_mask.sum())
    val_rows = int(holdout.val_mask.sum())
    test_rows = int(holdout.test_mask.sum())
    if min(train_rows, val_rows, test_rows) == 0:
        min_ts = frame["bucket_start_ts"].min()
        max_ts = frame["bucket_start_ts"].max()
        raise ValueError(
            "chronological split has empty partition(s): "
            f"train={train_rows}, validation={val_rows}, test={test_rows}. "
            f"Available data window: {min_ts} to {max_ts}. "
            "Update configs/split_policy.yaml or configs/training.yaml."
        )
    return PreparedDataset(frame=frame, holdout=holdout, rolling=rolling)


def build_split_manifest(payload: PreparedDataset, context: TrainingContext) -> dict[str, Any]:
    manifest = {
        "run_id": context.run_id,
        "feature_version": context.feature_version,
        "policy_version": context.policy_version,
        "split_policy_version": context.split_policy_version,
        "rows_total": int(len(payload.frame)),
        "zones_total": int(payload.frame["zone_id"].nunique()),
        "holdout": payload.holdout.manifest,
        "rolling": [entry.manifest for entry in payload.rolling],
    }
    return manifest


def persist_split_manifest(context: TrainingContext, manifest: dict[str, Any]) -> Path:
    run_dir = ensure_run_dir(context)
    out_path = run_dir / "split_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare training dataset and split manifests")
    parser.add_argument("--show-only", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--training-config", default="configs/training.yaml")
    parser.add_argument("--split-policy", default="configs/split_policy.yaml")
    parser.add_argument("--model-search-space", default="configs/model_search_space.yaml")
    parser.add_argument("--champion-policy", default="configs/champion_policy.yaml")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    context, _, split_cfg, _, _ = load_training_bundle(
        training_config_path=args.training_config,
        split_policy_path=args.split_policy,
        model_search_path=args.model_search_space,
        champion_policy_path=args.champion_policy,
        run_id=args.run_id,
    )
    prepared = prepare_dataset(context, split_cfg)
    manifest = build_split_manifest(prepared, context)
    if not args.show_only:
        path = persist_split_manifest(context, manifest)
        manifest["manifest_path"] = str(path)
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
