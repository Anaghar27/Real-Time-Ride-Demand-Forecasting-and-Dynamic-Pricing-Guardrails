"""
Chronological split strategies for leakage-safe training.
It helps build time-based splits, train/evaluate models, and apply the champion selection policy.
Runs log to MLflow and write artifacts under `reports/training/<run_id>/` for traceability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SplitResult:
    split_id: str
    train_mask: pd.Series
    val_mask: pd.Series
    test_mask: pd.Series
    manifest: dict[str, Any]


def _window_bounds(df: pd.DataFrame, start: datetime, end: datetime) -> pd.Series:
    return (df["bucket_start_ts"] >= start) & (df["bucket_start_ts"] < end)


def _assert_strict_temporal_order(train_end: datetime, val_end: datetime, test_end: datetime) -> None:
    if not (train_end < val_end < test_end):
        raise ValueError("split boundaries must satisfy train_end < val_end < test_end")


def _count_zones(df: pd.DataFrame, mask: pd.Series) -> int:
    if not mask.any():
        return 0
    return int(df.loc[mask, "zone_id"].nunique())


def _count_rows(mask: pd.Series) -> int:
    return int(mask.sum())


def _parse_timedelta(value: object, *, field_name: str) -> pd.Timedelta:
    if value is None:
        raise ValueError(f"{field_name} is required for auto split policy")
    if isinstance(value, int | float):
        raise TypeError(f"{field_name} must be a duration string like '60D' or '12H', got number: {value!r}")
    try:
        delta = pd.Timedelta(str(value).strip().lower())
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"{field_name} must be a duration string like '60D' or '12H', got: {value!r}") from exc
    if delta <= pd.Timedelta(0):
        raise ValueError(f"{field_name} must be > 0, got: {value!r}")
    return delta


def _infer_bucket_width(ordered: pd.DataFrame) -> pd.Timedelta:
    unique_ts = pd.DatetimeIndex(ordered["bucket_start_ts"].dropna().sort_values().unique())
    if len(unique_ts) < 2:
        return pd.Timedelta(minutes=15)
    diffs = unique_ts.to_series().diff().dropna()
    diffs = diffs[diffs > pd.Timedelta(0)]
    if diffs.empty:
        return pd.Timedelta(minutes=15)
    return pd.Timedelta(diffs.min())


def _build_auto_chronological_holdout(ordered: pd.DataFrame, auto_cfg: dict[str, Any]) -> dict[str, Any]:
    train_duration = _parse_timedelta(auto_cfg.get("train_duration"), field_name="auto_chronological_holdout.train_duration")
    val_duration = _parse_timedelta(auto_cfg.get("val_duration"), field_name="auto_chronological_holdout.val_duration")
    test_duration = _parse_timedelta(auto_cfg.get("test_duration"), field_name="auto_chronological_holdout.test_duration")

    gap_minutes = int(auto_cfg.get("gap_minutes", 0))
    if gap_minutes < 0:
        raise ValueError("auto_chronological_holdout.gap_minutes must be >= 0")

    bucket_width = _infer_bucket_width(ordered)
    available_start = pd.Timestamp(ordered["bucket_start_ts"].min())
    available_end_exclusive = pd.Timestamp(ordered["bucket_start_ts"].max()) + bucket_width

    gap = pd.Timedelta(minutes=gap_minutes)
    if val_duration <= gap or test_duration <= gap:
        raise ValueError(
            "auto_chronological_holdout val/test durations must be larger than gap_minutes "
            f"(val_duration={val_duration}, test_duration={test_duration}, gap_minutes={gap_minutes})."
        )

    min_train_duration = _parse_timedelta(
        auto_cfg.get("min_train_duration", "1D"),
        field_name="auto_chronological_holdout.min_train_duration",
    )

    required_min = min_train_duration + val_duration + test_duration
    available = available_end_exclusive - available_start
    if available < required_min:
        raise ValueError(
            "not enough data for auto split policy: "
            f"available={available}, required_min={required_min}. "
            "Expand configs/training.yaml date window or reduce auto split durations."
        )

    test_end = available_end_exclusive.to_pydatetime()
    val_end = (available_end_exclusive - test_duration).to_pydatetime()
    train_end = (available_end_exclusive - test_duration - val_duration).to_pydatetime()

    train_start_candidate = available_end_exclusive - test_duration - val_duration - train_duration
    train_start = max(available_start, train_start_candidate).to_pydatetime()

    return {
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "val_start": train_end.isoformat(),
        "val_end": val_end.isoformat(),
        "test_start": val_end.isoformat(),
        "test_end": test_end.isoformat(),
        "gap_minutes": gap_minutes,
    }


def build_chronological_split(df: pd.DataFrame, split_cfg: dict[str, Any]) -> SplitResult:
    """Build one deterministic train/validation/test split from explicit timestamps."""

    if "bucket_start_ts" not in df.columns or "zone_id" not in df.columns:
        raise ValueError("dataset must contain bucket_start_ts and zone_id")

    ordered = df.sort_values(["bucket_start_ts", "zone_id"]).reset_index(drop=True)
    auto_cfg = dict(split_cfg.get("auto_chronological_holdout", {}))
    if bool(auto_cfg.get("enabled", False)):
        windows = _build_auto_chronological_holdout(ordered, auto_cfg)
    else:
        windows = dict(split_cfg.get("chronological_holdout", {}))
    gap_minutes = int(windows.get("gap_minutes", 0))
    gap = timedelta(minutes=gap_minutes)

    train_start = pd.Timestamp(str(windows["train_start"]), tz="UTC").to_pydatetime()
    train_end = pd.Timestamp(str(windows["train_end"]), tz="UTC").to_pydatetime()
    val_start = pd.Timestamp(str(windows["val_start"]), tz="UTC").to_pydatetime() + gap
    val_end = pd.Timestamp(str(windows["val_end"]), tz="UTC").to_pydatetime()
    test_start = pd.Timestamp(str(windows["test_start"]), tz="UTC").to_pydatetime() + gap
    test_end = pd.Timestamp(str(windows["test_end"]), tz="UTC").to_pydatetime()

    _assert_strict_temporal_order(train_end, val_end, test_end)

    train_mask = _window_bounds(ordered, train_start, train_end)
    val_mask = _window_bounds(ordered, val_start, val_end)
    test_mask = _window_bounds(ordered, test_start, test_end)

    overlap = (train_mask & val_mask) | (train_mask & test_mask) | (val_mask & test_mask)
    if overlap.any():
        raise ValueError("split roles overlap; leakage risk")

    manifest = {
        "split_id": "chronological_holdout",
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "val_start": val_start.isoformat(),
        "val_end": val_end.isoformat(),
        "test_start": test_start.isoformat(),
        "test_end": test_end.isoformat(),
        "gap_minutes": gap_minutes,
        "row_counts": {
            "train": _count_rows(train_mask),
            "validation": _count_rows(val_mask),
            "test": _count_rows(test_mask),
        },
        "zone_counts": {
            "train": _count_zones(ordered, train_mask),
            "validation": _count_zones(ordered, val_mask),
            "test": _count_zones(ordered, test_mask),
        },
    }

    return SplitResult(
        split_id="chronological_holdout",
        train_mask=train_mask,
        val_mask=val_mask,
        test_mask=test_mask,
        manifest=manifest,
    )


def build_rolling_origin_splits(df: pd.DataFrame, split_cfg: dict[str, Any]) -> list[SplitResult]:
    """Build optional rolling-origin windows for stability checks."""

    rolling_cfg = dict(split_cfg.get("rolling_origin", {}))
    if not bool(rolling_cfg.get("enabled", False)):
        return []

    ordered = df.sort_values(["bucket_start_ts", "zone_id"]).reset_index(drop=True)
    fold_count = int(rolling_cfg.get("fold_count", 0))
    train_days = int(rolling_cfg.get("train_days", 21))
    val_days = int(rolling_cfg.get("val_days", 7))
    test_days = int(rolling_cfg.get("test_days", 7))
    stride_days = int(rolling_cfg.get("stride_days", 7))
    gap_minutes = int(rolling_cfg.get("gap_minutes", 0))

    if fold_count <= 0:
        return []

    min_ts = ordered["bucket_start_ts"].min()
    if pd.isna(min_ts):
        return []

    start_anchor = pd.Timestamp(min_ts)
    results: list[SplitResult] = []
    for fold in range(fold_count):
        train_start = start_anchor + pd.Timedelta(days=fold * stride_days)
        train_end = train_start + pd.Timedelta(days=train_days)
        val_start = train_end + pd.Timedelta(minutes=gap_minutes)
        val_end = val_start + pd.Timedelta(days=val_days)
        test_start = val_end + pd.Timedelta(minutes=gap_minutes)
        test_end = test_start + pd.Timedelta(days=test_days)

        if test_end > ordered["bucket_start_ts"].max() + pd.Timedelta(minutes=15):
            break

        train_mask = _window_bounds(ordered, train_start.to_pydatetime(), train_end.to_pydatetime())
        val_mask = _window_bounds(ordered, val_start.to_pydatetime(), val_end.to_pydatetime())
        test_mask = _window_bounds(ordered, test_start.to_pydatetime(), test_end.to_pydatetime())

        split_id = f"rolling_fold_{fold + 1}"
        results.append(
            SplitResult(
                split_id=split_id,
                train_mask=train_mask,
                val_mask=val_mask,
                test_mask=test_mask,
                manifest={
                    "split_id": split_id,
                    "train_start": train_start.isoformat(),
                    "train_end": train_end.isoformat(),
                    "val_start": val_start.isoformat(),
                    "val_end": val_end.isoformat(),
                    "test_start": test_start.isoformat(),
                    "test_end": test_end.isoformat(),
                    "gap_minutes": gap_minutes,
                    "row_counts": {
                        "train": _count_rows(train_mask),
                        "validation": _count_rows(val_mask),
                        "test": _count_rows(test_mask),
                    },
                    "zone_counts": {
                        "train": _count_zones(ordered, train_mask),
                        "validation": _count_zones(ordered, val_mask),
                        "test": _count_zones(ordered, test_mask),
                    },
                },
            )
        )

    return results
