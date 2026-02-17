"""Configuration loading for Phase 4 training."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SplitWindow:
    train_start: datetime
    train_end: datetime
    val_start: datetime
    val_end: datetime
    test_start: datetime
    test_end: datetime
    gap_minutes: int


@dataclass(frozen=True)
class TrainingContext:
    run_id: str
    experiment_name: str
    feature_version: str
    policy_version: str
    split_policy_version: str
    start_ts: datetime
    end_ts: datetime
    zone_ids: list[int] | None
    timezone: str
    output_dir: Path
    quick_mode: bool


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return dict(yaml.safe_load(handle) or {})


def parse_zone_ids(zones: str | None) -> list[int] | None:
    if not zones:
        return None
    values = [token.strip() for token in zones.split(",") if token.strip()]
    return [int(value) for value in values] if values else None


def _to_utc_window(start_date: str, end_date: str, tz_name: str) -> tuple[datetime, datetime]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("training end_date must be >= start_date")

    tzinfo = ZoneInfo(tz_name)
    start_local = datetime.combine(start, time.min, tzinfo=tzinfo)
    end_local_exclusive = datetime.combine(end + timedelta(days=1), time.min, tzinfo=tzinfo)
    return start_local.astimezone(UTC), end_local_exclusive.astimezone(UTC)


def _resolve_auto_data_window(
    *,
    data_cfg: dict[str, Any],
    timezone: str,
    zone_ids: list[int] | None,
) -> tuple[str, str]:
    auto_cfg = dict(data_cfg.get("auto_window", {}) or {})
    if not bool(auto_cfg.get("enabled", False)):
        return str(data_cfg.get("start_date", "2024-01-01")), str(data_cfg.get("end_date", "2024-01-31"))

    source = str(auto_cfg.get("source", "raw_trips")).strip().lower()
    lookback_days = int(auto_cfg.get("lookback_days", 30))
    end_offset_days = int(auto_cfg.get("end_offset_days", 0))
    if lookback_days <= 0:
        raise ValueError("data.auto_window.lookback_days must be > 0")
    if end_offset_days < 0:
        raise ValueError("data.auto_window.end_offset_days must be >= 0")

    tzinfo = ZoneInfo(timezone)
    max_ts: datetime | None = None
    min_ts: datetime | None = None

    from sqlalchemy import text

    from src.common.db import engine

    if source == "raw_trips":
        with engine.begin() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        MIN(pickup_datetime) AS min_ts,
                        MAX(pickup_datetime) AS max_ts
                    FROM raw_trips
                    WHERE (:zone_ids IS NULL OR pickup_location_id = ANY(CAST(:zone_ids AS INTEGER[])))
                    """
                ),
                {"zone_ids": zone_ids},
            ).mappings().one()
        min_ts = row["min_ts"]
        max_ts = row["max_ts"]
    elif source == "fact_demand_features":
        with engine.begin() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        MIN(bucket_start_ts) AS min_ts,
                        MAX(bucket_start_ts) AS max_ts
                    FROM fact_demand_features
                    WHERE feature_version = :feature_version
                      AND (:zone_ids IS NULL OR zone_id = ANY(CAST(:zone_ids AS INTEGER[])))
                    """
                ),
                {"feature_version": str(data_cfg.get("feature_version", "v1")), "zone_ids": zone_ids},
            ).mappings().one()
        min_ts = row["min_ts"]
        max_ts = row["max_ts"]
    else:
        raise ValueError("data.auto_window.source must be one of: raw_trips, fact_demand_features")

    if max_ts is None:
        raise ValueError(
            "auto window selection failed: no data found for configured source. "
            "Load raw trips (`make ingest-run-sample`) or build features first."
        )

    anchor_local = max_ts.astimezone(tzinfo) - timedelta(days=end_offset_days)
    end_date = anchor_local.date()
    start_date = end_date - timedelta(days=lookback_days - 1)

    if min_ts is not None:
        min_date = min_ts.astimezone(tzinfo).date()
        if start_date < min_date:
            start_date = min_date

    return start_date.isoformat(), end_date.isoformat()


def resolve_paths(
    *,
    training_config_path: str,
    split_policy_path: str,
    model_search_path: str,
    champion_policy_path: str,
) -> dict[str, Path]:
    return {
        "training": PROJECT_ROOT / training_config_path,
        "split": PROJECT_ROOT / split_policy_path,
        "search": PROJECT_ROOT / model_search_path,
        "champion": PROJECT_ROOT / champion_policy_path,
    }


def load_training_bundle(
    *,
    training_config_path: str = "configs/training.yaml",
    split_policy_path: str = "configs/split_policy.yaml",
    model_search_path: str = "configs/model_search_space.yaml",
    champion_policy_path: str = "configs/champion_policy.yaml",
    run_id: str | None = None,
) -> tuple[TrainingContext, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = resolve_paths(
        training_config_path=training_config_path,
        split_policy_path=split_policy_path,
        model_search_path=model_search_path,
        champion_policy_path=champion_policy_path,
    )

    training_cfg = load_yaml(paths["training"])
    split_cfg = load_yaml(paths["split"])
    search_cfg = load_yaml(paths["search"])
    champion_cfg = load_yaml(paths["champion"])

    data_cfg = dict(training_cfg.get("data", {}))
    runtime_cfg = dict(training_cfg.get("runtime", {}))
    tracking_cfg = dict(training_cfg.get("tracking", {}))

    timezone = str(data_cfg.get("feature_timezone", "UTC"))
    zone_ids = parse_zone_ids(data_cfg.get("zones"))
    start_date, end_date = _resolve_auto_data_window(data_cfg=data_cfg, timezone=timezone, zone_ids=zone_ids)
    training_cfg.setdefault("data", {})
    training_cfg["data"]["start_date"] = start_date
    training_cfg["data"]["end_date"] = end_date
    start_ts, end_ts = _to_utc_window(start_date, end_date, timezone)

    context = TrainingContext(
        run_id=run_id or str(uuid.uuid4()),
        experiment_name=str(tracking_cfg.get("experiment_name", "ride-demand-training")),
        feature_version=str(data_cfg.get("feature_version", "v1")),
        policy_version=str(data_cfg.get("policy_version", "p1")),
        split_policy_version=str(split_cfg.get("policy_version", "sp1")),
        start_ts=start_ts,
        end_ts=end_ts,
        zone_ids=zone_ids,
        timezone=timezone,
        output_dir=PROJECT_ROOT / str(runtime_cfg.get("reports_dir", "reports/training")),
        quick_mode=bool(runtime_cfg.get("quick_mode", True)),
    )

    return context, training_cfg, split_cfg, search_cfg, champion_cfg


def ensure_run_dir(context: TrainingContext) -> Path:
    run_dir = context.output_dir / context.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir
