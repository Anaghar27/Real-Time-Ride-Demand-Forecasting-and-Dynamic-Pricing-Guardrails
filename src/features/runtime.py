"""Shared runtime helpers for the Phase 2 feature pipeline."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class FeatureWindow:
    """Canonical feature build window in UTC (end-exclusive)."""

    run_start_ts: datetime
    run_end_ts: datetime
    feature_tz: str


@dataclass(frozen=True)
class FeatureParams:
    """Parameters passed to SQL transform scripts."""

    run_id: str
    feature_version: str
    run_start_ts: datetime
    run_end_ts: datetime
    history_start_ts: datetime
    feature_tz: str
    lag_null_policy: str
    zone_ids: list[int] | None


def parse_zone_ids(zones_arg: str | None) -> list[int] | None:
    """Parse optional comma-separated zone ids."""

    if not zones_arg:
        return None
    zone_values = [item.strip() for item in zones_arg.split(",") if item.strip()]
    if not zone_values:
        return None
    return [int(value) for value in zone_values]


def build_feature_window(start_date: str, end_date: str, feature_tz: str) -> FeatureWindow:
    """Build UTC timestamps for an inclusive date interval."""

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end-date must be on or after start-date")

    tzinfo = ZoneInfo(feature_tz)
    start_local = datetime.combine(start, time.min, tzinfo=tzinfo)
    end_local_exclusive = datetime.combine(end + timedelta(days=1), time.min, tzinfo=tzinfo)

    return FeatureWindow(
        run_start_ts=start_local.astimezone(UTC),
        run_end_ts=end_local_exclusive.astimezone(UTC),
        feature_tz=feature_tz,
    )


def build_feature_params(
    *,
    start_date: str,
    end_date: str,
    feature_version: str,
    zones_arg: str | None,
    run_id: str | None = None,
) -> FeatureParams:
    """Create normalized SQL parameter payload for feature steps."""

    feature_tz = os.getenv("FEATURE_TIMEZONE", "UTC")
    lag_null_policy = os.getenv("FEATURE_LAG_NULL_POLICY", "zero")
    if lag_null_policy not in {"zero", "keep_nulls"}:
        raise ValueError("FEATURE_LAG_NULL_POLICY must be one of: zero, keep_nulls")

    window = build_feature_window(start_date=start_date, end_date=end_date, feature_tz=feature_tz)
    zones = parse_zone_ids(zones_arg)

    return FeatureParams(
        run_id=run_id or str(uuid.uuid4()),
        feature_version=feature_version,
        run_start_ts=window.run_start_ts,
        run_end_ts=window.run_end_ts,
        history_start_ts=window.run_start_ts - timedelta(days=8),
        feature_tz=window.feature_tz,
        lag_null_policy=lag_null_policy,
        zone_ids=zones,
    )


def exec_sql_file(engine: Engine, sql_file: Path, params: dict[str, object]) -> None:
    """Execute a SQL file with bind parameters in a single transaction."""

    sql_text = sql_file.read_text(encoding="utf-8")
    with engine.begin() as connection:
        connection.execute(text(sql_text), params)


def resolve_sql_file(candidates: list[str]) -> Path:
    """Resolve SQL file path from a list of repo-relative candidates."""

    for candidate in candidates:
        candidate_path = PROJECT_ROOT / candidate
        if candidate_path.exists():
            return candidate_path
    raise FileNotFoundError(f"None of the SQL files exist: {candidates}")


def get_source_bounds(engine: Engine, params: FeatureParams) -> tuple[datetime, datetime]:
    """Return source min/max timestamps for the run interval and zone scope."""

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    MIN(pickup_datetime) AS source_min_ts,
                    MAX(pickup_datetime) AS source_max_ts
                FROM raw_trips
                WHERE pickup_datetime >= :run_start_ts
                  AND pickup_datetime < :run_end_ts
                  AND (
                      :zone_ids IS NULL
                      OR pickup_location_id = ANY(CAST(:zone_ids AS INTEGER[]))
                  )
                """
            ),
            {
                "run_start_ts": params.run_start_ts,
                "run_end_ts": params.run_end_ts,
                "zone_ids": params.zone_ids,
            },
        ).mappings().one()

    source_min = row["source_min_ts"] or params.run_start_ts
    source_max = row["source_max_ts"] or params.run_end_ts
    return source_min, source_max
