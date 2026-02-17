"""
Shared utilities for Phase 3 EDA workflows.
It reads from the feature tables and produces reproducible summaries and governance artifacts.
Run it via `make eda-*` targets or through the EDA orchestrator for a full end-to-end report.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.db import engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class EDAParams:
    run_id: str
    data_start_ts: datetime
    data_end_ts: datetime
    feature_version: str
    policy_version: str
    zone_ids: list[int] | None
    output_dir: Path
    docs_dir: Path
    top_n_zones: int
    bottom_n_zones: int


def resolve_sql_file(candidates: list[str]) -> Path:
    for candidate in candidates:
        file_path = PROJECT_ROOT / candidate
        if file_path.exists():
            return file_path
    raise FileNotFoundError(f"None of SQL files exist: {candidates}")


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return dict(yaml.safe_load(handle) or {})


def parse_zone_ids(zones: str | None) -> list[int] | None:
    if not zones:
        return None
    values = [item.strip() for item in zones.split(",") if item.strip()]
    return [int(value) for value in values] if values else None


def build_window(start_date: str, end_date: str, feature_tz: str) -> tuple[datetime, datetime]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    tzinfo = ZoneInfo(feature_tz)
    start_local = datetime.combine(start, time.min, tzinfo=tzinfo)
    end_local = datetime.combine(end + timedelta(days=1), time.min, tzinfo=tzinfo)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def build_eda_params(
    *,
    run_id: str | None,
    start_date: str,
    end_date: str,
    feature_version: str,
    policy_version: str,
    zones: str | None,
    config: dict[str, Any],
) -> EDAParams:
    feature_tz = str(config.get("feature_timezone", "UTC"))
    data_start_ts, data_end_ts = build_window(start_date, end_date, feature_tz)

    reporting_cfg = dict(config.get("reporting", {}))
    output_dir = PROJECT_ROOT / str(reporting_cfg.get("output_dir", "reports/eda"))
    docs_dir = PROJECT_ROOT / str(reporting_cfg.get("docs_dir", "docs/eda"))

    return EDAParams(
        run_id=run_id or str(uuid.uuid4()),
        data_start_ts=data_start_ts,
        data_end_ts=data_end_ts,
        feature_version=feature_version,
        policy_version=policy_version,
        zone_ids=parse_zone_ids(zones),
        output_dir=output_dir,
        docs_dir=docs_dir,
        top_n_zones=int(reporting_cfg.get("top_n_zones", 10)),
        bottom_n_zones=int(reporting_cfg.get("bottom_n_zones", 10)),
    )


def run_sql_file(db_engine: Engine, sql_path: Path, params: dict[str, object] | None = None) -> None:
    payload = params or {}
    sql_text = sql_path.read_text(encoding="utf-8")
    with db_engine.begin() as connection:
        connection.execute(text(sql_text), payload)


def ensure_eda_tables(db_engine: Engine) -> None:
    for sql_name in [
        "sql/eda/eda_run_log.sql",
        "sql/eda/seasonality_metrics.sql",
        "sql/eda/zone_sparsity_metrics.sql",
        "sql/eda/fallback_policy_table.sql",
    ]:
        run_sql_file(db_engine, resolve_sql_file([sql_name]))


def fetch_eda_base(params: EDAParams) -> pd.DataFrame:
    sql_path = resolve_sql_file(["sql/eda/eda_base_extract.sql"])
    sql_text = sql_path.read_text(encoding="utf-8")
    return pd.read_sql(
        text(sql_text),
        con=engine,
        params={
            "data_start_ts": params.data_start_ts,
            "data_end_ts": params.data_end_ts,
            "feature_version": params.feature_version,
            "zone_ids": params.zone_ids,
        },
    )


def ensure_run_dir(params: EDAParams) -> Path:
    run_dir = params.output_dir / params.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    params.docs_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_dataframe_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def init_eda_run(params: EDAParams) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO eda_run_log (
                    run_id,
                    started_at,
                    data_start_ts,
                    data_end_ts,
                    feature_version,
                    policy_version,
                    status,
                    failure_reason
                )
                VALUES (
                    :run_id,
                    :started_at,
                    :data_start_ts,
                    :data_end_ts,
                    :feature_version,
                    :policy_version,
                    'running',
                    NULL
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    started_at = EXCLUDED.started_at,
                    data_start_ts = EXCLUDED.data_start_ts,
                    data_end_ts = EXCLUDED.data_end_ts,
                    feature_version = EXCLUDED.feature_version,
                    policy_version = EXCLUDED.policy_version,
                    status = EXCLUDED.status,
                    failure_reason = NULL,
                    ended_at = NULL
                """
            ),
            {
                "run_id": params.run_id,
                "started_at": datetime.now(tz=UTC),
                "data_start_ts": params.data_start_ts,
                "data_end_ts": params.data_end_ts,
                "feature_version": params.feature_version,
                "policy_version": params.policy_version,
            },
        )


def finalize_eda_run(run_id: str, status: str, failure_reason: str | None = None) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE eda_run_log
                SET status = :status,
                    failure_reason = :failure_reason,
                    ended_at = :ended_at
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "failure_reason": failure_reason,
                "ended_at": datetime.now(tz=UTC),
            },
        )


def persist_check_result(run_id: str, check_name: str, severity: str, passed: bool, details: dict[str, Any]) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO eda_check_results (run_id, check_name, severity, passed, details, created_at)
                VALUES (:run_id, :check_name, :severity, :passed, CAST(:details AS JSONB), :created_at)
                """
            ),
            {
                "run_id": run_id,
                "check_name": check_name,
                "severity": severity,
                "passed": passed,
                "details": json.dumps(details, default=str),
                "created_at": datetime.now(tz=UTC),
            },
        )
