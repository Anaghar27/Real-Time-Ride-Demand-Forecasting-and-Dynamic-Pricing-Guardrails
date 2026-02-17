"""
Tests for integration eda run.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok
from src.eda.eda_orchestrator import run_eda_pipeline
from src.eda.utils import build_eda_params, load_yaml

if os.getenv("RUN_EDA_INTEGRATION") != "1":
    pytest.skip("Set RUN_EDA_INTEGRATION=1 to run EDA integration tests", allow_module_level=True)


def _resolve_available_feature_window() -> tuple[str, str, str]:
    row = pd.read_sql(
        text(
            """
            SELECT
                feature_version,
                CAST(MIN(bucket_start_ts) AS DATE) AS start_date
            FROM fact_demand_features
            GROUP BY feature_version
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """
        ),
        con=engine,
    )
    if row.empty:
        pytest.skip("fact_demand_features is empty; run Phase 2 feature build first")
    feature_version = str(row.iloc[0]["feature_version"])
    start_date = str(row.iloc[0]["start_date"])
    return start_date, start_date, feature_version


@pytest.mark.integration
def test_full_eda_run_small_window() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable")

    cfg = load_yaml(Path("configs/eda.yaml"))
    thresholds = load_yaml(Path("configs/eda_thresholds.yaml"))
    start_date, end_date, feature_version = _resolve_available_feature_window()
    params = build_eda_params(
        run_id=None,
        start_date=start_date,
        end_date=end_date,
        feature_version=feature_version,
        policy_version="p1",
        zones=None,
        config=cfg,
    )
    result = run_eda_pipeline(params, cfg, thresholds)
    assert result["run_id"]


@pytest.mark.integration
def test_policy_table_persistence_and_report_generation() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable")

    latest_run = pd.read_sql(
        text("SELECT run_id FROM eda_run_log WHERE status = 'succeeded' ORDER BY started_at DESC LIMIT 1"), con=engine
    )
    if latest_run.empty:
        pytest.skip("No succeeded EDA run found")

    run_id = str(latest_run.iloc[0]["run_id"])
    policy_rows = int(
        pd.read_sql(text("SELECT COUNT(*) AS n FROM zone_fallback_policy WHERE run_id = :run_id"), con=engine, params={"run_id": run_id}).iloc[0]["n"]
    )
    assert policy_rows > 0

    report_text = Path("docs/eda/phase3_report.md").read_text(encoding="utf-8")
    assert "## Objective" in report_text


@pytest.mark.integration
def test_rerun_same_inputs_yields_same_assignments() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable")

    cfg = load_yaml(Path("configs/eda.yaml"))
    thresholds = load_yaml(Path("configs/eda_thresholds.yaml"))
    start_date, end_date, feature_version = _resolve_available_feature_window()

    p1 = build_eda_params(
        run_id=None,
        start_date=start_date,
        end_date=end_date,
        feature_version=feature_version,
        policy_version="p1",
        zones=None,
        config=cfg,
    )
    p2 = build_eda_params(
        run_id=None,
        start_date=start_date,
        end_date=end_date,
        feature_version=feature_version,
        policy_version="p1",
        zones=None,
        config=cfg,
    )

    r1 = run_eda_pipeline(p1, cfg, thresholds)
    a1 = pd.read_sql(
        text("SELECT zone_id, sparsity_class, fallback_method FROM zone_fallback_policy WHERE run_id = :run_id ORDER BY zone_id"),
        con=engine,
        params={"run_id": r1["run_id"]},
    )
    r2 = run_eda_pipeline(p2, cfg, thresholds)
    a2 = pd.read_sql(
        text("SELECT zone_id, sparsity_class, fallback_method FROM zone_fallback_policy WHERE run_id = :run_id ORDER BY zone_id"),
        con=engine,
        params={"run_id": r2["run_id"]},
    )

    assert a1.equals(a2)
