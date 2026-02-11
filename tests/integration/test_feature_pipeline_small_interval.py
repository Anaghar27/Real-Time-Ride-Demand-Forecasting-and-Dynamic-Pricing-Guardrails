from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from src.common.db import engine
from src.common.db import test_connection as db_connection_ok
from src.features.build_feature_pipeline import build_feature_pipeline
from src.features.ddl import apply_feature_ddl
from src.ingestion.ddl import apply_ingestion_ddl

if os.getenv("RUN_FEATURE_INTEGRATION") != "1":
    pytest.skip("Set RUN_FEATURE_INTEGRATION=1 to run feature integration tests", allow_module_level=True)


@pytest.mark.integration
def test_feature_pipeline_small_interval() -> None:
    if not db_connection_ok():
        pytest.skip("Postgres unavailable in local test environment")

    apply_ingestion_ddl(engine)
    apply_feature_ddl(engine)

    with engine.begin() as connection:
        zone_count = int(connection.execute(text("SELECT COUNT(*) FROM dim_zone")).scalar() or 0)
    if zone_count == 0:
        pytest.skip("dim_zone is empty; run Phase 1 zone load first")

    result = build_feature_pipeline(
        start_date="2024-01-01",
        end_date="2024-01-01",
        zones=None,
        feature_version="it_v1",
        dry_run=False,
    )

    assert result["run_id"]
    assert result["publish"] is not None
