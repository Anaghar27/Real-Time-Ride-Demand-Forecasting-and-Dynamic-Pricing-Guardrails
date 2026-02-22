# This test module checks the deterministic idempotency mechanics for Phase 5 scoring.
# It exists to ensure that a forecast window can be rerun safely without creating duplicate logical forecasts.
# We validate that the forecast run key is stable and that the writer enforces required columns for upserts.
# These tests are lightweight and avoid a real database by focusing on pure logic and input validation.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from src.scoring import forecast_writer
from src.scoring.scoring_orchestrator import _forecast_run_key


def test_forecast_run_key_is_deterministic() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    key1 = _forecast_run_key(model_version="4", forecast_start_ts=start, horizon_buckets=96)
    key2 = _forecast_run_key(model_version="4", forecast_start_ts=start, horizon_buckets=96)
    assert key1 == key2

    changed = _forecast_run_key(model_version="5", forecast_start_ts=start, horizon_buckets=96)
    assert changed != key1


def test_writer_requires_contract_columns() -> None:
    with pytest.raises(ValueError):
        forecast_writer.upsert_demand_forecast(engine=None, forecasts=pd.DataFrame())  # type: ignore[arg-type]
