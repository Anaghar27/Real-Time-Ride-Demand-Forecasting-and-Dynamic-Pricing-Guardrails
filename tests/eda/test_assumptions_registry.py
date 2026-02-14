from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.eda.assumptions_registry import build_assumptions_payload
from src.eda.utils import EDAParams


def test_assumptions_registry_schema() -> None:
    params = EDAParams(
        run_id="r1",
        data_start_ts=datetime(2024, 1, 1, tzinfo=UTC),
        data_end_ts=datetime(2024, 1, 2, tzinfo=UTC),
        feature_version="v1",
        policy_version="p1",
        zone_ids=None,
        output_dir=Path("reports/eda"),
        docs_dir=Path("docs/eda"),
        top_n_zones=10,
        bottom_n_zones=10,
    )
    payload = build_assumptions_payload(params, {"sparsity_thresholds": {}}, source_row_count=10)

    assert payload["run_id"] == "r1"
    assert "data_assumptions" in payload
    assert "feature_assumptions" in payload
    assert "eda_assumptions" in payload
    assert "model_handoff_assumptions" in payload
