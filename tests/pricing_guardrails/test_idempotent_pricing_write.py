# This test file validates idempotency primitives for pricing output writes.
# It exists to guarantee reruns update logical rows instead of creating duplicates.
# The tests verify deterministic run key generation and contract-column validation.
# They intentionally avoid a live database to stay lightweight in CI.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from src.pricing_guardrails.pricing_writer import pricing_run_key, upsert_pricing_decisions


def test_pricing_run_key_is_deterministic() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 1, 0, tzinfo=UTC)

    key1 = pricing_run_key(
        pricing_policy_version="pr1",
        forecast_run_id="forecast-123",
        target_bucket_start=start,
        target_bucket_end=end,
    )
    key2 = pricing_run_key(
        pricing_policy_version="pr1",
        forecast_run_id="forecast-123",
        target_bucket_start=start,
        target_bucket_end=end,
    )
    key3 = pricing_run_key(
        pricing_policy_version="pr2",
        forecast_run_id="forecast-123",
        target_bucket_start=start,
        target_bucket_end=end,
    )

    assert key1 == key2
    assert key1 != key3


def test_writer_requires_contract_columns() -> None:
    with pytest.raises(ValueError):
        upsert_pricing_decisions(
            engine=None,  # type: ignore[arg-type]
            pricing_output_table_name="pricing_decisions",
            pricing_frame=pd.DataFrame(),
        )
