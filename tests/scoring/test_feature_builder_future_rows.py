# This test module exercises future feature construction for Phase 5 scoring.
# It exists to confirm timestamp alignment, lag/rolling calculations, and recursion behavior for multi-step horizons.
# The tests use a tiny synthetic history matrix so they are deterministic and do not require Postgres.
# This helps prevent accidental leakage or off-by-one errors when extending features into the future.

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd

from src.scoring.feature_builder import HistoryMatrix, build_step_features


def _history_matrix() -> HistoryMatrix:
    zone_ids = [1, 2]
    history_len = 10
    horizon = 3
    values = np.full((len(zone_ids), history_len + horizon), np.nan, dtype=float)

    values[0, :history_len] = np.arange(history_len, dtype=float)  # 0..9
    values[1, :history_len] = 10.0 + np.arange(history_len, dtype=float)  # 10..19

    lineage = pd.DataFrame(
        {
            "zone_id": zone_ids,
            "coverage_ratio": [1.0, 1.0],
            "last_observed_bucket_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)] * 2,
        }
    )

    return HistoryMatrix(
        zone_ids=zone_ids,
        history_start_ts=datetime(2024, 12, 31, 0, 0, tzinfo=UTC),
        history_end_ts=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        bucket_minutes=15,
        values=values,
        history_len=history_len,
        zone_lineage=lineage,
    )


def test_step0_uses_last_observed_for_lag_1_and_no_recursion() -> None:
    history = _history_matrix()
    ts = datetime(2025, 1, 1, 0, 15, tzinfo=UTC)
    df = build_step_features(
        history=history,
        step_index=0,
        bucket_start_ts=ts,
        feature_tz="UTC",
        holidays=set(),
        lag_null_policy="zero",
    )

    assert df["used_recursive_features"].unique().tolist() == [False]
    assert df.loc[df["zone_id"] == 1, "lag_1"].iloc[0] == 9.0
    assert df.loc[df["zone_id"] == 2, "lag_1"].iloc[0] == 19.0

    expected_roll_mean_4_zone1 = np.mean([6.0, 7.0, 8.0, 9.0])
    assert df.loc[df["zone_id"] == 1, "roll_mean_4"].iloc[0] == expected_roll_mean_4_zone1


def test_step1_uses_predicted_previous_step_for_lag_1_and_marks_recursion() -> None:
    history = _history_matrix()
    # Simulate step-0 prediction being appended to the history buffer.
    history.values[:, history.history_len] = np.array([100.0, 200.0], dtype=float)

    ts = datetime(2025, 1, 1, 0, 30, tzinfo=UTC)
    df = build_step_features(
        history=history,
        step_index=1,
        bucket_start_ts=ts,
        feature_tz="UTC",
        holidays=set(),
        lag_null_policy="zero",
    )

    assert df["used_recursive_features"].unique().tolist() == [True]
    assert df.loc[df["zone_id"] == 1, "lag_1"].iloc[0] == 100.0
    assert df.loc[df["zone_id"] == 2, "lag_1"].iloc[0] == 200.0

