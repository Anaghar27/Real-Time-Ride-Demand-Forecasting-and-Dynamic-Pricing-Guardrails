from __future__ import annotations

import pandas as pd

from src.features.lag_rolling_features import add_lag_features_pandas


def test_lag_feature_correctness() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1, 1, 1, 1, 1],
            "bucket_start_ts": pd.date_range("2026-01-01", periods=5, freq="15min", tz="UTC"),
            "pickup_count": [10, 11, 12, 13, 14],
        }
    )

    out = add_lag_features_pandas(frame)
    row = out.iloc[4]

    assert row["lag_1"] == 13
    assert row["lag_2"] == 12
    assert row["lag_4"] == 10
    assert pd.isna(row["lag_96"])
    assert pd.isna(row["lag_672"])
