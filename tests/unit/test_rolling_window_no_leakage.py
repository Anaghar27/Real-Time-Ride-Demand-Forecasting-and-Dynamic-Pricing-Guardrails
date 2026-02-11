from __future__ import annotations

import pandas as pd

from src.features.lag_rolling_features import add_rolling_features_pandas


def test_rolling_window_excludes_current_row() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1, 1, 1, 1, 1],
            "bucket_start_ts": pd.date_range("2026-01-01", periods=5, freq="15min", tz="UTC"),
            "pickup_count": [1, 2, 3, 4, 100],
        }
    )

    out = add_rolling_features_pandas(frame)
    last = out.iloc[4]
    assert last["roll_mean_4"] == 2.5


def test_dst_transition_is_timezone_explicit() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1, 1, 1],
            "bucket_start_ts": pd.to_datetime(
                [
                    "2024-11-03T05:30:00Z",
                    "2024-11-03T05:45:00Z",
                    "2024-11-03T06:00:00Z",
                ],
                utc=True,
            ),
            "pickup_count": [3, 4, 5],
        }
    )

    out = add_rolling_features_pandas(frame)
    # Explicit UTC ordering ensures deterministic behavior through DST transitions.
    assert list(out["bucket_start_ts"]) == sorted(out["bucket_start_ts"].tolist())
