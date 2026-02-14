from __future__ import annotations

import pandas as pd

from src.eda.profile_seasonality import _safe_acf, _seasonality_index


def test_seasonality_index_positive_when_hour_pattern_exists() -> None:
    df = pd.DataFrame(
        {
            "hour_of_day": [0, 0, 1, 1, 2, 2, 3, 3],
            "pickup_count": [10, 12, 20, 22, 10, 12, 20, 22],
        }
    )
    value = _seasonality_index(df)
    assert value is not None
    assert value > 0


def test_safe_acf_short_series_returns_none() -> None:
    series = pd.Series([1, 2, 3])
    assert _safe_acf(series, lag=10) is None
