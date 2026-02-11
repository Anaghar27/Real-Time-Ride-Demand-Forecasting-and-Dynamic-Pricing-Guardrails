from __future__ import annotations

import pandas as pd
import pytest

from src.features.lag_rolling_features import apply_null_policy


def test_null_policy_zero_fills() -> None:
    frame = pd.DataFrame(
        {
            "lag_1": [None],
            "lag_2": [None],
            "lag_4": [None],
            "lag_96": [None],
            "lag_672": [None],
            "roll_mean_4": [None],
            "roll_mean_8": [None],
            "roll_std_8": [None],
            "roll_max_16": [None],
        }
    )

    out = apply_null_policy(frame, "zero")
    assert float(out.iloc[0]["lag_1"]) == 0.0
    assert float(out.iloc[0]["roll_max_16"]) == 0.0


def test_null_policy_keep_nulls() -> None:
    frame = pd.DataFrame(
        {
            "lag_1": [None],
            "lag_2": [None],
            "lag_4": [None],
            "lag_96": [None],
            "lag_672": [None],
            "roll_mean_4": [None],
            "roll_mean_8": [None],
            "roll_std_8": [None],
            "roll_max_16": [None],
        }
    )

    out = apply_null_policy(frame, "keep_nulls")
    assert pd.isna(out.iloc[0]["lag_1"])


def test_null_policy_rejects_unknown() -> None:
    frame = pd.DataFrame(
        {
            "lag_1": [None],
            "lag_2": [None],
            "lag_4": [None],
            "lag_96": [None],
            "lag_672": [None],
            "roll_mean_4": [None],
            "roll_mean_8": [None],
            "roll_std_8": [None],
            "roll_max_16": [None],
        }
    )

    with pytest.raises(ValueError):
        apply_null_policy(frame, "zone_prior")
