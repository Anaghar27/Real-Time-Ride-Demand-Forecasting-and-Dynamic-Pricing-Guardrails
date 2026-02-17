"""
Tests for split strategy.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

import pandas as pd

from src.training.split_strategy import build_chronological_split


def test_chronological_split_boundaries_and_non_overlap() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1] * 8,
            "bucket_start_ts": pd.date_range("2024-01-01", periods=8, freq="D", tz="UTC"),
        }
    )
    cfg = {
        "chronological_holdout": {
            "train_start": "2024-01-01T00:00:00+00:00",
            "train_end": "2024-01-04T00:00:00+00:00",
            "val_start": "2024-01-04T00:00:00+00:00",
            "val_end": "2024-01-06T00:00:00+00:00",
            "test_start": "2024-01-06T00:00:00+00:00",
            "test_end": "2024-01-09T00:00:00+00:00",
            "gap_minutes": 0,
        }
    }

    split = build_chronological_split(frame, cfg)
    assert int(split.train_mask.sum()) == 3
    assert int(split.val_mask.sum()) == 2
    assert int(split.test_mask.sum()) == 3

    assert not ((split.train_mask & split.val_mask).any())
    assert not ((split.train_mask & split.test_mask).any())
    assert not ((split.val_mask & split.test_mask).any())


def test_auto_chronological_split_resolves_from_available_data() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1] * 10,
            "bucket_start_ts": pd.date_range("2024-01-01", periods=10, freq="D", tz="UTC"),
        }
    )
    cfg = {
        "auto_chronological_holdout": {
            "enabled": True,
            "train_duration": "5D",
            "val_duration": "2D",
            "test_duration": "2D",
            "min_train_duration": "1D",
            "gap_minutes": 0,
        }
    }

    split = build_chronological_split(frame, cfg)
    assert int(split.train_mask.sum()) == 5
    assert int(split.val_mask.sum()) == 2
    assert int(split.test_mask.sum()) == 2

    assert not ((split.train_mask & split.val_mask).any())
    assert not ((split.train_mask & split.test_mask).any())
    assert not ((split.val_mask & split.test_mask).any())
