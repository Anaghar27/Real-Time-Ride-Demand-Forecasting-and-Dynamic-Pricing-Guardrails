"""
Tests for no leakage.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

import pandas as pd

from src.training.split_strategy import build_chronological_split


def test_train_validation_test_are_strictly_ordered() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1, 1, 1, 1],
            "bucket_start_ts": pd.to_datetime(
                [
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-02T00:00:00+00:00",
                    "2024-01-03T00:00:00+00:00",
                    "2024-01-04T00:00:00+00:00",
                ]
            ),
        }
    )
    cfg = {
        "chronological_holdout": {
            "train_start": "2024-01-01T00:00:00+00:00",
            "train_end": "2024-01-02T00:00:00+00:00",
            "val_start": "2024-01-02T00:00:00+00:00",
            "val_end": "2024-01-03T00:00:00+00:00",
            "test_start": "2024-01-03T00:00:00+00:00",
            "test_end": "2024-01-05T00:00:00+00:00",
            "gap_minutes": 0,
        }
    }

    split = build_chronological_split(frame, cfg)
    assert frame.loc[split.train_mask, "bucket_start_ts"].max() < frame.loc[split.val_mask, "bucket_start_ts"].min()
    assert frame.loc[split.val_mask, "bucket_start_ts"].max() < frame.loc[split.test_mask, "bucket_start_ts"].min()
