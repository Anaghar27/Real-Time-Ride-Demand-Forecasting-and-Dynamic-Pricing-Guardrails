"""
Unit tests for calendar feature derivation.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.features.calendar_features import derive_calendar_features


def test_calendar_feature_derivation_is_deterministic() -> None:
    ts = datetime(2026, 2, 14, 13, 45, tzinfo=UTC)
    first = derive_calendar_features(ts, feature_tz="UTC")
    second = derive_calendar_features(ts, feature_tz="UTC")
    assert first == second


def test_calendar_feature_values_match_expected() -> None:
    ts = datetime(2026, 2, 14, 13, 45, tzinfo=UTC)  # Saturday
    values = derive_calendar_features(ts, feature_tz="UTC")

    assert values["hour_of_day"] == 13
    assert values["quarter_hour_index"] == 55
    assert values["day_of_week"] == 6
    assert values["is_weekend"] is True
    assert values["month"] == 2
