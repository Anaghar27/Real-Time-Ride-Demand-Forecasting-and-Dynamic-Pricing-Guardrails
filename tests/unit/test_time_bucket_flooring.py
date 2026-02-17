"""
Unit tests for time bucket flooring.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.features.time_buckets import floor_timestamp_to_15m


def test_floor_timestamp_to_15m_rounds_down() -> None:
    ts = datetime(2026, 2, 11, 10, 29, 59, tzinfo=UTC)
    assert floor_timestamp_to_15m(ts) == datetime(2026, 2, 11, 10, 15, 0, tzinfo=UTC)


def test_floor_timestamp_to_15m_boundary_stable() -> None:
    ts = datetime(2026, 2, 11, 10, 30, 0, tzinfo=UTC)
    assert floor_timestamp_to_15m(ts) == ts


def test_floor_timestamp_requires_timezone() -> None:
    ts = datetime(2026, 2, 11, 10, 30, 0)
    with pytest.raises(ValueError):
        floor_timestamp_to_15m(ts)
