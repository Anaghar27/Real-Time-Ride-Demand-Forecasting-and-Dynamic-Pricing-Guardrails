"""
Tests for sparsity thresholds.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

from __future__ import annotations

from src.eda.zone_sparsity import classify_sparsity, max_consecutive_zeros

THRESHOLDS = {
    "robust": {"min_nonzero_ratio": 0.6, "min_active_days": 5, "min_coverage_ratio": 0.95},
    "medium": {"min_nonzero_ratio": 0.3, "min_active_days": 3, "min_coverage_ratio": 0.9},
    "sparse": {"min_nonzero_ratio": 0.1, "min_active_days": 1, "min_coverage_ratio": 0.8},
    "ultra_sparse": {"min_nonzero_ratio": 0.0, "min_active_days": 0, "min_coverage_ratio": 0.0},
}


def test_sparsity_class_boundary_behavior() -> None:
    robust = {"nonzero_ratio": 0.8, "active_days": 6, "coverage_ratio": 1.0}
    medium = {"nonzero_ratio": 0.4, "active_days": 4, "coverage_ratio": 0.92}
    sparse = {"nonzero_ratio": 0.12, "active_days": 2, "coverage_ratio": 0.85}
    ultra = {"nonzero_ratio": 0.01, "active_days": 0, "coverage_ratio": 0.5}

    assert classify_sparsity(robust, THRESHOLDS) == "robust"
    assert classify_sparsity(medium, THRESHOLDS) == "medium"
    assert classify_sparsity(sparse, THRESHOLDS) == "sparse"
    assert classify_sparsity(ultra, THRESHOLDS) == "ultra_sparse"


def test_max_consecutive_zeros_edge_cases() -> None:
    assert max_consecutive_zeros(__import__("pandas").Series([0, 0, 1, 0, 0, 0])) == 3
    assert max_consecutive_zeros(__import__("pandas").Series([1, 2, 3])) == 0
