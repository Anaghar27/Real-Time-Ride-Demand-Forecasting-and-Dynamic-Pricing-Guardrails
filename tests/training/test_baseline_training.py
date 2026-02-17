from __future__ import annotations

import numpy as np

from src.training.evaluate_models import compute_global_metrics


def test_baseline_metric_calculation_shape() -> None:
    y_true = np.array([10.0, 20.0, 30.0])
    y_pred = np.array([12.0, 18.0, 33.0])

    metrics = compute_global_metrics(y_true, y_pred)
    assert set(metrics.keys()) == {"mae", "rmse", "wape", "smape"}
    assert metrics["mae"] > 0
    assert metrics["rmse"] > 0
    assert metrics["wape"] > 0
    assert metrics["smape"] > 0
