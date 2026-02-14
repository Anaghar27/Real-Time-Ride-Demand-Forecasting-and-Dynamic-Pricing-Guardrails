from __future__ import annotations

import pandas as pd

from src.eda.fallback_policy import assign_fallback_policy


def test_fallback_assignment_determinism() -> None:
    sparsity_df = pd.DataFrame(
        {
            "zone_id": [1, 2, 3, 4],
            "sparsity_class": ["robust", "medium", "sparse", "ultra_sparse"],
        }
    )
    cfg = {
        "fallback_policy_mapping": {
            "robust": {"fallback_method": "zone_model", "fallback_priority": 1, "confidence_band": "tight"},
            "medium": {
                "fallback_method": "zone_model_conservative_smoothing",
                "fallback_priority": 2,
                "confidence_band": "medium",
            },
            "sparse": {
                "fallback_method": "borough_seasonal_baseline",
                "fallback_priority": 3,
                "confidence_band": "wide",
            },
            "ultra_sparse": {
                "fallback_method": "city_seasonal_baseline",
                "fallback_priority": 4,
                "confidence_band": "widest",
            },
        }
    }

    out1 = assign_fallback_policy(sparsity_df, cfg)
    out2 = assign_fallback_policy(sparsity_df, cfg)
    assert out1.equals(out2)
