"""
Phase 3.3 governance assumptions and documentation registry.
It reads from the feature tables and produces reproducible summaries and governance artifacts.
Run it via `make eda-*` targets or through the EDA orchestrator for a full end-to-end report.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import text

from src.common.db import engine
from src.eda.utils import EDAParams, build_eda_params, ensure_run_dir, load_yaml


def build_assumptions_payload(params: EDAParams, thresholds_cfg: dict[str, Any], source_row_count: int) -> dict[str, Any]:
    return {
        "run_id": params.run_id,
        "version": 1,
        "data_assumptions": {
            "timezone": "UTC TIMESTAMPTZ storage; configurable local derivation via FEATURE_TIMEZONE",
            "bucket_definition": "15-minute buckets, left-closed right-open [bucket_start_ts, bucket_start_ts+15m)",
            "missing_bucket_handling": "zero-filled from zone-time spine",
            "zero_demand_interpretation": "no observed pickups in bucket, not missing data",
            "outlier_treatment": "descriptive only in Phase 3; no row removal",
        },
        "feature_assumptions": {
            "availability": "calendar, lag, and rolling features from fact_demand_features only",
            "leakage_boundaries": "lags/rolling based on prior rows only",
            "null_policy": "configured in Phase 2 FEATURE_LAG_NULL_POLICY",
        },
        "eda_assumptions": {
            "data_window": {
                "start": params.data_start_ts.isoformat(),
                "end": params.data_end_ts.isoformat(),
                "feature_version": params.feature_version,
                "source_rows": source_row_count,
            },
            "sparsity_thresholds": thresholds_cfg.get("sparsity_thresholds", {}),
            "holiday_source": "data/reference/holidays_us_nyc.csv",
        },
        "model_handoff_assumptions": {
            "fallback_policy_usage": "policy chosen by zone sparsity class and policy_version",
            "excluded_zone_rules": "none by default",
            "recommended_evaluation_slices": ["top volume zones", "ultra_sparse zones", "weekend peaks", "holiday buckets"],
        },
    }


def generate_docs(params: EDAParams, assumptions: dict[str, Any], thresholds_cfg: dict[str, Any]) -> dict[str, str]:
    run_dir = ensure_run_dir(params)
    assumptions_path = params.docs_dir / "assumptions_register.yaml"
    with assumptions_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(assumptions, handle, sort_keys=False)

    fallback_md = params.docs_dir / "fallback_policy.md"
    fallback_md.write_text(
        "\n".join(
            [
                "# Fallback Policy",
                "",
                "## Class Definitions",
                "- robust",
                "- medium",
                "- sparse",
                "- ultra_sparse",
                "",
                "## Threshold Table",
                "```yaml",
                yaml.safe_dump(thresholds_cfg.get("sparsity_thresholds", {}), sort_keys=False).strip(),
                "```",
                "",
                "## Examples",
                "- robust -> zone_model",
                "- medium -> zone_model_conservative_smoothing",
                "- sparse -> borough_seasonal_baseline",
                "- ultra_sparse -> city_seasonal_baseline",
                "",
                "## Caveats",
                "- sparse segments may require conservative monitoring windows",
                "",
                "## Operational Guidance",
                "- Always query zone_fallback_policy by policy_version and effective window",
            ]
        ),
        encoding="utf-8",
    )

    report_md = params.docs_dir / "phase3_report.md"
    report_md.write_text(
        "\n".join(
            [
                "# Phase 3 Report",
                "",
                "## Objective",
                "Profile seasonality and zone sparsity to inform Phase 4 model strategy.",
                "",
                "## Data Interval",
                f"- run_id: {params.run_id}",
                f"- feature_version: {params.feature_version}",
                f"- start: {params.data_start_ts.isoformat()}",
                f"- end: {params.data_end_ts.isoformat()}",
                "",
                "## Key Findings",
                "- Time and zone seasonality summaries persisted in EDA tables.",
                "- Sparse zone classes assigned with config-driven thresholds.",
                "",
                "## Sparse Zone Outcomes",
                "- See eda_zone_sparsity_summary and zone_fallback_policy for run-specific assignments.",
                "",
                "## Fallback Recommendation",
                "- Use zone-level model for robust/medium zones and baseline fallback for sparse segments.",
                "",
                "## Implications for Phase 4",
                "- Train segmented models and include fallback-aware evaluation slices.",
                "",
                f"Artifacts generated at: `{run_dir}`",
            ]
        ),
        encoding="utf-8",
    )

    data_dict_md = params.docs_dir / "data_dictionary_addendum.md"
    data_dict_md.write_text(
        "\n".join(
            [
                "# Data Dictionary Addendum",
                "",
                "## EDA Tables",
                "- eda_time_profile_summary: profile_type/profile_key aggregate moments",
                "- eda_zone_profile_summary: zone-level variation and periodicity scores",
                "- eda_seasonality_summary: global and ranked seasonality metrics",
                "- eda_zone_sparsity_summary: zone sparsity features and classes",
                "- zone_fallback_policy: zone-level fallback method assignment",
                "- eda_run_log: run status and reproducibility metadata",
                "- eda_check_results: critical/warning check outcomes",
            ]
        ),
        encoding="utf-8",
    )

    return {
        "assumptions": str(assumptions_path),
        "report": str(report_md),
        "fallback_policy": str(fallback_md),
        "data_dictionary": str(data_dict_md),
    }


def run_assumptions_registry(params: EDAParams, thresholds_cfg: dict[str, Any]) -> dict[str, Any]:
    source_row_count = int(
        pd.read_sql(
            text("SELECT COUNT(*) AS n FROM eda_zone_sparsity_summary WHERE run_id = :run_id"),
            con=engine,
            params={"run_id": params.run_id},
        )["n"].iloc[0]
    )
    assumptions = build_assumptions_payload(params, thresholds_cfg, source_row_count)
    doc_paths = generate_docs(params, assumptions, thresholds_cfg)
    return {"run_id": params.run_id, "doc_paths": doc_paths}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate assumptions registry and governance docs")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--feature-version", required=False)
    parser.add_argument("--policy-version", required=False)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--run-id", required=False)
    parser.add_argument("--config", default="configs/eda.yaml")
    parser.add_argument("--threshold-config", default="configs/eda_thresholds.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yaml(Path(args.config))
    threshold_cfg = load_yaml(Path(args.threshold_config))
    window_cfg = dict(cfg.get("analysis_window", {}))
    run_id = args.run_id or None
    if run_id is None:
        latest = pd.read_sql(
            text("SELECT run_id FROM eda_zone_sparsity_summary ORDER BY created_at DESC LIMIT 1"),
            con=engine,
        )
        if latest.empty:
            raise ValueError("No sparsity run available. Run eda-sparsity first or pass --run-id.")
        run_id = str(latest.iloc[0]["run_id"])

    params = build_eda_params(
        run_id=run_id,
        start_date=args.start_date or str(window_cfg.get("start_date")),
        end_date=args.end_date or str(window_cfg.get("end_date")),
        feature_version=args.feature_version or str(cfg.get("feature_version", "v1")),
        policy_version=args.policy_version or str(cfg.get("policy_version", "p1")),
        zones=args.zones,
        config=cfg,
    )
    result = run_assumptions_registry(params, threshold_cfg)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
