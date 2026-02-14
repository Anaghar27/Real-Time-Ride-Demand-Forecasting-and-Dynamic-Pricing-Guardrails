"""Phase 3 orchestrator for reproducible EDA runs and governance checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.eda.assumptions_registry import run_assumptions_registry
from src.eda.fallback_policy import run_fallback_policy
from src.eda.profile_seasonality import run_seasonality_profile
from src.eda.utils import (
    EDAParams,
    build_eda_params,
    ensure_eda_tables,
    finalize_eda_run,
    init_eda_run,
    load_yaml,
    persist_check_result,
    resolve_sql_file,
    run_sql_file,
)
from src.eda.zone_sparsity import run_zone_sparsity


class EDARunFailed(RuntimeError):
    """Critical EDA check failure."""


def _threshold_monotonicity_check(threshold_cfg: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    t = threshold_cfg["sparsity_thresholds"]
    passed = (
        t["robust"]["min_nonzero_ratio"] >= t["medium"]["min_nonzero_ratio"] >= t["sparse"]["min_nonzero_ratio"] >= t["ultra_sparse"]["min_nonzero_ratio"]
        and t["robust"]["min_coverage_ratio"] >= t["medium"]["min_coverage_ratio"] >= t["sparse"]["min_coverage_ratio"] >= t["ultra_sparse"]["min_coverage_ratio"]
    )
    return passed, {"thresholds": t}


def _run_validations(params: EDAParams, cfg: dict[str, Any], threshold_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    dupes = int(
        pd.read_sql(
            text(
                """
                SELECT COALESCE(SUM(cnt - 1), 0) AS dupes
                FROM (
                    SELECT zone_id, COUNT(*) AS cnt
                    FROM eda_zone_sparsity_summary
                    WHERE run_id = :run_id
                    GROUP BY zone_id
                    HAVING COUNT(*) > 1
                ) d
                """
            ),
            con=engine,
            params={"run_id": params.run_id},
        )["dupes"].iloc[0]
    )
    checks.append({"name": "duplicate_zone_rows", "severity": "critical", "passed": dupes == 0, "details": {"duplicates": dupes}})

    missing_assign = int(
        pd.read_sql(
            text(
                """
                SELECT COUNT(*) AS missing
                FROM eda_zone_sparsity_summary s
                LEFT JOIN zone_fallback_policy p
                  ON s.zone_id = p.zone_id
                 AND p.run_id = :run_id
                WHERE s.run_id = :run_id
                  AND p.zone_id IS NULL
                """
            ),
            con=engine,
            params={"run_id": params.run_id},
        )["missing"].iloc[0]
    )
    checks.append({"name": "missing_fallback_assignment", "severity": "critical", "passed": missing_assign == 0, "details": {"missing": missing_assign}})

    mono_passed, mono_details = _threshold_monotonicity_check(threshold_cfg)
    checks.append({"name": "threshold_monotonicity", "severity": "critical", "passed": mono_passed, "details": mono_details})

    coverage = pd.read_sql(
        text(
            """
            SELECT
                COUNT(DISTINCT s.zone_id) AS sparse_zones,
                COUNT(DISTINCT p.zone_id) AS policy_zones
            FROM eda_zone_sparsity_summary s
            LEFT JOIN zone_fallback_policy p
              ON s.zone_id = p.zone_id AND p.run_id = :run_id
            WHERE s.run_id = :run_id
            """
        ),
        con=engine,
        params={"run_id": params.run_id},
    ).iloc[0]
    checks.append(
        {
            "name": "policy_coverage_100pct",
            "severity": "critical",
            "passed": int(coverage["sparse_zones"]) == int(coverage["policy_zones"]),
            "details": {"sparse_zones": int(coverage["sparse_zones"]), "policy_zones": int(coverage["policy_zones"])},
        }
    )

    report_path = params.docs_dir / "phase3_report.md"
    required_sections = list(cfg.get("checks", {}).get("required_report_sections", []))
    report_text = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
    missing_sections = [section for section in required_sections if f"## {section}" not in report_text]
    checks.append(
        {
            "name": "report_completeness",
            "severity": "critical",
            "passed": len(missing_sections) == 0,
            "details": {"missing_sections": missing_sections},
        }
    )

    for check in checks:
        persist_check_result(
            run_id=params.run_id,
            check_name=str(check["name"]),
            severity=str(check["severity"]),
            passed=bool(check["passed"]),
            details=dict(check["details"]),
        )

    return checks


def run_eda_pipeline(params: EDAParams, cfg: dict[str, Any], threshold_cfg: dict[str, Any]) -> dict[str, Any]:
    ensure_eda_tables(engine)
    run_sql_file(engine, resolve_sql_file(["sql/eda/seasonality_metrics.sql"]))
    run_sql_file(engine, resolve_sql_file(["sql/eda/zone_sparsity_metrics.sql"]))
    run_sql_file(engine, resolve_sql_file(["sql/eda/fallback_policy_table.sql"]))
    run_sql_file(engine, resolve_sql_file(["sql/eda/eda_run_log.sql"]))

    init_eda_run(params)
    try:
        seasonality = run_seasonality_profile(params)
        sparsity = run_zone_sparsity(params, threshold_cfg)
        fallback = run_fallback_policy(params, threshold_cfg)
        docs = run_assumptions_registry(params, threshold_cfg)

        checks = _run_validations(params, cfg, threshold_cfg)
        critical_failed = [c for c in checks if c["severity"] == "critical" and not c["passed"]]
        if critical_failed:
            finalize_eda_run(params.run_id, "failed", json.dumps(critical_failed))
            raise EDARunFailed(json.dumps(critical_failed))

        finalize_eda_run(params.run_id, "succeeded")
        return {
            "run_id": params.run_id,
            "seasonality": seasonality,
            "sparsity": sparsity,
            "fallback": fallback,
            "docs": docs,
            "checks": checks,
        }
    except Exception as exc:  # noqa: BLE001
        finalize_eda_run(params.run_id, "failed", str(exc))
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 3 EDA orchestrator")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--feature-version", required=False)
    parser.add_argument("--policy-version", required=False)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default="configs/eda.yaml")
    parser.add_argument("--threshold-config", default="configs/eda_thresholds.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yaml(Path(args.config))
    threshold_cfg = load_yaml(Path(args.threshold_config))
    window_cfg = dict(cfg.get("analysis_window", {}))

    params = build_eda_params(
        run_id=args.run_id,
        start_date=args.start_date or str(window_cfg.get("start_date")),
        end_date=args.end_date or str(window_cfg.get("end_date")),
        feature_version=args.feature_version or str(cfg.get("feature_version", "v1")),
        policy_version=args.policy_version or str(cfg.get("policy_version", "p1")),
        zones=args.zones,
        config=cfg,
    )

    result = run_eda_pipeline(params, cfg, threshold_cfg)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
