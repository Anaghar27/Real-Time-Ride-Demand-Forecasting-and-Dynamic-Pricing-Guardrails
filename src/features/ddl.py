"""DDL helpers for Phase 2 feature tables."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FEATURE_DDL_ORDER: list[list[str]] = [
    ["sql/ddl/211_feature_batch_log.sql", "sql/ddl/feature_batch_log.sql"],
    ["sql/ddl/212_feature_check_results.sql", "sql/ddl/feature_check_results.sql"],
    ["sql/ddl/210_fact_demand_features.sql", "sql/ddl/fact_demand_features.sql"],
]


def apply_feature_ddl(engine: Engine, ddl_dir: Path | None = None) -> None:
    """Apply feature DDL files in deterministic order."""

    ddl_base = ddl_dir or (PROJECT_ROOT / "sql/ddl")
    with engine.begin() as connection:
        for ddl_candidates in FEATURE_DDL_ORDER:
            ddl_path = None
            for candidate in ddl_candidates:
                resolved = PROJECT_ROOT / candidate
                if resolved.exists():
                    ddl_path = resolved
                    break
                fallback = ddl_base / Path(candidate).name
                if fallback.exists():
                    ddl_path = fallback
                    break
            if ddl_path is None:
                raise FileNotFoundError(f"No DDL file found for candidates: {ddl_candidates}")
            sql_text = ddl_path.read_text(encoding="utf-8")
            connection.exec_driver_sql(sql_text)
