"""DDL helpers for ingestion tables."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.engine import Engine

DDL_ORDER = [
    "ingestion_batch_log.sql",
    "ingestion_check_results.sql",
    "stg_raw_trips.sql",
    "raw_trips.sql",
    "dim_zone.sql",
    "ingestion_watermark.sql",
]


def apply_ingestion_ddl(engine: Engine, ddl_dir: Path | None = None) -> None:
    """Apply ingestion DDL files in deterministic order."""

    ddl_path = ddl_dir or Path("sql/ddl")
    with engine.begin() as connection:
        for ddl_file in DDL_ORDER:
            sql_text = (ddl_path / ddl_file).read_text(encoding="utf-8")
            connection.exec_driver_sql(sql_text)
