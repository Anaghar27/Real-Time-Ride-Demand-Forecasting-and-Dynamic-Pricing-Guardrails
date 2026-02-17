"""
Step 2.3 Add deterministic calendar features to demand table.
It contributes to a leakage-safe, 15-minute demand feature set materialized in Postgres.
Run it via the Phase 2 Make targets or through `src.features.build_feature_pipeline`.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.features.runtime import (
    FeatureParams,
    build_feature_params,
    exec_sql_file,
    resolve_sql_file,
)

TRANSFORM_SQL = resolve_sql_file(
    [
        "sql/transforms/203_add_calendar_features.sql",
        "sql/transforms/add_calendar_features.sql",
    ]
)


def ensure_holiday_table() -> None:
    """Create dim_holiday table if not present."""

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_holiday (
                    holiday_date DATE PRIMARY KEY,
                    holiday_name TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    city_code TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )


def derive_calendar_features(ts: datetime, feature_tz: str = "UTC") -> dict[str, int | bool]:
    """Derive deterministic calendar attributes from a timestamp."""

    local_ts = pd.Timestamp(ts).tz_convert(feature_tz)
    hour_of_day = int(local_ts.hour)
    day_of_week = int(local_ts.isoweekday())
    return {
        "hour_of_day": hour_of_day,
        "quarter_hour_index": hour_of_day * 4 + int(local_ts.minute // 15),
        "day_of_week": day_of_week,
        "is_weekend": day_of_week in {6, 7},
        "week_of_year": int(local_ts.isocalendar().week),
        "month": int(local_ts.month),
    }


def sync_holidays(holiday_csv: Path) -> int:
    """Upsert holiday reference data into dim_holiday."""

    if not holiday_csv.exists():
        raise FileNotFoundError(f"Holiday file not found: {holiday_csv}")

    holiday_df = pd.read_csv(holiday_csv)
    required = {"holiday_date", "holiday_name", "country_code"}
    missing = required.difference(holiday_df.columns)
    if missing:
        raise ValueError(f"Holiday CSV missing required columns: {sorted(missing)}")

    holiday_df["holiday_date"] = pd.to_datetime(holiday_df["holiday_date"], utc=True).dt.date

    with engine.begin() as connection:
        for record in holiday_df.to_dict(orient="records"):
            connection.execute(
                text(
                    """
                    INSERT INTO dim_holiday (
                        holiday_date,
                        holiday_name,
                        country_code,
                        city_code,
                        created_at
                    )
                    VALUES (
                        :holiday_date,
                        :holiday_name,
                        :country_code,
                        :city_code,
                        :created_at
                    )
                    ON CONFLICT (holiday_date) DO UPDATE SET
                        holiday_name = EXCLUDED.holiday_name,
                        country_code = EXCLUDED.country_code,
                        city_code = EXCLUDED.city_code
                    """
                ),
                {
                    "holiday_date": record["holiday_date"],
                    "holiday_name": record["holiday_name"],
                    "country_code": record["country_code"],
                    "city_code": record.get("city_code"),
                    "created_at": datetime.now(tz=UTC),
                },
            )

    return int(len(holiday_df))


def add_calendar_features(params: FeatureParams) -> dict[str, int | str]:
    """Populate calendar-enriched demand features table."""

    holiday_file = Path(os.getenv("HOLIDAY_REFERENCE_FILE", "data/reference/holidays_us_nyc.csv"))
    ensure_holiday_table()
    holiday_rows = sync_holidays(holiday_file)

    exec_sql_file(
        engine,
        TRANSFORM_SQL,
        {
            "run_id": params.run_id,
            "feature_version": params.feature_version,
            "run_start_ts": params.run_start_ts,
            "run_end_ts": params.run_end_ts,
            "feature_tz": params.feature_tz,
            "zone_ids": params.zone_ids,
        },
    )

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    COUNT(*) AS row_count,
                    SUM(CASE WHEN hour_of_day IS NULL OR quarter_hour_index IS NULL OR day_of_week IS NULL THEN 1 ELSE 0 END) AS core_nulls
                FROM fct_zone_demand_calendar_15m
                WHERE bucket_start_ts >= :run_start_ts
                  AND bucket_start_ts < :run_end_ts
                  AND (
                      :zone_ids IS NULL
                      OR zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
                  )
                """
            ),
            {
                "run_start_ts": params.run_start_ts,
                "run_end_ts": params.run_end_ts,
                "zone_ids": params.zone_ids,
            },
        ).mappings().one()

    return {
        "run_id": params.run_id,
        "holiday_rows": holiday_rows,
        "row_count": int(row["row_count"]),
        "core_nulls": int(row["core_nulls"] or 0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build calendar features")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    params = build_feature_params(
        start_date=args.start_date,
        end_date=args.end_date,
        feature_version=args.feature_version,
        zones_arg=args.zones,
        run_id=args.run_id,
    )
    result = add_calendar_features(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
