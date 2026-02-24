# This module builds baseline demand references used to normalize forecast demand into a pricing signal.
# It exists because raw forecasts alone are not enough to decide if demand is unusually high or normal.
# The logic uses historical feature data and falls back from zone to borough to city when data is sparse.
# Provenance columns are emitted so every row shows which baseline tier was actually used.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.pricing_guardrails.pricing_config import PricingConfig


@dataclass(frozen=True)
class BaselineTables:
    zone: pd.DataFrame
    borough: pd.DataFrame
    city: pd.DataFrame
    zone_lookup: pd.DataFrame


def _table_exists(*, engine: Engine, table_name: str) -> bool:
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                LIMIT 1
                """
            ),
            {"table_name": table_name},
        ).fetchone()
    return row is not None


def _ensure_time_keys(forecasts: pd.DataFrame, *, run_timezone: str) -> pd.DataFrame:
    frame = forecasts.copy()
    if frame.empty:
        return frame

    frame["bucket_start_ts"] = pd.to_datetime(frame["bucket_start_ts"], utc=True)
    local_ts = frame["bucket_start_ts"].dt.tz_convert(run_timezone)
    if "day_of_week" not in frame.columns:
        frame["day_of_week"] = local_ts.dt.dayofweek.astype(int)
    if "quarter_hour_index" not in frame.columns:
        frame["quarter_hour_index"] = (local_ts.dt.hour * 4 + (local_ts.dt.minute // 15)).astype(int)
    return frame


def build_baseline_tables(
    *,
    engine: Engine,
    pricing_config: PricingConfig,
    forecast_window_start: datetime,
    feature_version: str | None,
) -> BaselineTables:
    if not _table_exists(engine=engine, table_name="fact_demand_features"):
        empty = pd.DataFrame()
        return BaselineTables(zone=empty, borough=empty, city=empty, zone_lookup=empty)

    history_start = forecast_window_start - timedelta(days=pricing_config.baseline_lookback_days)
    params: dict[str, Any] = {
        "history_start": history_start,
        "history_end": forecast_window_start,
        "feature_version": feature_version,
    }

    feature_filter = ""
    if feature_version:
        feature_filter = "AND f.feature_version = :feature_version"

    zone_query = text(
        f"""
        SELECT
            f.zone_id,
            f.day_of_week,
            f.quarter_hour_index,
            AVG(f.pickup_count::DOUBLE PRECISION) AS baseline_expected_demand_zone
        FROM fact_demand_features f
        WHERE f.bucket_start_ts >= :history_start
          AND f.bucket_start_ts < :history_end
          {feature_filter}
        GROUP BY f.zone_id, f.day_of_week, f.quarter_hour_index
        """
    )
    zone_baseline = pd.read_sql_query(zone_query, con=engine, params=params)

    if _table_exists(engine=engine, table_name="dim_zone"):
        borough_query = text(
            f"""
            SELECT
                d.borough,
                f.day_of_week,
                f.quarter_hour_index,
                AVG(f.pickup_count::DOUBLE PRECISION) AS baseline_expected_demand_borough
            FROM fact_demand_features f
            JOIN dim_zone d
              ON d.location_id = f.zone_id
            WHERE f.bucket_start_ts >= :history_start
              AND f.bucket_start_ts < :history_end
              {feature_filter}
            GROUP BY d.borough, f.day_of_week, f.quarter_hour_index
            """
        )
        borough_baseline = pd.read_sql_query(borough_query, con=engine, params=params)
        zone_lookup = pd.read_sql_query(text("SELECT location_id AS zone_id, borough FROM dim_zone"), con=engine)
    else:
        borough_baseline = pd.DataFrame(columns=["borough", "day_of_week", "quarter_hour_index"])  # pragma: no cover
        zone_lookup = pd.DataFrame(columns=["zone_id", "borough"])

    city_query = text(
        f"""
        SELECT
            f.day_of_week,
            f.quarter_hour_index,
            AVG(f.pickup_count::DOUBLE PRECISION) AS baseline_expected_demand_city
        FROM fact_demand_features f
        WHERE f.bucket_start_ts >= :history_start
          AND f.bucket_start_ts < :history_end
          {feature_filter}
        GROUP BY f.day_of_week, f.quarter_hour_index
        """
    )
    city_baseline = pd.read_sql_query(city_query, con=engine, params=params)

    return BaselineTables(zone=zone_baseline, borough=borough_baseline, city=city_baseline, zone_lookup=zone_lookup)


def merge_baseline_reference(
    *,
    forecasts: pd.DataFrame,
    baseline_tables: BaselineTables,
    pricing_config: PricingConfig,
) -> pd.DataFrame:
    frame = _ensure_time_keys(forecasts, run_timezone=pricing_config.run_timezone)
    if frame.empty:
        return frame

    merged = frame.merge(
        baseline_tables.zone,
        on=["zone_id", "day_of_week", "quarter_hour_index"],
        how="left",
    )

    if not baseline_tables.zone_lookup.empty:
        merged = merged.merge(baseline_tables.zone_lookup, on="zone_id", how="left")
    else:
        merged["borough"] = np.nan

    merged = merged.merge(
        baseline_tables.borough,
        on=["borough", "day_of_week", "quarter_hour_index"],
        how="left",
    )
    merged = merged.merge(
        baseline_tables.city,
        on=["day_of_week", "quarter_hour_index"],
        how="left",
    )

    zone_values = merged["baseline_expected_demand_zone"]
    borough_values = merged["baseline_expected_demand_borough"]
    city_values = merged["baseline_expected_demand_city"]

    baseline = zone_values.copy()
    level = pd.Series("zone", index=merged.index, dtype="object")

    use_borough = baseline.isna() & borough_values.notna()
    baseline = baseline.where(~use_borough, borough_values)
    level = level.where(~use_borough, "borough")

    use_city = baseline.isna() & city_values.notna()
    baseline = baseline.where(~use_city, city_values)
    level = level.where(~use_city, "city")

    use_global = baseline.isna()
    baseline = baseline.fillna(pricing_config.baseline_min_value)
    level = level.where(~use_global, "global")

    merged["baseline_expected_demand"] = baseline.clip(lower=pricing_config.baseline_min_value).astype(float)
    merged["baseline_reference_level"] = level
    merged["fallback_applied"] = merged["baseline_reference_level"].isin(["borough", "city", "global"])

    return merged


def attach_baseline_reference(
    *,
    engine: Engine,
    forecasts: pd.DataFrame,
    pricing_config: PricingConfig,
    forecast_window_start: datetime,
) -> pd.DataFrame:
    feature_version = None
    if "feature_version" in forecasts.columns and not forecasts["feature_version"].isna().all():
        feature_version = str(forecasts["feature_version"].iloc[0])

    baseline_tables = build_baseline_tables(
        engine=engine,
        pricing_config=pricing_config,
        forecast_window_start=forecast_window_start,
        feature_version=feature_version,
    )
    return merge_baseline_reference(forecasts=forecasts, baseline_tables=baseline_tables, pricing_config=pricing_config)
