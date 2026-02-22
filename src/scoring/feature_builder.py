# This module builds leakage-safe feature rows for future demand forecasts at a 15-minute cadence.
# It exists because `fact_demand_features` only contains observed history, so we must deterministically extend features into the future.
# The builder computes calendar attributes directly from timestamps and derives lag/rolling features from observed pickup history (plus recursion when forecasting multiple steps).
# Keeping this logic centralized helps prevent accidental leakage and makes backfills reproducible.

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

MAX_LAG_BUCKETS = 672
MAX_ROLLING_BUCKETS = 16


def floor_to_bucket(ts: datetime, bucket_minutes: int) -> datetime:
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    ts_utc = ts.astimezone(UTC)
    minute = (ts_utc.minute // bucket_minutes) * bucket_minutes
    return ts_utc.replace(minute=minute, second=0, microsecond=0)


def ceil_to_bucket(ts: datetime, bucket_minutes: int) -> datetime:
    floored = floor_to_bucket(ts, bucket_minutes)
    aligned = (
        ts.astimezone(UTC).second == 0
        and ts.astimezone(UTC).microsecond == 0
        and (ts.astimezone(UTC).minute % bucket_minutes == 0)
    )
    return floored if aligned else floored + timedelta(minutes=bucket_minutes)


def build_forecast_window(
    *, scoring_created_at: datetime, horizon_buckets: int, bucket_minutes: int
) -> tuple[datetime, datetime]:
    forecast_start = ceil_to_bucket(scoring_created_at, bucket_minutes)
    forecast_end = forecast_start + timedelta(minutes=bucket_minutes * horizon_buckets)
    return forecast_start, forecast_end


def build_history_window(
    *, forecast_start_ts: datetime, history_days: int, history_extra_hours: int
) -> tuple[datetime, datetime]:
    history_end = forecast_start_ts
    history_start = forecast_start_ts - timedelta(days=history_days, hours=history_extra_hours)
    return history_start, history_end


def load_zone_ids_for_scoring(
    *,
    engine: Engine,
    as_of_ts: datetime,
    feature_version: str,
    bucket_minutes: int,
    max_zones: int | None,
) -> tuple[list[int], datetime]:
    bucket_width = timedelta(minutes=bucket_minutes)
    expected_latest = as_of_ts - bucket_width

    def _fetch_for_bucket(bucket_ts: datetime) -> list[int]:
        with engine.begin() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT DISTINCT zone_id
                    FROM fact_demand_features
                    WHERE bucket_start_ts = :bucket_start_ts
                      AND feature_version = :feature_version
                    ORDER BY zone_id
                    """
                ),
                {"bucket_start_ts": bucket_ts, "feature_version": feature_version},
            ).mappings().all()
        return [int(row["zone_id"]) for row in rows]

    zone_ids = _fetch_for_bucket(expected_latest)
    latest_bucket = expected_latest

    if not zone_ids:
        with engine.begin() as connection:
            latest_bucket_row = connection.execute(
                text(
                    """
                    SELECT MAX(bucket_start_ts) AS max_ts
                    FROM fact_demand_features
                    WHERE feature_version = :feature_version
                    """
                ),
                {"feature_version": feature_version},
            ).mappings().one()
        latest_bucket = latest_bucket_row["max_ts"]
        if latest_bucket is None:
            raise RuntimeError("fact_demand_features is empty; cannot determine scoring zones")
        zone_ids = _fetch_for_bucket(latest_bucket)

    if max_zones is not None:
        zone_ids = zone_ids[: max(0, int(max_zones))]

    return zone_ids, latest_bucket


def _load_pickup_history(
    *,
    engine: Engine,
    zone_ids: list[int],
    history_start_ts: datetime,
    history_end_ts: datetime,
    feature_version: str,
) -> pd.DataFrame:
    query = text(
        """
        SELECT zone_id, bucket_start_ts, pickup_count
        FROM fact_demand_features
        WHERE bucket_start_ts >= :history_start_ts
          AND bucket_start_ts < :history_end_ts
          AND feature_version = :feature_version
          AND zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
        ORDER BY bucket_start_ts, zone_id
        """
    )
    return pd.read_sql_query(
        query,
        con=engine,
        params={
            "history_start_ts": history_start_ts,
            "history_end_ts": history_end_ts,
            "feature_version": feature_version,
            "zone_ids": zone_ids,
        },
    )


def load_holiday_dates(*, engine: Engine, start_date: date, end_date: date) -> set[date]:
    try:
        with engine.begin() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT holiday_date
                    FROM dim_holiday
                    WHERE holiday_date >= :start_date
                      AND holiday_date <= :end_date
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).mappings().all()
        return {row["holiday_date"] for row in rows}
    except Exception:
        return set()


@dataclass(frozen=True)
class HistoryMatrix:
    zone_ids: list[int]
    history_start_ts: datetime
    history_end_ts: datetime
    bucket_minutes: int
    values: np.ndarray
    history_len: int
    zone_lineage: pd.DataFrame


def build_history_matrix(
    *,
    engine: Engine,
    zone_ids: list[int],
    history_start_ts: datetime,
    history_end_ts: datetime,
    feature_version: str,
    horizon_buckets: int,
    bucket_minutes: int,
    lag_null_policy: str,
) -> HistoryMatrix:
    if lag_null_policy not in {"zero", "keep_nulls"}:
        raise ValueError("lag_null_policy must be one of: zero, keep_nulls")

    bucket_width = timedelta(minutes=bucket_minutes)
    expected_index = pd.date_range(
        start=pd.Timestamp(history_start_ts),
        end=pd.Timestamp(history_end_ts) - bucket_width,
        freq=f"{bucket_minutes}min",
        tz="UTC",
    )
    if len(expected_index) == 0:
        raise ValueError("history window produced empty bucket index")

    history = _load_pickup_history(
        engine=engine,
        zone_ids=zone_ids,
        history_start_ts=history_start_ts,
        history_end_ts=history_end_ts,
        feature_version=feature_version,
    )
    if history.empty:
        raise RuntimeError("No history rows returned from fact_demand_features for scoring window")

    history["bucket_start_ts"] = pd.to_datetime(history["bucket_start_ts"], utc=True)
    history["zone_id"] = history["zone_id"].astype(int)

    pivot = history.pivot(index="bucket_start_ts", columns="zone_id", values="pickup_count").sort_index()
    pivot = pivot.reindex(index=expected_index)
    pivot = pivot.reindex(columns=zone_ids)

    expected_rows = int(len(expected_index))
    observed_rows = pivot.notna().sum(axis=0).astype(int)
    coverage_ratio = (observed_rows / max(expected_rows, 1)).astype(float)
    last_valid = pivot.apply(lambda series: series.last_valid_index(), axis=0)
    last_valid = pd.to_datetime(last_valid, utc=True)

    if lag_null_policy == "zero":
        pivot = pivot.fillna(0.0)

    history_values = pivot.to_numpy(dtype=float).T
    history_len = history_values.shape[1]
    values = np.full((len(zone_ids), history_len + horizon_buckets), np.nan, dtype=float)
    values[:, :history_len] = history_values

    fallback_last = pd.Timestamp(history_start_ts)
    if fallback_last.tz is None:
        fallback_last = fallback_last.tz_localize("UTC")
    else:
        fallback_last = fallback_last.tz_convert("UTC")
    last_observed_bucket_ts = last_valid.reindex(zone_ids).fillna(fallback_last)
    lineage = pd.DataFrame(
        {
            "zone_id": zone_ids,
            "observed_rows": observed_rows.reindex(zone_ids).to_numpy(dtype=int),
            "expected_rows": expected_rows,
            "coverage_ratio": coverage_ratio.reindex(zone_ids).to_numpy(dtype=float),
            "last_observed_bucket_ts": last_observed_bucket_ts.to_numpy(),
        }
    )

    return HistoryMatrix(
        zone_ids=zone_ids,
        history_start_ts=history_start_ts,
        history_end_ts=history_end_ts,
        bucket_minutes=bucket_minutes,
        values=values,
        history_len=history_len,
        zone_lineage=lineage,
    )


def _calendar_features(bucket_start_ts: datetime, *, bucket_minutes: int, feature_tz: str, holidays: set[date]) -> dict[str, Any]:
    local_ts = pd.Timestamp(bucket_start_ts).tz_convert(feature_tz)
    hour_of_day = int(local_ts.hour)
    day_of_week = int(local_ts.isoweekday())
    return {
        "hour_of_day": hour_of_day,
        "quarter_hour_index": hour_of_day * 4 + int(local_ts.minute // bucket_minutes),
        "day_of_week": day_of_week,
        "is_weekend": day_of_week in {6, 7},
        "week_of_year": int(local_ts.isocalendar().week),
        "month": int(local_ts.month),
        "is_holiday": bool(local_ts.date() in holidays),
    }


def _nanstd_samp(window: np.ndarray) -> np.ndarray:
    counts = np.sum(~np.isnan(window), axis=1)
    std = np.asarray(np.nanstd(window, axis=1, ddof=1), dtype=float)
    std[counts < 2] = np.nan
    return std


def build_step_features(
    *,
    history: HistoryMatrix,
    step_index: int,
    bucket_start_ts: datetime,
    feature_tz: str,
    holidays: set[date],
    lag_null_policy: str,
) -> pd.DataFrame:
    if lag_null_policy not in {"zero", "keep_nulls"}:
        raise ValueError("lag_null_policy must be one of: zero, keep_nulls")

    zone_count = len(history.zone_ids)
    col = history.history_len + step_index

    def lag(offset: int) -> np.ndarray:
        idx = col - offset
        if idx < 0:
            return np.full(zone_count, np.nan, dtype=float)
        return history.values[:, idx]

    def roll(window_buckets: int, fn: str) -> np.ndarray:
        start = max(0, col - window_buckets)
        window = history.values[:, start:col]
        if window.size == 0:
            return np.full(zone_count, np.nan, dtype=float)
        if fn == "mean":
            return np.asarray(np.nanmean(window, axis=1), dtype=float)
        if fn == "std_samp":
            return _nanstd_samp(window)
        if fn == "max":
            return np.asarray(np.nanmax(window, axis=1), dtype=float)
        raise ValueError(f"unsupported rolling fn: {fn}")

    feature_rows = pd.DataFrame(
        {
            "zone_id": history.zone_ids,
            "bucket_start_ts": pd.Timestamp(bucket_start_ts),
            "used_recursive_features": bool(step_index > 0),
            "lag_1": lag(1),
            "lag_2": lag(2),
            "lag_4": lag(4),
            "lag_96": lag(96),
            "lag_672": lag(672),
            "roll_mean_4": roll(4, "mean"),
            "roll_mean_8": roll(8, "mean"),
            "roll_std_8": roll(8, "std_samp"),
            "roll_max_16": roll(16, "max"),
        }
    )

    if lag_null_policy == "zero":
        for column in [
            "lag_1",
            "lag_2",
            "lag_4",
            "lag_96",
            "lag_672",
            "roll_mean_4",
            "roll_mean_8",
            "roll_std_8",
            "roll_max_16",
        ]:
            feature_rows[column] = feature_rows[column].fillna(0.0)

    cal = _calendar_features(bucket_start_ts, bucket_minutes=history.bucket_minutes, feature_tz=feature_tz, holidays=holidays)
    for key, value in cal.items():
        feature_rows[key] = value

    return feature_rows
