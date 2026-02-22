# This module computes uncertainty estimates for each demand forecast row.
# It exists to provide downstream pricing guardrails with a sensible confidence score and prediction interval, not just a point estimate.
# The primary method is residual-based intervals using recent backtest errors grouped by segment and hour-of-day.
# When reference data is missing or stale, the module refreshes it in Postgres and logs simple diagnostics for operators.

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.scoring.scoring_config import FEATURE_COLUMNS, ScoringConfig

LOGGER = logging.getLogger("scoring")


@dataclass(frozen=True)
class ConfidenceReference:
    table: pd.DataFrame
    updated_at: datetime | None
    source_window: str | None


def _table_exists(engine: Engine, table_name: str) -> bool:
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


def load_zone_policy(
    *, engine: Engine, policy_version: str, as_of_ts: datetime
) -> pd.DataFrame:
    if not _table_exists(engine, "zone_fallback_policy"):
        LOGGER.warning("zone_fallback_policy table missing; proceeding without segment-based confidence adjustments")
        return pd.DataFrame(columns=["zone_id", "segment_key", "fallback_method", "confidence_band"])

    query = text(
        """
        SELECT DISTINCT ON (zone_id)
            zone_id,
            sparsity_class AS segment_key,
            fallback_method,
            confidence_band
        FROM zone_fallback_policy
        WHERE policy_version = :policy_version
          AND effective_from <= :as_of_ts
          AND (effective_to IS NULL OR effective_to > :as_of_ts)
        ORDER BY zone_id, effective_from DESC
        """
    )
    df = pd.read_sql_query(
        query,
        con=engine,
        params={"policy_version": policy_version, "as_of_ts": as_of_ts},
    )
    if df.empty:
        return pd.DataFrame(columns=["zone_id", "segment_key", "fallback_method", "confidence_band"])
    df["zone_id"] = df["zone_id"].astype(int)
    df["segment_key"] = df["segment_key"].astype(str)
    return df


def _load_reference(engine: Engine) -> ConfidenceReference:
    if not _table_exists(engine, "confidence_reference"):
        return ConfidenceReference(table=pd.DataFrame(), updated_at=None, source_window=None)

    df = pd.read_sql_query(
        text(
            """
            SELECT
                segment_key,
                hour_of_day,
                q50_abs_error,
                q90_abs_error,
                q95_abs_error,
                updated_at,
                source_window
            FROM confidence_reference
            """
        ),
        con=engine,
    )
    if df.empty:
        return ConfidenceReference(table=df, updated_at=None, source_window=None)

    df["segment_key"] = df["segment_key"].astype(str)
    df["hour_of_day"] = df["hour_of_day"].astype(int)
    updated_at = pd.to_datetime(df["updated_at"], utc=True).max().to_pydatetime()
    source_window = str(df["source_window"].iloc[0]) if "source_window" in df.columns else None
    return ConfidenceReference(table=df, updated_at=updated_at, source_window=source_window)


def _upsert_reference(
    *, engine: Engine, reference_rows: pd.DataFrame, updated_at: datetime, source_window: str
) -> None:
    rows = reference_rows.copy()
    rows["updated_at"] = updated_at
    rows["source_window"] = source_window
    payload: list[dict[str, Any]] = rows.to_dict(orient="records")

    statement = text(
        """
        INSERT INTO confidence_reference (
            segment_key,
            hour_of_day,
            q50_abs_error,
            q90_abs_error,
            q95_abs_error,
            updated_at,
            source_window
        ) VALUES (
            :segment_key,
            :hour_of_day,
            :q50_abs_error,
            :q90_abs_error,
            :q95_abs_error,
            :updated_at,
            :source_window
        )
        ON CONFLICT (segment_key, hour_of_day) DO UPDATE SET
            q50_abs_error = EXCLUDED.q50_abs_error,
            q90_abs_error = EXCLUDED.q90_abs_error,
            q95_abs_error = EXCLUDED.q95_abs_error,
            updated_at = EXCLUDED.updated_at,
            source_window = EXCLUDED.source_window
        """
    )
    with engine.begin() as connection:
        connection.execute(statement, payload)


def _fetch_backtest_frame(
    *,
    engine: Engine,
    feature_version: str,
    policy_version: str,
    start_ts: datetime,
    end_ts: datetime,
    zone_ids: list[int],
) -> pd.DataFrame:
    has_policy = _table_exists(engine, "zone_fallback_policy")
    if has_policy:
        query = text(
            """
            WITH latest_policy AS (
                SELECT DISTINCT ON (zone_id)
                    zone_id,
                    sparsity_class
                FROM zone_fallback_policy
                WHERE policy_version = :policy_version
                ORDER BY zone_id, effective_from DESC
            )
            SELECT
                f.zone_id,
                f.bucket_start_ts,
                f.pickup_count,
                f.hour_of_day,
                f.quarter_hour_index,
                f.day_of_week,
                f.is_weekend,
                f.week_of_year,
                f.month,
                f.is_holiday,
                f.lag_1,
                f.lag_2,
                f.lag_4,
                f.lag_96,
                f.lag_672,
                f.roll_mean_4,
                f.roll_mean_8,
                f.roll_std_8,
                f.roll_max_16,
                COALESCE(p.sparsity_class, 'unknown') AS segment_key
            FROM fact_demand_features f
            LEFT JOIN latest_policy p
              ON f.zone_id = p.zone_id
            WHERE f.bucket_start_ts >= :start_ts
              AND f.bucket_start_ts < :end_ts
              AND f.feature_version = :feature_version
              AND f.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
            ORDER BY f.bucket_start_ts, f.zone_id
            """
        )
    else:
        query = text(
            """
            SELECT
                f.zone_id,
                f.bucket_start_ts,
                f.pickup_count,
                f.hour_of_day,
                f.quarter_hour_index,
                f.day_of_week,
                f.is_weekend,
                f.week_of_year,
                f.month,
                f.is_holiday,
                f.lag_1,
                f.lag_2,
                f.lag_4,
                f.lag_96,
                f.lag_672,
                f.roll_mean_4,
                f.roll_mean_8,
                f.roll_std_8,
                f.roll_max_16,
                'all' AS segment_key
            FROM fact_demand_features f
            WHERE f.bucket_start_ts >= :start_ts
              AND f.bucket_start_ts < :end_ts
              AND f.feature_version = :feature_version
              AND f.zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
            ORDER BY f.bucket_start_ts, f.zone_id
            """
        )

    return pd.read_sql_query(
        query,
        con=engine,
        params={
            "feature_version": feature_version,
            "policy_version": policy_version,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "zone_ids": zone_ids,
        },
    )


def compute_reference_from_backtest(
    *,
    engine: Engine,
    model: Any,
    config: ScoringConfig,
    as_of_ts: datetime,
    zone_ids: list[int],
) -> ConfidenceReference:
    bucket_width = timedelta(minutes=config.bucket_minutes)
    end_ts = as_of_ts - bucket_width
    start_ts = end_ts - timedelta(days=config.confidence_backtest_days)
    frame = _fetch_backtest_frame(
        engine=engine,
        feature_version=config.feature_version,
        policy_version=config.policy_version,
        start_ts=start_ts,
        end_ts=end_ts,
        zone_ids=zone_ids,
    )
    if frame.empty:
        raise RuntimeError("Backtest frame empty; cannot compute confidence reference")

    x = frame[FEATURE_COLUMNS].copy()
    y_true = frame["pickup_count"].to_numpy(dtype=float)
    y_pred = np.clip(np.asarray(model.predict(x), dtype=float), 0.0, None)
    abs_err = np.abs(y_true - y_pred)
    frame["abs_error"] = abs_err
    frame["segment_key"] = frame["segment_key"].astype(str).fillna("unknown")
    frame["hour_of_day"] = frame["hour_of_day"].astype(int)

    grouped = frame.groupby(["segment_key", "hour_of_day"])["abs_error"]
    reference_rows = grouped.quantile([0.5, 0.9, 0.95]).unstack(level=-1).reset_index()
    reference_rows = reference_rows.rename(columns={0.5: "q50_abs_error", 0.9: "q90_abs_error", 0.95: "q95_abs_error"})

    reference_rows["q50_abs_error"] = reference_rows["q50_abs_error"].astype(float)
    reference_rows["q90_abs_error"] = reference_rows["q90_abs_error"].astype(float)
    reference_rows["q95_abs_error"] = reference_rows["q95_abs_error"].astype(float)

    updated_at = datetime.now(tz=UTC)
    source_window = f"{start_ts.date().isoformat()}..{end_ts.date().isoformat()}"
    _upsert_reference(engine=engine, reference_rows=reference_rows, updated_at=updated_at, source_window=source_window)

    return ConfidenceReference(table=reference_rows.assign(updated_at=updated_at, source_window=source_window), updated_at=updated_at, source_window=source_window)


def ensure_confidence_reference(
    *,
    engine: Engine,
    model: Any,
    config: ScoringConfig,
    as_of_ts: datetime,
    zone_ids: list[int],
) -> ConfidenceReference:
    existing = _load_reference(engine)
    if existing.updated_at is None:
        return compute_reference_from_backtest(engine=engine, model=model, config=config, as_of_ts=as_of_ts, zone_ids=zone_ids)

    age = datetime.now(tz=UTC) - existing.updated_at
    if age > timedelta(hours=config.confidence_refresh_hours):
        return compute_reference_from_backtest(engine=engine, model=model, config=config, as_of_ts=as_of_ts, zone_ids=zone_ids)

    return existing


def _half_width_for_quantile(row: pd.Series, quantile: float) -> float:
    if quantile >= 0.95:
        return float(row.get("q95_abs_error", 0.0) or 0.0)
    if quantile >= 0.90:
        return float(row.get("q90_abs_error", 0.0) or 0.0)
    return float(row.get("q50_abs_error", 0.0) or 0.0)


def apply_confidence(
    *,
    forecasts: pd.DataFrame,
    reference: ConfidenceReference,
    zone_policy: pd.DataFrame,
    config: ScoringConfig,
) -> pd.DataFrame:
    if forecasts.empty:
        raise ValueError("forecasts empty")
    if reference.table.empty:
        raise ValueError("confidence reference empty")

    scored = forecasts.copy()
    scored["hour_of_day"] = scored["hour_of_day"].astype(int)
    scored["zone_id"] = scored["zone_id"].astype(int)

    if not zone_policy.empty:
        policy = zone_policy[["zone_id", "segment_key"]].copy()
        policy["segment_key"] = policy["segment_key"].astype(str)
        scored = scored.merge(policy, on="zone_id", how="left")
        scored["segment_key"] = scored["segment_key"].fillna("unknown")
    else:
        scored["segment_key"] = "all"

    ref = reference.table.copy()
    ref["segment_key"] = ref["segment_key"].astype(str)
    ref["hour_of_day"] = ref["hour_of_day"].astype(int)

    merged = scored.merge(ref, on=["segment_key", "hour_of_day"], how="left", suffixes=("", "_ref"))

    fallback_ref = ref.groupby("hour_of_day")[["q50_abs_error", "q90_abs_error", "q95_abs_error"]].median().reset_index()
    fallback_ref = fallback_ref.rename(
        columns={
            "q50_abs_error": "fallback_q50_abs_error",
            "q90_abs_error": "fallback_q90_abs_error",
            "q95_abs_error": "fallback_q95_abs_error",
        }
    )
    merged = merged.merge(fallback_ref, on="hour_of_day", how="left")

    for q in ["q50_abs_error", "q90_abs_error", "q95_abs_error"]:
        merged[q] = merged[q].fillna(merged[f"fallback_{q}"]).fillna(0.0).astype(float)

    half_width = merged.apply(lambda r: _half_width_for_quantile(r, config.confidence_interval_quantile), axis=1).astype(float)
    merged["y_pred_lower"] = np.clip(merged["y_pred"].astype(float) - half_width, 0.0, None)
    merged["y_pred_upper"] = np.clip(merged["y_pred"].astype(float) + half_width, 0.0, None)

    relative_width = (half_width / np.maximum(merged["y_pred"].astype(float), 1.0)).astype(float)
    base_conf = (1.0 / (1.0 + relative_width)).astype(float)

    multipliers = {
        "robust": 1.0,
        "medium": 0.85,
        "sparse": 0.7,
        "ultra_sparse": 0.55,
        "unknown": 0.8,
        "all": 0.9,
    }
    merged["confidence_score"] = base_conf * merged["segment_key"].map(multipliers).fillna(0.8).astype(float)
    merged["confidence_score"] = merged["confidence_score"].clip(lower=0.0, upper=1.0)

    def band(score: float) -> str:
        if score >= 0.75:
            return "low"
        if score >= 0.5:
            return "medium"
        return "high"

    merged["uncertainty_band"] = merged["confidence_score"].astype(float).map(band)

    return merged.drop(
        columns=[
            "segment_key",
            "q50_abs_error",
            "q90_abs_error",
            "q95_abs_error",
            "fallback_q50_abs_error",
            "fallback_q90_abs_error",
            "fallback_q95_abs_error",
        ],
        errors="ignore",
    )


def write_confidence_diagnostics(*, reference: ConfidenceReference, output_path: Path) -> None:
    df = reference.table.copy()
    if df.empty:
        return
    df["hour_of_day"] = df["hour_of_day"].astype(int)

    fig, ax = plt.subplots(figsize=(10, 5))
    for seg, part in df.groupby("segment_key"):
        part = part.sort_values("hour_of_day")
        ax.plot(part["hour_of_day"], part["q95_abs_error"], label=str(seg))

    ax.set_title("Confidence reference: q95 absolute error by hour")
    ax.set_xlabel("hour_of_day")
    ax.set_ylabel("q95_abs_error")
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


def write_reference_snapshot(*, reference: ConfidenceReference, output_path: Path) -> None:
    payload = {
        "updated_at": reference.updated_at.isoformat() if reference.updated_at else None,
        "source_window": reference.source_window,
        "rows": int(len(reference.table)),
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
