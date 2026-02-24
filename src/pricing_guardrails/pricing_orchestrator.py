# This module is the end-to-end entrypoint for the pricing guardrails pipeline.
# It exists to run policy loading, multiplier computation, guardrails, checks, and writes in one audited flow.
# The orchestrator supports both latest-run and explicit-window replay modes for operations.
# It also writes run artifacts and updates run logs so each pricing decision window is traceable.

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.common.logging import configure_logging
from src.pricing_guardrails.baseline_reference import attach_baseline_reference
from src.pricing_guardrails.cap_guardrail import apply_cap_guardrail
from src.pricing_guardrails.multiplier_engine import compute_raw_multiplier
from src.pricing_guardrails.policy_loader import (
    PolicyBundle,
    load_policy_bundle,
    persist_policy_snapshots,
    upsert_reason_code_reference,
)
from src.pricing_guardrails.pricing_checks import (
    PricingCheckError,
    enforce_pricing_checks,
    run_pricing_checks,
)
from src.pricing_guardrails.pricing_config import (
    PricingConfig,
    load_pricing_config,
    resolve_pricing_created_at,
)
from src.pricing_guardrails.pricing_writer import (
    PricingRunLogRow,
    pricing_run_key,
    upsert_pricing_decisions,
    upsert_pricing_run_log,
    utc_now,
)
from src.pricing_guardrails.rate_limiter import apply_rate_limiter, load_previous_final_multipliers
from src.pricing_guardrails.reason_codes import apply_reason_codes

LOGGER = logging.getLogger("pricing")

SQL_ORDER = [
    "sql/pricing_guardrails/create_pricing_tables.sql",
    "sql/pricing_guardrails/create_pricing_run_log.sql",
    "sql/pricing_guardrails/create_pricing_policy_tables.sql",
    "sql/pricing_guardrails/create_reason_code_reference.sql",
]
STEP_ORDER = [
    "load-policy",
    "compute-raw",
    "apply-caps",
    "apply-rate-limit",
    "reason-codes",
    "validate",
    "save",
]


def apply_pricing_sql() -> None:
    with engine.begin() as connection:
        for sql_file in SQL_ORDER:
            sql_text = Path(sql_file).read_text(encoding="utf-8")
            connection.exec_driver_sql(sql_text)


def _lock_key() -> int:
    digest = hashlib.sha256(b"pricing_guardrails_pipeline").digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=False)


def _acquire_overlap_lock(lock_key: int) -> bool:
    with engine.begin() as connection:
        row = connection.execute(text("SELECT pg_try_advisory_lock(:key) AS locked"), {"key": lock_key}).mappings().one()
    return bool(row["locked"])


def _release_overlap_lock(lock_key: int) -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})


def _table_exists(table_name: str) -> bool:
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


def _latest_forecast_run_id(*, forecast_table_name: str) -> str | None:
    if _table_exists("scoring_run_log"):
        row = pd.read_sql_query(
            text(
                """
                SELECT run_id
                FROM scoring_run_log
                WHERE status = 'succeeded'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            con=engine,
        )
        if not row.empty:
            return str(row.iloc[0]["run_id"])

    query = text(
        f"""
        SELECT run_id
        FROM {forecast_table_name}
        GROUP BY run_id
        ORDER BY MAX(forecast_created_at) DESC, run_id DESC
        LIMIT 1
        """
    )
    frame = pd.read_sql_query(query, con=engine)
    if frame.empty:
        return None
    return str(frame.iloc[0]["run_id"])


def _select_forecast_rows(
    *,
    pricing_config: PricingConfig,
    forecast_run_id_override: str | None,
    forecast_start_override: datetime | None,
    forecast_end_override: datetime | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    table = pricing_config.forecast_table_name

    mode = pricing_config.forecast_selection_mode
    explicit_run_id = forecast_run_id_override or pricing_config.explicit_forecast_run_id
    explicit_start = forecast_start_override or pricing_config.explicit_window_start
    explicit_end = forecast_end_override or pricing_config.explicit_window_end

    if mode == "latest_run":
        selected_run_id = _latest_forecast_run_id(forecast_table_name=table)
        if selected_run_id is None:
            return pd.DataFrame(), {"forecast_run_id": None}
        query = text(
            f"""
            SELECT
                zone_id,
                bucket_start_ts,
                forecast_created_at,
                horizon_index,
                y_pred,
                y_pred_lower,
                y_pred_upper,
                confidence_score,
                uncertainty_band,
                model_name,
                model_version,
                model_stage,
                feature_version,
                run_id AS forecast_run_id
            FROM {table}
            WHERE run_id = :run_id
            ORDER BY bucket_start_ts, zone_id
            """
        )
        frame = pd.read_sql_query(query, con=engine, params={"run_id": selected_run_id})
        return frame, {"forecast_run_id": selected_run_id}

    if mode == "explicit_run_id":
        if not explicit_run_id:
            raise ValueError("explicit_run_id mode requires a forecast run id")
        query = text(
            f"""
            SELECT
                zone_id,
                bucket_start_ts,
                forecast_created_at,
                horizon_index,
                y_pred,
                y_pred_lower,
                y_pred_upper,
                confidence_score,
                uncertainty_band,
                model_name,
                model_version,
                model_stage,
                feature_version,
                run_id AS forecast_run_id
            FROM {table}
            WHERE run_id = :run_id
            ORDER BY bucket_start_ts, zone_id
            """
        )
        frame = pd.read_sql_query(query, con=engine, params={"run_id": explicit_run_id})
        return frame, {"forecast_run_id": explicit_run_id}

    if mode == "explicit_window":
        if explicit_start is None or explicit_end is None:
            raise ValueError("explicit_window mode requires start and end timestamps")

        if explicit_run_id is None:
            run_id_query = text(
                f"""
                SELECT run_id
                FROM {table}
                WHERE bucket_start_ts >= :start_ts
                  AND bucket_start_ts < :end_ts
                GROUP BY run_id
                ORDER BY MAX(forecast_created_at) DESC, run_id DESC
                LIMIT 1
                """
            )
            run_id_frame = pd.read_sql_query(
                run_id_query,
                con=engine,
                params={"start_ts": explicit_start, "end_ts": explicit_end},
            )
            if run_id_frame.empty:
                return pd.DataFrame(), {"forecast_run_id": None}
            explicit_run_id = str(run_id_frame.iloc[0]["run_id"])

        query = text(
            f"""
            SELECT
                zone_id,
                bucket_start_ts,
                forecast_created_at,
                horizon_index,
                y_pred,
                y_pred_lower,
                y_pred_upper,
                confidence_score,
                uncertainty_band,
                model_name,
                model_version,
                model_stage,
                feature_version,
                run_id AS forecast_run_id
            FROM {table}
            WHERE run_id = :run_id
              AND bucket_start_ts >= :start_ts
              AND bucket_start_ts < :end_ts
            ORDER BY bucket_start_ts, zone_id
            """
        )
        frame = pd.read_sql_query(
            query,
            con=engine,
            params={"run_id": explicit_run_id, "start_ts": explicit_start, "end_ts": explicit_end},
        )
        return frame, {"forecast_run_id": explicit_run_id, "target_bucket_start": explicit_start, "target_bucket_end": explicit_end}

    raise ValueError(f"Unsupported forecast selection mode: {mode}")


def _load_zone_classes(*, policy_version: str, as_of_ts: datetime) -> pd.DataFrame:
    if not _table_exists("zone_fallback_policy"):
        return pd.DataFrame(columns=["zone_id", "zone_class"])

    query = text(
        """
        SELECT DISTINCT ON (zone_id)
            zone_id,
            sparsity_class AS zone_class
        FROM zone_fallback_policy
        WHERE policy_version = :policy_version
          AND effective_from <= :as_of_ts
          AND (effective_to IS NULL OR effective_to > :as_of_ts)
        ORDER BY zone_id, effective_from DESC
        """
    )
    frame = pd.read_sql_query(query, con=engine, params={"policy_version": policy_version, "as_of_ts": as_of_ts})
    if frame.empty:
        return pd.DataFrame(columns=["zone_id", "zone_class"])
    frame["zone_id"] = frame["zone_id"].astype(int)
    frame["zone_class"] = frame["zone_class"].astype(str)
    return frame


def _reports_dir(run_id: str) -> Path:
    out = Path("reports/pricing_guardrails") / run_id
    out.mkdir(parents=True, exist_ok=True)
    return out


def _write_artifacts(
    *,
    run_id: str,
    priced_frame: pd.DataFrame,
    run_summary: dict[str, Any],
    sample_size: int,
) -> str:
    out_dir = _reports_dir(run_id)

    sample = priced_frame.head(sample_size)
    sample.to_csv(out_dir / "pricing_sample.csv", index=False)

    if priced_frame.empty:
        guardrail_stats = pd.DataFrame(
            [{"metric": "row_count", "value": 0}, {"metric": "cap_applied_count", "value": 0}]
        )
        reason_summary = pd.DataFrame(columns=["reason_code", "row_count"])
    else:
        guardrail_stats = pd.DataFrame(
            [
                {"metric": "row_count", "value": int(len(priced_frame))},
                {"metric": "zone_count", "value": int(priced_frame["zone_id"].nunique())},
                {
                    "metric": "cap_applied_count",
                    "value": int(priced_frame["cap_applied"].fillna(False).astype(bool).sum()),
                },
                {
                    "metric": "rate_limited_count",
                    "value": int(priced_frame["rate_limit_applied"].fillna(False).astype(bool).sum()),
                },
                {
                    "metric": "low_confidence_count",
                    "value": int(priced_frame["low_confidence_adjusted"].fillna(False).astype(bool).sum()),
                },
                {
                    "metric": "avg_final_multiplier",
                    "value": float(priced_frame["final_multiplier"].astype(float).mean()),
                },
            ]
        )
        exploded = priced_frame[["reason_codes_json"]].explode("reason_codes_json")
        reason_summary = (
            exploded.groupby("reason_codes_json").size().reset_index(name="row_count").rename(columns={"reason_codes_json": "reason_code"})
        )

    guardrail_stats.to_csv(out_dir / "guardrail_stats.csv", index=False)
    reason_summary.to_csv(out_dir / "reason_code_summary.csv", index=False)
    (out_dir / "run_summary.json").write_text(json.dumps(run_summary, indent=2, default=str), encoding="utf-8")

    return str(out_dir)


def _build_running_log(
    *,
    run_id: str,
    started_at: datetime,
    pricing_config: PricingConfig,
) -> PricingRunLogRow:
    return PricingRunLogRow(
        run_id=run_id,
        pricing_run_key=None,
        started_at=started_at,
        ended_at=None,
        status="running",
        failure_reason=None,
        pricing_policy_version=pricing_config.pricing_policy_version,
        forecast_run_id=None,
        target_bucket_start=None,
        target_bucket_end=None,
        zone_count=None,
        row_count=None,
        cap_applied_count=None,
        rate_limited_count=None,
        low_confidence_count=None,
        latency_ms=None,
        config_snapshot={"pricing": pricing_config.to_dict()},
        check_summary=None,
        artifacts_path=None,
    )


def _finalize_run_log(
    *,
    run_id: str,
    started_at: datetime,
    status: str,
    pricing_config: PricingConfig,
    pricing_run_key_value: str | None,
    forecast_run_id: str | None,
    target_bucket_start: datetime | None,
    target_bucket_end: datetime | None,
    priced_frame: pd.DataFrame,
    check_summary: dict[str, Any] | None,
    artifacts_path: str | None,
    failure_reason: str | None,
) -> None:
    ended_at = utc_now()
    latency_ms = (ended_at - started_at).total_seconds() * 1000.0

    row = PricingRunLogRow(
        run_id=run_id,
        pricing_run_key=pricing_run_key_value,
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        failure_reason=failure_reason,
        pricing_policy_version=pricing_config.pricing_policy_version,
        forecast_run_id=forecast_run_id,
        target_bucket_start=target_bucket_start,
        target_bucket_end=target_bucket_end,
        zone_count=int(priced_frame["zone_id"].nunique()) if not priced_frame.empty else 0,
        row_count=int(len(priced_frame)),
        cap_applied_count=int(priced_frame["cap_applied"].fillna(False).astype(bool).sum()) if not priced_frame.empty else 0,
        rate_limited_count=(
            int(priced_frame["rate_limit_applied"].fillna(False).astype(bool).sum()) if not priced_frame.empty else 0
        ),
        low_confidence_count=(
            int(priced_frame["low_confidence_adjusted"].fillna(False).astype(bool).sum()) if not priced_frame.empty else 0
        ),
        latency_ms=latency_ms,
        config_snapshot={"pricing": pricing_config.to_dict()},
        check_summary=check_summary,
        artifacts_path=artifacts_path,
    )
    upsert_pricing_run_log(engine=engine, row=row)


def _step_reached(*, requested_step: str, checkpoint: str) -> bool:
    return STEP_ORDER.index(requested_step) >= STEP_ORDER.index(checkpoint)


def run_pricing(
    *,
    run_id: str | None = None,
    step: str = "save",
    forecast_run_id_override: str | None = None,
    forecast_start_override: datetime | None = None,
    forecast_end_override: datetime | None = None,
    pricing_created_at_override: datetime | None = None,
    config: PricingConfig | None = None,
) -> dict[str, Any]:
    if step not in STEP_ORDER:
        raise ValueError(f"step must be one of {STEP_ORDER}, got {step!r}")

    configure_logging()
    apply_pricing_sql()

    pricing_config = config or load_pricing_config()
    current_run_id = run_id or str(uuid.uuid4())
    started_at = utc_now()

    upsert_pricing_run_log(engine=engine, row=_build_running_log(run_id=current_run_id, started_at=started_at, pricing_config=pricing_config))

    lock_key = _lock_key()
    if not _acquire_overlap_lock(lock_key):
        _finalize_run_log(
            run_id=current_run_id,
            started_at=started_at,
            status="skipped_overlap",
            pricing_config=pricing_config,
            pricing_run_key_value=None,
            forecast_run_id=None,
            target_bucket_start=None,
            target_bucket_end=None,
            priced_frame=pd.DataFrame(),
            check_summary=None,
            artifacts_path=None,
            failure_reason="Skipped due to advisory overlap lock.",
        )
        return {"run_id": current_run_id, "status": "skipped", "message": "Overlap lock is active."}

    bundle: PolicyBundle | None = None
    forecast_meta: dict[str, Any] = {}
    pricing_key: str | None = None
    artifacts_path: str | None = None
    check_summary: dict[str, Any] | None = None

    try:
        if _step_reached(requested_step=step, checkpoint="load-policy"):
            bundle = load_policy_bundle(pricing_config=pricing_config)
            if pricing_config.policy_snapshot_enabled:
                persist_policy_snapshots(engine=engine, bundle=bundle)
            upsert_reason_code_reference(engine=engine, bundle=bundle)

        if step == "load-policy":
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded",
                pricing_config=pricing_config,
                pricing_run_key_value=None,
                forecast_run_id=None,
                target_bucket_start=None,
                target_bucket_end=None,
                priced_frame=pd.DataFrame(),
                check_summary={"passed": True, "failures": [], "warnings": []},
                artifacts_path=None,
                failure_reason=None,
            )
            return {"run_id": current_run_id, "status": "succeeded", "step": step}

        assert bundle is not None

        forecast_frame, forecast_meta = _select_forecast_rows(
            pricing_config=pricing_config,
            forecast_run_id_override=forecast_run_id_override,
            forecast_start_override=forecast_start_override,
            forecast_end_override=forecast_end_override,
        )
        if forecast_frame.empty:
            check_summary = {
                "passed": True,
                "failures": [],
                "warnings": [{"check": "empty_forecast_input", "message": "No forecast rows selected."}],
            }
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=pd.DataFrame(),
                run_summary={
                    "run_id": current_run_id,
                    "status": "succeeded_no_data",
                    "step": step,
                    "check_summary": check_summary,
                },
                sample_size=pricing_config.report_sample_size,
            )
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded_no_data",
                pricing_config=pricing_config,
                pricing_run_key_value=None,
                forecast_run_id=forecast_meta.get("forecast_run_id"),
                target_bucket_start=None,
                target_bucket_end=None,
                priced_frame=pd.DataFrame(),
                check_summary=check_summary,
                artifacts_path=artifacts_path,
                failure_reason=None,
            )
            return {
                "run_id": current_run_id,
                "status": "succeeded_no_data",
                "row_count": 0,
                "step": step,
                "artifacts_path": artifacts_path,
            }

        forecast_frame["bucket_start_ts"] = pd.to_datetime(forecast_frame["bucket_start_ts"], utc=True)
        if pricing_config.max_zones is not None:
            keep_zones = sorted(forecast_frame["zone_id"].astype(int).unique())[: pricing_config.max_zones]
            forecast_frame = forecast_frame[forecast_frame["zone_id"].isin(keep_zones)].copy()

        target_bucket_start = forecast_start_override or pricing_config.explicit_window_start
        target_bucket_end = forecast_end_override or pricing_config.explicit_window_end
        if target_bucket_start is None:
            target_bucket_start = forecast_frame["bucket_start_ts"].min().to_pydatetime()
        if target_bucket_end is None:
            # End-exclusive bound from observed horizon index.
            target_bucket_end = (forecast_frame["bucket_start_ts"].max() + pd.Timedelta(minutes=15)).to_pydatetime()

        forecast_run_id = str(forecast_meta.get("forecast_run_id") or forecast_frame["forecast_run_id"].iloc[0])

        pricing_key = pricing_run_key(
            pricing_policy_version=pricing_config.pricing_policy_version,
            forecast_run_id=forecast_run_id,
            target_bucket_start=target_bucket_start,
            target_bucket_end=target_bucket_end,
        )

        zone_classes = _load_zone_classes(policy_version=pricing_config.pricing_policy_version, as_of_ts=target_bucket_start)
        enriched = forecast_frame.merge(zone_classes, on="zone_id", how="left")

        with_baseline = attach_baseline_reference(
            engine=engine,
            forecasts=enriched,
            pricing_config=pricing_config,
            forecast_window_start=target_bucket_start,
        )

        raw = compute_raw_multiplier(
            forecasts_with_baseline=with_baseline,
            pricing_config=pricing_config,
            multiplier_rules=bundle.multiplier_rules,
        )
        if step == "compute-raw":
            priced_frame = raw.copy()
            run_summary = {
                "run_id": current_run_id,
                "status": "succeeded",
                "step": step,
                "row_count": int(len(priced_frame)),
                "forecast_run_id": forecast_run_id,
            }
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=priced_frame,
                run_summary=run_summary,
                sample_size=pricing_config.report_sample_size,
            )
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded",
                pricing_config=pricing_config,
                pricing_run_key_value=pricing_key,
                forecast_run_id=forecast_run_id,
                target_bucket_start=target_bucket_start,
                target_bucket_end=target_bucket_end,
                priced_frame=priced_frame,
                check_summary={"passed": True, "failures": [], "warnings": []},
                artifacts_path=artifacts_path,
                failure_reason=None,
            )
            return run_summary | {"artifacts_path": artifacts_path}

        capped = apply_cap_guardrail(raw_frame=raw, pricing_config=pricing_config)
        if step == "apply-caps":
            priced_frame = capped.copy()
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=priced_frame,
                run_summary={"run_id": current_run_id, "status": "succeeded", "step": step},
                sample_size=pricing_config.report_sample_size,
            )
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded",
                pricing_config=pricing_config,
                pricing_run_key_value=pricing_key,
                forecast_run_id=forecast_run_id,
                target_bucket_start=target_bucket_start,
                target_bucket_end=target_bucket_end,
                priced_frame=priced_frame,
                check_summary={"passed": True, "failures": [], "warnings": []},
                artifacts_path=artifacts_path,
                failure_reason=None,
            )
            return {
                "run_id": current_run_id,
                "status": "succeeded",
                "step": step,
                "row_count": int(len(priced_frame)),
                "artifacts_path": artifacts_path,
            }

        previous_bucket_ts = pd.Timestamp(target_bucket_start)
        if previous_bucket_ts.tz is None:
            previous_bucket_ts = previous_bucket_ts.tz_localize("UTC")
        else:
            previous_bucket_ts = previous_bucket_ts.tz_convert("UTC")

        previous_map = load_previous_final_multipliers(
            engine=engine,
            pricing_output_table_name=pricing_config.pricing_output_table_name,
            zone_ids=sorted(capped["zone_id"].astype(int).unique().tolist()),
            before_bucket_ts=previous_bucket_ts,
        )
        rate_limited = apply_rate_limiter(
            capped_frame=capped,
            pricing_config=pricing_config,
            previous_multiplier_map=previous_map,
        )
        if step == "apply-rate-limit":
            priced_frame = rate_limited.copy()
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=priced_frame,
                run_summary={"run_id": current_run_id, "status": "succeeded", "step": step},
                sample_size=pricing_config.report_sample_size,
            )
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded",
                pricing_config=pricing_config,
                pricing_run_key_value=pricing_key,
                forecast_run_id=forecast_run_id,
                target_bucket_start=target_bucket_start,
                target_bucket_end=target_bucket_end,
                priced_frame=priced_frame,
                check_summary={"passed": True, "failures": [], "warnings": []},
                artifacts_path=artifacts_path,
                failure_reason=None,
            )
            return {
                "run_id": current_run_id,
                "status": "succeeded",
                "step": step,
                "row_count": int(len(priced_frame)),
                "artifacts_path": artifacts_path,
            }

        reasoned = apply_reason_codes(
            priced_frame=rate_limited,
            reason_code_config=bundle.reason_codes,
            high_demand_ratio_threshold=float(bundle.multiplier_rules.get("high_demand_ratio_threshold", 1.25)),
        )
        if step == "reason-codes":
            priced_frame = reasoned.copy()
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=priced_frame,
                run_summary={"run_id": current_run_id, "status": "succeeded", "step": step},
                sample_size=pricing_config.report_sample_size,
            )
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status="succeeded",
                pricing_config=pricing_config,
                pricing_run_key_value=pricing_key,
                forecast_run_id=forecast_run_id,
                target_bucket_start=target_bucket_start,
                target_bucket_end=target_bucket_end,
                priced_frame=priced_frame,
                check_summary={"passed": True, "failures": [], "warnings": []},
                artifacts_path=artifacts_path,
                failure_reason=None,
            )
            return {
                "run_id": current_run_id,
                "status": "succeeded",
                "step": step,
                "row_count": int(len(priced_frame)),
                "artifacts_path": artifacts_path,
            }

        pricing_created_at = resolve_pricing_created_at(pricing_config, override_ts=pricing_created_at_override)
        final_frame = reasoned.copy()
        final_frame["pricing_created_at"] = pricing_created_at
        final_frame["pricing_run_key"] = pricing_key
        final_frame["pricing_policy_version"] = pricing_config.pricing_policy_version
        final_frame["run_id"] = current_run_id
        final_frame["status"] = "ready"
        final_frame["fallback_applied"] = (
            final_frame["fallback_applied"].fillna(False).astype(bool)
            | final_frame["cold_start_used"].fillna(False).astype(bool)
            | (final_frame["baseline_reference_level"].astype(str) == "global")
        )

        checks = run_pricing_checks(
            pricing_frame=final_frame,
            expected_zones=int(final_frame["zone_id"].nunique()),
            expected_buckets=int(final_frame["bucket_start_ts"].nunique()),
            pricing_config=pricing_config,
        )
        check_summary = checks.to_dict()

        if step == "validate":
            artifacts_path = _write_artifacts(
                run_id=current_run_id,
                priced_frame=final_frame,
                run_summary={
                    "run_id": current_run_id,
                    "status": "succeeded" if checks.passed else "failed",
                    "step": step,
                    "check_summary": check_summary,
                },
                sample_size=pricing_config.report_sample_size,
            )
            status = "succeeded" if checks.passed else "failed"
            _finalize_run_log(
                run_id=current_run_id,
                started_at=started_at,
                status=status,
                pricing_config=pricing_config,
                pricing_run_key_value=pricing_key,
                forecast_run_id=forecast_run_id,
                target_bucket_start=target_bucket_start,
                target_bucket_end=target_bucket_end,
                priced_frame=final_frame,
                check_summary=check_summary,
                artifacts_path=artifacts_path,
                failure_reason=None if checks.passed else "Pricing validation checks failed.",
            )
            return {
                "run_id": current_run_id,
                "status": status,
                "step": step,
                "check_summary": check_summary,
                "artifacts_path": artifacts_path,
            }

        if not checks.passed:
            enforce_pricing_checks(checks, strict_checks=True)

        written = upsert_pricing_decisions(
            engine=engine,
            pricing_output_table_name=pricing_config.pricing_output_table_name,
            pricing_frame=final_frame,
        )
        final_frame["status"] = "published"

        run_summary = {
            "run_id": current_run_id,
            "status": "succeeded",
            "step": step,
            "pricing_run_key": pricing_key,
            "forecast_run_id": forecast_run_id,
            "target_bucket_start": target_bucket_start.isoformat(),
            "target_bucket_end": target_bucket_end.isoformat(),
            "row_count": int(len(final_frame)),
            "zone_count": int(final_frame["zone_id"].nunique()),
            "written_rows": int(written),
            "cap_applied_count": int(final_frame["cap_applied"].fillna(False).astype(bool).sum()),
            "rate_limited_count": int(final_frame["rate_limit_applied"].fillna(False).astype(bool).sum()),
            "low_confidence_count": int(final_frame["low_confidence_adjusted"].fillna(False).astype(bool).sum()),
            "check_summary": check_summary,
        }
        artifacts_path = _write_artifacts(
            run_id=current_run_id,
            priced_frame=final_frame,
            run_summary=run_summary,
            sample_size=pricing_config.report_sample_size,
        )

        _finalize_run_log(
            run_id=current_run_id,
            started_at=started_at,
            status="succeeded",
            pricing_config=pricing_config,
            pricing_run_key_value=pricing_key,
            forecast_run_id=forecast_run_id,
            target_bucket_start=target_bucket_start,
            target_bucket_end=target_bucket_end,
            priced_frame=final_frame,
            check_summary=check_summary,
            artifacts_path=artifacts_path,
            failure_reason=None,
        )
        return run_summary | {"artifacts_path": artifacts_path}

    except PricingCheckError as exc:
        LOGGER.exception("Pricing checks failed for run_id=%s", current_run_id)
        error_details = exc.details if isinstance(exc.details, dict) else {"message": str(exc)}
        artifacts_path = _write_artifacts(
            run_id=current_run_id,
            priced_frame=pd.DataFrame(),
            run_summary={
                "run_id": current_run_id,
                "status": "failed",
                "error": str(exc),
                "check_summary": error_details,
            },
            sample_size=pricing_config.report_sample_size,
        )
        _finalize_run_log(
            run_id=current_run_id,
            started_at=started_at,
            status="failed",
            pricing_config=pricing_config,
            pricing_run_key_value=pricing_key,
            forecast_run_id=forecast_meta.get("forecast_run_id") if forecast_meta else None,
            target_bucket_start=forecast_start_override or pricing_config.explicit_window_start,
            target_bucket_end=forecast_end_override or pricing_config.explicit_window_end,
            priced_frame=pd.DataFrame(),
            check_summary=error_details,
            artifacts_path=artifacts_path,
            failure_reason=str(exc),
        )
        if pricing_config.strict_checks:
            raise
        return {"run_id": current_run_id, "status": "failed", "error": str(exc), "artifacts_path": artifacts_path}
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Pricing run failed for run_id=%s", current_run_id)
        artifacts_path = _write_artifacts(
            run_id=current_run_id,
            priced_frame=pd.DataFrame(),
            run_summary={"run_id": current_run_id, "status": "failed", "error": str(exc)},
            sample_size=pricing_config.report_sample_size,
        )
        _finalize_run_log(
            run_id=current_run_id,
            started_at=started_at,
            status="failed",
            pricing_config=pricing_config,
            pricing_run_key_value=pricing_key,
            forecast_run_id=forecast_meta.get("forecast_run_id") if forecast_meta else None,
            target_bucket_start=forecast_start_override or pricing_config.explicit_window_start,
            target_bucket_end=forecast_end_override or pricing_config.explicit_window_end,
            priced_frame=pd.DataFrame(),
            check_summary=check_summary,
            artifacts_path=artifacts_path,
            failure_reason=str(exc),
        )
        raise
    finally:
        _release_overlap_lock(lock_key)


def _parse_iso_ts(value: str | None) -> datetime | None:
    if value is None or value.strip() == "":
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pricing guardrails orchestrator")
    parser.add_argument("--run-id", type=str, default=None, help="Optional run id for traceability")
    parser.add_argument("--step", type=str, default="save", choices=STEP_ORDER)
    parser.add_argument("--forecast-run-id", type=str, default=None)
    parser.add_argument("--forecast-start-ts", type=str, default=None)
    parser.add_argument("--forecast-end-ts", type=str, default=None)
    parser.add_argument("--pricing-created-at", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_pricing(
        run_id=args.run_id,
        step=args.step,
        forecast_run_id_override=args.forecast_run_id,
        forecast_start_override=_parse_iso_ts(args.forecast_start_ts),
        forecast_end_override=_parse_iso_ts(args.forecast_end_ts),
        pricing_created_at_override=_parse_iso_ts(args.pricing_created_at),
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
