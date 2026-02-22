# This module is the end-to-end entrypoint for Phase 5 scoring.
# It ties together model loading, future feature construction, prediction, confidence estimation, quality checks, and persistence.
# The orchestrator exists so scoring can run both ad-hoc (CLI/Make) and on a schedule (Prefect) with the same idempotent behavior.
# It writes run logs and artifacts so every forecast window is traceable and debuggable.

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.common.logging import configure_logging
from src.common.settings import get_settings
from src.scoring.confidence import (
    apply_confidence,
    ensure_confidence_reference,
    load_zone_policy,
    write_confidence_diagnostics,
    write_reference_snapshot,
)
from src.scoring.feature_builder import (
    build_forecast_window,
    build_history_matrix,
    build_history_window,
    build_step_features,
    load_holiday_dates,
    load_zone_ids_for_scoring,
)
from src.scoring.forecast_writer import (
    ScoringRunLogRow,
    upsert_demand_forecast,
    upsert_scoring_run_log,
    utc_now,
)
from src.scoring.model_loader import LoadedModel, load_champion_model
from src.scoring.scoring_checks import ScoringCheckError, enforce_checks, run_checks
from src.scoring.scoring_config import FEATURE_COLUMNS, ScoringConfig, load_scoring_config

LOGGER = logging.getLogger("scoring")

SCORING_SQL_ORDER = [
    "sql/scoring/create_forecast_tables.sql",
    "sql/scoring/create_scoring_run_log.sql",
    "sql/scoring/create_confidence_reference.sql",
]


def apply_scoring_sql() -> None:
    with engine.begin() as connection:
        for sql_file in SCORING_SQL_ORDER:
            sql_text = Path(sql_file).read_text(encoding="utf-8")
            connection.exec_driver_sql(sql_text)


def _forecast_run_key(*, model_version: str, forecast_start_ts: datetime, horizon_buckets: int) -> str:
    raw = f"{model_version}|{forecast_start_ts.isoformat()}|{horizon_buckets}".encode()
    return hashlib.sha256(raw).hexdigest()[:24]


def _acquire_overlap_lock(lock_key: int) -> bool:
    with engine.begin() as connection:
        row = connection.execute(text("SELECT pg_try_advisory_lock(:key) AS locked"), {"key": lock_key}).mappings().one()
    return bool(row["locked"])


def _release_overlap_lock(lock_key: int) -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})


def _lock_key() -> int:
    # Stable 32-bit key for pg advisory lock.
    digest = hashlib.sha256(b"scoring_demand_forecast").digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=False)


def _ensure_reports_dir(base_dir: str, run_id: str) -> Path:
    out_dir = Path(base_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _log_to_mlflow(
    *,
    config: ScoringConfig,
    loaded: LoadedModel,
    run_id: str,
    forecast_run_key: str,
    metrics: dict[str, float],
    artifacts_dir: Path,
) -> None:
    settings = get_settings()
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    try:
        mlflow.set_experiment(config.mlflow_experiment_name)
        run_name = f"scoring | model={loaded.model_name} v{loaded.model_version} | key={forecast_run_key}"
        tags = {
            "run_id": run_id,
            "forecast_run_key": forecast_run_key,
            "model_name": loaded.model_name,
            "model_stage": loaded.model_stage,
            "model_version": loaded.model_version,
            "feature_version": config.feature_version,
        }
        with mlflow.start_run(run_name=run_name, tags=tags):
            mlflow.log_metrics(metrics)
            for path in artifacts_dir.glob("*"):
                if path.is_file():
                    mlflow.log_artifact(str(path), artifact_path="scoring")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("MLflow scoring logging failed: %s", exc)


@dataclass(frozen=True)
class ScoringResult:
    run_id: str
    status: str
    message: str | None
    forecast_run_key: str | None
    model: dict[str, Any] | None
    window: dict[str, str]
    counts: dict[str, Any]
    checks: dict[str, Any] | None
    artifacts_dir: str | None


def run_scoring(
    *,
    run_id: str | None = None,
    config: ScoringConfig | None = None,
    forecast_start_override: datetime | None = None,
    forecast_end_override: datetime | None = None,
    validate_only: bool = False,
) -> dict[str, Any]:
    configure_logging()
    apply_scoring_sql()

    cfg = config or load_scoring_config()
    settings = get_settings()
    actual_run_id = run_id or str(uuid.uuid4())

    scoring_created_at = utc_now()
    forecast_start_ts, forecast_end_ts = build_forecast_window(
        scoring_created_at=scoring_created_at,
        horizon_buckets=cfg.horizon_buckets,
        bucket_minutes=cfg.bucket_minutes,
    )
    if cfg.forecast_start_override:
        forecast_start_ts = cfg.forecast_start_override
    if cfg.forecast_end_override:
        forecast_end_ts = cfg.forecast_end_override
    if forecast_start_override:
        forecast_start_ts = forecast_start_override
    if forecast_end_override:
        forecast_end_ts = forecast_end_override

    bucket_width = timedelta(minutes=cfg.bucket_minutes)
    if forecast_end_ts <= forecast_start_ts:
        raise ValueError("forecast_end_ts must be > forecast_start_ts")
    horizon_buckets = int((forecast_end_ts - forecast_start_ts) / bucket_width)
    if forecast_start_ts + horizon_buckets * bucket_width != forecast_end_ts:
        raise ValueError("forecast window must align to bucket_minutes")
    if horizon_buckets <= 0:
        raise ValueError("horizon_buckets must be > 0")

    lock_key = _lock_key()
    started_at = utc_now()
    overlap_locked = _acquire_overlap_lock(lock_key)

    log_row = ScoringRunLogRow(
        run_id=actual_run_id,
        started_at=started_at,
        ended_at=None,
        status="running" if overlap_locked else "skipped_overlap",
        failure_reason=None,
        model_name=cfg.model_name,
        model_version=None,
        model_stage=cfg.model_stage,
        feature_version=cfg.feature_version,
        forecast_run_key=None,
        scoring_created_at=scoring_created_at,
        forecast_start_ts=forecast_start_ts,
        forecast_end_ts=forecast_end_ts,
        horizon_buckets=horizon_buckets,
        bucket_minutes=cfg.bucket_minutes,
        zone_count=None,
        row_count=None,
        latency_ms=None,
        confidence_reference_updated_at=None,
        config_snapshot={"scoring": cfg.to_dict()},
    )
    upsert_scoring_run_log(engine=engine, row=log_row)

    if not overlap_locked:
        return asdict(
            ScoringResult(
                run_id=actual_run_id,
                status="skipped",
                message="Skipped scoring run due to overlap lock.",
                forecast_run_key=None,
                model=None,
                window={
                    "scoring_created_at": scoring_created_at.isoformat(),
                    "forecast_start_ts": forecast_start_ts.isoformat(),
                    "forecast_end_ts": forecast_end_ts.isoformat(),
                },
                counts={"zone_count": 0, "row_count": 0},
                checks=None,
                artifacts_dir=None,
            )
        )

    artifacts_dir = _ensure_reports_dir(cfg.reports_dir, actual_run_id)

    try:
        zone_ids, latest_bucket_ts = load_zone_ids_for_scoring(
            engine=engine,
            as_of_ts=forecast_start_ts,
            feature_version=cfg.feature_version,
            bucket_minutes=cfg.bucket_minutes,
            max_zones=cfg.max_zones,
        )
        latest_bucket = pd.Timestamp(latest_bucket_ts)
        if latest_bucket.tz is None:
            latest_bucket = latest_bucket.tz_localize("UTC")
        else:
            latest_bucket = latest_bucket.tz_convert("UTC")
        forecast_start_pd = pd.Timestamp(forecast_start_ts)
        if forecast_start_pd.tz is None:
            forecast_start_pd = forecast_start_pd.tz_localize("UTC")
        else:
            forecast_start_pd = forecast_start_pd.tz_convert("UTC")
        last_observed_end = latest_bucket + pd.Timedelta(minutes=cfg.bucket_minutes)
        staleness = forecast_start_pd - last_observed_end
        if staleness > pd.Timedelta(minutes=cfg.max_feature_staleness_minutes):
            staleness_minutes = staleness.total_seconds() / 60.0
            raise RuntimeError(
                "fact_demand_features is stale for scoring: "
                f"feature_version={cfg.feature_version!r} "
                f"latest_bucket_start_ts={latest_bucket.isoformat()} "
                f"forecast_start_ts={forecast_start_pd.isoformat()} "
                f"staleness_minutes={staleness_minutes:.1f} "
                f"> max_feature_staleness_minutes={cfg.max_feature_staleness_minutes}. "
                "Backfill features further forward or use --forecast-start-ts/--forecast-end-ts to score within available history."
            )
        history_start_ts, history_end_ts = build_history_window(
            forecast_start_ts=forecast_start_ts,
            history_days=cfg.history_days,
            history_extra_hours=cfg.history_extra_hours,
        )

        loaded = load_champion_model(
            tracking_uri=settings.MLFLOW_TRACKING_URI,
            model_name=cfg.model_name,
            model_stage=cfg.model_stage,
            required_feature_columns=FEATURE_COLUMNS,
        )

        history = build_history_matrix(
            engine=engine,
            zone_ids=zone_ids,
            history_start_ts=history_start_ts,
            history_end_ts=history_end_ts,
            feature_version=cfg.feature_version,
            horizon_buckets=horizon_buckets,
            bucket_minutes=cfg.bucket_minutes,
            lag_null_policy=cfg.lag_null_policy,
        )

        holiday_dates = load_holiday_dates(
            engine=engine,
            start_date=forecast_start_ts.date(),
            end_date=(forecast_end_ts - bucket_width).date(),
        )

        predict_elapsed_ms = 0.0
        step_frames: list[pd.DataFrame] = []
        for step in range(horizon_buckets):
            ts = forecast_start_ts + step * bucket_width
            step_df = build_step_features(
                history=history,
                step_index=step,
                bucket_start_ts=ts,
                feature_tz=cfg.run_timezone,
                holidays=holiday_dates,
                lag_null_policy=cfg.lag_null_policy,
            )
            x = step_df[FEATURE_COLUMNS]
            start = time.perf_counter()
            preds = np.asarray(loaded.model.predict(x), dtype=float)
            predict_elapsed_ms += (time.perf_counter() - start) * 1000.0
            preds = np.clip(preds, 0.0, None)
            step_df["y_pred"] = preds
            step_df["horizon_index"] = int(step + 1)
            history.values[:, history.history_len + step] = preds
            step_frames.append(step_df)

        forecast_frame = pd.concat(step_frames, ignore_index=True)
        zone_policy = load_zone_policy(engine=engine, policy_version=cfg.policy_version, as_of_ts=forecast_start_ts)

        reference = ensure_confidence_reference(
            engine=engine, model=loaded.model, config=cfg, as_of_ts=forecast_start_ts, zone_ids=zone_ids
        )
        write_confidence_diagnostics(reference=reference, output_path=artifacts_dir / "confidence_diagnostics.png")
        write_reference_snapshot(reference=reference, output_path=artifacts_dir / "confidence_reference.json")

        forecast_scored = apply_confidence(
            forecasts=forecast_frame,
            reference=reference,
            zone_policy=zone_policy,
            config=cfg,
        )

        forecast_run_key = _forecast_run_key(
            model_version=loaded.model_version, forecast_start_ts=forecast_start_ts, horizon_buckets=horizon_buckets
        )
        forecast_created_at = scoring_created_at

        publish_frame = pd.DataFrame(
            {
                "forecast_run_key": forecast_run_key,
                "zone_id": forecast_scored["zone_id"].astype(int),
                "bucket_start_ts": pd.to_datetime(forecast_scored["bucket_start_ts"], utc=True),
                "forecast_created_at": forecast_created_at,
                "horizon_index": forecast_scored["horizon_index"].astype(int),
                "y_pred": forecast_scored["y_pred"].astype(float),
                "y_pred_lower": forecast_scored["y_pred_lower"].astype(float),
                "y_pred_upper": forecast_scored["y_pred_upper"].astype(float),
                "confidence_score": forecast_scored["confidence_score"].astype(float),
                "uncertainty_band": forecast_scored["uncertainty_band"].astype(str),
                "used_recursive_features": forecast_scored["used_recursive_features"].astype(bool),
                "model_name": loaded.model_name,
                "model_version": loaded.model_version,
                "model_stage": loaded.model_stage,
                "feature_version": cfg.feature_version,
                "run_id": actual_run_id,
                "scoring_window_start": history_start_ts,
                "scoring_window_end": history_end_ts,
            }
        )

        check_summary = run_checks(
            forecasts=publish_frame,
            zone_count=len(zone_ids),
            horizon_buckets=horizon_buckets,
            forecast_start_ts=forecast_start_ts,
            zone_lineage=history.zone_lineage,
            min_zone_coverage_pct=cfg.min_zone_coverage_pct,
            max_feature_staleness_minutes=cfg.max_feature_staleness_minutes,
        )
        enforce_checks(check_summary)

        sample = publish_frame.sort_values(["zone_id", "bucket_start_ts"]).head(200)
        sample.to_csv(artifacts_dir / "forecast_sample.csv", index=False)
        history.zone_lineage.to_csv(artifacts_dir / "coverage_summary.csv", index=False)

        if validate_only:
            ended_at = utc_now()
            upsert_scoring_run_log(
                engine=engine,
                row=ScoringRunLogRow(
                    run_id=actual_run_id,
                    started_at=started_at,
                    ended_at=ended_at,
                    status="validated",
                    failure_reason=None,
                    model_name=loaded.model_name,
                    model_version=loaded.model_version,
                    model_stage=loaded.model_stage,
                    feature_version=cfg.feature_version,
                    forecast_run_key=forecast_run_key,
                    scoring_created_at=scoring_created_at,
                    forecast_start_ts=forecast_start_ts,
                    forecast_end_ts=forecast_end_ts,
                    horizon_buckets=horizon_buckets,
                    bucket_minutes=cfg.bucket_minutes,
                    zone_count=len(zone_ids),
                    row_count=int(len(publish_frame)),
                    latency_ms=float(predict_elapsed_ms / max(len(publish_frame), 1)),
                    confidence_reference_updated_at=reference.updated_at,
                    config_snapshot={"scoring": cfg.to_dict(), "checks": check_summary.to_dict()},
                ),
            )
            return asdict(
                ScoringResult(
                    run_id=actual_run_id,
                    status="validated",
                    message="Scoring checks passed; forecasts not written (validate-only).",
                    forecast_run_key=forecast_run_key,
                    model={
                        "model_name": loaded.model_name,
                        "model_stage": loaded.model_stage,
                        "model_version": loaded.model_version,
                        "mlflow_run_id": loaded.mlflow_run_id,
                    },
                    window={
                        "scoring_created_at": scoring_created_at.isoformat(),
                        "forecast_start_ts": forecast_start_ts.isoformat(),
                        "forecast_end_ts": forecast_end_ts.isoformat(),
                        "history_start_ts": history_start_ts.isoformat(),
                        "history_end_ts": history_end_ts.isoformat(),
                        "latest_observed_bucket_ts": pd.Timestamp(latest_bucket_ts).isoformat(),
                    },
                    counts={"zone_count": len(zone_ids), "row_count": int(len(publish_frame))},
                    checks=check_summary.to_dict(),
                    artifacts_dir=str(artifacts_dir),
                )
            )

        written_rows = upsert_demand_forecast(engine=engine, forecasts=publish_frame)

        metrics = {
            "row_count": float(written_rows),
            "zone_count": float(len(zone_ids)),
            "avg_confidence_score": float(publish_frame["confidence_score"].mean()),
            "pct_low_uncertainty": float((publish_frame["uncertainty_band"] == "low").mean()),
            "predict_latency_ms_per_row": float(predict_elapsed_ms / max(len(publish_frame), 1)),
        }
        (artifacts_dir / "run_summary.json").write_text(
            json.dumps(
                {
                    "run_id": actual_run_id,
                    "status": "succeeded",
                    "forecast_run_key": forecast_run_key,
                    "model": {
                        "model_name": loaded.model_name,
                        "model_stage": loaded.model_stage,
                        "model_version": loaded.model_version,
                        "mlflow_run_id": loaded.mlflow_run_id,
                    },
                    "window": {
                        "scoring_created_at": scoring_created_at.isoformat(),
                        "forecast_start_ts": forecast_start_ts.isoformat(),
                        "forecast_end_ts": forecast_end_ts.isoformat(),
                        "history_start_ts": history_start_ts.isoformat(),
                        "history_end_ts": history_end_ts.isoformat(),
                    },
                    "checks": check_summary.to_dict(),
                    "metrics": metrics,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        _log_to_mlflow(
            config=cfg,
            loaded=loaded,
            run_id=actual_run_id,
            forecast_run_key=forecast_run_key,
            metrics=metrics,
            artifacts_dir=artifacts_dir,
        )

        ended_at = utc_now()
        upsert_scoring_run_log(
            engine=engine,
            row=ScoringRunLogRow(
                run_id=actual_run_id,
                started_at=started_at,
                ended_at=ended_at,
                status="succeeded",
                failure_reason=None,
                model_name=loaded.model_name,
                model_version=loaded.model_version,
                model_stage=loaded.model_stage,
                feature_version=cfg.feature_version,
                forecast_run_key=forecast_run_key,
                scoring_created_at=scoring_created_at,
                forecast_start_ts=forecast_start_ts,
                forecast_end_ts=forecast_end_ts,
                horizon_buckets=horizon_buckets,
                bucket_minutes=cfg.bucket_minutes,
                zone_count=len(zone_ids),
                row_count=written_rows,
                latency_ms=float(predict_elapsed_ms / max(len(publish_frame), 1)),
                confidence_reference_updated_at=reference.updated_at,
                config_snapshot={"scoring": cfg.to_dict(), "checks": check_summary.to_dict()},
            ),
        )

        return asdict(
            ScoringResult(
                run_id=actual_run_id,
                status="succeeded",
                message=None,
                forecast_run_key=forecast_run_key,
                model={
                    "model_name": loaded.model_name,
                    "model_stage": loaded.model_stage,
                    "model_version": loaded.model_version,
                    "mlflow_run_id": loaded.mlflow_run_id,
                },
                window={
                    "scoring_created_at": scoring_created_at.isoformat(),
                    "forecast_start_ts": forecast_start_ts.isoformat(),
                    "forecast_end_ts": forecast_end_ts.isoformat(),
                    "history_start_ts": history_start_ts.isoformat(),
                    "history_end_ts": history_end_ts.isoformat(),
                    "latest_observed_bucket_ts": pd.Timestamp(latest_bucket_ts).isoformat(),
                },
                counts={"zone_count": len(zone_ids), "row_count": written_rows},
                checks=check_summary.to_dict(),
                artifacts_dir=str(artifacts_dir),
            )
        )

    except ScoringCheckError as exc:
        ended_at = utc_now()
        upsert_scoring_run_log(
            engine=engine,
            row=ScoringRunLogRow(
                run_id=actual_run_id,
                started_at=started_at,
                ended_at=ended_at,
                status="failed",
                failure_reason=str(exc),
                model_name=cfg.model_name,
                model_version=None,
                model_stage=cfg.model_stage,
                feature_version=cfg.feature_version,
                forecast_run_key=None,
                scoring_created_at=scoring_created_at,
                forecast_start_ts=forecast_start_ts,
                forecast_end_ts=forecast_end_ts,
                horizon_buckets=horizon_buckets,
                bucket_minutes=cfg.bucket_minutes,
                zone_count=None,
                row_count=None,
                latency_ms=None,
                confidence_reference_updated_at=None,
                config_snapshot={"scoring": cfg.to_dict(), "check_error": exc.details},
            ),
        )
        (artifacts_dir / "run_summary.json").write_text(
            json.dumps({"run_id": actual_run_id, "status": "failed", "error": str(exc), "details": exc.details}, indent=2),
            encoding="utf-8",
        )
        return asdict(
            ScoringResult(
                run_id=actual_run_id,
                status="failed",
                message=str(exc),
                forecast_run_key=None,
                model=None,
                window={
                    "scoring_created_at": scoring_created_at.isoformat(),
                    "forecast_start_ts": forecast_start_ts.isoformat(),
                    "forecast_end_ts": forecast_end_ts.isoformat(),
                },
                counts={"zone_count": 0, "row_count": 0},
                checks=exc.details,
                artifacts_dir=str(artifacts_dir),
            )
        )
    except Exception as exc:  # noqa: BLE001
        ended_at = utc_now()
        upsert_scoring_run_log(
            engine=engine,
            row=ScoringRunLogRow(
                run_id=actual_run_id,
                started_at=started_at,
                ended_at=ended_at,
                status="failed",
                failure_reason=str(exc),
                model_name=cfg.model_name,
                model_version=None,
                model_stage=cfg.model_stage,
                feature_version=cfg.feature_version,
                forecast_run_key=None,
                scoring_created_at=scoring_created_at,
                forecast_start_ts=forecast_start_ts,
                forecast_end_ts=forecast_end_ts,
                horizon_buckets=horizon_buckets,
                bucket_minutes=cfg.bucket_minutes,
                zone_count=None,
                row_count=None,
                latency_ms=None,
                confidence_reference_updated_at=None,
                config_snapshot={"scoring": cfg.to_dict()},
            ),
        )
        (artifacts_dir / "run_summary.json").write_text(
            json.dumps({"run_id": actual_run_id, "status": "failed", "error": str(exc)}, indent=2),
            encoding="utf-8",
        )
        return asdict(
            ScoringResult(
                run_id=actual_run_id,
                status="failed",
                message=str(exc),
                forecast_run_key=None,
                model=None,
                window={
                    "scoring_created_at": scoring_created_at.isoformat(),
                    "forecast_start_ts": forecast_start_ts.isoformat(),
                    "forecast_end_ts": forecast_end_ts.isoformat(),
                },
                counts={"zone_count": 0, "row_count": 0},
                checks=None,
                artifacts_dir=str(artifacts_dir),
            )
        )
    finally:
        _release_overlap_lock(lock_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 5 scoring orchestrator")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--forecast-start-ts", default=None, help="ISO8601 override, e.g. 2025-11-03T00:00:00+00:00")
    parser.add_argument("--forecast-end-ts", default=None, help="ISO8601 override end-exclusive")
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_override = datetime.fromisoformat(args.forecast_start_ts.replace("Z", "+00:00")) if args.forecast_start_ts else None
    end_override = datetime.fromisoformat(args.forecast_end_ts.replace("Z", "+00:00")) if args.forecast_end_ts else None
    result = run_scoring(
        run_id=args.run_id,
        forecast_start_override=start_override,
        forecast_end_override=end_override,
        validate_only=bool(args.validate_only),
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
