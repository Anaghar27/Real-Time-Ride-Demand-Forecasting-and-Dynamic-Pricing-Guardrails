"""Orchestrator for Phase 2 feature pipeline build and publish."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from sqlalchemy import text

from src.common.db import engine
from src.features.aggregate_pickups import aggregate_pickups
from src.features.calendar_features import add_calendar_features
from src.features.ddl import apply_feature_ddl
from src.features.lag_rolling_features import add_lag_rolling_features
from src.features.publish_features import publish_features
from src.features.quality_checks import FeatureQualityError, run_feature_quality_checks
from src.features.runtime import build_feature_params, get_source_bounds
from src.features.time_buckets import build_time_buckets
from src.ingestion.ddl import apply_ingestion_ddl


def _upsert_run_log(
    *,
    run_id: str,
    feature_version: str,
    run_start_ts: datetime,
    run_end_ts: datetime,
    zone_filter: str | None,
    lag_null_policy: str,
    state: str,
    error_message: str | None = None,
    source_min_ts: datetime | None = None,
    source_max_ts: datetime | None = None,
    row_count: int | None = None,
    mark_complete: bool = False,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO feature_batch_log (
                    run_id,
                    feature_version,
                    run_start_ts,
                    run_end_ts,
                    zone_filter,
                    lag_null_policy,
                    source_min_ts,
                    source_max_ts,
                    row_count,
                    state,
                    error_message,
                    started_at,
                    completed_at
                )
                VALUES (
                    :run_id,
                    :feature_version,
                    :run_start_ts,
                    :run_end_ts,
                    :zone_filter,
                    :lag_null_policy,
                    :source_min_ts,
                    :source_max_ts,
                    :row_count,
                    :state,
                    :error_message,
                    :started_at,
                    :completed_at
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    feature_version = EXCLUDED.feature_version,
                    run_start_ts = EXCLUDED.run_start_ts,
                    run_end_ts = EXCLUDED.run_end_ts,
                    zone_filter = EXCLUDED.zone_filter,
                    lag_null_policy = EXCLUDED.lag_null_policy,
                    source_min_ts = EXCLUDED.source_min_ts,
                    source_max_ts = EXCLUDED.source_max_ts,
                    row_count = EXCLUDED.row_count,
                    state = EXCLUDED.state,
                    error_message = EXCLUDED.error_message,
                    completed_at = EXCLUDED.completed_at
                """
            ),
            {
                "run_id": run_id,
                "feature_version": feature_version,
                "run_start_ts": run_start_ts,
                "run_end_ts": run_end_ts,
                "zone_filter": zone_filter,
                "lag_null_policy": lag_null_policy,
                "source_min_ts": source_min_ts,
                "source_max_ts": source_max_ts,
                "row_count": row_count,
                "state": state,
                "error_message": error_message,
                "started_at": datetime.now(tz=UTC),
                "completed_at": datetime.now(tz=UTC) if mark_complete else None,
            },
        )


def build_feature_pipeline(
    *,
    start_date: str,
    end_date: str,
    zones: str | None,
    feature_version: str,
    dry_run: bool,
    run_id: str | None = None,
) -> dict[str, object]:
    """Execute full feature chain from time buckets to publish."""

    params = build_feature_params(
        start_date=start_date,
        end_date=end_date,
        feature_version=feature_version,
        zones_arg=zones,
        run_id=run_id,
    )

    apply_ingestion_ddl(engine)
    apply_feature_ddl(engine)

    _upsert_run_log(
        run_id=params.run_id,
        feature_version=params.feature_version,
        run_start_ts=params.run_start_ts,
        run_end_ts=params.run_end_ts,
        zone_filter=zones,
        lag_null_policy=params.lag_null_policy,
        state="running",
    )

    try:
        step_results: dict[str, object] = {}
        step_results["time_buckets"] = build_time_buckets(params)
        step_results["aggregate_pickups"] = aggregate_pickups(params)
        step_results["calendar_features"] = add_calendar_features(params)
        step_results["lag_rolling_features"] = add_lag_rolling_features(params)
        step_results["quality_checks"] = run_feature_quality_checks(params)

        publish_result: dict[str, int | str] | None = None
        if not dry_run:
            publish_result = publish_features(params)

        source_min_ts, source_max_ts = get_source_bounds(engine, params)
        row_count = int(publish_result["row_count"]) if publish_result else 0

        _upsert_run_log(
            run_id=params.run_id,
            feature_version=params.feature_version,
            run_start_ts=params.run_start_ts,
            run_end_ts=params.run_end_ts,
            zone_filter=zones,
            lag_null_policy=params.lag_null_policy,
            source_min_ts=source_min_ts,
            source_max_ts=source_max_ts,
            row_count=row_count,
            state="succeeded" if not dry_run else "validated",
            mark_complete=True,
        )

        return {
            "run_id": params.run_id,
            "feature_version": params.feature_version,
            "dry_run": dry_run,
            "steps": step_results,
            "publish": publish_result,
        }

    except FeatureQualityError as exc:
        _upsert_run_log(
            run_id=params.run_id,
            feature_version=params.feature_version,
            run_start_ts=params.run_start_ts,
            run_end_ts=params.run_end_ts,
            zone_filter=zones,
            lag_null_policy=params.lag_null_policy,
            state="failed",
            error_message=str(exc),
            mark_complete=True,
        )
        raise
    except Exception as exc:  # noqa: BLE001
        _upsert_run_log(
            run_id=params.run_id,
            feature_version=params.feature_version,
            run_start_ts=params.run_start_ts,
            run_end_ts=params.run_end_ts,
            zone_filter=zones,
            lag_null_policy=params.lag_null_policy,
            state="failed",
            error_message=str(exc),
            mark_complete=True,
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build end-to-end Phase 2 feature pipeline")
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--zones", default=None, help="Optional comma-separated zone ids")
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = build_feature_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        zones=args.zones,
        feature_version=args.feature_version,
        dry_run=args.dry_run,
        run_id=args.run_id,
    )
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
