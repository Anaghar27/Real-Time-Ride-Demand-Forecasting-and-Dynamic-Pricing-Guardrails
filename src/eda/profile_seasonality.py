"""Phase 3.1 seasonality and zone behavior profiling."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import text
from statsmodels.tsa.stattools import acf

from src.common.db import engine
from src.eda.utils import (
    EDAParams,
    build_eda_params,
    ensure_eda_tables,
    ensure_run_dir,
    fetch_eda_base,
    load_yaml,
    save_dataframe_csv,
)


def _profile_stats(df: pd.DataFrame, group_col: str, profile_type: str, params: EDAParams) -> pd.DataFrame:
    agg = (
        df.groupby(group_col)["pickup_count"]
        .agg(
            mean_pickup_count="mean",
            median_pickup_count="median",
            row_count="count",
        )
        .reset_index()
    )
    p90 = df.groupby(group_col)["pickup_count"].quantile(0.9).reset_index(name="p90_pickup_count")
    out = agg.merge(p90, on=group_col, how="left")
    out["profile_type"] = profile_type
    out["profile_key"] = out[group_col].astype(str)
    out["run_id"] = params.run_id
    out["feature_version"] = params.feature_version
    out["data_start_ts"] = params.data_start_ts
    out["data_end_ts"] = params.data_end_ts
    return out[
        [
            "run_id",
            "feature_version",
            "profile_type",
            "profile_key",
            "mean_pickup_count",
            "median_pickup_count",
            "p90_pickup_count",
            "row_count",
            "data_start_ts",
            "data_end_ts",
        ]
    ]


def _safe_acf(series: pd.Series, lag: int) -> float | None:
    if len(series) <= lag + 1:
        return None
    if float(series.var(ddof=1) or 0.0) == 0.0:
        return None
    values = acf(series.values, nlags=lag, fft=True, missing="drop")
    return float(values[lag])


def _seasonality_index(zone_df: pd.DataFrame) -> float | None:
    total_var = float(zone_df["pickup_count"].var(ddof=1) or 0.0)
    if total_var <= 0.0:
        return None
    seasonal_var = float(zone_df.groupby("hour_of_day")["pickup_count"].mean().var(ddof=1) or 0.0)
    return seasonal_var / total_var


def _persist_table(table_name: str, run_id: str, df: pd.DataFrame) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"DELETE FROM {table_name} WHERE run_id = :run_id"), {"run_id": run_id})
    df.to_sql(table_name, con=engine, if_exists="append", index=False, method="multi")


def _plot_hourly(df: pd.DataFrame, output_dir: Path) -> None:
    hourly = df.groupby("hour_of_day")["pickup_count"]
    summary = hourly.agg(["mean", "median"]).reset_index()
    p90 = hourly.quantile(0.9).reset_index(name="p90")
    summary = summary.merge(p90, on="hour_of_day")

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(summary["hour_of_day"], summary["mean"], label="mean")
    ax.plot(summary["hour_of_day"], summary["median"], label="median")
    ax.plot(summary["hour_of_day"], summary["p90"], label="p90")
    ax.set_title("Hourly Demand Curve")
    ax.set_xlabel("hour_of_day")
    ax.set_ylabel("pickup_count")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "hourly_demand_curve.png")
    plt.close(fig)


def _plot_weekday_weekend(df: pd.DataFrame, output_dir: Path) -> None:
    overlay = df.groupby(["is_weekend", "hour_of_day"])["pickup_count"].mean().reset_index()
    fig, ax = plt.subplots(figsize=(10, 4))
    for is_weekend, label in [(False, "weekday"), (True, "weekend")]:
        subset = overlay[overlay["is_weekend"] == is_weekend]
        ax.plot(subset["hour_of_day"], subset["pickup_count"], label=label)
    ax.set_title("Weekday vs Weekend Overlay")
    ax.set_xlabel("hour_of_day")
    ax.set_ylabel("mean pickup_count")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "weekday_weekend_overlay.png")
    plt.close(fig)


def _plot_zone_histograms(zone_summary: pd.DataFrame, output_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(zone_summary["total_pickups"], bins=30)
    ax.set_title("Zone Volume Distribution")
    ax.set_xlabel("total_pickups")
    fig.tight_layout()
    fig.savefig(output_dir / "zone_volume_distribution_hist.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(zone_summary["zero_demand_ratio"], bins=30)
    ax.set_title("Zero-Demand Ratio Distribution")
    ax.set_xlabel("zero_demand_ratio")
    fig.tight_layout()
    fig.savefig(output_dir / "zero_ratio_hist.png")
    plt.close(fig)


def _plot_top_bottom_profiles(df: pd.DataFrame, zone_summary: pd.DataFrame, output_dir: Path, top_n: int, bottom_n: int) -> None:
    top_zones = zone_summary.nlargest(top_n, "total_pickups")["zone_id"].tolist()
    bottom_zones = zone_summary.nsmallest(bottom_n, "total_pickups")["zone_id"].tolist()

    for label, zones in [("top", top_zones), ("bottom", bottom_zones)]:
        profile = (
            df[df["zone_id"].isin(zones)]
            .groupby(["zone_id", "hour_of_day"])["pickup_count"]
            .mean()
            .reset_index()
        )
        fig, ax = plt.subplots(figsize=(10, 5))
        for zone_id in zones:
            subset = profile[profile["zone_id"] == zone_id]
            ax.plot(subset["hour_of_day"], subset["pickup_count"], label=f"zone_{zone_id}")
        ax.set_title(f"{label.title()} Zone Hourly Profiles")
        ax.set_xlabel("hour_of_day")
        ax.set_ylabel("mean pickup_count")
        ax.legend(ncols=2, fontsize=8)
        fig.tight_layout()
        fig.savefig(output_dir / f"{label}_zone_profiles.png")
        plt.close(fig)


def _plot_heatmap(df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    matrix = (
        df.groupby(["zone_id", "hour_of_day"])["pickup_count"]
        .mean()
        .unstack(fill_value=0.0)
        .sort_index()
    )

    fig, ax = plt.subplots(figsize=(12, 8))
    heat = ax.imshow(matrix.to_numpy(), aspect="auto", interpolation="nearest")
    ax.set_title("Zone-Hour Heatmap")
    ax.set_xlabel("hour_of_day")
    ax.set_ylabel("zone_id index")
    fig.colorbar(heat, ax=ax)
    fig.tight_layout()
    fig.savefig(output_dir / "zone_hour_heatmap.png")
    plt.close(fig)

    return matrix.reset_index()


def run_seasonality_profile(params: EDAParams) -> dict[str, Any]:
    ensure_eda_tables(engine)
    run_dir = ensure_run_dir(params)
    df = fetch_eda_base(params)
    if df.empty:
        raise ValueError("No fact_demand_features data found for EDA window")

    df["bucket_start_ts"] = pd.to_datetime(df["bucket_start_ts"], utc=True)
    df["date"] = df["bucket_start_ts"].dt.date

    time_profiles = pd.concat(
        [
            _profile_stats(df, "hour_of_day", "hour_of_day", params),
            _profile_stats(df, "day_of_week", "day_of_week", params),
            _profile_stats(df, "month", "month", params),
            _profile_stats(df, "is_weekend", "is_weekend", params),
            _profile_stats(df, "is_holiday", "is_holiday", params),
        ],
        ignore_index=True,
    )

    zone_group = df.groupby("zone_id")
    zone_summary = zone_group["pickup_count"].agg(
        total_pickups="sum",
        mean_pickup_count="mean",
        variance_pickup_count="var",
    ).reset_index()
    zone_summary["variance_pickup_count"] = zone_summary["variance_pickup_count"].fillna(0.0)
    zone_summary["coeff_variation"] = np.where(
        zone_summary["mean_pickup_count"] > 0,
        np.sqrt(zone_summary["variance_pickup_count"]) / zone_summary["mean_pickup_count"],
        np.nan,
    )
    zero_ratio = zone_group.apply(lambda x: float((x["pickup_count"] == 0).mean()), include_groups=False).reset_index(name="zero_demand_ratio")
    zone_summary = zone_summary.merge(zero_ratio, on="zone_id", how="left")

    peak = (
        df.groupby(["zone_id", "hour_of_day"])["pickup_count"].sum().reset_index(name="hour_pickups")
        .sort_values(["zone_id", "hour_pickups"], ascending=[True, False])
        .drop_duplicates("zone_id")
        [["zone_id", "hour_pickups"]]
        .rename(columns={"hour_pickups": "peak_hour_pickups"})
    )
    zone_summary = zone_summary.merge(peak, on="zone_id", how="left")
    zone_summary["peak_hour_concentration"] = np.where(
        zone_summary["total_pickups"] > 0,
        zone_summary["peak_hour_pickups"] / zone_summary["total_pickups"],
        0.0,
    )

    seasonality_scores = []
    for zone_id, zone_df in df.groupby("zone_id"):
        series = zone_df.sort_values("bucket_start_ts")["pickup_count"]
        seasonality_scores.append(
            {
                "zone_id": int(zone_id),
                "intraday_periodicity_score": _safe_acf(series, 96),
                "weekly_periodicity_score": _safe_acf(series, 672),
                "seasonality_strength_index": _seasonality_index(zone_df),
            }
        )
    score_df = pd.DataFrame(seasonality_scores)
    zone_summary = zone_summary.merge(score_df, on="zone_id", how="left")
    zone_summary["run_id"] = params.run_id
    zone_summary["feature_version"] = params.feature_version
    zone_summary["data_start_ts"] = params.data_start_ts
    zone_summary["data_end_ts"] = params.data_end_ts
    zone_summary["created_at"] = pd.Timestamp.utcnow()

    zone_summary = zone_summary[
        [
            "run_id",
            "feature_version",
            "zone_id",
            "total_pickups",
            "mean_pickup_count",
            "variance_pickup_count",
            "coeff_variation",
            "zero_demand_ratio",
            "peak_hour_concentration",
            "intraday_periodicity_score",
            "weekly_periodicity_score",
            "seasonality_strength_index",
            "data_start_ts",
            "data_end_ts",
            "created_at",
        ]
    ]

    seasonality_metrics = []
    global_series = df.sort_values("bucket_start_ts").groupby("bucket_start_ts")["pickup_count"].sum()
    for lag in [1, 4, 96, 672]:
        score = _safe_acf(global_series, lag)
        if score is not None:
            seasonality_metrics.append(
                {
                    "run_id": params.run_id,
                    "feature_version": params.feature_version,
                    "metric_name": f"global_acf_lag_{lag}",
                    "metric_scope": "global",
                    "metric_value": score,
                    "metric_rank": None,
                    "details": {"lag": lag},
                    "data_start_ts": params.data_start_ts,
                    "data_end_ts": params.data_end_ts,
                }
            )

    top_zone = zone_summary.sort_values("total_pickups", ascending=False).head(params.top_n_zones)
    for idx, record in enumerate(top_zone.to_dict(orient="records"), start=1):
        seasonality_metrics.append(
            {
                "run_id": params.run_id,
                "feature_version": params.feature_version,
                "metric_name": "top_zone_total_pickups",
                "metric_scope": str(record["zone_id"]),
                "metric_value": float(record["total_pickups"]),
                "metric_rank": idx,
                "details": {"zone_id": int(record["zone_id"])},
                "data_start_ts": params.data_start_ts,
                "data_end_ts": params.data_end_ts,
            }
        )

    seasonality_summary = pd.DataFrame(seasonality_metrics)
    if not seasonality_summary.empty:
        seasonality_summary["details"] = seasonality_summary["details"].apply(json.dumps)

    _persist_table("eda_time_profile_summary", params.run_id, time_profiles)
    _persist_table("eda_zone_profile_summary", params.run_id, zone_summary)
    if not seasonality_summary.empty:
        _persist_table("eda_seasonality_summary", params.run_id, seasonality_summary)

    _plot_hourly(df, run_dir)
    _plot_weekday_weekend(df, run_dir)
    _plot_zone_histograms(zone_summary, run_dir)
    _plot_top_bottom_profiles(df, zone_summary, run_dir, params.top_n_zones, params.bottom_n_zones)

    heatmap_matrix = _plot_heatmap(df, run_dir)
    dow_hour = (
        df.groupby(["day_of_week", "hour_of_day"])["pickup_count"]
        .mean()
        .reset_index(name="mean_pickup_count")
    )

    save_dataframe_csv(heatmap_matrix, run_dir / "zone_hour_heatmap_matrix.csv")
    save_dataframe_csv(dow_hour, run_dir / "day_of_week_hour_profile.csv")
    save_dataframe_csv(zone_summary.sort_values("total_pickups", ascending=False).head(params.top_n_zones), run_dir / "top_zones.csv")
    save_dataframe_csv(zone_summary.sort_values("total_pickups", ascending=True).head(params.bottom_n_zones), run_dir / "bottom_zones.csv")

    return {
        "run_id": params.run_id,
        "rows": int(len(df)),
        "zones": int(df["zone_id"].nunique()),
        "time_profile_rows": int(len(time_profiles)),
        "zone_profile_rows": int(len(zone_summary)),
        "seasonality_rows": int(len(seasonality_summary)),
        "output_dir": str(run_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run seasonality and zone behavior profiling")
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--end-date", required=False)
    parser.add_argument("--feature-version", required=False)
    parser.add_argument("--policy-version", required=False)
    parser.add_argument("--zones", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default="configs/eda.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_yaml(Path(args.config))
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
    result = run_seasonality_profile(params)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
