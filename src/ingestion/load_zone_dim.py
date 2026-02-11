"""Load and validate taxi zone reference dimension."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.common.db import engine
from src.ingestion.ddl import apply_ingestion_ddl

ZONE_FILE = Path("data/landing/reference/taxi_zone_lookup.csv")


def load_zone_dim(zone_file: Path = ZONE_FILE) -> dict[str, float | int]:
    """Load dim_zone table and return persisted join coverage metrics."""

    if not zone_file.exists():
        raise FileNotFoundError(f"Zone lookup file not found: {zone_file}")

    apply_ingestion_ddl(engine)
    zone_df = pd.read_csv(zone_file)
    zone_df = zone_df.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
            "Service Zone": "service_zone",
        }
    )

    required_columns = ["location_id", "borough", "zone", "service_zone"]
    missing_columns = [column for column in required_columns if column not in zone_df.columns]
    if missing_columns:
        raise RuntimeError(f"Missing required zone columns: {missing_columns}")

    zone_df = zone_df[required_columns].drop_duplicates(subset=["location_id"]).copy()
    zone_df["location_id"] = pd.to_numeric(zone_df["location_id"], errors="coerce").astype("Int64")
    zone_df = zone_df.dropna(subset=["location_id", "borough", "zone"])
    zone_df["ingested_at"] = datetime.now(tz=UTC)

    records = zone_df.to_dict(orient="records")
    upsert_sql = text(
        """
        INSERT INTO dim_zone (location_id, borough, zone, service_zone, ingested_at)
        VALUES (:location_id, :borough, :zone, :service_zone, :ingested_at)
        ON CONFLICT (location_id)
        DO UPDATE SET
            borough = EXCLUDED.borough,
            zone = EXCLUDED.zone,
            service_zone = EXCLUDED.service_zone,
            ingested_at = EXCLUDED.ingested_at
        """
    )

    with engine.begin() as connection:
        for record in records:
            connection.execute(upsert_sql, record)

        coverage = connection.execute(
            text(
                """
                WITH trip_counts AS (
                    SELECT
                        COUNT(*) AS total_rows,
                        SUM(CASE WHEN p.location_id IS NOT NULL THEN 1 ELSE 0 END) AS pickup_matched,
                        SUM(CASE WHEN d.location_id IS NOT NULL THEN 1 ELSE 0 END) AS dropoff_matched
                    FROM raw_trips r
                    LEFT JOIN dim_zone p ON r.pickup_location_id = p.location_id
                    LEFT JOIN dim_zone d ON r.dropoff_location_id = d.location_id
                )
                SELECT
                    total_rows,
                    CASE WHEN total_rows = 0 THEN 0 ELSE (pickup_matched::float / total_rows) END AS pickup_coverage,
                    CASE WHEN total_rows = 0 THEN 0 ELSE (dropoff_matched::float / total_rows) END AS dropoff_coverage
                FROM trip_counts
                """
            )
        ).mappings().one()

        connection.execute(
            text(
                """
                INSERT INTO zone_join_coverage_report (reported_at, total_rows, pickup_coverage_pct, dropoff_coverage_pct)
                VALUES (:reported_at, :total_rows, :pickup_coverage, :dropoff_coverage)
                """
            ),
            {
                "reported_at": datetime.now(tz=UTC),
                "total_rows": int(coverage["total_rows"]),
                "pickup_coverage": float(coverage["pickup_coverage"]),
                "dropoff_coverage": float(coverage["dropoff_coverage"]),
            },
        )

    result = {
        "dim_zone_rows": len(records),
        "trip_rows": int(coverage["total_rows"]),
        "pickup_coverage_pct": round(float(coverage["pickup_coverage"]) * 100, 2),
        "dropoff_coverage_pct": round(float(coverage["dropoff_coverage"]) * 100, 2),
    }
    return result


def main() -> None:
    print(json.dumps(load_zone_dim(), indent=2))


if __name__ == "__main__":
    main()
