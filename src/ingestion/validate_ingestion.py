"""Validation-only entrypoint for ingestion checks."""

from __future__ import annotations

import json
import os

from src.ingestion.load_raw_trips import run_sample_ingestion


def main() -> None:
    result = run_sample_ingestion(
        "data/landing/tlc/year=*/month=*/*.parquet",
        validate_only=True,
        max_rows_per_file=int(os.getenv("INGEST_SAMPLE_MAX_ROWS", "200000")),
    )
    print(json.dumps({"validated_batches": result}, indent=2, default=str))


if __name__ == "__main__":
    main()
