import os
from pathlib import Path

import pytest

from src.common.db import test_connection
from src.ingestion.fetch import download_sample_files
from src.ingestion.load_raw_trips import run_sample_ingestion

if os.getenv("RUN_INGESTION_INTEGRATION") != "1":
    pytest.skip("Set RUN_INGESTION_INTEGRATION=1 to run ingestion integration tests", allow_module_level=True)


@pytest.mark.integration
def test_sample_ingestion_runs_end_to_end() -> None:
    if not test_connection():
        pytest.skip("Postgres unavailable in local test environment")

    download_sample_files(["2024-01", "2024-02"])
    summaries = run_sample_ingestion("data/landing/tlc/year=*/month=*/*.parquet", validate_only=False)

    assert len(summaries) >= 2
    assert all(summary["state"] in {"succeeded", "failed"} for summary in summaries)
    assert any(Path(summary["source_file"]).exists() for summary in summaries)
