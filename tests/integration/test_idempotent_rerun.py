import os

import pytest

from src.common.db import test_connection
from src.ingestion.load_raw_trips import run_sample_ingestion

if os.getenv("RUN_INGESTION_INTEGRATION") != "1":
    pytest.skip("Set RUN_INGESTION_INTEGRATION=1 to run ingestion integration tests", allow_module_level=True)


@pytest.mark.integration
def test_idempotent_rerun_returns_no_new_updates() -> None:
    if not test_connection():
        pytest.skip("Postgres unavailable in local test environment")

    first_run = run_sample_ingestion("data/landing/tlc/year=*/month=*/*.parquet", validate_only=False)
    second_run = run_sample_ingestion("data/landing/tlc/year=*/month=*/*.parquet", validate_only=False)

    assert len(first_run) == len(second_run)
    assert all(summary["inserted_or_updated"] == 0 for summary in second_run)
