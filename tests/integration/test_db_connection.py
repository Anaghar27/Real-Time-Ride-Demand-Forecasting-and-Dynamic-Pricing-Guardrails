import pytest

from src.common.db import test_connection as db_test_connection


@pytest.mark.integration
def test_db_connection_optional() -> None:
    if not db_test_connection():
        pytest.skip("Postgres unavailable in local test environment")
    assert db_test_connection() is True
