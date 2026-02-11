from sqlalchemy import create_engine, text

from src.ingestion.gate import evaluate_phase1_gate


def test_gate_fails_with_insufficient_batches() -> None:
    test_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with test_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE ingestion_batch_log (
                    batch_id TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    state TEXT NOT NULL
                )
                """
            )
        )

    passed, details = evaluate_phase1_gate(test_engine, min_successful_batches=2, run_tests=False)

    assert passed is False
    assert "insufficient_successful_sample_batches" in details["reasons"]
