from src.ingestion.load_raw_trips import _batch_key_for_file


def test_batch_key_is_deterministic() -> None:
    source_file = "data/landing/tlc/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    checksum = "abc123"

    first = _batch_key_for_file(source_file=__import__("pathlib").Path(source_file), checksum=checksum)
    second = _batch_key_for_file(source_file=__import__("pathlib").Path(source_file), checksum=checksum)

    assert first == second
