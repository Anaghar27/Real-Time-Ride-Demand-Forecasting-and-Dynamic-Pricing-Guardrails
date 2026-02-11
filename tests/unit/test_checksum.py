from pathlib import Path

from src.ingestion.utils import sha256sum


def test_sha256sum_is_stable(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("phase1-checksum", encoding="utf-8")

    first = sha256sum(file_path)
    second = sha256sum(file_path)

    assert first == second
    assert len(first) == 64
