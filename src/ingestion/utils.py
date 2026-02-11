"""Shared ingestion utility functions."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256sum(file_path: Path) -> str:
    """Return SHA-256 checksum for a file."""

    digest = hashlib.sha256()
    with file_path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
