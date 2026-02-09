"""FastAPI application entrypoint."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import FastAPI

from src.common.db import test_connection
from src.common.logging import configure_logging
from src.common.settings import get_settings

configure_logging()
settings = get_settings()
app = FastAPI(title=settings.PROJECT_NAME, version="0.1.0")


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat()


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "environment": settings.ENV,
        "timestamp": _utc_timestamp(),
    }


@app.get("/ready")
def ready() -> dict[str, object]:
    is_ready = test_connection()
    return {
        "ready": is_ready,
        "database": "reachable" if is_ready else "unreachable",
        "timestamp": _utc_timestamp(),
    }


@app.get("/version")
def version() -> dict[str, str]:
    return {
        "project": settings.PROJECT_NAME,
        "version": app.version,
    }
