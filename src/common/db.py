"""
Database connection utilities.
It centralizes cross-cutting concerns like settings, logging, and database access used by the pipelines.
Keeping these helpers isolated reduces duplication and keeps domain modules focused on business logic.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.common.settings import get_settings

_SETTINGS = get_settings()

engine: Engine = create_engine(_SETTINGS.DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, class_=Session)


def test_connection() -> bool:
    """Return True if the database can be reached and queried."""

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
