"""
Logging configuration helpers.
It centralizes cross-cutting concerns like settings, logging, and database access used by the pipelines.
Keeping these helpers isolated reduces duplication and keeps domain modules focused on business logic.
"""

from __future__ import annotations

import logging

from src.common.settings import get_settings

_LOGGING_CONFIGURED = False


def configure_logging() -> None:
    """Configure process-wide logging from environment settings."""

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    settings = get_settings()
    level_name = settings.LOG_LEVEL.upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    _LOGGING_CONFIGURED = True
