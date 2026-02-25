# This file wraps database access so API services can run parameterized SQL safely.
# It exists to keep SQL execution details out of router code and make testing easier.
# The helper also centralizes table-existence checks and optional request logging writes.
# Keeping this layer small makes query behavior easier to audit and troubleshoot.

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class DatabaseClient:
    """Minimal SQLAlchemy wrapper for API read/write access."""

    def __init__(self, *, database_url: str) -> None:
        self._engine: Engine = create_engine(database_url, pool_pre_ping=True, future=True)
        self._request_log_table_available: bool | None = None

    @property
    def engine(self) -> Engine:
        return self._engine

    def can_connect(self) -> bool:
        try:
            with self._engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False

    def table_exists(self, table_name: str) -> bool:
        self._validate_identifier(table_name)
        query = text("SELECT to_regclass(:table_name) IS NOT NULL AS exists_flag")
        with self._engine.connect() as connection:
            result = connection.execute(query, {"table_name": table_name}).scalar_one()
        return bool(result)

    def fetch_all(self, query: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        with self._engine.connect() as connection:
            rows = connection.execute(text(query), dict(params or {})).mappings().all()
        return [dict(row) for row in rows]

    def fetch_one(self, query: str, params: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        with self._engine.connect() as connection:
            row = connection.execute(text(query), dict(params or {})).mappings().first()
        return dict(row) if row is not None else None

    def fetch_scalar(self, query: str, params: Mapping[str, Any] | None = None) -> Any:
        with self._engine.connect() as connection:
            return connection.execute(text(query), dict(params or {})).scalar_one()

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> None:
        with self._engine.begin() as connection:
            connection.execute(text(query), dict(params or {}))

    def log_request(
        self,
        *,
        table_name: str,
        request_id: str,
        path: str,
        method: str,
        status_code: int,
        duration_ms: float,
    ) -> None:
        safe_table = self._validate_identifier(table_name)
        if self._request_log_table_available is not True:
            self._request_log_table_available = self.table_exists(safe_table)
        if not self._request_log_table_available:
            return

        query = f"""
        INSERT INTO {safe_table} (request_id, path, method, status_code, duration_ms, created_at)
        VALUES (:request_id, :path, :method, :status_code, :duration_ms, NOW())
        """
        self.execute(
            query,
            {
                "request_id": request_id,
                "path": path,
                "method": method,
                "status_code": status_code,
                "duration_ms": duration_ms,
            },
        )

    def _validate_identifier(self, identifier: str) -> str:
        if not _IDENTIFIER_RE.match(identifier):
            raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
        return identifier
