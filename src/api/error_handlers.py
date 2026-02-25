# This file defines consistent API error payloads and exception handlers.
# It exists so every endpoint returns the same error shape with request trace fields.
# The handlers translate validation, HTTP, and unexpected failures into safe client messages.
# Centralized error handling prevents stack traces from leaking in production responses.

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class APIError(Exception):
    """Domain error type with structured API details."""

    def __init__(
        self,
        *,
        status_code: int,
        error_code: str,
        message: str,
        details: Any | None = None,
    ) -> None:
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.details = details
        super().__init__(message)


def _request_id(request: Request) -> str:
    return str(getattr(request.state, "request_id", "unknown"))


def _error_body(
    *, request: Request, error_code: str, message: str, details: Any | None = None
) -> dict[str, Any]:
    return {
        "error_code": error_code,
        "message": message,
        "details": details,
        "request_id": _request_id(request),
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }


def register_error_handlers(app: FastAPI) -> None:
    """Register global exception handlers."""

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(
                request=request,
                error_code=exc.error_code,
                message=exc.message,
                details=exc.details,
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=_error_body(
                request=request,
                error_code="VALIDATION_ERROR",
                message="Invalid request parameters.",
                details=exc.errors(),
            ),
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(
                request=request,
                error_code="HTTP_ERROR",
                message=str(exc.detail),
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, _: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=_error_body(
                request=request,
                error_code="INTERNAL_SERVER_ERROR",
                message="The server encountered an unexpected error.",
            ),
        )
