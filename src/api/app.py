# This file builds the FastAPI application and registers all API routers.
# It exists so startup behavior, middleware, and error handling are configured in one place.
# The app adds request IDs, timing headers, and optional request logging for operations visibility.
# Keeping bootstrap logic centralized makes deployment and testing more predictable.

from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import RequestResponseEndpoint

from src.api.api_config import get_api_config
from src.api.dependencies import get_database_client
from src.api.error_handlers import register_error_handlers
from src.api.routers.diagnostics import router as diagnostics_router
from src.api.routers.forecast import router as forecast_router
from src.api.routers.health import router as health_router
from src.api.routers.metadata import router as metadata_router
from src.api.routers.pricing import router as pricing_router
from src.common.logging import configure_logging

API_HTTP_REQUESTS_TOTAL = Counter(
    "api_http_requests_total",
    "Total number of HTTP requests processed by the API.",
    ["method", "path", "status_code"],
)
API_HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "api_http_request_duration_seconds",
    "API request duration in seconds.",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
API_HTTP_INFLIGHT_REQUESTS = Gauge(
    "api_http_inflight_requests",
    "Number of API requests currently being processed.",
    ["method", "path"],
)


def create_app() -> FastAPI:
    """Create configured FastAPI application instance."""

    configure_logging()
    config = get_api_config()

    app = FastAPI(
        title=config.api_name,
        description=(
            "Versioned API for real-time demand forecasts and dynamic pricing guardrail outputs. "
            "Responses include machine fields, optional plain-language summaries, and schema metadata."
        ),
        version=config.app_version,
        openapi_tags=[
            {"name": "health", "description": "Service liveness, readiness, and version metadata."},
            {"name": "pricing", "description": "Pricing decisions and pricing run summaries."},
            {
                "name": "forecast",
                "description": "Demand forecasts with confidence and run summaries.",
            },
            {
                "name": "metadata",
                "description": "Reference catalogs and schema compatibility metadata.",
            },
            {"name": "diagnostics", "description": "Coverage and guardrail diagnostics snapshots."},
        ],
    )

    if config.allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.middleware("http")
    async def request_context_middleware(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id

        method_label = request.method
        path_label = request.url.path
        started = time.perf_counter()
        status_code = 500
        API_HTTP_INFLIGHT_REQUESTS.labels(method=method_label, path=path_label).inc()
        try:
            response: Response = await call_next(request)
            status_code = response.status_code
            duration_ms = (time.perf_counter() - started) * 1000.0

            response.headers["x-request-id"] = request_id
            response.headers["x-response-time-ms"] = f"{duration_ms:.2f}"

            if config.enable_request_logging:
                try:
                    db = get_database_client()
                    db.log_request(
                        table_name=config.request_log_table_name,
                        request_id=request_id,
                        path=request.url.path,
                        method=request.method,
                        status_code=response.status_code,
                        duration_ms=duration_ms,
                    )
                except Exception:
                    pass

            return response
        finally:
            duration_s = time.perf_counter() - started
            API_HTTP_REQUESTS_TOTAL.labels(
                method=method_label,
                path=path_label,
                status_code=str(status_code),
            ).inc()
            API_HTTP_REQUEST_DURATION_SECONDS.labels(
                method=method_label,
                path=path_label,
            ).observe(duration_s)
            API_HTTP_INFLIGHT_REQUESTS.labels(method=method_label, path=path_label).dec()

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.on_event("startup")
    def startup_checks() -> None:
        try:
            db = get_database_client()
            app.state.db_connected_at_startup = db.can_connect()
        except Exception:
            app.state.db_connected_at_startup = False

    register_error_handlers(app)

    app.include_router(health_router)
    app.include_router(pricing_router, prefix=config.api_version_path)
    app.include_router(forecast_router, prefix=config.api_version_path)
    app.include_router(metadata_router, prefix=config.api_version_path)
    app.include_router(diagnostics_router, prefix=config.api_version_path)

    return app


app = create_app()
