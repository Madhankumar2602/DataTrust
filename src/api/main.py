"""
main.py — FastAPI application entry point for DataTrust.
"""

from __future__ import annotations

import time

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.exc import SQLAlchemyError

from src.api.routes import router
from src.config import settings
from src.logger import get_logger

logger = get_logger("api")

app = FastAPI(
    title="DataTrust API",
    description=(
        "DataTrust Automated Data Quality & Pipeline Observability Platform REST API. "
        "Provides endpoints for querying Data Health Scores, validation reports, "
        "pipeline audit runs, and statistical time-series anomalies."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS is configuration-driven: CORS_ALLOW_ORIGINS names the origins allowed to
# call this API, as a comma-separated list. Left unset it stays open, which
# suits local development and a public read-only API; a deployment names its
# dashboard origin instead. Credentials are only enabled for explicit origins,
# because a "*" wildcard combined with credentials is rejected by browsers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log method, path, status and duration for every request.

    Deliberately minimal: no bodies, no headers, no query strings, so
    credentials and connection strings can never reach the log file.
    """
    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "%s %s -> %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.exception_handler(SQLAlchemyError)
async def handle_database_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    """Return 503 for any unhandled database failure.

    The driver's message is logged server-side but never returned: it can
    carry table names, SQL fragments and connection details.
    """
    logger.error("Database error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Database unavailable. Please try again later."},
    )


# Include API routes
app.include_router(router)


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    """Redirect root requests directly to interactive Swagger API documentation."""
    return RedirectResponse(url="/docs")
