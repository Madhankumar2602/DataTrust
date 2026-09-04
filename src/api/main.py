"""
main.py — FastAPI application entry point for DataTrust.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from src.api.routes import router
from src.config import settings

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

# CORS configuration (permissive for portfolio frontend and local development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    """Redirect root requests directly to interactive Swagger API documentation."""
    return RedirectResponse(url="/docs")
