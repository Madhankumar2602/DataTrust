"""
schemas.py — Pydantic response models for DataTrust REST API.
"""

from __future__ import annotations

from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class APIHealthResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str = Field(..., description="API operational status (healthy/degraded)")
    database: str = Field(..., description="Database connection status (connected/disconnected)")
    environment: str = Field(..., description="Application environment")
    version: str = Field(default="1.0.0", description="DataTrust API version")
    timestamp: str = Field(..., description="UTC ISO timestamp of the health check")


class CategoryScoreDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    score: float
    max_score: float
    details: str | None = None


class HealthScoreResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: int
    pipeline_name: str
    health_score: float
    status: str
    total_checks: int
    passed: int
    warnings: int
    failed: int
    category_scores: dict[str, Any] | None = None
    evaluated_at: str


class PipelineRunItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: int
    pipeline_name: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    status: str
    rows_processed: int | None = None
    health_score: float | None = None


class PipelineRunsListResponse(BaseModel):
    total_runs: int
    runs: list[PipelineRunItem]


class QualityResultItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    result_id: int
    run_id: int
    check_name: str
    category: str
    status: str
    severity: str
    affected_rows: int
    affected_percentage: float
    message: str
    created_at: str


class QualityResultsResponse(BaseModel):
    run_id: int
    total_results: int
    passed: int
    warnings: int
    failed: int
    results: list[QualityResultItem]


class AnomalyItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    anomaly_id: int
    metric: str
    period: str
    value: float
    expected_value: float
    deviation_pct: float
    severity: str
    message: str
    detected_at: str


class AnomaliesResponse(BaseModel):
    total_anomalies: int
    anomalies: list[AnomalyItem]


class SummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    service: str = "DataTrust"
    status: str
    latest_run_id: int | None = None
    latest_health_score: float | None = None
    health_status: str | None = None
    total_rows_processed: int | None = None
    quality_failures: int = 0
    quality_warnings: int = 0
    anomaly_count: int = 0
    last_run_timestamp: str | None = None
