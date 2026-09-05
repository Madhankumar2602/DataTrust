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
    """Latest Data Health Score with its explainable per-dimension breakdown.

    `health_status` and `pipeline_status` are deliberately separate fields:
    a run can execute perfectly (pipeline_status="SUCCESS") and still describe
    data in poor condition (health_status="Poor").
    """

    model_config = ConfigDict(from_attributes=True)

    run_id: int
    pipeline_name: str
    health_score: float = Field(..., description="Weighted Data Health Score, 0-100")
    health_status: str | None = Field(
        default=None,
        description="Health tier from HealthScorer: Healthy / Warning / Poor / Critical",
    )
    pipeline_status: str = Field(..., description="Pipeline execution result, e.g. SUCCESS")
    total_checks: int
    passed: int
    warnings: int
    failed: int
    category_scores: dict[str, Any] | None = Field(
        default=None,
        description="Per-dimension score breakdown exactly as HealthScorer produced it",
    )
    evaluated_at: str


class PipelineRunItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: int
    pipeline_name: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    status: str = Field(..., description="Pipeline execution result, e.g. SUCCESS")
    rows_processed: int | None = None
    rows_failed: int | None = Field(default=0, description="Rows that failed to process")
    error_message: str | None = Field(
        default=None, description="Failure reason, populated only when the run failed"
    )
    health_score: float | None = None
    health_status: str | None = Field(default=None, description="Health tier for this run")


class PipelineRunsListResponse(BaseModel):
    """One page of pipeline runs. `total_runs` is the full stored count."""

    total_runs: int = Field(..., description="Total runs stored, independent of pagination")
    returned: int = Field(..., description="Runs in this page")
    limit: int
    offset: int
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
    """One page of anomalies. `total_anomalies` is the full stored count."""

    total_anomalies: int = Field(
        ..., description="Total anomalies stored, independent of pagination"
    )
    returned: int = Field(..., description="Anomalies in this page")
    limit: int
    offset: int
    anomalies: list[AnomalyItem]


class SummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    service: str = "DataTrust"
    status: str = Field(..., description="Service state: OPERATIONAL or INITIALIZING")
    latest_run_id: int | None = None
    latest_health_score: float | None = None
    health_status: str | None = Field(
        default=None,
        description="Health tier of the latest run: Healthy / Warning / Poor / Critical",
    )
    pipeline_status: str | None = Field(
        default=None, description="Execution result of the latest run, e.g. SUCCESS"
    )
    total_rows_processed: int | None = None
    quality_failures: int = Field(default=0, description="FAIL checks in the latest run")
    quality_warnings: int = Field(default=0, description="WARNING checks in the latest run")
    anomaly_count: int = Field(default=0, description="Total anomalies stored across all runs")
    last_run_timestamp: str | None = None
