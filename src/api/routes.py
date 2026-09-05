"""
routes.py — REST API endpoint definitions for DataTrust.
"""

from __future__ import annotations

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db_session, get_repository
from src.api.schemas import (
    AnomaliesResponse,
    AnomalyItem,
    APIHealthResponse,
    HealthScoreResponse,
    PipelineRunItem,
    PipelineRunsListResponse,
    QualityResultItem,
    QualityResultsResponse,
    SummaryResponse,
)
from src.config import settings
from src.database.repository import QualityRepository
from src.logger import get_logger

logger = get_logger("api.routes")
router = APIRouter()


@router.get(
    "/health",
    response_model=APIHealthResponse,
    summary="Service Health Check",
    tags=["System"],
)
def check_health(session: Session = Depends(get_db_session)) -> APIHealthResponse:
    """Check API liveness and database connectivity."""
    db_status = "connected"
    api_status = "healthy"
    try:
        session.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning(f"Database health check failed: {exc}")
        db_status = "disconnected"
        api_status = "degraded"

    return APIHealthResponse(
        status=api_status,
        database=db_status,
        environment=settings.APP_ENV,
        version="1.0.0",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get(
    "/api/v1/health-score",
    response_model=HealthScoreResponse,
    summary="Get Latest Data Health Score",
    tags=["Health Score"],
)
def get_latest_health_score(
    repo: QualityRepository = Depends(get_repository),
) -> HealthScoreResponse:
    """Retrieve the most recent computed Data Health Score and validation breakdown."""
    latest_run = repo.get_latest_run()
    if not latest_run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No pipeline runs found. Please execute the pipeline first.",
        )

    results = latest_run.quality_results
    passed = sum(1 for r in results if r.status == "PASS")
    warnings = sum(1 for r in results if r.status == "WARNING")
    failed = sum(1 for r in results if r.status == "FAIL")

    return HealthScoreResponse(
        run_id=latest_run.run_id,
        pipeline_name=latest_run.pipeline_name,
        health_score=float(latest_run.health_score or 0.0),
        # Tier and breakdown are read back exactly as HealthScorer computed them
        # at run time; the API never recalculates a score.
        health_status=latest_run.health_status,
        pipeline_status=latest_run.status,
        total_checks=len(results),
        passed=passed,
        warnings=warnings,
        failed=failed,
        category_scores=latest_run.category_scores,
        evaluated_at=(
            latest_run.started_at.isoformat()
            if latest_run.started_at
            else datetime.now(timezone.utc).isoformat()
        ),
    )


@router.get(
    "/api/v1/pipeline-runs",
    response_model=PipelineRunsListResponse,
    summary="List Pipeline Runs",
    tags=["Pipeline Runs"],
)
def list_pipeline_runs(
    limit: int = Query(default=20, ge=1, le=100, description="Maximum runs to return"),
    offset: int = Query(default=0, ge=0, description="Runs to skip, newest first"),
    repo: QualityRepository = Depends(get_repository),
) -> PipelineRunsListResponse:
    """Retrieve a historical page of DataTrust pipeline execution runs."""
    runs = repo.get_runs(limit=limit, offset=offset)
    items = [
        PipelineRunItem(
            run_id=r.run_id,
            pipeline_name=r.pipeline_name,
            started_at=r.started_at.isoformat() if r.started_at else "",
            finished_at=r.finished_at.isoformat() if r.finished_at else None,
            duration_seconds=r.duration_seconds,
            status=r.status,
            rows_processed=r.rows_processed,
            rows_failed=r.rows_failed,
            error_message=r.error_message,
            health_score=r.health_score,
            health_status=r.health_status,
        )
        for r in runs
    ]
    return PipelineRunsListResponse(
        total_runs=repo.count_runs(),
        returned=len(items),
        limit=limit,
        offset=offset,
        runs=items,
    )


@router.get(
    "/api/v1/quality-results/{run_id}",
    response_model=QualityResultsResponse,
    summary="Get Quality Results for a Run",
    tags=["Quality Validation"],
)
def get_quality_results_for_run(
    run_id: int,
    repo: QualityRepository = Depends(get_repository),
) -> QualityResultsResponse:
    """Retrieve detailed validation check findings and affected row counts for a specific run ID."""
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline run #{run_id} not found.",
        )

    results = repo.get_quality_results(run_id=run_id)
    items = [
        QualityResultItem(
            result_id=r.result_id,
            run_id=r.run_id,
            check_name=r.check_name,
            category=r.category,
            status=r.status,
            severity=r.severity,
            affected_rows=r.affected_rows,
            affected_percentage=r.affected_percentage,
            message=r.message,
            created_at=r.created_at.isoformat() if r.created_at else "",
        )
        for r in results
    ]

    passed = sum(1 for r in items if r.status == "PASS")
    warnings = sum(1 for r in items if r.status == "WARNING")
    failed = sum(1 for r in items if r.status == "FAIL")

    return QualityResultsResponse(
        run_id=run_id,
        total_results=len(items),
        passed=passed,
        warnings=warnings,
        failed=failed,
        results=items,
    )


@router.get(
    "/api/v1/anomalies",
    response_model=AnomaliesResponse,
    summary="List Detected Anomalies",
    tags=["Anomaly Detection"],
)
def list_anomalies(
    limit: int = Query(default=50, ge=1, le=200, description="Maximum anomalies to return"),
    offset: int = Query(default=0, ge=0, description="Anomalies to skip, newest first"),
    repo: QualityRepository = Depends(get_repository),
) -> AnomaliesResponse:
    """Retrieve statistical time-series anomalies across revenue, volume, and cancellations."""
    anomalies = repo.get_anomalies(limit=limit, offset=offset)
    items = [
        AnomalyItem(
            anomaly_id=a.anomaly_id,
            metric=a.metric,
            period=a.period,
            value=a.value,
            expected_value=a.expected_value,
            deviation_pct=a.deviation_pct,
            severity=a.severity,
            message=a.message,
            detected_at=a.detected_at.isoformat() if a.detected_at else "",
        )
        for a in anomalies
    ]
    return AnomaliesResponse(
        total_anomalies=repo.count_anomalies(),
        returned=len(items),
        limit=limit,
        offset=offset,
        anomalies=items,
    )


@router.get(
    "/api/v1/summary",
    response_model=SummaryResponse,
    summary="Executive Summary",
    tags=["System"],
)
def get_summary(
    repo: QualityRepository = Depends(get_repository),
) -> SummaryResponse:
    """Retrieve an executive summary of current data pipeline health, validation, and anomalies."""
    latest_run = repo.get_latest_run()
    # Real stored total, not the size of a capped page.
    anomaly_total = repo.count_anomalies()

    if not latest_run:
        return SummaryResponse(
            status="INITIALIZING",
            latest_run_id=None,
            latest_health_score=None,
            health_status=None,
            pipeline_status=None,
            total_rows_processed=0,
            quality_failures=0,
            quality_warnings=0,
            anomaly_count=anomaly_total,
            last_run_timestamp=None,
        )

    results = latest_run.quality_results
    failures = sum(1 for r in results if r.status == "FAIL")
    warnings = sum(1 for r in results if r.status == "WARNING")

    return SummaryResponse(
        status="OPERATIONAL",
        latest_run_id=latest_run.run_id,
        latest_health_score=float(latest_run.health_score or 0.0),
        # Health tier, NOT the execution status - these are different concepts.
        health_status=latest_run.health_status,
        pipeline_status=latest_run.status,
        total_rows_processed=latest_run.rows_processed,
        quality_failures=failures,
        quality_warnings=warnings,
        anomaly_count=anomaly_total,
        last_run_timestamp=latest_run.finished_at.isoformat() if latest_run.finished_at else (
            latest_run.started_at.isoformat() if latest_run.started_at else None
        ),
    )
