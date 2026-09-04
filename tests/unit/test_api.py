"""
Phase 9 unit tests for FastAPI REST API endpoints.
"""

from __future__ import annotations

from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.api.dependencies import get_db_session
from src.api.main import app
from src.database.models import (
    AnomalyResult,
    Base,
    PipelineRun,
    QualityResult,
)


@pytest.fixture
def test_db_session():
    """Provide an isolated in-memory SQLite session for API testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, _connection_record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    with session_factory() as session:
        yield session

    engine.dispose()


@pytest.fixture
def client(test_db_session):
    """FastAPI TestClient configured with the overridden database session."""
    def override_get_db_session():
        yield test_db_session

    app.dependency_overrides[get_db_session] = override_get_db_session
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_root_redirect_to_docs(client):
    """Root URL should redirect to Swagger /docs."""
    response = client.get("/", follow_redirects=False)
    assert response.status_code in [302, 307]
    assert response.headers["location"] == "/docs"


def test_health_check(client):
    """Health check endpoint should return 200 and healthy status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert data["version"] == "1.0.0"
    assert "timestamp" in data


def test_health_score_not_found_when_empty(client):
    """Health score should return 404 if no runs exist."""
    response = client.get("/api/v1/health-score")
    assert response.status_code == 404
    assert "No pipeline runs found" in response.json()["detail"]


def test_health_score_with_seeded_run(client, test_db_session):
    """Health score should accurately reflect the latest pipeline run."""
    run = PipelineRun(
        pipeline_name="test_pipeline",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        duration_seconds=1.5,
        status="COMPLETED",
        rows_processed=1000,
        health_score=88.5,
    )
    test_db_session.add(run)
    test_db_session.commit()
    test_db_session.refresh(run)

    # Add quality results
    qr1 = QualityResult(
        run_id=run.run_id,
        check_name="schema_check",
        category="schema",
        status="PASS",
        severity="INFO",
        affected_rows=0,
        affected_percentage=0.0,
        message="Schema verified",
    )
    qr2 = QualityResult(
        run_id=run.run_id,
        check_name="null_check",
        category="completeness",
        status="WARNING",
        severity="WARNING",
        affected_rows=5,
        affected_percentage=0.5,
        message="5 null values",
    )
    test_db_session.add_all([qr1, qr2])
    test_db_session.commit()

    response = client.get("/api/v1/health-score")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run.run_id
    assert data["health_score"] == 88.5
    assert data["total_checks"] == 2
    assert data["passed"] == 1
    assert data["warnings"] == 1
    assert data["failed"] == 0


def test_pipeline_runs_list(client, test_db_session):
    """Pipeline runs endpoint should return paginated list."""
    run = PipelineRun(
        pipeline_name="daily_etl",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        duration_seconds=3.2,
        status="SUCCESS",
        rows_processed=541909,
        health_score=75.0,
    )
    test_db_session.add(run)
    test_db_session.commit()

    response = client.get("/api/v1/pipeline-runs?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert data["total_runs"] == 1
    assert len(data["runs"]) == 1
    assert data["runs"][0]["pipeline_name"] == "daily_etl"


def test_quality_results_for_run_success(client, test_db_session):
    """Quality results for a valid run_id should return all associated checks."""
    run = PipelineRun(
        pipeline_name="retail_etl",
        started_at=datetime.now(timezone.utc),
        duration_seconds=1.8,
        status="COMPLETED",
        rows_processed=1000,
        health_score=90.0,
    )
    test_db_session.add(run)
    test_db_session.commit()
    test_db_session.refresh(run)

    qr = QualityResult(
        run_id=run.run_id,
        check_name="unit_price_check",
        category="validity",
        status="FAIL",
        severity="CRITICAL",
        affected_rows=2,
        affected_percentage=0.01,
        message="Negative price found",
    )
    test_db_session.add(qr)
    test_db_session.commit()

    response = client.get(f"/api/v1/quality-results/{run.run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run.run_id
    assert data["total_results"] == 1
    assert data["failed"] == 1
    assert data["results"][0]["check_name"] == "unit_price_check"


def test_quality_results_for_run_not_found(client):
    """Quality results for a nonexistent run_id should return 404."""
    response = client.get("/api/v1/quality-results/99999")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_anomalies_endpoint(client, test_db_session):
    """Anomalies endpoint should return persisted anomalies."""
    anomaly = AnomalyResult(
        metric="revenue",
        period="2011-11",
        value=1461756.25,
        expected_value=749826.76,
        deviation_pct=94.95,
        severity="WARNING",
        message="Revenue spike",
    )
    test_db_session.add(anomaly)
    test_db_session.commit()

    response = client.get("/api/v1/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert data["total_anomalies"] == 1
    assert data["anomalies"][0]["metric"] == "revenue"
    assert data["anomalies"][0]["period"] == "2011-11"
    assert data["anomalies"][0]["severity"] == "WARNING"


def test_summary_endpoint(client, test_db_session):
    """Summary endpoint should return compact overview."""
    run = PipelineRun(
        pipeline_name="production_pipeline",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        duration_seconds=5.0,
        status="OPERATIONAL",
        rows_processed=541909,
        health_score=85.0,
    )
    test_db_session.add(run)
    test_db_session.commit()

    response = client.get("/api/v1/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "DataTrust"
    assert data["status"] == "OPERATIONAL"
    assert data["latest_health_score"] == 85.0
    assert data["total_rows_processed"] == 541909
