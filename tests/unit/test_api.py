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


@pytest.fixture
def broken_db_client():
    """TestClient wired to a database that cannot be opened at all.

    The URL points into a directory that does not exist, so every connection
    attempt raises a genuine SQLAlchemyError. That exercises the API's
    database-failure path without mocking the repository or the session.
    """
    engine = create_engine(
        "sqlite:///./__datatrust_missing_dir__/unreachable.db",
        connect_args={"check_same_thread": False},
    )
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    def override_get_db_session():
        with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def _seed_run(session, **overrides):
    """Insert one pipeline run, defaulting every required column."""
    now = datetime.now(timezone.utc)
    values = {
        "pipeline_name": "test_pipeline",
        "started_at": now,
        "finished_at": now,
        "duration_seconds": 1.0,
        "status": "SUCCESS",
        "rows_processed": 100,
        "health_score": 72.08,
    }
    values.update(overrides)
    run = PipelineRun(**values)
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


CATEGORY_SCORES = {
    "completeness": {"score": 18.75, "max_score": 20.0, "details": "7.5/8 rule points earned."},
    "validity": {"score": 13.33, "max_score": 20.0, "details": "2.0/3 rule points earned."},
    "uniqueness": {"score": 10.0, "max_score": 20.0, "details": "0.5/1 rule points earned."},
    "schema": {"score": 20.0, "max_score": 20.0, "details": "3.0/3 rule points earned."},
    "business_rules": {"score": 10.0, "max_score": 20.0, "details": "0.5/1 rule points earned."},
}


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
    # New metadata fields must be present and default sanely for older-style rows.
    assert data["runs"][0]["rows_failed"] == 0
    assert data["runs"][0]["error_message"] is None


def test_pipeline_runs_list_exposes_failure_metadata(client, test_db_session):
    """A FAILED run's rows_failed and error_message must reach the API."""
    _seed_run(
        test_db_session,
        pipeline_name="daily_etl",
        status="FAILED",
        rows_processed=0,
        rows_failed=25,
        error_message="[LOAD] connection refused",
    )

    response = client.get("/api/v1/pipeline-runs?limit=10")
    assert response.status_code == 200
    run_item = response.json()["runs"][0]
    assert run_item["status"] == "FAILED"
    assert run_item["rows_failed"] == 25
    assert run_item["error_message"] == "[LOAD] connection refused"


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


# ---------------------------------------------------------------------------
# Phase 9 / M2: health tier, explainability, pagination, failure handling
# ---------------------------------------------------------------------------


def test_health_score_returns_health_tier_not_pipeline_status(client, test_db_session):
    """health_status must be the HealthScorer tier; pipeline_status the run result."""
    _seed_run(test_db_session, status="SUCCESS", health_status="Poor", health_score=72.08)

    data = client.get("/api/v1/health-score").json()

    assert data["health_status"] == "Poor"
    assert data["pipeline_status"] == "SUCCESS"
    assert data["health_status"] != data["pipeline_status"]


def test_health_score_returns_category_breakdown(client, test_db_session):
    """The stored per-dimension breakdown must reach the API unchanged."""
    _seed_run(test_db_session, health_status="Poor", category_scores=CATEGORY_SCORES)

    data = client.get("/api/v1/health-score").json()

    assert data["category_scores"] is not None
    assert set(data["category_scores"]) == {
        "completeness",
        "validity",
        "uniqueness",
        "schema",
        "business_rules",
    }
    assert data["category_scores"]["schema"]["score"] == 20.0
    assert data["category_scores"]["validity"]["max_score"] == 20.0


def test_health_score_tolerates_run_stored_before_this_feature(client, test_db_session):
    """Runs persisted before the tier columns existed must still return 200."""
    _seed_run(test_db_session, health_status=None, category_scores=None)

    response = client.get("/api/v1/health-score")

    assert response.status_code == 200
    assert response.json()["health_status"] is None
    assert response.json()["category_scores"] is None


def test_summary_reports_health_tier_and_pipeline_status_separately(client, test_db_session):
    """Summary must not report the execution status where the tier belongs."""
    _seed_run(test_db_session, status="SUCCESS", health_status="Warning", health_score=80.0)

    data = client.get("/api/v1/summary").json()

    assert data["status"] == "OPERATIONAL"          # service state
    assert data["health_status"] == "Warning"       # data health tier
    assert data["pipeline_status"] == "SUCCESS"     # execution result


def test_pipeline_runs_pagination(client, test_db_session):
    """limit/offset must page newest-first while total_runs stays the full count."""
    for name in ("run_one", "run_two", "run_three"):
        _seed_run(test_db_session, pipeline_name=name)

    data = client.get("/api/v1/pipeline-runs?limit=2&offset=1").json()

    assert data["total_runs"] == 3      # true stored total, not the page size
    assert data["returned"] == 2
    assert data["limit"] == 2
    assert data["offset"] == 1
    assert [r["pipeline_name"] for r in data["runs"]] == ["run_two", "run_one"]


def test_pipeline_runs_total_is_not_the_page_size(client, test_db_session):
    """A page smaller than the collection must still report the real total."""
    for _ in range(3):
        _seed_run(test_db_session)

    data = client.get("/api/v1/pipeline-runs?limit=1").json()

    assert data["total_runs"] == 3
    assert data["returned"] == 1


def test_anomalies_pagination_and_total(client, test_db_session):
    """Anomaly totals must count every stored row, not the returned page."""
    for period in ("2011-09", "2011-10", "2011-11"):
        test_db_session.add(
            AnomalyResult(
                metric="revenue",
                period=period,
                value=100.0,
                expected_value=50.0,
                deviation_pct=100.0,
                severity="WARNING",
                message=f"spike in {period}",
            )
        )
    test_db_session.commit()

    data = client.get("/api/v1/anomalies?limit=2&offset=1").json()

    assert data["total_anomalies"] == 3
    assert data["returned"] == 2
    assert data["offset"] == 1
    assert [a["period"] for a in data["anomalies"]] == ["2011-10", "2011-09"]


def test_pagination_rejects_invalid_bounds(client):
    """Out-of-range pagination is refused by validation, not by the database."""
    assert client.get("/api/v1/pipeline-runs?limit=0").status_code == 422
    assert client.get("/api/v1/pipeline-runs?offset=-1").status_code == 422
    assert client.get("/api/v1/anomalies?limit=500").status_code == 422


def test_database_failure_returns_503(broken_db_client):
    """A database error must surface as 503, never as an unhandled 500."""
    paths = (
        "/api/v1/health-score",
        "/api/v1/pipeline-runs",
        "/api/v1/anomalies",
        "/api/v1/summary",
    )
    for path in paths:
        response = broken_db_client.get(path)
        assert response.status_code == 503, path
        assert response.json()["detail"] == "Database unavailable. Please try again later."


def test_database_failure_does_not_leak_internals(broken_db_client):
    """The client must never see SQL, table names or driver text."""
    detail = broken_db_client.get("/api/v1/pipeline-runs").json()["detail"].lower()

    for leak in ("unable to open", "sql", "sqlite", "traceback", "select"):
        assert leak not in detail


def test_health_endpoint_reports_degraded_when_database_is_down(broken_db_client):
    """/health already handles failure itself and must keep doing so."""
    data = broken_db_client.get("/health").json()

    assert data["status"] == "degraded"
    assert data["database"] == "disconnected"
