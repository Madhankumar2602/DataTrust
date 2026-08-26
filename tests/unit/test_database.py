"""Isolated Phase 4 database tests using in-memory SQLite, never PostgreSQL."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base, PipelineRun, QualityResult
from src.database.repository import QualityRepository


@pytest.fixture
def repository():
    """Create a fresh database for every test without using user credentials."""
    engine = create_database_engine("sqlite+pysqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, _connection_record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    session = create_session_factory(engine)()
    yield QualityRepository(session)
    session.close()
    engine.dispose()


@pytest.fixture
def quality_report():
    return {
        "dataset_name": "Test Dataset",
        "validated_at": "2026-08-25T12:00:00+00:00",
        "total_rows": 3,
        "results": [
            {"check_name": "column_presence", "category": "schema", "status": "PASS", "severity": "INFO", "affected_rows": 0, "affected_percentage": 0.0, "message": "Schema matches."},
            {"check_name": "duplicate_rows", "category": "uniqueness", "status": "WARNING", "severity": "MEDIUM", "affected_rows": 1, "affected_percentage": 33.33, "message": "Duplicate found."},
            {"check_name": "unit_price_validity", "category": "validity", "status": "FAIL", "severity": "HIGH", "affected_rows": 1, "affected_percentage": 33.33, "message": "Negative price."},
        ],
    }


def test_model_creation(repository):
    run = PipelineRun(pipeline_name="test", started_at=datetime.now(timezone.utc), finished_at=datetime.now(timezone.utc), duration_seconds=0.0, status="COMPLETED", rows_processed=0, health_score=100.0)
    repository.session.add(run)
    repository.session.commit()
    assert run.run_id is not None


def test_save_and_retrieve_run_and_results(repository, quality_report):
    run = repository.save_run(quality_report, {"score": 72.5})
    assert run.health_score == 72.5
    assert run.rows_processed == 3
    assert len(repository.get_quality_results(run.run_id)) == 3
    assert repository.get_latest_run().run_id == run.run_id


def test_relationship_and_foreign_key(repository, quality_report):
    run = repository.save_run(quality_report, {"score": 72.5})
    assert len(run.quality_results) == 3
    assert run.quality_results[0].pipeline_run.run_id == run.run_id
    repository.session.add(QualityResult(run_id=9999, check_name="orphan", category="test", status="FAIL", severity="HIGH", affected_rows=1, affected_percentage=1.0, message="Must fail."))
    with pytest.raises(IntegrityError):
        repository.session.commit()
    repository.session.rollback()


def test_historical_scores_and_status_queries(repository, quality_report):
    first = repository.save_run(quality_report, {"score": 72.5})
    second = repository.save_run(quality_report, {"score": 88.0})
    assert [run.run_id for run in repository.get_recent_runs()] == [second.run_id, first.run_id]
    assert [run.health_score for run in repository.get_health_score_history()] == [72.5, 88.0]
    assert len(repository.get_failed_checks(first.run_id)) == 1
    assert len(repository.get_warning_checks(first.run_id)) == 1
