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
            {
                "check_name": "column_presence",
                "category": "schema",
                "status": "PASS",
                "severity": "INFO",
                "affected_rows": 0,
                "affected_percentage": 0.0,
                "message": "Schema matches.",
            },
            {
                "check_name": "duplicate_rows",
                "category": "uniqueness",
                "status": "WARNING",
                "severity": "MEDIUM",
                "affected_rows": 1,
                "affected_percentage": 33.33,
                "message": "Duplicate found.",
            },
            {
                "check_name": "unit_price_validity",
                "category": "validity",
                "status": "FAIL",
                "severity": "HIGH",
                "affected_rows": 1,
                "affected_percentage": 33.33,
                "message": "Negative price.",
            },
        ],
    }


def test_model_creation(repository):
    run = PipelineRun(
        pipeline_name="test",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        duration_seconds=0.0,
        status="COMPLETED",
        rows_processed=0,
        health_score=100.0,
    )
    repository.session.add(run)
    repository.session.commit()
    assert run.run_id is not None
    assert run.rows_failed == 0
    assert run.error_message is None


def test_create_run_starts_in_running_state(repository):
    run = repository.create_run("test_pipeline")
    assert run.run_id is not None
    assert run.status == "RUNNING"
    assert run.rows_processed == 0
    assert run.rows_failed == 0
    assert run.finished_at is None


def test_complete_run_marks_success_and_computes_duration(repository):
    started = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    run = repository.create_run("test_pipeline", started_at=started)

    finished = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    completed = repository.complete_run(
        run.run_id, rows_processed=100, rows_failed=0, finished_at=finished
    )

    assert completed.status == "SUCCESS"
    assert completed.rows_processed == 100
    assert completed.rows_failed == 0
    assert completed.duration_seconds == 5.0
    assert completed.error_message is None


def test_fail_run_records_error_message(repository):
    started = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    run = repository.create_run("test_pipeline", started_at=started)

    finished = datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc)
    failed = repository.fail_run(
        run.run_id,
        "Extraction failed: source file missing",
        rows_processed=0,
        rows_failed=10,
        finished_at=finished,
    )

    assert failed.status == "FAILED"
    assert failed.rows_failed == 10
    assert failed.error_message == "Extraction failed: source file missing"
    assert failed.duration_seconds == 2.0


def test_save_failed_run_creates_traceable_record_in_one_call(repository):
    started = datetime.now(timezone.utc)
    failed = repository.save_failed_run(
        pipeline_name="online_retail_etl",
        started_at=started,
        error_message="[LOAD] connection refused",
        rows_processed=0,
        rows_failed=50,
    )

    assert failed.run_id is not None
    assert failed.status == "FAILED"
    assert failed.error_message == "[LOAD] connection refused"
    assert failed.rows_failed == 50
    # The failed run must actually be retrievable, not just returned in-memory.
    assert repository.get_latest_run().run_id == failed.run_id


def test_fail_run_missing_run_id_raises(repository):
    with pytest.raises(ValueError):
        repository.fail_run(999999, "no such run")


def test_record_run_results_keeps_run_open_and_stores_health(repository, quality_report):
    """Scoring output attaches to the open run without finishing it."""
    run = repository.create_run("datatrust_daily_pipeline")
    score_report = {
        "score": 72.5,
        "status": "Poor",
        "category_scores": {"schema": {"score": 20.0, "max_score": 20.0}},
    }

    recorded = repository.record_run_results(run.run_id, quality_report, score_report)

    assert recorded.run_id == run.run_id
    assert recorded.status == "RUNNING"
    assert recorded.finished_at is None
    assert recorded.health_score == 72.5
    assert recorded.health_status == "Poor"
    assert recorded.category_scores == {"schema": {"score": 20.0, "max_score": 20.0}}
    # Quality results must still be persisted, exactly as save_run does it.
    assert len(repository.get_quality_results(run.run_id)) == 3


def test_complete_run_after_record_results_retains_health_and_quality(repository, quality_report):
    """Completing an already-scored run must not discard its health or results."""
    run = repository.create_run("datatrust_daily_pipeline")
    repository.record_run_results(
        run.run_id, quality_report, {"score": 88.0, "status": "Good"}
    )

    completed = repository.complete_run(
        run.run_id, rows_processed=3, rows_failed=0, status="COMPLETED"
    )

    assert completed.status == "COMPLETED"
    assert completed.rows_processed == 3
    assert completed.health_score == 88.0
    assert completed.health_status == "Good"
    assert len(repository.get_quality_results(run.run_id)) == 3


def test_complete_run_can_store_health_and_quality_in_one_call(repository, quality_report):
    """A single-shot completion stores the same fields as the two-step path."""
    run = repository.create_run("datatrust_daily_pipeline")

    completed = repository.complete_run(
        run.run_id,
        rows_processed=3,
        status="COMPLETED",
        score_report={"score": 61.5, "status": "Poor"},
        quality_report=quality_report,
    )

    assert completed.status == "COMPLETED"
    assert completed.health_score == 61.5
    assert completed.health_status == "Poor"
    assert len(repository.get_quality_results(run.run_id)) == 3


def test_get_latest_finished_run_ignores_running_rows(repository, quality_report):
    """An in-flight run must never be served as the latest health result."""
    finished = repository.save_run(quality_report, {"score": 91.0}, status="COMPLETED")
    active = repository.create_run("datatrust_daily_pipeline")

    # The newest row is the active one, but health must come from the finished run.
    assert repository.get_latest_run().run_id == active.run_id
    assert repository.get_latest_finished_run().run_id == finished.run_id
    assert repository.get_latest_finished_run().health_score == 91.0


def test_get_latest_finished_run_still_surfaces_failures(repository):
    """A FAILED run has finished; hiding it would misreport a broken pipeline."""
    failed = repository.save_failed_run(
        pipeline_name="datatrust_daily_pipeline",
        started_at=datetime.now(timezone.utc),
        error_message="[quality_validation] boom",
    )
    repository.create_run("datatrust_daily_pipeline")

    assert repository.get_latest_finished_run().run_id == failed.run_id
    assert repository.get_latest_finished_run().status == "FAILED"


def test_get_latest_finished_run_returns_none_when_only_running(repository):
    repository.create_run("datatrust_daily_pipeline")
    assert repository.get_latest_finished_run() is None


def test_save_run_remains_the_standalone_path(repository, quality_report):
    """run_database.py still creates a complete run in one call, unchanged."""
    run = repository.save_run(quality_report, {"score": 72.5, "status": "Poor"})

    assert run.status == "COMPLETED"
    assert run.rows_processed == 3
    assert run.health_score == 72.5
    assert run.health_status == "Poor"
    assert run.finished_at is not None
    assert len(repository.get_quality_results(run.run_id)) == 3
    # A standalone run is finished the moment it is stored.
    assert repository.get_latest_finished_run().run_id == run.run_id


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
    repository.session.add(
        QualityResult(
            run_id=9999,
            check_name="orphan",
            category="test",
            status="FAIL",
            severity="HIGH",
            affected_rows=1,
            affected_percentage=1.0,
            message="Must fail.",
        )
    )
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
