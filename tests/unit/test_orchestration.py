"""
Phase 7 unit tests for Pipeline Orchestration and Airflow DAG definition.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import selectinload, sessionmaker
from sqlalchemy.pool import StaticPool

from dags import datatrust_pipeline
from dags.datatrust_pipeline import (
    DEFAULT_ARGS,
    close_pipeline_run,
    handle_task_failure,
    open_pipeline_run,
    run_anomaly_detection,
    run_extract_transform_load,
    run_health_scoring_and_persistence,
    run_quality_validation,
)
from src.database.models import Base, PipelineRun, RetailTransaction
from src.etl.pipeline import ETLPipelineResult


def make_task_instance(**xcom: object) -> MagicMock:
    """Build a task instance whose xcom_pull answers per task_id, as Airflow does."""
    task_instance = MagicMock()
    task_instance.xcom_pull.side_effect = lambda task_ids=None, **_: xcom.get(task_ids)
    return task_instance


@pytest.fixture
def mock_db():
    """Provide an isolated in-memory SQLite database for testing orchestration tasks."""
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

    yield engine, session_factory

    engine.dispose()


@pytest.fixture
def lifecycle_db(mock_db):
    """Point the run-lifecycle service at the isolated in-memory database."""
    engine, session_factory = mock_db
    with patch("src.pipeline.run_lifecycle.create_database_engine", return_value=engine), \
         patch("src.pipeline.run_lifecycle.create_session_factory", return_value=session_factory):
        yield engine, session_factory


def stored_runs(session_factory) -> list[PipelineRun]:
    """Every stored run, with quality results eagerly loaded for use after close."""
    with session_factory() as session:
        return list(
            session.query(PipelineRun)
            .options(selectinload(PipelineRun.quality_results))
            .order_by(PipelineRun.run_id)
            .all()
        )


def test_dag_default_args_configuration():
    """Verify DAG default arguments meet production reliability requirements."""
    assert DEFAULT_ARGS["owner"] == "datatrust"
    assert DEFAULT_ARGS["depends_on_past"] is False
    assert DEFAULT_ARGS["retries"] == 2
    assert DEFAULT_ARGS["retry_delay"].total_seconds() == 300  # 5 minutes
    assert DEFAULT_ARGS["email_on_failure"] is False


def test_dag_callables_structure():
    """Verify all 4 core pipeline stage callables are defined and callable."""
    callables = [
        run_extract_transform_load,
        run_quality_validation,
        run_health_scoring_and_persistence,
        run_anomaly_detection,
    ]
    for func in callables:
        assert callable(func), f"{func.__name__} must be a valid Python callable"


def test_orchestration_tasks_flow_isolated(mock_db, lifecycle_db):
    """
    Test the sequential execution of all pipeline stages using an in-memory database,
    from the run being opened through to it being COMPLETED.
    """
    engine, session_factory = mock_db

    sample_df = pd.DataFrame(
        {
            "InvoiceNo": ["536365", "536366", "C536367"],
            "StockCode": ["85123A", "71053", "84029G"],
            "Description": ["WHITE HANGING HEART", "WHITE METAL LANTERN", "KNITTED UNION FLAG"],
            "Quantity": [6, 2, -1],
            "InvoiceDate": ["2010-12-01 08:26:00", "2010-12-01 08:28:00", "2010-12-01 08:34:00"],
            "UnitPrice": [2.55, 3.39, 4.25],
            "CustomerID": [17850.0, 17850.0, 17850.0],
            "Country": ["United Kingdom", "United Kingdom", "United Kingdom"],
        }
    )

    with patch("src.etl.pipeline.extract_data") as mock_extract, \
         patch("src.etl.pipeline.create_database_engine", return_value=engine), \
         patch("src.etl.pipeline.create_session_factory", return_value=session_factory), \
         patch("dags.datatrust_pipeline.create_database_engine", return_value=engine), \
         patch("dags.datatrust_pipeline.create_session_factory", return_value=session_factory):

        mock_extract.return_value = MagicMock(
            dataframe=sample_df,
            rows_extracted=len(sample_df),
        )

        # Stage 0: open the single run that represents this DAG execution
        run_id = open_pipeline_run()
        assert isinstance(run_id, int)
        with session_factory() as session:
            opened = session.get(PipelineRun, run_id)
            assert opened.status == "RUNNING"
            assert opened.rows_processed == 0
            assert opened.rows_failed == 0

        # Stage 1: ETL, executed by the reusable pipeline in src/etl/pipeline.py
        etl_result = run_extract_transform_load()
        assert etl_result["status"] == "SUCCESS"
        assert etl_result["rows_loaded"] == 3

        # Verify records exist in DB
        with session_factory() as session:
            count = session.query(RetailTransaction).count()
            assert count == 3

        # Stage 2: Quality Validation
        quality_result = run_quality_validation()
        assert "summary" in quality_result
        assert quality_result["total_rows"] == 3
        assert quality_result["summary"]["total_checks"] > 0

        # This stage reads the stored snapshot back out of the database, where
        # completeness and validity used to match source column names literally:
        # validity found no columns and returned nothing, so the validity and
        # business_rules dimensions could never be earned on a scheduled run.
        categories = [result["category"] for result in quality_result["results"]]
        assert "completeness" in categories
        assert "validity" in categories
        validity_checks = {
            result["check_name"]
            for result in quality_result["results"]
            if result["category"] == "validity"
        }
        assert "unit_price_validity" in validity_checks
        assert "quantity_cancellation_logic" in validity_checks

        # Stage 3: Health Scoring, recorded against the run opened at stage 0
        task_instance = make_task_instance(
            start_pipeline_run=run_id,
            quality_validation=quality_result,
            extract_transform_load=etl_result,
        )
        scoring_result = run_health_scoring_and_persistence(ti=task_instance)

        # Regression: the stored snapshot read back from the database used to be
        # validated against the source column names, which reported every column
        # missing and forced this score to 0.0/Critical on every scheduled run.
        assert scoring_result["health_score"] > 0.0
        assert scoring_result["status"] in ["Healthy", "Warning", "Poor", "Critical"]
        # The same run is updated, never a second one.
        assert scoring_result["run_id"] == run_id

        with session_factory() as session:
            run = session.get(PipelineRun, run_id)
            assert run.health_score == scoring_result["health_score"]
            assert len(run.quality_results) > 0
            # The persisted results must include the dimensions that the stored
            # representation previously skipped entirely.
            persisted = {result.category for result in run.quality_results}
            assert {"completeness", "validity"} <= persisted
            # Still open: the run is only completed after the last stage.
            assert run.status == "RUNNING"

        # Stage 4: Anomaly Detection
        anomaly_result = run_anomaly_detection()
        assert "anomalies_detected" in anomaly_result
        assert "anomalies_persisted" in anomaly_result

        # Stage 5: close the run
        completion = close_pipeline_run(ti=task_instance)
        assert completion == {"run_id": run_id, "status": "COMPLETED"}

    runs = stored_runs(session_factory)
    assert len(runs) == 1, "one DAG execution must produce exactly one pipeline run"
    final = runs[0]
    assert final.status == "COMPLETED"
    assert final.rows_processed == 3
    assert final.rows_failed == 0
    assert final.finished_at is not None
    assert final.duration_seconds >= 0.0
    # Health and quality output recorded at stage 3 survives completion.
    assert final.health_score == scoring_result["health_score"]
    assert final.health_status == scoring_result["status"]
    assert final.category_scores == scoring_result["category_scores"]
    assert len(final.quality_results) > 0


def test_etl_failure_bubbles_up(mock_db):
    """An ETL failure must reach Airflow unchanged so retries behave normally.

    Recording the FAILED state is the DAG's failure callback's job, verified in
    `test_orchestrated_etl_does_not_record_its_own_run` and the callback tests.
    """
    engine, session_factory = mock_db

    missing_source = FileNotFoundError("Source CSV missing")

    with patch("src.etl.pipeline.extract_data", side_effect=missing_source), \
         patch("src.etl.pipeline.create_database_engine", return_value=engine), \
         patch("src.etl.pipeline.create_session_factory", return_value=session_factory):

        with pytest.raises(FileNotFoundError):
            run_extract_transform_load()


def test_failure_callback_marks_the_same_run_failed(lifecycle_db):
    """Any ultimately-failed task updates the one run, it never inserts another."""
    _, session_factory = lifecycle_db
    run_id = open_pipeline_run()

    handle_task_failure(
        {
            "ti": make_task_instance(start_pipeline_run=run_id),
            "exception": RuntimeError("scoring exploded"),
        }
    )

    runs = stored_runs(session_factory)
    assert len(runs) == 1
    assert runs[0].run_id == run_id
    assert runs[0].status == "FAILED"
    assert "scoring exploded" in runs[0].error_message
    assert runs[0].finished_at is not None
    assert runs[0].duration_seconds >= 0.0


@pytest.mark.parametrize(
    "failing_task",
    ["extract_transform_load", "quality_validation",
     "health_scoring_and_persistence", "anomaly_detection"],
)
def test_any_stage_failure_marks_the_same_run_failed(lifecycle_db, failing_task):
    """ETL, quality, scoring and anomaly failures all land on the same run."""
    _, session_factory = lifecycle_db
    run_id = open_pipeline_run()

    task_instance = make_task_instance(start_pipeline_run=run_id)
    task_instance.task_id = failing_task
    handle_task_failure(
        {"ti": task_instance, "exception": RuntimeError(f"{failing_task} exploded")}
    )

    runs = stored_runs(session_factory)
    assert len(runs) == 1
    assert runs[0].status == "FAILED"
    assert failing_task in runs[0].error_message


def test_retry_does_not_create_duplicate_runs(lifecycle_db):
    """Airflow retries reuse the run opened once for the DAG execution."""
    _, session_factory = lifecycle_db
    run_id = open_pipeline_run()
    task_instance = make_task_instance(start_pipeline_run=run_id)

    # Two retried attempts of the same task, then a successful completion. The
    # callback fires only after retries are exhausted, so an attempt that later
    # succeeds never reaches it - but even a repeated call must not insert a row.
    handle_task_failure({"ti": task_instance, "exception": RuntimeError("attempt 1")})
    handle_task_failure({"ti": task_instance, "exception": RuntimeError("attempt 2")})
    close_pipeline_run(ti=task_instance)

    runs = stored_runs(session_factory)
    assert len(runs) == 1, "retries must never produce extra pipeline_runs rows"
    assert runs[0].run_id == run_id
    assert runs[0].status == "COMPLETED"


def test_failure_before_run_is_opened_is_ignored(lifecycle_db):
    """If the opening task itself fails there is no run to mark, and no crash."""
    _, session_factory = lifecycle_db

    handle_task_failure(
        {"ti": make_task_instance(), "exception": RuntimeError("database unreachable")}
    )

    assert stored_runs(session_factory) == []


def test_failure_recording_never_masks_the_original_exception(lifecycle_db):
    """A database problem while recording FAILED must not raise over the cause."""
    _, session_factory = lifecycle_db
    run_id = open_pipeline_run()

    with patch(
        "src.pipeline.run_lifecycle.create_database_engine",
        side_effect=RuntimeError("database unreachable"),
    ):
        # Returns rather than raising, so Airflow still surfaces the real failure.
        handle_task_failure(
            {
                "ti": make_task_instance(start_pipeline_run=run_id),
                "exception": RuntimeError("original cause"),
            }
        )

    assert stored_runs(session_factory)[0].status == "RUNNING"


def test_orchestrated_etl_does_not_record_its_own_run(mock_db, lifecycle_db):
    """The ETL stage must not write a second run; the DAG lifecycle owns the record."""
    engine, session_factory = mock_db
    run_id = open_pipeline_run()

    missing_source = FileNotFoundError("Source CSV missing")
    with patch("src.etl.pipeline.extract_data", side_effect=missing_source), \
         patch("src.etl.pipeline.create_database_engine", return_value=engine), \
         patch("src.etl.pipeline.create_session_factory", return_value=session_factory):

        with pytest.raises(FileNotFoundError):
            run_extract_transform_load()

    runs = stored_runs(session_factory)
    assert len(runs) == 1
    assert runs[0].run_id == run_id
    # Still RUNNING: recording the failure is the callback's job, once retries end.
    assert runs[0].status == "RUNNING"


def test_dag_etl_task_delegates_to_reusable_pipeline():
    """The DAG must call the shared pipeline instead of re-implementing ETL."""
    now = datetime.now(timezone.utc)
    pipeline_result = ETLPipelineResult(
        pipeline_name="datatrust_daily_pipeline",
        status="SUCCESS",
        started_at=now,
        finished_at=now,
        duration_seconds=1.5,
        rows_extracted=10,
        rows_transformed=10,
        rows_loaded=10,
    )

    with patch(
        "dags.datatrust_pipeline.run_etl_pipeline",
        return_value=pipeline_result,
    ) as mock_pipeline:
        summary = run_extract_transform_load()

    # Airflow orchestrates only: it stops at the load stage because validation and
    # scoring are separate tasks, and asks for the raw exception on failure.
    mock_pipeline.assert_called_once_with(
        pipeline_name="datatrust_daily_pipeline",
        load_only=True,
        raise_on_failure=True,
        track_run=False,
    )
    assert summary == {
        "status": "SUCCESS",
        "rows_extracted": 10,
        "rows_transformed": 10,
        "rows_loaded": 10,
        "rows_failed": 0,
        "duration_seconds": 1.5,
    }


def test_dag_holds_no_second_etl_implementation():
    """Extraction, transformation and loading must live only in src/etl/pipeline.py."""
    for reimplemented in ("extract_data", "transform_data", "load_transformed_data"):
        assert not hasattr(datatrust_pipeline, reimplemented), (
            f"{reimplemented} is used directly by the DAG again; "
            "ETL belongs to the reusable pipeline"
        )

    assert hasattr(datatrust_pipeline, "run_etl_pipeline")


def test_task_dependency_chain_is_preserved():
    """Processing order is unchanged, now bookended by the run lifecycle."""
    if datatrust_pipeline.dag is None:
        pytest.skip("Airflow is not installed in this environment")

    tasks = datatrust_pipeline.dag.task_dict
    assert tasks["start_pipeline_run"].downstream_task_ids == {"extract_transform_load"}
    assert tasks["extract_transform_load"].downstream_task_ids == {"quality_validation"}
    assert tasks["quality_validation"].downstream_task_ids == {"health_scoring_and_persistence"}
    assert tasks["health_scoring_and_persistence"].downstream_task_ids == {"anomaly_detection"}
    assert tasks["anomaly_detection"].downstream_task_ids == {"complete_pipeline_run"}
    assert tasks["complete_pipeline_run"].downstream_task_ids == set()


def test_failure_callback_is_registered_for_every_task():
    """Marking a run FAILED must be DAG-level config, not per-task code."""
    assert DEFAULT_ARGS["on_failure_callback"] is handle_task_failure
