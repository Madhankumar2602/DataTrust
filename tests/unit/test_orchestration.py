"""
Phase 7 unit tests for Pipeline Orchestration and Airflow DAG definition.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dags import datatrust_pipeline
from dags.datatrust_pipeline import (
    DEFAULT_ARGS,
    run_anomaly_detection,
    run_extract_transform_load,
    run_health_scoring_and_persistence,
    run_quality_validation,
)
from src.database.models import Base, PipelineRun, RetailTransaction
from src.etl.pipeline import ETLPipelineResult


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


def test_orchestration_tasks_flow_isolated(mock_db):
    """
    Test the sequential execution of all 4 pipeline stages using an in-memory database.
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

        # Stage 3: Health Scoring & Persistence
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = quality_result
        scoring_result = run_health_scoring_and_persistence(ti=mock_ti)

        assert scoring_result["health_score"] >= 0.0
        assert scoring_result["status"] in ["Excellent", "Good", "Poor", "Critical"]
        assert scoring_result["run_id"] is not None

        # Verify DB stored run
        with session_factory() as session:
            run = session.query(PipelineRun).filter_by(run_id=scoring_result["run_id"]).first()
            assert run is not None
            assert run.health_score == scoring_result["health_score"]
            assert len(run.quality_results) > 0

        # Stage 4: Anomaly Detection
        anomaly_result = run_anomaly_detection()
        assert "anomalies_detected" in anomaly_result
        assert "anomalies_persisted" in anomaly_result


def test_etl_failure_bubbles_up_and_records_failed_run(mock_db):
    """An ETL failure must reach Airflow unchanged and leave a FAILED run record."""
    engine, session_factory = mock_db

    missing_source = FileNotFoundError("Source CSV missing")

    with patch("src.etl.pipeline.extract_data", side_effect=missing_source), \
         patch("src.etl.pipeline.create_database_engine", return_value=engine), \
         patch("src.etl.pipeline.create_session_factory", return_value=session_factory):

        # The original exception type reaches Airflow, so retries behave normally.
        with pytest.raises(FileNotFoundError):
            run_extract_transform_load()

    with session_factory() as session:
        failed = session.query(PipelineRun).filter_by(status="FAILED").one()

    assert failed.pipeline_name == "datatrust_daily_pipeline"
    assert "EXTRACT" in failed.error_message


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
    )
    assert summary == {
        "status": "SUCCESS",
        "rows_extracted": 10,
        "rows_transformed": 10,
        "rows_loaded": 10,
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
    """Ordering must remain ETL -> quality -> scoring -> anomaly detection."""
    if datatrust_pipeline.dag is None:
        pytest.skip("Airflow is not installed in this environment")

    tasks = datatrust_pipeline.dag.task_dict
    assert tasks["extract_transform_load"].downstream_task_ids == {"quality_validation"}
    assert tasks["quality_validation"].downstream_task_ids == {"health_scoring_and_persistence"}
    assert tasks["health_scoring_and_persistence"].downstream_task_ids == {"anomaly_detection"}
