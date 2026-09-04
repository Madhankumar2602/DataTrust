"""
Phase 7 unit tests for Pipeline Orchestration and Airflow DAG definition.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dags.datatrust_pipeline import (
    DEFAULT_ARGS,
    run_anomaly_detection,
    run_extract_transform_load,
    run_health_scoring_and_persistence,
    run_quality_validation,
)
from src.database.models import Base, PipelineRun, RetailTransaction


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

    with patch("dags.datatrust_pipeline.extract_data") as mock_extract, \
         patch("dags.datatrust_pipeline.create_database_engine", return_value=engine), \
         patch("dags.datatrust_pipeline.create_session_factory", return_value=session_factory):

        mock_extract.return_value = MagicMock(
            dataframe=sample_df,
            rows_extracted=len(sample_df),
        )

        # Stage 1: ETL
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


def test_etl_failure_bubbles_up():
    """Verify that if ETL extraction fails, the task raises an exception."""
    with patch("dags.datatrust_pipeline.extract_data", side_effect=FileNotFoundError("Source CSV missing")):
        with pytest.raises(FileNotFoundError):
            run_extract_transform_load()
