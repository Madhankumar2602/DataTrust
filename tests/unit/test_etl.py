"""Phase 5 ETL tests using tiny CSV fixtures and isolated in-memory databases."""

import pandas as pd
import pytest

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base, RetailTransaction
from src.etl.extractor import extract_data
from src.etl.loader import load_transformed_data
from src.etl.pipeline import run_etl_pipeline
from src.etl.transformer import TransformationError, transform_data


@pytest.fixture
def retail_df():
    return pd.DataFrame({
        "InvoiceNo": ["536365", "C536366"], "StockCode": ["85123A", "71053"],
        "Description": ["HEART", "LANTERN"], "Quantity": [6, -2],
        "InvoiceDate": ["12/1/2010 8:26", "12/1/2010 8:28"], "UnitPrice": [2.5, 3.0],
        "CustomerID": [17850.0, 17850.0], "Country": ["United Kingdom", "United Kingdom"],
    })


@pytest.fixture
def csv_path(tmp_path, retail_df):
    path = tmp_path / "retail.csv"
    retail_df.to_csv(path, index=False)
    return path


def test_extractor_loads_csv(csv_path):
    result = extract_data(csv_path)
    assert result.rows_extracted == 2
    assert result.columns_extracted == 8


def test_extractor_missing_source_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract_data(tmp_path / "missing.csv")


def test_extractor_unreadable_source_fails(tmp_path):
    with pytest.raises(ValueError):
        extract_data(tmp_path)


def test_transformer_is_lossless_and_adds_fields(retail_df):
    result = transform_data(retail_df)
    assert result.rows_transformed == len(retail_df)
    assert result.dataframe["InvoiceDate"].tolist() == [
        "2010-12-01T08:26:00",
        "2010-12-01T08:28:00",
    ]
    assert result.dataframe["IsCancellation"].tolist() == [False, True]
    assert result.dataframe["Revenue"].tolist() == [15.0, -6.0]


def test_transformer_rejects_invalid_date(retail_df):
    retail_df.loc[0, "InvoiceDate"] = "not-a-date"
    with pytest.raises(TransformationError):
        transform_data(retail_df)


def test_loader_inserts_and_verifies_rows(retail_df):
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = create_session_factory(engine)()
    result = load_transformed_data(session, transform_data(retail_df).dataframe)
    assert result.rows_loaded == 2
    assert session.query(RetailTransaction).count() == 2
    session.close()


def test_pipeline_succeeds_despite_quality_failure(csv_path, retail_df):
    retail_df.loc[0, "UnitPrice"] = -2.0
    retail_df.to_csv(csv_path, index=False)
    result = run_etl_pipeline(csv_path, "sqlite+pysqlite:///:memory:")
    assert result.status == "SUCCESS"
    assert result.rows_extracted == result.rows_transformed == result.rows_loaded == 2
    assert result.quality_failures == 1


def test_pipeline_success_records_zero_rows_failed_and_duration(csv_path, tmp_path):
    db_url = f"sqlite+pysqlite:///{tmp_path / 'run.db'}"
    result = run_etl_pipeline(csv_path, db_url)
    assert result.status == "SUCCESS"
    assert result.rows_failed == 0
    assert result.duration_seconds >= 0.0
    assert result.run_id is not None

    # The metadata must actually be persisted, not just returned in-memory.
    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    stored = repo_get_run(session, result.run_id)
    session.close()
    assert stored is not None
    assert stored.status == "SUCCESS"
    assert stored.rows_failed == 0
    assert stored.rows_processed == 2
    assert stored.error_message is None


def test_pipeline_stops_on_extraction_failure(tmp_path):
    db_url = f"sqlite+pysqlite:///{tmp_path / 'run.db'}"
    result = run_etl_pipeline(tmp_path / "absent.csv", db_url)
    assert result.status == "FAILED"
    assert result.error_stage == "EXTRACT"
    assert result.rows_failed == 0
    assert result.error_message

    # A FAILED run must be traceable in the database, not silently dropped.
    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    stored = repo_get_run(session, result.run_id)
    session.close()
    assert stored is not None
    assert stored.status == "FAILED"
    assert stored.error_message
    assert "EXTRACT" in stored.error_message


def test_pipeline_stops_on_transformation_failure(csv_path, retail_df, tmp_path):
    retail_df.loc[0, "InvoiceDate"] = "bad-date"
    retail_df.to_csv(csv_path, index=False)
    db_url = f"sqlite+pysqlite:///{tmp_path / 'run.db'}"
    result = run_etl_pipeline(csv_path, db_url)
    assert result.status == "FAILED"
    assert result.error_stage == "TRANSFORM"
    # Both rows were extracted but neither made it through the pipeline.
    assert result.rows_failed == 2

    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    stored = repo_get_run(session, result.run_id)
    session.close()
    assert stored.rows_failed == 2
    assert stored.status == "FAILED"


def test_pipeline_surfaces_load_failure(csv_path):
    result = run_etl_pipeline(csv_path, "not-a-database-url")
    assert result.status == "FAILED"
    assert result.error_stage == "LOAD"
    # Both rows were extracted/transformed but the load never happened.
    assert result.rows_failed == 2
    # The failing URL is unusable for tracking too, so persistence is skipped
    # rather than raising a second exception over the original failure.
    assert result.run_id is None


def repo_get_run(session, run_id):
    """Small local helper: fetch a PipelineRun without pulling in the full repository fixture."""
    from src.database.models import PipelineRun

    return session.get(PipelineRun, run_id)
