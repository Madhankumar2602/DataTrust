"""M5.4 tests: incremental ETL, watermarking and idempotency.

Every test runs against its own temporary SQLite file, so nothing here touches a
real warehouse. Batches are written as CSV fixtures because the source really is
a file, and an incremental run has to decide what is new by reading it.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base, EtlWatermark, RetailTransaction
from src.database.repository import QualityRepository
from src.etl.pipeline import run_etl_pipeline
from src.etl.watermark import (
    filter_candidates_by_watermark,
    next_watermark,
    source_fingerprints,
    stored_fingerprints,
    subtract_already_loaded,
)

PIPELINE = "incremental_test_pipeline"


def make_rows(start: int, count: int, day: str = "2011-01-01", hour: int = 8):
    """Build `count` distinct source rows on one day."""
    return pd.DataFrame({
        "InvoiceNo": [f"5{start + i:05d}" for i in range(count)],
        "StockCode": [f"SKU{start + i:04d}" for i in range(count)],
        "Description": [f"ITEM {start + i}" for i in range(count)],
        "Quantity": [1 + (i % 5) for i in range(count)],
        "InvoiceDate": [f"{day} {hour}:{i % 60:02d}:00" for i in range(count)],
        "UnitPrice": [round(1.5 + i * 0.01, 2) for i in range(count)],
        "CustomerID": [17850.0 + (i % 7) for i in range(count)],
        "Country": ["United Kingdom"] * count,
    })


@pytest.fixture
def db_url(tmp_path):
    """An isolated SQLite database, created fresh for each test."""
    url = f"sqlite+pysqlite:///{tmp_path / 'incremental.db'}"
    engine = create_database_engine(url)
    Base.metadata.create_all(engine)
    engine.dispose()
    return url


@pytest.fixture
def csv_path(tmp_path):
    return tmp_path / "source.csv"


def write_csv(path, frame: pd.DataFrame):
    frame.to_csv(path, index=False)
    return path


def stored_count(db_url) -> int:
    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    count = session.query(RetailTransaction).count()
    session.close()
    engine.dispose()
    return count


def read_watermark(db_url, pipeline_name: str = PIPELINE):
    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    watermark = session.get(EtlWatermark, pipeline_name)
    value = watermark.watermark_value if watermark else None
    session.close()
    engine.dispose()
    return value


def run(csv_path, db_url, **kwargs):
    return run_etl_pipeline(
        csv_path, db_url, PIPELINE, load_only=True, incremental=True, **kwargs
    )


# ── 1. First run with no checkpoint ──────────────────────────────────────────


def test_first_run_without_checkpoint_loads_everything(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 25))

    result = run(csv_path, db_url)

    assert result.status == "SUCCESS"
    assert result.mode == "incremental"
    assert result.watermark_before is None
    assert result.rows_loaded == 25
    assert stored_count(db_url) == 25
    # The checkpoint exists only because the load succeeded.
    assert read_watermark(db_url) is not None
    assert result.watermark_after is not None


# ── 2. Second run, unchanged source ──────────────────────────────────────────


def test_rerun_of_unchanged_source_loads_nothing(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 25))
    run(csv_path, db_url)
    checkpoint_after_first = read_watermark(db_url)

    second = run(csv_path, db_url)

    assert second.status == "SUCCESS"
    assert second.rows_loaded == 0
    assert stored_count(db_url) == 25, "a rerun must not duplicate the snapshot"
    assert read_watermark(db_url) == checkpoint_after_first


# ── 3. Source gains new records ──────────────────────────────────────────────


def test_only_new_records_are_processed(csv_path, db_url):
    batch_a = make_rows(1, 20, day="2011-01-01")
    write_csv(csv_path, batch_a)
    run(csv_path, db_url)

    batch_b = make_rows(500, 8, day="2011-02-01")
    write_csv(csv_path, pd.concat([batch_a, batch_b], ignore_index=True))
    second = run(csv_path, db_url)

    assert second.rows_loaded == 8
    # The whole file is still read, but the old rows are neither transformed
    # again nor written again.
    assert second.rows_extracted == 28
    assert second.rows_skipped == 20
    assert stored_count(db_url) == 28


# ── 4. Same incremental batch twice ──────────────────────────────────────────


def test_same_incremental_batch_twice_does_not_duplicate(csv_path, db_url):
    batch_a = make_rows(1, 20, day="2011-01-01")
    write_csv(csv_path, batch_a)
    run(csv_path, db_url)

    combined = pd.concat([batch_a, make_rows(500, 8, day="2011-02-01")], ignore_index=True)
    write_csv(csv_path, combined)

    first = run(csv_path, db_url)
    second = run(csv_path, db_url)

    assert first.rows_loaded == 8
    assert second.rows_loaded == 0
    assert stored_count(db_url) == 28


# ── 5. Ties on the watermark timestamp ───────────────────────────────────────


def test_rows_sharing_the_watermark_timestamp_are_not_skipped(csv_path, db_url):
    """The boundary case a `>` comparison silently drops."""
    shared = "2011-03-01 10:00:00"
    first_half = make_rows(1, 5)
    first_half["InvoiceDate"] = shared
    write_csv(csv_path, first_half)
    run(csv_path, db_url)
    assert stored_count(db_url) == 5

    # Five more distinct rows carrying the exact same timestamp.
    second_half = make_rows(900, 5)
    second_half["InvoiceDate"] = shared
    write_csv(csv_path, pd.concat([first_half, second_half], ignore_index=True))

    result = run(csv_path, db_url)

    assert result.rows_loaded == 5, "rows on the boundary timestamp must still load"
    assert stored_count(db_url) == 10

    # And running it again must not duplicate those boundary rows.
    assert run(csv_path, db_url).rows_loaded == 0
    assert stored_count(db_url) == 10


def test_watermark_filter_is_inclusive():
    frame = make_rows(1, 3)
    frame["InvoiceDate"] = "2011-03-01 10:00:00"
    boundary = pd.Timestamp("2011-03-01 10:00:00").to_pydatetime()

    kept = filter_candidates_by_watermark(frame, boundary)

    assert len(kept) == 3


def test_unparseable_dates_always_reach_the_transformer():
    """Bad dates must not be filtered away; the transformer still has to reject them."""
    frame = make_rows(1, 2)
    frame.loc[0, "InvoiceDate"] = "not-a-date"
    boundary = pd.Timestamp("2050-01-01").to_pydatetime()

    kept = filter_candidates_by_watermark(frame, boundary)

    assert len(kept) == 1
    assert kept.iloc[0]["InvoiceDate"] == "not-a-date"


# ── 6. Failure during an incremental load ────────────────────────────────────


def test_failed_incremental_run_does_not_advance_the_checkpoint(csv_path, db_url):
    batch_a = make_rows(1, 20, day="2011-01-01")
    write_csv(csv_path, batch_a)
    run(csv_path, db_url)
    checkpoint_before = read_watermark(db_url)
    rows_before = stored_count(db_url)

    write_csv(
        csv_path,
        pd.concat([batch_a, make_rows(500, 8, day="2011-02-01")], ignore_index=True),
    )

    with patch(
        "src.etl.pipeline.load_transformed_data",
        side_effect=RuntimeError("database went away mid-load"),
    ):
        failed = run(csv_path, db_url, track_run=False)

    assert failed.status == "FAILED"
    assert failed.error_stage == "LOAD"
    assert read_watermark(db_url) == checkpoint_before, "checkpoint must not advance"
    assert stored_count(db_url) == rows_before
    assert failed.watermark_after == checkpoint_before

    # The retry, with nothing else changed, must pick the failed batch back up.
    retried = run(csv_path, db_url)

    assert retried.status == "SUCCESS"
    assert retried.rows_loaded == 8
    assert stored_count(db_url) == 28
    assert read_watermark(db_url) > checkpoint_before


def test_failure_before_any_checkpoint_leaves_none(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 10))

    with patch(
        "src.etl.pipeline.load_transformed_data",
        side_effect=RuntimeError("load exploded"),
    ):
        failed = run(csv_path, db_url, track_run=False)

    assert failed.status == "FAILED"
    assert read_watermark(db_url) is None
    assert stored_count(db_url) == 0


# ── 7. Empty incremental batch ───────────────────────────────────────────────


def test_empty_incremental_batch_succeeds_without_inserting(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 12))
    run(csv_path, db_url)

    result = run(csv_path, db_url)

    assert result.status == "SUCCESS"
    assert result.error_message is None
    assert result.rows_loaded == 0
    assert stored_count(db_url) == 12


def test_quiet_incremental_run_records_no_health_score(csv_path, db_url):
    """An empty batch must not be scored as though the data had degraded.

    Validating nothing produces a near-zero "Critical" report. Persisting that
    on every quiet run would crash the health trend the dashboard and API read.
    """
    from src.database.models import PipelineRun

    write_csv(csv_path, make_rows(1, 40))
    first = run_etl_pipeline(csv_path, db_url, PIPELINE, incremental=True)
    second = run_etl_pipeline(csv_path, db_url, PIPELINE, incremental=True)

    assert first.health_score is not None
    assert second.status == "SUCCESS"
    assert second.rows_loaded == 0
    assert second.health_score is None, "a no-op run must not invent a score"

    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    persisted = session.query(PipelineRun).all()
    session.close()
    engine.dispose()

    assert [run.health_status for run in persisted] != ["Healthy", "Critical"]
    assert all(run.health_status != "Critical" for run in persisted)


def test_failed_incremental_run_reports_only_the_batch_as_failed(csv_path, db_url):
    """rows_failed is the batch in flight, not the size of the source file."""
    batch_a = make_rows(1, 40, day="2011-01-01")
    write_csv(csv_path, batch_a)
    run(csv_path, db_url)

    write_csv(
        csv_path,
        pd.concat([batch_a, make_rows(700, 6, day="2011-05-01")], ignore_index=True),
    )
    with patch(
        "src.etl.pipeline.load_transformed_data",
        side_effect=RuntimeError("load exploded"),
    ):
        failed = run(csv_path, db_url, track_run=False)

    assert failed.status == "FAILED"
    assert failed.rows_extracted == 46
    assert failed.rows_failed == 6, "only the six new rows were ever in flight"


def test_full_mode_failure_still_reports_every_row(csv_path, db_url):
    """The pre-existing full-mode meaning of rows_failed is unchanged."""
    write_csv(csv_path, make_rows(1, 12))

    with patch(
        "src.etl.pipeline.load_transformed_data",
        side_effect=RuntimeError("load exploded"),
    ):
        failed = run_etl_pipeline(
            csv_path, db_url, PIPELINE, load_only=True, track_run=False
        )

    assert failed.rows_failed == 12


# ── 9. Legitimate source duplicates ──────────────────────────────────────────


def test_legitimate_source_duplicates_are_loaded_not_collapsed(csv_path, db_url):
    """Duplicate source rows are a quality finding, so they must survive the load."""
    rows = make_rows(1, 3)
    duplicated = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)
    write_csv(csv_path, duplicated)

    result = run(csv_path, db_url)

    assert result.rows_loaded == 4, "the duplicate row must be stored, not deduplicated"
    assert stored_count(db_url) == 4


def test_duplicates_are_reconciled_by_occurrence_not_by_value(csv_path, db_url):
    """A rerun that adds one more copy loads exactly one more row.

    The extra copy carries the timestamp of the row it duplicates, which here is
    older than the watermark, so it is only visible once the window is widened.
    That is the late-arrival case: a strict watermark cannot see below itself.
    """
    rows = make_rows(1, 3)
    write_csv(csv_path, pd.concat([rows, rows.iloc[[0]]], ignore_index=True))
    run(csv_path, db_url)
    assert stored_count(db_url) == 4

    # The source now legitimately holds a third copy of that same row.
    write_csv(csv_path, pd.concat([rows, rows.iloc[[0]], rows.iloc[[0]]], ignore_index=True))
    result = run(csv_path, db_url, lookback_days=1)

    assert result.rows_loaded == 1, "exactly the one extra occurrence"
    assert stored_count(db_url) == 5

    # And the widened window is still idempotent.
    assert run(csv_path, db_url, lookback_days=1).rows_loaded == 0
    assert stored_count(db_url) == 5


def test_a_strict_watermark_cannot_see_below_itself(csv_path, db_url):
    """Documents the limitation lookback_days exists to address."""
    rows = make_rows(1, 3)
    write_csv(csv_path, rows)
    run(csv_path, db_url)

    # A record arriving stamped earlier than the last load.
    late = make_rows(900, 1, day="2010-12-01")
    write_csv(csv_path, pd.concat([rows, late], ignore_index=True))

    without_lookback = run(csv_path, db_url)
    assert without_lookback.rows_loaded == 0, "below the watermark, so not seen"

    with_lookback = run(csv_path, db_url, lookback_days=60)
    assert with_lookback.rows_loaded == 1, "the widened window finds it"
    assert stored_count(db_url) == 4


def test_lookback_never_reloads_rows_already_stored(csv_path, db_url):
    """Widening the window must stay idempotent, not re-insert history."""
    write_csv(csv_path, make_rows(1, 30))
    run(csv_path, db_url)

    result = run(csv_path, db_url, lookback_days=3650)

    assert result.rows_loaded == 0
    assert result.rows_already_loaded == 30
    assert stored_count(db_url) == 30


def test_quality_engine_still_detects_stored_duplicates(csv_path, db_url):
    """Idempotency must not blind the duplicate-detection quality check."""
    from src.quality.uniqueness import UniquenessCheck

    rows = make_rows(1, 3)
    write_csv(csv_path, pd.concat([rows, rows.iloc[[0]]], ignore_index=True))
    run(csv_path, db_url)

    engine = create_database_engine(db_url)
    stored = pd.read_sql("SELECT * FROM retail_transactions", engine)
    engine.dispose()

    business_columns = [c for c in stored.columns if c not in ("transaction_id", "loaded_at")]
    results = UniquenessCheck().run(stored[business_columns])

    assert results[0].affected_rows == 1


def test_subtract_preserves_extra_source_occurrences():
    source = pd.concat([make_rows(1, 1)] * 3, ignore_index=True)
    stored = pd.DataFrame({
        "invoice_no": ["500001"],
        "stock_code": ["SKU0001"],
        "description": ["ITEM 1"],
        "quantity": [1.0],
        "invoice_date": [pd.Timestamp("2011-01-01 08:00:00")],
        "unit_price": [1.5],
        "customer_id": [17850.0],
        "country": ["United Kingdom"],
    })

    remaining, already = subtract_already_loaded(source, stored)

    assert already == 1
    assert len(remaining) == 2


# ── Fingerprint and watermark helpers ────────────────────────────────────────


def test_fingerprint_survives_the_database_round_trip(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 4))
    run(csv_path, db_url)

    engine = create_database_engine(db_url)
    stored = pd.read_sql("SELECT * FROM retail_transactions", engine)
    engine.dispose()

    source = make_rows(1, 4)
    source["InvoiceDate"] = pd.to_datetime(source["InvoiceDate"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    assert set(source_fingerprints(source)) == set(stored_fingerprints(stored))


def test_watermark_never_moves_backwards():
    older = pd.Timestamp("2011-01-01").to_pydatetime()
    newer = pd.Timestamp("2011-06-01").to_pydatetime()
    frame = make_rows(1, 2, day="2011-01-01")
    frame["InvoiceDate"] = pd.to_datetime(frame["InvoiceDate"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    assert next_watermark(frame, newer) == newer
    assert next_watermark(pd.DataFrame(columns=frame.columns), older) == older


def test_repository_advance_is_monotonic(db_url):
    engine = create_database_engine(db_url)
    session = create_session_factory(engine)()
    repository = QualityRepository(session)

    newer = pd.Timestamp("2011-06-01").to_pydatetime()
    older = pd.Timestamp("2011-01-01").to_pydatetime()
    repository.advance_watermark(PIPELINE, newer, rows_loaded=5)
    repository.advance_watermark(PIPELINE, older, rows_loaded=1)

    assert repository.get_watermark(PIPELINE).watermark_value == newer
    session.close()
    engine.dispose()


# ── Full-refresh and existing-warehouse safety ───────────────────────────────


def test_full_refresh_rewrites_the_snapshot_and_resets_the_checkpoint(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 20))
    run(csv_path, db_url)

    smaller = make_rows(1, 5)
    write_csv(csv_path, smaller)
    result = run_etl_pipeline(
        csv_path, db_url, PIPELINE, load_only=True, incremental=True, full_refresh=True
    )

    assert result.mode == "full"
    assert result.rows_loaded == 5
    assert stored_count(db_url) == 5, "full refresh replaces rather than appends"
    assert read_watermark(db_url) is not None


def test_enabling_incremental_on_a_populated_table_does_not_reload_it(csv_path, db_url):
    """A full load first, then incremental: the existing rows must not double."""
    write_csv(csv_path, make_rows(1, 30))
    full = run_etl_pipeline(csv_path, db_url, PIPELINE, load_only=True)

    assert full.rows_loaded == 30
    assert read_watermark(db_url) is None, "a full run stores no checkpoint"

    result = run(csv_path, db_url)

    assert result.rows_loaded == 0, "existing rows must be recognised, not reloaded"
    assert stored_count(db_url) == 30


# ── 8. Airflow still delegates to the reusable ETL ───────────────────────────


def test_dag_still_delegates_incremental_capable_pipeline():
    """The DAG must keep calling src/etl/pipeline.py, not grow its own loader."""
    from dags import datatrust_pipeline

    for reimplemented in ("extract_data", "transform_data", "load_transformed_data"):
        assert not hasattr(datatrust_pipeline, reimplemented), (
            f"{reimplemented} is used directly by the DAG again; "
            "ETL belongs to the reusable pipeline"
        )

    assert hasattr(datatrust_pipeline, "run_etl_pipeline")
    # The watermark machinery stays in src/ too.
    for helper in ("subtract_already_loaded", "filter_candidates_by_watermark"):
        assert not hasattr(datatrust_pipeline, helper)


def test_dag_etl_task_reaches_the_reusable_pipeline():
    from dags.datatrust_pipeline import run_extract_transform_load

    with patch(
        "dags.datatrust_pipeline.run_etl_pipeline", return_value=_fake_result()
    ) as mock_pipeline:
        run_extract_transform_load()

    assert mock_pipeline.call_count == 1
    kwargs = mock_pipeline.call_args.kwargs
    assert kwargs["load_only"] is True
    assert kwargs["raise_on_failure"] is True
    assert kwargs["track_run"] is False


def _fake_result():
    from datetime import datetime, timezone

    from src.etl.pipeline import ETLPipelineResult

    now = datetime.now(timezone.utc)
    return ETLPipelineResult(
        pipeline_name="datatrust_daily_pipeline",
        status="SUCCESS",
        started_at=now,
        finished_at=now,
        duration_seconds=0.5,
        rows_extracted=10,
        rows_transformed=10,
        rows_loaded=10,
    )


# NOTE: run_etl.py's argparse surface is deliberately not imported here. That
# module rebinds sys.stdout at import time, which detaches pytest's capture and
# breaks every test that runs after it. The flags are covered by exercising
# run_etl_pipeline's keyword arguments directly, above.


# ── Full mode is untouched ───────────────────────────────────────────────────


def test_default_mode_still_replaces_the_snapshot(csv_path, db_url):
    write_csv(csv_path, make_rows(1, 10))
    run_etl_pipeline(csv_path, db_url, PIPELINE, load_only=True)

    write_csv(csv_path, make_rows(500, 4))
    result = run_etl_pipeline(csv_path, db_url, PIPELINE, load_only=True)

    assert result.mode == "full"
    assert result.rows_loaded == 4
    assert stored_count(db_url) == 4, "the default path still replaces the snapshot"
