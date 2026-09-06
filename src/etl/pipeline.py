"""Composable Phase 5 ETL pipeline: extract, transform, load, validate, score, persist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import pandas as pd

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base
from src.database.repository import QualityRepository
from src.etl.extractor import extract_data
from src.etl.loader import MODE_APPEND, MODE_REPLACE, load_transformed_data
from src.etl.transformer import transform_data
from src.etl.watermark import (
    STORED_FINGERPRINT_COLUMNS,
    filter_candidates_by_watermark,
    next_watermark,
    reconciliation_floor,
    subtract_already_loaded,
)
from src.logger import get_logger
from src.quality.engine import QualityEngine
from src.scoring.scorer import HealthScorer

logger = get_logger(__name__)


@dataclass
class ETLPipelineResult:
    pipeline_name: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    health_score: float | None = None
    quality_failures: int = 0
    quality_warnings: int = 0
    run_id: int | None = None
    error_stage: str | None = None
    error_message: str | None = None
    rows_failed: int = 0
    # Incremental bookkeeping. `rows_skipped` counts source rows the run
    # deliberately did not reload, so an incremental run reports honestly rather
    # than looking like it processed everything.
    mode: str = "full"
    rows_skipped: int = 0
    rows_already_loaded: int = 0
    watermark_before: datetime | None = None
    watermark_after: datetime | None = None


def run_etl_pipeline(
    source_path: str | Path | None = None,
    database_url: str | None = None,
    pipeline_name: str = "online_retail_etl",
    *,
    load_only: bool = False,
    raise_on_failure: bool = False,
    track_run: bool = True,
    incremental: bool = False,
    full_refresh: bool = False,
    lookback_days: int = 0,
) -> ETLPipelineResult:
    """Run the complete ETL flow; data-quality findings do not fail execution.

    `load_only` stops after the load stage, for callers such as the Airflow DAG
    that run validation, scoring and persistence as their own downstream tasks.
    `raise_on_failure` re-raises the original exception once the FAILED run has
    been recorded, so a scheduler can see the failure and apply its retries.
    `track_run=False` suppresses this function's own FAILED record, for callers
    that already own a pipeline_runs row for the wider execution; without it an
    orchestrated failure would be written twice, as two unrelated runs.

    `incremental=True` loads only the source rows the target table does not
    already hold, resuming from the stored watermark and appending rather than
    rewriting the snapshot. `full_refresh=True` forces the whole source through
    even in incremental mode and resets the checkpoint afterwards. Both default
    to off, so the existing full-snapshot behaviour is unchanged.

    `lookback_days` re-examines that many days below the watermark, which is how
    a record that arrives stamped earlier than the last load still gets picked
    up. It can only find missed rows: reconciliation removes anything already
    stored, so widening the window never duplicates data.
    """
    started_at = datetime.now(timezone.utc)
    timer = perf_counter()
    counts: dict[str, int] = {"extracted": 0, "transformed": 0, "loaded": 0}
    incremental_mode = incremental and not full_refresh
    mode_label = "incremental" if incremental_mode else "full"
    watermark_before: datetime | None = None
    watermark_after: datetime | None = None
    rows_skipped = 0
    rows_already_loaded = 0
    # How many rows this run is actually attempting. It starts as everything
    # extracted and narrows as an incremental run filters the batch down, so a
    # failure reports the rows that were really in flight rather than the size
    # of the source file.
    rows_in_flight = 0
    stage = "EXTRACT"
    try:
        extraction = extract_data(source_path)
        counts["extracted"] = extraction.rows_extracted
        rows_in_flight = counts["extracted"]

        # Built at the stage that first needs the database, so a bad connection
        # string is still reported against the stage that tried to use it.
        session_factory = None

        source_frame = extraction.dataframe
        reconcile_floor: datetime | None = None
        if incremental_mode:
            stage = "WATERMARK"
            session_factory = _open_session_factory(database_url)
            with session_factory() as session:
                watermark_before = _resolve_watermark(
                    QualityRepository(session), pipeline_name
                )
            # The same floor drives both the source filter and the stored window
            # they are reconciled against; if they disagreed, rows outside the
            # narrower window would look new and be inserted twice.
            reconcile_floor = reconciliation_floor(watermark_before, lookback_days)
            # Inclusive comparison: rows sharing the boundary timestamp must
            # reach reconciliation instead of being silently dropped here.
            source_frame = filter_candidates_by_watermark(source_frame, reconcile_floor)
            rows_skipped = counts["extracted"] - len(source_frame)
            rows_in_flight = len(source_frame)
            logger.info(
                "[WATERMARK] resume_from=%s floor=%s candidates=%s skipped=%s",
                watermark_before,
                reconcile_floor,
                len(source_frame),
                rows_skipped,
            )

        stage = "TRANSFORM"
        transformation = transform_data(source_frame)
        counts["transformed"] = transformation.rows_transformed
        if not incremental_mode and counts["transformed"] != counts["extracted"]:
            logger.warning(
                "[TRANSFORM] Row-count change extracted=%s transformed=%s",
                counts["extracted"],
                counts["transformed"],
            )

        to_load = transformation.dataframe
        if incremental_mode and reconcile_floor is not None:
            stage = "RECONCILE"
            with session_factory() as session:
                stored_rows = _stored_window(
                    QualityRepository(session), reconcile_floor
                )
            to_load, rows_already_loaded = subtract_already_loaded(to_load, stored_rows)
            rows_skipped += rows_already_loaded
            rows_in_flight = len(to_load)
            logger.info(
                "[RECONCILE] already_loaded=%s remaining=%s",
                rows_already_loaded,
                len(to_load),
            )

        stage = "LOAD"
        if session_factory is None:
            session_factory = _open_session_factory(database_url)
        quality_report = None
        score_report = None
        stored_run = None
        load_mode = MODE_APPEND if incremental_mode else MODE_REPLACE
        with session_factory() as session:
            load_result = load_transformed_data(session, to_load, mode=load_mode)
            counts["loaded"] = load_result.rows_loaded

            nothing_new = incremental_mode and to_load.empty
            if load_only or nothing_new:
                # An incremental run with nothing new has no batch to assess.
                # Scoring an empty frame would yield a near-zero "Critical"
                # result and persist it as though the data had degraded, so a
                # quiet run records no score rather than a misleading one.
                if nothing_new and not load_only:
                    logger.info(
                        "[VALIDATE] No new rows; skipping validation and scoring "
                        "so an empty batch cannot be recorded as a health result."
                    )
                session.commit()
            else:
                stage = "VALIDATE"
                # An incremental run validates the batch it actually processed;
                # a full run validates the whole snapshot, exactly as before.
                quality_report = QualityEngine("UCI Online Retail Transformed").run(
                    to_load
                )
                stage = "SCORE"
                score_report = HealthScorer().calculate_score(quality_report)

                stage = "PERSIST"
                stored_run = QualityRepository(session).save_run(
                    quality_report,
                    score_report,
                    pipeline_name,
                    started_at,
                    datetime.now(timezone.utc),
                    "SUCCESS",
                )

        # The load is committed only once the `with` block above exits without
        # raising, so the checkpoint is advanced strictly afterwards: a failure
        # anywhere above leaves the previous resume point untouched.
        stage = "CHECKPOINT"
        watermark_after = watermark_before
        if incremental or full_refresh:
            watermark_after = _advance_checkpoint(
                session_factory=session_factory,
                pipeline_name=pipeline_name,
                processed=transformation.dataframe,
                previous=watermark_before,
                rows_loaded=counts["loaded"],
                run_id=stored_run.run_id if stored_run else None,
                full_refresh=full_refresh,
            )

        finished_at = datetime.now(timezone.utc)
        return ETLPipelineResult(
            pipeline_name, "SUCCESS", started_at, finished_at, round(perf_counter() - timer, 4),
            counts["extracted"], counts["transformed"], counts["loaded"],
            score_report["score"] if score_report else None,
            quality_report["summary"]["failed"] if quality_report else 0,
            quality_report["summary"]["warnings"] if quality_report else 0,
            stored_run.run_id if stored_run else None,
            mode=mode_label,
            rows_skipped=rows_skipped,
            rows_already_loaded=rows_already_loaded,
            watermark_before=watermark_before,
            watermark_after=watermark_after,
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        duration = round(perf_counter() - timer, 4)
        error_message = str(exc)
        logger.exception("[%s] ETL pipeline failed: %s", stage, exc)

        # Rows that were read but never made it through the (all-or-nothing) load
        # stage count as failed; rows never even extracted are not "failed", just
        # unattempted. An incremental run counts only the batch it was actually
        # attempting, not every row in the source file.
        rows_failed = 0 if counts["loaded"] else rows_in_flight

        failed_run_id = None
        if track_run:
            failed_run_id = _persist_failed_run(
                pipeline_name=pipeline_name,
                started_at=started_at,
                finished_at=finished_at,
                error_message=f"[{stage}] {error_message}",
                rows_processed=counts["loaded"],
                rows_failed=rows_failed,
                database_url=database_url,
            )

        if raise_on_failure:
            # The FAILED run is recorded; a bare re-raise keeps the original
            # exception and traceback intact for the caller's error handling.
            raise

        return ETLPipelineResult(
            pipeline_name, "FAILED", started_at, finished_at, duration,
            counts["extracted"],
            counts["transformed"],
            counts["loaded"],
            run_id=failed_run_id,
            error_stage=stage,
            error_message=error_message,
            rows_failed=rows_failed,
            mode=mode_label,
            rows_skipped=rows_skipped,
            rows_already_loaded=rows_already_loaded,
            # Unchanged by definition: the checkpoint is only ever advanced
            # after a committed load, which this run never reached.
            watermark_before=watermark_before,
            watermark_after=watermark_before,
        )


def _open_session_factory(database_url: str | None):
    """Create the engine, ensure the schema exists, and return a session factory."""
    engine = create_database_engine(database_url)
    Base.metadata.create_all(engine)
    return create_session_factory(engine)


def _resolve_watermark(
    repository: QualityRepository, pipeline_name: str
) -> datetime | None:
    """Where an incremental run should resume from.

    The stored checkpoint wins. With no checkpoint the table itself is consulted,
    so enabling incremental mode against an already-populated warehouse resumes
    from the real data instead of reloading every row. An empty table means a
    genuine first load and returns None.
    """
    watermark = repository.get_watermark(pipeline_name)
    if watermark is not None:
        return watermark.watermark_value

    loaded_high_water_mark = repository.get_loaded_high_water_mark()
    if loaded_high_water_mark is not None:
        logger.info(
            "[WATERMARK] No checkpoint for '%s'; resuming from stored data at %s",
            pipeline_name,
            loaded_high_water_mark,
        )
    return loaded_high_water_mark


def _stored_window(
    repository: QualityRepository, watermark: datetime
) -> pd.DataFrame:
    """Rows already stored at or after the watermark, as a frame to reconcile against."""
    rows = repository.get_rows_at_or_after(watermark)
    return pd.DataFrame(
        [
            {
                "invoice_no": row.invoice_no,
                "stock_code": row.stock_code,
                "description": row.description,
                "quantity": row.quantity,
                "invoice_date": row.invoice_date,
                "unit_price": row.unit_price,
                "customer_id": row.customer_id,
                "country": row.country,
            }
            for row in rows
        ],
        columns=list(STORED_FINGERPRINT_COLUMNS),
    )


def _advance_checkpoint(
    session_factory,
    pipeline_name: str,
    processed: pd.DataFrame,
    previous: datetime | None,
    rows_loaded: int,
    run_id: int | None,
    full_refresh: bool,
) -> datetime | None:
    """Move the checkpoint to the newest timestamp this run proved loaded.

    An empty batch keeps the previous value: there is nothing new to record, and
    rewriting it would claim progress that did not happen.
    """
    target = next_watermark(processed, previous)
    if target is None:
        return previous

    with session_factory() as session:
        repository = QualityRepository(session)
        if full_refresh:
            # The snapshot was rewritten from scratch, so any older checkpoint
            # describes a table that no longer exists.
            repository.reset_watermark(pipeline_name)
        watermark = repository.advance_watermark(
            pipeline_name,
            watermark_value=target,
            rows_loaded=rows_loaded,
            run_id=run_id,
        )
        logger.info(
            "[CHECKPOINT] '%s' advanced to %s", pipeline_name, watermark.watermark_value
        )
        return watermark.watermark_value


def _persist_failed_run(
    pipeline_name: str,
    started_at: datetime,
    finished_at: datetime,
    error_message: str,
    rows_processed: int,
    rows_failed: int,
    database_url: str | None,
) -> int | None:
    """Best-effort persistence of a FAILED run record so failures stay traceable.

    Runs in its own engine/session, separate from whatever connection (if any)
    the failed pipeline stage was using. If the database itself is unreachable
    (e.g. the failure *was* a bad connection string), this logs and gives up
    rather than raising a second exception over the original failure.
    """
    try:
        engine = create_database_engine(database_url)
        Base.metadata.create_all(engine)
        session_factory = create_session_factory(engine)
        with session_factory() as session:
            failed_run = QualityRepository(session).save_failed_run(
                pipeline_name=pipeline_name,
                started_at=started_at,
                error_message=error_message,
                finished_at=finished_at,
                rows_processed=rows_processed,
                rows_failed=rows_failed,
            )
            return failed_run.run_id
    except Exception as persist_exc:
        logger.warning(
            "Could not persist FAILED pipeline run record: %s", persist_exc
        )
        return None
