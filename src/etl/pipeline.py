"""Composable Phase 5 ETL pipeline: extract, transform, load, validate, score, persist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base
from src.database.repository import QualityRepository
from src.etl.extractor import extract_data
from src.etl.loader import load_transformed_data
from src.etl.transformer import transform_data
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


def run_etl_pipeline(
    source_path: str | Path | None = None,
    database_url: str | None = None,
    pipeline_name: str = "online_retail_etl",
    *,
    load_only: bool = False,
    raise_on_failure: bool = False,
) -> ETLPipelineResult:
    """Run the complete ETL flow; data-quality findings do not fail execution.

    `load_only` stops after the load stage, for callers such as the Airflow DAG
    that run validation, scoring and persistence as their own downstream tasks.
    `raise_on_failure` re-raises the original exception once the FAILED run has
    been recorded, so a scheduler can see the failure and apply its retries.
    """
    started_at = datetime.now(timezone.utc)
    timer = perf_counter()
    counts: dict[str, int] = {"extracted": 0, "transformed": 0, "loaded": 0}
    stage = "EXTRACT"
    try:
        extraction = extract_data(source_path)
        counts["extracted"] = extraction.rows_extracted

        stage = "TRANSFORM"
        transformation = transform_data(extraction.dataframe)
        counts["transformed"] = transformation.rows_transformed
        if counts["transformed"] != counts["extracted"]:
            logger.warning(
                "[TRANSFORM] Row-count change extracted=%s transformed=%s",
                counts["extracted"],
                counts["transformed"],
            )

        stage = "LOAD"
        engine = create_database_engine(database_url)
        Base.metadata.create_all(engine)
        session_factory = create_session_factory(engine)
        quality_report = None
        score_report = None
        stored_run = None
        with session_factory() as session:
            load_result = load_transformed_data(session, transformation.dataframe)
            counts["loaded"] = load_result.rows_loaded

            if load_only:
                session.commit()
            else:
                stage = "VALIDATE"
                quality_report = QualityEngine("UCI Online Retail Transformed").run(
                    transformation.dataframe
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

        finished_at = datetime.now(timezone.utc)
        return ETLPipelineResult(
            pipeline_name, "SUCCESS", started_at, finished_at, round(perf_counter() - timer, 4),
            counts["extracted"], counts["transformed"], counts["loaded"],
            score_report["score"] if score_report else None,
            quality_report["summary"]["failed"] if quality_report else 0,
            quality_report["summary"]["warnings"] if quality_report else 0,
            stored_run.run_id if stored_run else None,
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        duration = round(perf_counter() - timer, 4)
        error_message = str(exc)
        logger.exception("[%s] ETL pipeline failed: %s", stage, exc)

        # Rows that were read but never made it through the (all-or-nothing) load
        # stage count as failed; rows never even extracted are not "failed", just
        # unattempted.
        rows_failed = 0 if counts["loaded"] else counts["extracted"]

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
        )


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
