"""Lifecycle of one logical pipeline run, shared by orchestration trigger points.

A scheduler (Airflow) opens a run before the first processing stage, records the
scoring output once it exists, and finishes the run when every stage has
succeeded — or marks it failed. All database work is delegated to
`QualityRepository`; this module only owns connection handling, so no lifecycle
SQL has to live inside a DAG task body.

Each function opens its own short-lived session on purpose: scheduler stages run
as independent processes, so a session cannot be shared between them.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import RUN_STATUS_COMPLETED, Base
from src.database.repository import QualityRepository
from src.logger import get_logger

logger = get_logger(__name__)

DEFAULT_PIPELINE_NAME = "datatrust_daily_pipeline"

# Failure messages are stored for operators, not for post-mortem debugging; the
# full traceback stays in the scheduler's own task logs.
MAX_ERROR_MESSAGE_LENGTH = 1000


@contextmanager
def _repository(database_url: str | None) -> Iterator[QualityRepository]:
    engine = create_database_engine(database_url)
    Base.metadata.create_all(engine)
    with create_session_factory(engine)() as session:
        yield QualityRepository(session)


def _concise(error_message: str) -> str:
    collapsed = " ".join(str(error_message).split())
    if len(collapsed) <= MAX_ERROR_MESSAGE_LENGTH:
        return collapsed
    return f"{collapsed[:MAX_ERROR_MESSAGE_LENGTH]}..."


def start_pipeline_run(
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
    database_url: str | None = None,
) -> int:
    """Open a RUNNING run and return the run_id identifying this execution."""
    with _repository(database_url) as repository:
        pipeline_run = repository.create_run(pipeline_name)
        run_id = pipeline_run.run_id
    logger.info("[LIFECYCLE] Started pipeline run #%s (%s)", run_id, pipeline_name)
    return run_id


def record_pipeline_run_results(
    run_id: int,
    quality_report: dict[str, Any],
    score_report: dict[str, Any],
    database_url: str | None = None,
) -> None:
    """Store quality results and health scores against a run that is still open."""
    with _repository(database_url) as repository:
        repository.record_run_results(run_id, quality_report, score_report)
    logger.info(
        "[LIFECYCLE] Recorded results for run #%s: score=%s status=%s",
        run_id,
        score_report.get("score"),
        score_report.get("status"),
    )


def complete_pipeline_run(
    run_id: int,
    rows_processed: int = 0,
    rows_failed: int = 0,
    database_url: str | None = None,
) -> None:
    """Mark a run COMPLETED once every stage of the execution has succeeded."""
    with _repository(database_url) as repository:
        repository.complete_run(
            run_id,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            status=RUN_STATUS_COMPLETED,
        )
    logger.info("[LIFECYCLE] Completed pipeline run #%s", run_id)


def fail_pipeline_run(
    run_id: int,
    error_message: str,
    rows_processed: int = 0,
    rows_failed: int = 0,
    database_url: str | None = None,
) -> bool:
    """Mark a run FAILED. Best effort: never raises, so it cannot mask the cause.

    Returns whether the FAILED state was persisted, so callers can log the
    difference between "recorded" and "database was unreachable too".
    """
    try:
        with _repository(database_url) as repository:
            repository.fail_run(
                run_id,
                error_message=_concise(error_message),
                rows_processed=rows_processed,
                rows_failed=rows_failed,
            )
    except Exception as exc:
        logger.warning(
            "[LIFECYCLE] Could not record FAILED state for run #%s: %s", run_id, exc
        )
        return False

    logger.info("[LIFECYCLE] Marked pipeline run #%s FAILED", run_id)
    return True
