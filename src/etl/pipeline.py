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


def run_etl_pipeline(
    source_path: str | Path | None = None,
    database_url: str | None = None,
    pipeline_name: str = "online_retail_etl",
) -> ETLPipelineResult:
    """Run the complete ETL flow; data-quality findings do not fail execution."""
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
        with session_factory() as session:
            load_result = load_transformed_data(session, transformation.dataframe)
            counts["loaded"] = load_result.rows_loaded

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
            counts["extracted"], counts["transformed"], counts["loaded"], score_report["score"],
            quality_report["summary"]["failed"],
            quality_report["summary"]["warnings"],
            stored_run.run_id,
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        logger.exception("[%s] ETL pipeline failed: %s", stage, exc)
        return ETLPipelineResult(
            pipeline_name, "FAILED", started_at, finished_at, round(perf_counter() - timer, 4),
            counts["extracted"],
            counts["transformed"],
            counts["loaded"],
            error_stage=stage,
            error_message=str(exc),
        )
