"""Extract the immutable Online Retail source through the shared loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from src.ingestion.loader import load_csv
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ExtractionResult:
    dataframe: pd.DataFrame
    source_path: Path
    rows_extracted: int
    columns_extracted: int
    duration_seconds: float


def extract_data(source_path: str | Path | None = None) -> ExtractionResult:
    """Read the raw source without changing it and return extraction metrics."""
    started = perf_counter()
    logger.info("[EXTRACT] Starting extraction from %s", source_path or "configured raw path")
    dataframe = load_csv(source_path)
    result = ExtractionResult(
        dataframe,
        Path(source_path) if source_path else Path(settings.RAW_DATA_PATH),
        len(dataframe),
        len(dataframe.columns),
        round(perf_counter() - started, 4),
    )
    logger.info(
        "[EXTRACT] SUCCESS rows=%s columns=%s duration=%.4fs",
        result.rows_extracted,
        result.columns_extracted,
        result.duration_seconds,
    )
    return result
