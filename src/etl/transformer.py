"""Lossless, business-aware transformations for the Online Retail dataset."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)


class TransformationError(ValueError):
    """Raised when a required ETL transformation cannot be performed safely."""


@dataclass
class TransformationResult:
    dataframe: pd.DataFrame
    rows_transformed: int
    duration_seconds: float


def transform_data(dataframe: pd.DataFrame) -> TransformationResult:
    """Normalize types and add derived fields without removing source records."""
    required = {"InvoiceNo", "InvoiceDate", "Quantity", "UnitPrice"}
    missing = required - set(dataframe.columns)
    if missing:
        raise TransformationError(f"Cannot transform dataset; missing columns: {sorted(missing)}")

    started = perf_counter()
    logger.info("[TRANSFORM] Starting lossless transformation rows=%s", len(dataframe))
    transformed = dataframe.copy()
    try:
        transformed["Quantity"] = pd.to_numeric(transformed["Quantity"], errors="raise")
        transformed["UnitPrice"] = pd.to_numeric(transformed["UnitPrice"], errors="raise")
        parsed_dates = pd.to_datetime(transformed["InvoiceDate"], errors="coerce", format="mixed")
    except (TypeError, ValueError) as exc:
        raise TransformationError(f"Unable to normalize numeric fields: {exc}") from exc

    invalid_dates = parsed_dates.isna() & transformed["InvoiceDate"].notna()
    if invalid_dates.any():
        raise TransformationError(
            f"InvoiceDate contains {int(invalid_dates.sum())} "
            "unparseable non-null value(s)."
        )

    # Store normalized dates as ISO strings so the existing Phase 2 schema
    # contract remains valid while every value has been parsed consistently.
    transformed["InvoiceDate"] = parsed_dates.dt.strftime("%Y-%m-%dT%H:%M:%S")
    transformed["IsCancellation"] = (
        transformed["InvoiceNo"].astype(str).str.startswith("C", na=False)
    )
    transformed["Revenue"] = transformed["Quantity"] * transformed["UnitPrice"]
    result = TransformationResult(transformed, len(transformed), round(perf_counter() - started, 4))
    logger.info(
        "[TRANSFORM] SUCCESS rows=%s duration=%.4fs",
        result.rows_transformed,
        result.duration_seconds,
    )
    return result
