"""Incremental selection for the Online Retail ETL: watermark plus reconciliation.

Why both, and not just a watermark
──────────────────────────────────
`invoice_date` is the only source-derived ordering field, but it is nowhere near
unique: the source carries 541,909 rows across roughly 23,000 distinct
timestamps, up to 1,114 rows share a single timestamp, and 15 rows sit on the
newest one. A watermark alone therefore cannot decide anything on its own:

  * `invoice_date > watermark` silently drops every row sharing the boundary
    timestamp — those 15 rows would never load.
  * `invoice_date >= watermark` keeps them, but re-inserts the ones already
    loaded on every rerun.

So the watermark is used only as a cheap coarse filter (inclusive, so nothing is
ever skipped), and the boundary is settled exactly by reconciling the candidate
rows against what the table already holds.

Reconciliation is a multiset difference, not a de-duplication
─────────────────────────────────────────────────────────────
The source legitimately contains 5,268 exact duplicate rows, and DataTrust
reports them as a quality finding, so they must survive the load. Rows are
therefore matched by *occurrence*: if the source holds three identical rows and
the table already holds two of them, exactly one more is loaded — not zero
(which would lose a real row) and not three (which would duplicate on rerun).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)

# The source fields that identify a record. Derived columns (IsCancellation,
# Revenue) are pure functions of these and would add nothing, and the database's
# own columns (transaction_id, loaded_at) describe the load, not the record.
FINGERPRINT_COLUMNS = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)

# Stored column name for each fingerprint field, so both sides of the
# reconciliation are built from the same list.
STORED_FINGERPRINT_COLUMNS = (
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
)

WATERMARK_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


@dataclass
class IncrementalSelection:
    """The outcome of deciding what an incremental run should load."""

    dataframe: pd.DataFrame
    rows_considered: int = 0
    rows_already_loaded: int = 0
    new_watermark: datetime | None = None
    previous_watermark: datetime | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def rows_selected(self) -> int:
        return len(self.dataframe)


def _normalise_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return ""
    return str(value).strip()


def _normalise_number(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _normalise_timestamp(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        return ""
    return stamp.strftime(WATERMARK_TIMESTAMP_FORMAT)


def _fingerprint_frame(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    """Build one comparable identity per row.

    Both sides of the reconciliation pass through this same normalisation, so a
    value that survived a round trip through the database still matches the
    source row it came from.
    """
    invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer, country = (
        columns
    )
    parts = [
        frame[invoice_no].map(_normalise_text),
        frame[stock_code].map(_normalise_text),
        frame[description].map(_normalise_text),
        frame[quantity].map(_normalise_number),
        frame[invoice_date].map(_normalise_timestamp),
        frame[unit_price].map(_normalise_number),
        frame[customer].map(_normalise_number),
        frame[country].map(_normalise_text),
    ]
    return pd.Series(
        ["\x1f".join(values) for values in zip(*(part.tolist() for part in parts))],
        index=frame.index,
        dtype="object",
    )


def source_fingerprints(frame: pd.DataFrame) -> pd.Series:
    """Fingerprints for a transformed source frame (canonical column names)."""
    return _fingerprint_frame(frame, FINGERPRINT_COLUMNS)


def stored_fingerprints(frame: pd.DataFrame) -> pd.Series:
    """Fingerprints for rows read back from retail_transactions."""
    return _fingerprint_frame(frame, STORED_FINGERPRINT_COLUMNS)


def parse_source_dates(frame: pd.DataFrame, column: str = "InvoiceDate") -> pd.Series:
    """Parse raw source dates the same way the transformer does."""
    return pd.to_datetime(frame[column], errors="coerce", format="mixed")


def reconciliation_floor(
    watermark: datetime | None, lookback_days: int = 0
) -> datetime | None:
    """The oldest timestamp an incremental run will look at.

    A watermark alone cannot see a row that arrives *below* it — a late record
    stamped before the last load is simply older than the resume point. Widening
    the window by `lookback_days` re-examines that recent history. Doing so is
    always safe: anything already stored is filtered out by reconciliation, so a
    lookback can only find missed rows, never duplicate loaded ones.
    """
    if watermark is None:
        return None
    if lookback_days <= 0:
        return watermark
    return watermark - timedelta(days=lookback_days)


def filter_candidates_by_watermark(
    frame: pd.DataFrame,
    watermark: datetime | None,
    column: str = "InvoiceDate",
) -> pd.DataFrame:
    """Narrow a raw source frame to rows at or after the watermark.

    The comparison is deliberately inclusive: rows sharing the boundary
    timestamp must reach the reconciliation step rather than being skipped here.
    Rows whose date cannot be parsed are always kept, so the transformer still
    sees and rejects them exactly as it does in a full load.
    """
    if watermark is None:
        return frame

    parsed = parse_source_dates(frame, column)
    keep = parsed.isna() | (parsed >= pd.Timestamp(watermark))
    return frame.loc[keep]


def subtract_already_loaded(
    candidates: pd.DataFrame,
    stored: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """Remove rows the table already holds, counting occurrences rather than values.

    Returns the rows still to load and how many were recognised as already
    present. Duplicate source rows are preserved: only as many occurrences are
    dropped as the table actually holds.
    """
    if candidates.empty:
        return candidates, 0

    candidate_keys = source_fingerprints(candidates)
    if stored.empty:
        return candidates, 0

    remaining = Counter(stored_fingerprints(stored).tolist())

    keep_mask = []
    already_loaded = 0
    for key in candidate_keys.tolist():
        if remaining.get(key, 0) > 0:
            remaining[key] -= 1
            keep_mask.append(False)
            already_loaded += 1
        else:
            keep_mask.append(True)

    return candidates.loc[pd.Series(keep_mask, index=candidates.index)], already_loaded


def next_watermark(
    transformed: pd.DataFrame,
    previous: datetime | None,
    column: str = "InvoiceDate",
) -> datetime | None:
    """Highest timestamp actually processed, never moving the watermark backwards."""
    if transformed.empty:
        return previous

    parsed = pd.to_datetime(transformed[column], errors="coerce")
    highest = parsed.max()
    if pd.isna(highest):
        return previous

    candidate = highest.to_pydatetime()
    if previous is not None and candidate < previous:
        return previous
    return candidate
