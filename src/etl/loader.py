"""Load transformed Online Retail data into the MySQL target table."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import pandas as pd
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from src.config import settings
from src.database.models import RetailTransaction
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LoadResult:
    rows_loaded: int
    duration_seconds: float


# Number of rows sent to MySQL per batch.
BATCH_SIZE = 10_000

# Replace the whole snapshot (the original behaviour) or add to it. Appending is
# what makes an incremental run cheap: already-loaded rows are neither deleted
# nor rewritten.
MODE_REPLACE = "replace"
MODE_APPEND = "append"


def load_transformed_data(
    session: Session,
    dataframe: pd.DataFrame,
    mode: str = MODE_REPLACE,
) -> LoadResult:
    """
    Write the transformed rows into MySQL and verify the resulting row count.

    `mode="replace"` (the default) clears the previous snapshot first, which is
    the full-refresh behaviour every existing caller relies on. `mode="append"`
    leaves stored rows untouched and only adds the supplied ones, for
    incremental loads that have already worked out which rows are new.

    Data is inserted in batches to avoid building one extremely large
    database operation for the complete dataset.
    """
    if mode not in (MODE_REPLACE, MODE_APPEND):
        raise ValueError(
            f"Unknown load mode '{mode}'. Expected '{MODE_REPLACE}' or '{MODE_APPEND}'."
        )
    required = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
        "IsCancellation",
        "Revenue",
    }

    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(
            f"Cannot load transformed data; missing columns: {sorted(missing)}"
        )

    started = perf_counter()

    logger.info(
        "[LOAD] Starting MySQL %s load rows=%s batch_size=%s",
        mode,
        len(dataframe),
        BATCH_SIZE,
    )

    try:
        if mode == MODE_REPLACE:
            # Remove the previous transformed snapshot.
            session.execute(delete(RetailTransaction))
            rows_already_present = 0
        else:
            # Appending: the expected final count builds on what is already
            # stored, so the integrity check below still means something.
            rows_already_present = int(
                session.scalar(select(func.count()).select_from(RetailTransaction)) or 0
            )

        # Convert the DataFrame to database-ready records.
        records = _build_records(dataframe)

        # Insert records in batches.
        total_records = len(records)

        for start in range(0, total_records, BATCH_SIZE):
            end = min(start + BATCH_SIZE, total_records)
            batch = records[start:end]

            session.execute(
                insert(RetailTransaction),
                batch,
            )

            logger.info(
                "[LOAD] Inserted rows %s-%s of %s",
                start + 1,
                end,
                total_records,
            )

        # Verify database row count before committing.
        table_count = (
            session.scalar(
                select(func.count()).select_from(RetailTransaction)
            )
            or 0
        )

        expected_count = rows_already_present + total_records

        if table_count != expected_count:
            raise RuntimeError(
                f"Row-count integrity failure: "
                f"expected {expected_count}, loaded {table_count}."
            )

        session.flush()

    except Exception:
        session.rollback()
        raise

    result = LoadResult(
        # What this load contributed, not the size of the table. In replace mode
        # the two are identical, so existing callers see no change.
        rows_loaded=int(total_records),
        duration_seconds=round(perf_counter() - started, 4),
    )

    logger.info(
        "[LOAD] SUCCESS rows=%s duration=%.4fs",
        result.rows_loaded,
        result.duration_seconds,
    )

    return result


def _build_records(dataframe: pd.DataFrame) -> list[dict]:
    """Convert the transformed DataFrame into MySQL-ready records."""

    records = dataframe[
        [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
            "IsCancellation",
            "Revenue",
        ]
    ].copy()

    # Shared with the schema check, so the stored representation it validates is
    # always the one this loader actually writes.
    records = records.rename(columns=dict(settings.STORED_COLUMN_MAP))

    records["invoice_no"] = records["invoice_no"].astype(str)
    records["stock_code"] = records["stock_code"].astype(str)

    records["description"] = records["description"].where(
        records["description"].notna(),
        None,
    )

    records["country"] = records["country"].where(
        records["country"].notna(),
        None,
    )

    records["customer_id"] = records["customer_id"].where(
        records["customer_id"].notna(),
        None,
    )

    records["invoice_date"] = [
        ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
        for ts in pd.to_datetime(records["invoice_date"])
    ]

    return records.to_dict(orient="records")
