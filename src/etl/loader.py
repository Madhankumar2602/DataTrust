"""Load transformed Online Retail data into the MySQL target table."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import pandas as pd
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from src.database.models import RetailTransaction
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LoadResult:
    rows_loaded: int
    duration_seconds: float


# Number of rows sent to MySQL per batch.
BATCH_SIZE = 10_000


def load_transformed_data(session: Session, dataframe: pd.DataFrame) -> LoadResult:
    """
    Replace the current transformed snapshot and verify its row count.

    Data is inserted in batches to avoid building one extremely large
    database operation for the complete dataset.
    """
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
        "[LOAD] Starting MySQL snapshot load rows=%s batch_size=%s",
        len(dataframe),
        BATCH_SIZE,
    )

    try:
        # Remove the previous transformed snapshot.
        session.execute(delete(RetailTransaction))

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
        loaded = (
            session.scalar(
                select(func.count()).select_from(RetailTransaction)
            )
            or 0
        )

        if loaded != total_records:
            raise RuntimeError(
                f"Row-count integrity failure: "
                f"expected {total_records}, loaded {loaded}."
            )

        session.flush()

    except Exception:
        session.rollback()
        raise

    result = LoadResult(
        rows_loaded=int(loaded),
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

    records = records.rename(
        columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country",
            "IsCancellation": "is_cancellation",
            "Revenue": "revenue",
        }
    )

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

    records["invoice_date"] = pd.to_datetime(
        records["invoice_date"]
    ).dt.to_pydatetime()

    return records.to_dict(orient="records")