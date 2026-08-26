"""
loader.py — Data ingestion layer for DataTrust.

What is the "ingestion layer"?
──────────────────────────────
In a data pipeline, the FIRST step is loading raw data into memory.
The ingestion layer is responsible ONLY for:
  1. Reading data from a source (CSV, Excel, database, API)
  2. Returning it as a Pandas DataFrame
  3. Doing NOTHING else — no cleaning, no analysis

Why keep it separate?
  - The profiler, quality engine, and dashboard all need to load data.
  - If loading logic is scattered across every file, a path change
    breaks everything.
  - With this loader, ONE fix fixes ALL consumers.

The UCI Online Retail dataset
─────────────────────────────
Source: https://archive.ics.uci.edu/dataset/352/online+retail
Format: Excel (.xlsx) / CSV
Encoding: Some rows use Latin-1 characters (£ signs, accented names).
          We handle this with encoding='latin-1'.

Columns:
  InvoiceNo   — 6-digit invoice number; starts with 'C' = cancellation
  StockCode   — 5-digit product code
  Description — Product name
  Quantity    — Units per transaction (negative = cancellation)
  InvoiceDate — Date and time of invoice
  UnitPrice   — Price per unit in sterling (£)
  CustomerID  — 5-digit customer number (can be missing)
  Country     — Country of the customer
"""

from pathlib import Path

import pandas as pd

from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)


def load_csv(file_path: str | Path | None = None, encoding: str = "latin-1") -> pd.DataFrame:
    """
    Load the Online Retail dataset from a CSV file.

    Args:
        file_path: Path to the CSV. Defaults to settings.RAW_DATA_PATH.
        encoding:  File encoding. The UCI dataset uses 'latin-1'.

    Returns:
        pd.DataFrame with the raw, unmodified dataset.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError:        If the file is empty or unreadable.
    """
    path = Path(file_path) if file_path else Path(settings.RAW_DATA_PATH)

    logger.info(f"Loading CSV from: {path}")

    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {path}\n"
            "Please download it from: "
            "https://archive.ics.uci.edu/dataset/352/online+retail\n"
            "and place it at: data/raw/online_retail.csv"
        )

    try:
        df = pd.read_csv(path, encoding=encoding, low_memory=False)
    except Exception as exc:
        raise ValueError(f"Failed to read CSV file: {exc}") from exc

    if df.empty:
        raise ValueError(f"The dataset at {path} is empty.")

    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


def load_excel(file_path: str | Path, sheet_name: str = "Online Retail") -> pd.DataFrame:
    """
    Load the Online Retail dataset from the original Excel file.

    The UCI dataset is distributed as an .xlsx file.
    We convert it to CSV first (see scripts/convert_excel.py),
    but this function remains available for direct Excel loading.

    Args:
        file_path:  Path to the .xlsx file.
        sheet_name: Name of the sheet inside the Excel workbook.

    Returns:
        pd.DataFrame with the raw dataset.
    """
    path = Path(file_path)

    logger.info(f"Loading Excel from: {path} (sheet: '{sheet_name}')")

    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    try:
        df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    except Exception as exc:
        raise ValueError(f"Failed to read Excel file: {exc}") from exc

    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns from Excel")
    return df


def get_dataset_info(df: pd.DataFrame) -> dict:
    """
    Return basic structural information about a loaded DataFrame.

    This is a quick sanity-check, NOT a full profile.
    The full profile is produced by src/profiling/profiler.py.

    Args:
        df: Any loaded DataFrame.

    Returns:
        dict with shape, columns, dtypes, and memory usage.
    """
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
    }
