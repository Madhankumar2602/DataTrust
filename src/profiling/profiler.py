"""
profiler.py — Dataset profiling for DataTrust.

What is "data profiling"?
──────────────────────────
Before cleaning or validating data, you need to UNDERSTAND it.
Profiling answers:
  - How big is this dataset?
  - What columns does it have?
  - Are there missing values?  How many?
  - Are there duplicates?
  - What are the value ranges?
  - What is the date range of the data?
  - Are there business-logic anomalies (negative prices, cancelled orders)?

Why profile BEFORE cleaning?
──────────────────────────────
If you clean first, you lose the ability to report on the original state.
The profile is a "snapshot" of the data AS RECEIVED — it tells you exactly
what the data quality problems are, which informs your validation rules.

This profiler is intentionally simple and transparent.
No magic. Every number can be traced to the Pandas operation that produced it.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)


class DataProfiler:
    """
    Profiles a Pandas DataFrame and produces a structured report.

    Usage:
        from src.profiling.profiler import DataProfiler
        profiler = DataProfiler(df, dataset_name="UCI Online Retail")
        report = profiler.run()
    """

    def __init__(self, df: pd.DataFrame, dataset_name: str = "Unknown Dataset") -> None:
        """
        Args:
            df:           The raw, unmodified DataFrame to profile.
            dataset_name: A human-readable name used in the report.
        """
        self.df = df.copy()  # Never mutate the caller's DataFrame
        self.dataset_name = dataset_name
        self.profiled_at = datetime.now(timezone.utc).isoformat()
        logger.info(f"DataProfiler initialised for: {dataset_name}")

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """
        Run all profiling steps and return a complete profile report.

        Returns:
            A dict that can be serialised to JSON and saved as a report.
        """
        logger.info("Starting dataset profiling ...")

        report: dict[str, Any] = {
            "dataset_name": self.dataset_name,
            "profiled_at": self.profiled_at,
            "overview": self._profile_overview(),
            "columns": self._profile_columns(),
            "missing_values": self._profile_missing_values(),
            "duplicates": self._profile_duplicates(),
            "date_analysis": self._profile_dates(),
            "business_analysis": self._profile_business_rules(),
            "quality_flags": self._generate_quality_flags(),
        }

        logger.info("Profiling complete.")
        return report

    # ── Private profiling steps ───────────────────────────────────────────────

    def _profile_overview(self) -> dict[str, Any]:
        """High-level structural overview of the dataset."""
        logger.info("  → Overview ...")
        return {
            "rows": int(len(self.df)),
            "columns": int(len(self.df.columns)),
            "column_names": list(self.df.columns),
            "memory_usage_mb": round(
                self.df.memory_usage(deep=True).sum() / 1024 / 1024, 2
            ),
        }

    def _profile_columns(self) -> list[dict[str, Any]]:
        """
        Per-column statistics.

        For each column we record:
          - data type
          - null count and percentage
          - unique value count
          - sample values (first 5 distinct values)
          - numeric stats (min, max, mean, std) if applicable
        """
        logger.info("  → Column profiles ...")
        total_rows = len(self.df)
        profiles = []

        for col in self.df.columns:
            series = self.df[col]
            null_count = int(series.isnull().sum())
            unique_count = int(series.nunique(dropna=True))

            col_profile: dict[str, Any] = {
                "name": col,
                "dtype": str(series.dtype),
                "null_count": null_count,
                "null_percentage": round(null_count / total_rows * 100, 2),
                "unique_count": unique_count,
                "sample_values": [
                    str(v) for v in series.dropna().unique()[:5].tolist()
                ],
            }

            # Numeric statistics
            if pd.api.types.is_numeric_dtype(series):
                col_profile["stats"] = {
                    "min": _safe_float(series.min()),
                    "max": _safe_float(series.max()),
                    "mean": _safe_float(series.mean()),
                    "std": _safe_float(series.std()),
                    "median": _safe_float(series.median()),
                }

            profiles.append(col_profile)

        return profiles

    def _profile_missing_values(self) -> dict[str, Any]:
        """
        Aggregated missing-value analysis across the entire dataset.

        Returns both a column-by-column breakdown AND a total summary.
        """
        logger.info("  → Missing values ...")
        total_rows = len(self.df)
        total_cells = total_rows * len(self.df.columns)

        per_column: dict[str, Any] = {}
        for col in self.df.columns:
            missing = int(self.df[col].isnull().sum())
            if missing > 0:
                per_column[col] = {
                    "missing_count": missing,
                    "missing_percentage": round(missing / total_rows * 100, 2),
                }

        total_missing = int(self.df.isnull().sum().sum())

        return {
            "total_missing_cells": total_missing,
            "total_missing_percentage": round(total_missing / total_cells * 100, 2),
            "columns_with_missing": len(per_column),
            "per_column": per_column,
        }

    def _profile_duplicates(self) -> dict[str, Any]:
        """
        Detect fully duplicated rows (every column identical).

        Note: The UCI dataset may have partial duplicates (same invoice,
        different product). This check looks for EXACT duplicates only.
        """
        logger.info("  → Duplicate rows ...")
        dup_mask = self.df.duplicated()
        dup_count = int(dup_mask.sum())

        return {
            "duplicate_row_count": dup_count,
            "duplicate_percentage": round(dup_count / len(self.df) * 100, 2),
            "unique_rows": int(len(self.df) - dup_count),
        }

    def _profile_dates(self) -> dict[str, Any]:
        """
        Analyse the InvoiceDate column to understand the data's time range.

        The UCI dataset spans 2010-12-01 to 2011-12-09.
        """
        logger.info("  → Date range ...")

        if "InvoiceDate" not in self.df.columns:
            return {"error": "InvoiceDate column not found"}

        # Parse dates — the CSV format is '12/1/2010 8:26'
        dates = pd.to_datetime(self.df["InvoiceDate"], errors="coerce")
        valid_dates = dates.dropna()

        if valid_dates.empty:
            return {"error": "No valid dates could be parsed"}

        min_date = valid_dates.min()
        max_date = valid_dates.max()
        span_days = (max_date - min_date).days

        # Daily transaction counts
        daily_counts = (
            valid_dates.dt.date.value_counts().sort_index()
        )

        return {
            "earliest_date": min_date.isoformat(),
            "latest_date": max_date.isoformat(),
            "date_span_days": span_days,
            "invalid_dates": int(dates.isnull().sum()),
            "daily_volume": {
                "min_transactions_per_day": int(daily_counts.min()),
                "max_transactions_per_day": int(daily_counts.max()),
                "mean_transactions_per_day": round(float(daily_counts.mean()), 1),
                "total_trading_days": int(len(daily_counts)),
            },
        }

    def _profile_business_rules(self) -> dict[str, Any]:
        """
        Dataset-specific business analysis for the UCI Online Retail data.

        Key domain knowledge:
          - Invoices starting with 'C' are CANCELLATIONS (not errors).
            Cancellations have negative Quantity values — this is EXPECTED.
          - Normal transactions should have Quantity > 0 and UnitPrice > 0.
          - CustomerID is optional (guest/anonymous purchases exist).
        """
        logger.info("  → Business analysis ...")
        result: dict[str, Any] = {}

        # ── Customers ────────────────────────────────────────────────────────
        if "CustomerID" in self.df.columns:
            result["customers"] = {
                "unique_customers": int(self.df["CustomerID"].nunique(dropna=True)),
                "rows_without_customer_id": int(self.df["CustomerID"].isnull().sum()),
            }

        # ── Products ─────────────────────────────────────────────────────────
        if "StockCode" in self.df.columns:
            result["products"] = {
                "unique_products": int(self.df["StockCode"].nunique()),
            }

        # ── Countries ────────────────────────────────────────────────────────
        if "Country" in self.df.columns:
            country_counts = self.df["Country"].value_counts()
            result["geography"] = {
                "unique_countries": int(self.df["Country"].nunique()),
                "top_5_countries": {
                    k: int(v) for k, v in country_counts.head(5).items()
                },
            }

        # ── Invoices ─────────────────────────────────────────────────────────
        if "InvoiceNo" in self.df.columns:
            invoice_str = self.df["InvoiceNo"].astype(str)
            cancelled_mask = invoice_str.str.startswith("C")
            result["invoices"] = {
                "unique_invoices": int(self.df["InvoiceNo"].nunique()),
                "cancellation_invoices": int(cancelled_mask.sum()),
                "cancellation_percentage": round(
                    cancelled_mask.sum() / len(self.df) * 100, 2
                ),
            }

        # ── Quantity ─────────────────────────────────────────────────────────
        if "Quantity" in self.df.columns:
            qty = self.df["Quantity"]
            neg_qty = qty < 0
            zero_qty = qty == 0
            # Negative quantities on NON-cancellation rows = potential issue
            non_cancel = ~self.df["InvoiceNo"].astype(str).str.startswith("C")
            suspicious_negative = (
                (neg_qty & non_cancel).sum() if "InvoiceNo" in self.df.columns else 0
            )

            result["quantity"] = {
                "negative_quantity_rows": int(neg_qty.sum()),
                "zero_quantity_rows": int(zero_qty.sum()),
                "negative_on_non_cancellation": int(suspicious_negative),
                "min_quantity": int(qty.min()),
                "max_quantity": int(qty.max()),
            }

        # ── Unit Price ───────────────────────────────────────────────────────
        if "UnitPrice" in self.df.columns:
            price = self.df["UnitPrice"]
            result["unit_price"] = {
                "zero_price_rows": int((price == 0).sum()),
                "negative_price_rows": int((price < 0).sum()),
                "min_price": round(float(price.min()), 2),
                "max_price": round(float(price.max()), 2),
                "mean_price": round(float(price.mean()), 2),
            }

        return result

    def _generate_quality_flags(self) -> list[dict[str, str]]:
        """
        Return a list of high-level quality observations (NOT scores).

        These are human-readable flags that appear at the top of the report.
        The actual scoring happens in src/scoring/scorer.py (Phase 4).
        """
        flags = []
        df = self.df

        # Missing CustomerID
        if "CustomerID" in df.columns:
            missing_pct = df["CustomerID"].isnull().mean() * 100
            if missing_pct > 20:
                flags.append({
                    "flag": "HIGH_MISSING_CUSTOMER_ID",
                    "severity": "WARNING",
                    "message": f"{missing_pct:.1f}% of rows have no CustomerID.",
                    "note": "This is expected for guest purchases in retail data.",
                })

        # Duplicate rows
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            flags.append({
                "flag": "DUPLICATE_ROWS_DETECTED",
                "severity": "WARNING",
                "message": f"{dup_count:,} fully duplicate rows found.",
            })

        # Expected columns check
        missing_cols = [
            c for c in settings.EXPECTED_COLUMNS if c not in df.columns
        ]
        extra_cols = [
            c for c in df.columns if c not in settings.EXPECTED_COLUMNS
        ]
        if missing_cols:
            flags.append({
                "flag": "MISSING_EXPECTED_COLUMNS",
                "severity": "CRITICAL",
                "message": f"Expected columns not found: {missing_cols}",
            })
        if extra_cols:
            flags.append({
                "flag": "UNEXPECTED_EXTRA_COLUMNS",
                "severity": "INFO",
                "message": f"Extra columns present: {extra_cols}",
            })

        if not flags:
            flags.append({
                "flag": "NO_CRITICAL_FLAGS",
                "severity": "OK",
                "message": "No critical structural issues detected.",
            })

        return flags

    # ── Report output helpers ─────────────────────────────────────────────────

    def save_report(self, report: dict[str, Any], output_path: str | Path | None = None) -> Path:
        """
        Save the profile report as a JSON file.

        Args:
            report:      The dict returned by run().
            output_path: Where to save. Defaults to reports/profile_<timestamp>.json

        Returns:
            The Path where the file was saved.
        """
        settings.ensure_directories()

        if output_path is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            output_path = Path(settings.REPORTS_PATH) / f"profile_{timestamp}.json"

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Profile report saved to: {output_path}")
        return output_path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(value: Any) -> float | None:
    """Convert numpy scalar to Python float; return None for NaN/Inf."""
    try:
        v = float(value)
        return None if (np.isnan(v) or np.isinf(v)) else round(v, 4)
    except (TypeError, ValueError):
        return None
