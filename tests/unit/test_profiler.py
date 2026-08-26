"""
test_profiler.py — Unit tests for the DataProfiler class.

These tests use a small, hand-crafted DataFrame so they run instantly
without requiring the full 541,000-row UCI dataset.

Why write tests?
────────────────
When we later change the profiler code (e.g. add a new stat), tests
immediately tell us if we accidentally broke existing behaviour.

Run with:
    pytest tests/unit/test_profiler.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add project root to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.profiling.profiler import DataProfiler


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """
    A minimal DataFrame that mimics the UCI Online Retail structure.

    We include deliberate quality problems so we can assert they are detected:
      - Row 3: Missing CustomerID (guest purchase)
      - Row 4: Cancelled invoice (InvoiceNo starts with 'C', Quantity < 0)
      - Row 5: Exact duplicate of Row 1
    """
    return pd.DataFrame({
        "InvoiceNo":   ["536365", "536366", "536367", "C536368", "536365"],
        "StockCode":   ["85123A", "71053",  "84406B", "84406B",  "85123A"],
        "Description": ["WHITE HANGING HEART", "WHITE METAL LANTERN",
                        "CREAM CUPID HEARTS",  "CREAM CUPID HEARTS",
                        "WHITE HANGING HEART"],
        "Quantity":    [6,        6,         8,       -8,         6],
        "InvoiceDate": ["12/1/2010 8:26", "12/1/2010 8:26",
                        "12/1/2010 8:28", "12/1/2010 8:28", "12/1/2010 8:26"],
        "UnitPrice":   [2.55,    3.39,     2.75,     2.75,     2.55],
        "CustomerID":  [17850.0, 17850.0, 13047.0,  np.nan,  17850.0],
        "Country":     ["United Kingdom", "United Kingdom",
                        "United Kingdom", "United Kingdom", "United Kingdom"],
    })


@pytest.fixture
def profiler(sample_df) -> DataProfiler:
    return DataProfiler(sample_df, dataset_name="Test Dataset")


# ── Overview tests ────────────────────────────────────────────────────────────

class TestOverview:
    def test_row_count(self, profiler, sample_df):
        """Profiler must report the correct number of rows."""
        report = profiler.run()
        assert report["overview"]["rows"] == len(sample_df)

    def test_column_count(self, profiler, sample_df):
        """Profiler must report the correct number of columns."""
        report = profiler.run()
        assert report["overview"]["columns"] == len(sample_df.columns)

    def test_column_names(self, profiler, sample_df):
        """Column names in the report must match the DataFrame."""
        report = profiler.run()
        assert report["overview"]["column_names"] == list(sample_df.columns)


# ── Missing value tests ───────────────────────────────────────────────────────

class TestMissingValues:
    def test_detects_missing_customer_id(self, profiler):
        """CustomerID has 1 missing value in our fixture."""
        report = profiler.run()
        per_col = report["missing_values"]["per_column"]
        assert "CustomerID" in per_col
        assert per_col["CustomerID"]["missing_count"] == 1

    def test_missing_percentage_calculation(self, profiler):
        """1 missing out of 5 rows = 20.0%."""
        report = profiler.run()
        pct = report["missing_values"]["per_column"]["CustomerID"]["missing_percentage"]
        assert pct == 20.0

    def test_no_false_positives(self, profiler):
        """InvoiceNo has no missing values and must NOT appear in per_column."""
        report = profiler.run()
        per_col = report["missing_values"]["per_column"]
        assert "InvoiceNo" not in per_col


# ── Duplicate tests ───────────────────────────────────────────────────────────

class TestDuplicates:
    def test_detects_duplicate_row(self, profiler):
        """Row 5 is an exact copy of Row 1 — expect 1 duplicate."""
        report = profiler.run()
        assert report["duplicates"]["duplicate_row_count"] == 1

    def test_no_duplicates_on_clean_data(self):
        """A DataFrame with no duplicates should report 0."""
        clean_df = pd.DataFrame({
            "InvoiceNo": ["A", "B", "C"],
            "Quantity":  [1, 2, 3],
        })
        profiler = DataProfiler(clean_df, "Clean")
        report = profiler.run()
        assert report["duplicates"]["duplicate_row_count"] == 0


# ── Business analysis tests ───────────────────────────────────────────────────

class TestBusinessAnalysis:
    def test_cancellation_detection(self, profiler):
        """Invoice C536368 is a cancellation — expect 1 detected."""
        report = profiler.run()
        inv = report["business_analysis"]["invoices"]
        assert inv["cancellation_invoices"] == 1

    def test_negative_quantity_count(self, profiler):
        """Row 4 has Quantity=-8 — expect 1 negative quantity row."""
        report = profiler.run()
        qty = report["business_analysis"]["quantity"]
        assert qty["negative_quantity_rows"] == 1

    def test_unique_customers(self, profiler):
        """Fixture has 2 unique non-null customers: 17850 and 13047."""
        report = profiler.run()
        customers = report["business_analysis"]["customers"]
        assert customers["unique_customers"] == 2


# ── Date analysis tests ───────────────────────────────────────────────────────

class TestDateAnalysis:
    def test_date_range_parsed(self, profiler):
        """Date analysis must succeed without errors."""
        report = profiler.run()
        assert "error" not in report["date_analysis"]

    def test_earliest_date(self, profiler):
        """All dates in fixture are 2010-12-01; earliest must reflect this."""
        report = profiler.run()
        assert "2010-12-01" in report["date_analysis"]["earliest_date"]
