"""
test_quality.py — Unit tests for Phase 2 Data Quality Validation Engine.
"""

import pytest
import pandas as pd
import numpy as np

from src.quality.base import CheckStatus
from src.quality.schema import SchemaCheck
from src.quality.completeness import CompletenessCheck
from src.quality.validity import ValidityCheck
from src.quality.uniqueness import UniquenessCheck
from src.quality.engine import QualityEngine
from src.config import settings

# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_df() -> pd.DataFrame:
    """A perfectly clean DataFrame matching the expected schema."""
    return pd.DataFrame({
        "InvoiceNo": ["536365", "536366"],
        "StockCode": ["85123A", "71053"],
        "Description": ["WHITE HANGING HEART", "WHITE METAL LANTERN"],
        "Quantity": [6, 6],
        "InvoiceDate": ["12/1/2010 8:26", "12/1/2010 8:28"],
        "UnitPrice": [2.55, 3.39],
        "CustomerID": [17850.0, 17850.0],
        "Country": ["United Kingdom", "United Kingdom"],
    })

# ── Schema Tests ─────────────────────────────────────────────────────────────

def test_schema_clean(clean_df):
    results = SchemaCheck().run(clean_df)
    assert all(r.passed for r in results)

def test_schema_missing_column(clean_df):
    df = clean_df.drop(columns=["CustomerID"])
    results = SchemaCheck().run(df)
    
    # One check should FAIL (column_presence)
    presence_check = next(r for r in results if r.check_name == "column_presence")
    assert presence_check.status == CheckStatus.FAIL
    assert "CustomerID" in presence_check.metadata["missing_columns"]

def test_schema_unexpected_column(clean_df):
    df = clean_df.copy()
    df["ExtraCol"] = "test"
    results = SchemaCheck().run(df)
    
    # Should be INFO
    unexp_check = next(r for r in results if r.check_name == "unexpected_columns")
    assert unexp_check.status == CheckStatus.INFO
    assert "ExtraCol" in unexp_check.metadata["unexpected_columns"]

def test_schema_many_unexpected_columns_warn(clean_df):
    df = clean_df.assign(ExtraOne="x", ExtraTwo="x", ExtraThree="x", ExtraFour="x")
    results = SchemaCheck().run(df)
    unexp_check = next(r for r in results if r.check_name == "unexpected_columns")
    assert unexp_check.status == CheckStatus.WARNING

def test_schema_dtype_mismatch(clean_df):
    df = clean_df.copy()
    df["Quantity"] = df["Quantity"].astype(float) # Expected int64
    results = SchemaCheck().run(df)
    
    dtype_check = next(r for r in results if r.check_name == "dtype_compatibility")
    assert dtype_check.status == CheckStatus.WARNING

def test_schema_uses_configured_contract(clean_df):
    """The default schema contract must remain centrally configurable."""
    check = SchemaCheck()
    assert check.expected_columns == settings.EXPECTED_COLUMNS
    assert check.expected_dtypes == settings.EXPECTED_DTYPES
    assert all(result.passed for result in check.run(clean_df))

# ── Completeness Tests ───────────────────────────────────────────────────────

def test_completeness_guest_customer(clean_df):
    df = clean_df.copy()
    df.loc[0, "CustomerID"] = np.nan
    results = CompletenessCheck().run(df)
    
    cust_check = next(r for r in results if r.check_name == "CustomerID_completeness")
    assert cust_check.status == CheckStatus.INFO # Business logic: Guest checkout is OK

def test_completeness_missing_description(clean_df):
    df = clean_df.copy()
    df.loc[0, "Description"] = np.nan
    results = CompletenessCheck().run(df)
    
    desc_check = next(r for r in results if r.check_name == "Description_completeness")
    assert desc_check.status == CheckStatus.WARNING

def test_completeness_missing_critical(clean_df):
    df = clean_df.copy()
    df.loc[0, "Quantity"] = np.nan
    results = CompletenessCheck().run(df)
    
    qty_check = next(r for r in results if r.check_name == "Quantity_completeness")
    assert qty_check.status == CheckStatus.FAIL

# ── Validity Tests ───────────────────────────────────────────────────────────

def test_validity_negative_price(clean_df):
    df = clean_df.copy()
    df.loc[0, "UnitPrice"] = -5.0
    results = ValidityCheck().run(df)
    
    price_check = next(r for r in results if r.check_name == "unit_price_validity")
    assert price_check.status == CheckStatus.FAIL

def test_validity_zero_quantity(clean_df):
    df = clean_df.copy()
    df.loc[0, "Quantity"] = 0
    results = ValidityCheck().run(df)
    
    qty_check = next(r for r in results if r.check_name == "quantity_zero")
    assert qty_check.status == CheckStatus.FAIL

def test_validity_valid_cancellation(clean_df):
    df = clean_df.copy()
    df.loc[0, "InvoiceNo"] = "C536365"
    df.loc[0, "Quantity"] = -6
    results = ValidityCheck().run(df)
    
    logic_check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert logic_check.status == CheckStatus.PASS

def test_validity_invalid_negative_quantity(clean_df):
    df = clean_df.copy()
    df.loc[0, "InvoiceNo"] = "536365" # Not a 'C' invoice
    df.loc[0, "Quantity"] = -6
    results = ValidityCheck().run(df)
    
    logic_check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert logic_check.status == CheckStatus.FAIL

def test_validity_administrative_adjustment_is_warning(clean_df):
    """Known UCI-style negative zero-price adjustments are ambiguous, not a hard failure."""
    df = clean_df.copy()
    df.loc[0, ["Quantity", "UnitPrice", "CustomerID"]] = [-6, 0.0, np.nan]
    results = ValidityCheck().run(df)
    logic_check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert logic_check.status == CheckStatus.WARNING
    assert logic_check.metadata["administrative_adjustment_rows"] == 1

def test_validity_invalid_date(clean_df):
    df = clean_df.copy()
    df.loc[0, "InvoiceDate"] = "Not a date"
    results = ValidityCheck().run(df)
    
    date_check = next(r for r in results if r.check_name == "invoice_date_validity")
    assert date_check.status == CheckStatus.FAIL

def test_validity_positive_unit_price_passes(clean_df):
    results = ValidityCheck().run(clean_df)
    price_check = next(r for r in results if r.check_name == "unit_price_validity")
    assert price_check.status == CheckStatus.PASS

# ── Uniqueness Tests ─────────────────────────────────────────────────────────

def test_uniqueness_clean(clean_df):
    results = UniquenessCheck().run(clean_df)
    assert results[0].status == CheckStatus.PASS

def test_uniqueness_duplicate(clean_df):
    # Append the first row again
    df = pd.concat([clean_df, clean_df.iloc[[0]]], ignore_index=True)
    results = UniquenessCheck().run(df)
    assert results[0].status == CheckStatus.WARNING

# ── Engine Tests ─────────────────────────────────────────────────────────────

def test_engine_run(clean_df):
    engine = QualityEngine("Test Dataset")
    report = engine.run(clean_df)
    
    assert report["dataset_name"] == "Test Dataset"
    assert report["total_rows"] == 2
    assert "summary" in report
    assert "results" in report
    assert len(report["results"]) > 0
    assert {"check_name", "category", "status", "severity", "affected_rows",
            "affected_percentage", "message", "metadata"}.issubset(report["results"][0])
