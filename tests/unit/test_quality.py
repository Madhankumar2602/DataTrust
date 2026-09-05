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


@pytest.fixture
def stored_df() -> pd.DataFrame:
    """The same data as it comes back from the retail_transactions table.

    Column names and dtypes match what `pd.read_sql("SELECT * FROM
    retail_transactions")` actually returns after the ETL load.
    """
    return pd.DataFrame({
        "transaction_id": [1, 2],
        "invoice_no": ["536365", "536366"],
        "stock_code": ["85123A", "71053"],
        "description": ["WHITE HANGING HEART", "WHITE METAL LANTERN"],
        "quantity": [6.0, 6.0],
        "invoice_date": ["2010-12-01T08:26:00", "2010-12-01T08:28:00"],
        "unit_price": [2.55, 3.39],
        "customer_id": [17850.0, 17850.0],
        "country": ["United Kingdom", "United Kingdom"],
        "is_cancellation": [0, 0],
        "revenue": [15.3, 20.34],
        "loaded_at": ["2026-01-01T00:00:00", "2026-01-01T00:00:00"],
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
    df["Quantity"] = df["Quantity"].astype(float)  # Expected int64
    results = SchemaCheck().run(df)

    dtype_check = next(r for r in results if r.check_name == "dtype_compatibility")
    assert dtype_check.status == CheckStatus.WARNING


def test_schema_uses_configured_contract(clean_df):
    """The default schema contract must remain centrally configurable."""
    check = SchemaCheck()
    assert check.expected_columns == settings.EXPECTED_COLUMNS
    assert check.expected_dtypes == settings.EXPECTED_DTYPES
    assert all(result.passed for result in check.run(clean_df))


# ── Stored (retail_transactions) representation ──────────────────────────────
# Regression cover for a defect where the Airflow quality task validated data
# read back from MySQL against the source column names, reported all 8 columns
# as missing, and forced every scheduled run's health score to 0.


def test_schema_accepts_stored_representation(stored_df):
    """Data read back from retail_transactions must not look entirely missing."""
    results = SchemaCheck().run(stored_df)

    presence = next(r for r in results if r.check_name == "column_presence")
    assert presence.status == CheckStatus.PASS
    assert presence.metadata["representation"] == "stored"

    unexpected = next(r for r in results if r.check_name == "unexpected_columns")
    assert unexpected.status == CheckStatus.PASS

    dtypes = next(r for r in results if r.check_name == "dtype_compatibility")
    assert dtypes.status == CheckStatus.PASS


def test_stored_representation_does_not_zero_the_health_score(stored_df):
    """The end-to-end defect: a valid stored snapshot scored 0.0/Critical."""
    from src.scoring.scorer import HealthScorer

    report = QualityEngine("stored snapshot").run(stored_df)
    score_report = HealthScorer().calculate_score(report)

    assert "failure_reason" not in score_report
    assert score_report["score"] > 0.0
    assert score_report["status"] != "Critical"


def test_source_representation_still_validates_source_columns(clean_df):
    """The pre-existing source behaviour must be untouched."""
    results = SchemaCheck().run(clean_df)

    presence = next(r for r in results if r.check_name == "column_presence")
    assert presence.status == CheckStatus.PASS
    assert presence.metadata["representation"] == "source"
    assert all(r.passed for r in results)


def test_missing_stored_column_still_fails(stored_df):
    """A genuinely absent stored column must still be reported as missing."""
    df = stored_df.drop(columns=["unit_price"])

    presence = next(
        r for r in SchemaCheck().run(df) if r.check_name == "column_presence"
    )

    assert presence.status == CheckStatus.FAIL
    assert "unit_price" in presence.metadata["missing_columns"]


def test_unexpected_stored_column_still_reported(stored_df):
    """Unexpected columns keep their existing INFO/WARNING behaviour."""
    df = stored_df.copy()
    df["surprise_column"] = "x"

    unexpected = next(
        r for r in SchemaCheck().run(df) if r.check_name == "unexpected_columns"
    )

    assert unexpected.status == CheckStatus.INFO
    assert "surprise_column" in unexpected.metadata["unexpected_columns"]


def test_stored_dtype_drift_is_still_detected(stored_df):
    """Type drift in the stored representation must still surface."""
    df = stored_df.copy()
    df["unit_price"] = df["unit_price"].astype(str)

    dtypes = next(
        r for r in SchemaCheck().run(df) if r.check_name == "dtype_compatibility"
    )

    assert dtypes.status == CheckStatus.WARNING
    assert dtypes.metadata["mismatches"][0]["column"] == "unit_price"


def test_unrecognised_frame_falls_back_to_source_contract():
    """An unrecognisable frame must fail loudly, not select a passing schema."""
    df = pd.DataFrame({"foo": [1], "bar": [2]})

    presence = next(
        r for r in SchemaCheck().run(df) if r.check_name == "column_presence"
    )

    assert presence.status == CheckStatus.FAIL
    assert presence.metadata["representation"] == "source"
    assert presence.metadata["missing_columns"] == settings.EXPECTED_COLUMNS


def test_explicitly_configured_columns_override_detection(stored_df):
    """Injected expectations win; detection only applies when none are given."""
    check = SchemaCheck(expected_columns=["invoice_no", "definitely_absent"])

    presence = next(
        r for r in check.run(stored_df) if r.check_name == "column_presence"
    )

    assert presence.metadata["representation"] == "configured"
    assert presence.metadata["missing_columns"] == ["definitely_absent"]


def test_stored_representation_derives_from_one_column_map():
    """The stored contract is derived from the map the ETL loader writes with."""
    for source_column in settings.EXPECTED_COLUMNS:
        stored_column = settings.STORED_COLUMN_MAP[source_column]
        assert stored_column in settings.STORED_EXPECTED_COLUMNS

# ── Completeness Tests ───────────────────────────────────────────────────────


def test_completeness_guest_customer(clean_df):
    df = clean_df.copy()
    df.loc[0, "CustomerID"] = np.nan
    results = CompletenessCheck().run(df)

    cust_check = next(r for r in results if r.check_name == "CustomerID_completeness")
    assert cust_check.status == CheckStatus.INFO  # Business logic: Guest checkout is OK


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
    df.loc[0, "InvoiceNo"] = "536365"  # Not a 'C' invoice
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

# ── Stored-representation completeness and validity ──────────────────────────
# Regression cover for a defect where both checks matched source column names
# literally. On stored data CompletenessCheck applied the wrong null policy and
# ValidityCheck found no columns at all, returning zero results and leaving the
# validity and business_rules score dimensions permanently unearned.


def test_stored_completeness_treats_customer_id_as_guest_checkout(stored_df):
    """customer_id nulls are guest checkouts, exactly as CustomerID nulls are."""
    df = stored_df.copy()
    df.loc[0, "customer_id"] = np.nan

    results = CompletenessCheck().run(df)

    check = next(r for r in results if r.check_name == "customer_id_completeness")
    assert check.status == CheckStatus.INFO
    assert check.status != CheckStatus.FAIL


def test_stored_completeness_keeps_description_warning(stored_df):
    df = stored_df.copy()
    df.loc[0, "description"] = np.nan

    results = CompletenessCheck().run(df)

    check = next(r for r in results if r.check_name == "description_completeness")
    assert check.status == CheckStatus.WARNING


def test_stored_completeness_still_fails_required_columns(stored_df):
    """A required column keeps failing; only the documented exceptions are spared."""
    df = stored_df.copy()
    df.loc[0, "quantity"] = np.nan

    results = CompletenessCheck().run(df)

    check = next(r for r in results if r.check_name == "quantity_completeness")
    assert check.status == CheckStatus.FAIL


def test_stored_validity_rules_execute(stored_df):
    """The rules used to find no columns and silently return nothing."""
    results = ValidityCheck().run(stored_df)

    assert results, "ValidityCheck must produce results for stored data"
    assert {r.check_name for r in results} == {
        "unit_price_validity",
        "quantity_zero",
        "quantity_cancellation_logic",
        "invoice_date_validity",
    }


def test_stored_negative_unit_price_is_detected(stored_df):
    df = stored_df.copy()
    df.loc[0, "unit_price"] = -5.0

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "unit_price_validity")
    assert check.status == CheckStatus.FAIL
    assert "unit_price" in check.message


def test_stored_zero_quantity_is_detected(stored_df):
    df = stored_df.copy()
    df.loc[0, "quantity"] = 0.0

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "quantity_zero")
    assert check.status == CheckStatus.FAIL


def test_stored_unparseable_invoice_date_is_detected(stored_df):
    df = stored_df.copy()
    df.loc[0, "invoice_date"] = "Not a date"

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "invoice_date_validity")
    assert check.status == CheckStatus.FAIL


def test_stored_negative_quantity_without_c_prefix_fails(stored_df):
    """The cancellation business logic must hold on stored names too."""
    df = stored_df.copy()
    df.loc[0, "invoice_no"] = "536365"
    df.loc[0, "quantity"] = -6.0

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert check.status == CheckStatus.FAIL


def test_stored_valid_cancellation_passes(stored_df):
    df = stored_df.copy()
    df.loc[0, "invoice_no"] = "C536365"
    df.loc[0, "quantity"] = -6.0

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert check.status == CheckStatus.PASS


def test_stored_administrative_adjustment_carve_out_survives(stored_df):
    """The zero-price, no-customer carve-out must not be lost on stored data."""
    df = stored_df.copy()
    df.loc[0, ["quantity", "unit_price", "customer_id"]] = [-6.0, 0.0, np.nan]

    results = ValidityCheck().run(df)

    check = next(r for r in results if r.check_name == "quantity_cancellation_logic")
    assert check.status == CheckStatus.WARNING
    assert check.metadata["administrative_adjustment_rows"] == 1


def test_cancellation_check_name_is_stable_across_representations(stored_df, clean_df):
    """The scorer maps this exact name onto business_rules; it must not drift."""
    stored_names = {r.check_name for r in ValidityCheck().run(stored_df)}
    source_names = {r.check_name for r in ValidityCheck().run(clean_df)}

    assert "quantity_cancellation_logic" in stored_names
    assert stored_names == source_names


def test_engine_explicit_stored_representation_runs_both_checks(stored_df):
    df = stored_df.copy()
    df.loc[0, "customer_id"] = np.nan

    report = QualityEngine("stored", representation="stored").run(df)

    customer = next(
        r for r in report["results"] if r["check_name"] == "customer_id_completeness"
    )
    assert customer["status"] == "INFO"
    assert any(r["category"] == "validity" for r in report["results"])


def test_engine_explicit_source_representation_is_unchanged(clean_df):
    df = clean_df.copy()
    df.loc[0, "CustomerID"] = np.nan

    report = QualityEngine("source", representation="source").run(df)

    customer = next(
        r for r in report["results"] if r["check_name"] == "CustomerID_completeness"
    )
    assert customer["status"] == "INFO"
    assert any(r["category"] == "validity" for r in report["results"])


def test_engine_auto_representation_handles_both(clean_df, stored_df):
    source_report = QualityEngine("auto-source").run(clean_df)
    stored_report = QualityEngine("auto-stored").run(stored_df)

    assert source_report["contract"]["representation"] == "source"
    assert stored_report["contract"]["representation"] == "stored"
    for report in (source_report, stored_report):
        categories = {r["category"] for r in report["results"]}
        assert "validity" in categories
        assert "completeness" in categories


def test_stored_validity_and_business_rules_dimensions_are_earned(stored_df):
    """Both dimensions used to score 0/20 because no results ever reached them."""
    from src.scoring.scorer import HealthScorer

    report = QualityEngine("stored").run(stored_df)
    score_report = HealthScorer().calculate_score(report)

    assert score_report["category_scores"]["validity"]["score"] > 0.0
    assert score_report["category_scores"]["business_rules"]["score"] > 0.0
    assert "No validation results" not in (
        score_report["category_scores"]["validity"]["details"]
    )


def test_transformed_frame_behaviour_is_unchanged(clean_df):
    """The transformer's extra columns must still resolve as source data."""
    df = clean_df.copy()
    df["IsCancellation"] = [False, False]
    df["Revenue"] = [15.3, 20.34]

    report = QualityEngine("transformed").run(df)

    assert report["contract"]["representation"] == "source"
    customer = next(
        r for r in report["results"] if r["check_name"] == "CustomerID_completeness"
    )
    assert customer["status"] == "PASS"
    assert any(r["check_name"] == "unit_price_validity" for r in report["results"])


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
