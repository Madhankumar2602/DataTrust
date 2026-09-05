"""
validity.py — Validity validation check for DataTrust.

What is validity?
─────────────────
Validity ensures data conforms to business rules and expected ranges.
For example, a price shouldn't be negative, and a date shouldn't be in the future.

Business logic applied:
  - UnitPrice: Must be >= 0. Negative prices are a FAIL. Zero prices are WARNING.
  - Quantity: Must not be zero.
    * Positive is a purchase.
    * Negative is a cancellation (IF InvoiceNo starts with 'C').
    * Negative WITHOUT 'C' in InvoiceNo is a FAIL.
  - InvoiceDate: Must be a valid date parseable by pandas.
"""

from __future__ import annotations

import pandas as pd

from src.logger import get_logger
from src.quality.base import BaseCheck, CheckResult, CheckStatus, Severity

logger = get_logger(__name__)


class ValidityCheck(BaseCheck):
    """
    Checks for business-logic validity across the dataset.

    Usage:
        from src.quality.validity import ValidityCheck
        results = ValidityCheck().run(df)
    """

    name = "ValidityCheck"
    category = "validity"

    def run(self, df: pd.DataFrame) -> list[CheckResult]:
        logger.info("Running ValidityCheck ...")
        results = []
        total_rows = len(df)

        if total_rows == 0:
            return results

        if "UnitPrice" in df.columns:
            results.append(self._check_unit_price(df, total_rows))

        if "Quantity" in df.columns and "InvoiceNo" in df.columns:
            results.extend(self._check_quantity(df, total_rows))

        if "InvoiceDate" in df.columns:
            results.append(self._check_invoice_date(df, total_rows))

        passed = sum(1 for r in results if r.passed)
        logger.info(f"  ValidityCheck: {passed}/{len(results)} rules passed")
        return results

    def _check_unit_price(self, df: pd.DataFrame, total_rows: int) -> CheckResult:
        price = df["UnitPrice"]
        # Convert to numeric in case of type mismatch (coercing errors to NaN)
        price_num = pd.to_numeric(price, errors="coerce")

        negative_count = int((price_num < 0).sum())
        zero_count = int((price_num == 0).sum())

        if negative_count > 0:
            return CheckResult(
                check_name="unit_price_validity",
                category=self.category,
                status=CheckStatus.FAIL,
                severity=Severity.HIGH,
                message=f"UnitPrice is negative in {negative_count} rows. Prices must be >= 0.",
                affected_rows=negative_count,
                affected_pct=round(negative_count / total_rows * 100, 2)
            )

        if zero_count > 0:
            return CheckResult(
                check_name="unit_price_validity",
                category=self.category,
                status=CheckStatus.WARNING,
                severity=Severity.LOW,
                message=(
                    f"UnitPrice is zero in {zero_count} rows. "
                    "This may indicate free items or missing data."
                ),
                affected_rows=zero_count,
                affected_pct=round(zero_count / total_rows * 100, 2)
            )

        return CheckResult(
            check_name="unit_price_validity",
            category=self.category,
            status=CheckStatus.PASS,
            severity=Severity.INFO,
            message="All UnitPrice values are > 0."
        )

    def _check_quantity(self, df: pd.DataFrame, total_rows: int) -> list[CheckResult]:
        results = []
        qty = pd.to_numeric(df["Quantity"], errors="coerce")
        inv = df["InvoiceNo"].astype(str)

        # 1. Zero quantity check
        zero_count = int((qty == 0).sum())
        if zero_count > 0:
            results.append(CheckResult(
                check_name="quantity_zero",
                category=self.category,
                status=CheckStatus.FAIL,
                severity=Severity.MEDIUM,
                message=(
                    f"Quantity is zero in {zero_count} rows. "
                    "Transactions must have non-zero quantity."
                ),
                affected_rows=zero_count,
                affected_pct=round(zero_count / total_rows * 100, 2)
            ))
        else:
            results.append(CheckResult(
                check_name="quantity_zero",
                category=self.category,
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message="No zero-quantity rows found."
            ))

        # 2. Cancellation logic check
        is_cancel = inv.str.startswith("C", na=False)
        is_negative = qty < 0

        # UCI contains a set of negative, zero-price rows with no CustomerID.
        # Many also have no description. They look like administrative adjustments, not sales or
        # normal C-prefixed cancellations. They remain visible as a warning,
        # but are not treated as definite invalid data without source-system
        # documentation proving otherwise.
        is_adjustment = pd.Series(False, index=df.index)
        adjustment_columns = {"UnitPrice", "CustomerID"}
        if adjustment_columns.issubset(df.columns):
            zero_price = pd.to_numeric(df["UnitPrice"], errors="coerce").eq(0)
            missing_customer = df["CustomerID"].isna()
            is_adjustment = is_negative & ~is_cancel & zero_price & missing_customer

        # Negative qty but NOT a cancellation or documented adjustment pattern.
        invalid_negative = int((is_negative & ~is_cancel & ~is_adjustment).sum())
        adjustment_count = int(is_adjustment.sum())
        # Cancellation but NOT a negative qty (unexpected)
        invalid_cancellation = int((is_cancel & ~is_negative).sum())

        total_invalid_qty = invalid_negative + invalid_cancellation

        if total_invalid_qty > 0:
            results.append(CheckResult(
                check_name="quantity_cancellation_logic",
                category=self.category,
                status=CheckStatus.FAIL,
                severity=Severity.HIGH,
                message=(
                    f"Found {invalid_negative} negative quantities without 'C' invoice prefix, "
                    f"and {invalid_cancellation} 'C' invoices without negative quantity."
                ),
                affected_rows=total_invalid_qty,
                affected_pct=round(total_invalid_qty / total_rows * 100, 2)
            ))
        elif adjustment_count > 0:
            results.append(CheckResult(
                check_name="quantity_cancellation_logic",
                category=self.category,
                status=CheckStatus.WARNING,
                severity=Severity.MEDIUM,
                message=(
                    f"Found {adjustment_count} negative, zero-price rows "
                    "without a 'C' invoice prefix. "
                    "They match an administrative-adjustment pattern and should be investigated."
                ),
                affected_rows=adjustment_count,
                affected_pct=round(adjustment_count / total_rows * 100, 2),
                metadata={"administrative_adjustment_rows": adjustment_count},
            ))
        else:
            cancellations = int(is_cancel.sum())
            results.append(CheckResult(
                check_name="quantity_cancellation_logic",
                category=self.category,
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message=(
                    "Cancellation logic is consistent. "
                    f"Found {cancellations} valid cancellations."
                ),
                metadata={"valid_cancellations": cancellations}
            ))

        return results

    def _check_invoice_date(self, df: pd.DataFrame, total_rows: int) -> CheckResult:
        # ``format='mixed'`` handles the source CSV's date strings and avoids
        # pandas guessing a single format from the first value.
        dates = pd.to_datetime(df["InvoiceDate"], errors="coerce", format="mixed")
        # We only count it as invalid if the original value was NOT null
        # (Missing values are handled by CompletenessCheck, not ValidityCheck)
        original_missing = df["InvoiceDate"].isnull()
        parse_errors = int((dates.isnull() & ~original_missing).sum())

        if parse_errors > 0:
            return CheckResult(
                check_name="invoice_date_validity",
                category=self.category,
                status=CheckStatus.FAIL,
                severity=Severity.HIGH,
                message=f"Failed to parse {parse_errors} InvoiceDate values as datetimes.",
                affected_rows=parse_errors,
                affected_pct=round(parse_errors / total_rows * 100, 2)
            )

        return CheckResult(
            check_name="invoice_date_validity",
            category=self.category,
            status=CheckStatus.PASS,
            severity=Severity.INFO,
            message="All InvoiceDate values parsed successfully."
        )
