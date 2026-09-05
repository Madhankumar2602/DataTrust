"""
completeness.py — Completeness validation check for DataTrust.

What is completeness?
─────────────────────
Completeness measures whether expected data is missing (nulls, NaNs).

However, NOT ALL MISSING DATA IS AN ERROR.
In the UCI dataset:
- CustomerID is missing for 24.9% of rows. These represent guest purchases.
  We must report them as INFO, not FAIL, so we don't artificially lower the
  data quality score.
- Description missing is unusual (0.3% of rows) and is reported as a WARNING.
- Core fields like InvoiceNo or Quantity missing would be CRITICAL.
"""

from __future__ import annotations

import pandas as pd

from src.logger import get_logger
from src.quality.base import BaseCheck, CheckResult, CheckStatus, Severity
from src.quality.representation import columns_for

logger = get_logger(__name__)


class CompletenessCheck(BaseCheck):
    """
    Checks for missing values (nulls) across columns.

    Business logic applied:
      - CustomerID: Nulls are expected (guest checkouts). INFO.
      - Description: Nulls are unexpected but not fatal. WARNING.
      - Other columns: Nulls are not expected. FAIL.

    The policy is stated against canonical source column names and applied to
    whichever names the frame actually uses, so stored data read back from
    retail_transactions gets the same business treatment as the source data.

    Usage:
        from src.quality.completeness import CompletenessCheck
        results = CompletenessCheck().run(df)
    """

    name = "CompletenessCheck"
    category = "completeness"

    def __init__(self, columns: dict[str, str] | None = None) -> None:
        """
        Args:
            columns: Canonical source column name -> the name it carries in the
                     frames this check will see. QualityEngine supplies it when
                     the representation is already known; left unset, each frame
                     is resolved on its own.
        """
        self._columns = columns

    def run(self, df: pd.DataFrame) -> list[CheckResult]:
        logger.info("Running CompletenessCheck ...")
        total_rows = len(df)
        if total_rows == 0:
            return []

        columns = self._columns or columns_for(df)
        # Nulls here are a documented business reality, not a data defect.
        guest_customer_column = columns.get("CustomerID")
        optional_description_column = columns.get("Description")

        results = []
        for col in df.columns:
            missing_count = int(df[col].isnull().sum())
            if missing_count == 0:
                results.append(
                    CheckResult(
                        check_name=f"{col}_completeness",
                        category=self.category,
                        status=CheckStatus.PASS,
                        severity=Severity.INFO,
                        message=f"{col} is 100% complete.",
                    )
                )
                continue

            missing_pct = round(missing_count / total_rows * 100, 2)

            # Apply dataset-specific business logic
            if col == guest_customer_column:
                results.append(
                    CheckResult(
                        check_name=f"{col}_completeness",
                        category=self.category,
                        status=CheckStatus.INFO,
                        severity=Severity.INFO,
                        message=(
                            f"{col} is missing {missing_count} values ({missing_pct}%). "
                            "This is expected for guest transactions."
                        ),
                        affected_rows=missing_count,
                        affected_pct=missing_pct,
                    )
                )
            elif col == optional_description_column:
                results.append(
                    CheckResult(
                        check_name=f"{col}_completeness",
                        category=self.category,
                        status=CheckStatus.WARNING,
                        severity=Severity.LOW,
                        message=f"{col} is missing {missing_count} values ({missing_pct}%).",
                        affected_rows=missing_count,
                        affected_pct=missing_pct,
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_name=f"{col}_completeness",
                        category=self.category,
                        status=CheckStatus.FAIL,
                        severity=Severity.HIGH,
                        message=f"{col} is missing {missing_count} values ({missing_pct}%).",
                        affected_rows=missing_count,
                        affected_pct=missing_pct,
                    )
                )

        passed = sum(1 for r in results if r.passed or r.status == CheckStatus.INFO)
        logger.info(f"  CompletenessCheck: {passed}/{len(results)} columns passed or acceptable")
        return results
