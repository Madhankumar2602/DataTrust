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

logger = get_logger(__name__)


class CompletenessCheck(BaseCheck):
    """
    Checks for missing values (nulls) across columns.

    Business logic applied:
      - CustomerID: Nulls are expected (guest checkouts). INFO.
      - Description: Nulls are unexpected but not fatal. WARNING.
      - Other columns: Nulls are not expected. FAIL.

    Usage:
        from src.quality.completeness import CompletenessCheck
        results = CompletenessCheck().run(df)
    """

    name = "CompletenessCheck"
    category = "completeness"

    def run(self, df: pd.DataFrame) -> list[CheckResult]:
        logger.info("Running CompletenessCheck ...")
        total_rows = len(df)
        if total_rows == 0:
            return []

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
            if col == "CustomerID":
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
            elif col == "Description":
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
