"""
uniqueness.py — Uniqueness validation check for DataTrust.

What is uniqueness?
───────────────────
Uniqueness ensures that there are no duplicate records in the dataset.
In the UCI dataset, exact row-level duplicates are considered a WARNING, 
as it's possible (though unlikely) for a customer to buy the exact same item
in the exact same second, but it's more likely a system glitch.
"""

from __future__ import annotations

import pandas as pd

from src.logger import get_logger
from src.quality.base import BaseCheck, CheckResult, CheckStatus, Severity

logger = get_logger(__name__)


class UniquenessCheck(BaseCheck):
    """
    Checks for exact duplicate rows in the dataset.

    Usage:
        from src.quality.uniqueness import UniquenessCheck
        results = UniquenessCheck().run(df)
    """

    name = "UniquenessCheck"
    category = "uniqueness"

    def run(self, df: pd.DataFrame) -> list[CheckResult]:
        logger.info("Running UniquenessCheck ...")
        total_rows = len(df)
        
        if total_rows == 0:
            return []

        # Find EXACT duplicates (all columns match)
        dup_count = int(df.duplicated().sum())
        
        if dup_count == 0:
            result = CheckResult(
                check_name="duplicate_rows",
                category=self.category,
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message="No duplicate rows found."
            )
        else:
            dup_pct = round(dup_count / total_rows * 100, 2)
            result = CheckResult(
                check_name="duplicate_rows",
                category=self.category,
                status=CheckStatus.WARNING,
                severity=Severity.MEDIUM,
                message=f"Found {dup_count} exact duplicate rows ({dup_pct}%).",
                affected_rows=dup_count,
                affected_pct=dup_pct
            )

        logger.info(f"  UniquenessCheck: {result.status.value}")
        return [result]
