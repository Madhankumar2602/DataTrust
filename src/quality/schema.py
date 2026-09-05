"""
schema.py — Schema validation check for DataTrust.

What is schema validation?
──────────────────────────
A "schema" is the contract that describes what columns a dataset must have,
in what order, and what data types they must contain.

If the schema changes unexpectedly — for example a column is renamed from
"UnitPrice" to "Price" in an upstream system — every downstream query,
dashboard, and ML model that relies on "UnitPrice" will silently break.

Schema validation is the FIRST check to run because if the schema is wrong,
the completeness and validity checks may themselves crash or produce nonsense.

What we check here:
  1. Missing columns   — expected columns that are absent from the DataFrame.
  2. Unexpected columns — columns present that we did not expect.
  3. Data-type mismatches — columns whose dtype differs from the expected type.

UCI Online Retail expected schema
──────────────────────────────────
  InvoiceNo   → object  (string — may start with 'C' for cancellations)
  StockCode   → object  (string — 5-digit product code)
  Description → object  (string — product name)
  Quantity    → int64   (integer — negative for cancellations)
  InvoiceDate → object  (string — parsed separately in freshness checks)
  UnitPrice   → float64 (decimal — price per unit in GBP)
  CustomerID  → float64 (decimal — float because of NaN values; no int NaN in pandas)
  Country     → object  (string)
"""

from __future__ import annotations

import pandas as pd

from src.config import settings
from src.logger import get_logger
from src.quality.base import BaseCheck, CheckResult, CheckStatus, Severity
from src.quality.representation import STORED, detect_representation

logger = get_logger(__name__)


class SchemaCheck(BaseCheck):
    """
    Validates the DataFrame schema against the expected UCI Online Retail schema.

    Three sub-checks:
      1. column_presence  — Are all expected columns present?
      2. unexpected_cols  — Are there columns we did not expect?
      3. dtype_mismatch   — Do column dtypes match expectations?

    Usage:
        from src.quality.schema import SchemaCheck
        results = SchemaCheck().run(df)
    """

    name = "SchemaCheck"
    category = "schema"

    def __init__(
        self,
        expected_columns: list[str] | None = None,
        expected_dtypes: dict[str, str] | None = None,
    ) -> None:
        """
        Args:
            expected_columns: List of columns that MUST be present.
                              Defaults to settings.EXPECTED_COLUMNS.
            expected_dtypes:  Dict of {column: expected_dtype}.
                              Defaults to settings.EXPECTED_DTYPES.

        Passing either argument pins the check to those expectations. Passing
        neither lets `run` select the representation the DataFrame is actually
        in, because the same dataset is validated both as source columns and,
        after the ETL load, as the stored retail_transactions columns.
        """
        self._pinned = expected_columns is not None or expected_dtypes is not None
        self.expected_columns = expected_columns or list(settings.EXPECTED_COLUMNS)
        self.expected_dtypes = expected_dtypes or dict(settings.EXPECTED_DTYPES)

    def run(self, df: pd.DataFrame) -> list[CheckResult]:
        """Run all three schema sub-checks and return their results."""
        logger.info("Running SchemaCheck ...")
        representation, expected_columns, expected_dtypes = self._expectations_for(df)
        logger.info(f"  SchemaCheck: validating the {representation} representation")
        results = [
            self._check_missing_columns(df, expected_columns, representation),
            self._check_unexpected_columns(df, expected_columns),
            self._check_dtype_mismatches(df, expected_dtypes),
        ]
        passed = sum(1 for r in results if r.passed)
        logger.info(f"  SchemaCheck: {passed}/{len(results)} sub-checks passed")
        return results

    def _expectations_for(
        self, df: pd.DataFrame
    ) -> tuple[str, list[str], dict[str, str]]:
        """Choose which representation of the dataset this DataFrame holds.

        The pipeline validates the same data twice: as source columns before the
        load, and as stored retail_transactions columns when a later stage reads
        the table back. Comparing stored data against source names reported every
        column as missing, so the check now matches the names it was handed.
        Ties and unrecognised frames fall back to the source contract, so genuinely
        broken data still fails loudly instead of selecting itself a passing schema.
        """
        if self._pinned:
            return "configured", self.expected_columns, self.expected_dtypes

        if detect_representation(df) == STORED:
            return (
                "stored",
                list(settings.STORED_EXPECTED_COLUMNS),
                dict(settings.STORED_EXPECTED_DTYPES),
            )
        return "source", self.expected_columns, self.expected_dtypes

    # ── Sub-checks ────────────────────────────────────────────────────────────

    def _check_missing_columns(
        self,
        df: pd.DataFrame,
        expected_columns: list[str],
        representation: str,
    ) -> CheckResult:
        """
        Detect expected columns that are absent from the DataFrame.

        A missing column is CRITICAL — any downstream check referencing
        that column will crash or produce wrong results.
        """
        actual_cols = set(df.columns)
        missing = [c for c in expected_columns if c not in actual_cols]

        if not missing:
            return CheckResult(
                check_name="column_presence",
                category="schema",
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message="All expected columns are present.",
                metadata={"expected": expected_columns, "representation": representation},
            )

        return CheckResult(
            check_name="column_presence",
            category="schema",
            status=CheckStatus.FAIL,
            severity=Severity.CRITICAL,
            message=(
                f"{len(missing)} expected column(s) are missing: {missing}. "
                "Downstream checks will be unreliable."
            ),
            affected_rows=len(df),
            affected_pct=100.0,
            metadata={
                "missing_columns": missing,
                "expected": expected_columns,
                "actual": list(df.columns),
                "representation": representation,
            },
        )

    def _check_unexpected_columns(
        self, df: pd.DataFrame, expected_columns: list[str]
    ) -> CheckResult:
        """
        Detect columns in the DataFrame that we did not expect.

        Unexpected columns are not necessarily an error — they may be added
        intentionally. We report them as INFO so the team is aware.
        Severity escalates to WARNING if more than 3 unexpected columns appear,
        which might indicate a schema that has significantly drifted.
        """
        actual_cols = set(df.columns)
        expected_set = set(expected_columns)
        unexpected = sorted(actual_cols - expected_set)

        if not unexpected:
            return CheckResult(
                check_name="unexpected_columns",
                category="schema",
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message="No unexpected columns detected.",
            )

        # Escalate severity if many unexpected columns appear (possible schema drift)
        is_warning = len(unexpected) > settings.UNEXPECTED_COLUMN_WARNING_COUNT
        severity = Severity.MEDIUM if is_warning else Severity.LOW
        status = CheckStatus.WARNING if is_warning else CheckStatus.INFO

        return CheckResult(
            check_name="unexpected_columns",
            category="schema",
            status=status,
            severity=severity,
            message=(
                f"{len(unexpected)} unexpected column(s) found: {unexpected}. "
                "These may indicate schema drift or intentional additions."
            ),
            metadata={"unexpected_columns": unexpected},
        )

    def _check_dtype_mismatches(
        self, df: pd.DataFrame, expected_dtypes: dict[str, str]
    ) -> CheckResult:
        """
        Verify that each column's actual dtype matches the expected dtype.

        Why does this matter?
        If upstream sends CustomerID as 'object' (string) instead of 'float64',
        any arithmetic on it (computing average customer spend) will crash.

        We only check columns that are both expected AND present.
        """
        mismatches: list[dict] = []

        for col, expected_dtype in expected_dtypes.items():
            if col not in df.columns:
                continue  # Missing columns handled by _check_missing_columns

            actual_dtype = str(df[col].dtype)

            # Allow int64 ↔ int32 / int16 / int8 as equivalent (all are integers)
            if not _dtypes_compatible(actual_dtype, expected_dtype):
                mismatches.append({
                    "column": col,
                    "expected_dtype": expected_dtype,
                    "actual_dtype": actual_dtype,
                })

        if not mismatches:
            return CheckResult(
                check_name="dtype_compatibility",
                category="schema",
                status=CheckStatus.PASS,
                severity=Severity.INFO,
                message="All column data types match expectations.",
                metadata={"checked_columns": list(expected_dtypes.keys())},
            )

        return CheckResult(
            check_name="dtype_compatibility",
            category="schema",
            status=CheckStatus.WARNING,
            severity=Severity.MEDIUM,
            message=(
                f"{len(mismatches)} column(s) have unexpected data types. "
                "This may cause arithmetic or join operations to fail."
            ),
            metadata={"mismatches": mismatches},
        )


# ── Helper ────────────────────────────────────────────────────────────────────

def _dtypes_compatible(actual: str, expected: str) -> bool:
    """
    Return True if the actual dtype is compatible with the expected dtype.

    We treat all integer variants (int8, int16, int32, int64) as equivalent
    because pandas may downcast on certain operations.
    Similarly float32 and float64 are considered compatible.
    """
    int_family = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}
    float_family = {"float16", "float32", "float64"}

    if expected in int_family and actual in int_family:
        return True
    if expected in float_family and actual in float_family:
        return True
    return actual == expected
