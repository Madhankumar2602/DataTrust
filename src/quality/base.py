"""
base.py — Shared types and base class for the DataTrust quality engine.

Why a base module?
──────────────────
Every check (schema, completeness, validity, uniqueness) produces the same
kind of structured result. By defining CheckResult and CheckStatus once here,
every check speaks the same "language" — consistent fields, consistent
serialisation, consistent status values.

This is the foundation that engine.py assembles and test_*.py imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


# ── Status Enum ───────────────────────────────────────────────────────────────

class CheckStatus(str, Enum):
    """
    The four possible outcomes of a quality check.

    Why use an Enum instead of plain strings?
    - Typos like "FAILL" are caught at import time, not at runtime.
    - IDE auto-complete works on Enum members.
    - str Enum means CheckStatus.PASS == "PASS" is True, so JSON serialisation
      just works with the string value.

    PASS    — Check met its threshold; no action needed.
    WARNING — Check is outside the ideal range but not breaking.
    FAIL    — Check failed; data quality is compromised.
    INFO    — Informational only; not a problem (e.g. guest transactions).
    """

    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"
    INFO = "INFO"


# ── Severity Enum ─────────────────────────────────────────────────────────────

class Severity(str, Enum):
    """
    How serious is the issue if the check does not pass?

    CRITICAL — Pipeline should stop; data is untrustworthy.
    HIGH     — Significant quality problem; must be investigated.
    MEDIUM   — Moderate issue; should be monitored.
    LOW      — Minor issue; can be noted and accepted.
    INFO     — No severity; purely informational.
    """

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


# ── CheckResult dataclass ──────────────────────────────────────────────────────

@dataclass
class CheckResult:
    """
    A single structured result from one quality check.

    Why a dataclass?
    ────────────────
    A dataclass automatically generates __init__, __repr__, and __eq__
    from the field declarations. This removes boilerplate and makes
    equality checks in tests trivially simple.

    Fields:
        check_name        — Human-readable name of the check.
        category          — Which quality dimension (schema/completeness/etc.)
        status            — PASS / WARNING / FAIL / INFO
        severity          — How serious is a non-PASS result?
        message           — Plain-English description of the finding.
        affected_rows     — How many rows are affected (0 if not applicable).
        affected_pct      — What percentage of total rows are affected.
        metadata          — Extra key-value pairs specific to each check type.
    """

    check_name: str
    category: str
    status: CheckStatus
    severity: Severity
    message: str
    affected_rows: int = 0
    affected_pct: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """
        Serialise this result to a plain dict for JSON output.

        We explicitly convert Enum values to their string representation
        so json.dump() doesn't choke on them.
        """
        d = asdict(self)
        d["status"] = self.status.value
        d["severity"] = self.severity.value
        # Keep the original short name for compatibility with existing reports,
        # while exposing the clearer public field name used by Phase 2.
        d["affected_percentage"] = d["affected_pct"]
        return d

    @property
    def passed(self) -> bool:
        """True if this check PASSED (convenience for assertions)."""
        return self.status == CheckStatus.PASS

    @property
    def failed(self) -> bool:
        """True if this check FAILED."""
        return self.status == CheckStatus.FAIL


# ── Base check class ──────────────────────────────────────────────────────────

class BaseCheck:
    """
    Abstract base class for all DataTrust quality checks.

    Every check subclass must implement run() which accepts a DataFrame
    and returns a list of CheckResult objects.

    Why a list?
    ───────────
    A single check may produce multiple results.
    For example, CompletenessCheck runs once per column — 8 columns = 8 results.
    SchemaCheck may report both a missing column AND an unexpected column.
    """

    name: str = "BaseCheck"
    category: str = "base"

    def run(self, df: "pd.DataFrame") -> list[CheckResult]:  # noqa: F821
        """
        Execute the check against the provided DataFrame.

        Args:
            df: The DataFrame to validate. Never mutate it.

        Returns:
            A list of CheckResult objects.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement run()"
        )
