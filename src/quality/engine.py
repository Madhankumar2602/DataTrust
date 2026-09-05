"""
engine.py — Main Data Quality Engine for DataTrust.

What does the engine do?
────────────────────────
It orchestrates the various quality checks (Schema, Completeness, Validity, Uniqueness).
Instead of calling them one by one, you just pass a DataFrame to the Engine,
and it runs the entire suite, returning a consolidated report.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import settings
from src.logger import get_logger
from src.quality.base import CheckResult, CheckStatus, Severity
from src.quality.schema import SchemaCheck
from src.quality.completeness import CompletenessCheck
from src.quality.validity import ValidityCheck
from src.quality.uniqueness import UniquenessCheck

logger = get_logger(__name__)


class QualityEngine:
    """
    Orchestrates data quality validation checks.
    """

    def __init__(self, dataset_name: str = "Unknown Dataset"):
        self.dataset_name = dataset_name
        # Register all the checks we want to run
        self.checks = [
            SchemaCheck(),
            CompletenessCheck(),
            ValidityCheck(),
            UniquenessCheck(),
        ]

    def run(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Run all registered checks against the DataFrame.
        """
        logger.info(f"Starting Data Quality Validation for {self.dataset_name}...")
        start_time = datetime.now(timezone.utc)

        all_results: list[CheckResult] = []

        for check in self.checks:
            try:
                results = check.run(df)
                all_results.extend(results)
            except Exception as exc:
                # A failed validator must be visible in the report. Silently
                # omitting it could make unhealthy data look trustworthy.
                logger.exception("Check %s failed with error: %s", check.name, exc)
                all_results.append(
                    CheckResult(
                        check_name=check.name,
                        category=check.category,
                        status=CheckStatus.FAIL,
                        severity=Severity.CRITICAL,
                        message=f"The check could not run: {exc}",
                        affected_rows=len(df),
                        affected_pct=100.0 if len(df) else 0.0,
                        metadata={"exception_type": type(exc).__name__},
                    )
                )

        end_time = datetime.now(timezone.utc)

        # Build the final structured report
        report = {
            "dataset_name": self.dataset_name,
            "validated_at": end_time.isoformat(),
            "duration_seconds": round((end_time - start_time).total_seconds(), 2),
            "total_rows": len(df),
            "summary": {
                "total_checks": len(all_results),
                "passed": sum(1 for r in all_results if r.passed),
                "warnings": sum(1 for r in all_results if r.status.value == "WARNING"),
                "failed": sum(1 for r in all_results if r.failed),
                "info": sum(1 for r in all_results if r.status.value == "INFO"),
            },
            "results": [r.to_dict() for r in all_results],
        }

        logger.info("Validation complete.")
        return report

    def save_report(self, report: dict[str, Any], output_path: str | Path | None = None) -> Path:
        """
        Save the validation report as a JSON file.
        """
        settings.ensure_directories()

        if output_path is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            output_path = Path(settings.REPORTS_PATH) / f"quality_{timestamp}.json"

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Quality report saved to: {output_path}")
        return output_path
