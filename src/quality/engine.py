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
from src.contracts.loader import load_contract
from src.contracts.models import DataContract
from src.logger import get_logger
from src.quality.base import CheckResult, CheckStatus, Severity
from src.quality.schema import SchemaCheck
from src.quality.completeness import CompletenessCheck
from src.quality.validity import ValidityCheck
from src.quality.uniqueness import UniquenessCheck

logger = get_logger(__name__)


# How the engine decides which representation of the dataset it was handed.
# "auto" keeps the existing detection in SchemaCheck; the explicit values pin the
# expectations, for callers that already know what they fetched.
REPRESENTATIONS = ("auto", "source", "stored")


class QualityEngine:
    """
    Orchestrates data quality validation checks.

    Expectations come from the versioned data contract; the checks below remain
    the validators. Only SchemaCheck is contract-driven today — completeness,
    validity and uniqueness keep their own rules.
    """

    def __init__(
        self,
        dataset_name: str = "Unknown Dataset",
        representation: str = "auto",
        contract: DataContract | None = None,
    ):
        if representation not in REPRESENTATIONS:
            raise ValueError(
                f"Unknown representation '{representation}'. "
                f"Expected one of: {', '.join(REPRESENTATIONS)}"
            )

        self.dataset_name = dataset_name
        self.representation = representation
        self.contract = contract or load_contract(
            settings.CONTRACT_NAME, settings.CONTRACT_VERSION
        )
        # Register all the checks we want to run
        self.checks = [
            self._build_schema_check(),
            CompletenessCheck(),
            ValidityCheck(),
            UniquenessCheck(),
        ]

    def _build_schema_check(self) -> SchemaCheck:
        """Feed the contract's expectations into the existing schema validator.

        Leaving both arguments unset for "auto" preserves SchemaCheck's own
        source/stored detection, which is itself driven by contract-derived
        settings, so the two representations cannot drift apart.
        """
        if self.representation == "source":
            return SchemaCheck(
                expected_columns=self.contract.source_columns(),
                expected_dtypes=self.contract.source_dtypes(),
            )
        if self.representation == "stored":
            return SchemaCheck(
                expected_columns=self.contract.stored_columns(),
                expected_dtypes=self.contract.stored_dtypes(),
            )
        return SchemaCheck()

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

        # Build the final structured report. Contract provenance sits at report
        # level, never as a CheckResult: an extra result would join a category
        # average and quietly move the health score.
        report = {
            "dataset_name": self.dataset_name,
            "contract": {
                "name": self.contract.contract_name,
                "version": self.contract.contract_version,
                "representation": self._effective_representation(all_results),
            },
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

    def _effective_representation(self, results: list[CheckResult]) -> str:
        """Report which representation was actually validated.

        Under "auto" the choice is made inside SchemaCheck, so read it back from
        the result rather than restating the request.
        """
        if self.representation != "auto":
            return self.representation

        for result in results:
            if result.check_name == "column_presence":
                return result.metadata.get("representation", "source")
        return "source"

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
