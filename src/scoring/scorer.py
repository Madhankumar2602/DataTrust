"""
scorer.py — Data Health Score calculator for DataTrust.

What is the Data Health Score?
──────────────────────────────
It is a transparent 0-100 score representing the trustworthiness of the dataset.
It takes the raw results from the QualityEngine (schema, completeness, etc.)
and converts them into a weighted score based on rules defined in config.py.
"""

from __future__ import annotations

from typing import Any

from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)


class HealthScorer:
    """
    Calculates the Data Health Score from a QualityEngine report.

    Usage:
        from src.scoring.scorer import HealthScorer
        scorer = HealthScorer()
        score_report = scorer.calculate_score(quality_report)
    """

    def __init__(self):
        self.weights = settings.SCORE_WEIGHTS
        self.thresholds = {
            "CRITICAL": settings.QUALITY_THRESHOLD_CRITICAL,
            "POOR": settings.QUALITY_THRESHOLD_POOR,
            "WARNING": settings.QUALITY_THRESHOLD_WARNING,
        }

    def calculate_score(self, quality_report: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a QualityEngine report into a Data Health Score.

        Logic:
        1. If ANY schema check fails (CRITICAL severity), the overall score is 0.
        2. Otherwise, we calculate points for each category in SCORE_WEIGHTS.
           - PASS or INFO = 1.0 points
           - WARNING      = 0.5 points
           - FAIL         = 0.0 points
        3. A dimension without any results is marked as not assessed and gets
           no points. This prevents future, unimplemented checks from making
           the dataset look healthier than it is.
        """
        logger.info("Calculating Data Health Score...")

        results = quality_report.get("results", [])

        # 1. Check for critical schema failures
        if self._has_critical_schema_failure(results):
            logger.error("CRITICAL SCHEMA FAILURE DETECTED. Score set to 0.")
            return self._build_zero_score_report(
                "CRITICAL SCHEMA FAILURE", quality_report
            )

        # 2. Group results by category
        grouped_results = self._group_results_by_category(results)

        # 3. Calculate category scores
        category_scores = {}
        total_score = 0.0

        for category, max_score in self.weights.items():
            checks_in_category = grouped_results.get(category, [])

            if not checks_in_category:
                category_scores[category] = {
                    "score": 0.0,
                    "max_score": max_score,
                    "details": "No validation results available for this dimension.",
                    "included_checks": [],
                }
            else:
                # Calculate based on checks
                points_earned = 0.0
                for check in checks_in_category:
                    status = check.get("status")
                    if status in ("PASS", "INFO"):
                        points_earned += 1.0
                    elif status == "WARNING":
                        points_earned += 0.5
                    # FAIL gets 0.0 points

                # Normalize to max_score
                category_percentage = points_earned / len(checks_in_category)
                actual_score = round(category_percentage * max_score, 2)

                category_scores[category] = {
                    "score": actual_score,
                    "max_score": max_score,
                    "details": f"{points_earned}/{len(checks_in_category)} rule points earned.",
                    "included_checks": [
                        {
                            "check_name": check.get("check_name", "unnamed_check"),
                            "status": check.get("status", "UNKNOWN"),
                            "points": self._status_points(check.get("status")),
                        }
                        for check in checks_in_category
                    ],
                }
                total_score += actual_score

        final_score = round(total_score, 2)
        status_label = self._determine_status(final_score)

        logger.info(f"Final Score: {final_score}/100 ({status_label})")

        return {
            "score": final_score,
            "status": status_label,
            "category_scores": category_scores,
            "dataset_name": quality_report.get("dataset_name", "Unknown Dataset"),
            "validated_at": quality_report.get("validated_at"),
            "scoring_rules": {
                "PASS": 1.0,
                "INFO": 1.0,
                "WARNING": 0.5,
                "FAIL": 0.0,
            },
        }

    def _has_critical_schema_failure(self, results: list[dict[str, Any]]) -> bool:
        """Return True if any schema check failed with CRITICAL severity."""
        for r in results:
            if (
                r.get("category") == "schema"
                and r.get("status") == "FAIL"
                and r.get("severity") == "CRITICAL"
            ):
                return True
        return False

    def _group_results_by_category(
        self, results: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Group the flat list of CheckResults by their category."""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in results:
            cat = self._score_category(r)
            if cat not in grouped:
                grouped[cat] = []
            grouped[cat].append(r)
        return grouped

    @staticmethod
    def _score_category(result: dict[str, Any]) -> str:
        """Map Phase 2 results to the explainable Phase 3 score dimensions."""
        # The cancellation rule is business-aware validation, so show it as
        # its own manager-facing dimension rather than hiding it under validity.
        if result.get("check_name") == "quantity_cancellation_logic":
            return "business_rules"
        return result.get("category", "unknown")

    def _determine_status(self, score: float) -> str:
        """Convert a numerical score to a human-readable status label."""
        if score >= self.thresholds["WARNING"]:
            return "Healthy"
        elif score >= self.thresholds["POOR"]:
            return "Warning"
        elif score >= self.thresholds["CRITICAL"]:
            return "Poor"
        else:
            return "Critical"

    @staticmethod
    def _status_points(status: str | None) -> float:
        """Return the transparent points value assigned to a check status."""
        return {"PASS": 1.0, "INFO": 1.0, "WARNING": 0.5}.get(status, 0.0)

    def _build_zero_score_report(
        self,
        reason: str,
        quality_report: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a report forced to 0 due to a critical failure."""
        category_scores = {}
        for category, max_score in self.weights.items():
            category_scores[category] = {
                "score": 0.0,
                "max_score": max_score,
                "details": reason,
                "included_checks": [],
            }

        return {
            "score": 0.0,
            "status": "Critical",
            "category_scores": category_scores,
            "dataset_name": (quality_report or {}).get("dataset_name", "Unknown Dataset"),
            "validated_at": (quality_report or {}).get("validated_at"),
            "failure_reason": reason,
            "scoring_rules": {"PASS": 1.0, "INFO": 1.0, "WARNING": 0.5, "FAIL": 0.0},
        }
