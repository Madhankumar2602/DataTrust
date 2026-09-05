"""
test_scorer.py — Unit tests for Phase 3 Data Health Score.
"""

import pytest

from src.scoring.scorer import HealthScorer

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def scorer():
    return HealthScorer()


@pytest.fixture
def empty_report():
    return {
        "dataset_name": "Test Dataset",
        "results": []
    }

# ── Tests ────────────────────────────────────────────────────────────────────


def test_scorer_no_results_are_not_assessed(scorer, empty_report):
    """Future or missing checks must not inflate the score."""
    score_report = scorer.calculate_score(empty_report)
    assert score_report["score"] == 0.0
    assert score_report["status"] == "Critical"
    assert "No validation results" in score_report["category_scores"]["schema"]["details"]


def test_scorer_critical_schema_failure(scorer, empty_report):
    """A critical schema failure should force the score to 0."""
    empty_report["results"] = [
        {"category": "schema", "status": "FAIL", "severity": "CRITICAL"}
    ]
    score_report = scorer.calculate_score(empty_report)
    assert score_report["score"] == 0.0
    assert score_report["status"] == "Critical"
    assert "CRITICAL SCHEMA FAILURE" in score_report.get("failure_reason", "")
    assert score_report["dataset_name"] == "Test Dataset"


def test_scorer_category_math(scorer, empty_report):
    """
    Test the point math: PASS=1, WARNING=0.5, FAIL=0.
    Let's test the 'completeness' category.
    """
    empty_report["results"] = [
        {"category": "completeness", "status": "PASS"},    # 1.0
        {"category": "completeness", "status": "INFO"},    # 1.0
        {"category": "completeness", "status": "WARNING"},  # 0.5
        {"category": "completeness", "status": "FAIL"}     # 0.0
    ]
    # Total points earned = 2.5
    # Total checks = 4
    # Expected percentage = 2.5 / 4 = 62.5%
    # Completeness weight is 20, so expected category score = 20 * 0.625 = 12.5.

    score_report = scorer.calculate_score(empty_report)
    comp_score = score_report["category_scores"]["completeness"]["score"]

    assert comp_score == 12.5
    assert score_report["category_scores"]["completeness"]["included_checks"][2] == {
        "check_name": "unnamed_check", "status": "WARNING", "points": 0.5
    }


def test_scorer_business_rule_is_scored_separately(scorer, empty_report):
    """
    Cancellation logic is a business rule, separate from generic validity.
    """
    empty_report["results"] = [
        {"category": "validity", "check_name": "quantity_cancellation_logic", "status": "WARNING"}
    ]
    score_report = scorer.calculate_score(empty_report)

    assert score_report["category_scores"]["business_rules"]["score"] == 10.0
    assert score_report["category_scores"]["validity"]["score"] == 0.0
