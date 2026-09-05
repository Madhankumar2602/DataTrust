"""Phase 6 unit tests for AnomalyDetector and business classification rules."""

from __future__ import annotations

import pandas as pd
import pytest

from src.anomaly.detector import AnomalyDetector
from src.anomaly.rules import classify_anomaly


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_revenue_df(values: list[float], start: str = "2010-01") -> pd.DataFrame:
    """Build a minimal DataFrame with monthly revenue over successive months."""
    periods = pd.period_range(start=start, periods=len(values), freq="M")
    dates = [p.to_timestamp() + pd.to_timedelta(1, unit="D") for p in periods]
    return pd.DataFrame(
        {
            "invoice_date": dates,
            "revenue": values,
            "is_cancellation": [False] * len(values),
        }
    )


def _make_cancellation_df(rates: list[float], start: str = "2010-01") -> pd.DataFrame:
    """Build a DataFrame where is_cancellation reflects the desired monthly rate."""
    rows: list[dict] = []
    periods = pd.period_range(start=start, periods=len(rates), freq="M")
    for period, rate in zip(periods, rates):
        base_date = period.to_timestamp()
        n = 10  # transactions per month
        n_cancel = round(rate * n)
        for i in range(n_cancel):
            rows.append({
                "invoice_date": base_date + pd.to_timedelta(i, unit="D"),
                "revenue": -1.0,
                "is_cancellation": True,
            })
        for i in range(n - n_cancel):
            rows.append({
                "invoice_date": base_date + pd.to_timedelta(i, unit="D"),
                "revenue": 10.0,
                "is_cancellation": False,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Revenue anomaly
# ---------------------------------------------------------------------------

def test_revenue_anomaly():
    """A month with revenue 10x the baseline should be flagged."""
    values = [1000.0] * 8 + [10000.0]
    df = _make_revenue_df(values)
    detector = AnomalyDetector(z_threshold=2.0)
    results = detector.detect(df, "revenue")
    assert len(results) == 1
    assert results[0].metric == "revenue"
    assert results[0].deviation_pct > 0


# ---------------------------------------------------------------------------
# Transaction anomaly
# ---------------------------------------------------------------------------

def test_transaction_anomaly():
    """A month with 10x the usual transaction count should be flagged."""
    normal_counts = [5] * 8
    spike_count = 50
    periods = pd.period_range(start="2010-01", periods=len(normal_counts) + 1, freq="M")
    rows: list[dict] = []
    for period, count in zip(periods, normal_counts + [spike_count]):
        base_date = period.to_timestamp()
        for i in range(count):
            rows.append({
                "invoice_date": base_date + pd.to_timedelta(i, unit="D"),
                "revenue": 10.0,
                "is_cancellation": False,
            })
    df = pd.DataFrame(rows)
    detector = AnomalyDetector(z_threshold=2.0)
    results = detector.detect(df, "transactions")
    assert any(r.metric == "transactions" for r in results)


# ---------------------------------------------------------------------------
# Cancellation decrease must NOT be flagged
# ---------------------------------------------------------------------------

def test_cancellation_decrease_is_ignored():
    """A dramatic drop in cancellation rate is healthy and must not be an anomaly."""
    # Five months of high cancellations, then one month of near-zero cancellations.
    rates = [0.8, 0.8, 0.8, 0.8, 0.8, 0.0]
    df = _make_cancellation_df(rates)
    detector = AnomalyDetector(z_threshold=2.0)
    results = detector.detect(df, "cancellations")
    # Any flagged period must NOT be a decrease (deviation_pct must be positive).
    for r in results:
        assert r.deviation_pct > 0, "A cancellation decrease should never be flagged."


# ---------------------------------------------------------------------------
# Severity thresholds (business rules)
# ---------------------------------------------------------------------------

def test_warning_severity():
    """50-99% deviation on revenue should produce WARNING, not CRITICAL."""
    is_anomaly, severity = classify_anomaly("revenue", 75.0)
    assert is_anomaly is True
    assert severity == "WARNING"


def test_critical_severity():
    """>=100% deviation on transactions should produce CRITICAL."""
    is_anomaly, severity = classify_anomaly("transactions", 103.0)
    assert is_anomaly is True
    assert severity == "CRITICAL"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_dataframe():
    """An empty DataFrame must return an empty list without raising."""
    df = pd.DataFrame(columns=["invoice_date", "revenue", "is_cancellation"])
    detector = AnomalyDetector()
    results = detector.detect(df, "revenue")
    assert results == []


def test_insufficient_history():
    """Fewer than 4 monthly periods should return an empty list (no baseline)."""
    values = [1000.0, 1000.0, 9999.0]  # 3 months only
    df = _make_revenue_df(values)
    detector = AnomalyDetector(z_threshold=2.0)
    results = detector.detect(df, "revenue")
    assert results == []


def test_zero_standard_deviation():
    """Perfectly uniform series (std == 0) must return an empty list."""
    values = [1000.0, 1000.0, 1000.0, 1000.0, 1000.0]
    df = _make_revenue_df(values)
    detector = AnomalyDetector(z_threshold=2.0)
    results = detector.detect(df, "revenue")
    assert results == []


def test_unsupported_metric():
    """Passing an unknown metric name must raise ValueError."""
    values = [1000.0, 1000.0, 1000.0, 1000.0, 9999.0]
    df = _make_revenue_df(values)
    detector = AnomalyDetector(z_threshold=2.0)
    with pytest.raises(ValueError, match="Unsupported anomaly metric"):
        detector.detect(df, "profit")


# ---------------------------------------------------------------------------
# Business rule classification (direct)
# ---------------------------------------------------------------------------

def test_business_rule_classification():
    """Directly verify classify_anomaly returns expected (is_anomaly, severity) pairs."""
    assert classify_anomaly("revenue", 40.0) == (False, "INFO")
    assert classify_anomaly("revenue", 60.0) == (True, "WARNING")
    assert classify_anomaly("revenue", 105.0) == (True, "CRITICAL")
    assert classify_anomaly("transactions", 40.0) == (False, "INFO")
    assert classify_anomaly("transactions", 55.0) == (True, "WARNING")
    assert classify_anomaly("transactions", 100.0) == (True, "CRITICAL")
    # Cancellations: only increases matter
    assert classify_anomaly("cancellations", -80.0) == (False, "INFO")  # decrease
    assert classify_anomaly("cancellations", 60.0) == (True, "WARNING")  # increase
    assert classify_anomaly("cancellations", 120.0) == (True, "CRITICAL")  # big increase
