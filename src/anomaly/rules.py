"""Business-aware anomaly interpretation rules for DataTrust."""

from __future__ import annotations


def classify_anomaly(
    metric: str,
    deviation_pct: float,
) -> tuple[bool, str]:
    """Determine whether a statistical deviation is business-relevant."""

    absolute_deviation = abs(deviation_pct)

    if metric == "revenue":
        if absolute_deviation >= 100:
            return True, "CRITICAL"

        if absolute_deviation >= 50:
            return True, "WARNING"

        return False, "INFO"

    if metric == "transactions":
        if absolute_deviation >= 100:
            return True, "CRITICAL"

        if absolute_deviation >= 50:
            return True, "WARNING"

        return False, "INFO"

    if metric == "cancellations":
        # Higher cancellation rates are concerning.
        # Lower cancellation rates are generally healthy.
        if deviation_pct >= 100:
            return True, "CRITICAL"

        if deviation_pct >= 50:
            return True, "WARNING"

        return False, "INFO"

    return absolute_deviation >= 50, "WARNING"
