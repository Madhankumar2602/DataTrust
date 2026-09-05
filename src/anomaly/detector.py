"""Explainable anomaly detection for DataTrust retail metrics."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.anomaly.rules import classify_anomaly


@dataclass
class AnomalyDetectionResult:
    metric: str
    period: str
    value: float
    expected: float
    deviation_pct: float
    severity: str
    message: str


class AnomalyDetector:
    """Detect unusual changes using historical statistical baselines."""

    def __init__(self, z_threshold: float = 3.0) -> None:
        self.z_threshold = z_threshold

    def detect(
        self,
        dataframe: pd.DataFrame,
        metric: str,
        date_column: str = "invoice_date",
    ) -> list[AnomalyDetectionResult]:
        """Detect statistically unusual and business-relevant values."""

        if dataframe.empty:
            return []

        data = dataframe.copy()

        data[date_column] = pd.to_datetime(
            data[date_column],
            errors="coerce",
        )

        data = data.dropna(subset=[date_column])

        if data.empty:
            return []

        data["period"] = data[date_column].dt.to_period("M").astype(str)

        if metric == "revenue":
            monthly = (
                data.groupby("period")["revenue"]
                .sum()
                .reset_index(name="value")
            )

        elif metric == "transactions":
            monthly = (
                data.groupby("period")
                .size()
                .reset_index(name="value")
            )

        elif metric == "cancellations":
            monthly = (
                data.assign(
                    cancellation=data["is_cancellation"].astype(int)
                )
                .groupby("period")["cancellation"]
                .mean()
                .mul(100)
                .reset_index(name="value")
            )

        else:
            raise ValueError(
                f"Unsupported anomaly metric: {metric}"
            )

        if len(monthly) < 4:
            return []

        values = monthly["value"].astype(float)

        mean = values.mean()
        std = values.std()

        if std == 0 or pd.isna(std):
            return []

        results: list[AnomalyDetectionResult] = []

        for _, row in monthly.iterrows():
            value = float(row["value"])

            z_score = abs(value - mean) / std

            if z_score < self.z_threshold:
                continue

            deviation_pct = (
                ((value - mean) / mean) * 100
                if mean != 0
                else 0.0
            )

            is_anomaly, severity = classify_anomaly(
                metric,
                deviation_pct,
            )

            if not is_anomaly:
                continue

            direction = "above" if value > mean else "below"

            message = (
                f"{metric.title()} is "
                f"{abs(deviation_pct):.1f}% "
                f"{direction} the historical baseline."
            )

            results.append(
                AnomalyDetectionResult(
                    metric=metric,
                    period=str(row["period"]),
                    value=round(value, 4),
                    expected=round(mean, 4),
                    deviation_pct=round(deviation_pct, 2),
                    severity=severity,
                    message=message,
                )
            )

        return results
