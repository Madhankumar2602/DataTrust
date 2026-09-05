"""Run DataTrust anomaly detection against the current MySQL snapshot."""

from __future__ import annotations


import pandas as pd
from sqlalchemy import text

from src.anomaly.detector import AnomalyDetectionResult, AnomalyDetector
from src.database.connection import create_database_engine, create_session_factory
from src.database.repository import QualityRepository


def main() -> None:
    print("=" * 65)
    print("  DataTrust - Phase 6: Anomaly Detection")
    print("=" * 65)

    engine = create_database_engine()

    query = text(
        """
        SELECT
            invoice_date,
            revenue,
            is_cancellation
        FROM retail_transactions
        """
    )

    with engine.connect() as connection:
        dataframe = pd.read_sql(query, connection)

    print(f"[OK] Loaded {len(dataframe):,} transactions")

    detector = AnomalyDetector(z_threshold=2.0)

    all_anomalies: list[AnomalyDetectionResult] = []

    for metric in ["revenue", "transactions", "cancellations"]:
        print(f"\n--- {metric.upper()} ---")

        results = detector.detect(
            dataframe,
            metric,
        )

        if not results:
            print("  No anomalies detected")
            continue

        for result in results:
            print(
                f"  [{result.severity}] "
                f"{result.period} | "
                f"value={result.value:.2f} | "
                f"expected={result.expected:.2f} | "
                f"{result.deviation_pct:+.2f}% | "
                f"{result.message}"
            )

        all_anomalies.extend(results)

    print()
    print(f"[OK] Detected {len(all_anomalies)} anomalies")

    if all_anomalies:
        session_factory = create_session_factory(engine)
        with session_factory() as session:
            repo = QualityRepository(session)
            repo.save_anomalies(all_anomalies)

        print(f"[OK] Persisted {len(all_anomalies)} anomalies to MySQL")
    else:
        print("[OK] Nothing to persist")

    engine.dispose()


if __name__ == "__main__":
    main()
