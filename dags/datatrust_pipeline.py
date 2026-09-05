"""
DataTrust End-to-End Pipeline Orchestration DAG.

Orchestrates:
1. Extract, Transform, Load (ETL) -> MySQL retail_transactions table
2. Data Quality Validation Engine -> Quality Report
3. Health Scoring & Persistence -> MySQL pipeline_runs & quality_results tables
4. Anomaly Detection & Persistence -> MySQL anomaly_results table

The business logic remains isolated within `src/` modules. This DAG strictly
acts as the orchestrator managing stage dependencies, retries, and logging.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path when executed in Airflow workers
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.anomaly.detector import AnomalyDetector
from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base
from src.database.repository import QualityRepository
from src.etl.pipeline import run_etl_pipeline
from src.quality.engine import QualityEngine
from src.scoring.scorer import HealthScorer

logger = logging.getLogger("datatrust.pipeline")


# ---------------------------------------------------------------------------
# Task 1: Extract, Transform, Load (ETL)
# ---------------------------------------------------------------------------

def run_extract_transform_load(**context: Any) -> dict[str, Any]:
    """
    Extract raw Online Retail data, normalize and derive features losslessly,
    and refresh the MySQL retail_transactions snapshot table.

    Execution is delegated to the reusable pipeline in `src/etl/pipeline.py`,
    which owns extraction, transformation, loading and ETL failure tracking.
    This task only orchestrates: it stops at the load stage because validation,
    scoring and anomaly detection are separate downstream tasks, and it lets the
    original exception reach Airflow so retries behave normally.
    """
    logger.info("[ORCHESTRATION] Stage 1/4: Starting Extract, Transform, Load (ETL)...")

    result = run_etl_pipeline(
        pipeline_name="datatrust_daily_pipeline",
        load_only=True,
        raise_on_failure=True,
    )

    summary = {
        "status": result.status,
        "rows_extracted": result.rows_extracted,
        "rows_transformed": result.rows_transformed,
        "rows_loaded": result.rows_loaded,
        "duration_seconds": result.duration_seconds,
    }
    logger.info("[ORCHESTRATION] Stage 1/4 Complete: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# Task 2: Data Quality Validation
# ---------------------------------------------------------------------------

def run_quality_validation(**context: Any) -> dict[str, Any]:
    """
    Execute Data Quality Engine against the current transformed data,
    evaluating schema contract, completeness, validity, and uniqueness.
    """
    import pandas as pd
    from sqlalchemy import text

    logger.info("[ORCHESTRATION] Stage 2/4: Starting Data Quality Validation...")

    engine = create_database_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM retail_transactions"), conn)

    logger.info("[QUALITY] Loaded %s records for quality validation", f"{len(df):,}")

    quality_engine = QualityEngine(dataset_name="UCI Online Retail Transformed")
    report = quality_engine.run(df)
    report_path = quality_engine.save_report(report)

    summary = report["summary"]
    logger.info(
        "[QUALITY] Validation complete: Total=%s Passed=%s Warnings=%s Failed=%s",
        summary["total_checks"],
        summary["passed"],
        summary["warnings"],
        summary["failed"],
    )
    logger.info("[ORCHESTRATION] Stage 2/4 Complete. Report saved to %s", report_path)

    return {
        "dataset_name": report["dataset_name"],
        "total_rows": report["total_rows"],
        "summary": summary,
        "results": report["results"],
        "validated_at": report["validated_at"],
        "duration_seconds": report["duration_seconds"],
    }


# ---------------------------------------------------------------------------
# Task 3: Data Health Scoring & Persistence
# ---------------------------------------------------------------------------

def run_health_scoring_and_persistence(**context: Any) -> dict[str, Any]:
    """
    Calculate the 0–100 Data Health Score from the validation report
    and atomically persist the pipeline run and individual results to MySQL.
    """
    # Retrieve quality report from previous task XCom if available, or generate fresh
    ti = context.get("ti")
    quality_report = None
    if ti is not None:
        quality_report = ti.xcom_pull(task_ids="quality_validation")

    if not quality_report:
        logger.info("[SCORING] No XCom quality report found. Generating validation report...")
        import pandas as pd
        from sqlalchemy import text

        engine = create_database_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM retail_transactions"), conn)
        quality_report = QualityEngine("UCI Online Retail Transformed").run(df)

    logger.info("[ORCHESTRATION] Stage 3/4: Calculating Health Score & Persisting Run...")

    scorer = HealthScorer()
    score_report = scorer.calculate_score(quality_report)

    # Persist to MySQL pipeline_runs and quality_results
    engine = create_database_engine()
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    started_at = datetime.now(timezone.utc)
    with session_factory() as session:
        repo = QualityRepository(session)
        pipeline_run = repo.save_run(
            quality_report=quality_report,
            score_report=score_report,
            pipeline_name="datatrust_daily_pipeline",
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            status="SUCCESS",
        )
        run_id = pipeline_run.run_id
        health_score = pipeline_run.health_score

    logger.info(
        "[SCORING] Pipeline Run #%s saved. Health Score: %.2f/100 (Status: %s)",
        run_id,
        health_score,
        score_report.get("status", "UNKNOWN"),
    )
    logger.info("[ORCHESTRATION] Stage 3/4 Complete.")

    return {
        "run_id": run_id,
        "health_score": health_score,
        "status": score_report.get("status"),
        "category_scores": score_report.get("category_scores"),
    }


# ---------------------------------------------------------------------------
# Task 4: Anomaly Detection & Persistence
# ---------------------------------------------------------------------------

def run_anomaly_detection(**context: Any) -> dict[str, Any]:
    """
    Execute statistical z-score anomaly detection across revenue, transactions,
    and cancellation rates, applying business rules and persisting anomalies to MySQL.
    """
    import pandas as pd
    from sqlalchemy import text

    logger.info("[ORCHESTRATION] Stage 4/4: Starting Anomaly Detection...")

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
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logger.info("[ANOMALY] Loaded %s transactions for anomaly analysis", f"{len(df):,}")

    detector = AnomalyDetector(z_threshold=2.0)
    all_anomalies = []

    for metric in ["revenue", "transactions", "cancellations"]:
        results = detector.detect(df, metric)
        if results:
            logger.info("[ANOMALY] %s: Found %d anomaly period(s)", metric.upper(), len(results))
            for r in results:
                logger.info(
                    "  [%s] %s | value=%.2f | expected=%.2f | %+.2f%% | %s",
                    r.severity,
                    r.period,
                    r.value,
                    r.expected,
                    r.deviation_pct,
                    r.message,
                )
            all_anomalies.extend(results)
        else:
            logger.info("[ANOMALY] %s: No anomalies detected", metric.upper())

    # Persist anomalies to MySQL anomaly_results
    if all_anomalies:
        Base.metadata.create_all(engine)
        session_factory = create_session_factory(engine)
        with session_factory() as session:
            repo = QualityRepository(session)
            persisted = repo.save_anomalies(all_anomalies)
            persisted_count = len(persisted)
        logger.info("[ANOMALY] Persisted %d anomalies to MySQL anomaly_results", persisted_count)
    else:
        persisted_count = 0
        logger.info("[ANOMALY] No anomalies to persist")

    logger.info("[ORCHESTRATION] Stage 4/4 Complete.")

    return {
        "anomalies_detected": len(all_anomalies),
        "anomalies_persisted": persisted_count,
    }


# ---------------------------------------------------------------------------
# Airflow DAG Definition
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "datatrust",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="datatrust_pipeline",
        default_args=DEFAULT_ARGS,
        description=(
            "DataTrust Automated Data Quality, Observability, "
            "and Anomaly Detection Pipeline"
        ),
        schedule_interval="@daily",
        catchup=False,
        tags=["datatrust", "quality", "observability", "etl", "anomaly"],
    ) as dag:

        task_etl = PythonOperator(
            task_id="extract_transform_load",
            python_callable=run_extract_transform_load,
            doc_md=(
                "Extracts raw CSV, normalizes types, and loads snapshot "
                "to MySQL retail_transactions."
            ),
        )

        task_quality = PythonOperator(
            task_id="quality_validation",
            python_callable=run_quality_validation,
            doc_md=(
                "Evaluates completeness, schema, validity, and uniqueness "
                "on the loaded dataset."
            ),
        )

        task_scoring = PythonOperator(
            task_id="health_scoring_and_persistence",
            python_callable=run_health_scoring_and_persistence,
            doc_md="Computes 0-100 Health Score and persists run history into MySQL pipeline_runs.",
        )

        task_anomaly = PythonOperator(
            task_id="anomaly_detection",
            python_callable=run_anomaly_detection,
            doc_md=(
                "Detects monthly statistical deviations and persists "
                "anomalies to MySQL anomaly_results."
            ),
        )

        # Strictly ordered pipeline dependency chain
        task_etl >> task_quality >> task_scoring >> task_anomaly

except ImportError:
    # Airflow is not installed in the current environment (e.g. native Windows dev environment)
    # The callables remain directly importable and testable.
    dag = None
