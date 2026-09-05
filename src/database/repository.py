"""Persistence and historical-query operations for DataTrust pipeline runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session, selectinload

from src.database.models import AnomalyResult, PipelineRun, QualityResult

if TYPE_CHECKING:
    from src.anomaly.detector import AnomalyDetectionResult


def _parse_timestamp(value: str | None, fallback: datetime) -> datetime:
    """Parse an ISO timestamp emitted by DataTrust, retaining UTC awareness."""
    if not value:
        return fallback
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class QualityRepository:
    """Stores Phase 2/3 output and exposes queries for later product layers."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def save_run(
        self,
        quality_report: dict[str, Any],
        score_report: dict[str, Any],
        pipeline_name: str = "online_retail_quality_pipeline",
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        status: str = "COMPLETED",
    ) -> PipelineRun:
        """Atomically save one pipeline run and all of its quality results."""
        finished = finished_at or datetime.now(timezone.utc)
        started = started_at or _parse_timestamp(quality_report.get("validated_at"), finished)
        duration = max((finished - started).total_seconds(), 0.0)
        pipeline_run = PipelineRun(
            pipeline_name=pipeline_name,
            started_at=started,
            finished_at=finished,
            duration_seconds=round(duration, 4),
            status=status,
            rows_processed=int(quality_report.get("total_rows", 0)),
            health_score=float(score_report.get("score", 0.0)),
            health_status=score_report.get("status"),
            category_scores=score_report.get("category_scores"),
        )
        try:
            self.session.add(pipeline_run)
            self.session.flush()
            self.session.add_all(
                [
                    QualityResult(
                        run_id=pipeline_run.run_id,
                        check_name=result.get("check_name", "unknown"),
                        category=result.get("category", "unknown"),
                        status=result.get("status", "UNKNOWN"),
                        severity=result.get("severity", "INFO"),
                        affected_rows=int(result.get("affected_rows", 0)),
                        affected_percentage=float(
                            result.get(
                                "affected_percentage",
                                result.get("affected_pct", 0.0),
                            )
                        ),
                        message=result.get("message", ""),
                    )
                    for result in quality_report.get("results", [])
                ]
            )
            self.session.commit()
            self.session.refresh(pipeline_run)
            return pipeline_run
        except Exception:
            self.session.rollback()
            raise

    def get_latest_run(self) -> PipelineRun | None:
        """Return the most recently stored run with its quality results."""
        return self.session.scalar(self._runs_query().order_by(PipelineRun.run_id.desc()).limit(1))

    def get_run(self, run_id: int) -> PipelineRun | None:
        """Return one stored pipeline run by ID with its quality results."""
        return self.session.scalar(self._runs_query().where(PipelineRun.run_id == run_id))

    def get_recent_runs(self, limit: int = 10, offset: int = 0) -> list[PipelineRun]:
        """Return newest pipeline runs, bounded to a safe positive limit."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset must not be negative")
        statement = (
            self._runs_query()
            .order_by(PipelineRun.run_id.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(statement))

    def get_runs(self, limit: int = 10, offset: int = 0) -> list[PipelineRun]:
        """Alias for get_recent_runs."""
        return self.get_recent_runs(limit=limit, offset=offset)

    def count_runs(self) -> int:
        """Return how many pipeline runs are stored, ignoring pagination."""
        return int(self.session.scalar(select(func.count()).select_from(PipelineRun)) or 0)

    def get_quality_results(self, run_id: int) -> list[QualityResult]:
        """Return individual results for one stored pipeline run."""
        statement = (
            select(QualityResult)
            .where(QualityResult.run_id == run_id)
            .order_by(QualityResult.result_id)
        )
        return list(self.session.scalars(statement))

    def get_health_score_history(self, limit: int = 30) -> list[PipelineRun]:
        """Return runs containing historical scores, oldest first for trend charts."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        statement = select(PipelineRun).order_by(PipelineRun.run_id.desc()).limit(limit)
        return list(reversed(list(self.session.scalars(statement))))

    def get_failed_checks(self, run_id: int | None = None) -> list[QualityResult]:
        """Return FAIL quality results, optionally limited to a run."""
        return self._results_by_status("FAIL", run_id)

    def get_warning_checks(self, run_id: int | None = None) -> list[QualityResult]:
        """Return WARNING quality results, optionally limited to a run."""
        return self._results_by_status("WARNING", run_id)

    def save_anomalies(self, anomalies: list[AnomalyDetectionResult]) -> list[AnomalyResult]:
        """Persist a list of detected anomalies and return the saved ORM rows."""
        if not anomalies:
            return []
        rows = [
            AnomalyResult(
                metric=a.metric,
                period=a.period,
                value=a.value,
                expected_value=a.expected,
                deviation_pct=a.deviation_pct,
                severity=a.severity,
                message=a.message,
            )
            for a in anomalies
        ]
        try:
            self.session.add_all(rows)
            self.session.commit()
            for row in rows:
                self.session.refresh(row)
            return rows
        except Exception:
            self.session.rollback()
            raise

    def get_anomalies(self, limit: int = 50, offset: int = 0) -> list[AnomalyResult]:
        """Return the most recently detected anomalies, newest first."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset must not be negative")
        statement = (
            select(AnomalyResult)
            .order_by(AnomalyResult.anomaly_id.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(statement))

    def count_anomalies(self) -> int:
        """Return how many anomalies are stored, ignoring pagination."""
        return int(self.session.scalar(select(func.count()).select_from(AnomalyResult)) or 0)

    def _runs_query(self) -> Select[tuple[PipelineRun]]:
        return select(PipelineRun).options(selectinload(PipelineRun.quality_results))

    def _results_by_status(self, status: str, run_id: int | None) -> list[QualityResult]:
        statement = select(QualityResult).where(QualityResult.status == status)
        if run_id is not None:
            statement = statement.where(QualityResult.run_id == run_id)
        return list(self.session.scalars(statement.order_by(QualityResult.result_id)))
