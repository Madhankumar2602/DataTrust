"""Persistence and historical-query operations for DataTrust pipeline runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session, selectinload

from src.database.models import (
    RUN_STATUS_RUNNING,
    AnomalyResult,
    PipelineRun,
    QualityResult,
)

if TYPE_CHECKING:
    from src.anomaly.detector import AnomalyDetectionResult


def _parse_timestamp(value: str | None, fallback: datetime) -> datetime:
    """Parse an ISO timestamp emitted by DataTrust, retaining UTC awareness."""
    if not value:
        return fallback
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _as_utc(value: datetime) -> datetime:
    """Assume UTC for a naive datetime (SQLite drops tzinfo on read-back)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _apply_score_report(pipeline_run: PipelineRun, score_report: dict[str, Any]) -> None:
    """Copy HealthScorer output onto a run, exactly as `save_run` stores it."""
    pipeline_run.health_score = float(score_report.get("score", 0.0))
    pipeline_run.health_status = score_report.get("status")
    pipeline_run.category_scores = score_report.get("category_scores")


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
        rows_failed: int = 0,
        error_message: str | None = None,
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
            rows_failed=rows_failed,
            error_message=error_message,
            health_score=float(score_report.get("score", 0.0)),
            health_status=score_report.get("status"),
            category_scores=score_report.get("category_scores"),
        )
        try:
            self.session.add(pipeline_run)
            self.session.flush()
            self.session.add_all(self._quality_result_rows(pipeline_run.run_id, quality_report))
            self.session.commit()
            self.session.refresh(pipeline_run)
            return pipeline_run
        except Exception:
            self.session.rollback()
            raise

    def create_run(
        self,
        pipeline_name: str,
        started_at: datetime | None = None,
        status: str = "RUNNING",
    ) -> PipelineRun:
        """Persist a new pipeline run immediately, before its outcome is known."""
        pipeline_run = PipelineRun(
            pipeline_name=pipeline_name,
            started_at=started_at or datetime.now(timezone.utc),
            duration_seconds=0.0,
            status=status,
            rows_processed=0,
            rows_failed=0,
            health_score=0.0,
        )
        try:
            self.session.add(pipeline_run)
            self.session.commit()
            self.session.refresh(pipeline_run)
            return pipeline_run
        except Exception:
            self.session.rollback()
            raise

    def record_run_results(
        self,
        run_id: int,
        quality_report: dict[str, Any],
        score_report: dict[str, Any],
    ) -> PipelineRun:
        """Attach quality results and health scores to a run that is still open.

        Used by orchestration, where scoring happens in its own stage but the run
        is only finished once every later stage has succeeded.
        """
        pipeline_run = self._require_run(run_id)
        _apply_score_report(pipeline_run, score_report)
        try:
            self.session.add_all(self._quality_result_rows(run_id, quality_report))
            self.session.commit()
            self.session.refresh(pipeline_run)
            return pipeline_run
        except Exception:
            self.session.rollback()
            raise

    def complete_run(
        self,
        run_id: int,
        rows_processed: int = 0,
        rows_failed: int = 0,
        finished_at: datetime | None = None,
        status: str = "SUCCESS",
        score_report: dict[str, Any] | None = None,
        quality_report: dict[str, Any] | None = None,
    ) -> PipelineRun:
        """Mark an existing run successful/completed with its final counters.

        `score_report` and `quality_report` are optional: pass them when one call
        should both finish the run and store its health/quality output, or leave
        them out when `record_run_results` already stored that output.
        """
        return self._finish_run(
            run_id,
            status=status,
            finished_at=finished_at,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            score_report=score_report,
            quality_report=quality_report,
        )

    def fail_run(
        self,
        run_id: int,
        error_message: str,
        rows_processed: int = 0,
        rows_failed: int = 0,
        finished_at: datetime | None = None,
        status: str = "FAILED",
    ) -> PipelineRun:
        """Mark an existing run failed and persist a useful error message."""
        return self._finish_run(
            run_id,
            status=status,
            finished_at=finished_at,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            error_message=error_message,
        )

    def save_failed_run(
        self,
        pipeline_name: str,
        started_at: datetime,
        error_message: str,
        finished_at: datetime | None = None,
        rows_processed: int = 0,
        rows_failed: int = 0,
        status: str = "FAILED",
    ) -> PipelineRun:
        """Create and immediately mark failed a run that never reached completion.

        Used when a pipeline fails before any run row exists yet (e.g. it failed
        before calling `create_run`), so the failure is still traceable.
        """
        pipeline_run = self.create_run(pipeline_name, started_at, status="RUNNING")
        return self.fail_run(
            pipeline_run.run_id,
            error_message,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            finished_at=finished_at,
            status=status,
        )

    def _require_run(self, run_id: int) -> PipelineRun:
        pipeline_run = self.session.get(PipelineRun, run_id)
        if pipeline_run is None:
            raise ValueError(f"Pipeline run #{run_id} not found.")
        return pipeline_run

    def _quality_result_rows(
        self, run_id: int, quality_report: dict[str, Any]
    ) -> list[QualityResult]:
        """Build the QualityResult rows for one run from a validation report."""
        return [
            QualityResult(
                run_id=run_id,
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

    def _finish_run(
        self,
        run_id: int,
        status: str,
        finished_at: datetime | None,
        rows_processed: int,
        rows_failed: int,
        error_message: str | None = None,
        score_report: dict[str, Any] | None = None,
        quality_report: dict[str, Any] | None = None,
    ) -> PipelineRun:
        pipeline_run = self._require_run(run_id)
        finished = _as_utc(finished_at or datetime.now(timezone.utc))
        pipeline_run.finished_at = finished
        pipeline_run.duration_seconds = round(
            max((finished - _as_utc(pipeline_run.started_at)).total_seconds(), 0.0), 4
        )
        pipeline_run.status = status
        pipeline_run.rows_processed = rows_processed
        pipeline_run.rows_failed = rows_failed
        if error_message is not None:
            pipeline_run.error_message = error_message
        if score_report is not None:
            _apply_score_report(pipeline_run, score_report)
        if quality_report is not None:
            self.session.add_all(self._quality_result_rows(run_id, quality_report))
        try:
            self.session.commit()
            self.session.refresh(pipeline_run)
            return pipeline_run
        except Exception:
            self.session.rollback()
            raise

    def get_latest_run(self) -> PipelineRun | None:
        """Return the most recently stored run with its quality results."""
        return self.session.scalar(self._runs_query().order_by(PipelineRun.run_id.desc()).limit(1))

    def get_latest_finished_run(self) -> PipelineRun | None:
        """Return the newest run that has reached a terminal state.

        A RUNNING run has no health score yet, so health figures must come from
        the last run that actually finished rather than from an in-flight one.
        Deliberately not named "completed": a FAILED run has finished too, and
        hiding it would misreport a broken pipeline as merely stale.
        """
        statement = (
            self._runs_query()
            .where(PipelineRun.status != RUN_STATUS_RUNNING)
            .order_by(PipelineRun.run_id.desc())
            .limit(1)
        )
        return self.session.scalar(statement)

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
