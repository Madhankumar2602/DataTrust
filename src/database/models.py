"""SQLAlchemy models for DataTrust Phase 4 historical quality storage."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Base class for DataTrust database models."""


class PipelineRun(Base):
    """One completed DataTrust execution and its overall health score."""

    __tablename__ = "pipeline_runs"
    __table_args__ = (
        CheckConstraint("duration_seconds >= 0", name="ck_pipeline_runs_duration_non_negative"),
        CheckConstraint("rows_processed >= 0", name="ck_pipeline_runs_rows_non_negative"),
        CheckConstraint("health_score >= 0 AND health_score <= 100", name="ck_pipeline_runs_score_range"),
        Index("ix_pipeline_runs_name_started", "pipeline_name", "started_at"),
    )

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    rows_processed: Mapped[int] = mapped_column(Integer, nullable=False)
    health_score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    quality_results: Mapped[list["QualityResult"]] = relationship(back_populates="pipeline_run", cascade="all, delete-orphan")


class QualityResult(Base):
    """One structured Phase 2 validation result belonging to a pipeline run."""

    __tablename__ = "quality_results"
    __table_args__ = (
        CheckConstraint("affected_rows >= 0", name="ck_quality_results_rows_non_negative"),
        CheckConstraint("affected_percentage >= 0 AND affected_percentage <= 100", name="ck_quality_results_percentage_range"),
        Index("ix_quality_results_run_category", "run_id", "category"),
        Index("ix_quality_results_status", "status"),
    )

    result_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"), nullable=False, index=True)
    check_name: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    affected_rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    affected_percentage: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    pipeline_run: Mapped[PipelineRun] = relationship(back_populates="quality_results")


class RetailTransaction(Base):
    """Current transformed snapshot loaded by the Phase 5 ETL pipeline.

    The table intentionally has no business-key uniqueness constraint: exact
    duplicates remain stored so DataTrust can report them as quality findings.
    """

    __tablename__ = "retail_transactions"
    __table_args__ = (Index("ix_retail_transactions_invoice_no", "invoice_no"),)

    transaction_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_no: Mapped[str] = mapped_column(String(30), nullable=False)
    stock_code: Mapped[str] = mapped_column(String(30), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    invoice_date: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, index=True)
    unit_price: Mapped[float] = mapped_column(Float, nullable=False)
    customer_id: Mapped[float | None] = mapped_column(Float)
    country: Mapped[str | None] = mapped_column(String(100))
    is_cancellation: Mapped[bool] = mapped_column(nullable=False, index=True)
    revenue: Mapped[float] = mapped_column(Float, nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
