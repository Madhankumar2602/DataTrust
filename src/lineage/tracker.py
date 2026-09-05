"""
tracker.py — Basic Data Lineage Tracker for DataTrust.

This module provides a LineageTracker class that acts as a stateful
record of an ETL run. It captures exactly where data came from, when it
was processed, what transformations were applied, and where it was saved.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.logger import get_logger

logger = get_logger(__name__)


class LineageTracker:
    """
    Tracks the lifecycle of a dataset through the ETL pipeline.
    """

    def __init__(self, dataset_name: str):
        self.lineage_data: dict[str, Any] = {
            "dataset_name": dataset_name,
            "lineage_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "extraction": {},
            "transformation": {},
            "load": {}
        }
        logger.info(
            f"[Lineage] Started tracking for {dataset_name} "
            f"(ID: {self.lineage_data['lineage_id']})"
        )

    def record_extraction(self, source_path: str | Path, row_count: int):
        """Record details about the data extraction phase."""
        self.lineage_data["extraction"] = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source_path": str(source_path),
            "rows_extracted": row_count
        }

    def record_transformation(self, audit_log: dict[str, Any]):
        """Record the audit log produced during transformation."""
        self.lineage_data["transformation"] = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "audit_log": audit_log
        }

    def record_load(self, output_path: str | Path):
        """Record where the final processed data was saved."""
        self.lineage_data["load"] = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "output_path": str(output_path)
        }

    def save_lineage(self) -> Path:
        """
        Save the lineage metadata as a JSON file alongside the output file.

        Example:
        If output is `data/processed/clean_online_retail.parquet`,
        lineage is saved as `data/processed/clean_online_retail.lineage.json`.
        """
        if not self.lineage_data["load"].get("output_path"):
            raise ValueError("Cannot save lineage: output_path has not been recorded.")

        output_path = Path(self.lineage_data["load"]["output_path"])

        # Create sidecar filename
        lineage_filename = output_path.stem + ".lineage.json"
        lineage_path = output_path.parent / lineage_filename

        with open(lineage_path, "w", encoding="utf-8") as f:
            json.dump(self.lineage_data, f, indent=2)

        logger.info(f"[Lineage] Saved lineage metadata to: {lineage_path}")
        return lineage_path
