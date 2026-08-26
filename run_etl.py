"""Manual entry point for the DataTrust Phase 5 ETL pipeline."""

from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from src.etl.pipeline import run_etl_pipeline


def main() -> None:
    result = run_etl_pipeline()
    print("\nDATATRUST ETL RUN\n")
    print(f"Status: {result.status}")
    print(f"Rows extracted:   {result.rows_extracted:,}")
    print(f"Rows transformed: {result.rows_transformed:,}")
    print(f"Rows loaded:      {result.rows_loaded:,}")
    if result.status == "SUCCESS":
        print(f"Health Score: {result.health_score}/100")
        print(f"Quality failures: {result.quality_failures}")
        print(f"Warnings: {result.quality_warnings}")
        print(f"Pipeline run ID: {result.run_id}")
    else:
        print(f"Failed stage: {result.error_stage}")
        print(f"Error: {result.error_message}")
    print(f"Duration: {result.duration_seconds:.2f}s")
    raise SystemExit(0 if result.status == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
