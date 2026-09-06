"""Manual entry point for the DataTrust Phase 5 ETL pipeline."""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from src.etl.pipeline import run_etl_pipeline


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DataTrust ETL pipeline.")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Load only source rows the warehouse does not already hold.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Rebuild the whole snapshot and reset the checkpoint.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        metavar="N",
        help="Also re-examine N days below the checkpoint, for late-arriving rows.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_etl_pipeline(
        incremental=args.incremental,
        full_refresh=args.full_refresh,
        lookback_days=args.lookback_days,
    )
    print("\nDATATRUST ETL RUN\n")
    print(f"Status: {result.status}")
    print(f"Mode:   {result.mode}")
    print(f"Rows extracted:   {result.rows_extracted:,}")
    print(f"Rows transformed: {result.rows_transformed:,}")
    print(f"Rows loaded:      {result.rows_loaded:,}")
    if result.mode == "incremental":
        print(f"Rows skipped:     {result.rows_skipped:,}")
        print(f"Checkpoint before: {result.watermark_before}")
        print(f"Checkpoint after:  {result.watermark_after}")
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
