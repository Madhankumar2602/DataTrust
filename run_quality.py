"""
run_quality.py — Entry point for Phase 2: Data Quality Validation Engine.

Run this script to:
  1. Load the UCI Online Retail dataset from data/raw/
  2. Run the Data Quality Validation Engine
  3. Save the report to reports/
  4. Print a human-readable summary to the console

Usage:
    python run_quality.py
"""

import sys
import io
from pathlib import Path

# Force stdout to utf-8 so Unicode characters work on Windows.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Make sure Python can find the 'src' package when running from project root
sys.path.insert(0, str(Path(__file__).parent))

from src.config import settings
from src.ingestion.loader import load_csv
from src.quality.engine import QualityEngine
from src.logger import get_logger

logger = get_logger("run_quality")


def main() -> None:
    print("=" * 65)
    print("  DataTrust - Phase 2: Data Quality Validation Engine")
    print("=" * 65)

    # ── Step 1: Load the dataset ───────────────────────────────────────────
    print(f"\n[1/4] Loading dataset from: {settings.RAW_DATA_PATH}")
    try:
        df = load_csv()
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

    print(f"      [OK] Loaded {len(df):,} rows x {len(df.columns)} columns")

    # ── Step 2: Run the quality engine ─────────────────────────────────────
    print("\n[2/4] Running Data Quality Validation Engine ...")
    engine = QualityEngine(dataset_name="UCI Online Retail")
    report = engine.run(df)

    # ── Step 3: Save the report ────────────────────────────────────────────
    print("\n[3/4] Saving report ...")
    report_path = engine.save_report(report)
    print(f"      [OK] Report saved to: {report_path}")

    # ── Step 4: Print human-readable summary ──────────────────────────────
    print("\n[4/4] Validation Summary")
    print_summary(report)

    print("\n" + "=" * 65)
    print("  [DONE] Phase 2 Complete!")
    print(f"  Full report: {report_path}")
    print("=" * 65)


def print_summary(report: dict) -> None:
    """Print a clean, human-readable version of the validation report."""
    print("\nDATATRUST DATA QUALITY VALIDATION\n")
    
    # Define severity markers for visual clarity
    markers = {
        "PASS": "[OK]  ",
        "WARNING": "[WARN]",
        "FAIL": "[FAIL]",
        "INFO": "[INFO]"
    }
    
    # Group results by category
    categories = {}
    for result in report["results"]:
        cat = result["category"].title()
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(result)
        
    for cat, results in categories.items():
        print(f"── {cat} ───────────────────────────────────────")
        for r in results:
            marker = markers.get(r["status"], "[    ]")
            print(f"  {marker} {r['check_name']}")
            print(f"         {r['message']}")
            if r['affected_rows'] > 0:
                print(f"         Affected: {r['affected_rows']:,} rows ({r['affected_pct']}%)")
        print()
        
    summary = report["summary"]
    print("── Summary ────────────────────────────────────────")
    print(f"  Total Checks: {summary['total_checks']}")
    print(f"  Passed:       {summary['passed']}")
    print(f"  Warnings:     {summary['warnings']}")
    print(f"  Failed:       {summary['failed']}")
    print(f"  Info:         {summary['info']}")
    print(f"  Time taken:   {report['duration_seconds']}s")


if __name__ == "__main__":
    main()
