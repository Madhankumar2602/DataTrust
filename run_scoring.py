"""
run_scoring.py — Entry point for Phase 3: Data Health Score.

Run this script to:
  1. Load the dataset
  2. Run the Data Quality Engine (Phase 2)
  3. Calculate the Data Health Score (Phase 3)
  4. Print a human-readable summary to the console

Usage:
    python run_scoring.py
"""

import sys
import io
from pathlib import Path
import json

# Force stdout to utf-8 so Unicode characters work on Windows.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Make sure Python can find the 'src' package when running from project root
sys.path.insert(0, str(Path(__file__).parent))

from src.config import settings
from src.ingestion.loader import load_csv
from src.quality.engine import QualityEngine
from src.scoring.scorer import HealthScorer
from src.logger import get_logger

logger = get_logger("run_scoring")


def main() -> None:
    print("=" * 65)
    print("  DataTrust - Phase 3: Data Health Score")
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
    quality_report = engine.run(df)
    print("      [OK] Validation checks completed.")

    # ── Step 3: Calculate Health Score ─────────────────────────────────────
    print("\n[3/4] Calculating Data Health Score ...")
    scorer = HealthScorer()
    score_report = scorer.calculate_score(quality_report)

    # Save the score report alongside the quality report
    score_path = Path(settings.REPORTS_PATH) / "health_score_latest.json"
    with open(score_path, "w", encoding="utf-8") as f:
        json.dump(score_report, f, indent=2)

    print(f"      [OK] Score calculated and saved to: {score_path}")

    # ── Step 4: Print human-readable summary ──────────────────────────────
    print("\n[4/4] Data Health Score Summary")
    print_summary(score_report)

    print("\n" + "=" * 65)
    print("  [DONE] Phase 3 Complete!")
    print(f"  Full report: {score_path}")
    print("=" * 65)


def print_summary(report: dict) -> None:
    """Print a clean, human-readable version of the score report."""
    print("\nDATATRUST DATA HEALTH SCORE\n")

    if "failure_reason" in report:
        print(f"  CRITICAL FAILURE: {report['failure_reason']}")
        print(f"  OVERALL SCORE: {report['score']}/100")
        return

    score = report["score"]
    status = report["status"]

    print(f"  OVERALL SCORE: {score}/100  ({status})")
    print("\n  Category Breakdown:")
    print("  -------------------")

    for category, details in report["category_scores"].items():
        cat_score = details["score"]
        max_score = details["max_score"]
        cat_name = category.title()

        print(f"  {cat_name:<15}: {cat_score:>5.1f} / {max_score:<5.1f}  - {details['details']}")


if __name__ == "__main__":
    main()
