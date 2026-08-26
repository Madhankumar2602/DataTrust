"""
run_profiler.py — Entry point for Phase 1: Dataset Profiling.

Run this script to:
  1. Load the UCI Online Retail dataset from data/raw/
  2. Profile it completely
  3. Save the report to reports/
  4. Print a human-readable summary to the console

Usage:
    python run_profiler.py

Prerequisites:
    - data/raw/online_retail.csv must exist
    - pip install -r requirements.txt must have been run
"""

import io
import sys
# Force stdout to utf-8 so Unicode box-drawing characters work on Windows.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import json
from pathlib import Path

# Make sure Python can find the 'src' package when running from project root
sys.path.insert(0, str(Path(__file__).parent))

from src.config import settings
from src.ingestion.loader import load_csv, get_dataset_info
from src.profiling.profiler import DataProfiler
from src.logger import get_logger

logger = get_logger("run_profiler")


def main() -> None:
    print("=" * 65)
    print("  DataTrust - Phase 1: Dataset Profiling")
    print("=" * 65)

    # ── Step 1: Load the dataset ───────────────────────────────────────────
    print(f"\n[1/4] Loading dataset from: {settings.RAW_DATA_PATH}")
    try:
        df = load_csv()
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

    info = get_dataset_info(df)
    print(f"      [OK] Loaded {info['rows']:,} rows x {info['columns']} columns")
    print(f"      [OK] Memory usage: {info['memory_usage_mb']} MB")

    # ── Step 2: Run the profiler ───────────────────────────────────────────
    print("\n[2/4] Profiling dataset (this may take 15-30 seconds for 541k rows)...")
    profiler = DataProfiler(df, dataset_name="UCI Online Retail")
    report = profiler.run()

    # ── Step 3: Save the report ────────────────────────────────────────────
    print("\n[3/4] Saving report ...")
    report_path = profiler.save_report(report, Path(settings.REPORTS_PATH) / "phase1_profile.json")
    print(f"      [OK] Report saved to: {report_path}")

    # -- Step 4: Print human-readable summary
    print("\n[4/4] Profile Summary")
    print_summary(report)

    print("\n" + "=" * 65)
    print("  [DONE] Phase 1 Complete!")
    print(f"  Full report: {report_path}")
    print("=" * 65)


def print_summary(report: dict) -> None:
    """Print a clean, human-readable version of the profile report."""

    ov  = report["overview"]
    mv = report["missing_values"]
    dup = report["duplicates"]
    dates = report["date_analysis"]
    biz = report["business_analysis"]
    flags = report["quality_flags"]

    # -- Overview
    print("\n  [OVERVIEW]")
    print(f"     Rows:            {ov['rows']:>12,}")
    print(f"     Columns:         {ov['columns']:>12}")
    print(f"     Memory:          {ov['memory_usage_mb']:>11.2f} MB")
    print(f"     Columns:         {', '.join(ov['column_names'])}")

    # -- Missing Values
    print("\n  [MISSING VALUES]")
    print(f"     Total missing cells:  {mv['total_missing_cells']:>10,}")
    print(f"     Missing percentage:   {mv['total_missing_percentage']:>9.2f}%")
    print(f"     Columns with missing: {mv['columns_with_missing']:>10}")
    if mv["per_column"]:
        for col, stats in mv["per_column"].items():
            print(
                f"       • {col:<20} {stats['missing_count']:>8,} rows "
                f"({stats['missing_percentage']:.1f}%)"
            )

    # -- Duplicates
    print("\n  [DUPLICATES]")
    print(f"     Duplicate rows:      {dup['duplicate_row_count']:>10,}")
    print(f"     Duplicate %:         {dup['duplicate_percentage']:>9.2f}%")

    # -- Date Range
    if "error" not in dates:
        print("\n  [DATE RANGE]")
        print(f"     Earliest date:  {dates['earliest_date']}")
        print(f"     Latest date:    {dates['latest_date']}")
        print(f"     Span:           {dates['date_span_days']} days")
        dv = dates["daily_volume"]
        print(f"     Trading days:   {dv['total_trading_days']}")
        print(f"     Avg txns/day:   {dv['mean_transactions_per_day']:,.1f}")
        print(f"     Max txns/day:   {dv['max_transactions_per_day']:,}")

    # -- Business Analysis
    print("\n  [BUSINESS ANALYSIS]")
    if "customers" in biz:
        print(f"     Unique customers:    {biz['customers']['unique_customers']:>10,}")
        print(f"     Rows w/o CustomerID: {biz['customers']['rows_without_customer_id']:>10,}")
    if "products" in biz:
        print(f"     Unique products:     {biz['products']['unique_products']:>10,}")
    if "geography" in biz:
        print(f"     Unique countries:    {biz['geography']['unique_countries']:>10}")
        top = biz["geography"]["top_5_countries"]
        print(f"     Top countries:       {list(top.keys())}")
    if "invoices" in biz:
        iv = biz["invoices"]
        print(f"     Unique invoices:     {iv['unique_invoices']:>10,}")
        print(f"     Cancellations:       {iv['cancellation_invoices']:>10,} ({iv['cancellation_percentage']:.1f}%)")
    if "quantity" in biz:
        q = biz["quantity"]
        print(f"     Negative qty rows:   {q['negative_quantity_rows']:>10,}  (expected: cancellations)")
        print(f"     Zero qty rows:       {q['zero_quantity_rows']:>10,}")
        print(f"     Qty range:           {q['min_quantity']:,} → {q['max_quantity']:,}")
    if "unit_price" in biz:
        p = biz["unit_price"]
        print(f"     Zero-price rows:     {p['zero_price_rows']:>10,}")
        print(f"     Negative-price rows: {p['negative_price_rows']:>10,}")
        print(f"     Price range:         £{p['min_price']} → £{p['max_price']}")

    # -- Quality Flags
    print("\n  [QUALITY FLAGS]")
    severity_marker = {
        "CRITICAL": "[!!]",
        "WARNING":  "[! ]",
        "INFO":     "[i ]",
        "OK":       "[OK]",
    }
    for flag in flags:
        marker = severity_marker.get(flag["severity"], "[  ]")
        print(f"     {marker} [{flag['severity']:<8}] {flag['flag']}")
        print(f"            {flag['message']}")
        if "note" in flag:
            print(f"            Note: {flag['note']}")

    # -- Per-column summary
    print("\n  [COLUMN DETAIL]")
    print(f"     {'Column':<20} {'Type':<12} {'Nulls':>8} {'Null%':>7} {'Unique':>10}")
    print("     " + "-" * 60)
    for col in report["columns"]:
        print(
            f"     {col['name']:<20} {col['dtype']:<12} "
            f"{col['null_count']:>8,} {col['null_percentage']:>6.1f}% "
            f"{col['unique_count']:>10,}"
        )


if __name__ == "__main__":
    main()
