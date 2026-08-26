# DataTrust — Automated Data Quality & Pipeline Observability Platform

> A portfolio-quality Data Engineering project built incrementally — working, explainable, and testable at every step.

---

## What is DataTrust?

DataTrust monitors data pipelines and automatically determines whether incoming data can be trusted. It detects missing values, duplicate records, invalid values, schema drift, volume anomalies, freshness problems, and pipeline failures — then calculates an overall **Data Health Score**.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Data Processing | Pandas, NumPy |
| Database | PostgreSQL + SQLAlchemy (Phase 4) |
| Orchestration | Apache Airflow (Phase 7+) |
| API | FastAPI (Phase 8+) |
| Dashboard | Streamlit + Plotly (Phase 9+) |
| Testing | Pytest |
| Containerization | Docker + Docker Compose (Phase 15+) |
| CI/CD | GitHub Actions (Phase 16+) |

---

## Dataset

**UCI Online Retail Dataset**
- Source: https://archive.ics.uci.edu/dataset/352/online+retail
- License: CC BY 4.0
- Rows: 541,909 | Columns: 8 | Date range: Dec 2010 – Dec 2011
- UK-based online retailer selling gift ware

> The original dataset is never modified. All transformations work on copies.

---

## Project Structure

```
DataTrust/
├── data/
│   ├── raw/                  # Original dataset (never modified)
│   ├── processed/            # Cleaned / transformed outputs
│   └── test/                 # Controlled failure datasets
│       ├── missing_values/
│       ├── duplicates/
│       ├── invalid_values/
│       ├── schema_drift/
│       ├── volume_anomaly/
│       └── freshness/
├── src/
│   ├── config.py             # Central configuration
│   ├── logger.py             # Shared logging
│   ├── ingestion/            # Data loading layer
│   │   └── loader.py
│   ├── profiling/            # Dataset profiling
│   │   └── profiler.py
│   ├── quality/              # Data quality engine (Phase 3+)
│   ├── scoring/              # Health score calculation (Phase 4+)
│   ├── database/             # PostgreSQL models (Phase 5+)
│   ├── api/                  # FastAPI endpoints (Phase 8+)
│   ├── pipeline/             # ETL pipeline (Phase 6+)
│   ├── anomaly/              # Anomaly detection (Phase 11+)
│   └── lineage/              # Data lineage (Phase 13+)
├── tests/
│   ├── unit/                 # Fast tests, no external deps
│   └── integration/          # Tests requiring DB / API
├── dags/                     # Airflow DAGs (Phase 7+)
├── dashboard/                # Streamlit app (Phase 9+)
├── docker/                   # Dockerfiles (Phase 15+)
├── .github/workflows/        # CI/CD (Phase 16+)
├── reports/                  # Generated quality reports (JSON)
├── logs/                     # Runtime logs
├── config/                   # External config files
├── run_profiler.py           # Phase 1 entry point
├── requirements.txt
├── setup.cfg                 # pytest + flake8 config
├── .env.example              # Environment variable template
└── .gitignore
```

---

## Setup Instructions

### Prerequisites
- Python 3.11 or 3.13
- Git (download from https://git-scm.com if not installed)

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/DataTrust.git
cd DataTrust
```

### 2. Create a virtual environment
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac / Linux
source .venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

> **Note on Python 3.13:** `numpy 1.26.x` does not ship pre-built wheels for Python 3.13.
> This project uses `numpy>=2.1.3` which has official Python 3.13 support.

### 4. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your settings
```

### 5. Place the dataset
Download the UCI Online Retail dataset from:
https://archive.ics.uci.edu/dataset/352/online+retail

Place `online_retail.csv` (or convert the Excel file) at:
```
data/raw/online_retail.csv
```

---

## Running Phase 1 — Dataset Profiling

```bash
python run_profiler.py
```

**Output:**
- Console summary with all profiling stats
- JSON report saved to `reports/phase1_profile.json`
- Log file at `logs/datatrust.log`

---

## Running Phase 2 — Data Quality Validation

```bash
python run_quality.py
```

This creates a timestamped JSON report in `reports/`. Every result includes a
check name, category, status, severity, affected rows/percentage, plain-English
message, and check-specific metadata.

Phase 2 rules are deliberately business-aware: missing `CustomerID` values are
reported as `INFO` because guest transactions are valid; cancellation invoices
(`InvoiceNo` starting with `C`) may have negative quantities; negative prices
are failures; zero-price records, missing descriptions, and exact duplicate
rows are warnings.

---

## Running Phase 3 — Data Health Score

```bash
python run_scoring.py
```

This runs the Phase 2 checks, converts their structured results into a
0–100 score, and saves [health_score_latest.json](reports/health_score_latest.json).
The score uses five equally weighted dimensions: schema, completeness, validity,
uniqueness, and business rules. `PASS`/`INFO` earn full rule points, `WARNING`
earns half, and `FAIL` earns none. The report lists every contributing check,
so the score is explainable rather than a black box.

---

## Phase 4 — PostgreSQL Historical Quality Storage

Phase 4 stores one pipeline run and its individual validation results so later
product layers can show quality history without re-reading old JSON files.

```
Dataset -> Quality Engine -> Health Score -> PostgreSQL
                                            |          |
                                      pipeline_runs  quality_results
```

### Database setup

1. Create a local PostgreSQL database and user.
2. Copy `.env.example` to an untracked `.env` file.
3. Set `DATABASE_URL` using the supplied PostgreSQL URL template. Never commit
   real credentials.
4. Install dependencies and initialize the schema:

```bash
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python scripts/init_db.py
```

The initialization command is safe to run repeatedly. It verifies the
connection and creates `pipeline_runs` and `quality_results` when absent.

### Store a run and query history

```bash
.venv\Scripts\python run_database.py
```

`pipeline_runs` stores timing, status, row count, and health score. Every
structured Phase 2 result is stored in `quality_results` through the `run_id`
foreign key. The repository supports latest/recent runs, results for a run,
historical scores, failed checks, and warnings.

Example PostgreSQL queries:

```sql
-- Most recent run
SELECT run_id, pipeline_name, health_score, status, finished_at
FROM pipeline_runs ORDER BY run_id DESC LIMIT 1;

-- Quality trend
SELECT started_at, health_score
FROM pipeline_runs ORDER BY started_at;

-- Failures for a particular run
SELECT check_name, category, severity, affected_rows, message
FROM quality_results WHERE run_id = 1 AND status = 'FAIL';
```

### Cancellation-rule investigation

The 1,336 negative quantities without a `C` prefix are not automatically
treated as corruption. All 1,336 are zero-price records with missing
`CustomerID` (and 862 also have no `Description`), which is an
administrative-adjustment pattern.
DataTrust reports this as a medium `WARNING`: it stays visible for
investigation, but is not claimed to be a definite failed cancellation rule.

---

## Phase 5 — Lossless ETL Pipeline

```
data/raw/online_retail.csv
  -> Extract -> Transform -> PostgreSQL retail_transactions
  -> Validate -> Score -> pipeline_runs + quality_results
```

Run the complete pipeline after configuring PostgreSQL:

```bash
.venv\Scripts\python run_etl.py
```

The raw CSV is immutable: extraction reads it only, and transformation works on
a DataFrame copy. The pipeline never deletes duplicates, missing CustomerIDs,
negative prices, or suspicious adjustment rows. Those records are deliberately
preserved so the quality engine can detect and report them.

Transformations are limited to safe normalization and derivation:

- `InvoiceDate` is parsed, verified, and normalized to ISO format.
- `Quantity` and `UnitPrice` are converted to numeric values.
- `IsCancellation` is derived from the `C` invoice prefix.
- `Revenue` is `Quantity * UnitPrice`; cancelled transactions therefore have
  negative revenue, and no quality concern is hidden.

The PostgreSQL `retail_transactions` table is the current transformed snapshot.
Each load replaces that snapshot and verifies the exact row count. The original
source remains untouched; historical execution and quality history stay in
`pipeline_runs` and `quality_results`.

An ETL execution status is intentionally independent from data quality. For
example, an ETL run can be `SUCCESS` with a 72.08 health score and quality
warnings/failures: processing worked, but DataTrust found concerns that deserve
attention. Extraction, transformation, and database failures instead return a
clear failed stage and stop the pipeline.

---

## Running Tests

```bash
# All tests
pytest

# Only unit tests (fast, no external dependencies)
pytest -m unit

# Specific test file
pytest tests/unit/test_profiler.py -v
```

---

## Phase 1 Findings — Real Dataset Profile

| Metric | Value |
|---|---|
| Total rows | 541,909 |
| Total columns | 8 |
| Memory usage | 173 MB |
| Date range | 2010-12-01 → 2011-12-09 (373 days) |
| Trading days | 305 |
| Avg transactions/day | 1,776 |
| Unique customers | 4,372 |
| Unique products | 4,070 |
| Countries | 38 |
| Unique invoices | 25,900 |

### Quality Issues Found

| Issue | Count | % | Verdict |
|---|---|---|---|
| Missing CustomerID | 135,080 | 24.9% | Expected (guest purchases) |
| Missing Description | 1,454 | 0.3% | Worth investigating |
| Duplicate rows | 5,268 | 0.97% | Needs cleaning |
| Cancellation invoices | 9,288 | 1.7% | Expected business behaviour |
| Negative quantities | 10,624 | 2.0% | Mostly valid (cancellations) |
| Zero-price items | 2,515 | 0.5% | Possible data issue |
| Negative prices | 2 | <0.01% | Definite data issue |

> Key insight: **24.9% missing CustomerID is not a bug** — it represents anonymous/guest transactions, which is normal for online retail. The profiler documents this so we don't incorrectly penalise it in the quality score.

---

## Build Milestones

- [x] **Phase 1** — Environment, project structure, dataset profiling
- [x] **Phase 2** — Data quality validation engine
- [x] **Phase 3** — Explainable Data Health Score
- [x] **Phase 4** — PostgreSQL historical quality storage
- [x] **Phase 5** — Lossless ETL pipeline
- [ ] Phase 6 — Apache Airflow orchestration
- [ ] Phase 8 — FastAPI backend
- [ ] Phase 9 — Streamlit dashboard
- [ ] Phase 10 — Schema drift detection
- [ ] Phase 11 — Volume/freshness anomaly detection
- [ ] Phase 12 — Pipeline observability
- [ ] Phase 13 — Data lineage
- [ ] Phase 14 — Pytest testing suite
- [ ] Phase 15 — Docker/Docker Compose
- [ ] Phase 16 — GitHub Actions CI/CD
- [ ] Phase 17 — AWS deployment (optional)

---

## Environment Issues Encountered & Fixed

### numpy 1.26.4 incompatible with Python 3.13
**Problem:** numpy 1.26.x has no pre-built wheel for Python 3.13. Pip tried to compile from source and failed because no C compiler (gcc/cl.exe) was found.

**Fix:** Use `numpy>=2.1.3` which ships official Python 3.13 wheels.

### UnicodeEncodeError on Windows terminal
**Problem:** Windows PowerShell uses cp1252 encoding by default. Unicode emoji (✓, ❌) caused `charmap` encode errors.

**Fix:** Set `sys.stdout` to UTF-8 at script startup. Replaced remaining emojis with plain ASCII markers.

---

## License

Dataset: UCI Machine Learning Repository — CC BY 4.0
Project code: MIT
