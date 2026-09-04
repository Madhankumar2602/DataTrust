# DataTrust — Automated Data Quality & Pipeline Observability Platform

> **DataTrust** determines whether data can be trusted by validating data quality, calculating a health score, detecting anomalies, storing historical results, and exposing the results through an observability dashboard.

---

## Architecture

```
Raw CSV Dataset
       │
       ▼
 ┌───────────┐
 │    ETL    │  Lossless extraction & transformation (batch load)
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │   MySQL   │  Normalized staging table (`retail_transactions`)
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │  Quality  │  Schema, Completeness, Validity, Uniqueness checks
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │  Scoring  │  Weighted 0–100 Data Health Score & tier categorization
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │  Anomaly  │  Statistical rolling Z-score & domain heuristics
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │  History  │  Persistent audit log (`pipeline_runs`, `quality_results`, `anomaly_results`)
 └─────┬─────┘
       │
       ▼
 ┌───────────┐
 │ Dashboard │  Streamlit + Plotly interactive observability UI
 └───────────┘
```

- **Orchestration**: Apache Airflow DAG (`dags/datatrust_pipeline.py`) coordinates the sequential execution, dependencies, and retries across stages.
- **Continuous Integration**: GitHub Actions (`.github/workflows/ci.yml`) validates the entire test suite on every push and pull request.

---

## Technology Stack

| Layer | Technology | Description |
|---|---|---|
| **Runtime** | Python 3.13 | Core execution environment |
| **Data Processing** | Pandas 2.2, NumPy 2.1 | In-memory extraction, transformation, and statistical calculations |
| **Database & ORM** | MySQL 8.0+, SQLAlchemy 2.0 | Transaction staging, pipeline run tracking, quality metrics, anomaly records |
| **Observability** | Streamlit, Plotly | Interactive analytics and data quality monitoring dashboard |
| **Orchestration** | Apache Airflow | Multi-stage pipeline scheduling and execution flow |
| **Testing** | Pytest | Comprehensive unit test suite (isolated in-memory SQLite fixtures) |
| **CI/CD** | GitHub Actions | Automated build and test validation |

---

## Project Structure

```
DataTrust/
├── .github/workflows/
│   └── ci.yml                 # GitHub Actions CI workflow
├── dags/
│   └── datatrust_pipeline.py  # Airflow DAG definition and stage callables
├── dashboard/
│   └── app.py                 # Streamlit observability dashboard
├── data/
│   ├── raw/                   # Immutable raw dataset (online_retail.csv)
│   └── processed/             # Cleaned Parquet exports (when needed)
├── src/
│   ├── config.py              # Centralized environment & settings configuration
│   ├── logger.py              # Structured application logger
│   ├── ingestion/
│   │   └── loader.py          # Pure CSV / Excel ingestion loader
│   ├── profiling/
│   │   └── profiler.py        # Dataset statistical profiler
│   ├── quality/
│   │   ├── base.py            # Quality check base classes and result dataclasses
│   │   ├── schema.py          # Schema contract validation
│   │   ├── completeness.py    # Missing value and null ratio rules
│   │   ├── validity.py        # Domain rules, range checks, cancellation checks
│   │   ├── uniqueness.py      # Duplicate record detection
│   │   └── engine.py          # Validation engine orchestrator
│   ├── scoring/
│   │   └── scorer.py          # Health Score calculation engine (0–100)
│   ├── database/
│   │   ├── connection.py      # Database engine & session factory
│   │   ├── models.py          # SQLAlchemy ORM models (DeclarativeBase)
│   │   └── repository.py      # QualityRepository CRUD & querying abstraction
│   ├── etl/
│   │   ├── extractor.py       # Source CSV extraction
│   │   ├── transformer.py     # Lossless feature normalization and typing
│   │   ├── loader.py          # Chunked batch loader into MySQL
│   │   └── pipeline.py        # Composable ETL runner
│   └── anomaly/
│       ├── detector.py        # Rolling Z-score anomaly detector
│       └── rules.py           # Severity classification & domain rules
├── tests/
│   └── unit/
│       ├── test_profiler.py
│       ├── test_quality.py
│       ├── test_scorer.py
│       ├── test_database.py
│       ├── test_etl.py
│       ├── test_anomaly.py
│       ├── test_orchestration.py
│       └── test_api.py
├── scripts/
│   ├── init_db.py             # Create tables only (idempotent, loads no data)
│   └── smoke_test_api.py      # HTTP smoke test against a running API
├── reports/                   # Generated JSON validation and profile reports
├── logs/                      # Application execution logs
├── run_profiler.py            # CLI: Run dataset profiler
├── run_quality.py             # CLI: Run data quality checks
├── run_scoring.py             # CLI: Run health scoring calculation
├── run_database.py            # CLI: Run historical persistence check
├── run_etl.py                 # CLI: Run complete 541k-row ETL pipeline
├── run_anomaly.py             # CLI: Run time-series anomaly detection
├── Dockerfile                 # One image for API, dashboard and db-init
├── docker-compose.yml         # Local stack: MySQL + FastAPI + Streamlit
├── .dockerignore              # Keeps secrets and datasets out of the image
├── requirements.txt           # Project dependencies
├── setup.cfg                  # Pytest & Flake8 configuration
├── .env.example               # Environment variables template
├── .gitattributes             # Line-ending normalisation
└── .gitignore                 # Git ignore configuration
```

---

## Setup & Quickstart

### 1. Prerequisites
- Python 3.13 (or 3.11+)
- MySQL Server (running locally or in container on port 3306)
- Git

### 2. Virtual Environment Setup
```bash
# Create and activate virtual environment
python -m venv .venv

# Windows PowerShell:
.venv\Scripts\Activate.ps1

# Linux / macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `.env` and configure your MySQL credentials:
```bash
cp .env.example .env
```
Inside `.env`:
```ini
DATABASE_URL=mysql+mysqlconnector://root:your_password@localhost:3306/datatrust
APP_ENV=development
LOG_LEVEL=INFO
```

Make sure the `datatrust` database exists in MySQL:
```sql
CREATE DATABASE IF NOT EXISTS datatrust;
```

---

## Running the Pipeline

You can run individual pipeline stages via CLI runners:

### 1. Dataset Profiling
```bash
python run_profiler.py
```
*Profiles 541,909 rows, generating summary stats, missing rates, and duplicate counts into `reports/phase1_profile.json`.*

### 2. Data Quality Validation
```bash
python run_quality.py
```
*Executes 16 quality checks across schema, completeness, validity, and uniqueness, saving results to `reports/quality_<timestamp>.json`.*

### 3. Data Health Scoring
```bash
python run_scoring.py
```
*Computes the weighted 0–100 Data Health Score and assigns a status tier (`EXCELLENT`, `GOOD`, `POOR`, `CRITICAL`).*

### 4. Database Persistence
```bash
python run_database.py
```
*Runs quality checks, computes health scores, and persists run metadata to MySQL tables.*

### 5. Full ETL Pipeline (541k Rows)
```bash
python run_etl.py
```
*Extracts raw data, applies lossless transformations (`is_cancellation`, `revenue`, ISO timestamps), and batch-loads 541,909 records into MySQL.*

### 6. Time-Series Anomaly Detection
```bash
python run_anomaly.py
```
*Analyzes monthly trends in revenue, volume, and cancellation rates, detecting and persisting anomalies to MySQL.*

---

## Observability Dashboard

Launch the interactive Streamlit dashboard:
```bash
streamlit run dashboard/app.py
```
Navigate to `http://localhost:8501` to view:
- **System Overview**: Overall health gauges, latest pipeline metrics, and quality distribution.
- **Data Quality Explorer**: Breakdown of pass/warning/failure checks and affected row counts.
- **Retail Analytics**: Transaction snapshots, top products, and geographical distribution.
- **Pipeline History**: Historical trend charts of data health scores over time.
- **Anomaly Detection Radar**: KPI cards, deviation bar charts, and detailed anomaly history.

---

## Airflow Orchestration

The pipeline DAG is defined in [`dags/datatrust_pipeline.py`](file:///C:/Users/madhan/OneDrive/Desktop/DataTrust/dags/datatrust_pipeline.py) with 4 sequential tasks:
```
[extract_transform_load] ──▶ [quality_validation] ──▶ [health_scoring_and_persistence] ──▶ [anomaly_detection]
```
All tasks use modular callables referencing core `src/` modules, allowing direct execution and testing even outside of an Airflow cluster.

---

## Running with Docker

The Compose stack runs the three services the application needs: **MySQL**,
the **FastAPI** service and the **Streamlit** dashboard. Airflow is not part of
it — orchestration stays local for this project, so the stack contains no
scheduler, webserver or Airflow metadata database.

### Prerequisites
- **Docker Desktop** (or Docker Engine) with the `docker compose` CLI
- The repository checked out locally — everything else lives in the image
- **No local Python virtual environment is required to run the stack.** The
  containers install from `requirements.txt` themselves. The venv is only for
  running the test suite and the CLI runners outside Docker.

### Environment setup
Copy the template and fill in your own values:

```bash
cp .env.example .env          # PowerShell: copy .env.example .env
```

| Variable | Required | Purpose |
|---|---|---|
| `MYSQL_ROOT_PASSWORD` | yes | Root password for the MySQL container |
| `MYSQL_USER` | yes | Application account the API and dashboard connect as |
| `MYSQL_PASSWORD` | yes | Password for that account |
| `MYSQL_DATABASE` | no | Schema name; defaults to `datatrust` |
| `MYSQL_HOST_PORT` | no | Host port for MySQL; defaults to `3307` |

Compose fails with a clear error if any of the three required values is missing,
rather than falling back to a default password. `.env` is gitignored and is
never copied into the image.

### Start the stack
```bash
docker compose config     # validate the compose file and .env before building
docker compose build
docker compose up -d
docker compose ps
```

| Service | Role | Address |
|---|---|---|
| `mysql` | MySQL 8.0; data in the named volume `datatrust_mysql_data` | `localhost:3307` (3306 stays free for a local MySQL) |
| `db-init` | One-shot: creates tables, then exits. Loads no data. | — |
| `api` | FastAPI served by Uvicorn; `/health` reports database connectivity | http://localhost:8000 · docs at http://localhost:8000/docs |
| `dashboard` | Streamlit; reads MySQL directly for heavy retail queries | http://localhost:8501 |

### What happens on first start
1. `mysql` starts and creates the database named by `MYSQL_DATABASE`.
2. `db-init` runs `scripts/init_db.py` once, creates any missing tables from
   `src/database/models.py`, and exits. **It loads no data.**
3. `api` waits for the healthcheck and for `db-init` to finish, then serves.
4. `dashboard` starts once MySQL is healthy.

The retail dataset is never imported automatically — restarting the stack
cannot re-load 541,909 rows. Loading data is one explicit command:

```bash
docker compose run --rm api python run_etl.py
```

`data/`, `reports/` and `logs/` are bind-mounted from the host, so the ETL
reads the CSV you already have and writes reports back where you can see them.

### Configuration
Containers do not use the `DATABASE_URL` from your `.env`; that value points at
`localhost`, which inside a container means the container itself. Compose builds
a container-specific URL from the MySQL variables instead:

```
mysql+mysqlconnector://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql:3306/${MYSQL_DATABASE}
```

The hostname `mysql` is the Compose service name. No credentials appear in any
source file or in the image.

### Checking status and logs
`docker compose ps` shows each service's state and health. `mysql`, `api` and
`dashboard` carry healthchecks, so a healthy stack reads `running (healthy)`;
`db-init` is expected to show `exited (0)` once it has created the tables.

```bash
docker compose ps
docker compose logs --tail=100 mysql
docker compose logs --tail=100 api
docker compose logs --tail=100 dashboard
docker compose logs -f api          # follow one service
```

If `api` is unhealthy, its log usually points at MySQL — check `mysql` first.

### Data persistence
MySQL data lives in the named volume `datatrust_mysql_data`:

```bash
docker compose down     # containers removed, data kept
docker compose up -d    # same data comes back
docker compose down -v  # DESTROYS the volume and all data
```

### Rebuilding after a change
```bash
docker compose build
docker compose up -d
```

Use `docker compose build --no-cache api` if a dependency change is not picked
up, and `docker compose restart api` for a code-only change.

### Verifying a running stack
```bash
python scripts/smoke_test_api.py --base-url http://127.0.0.1:8000
```

The smoke test checks status codes, response shape, pagination, 404/422
handling and that error responses expose no database internals. Before the ETL
has been run, `/api/v1/health-score` correctly answers 404 — that is the
expected empty-data response, not a failure.

---

## Testing & Quality Assurance

Run the complete test suite with Pytest:
```bash
pytest -v
```
All 64 unit tests use in-memory SQLite fixtures with zero external database requirements:
- `test_profiler.py` — Profiling stats and null calculation
- `test_quality.py` — Schema, completeness, validity, uniqueness rules
- `test_scorer.py` — Scoring formulas, weights, and tier classification
- `test_database.py` — SQLAlchemy ORM models, relations, and repository queries
- `test_etl.py` — Extractor, Transformer, Batch Loader, and Error handling
- `test_anomaly.py` — Statistical anomaly detection, baseline history, and business rules
- `test_orchestration.py` — Airflow DAG task callables, sequential execution, and failure bubbling

---

## Continuous Integration (GitHub Actions)

Every pull request and push to `main` triggers `.github/workflows/ci.yml`:
1. Sets up Python 3.13 environment
2. Installs pinned dependencies from `requirements.txt`
3. Sets in-memory test database environment (`sqlite+pysqlite:///:memory:`)
4. Runs full test suite (`pytest -v`)
