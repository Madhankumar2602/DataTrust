"""
config.py — Central configuration for DataTrust.

Why a central config?
─────────────────────
Instead of hard-coding paths, thresholds, or filenames throughout the
codebase, we put all settings here.  Any future change (e.g. moving the
data folder) requires editing ONE file, not hunting through every script.

How to use:
    from src.config import settings
    print(settings.RAW_DATA_PATH)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env file if it exists ────────────────────────────────────────────
# load_dotenv() reads a .env file and injects its contents into os.environ.
# This means we can keep secrets out of source code entirely.
load_dotenv()

# ── Project root ────────────────────────────────────────────────────────────
# Path(__file__) = src/config.py
# .parent        = src/
# .parent.parent = DataTrust/   <-- project root
PROJECT_ROOT = Path(__file__).parent.parent


class Settings:
    """
    All DataTrust configuration in one place.

    Environment variables take priority over defaults.
    This allows Docker / CI / production to override values
    without changing source code.
    """

    # ── Data paths ──────────────────────────────────────────────────────────
    RAW_DATA_PATH: Path = PROJECT_ROOT / os.getenv(
        "RAW_DATA_PATH", "data/raw/online_retail.csv"
    )
    PROCESSED_DATA_PATH: Path = PROJECT_ROOT / os.getenv(
        "PROCESSED_DATA_PATH", "data/processed"
    )
    REPORTS_PATH: Path = PROJECT_ROOT / os.getenv("REPORTS_PATH", "reports")
    LOGS_PATH: Path = PROJECT_ROOT / "logs"

    # ── Dataset metadata ────────────────────────────────────────────────────
    DATASET_NAME: str = "UCI Online Retail"
    EXPECTED_COLUMNS: list[str] = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]
    # Phase 2 schema contract. Keep it here so the quality engine has one
    # configurable source of truth rather than embedding the contract in checks.
    EXPECTED_DTYPES: dict[str, str] = {
        "InvoiceNo": "object",
        "StockCode": "object",
        "Description": "object",
        "Quantity": "int64",
        "InvoiceDate": "object",
        "UnitPrice": "float64",
        "CustomerID": "float64",
        "Country": "object",
    }
    UNEXPECTED_COLUMN_WARNING_COUNT: int = int(
        os.getenv("UNEXPECTED_COLUMN_WARNING_COUNT", 3)
    )

    # ── Quality score thresholds ────────────────────────────────────────────
    QUALITY_THRESHOLD_CRITICAL: float = float(
        os.getenv("QUALITY_THRESHOLD_CRITICAL", 50)
    )
    QUALITY_THRESHOLD_POOR: float = float(os.getenv("QUALITY_THRESHOLD_POOR", 75))
    QUALITY_THRESHOLD_WARNING: float = float(
        os.getenv("QUALITY_THRESHOLD_WARNING", 90)
    )

    # ── Phase 3 Data Health Score weights (must sum to 100) ─────────────────
    # These dimensions are all assessed by the Phase 2 validation engine.
    # Freshness and volume will be added only when those checks exist.
    SCORE_WEIGHTS: dict[str, float] = {
        "completeness": 20.0,
        "validity": 20.0,
        "uniqueness": 20.0,
        "schema": 20.0,
        "business_rules": 20.0,
    }

    # ── Logging ─────────────────────────────────────────────────────────────
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # ── Application ─────────────────────────────────────────────────────────
    APP_ENV: str = os.getenv("APP_ENV", "development")

    @property
    def DATABASE_URL(self) -> str:
      """Return the database URL supplied through the untracked environment."""
      database_url = os.getenv("DATABASE_URL")
      if not database_url:
         raise ValueError(
            "DATABASE_URL is not configured. Copy .env.example to .env "
            "and set your database connection URL."
        )
      return database_url

    def ensure_directories(self) -> None:
        """Create required directories if they don't already exist."""
        for path in [
            self.PROCESSED_DATA_PATH,
            self.REPORTS_PATH,
            self.LOGS_PATH,
        ]:
            Path(path).mkdir(parents=True, exist_ok=True)


# ── Single shared instance ───────────────────────────────────────────────────
# Every module imports THIS object — no need to instantiate Settings yourself.
settings = Settings()
