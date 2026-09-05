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

from src.contracts.loader import load_contract

# ── Load .env file if it exists ────────────────────────────────────────────
# load_dotenv() reads a .env file and injects its contents into os.environ.
# This means we can keep secrets out of source code entirely.
load_dotenv()

# ── Project root ────────────────────────────────────────────────────────────
# Path(__file__) = src/config.py
# .parent        = src/
# .parent.parent = DataTrust/   <-- project root
PROJECT_ROOT = Path(__file__).parent.parent

# ── Data contract ───────────────────────────────────────────────────────────
# The versioned contract in config/contracts/ is the single declaration of the
# expected schema. Both representations are derived from it below, so the names
# the ETL loader writes and the names the schema check validates cannot drift
# apart. Set CONTRACT_VERSION to pin a version; "latest" takes the highest.
CONTRACT_NAME: str = os.getenv("CONTRACT_NAME", "online_retail")
CONTRACT_VERSION: str = os.getenv("CONTRACT_VERSION", "latest")

_CONTRACT = load_contract(CONTRACT_NAME, CONTRACT_VERSION)


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

    # Which contract these expectations came from, for report provenance.
    CONTRACT_NAME: str = CONTRACT_NAME
    CONTRACT_VERSION: str = _CONTRACT.contract_version

    # ── Source representation, from the contract ────────────────────────────
    EXPECTED_COLUMNS: list[str] = _CONTRACT.source_columns()
    EXPECTED_DTYPES: dict[str, str] = _CONTRACT.source_dtypes()
    UNEXPECTED_COLUMN_WARNING_COUNT: int = int(
        os.getenv("UNEXPECTED_COLUMN_WARNING_COUNT", 3)
    )

    # ── Stored (retail_transactions) representation, from the same contract ──
    # Deriving both representations from one document is what keeps the names
    # the ETL loader writes and the names the schema check validates in step.
    # Columns whose pandas dtype varies between backends carry no stored_dtype
    # in the contract and are therefore absent here: their presence is still
    # validated, and the database enforces their real types.
    STORED_COLUMN_MAP: dict[str, str] = _CONTRACT.stored_column_map()
    STORED_EXPECTED_COLUMNS: list[str] = _CONTRACT.stored_columns()
    STORED_EXPECTED_DTYPES: dict[str, str] = _CONTRACT.stored_dtypes()

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
