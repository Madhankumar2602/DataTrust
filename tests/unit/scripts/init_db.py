"""Create the DataTrust PostgreSQL tables. Safe to run repeatedly."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from src.database.connection import create_database_engine
from src.database.models import Base


def main() -> None:
    """Verify the configured database and create tables if they are absent."""
    try:
        engine = create_database_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        Base.metadata.create_all(engine)
    except Exception as exc:
        print(f"[ERROR] Database initialization failed: {exc}")
        print("Set DATABASE_URL in an untracked .env file, then run this command again.")
        raise SystemExit(1) from exc

    print("[OK] PostgreSQL connection verified.")
    print("[OK] DataTrust tables are ready: pipeline_runs, quality_results.")


if __name__ == "__main__":
    main()
