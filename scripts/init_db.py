"""
init_db.py - create the DataTrust tables if they are absent.

Run once against a fresh database (the docker-compose stack runs it
automatically as the `db-init` service). It is deliberately narrow:

  * it CREATES TABLES ONLY - it never loads data, so starting the stack can
    never re-import the 541,909-row retail dataset;
  * it is idempotent - SQLAlchemy's create_all skips tables that exist, so
    re-running it on a populated database changes nothing;
  * it owns no schema of its own - the tables come from src/database/models.py,
    the same definitions the application and the tests use.

Loading data stays an explicit, separate action: `python run_etl.py`.

Usage:
    python scripts/init_db.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.database.connection import create_database_engine
from src.database.models import Base

# A container can win the race against a MySQL server that has just reported
# healthy but is not yet accepting application connections.
MAX_ATTEMPTS = 12
RETRY_SECONDS = 5


def wait_for_database(engine, max_attempts: int = MAX_ATTEMPTS) -> None:
    """Block until the database accepts a connection, or give up loudly."""
    for attempt in range(1, max_attempts + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print(f"[OK] Database reachable (attempt {attempt}).")
            return
        except SQLAlchemyError as exc:
            if attempt == max_attempts:
                print(f"[ERROR] Database unreachable after {max_attempts} attempts: {exc}")
                raise
            print(f"[WAIT] Database not ready (attempt {attempt}/{max_attempts}); retrying...")
            time.sleep(RETRY_SECONDS)


def main() -> None:
    """Verify connectivity, then create any missing DataTrust tables."""
    print("=" * 65)
    print("  DataTrust - database initialisation (schema only, no data)")
    print("=" * 65)

    try:
        engine = create_database_engine()
        wait_for_database(engine)

        before = set(inspect(engine).get_table_names())
        Base.metadata.create_all(engine)
        after = set(inspect(engine).get_table_names())
    except SQLAlchemyError as exc:
        print(f"[ERROR] Initialisation failed: {exc}")
        print("Check DATABASE_URL and make sure the database server is running.")
        raise SystemExit(1) from exc

    created = sorted(after - before)
    existing = sorted(after & before)

    if created:
        print(f"[OK] Created tables: {', '.join(created)}")
    if existing:
        print(f"[OK] Already present: {', '.join(existing)}")
    print("[OK] Schema ready. Load data separately with: python run_etl.py")


if __name__ == "__main__":
    main()
