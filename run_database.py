"""Check DATABASE_URL in your untracked .env file and initialize PostgreSQL first."""

from __future__ import annotations

import io
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import create_database_engine, create_session_factory
from src.database.models import Base
from src.database.repository import QualityRepository
from src.ingestion.loader import load_csv
from src.quality.engine import QualityEngine
from src.scoring.scorer import HealthScorer


def main() -> None:
    """Validate the source data, calculate its score, and store the results."""
    print("=" * 65)
    print("  DataTrust - Phase 4: Historical Quality Storage")
    print("=" * 65)
    started_at = datetime.now(timezone.utc)

    try:
        engine = create_database_engine()
        Base.metadata.create_all(engine)
        session_factory = create_session_factory(engine)
        df = load_csv()
        quality_report = QualityEngine("UCI Online Retail").run(df)
        score_report = HealthScorer().calculate_score(quality_report)
        with session_factory() as session:
            pipeline_run = QualityRepository(session).save_run(
                quality_report=quality_report,
                score_report=score_report,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
            stored_results = len(pipeline_run.quality_results)
    except Exception as exc:
        print(f"[ERROR] Phase 4 persistence failed: {exc}")
        print("Check DATABASE_URL in your untracked .env file and make sure MySQL is running.")
        raise SystemExit(1) from exc

    print(f"[OK] Stored pipeline run #{pipeline_run.run_id}")
    print(f"[OK] Health score: {pipeline_run.health_score}/100 ({pipeline_run.status})")
    print(f"[OK] Quality results stored: {stored_results}")


if __name__ == "__main__":
    main()
