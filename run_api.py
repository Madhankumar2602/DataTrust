"""
run_api.py — CLI entry point to launch the DataTrust FastAPI application with Uvicorn.
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path
import uvicorn

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))


def main() -> None:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    reload = os.getenv("APP_ENV", "development").lower() == "development"

    print("=" * 65)
    print("  DataTrust - Phase 8/9: REST API Service")
    print(f"  Starting server at http://{host}:{port}")
    print(f"  Interactive Docs:   http://{host}:{port}/docs")
    print(f"  Health Check:       http://{host}:{port}/health")
    print("=" * 65)

    uvicorn.run(
        "src.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
