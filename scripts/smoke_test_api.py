"""
smoke_test_api.py - end-to-end smoke test against a RUNNING DataTrust API.

The unit suite in tests/unit/test_api.py exercises the routes against an
in-memory SQLite database. This script does the thing that suite cannot: it
talks HTTP to a real server backed by the real configured database, so it
catches startup failures, wiring mistakes and schema drift.

It is deliberately stdlib-only (urllib), adds no dependency, and takes a
base URL, so the same script verifies a local server now, the Docker stack
later, and a deployed instance after that.

Usage:
    python scripts/smoke_test_api.py
    python scripts/smoke_test_api.py --base-url http://127.0.0.1:8000

Exit code 0 if every check passed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
HEALTH_TIERS = {"Healthy", "Warning", "Poor", "Critical"}
PIPELINE_STATUSES = {"SUCCESS", "FAILED", "COMPLETED", "RUNNING"}

# Substrings that must never appear in an error response body.
LEAK_MARKERS = ("traceback", "sqlalchemy", "sqlite3", "select ", "mysql", "password")

PASS, FAIL, INFO = "PASS", "FAIL", "INFO"
results: list[tuple[str, str, str]] = []


def record(status: str, name: str, detail: str = "") -> None:
    results.append((status, name, detail))
    print(f"  [{status}] {name}{(' - ' + detail) if detail else ''}")


def get(base_url: str, path: str, timeout: float = 15.0) -> tuple[int, Any, str, str]:
    """Return (status_code, parsed_or_text_body, content_type, transport_error)."""
    url = base_url.rstrip("/") + path
    request = urllib.request.Request(url, headers={"Accept": "*/*"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            content_type = response.headers.get_content_type()
            return response.status, _parse(raw, content_type), content_type, ""
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        content_type = exc.headers.get_content_type() if exc.headers else ""
        return exc.code, _parse(raw, content_type), content_type, ""
    except urllib.error.URLError as exc:
        return 0, None, "", str(exc.reason)
    except OSError as exc:                       # pragma: no cover - defensive
        return 0, None, "", str(exc)


def _parse(raw: bytes, content_type: str) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if "json" in content_type:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def check_no_leak(name: str, body: Any) -> None:
    """An error body must not expose driver, SQL or credential detail."""
    text = json.dumps(body) if isinstance(body, (dict, list)) else str(body)
    lowered = text.lower()
    leaked = [marker for marker in LEAK_MARKERS if marker in lowered]
    if leaked:
        record(FAIL, f"{name}: no internal detail leaked", f"found {leaked}")
    else:
        record(PASS, f"{name}: no internal detail leaked")


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test a running DataTrust API.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()
    base_url = args.base_url

    print(f"\nDataTrust API smoke test -> {base_url}\n" + "-" * 62)

    # ---- 1. Is the server up at all? -------------------------------------
    print("\n/health")
    status, body, _, transport_error = get(base_url, "/health")
    if transport_error:
        print(f"  [FAIL] server unreachable - {transport_error}")
        print("\nSERVER NOT RUNNING. Start it with:  python run_api.py")
        print("This is a server/startup problem, not a database problem.\n")
        return 1

    database_up = False
    if status == 200 and isinstance(body, dict):
        record(PASS, "GET /health -> 200")
        database_up = body.get("database") == "connected"
        record(
            PASS if database_up else INFO,
            f"database reported {body.get('database')!r}, api {body.get('status')!r}",
            "" if database_up else "database is DOWN - data endpoints will answer 503",
        )
    else:
        record(FAIL, f"GET /health -> {status}", "expected 200")

    # ---- 2. Interactive docs --------------------------------------------
    print("\n/docs and /openapi.json")
    status, _, content_type, _ = get(base_url, "/docs")
    record(PASS if status == 200 else FAIL, f"GET /docs -> {status}", content_type)

    status, body, _, _ = get(base_url, "/openapi.json")
    if status == 200 and isinstance(body, dict):
        documented = set(body.get("paths", {}))
        expected = {
            "/health",
            "/api/v1/health-score",
            "/api/v1/pipeline-runs",
            "/api/v1/quality-results/{run_id}",
            "/api/v1/anomalies",
            "/api/v1/summary",
        }
        missing = expected - documented
        record(
            PASS if not missing else FAIL,
            "openapi documents every endpoint",
            "" if not missing else f"missing {sorted(missing)}",
        )
    else:
        record(FAIL, f"GET /openapi.json -> {status}")

    # ---- 3. Summary ------------------------------------------------------
    print("\n/api/v1/summary")
    status, body, _, _ = get(base_url, "/api/v1/summary")
    if status == 503:
        record(
            INFO,
            "GET /api/v1/summary -> 503",
            "database unavailable (expected while MySQL is down)",
        )
        check_no_leak("summary", body)
    elif status == 200 and isinstance(body, dict):
        record(PASS, "GET /api/v1/summary -> 200")
        record(
            PASS if "pipeline_status" in body else FAIL,
            "summary separates health tier from pipeline status",
            f"health_status={body.get('health_status')!r} "
            f"pipeline_status={body.get('pipeline_status')!r}",
        )
        if body.get("health_status") in PIPELINE_STATUSES:
            record(FAIL, "health_status must be a tier, not an execution status")
        record(
            INFO,
            f"service status={body.get('status')!r} "
            f"anomaly_count={body.get('anomaly_count')}",
        )
    else:
        record(FAIL, f"GET /api/v1/summary -> {status}")

    # ---- 4. Health score -------------------------------------------------
    print("\n/api/v1/health-score")
    status, body, _, _ = get(base_url, "/api/v1/health-score")
    if status == 404:
        record(
            INFO,
            "GET /api/v1/health-score -> 404",
            "no pipeline runs stored yet - expected empty-data response",
        )
    elif status == 503:
        record(INFO, "GET /api/v1/health-score -> 503", "database unavailable")
        check_no_leak("health-score", body)
    elif status == 200 and isinstance(body, dict):
        record(PASS, "GET /api/v1/health-score -> 200")
        tier = body.get("health_status")
        if tier is None:
            record(
                INFO,
                "health_status is null",
                "run the ALTER TABLE migration, then re-run the pipeline to populate it",
            )
        else:
            record(
                PASS if tier in HEALTH_TIERS else FAIL,
                f"health_status={tier!r} is a health tier",
                "" if tier in HEALTH_TIERS else f"expected one of {sorted(HEALTH_TIERS)}",
            )
        record(
            PASS if "pipeline_status" in body else FAIL,
            f"pipeline_status={body.get('pipeline_status')!r} reported separately",
        )
        breakdown = body.get("category_scores")
        if breakdown is None:
            record(
                INFO,
                "category_scores is null",
                "same cause as a null tier - migrate, then re-run the pipeline",
            )
        else:
            record(
                PASS if isinstance(breakdown, dict) and breakdown else FAIL,
                "category_scores returned "
                f"{sorted(breakdown) if isinstance(breakdown, dict) else breakdown}",
            )
        record(
            INFO,
            f"health_score={body.get('health_score')} "
            f"over {body.get('total_checks')} checks",
        )
    else:
        record(FAIL, f"GET /api/v1/health-score -> {status}")

    # ---- 5. Pipeline runs + pagination -----------------------------------
    print("\n/api/v1/pipeline-runs")
    status, body, _, _ = get(base_url, "/api/v1/pipeline-runs?limit=2&offset=0")
    total_runs = 0
    if status == 503:
        record(INFO, "GET /api/v1/pipeline-runs -> 503", "database unavailable")
        check_no_leak("pipeline-runs", body)
    elif status == 200 and isinstance(body, dict):
        record(PASS, "GET /api/v1/pipeline-runs?limit=2 -> 200")
        required = {"total_runs", "returned", "limit", "offset", "runs"}
        missing = required - set(body)
        record(
            PASS if not missing else FAIL,
            "pagination envelope present",
            "" if not missing else f"missing {sorted(missing)}",
        )
        total_runs = body.get("total_runs", 0)
        returned = body.get("returned", 0)
        record(
            PASS if total_runs >= returned else FAIL,
            f"total_runs={total_runs} >= returned={returned}",
            "total must be the stored count, not the page size",
        )
        if total_runs >= 2:
            _, first, _, _ = get(base_url, "/api/v1/pipeline-runs?limit=1&offset=0")
            _, second, _, _ = get(base_url, "/api/v1/pipeline-runs?limit=1&offset=1")
            first_id = first["runs"][0]["run_id"] if first.get("runs") else None
            second_id = second["runs"][0]["run_id"] if second.get("runs") else None
            record(
                PASS if first_id != second_id and second_id is not None else FAIL,
                f"offset pages correctly (run_id {first_id} then {second_id})",
            )
        else:
            record(INFO, "offset paging not exercised", f"only {total_runs} run(s) stored")
    else:
        record(FAIL, f"GET /api/v1/pipeline-runs -> {status}")

    # ---- 6. Anomalies ----------------------------------------------------
    print("\n/api/v1/anomalies")
    status, body, _, _ = get(base_url, "/api/v1/anomalies?limit=1")
    if status == 503:
        record(INFO, "GET /api/v1/anomalies -> 503", "database unavailable")
    elif status == 200 and isinstance(body, dict):
        record(PASS, "GET /api/v1/anomalies?limit=1 -> 200")
        record(
            PASS if body.get("total_anomalies", 0) >= body.get("returned", 0) else FAIL,
            f"total_anomalies={body.get('total_anomalies')} >= returned={body.get('returned')}",
        )
    else:
        record(FAIL, f"GET /api/v1/anomalies -> {status}")

    # ---- 7. 404 and input validation -------------------------------------
    print("\nerror handling")
    status, body, _, _ = get(base_url, "/api/v1/quality-results/999999999")
    if status == 404:
        record(PASS, "GET /api/v1/quality-results/999999999 -> 404")
        check_no_leak("quality-results 404", body)
    elif status == 503:
        record(INFO, "quality-results -> 503", "database unavailable, 404 path not exercised")
    else:
        record(FAIL, f"GET /api/v1/quality-results/999999999 -> {status}", "expected 404")

    status, _, _, _ = get(base_url, "/api/v1/pipeline-runs?limit=0")
    record(
        PASS if status == 422 else FAIL,
        f"GET /api/v1/pipeline-runs?limit=0 -> {status}",
        "expected 422",
    )

    status, _, _, _ = get(base_url, "/api/v1/anomalies?limit=500")
    record(
        PASS if status == 422 else FAIL,
        f"GET /api/v1/anomalies?limit=500 -> {status}",
        "expected 422",
    )

    # ---- verdict ---------------------------------------------------------
    passed = sum(1 for status, _, _ in results if status == PASS)
    failed = sum(1 for status, _, _ in results if status == FAIL)
    notes = sum(1 for status, _, _ in results if status == INFO)

    print("\n" + "-" * 62)
    print(f"{passed} passed, {failed} failed, {notes} informational")
    if failed:
        print("\nFAILED CHECKS:")
        for status, name, detail in results:
            if status == FAIL:
                print(f"  - {name}{(' - ' + detail) if detail else ''}")
    if not database_up:
        print("\nNOTE: the database was reported down, so data endpoints were")
        print("only checked for correct 503 behaviour, not for content.")
    print()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
