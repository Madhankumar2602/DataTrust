"""Unit tests for the versioned data contract (M5 Task 3B).

The contract declares the expectation; the existing quality checks remain the
validators. These tests cover loading and version resolution, and prove the
contract reproduces the schema behaviour that was previously hardcoded.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.config import settings
from src.contracts.loader import (
    ContractError,
    available_versions,
    load_contract,
    resolve_version,
)
from src.contracts.models import DataContract
from src.database.models import RetailTransaction
from src.quality.engine import QualityEngine
from src.scoring.scorer import HealthScorer

MINIMAL_CONTRACT = {
    "contract_name": "demo",
    "contract_version": "1.0.0",
    "representation": "source",
    "description": "Minimal contract used for loader tests.",
    "enforced_sections": ["columns", "dtypes"],
    "columns": [
        {
            "name": "Alpha",
            "stored_name": "alpha",
            "dtype": "object",
            "stored_dtype": "object",
            "nullable": False,
            "on_null": "FAIL",
        }
    ],
    "derived_columns": [],
    "stored_only_columns": [],
}


def write_contract(root, name: str, version: str, document: dict | None = None):
    """Write one contract version into a temporary contract root."""
    document = dict(document or MINIMAL_CONTRACT)
    document["contract_name"] = name
    document.setdefault("contract_version", version)
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"v{version}.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    load_contract.cache_clear()
    return path


@pytest.fixture
def source_df() -> pd.DataFrame:
    return pd.DataFrame({
        "InvoiceNo": ["536365", "536366"],
        "StockCode": ["85123A", "71053"],
        "Description": ["WHITE HANGING HEART", "WHITE METAL LANTERN"],
        "Quantity": [6, 6],
        "InvoiceDate": ["12/1/2010 8:26", "12/1/2010 8:28"],
        "UnitPrice": [2.55, 3.39],
        "CustomerID": [17850.0, 17850.0],
        "Country": ["United Kingdom", "United Kingdom"],
    })


@pytest.fixture
def transformed_df(source_df) -> pd.DataFrame:
    """Source columns plus the two columns the transformer derives."""
    df = source_df.copy()
    df["IsCancellation"] = [False, False]
    df["Revenue"] = [15.3, 20.34]
    return df


@pytest.fixture
def stored_df() -> pd.DataFrame:
    return pd.DataFrame({
        "transaction_id": [1, 2],
        "invoice_no": ["536365", "536366"],
        "stock_code": ["85123A", "71053"],
        "description": ["WHITE HANGING HEART", "WHITE METAL LANTERN"],
        "quantity": [6.0, 6.0],
        "invoice_date": ["2010-12-01T08:26:00", "2010-12-01T08:28:00"],
        "unit_price": [2.55, 3.39],
        "customer_id": [17850.0, 17850.0],
        "country": ["United Kingdom", "United Kingdom"],
        "is_cancellation": [0, 0],
        "revenue": [15.3, 20.34],
        "loaded_at": ["2026-01-01T00:00:00", "2026-01-01T00:00:00"],
    })


# ── Loading ──────────────────────────────────────────────────────────────────


def test_shipped_contract_loads():
    contract = load_contract("online_retail", "1.0.0")

    assert isinstance(contract, DataContract)
    assert contract.contract_name == "online_retail"
    assert contract.contract_version == "1.0.0"
    assert contract.representation == "source"
    assert contract.enforced_sections == ["columns", "dtypes"]


def test_contract_declares_verified_representation_details():
    """The dtype quirks confirmed against a real database must be preserved."""
    contract = load_contract("online_retail", "1.0.0")
    columns = {column.name: column for column in contract.columns}

    # Quantity is int64 at source but stored in a Float column.
    assert columns["Quantity"].dtype == "int64"
    assert columns["Quantity"].stored_dtype == "float64"
    # SQLite returns DATETIME as object, MySQL as datetime64[ns]; assert neither.
    assert columns["InvoiceDate"].stored_dtype is None
    # Nullability is a source-level statement, not the database's.
    assert columns["CustomerID"].nullable is True
    assert columns["CustomerID"].on_null == "INFO"
    assert columns["Description"].nullable is True
    assert columns["Description"].on_null == "WARNING"
    assert columns["Country"].nullable is False


@pytest.mark.parametrize(
    "mutation",
    [
        {"contract_version": "not-a-version"},
        {"representation": "stored"},
        {"columns": []},
        {"unexpected_top_level_key": True},
    ],
)
def test_malformed_contract_is_rejected(tmp_path, mutation):
    document = dict(MINIMAL_CONTRACT)
    document.update(mutation)
    directory = tmp_path / "demo"
    directory.mkdir(parents=True)
    (directory / "v1.0.0.json").write_text(json.dumps(document), encoding="utf-8")
    load_contract.cache_clear()

    with pytest.raises(ContractError):
        load_contract("demo", "1.0.0", root=tmp_path)


def test_invalid_json_is_rejected(tmp_path):
    directory = tmp_path / "demo"
    directory.mkdir(parents=True)
    (directory / "v1.0.0.json").write_text("{not json", encoding="utf-8")
    load_contract.cache_clear()

    with pytest.raises(ContractError):
        load_contract("demo", "1.0.0", root=tmp_path)


def test_duplicate_column_names_are_rejected(tmp_path):
    document = dict(MINIMAL_CONTRACT)
    document["columns"] = list(MINIMAL_CONTRACT["columns"]) * 2
    directory = tmp_path / "demo"
    directory.mkdir(parents=True)
    (directory / "v1.0.0.json").write_text(json.dumps(document), encoding="utf-8")
    load_contract.cache_clear()

    with pytest.raises(ContractError):
        load_contract("demo", "1.0.0", root=tmp_path)


def test_version_in_document_must_match_the_filename(tmp_path):
    """A pinned version must never load rules belonging to another version."""
    document = dict(MINIMAL_CONTRACT)
    document["contract_version"] = "9.9.9"
    directory = tmp_path / "demo"
    directory.mkdir(parents=True)
    (directory / "v1.0.0.json").write_text(json.dumps(document), encoding="utf-8")
    load_contract.cache_clear()

    with pytest.raises(ContractError, match="declares version"):
        load_contract("demo", "1.0.0", root=tmp_path)


# ── Version resolution ───────────────────────────────────────────────────────


def test_explicit_version_resolution(tmp_path):
    write_contract(tmp_path, "demo", "1.0.0")
    write_contract(tmp_path, "demo", "1.1.0")

    assert resolve_version("demo", "1.0.0", root=tmp_path) == "1.0.0"
    assert load_contract("demo", "1.0.0", root=tmp_path).contract_version == "1.0.0"


def test_latest_resolves_to_highest_semantic_version(tmp_path):
    """Ordering is semantic, not lexicographic: 1.10.0 beats 1.9.0."""
    for version in ("1.0.0", "1.9.0", "1.10.0", "2.0.0"):
        write_contract(tmp_path, "demo", version)

    assert available_versions("demo", root=tmp_path) == ["1.0.0", "1.9.0", "1.10.0", "2.0.0"]
    assert resolve_version("demo", "latest", root=tmp_path) == "2.0.0"


def test_unknown_version_fails_clearly(tmp_path):
    write_contract(tmp_path, "demo", "1.0.0")

    with pytest.raises(ContractError, match="no version '3.0.0'"):
        load_contract("demo", "3.0.0", root=tmp_path)


def test_unknown_contract_name_fails_clearly(tmp_path):
    with pytest.raises(ContractError, match="No contract versions found"):
        load_contract("does_not_exist", "latest", root=tmp_path)


def test_adding_a_version_never_overwrites_an_existing_one(tmp_path):
    first = write_contract(tmp_path, "demo", "1.0.0")
    write_contract(tmp_path, "demo", "1.1.0")

    assert first.exists()
    assert available_versions("demo", root=tmp_path) == ["1.0.0", "1.1.0"]
    assert load_contract("demo", "1.0.0", root=tmp_path).contract_version == "1.0.0"


# ── Contract drives the configured expectations ──────────────────────────────


def test_every_stored_name_matches_a_real_database_column():
    """The guard for the source/stored drift fixed in Task 3A."""
    actual_columns = {column.name for column in RetailTransaction.__table__.columns}
    contract = load_contract("online_retail", "1.0.0")

    for stored_name in contract.stored_columns():
        assert stored_name in actual_columns, (
            f"Contract expects stored column '{stored_name}', "
            f"which retail_transactions does not have"
        )


def test_contract_derived_source_columns_match_existing_expectations():
    """Behaviour preservation: the contract reproduces the previous constants."""
    assert settings.EXPECTED_COLUMNS == [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country",
    ]


def test_contract_derived_source_dtypes_match_existing_expectations():
    assert settings.EXPECTED_DTYPES == {
        "InvoiceNo": "object",
        "StockCode": "object",
        "Description": "object",
        "Quantity": "int64",
        "InvoiceDate": "object",
        "UnitPrice": "float64",
        "CustomerID": "float64",
        "Country": "object",
    }


def test_contract_derived_stored_expectations_match_existing_values():
    assert settings.STORED_EXPECTED_COLUMNS == [
        "invoice_no", "stock_code", "description", "quantity", "invoice_date",
        "unit_price", "customer_id", "country", "is_cancellation", "revenue",
        "transaction_id", "loaded_at",
    ]
    assert settings.STORED_EXPECTED_DTYPES == {
        "invoice_no": "object",
        "stock_code": "object",
        "description": "object",
        "quantity": "float64",
        "unit_price": "float64",
        "customer_id": "float64",
        "country": "object",
        "revenue": "float64",
    }


def test_stored_column_map_used_by_the_etl_loader_comes_from_the_contract():
    """One mapping writes the table and validates it, so they cannot diverge."""
    contract = load_contract("online_retail", "1.0.0")

    assert settings.STORED_COLUMN_MAP == contract.stored_column_map()
    assert settings.STORED_COLUMN_MAP["Quantity"] == "quantity"
    assert settings.STORED_COLUMN_MAP["IsCancellation"] == "is_cancellation"


# ── Engine integration ───────────────────────────────────────────────────────


def test_source_representation_validates_against_source_columns(source_df):
    report = QualityEngine("source", representation="source").run(source_df)

    presence = next(
        r for r in report["results"] if r["check_name"] == "column_presence"
    )
    assert presence["status"] == "PASS"
    assert report["contract"]["representation"] == "source"


def test_stored_representation_validates_against_stored_columns(stored_df):
    report = QualityEngine("stored", representation="stored").run(stored_df)

    presence = next(
        r for r in report["results"] if r["check_name"] == "column_presence"
    )
    assert presence["status"] == "PASS"
    assert report["contract"]["representation"] == "stored"


def test_auto_detects_each_representation(source_df, stored_df):
    assert QualityEngine("s").run(source_df)["contract"]["representation"] == "source"
    assert QualityEngine("t").run(stored_df)["contract"]["representation"] == "stored"


def test_transformed_frame_keeps_its_existing_behaviour(transformed_df):
    """The derived columns stay informational, exactly as before the contract."""
    report = QualityEngine("transformed").run(transformed_df)

    presence = next(
        r for r in report["results"] if r["check_name"] == "column_presence"
    )
    unexpected = next(
        r for r in report["results"] if r["check_name"] == "unexpected_columns"
    )
    assert presence["status"] == "PASS"
    assert unexpected["status"] == "INFO"
    assert sorted(unexpected["metadata"]["unexpected_columns"]) == [
        "IsCancellation", "Revenue",
    ]


def test_explicit_representation_overrides_detection(stored_df):
    """Pinning source against stored data must fail, not silently auto-correct."""
    report = QualityEngine("mismatch", representation="source").run(stored_df)

    presence = next(
        r for r in report["results"] if r["check_name"] == "column_presence"
    )
    assert presence["status"] == "FAIL"
    assert report["contract"]["representation"] == "source"


def test_unknown_representation_fails_immediately():
    with pytest.raises(ValueError, match="Unknown representation"):
        QualityEngine("bad", representation="nonsense")


def test_report_carries_contract_provenance(source_df):
    report = QualityEngine("provenance").run(source_df)

    assert report["contract"] == {
        "name": "online_retail",
        "version": "1.0.0",
        "representation": "source",
    }


def test_provenance_is_not_a_check_result(source_df):
    """Provenance must not join a category average and move the health score."""
    report = QualityEngine("provenance").run(source_df)

    names = [result["check_name"] for result in report["results"]]
    assert not any("contract" in name.lower() for name in names)


def test_clean_source_frame_still_scores_one_hundred(source_df):
    """The contract must not change the score of a known-good source frame."""
    report = QualityEngine("clean").run(source_df)
    score_report = HealthScorer().calculate_score(report)

    assert score_report["score"] == 100.0
    assert score_report["status"] == "Healthy"
    assert set(score_report["category_scores"]) == {
        "completeness", "validity", "uniqueness", "schema", "business_rules",
    }
