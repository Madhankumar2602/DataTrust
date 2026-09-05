"""Typed model of a DataTrust data contract document.

The contract describes the EXPECTATION for a dataset. It performs no validation
of data itself: the quality engine's existing checks remain the validators and
read their expectations from here.

This module deliberately imports nothing from `src` — `src.logger` imports
`src.config`, so a contract that reached back into either would make
`src.config` circular once it derives its schema from the contract.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# How a null in a column is reported. Mirrors the statuses the quality engine
# already produces, so a declared policy maps onto existing behaviour.
NullPolicy = Literal["INFO", "WARNING", "FAIL"]

SEMANTIC_VERSION_PATTERN = r"^\d+\.\d+\.\d+$"


class ColumnContract(BaseModel):
    """One source column and how it appears once stored."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1, description="Canonical (source) column name")
    stored_name: str = Field(min_length=1, description="Name in retail_transactions")
    dtype: str = Field(min_length=1, description="Expected pandas dtype at source")
    # None means "presence is validated but the type is not asserted", used where
    # the pandas dtype legitimately differs between database backends.
    stored_dtype: str | None = None
    nullable: bool
    on_null: NullPolicy
    # Declared for completeness; nothing enforces these in contract release 1.0.0.
    constraints: dict[str, Any] | None = None
    note: str | None = None


class DerivedColumnContract(BaseModel):
    """A column the pipeline computes rather than receiving from the source."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1)
    stored_name: str = Field(min_length=1)
    produced_by: str = Field(min_length=1)
    stored_dtype: str | None = None
    note: str | None = None


class DataContract(BaseModel):
    """A single versioned contract document.

    `representation` is fixed to "source": the source names are canonical and the
    stored names hang off them as a projection. Allowing a second, independently
    authored stored contract is what let the two representations drift apart
    before, so the model refuses to express one.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    contract_name: str = Field(min_length=1)
    contract_version: str = Field(pattern=SEMANTIC_VERSION_PATTERN)
    representation: Literal["source"]
    description: str = ""
    # Names the parts of this document that are actually enforced today, so the
    # contract cannot overstate itself while later sections are still declarative.
    enforced_sections: list[str] = Field(default_factory=list)
    columns: list[ColumnContract] = Field(min_length=1)
    derived_columns: list[DerivedColumnContract] = Field(default_factory=list)
    stored_only_columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _names_must_be_unique(self) -> DataContract:
        """Reject a document that names the same column twice."""
        source_names = [column.name for column in self.columns]
        source_names += [column.name for column in self.derived_columns]
        _reject_duplicates(source_names, "column name")

        stored = [column.stored_name for column in self.columns]
        stored += [column.stored_name for column in self.derived_columns]
        stored += list(self.stored_only_columns)
        _reject_duplicates(stored, "stored column name")
        return self

    # ── Source representation ────────────────────────────────────────────────

    def source_columns(self) -> list[str]:
        """Expected source column names, in contract order."""
        return [column.name for column in self.columns]

    def source_dtypes(self) -> dict[str, str]:
        """Expected pandas dtype for each source column."""
        return {column.name: column.dtype for column in self.columns}

    # ── Stored representation ────────────────────────────────────────────────

    def stored_columns(self) -> list[str]:
        """Every column expected in the stored table, in contract order."""
        return (
            [column.stored_name for column in self.columns]
            + [column.stored_name for column in self.derived_columns]
            + list(self.stored_only_columns)
        )

    def stored_dtypes(self) -> dict[str, str]:
        """Stored dtypes, omitting columns whose type varies between backends."""
        typed: dict[str, str] = {}
        for column in list(self.columns) + list(self.derived_columns):
            if column.stored_dtype is not None:
                typed[column.stored_name] = column.stored_dtype
        return typed

    def stored_column_map(self) -> dict[str, str]:
        """Source name to stored name, including the derived columns.

        The ETL loader renames with this map and the schema check validates with
        it, so the written and the validated representations cannot diverge.
        """
        mapping = {column.name: column.stored_name for column in self.columns}
        mapping.update(
            {column.name: column.stored_name for column in self.derived_columns}
        )
        return mapping

    def derived_stored_columns(self) -> list[str]:
        return [column.stored_name for column in self.derived_columns]

    # ── Declared, not yet enforced ───────────────────────────────────────────

    def null_policies(self) -> dict[str, NullPolicy]:
        """Declared per-column null handling, keyed by source column name."""
        return {column.name: column.on_null for column in self.columns}

    def is_enforced(self, section: str) -> bool:
        return section in self.enforced_sections


def _reject_duplicates(values: list[str], label: str) -> None:
    seen: set[str] = set()
    duplicates = sorted({value for value in values if value in seen or seen.add(value)})
    if duplicates:
        raise ValueError(f"Duplicate {label}(s) in contract: {duplicates}")
