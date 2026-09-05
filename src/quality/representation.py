"""Which representation a DataFrame holds, and the column names to use for it.

The same dataset is validated twice: as source columns before the ETL load, and
as the stored retail_transactions columns once a later stage reads the table
back. The versioned contract declares one source-to-stored column mapping, and
this module is the single place that turns it into "what is this column called
here?" — so no individual check has to carry its own copy of the naming.

Detection lives here too, so SchemaCheck, CompletenessCheck and ValidityCheck all
answer the question the same way instead of each guessing separately.
"""

from __future__ import annotations

import pandas as pd

from src.config import settings

SOURCE = "source"
STORED = "stored"


def detect_representation(df: pd.DataFrame) -> str:
    """Return which representation a frame holds.

    Whichever contract's names the frame matches more of wins. Ties and
    unrecognised frames resolve to source, so genuinely broken data still fails
    against the source contract rather than selecting itself a kinder one.
    """
    present = set(df.columns)
    source_matches = len(present & set(settings.EXPECTED_COLUMNS))
    stored_matches = len(present & set(settings.STORED_EXPECTED_COLUMNS))
    return STORED if stored_matches > source_matches else SOURCE


def resolve_columns(
    representation: str,
    stored_column_map: dict[str, str] | None = None,
) -> dict[str, str]:
    """Map each canonical (source) column name to its name in `representation`.

    The stored mapping comes from the contract, so there is exactly one
    authoritative source-to-stored naming in the codebase.
    """
    mapping = dict(
        stored_column_map if stored_column_map is not None else settings.STORED_COLUMN_MAP
    )
    if representation == STORED:
        return mapping
    return {canonical: canonical for canonical in mapping}


def columns_for(
    df: pd.DataFrame,
    representation: str | None = None,
    stored_column_map: dict[str, str] | None = None,
) -> dict[str, str]:
    """Resolve column names for an explicit representation, or detect the frame's."""
    return resolve_columns(
        representation or detect_representation(df), stored_column_map
    )
