"""Load versioned data contracts from config/contracts/.

Layout, one file per version, never overwritten:

    config/contracts/<contract_name>/v<major>.<minor>.<patch>.json

Imports nothing from `src`: `src.config` derives its schema from a contract, and
`src.logger` imports `src.config`, so reaching back into either would be circular.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pydantic import ValidationError

from src.contracts.models import DataContract

# <repo>/src/contracts/loader.py -> <repo>/config/contracts
CONTRACTS_ROOT = Path(__file__).resolve().parents[2] / "config" / "contracts"

LATEST = "latest"


class ContractError(Exception):
    """Raised when a contract cannot be found, parsed or validated."""


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _contract_dir(contract_name: str, root: Path | None = None) -> Path:
    return (root or CONTRACTS_ROOT) / contract_name


def available_versions(contract_name: str, root: Path | None = None) -> list[str]:
    """Return every stored version of a contract, oldest first."""
    directory = _contract_dir(contract_name, root)
    if not directory.is_dir():
        return []

    versions = []
    for path in directory.glob("v*.json"):
        version = path.stem.lstrip("v")
        try:
            _version_key(version)
        except ValueError:
            # Not a version file (e.g. a note or a draft); ignore rather than fail.
            continue
        versions.append(version)
    return sorted(versions, key=_version_key)


def resolve_version(
    contract_name: str, version: str = LATEST, root: Path | None = None
) -> str:
    """Resolve "latest" to a concrete version, or confirm an explicit one exists."""
    versions = available_versions(contract_name, root)
    if not versions:
        raise ContractError(
            f"No contract versions found for '{contract_name}' in "
            f"{_contract_dir(contract_name, root)}"
        )

    if version == LATEST:
        return versions[-1]

    if version not in versions:
        raise ContractError(
            f"Contract '{contract_name}' has no version '{version}'. "
            f"Available versions: {', '.join(versions)}"
        )
    return version


def contract_path(
    contract_name: str, version: str = LATEST, root: Path | None = None
) -> Path:
    """Return the file backing a contract version."""
    resolved = resolve_version(contract_name, version, root)
    return _contract_dir(contract_name, root) / f"v{resolved}.json"


def load_contract(
    contract_name: str, version: str = LATEST, root: Path | None = None
) -> DataContract:
    """Load and validate one contract version.

    Results are cached because the documents are immutable on disk; call
    `load_contract.cache_clear()` after writing a new one in a test.
    """
    return _load_contract_cached(contract_name, version, root or CONTRACTS_ROOT)


@lru_cache(maxsize=None)
def _load_contract_cached(
    contract_name: str, version: str, root: Path
) -> DataContract:
    path = contract_path(contract_name, version, root)

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ContractError(f"Contract {path} is not valid JSON: {exc}") from exc

    try:
        contract = DataContract.model_validate(document)
    except ValidationError as exc:
        raise ContractError(f"Contract {path} is invalid: {exc}") from exc

    # The filename is the addressable version, so a document disagreeing with it
    # would make a pinned version load different rules than the pin requested.
    expected_version = path.stem.lstrip("v")
    if contract.contract_version != expected_version:
        raise ContractError(
            f"Contract {path} declares version '{contract.contract_version}' "
            f"but is stored as version '{expected_version}'."
        )
    if contract.contract_name != contract_name:
        raise ContractError(
            f"Contract {path} declares name '{contract.contract_name}' "
            f"but is stored under '{contract_name}'."
        )

    return contract


def cache_clear() -> None:
    """Forget every cached contract; used by tests that write contracts."""
    _load_contract_cached.cache_clear()


load_contract.cache_clear = cache_clear  # type: ignore[attr-defined]
