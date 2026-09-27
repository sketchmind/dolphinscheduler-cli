"""Read exact admission membership without importing the profile compiler."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from runtime_bundle_manifest import natural_version_key

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

DEFAULT_VERSION_PROFILE_LEDGER = Path(__file__).with_name(
    "version_profile_decisions.json"
)
_EXACT_VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?\Z")


def load_version_profile_ledger(
    path: Path = DEFAULT_VERSION_PROFILE_LEDGER,
) -> dict[str, Any]:
    """Load detached reviewed decisions; source inventory is a separate fact."""
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = "version profile ledger must contain a JSON object"
        raise TypeError(message)
    return copy.deepcopy(loaded)


def exact_versions(values: Iterable[str], *, label: str) -> tuple[str, ...]:
    """Require a nonempty, unique, naturally ordered exact-version domain."""
    versions = tuple(values)
    if not versions or any(
        not isinstance(version, str) or _EXACT_VERSION.fullmatch(version) is None
        for version in versions
    ):
        message = f"{label} must contain exact versions"
        raise ValueError(message)
    if len(set(versions)) != len(versions):
        message = f"{label} contains duplicate versions"
        raise ValueError(message)
    if versions != tuple(sorted(versions, key=natural_version_key)):
        message = f"{label} versions are out of canonical order"
        raise ValueError(message)
    return versions


def reviewed_profile_versions(
    ledger: Mapping[str, object] | None = None,
) -> tuple[str, ...]:
    """Return the ledger's own exact members, without granting source candidates."""
    document = load_version_profile_ledger() if ledger is None else ledger
    sources = document.get("sources")
    if not isinstance(sources, dict):
        message = "ledger.sources must be a mapping"
        raise TypeError(message)
    return exact_versions(sources, label="ledger.sources")


def select_exact_versions(
    available: Iterable[str],
    requested: Iterable[str] | None,
    *,
    label: str,
) -> tuple[str, ...]:
    """Select a validated subset while rejecting every unreviewed coordinate."""
    members = exact_versions(available, label=label)
    selected = (
        members if requested is None else exact_versions(requested, label="selected")
    )
    missing = sorted(set(selected) - set(members), key=natural_version_key)
    if missing:
        message = f"{label} missing selected versions: {', '.join(missing)}"
        raise ValueError(message)
    return selected
