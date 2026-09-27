from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

HiveCliScriptExecution = Literal[
    "hive-e-whole-command",
    "generated-file-sql-only",
]


@dataclass(frozen=True, slots=True)
class HiveCliAuthoringSurface:
    """HIVECLI SCRIPT execution and substitution behavior for one release."""

    available: bool
    script_execution: HiveCliScriptExecution | None


_HIVECLI_ABSENT = HiveCliAuthoringSurface(
    available=False,
    script_execution=None,
)
_HIVECLI_HIVE_E = HiveCliAuthoringSurface(
    available=True,
    script_execution="hive-e-whole-command",
)
_HIVECLI_GENERATED_FILE = HiveCliAuthoringSurface(
    available=True,
    script_execution="generated-file-sql-only",
)


def _hive_cli_surface(version: str) -> HiveCliAuthoringSurface:
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return _HIVECLI_ABSENT
    if version in {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }:
        return _HIVECLI_HIVE_E
    if version in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }:
        return _HIVECLI_GENERATED_FILE
    message = f"No exact HIVECLI authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
