from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dsctl.command_contract import (
    COMMAND_CATALOG,
    CommandContractError,
    InputContract,
)
from dsctl.command_references import (
    LEGACY_COMMAND_PATTERN_FIELDS,
    project_command_references,
)
from dsctl.services.schema import get_schema_result

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from dsctl.support.yaml_io import JsonValue


_METAVARS = frozenset(
    {
        "ACTION",
        "CLUSTER",
        "DATASOURCE",
        "DIR",
        "ENVIRONMENT",
        "FILE",
        "GROUP",
        "KEY",
        "NAME",
        "PLUGIN",
        "PROJECT",
        "TASK",
        "TASK_GROUP",
        "TASK_INSTANCE",
        "TYPE",
        "VALUE",
        "WORKFLOW",
        "WORKFLOW_INSTANCE",
    }
)
_ANGLE_METAVAR = re.compile(r"<[^>]+>")
_REPO_ROOT = Path(__file__).resolve().parents[1]
_PUBLIC_COMMAND_FILES = (
    _REPO_ROOT / "README.md",
    *sorted((_REPO_ROOT / "docs").rglob("*.md")),
    *sorted((_REPO_ROOT / "skills").rglob("*.md")),
    *sorted((_REPO_ROOT / "src" / "dsctl").rglob("*.py")),
)


def test_input_contract_rejects_ambiguous_discovery_references() -> None:
    with pytest.raises(
        CommandContractError,
        match="discovery command and pattern are mutually exclusive",
    ):
        InputContract(
            name="project",
            kind="option",
            value_type="string",
            description="Project selector.",
            discovery_command="dsctl project list",
            discovery_command_pattern="dsctl workflow list --project PROJECT",
        )


def test_command_reference_projection_rejects_divergent_pattern_inputs() -> None:
    with pytest.raises(
        ValueError,
        match="target_command and target_command_pattern must match",
    ):
        project_command_references(
            {
                "target_command": "dsctl workflow get WORKFLOW",
                "target_command_pattern": "dsctl task get TASK",
            }
        )


def test_full_schema_exposes_canonical_shell_safe_command_patterns() -> None:
    data = get_schema_result(full=True).data
    assert isinstance(data, dict)

    canonical_patterns = 0
    for mapping in _iter_mappings(data):
        for field, value in mapping.items():
            if field.endswith(("_command", "_commands")):
                assert not _contains_metavar(value), (
                    f"{field} must be executable; placeholders belong in a pattern"
                )
            if not field.endswith(("_command_pattern", "_command_patterns")):
                continue
            for command in _command_values(value):
                tokens = shlex.split(command)
                assert tokens
                assert tokens[0] == "dsctl"
                assert _ANGLE_METAVAR.search(command) is None
                assert _contains_metavar(command)
                canonical_patterns += 1
        for legacy_field, pattern_field in LEGACY_COMMAND_PATTERN_FIELDS.items():
            if pattern_field in mapping:
                assert legacy_field not in mapping

    assert canonical_patterns > 50


def test_public_command_notation_never_uses_shell_redirection_placeholders() -> None:
    violations: list[str] = []
    for path in _PUBLIC_COMMAND_FILES:
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if _ANGLE_METAVAR.search(line) and (
                "dsctl" in line
                or "schema --" in line
                or "capabilities --" in line
                or "enum list" in line
            ):
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{line_number}")

    assert violations == []


def test_global_option_examples_are_concrete_copyable_commands() -> None:
    for option in COMMAND_CATALOG.global_options:
        tokens = shlex.split(option.example)
        assert tokens
        assert tokens[0] == "dsctl"
        assert _ANGLE_METAVAR.search(option.example) is None
        assert "..." not in tokens
        assert not _contains_metavar(option.example)


def _iter_mappings(value: JsonValue) -> Iterator[Mapping[str, JsonValue]]:
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _iter_mappings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_mappings(item)


def _contains_metavar(value: JsonValue | object) -> bool:
    return any(_command_contains_metavar(item) for item in _command_values(value))


def _command_values(value: object) -> Sequence[str]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    return ()


def _command_contains_metavar(command: str) -> bool:
    if _ANGLE_METAVAR.search(command):
        return True
    for token in shlex.split(command):
        normalized = token.strip("[]")
        if normalized in _METAVARS:
            return True
        if any(part in _METAVARS for part in normalized.split("=")):
            return True
    return False
