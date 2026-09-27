from __future__ import annotations

import re
import shlex
from typing import TYPE_CHECKING

from dsctl.command_contract import semantic_value_name

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject, JsonValue


LEGACY_COMMAND_PATTERN_FIELDS = {
    "discovery_command": "discovery_command_pattern",
    "export_command": "export_command_pattern",
    "file_source_command": "file_source_command_pattern",
    "file_target_command": "file_target_command_pattern",
    "inspect_command": "inspect_command_pattern",
    "lint_command": "lint_command_pattern",
    "related_commands": "related_command_patterns",
    "target_command": "target_command_pattern",
    "target_commands": "target_command_patterns",
}

_KNOWN_METAVARS = frozenset(
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
        "NAMESPACE",
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
_ANGLE_METAVAR = re.compile(r"<(?P<name>[A-Za-z][A-Za-z0-9_-]*)>")
_METAVAR_ALIASES = {
    "workflow_instance_id": "WORKFLOW_INSTANCE",
    "task_instance_id": "TASK_INSTANCE",
}


def project_command_references(value: object) -> JsonValue:
    """Normalize unbound command references to one canonical pattern field."""
    if isinstance(value, list):
        return [project_command_references(item) for item in value]
    if isinstance(value, dict):
        return _project_mapping(value)
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    message = f"command reference data is not JSON-safe: {type(value).__name__}"
    raise TypeError(message)


def _project_mapping(value: dict[object, object]) -> JsonObject:
    if not all(isinstance(key, str) for key in value):
        message = "command reference mappings require string keys"
        raise TypeError(message)
    source = {str(key): item for key, item in value.items()}
    if not source:
        return {}

    projected: JsonObject = {
        key: project_command_references(item) for key, item in source.items()
    }
    for legacy_field, pattern_field in LEGACY_COMMAND_PATTERN_FIELDS.items():
        pattern_value = projected.get(pattern_field)
        if pattern_value is not None:
            normalized_pattern = _normalize_pattern_value(pattern_value)
            legacy_value = projected.get(legacy_field)
            if legacy_value is not None:
                normalized_legacy = _normalize_pattern_value(legacy_value)
                if normalized_legacy != normalized_pattern:
                    message = f"{legacy_field} and {pattern_field} must match"
                    raise ValueError(message)
            projected[pattern_field] = normalized_pattern
            projected.pop(legacy_field, None)
            continue

        legacy_value = projected.get(legacy_field)
        if not _contains_placeholder(legacy_value):
            continue
        normalized = _normalize_pattern_value(legacy_value)
        projected.pop(legacy_field, None)
        projected[pattern_field] = normalized

    for field, item in tuple(projected.items()):
        if field.endswith(("_command_pattern", "_command_patterns")):
            projected[field] = _normalize_pattern_value(item)
    return projected


def _normalize_pattern_value(value: object) -> JsonValue:
    if isinstance(value, str):
        normalized = _ANGLE_METAVAR.sub(_replace_angle_metavar, value)
        _validate_pattern(normalized)
        return normalized
    if isinstance(value, list):
        normalized_items: list[JsonValue] = []
        for item in value:
            if not isinstance(item, str):
                message = "command pattern lists may contain only strings"
                raise TypeError(message)
            normalized_items.append(_normalize_pattern_value(item))
        return normalized_items
    message = "command patterns must be strings or lists of strings"
    raise ValueError(message)


def _replace_angle_metavar(match: re.Match[str]) -> str:
    name = match.group("name").replace("-", "_")
    return _METAVAR_ALIASES.get(name, semantic_value_name(name))


def _contains_placeholder(value: JsonValue | None) -> bool:
    if isinstance(value, list):
        return any(_contains_placeholder(item) for item in value)
    if not isinstance(value, str):
        return False
    if _ANGLE_METAVAR.search(value):
        return True
    return any(_token_contains_metavar(token) for token in shlex.split(value))


def _token_contains_metavar(token: str) -> bool:
    normalized = token.strip("[]")
    if normalized in _KNOWN_METAVARS:
        return True
    return any(part in _KNOWN_METAVARS for part in normalized.split("="))


def _validate_pattern(command: str) -> None:
    tokens = shlex.split(command)
    if not tokens or tokens[0] != "dsctl":
        message = "command patterns must start with dsctl"
        raise ValueError(message)
    if _ANGLE_METAVAR.search(command):
        message = "command patterns must use uppercase metavars"
        raise ValueError(message)


__all__ = ["LEGACY_COMMAND_PATTERN_FIELDS", "project_command_references"]
