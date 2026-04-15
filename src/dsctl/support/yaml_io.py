from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

import yaml

from dsctl.support.json_types import JsonObject, JsonValue, is_json_value


def dump_yaml_document(data: JsonObject) -> str:
    """Render a JSON-safe object as a YAML document."""
    return yaml.safe_dump(
        data,
        sort_keys=False,
        allow_unicode=True,
    )


def compact_yaml_mapping(data: Mapping[str, JsonValue]) -> JsonObject:
    """Drop `None` and empty collection values from one mapping."""
    compact: dict[str, JsonValue] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, Mapping) and not value:
            continue
        if (
            isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
            and not value
        ):
            continue
        compact[key] = value
    return compact


def parse_json_text(value: JsonValue) -> JsonValue:
    """Parse one JSON string when needed, otherwise return the JSON value unchanged."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value
    if is_json_value(parsed):
        return parsed
    return value


__all__ = [
    "JsonObject",
    "JsonValue",
    "compact_yaml_mapping",
    "dump_yaml_document",
    "parse_json_text",
]
