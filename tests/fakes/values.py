"""In-memory values collaborators with explicit test-owned state."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dsctl.support.json_types import is_json_value
from tests.fakes.common import (
    FakeEnumValue,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue


def _json_array(value: str, *, label: str) -> list[dict[str, object]]:
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        message = f"{label} must decode to a JSON array"
        raise TypeError(message)
    items: list[dict[str, object]] = []
    for item in parsed:
        if not isinstance(item, dict):
            message = f"{label} items must be JSON objects"
            raise TypeError(message)
        items.append(dict(item))
    return items


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_json_value(value: object) -> JsonValue | None:
    if value is None:
        return None
    if not is_json_value(value):
        message = "Expected a JSON-compatible fake payload value"
        raise TypeError(message)
    return value


def _optional_enum(value: object) -> FakeEnumValue | None:
    if value is None:
        return None
    return FakeEnumValue(str(value))


def _global_param_map(value: str | None) -> dict[str, str] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        return None
    global_param_map: dict[str, str] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        prop = item.get("prop")
        raw_value = item.get("value")
        if not isinstance(prop, str):
            continue
        global_param_map[prop] = "" if raw_value is None else str(raw_value)
    return global_param_map


def _require_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        message = "Expected an int-compatible fake payload value"
        raise TypeError(message)
    return int(value)
