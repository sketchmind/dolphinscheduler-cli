from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import TypeAlias, TypeGuard, cast

from dsctl.errors import UserInputError

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | Sequence["JsonValue"] | Mapping[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


def is_json_value(value: object) -> TypeGuard[JsonValue]:
    """Return whether a Python value can be serialized as JSON."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and is_json_value(item) for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(is_json_value(item) for item in value)
    return False


def require_json_object(value: object, *, label: str) -> JsonObject:
    """Validate a boundary value and return it as a JSON object."""
    if not isinstance(value, dict):
        message = f"{label} must be a JSON object"
        raise UserInputError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"{label} must use string keys"
        raise UserInputError(message)
    if not all(is_json_value(item) for item in value.values()):
        message = f"{label} must contain only JSON-compatible values"
        raise UserInputError(message)
    return cast("JsonObject", value)


def merge_json_object(
    base: Mapping[str, JsonValue],
    patch: Mapping[str, JsonValue],
) -> JsonObject:
    """Recursively merge two JSON objects without mutating the inputs."""
    merged = deepcopy(dict(base))
    for key, value in patch.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, Mapping):
            merged[key] = merge_json_object(
                require_json_object(current, label=f"json object merge base.{key}"),
                require_json_object(
                    dict(value),
                    label=f"json object merge patch.{key}",
                ),
            )
            continue
        merged[key] = deepcopy(value)
    return merged
