"""Lossless table encoding for explicitly declared CLI object collections."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dsctl.errors import OutputContractError

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue


def render_compact_table(
    payload: JsonObject,
    *,
    row_path: str,
    columns: tuple[str, ...],
    action: str,
) -> str:
    """Encode a logical row collection without changing surrounding metadata."""
    path = row_path.split(".")
    value: JsonValue = payload
    for part in path:
        if not isinstance(value, dict) or part not in value:
            raise _table_error(action, row_path, "the declared row path is absent")
        value = value[part]
    if not isinstance(value, list):
        raise _table_error(action, row_path, "the declared row value is not a list")

    if columns and columns != ("*",):
        headers = list(dict.fromkeys(columns))
    elif value and isinstance(value[0], dict):
        headers = sorted(value[0])
    else:
        headers = []
    fields = set(headers)
    for row in value:
        if not isinstance(row, dict):
            raise _table_error(action, row_path, "a row is not an object")
        if row.keys() != fields:
            raise _table_error(action, row_path, "rows do not have the same fields")

    # Serialize each row once. Nested cells retain their native JSON structure;
    # only the declared row path gets table encoding and row-oriented newlines.
    lines = [
        _encode([row[column] for column in headers])
        for row in value
        if isinstance(row, dict)
    ]
    body = ",\n".join(lines)
    row_body = "[\n" + body + "\n]" if lines else "[]"
    table = '{"columns":' + _encode(headers) + ',"rows":' + row_body + "}"
    return _encode_at_path(payload, path, table)


def _encode(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _encode_at_path(value: JsonValue, path: list[str], replacement: str) -> str:
    """Replace one encoded value; never substitute text inside business strings."""
    if not path:
        return replacement
    if not isinstance(value, dict):
        # The complete path was checked before any encoding.
        message = "Validated compact row path changed during rendering"
        raise TypeError(message)
    return (
        "{"
        + ",".join(
            _encode(key)
            + ":"
            + (
                _encode_at_path(value[key], path[1:], replacement)
                if key == path[0]
                else _encode(value[key])
            )
            for key in sorted(value)
        )
        + "}"
    )


def _table_error(action: str, row_path: str, reason: str) -> OutputContractError:
    return OutputContractError(
        f"Cannot encode {action} as a table: {reason}.",
        details={
            "action": action,
            "row_path": row_path,
            "format": "json-compact",
            "reason": reason,
        },
        suggestion=(
            "Use --format json to inspect the original fields. "
            "A declared compact list must preserve its row field contract."
        ),
    )
