"""Small value validators shared by independent live-evidence schemas."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

_HMAC_SHA256 = re.compile(r"hmac-sha256:[0-9a-f]{64}\Z")


def required_mapping(value: object, *, label: str) -> dict[str, object]:
    """Return a string-keyed object or fail closed."""
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        message = f"{label} must be an object with string keys"
        raise TypeError(message)
    return value


def required_mapping_view(value: object, *, label: str) -> Mapping[str, object]:
    """Return the same validated object through a read-only mapping type."""
    return required_mapping(value, label=label)


def required_list(value: object, *, label: str) -> list[object]:
    """Return a JSON-style list or fail closed."""
    if not isinstance(value, list):
        message = f"{label} must be a list"
        raise TypeError(message)
    return value


def required_text(value: object, *, label: str) -> str:
    """Return stripped, non-empty text or fail closed."""
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value.strip()


def required_hmac_sha256(value: object, *, label: str) -> str:
    """Return one keyed SHA-256 identity or fail closed."""
    text = required_text(value, label=label)
    if _HMAC_SHA256.fullmatch(text) is None:
        message = f"{label} must be a keyed HMAC-SHA256 identity"
        raise ValueError(message)
    return text


def required_timestamp(value: object, *, label: str) -> datetime:
    """Return one timezone-aware ISO timestamp with its source offset."""
    text = required_text(value, label=label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        message = f"{label} must be an ISO-8601 timestamp"
        raise ValueError(message) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        message = f"{label} must include a timezone"
        raise ValueError(message)
    return parsed


def required_utc_timestamp(value: object, *, label: str) -> datetime:
    """Return one timezone-aware ISO timestamp normalized to UTC."""
    return required_timestamp(value, label=label).astimezone(UTC)


def required_int(value: object, *, label: str) -> int:
    """Return an integer while rejecting booleans."""
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"{label} must be an integer"
        raise TypeError(message)
    return value


def required_bool(value: object, *, label: str) -> bool:
    """Return a boolean or fail closed."""
    if not isinstance(value, bool):
        message = f"{label} must be a boolean"
        raise TypeError(message)
    return value


def require_constant_equal(value: object, expected: object, *, label: str) -> None:
    """Require exact value and runtime type equality."""
    if value != expected or type(value) is not type(expected):
        message = f"{label} must equal {expected!r}"
        raise ValueError(message)


__all__ = [
    "require_constant_equal",
    "required_bool",
    "required_hmac_sha256",
    "required_int",
    "required_list",
    "required_mapping",
    "required_mapping_view",
    "required_text",
    "required_timestamp",
    "required_utc_timestamp",
]
