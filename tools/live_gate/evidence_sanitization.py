"""Shared fail-closed text sanitization for tracked live evidence."""

from __future__ import annotations

import re

_SENSITIVE_TEXT_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\b(?:bearer|basic)\s+\S+",
        (
            r"(?<![A-Z0-9])(?:[A-Z0-9]+[_-])*(?:api[_-]?(?:key|token)|"
            r"authorization|credentials?|password|secret(?:[_-]access[_-]key)?|"
            r"tokens?|access[_-]?key)\s*[:=]\s*\S+"
        ),
        r"-----BEGIN(?: [A-Z0-9]+)* PRIVATE KEY-----",
    )
)


def reject_sensitive_text(value: object) -> None:
    """Reject recognizable credential material anywhere in a JSON-like value."""
    if isinstance(value, dict):
        for nested in value.values():
            reject_sensitive_text(nested)
    elif isinstance(value, list):
        for nested in value:
            reject_sensitive_text(nested)
    elif isinstance(value, str) and any(
        pattern.search(value) is not None for pattern in _SENSITIVE_TEXT_PATTERNS
    ):
        message = "evidence contains sensitive text"
        raise ValueError(message)


__all__ = ["reject_sensitive_text"]
