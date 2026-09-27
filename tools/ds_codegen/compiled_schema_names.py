"""Readable physical schema names, separate from full contract identities."""

from __future__ import annotations

import re
from typing import Literal

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def schema_symbol_name(schema: str, digest: str) -> str:
    """Abbreviate only physical symbols; the compiler checks name collisions."""
    if _DIGEST.fullmatch(digest) is None:
        message = "compiled schema name requires a full SHA-256 digest"
        raise ValueError(message)
    fingerprint = digest.removeprefix("sha256:")
    semantic_name = schema.removesuffix(f"_{fingerprint}")
    return f"{semantic_name}_{fingerprint[:12]}"


def schema_module_name(
    domain: str,
    role: Literal["request", "response"],
    schema: str,
    digest: str,
) -> str:
    """Place an internal request or response schema in its domain's package."""
    return f"_schemas.{domain}.{role}_{schema_symbol_name(schema, digest)}"


__all__ = ["schema_module_name", "schema_symbol_name"]
