from __future__ import annotations

DEFAULT_DS_VERSION = "3.4.1"


def normalize_version(version: str) -> str:
    """Normalize common DS version spellings into the canonical dotted form."""
    normalized = version.strip().lower().removeprefix("v").removeprefix("ds_")
    return normalized.replace("_", ".")


__all__ = ["DEFAULT_DS_VERSION", "normalize_version"]
