from __future__ import annotations

QUARTZ_CRON_FIELD_COUNTS = frozenset({6, 7})


def quartz_cron_field_count(value: str) -> int:
    """Return the number of whitespace-delimited fields in one cron string."""
    return len(value.split())


def normalize_quartz_cron_text(value: str, *, label: str = "cron") -> str:
    """Normalize one Quartz cron string and reject non-Quartz field counts."""
    normalized = value.strip()
    if not normalized:
        message = f"{label} must not be empty"
        raise ValueError(message)
    field_count = quartz_cron_field_count(normalized)
    if field_count not in QUARTZ_CRON_FIELD_COUNTS:
        message = (
            f"{label} must be a Quartz cron expression with 6 or 7 fields "
            f"(seconds first); got {field_count}"
        )
        raise ValueError(message)
    return normalized


def quartz_cron_suggestion(field_count: int) -> str | None:
    """Return one targeted CLI suggestion for common cron shape mistakes."""
    if field_count != 5:
        return None
    return (
        "Use a DolphinScheduler Quartz cron such as `0 0 2 * * ?` instead of "
        "a five-field Unix cron such as `0 2 * * *`."
    )
