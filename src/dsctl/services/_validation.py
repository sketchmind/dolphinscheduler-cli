from __future__ import annotations

from dsctl.errors import UserInputError
from dsctl.support.quartz import (
    QUARTZ_CRON_FIELD_COUNTS,
    normalize_quartz_cron_text,
    quartz_cron_field_count,
    quartz_cron_suggestion,
)


def require_non_empty_text(value: str, *, label: str) -> str:
    """Normalize one required CLI text field."""
    normalized = value.strip()
    if not normalized:
        message = f"{label} must not be empty"
        raise UserInputError(
            message,
            suggestion=f"Pass one non-empty {label} value.",
        )
    return normalized


def require_quartz_cron_text(value: str, *, label: str) -> str:
    """Normalize one Quartz cron string for CLI-facing schedule inputs."""
    try:
        return normalize_quartz_cron_text(value, label=label)
    except ValueError as exc:
        normalized = value.strip()
        details: dict[str, str | int | list[int]] = {
            "field": label,
            "format": "quartz",
            "expected_field_counts": sorted(QUARTZ_CRON_FIELD_COUNTS),
        }
        if normalized:
            field_count = quartz_cron_field_count(normalized)
            details["field_count"] = field_count
        else:
            field_count = 0
        raise UserInputError(
            str(exc),
            details=details,
            suggestion=quartz_cron_suggestion(field_count),
        ) from exc


def require_positive_int(value: int, *, label: str) -> int:
    """Require one positive CLI integer value."""
    if value < 1:
        message = f"{label} must be greater than or equal to 1"
        raise UserInputError(
            message,
            details={label: value},
            suggestion=f"Pass {label} as an integer greater than or equal to 1.",
        )
    return value


def require_non_negative_int(value: int, *, label: str) -> int:
    """Require one non-negative CLI integer value."""
    if value < 0:
        message = f"{label} must be greater than or equal to 0"
        raise UserInputError(
            message,
            details={label: value},
            suggestion=f"Pass {label} as an integer greater than or equal to 0.",
        )
    return value


def require_delete_force(*, force: bool, resource_label: str) -> None:
    """Require `--force` before executing one destructive delete command."""
    if force:
        return
    message = f"{resource_label} deletion requires --force"
    raise UserInputError(
        message,
        suggestion="Retry the same command with --force.",
    )
