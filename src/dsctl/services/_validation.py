from __future__ import annotations

from time import strptime, struct_time

from dsctl.errors import UserInputError
from dsctl.support.quartz import (
    QUARTZ_CRON_FIELD_COUNTS,
    normalize_quartz_cron_text,
    quartz_cron_field_count,
    quartz_cron_suggestion,
)

DS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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


def optional_ds_datetime(value: str | None, *, label: str) -> str | None:
    """Normalize one optional DS datetime string."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parse_ds_datetime(normalized)
    except ValueError as error:
        message = f"{label} must match DS datetime format {DS_DATETIME_FORMAT!r}"
        raise UserInputError(
            message,
            suggestion=(
                f"Pass --{label} in '{DS_DATETIME_FORMAT}' format, for "
                "example '2026-04-11 10:00:00'."
            ),
        ) from error
    return normalized


def validate_ds_datetime_range(
    start: str | None,
    end: str | None,
    *,
    start_label: str = "start",
    end_label: str = "end",
) -> None:
    """Require an optional DS datetime range to be ordered when both ends exist."""
    if start is None or end is None:
        return
    start_value = parse_ds_datetime(start)
    end_value = parse_ds_datetime(end)
    if end_value < start_value:
        message = f"{end_label} must be greater than or equal to {start_label}"
        raise UserInputError(
            message,
            suggestion=(
                f"Pass an --{end_label} value that is later than or equal "
                f"to --{start_label}."
            ),
        )


def parse_ds_datetime(value: str) -> struct_time:
    """Parse one DS datetime string using the CLI-supported wire format."""
    return strptime(value, DS_DATETIME_FORMAT)


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
