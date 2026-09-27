"""Service policy for schedule environments that old masters do not inherit."""

from __future__ import annotations

from typing import TypedDict

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.schedule_environment import (
    schedule_environment_inheritance_supported,
    schedule_environment_limitation,
    workflow_environment_inheritance_supported,
)


class ScheduleEnvironmentWarning(TypedDict):
    """One output warning about a stored, ineffective schedule environment."""

    code: str
    message: str
    suggestion: str
    ds_version: str
    reason: str | None


def require_schedule_environment_inheritance(
    ds_version: str,
    environment_code: int | None,
) -> None:
    """Reject a selected positive schedule environment before a write."""
    if (
        environment_code is None
        or environment_code <= 0
        or schedule_environment_inheritance_supported(ds_version)
    ):
        return
    message = (
        f"DolphinScheduler {ds_version} stores schedule environmentCode "
        "but does not apply it to tasks with no explicit environment_code."
    )
    raise UnsupportedFeatureError(
        message,
        suggestion=(
            "Set environment_code on each task that needs this environment "
            "(including manual runs), use `--environment-code 0` to clear or "
            "bypass a project preference, or use DolphinScheduler 3.2.2+."
        ),
        details={
            "ds_version": ds_version,
            "environment_code": environment_code,
            "reason": schedule_environment_limitation(ds_version),
        },
    )


def schedule_environment_warning(
    ds_version: str,
    environment_code: int | None,
) -> tuple[list[str], list[ScheduleEnvironmentWarning]]:
    """Report one legacy inheritance warning for a result, if applicable."""
    if (
        environment_code is None
        or environment_code <= 0
        or schedule_environment_inheritance_supported(ds_version)
    ):
        return [], []
    message = (
        f"DolphinScheduler {ds_version} stores positive schedule environments "
        "but does not apply them to tasks without an explicit environment_code."
    )
    return [message], [
        {
            "code": "schedule_environment_not_inherited",
            "message": message,
            "ds_version": ds_version,
            "reason": schedule_environment_limitation(ds_version),
            "suggestion": (
                "Set environment_code on each task that needs this environment "
                "(the task setting also applies to manual runs), clear the "
                "schedule with `--environment-code 0`, or use DolphinScheduler "
                "3.2.2+."
            ),
        }
    ]


def require_workflow_environment_inheritance(
    ds_version: str,
    environment_code: int | None,
) -> None:
    """Reject a selected run environment that old masters cannot pass to tasks."""
    if (
        environment_code is None
        or environment_code <= 0
        or workflow_environment_inheritance_supported(ds_version)
    ):
        return
    if (
        schedule_environment_limitation(ds_version)
        == "schedule_environment_unavailable"
    ):
        message = f"DolphinScheduler {ds_version} has no environment feature."
        raise UnsupportedFeatureError(
            message,
            suggestion=(
                "Remove `--environment-code`, or use a DolphinScheduler release "
                "with environment support."
            ),
            details={
                "ds_version": ds_version,
                "environment_code": environment_code,
                "reason": "schedule_environment_unavailable",
            },
        )
    message = (
        f"DolphinScheduler {ds_version} cannot apply a workflow run "
        "environmentCode to tasks with no explicit environment_code."
    )
    raise UnsupportedFeatureError(
        message,
        suggestion=(
            "Set environment_code on each task that needs this environment "
            "(the task setting also applies to scheduled and manual runs), "
            "use `--environment-code 0` to bypass a project preference, or "
            "use DolphinScheduler 3.2.2+."
        ),
        details={
            "ds_version": ds_version,
            "environment_code": environment_code,
            "reason": "task_does_not_inherit_environment",
        },
    )
