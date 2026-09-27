"""Exact upstream runtime behavior for schedule-selected environments."""

from __future__ import annotations

from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES


def schedule_environment_inheritance_supported(ds_version: str) -> bool:
    """Whether a schedule environment reaches a task with native default -1."""
    return schedule_environment_limitation(ds_version) is None


def workflow_environment_inheritance_supported(ds_version: str) -> bool:
    """Whether a native-default task inherits a workflow environment."""
    try:
        return RUNTIME_INSTANCE_PROFILES[ds_version].task_inherits_workflow_environment
    except KeyError as exc:
        message = f"DS {ds_version} has no reviewed schedule environment profile"
        raise UnsupportedFeatureError(message) from exc


def schedule_environment_limitation(ds_version: str) -> str | None:
    """Return the first exact upstream boundary that blocks inheritance."""
    try:
        recipe = RUNTIME_INSTANCE_PROFILES[ds_version]
    except KeyError as exc:
        message = f"DS {ds_version} has no reviewed schedule environment profile"
        raise UnsupportedFeatureError(message) from exc
    if ds_version == "1.3.9":
        return "schedule_environment_unavailable"
    if not recipe.schedule_forwards_environment:
        return "scheduler_omits_environment"
    if not recipe.task_inherits_workflow_environment:
        return "task_does_not_inherit_environment"
    return None
