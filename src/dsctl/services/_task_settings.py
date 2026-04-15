from __future__ import annotations

from dsctl.models.task_spec import TaskRunFlag


def task_flag_value(flag: TaskRunFlag | str | None) -> str:
    """Return one DS-native task run flag, defaulting to YES."""
    if isinstance(flag, TaskRunFlag):
        return flag.value
    if isinstance(flag, str):
        return flag
    return TaskRunFlag.YES.value


def task_environment_code_value(environment_code: int | None) -> int:
    """Return the DS-native task environment code, using -1 for no environment."""
    return -1 if environment_code is None else environment_code


def task_resource_limit_value(limit: int | None) -> int:
    """Return one DS-native task resource limit, using -1 for no limit."""
    return -1 if limit is None else limit


def task_group_values(
    task_group_id: int | None,
    task_group_priority: int | None,
) -> tuple[int | None, int | None]:
    """Return DS-native task-group fields or omit them when no group is used."""
    if task_group_id is None:
        return None, None
    return task_group_id, (0 if task_group_priority is None else task_group_priority)


def task_timeout_settings(
    timeout: int,
    *,
    notify_strategy: str | None = None,
) -> tuple[str, str | None]:
    """Return DS-native timeout flag and notify strategy for one task timeout."""
    if timeout <= 0:
        return "CLOSE", None
    if notify_strategy is None:
        return "OPEN", "WARN"
    return "OPEN", notify_strategy
