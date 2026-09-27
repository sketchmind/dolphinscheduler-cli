from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from dsctl.models.task_spec import TaskRunFlag
from dsctl.upstream.serialization import optional_text

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from dsctl.models.workflow_spec import WorkflowTaskSpec
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.serialization import TaskData


def task_flag_value(flag: TaskRunFlag | str | None) -> str:
    """Return one DS-native task run flag, defaulting to YES at the upstream seam."""
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


@dataclass(frozen=True, slots=True)
class _TaskNodeField:
    native_name: str
    encode: Callable[[WorkflowTaskSpec], int | str | None]


_TASK_NODE_FIELDS = {
    "description": _TaskNodeField("description", lambda task: task.description or ""),
    "flag": _TaskNodeField("flag", lambda task: task_flag_value(task.flag)),
    "priority": _TaskNodeField("taskPriority", lambda task: task.priority.value),
    "worker_group": _TaskNodeField(
        "workerGroup", lambda task: task.worker_group or "default"
    ),
    "environment_code": _TaskNodeField(
        "environmentCode",
        lambda task: task_environment_code_value(task.environment_code),
    ),
    "task_group_id": _TaskNodeField(
        "taskGroupId",
        lambda task: 0 if task.task_group_id is None else task.task_group_id,
    ),
    "task_group_priority": _TaskNodeField(
        "taskGroupPriority",
        lambda task: (
            0
            if task.task_group_id is None or task.task_group_priority is None
            else task.task_group_priority
        ),
    ),
    "retry.times": _TaskNodeField("failRetryTimes", lambda task: task.retry.times),
    "retry.interval": _TaskNodeField(
        "failRetryInterval", lambda task: task.retry.interval
    ),
    "timeout": _TaskNodeField("timeout", lambda task: task.timeout),
    "timeout_notify_strategy": _TaskNodeField(
        "timeoutNotifyStrategy",
        lambda task: task_timeout_settings(
            task.timeout, notify_strategy=task.timeout_notify_strategy
        )[1],
    ),
    "delay": _TaskNodeField("delayTime", lambda task: task.delay),
    "cpu_quota": _TaskNodeField(
        "cpuQuota", lambda task: task_resource_limit_value(task.cpu_quota)
    ),
    "memory_max": _TaskNodeField(
        "memoryMax", lambda task: task_resource_limit_value(task.memory_max)
    ),
}


def task_node_native_name(authoring_path: str) -> str:
    """Return the native TaskDefinition field owned by one authoring setting."""
    return _TASK_NODE_FIELDS[authoring_path].native_name


def encode_task_node_fields(
    task: WorkflowTaskSpec, fields: Sequence[str]
) -> dict[str, int | str | None]:
    """Encode settings in the caller's wire order with DS-native defaults."""
    return {
        binding.native_name: binding.encode(task)
        for path in fields
        for binding in (_TASK_NODE_FIELDS[path],)
    }


def decode_task_node_fields(task: TaskData) -> JsonObject:
    """Project direct runtime settings into their ordered authoring structure."""
    native = cast("Mapping[str, JsonValue]", task)
    fields: JsonObject = {}
    for path in (
        "worker_group",
        "priority",
        "retry.times",
        "retry.interval",
        "timeout",
        "delay",
    ):
        value = native[task_node_native_name(path)]
        if path.startswith("retry."):
            retry = cast("JsonObject", fields.setdefault("retry", {}))
            retry[path.removeprefix("retry.")] = value
        else:
            fields[path] = value
    return fields


def optional_task_node_fields(task: TaskData) -> JsonObject:
    """Omit default native settings without broadening preservation ownership."""
    native = cast("Mapping[str, JsonValue]", task)
    fields: JsonObject = {}
    flag = optional_text(cast("str | None", native.get(task_node_native_name("flag"))))
    if flag is not None and flag != "YES":
        fields["flag"] = flag
    environment = native.get(task_node_native_name("environment_code"))
    if isinstance(environment, int) and environment > 0:
        fields["environment_code"] = environment
    strategy = optional_text(
        cast("str | None", native.get(task_node_native_name("timeout_notify_strategy")))
    )
    timeout = native[task_node_native_name("timeout")]
    if (
        isinstance(timeout, int)
        and timeout > 0
        and strategy is not None
        and strategy != "WARN"
    ):
        fields["timeout_notify_strategy"] = strategy
    for path in ("cpu_quota", "memory_max"):
        value = native.get(task_node_native_name(path))
        if isinstance(value, int) and value != -1:
            fields[path] = value
    group = native.get(task_node_native_name("task_group_id"))
    if isinstance(group, int) and group > 0:
        fields["task_group_id"] = group
        priority = native.get(task_node_native_name("task_group_priority"))
        if isinstance(priority, int):
            fields["task_group_priority"] = priority
    return fields
