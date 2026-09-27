from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.cli_surface import TASK_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.models.common import is_yaml_object
from dsctl.models.task_spec import canonical_task_type, normalize_task_params
from dsctl.output import require_json_object, require_json_value
from dsctl.support.yaml_io import JsonObject, parse_json_text
from dsctl.upstream.serialization import (
    enum_value,
    optional_text,
    require_resource_int,
)
from dsctl.upstream.task_settings import (
    task_environment_code_value,
    task_group_values,
    task_resource_limit_value,
    task_timeout_settings,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
    from dsctl.upstream.protocol import (
        StringEnumValue,
        TaskPayloadRecord,
        WorkflowDagRecord,
    )

_TASK_UPDATE_SCHEMA_SUGGESTION = (
    "Run `dsctl schema --command task.update` and inspect "
    "set.supported_keys. For structural definition changes, use `dsctl "
    "workflow edit --patch|--file`; for finished instance repair, use "
    "`dsctl workflow-instance edit --patch|--file`."
)
_RAW_SCRIPT_UPDATE_TASK_TYPES = frozenset({"PYTHON", "REMOTESHELL", "SHELL"})


@dataclass(frozen=True)
class TaskUpdateCompilation:
    """Canonical mutation request plus the state it is expected to produce."""

    payload: JsonObject
    current_upstream_codes: tuple[int, ...]
    updated_upstream_codes: tuple[int, ...]
    current_projection: JsonObject
    expected_projection: JsonObject
    updated_fields: tuple[str, ...]
    no_change: bool


def compile_task_update(
    *,
    current_task: TaskPayloadRecord,
    dag: WorkflowDagRecord,
    update_spec: WorkflowPatchTaskSetSpec,
    requested_fields: Sequence[str],
    task_code: int,
) -> TaskUpdateCompilation:
    """Compile one task patch into its DS-native payload and dependency codes."""
    task_name_by_code = _task_name_by_code(dag)
    current_dependency_names = _task_dependency_names(
        dag,
        task_code=task_code,
        task_name_by_code=task_name_by_code,
    )
    current_upstream_codes = _upstream_codes_for_dependencies(
        dependency_names=current_dependency_names,
        task_code=task_code,
        task_name_by_code=task_name_by_code,
    )
    current_command = _task_command_value(current_task)
    updated_task_params = _updated_task_params_document(
        current_task=current_task,
        update_spec=update_spec,
    )
    updated_dependency_names = (
        current_dependency_names
        if "depends_on" not in update_spec.model_fields_set
        else list(update_spec.depends_on or [])
    )
    updated_upstream_codes = _upstream_codes_for_dependencies(
        dependency_names=updated_dependency_names,
        task_code=task_code,
        task_name_by_code=task_name_by_code,
    )
    updated_description = (
        update_spec.description
        if "description" in update_spec.model_fields_set
        else current_task.description
    )
    updated_worker_group = (
        update_spec.worker_group
        if "worker_group" in update_spec.model_fields_set
        else current_task.workerGroup
    )
    updated_flag = _updated_task_flag(
        current_task=current_task,
        update_spec=update_spec,
    )
    updated_environment_code = _updated_environment_code(
        current_task=current_task,
        update_spec=update_spec,
    )
    updated_task_group_id, updated_task_group_priority = _updated_task_group_fields(
        current_task=current_task,
        update_spec=update_spec,
    )
    updated_priority = (
        enum_value(update_spec.priority)
        if "priority" in update_spec.model_fields_set
        else enum_value(current_task.taskPriority)
    )
    updated_retry_times = (
        update_spec.retry.times
        if "retry" in update_spec.model_fields_set and update_spec.retry is not None
        else current_task.failRetryTimes
    )
    updated_retry_interval = (
        update_spec.retry.interval
        if "retry" in update_spec.model_fields_set and update_spec.retry is not None
        else current_task.failRetryInterval
    )
    updated_timeout = (
        update_spec.timeout
        if "timeout" in update_spec.model_fields_set and update_spec.timeout is not None
        else current_task.timeout
    )
    (
        updated_timeout_flag,
        updated_timeout_notify_strategy,
    ) = _updated_timeout_fields(current_task=current_task, update_spec=update_spec)
    updated_delay = (
        update_spec.delay
        if "delay" in update_spec.model_fields_set and update_spec.delay is not None
        else current_task.delayTime
    )
    updated_cpu_quota = _updated_resource_limit(
        current=current_task.cpuQuota,
        update_spec=update_spec,
        field_name="cpu_quota",
    )
    updated_memory_max = _updated_resource_limit(
        current=current_task.memoryMax,
        update_spec=update_spec,
        field_name="memory_max",
    )
    updated_command = (
        update_spec.command
        if "command" in update_spec.model_fields_set
        else current_command
    )
    current_view = {
        "description": _description_view(current_task.description),
        "command": current_command,
        "worker_group": _worker_group_view(current_task.workerGroup),
        "flag": optional_text(enum_value(current_task.flag)),
        "environment_code": _environment_code_view(current_task.environmentCode),
        "cpu_quota": _resource_limit_view(current_task.cpuQuota),
        "memory_max": _resource_limit_view(current_task.memoryMax),
        "priority": enum_value(current_task.taskPriority),
        "retry.times": current_task.failRetryTimes,
        "retry.interval": current_task.failRetryInterval,
        "task_group_id": _task_group_id_view(current_task.taskGroupId),
        "task_group_priority": _task_group_priority_view(
            current_task.taskGroupId,
            current_task.taskGroupPriority,
        ),
        "timeout": current_task.timeout,
        "timeout_notify_strategy": _timeout_notify_strategy_view(
            current_task.timeout,
            current_task.timeoutNotifyStrategy,
        ),
        "delay": current_task.delayTime,
        "depends_on": sorted(current_dependency_names),
    }
    updated_view = {
        "description": _description_view(updated_description),
        "command": updated_command,
        "worker_group": _worker_group_view(updated_worker_group),
        "flag": optional_text(updated_flag),
        "environment_code": _environment_code_view(updated_environment_code),
        "cpu_quota": _resource_limit_view(updated_cpu_quota),
        "memory_max": _resource_limit_view(updated_memory_max),
        "priority": updated_priority,
        "retry.times": updated_retry_times,
        "retry.interval": updated_retry_interval,
        "task_group_id": _task_group_id_view(updated_task_group_id),
        "task_group_priority": _task_group_priority_view(
            updated_task_group_id,
            updated_task_group_priority,
        ),
        "timeout": updated_timeout,
        "timeout_notify_strategy": _timeout_notify_strategy_view(
            updated_timeout,
            updated_timeout_notify_strategy,
        ),
        "delay": updated_delay,
        "depends_on": sorted(updated_dependency_names),
    }
    updated_fields = [
        field
        for field in requested_fields
        if current_view[field] != updated_view[field]
    ]
    no_change = not updated_fields
    task_type = optional_text(current_task.taskType)
    if task_type is None:
        message = "Task payload was missing taskType"
        raise ApiTransportError(message, details={"resource": TASK_RESOURCE})
    if (
        canonical_task_type(task_type) == "DYNAMIC"
        and "retry" in update_spec.model_fields_set
        and updated_retry_times != 0
    ):
        message = (
            "DYNAMIC retry.times must be 0 because ordinary task retry does not "
            "reset failed child workflows"
        )
        raise _task_update_user_input_error(message)
    payload = {
        "name": current_task.name,
        "description": _description_payload(updated_description),
        "taskType": task_type,
        "taskParams": _json_text(updated_task_params),
        "flag": updated_flag,
        "taskPriority": updated_priority,
        "workerGroup": _worker_group_payload(
            updated_worker_group,
            explicit="worker_group" in update_spec.model_fields_set,
        ),
        "environmentCode": updated_environment_code,
        "failRetryTimes": updated_retry_times,
        "failRetryInterval": updated_retry_interval,
        "timeoutFlag": updated_timeout_flag,
        "timeoutNotifyStrategy": updated_timeout_notify_strategy,
        "timeout": updated_timeout,
        "delayTime": updated_delay,
        "resourceIds": current_task.resourceIds,
        "taskGroupId": updated_task_group_id,
        "taskGroupPriority": updated_task_group_priority,
        "cpuQuota": updated_cpu_quota,
        "memoryMax": updated_memory_max,
        "taskExecuteType": enum_value(current_task.taskExecuteType),
    }
    return TaskUpdateCompilation(
        payload=require_json_object(
            {key: value for key, value in payload.items() if value is not None},
            label="task update payload",
        ),
        current_upstream_codes=tuple(current_upstream_codes),
        updated_upstream_codes=tuple(updated_upstream_codes),
        current_projection=require_json_object(
            {field: current_view[field] for field in requested_fields},
            label="current task update projection",
        ),
        expected_projection=require_json_object(
            {field: updated_view[field] for field in requested_fields},
            label="expected task update projection",
        ),
        updated_fields=tuple(updated_fields),
        no_change=no_change,
    )


def _task_name_by_code(dag: WorkflowDagRecord) -> dict[int, str]:
    names: dict[int, str] = {}
    for task in dag.taskDefinitionList or []:
        name = optional_text(task.name)
        if name is None:
            continue
        names[
            require_resource_int(
                task.code,
                resource=TASK_RESOURCE,
                field_name="task.code",
            )
        ] = name
    return names


def _task_dependency_names(
    dag: WorkflowDagRecord,
    *,
    task_code: int,
    task_name_by_code: Mapping[int, str],
) -> list[str]:
    dependency_names: list[str] = []
    for relation in dag.workflowTaskRelationList or []:
        post_task_code = require_resource_int(
            relation.postTaskCode,
            resource=WORKFLOW_RESOURCE,
            field_name="relation.postTaskCode",
        )
        if post_task_code != task_code:
            continue
        pre_task_code = require_resource_int(
            relation.preTaskCode,
            resource=WORKFLOW_RESOURCE,
            field_name="relation.preTaskCode",
        )
        if pre_task_code == 0:
            continue
        dependency_name = task_name_by_code.get(pre_task_code)
        if dependency_name is None:
            message = (
                f"Workflow DAG payload was missing task name for dependency code "
                f"{pre_task_code}"
            )
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        dependency_names.append(dependency_name)
    return dependency_names


def _upstream_codes_for_dependencies(
    *,
    dependency_names: Sequence[str],
    task_code: int,
    task_name_by_code: Mapping[int, str],
) -> list[int]:
    task_code_by_name = {name: code for code, name in task_name_by_code.items()}
    upstream_codes: list[int] = []
    for dependency_name in dependency_names:
        normalized_name = dependency_name.strip()
        if not normalized_name:
            message = "depends_on must not contain empty task names"
            raise _task_update_user_input_error(message)
        code = task_code_by_name.get(normalized_name)
        if code is None:
            message = f"Task dependency '{normalized_name}' was not found"
            raise _task_update_user_input_error(message)
        if code == task_code:
            message = "A task cannot depend on itself"
            raise _task_update_user_input_error(message)
        upstream_codes.append(code)
    return upstream_codes


def _updated_task_params_document(
    *,
    current_task: TaskPayloadRecord,
    update_spec: WorkflowPatchTaskSetSpec,
) -> JsonObject:
    payload = parse_json_text(current_task.taskParams)
    task_params = require_json_object(payload, label="task params")
    if "command" not in update_spec.model_fields_set:
        return task_params
    task_type = optional_text(current_task.taskType)
    if task_type is None:
        message = "Task payload was missing taskType"
        raise ApiTransportError(message, details={"resource": TASK_RESOURCE})
    normalized_task_type = canonical_task_type(task_type)
    if normalized_task_type not in _RAW_SCRIPT_UPDATE_TASK_TYPES:
        message = (
            "command updates are only supported for SHELL, PYTHON, and "
            "REMOTESHELL tasks"
        )
        raise _task_update_user_input_error(message)
    command = update_spec.command
    if command is None or not command.strip():
        message = "command must not be empty"
        raise _task_update_user_input_error(message)
    updated_task_params = dict(task_params)
    updated_task_params["rawScript"] = command
    if not is_yaml_object(updated_task_params):
        message = "Updated task params did not serialize to a YAML object"
        raise TypeError(message)
    normalized = normalize_task_params(
        normalized_task_type,
        updated_task_params,
    )
    return require_json_object(normalized, label="task params")


def _updated_task_flag(
    *,
    current_task: TaskPayloadRecord,
    update_spec: WorkflowPatchTaskSetSpec,
) -> str | None:
    if "flag" not in update_spec.model_fields_set:
        return optional_text(enum_value(current_task.flag))
    flag = optional_text(enum_value(update_spec.flag))
    if flag is None:
        message = "flag must be YES or NO"
        raise _task_update_user_input_error(message)
    return flag


def _updated_environment_code(
    *,
    current_task: TaskPayloadRecord,
    update_spec: WorkflowPatchTaskSetSpec,
) -> int:
    if "environment_code" not in update_spec.model_fields_set:
        return current_task.environmentCode
    return task_environment_code_value(update_spec.environment_code)


def _updated_task_group_fields(
    *,
    current_task: TaskPayloadRecord,
    update_spec: WorkflowPatchTaskSetSpec,
) -> tuple[int, int]:
    current_task_group_id = _task_group_id_view(current_task.taskGroupId)
    current_task_group_priority = _task_group_priority_view(
        current_task.taskGroupId,
        current_task.taskGroupPriority,
    )
    task_group_id_explicit = "task_group_id" in update_spec.model_fields_set
    task_group_priority_explicit = "task_group_priority" in update_spec.model_fields_set
    updated_task_group_id = (
        update_spec.task_group_id if task_group_id_explicit else current_task_group_id
    )
    updated_task_group_priority = (
        update_spec.task_group_priority
        if task_group_priority_explicit
        else current_task_group_priority
    )
    if updated_task_group_id is None:
        if not task_group_priority_explicit:
            return 0, 0
        if updated_task_group_priority is not None:
            message = "task_group_priority requires task_group_id"
            raise _task_update_user_input_error(message)
        return 0, 0
    if (
        task_group_id_explicit
        and not task_group_priority_explicit
        and updated_task_group_id != current_task_group_id
    ):
        updated_task_group_priority = 0
    _, task_group_priority = task_group_values(
        updated_task_group_id,
        updated_task_group_priority,
    )
    return (
        updated_task_group_id,
        0 if task_group_priority is None else task_group_priority,
    )


def _updated_timeout_fields(
    *,
    current_task: TaskPayloadRecord,
    update_spec: WorkflowPatchTaskSetSpec,
) -> tuple[str | None, str | None]:
    timeout_explicit = "timeout" in update_spec.model_fields_set
    notify_strategy_explicit = "timeout_notify_strategy" in update_spec.model_fields_set
    updated_timeout = (
        update_spec.timeout
        if timeout_explicit and update_spec.timeout is not None
        else current_task.timeout
    )
    if notify_strategy_explicit:
        if updated_timeout <= 0:
            message = "timeout_notify_strategy requires timeout > 0"
            raise _task_update_user_input_error(message)
        notify_strategy = optional_text(enum_value(update_spec.timeout_notify_strategy))
        return task_timeout_settings(updated_timeout, notify_strategy=notify_strategy)
    if updated_timeout <= 0:
        return task_timeout_settings(updated_timeout)
    if not timeout_explicit:
        return (
            optional_text(enum_value(current_task.timeoutFlag)),
            optional_text(enum_value(current_task.timeoutNotifyStrategy)),
        )
    current_notify_strategy = optional_text(
        enum_value(current_task.timeoutNotifyStrategy)
    )
    return task_timeout_settings(
        updated_timeout,
        notify_strategy=current_notify_strategy,
    )


def _updated_resource_limit(
    *,
    current: int | None,
    update_spec: WorkflowPatchTaskSetSpec,
    field_name: str,
) -> int | None:
    if field_name not in update_spec.model_fields_set:
        return current
    value = getattr(update_spec, field_name)
    if value is None:
        return task_resource_limit_value(None)
    if not isinstance(value, int):
        message = f"Task update field {field_name!r} was not an integer"
        raise TypeError(message)
    return task_resource_limit_value(value)


def _task_command_value(task: TaskPayloadRecord) -> str | None:
    task_type = optional_text(task.taskType)
    if (
        task_type is None
        or canonical_task_type(task_type) not in _RAW_SCRIPT_UPDATE_TASK_TYPES
    ):
        return None
    payload = parse_json_text(task.taskParams)
    task_params = require_json_object(payload, label="task params")
    raw_script = task_params.get("rawScript")
    if not isinstance(raw_script, str) or not raw_script.strip():
        return None
    return raw_script


def _description_payload(value: str | None) -> str:
    return "" if value is None else value


def _description_view(value: str | None) -> str | None:
    return optional_text(value)


def _worker_group_payload(
    value: str | None,
    *,
    explicit: bool,
) -> str | None:
    if not explicit:
        return value
    return value or "default"


def _worker_group_view(value: str | None) -> str | None:
    normalized = optional_text(value)
    if normalized == "default":
        return None
    return normalized


def _environment_code_view(value: int | None) -> int | None:
    if value is None or value <= 0:
        return None
    return value


def _task_group_id_view(value: int | None) -> int | None:
    if value is None or value <= 0:
        return None
    return value


def _task_group_priority_view(
    task_group_id: int | None,
    task_group_priority: int | None,
) -> int | None:
    if task_group_id is None or task_group_id <= 0:
        return None
    return task_group_priority


def _timeout_notify_strategy_view(
    timeout: int,
    strategy: StringEnumValue | str | None,
) -> str | None:
    if timeout <= 0:
        return None
    if strategy is None:
        return "WARN"
    value = getattr(strategy, "value", strategy)
    if isinstance(value, str):
        normalized = optional_text(value)
        return "WARN" if normalized is None else normalized
    normalized = optional_text(str(value))
    return "WARN" if normalized is None else normalized


def _resource_limit_view(value: int | None) -> int | None:
    if value is None or value == -1:
        return None
    return value


def _task_update_user_input_error(
    message: str,
    *,
    details: JsonObject | None = None,
) -> UserInputError:
    return UserInputError(
        message,
        details=details,
        suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION,
    )


def _json_text(value: JsonObject) -> str:
    return json.dumps(
        require_json_value(value, label="task JSON text"),
        ensure_ascii=False,
        separators=(",", ":"),
    )


__all__ = ["TaskUpdateCompilation", "compile_task_update"]
