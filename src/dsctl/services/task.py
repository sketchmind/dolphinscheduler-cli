from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypedDict, cast

import yaml
from pydantic import ValidationError

from dsctl.cli_surface import TASK_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
)
from dsctl.models.common import is_yaml_object
from dsctl.models.task_spec import canonical_task_type, normalize_task_params
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.models.workflow_spec import COMMAND_TASK_TYPES
from dsctl.output import (
    CommandResult,
    dry_run_result,
    require_json_object,
    require_json_value,
)
from dsctl.services._serialization import (
    TaskListItem,
    enum_value,
    optional_text,
    require_resource_int,
    serialize_task,
    serialize_task_ref,
)
from dsctl.services._task_settings import (
    task_environment_code_value,
    task_group_values,
    task_resource_limit_value,
    task_timeout_settings,
)
from dsctl.services.resolver import ResolvedProject, ResolvedTask, ResolvedWorkflow
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import task as resolve_task
from dsctl.services.resolver import workflow as resolve_workflow
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    with_selection_source,
)
from dsctl.support.yaml_io import JsonObject, parse_json_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.services.selection import SelectionData
    from dsctl.upstream.protocol import (
        StringEnumValue,
        TaskPayloadRecord,
        WorkflowDagRecord,
    )

_TASK_UPDATE_KEY_PATHS = {
    "command": ("command",),
    "cpu_quota": ("cpu_quota",),
    "delay": ("delay",),
    "depends_on": ("depends_on",),
    "description": ("description",),
    "environment_code": ("environment_code",),
    "flag": ("flag",),
    "memory_max": ("memory_max",),
    "priority": ("priority",),
    "retry.interval": ("retry", "interval"),
    "retry.times": ("retry", "times"),
    "task_group_id": ("task_group_id",),
    "task_group_priority": ("task_group_priority",),
    "timeout": ("timeout",),
    "timeout_notify_strategy": ("timeout_notify_strategy",),
    "worker_group": ("worker_group",),
}

_NULLABLE_TASK_UPDATE_KEYS = frozenset(
    {
        "cpu_quota",
        "description",
        "environment_code",
        "memory_max",
        "task_group_id",
        "task_group_priority",
        "worker_group",
    }
)
_TASK_UPDATE_SCHEMA_SUGGESTION = (
    "Run `dsctl schema` and inspect task.update option set.supported_keys. "
    "For structural task changes such as rename, type changes, or add/remove, "
    "use `dsctl workflow edit --patch ...`."
)
_TASK_UPDATE_INVALID_STATE_SUGGESTION = (
    "Inspect the containing workflow definition state; if the workflow is "
    "online, bring it offline before retrying `task update`."
)


class TaskUpdateWarningDetail(TypedDict):
    """Structured warning emitted when one task update changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool


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


def list_tasks_result(
    *,
    project: str | None = None,
    workflow: str | None = None,
    search: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List tasks inside one resolved workflow."""
    normalized_search = optional_text(search)
    return run_with_service_runtime(
        env_file,
        _list_tasks_result,
        project=project,
        workflow=workflow,
        search=normalized_search,
    )


def get_task_result(
    task: str,
    *,
    project: str | None = None,
    workflow: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Get one task definition inside one resolved workflow."""
    return run_with_service_runtime(
        env_file,
        _get_task_result,
        task=task,
        project=project,
        workflow=workflow,
    )


def update_task_result(
    task: str,
    *,
    project: str | None = None,
    workflow: str | None = None,
    set_values: Sequence[str],
    dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Update one task definition using inline `--set key=value` mutations."""
    update_spec, requested_fields = _load_task_update_set_or_error(set_values)
    return run_with_service_runtime(
        env_file,
        _update_task_result,
        task=task,
        project=project,
        workflow=workflow,
        update_spec=update_spec,
        requested_fields=requested_fields,
        dry_run=dry_run,
    )


def _list_tasks_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
    search: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    selected_workflow = require_workflow_selection(workflow, runtime=runtime)
    resolved_workflow = resolve_workflow(
        selected_workflow.value,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    tasks = runtime.upstream.tasks.list(
        project_code=resolved_project.code,
        workflow_code=resolved_workflow.code,
    )

    data: list[TaskListItem] = [
        serialize_task_ref(task_item)
        for task_item in tasks
        if search is None
        or (task_item.name is not None and search.lower() in task_item.name.lower())
    ]
    return CommandResult(
        data=require_json_value(data, label="task list data"),
        resolved={
            "project": _resolved_project_selection(
                resolved_project.name,
                resolved_project.code,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                resolved_workflow,
                selected_workflow,
            ),
            "search": search,
        },
    )


def _get_task_result(
    runtime: ServiceRuntime,
    *,
    task: str,
    project: str | None,
    workflow: str | None,
) -> CommandResult:
    (
        selected_project,
        resolved_project,
        selected_workflow,
        resolved_workflow,
        resolved_task,
    ) = _resolve_task_scope(
        runtime,
        task=task,
        project=project,
        workflow=workflow,
    )
    payload = runtime.upstream.tasks.get(code=resolved_task.code)

    return CommandResult(
        data=require_json_object(serialize_task(payload), label="task data"),
        resolved={
            "project": _resolved_project_selection(
                resolved_project.name,
                resolved_project.code,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                resolved_workflow,
                selected_workflow,
            ),
            "task": require_json_object(
                resolved_task.to_data(),
                label="resolved task",
            ),
        },
    )


def _update_task_result(
    runtime: ServiceRuntime,
    *,
    task: str,
    project: str | None,
    workflow: str | None,
    update_spec: WorkflowPatchTaskSetSpec,
    requested_fields: list[str],
    dry_run: bool,
) -> CommandResult:
    (
        selected_project,
        resolved_project,
        selected_workflow,
        resolved_workflow,
        resolved_task,
    ) = _resolve_task_scope(
        runtime,
        task=task,
        project=project,
        workflow=workflow,
    )
    dag = runtime.upstream.workflows.describe(
        project_code=resolved_project.code,
        code=resolved_workflow.code,
    )
    current_task = _task_payload_from_dag(dag, task_code=resolved_task.code)
    payload, upstream_codes, updated_fields, no_change = _compile_task_update(
        current_task=current_task,
        dag=dag,
        update_spec=update_spec,
        requested_fields=requested_fields,
        task_code=resolved_task.code,
    )
    task_definition_json = _json_text(payload)
    request_path = (
        f"/projects/{resolved_project.code}/task-definition/"
        f"{resolved_task.code}/with-upstream"
    )
    form_data: JsonObject = {"taskDefinitionJsonObj": task_definition_json}
    if upstream_codes:
        form_data["upstreamCodes"] = ",".join(str(code) for code in upstream_codes)
    resolved_data = {
        "project": _resolved_project_selection(
            resolved_project.name,
            resolved_project.code,
            selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            resolved_workflow,
            selected_workflow,
        ),
        "task": require_json_object(
            resolved_task.to_data(),
            label="resolved task",
        ),
    }
    resolved_json = require_json_object(resolved_data, label="task update resolved")
    if dry_run:
        return dry_run_result(
            method="PUT",
            path=request_path,
            form_data=form_data,
            resolved=resolved_json,
            extra_data=require_json_object(
                {
                    "updated_fields": updated_fields,
                    "no_change": no_change,
                },
                label="task update dry-run data",
            ),
        )
    if no_change:
        no_change_warning = "task update: no persistent changes detected"
        return CommandResult(
            data=require_json_object(serialize_task(current_task), label="task data"),
            resolved=resolved_json,
            warnings=[no_change_warning],
            warning_details=[
                require_json_object(
                    TaskUpdateWarningDetail(
                        code="task_update_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="task update warning detail",
                )
            ],
        )
    try:
        runtime.upstream.tasks.update(
            project_code=resolved_project.code,
            code=resolved_task.code,
            task_definition_json=task_definition_json,
            upstream_codes=upstream_codes,
        )
    except ApiResultError as exc:
        _raise_task_update_error(exc, resolved_task=resolved_task)
        message = "task update error mapping must raise"
        raise AssertionError(message) from exc

    refreshed = runtime.upstream.tasks.get(code=resolved_task.code)
    return CommandResult(
        data=require_json_object(serialize_task(refreshed), label="task data"),
        resolved=resolved_json,
    )


def _resolve_task_scope(
    runtime: ServiceRuntime,
    *,
    task: str,
    project: str | None,
    workflow: str | None,
) -> tuple[
    SelectedValue,
    ResolvedProject,
    SelectedValue,
    ResolvedWorkflow,
    ResolvedTask,
]:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    selected_workflow = require_workflow_selection(workflow, runtime=runtime)
    resolved_workflow = resolve_workflow(
        selected_workflow.value,
        adapter=runtime.upstream.workflows,
        project_code=resolved_project.code,
    )
    resolved_task = resolve_task(
        task,
        adapter=runtime.upstream.tasks,
        project_code=resolved_project.code,
        workflow_code=resolved_workflow.code,
    )
    return (
        selected_project,
        resolved_project,
        selected_workflow,
        resolved_workflow,
        resolved_task,
    )


def _load_task_update_set_or_error(
    set_values: Sequence[str],
) -> tuple[WorkflowPatchTaskSetSpec, list[str]]:
    try:
        return _parse_task_update_set(set_values)
    except ValidationError as exc:
        raise UserInputError(
            exc.json(indent=2),
            suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION,
        ) from exc


def _parse_task_update_set(
    set_values: Sequence[str],
) -> tuple[WorkflowPatchTaskSetSpec, list[str]]:
    if not set_values:
        message = "At least one --set KEY=VALUE is required"
        raise UserInputError(message, suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION)
    document: dict[str, object] = {}
    requested_fields: list[str] = []
    seen_fields: set[str] = set()
    for item in set_values:
        key, separator, raw_value = item.partition("=")
        normalized_key = key.strip()
        if not separator or not normalized_key:
            message = f"Invalid --set value {item!r}; expected KEY=VALUE"
            raise UserInputError(message, suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION)
        path = _TASK_UPDATE_KEY_PATHS.get(normalized_key)
        if path is None:
            message = f"Unsupported task update field {normalized_key!r}"
            raise UserInputError(message, suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION)
        if normalized_key in seen_fields:
            message = (
                f"Task update field {normalized_key!r} was specified more than once"
            )
            raise UserInputError(message, suggestion=_TASK_UPDATE_SCHEMA_SUGGESTION)
        seen_fields.add(normalized_key)
        requested_fields.append(normalized_key)
        value = _parse_task_update_value(normalized_key, raw_value)
        _assign_task_update_value(document, path, value)
    return WorkflowPatchTaskSetSpec.model_validate(document), requested_fields


def _parse_task_update_value(key: str, raw_value: str) -> object:
    if key == "depends_on":
        normalized = raw_value.strip()
        if not normalized:
            return []
        parsed = yaml.safe_load(normalized)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, str):
            return [item.strip() for item in parsed.split(",") if item.strip()]
        message = "depends_on must be a YAML list or a comma-separated string"
        raise _task_update_user_input_error(message)
    if key == "command" and not raw_value.strip():
        message = "command must not be empty"
        raise _task_update_user_input_error(message)
    parsed = yaml.safe_load(raw_value)
    if parsed is None and key not in _NULLABLE_TASK_UPDATE_KEYS:
        message = f"{key} does not support null"
        raise _task_update_user_input_error(message)
    return parsed


def _assign_task_update_value(
    document: dict[str, object],
    path: Sequence[str],
    value: object,
) -> None:
    current = document
    for segment in path[:-1]:
        nested = current.get(segment)
        if nested is None:
            nested_mapping: dict[str, object] = {}
            current[segment] = nested_mapping
            current = nested_mapping
            continue
        if not isinstance(nested, dict):
            message = (
                f"Task update path {'.'.join(path)!r} conflicts with another field"
            )
            raise _task_update_user_input_error(message)
        current = nested
    leaf = path[-1]
    if leaf in current:
        message = f"Task update field {'.'.join(path)!r} was specified more than once"
        raise _task_update_user_input_error(message)
    current[leaf] = value


def _compile_task_update(
    *,
    current_task: TaskPayloadRecord,
    dag: WorkflowDagRecord,
    update_spec: WorkflowPatchTaskSetSpec,
    requested_fields: Sequence[str],
    task_code: int,
) -> tuple[JsonObject, list[int], list[str], bool]:
    task_name_by_code = _task_name_by_code(dag)
    current_dependency_names = _task_dependency_names(
        dag,
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
        "depends_on": current_dependency_names,
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
        "depends_on": updated_dependency_names,
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
    return (
        require_json_object(
            {key: value for key, value in payload.items() if value is not None},
            label="task update payload",
        ),
        updated_upstream_codes,
        updated_fields,
        no_change,
    )


def _task_payload_from_dag(
    dag: WorkflowDagRecord,
    *,
    task_code: int,
) -> TaskPayloadRecord:
    for task in dag.taskDefinitionList or []:
        code = require_resource_int(
            task.code,
            resource=TASK_RESOURCE,
            field_name="task.code",
        )
        if code == task_code:
            return task
    message = f"Workflow DAG payload was missing task definition {task_code}"
    raise ApiTransportError(message, details={"resource": TASK_RESOURCE})


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
    if normalized_task_type not in COMMAND_TASK_TYPES:
        message = (
            "command updates are only supported for SHELL, PYTHON, and "
            "REMOTESHELL tasks"
        )
        raise _task_update_user_input_error(message)
    command = optional_text(update_spec.command)
    if command is None:
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
    if task_type is None or canonical_task_type(task_type) not in COMMAND_TASK_TYPES:
        return None
    payload = parse_json_text(task.taskParams)
    task_params = require_json_object(payload, label="task params")
    raw_script = task_params.get("rawScript")
    return optional_text(raw_script if isinstance(raw_script, str) else None)


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


def _raise_task_update_error(
    exc: ApiResultError,
    *,
    resolved_task: ResolvedTask,
) -> None:
    result_code = exc.details.get("result_code")
    if result_code in {50030, 50064}:
        message = f"Task '{resolved_task.name}' was not found"
        raise NotFoundError(message, details=exc.details) from exc
    if result_code == 50020:
        raise _task_update_user_input_error(exc.message, details=exc.details) from exc
    if result_code == 50045:
        raise ConflictError(exc.message, details=exc.details) from exc
    if result_code == 50056:
        raise InvalidStateError(
            exc.message,
            details=exc.details,
            suggestion=_TASK_UPDATE_INVALID_STATE_SUGGESTION,
        ) from exc
    if result_code in {50057, 50063}:
        message = "Task update did not change any persisted fields"
        raise _task_update_user_input_error(message, details=exc.details) from exc
    raise exc


def _json_text(value: object) -> str:
    return json.dumps(
        require_json_value(value, label="task JSON text"),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _resolved_project_selection(
    name: str,
    code: int,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source({"name": name, "code": code}, selection)


def _resolved_workflow_selection(
    workflow: ResolvedWorkflow,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)
