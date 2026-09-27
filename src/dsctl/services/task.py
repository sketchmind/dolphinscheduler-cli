from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, cast

import yaml
from pydantic import ValidationError

from dsctl.cli_surface import TASK_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.output import (
    CommandResult,
    dry_run_result,
    require_json_object,
    require_json_value,
)
from dsctl.services.runtime import (
    TaskDefinitionServiceRuntime,
    run_with_task_definition_service_runtime,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    with_selection_source,
)
from dsctl.upstream.legacy_task_definitions import (
    LegacyTaskDefinitions,
    LegacyTaskSelector,
    LegacyWorkflowSelector,
)
from dsctl.upstream.serialization import optional_text
from dsctl.upstream.task_definitions import (
    TaskSelector,
    TaskUpdateIntent,
    WorkflowSelector,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.services.selection import SelectionData
    from dsctl.support.yaml_io import JsonObject, JsonValue
    from dsctl.upstream.legacy_task_definitions import PreparedLegacyTaskUpdate
    from dsctl.upstream.resolver import ResolvedTask, ResolvedWorkflow
    from dsctl.upstream.serialization import TaskData
    from dsctl.upstream.task_definitions import PreparedTaskUpdate

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
    "Run `dsctl schema --command task.update` and inspect "
    "set.supported_keys. For structural definition changes, use `dsctl "
    "workflow edit --patch|--file`; for finished instance repair, use "
    "`dsctl workflow-instance edit --patch|--file`."
)
_TASK_UPDATE_INVALID_STATE_SUGGESTION = (
    "Inspect the containing workflow definition state; if the workflow is "
    "online, bring it offline before retrying `task update`."
)
_TASK_PERMISSION_RESULT_CODES = frozenset({30001, 30002, 30003, 1400001})
_TASK_NOT_FOUND_RESULT_CODES = frozenset({50030, 50064})
_WORKFLOW_NOT_FOUND_RESULT_CODES = frozenset({50003})


class TaskUpdateWarningDetail(TypedDict):
    """Structured warning emitted when one task update changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool


class TaskUpdateChangeData(TypedDict):
    """One canonical task field change shown by update dry runs."""

    field: str
    before: JsonValue
    after: JsonValue


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
    return run_with_task_definition_service_runtime(
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
    return run_with_task_definition_service_runtime(
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
    """Update one existing task definition using inline KEY=VALUE mutations."""
    update_spec, requested_fields = _load_task_update_set_or_error(set_values)
    return run_with_task_definition_service_runtime(
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
    runtime: TaskDefinitionServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
    search: str | None,
) -> CommandResult:
    selected_project, selected_workflow = _task_scope_selections(
        runtime,
        project=project,
        workflow=workflow,
    )
    if isinstance(runtime.definitions, LegacyTaskDefinitions):
        try:
            legacy_listing = runtime.definitions.list(
                LegacyWorkflowSelector(
                    project=selected_project.value,
                    workflow=selected_workflow.value,
                )
            )
        except ApiResultError as exc:
            _raise_task_list_error(exc, workflow=selected_workflow.value)
            message = "legacy task list error mapping must raise"
            raise AssertionError(message) from exc
        legacy_data = [
            task_item.to_data()
            for task_item in legacy_listing.tasks
            if search is None or search.lower() in task_item.ref.name.lower()
        ]
        return CommandResult(
            data=require_json_value(legacy_data, label="task list data"),
            resolved={
                "project": with_selection_source(
                    cast("SelectionData", legacy_listing.scope.project.to_data()),
                    selected_project,
                ),
                "workflow": with_selection_source(
                    cast("SelectionData", legacy_listing.scope.workflow.to_data()),
                    selected_workflow,
                ),
                "search": search,
            },
        )
    try:
        listing = runtime.definitions.list(
            WorkflowSelector(
                project=selected_project.value,
                workflow=selected_workflow.value,
            )
        )
    except ApiResultError as exc:
        _raise_task_list_error(exc, workflow=selected_workflow.value)
        message = "task list error mapping must raise"
        raise AssertionError(message) from exc
    except (NotFoundError, PermissionDeniedError) as exc:
        _raise_task_list_domain_error(exc, workflow=selected_workflow.value)
        message = "task list domain error mapping must raise"
        raise AssertionError(message) from exc

    data = [
        task_item.to_data()
        for task_item in listing.tasks
        if search is None
        or (task_item.name is not None and search.lower() in task_item.name.lower())
    ]
    return CommandResult(
        data=require_json_value(data, label="task list data"),
        resolved={
            "project": _resolved_project_selection(
                listing.scope.project.name,
                listing.scope.project.code,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                listing.scope.workflow,
                selected_workflow,
            ),
            "search": search,
        },
    )


def _get_task_result(
    runtime: TaskDefinitionServiceRuntime,
    *,
    task: str,
    project: str | None,
    workflow: str | None,
) -> CommandResult:
    selected_project, selected_workflow = _task_scope_selections(
        runtime,
        project=project,
        workflow=workflow,
    )
    if isinstance(runtime.definitions, LegacyTaskDefinitions):
        try:
            legacy_task_read = runtime.definitions.get(
                LegacyTaskSelector(
                    project=selected_project.value,
                    workflow=selected_workflow.value,
                    task=task,
                )
            )
        except ApiResultError as exc:
            _raise_legacy_task_access_error(
                exc,
                task=task,
                workflow=selected_workflow.value,
                operation="read",
            )
            message = "legacy task read error mapping must raise"
            raise AssertionError(message) from exc
        return CommandResult(
            data=require_json_object(
                legacy_task_read.view.to_data(),
                label="task data",
            ),
            resolved={
                "project": with_selection_source(
                    cast(
                        "SelectionData",
                        legacy_task_read.scope.workflow.project.to_data(),
                    ),
                    selected_project,
                ),
                "workflow": with_selection_source(
                    cast(
                        "SelectionData",
                        legacy_task_read.scope.workflow.workflow.to_data(),
                    ),
                    selected_workflow,
                ),
                "task": require_json_object(
                    legacy_task_read.scope.task.to_data(),
                    label="resolved task",
                ),
            },
        )
    try:
        task_read = runtime.definitions.get(
            TaskSelector(
                project=selected_project.value,
                workflow=selected_workflow.value,
                task=task,
            )
        )
    except ApiResultError as exc:
        _raise_task_access_error(exc, task=task, operation="read")
        message = "task read error mapping must raise"
        raise AssertionError(message) from exc

    return CommandResult(
        data=require_json_object(task_read.view.to_data(), label="task data"),
        resolved={
            "project": _resolved_project_selection(
                task_read.scope.project.name,
                task_read.scope.project.code,
                selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                task_read.scope.workflow,
                selected_workflow,
            ),
            "task": require_json_object(
                task_read.scope.task.to_data(),
                label="resolved task",
            ),
        },
    )


def _update_task_result(
    runtime: TaskDefinitionServiceRuntime,
    *,
    task: str,
    project: str | None,
    workflow: str | None,
    update_spec: WorkflowPatchTaskSetSpec,
    requested_fields: list[str],
    dry_run: bool,
) -> CommandResult:
    selected_project, selected_workflow = _task_scope_selections(
        runtime,
        project=project,
        workflow=workflow,
    )
    if isinstance(runtime.definitions, LegacyTaskDefinitions):
        try:
            legacy_prepared = runtime.definitions.prepare_update(
                LegacyTaskSelector(
                    project=selected_project.value,
                    workflow=selected_workflow.value,
                    task=task,
                ),
                patch=update_spec,
                requested_fields=tuple(requested_fields),
            )
        except ApiResultError as exc:
            _raise_legacy_task_access_error(
                exc,
                task=task,
                workflow=selected_workflow.value,
                operation="read",
            )
            message = "legacy task update preparation error mapping must raise"
            raise AssertionError(message) from exc
        workflow_scope = legacy_prepared.current.scope.workflow
        legacy_resolved_data = require_json_object(
            {
                "project": with_selection_source(
                    cast("SelectionData", workflow_scope.project.to_data()),
                    selected_project,
                ),
                "workflow": with_selection_source(
                    cast("SelectionData", workflow_scope.workflow.to_data()),
                    selected_workflow,
                ),
                "task": legacy_prepared.current.scope.task.to_data(),
            },
            label="task update resolved",
        )
        if dry_run:
            return _task_update_dry_run_result(
                legacy_prepared,
                resolved=legacy_resolved_data,
            )
        if legacy_prepared.no_change:
            return _task_update_no_change_result(
                legacy_prepared.current.view.to_data(),
                resolved=legacy_resolved_data,
            )
        try:
            legacy_outcome = runtime.definitions.apply(legacy_prepared)
        except ApiResultError as exc:
            _raise_legacy_task_update_error(
                exc,
                task=legacy_prepared.current.scope.task.name,
                workflow=selected_workflow.value,
            )
            message = "legacy task update error mapping must raise"
            raise AssertionError(message) from exc
        return CommandResult(
            data=require_json_object(
                legacy_outcome.value.view.to_data(),
                label="task data",
            ),
            resolved=legacy_resolved_data,
        )
    try:
        prepared = runtime.definitions.prepare_update(
            TaskUpdateIntent(
                selector=TaskSelector(
                    project=selected_project.value,
                    workflow=selected_workflow.value,
                    task=task,
                ),
                patch=update_spec,
                requested_fields=tuple(requested_fields),
            )
        )
    except ApiResultError as exc:
        _raise_task_access_error(exc, task=task, operation="read")
        message = "task update preparation error mapping must raise"
        raise AssertionError(message) from exc
    scope = prepared.current.scope
    resolved_data = {
        "project": _resolved_project_selection(
            scope.project.name,
            scope.project.code,
            selected_project,
        ),
        "workflow": _resolved_workflow_selection(
            scope.workflow,
            selected_workflow,
        ),
        "task": require_json_object(
            scope.task.to_data(),
            label="resolved task",
        ),
    }
    resolved_json = require_json_object(resolved_data, label="task update resolved")
    if dry_run:
        return _task_update_dry_run_result(
            prepared,
            resolved=resolved_json,
        )
    if prepared.no_change:
        return _task_update_no_change_result(
            prepared.current.view.to_data(),
            resolved=resolved_json,
        )
    try:
        outcome = runtime.definitions.apply(prepared)
    except ApiResultError as exc:
        _raise_task_update_error(exc, resolved_task=scope.task)
        message = "task update error mapping must raise"
        raise AssertionError(message) from exc

    return CommandResult(
        data=require_json_object(outcome.value.view.to_data(), label="task data"),
        resolved=resolved_json,
    )


def _task_scope_selections(
    runtime: TaskDefinitionServiceRuntime,
    *,
    project: str | None,
    workflow: str | None,
) -> tuple[SelectedValue, SelectedValue]:
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = require_workflow_selection(
        workflow,
    )
    return selected_project, selected_workflow


def _task_update_dry_run_result(
    prepared: PreparedTaskUpdate | PreparedLegacyTaskUpdate,
    *,
    resolved: JsonObject,
) -> CommandResult:
    request = prepared.request
    if request.content is not None:
        message = "Task update wire unexpectedly emitted a raw content body"
        raise RuntimeError(message)
    params = (
        None
        if request.query is None
        else require_json_object(dict(request.query), label="task update query")
    )
    form_data = (
        None
        if request.form is None
        else require_json_object(dict(request.form), label="task update form")
    )
    return dry_run_result(
        method=request.method,
        path=request.path,
        params=params,
        json_body=request.json,
        form_data=form_data,
        requests=[] if prepared.no_change else None,
        resolved=resolved,
        extra_data=require_json_object(
            {
                "changes": _task_update_changes(prepared),
                "no_change": prepared.no_change,
            },
            label="task update dry-run data",
        ),
    )


def _task_update_changes(
    prepared: PreparedTaskUpdate | PreparedLegacyTaskUpdate,
) -> list[TaskUpdateChangeData]:
    return [
        {
            "field": field_name,
            "before": require_json_value(
                prepared.initial_projection[field_name],
                label=f"task update before {field_name}",
            ),
            "after": require_json_value(
                prepared.expected_projection[field_name],
                label=f"task update after {field_name}",
            ),
        }
        for field_name in prepared.updated_fields
    ]


def _task_update_no_change_result(
    task_data: JsonObject | TaskData,
    *,
    resolved: JsonObject,
) -> CommandResult:
    no_change_warning = "task update: no persistent changes detected"
    return CommandResult(
        data=require_json_object(
            cast("JsonObject", task_data),
            label="task data",
        ),
        resolved=resolved,
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
    if key == "command":
        if not raw_value.strip():
            message = "command must not be empty"
            raise _task_update_user_input_error(message)
        return raw_value
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


def _raise_task_update_error(
    exc: ApiResultError,
    *,
    resolved_task: ResolvedTask,
) -> None:
    result_code = exc.details.get("result_code")
    if result_code in _TASK_PERMISSION_RESULT_CODES | _TASK_NOT_FOUND_RESULT_CODES:
        _raise_task_access_error(
            exc,
            task=resolved_task.name,
            operation="write",
        )
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


def _raise_legacy_task_update_error(
    exc: ApiResultError,
    *,
    task: str,
    workflow: str,
) -> None:
    result_code = exc.result_code
    if result_code in _TASK_PERMISSION_RESULT_CODES | {30001, 50003}:
        _raise_legacy_task_access_error(
            exc,
            task=task,
            workflow=workflow,
            operation="write",
        )
    if result_code == 50008:
        raise InvalidStateError(
            exc.message,
            details={**exc.details, "resource": TASK_RESOURCE, "task": task},
            suggestion=_TASK_UPDATE_INVALID_STATE_SUGGESTION,
        ) from exc
    if result_code in {50017, 50019, 50020}:
        raise _task_update_user_input_error(exc.message, details=exc.details) from exc
    raise exc


def _raise_legacy_task_access_error(
    exc: ApiResultError,
    *,
    task: str,
    workflow: str,
    operation: str,
) -> None:
    if exc.result_code == 50003:
        message = f"Workflow '{workflow}' was not found"
        raise NotFoundError(
            message,
            details={
                **exc.details,
                "resource": TASK_RESOURCE,
                "workflow": workflow,
                "task": task,
            },
            suggestion=(
                "Run `dsctl workflow list` in the selected project, then retry "
                "with an existing workflow name or id."
            ),
        ) from exc
    if exc.result_code == 30001:
        permission = "write" if operation == "write" else "read"
        message = (
            f"Task '{task}' requires {permission} permission for the selected project"
        )
        raise PermissionDeniedError(
            message,
            details={**exc.details, "resource": TASK_RESOURCE, "task": task},
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                f"project {permission} permission, then retry."
            ),
        ) from exc
    _raise_task_access_error(exc, task=task, operation=operation)


def _raise_task_access_error(
    exc: ApiResultError,
    *,
    task: str,
    operation: str,
) -> None:
    result_code = exc.result_code
    details = {
        **exc.details,
        "resource": TASK_RESOURCE,
        "task": task,
    }
    if result_code in _TASK_PERMISSION_RESULT_CODES:
        required_permission = "write" if result_code == 30003 else operation
        message = (
            f"Task '{task}' requires {required_permission} permission for the "
            "selected project"
        )
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                f"the required project {required_permission} permission, then retry."
            ),
        ) from exc
    if result_code in _TASK_NOT_FOUND_RESULT_CODES:
        message = f"Task '{task}' was not found"
        raise NotFoundError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl task list` in the selected project and workflow, then "
                "retry with an existing task name or code."
            ),
        ) from exc
    raise exc


def _raise_task_list_error(
    exc: ApiResultError,
    *,
    workflow: str,
) -> None:
    details = {
        **exc.details,
        "resource": TASK_RESOURCE,
        "workflow": workflow,
    }
    if exc.result_code in _TASK_PERMISSION_RESULT_CODES:
        message = "Task list requires read permission for the selected project"
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "project read permission, then retry."
            ),
        ) from exc
    if exc.result_code in _WORKFLOW_NOT_FOUND_RESULT_CODES:
        message = f"Workflow '{workflow}' was not found"
        raise NotFoundError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow list` in the selected project, then retry "
                "with an existing workflow name or code."
            ),
        ) from exc
    raise exc


def _raise_task_list_domain_error(
    exc: NotFoundError | PermissionDeniedError,
    *,
    workflow: str,
) -> None:
    """Keep task-list diagnostics stable across the deep definition resolver."""
    cause = exc.__cause__
    result_code = cause.result_code if isinstance(cause, ApiResultError) else None
    details = {
        **exc.details,
        "resource": TASK_RESOURCE,
        "workflow": workflow,
    }
    if result_code is not None:
        details["result_code"] = result_code
    if isinstance(exc, PermissionDeniedError):
        message = "Task list requires read permission for the selected project"
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "project read permission, then retry."
            ),
        ) from exc
    message = f"Workflow '{workflow}' was not found"
    raise NotFoundError(
        message,
        details=details,
        suggestion=(
            "Run `dsctl workflow list` in the selected project, then retry "
            "with an existing workflow name or code."
        ),
    ) from exc


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
