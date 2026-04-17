from __future__ import annotations

import time
from collections import deque
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import TASK_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    TaskNotDispatchedError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._runtime_support import (
    get_workflow_instance,
    require_workflow_instance_project_code,
)
from dsctl.services._serialization import (
    TaskInstanceData,
    TaskLogData,
    enum_value,
    optional_text,
    serialize_task_instance,
)
from dsctl.services._validation import (
    optional_ds_datetime,
    require_non_negative_int,
    require_positive_int,
    validate_ds_datetime_range,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import (
    ResolvedProject,
    ResolvedWorkflow,
)
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import workflow as resolve_workflow
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    with_selection_source,
)
from dsctl.upstream.runtime_enums import (
    TASK_EXECUTION_FINISHED_STATES,
    TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES,
    task_execute_type_value,
    task_execution_status_value,
    workflow_execution_status_is_final,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import TaskInstanceRecord, WorkflowInstanceRecord

ResolvedMetadataValue: TypeAlias = int | str | None
ResolvedMetadata: TypeAlias = dict[str, ResolvedMetadataValue]
TaskInstanceListResolvedValue: TypeAlias = int | str | bool | None | ResolvedMetadata
TaskInstanceListResolvedData: TypeAlias = dict[str, TaskInstanceListResolvedValue]


LOG_CHUNK_SIZE = 1000
MAX_LOG_CHUNKS = 200
DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS = 5
DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS = 600
TASK_INSTANCE_NOT_FOUND = 10008
TASK_INSTANCE_LOG_PATH_EMPTY = 10103
TASK_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE = 10021
TASK_INSTANCE_STATE_OPERATION_ERROR = 10166
TASK_SAVEPOINT_ERROR = 10196
TASK_STOP_ERROR = 10197
SUB_WORKFLOW_INSTANCE_NOT_EXIST = 50007
USER_NO_OPERATION_PERM = 30001


def _task_instance_get_command(
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> str:
    return (
        "dsctl task-instance get "
        f"{task_instance_id} --workflow-instance {workflow_instance_id}"
    )


def _workflow_instance_get_command(workflow_instance_id: int) -> str:
    return f"dsctl workflow-instance get {workflow_instance_id}"


class WorkflowInstanceSelectionData(TypedDict):
    """Resolved workflow-instance selector emitted in JSON envelopes."""

    id: int


class TaskInstanceSelectionData(TypedDict):
    """Resolved task-instance selector emitted in JSON envelopes."""

    id: int


class TaskInstanceActionData(TypedDict):
    """CLI task-instance action payload with a refreshed task snapshot."""

    requested: bool
    taskInstance: TaskInstanceData


class TaskInstanceSubWorkflowData(TypedDict):
    """DS-native sub-workflow relation payload emitted for one task instance."""

    subWorkflowInstanceId: int


TaskInstancePageData = PageData[TaskInstanceData]


def list_task_instances_result(
    *,
    workflow_instance: int | None = None,
    project: str | None = None,
    workflow: str | None = None,
    workflow_instance_name: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    search: str | None = None,
    task: str | None = None,
    task_code: int | None = None,
    executor: str | None = None,
    state: str | None = None,
    host: str | None = None,
    start: str | None = None,
    end: str | None = None,
    execute_type: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List task instances with project-scoped DS runtime filters."""
    normalized_workflow_instance = (
        None
        if workflow_instance is None
        else require_positive_int(
            workflow_instance,
            label="workflow_instance",
        )
    )
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    normalized_project = optional_text(project)
    normalized_workflow = optional_text(workflow)
    normalized_workflow_instance_name = optional_text(workflow_instance_name)
    normalized_search = optional_text(search)
    normalized_task = optional_text(task)
    normalized_task_code = (
        None
        if task_code is None
        else require_positive_int(task_code, label="task_code")
    )
    normalized_executor = optional_text(executor)
    normalized_state = _normalized_task_instance_state(state)
    normalized_host = optional_text(host)
    normalized_start = optional_ds_datetime(start, label="start")
    normalized_end = optional_ds_datetime(end, label="end")
    validate_ds_datetime_range(normalized_start, normalized_end)
    normalized_execute_type = _normalized_task_execute_type(execute_type)
    return run_with_service_runtime(
        env_file,
        _list_task_instances_result,
        workflow_instance_id=normalized_workflow_instance,
        project=normalized_project,
        workflow=normalized_workflow,
        workflow_instance_name=normalized_workflow_instance_name,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
        search=normalized_search,
        task=normalized_task,
        task_code=normalized_task_code,
        executor=normalized_executor,
        state=normalized_state,
        host=normalized_host,
        start=normalized_start,
        end=normalized_end,
        execute_type=normalized_execute_type,
    )


def get_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    env_file: str | None = None,
) -> CommandResult:
    """Get one task instance by id within one workflow instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    return run_with_service_runtime(
        env_file,
        _get_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
    )


def watch_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    interval_seconds: int = DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
    env_file: str | None = None,
) -> CommandResult:
    """Poll one task instance until it reaches a finished state."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    normalized_interval_seconds = require_positive_int(
        interval_seconds,
        label="interval_seconds",
    )
    normalized_timeout_seconds = require_non_negative_int(
        timeout_seconds,
        label="timeout_seconds",
    )
    return run_with_service_runtime(
        env_file,
        _watch_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        interval_seconds=normalized_interval_seconds,
        timeout_seconds=normalized_timeout_seconds,
    )


def get_sub_workflow_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    env_file: str | None = None,
) -> CommandResult:
    """Return the child workflow instance for one SUB_WORKFLOW task instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    return run_with_service_runtime(
        env_file,
        _get_sub_workflow_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
    )


def get_task_instance_log_result(
    task_instance: int,
    *,
    tail: int = 200,
    env_file: str | None = None,
) -> CommandResult:
    """Fetch the tail of one task-instance log using chunked log reads."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_tail = require_positive_int(tail, label="tail")
    return run_with_service_runtime(
        env_file,
        _get_task_instance_log_result,
        task_instance_id=normalized_task_instance,
        tail=normalized_tail,
    )


def force_success_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    env_file: str | None = None,
) -> CommandResult:
    """Force one failed task instance into FORCED_SUCCESS."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    return run_with_service_runtime(
        env_file,
        _force_success_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
    )


def savepoint_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    env_file: str | None = None,
) -> CommandResult:
    """Request one savepoint for a running task instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    return run_with_service_runtime(
        env_file,
        _savepoint_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
    )


def stop_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    env_file: str | None = None,
) -> CommandResult:
    """Request stop for one task instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    return run_with_service_runtime(
        env_file,
        _stop_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
    )


def _list_task_instances_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int | None,
    project: str | None,
    workflow: str | None,
    workflow_instance_name: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
    search: str | None,
    task: str | None,
    task_code: int | None,
    executor: str | None,
    state: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    execute_type: str | None,
) -> CommandResult:
    resolved_project, selected_project, resolved_workflow = (
        _resolve_task_instance_list_scope(
            runtime,
            workflow_instance_id=workflow_instance_id,
            project=project,
            workflow=workflow,
        )
    )
    workflow_definition_name = (
        None if resolved_workflow is None else resolved_workflow.name
    )
    data = require_json_object(
        requested_page_data(
            lambda current_page_no, current_page_size: (
                runtime.upstream.task_instances.list(
                    project_code=resolved_project.code,
                    workflow_instance_id=workflow_instance_id,
                    workflow_instance_name=workflow_instance_name,
                    workflow_definition_name=workflow_definition_name,
                    page_no=current_page_no,
                    page_size=current_page_size,
                    search=search,
                    task_name=task,
                    task_code=task_code,
                    executor=executor,
                    state=state,
                    host=host,
                    start_time=start,
                    end_time=end,
                    task_execute_type=execute_type,
                )
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            resource=TASK_INSTANCE_RESOURCE,
            serialize_item=serialize_task_instance,
            max_pages=MAX_AUTO_EXHAUST_PAGES,
        ),
        label="task-instance list data",
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            _task_instance_list_resolved(
                resolved_project=resolved_project,
                selected_project=selected_project,
                resolved_workflow=resolved_workflow,
                workflow_instance_id=workflow_instance_id,
                workflow_instance_name=workflow_instance_name,
                page_no=page_no,
                page_size=page_size,
                all_pages=all_pages,
                search=search,
                task=task,
                task_code=task_code,
                executor=executor,
                state=state,
                host=host,
                start=start,
                end=end,
                execute_type=execute_type,
            ),
            label="task-instance list resolved",
        ),
    )


def _resolve_task_instance_list_scope(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int | None,
    project: str | None,
    workflow: str | None,
) -> tuple[ResolvedProject, SelectedValue | None, ResolvedWorkflow | None]:
    if workflow_instance_id is not None:
        workflow_instance = get_workflow_instance(
            runtime,
            workflow_instance_id=workflow_instance_id,
        )
        project_code = require_workflow_instance_project_code(
            workflow_instance.projectCode,
        )
        resolved_project = resolve_project(
            str(project_code),
            adapter=runtime.upstream.projects,
        )
        selected_project: SelectedValue | None = None
        if project is not None:
            explicit_project = resolve_project(
                project,
                adapter=runtime.upstream.projects,
            )
            if explicit_project.code != project_code:
                message = (
                    "Selected project does not match the workflow instance project"
                )
                raise UserInputError(
                    message,
                    details={
                        "project": project,
                        "project_code": explicit_project.code,
                        "workflow_instance_id": workflow_instance_id,
                        "workflow_instance_project_code": project_code,
                    },
                    suggestion=(
                        "Use the project that owns the workflow instance, or omit "
                        "--project when --workflow-instance is already provided."
                    ),
                )
            resolved_project = explicit_project
            selected_project = SelectedValue(value=project, source="flag")
        resolved_workflow = (
            None
            if workflow is None
            else resolve_workflow(
                workflow,
                adapter=runtime.upstream.workflows,
                project_code=resolved_project.code,
            )
        )
        return resolved_project, selected_project, resolved_workflow

    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    resolved_workflow = (
        None
        if workflow is None
        else resolve_workflow(
            workflow,
            adapter=runtime.upstream.workflows,
            project_code=resolved_project.code,
        )
    )
    return resolved_project, selected_project, resolved_workflow


def _task_instance_list_resolved(
    *,
    resolved_project: ResolvedProject,
    selected_project: SelectedValue | None,
    resolved_workflow: ResolvedWorkflow | None,
    workflow_instance_id: int | None,
    workflow_instance_name: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
    search: str | None,
    task: str | None,
    task_code: int | None,
    executor: str | None,
    state: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    execute_type: str | None,
) -> TaskInstanceListResolvedData:
    project_data = _project_metadata(resolved_project)
    if selected_project is not None:
        project_data = dict(with_selection_source(project_data, selected_project))
    resolved: TaskInstanceListResolvedData = {
        "project": project_data,
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    optional_fields: dict[str, TaskInstanceListResolvedValue] = {
        "workflow_instance": workflow_instance_id,
        "workflow_instance_name": workflow_instance_name,
        "workflow": (
            None if resolved_workflow is None else _workflow_metadata(resolved_workflow)
        ),
        "search": search,
        "task": task,
        "task_code": task_code,
        "executor": executor,
        "state": state,
        "host": host,
        "start": start,
        "end": end,
        "execute_type": execute_type,
    }
    resolved.update(
        {key: value for key, value in optional_fields.items() if value is not None}
    )
    return resolved


def _project_metadata(project: ResolvedProject) -> ResolvedMetadata:
    return {
        "code": project.code,
        "name": project.name,
        "description": project.description,
    }


def _workflow_metadata(workflow: ResolvedWorkflow) -> ResolvedMetadata:
    return {
        "code": workflow.code,
        "name": workflow.name,
        "version": workflow.version,
    }


def _get_task_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> CommandResult:
    _, payload = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_task_instance(payload),
            label="task-instance data",
        ),
        resolved=require_json_object(
            {
                "workflowInstance": WorkflowInstanceSelectionData(
                    id=workflow_instance_id
                ),
                "taskInstance": TaskInstanceSelectionData(id=task_instance_id),
            },
            label="task-instance get resolved",
        ),
    )


def _watch_task_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    interval_seconds: int,
    timeout_seconds: int,
) -> CommandResult:
    workflow_instance = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    project_code = require_workflow_instance_project_code(workflow_instance.projectCode)
    started_at = time.monotonic()
    while True:
        try:
            payload = runtime.upstream.task_instances.get(
                project_code=project_code,
                task_instance_id=task_instance_id,
            )
        except ApiResultError as exc:
            raise _task_instance_not_found(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            ) from exc
        if payload.workflowInstanceId != workflow_instance_id:
            raise _task_instance_not_found(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            )
        state_name = enum_value(payload.state)
        if _task_instance_is_finished(state_name):
            return CommandResult(
                data=require_json_object(
                    serialize_task_instance(payload),
                    label="task-instance data",
                ),
                resolved=require_json_object(
                    _task_instance_resolved(
                        task_instance_id=task_instance_id,
                        workflow_instance_id=workflow_instance_id,
                    ),
                    label="task-instance watch resolved",
                ),
            )
        if timeout_seconds > 0 and (time.monotonic() - started_at) >= timeout_seconds:
            message = (
                "Timed out waiting for the task instance to reach a finished state."
            )
            raise WaitTimeoutError(
                message,
                details={
                    "resource": TASK_INSTANCE_RESOURCE,
                    "id": task_instance_id,
                    "workflow_instance_id": workflow_instance_id,
                    "last_state": state_name,
                    "timeout_seconds": timeout_seconds,
                },
                suggestion=(
                    "Retry with a larger --timeout-seconds value or inspect the "
                    "current state with "
                    f"`task-instance get {task_instance_id} --workflow-instance "
                    f"{workflow_instance_id}`."
                ),
            )
        time.sleep(interval_seconds)


def _get_sub_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> CommandResult:
    project_code, _ = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    try:
        relation = runtime.upstream.workflow_instances.sub_workflow_instance_by_task(
            project_code=project_code,
            task_instance_id=task_instance_id,
        )
    except ApiResultError as exc:
        raise _task_instance_sub_workflow_error(
            exc,
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        ) from exc
    sub_workflow_instance_id = relation.subWorkflowInstanceId
    if not isinstance(sub_workflow_instance_id, int) or sub_workflow_instance_id <= 0:
        raise _task_sub_workflow_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
    return CommandResult(
        data=require_json_object(
            TaskInstanceSubWorkflowData(subWorkflowInstanceId=sub_workflow_instance_id),
            label="task-instance sub-workflow data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            ),
            label="task-instance sub-workflow resolved",
        ),
    )


def _get_task_instance_log_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    tail: int,
) -> CommandResult:
    lines: deque[str] = deque(maxlen=tail)
    skip_line_num = 0
    for _ in range(MAX_LOG_CHUNKS):
        try:
            chunk = runtime.upstream.task_instances.log_chunk(
                task_instance_id=task_instance_id,
                skip_line_num=skip_line_num,
                limit=LOG_CHUNK_SIZE,
            )
        except ApiResultError as exc:
            raise _task_instance_log_error(
                exc,
                task_instance_id=task_instance_id,
            ) from exc
        chunk_lines = (chunk.message or "").splitlines()
        lines.extend(chunk_lines)
        if chunk.lineNum < LOG_CHUNK_SIZE:
            break
        skip_line_num += chunk.lineNum
    else:
        message = "Refusing to fetch more task log chunks than the safety limit"
        raise UserInputError(
            message,
            details={
                "task_instance_id": task_instance_id,
                "max_chunks": MAX_LOG_CHUNKS,
            },
            suggestion=(
                "Inspect the task log in the DS UI or worker log storage if you "
                "need more output than the CLI safety limit allows."
            ),
        )

    data = require_json_object(
        TaskLogData(
            text="\n".join(lines),
            lineCount=len(lines),
        ),
        label="task-instance log data",
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            {
                "taskInstance": TaskInstanceSelectionData(id=task_instance_id),
                "tail": tail,
            },
            label="task-instance log resolved",
        ),
    )


def _force_success_task_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> CommandResult:
    workflow_instance = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    workflow_state = enum_value(workflow_instance.state)
    if not _workflow_instance_is_final(workflow_state):
        message = "Force-success requires the owning workflow instance to be finished."
        raise InvalidStateError(
            message,
            details={
                "resource": TASK_INSTANCE_RESOURCE,
                "id": task_instance_id,
                "workflow_instance_id": workflow_instance_id,
                "workflow_state": workflow_state,
            },
            suggestion=(
                f"Run `{_workflow_instance_get_command(workflow_instance_id)}` to "
                "inspect the owning workflow instance. Wait for it to reach a "
                "final state, then retry `task-instance force-success`."
            ),
        )
    project_code, task_instance = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        workflow_instance=workflow_instance,
    )
    _require_task_instance_force_success_state(
        task_instance,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    try:
        runtime.upstream.task_instances.force_success(
            project_code=project_code,
            task_instance_id=task_instance_id,
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="force-success",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        ) from exc
    _, refreshed_payload = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        workflow_instance=workflow_instance,
    )
    return CommandResult(
        data=require_json_object(
            serialize_task_instance(refreshed_payload),
            label="task-instance data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            ),
            label="task-instance force-success resolved",
        ),
    )


def _savepoint_task_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> CommandResult:
    project_code, task_instance = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    _require_task_instance_active(
        task_instance,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        action="savepoint",
    )
    try:
        runtime.upstream.task_instances.savepoint(
            project_code=project_code,
            task_instance_id=task_instance_id,
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="savepoint",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        ) from exc
    _, refreshed_payload = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            TaskInstanceActionData(
                requested=True,
                taskInstance=serialize_task_instance(refreshed_payload),
            ),
            label="task-instance savepoint data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            ),
            label="task-instance savepoint resolved",
        ),
    )


def _stop_task_instance_result(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> CommandResult:
    project_code, task_instance = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    _require_task_instance_active(
        task_instance,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        action="stop",
    )
    try:
        runtime.upstream.task_instances.stop(
            project_code=project_code,
            task_instance_id=task_instance_id,
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="stop",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        ) from exc
    _, refreshed_payload = _task_instance_context(
        runtime,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            TaskInstanceActionData(
                requested=True,
                taskInstance=serialize_task_instance(refreshed_payload),
            ),
            label="task-instance stop data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
            ),
            label="task-instance stop resolved",
        ),
    )


def _normalized_task_instance_state(value: str | None) -> str | None:
    normalized = optional_text(value)
    if normalized is None:
        return None
    candidate = normalized.upper()
    try:
        return task_execution_status_value(candidate)
    except KeyError as exc:
        message = "Task instance state must be one of the DS execution status names"
        raise UserInputError(
            message,
            details={"state": value},
            suggestion=(
                "Run `dsctl enum list task_execution_status` to inspect the "
                "supported DS task-instance states."
            ),
        ) from exc


def _normalized_task_execute_type(value: str | None) -> str | None:
    normalized = optional_text(value)
    if normalized is None:
        return None
    candidate = normalized.upper()
    try:
        return task_execute_type_value(candidate)
    except KeyError as exc:
        message = "Task execute type must be one of the DS task execute-type names"
        raise UserInputError(
            message,
            details={"execute_type": value},
            suggestion=("Use BATCH or STREAM for --execute-type."),
        ) from exc


def _task_instance_context(
    runtime: ServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    workflow_instance: WorkflowInstanceRecord | None = None,
) -> tuple[int, TaskInstanceRecord]:
    owning_workflow_instance = (
        workflow_instance
        if workflow_instance is not None
        else get_workflow_instance(
            runtime,
            workflow_instance_id=workflow_instance_id,
        )
    )
    project_code = require_workflow_instance_project_code(
        owning_workflow_instance.projectCode
    )
    try:
        payload = runtime.upstream.task_instances.get(
            project_code=project_code,
            task_instance_id=task_instance_id,
        )
    except ApiResultError as exc:
        raise _task_instance_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        ) from exc
    if payload.workflowInstanceId != workflow_instance_id:
        raise _task_instance_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
    return project_code, payload


def _task_instance_not_found(
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> NotFoundError:
    message = (
        f"Task instance id {task_instance_id} was not found in workflow instance"
        f" {workflow_instance_id}"
    )
    return NotFoundError(
        message,
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
            "workflow_instance_id": workflow_instance_id,
        },
    )


def _task_instance_resolved(
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> dict[str, WorkflowInstanceSelectionData | TaskInstanceSelectionData]:
    return {
        "workflowInstance": WorkflowInstanceSelectionData(id=workflow_instance_id),
        "taskInstance": TaskInstanceSelectionData(id=task_instance_id),
    }


def _task_sub_workflow_not_found(
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> NotFoundError:
    return NotFoundError(
        (
            "Sub-workflow instance for task instance id "
            f"{task_instance_id} was not found in workflow instance "
            f"{workflow_instance_id}"
        ),
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
            "workflow_instance_id": workflow_instance_id,
            "relation": "sub_workflow",
        },
    )


def _workflow_instance_is_final(state_name: str | None) -> bool:
    return workflow_execution_status_is_final(state_name)


def _task_instance_is_finished(state_name: str | None) -> bool:
    return state_name in TASK_EXECUTION_FINISHED_STATES


def _require_task_instance_force_success_state(
    task_instance: TaskInstanceRecord,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> None:
    state_name = enum_value(task_instance.state)
    if state_name in TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES:
        return
    command = _task_instance_get_command(
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    message = (
        "Force-success requires the task instance to be in FAILURE, "
        "NEED_FAULT_TOLERANCE, or KILL state."
    )
    raise InvalidStateError(
        message,
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
            "workflow_instance_id": workflow_instance_id,
            "state": state_name,
        },
        suggestion=(
            f"Run `{command}` "
            "to inspect the current task state. `task-instance force-success` "
            "only applies to FAILURE, NEED_FAULT_TOLERANCE, or KILL."
        ),
    )


def _require_task_instance_active(
    task_instance: TaskInstanceRecord,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    action: str,
) -> None:
    state_name = enum_value(task_instance.state)
    if state_name not in TASK_EXECUTION_FINISHED_STATES:
        return
    command = _task_instance_get_command(
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    message = f"Task-instance {action} requires the task instance to still be running."
    raise InvalidStateError(
        message,
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
            "workflow_instance_id": workflow_instance_id,
            "state": state_name,
        },
        suggestion=(
            f"Run `{command}` "
            "to inspect the current task state. `task-instance "
            f"{action}` only applies while the task instance is still running."
        ),
    )


def _task_instance_action_error(
    error: ApiResultError,
    *,
    action: str,
    task_instance_id: int,
    workflow_instance_id: int,
) -> ApiResultError | InvalidStateError | NotFoundError | PermissionDeniedError:
    details: dict[str, object] = {
        "resource": TASK_INSTANCE_RESOURCE,
        "id": task_instance_id,
        "workflow_instance_id": workflow_instance_id,
        "action": action,
    }
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        message = (
            f"Task instance id {task_instance_id} was not found in workflow "
            f"instance {workflow_instance_id}"
        )
        return NotFoundError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = (
            "The current user requires additional permissions for this "
            "task instance action."
        )
        return PermissionDeniedError(message, details=details)
    if error.result_code == TASK_INSTANCE_STATE_OPERATION_ERROR:
        command = _task_instance_get_command(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
        if action == "force-success":
            suggestion = (
                f"Run `{command}` "
                "to inspect the current task state, and confirm the owning "
                "workflow instance is already finished before retrying "
                "`task-instance force-success`."
            )
        else:
            suggestion = (
                f"Run `{command}` "
                "to inspect the current task state, then retry "
                f"`task-instance {action}` only while the task is still running."
            )
        return InvalidStateError(error.message, details=details, suggestion=suggestion)
    if error.result_code in {TASK_SAVEPOINT_ERROR, TASK_STOP_ERROR}:
        # These controller fallback codes only say the action failed; keep
        # the raw DS result instead of inventing a fake stable CLI semantic.
        return ApiResultError(
            result_code=error.result_code,
            result_message=error.result_message,
            details=details,
        )
    return error


def _task_instance_log_error(
    error: ApiResultError,
    *,
    task_instance_id: int,
) -> ApiResultError | TaskNotDispatchedError:
    if error.result_code != TASK_INSTANCE_LOG_PATH_EMPTY:
        return error
    return TaskNotDispatchedError(
        "Task instance log is not available because the task has not been dispatched.",
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
            "result_code": error.result_code,
            "result_message": error.result_message,
        },
        source=error.source,
        suggestion=(
            "Inspect the task instance state with `task-instance list "
            "--workflow-instance <workflow_instance_id>` or run "
            "`workflow-instance digest <workflow_instance_id>` to confirm "
            "whether DolphinScheduler has dispatched the task."
        ),
    )


def _task_instance_sub_workflow_error(
    error: ApiResultError,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
) -> ApiResultError | InvalidStateError | NotFoundError:
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        return _task_instance_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
    if error.result_code == TASK_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE:
        command = _task_instance_get_command(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
        return InvalidStateError(
            f"Task instance id {task_instance_id} is not a SUB_WORKFLOW task instance.",
            details={
                "resource": TASK_INSTANCE_RESOURCE,
                "id": task_instance_id,
                "workflow_instance_id": workflow_instance_id,
                "relation": "sub_workflow",
            },
            suggestion=(
                f"Run `{command}` "
                "to inspect the task type. Only SUB_WORKFLOW task instances "
                "have a child workflow instance."
            ),
        )
    if error.result_code == SUB_WORKFLOW_INSTANCE_NOT_EXIST:
        return _task_sub_workflow_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
    return error
