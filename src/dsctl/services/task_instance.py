from __future__ import annotations

import time
from collections import deque
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import TASK_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
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
from dsctl.services._validation import require_non_negative_int, require_positive_int
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.upstream.runtime_enums import (
    TASK_EXECUTION_FINISHED_STATES,
    TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES,
    task_execution_status_value,
    workflow_execution_status_is_final,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import TaskInstanceRecord, WorkflowInstanceRecord


LOG_CHUNK_SIZE = 1000
MAX_LOG_CHUNKS = 200
DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS = 5
DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS = 600
TASK_INSTANCE_NOT_FOUND = 10008
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
    workflow_instance: int,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    search: str | None = None,
    state: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List task instances inside one workflow instance."""
    normalized_workflow_instance = require_positive_int(
        workflow_instance,
        label="workflow_instance",
    )
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    normalized_search = optional_text(search)
    normalized_state = _normalized_task_instance_state(state)
    return run_with_service_runtime(
        env_file,
        _list_task_instances_result,
        workflow_instance_id=normalized_workflow_instance,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
        search=normalized_search,
        state=normalized_state,
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
    workflow_instance_id: int,
    page_no: int,
    page_size: int,
    all_pages: bool,
    search: str | None,
    state: str | None,
) -> CommandResult:
    workflow_instance = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    project_code = require_workflow_instance_project_code(
        workflow_instance.projectCode,
    )
    data = require_json_object(
        requested_page_data(
            lambda current_page_no, current_page_size: (
                runtime.upstream.task_instances.list(
                    workflow_instance_id=workflow_instance_id,
                    project_code=project_code,
                    page_no=current_page_no,
                    page_size=current_page_size,
                    search=search,
                    state=state,
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
    resolved: dict[str, int | str | None] = {
        "workflow_instance": workflow_instance_id,
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    if search is not None:
        resolved["search"] = search
    if state is not None:
        resolved["state"] = state
    return CommandResult(
        data=data,
        resolved=require_json_object(
            resolved,
            label="task-instance list resolved",
        ),
    )


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
        chunk = runtime.upstream.task_instances.log_chunk(
            task_instance_id=task_instance_id,
            skip_line_num=skip_line_num,
            limit=LOG_CHUNK_SIZE,
        )
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
