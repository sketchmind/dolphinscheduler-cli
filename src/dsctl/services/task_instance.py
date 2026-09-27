from __future__ import annotations

import shlex
import time
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import TASK_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ExecutionFailedError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    TaskNotDispatchedError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.execution_states import TASK_EXECUTION_SUCCESS_STATES
from dsctl.output import CommandResult, JsonObject, require_json_object
from dsctl.services._validation import (
    optional_ds_datetime,
    require_non_negative_int,
    require_positive_int,
    validate_ds_datetime_range,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    with_selection_source,
)
from dsctl.upstream.definition_models import NativeCode
from dsctl.upstream.instance_time_filters import instance_time_filter_contract
from dsctl.upstream.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    observation_time,
    requested_page_data,
)
from dsctl.upstream.runtime_enums import (
    TASK_EXECUTION_FINISHED_STATES,
    TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES,
    task_execute_type_value,
    task_execution_status_value,
    workflow_execution_status_is_final,
)
from dsctl.upstream.runtime_instances import (
    RUNTIME_INSTANCE_DOMAIN,
    LocatedTaskInstance,
    RuntimeInstanceDomain,
    TaskInstanceListing,
    TaskInstanceSnapshot,
)
from dsctl.upstream.serialization import (
    TaskLogData,
    enum_value,
    optional_text,
)

if TYPE_CHECKING:
    from dsctl.upstream.definition_models import ProjectRef

ResolvedMetadataValue: TypeAlias = int | str | None
ResolvedMetadata: TypeAlias = dict[str, ResolvedMetadataValue]
TaskInstanceListResolvedValue: TypeAlias = int | str | bool | None | ResolvedMetadata
TaskInstanceListResolvedData: TypeAlias = dict[str, TaskInstanceListResolvedValue]
RuntimeInstanceServiceRuntime = BoundDomainServiceRuntime[RuntimeInstanceDomain]


DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS = 5
DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS = 600
TASK_INSTANCE_NOT_FOUND = 10008
TASK_INSTANCE_LOG_PATH_EMPTY = 10103
TASK_INSTANCE_LOG_PATH_EMPTY_MARKER = "TaskInstanceLogPath is empty"
TASK_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE = 10021
TASK_INSTANCE_STATE_OPERATION_ERROR = 10166
TASK_SAVEPOINT_ERROR = 10196
TASK_STOP_ERROR = 10197
SUB_WORKFLOW_INSTANCE_NOT_EXIST = 50007
USER_NO_OPERATION_PERM = 30001
USER_NO_OPERATION_PROJECT_PERM = 30002


def _task_instance_get_command(
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project_selector: str,
) -> str:
    parts = [
        "dsctl",
        "task-instance",
        "get",
        str(task_instance_id),
        "--project",
        project_selector,
    ]
    if workflow_instance_id is not None:
        parts.extend(("--workflow-instance", str(workflow_instance_id)))
    return shlex.join(parts)


def _workflow_instance_get_command(
    workflow_instance_id: int,
    *,
    project_selector: str,
) -> str:
    return shlex.join(
        (
            "dsctl",
            "workflow-instance",
            "get",
            str(workflow_instance_id),
            "--project",
            project_selector,
        )
    )


def _task_instance_list_command(
    *,
    workflow_instance_id: int | None,
    project_selector: str,
) -> str:
    parts = ["dsctl", "task-instance", "list", "--project", project_selector]
    if workflow_instance_id is not None:
        parts.extend(("--workflow-instance", str(workflow_instance_id)))
    return shlex.join(parts)


def _unsupported_task_instance_workflow_filter(
    *,
    workflow: str,
    workflow_instance_id: int | None,
) -> UserInputError:
    if workflow_instance_id is not None:
        message = "`task-instance list` does not accept --workflow"
        suggestion = (
            "Drop --workflow; --workflow-instance already scopes the "
            "task-instance query to one workflow run."
        )
    else:
        message = (
            "`task-instance list` cannot reliably filter by workflow definition "
            "in DolphinScheduler 3.4.1"
        )
        suggestion = (
            "Use `dsctl workflow-instance list` in the selected project to find "
            f"an instance of workflow {workflow!r}, then pass its id to "
            "`dsctl task-instance list --workflow-instance`."
        )
    return UserInputError(
        message,
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "workflow": workflow,
            "workflow_instance_id": workflow_instance_id,
            "upstream_filter": "workflowDefinitionName",
            "reason": (
                "DS 3.4.1 ignores workflowDefinitionName for regular BATCH "
                "task-instance paging queries."
            ),
        },
        suggestion=suggestion,
    )


class WorkflowInstanceSelectionData(TypedDict):
    """Resolved workflow-instance selector emitted in JSON envelopes."""

    id: int


class TaskInstanceSelectionData(TypedDict):
    """Resolved task-instance selector emitted in JSON envelopes."""

    id: int


class TaskInstanceActionData(TypedDict):
    """CLI task-instance action payload with a refreshed task snapshot."""

    requested: bool
    taskInstance: JsonObject


class TaskInstanceSubWorkflowData(TypedDict):
    """DS-native sub-workflow relation payload emitted for one task instance."""

    subWorkflowInstanceId: int


TaskInstancePageData = PageData[JsonObject]


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
    if normalized_workflow is not None:
        raise _unsupported_task_instance_workflow_filter(
            workflow=normalized_workflow,
            workflow_instance_id=normalized_workflow_instance,
        )
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
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
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
    workflow_instance: int | None = None,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Get one task instance by id within one selected project."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = (
        None
        if workflow_instance is None
        else require_positive_int(workflow_instance, label="workflow_instance")
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _get_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
    )


def watch_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int | None = None,
    project: str | None = None,
    interval_seconds: int = DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
    exit_status: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Poll one task instance until it reaches a finished state."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = (
        None
        if workflow_instance is None
        else require_positive_int(workflow_instance, label="workflow_instance")
    )
    normalized_interval_seconds = require_positive_int(
        interval_seconds,
        label="interval_seconds",
        input_hint="--interval-seconds",
    )
    normalized_timeout_seconds = require_non_negative_int(
        timeout_seconds,
        label="timeout_seconds",
        input_hint="--timeout-seconds",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _watch_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
        interval_seconds=normalized_interval_seconds,
        timeout_seconds=normalized_timeout_seconds,
        exit_status=exit_status,
    )


def get_sub_workflow_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    project: str | None = None,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _get_sub_workflow_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
    )


def get_task_instance_log_result(
    task_instance: int,
    *,
    tail: int | None = None,
    start_line: int | None = None,
    limit: int | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Fetch a bounded tail of one task-instance log."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    window_mode = start_line is not None or limit is not None
    if window_mode and tail is not None:
        message = "--tail cannot be combined with --start-line or --limit"
        raise UserInputError(
            message,
            suggestion="Choose either --tail or a --start-line/--limit window.",
        )
    normalized_start = require_positive_int(
        start_line if start_line is not None else 1,
        label="start_line",
        input_hint="--start-line",
    )
    normalized_limit = require_positive_int(
        limit if limit is not None else 200, label="limit", input_hint="--limit"
    )
    normalized_tail = require_positive_int(
        tail if tail is not None else 200,
        label="tail",
        input_hint="--tail",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _get_task_instance_log_result,
        task_instance_id=normalized_task_instance,
        tail=normalized_tail,
        start_line=normalized_start if window_mode else None,
        limit=normalized_limit,
    )


def force_success_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int,
    project: str | None = None,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _force_success_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
    )


def savepoint_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int | None = None,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Request one savepoint for a running task instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = (
        None
        if workflow_instance is None
        else require_positive_int(workflow_instance, label="workflow_instance")
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _savepoint_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
    )


def stop_task_instance_result(
    task_instance: int,
    *,
    workflow_instance: int | None = None,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Request stop for one task instance."""
    normalized_task_instance = require_positive_int(
        task_instance,
        label="task_instance",
    )
    normalized_workflow_instance = (
        None
        if workflow_instance is None
        else require_positive_int(workflow_instance, label="workflow_instance")
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _stop_task_instance_result,
        task_instance_id=normalized_task_instance,
        workflow_instance_id=normalized_workflow_instance,
        project=optional_text(project),
    )


def _list_task_instances_result(
    runtime: RuntimeInstanceServiceRuntime,
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
    time_filter = instance_time_filter_contract(
        runtime.profile.ds_version, "task-instance.list"
    )
    time_filter.validate(start=start, end=end)
    del workflow
    selected_project = require_project_selection(project, runtime=runtime)

    instances = runtime.domain.instances

    def load(current_page_no: int, current_page_size: int) -> TaskInstanceListing:
        return instances.list_task_instances(
            project_selector=selected_project.value,
            workflow_instance_id=workflow_instance_id,
            workflow_instance_name=workflow_instance_name,
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

    observation_started_at = observation_time()
    try:
        initial = load(page_no, page_size)
    except ApiResultError as exc:
        raise _task_instance_list_error(
            exc,
            project=selected_project.value,
            workflow_instance_id=workflow_instance_id,
        ) from exc

    data = require_json_object(
        requested_page_data(
            lambda current_page_no, current_page_size: (
                initial.page
                if current_page_no == page_no and current_page_size == page_size
                else load(current_page_no, current_page_size).page
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            observation_started_at=observation_started_at,
            resource=TASK_INSTANCE_RESOURCE,
            serialize_item=lambda item: item.to_data(),
            max_pages=MAX_AUTO_EXHAUST_PAGES,
            translate_error=lambda exc: _task_instance_list_error(
                exc,
                project=selected_project.value,
                workflow_instance_id=workflow_instance_id,
            ),
        ),
        label="task-instance list data",
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            {
                "time_filter": time_filter.to_data(),
                **_task_instance_list_resolved(
                    resolved_project=initial.project,
                    selected_project=selected_project,
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
            },
            label="task-instance list resolved",
        ),
    )


def _task_instance_list_error(
    error: ApiResultError,
    *,
    project: str | None,
    workflow_instance_id: int | None,
) -> ApiResultError | PermissionDeniedError:
    if error.result_code not in {
        USER_NO_OPERATION_PERM,
        USER_NO_OPERATION_PROJECT_PERM,
    }:
        return error
    return PermissionDeniedError(
        "The current user does not have permission to list task instances.",
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "project": project,
            "workflow_instance_id": workflow_instance_id,
        },
        suggestion=(
            "Ask a DolphinScheduler administrator to grant access to the selected "
            "project, then retry."
        ),
    )


def _task_instance_list_resolved(
    *,
    resolved_project: ProjectRef,
    selected_project: SelectedValue,
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


def _project_metadata(project: ProjectRef) -> ResolvedMetadata:
    identity = "code" if isinstance(project.native, NativeCode) else "id"
    return {
        identity: project.native.value,
        "name": project.name,
        "description": project.description,
    }


def _get_task_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_task_instance_context(
        runtime,
        project=project,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            located.task.to_data(),
            label="task-instance data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
                project=located.project,
                selected_project=selected_project,
            ),
            label="task-instance get resolved",
        ),
    )


def _watch_task_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project: str | None,
    interval_seconds: int,
    timeout_seconds: int,
    exit_status: bool = False,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    started_at = time.monotonic()
    while True:
        located = _task_instance_context(
            runtime,
            project_selector=selected_project.value,
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
        payload = located.task
        state_name = enum_value(payload.state)
        if _task_instance_is_finished(state_name):
            return CommandResult(
                data=require_json_object(
                    payload.to_data(),
                    label="task-instance data",
                ),
                resolved=require_json_object(
                    _task_instance_resolved(
                        task_instance_id=task_instance_id,
                        workflow_instance_id=workflow_instance_id,
                        project=located.project,
                        selected_project=selected_project,
                    ),
                    label="task-instance watch resolved",
                ),
                failure=(
                    ExecutionFailedError(
                        f"Task instance {task_instance_id} finished in {state_name}.",
                        details={"state": state_name, "id": task_instance_id},
                        suggestion="Inspect the task log before retrying its workflow.",
                    )
                    if exit_status and state_name not in TASK_EXECUTION_SUCCESS_STATES
                    else None
                ),
            )
        if timeout_seconds > 0 and (time.monotonic() - started_at) >= timeout_seconds:
            message = (
                "Timed out waiting for the task instance to reach a finished state."
            )
            inspect_command = _task_instance_get_command(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
                project_selector=selected_project.value,
            )
            timeout_details: JsonObject = {
                "resource": TASK_INSTANCE_RESOURCE,
                "id": task_instance_id,
                "last_state": state_name,
                "timeout_seconds": timeout_seconds,
            }
            if workflow_instance_id is not None:
                timeout_details["workflow_instance_id"] = workflow_instance_id
            raise WaitTimeoutError(
                message,
                details=timeout_details,
                suggestion=(
                    "Retry with a larger --timeout-seconds value or inspect the "
                    f"current state with `{inspect_command}`."
                ),
            )
        time.sleep(interval_seconds)


def _get_sub_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_task_instance_context(
        runtime,
        project=project,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    try:
        sub_workflow_instance_id = runtime.domain.instances.sub_workflow_instance_id(
            located
        )
    except ApiResultError as exc:
        raise _task_instance_sub_workflow_error(
            exc,
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        ) from exc
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
                project=located.project,
                selected_project=selected_project,
            ),
            label="task-instance sub-workflow resolved",
        ),
    )


def _get_task_instance_log_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    tail: int,
    start_line: int | None = None,
    limit: int = 200,
) -> CommandResult:
    try:
        if start_line is None:
            log_tail = runtime.domain.instances.tail_task_log(
                task_instance_id=task_instance_id, max_lines=tail
            )
        else:
            log_tail = runtime.domain.instances.window_task_log(
                task_instance_id=task_instance_id, start_line=start_line, limit=limit
            )
    except ApiResultError as exc:
        raise _task_instance_log_error(
            exc,
            task_instance_id=task_instance_id,
        ) from exc

    data = require_json_object(
        TaskLogData(
            text=log_tail.text,
            lineCount=log_tail.line_count,
        ),
        label="task-instance log data",
    )
    if log_tail.window is not None:
        data["window"] = require_json_object(log_tail.window, label="task log window")
    return CommandResult(
        data=data,
        resolved=require_json_object(
            {
                "taskInstance": TaskInstanceSelectionData(id=task_instance_id),
                **(
                    {"tail": tail}
                    if start_line is None
                    else {"start_line": start_line, "limit": limit}
                ),
            },
            label="task-instance log resolved",
        ),
    )


def _force_success_task_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_task_instance_context(
        runtime,
        project=project,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    workflow = located.workflow_instance
    if workflow is None:
        message = "Force-success requires an owning workflow instance."
        raise InvalidStateError(message)
    workflow_state = enum_value(workflow.state)
    if not _workflow_instance_is_final(workflow_state):
        workflow_get_command = _workflow_instance_get_command(
            workflow_instance_id,
            project_selector=selected_project.value,
        )
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
                f"Run `{workflow_get_command}` to "
                "inspect the owning workflow instance. Wait for it to reach a "
                "final state, then retry `task-instance force-success`."
            ),
        )
    _require_task_instance_force_success_state(
        located.task,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        project_selector=selected_project.value,
    )
    try:
        runtime.domain.instances.task_action(
            located,
            action="force-success",
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="force-success",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        ) from exc
    refreshed = _task_instance_context(
        runtime,
        project_selector=selected_project.value,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            refreshed.task.to_data(),
            label="task-instance data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
                project=refreshed.project,
                selected_project=selected_project,
            ),
            label="task-instance force-success resolved",
        ),
    )


def _savepoint_task_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_task_instance_context(
        runtime,
        project=project,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    _require_task_instance_active(
        located.task,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        action="savepoint",
        project_selector=selected_project.value,
    )
    try:
        runtime.domain.instances.task_action(
            located,
            action="savepoint",
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="savepoint",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        ) from exc
    refreshed = _task_instance_context(
        runtime,
        project_selector=selected_project.value,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            TaskInstanceActionData(
                requested=True,
                taskInstance=refreshed.task.to_data(),
            ),
            label="task-instance savepoint data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
                project=refreshed.project,
                selected_project=selected_project,
            ),
            label="task-instance savepoint resolved",
        ),
    )


def _stop_task_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_task_instance_context(
        runtime,
        project=project,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    _require_task_instance_active(
        located.task,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        action="stop",
        project_selector=selected_project.value,
    )
    try:
        runtime.domain.instances.task_action(
            located,
            action="stop",
        )
    except ApiResultError as exc:
        raise _task_instance_action_error(
            exc,
            action="stop",
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=selected_project.value,
        ) from exc
    refreshed = _task_instance_context(
        runtime,
        project_selector=selected_project.value,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            TaskInstanceActionData(
                requested=True,
                taskInstance=refreshed.task.to_data(),
            ),
            label="task-instance stop data",
        ),
        resolved=require_json_object(
            _task_instance_resolved(
                task_instance_id=task_instance_id,
                workflow_instance_id=workflow_instance_id,
                project=refreshed.project,
                selected_project=selected_project,
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
                "Run `dsctl enum list task-execution-status` to inspect the "
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
            suggestion=(
                "Run `dsctl enum list task-execute-type` to inspect the "
                "supported DS task execute-type names."
            ),
        ) from exc


def _task_instance_context(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_selector: str,
    task_instance_id: int,
    workflow_instance_id: int | None,
) -> LocatedTaskInstance:
    try:
        return runtime.domain.instances.get_task_instance(
            project_selector=project_selector,
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
        )
    except ApiResultError as exc:
        translated = _task_instance_read_error(
            exc,
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        if translated is exc:
            raise
        raise translated from exc


def _selected_task_instance_context(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project: str | None,
    task_instance_id: int,
    workflow_instance_id: int | None,
) -> tuple[SelectedValue, LocatedTaskInstance]:
    """Resolve project selection before one direct project-scoped read."""
    selected_project = require_project_selection(project, runtime=runtime)
    return selected_project, _task_instance_context(
        runtime,
        project_selector=selected_project.value,
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
    )


def _task_instance_not_found(
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project_selector: str,
) -> NotFoundError:
    list_command = _task_instance_list_command(
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
    )
    details: JsonObject = {
        "resource": TASK_INSTANCE_RESOURCE,
        "id": task_instance_id,
    }
    if workflow_instance_id is None:
        message = (
            f"Task instance id {task_instance_id} was not found in project "
            f"{project_selector!r}"
        )
        suggestion = (
            f"Run `{list_command}` to inspect BATCH task instances. On versions "
            f"with standalone STREAM tasks, also run `{list_command} "
            "--execute-type STREAM`."
        )
    else:
        message = (
            f"Task instance id {task_instance_id} was not found in workflow instance"
            f" {workflow_instance_id}"
        )
        details["workflow_instance_id"] = workflow_instance_id
        suggestion = f"Run `{list_command}` to inspect available task instance ids."
    return NotFoundError(
        message,
        details=details,
        suggestion=suggestion,
    )


def _task_instance_read_error(
    error: ApiResultError,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project_selector: str,
) -> ApiResultError | NotFoundError | PermissionDeniedError:
    """Translate only unambiguous task-instance read failures."""
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        return _task_instance_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
    if error.result_code in {
        USER_NO_OPERATION_PERM,
        USER_NO_OPERATION_PROJECT_PERM,
    }:
        details: JsonObject = {
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
        }
        if workflow_instance_id is not None:
            details["workflow_instance_id"] = workflow_instance_id
        return PermissionDeniedError(
            "The current user does not have permission to access this task instance.",
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator to grant access to the "
                "selected project, then retry."
            ),
        )
    return error


def _task_instance_resolved(
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    project: ProjectRef,
    selected_project: SelectedValue,
) -> JsonObject:
    project_data: JsonObject = {
        (
            "code" if isinstance(project.native, NativeCode) else "id"
        ): project.native.value,
        "name": project.name,
        "description": project.description,
        "source": selected_project.source,
    }
    task_instance: JsonObject = {"id": task_instance_id}
    resolved: JsonObject = {
        "taskInstance": task_instance,
        "project": project_data,
    }
    if workflow_instance_id is not None:
        resolved["workflowInstance"] = {"id": workflow_instance_id}
    return resolved


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
    task_instance: TaskInstanceSnapshot,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    project_selector: str,
) -> None:
    state_name = enum_value(task_instance.state)
    if state_name in TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES:
        return
    command = _task_instance_get_command(
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
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
    task_instance: TaskInstanceSnapshot,
    *,
    task_instance_id: int,
    workflow_instance_id: int | None,
    action: str,
    project_selector: str,
) -> None:
    state_name = enum_value(task_instance.state)
    if state_name not in TASK_EXECUTION_FINISHED_STATES:
        return
    command = _task_instance_get_command(
        task_instance_id=task_instance_id,
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
    )
    message = f"Task-instance {action} requires the task instance to still be running."
    details: JsonObject = {
        "resource": TASK_INSTANCE_RESOURCE,
        "id": task_instance_id,
        "state": state_name,
    }
    if workflow_instance_id is not None:
        details["workflow_instance_id"] = workflow_instance_id
    raise InvalidStateError(
        message,
        details=details,
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
    workflow_instance_id: int | None,
    project_selector: str,
) -> ApiResultError | InvalidStateError | NotFoundError | PermissionDeniedError:
    details: dict[str, object] = {
        "resource": TASK_INSTANCE_RESOURCE,
        "id": task_instance_id,
        "action": action,
    }
    if workflow_instance_id is not None:
        details["workflow_instance_id"] = workflow_instance_id
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        list_command = _task_instance_list_command(
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        message = (
            f"Task instance id {task_instance_id} was not found in project "
            f"{project_selector!r}"
            if workflow_instance_id is None
            else (
                f"Task instance id {task_instance_id} was not found in workflow "
                f"instance {workflow_instance_id}"
            )
        )
        suggestion = (
            f"Run `{list_command}` to inspect BATCH task instances. On versions "
            f"with standalone STREAM tasks, also run `{list_command} "
            "--execute-type STREAM`."
            if workflow_instance_id is None
            else f"Run `{list_command}` to inspect available task instance ids."
        )
        return NotFoundError(
            message,
            details=details,
            source=error.source,
            suggestion=suggestion,
        )
    if error.result_code in {
        USER_NO_OPERATION_PERM,
        USER_NO_OPERATION_PROJECT_PERM,
    }:
        message = (
            "The current user requires additional permissions for this "
            "task instance action."
        )
        return PermissionDeniedError(message, details=details)
    if error.result_code == TASK_INSTANCE_STATE_OPERATION_ERROR:
        command = _task_instance_get_command(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
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
) -> ApiResultError | NotFoundError | TaskNotDispatchedError:
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        return NotFoundError(
            f"Task instance id {task_instance_id} was not found",
            details={
                "resource": TASK_INSTANCE_RESOURCE,
                "id": task_instance_id,
            },
            source=error.source,
            suggestion=(
                "Use `dsctl workflow-instance list` in the relevant project to "
                "find the owning workflow instance, then inspect it with "
                "`dsctl task-instance list --workflow-instance`."
            ),
        )
    if (
        error.result_code != TASK_INSTANCE_LOG_PATH_EMPTY
        or TASK_INSTANCE_LOG_PATH_EMPTY_MARKER not in error.result_message
    ):
        return error
    return TaskNotDispatchedError(
        "Task instance log is not available because the task has not been dispatched.",
        details={
            "resource": TASK_INSTANCE_RESOURCE,
            "id": task_instance_id,
        },
        source=error.source,
        suggestion=(
            "Use `dsctl workflow-instance list` in the relevant project to find "
            "the owning workflow instance, then inspect it with "
            "`dsctl task-instance list --workflow-instance`."
        ),
    )


def _task_instance_sub_workflow_error(
    error: ApiResultError,
    *,
    task_instance_id: int,
    workflow_instance_id: int,
    project_selector: str,
) -> ApiResultError | InvalidStateError | NotFoundError:
    if error.result_code == TASK_INSTANCE_NOT_FOUND:
        return _task_instance_not_found(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
    if error.result_code == TASK_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE:
        command = _task_instance_get_command(
            task_instance_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
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
