from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import TASK_GROUP_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    TaskGroupData,
    TaskGroupQueueData,
    require_resource_text,
    serialize_task_group,
    serialize_task_group_queue,
)
from dsctl.services._validation import (
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import task_group as resolve_task_group
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    with_selection_source,
)

if TYPE_CHECKING:
    from dsctl.services._resolver_models import (
        ResolvedProjectData,
        ResolvedTaskGroupData,
    )
    from dsctl.upstream.protocol import TaskGroupRecord

TASK_GROUP_NAME_EXISTS = 130001
TASK_GROUP_SIZE_ERROR = 130002
TASK_GROUP_STATUS_ERROR = 130003
CREATE_TASK_GROUP_ERROR = 130008
UPDATE_TASK_GROUP_ERROR = 130009
QUERY_TASK_GROUP_LIST_ERROR = 130010
CLOSE_TASK_GROUP_ERROR = 130011
START_TASK_GROUP_ERROR = 130012
QUERY_TASK_GROUP_QUEUE_LIST_ERROR = 130013
TASK_GROUP_QUEUE_ALREADY_START = 130017
TASK_GROUP_STATUS_CLOSED = 130018
TASK_GROUP_STATUS_OPENED = 130019
USER_NO_OPERATION_PROJECT_PERM = 30002
USER_NO_WRITE_PROJECT_PERM = 30003
NO_CURRENT_OPERATING_PERMISSION = 1400001
DESCRIPTION_TOO_LONG_ERROR = 1400004


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
TaskGroupPageData: TypeAlias = PageData[TaskGroupData]
TaskGroupQueuePageData: TypeAlias = PageData[TaskGroupQueueData]
DescriptionUpdate = str | _UnsetValue


class TaskGroupQueueForceStartData(TypedDict):
    """CLI confirmation payload for force-starting one queue item."""

    queueId: int
    forceStarted: bool


class TaskGroupQueuePriorityData(TypedDict):
    """CLI confirmation payload for updating one queue priority."""

    queueId: int
    priority: int


class TaskGroupListResolved(TypedDict, total=False):
    """Resolved metadata emitted for `task-group list`."""

    search: str | None
    status: str | None
    page_no: int
    page_size: int
    all: bool
    project: dict[str, str | int | None]


def _task_group_identifier(
    *,
    task_group_name: str | None,
    task_group_id: int | None,
) -> str:
    if task_group_name is not None:
        return task_group_name
    if task_group_id is not None:
        return str(task_group_id)
    return "TASK_GROUP"


def list_task_groups_result(
    *,
    project: str | None = None,
    search: str | None = None,
    status: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List task groups with optional project or status filtering."""
    normalized_project = _normalized_optional_text(project)
    normalized_search = _normalized_optional_text(search)
    status_filter = _parse_task_group_status_filter(status)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")
    if normalized_project is not None and (
        normalized_search is not None or status_filter is not None
    ):
        message = (
            "Task-group list cannot combine --project with --search or --status "
            "because DolphinScheduler 3.4.1 does not expose that filter shape"
        )
        raise UserInputError(
            message,
            suggestion=(
                "Retry with only --project, or drop --project and use --search/"
                "--status."
            ),
        )

    return run_with_service_runtime(
        env_file,
        _list_task_groups_result,
        project=normalized_project,
        search=normalized_search,
        status_filter=status_filter,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_task_group_result(
    task_group: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one task-group payload."""
    return run_with_service_runtime(
        env_file,
        _get_task_group_result,
        task_group=task_group,
    )


def create_task_group_result(
    *,
    name: str,
    group_size: int,
    project: str | None = None,
    description: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one task group from validated CLI input."""
    normalized_name = require_non_empty_text(name, label="task-group name")
    normalized_description = _task_group_description(description)
    require_positive_int(group_size, label="group_size")

    return run_with_service_runtime(
        env_file,
        _create_task_group_result,
        name=normalized_name,
        group_size=group_size,
        project=project,
        description=normalized_description,
    )


def update_task_group_result(
    task_group: str,
    *,
    name: str | None = None,
    group_size: int | None = None,
    description: DescriptionUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one task group while preserving omitted fields."""
    if name is None and group_size is None and isinstance(description, _UnsetValue):
        message = "Task-group update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --name, --group-size, or "
                "--description."
            ),
        )

    normalized_name = (
        require_non_empty_text(name, label="task-group name")
        if name is not None
        else None
    )
    normalized_description = (
        _task_group_description(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )
    if group_size is not None:
        require_positive_int(group_size, label="group_size")

    return run_with_service_runtime(
        env_file,
        _update_task_group_result,
        task_group=task_group,
        name=normalized_name,
        group_size=group_size,
        description=normalized_description,
    )


def close_task_group_result(
    task_group: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Close one task group."""
    return run_with_service_runtime(
        env_file,
        _close_task_group_result,
        task_group=task_group,
    )


def start_task_group_result(
    task_group: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Start one task group."""
    return run_with_service_runtime(
        env_file,
        _start_task_group_result,
        task_group=task_group,
    )


def list_task_group_queues_result(
    task_group: str,
    *,
    task_instance: str | None = None,
    workflow_instance: str | None = None,
    status: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List task-group queue rows for one task group."""
    normalized_task_instance = _normalized_optional_text(task_instance)
    normalized_workflow_instance = _normalized_optional_text(workflow_instance)
    queue_status_filter = _parse_task_group_queue_status_filter(status)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_task_group_queues_result,
        task_group=task_group,
        task_instance=normalized_task_instance,
        workflow_instance=normalized_workflow_instance,
        status_filter=queue_status_filter,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def force_start_task_group_queue_result(
    queue_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Force-start one task-group queue row."""
    require_positive_int(queue_id, label="queue_id")
    return run_with_service_runtime(
        env_file,
        _force_start_task_group_queue_result,
        queue_id=queue_id,
    )


def set_task_group_queue_priority_result(
    queue_id: int,
    *,
    priority: int,
    env_file: str | None = None,
) -> CommandResult:
    """Set one task-group queue priority."""
    require_positive_int(queue_id, label="queue_id")
    require_non_negative_int(priority, label="priority")
    return run_with_service_runtime(
        env_file,
        _set_task_group_queue_priority_result,
        queue_id=queue_id,
        priority=priority,
    )


def _list_task_groups_result(
    runtime: ServiceRuntime,
    *,
    project: str | None,
    search: str | None,
    status_filter: tuple[int, str] | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved = TaskGroupListResolved(
        search=search,
        status=None if status_filter is None else status_filter[1],
        page_no=page_no,
        page_size=page_size,
        all=all_pages,
    )
    if project is not None:
        selected_project = SelectedValue(value=project, source="flag")
        resolved_project = resolve_project(
            selected_project.value,
            adapter=runtime.upstream.projects,
        )
        data: TaskGroupPageData = requested_page_data(
            lambda current_page_no, current_page_size: adapter.list_by_project(
                project_code=resolved_project.code,
                page_no=current_page_no,
                page_size=current_page_size,
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            serialize_item=serialize_task_group,
            resource=TASK_GROUP_RESOURCE,
            max_pages=MAX_AUTO_EXHAUST_PAGES,
            translate_error=lambda error: _translate_task_group_api_error(
                error,
                operation="list",
                project_code=resolved_project.code,
            ),
        )
        resolved["project"] = _selected_project_data(
            resolved_project.to_data(),
            selected_project,
        )
    else:
        data = requested_page_data(
            lambda current_page_no, current_page_size: adapter.list(
                page_no=current_page_no,
                page_size=current_page_size,
                search=search,
                status=None if status_filter is None else status_filter[0],
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            serialize_item=serialize_task_group,
            resource=TASK_GROUP_RESOURCE,
            max_pages=MAX_AUTO_EXHAUST_PAGES,
            translate_error=lambda error: _translate_task_group_api_error(
                error,
                operation="list",
            ),
        )

    return CommandResult(
        data=require_json_object(data, label="task-group list data"),
        resolved=require_json_object(resolved, label="task-group list resolved"),
    )


def _get_task_group_result(
    runtime: ServiceRuntime,
    *,
    task_group: str,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved_task_group = resolve_task_group(task_group, adapter=adapter)
    fetched = adapter.get(task_group_id=resolved_task_group.id)
    return CommandResult(
        data=require_json_object(
            serialize_task_group(fetched),
            label="task-group data",
        ),
        resolved={
            "taskGroup": require_json_object(
                resolved_task_group.to_data(),
                label="resolved task-group",
            )
        },
    )


def _create_task_group_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    group_size: int,
    project: str | None,
    description: str,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    resolved_project = resolve_project(
        selected_project.value,
        adapter=runtime.upstream.projects,
    )
    adapter = runtime.upstream.task_groups
    try:
        created = adapter.create(
            project_code=resolved_project.code,
            name=name,
            description=description,
            group_size=group_size,
        )
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="create",
            task_group_name=name,
            project_code=resolved_project.code,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_task_group(created),
            label="task-group data",
        ),
        resolved={
            "project": require_json_object(
                _selected_project_data(resolved_project.to_data(), selected_project),
                label="resolved project",
            ),
            "taskGroup": require_json_object(
                _resolved_task_group_data_from_payload(created),
                label="resolved task-group",
            ),
        },
    )


def _update_task_group_result(
    runtime: ServiceRuntime,
    *,
    task_group: str,
    name: str | None,
    group_size: int | None,
    description: DescriptionUpdate,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved_task_group = resolve_task_group(task_group, adapter=adapter)
    current = adapter.get(task_group_id=resolved_task_group.id)
    next_name = (
        require_resource_text(
            current.name,
            resource=TASK_GROUP_RESOURCE,
            field_name="task_group.name",
        )
        if name is None
        else name
    )
    if isinstance(description, _UnsetValue):
        next_description = "" if current.description is None else current.description
    else:
        next_description = description
    next_group_size = current.groupSize if group_size is None else group_size
    if (
        next_name == current.name
        and next_description == (current.description or "")
        and next_group_size == current.groupSize
    ):
        message = "Task-group update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass a different --name, --group-size, or --description value."
            ),
        )

    try:
        updated = adapter.update(
            task_group_id=resolved_task_group.id,
            name=next_name,
            description=next_description,
            group_size=next_group_size,
        )
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="update",
            task_group_id=resolved_task_group.id,
            task_group_name=next_name,
            project_code=resolved_task_group.projectCode,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_task_group(updated),
            label="task-group data",
        ),
        resolved={
            "taskGroup": require_json_object(
                resolved_task_group.to_data(),
                label="resolved task-group",
            )
        },
    )


def _close_task_group_result(
    runtime: ServiceRuntime,
    *,
    task_group: str,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved_task_group = resolve_task_group(task_group, adapter=adapter)
    try:
        adapter.close(task_group_id=resolved_task_group.id)
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="close",
            task_group_id=resolved_task_group.id,
            task_group_name=resolved_task_group.name,
            project_code=resolved_task_group.projectCode,
        ) from error

    updated = adapter.get(task_group_id=resolved_task_group.id)
    return CommandResult(
        data=require_json_object(
            serialize_task_group(updated),
            label="task-group data",
        ),
        resolved={
            "taskGroup": require_json_object(
                resolved_task_group.to_data(),
                label="resolved task-group",
            )
        },
    )


def _start_task_group_result(
    runtime: ServiceRuntime,
    *,
    task_group: str,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved_task_group = resolve_task_group(task_group, adapter=adapter)
    try:
        adapter.start(task_group_id=resolved_task_group.id)
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="start",
            task_group_id=resolved_task_group.id,
            task_group_name=resolved_task_group.name,
            project_code=resolved_task_group.projectCode,
        ) from error

    updated = adapter.get(task_group_id=resolved_task_group.id)
    return CommandResult(
        data=require_json_object(
            serialize_task_group(updated),
            label="task-group data",
        ),
        resolved={
            "taskGroup": require_json_object(
                resolved_task_group.to_data(),
                label="resolved task-group",
            )
        },
    )


def _list_task_group_queues_result(
    runtime: ServiceRuntime,
    *,
    task_group: str,
    task_instance: str | None,
    workflow_instance: str | None,
    status_filter: tuple[int, str] | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    resolved_task_group = resolve_task_group(task_group, adapter=adapter)
    data: TaskGroupQueuePageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list_queues(
            group_id=resolved_task_group.id,
            task_instance_name=task_instance,
            workflow_instance_name=workflow_instance,
            status=None if status_filter is None else status_filter[0],
            page_no=current_page_no,
            page_size=current_page_size,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_task_group_queue,
        resource=TASK_GROUP_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_task_group_api_error(
            error,
            operation="queue.list",
            task_group_id=resolved_task_group.id,
            task_group_name=resolved_task_group.name,
            project_code=resolved_task_group.projectCode,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="task-group queue list data"),
        resolved={
            "taskGroup": require_json_object(
                resolved_task_group.to_data(),
                label="resolved task-group",
            ),
            "taskInstance": task_instance,
            "workflowInstance": workflow_instance,
            "status": None if status_filter is None else status_filter[1],
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _force_start_task_group_queue_result(
    runtime: ServiceRuntime,
    *,
    queue_id: int,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    try:
        adapter.force_start(queue_id=queue_id)
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="queue.force-start",
            queue_id=queue_id,
        ) from error

    data: TaskGroupQueueForceStartData = {
        "queueId": queue_id,
        "forceStarted": True,
    }
    return CommandResult(data=require_json_object(data, label="queue force-start data"))


def _set_task_group_queue_priority_result(
    runtime: ServiceRuntime,
    *,
    queue_id: int,
    priority: int,
) -> CommandResult:
    adapter = runtime.upstream.task_groups
    try:
        adapter.set_queue_priority(queue_id=queue_id, priority=priority)
    except ApiResultError as error:
        raise _translate_task_group_api_error(
            error,
            operation="queue.set-priority",
            queue_id=queue_id,
        ) from error

    data: TaskGroupQueuePriorityData = {
        "queueId": queue_id,
        "priority": priority,
    }
    return CommandResult(
        data=require_json_object(data, label="queue priority data"),
    )


def _selected_project_data(
    project: ResolvedProjectData,
    selected_project: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(
        {
            "code": project["code"],
            "name": project["name"],
            "description": project["description"],
        },
        selected_project,
    )


def _resolved_task_group_data_from_payload(
    task_group: TaskGroupRecord,
) -> ResolvedTaskGroupData:
    payload = serialize_task_group(task_group)
    return {
        "id": payload["id"],
        "name": require_resource_text(
            payload["name"],
            resource=TASK_GROUP_RESOURCE,
            field_name="task_group.name",
        ),
        "projectCode": payload["projectCode"],
    }


def _task_group_description(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _normalized_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _parse_task_group_status_filter(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    normalized = value.strip().lower().replace("_", "-")
    mapping = {
        "1": (1, "open"),
        "open": (1, "open"),
        "opened": (1, "open"),
        "yes": (1, "open"),
        "0": (0, "closed"),
        "close": (0, "closed"),
        "closed": (0, "closed"),
        "no": (0, "closed"),
    }
    parsed = mapping.get(normalized)
    if parsed is None:
        message = "Task-group status filter must be one of: open, closed, 1, 0"
        raise UserInputError(
            message,
            suggestion="Pass `open`/`closed` or `1`/`0`.",
        )
    return parsed


def _parse_task_group_queue_status_filter(
    value: str | None,
) -> tuple[int, str] | None:
    if value is None:
        return None
    normalized = value.strip().lower().replace("_", "-")
    mapping = {
        "-1": (-1, "WAIT_QUEUE"),
        "wait-queue": (-1, "WAIT_QUEUE"),
        "1": (1, "ACQUIRE_SUCCESS"),
        "acquire-success": (1, "ACQUIRE_SUCCESS"),
        "2": (2, "RELEASE"),
        "release": (2, "RELEASE"),
    }
    parsed = mapping.get(normalized)
    if parsed is None:
        message = (
            "Task-group queue status filter must be one of: WAIT_QUEUE, "
            "ACQUIRE_SUCCESS, RELEASE, -1, 1, 2"
        )
        raise UserInputError(
            message,
            suggestion=(
                "Pass `WAIT_QUEUE`, `ACQUIRE_SUCCESS`, `RELEASE`, or one of "
                "`-1`, `1`, `2`."
            ),
        )
    return parsed


def _translate_task_group_api_error(
    error: ApiResultError,
    *,
    operation: str,
    task_group_id: int | None = None,
    task_group_name: str | None = None,
    project_code: int | None = None,
    queue_id: int | None = None,
) -> Exception:
    result_code = error.result_code
    if result_code == TASK_GROUP_NAME_EXISTS:
        return ConflictError(
            f"Task-group name {task_group_name!r} already exists",
            details={"resource": TASK_GROUP_RESOURCE, "name": task_group_name},
        )
    if result_code == TASK_GROUP_SIZE_ERROR:
        return UserInputError(
            "Task-group group_size must be greater than or equal to 1",
            details={"resource": TASK_GROUP_RESOURCE},
            suggestion="Pass `--group-size` with a value greater than or equal to 1.",
        )
    if result_code == TASK_GROUP_STATUS_ERROR:
        identifier = _task_group_identifier(
            task_group_name=task_group_name,
            task_group_id=task_group_id,
        )
        return InvalidStateError(
            "Task-group is closed and cannot be updated",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "id": task_group_id,
                "name": task_group_name,
            },
            suggestion=(
                f"Run `dsctl task-group start {identifier}` first, then retry "
                "`task-group update`."
            ),
        )
    if result_code == TASK_GROUP_STATUS_CLOSED:
        identifier = _task_group_identifier(
            task_group_name=task_group_name,
            task_group_id=task_group_id,
        )
        return InvalidStateError(
            "Task-group is already closed",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "id": task_group_id,
                "name": task_group_name,
            },
            suggestion=f"Run `dsctl task-group start {identifier}` to reopen it.",
        )
    if result_code == TASK_GROUP_STATUS_OPENED:
        identifier = _task_group_identifier(
            task_group_name=task_group_name,
            task_group_id=task_group_id,
        )
        return InvalidStateError(
            "Task-group is already open",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "id": task_group_id,
                "name": task_group_name,
            },
            suggestion=(
                f"No need to start it again. Run `dsctl task-group close "
                f"{identifier}` if you want to stop new task-group allocations."
            ),
        )
    if result_code == TASK_GROUP_QUEUE_ALREADY_START:
        return InvalidStateError(
            "Task-group queue item has already acquired task-group resources",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "queue_id": queue_id,
            },
            suggestion=(
                "No need to force-start it again; the queue item has already "
                "acquired task-group resources."
            ),
        )
    if result_code in (
        USER_NO_OPERATION_PROJECT_PERM,
        USER_NO_WRITE_PROJECT_PERM,
        NO_CURRENT_OPERATING_PERMISSION,
    ):
        return PermissionDeniedError(
            "Current user does not have permission for this task-group operation",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "operation": operation,
                "id": task_group_id,
                "name": task_group_name,
                "project_code": project_code,
                "queue_id": queue_id,
            },
        )
    if result_code == DESCRIPTION_TOO_LONG_ERROR:
        return UserInputError(
            "Task-group description is too long",
            details={"resource": TASK_GROUP_RESOURCE, "operation": operation},
            suggestion="Shorten `--description`, then retry the same command.",
        )
    if result_code in (
        CREATE_TASK_GROUP_ERROR,
        UPDATE_TASK_GROUP_ERROR,
        QUERY_TASK_GROUP_LIST_ERROR,
        CLOSE_TASK_GROUP_ERROR,
        START_TASK_GROUP_ERROR,
        QUERY_TASK_GROUP_QUEUE_LIST_ERROR,
    ):
        return InvalidStateError(
            f"Task-group {operation} failed",
            details={
                "resource": TASK_GROUP_RESOURCE,
                "operation": operation,
                "id": task_group_id,
                "name": task_group_name,
                "project_code": project_code,
                "queue_id": queue_id,
                "result_code": result_code,
            },
            source=error.to_payload(),
            suggestion=(
                "Inspect the current task-group state and queue rows, then retry "
                "once the upstream state is ready for this operation."
            ),
        )
    return error
