from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import QUEUE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import optional_text, serialize_queue
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedQueueData
from dsctl.services.resolver import queue as resolve_queue
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import QueueRecord


QUEUE_NOT_EXIST = 10128
QUEUE_VALUE_EXIST = 10129
QUEUE_NAME_EXIST = 10130
NEED_NOT_UPDATE_QUEUE = 10132
DELETE_QUEUE_BY_ID_FAIL_USERS = 10308
DELETE_QUEUE_BY_ID_FAIL_TENANTS = 10309
USER_NO_OPERATION_PERM = 30001


class DeleteQueueData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    queue: ResolvedQueueData


def list_queues_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List queues with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_queues_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_queue_result(
    queue: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one queue payload."""
    return run_with_service_runtime(
        env_file,
        _get_queue_result,
        queue=queue,
    )


def create_queue_result(
    *,
    queue_name: str,
    queue: str,
    env_file: str | None = None,
) -> CommandResult:
    """Create one queue from validated CLI input."""
    normalized_queue_name = require_non_empty_text(
        queue_name,
        label="queue name",
    )
    normalized_queue = require_non_empty_text(
        queue,
        label="queue",
    )

    return run_with_service_runtime(
        env_file,
        _create_queue_result,
        queue_name=normalized_queue_name,
        queue=normalized_queue,
    )


def update_queue_result(
    queue_identifier: str,
    *,
    queue_name: str | None = None,
    queue: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one queue while preserving omitted fields."""
    if queue_name is None and queue is None:
        message = "Queue update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass at least one update flag such as --queue-name or --queue.",
        )

    normalized_queue_name = (
        require_non_empty_text(queue_name, label="queue name")
        if queue_name is not None
        else None
    )
    normalized_queue = (
        require_non_empty_text(queue, label="queue") if queue is not None else None
    )

    return run_with_service_runtime(
        env_file,
        _update_queue_result,
        queue_identifier=queue_identifier,
        queue_name=normalized_queue_name,
        queue=normalized_queue,
    )


def delete_queue_result(
    queue: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one queue after explicit confirmation."""
    require_delete_force(force=force, resource_label="Queue")

    return run_with_service_runtime(
        env_file,
        _delete_queue_result,
        queue=queue,
    )


def _list_queues_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.queues
    data = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_queue,
        resource=QUEUE_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_queue_api_error(
            error,
            operation="list",
            queue_name=search,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="queue list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_queue_result(
    runtime: ServiceRuntime,
    *,
    queue: str,
) -> CommandResult:
    adapter = runtime.upstream.queues
    resolved_queue = resolve_queue(queue, adapter=adapter)
    fetched_queue = adapter.get(queue_id=resolved_queue.id)
    return CommandResult(
        data=require_json_object(
            serialize_queue(fetched_queue),
            label="queue data",
        ),
        resolved={
            "queue": require_json_object(
                resolved_queue.to_data(),
                label="resolved queue",
            )
        },
    )


def _create_queue_result(
    runtime: ServiceRuntime,
    *,
    queue_name: str,
    queue: str,
) -> CommandResult:
    adapter = runtime.upstream.queues
    try:
        created_queue = adapter.create(queue=queue, queue_name=queue_name)
    except ApiResultError as error:
        raise _translate_queue_api_error(
            error,
            operation="create",
            queue_name=queue_name,
            queue=queue,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_queue(created_queue),
            label="queue data",
        ),
        resolved={
            "queue": require_json_object(
                _resolved_queue_data_from_record(created_queue),
                label="resolved queue",
            )
        },
    )


def _update_queue_result(
    runtime: ServiceRuntime,
    *,
    queue_identifier: str,
    queue_name: str | None,
    queue: str | None,
) -> CommandResult:
    adapter = runtime.upstream.queues
    resolved_queue = resolve_queue(queue_identifier, adapter=adapter)
    current_queue = adapter.get(queue_id=resolved_queue.id)
    next_queue_name = current_queue.queueName if queue_name is None else queue_name
    next_queue = current_queue.queue if queue is None else queue
    if next_queue_name is None or next_queue is None:
        message = "Queue payload was missing required fields"
        raise ApiTransportError(
            message,
            details={"resource": QUEUE_RESOURCE, "id": resolved_queue.id},
        )
    if next_queue_name == current_queue.queueName and next_queue == current_queue.queue:
        message = "Queue update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass a different --queue-name or --queue value.",
        )

    try:
        updated_queue = adapter.update(
            queue_id=resolved_queue.id,
            queue=next_queue,
            queue_name=next_queue_name,
        )
    except ApiResultError as error:
        raise _translate_queue_api_error(
            error,
            operation="update",
            queue_id=resolved_queue.id,
            queue_name=next_queue_name,
            queue=next_queue,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_queue(updated_queue),
            label="queue data",
        ),
        resolved={
            "queue": require_json_object(
                resolved_queue.to_data(),
                label="resolved queue",
            )
        },
    )


def _delete_queue_result(
    runtime: ServiceRuntime,
    *,
    queue: str,
) -> CommandResult:
    adapter = runtime.upstream.queues
    resolved_queue = resolve_queue(queue, adapter=adapter)
    try:
        deleted = adapter.delete(queue_id=resolved_queue.id)
    except ApiResultError as error:
        raise _translate_queue_api_error(
            error,
            operation="delete",
            queue_id=resolved_queue.id,
            queue_name=resolved_queue.queue_name,
            queue=resolved_queue.queue,
        ) from error

    data: DeleteQueueData = {
        "deleted": deleted,
        "queue": resolved_queue.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="queue delete data"),
        resolved={
            "queue": require_json_object(
                resolved_queue.to_data(),
                label="resolved queue",
            )
        },
    )


def _resolved_queue_data_from_record(queue_record: QueueRecord) -> ResolvedQueueData:
    queue_id = queue_record.id
    queue_name = queue_record.queueName
    if queue_id is None or queue_name is None:
        message = "Queue payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": QUEUE_RESOURCE},
        )
    return {
        "id": queue_id,
        "queueName": queue_name,
        "queue": queue_record.queue,
    }


def _translate_queue_api_error(
    error: ApiResultError,
    *,
    operation: str,
    queue_id: int | None = None,
    queue_name: str | None = None,
    queue: str | None = None,
) -> Exception:
    details: dict[str, str | int] = {"operation": operation}
    if queue_id is not None:
        details["id"] = queue_id
    if queue_name is not None:
        details["queueName"] = queue_name
    if queue is not None:
        details["queue"] = queue

    if error.result_code == QUEUE_NOT_EXIST:
        identifier = queue_id if queue_id is not None else queue_name
        message = f"Queue {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code in (QUEUE_VALUE_EXIST, QUEUE_NAME_EXIST):
        message = "Queue create/update conflicted with an existing queue"
        return ConflictError(message, details=details)
    if error.result_code in (
        DELETE_QUEUE_BY_ID_FAIL_USERS,
        DELETE_QUEUE_BY_ID_FAIL_TENANTS,
    ):
        message = "Queue is still in use and cannot be deleted"
        return ConflictError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Queue {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code == NEED_NOT_UPDATE_QUEUE:
        message = "Queue update requires at least one field change"
        return UserInputError(
            message,
            details=details,
            suggestion="Pass a different --queue-name or --queue value.",
        )
    return error
