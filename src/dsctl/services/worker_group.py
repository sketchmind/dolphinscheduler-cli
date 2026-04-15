from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import WORKER_GROUP_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    optional_text,
    serialize_worker_group,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageResult,
    collect_page,
    materialize_page_data,
    render_page_data,
)
from dsctl.services.resolver import ResolvedWorkerGroupData
from dsctl.services.resolver import worker_group as resolve_worker_group
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import WorkerGroupOperations, WorkerGroupRecord


NAME_NULL = 10134
NAME_EXIST = 10135
DELETE_WORKER_GROUP_BY_ID_FAIL = 10145
DELETE_WORKER_GROUP_NOT_EXIST = 10174
WORKER_ADDRESS_INVALID = 10177
USER_NO_OPERATION_PERM = 30001
DELETE_WORKER_GROUP_BY_ID_FAIL_ENV = 1400005
WORKER_GROUP_DEPENDENT_TASK_EXISTS = 1401000
WORKER_GROUP_DEPENDENT_SCHEDULER_EXISTS = 1401001
WORKER_GROUP_DEPENDENT_ENVIRONMENT_EXISTS = 1401002
WORKER_GROUP_NOT_EXIST = 1402001


class DeleteWorkerGroupData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    workerGroup: ResolvedWorkerGroupData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue
AddressesUpdate = list[str] | _UnsetValue

_WORKER_GROUP_READ_ONLY_SUGGESTION = (
    "Run `dsctl worker-group list` to select a DB-backed worker group row. "
    "Config-derived worker groups are read-only in CRUD APIs."
)


def list_worker_groups_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List worker groups with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_worker_groups_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_worker_group_result(
    worker_group: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one worker-group payload."""
    return run_with_service_runtime(
        env_file,
        _get_worker_group_result,
        worker_group=worker_group,
    )


def create_worker_group_result(
    *,
    name: str,
    addresses: Sequence[str] | None = None,
    description: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one worker group from validated CLI input."""
    normalized_name = require_non_empty_text(name, label="worker group name")
    normalized_addresses = _normalize_addresses(addresses or [])
    normalized_description = optional_text(description)

    return run_with_service_runtime(
        env_file,
        _create_worker_group_result,
        name=normalized_name,
        addr_list=_worker_group_addr_list(normalized_addresses),
        description=normalized_description,
    )


def update_worker_group_result(
    worker_group: str,
    *,
    name: str | None = None,
    addresses: AddressesUpdate = UNSET,
    description: DescriptionUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one worker group while preserving omitted fields."""
    if name is None and addresses is UNSET and description is UNSET:
        message = "Worker-group update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at least one update flag such as --name, --addr, or "
                "--description."
            ),
        )

    normalized_name = (
        require_non_empty_text(name, label="worker group name")
        if name is not None
        else None
    )
    normalized_addresses = (
        _normalize_addresses(addresses)
        if not isinstance(addresses, _UnsetValue)
        else UNSET
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )

    return run_with_service_runtime(
        env_file,
        _update_worker_group_result,
        worker_group=worker_group,
        name=normalized_name,
        addresses=normalized_addresses,
        description=normalized_description,
    )


def delete_worker_group_result(
    worker_group: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one worker group after explicit confirmation."""
    require_delete_force(force=force, resource_label="Worker-group")

    return run_with_service_runtime(
        env_file,
        _delete_worker_group_result,
        worker_group=worker_group,
    )


def _list_worker_groups_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.worker_groups
    if all_pages:
        page = _collect_all_worker_group_pages(
            adapter,
            search=search,
            page_no=page_no,
            page_size=page_size,
        )
        data = materialize_page_data(
            page,
            serialize_item=serialize_worker_group,
            resource=WORKER_GROUP_RESOURCE,
        )
    else:
        data = render_page_data(
            adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=search,
            ),
            serialize_item=serialize_worker_group,
            resource=WORKER_GROUP_RESOURCE,
        )

    return CommandResult(
        data=require_json_object(data, label="worker-group list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_worker_group_result(
    runtime: ServiceRuntime,
    *,
    worker_group: str,
) -> CommandResult:
    adapter = runtime.upstream.worker_groups
    resolved_worker_group = resolve_worker_group(worker_group, adapter=adapter)
    return CommandResult(
        data=require_json_object(
            serialize_worker_group(resolved_worker_group),
            label="worker-group data",
        ),
        resolved={
            "workerGroup": require_json_object(
                resolved_worker_group.to_data(),
                label="resolved worker group",
            )
        },
    )


def _create_worker_group_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    addr_list: str,
    description: str | None,
) -> CommandResult:
    adapter = runtime.upstream.worker_groups
    try:
        created_worker_group = adapter.create(
            name=name,
            addr_list=addr_list,
            description=description,
        )
    except ApiResultError as error:
        raise _translate_worker_group_api_error(
            error,
            operation="create",
            worker_group_name=name,
            addr_list=addr_list,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_worker_group(created_worker_group),
            label="worker-group data",
        ),
        resolved={
            "workerGroup": require_json_object(
                _resolved_worker_group_data_from_record(created_worker_group),
                label="resolved worker group",
            )
        },
    )


def _update_worker_group_result(
    runtime: ServiceRuntime,
    *,
    worker_group: str,
    name: str | None,
    addresses: AddressesUpdate,
    description: DescriptionUpdate,
) -> CommandResult:
    adapter = runtime.upstream.worker_groups
    resolved_worker_group = resolve_worker_group(worker_group, adapter=adapter)
    if resolved_worker_group.system_default or resolved_worker_group.id is None:
        message = "Config-derived worker groups cannot be updated through CRUD APIs"
        raise InvalidStateError(
            message,
            details=resolved_worker_group.to_details(),
            suggestion=_WORKER_GROUP_READ_ONLY_SUGGESTION,
        )

    current_description = optional_text(resolved_worker_group.description)
    current_addr_list = resolved_worker_group.addr_list or ""
    next_name = resolved_worker_group.name if name is None else name
    next_addr_list = (
        current_addr_list
        if isinstance(addresses, _UnsetValue)
        else _worker_group_addr_list(addresses)
    )
    next_description = (
        current_description if isinstance(description, _UnsetValue) else description
    )
    if (
        next_name == resolved_worker_group.name
        and next_addr_list == current_addr_list
        and next_description == current_description
    ):
        message = "Worker-group update requires at least one field change"
        raise UserInputError(
            message,
            suggestion=("Pass a different --name, --addr, or --description value."),
        )

    try:
        updated_worker_group = adapter.update(
            worker_group_id=resolved_worker_group.id,
            name=next_name,
            addr_list=next_addr_list,
            description=next_description,
        )
    except ApiResultError as error:
        raise _translate_worker_group_api_error(
            error,
            operation="update",
            worker_group_id=resolved_worker_group.id,
            worker_group_name=next_name,
            addr_list=next_addr_list,
        ) from error

    return CommandResult(
        data=require_json_object(
            serialize_worker_group(updated_worker_group),
            label="worker-group data",
        ),
        resolved={
            "workerGroup": require_json_object(
                resolved_worker_group.to_data(),
                label="resolved worker group",
            )
        },
    )


def _delete_worker_group_result(
    runtime: ServiceRuntime,
    *,
    worker_group: str,
) -> CommandResult:
    adapter = runtime.upstream.worker_groups
    resolved_worker_group = resolve_worker_group(worker_group, adapter=adapter)
    if resolved_worker_group.system_default or resolved_worker_group.id is None:
        message = "Config-derived worker groups cannot be deleted through CRUD APIs"
        raise InvalidStateError(
            message,
            details=resolved_worker_group.to_details(),
            suggestion=_WORKER_GROUP_READ_ONLY_SUGGESTION,
        )

    try:
        deleted = adapter.delete(worker_group_id=resolved_worker_group.id)
    except ApiResultError as error:
        raise _translate_worker_group_api_error(
            error,
            operation="delete",
            worker_group_id=resolved_worker_group.id,
            worker_group_name=resolved_worker_group.name,
            addr_list=resolved_worker_group.addr_list,
        ) from error

    data: DeleteWorkerGroupData = {
        "deleted": deleted,
        "workerGroup": resolved_worker_group.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="worker-group delete data"),
        resolved={
            "workerGroup": require_json_object(
                resolved_worker_group.to_data(),
                label="resolved worker group",
            )
        },
    )


def _collect_all_worker_group_pages(
    adapter: WorkerGroupOperations,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
) -> PageResult[WorkerGroupRecord]:
    first_page = adapter.list(
        page_no=page_no,
        page_size=page_size,
        search=search,
    )
    first_result = collect_page(first_page)
    last_page = first_result.total_pages or first_result.page_no or page_no
    first_page_no = first_result.page_no or page_no
    page_count = last_page - first_page_no + 1
    if page_count > MAX_AUTO_EXHAUST_PAGES:
        message = "Refusing to auto-exhaust more pages than the safety limit"
        raise UserInputError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page,
                "max_pages": MAX_AUTO_EXHAUST_PAGES,
            },
            suggestion=(
                "Retry without --all, or narrow the result set with --search "
                "before auto-exhausting pages again."
            ),
        )

    seen: set[str] = set()
    items: list[WorkerGroupRecord] = []
    _append_unique_worker_groups(items, seen, first_result.items)
    for current_page_no in range(first_page_no + 1, last_page + 1):
        next_page = adapter.list(
            page_no=current_page_no,
            page_size=page_size,
            search=search,
        )
        _append_unique_worker_groups(items, seen, next_page.totalList or [])

    return PageResult(
        items=items,
        page_no=first_result.page_no,
        page_size=first_result.page_size,
        total=len(items),
        total_pages=1 if items else 0,
        fetched_all=True,
    )


def _append_unique_worker_groups(
    collected: list[WorkerGroupRecord],
    seen: set[str],
    worker_groups: Sequence[WorkerGroupRecord],
) -> None:
    for worker_group in worker_groups:
        identity = _worker_group_identity(worker_group)
        if identity in seen:
            continue
        seen.add(identity)
        collected.append(worker_group)


def _worker_group_identity(worker_group: WorkerGroupRecord) -> str:
    if worker_group.id is not None:
        return f"id:{worker_group.id}"
    if worker_group.addrList is not None:
        return f"addr:{worker_group.addrList}"
    if worker_group.name is not None:
        return f"name:{worker_group.name}"
    message = "Worker-group payload was missing both id and fallback identity fields"
    raise ApiTransportError(message)


def _normalize_addresses(addresses: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for address in addresses:
        normalized_address = require_non_empty_text(address, label="worker address")
        if normalized_address in seen:
            continue
        seen.add(normalized_address)
        normalized.append(normalized_address)
    return normalized


def _worker_group_addr_list(addresses: Sequence[str]) -> str:
    return ",".join(addresses)


def _resolved_worker_group_data_from_record(
    worker_group: WorkerGroupRecord,
) -> ResolvedWorkerGroupData:
    worker_group_name = worker_group.name
    if worker_group_name is None:
        message = "Worker-group payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": WORKER_GROUP_RESOURCE},
        )
    return {
        "id": worker_group.id,
        "name": worker_group_name,
        "addrList": worker_group.addrList,
        "systemDefault": worker_group.systemDefault,
    }


def _translate_worker_group_api_error(
    error: ApiResultError,
    *,
    operation: str,
    worker_group_id: int | None = None,
    worker_group_name: str | None = None,
    addr_list: str | None = None,
) -> Exception:
    details: dict[str, str | int] = {"operation": operation}
    if worker_group_id is not None:
        details["id"] = worker_group_id
    if worker_group_name is not None:
        details["name"] = worker_group_name
    if addr_list is not None:
        details["addrList"] = addr_list

    if error.result_code in (WORKER_GROUP_NOT_EXIST, DELETE_WORKER_GROUP_NOT_EXIST):
        identifier = (
            worker_group_id if worker_group_id is not None else worker_group_name
        )
        message = f"Worker group {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == NAME_EXIST:
        message = "Worker-group create/update conflicted with an existing name"
        return ConflictError(message, details=details)
    if error.result_code in (
        DELETE_WORKER_GROUP_BY_ID_FAIL,
        DELETE_WORKER_GROUP_BY_ID_FAIL_ENV,
        WORKER_GROUP_DEPENDENT_TASK_EXISTS,
        WORKER_GROUP_DEPENDENT_SCHEDULER_EXISTS,
        WORKER_GROUP_DEPENDENT_ENVIRONMENT_EXISTS,
    ):
        message = "Worker group is still in use and cannot be deleted"
        return ConflictError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Worker-group {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code in (NAME_NULL, WORKER_ADDRESS_INVALID):
        message = "Worker-group input was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion=(
                "Verify `--name` is non-empty and each `--addr` value is a valid "
                "worker address such as `host:1234`, then retry."
            ),
        )
    return error
